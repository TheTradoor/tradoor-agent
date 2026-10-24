"""Supervisor: boots everything, restarts crashed components, runs the watchdog.

Lifecycle
---------
1. refuse to start if data/KILL exists (a human killed it; a human must clear it)
2. open db, build components, spawn each in a restart loop with backoff
3. watchdog every N sec: KILL file -> kill; stale heartbeat -> alert
4. on SIGINT/SIGTERM/stop: cancel components, close db
"""
from __future__ import annotations

import asyncio
import logging
import signal

from .bus import EventBus
from .component import Component, Context
from .config import Settings
from .db import Database
from .execution.paper import PaperBroker
from .housekeeper import Housekeeper
from .ingest.gmgn import GmgnClient
from .ingest.pumpportal import PumpPortalIngest
from .ingest.triggers import DexPaidDetector, MigrationTrigger, TrendingDetector
from .logging_setup import setup_logging
from .ops.telegram import TelegramOps
from .ops.web import WebDashboard
from .params import ParamStore
from .positions.manager import PositionManager
from .screener import Screener
from .scoring.scorer import Scorer
from .state import RuntimeState
from .tracker import OutcomeTracker
from .trader import Trader

log = logging.getLogger("agent.supervisor")


class KillFilePresent(RuntimeError):
    pass


def build_gmgn(settings: Settings) -> GmgnClient:
    return GmgnClient(
        settings.gmgn_api_key, settings.gmgn_host,
        rate_weight=settings.gmgn_rate_weight, budget_fraction=settings.gmgn_budget_fraction,
        cache_sec=settings.gmgn_cache_sec, timeout_sec=settings.gmgn_timeout_sec,
    )


def build_components(ctx: Context) -> list[Component]:
    """Phase 0: ops + housekeeping. Phase 1: PumpPortal ingest + GMGN screener."""
    comps: list[Component] = [Housekeeper(ctx)]
    ingest = PumpPortalIngest(ctx)
    ctx.ingest = ingest
    comps.append(ingest)
    if ctx.settings.gmgn_api_key:
        gmgn = build_gmgn(ctx.settings)
        screener = Screener(ctx, gmgn, ingest)
        ctx.gmgn, ctx.screener = gmgn, screener
        comps.append(screener)
        # phase 2: score -> risk -> paper broker -> position manager (all modes use the paper broker until phase 3)
        broker = PaperBroker(ctx, gmgn)
        # v0.8 trigger lanes + the outcome tracker that grades them
        dexpaid, trending, migration = DexPaidDetector(ctx), TrendingDetector(ctx, gmgn), MigrationTrigger(ctx)
        tracker = OutcomeTracker(ctx, gmgn)
        ctx.dexpaid, ctx.trending, ctx.tracker = dexpaid, trending, tracker
        comps += [Scorer(ctx), Trader(ctx, broker, ingest, gmgn), PositionManager(ctx, gmgn, broker, ingest), dexpaid, trending, migration, tracker]
    else:
        log.warning("GMGN_API_KEY kosong: screener/scorer/trader tidak dijalankan, hanya ingest")
    if ctx.settings.web_enabled:
        comps.append(WebDashboard(ctx))
    comps.append(TelegramOps(ctx))   # last: its startup message can include the dashboard URL
    return comps


class Supervisor:
    def __init__(self, settings: Settings, *, configure_logging: bool = True) -> None:
        self.settings = settings
        if configure_logging:
            setup_logging(settings.log_level, settings.data_dir)
        self.state = RuntimeState(mode=settings.mode)
        self.bus = EventBus()
        self.db = Database(settings.db_path, settings.timezone)
        self.params = ParamStore(self.db)
        self.ctx = Context(settings=self.settings, state=self.state, bus=self.bus, db=self.db, params=self.params)
        self._tasks: list[asyncio.Task] = []
        self._stale_alerted: set[str] = set()

    # ------------------------------------------------------------ public

    async def start(self) -> None:
        s = self.settings
        if s.kill_file.exists():
            raise KillFilePresent(
                f"KILL file ada di {s.kill_file} ({s.kill_file.read_text().strip()}). "
                "Hapus file itu kalau memang mau nyalain lagi."
            )

        self._install_signal_handlers()
        await self.db.open()
        await self.params.refresh(force=True)
        await self.db.log_event("agent.start", None, {"mode": s.mode.value})
        log.info("TheTradoor Agent starting · mode=%s · data=%s", s.mode.value, s.data_dir.resolve())

        for comp in build_components(self.ctx):
            self._tasks.append(asyncio.create_task(self._run_forever(comp), name=comp.name))
        self._tasks.append(asyncio.create_task(self._watchdog(), name="watchdog"))

        await self.state.stop.wait()
        await self._shutdown()

    def request_stop(self) -> None:
        self.state.request_stop()

    # ------------------------------------------------------------ internals

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
            except (NotImplementedError, RuntimeError):  # windows / nested loops
                pass

    def _on_signal(self, sig: signal.Signals) -> None:
        log.warning("signal %s received, stopping", sig.name)
        self.state.request_stop()

    async def _run_forever(self, comp: Component) -> None:
        """Run a component; on crash, restart with exponential backoff (max 60s)."""
        failures = 0
        while not self.state.stop.is_set():
            try:
                comp.beat()
                await comp.run()
                if self.state.stop.is_set():
                    return
                log.warning("%s exited without stop; restarting", comp.name)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                failures += 1
                self.state.bump(f"crash.{comp.name}")
                log.exception("%s crashed (#%d)", comp.name, failures)
                self.bus.alert(f"{comp.name} crashed (#{failures}): {type(e).__name__}: {e}", "error")
            delay = min(60, 2 ** min(failures, 6))
            try:
                await asyncio.wait_for(self.state.stop.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass

    async def _watchdog(self) -> None:
        s = self.settings
        while not self.state.stop.is_set():
            try:
                if s.kill_file.exists() and not self.state.kill_requested:
                    reason = f"kill file: {s.kill_file.read_text().strip() or 'manual'}"
                    log.critical("%s", reason)
                    self.bus.alert(f"KILL file detected, stopping. {reason}", "error")
                    self.state.request_kill(reason)
                    break

                stale = self.state.stale_components(s.heartbeat_stale_sec)
                for name in stale:
                    if name not in self._stale_alerted:
                        self._stale_alerted.add(name)
                        log.error("component %s heartbeat stale", name)
                        self.bus.alert(f"{name} heartbeat stale > {s.heartbeat_stale_sec}s", "warn")
                for name in list(self._stale_alerted):
                    if name not in stale:
                        self._stale_alerted.discard(name)
                        self.bus.alert(f"{name} recovered", "info")
            except Exception:
                log.exception("watchdog iteration failed")
            try:
                await asyncio.wait_for(self.state.stop.wait(), timeout=s.watchdog_interval_sec)
                return
            except asyncio.TimeoutError:
                pass

    async def _shutdown(self) -> None:
        log.info("shutting down (%s)", "KILL: " + self.state.kill_reason if self.state.kill_requested else "stop")
        # give components a moment to exit cleanly on their own
        done, pending = await asyncio.wait(self._tasks, timeout=8)
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        try:
            await self.db.log_event("agent.stop", None, {"kill": self.state.kill_requested, "reason": self.state.kill_reason})
        except Exception:
            pass
        await self.db.close()
        log.info("bye")
