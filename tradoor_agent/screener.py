"""Screener (phase 1): new launches -> cheap gates -> watchlist -> GMGN snapshot ->
hard gates -> candidates table (+ TOPIC_CANDIDATE for scoring in phase 2).

Budget math (defaults): watchlist <= 150, recheck every 30s, 2 weight per check
=> <= 10 weight/s, well under a Pro budget of 50.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from .bus import TOPIC_CANDIDATE, TOPIC_TOKEN_MIGRATED, TOPIC_TOKEN_NEW
from .component import Component, Context
from .gates.rules import cheap_gates, failed, hard_gates
from .ingest.gmgn import GmgnClient, GmgnError
from .ingest.pumpportal import PumpPortalIngest, dev_initial_buy
from .models import Candidate, Lane, TokenEvent, TokenSnapshot

# failing one of these twice in a row means the token's structure is bad; stop spending weight on it
STRUCTURAL = {"renounced_mint", "renounced_freeze", "rug_ratio", "bundler", "insider", "wash_trading", "dev_hold", "rat_trader"}
# gates a healthy token simply has not reached yet; failing only these is "not yet", not "bad"
GROWTH = {"holders", "liquidity", "mcap_min"}
CHECKS_PER_TICK = 8


@dataclass
class WatchEntry:
    evt: TokenEvent
    first_seen: float
    next_check: float
    checks: int = 0
    passes: int = 0
    structural_fails: int = 0
    migrated: bool = False
    alerted: bool = False
    last_snap: TokenSnapshot | None = None
    last_failed: list[str] = field(default_factory=list)
    last_check: float = 0.0
    last_candidate_id: int | None = None


class Screener(Component):
    name = "screener"

    def __init__(self, ctx: Context, gmgn: GmgnClient, ingest: PumpPortalIngest | None = None) -> None:
        super().__init__(ctx)
        self.gmgn = gmgn
        self.ingest = ingest
        self.watch: dict[str, WatchEntry] = {}
        self._alert_times: list[float] = []

    # ------------------------------------------------------------ loop

    async def run(self) -> None:
        bus, st = self.ctx.bus, self.ctx.state
        q_new = bus.subscribe(TOPIC_TOKEN_NEW, maxsize=5000)
        q_mig = bus.subscribe(TOPIC_TOKEN_MIGRATED, maxsize=1000)
        await self.gmgn.open()
        try:
            while not st.stop.is_set():
                self.beat()
                await self.ctx.params.refresh()
                self._drain_new(q_new)
                self._drain_migrations(q_mig)
                await self._run_due_checks()
                self._evict()
                st.counters["screen.watchlist"] = len(self.watch)
                if await self.sleep(1.0):
                    break
        finally:
            await self.gmgn.close()

    # ------------------------------------------------------------ intake

    def _drain_new(self, q: asyncio.Queue) -> None:
        p = self.ctx.p
        while True:
            try:
                evt: TokenEvent = q.get_nowait()
            except asyncio.QueueEmpty:
                return
            gates = cheap_gates(evt, max_dev_buy_sol=p("cheap_max_dev_buy_sol"), max_dev_supply_pct=p("cheap_max_dev_supply_pct"))
            bad = failed(gates)
            if bad:
                self.ctx.state.bump("screen.cheap_drop")
                self.log.debug("cheap drop %s %s: %s", evt.symbol, evt.mint[:8], bad)
                self.ctx.state.log("gates", "drop", f"DROP ${evt.symbol or evt.mint[:6]} · {','.join(bad)}", evt.mint)
                continue
            if evt.mint in self.watch:
                continue
            if len(self.watch) >= p("screen_max_watchlist"):
                self.ctx.state.bump("screen.watchlist_full")
                continue
            now = time.time()
            self.watch[evt.mint] = WatchEntry(evt=evt, first_seen=now, next_check=now + p("screen_first_check_sec"))
            self.ctx.state.bump("screen.watched")
            sol, share = dev_initial_buy(evt)
            self.ctx.state.log("gates", "watch", f"WATCH ${evt.symbol or evt.mint[:6]} · dev {sol:.2f} SOL ({share * 100:.1f}%)", evt.mint)

    def _drain_migrations(self, q: asyncio.Queue) -> None:
        while True:
            try:
                m = q.get_nowait()
            except asyncio.QueueEmpty:
                return
            e = self.watch.get(str(m.get("mint")))
            if e:
                e.migrated = True
                e.next_check = min(e.next_check, time.time() + 5)   # migration changes everything; look again soon

    # ------------------------------------------------------------ checks

    async def _run_due_checks(self) -> None:
        now = time.time()
        if now < self.gmgn.stats.cooldown_until:
            return                                  # rate limited: do not touch the API
        due = sorted((e for e in self.watch.values() if e.next_check <= now), key=lambda e: e.next_check)[:CHECKS_PER_TICK]
        if not due:
            return
        results = await asyncio.gather(*(self._check(e) for e in due), return_exceptions=True)
        for e, r in zip(due, results):
            if isinstance(r, Exception):
                self.ctx.state.bump("screen.check_error")
                self.log.error("check failed for %s: %s: %s", e.evt.mint[:8], type(r).__name__, r)

    async def _check(self, e: WatchEntry) -> None:
        p = self.ctx.p
        now = time.time()
        e.next_check = now + p("screen_recheck_sec")
        try:
            snap = await self.gmgn.snapshot(e.evt.mint)
            self.ctx.state.last_active["gmgn"] = time.time()
        except GmgnError as err:
            self.ctx.state.bump("gmgn.error")
            if err.rate_limited:
                self.ctx.state.log("gmgn", "warn", f"RATE LIMIT · cooldown {max(0, int(self.gmgn.stats.cooldown_until - time.time()))}s")
                return
            self.ctx.state.log("gmgn", "error", f"{e.evt.symbol or e.evt.mint[:6]} · {str(err)[:70]}", e.evt.mint)
            self.log.debug("gmgn snapshot failed %s: %s", e.evt.mint[:8], err)
            return
        except Exception:
            self.log.exception("snapshot crashed for %s", e.evt.mint[:8])
            return

        e.checks += 1
        e.last_snap = snap
        e.last_check = time.time()
        gates = hard_gates(
            snap,
            min_holders=p("gate_min_holders"), min_liquidity_usd=p("gate_min_liquidity_usd"),
            min_mcap_usd=p("gate_min_mcap_usd"), max_mcap_usd=p("gate_max_mcap_usd"),
            max_top10=p("gate_max_top10"), max_dev_hold=p("gate_max_dev_hold"),
            max_bundler=p("gate_max_bundler"), max_rat=p("gate_max_rat"), max_insider=p("gate_max_insider"),
            max_snipers=p("gate_max_snipers"), max_rug_ratio=p("gate_max_rug_ratio"),
            require_renounced=p("gate_require_renounced"),
        )
        bad = failed(gates)
        e.last_failed = bad
        age = now - e.first_seen
        passed = not bad
        if bad and set(bad) & STRUCTURAL:
            e.structural_fails += 1
        else:
            e.structural_fails = 0

        cand = Candidate(
            mint=snap.mint, ts=now, score=0.0, breakdown=snap.metrics(), gates=gates,
            token_age_sec=age, lane=Lane.SCREENED if passed else Lane.DROP,
            reason="pass" if passed else "fail:" + ",".join(bad),
        )
        cand.breakdown["symbol"] = snap.symbol or e.evt.symbol
        cand.breakdown["migrated"] = e.migrated
        cand.id = await self.ctx.db.insert_candidate(cand)
        e.last_candidate_id = cand.id

        sym = snap.symbol or e.evt.symbol or snap.mint[:6]
        mc = f"${snap.mcap_usd / 1000:.1f}k" if snap.mcap_usd else "mcap n/a"
        if passed:
            e.passes += 1
            self.ctx.state.bump("screen.pass")
            self.ctx.state.log("gates", "pass", f"PASS ${sym} · {mc} · holders {snap.holder_count or '?'} · top10 {(snap.top10_rate or 0) * 100:.0f}% · smart {snap.smart_wallets or 0}", snap.mint, candidate_id=cand.id)
            self.ctx.bus.publish(TOPIC_CANDIDATE, cand)
            if self.ingest and self.ctx.settings.pumpportal_trade_subs and len(self.ingest.trade_subs) < self.ctx.settings.pumpportal_trade_subs_max:
                await self.ingest.subscribe_trades([snap.mint])
            if not e.alerted and self._alert_budget_ok():
                e.alerted = True
                self.ctx.bus.alert(self._card(snap, e, age), "info")
        else:
            self.ctx.state.bump("screen.fail")
            # growth-only fails (too young to have holders / liquidity / mcap yet) are expected early on:
            # log them quietly as GROW so the feed highlights the fails that actually matter
            if set(bad) <= GROWTH:
                self.ctx.state.log("gates", "grow", f"GROW ${sym} · {mc} · {snap.holder_count or 0}h · nunggu {','.join(bad)}", snap.mint, candidate_id=cand.id)
            else:
                self.ctx.state.log("gates", "fail", f"FAIL ${sym} · {mc} · {','.join(bad)}", snap.mint, candidate_id=cand.id)

    # ------------------------------------------------------------ housekeeping

    def _evict(self) -> None:
        now = time.time()
        max_age = self.ctx.p("screen_max_age_sec")
        gone = []
        for mint, e in self.watch.items():
            if now - e.first_seen > max_age:
                gone.append((mint, "expired"))
            elif e.structural_fails >= 2:
                gone.append((mint, "structural"))
        for mint, why in gone:
            e = self.watch.pop(mint, None)
            self.ctx.state.bump(f"screen.evict.{why}")
            if e and why == "structural":
                self.ctx.state.log("gates", "drop", f"EVICT ${e.evt.symbol or mint[:6]} · struktural ×2 ({','.join(e.last_failed)})", mint)
        if gone and self.ingest and self.ingest.trade_subs:
            asyncio.create_task(self.ingest.unsubscribe_trades([m for m, _ in gone]))

    def _alert_budget_ok(self) -> bool:
        now = time.time()
        self._alert_times = [t for t in self._alert_times if now - t < 3600]
        if len(self._alert_times) >= self.ctx.p("screen_alert_max_per_hour"):
            return False
        self._alert_times.append(now)
        return True

    @staticmethod
    def _card(s: TokenSnapshot, e: WatchEntry, age: float) -> str:
        def pct(v):
            return "n/a" if v is None else f"{v * 100:.0f}%"
        def usd(v):
            return "n/a" if v is None else f"${v / 1000:.1f}k"
        prog = f" · bond {s.launchpad_progress * 100:.0f}%" if s.launchpad_progress is not None and not e.migrated else (" · migrated" if e.migrated else "")
        return (
            f"screened ${s.symbol or e.evt.symbol} · {int(age // 60)}m{prog}\n"
            f"mcap {usd(s.mcap_usd)} · liq {usd(s.liquidity_usd)} · holders {s.holder_count or 'n/a'}\n"
            f"top10 {pct(s.top10_rate)} · dev {pct(s.dev_hold_rate if s.dev_hold_rate is not None else s.creator_hold_rate)} · bundler {pct(s.bundler_rate)} · insider {pct(s.insider_rate)}\n"
            f"snipers {s.sniper_count if s.sniper_count is not None else 'n/a'} · smart {s.smart_wallets or 0} · renowned {s.renowned_wallets or 0}\n"
            f"1m {s.buys_1m or 0}b/{s.sells_1m or 0}s · 5m {s.buys_5m or 0}b/{s.sells_5m or 0}s\n"
            f"gmgn.ai/sol/token/{s.mint}"
        )
