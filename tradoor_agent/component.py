"""Component plumbing: a Context every component receives and the base class."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .bus import EventBus
from .config import Settings
from .db import Database
from .params import ParamStore
from .state import RuntimeState


@dataclass
class Context:
    settings: Settings
    state: RuntimeState
    bus: EventBus
    db: Database
    params: ParamStore
    # live component handles for ops surfaces (/status, dashboard); set by build_components
    gmgn: object | None = None
    ingest: object | None = None
    screener: object | None = None
    dexpaid: object | None = None
    trending: object | None = None
    tracker: object | None = None
    web_url: str = ""

    def p(self, key: str):
        """Setting with live override: ctx.p("gate_max_top10")."""
        return self.params.get(key, getattr(self.settings, key))

    def lane_p(self, lane: str, key: str, fallback: str):
        """Per-lane setting with inheritance: <prefix>_<key> when set, else the shared setting `fallback`.

            ctx.lane_p("migration", "min_holders", "trig_min_holders")  -> MIG_MIN_HOLDERS or TRIG_MIN_HOLDERS
            ctx.lane_p("dexpaid", "exit_sl_pct", "exit_sl_pct")         -> DEXPAID_EXIT_SL_PCT or EXIT_SL_PCT
        Both levels honour live overrides from the params table.
        """
        base = self.p(fallback)
        name = f"{Settings.lane_prefix(lane)}_{key}"
        if not hasattr(self.settings, name):
            return base
        raw = self.params.get(name, getattr(self.settings, name))
        if raw is None or raw == "":
            return base
        try:
            if isinstance(base, bool):
                return raw if isinstance(raw, bool) else str(raw).strip().lower() in ("1", "true", "yes", "on")
            if isinstance(base, int):
                return int(float(raw))
            if isinstance(base, float):
                return float(raw)
        except (TypeError, ValueError):
            return base
        return raw

    def lane_mode(self, lane: str) -> str:
        """trade | observe | off for a lane (grey follows fast; a disabled dexpaid is off)."""
        if lane == "dexpaid" and not self.p("dexpaid_enabled"):
            return "off"
        mode = str(self.p(f"{Settings.lane_prefix(lane)}_mode")).strip().lower()
        return mode if mode in Settings.LANE_MODES else "trade"


class Component:
    """A long-running task supervised by the Supervisor.

    Subclasses implement `run()` as a loop that exits when ctx.state.stop is set
    and call `self.beat()` regularly so the watchdog knows they are alive.
    """

    name: str = "component"

    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self.log = logging.getLogger(f"agent.{self.name}")

    def beat(self) -> None:
        self.ctx.state.heartbeat(self.name)

    async def sleep(self, seconds: float) -> bool:
        """Sleep but wake early on stop. Returns True if stop was requested."""
        try:
            await asyncio.wait_for(self.ctx.state.stop.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def run(self) -> None:  # pragma: no cover - abstract
        raise NotImplementedError
