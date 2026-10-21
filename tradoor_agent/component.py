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
    web_url: str = ""

    def p(self, key: str):
        """Setting with live override: ctx.p("gate_max_top10")."""
        return self.params.get(key, getattr(self.settings, key))


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
