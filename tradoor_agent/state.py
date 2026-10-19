"""Runtime state shared by every component (single process, single event loop)."""
from __future__ import annotations

import asyncio
import time
from collections import Counter
from dataclasses import dataclass, field

from .config import Mode


@dataclass
class RuntimeState:
    mode: Mode
    paused: bool = False
    kill_requested: bool = False
    kill_reason: str = ""
    started_at: float = field(default_factory=time.time)
    heartbeats: dict[str, float] = field(default_factory=dict)
    counters: Counter = field(default_factory=Counter)
    stop: asyncio.Event = field(default_factory=asyncio.Event)

    # --- signals ---

    def heartbeat(self, component: str) -> None:
        self.heartbeats[component] = time.time()

    def bump(self, key: str, n: int = 1) -> None:
        self.counters[key] += n

    def request_stop(self) -> None:
        self.stop.set()

    def request_kill(self, reason: str) -> None:
        """Kill = stop everything AND stay down until a human removes the KILL file."""
        self.kill_requested = True
        self.kill_reason = reason
        self.stop.set()

    # --- queries ---

    @property
    def uptime_sec(self) -> float:
        return time.time() - self.started_at

    @property
    def can_enter(self) -> bool:
        """May the pipeline open NEW positions right now?"""
        return not self.paused and not self.kill_requested and not self.stop.is_set()

    def stale_components(self, max_age_sec: float) -> list[str]:
        cutoff = time.time() - max_age_sec
        return [name for name, ts in self.heartbeats.items() if ts < cutoff]

    def snapshot(self) -> dict:
        now = time.time()
        return {
            "mode": self.mode.value,
            "paused": self.paused,
            "kill_requested": self.kill_requested,
            "uptime_sec": int(self.uptime_sec),
            "heartbeats_age_sec": {k: int(now - v) for k, v in self.heartbeats.items()},
            "counters": dict(self.counters),
        }
