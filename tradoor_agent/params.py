"""Live parameter overrides.

Settings come from .env and are fixed for the process lifetime. The `params`
table (edited with /set in Telegram, or by the slow loop later) overrides
them at runtime. ParamStore caches the table and refreshes every few seconds,
so hot-path code reads from memory.

    gates_cfg = ctx.params.get("gate_max_top10", settings.gate_max_top10)
"""
from __future__ import annotations

import time
from typing import Any

from .db import Database


class ParamStore:
    def __init__(self, db: Database, refresh_sec: float = 10.0) -> None:
        self._db = db
        self._refresh_sec = refresh_sec
        self._cache: dict[str, Any] = {}
        self._loaded_at = 0.0

    async def refresh(self, force: bool = False) -> None:
        if force or time.time() - self._loaded_at >= self._refresh_sec:
            self._cache = await self._db.all_params()
            self._loaded_at = time.time()

    def get(self, key: str, default: Any) -> Any:
        """Typed override: the override is coerced to the type of `default`."""
        if key not in self._cache:
            return default
        raw = self._cache[key]
        try:
            if isinstance(default, bool):
                if isinstance(raw, str):
                    return raw.strip().lower() in ("1", "true", "yes", "on")
                return bool(raw)
            if isinstance(default, int):
                return int(raw)
            if isinstance(default, float):
                return float(raw)
            if isinstance(default, str):
                return str(raw)
        except (TypeError, ValueError):
            return default
        return raw

    def snapshot(self) -> dict[str, Any]:
        return dict(self._cache)
