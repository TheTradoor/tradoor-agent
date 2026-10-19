"""Housekeeper: periodic maintenance. Prunes old events, keeps daily stats fresh."""
from __future__ import annotations

from .component import Component


class Housekeeper(Component):
    name = "housekeeper"

    INTERVAL_SEC = 300

    async def run(self) -> None:
        db = self.ctx.db
        retention = self.ctx.settings.events_retention_days
        while not self.ctx.state.stop.is_set():
            self.beat()
            try:
                await db.ensure_daily_stats_row()
                await db.refresh_daily_stats()
                pruned = await db.prune_events(retention)
                if pruned:
                    self.log.info("pruned %d old events", pruned)
            except Exception:
                self.log.exception("housekeeping failed")
            if await self.sleep(self.INTERVAL_SEC):
                return
