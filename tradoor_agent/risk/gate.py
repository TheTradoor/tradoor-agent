"""Risk gate: the only layer allowed to veto an entry. Pure checks over state + db.

Order matters: cheapest/most absolute first. The first failing rule is the reason.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from ..component import Context
from ..models import Lane


@dataclass
class RiskVerdict:
    allowed: bool
    reason: str
    size_sol: float
    checks: dict


class RiskGate:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx

    async def check(self, mint: str, lane: Lane) -> RiskVerdict:
        st, p, db = self.ctx.state, self.ctx.p, self.ctx.db
        now = time.time()
        checks: dict = {}

        def deny(reason: str) -> RiskVerdict:
            checks["denied"] = reason
            return RiskVerdict(False, reason, 0.0, checks)

        if st.kill_requested:
            return deny("kill")
        if st.paused:
            return deny("paused")
        if lane not in (Lane.FAST, Lane.GREY, Lane.DEXPAID):
            return deny(f"lane:{lane.value}")

        open_n = await db.open_positions_count()
        checks["open_positions"] = open_n
        if open_n >= p("max_open_positions"):
            return deny("max_open_positions")

        if await db.has_open_position(mint):
            return deny("already_in_position")

        last = await db.last_trade_ts_for_mint(mint)
        if last and now - last < p("reentry_cooldown_sec"):
            return deny("reentry_cooldown")

        pnl_today = await db.realized_pnl_today()
        checks["pnl_today"] = round(pnl_today, 4)
        if pnl_today <= -p("daily_loss_cap_sol"):
            return deny("daily_loss_cap")

        last_loss = await db.last_loss_ts()
        if last_loss and now - last_loss < p("cooldown_after_loss_sec"):
            checks["cooldown_left"] = int(p("cooldown_after_loss_sec") - (now - last_loss))
            return deny("cooldown_after_loss")

        size = float(p("position_size_sol"))
        if lane == Lane.GREY:
            size *= float(p("llm_lane_size_mult"))
        checks["size_sol"] = size
        return RiskVerdict(True, "ok", size, checks)
