"""Position manager (phase 2): watches open positions and exits them.

Exit rules, evaluated in this order on every refresh:
  dev_sold       creator_token_status became creator_close (dev dumped)      -> exit
  holders_drop   holders fell EXIT_HOLDERS_DROP_PCT below the peak             -> exit
  sl             price <= entry * (1 - EXIT_SL_PCT)                            -> exit
  tp             price >= entry * (1 + EXIT_TP_PCT)                            -> exit
  trail          after +EXIT_TRAIL_ARM_PCT, price <= peak * (1 - EXIT_TRAIL_PCT) -> exit
  time           held longer than EXIT_MAX_HOLD_SEC                             -> exit
Prices come from GMGN snapshots (cached ~15s). If GMGN cannot price a position for
a while the position is held, not guessed. Works for paper and live positions alike.
"""
from __future__ import annotations

import asyncio
import time

from ..bus import TOPIC_POSITION
from ..component import Component, Context
from ..execution.paper import PaperBroker
from ..ingest.gmgn import GmgnClient, GmgnError


class PositionManager(Component):
    name = "positions"

    def __init__(self, ctx: Context, gmgn: GmgnClient, broker: PaperBroker) -> None:
        super().__init__(ctx)
        self.gmgn = gmgn
        self.broker = broker
        self._price_fail: dict[int, int] = {}

    async def run(self) -> None:
        while not self.ctx.state.stop.is_set():
            self.beat()
            try:
                positions = await self.ctx.db.open_positions()
                self.ctx.state.counters["positions.open"] = len(positions)
                if positions and time.time() >= self.gmgn.stats.cooldown_until:
                    await asyncio.gather(*(self._manage(p) for p in positions), return_exceptions=True)
            except Exception:
                self.log.exception("position loop failed")
            if await self.sleep(self.ctx.p("position_check_sec")):
                return

    async def _manage(self, pos: dict) -> None:
        p, st = self.ctx.p, self.ctx.state
        meta = pos.get("meta") or {}
        sym = meta.get("symbol") or pos["mint"][:6]
        try:
            snap = await self.gmgn.snapshot(pos["mint"])
        except GmgnError as e:
            n = self._price_fail[pos["id"]] = self._price_fail.get(pos["id"], 0) + 1
            if n in (3, 10, 30):
                st.log("positions", "warn", f"${sym} · no price from GMGN ×{n} · holding", pos["mint"], position_id=pos["id"])
            return
        self._price_fail.pop(pos["id"], None)
        sol_usd = await self.gmgn.sol_price_usd()
        if not snap.price_usd or not sol_usd:
            return
        price_sol = snap.price_usd / sol_usd
        entry = float(pos["entry_price_sol"])
        change = price_sol / entry - 1.0
        now = time.time()
        held = now - float(pos["opened_at"])

        peak = max(float(meta.get("peak_price_sol") or entry), price_sol)
        peak_holders = max(int(meta.get("peak_holders") or 0), int(snap.holder_count or 0))
        meta.update({"peak_price_sol": peak, "peak_holders": peak_holders, "last_price_sol": price_sol, "last_change": round(change, 4),
                     "last_holders": snap.holder_count, "last_seen": now})

        reason = None
        if snap.creator_status == "creator_close" and meta.get("creator_status_at_entry") != "creator_close":
            reason = "dev_sold"
        elif peak_holders >= 30 and snap.holder_count is not None and snap.holder_count <= peak_holders * (1 - p("exit_holders_drop_pct")):
            reason = "holders_drop"
        elif change <= -p("exit_sl_pct"):
            reason = "sl"
        elif change >= p("exit_tp_pct"):
            reason = "tp"
        elif peak / entry - 1.0 >= p("exit_trail_arm_pct") and price_sol <= peak * (1 - p("exit_trail_pct")):
            reason = "trail"
        elif held >= p("exit_max_hold_sec"):
            reason = "time"

        if reason is None:
            await self.ctx.db.update_position_meta(pos["id"], meta)
            st.last_active["positions"] = now
            return

        trade = await self.broker.sell({**pos, "meta": meta}, snap.price_usd, reason, {"exit_change": round(change, 4), "exit_holders": snap.holder_count})
        if trade is None:
            st.log("positions", "error", f"SELL ${sym} failed · cannot price", pos["mint"], position_id=pos["id"])
            return
        st.bump("trade.sell")
        st.bump(f"trade.exit.{reason}")
        st.bump("trade.win" if trade["pnl_sol"] > 0 else "trade.loss")
        st.last_active["exec"] = now
        sign = "+" if trade["pnl_sol"] >= 0 else ""
        st.log("exec", "trade", f"SELL ${sym} · {reason} · {sign}{trade['pnl_sol']:.4f} SOL ({sign}{trade['pnl_pct']:.1f}%) · held {int(held)}s", pos["mint"],
               position_id=pos["id"], trade_id=trade["trade_id"])
        self.ctx.bus.alert(f"SELL ${sym} · {reason} · {sign}{trade['pnl_sol']:.4f} SOL ({sign}{trade['pnl_pct']:.1f}%) · {int(held)}s", "trade")
        self.ctx.bus.publish(TOPIC_POSITION, {"event": "close", "trade": trade})
