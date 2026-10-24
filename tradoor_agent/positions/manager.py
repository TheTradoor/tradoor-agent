"""Position manager: tick-driven exits with a GMGN fallback.

Two loops:
  fast  on every PumpPortal TradeEvent for a mint we hold, re-evaluate price exits
        (sl / tp / trail) at the tick price. This is what makes the stop loss honest:
        pump.fun moves 30-50% inside the 15s GMGN cache window.
  slow  every POSITION_CHECK_SEC: GMGN snapshot for the structural exits (dev_sold,
        holders_drop), the time stop, and price exits when ticks are stale
        (no PUMPPORTAL_API_KEY, subscription failed, or a quiet token).

On open: subscribe the mint's trade stream. On close: unsubscribe. Works for paper
and live positions alike; the broker decides what a "sell" is.
"""
from __future__ import annotations

import asyncio
import time

from ..bus import TOPIC_POSITION, TOPIC_TRADE
from ..component import Component, Context
from ..execution.paper import PaperBroker
from ..ingest.gmgn import GmgnClient, GmgnError
from ..ingest.pumpportal import PumpPortalIngest
from ..models import TradeEvent


class PositionManager(Component):
    name = "positions"

    def __init__(self, ctx: Context, gmgn: GmgnClient, broker: PaperBroker, ingest: PumpPortalIngest | None = None) -> None:
        super().__init__(ctx)
        self.gmgn = gmgn
        self.broker = broker
        self.ingest = ingest
        self._price_fail: dict[int, int] = {}
        self._open: dict[str, dict] = {}          # mint -> position row (cache of db, refreshed by the slow loop)
        self._last_tick: dict[str, float] = {}     # mint -> ts of last tick
        self._closing: set[int] = set()            # position ids with a sell in flight
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------ loops

    async def run(self) -> None:
        q_trade = self.ctx.bus.subscribe(TOPIC_TRADE, maxsize=20_000)
        q_pos = self.ctx.bus.subscribe(TOPIC_POSITION, maxsize=500)
        tick_task = asyncio.create_task(self._tick_loop(q_trade), name="positions_ticks")
        evt_task = asyncio.create_task(self._event_loop(q_pos), name="positions_events")
        try:
            await self._refresh_open()
            while not self.ctx.state.stop.is_set():
                self.beat()
                try:
                    await self._refresh_open()
                    positions = list(self._open.values())
                    self.ctx.state.counters["positions.open"] = len(positions)
                    if positions and time.time() >= self.gmgn.stats.cooldown_until:
                        await asyncio.gather(*(self._slow_check(p) for p in positions), return_exceptions=True)
                except Exception:
                    self.log.exception("position loop failed")
                if await self.sleep(self.ctx.p("position_check_sec")):
                    return
        finally:
            tick_task.cancel()
            evt_task.cancel()

    async def _refresh_open(self) -> None:
        rows = await self.ctx.db.open_positions()
        fresh = {r["mint"]: r for r in rows}
        # keep in-memory meta (peak etc.) for positions we already track
        for mint, row in fresh.items():
            old = self._open.get(mint)
            if old and old["id"] == row["id"]:
                row["meta"] = {**row.get("meta", {}), **old.get("meta", {})}
        self._open = fresh
        if self.ingest and self.ctx.p("tick_pricing") and self.ctx.settings.pumpportal_api_key:
            want = set(fresh)
            have = set(self.ingest.trade_subs)
            if want - have:
                await self.ingest.subscribe_trades(sorted(want - have))
            if have - want:
                await self.ingest.unsubscribe_trades(sorted(have - want))

    async def _event_loop(self, q: asyncio.Queue) -> None:
        while True:
            evt = await q.get()
            if evt.get("event") == "open":
                await self._refresh_open()

    async def _tick_loop(self, q: asyncio.Queue) -> None:
        while True:
            t: TradeEvent = await q.get()
            pos = self._open.get(t.mint)
            if pos is None or not t.price_sol:
                continue
            self._last_tick[t.mint] = t.ts
            try:
                await self._evaluate(pos, price_sol=t.price_sol, source="tick")
            except Exception:
                self.log.exception("tick eval failed for %s", t.mint[:8])

    # ------------------------------------------------------------ checks

    async def _slow_check(self, pos: dict) -> None:
        st = self.ctx.state
        meta = pos.get("meta") or {}
        sym = meta.get("symbol") or pos["mint"][:6]
        try:
            snap = await self.gmgn.snapshot(pos["mint"])
        except GmgnError:
            n = self._price_fail[pos["id"]] = self._price_fail.get(pos["id"], 0) + 1
            if n in (3, 10, 30):
                st.log("positions", "warn", f"${sym} · no data from GMGN ×{n} · holding", pos["mint"], position_id=pos["id"])
            return
        self._price_fail.pop(pos["id"], None)
        peak_holders = max(int(meta.get("peak_holders") or 0), int(snap.holder_count or 0))
        meta.update({"peak_holders": peak_holders, "last_holders": snap.holder_count, "creator_status": snap.creator_status})
        structural = None
        if snap.creator_status == "creator_close" and meta.get("creator_status_at_entry") != "creator_close":
            structural = "dev_sold"
        elif peak_holders >= 30 and snap.holder_count is not None and snap.holder_count <= peak_holders * (1 - self.ctx.p("exit_holders_drop_pct")):
            structural = "holders_drop"
        tick_fresh = time.time() - self._last_tick.get(pos["mint"], 0) < self.ctx.p("tick_stale_sec")
        sol_usd = await self.gmgn.sol_price_usd()
        price_sol = (snap.price_usd / sol_usd) if (snap.price_usd and sol_usd) else None
        await self._evaluate(pos, price_sol=price_sol, source="gmgn", structural=structural, price_exits=not tick_fresh, price_usd=snap.price_usd)

    def _exit_params(self, pos: dict) -> dict:
        """Exit profile for a position: <lane>_exit_* when set, else the global exit_* (both live-overridable)."""
        lane = str(pos.get("lane") or "fast")
        lp = lambda k: self.ctx.lane_p(lane, k, k)  # noqa: E731
        return {"tp": float(lp("exit_tp_pct")), "sl": float(lp("exit_sl_pct")), "arm": float(lp("exit_trail_arm_pct")), "trail": float(lp("exit_trail_pct")),
                "hold": int(lp("exit_max_hold_sec")), "flat_after": int(lp("exit_flat_after_sec")), "flat_min": float(self.ctx.p("exit_flat_min_pct"))}

    async def _evaluate(self, pos: dict, *, price_sol: float | None, source: str, structural: str | None = None, price_exits: bool = True, price_usd: float | None = None) -> None:
        p, st = self.ctx.p, self.ctx.state
        x = self._exit_params(pos)
        if pos["id"] in self._closing:
            return
        meta = pos.setdefault("meta", {})
        entry = float(pos["entry_price_sol"])
        now = time.time()
        held = now - float(pos["opened_at"])
        reason = structural
        change = None
        if price_sol:
            change = price_sol / entry - 1.0
            peak = max(float(meta.get("peak_price_sol") or entry), price_sol)
            meta.update({"peak_price_sol": peak, "last_price_sol": price_sol, "last_change": round(change, 4), "last_seen": now, "price_source": source})
            if reason is None and price_exits:
                if change <= -x["sl"]:
                    reason = "sl"
                elif change >= x["tp"]:
                    reason = "tp"
                elif peak / entry - 1.0 >= x["arm"] and price_sol <= peak * (1 - x["trail"]):
                    reason = "trail"
        if reason is None and held >= x["hold"]:
            reason = "time"
        # flat stop: the token never got going -> free the slot for the next trigger (peak is kept in meta across ticks)
        if reason is None and x["flat_min"] > 0 and x["flat_after"] > 0 and held >= x["flat_after"]:
            peak_change = float(meta.get("peak_price_sol") or entry) / entry - 1.0
            if peak_change < x["flat_min"] and (price_sol or source == "gmgn"):
                reason = "flat"
        if reason is None:
            if source == "gmgn":
                await self.ctx.db.update_position_meta(pos["id"], meta)
            st.last_active["positions"] = now
            return
        async with self._lock:
            if pos["id"] in self._closing or pos["mint"] not in self._open:
                return
            self._closing.add(pos["id"])
        try:
            await self._close(pos, reason, price_sol, price_usd, change, held, source)
        finally:
            self._closing.discard(pos["id"])

    async def _close(self, pos: dict, reason: str, price_sol: float | None, price_usd: float | None, change: float | None, held: float, source: str) -> None:
        st = self.ctx.state
        meta = pos.get("meta") or {}
        sym = meta.get("symbol") or pos["mint"][:6]
        if price_sol is None and price_usd is None:
            # time stop with no price at all: try GMGN once more, else hold
            try:
                snap = await self.gmgn.snapshot(pos["mint"])
                price_usd = snap.price_usd
            except GmgnError:
                return
        trade = await self.broker.sell({**pos, "meta": meta}, price_usd, reason,
                                       {"exit_change": round(change, 4) if change is not None else None, "exit_holders": meta.get("last_holders"), "exit_price_source": source},
                                       price_sol=price_sol)
        if trade is None:
            st.log("positions", "error", f"SELL ${sym} failed · cannot price", pos["mint"], position_id=pos["id"])
            return
        self._open.pop(pos["mint"], None)
        self._last_tick.pop(pos["mint"], None)
        if self.ingest and pos["mint"] in self.ingest.trade_subs:
            await self.ingest.unsubscribe_trades([pos["mint"]])
        st.bump("trade.sell")
        st.bump(f"trade.exit.{reason}")
        st.bump("trade.win" if trade["pnl_sol"] > 0 else "trade.loss")
        st.last_active["exec"] = time.time()
        sign = "+" if trade["pnl_sol"] >= 0 else ""
        st.log("exec", "trade", f"SELL ${sym} · {reason} · {sign}{trade['pnl_sol']:.4f} SOL ({sign}{trade['pnl_pct']:.1f}%) · held {int(held)}s · {source}", pos["mint"],
               position_id=pos["id"], trade_id=trade["trade_id"])
        self.ctx.bus.alert(f"SELL ${sym} · {reason} · {sign}{trade['pnl_sol']:.4f} SOL ({sign}{trade['pnl_pct']:.1f}%) · {int(held)}s", "trade")
        self.ctx.bus.publish(TOPIC_POSITION, {"event": "close", "trade": trade})
