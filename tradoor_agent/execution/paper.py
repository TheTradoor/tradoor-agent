"""Paper broker: fills orders against the latest GMGN price with a slippage + fee model.

Shares the orders / fills / positions / trades tables with the live broker (phase 3),
so the dashboard, risk gate and autopsy never care which one produced a row.

Price units: GMGN prices are USD per token; the book is kept in SOL, so every fill
converts with the SOL/USD price at fill time (stored in position meta for the autopsy).
"""
from __future__ import annotations

import time
import uuid

from ..component import Context
from ..ingest.gmgn import GmgnClient
from ..models import Lane, Order, OrderStatus, Position, PositionStatus, Side


class PaperBroker:
    mode = "paper"

    def __init__(self, ctx: Context, gmgn: GmgnClient) -> None:
        self.ctx = ctx
        self.gmgn = gmgn

    async def _sol_usd(self) -> float | None:
        return await self.gmgn.sol_price_usd()

    # ------------------------------------------------------------ buy

    async def buy(self, mint: str, lane: Lane, size_sol: float, price_usd: float, meta: dict) -> dict | None:
        """Open a paper position. Returns the position row dict, or None when it cannot price."""
        p = self.ctx.p
        sol_usd = await self._sol_usd()
        if not sol_usd or not price_usd or price_usd <= 0:
            return None
        slip = float(p("paper_slippage_bps")) / 10_000
        fee = float(p("paper_fee_bps")) / 10_000
        price_sol = price_usd / sol_usd
        fill_price_sol = price_sol * (1 + slip)
        fee_sol = size_sol * fee
        tokens = (size_sol - fee_sol) / fill_price_sol
        now = time.time()

        order = Order(mint=mint, side=Side.BUY, size_sol=size_sol, idempotency_key=f"paper-buy-{mint}-{uuid.uuid4().hex[:8]}",
                      mode=self.mode, expected_price_sol=price_sol, status=OrderStatus.CONFIRMED, tx_sig="", ts=now)
        oid = await self.ctx.db.insert_order(order)
        await self.ctx.db.insert_fill(oid, tokens, size_sol, fill_price_sol, fee_sol, slip * 10_000)
        pos = Position(mint=mint, lane=lane, entry_price_sol=fill_price_sol, size_sol=size_sol, tokens=tokens, opened_at=now, status=PositionStatus.OPEN)
        pmeta = {**meta, "entry_price_usd": price_usd, "sol_usd": sol_usd, "fee_sol": fee_sol, "peak_price_sol": fill_price_sol,
                 "peak_holders": meta.get("holders"), "entry_order_id": oid, "mode": self.mode}
        pid = await self.ctx.db.insert_position(pos, pmeta)
        await self.ctx.db.update_order(oid, OrderStatus.CONFIRMED.value, position_id=pid)
        row = {"id": pid, "mint": mint, "lane": lane.value, "opened_at": now, "entry_price_sol": fill_price_sol, "size_sol": size_sol, "tokens": tokens, "status": "open", "meta": pmeta}
        return row

    # ------------------------------------------------------------ sell

    async def sell(self, pos: dict, price_usd: float | None, reason: str, extra: dict | None = None, *, price_sol: float | None = None) -> dict | None:
        """Close a paper position. Price either as USD (GMGN) or directly in SOL per token (trade ticks).
        Returns the trade summary, or None when it cannot price."""
        p = self.ctx.p
        sol_usd = await self._sol_usd()
        if price_sol is None:
            if not sol_usd or not price_usd or price_usd <= 0:
                return None
            price_sol = price_usd / sol_usd
        elif price_sol <= 0:
            return None
        if price_usd is None and sol_usd:
            price_usd = price_sol * sol_usd
        slip = float(p("paper_slippage_bps")) / 10_000
        fee = float(p("paper_fee_bps")) / 10_000
        fill_price_sol = price_sol * (1 - slip)
        gross = pos["tokens"] * fill_price_sol
        fee_sol = gross * fee
        proceeds = gross - fee_sol
        pnl = proceeds - pos["size_sol"]
        pnl_pct = pnl / pos["size_sol"] * 100 if pos["size_sol"] else 0.0
        now = time.time()

        order = Order(mint=pos["mint"], side=Side.SELL, size_sol=proceeds, idempotency_key=f"paper-sell-{pos['id']}-{uuid.uuid4().hex[:8]}",
                      mode=self.mode, expected_price_sol=price_sol, status=OrderStatus.CONFIRMED, ts=now)
        oid = await self.ctx.db.insert_order(order, position_id=pos["id"])
        await self.ctx.db.insert_fill(oid, pos["tokens"], proceeds, fill_price_sol, fee_sol, slip * 10_000)
        meta = {**pos.get("meta", {}), "exit_price_usd": price_usd, "exit_sol_usd": sol_usd, "exit_reason": reason, "exit_order_id": oid, **(extra or {})}
        await self.ctx.db.close_position(pos["id"], now, meta)
        holding = now - float(pos["opened_at"])
        tid = await self.ctx.db.insert_trade(pos["id"], pos["mint"], pos["lane"], now, holding, pnl, pnl_pct, reason)
        return {"trade_id": tid, "position_id": pos["id"], "mint": pos["mint"], "pnl_sol": pnl, "pnl_pct": pnl_pct, "holding_sec": holding, "reason": reason, "proceeds_sol": proceeds}
