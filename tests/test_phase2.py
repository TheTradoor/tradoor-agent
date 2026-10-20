"""Phase 2 replay: ingest -> screener -> scorer -> trader (paper) -> position manager.

No network. The fake GMGN serves a strong candidate, then bumps its price so the
position manager takes profit; a second run drives a stop loss.

    python -m tests.test_phase2
"""
from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.test_phase1 import GMGN_INFO, GMGN_SEC, GOOD, FakeGmgn, frames  # noqa: E402
from tradoor_agent.bus import EventBus  # noqa: E402
from tradoor_agent.component import Context  # noqa: E402
from tradoor_agent.config import Mode, Settings  # noqa: E402
from tradoor_agent.db import Database  # noqa: E402
from tradoor_agent.execution.paper import PaperBroker  # noqa: E402
from tradoor_agent.ingest.pumpportal import PumpPortalIngest  # noqa: E402
from tradoor_agent.models import Lane  # noqa: E402
from tradoor_agent.params import ParamStore  # noqa: E402
from tradoor_agent.positions.manager import PositionManager  # noqa: E402
from tradoor_agent.risk.gate import RiskGate  # noqa: E402
from tradoor_agent.scoring.scorer import Prev, Scorer, score_candidate  # noqa: E402
from tradoor_agent.screener import Screener  # noqa: E402
from tradoor_agent.state import RuntimeState  # noqa: E402
from tradoor_agent.trader import Trader  # noqa: E402


class PricedGmgn(FakeGmgn):
    """GOOD's price follows self.mult; SOL/USD fixed at 100."""

    def __init__(self):
        super().__init__()
        self.mult = 1.0

    async def token_info(self, mint):
        info = await super().token_info(mint)
        if mint == GOOD:
            info = json.loads(json.dumps(info))
            base = float(GMGN_INFO[GOOD]["price"]["price"])
            info["price"]["price"] = str(base * self.mult)
            info["price"]["price_1m"] = str(base * self.mult / 1.125)
        return info

    async def sol_price_usd(self):
        return 100.0


def unit_tests() -> None:
    weights = {"momentum": 30, "growth": 25, "quality": 30, "context": 15}
    b = {**{k: v for k, v in GMGN_INFO[GOOD].items() if not isinstance(v, dict)}, "symbol": "GOOD"}
    # emulate what the screener stores (snapshot metrics)
    b.update({"price_usd": 0.000045, "mcap_usd": 45000, "holder_count": 180, "liquidity_usd": 24000, "launchpad_progress": 0.62,
              "buys_1m": 14, "sells_1m": 5, "swaps_1m": 19, "price_change_1m": 0.125, "price_change_5m": 0.5,
              "top10_rate": 0.21, "bundler_rate": 0.04, "sniper_hold_rate": 0.12, "dev_hold_rate": 0.02, "fresh_wallet_rate": 0.1,
              "smart_wallets": 3, "renowned_wallets": 1, "creator_created_count": 2, "entrapment_rate": 0.0, "twitter": "goodcoin",
              "creation_ts": time.time() - 300})
    score, comps = score_candidate(b, None, time.time(), weights)
    assert 75 <= score <= 100, (score, comps)
    # a bundled dump looks bad even with momentum
    bad = {**b, "top10_rate": 0.44, "bundler_rate": 0.19, "sniper_hold_rate": 0.29, "dev_hold_rate": 0.14, "smart_wallets": 0, "renowned_wallets": 0, "creator_created_count": 45}
    s2, _ = score_candidate(bad, None, time.time(), weights)
    assert s2 < score - 15, (score, s2)
    # growth from the previous check beats the fallback
    s3, c3 = score_candidate(b, Prev(ts=time.time() - 60, holders=120, mcap=30000), time.time(), weights)
    assert c3["g_hold"] == 1.0 and c3["g_mcap"] == 1.0 and s3 >= score
    # missing data is neutral, never a crash
    s4, _ = score_candidate({"symbol": "X"}, None, time.time(), weights)
    assert 0 <= s4 <= 100
    # entry filters
    from tradoor_agent.trader import Trader
    class _P:  # minimal ctx.p stand-in
        def __init__(self): self.v = dict(entry_max_chase_1m=0.25, entry_max_chase_5m=1.0, entry_min_holders=40, entry_max_sell_ratio_1m=0.55, entry_min_swaps_1m=8)
        def __call__(self, k): return self.v[k]
    tr = Trader.__new__(Trader); tr.ctx = type("C", (), {"p": _P()})()
    assert tr._entry_filter(b) is None
    assert tr._entry_filter({**b, "price_change_1m": 0.40}).startswith("chase_1m")
    assert tr._entry_filter({**b, "holder_count": 12}).startswith("holders")
    assert tr._entry_filter({**b, "buys_1m": 3, "sells_1m": 2}).startswith("quiet")
    assert tr._entry_filter({**b, "buys_1m": 4, "sells_1m": 9}).startswith("distribution")
    assert tr._entry_filter({"symbol": "X"}) == "quiet 0 swaps/1m"
    print("unit tests OK")


async def integration() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="tradoor-p2-"))
    settings = Settings(_env_file=None, mode=Mode.DRY_RUN, data_dir=tmp, gmgn_api_key="fake", screen_first_check_sec=0, screen_recheck_sec=1,
                        position_check_sec=1, score_high=70, exit_tp_pct=0.15, exit_sl_pct=0.10, position_size_sol=0.1, max_open_positions=2,
                        pumpportal_api_key="test", tick_pricing=True, tick_stale_sec=2)
    db = Database(settings.db_path, settings.timezone)
    await db.open()
    state, bus = RuntimeState(mode=settings.mode), EventBus()
    ctx = Context(settings=settings, state=state, bus=bus, db=db, params=ParamStore(db, refresh_sec=0.5))
    await ctx.params.refresh(force=True)
    gm = PricedGmgn()
    broker = PaperBroker(ctx, gm)
    ingest = PumpPortalIngest(ctx, message_source=frames())
    comps = [ingest, Screener(ctx, gm, ingest), Scorer(ctx), Trader(ctx, broker, ingest), PositionManager(ctx, gm, broker, ingest)]
    tasks = [asyncio.create_task(c.run()) for c in comps]
    try:
        await asyncio.sleep(3.0)
        # ---- a BUY happened on GOOD, and only one (risk gate blocks re-entry)
        pos = await db.open_positions()
        assert len(pos) == 1 and pos[0]["mint"] == GOOD, pos
        assert abs(pos[0]["size_sol"] - 0.1) < 1e-9 and pos[0]["meta"]["score"] >= 70 and pos[0]["lane"] == "fast"
        assert GOOD in ingest.trade_subs, "position must subscribe its trade stream"
        dec = await db.fetchall("SELECT action, reason FROM decisions ORDER BY id")
        assert dec[0]["action"] == "buy" and any(d["reason"] == "already_in_position" for d in dec[1:]), [dict(d) for d in dec]
        orders = await db.fetchall("SELECT side, status FROM orders")
        assert [(o["side"], o["status"]) for o in orders] == [("buy", "confirmed")]
        cand = await db.fetchone("SELECT score, breakdown_json FROM candidates WHERE mint=? AND score > 0 ORDER BY id LIMIT 1", (GOOD,))
        assert cand and cand["score"] >= 70 and json.loads(cand["breakdown_json"])["route"] == "fast"
        print(f"BUY ok · score {cand['score']} · entry {pos[0]['entry_price_sol']:.3e} SOL/token")

        # ---- price +25% -> TP
        gm.mult = 1.25
        gm._cache.clear()
        await asyncio.sleep(2.5)
        trades = await db.fetchall("SELECT * FROM trades")
        assert len(trades) == 1 and trades[0]["exit_reason"] == "tp" and trades[0]["pnl_sol"] > 0.015, [dict(t) for t in trades]
        assert await db.open_positions_count() == 0
        assert GOOD not in ingest.trade_subs, "closed position must unsubscribe"
        stats = await db.trade_stats()
        assert stats["wins"] == 1 and stats["streak"] == 1
        print(f"SELL tp ok · pnl {trades[0]['pnl_sol']:+.4f} SOL ({trades[0]['pnl_pct']:+.1f}%)")

        # ---- re-entry is blocked by the cooldown; after clearing it a new entry and a stop loss happen
        rg = RiskGate(ctx)
        v = await rg.check(GOOD, Lane.FAST)
        assert not v.allowed and v.reason == "reentry_cooldown", v
        await db.set_param("reentry_cooldown_sec", 0, source="test")
        await ctx.params.refresh(force=True)
        gm.mult = 1.0
        gm._cache.clear()
        await asyncio.sleep(2.5)
        pos2 = await db.open_positions()
        assert len(pos2) == 1
        # a single trade tick at -15% must trigger the stop loss immediately, without waiting for GMGN
        from tradoor_agent.bus import TOPIC_TRADE
        from tradoor_agent.models import Side, TradeEvent
        entry_sol = float(pos2[0]["entry_price_sol"])
        bus.publish(TOPIC_TRADE, TradeEvent(mint=GOOD, ts=time.time(), side=Side.SELL, sol=0.5, tokens=0.5 / (entry_sol * 0.85), trader="X", price_sol=entry_sol * 0.85))
        await asyncio.sleep(0.5)
        trades = await db.fetchall("SELECT exit_reason, pnl_sol FROM trades ORDER BY id")
        assert [t["exit_reason"] for t in trades] == ["tp", "sl"] and trades[1]["pnl_sol"] < 0, [dict(t) for t in trades]
        pm = await db.fetchone("SELECT meta_json FROM positions WHERE id=?", (pos2[0]["id"],))
        assert json.loads(pm["meta_json"])["exit_price_source"] == "tick"
        v = await rg.check(GOOD, Lane.FAST)
        assert not v.allowed and v.reason == "cooldown_after_loss", v
        eq = await db.equity_curve(settings.paper_start_sol)
        assert len(eq) == 2 and abs(eq[-1]["balance"] - (settings.paper_start_sol + trades[0]["pnl_sol"] + trades[1]["pnl_sol"])) < 1e-9
        kinds = [a["kind"] for a in state.activity]
        assert kinds.count("trade") >= 4
        print("SELL sl + cooldown ok ·", dict(state.counters))
    finally:
        state.request_stop()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=15)
        await db.close()
    print("integration OK")


if __name__ == "__main__":
    unit_tests()
    asyncio.run(integration())
    print("PHASE 2 TESTS PASSED")
