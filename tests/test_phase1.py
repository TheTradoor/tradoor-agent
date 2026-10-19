"""Phase 1 replay test: no network. Feeds recorded-style PumpPortal frames into the
ingest, swaps GMGN for a fake, runs the screener for a few seconds and checks the
candidates table.

    python -m tests.test_phase1
"""
from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tradoor_agent.bus import TOPIC_ALERT, TOPIC_CANDIDATE, EventBus  # noqa: E402
from tradoor_agent.component import Context  # noqa: E402
from tradoor_agent.config import Mode, Settings  # noqa: E402
from tradoor_agent.db import Database  # noqa: E402
from tradoor_agent.gates.rules import cheap_gates, failed, hard_gates  # noqa: E402
from tradoor_agent.ingest.gmgn import GmgnClient, GmgnError, build_snapshot  # noqa: E402
from tradoor_agent.ingest.pumpportal import PumpPortalIngest, parse_message  # noqa: E402
from tradoor_agent.models import TokenEvent  # noqa: E402
from tradoor_agent.params import ParamStore  # noqa: E402
from tradoor_agent.screener import Screener  # noqa: E402
from tradoor_agent.state import RuntimeState  # noqa: E402

GOOD = "GoodMint1111111111111111111111111111111111111"
BUND = "BundMint2222222222222222222222222222222222222"
DEVB = "DevBuy33333333333333333333333333333333333333"
RUGD = "RugDev44444444444444444444444444444444444444"

FRAMES = [
    {"message": "Successfully subscribed to token creation events."},
    {"signature": "sig1", "mint": GOOD, "traderPublicKey": "DevA", "txType": "create", "initialBuy": 20_000_000, "solAmount": 0.6,
     "bondingCurveKey": "bc1", "vTokensInBondingCurve": 1.05e9, "vSolInBondingCurve": 30.6, "marketCapSol": 29.1, "name": "Good Coin", "symbol": "GOOD", "uri": "https://x/y", "pool": "pump"},
    {"signature": "sig2", "mint": BUND, "traderPublicKey": "DevB", "txType": "create", "initialBuy": 10_000_000, "solAmount": 0.3,
     "marketCapSol": 28.5, "name": "Bundled", "symbol": "BUND", "uri": "", "pool": "pump"},
    {"signature": "sig3", "mint": DEVB, "traderPublicKey": "DevC", "txType": "create", "initialBuy": 120_000_000, "solAmount": 4.2,
     "marketCapSol": 35.0, "name": "Dev Heavy", "symbol": "DEVB", "uri": "", "pool": "pump"},
    {"signature": "sig4", "mint": RUGD, "traderPublicKey": "DevD", "txType": "create", "initialBuy": 0, "solAmount": 0,
     "marketCapSol": 28.0, "name": "Rugger", "symbol": "RUGD", "uri": "", "pool": "pump"},
    {"signature": "sig5", "mint": GOOD, "traderPublicKey": "Buyer1", "txType": "buy", "tokenAmount": 3_000_000, "solAmount": 0.1, "marketCapSol": 31.0, "pool": "pump"},
    {"signature": "sig6", "mint": GOOD, "txType": "migrate", "pool": "pump-amm"},
    {"weird": True},
]

# fake GMGN payloads in the documented nested shape
GMGN_INFO = {
    GOOD: {"symbol": "GOOD", "name": "Good Coin", "total_supply": 1e9, "liquidity": 24000, "holder_count": 180,
           "launchpad_platform": "pump", "launchpad_status": 1, "launchpad_progress": 0.62,
           "price": {"price": "0.000045", "buys_1m": 14, "sells_1m": 5, "volume_1m": 2100, "swaps_1m": 19, "buys_5m": 60, "sells_5m": 22},
           "dev": {"creator_address": "DevA", "creator_token_status": "creator_hold"},
           "stat": {"top_10_holder_rate": 0.21, "dev_team_hold_rate": 0.02, "top_bundler_trader_percentage": 0.04, "top_rat_trader_percentage": 0.03, "fresh_wallet_rate": 0.1},
           "wallet_tags_stat": {"smart_wallets": 3, "renowned_wallets": 1, "sniper_wallets": 4, "bundler_wallets": 2},
           "link": {"twitter_username": "goodcoin"}},
    BUND: {"symbol": "BUND", "total_supply": 1e9, "liquidity": 15000, "holder_count": 95, "price": {"price": "0.00003"},
           "stat": {"top_10_holder_rate": 0.52, "dev_team_hold_rate": 0.01, "top_bundler_trader_percentage": 0.38}},
    RUGD: {"symbol": "RUGD", "total_supply": 1e9, "liquidity": 9000, "holder_count": 60, "price": {"price": "0.00002"},
           "stat": {"top_10_holder_rate": 0.25, "dev_team_hold_rate": 0.05}},
}
GMGN_SEC = {
    GOOD: {"renounced_mint": True, "renounced_freeze_account": True, "rug_ratio": 0.0, "is_wash_trading": False, "sniper_count": 6,
           "suspected_insider_hold_rate": 0.05, "bundler_trader_amount_rate": 0.04, "rat_trader_amount_rate": 0.03, "top_10_holder_rate": 0.21},
    BUND: {"renounced_mint": True, "renounced_freeze_account": True, "rug_ratio": 0.1, "sniper_count": 9, "bundler_trader_amount_rate": 0.38, "top_10_holder_rate": 0.52},
    RUGD: {"renounced_mint": True, "renounced_freeze_account": True, "rug_ratio": 0.8, "sniper_count": 2, "top_10_holder_rate": 0.25},
}


class FakeGmgn(GmgnClient):
    def __init__(self):
        super().__init__("fake", rate_weight=50)
        self.calls = 0

    async def open(self):  # no session
        return None

    async def close(self):
        return None

    async def token_info(self, mint):
        self.calls += 1
        if mint not in GMGN_INFO:
            raise GmgnError("/v1/token/info", 404, api_code=1, api_error="NOT_FOUND")
        return GMGN_INFO[mint]

    async def token_security(self, mint):
        self.calls += 1
        return GMGN_SEC.get(mint, {})


async def frames():
    for f in FRAMES:
        yield json.dumps(f)
        await asyncio.sleep(0.01)
    await asyncio.sleep(60)   # keep the "connection" open until stop


def unit_tests() -> None:
    # parser
    kinds = [parse_message(json.dumps(f))[0] for f in FRAMES]
    assert kinds == ["ack", "create", "create", "create", "create", "trade", "migrate", "unknown"], kinds
    kind, evt = parse_message(json.dumps(FRAMES[1]))
    assert isinstance(evt, TokenEvent) and evt.creator == "DevA" and evt.symbol == "GOOD"
    _, trade = parse_message(json.dumps(FRAMES[5]))
    assert trade.side.value == "buy" and abs(trade.price_sol - 0.1 / 3_000_000) < 1e-15

    # cheap gates: dev-heavy launch dropped, others pass
    assert failed(cheap_gates(parse_message(json.dumps(FRAMES[3]))[1], max_dev_buy_sol=3.0, max_dev_supply_pct=0.08)) == ["dev_buy_sol", "dev_supply_pct"]
    assert failed(cheap_gates(evt, max_dev_buy_sol=3.0, max_dev_supply_pct=0.08)) == []

    # snapshot normalization (nested + string numbers + percent-vs-fraction)
    s = build_snapshot(GOOD, GMGN_INFO[GOOD], GMGN_SEC[GOOD])
    assert s.price_usd == 0.000045 and s.mcap_usd == 45000 and s.holder_count == 180 and s.top10_rate == 0.21
    assert s.smart_wallets == 3 and s.buys_1m == 14 and s.launchpad_progress == 0.62 and s.renounced_mint is True
    s2 = build_snapshot("x", {"stat": {"top_10_holder_rate": 21.0}}, {})
    assert s2.top10_rate == 0.21, "percent values must be normalized to fractions"

    th = dict(min_holders=30, min_liquidity_usd=3000, min_mcap_usd=8000, max_mcap_usd=250000, max_top10=0.30, max_dev_hold=0.10,
              max_bundler=0.20, max_rat=0.20, max_insider=0.20, max_snipers=15, max_rug_ratio=0.30, require_renounced=True)
    assert failed(hard_gates(s, **th)) == []
    assert failed(hard_gates(build_snapshot(BUND, GMGN_INFO[BUND], GMGN_SEC[BUND]), **th)) == ["top10", "bundler"]
    assert failed(hard_gates(build_snapshot(RUGD, GMGN_INFO[RUGD], GMGN_SEC[RUGD]), **th)) == ["rug_ratio"]
    print("unit tests OK")


async def integration() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="tradoor-p1-"))
    settings = Settings(_env_file=None, mode=Mode.DRY_RUN, data_dir=tmp, gmgn_api_key="fake",
                        screen_first_check_sec=0, screen_recheck_sec=1, screen_alert_max_per_hour=5)
    db = Database(settings.db_path, settings.timezone)
    await db.open()
    state, bus = RuntimeState(mode=settings.mode), EventBus()
    ctx = Context(settings=settings, state=state, bus=bus, db=db, params=ParamStore(db, refresh_sec=0.5))
    await ctx.params.refresh(force=True)

    alerts = bus.subscribe(TOPIC_ALERT)
    cands = bus.subscribe(TOPIC_CANDIDATE)
    gm = FakeGmgn()
    ingest = PumpPortalIngest(ctx, message_source=frames())
    screener = Screener(ctx, gm, ingest)
    tasks = [asyncio.create_task(ingest.run()), asyncio.create_task(screener.run())]
    try:
        await asyncio.sleep(3.5)

        # live param override through the params table
        await db.set_param("gate_max_top10", 0.10, source="test")
        await asyncio.sleep(3.0)
        state.request_stop()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10)
        await _assert_results(db, state, screener, alerts, cands, gm)
    finally:
        state.request_stop()
        await db.close()
    print("integration OK")


async def _assert_results(db, state, screener, alerts, cands, gm) -> None:

    rows = await db.fetchall("SELECT mint, lane, reason FROM candidates ORDER BY id")
    by_mint: dict[str, list[str]] = {}
    for r in rows:
        by_mint.setdefault(r["mint"], []).append(f"{r['lane']}:{r['reason']}")
    print("candidates:", json.dumps({k[:8]: v for k, v in by_mint.items()}, indent=1))

    assert DEVB not in by_mint, "dev-heavy launch must die at cheap gates, never reach GMGN"
    assert by_mint[GOOD][0] == "screened:pass"
    assert any(x.startswith("drop:fail:top10") for x in by_mint[GOOD]), "param override to 0.10 must flip GOOD to fail"
    assert by_mint[BUND][0] == "drop:fail:top10,bundler"
    assert by_mint[RUGD][0] == "drop:fail:rug_ratio"
    assert BUND not in screener.watch and RUGD not in screener.watch, "structural fails must be evicted after 2 checks"
    assert not alerts.empty() and "screened $GOOD" in alerts.get_nowait()["text"]
    assert not cands.empty()
    tok = await db.fetchone("SELECT bonded_at, symbol FROM tokens WHERE mint=?", (GOOD,))
    assert tok["bonded_at"] is not None and tok["symbol"] == "GOOD", "migration must be recorded"
    c = state.counters
    assert c["ws.tokens_new"] == 4 and c["ws.trades"] == 1 and c["ws.migrations"] == 1 and c["ws.unknown"] == 1 and c["screen.cheap_drop"] == 1
    print("counters:", dict(c))
    print(f"gmgn calls: {gm.calls}")


if __name__ == "__main__":
    unit_tests()
    asyncio.run(integration())
    print("PHASE 1 TESTS PASSED")
