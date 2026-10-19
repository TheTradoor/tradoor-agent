"""Dashboard test: boots ingest(replay) + screener(fake GMGN) + web on a local port,
exercises every API route, then (if Playwright is installed) screenshots the page.

    python -m tests.test_dashboard
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.test_phase1 import FakeGmgn, frames  # noqa: E402
from tradoor_agent.bus import EventBus  # noqa: E402
from tradoor_agent.component import Context  # noqa: E402
from tradoor_agent.config import Mode, Settings  # noqa: E402
from tradoor_agent.db import Database  # noqa: E402
from tradoor_agent.ingest.pumpportal import PumpPortalIngest  # noqa: E402
from tradoor_agent.ops.web import WebDashboard  # noqa: E402
from tradoor_agent.params import ParamStore  # noqa: E402
from tradoor_agent.screener import Screener  # noqa: E402
from tradoor_agent.state import RuntimeState  # noqa: E402

PORT = 18787
TOKEN = "test-token-123"
BASE = f"http://127.0.0.1:{PORT}"


async def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="tradoor-web-"))
    settings = Settings(_env_file=None, mode=Mode.DRY_RUN, data_dir=tmp, gmgn_api_key="fake",
                        screen_first_check_sec=0, screen_recheck_sec=1, web_port=PORT, web_token=TOKEN, web_host="127.0.0.1")
    db = Database(settings.db_path, settings.timezone)
    await db.open()
    state, bus = RuntimeState(mode=settings.mode), EventBus()
    ctx = Context(settings=settings, state=state, bus=bus, db=db, params=ParamStore(db, refresh_sec=0.5))
    await ctx.params.refresh(force=True)
    gm = FakeGmgn()
    ingest = PumpPortalIngest(ctx, message_source=frames())
    screener = Screener(ctx, gm, ingest)
    web = WebDashboard(ctx)
    ctx.ingest, ctx.gmgn, ctx.screener = ingest, gm, screener
    tasks = [asyncio.create_task(c.run()) for c in (ingest, screener, web)]
    try:
        await asyncio.sleep(3.0)
        async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as http:  # unsafe: accept cookies from an IP host
            # auth
            async with http.get(f"{BASE}/api/status") as r:
                assert r.status == 401, r.status
            async with http.get(f"{BASE}/") as r:
                assert r.status == 401 and "token" in await r.text()
            async with http.get(f"{BASE}/?token={TOKEN}") as r:
                assert r.status == 200 and "TheTradoor Agent" in await r.text()
                assert "tradoor_token" in r.cookies, "cookie must be set after token login"
            # cookie carries auth from now on
            st = await (await http.get(f"{BASE}/api/status")).json()
            assert st["mode"] == "dry_run" and st["ws"]["connected"] is False and st["gmgn"] is not None
            assert st["counters"]["ws.tokens_new"] == 4 and len(st["watchlist"]) >= 1
            assert "gate_max_top10" in st["settings"]
            c = await (await http.get(f"{BASE}/api/candidates?lane=all&minutes=60")).json()
            assert c["rows"] and {r["lane"] for r in c["rows"]} >= {"screened", "drop"}
            good = next(r for r in c["rows"] if r["lane"] == "screened")
            assert good["symbol"] == "GOOD" and good["mcap_usd"] == 45000 and good["top10"] == 0.21
            d = await (await http.get(f"{BASE}/api/candidate/{good['id']}")).json()
            assert d["gates"] and all(g["passed"] for g in d["gates"]) and d["breakdown"]["smart_wallets"] == 3
            only = await (await http.get(f"{BASE}/api/candidates?lane=screened")).json()
            assert all(r["lane"] == "screened" for r in only["rows"])
            tl = await (await http.get(f"{BASE}/api/timeline?minutes=30")).json()
            assert sum(p["launches"] for p in tl["points"]) == 4
            fr = await (await http.get(f"{BASE}/api/frames")).json()
            assert len(fr["frames"]) == 8 and fr["frames"][0]["kind"] == "unknown"
            tr = await (await http.get(f"{BASE}/api/trades")).json()
            assert tr["positions"] == [] and tr["trades"] == []
            fn = await (await http.get(f"{BASE}/api/funnel?minutes=60")).json()
            assert fn["checks"] > 0 and fn["mints"] >= 3 and fn["passed_mints"] >= 1, fn
            gates = {g["gate"]: g for g in fn["funnel"]}
            assert gates["top10"]["failed"] >= 1 and gates["entrapment"]["failed"] >= 1
            assert fn["top_mcap"][0]["symbol"] == "GOOD" and fn["top_mcap"][0]["failed"] == []
            assert fn["metrics"]["holders"]["max"] == 180
            act = await (await http.get(f"{BASE}/api/activity")).json()
            kinds = {i["kind"] for i in act["items"]}
            assert {"watch", "pass", "fail", "drop"} <= kinds or {"watch", "pass", "grow", "drop"} <= kinds, kinds
            assert any(i.get("candidate_id") for i in act["items"] if i["kind"] == "pass")
            eq = await (await http.get(f"{BASE}/api/equity")).json()
            assert eq["start_sol"] == settings.paper_start_sol and eq["points"] == []
            assert st["trade_stats"] == {"trades": 0, "pnl_sol": 0.0, "wins": 0, "losses": 0, "streak": 0}
            assert st["balance_sol"] == settings.paper_start_sol and st["pid"] > 0
            # seed fake closed trades so the equity chart / streak have something to show
            import time as _t
            now = _t.time()
            for i, pnl in enumerate([0.03, -0.02, 0.05, 0.04, -0.01, 0.06, 0.02]):
                pid = await db.execute("INSERT INTO positions(mint,lane,opened_at,closed_at,entry_price_sol,size_sol,tokens,status) VALUES(?,?,?,?,?,?,?,?)",
                                       (f"Mint{i}", "fast", now - 3600 + i * 400, now - 3600 + i * 400 + 120, 1e-7, 0.1, 1e6, "closed"))
                await db.execute("INSERT INTO trades(position_id,mint,lane,day,closed_at,holding_sec,pnl_sol,pnl_pct,exit_reason) VALUES(?,?,?,?,?,?,?,?,?)",
                                 (pid, f"Mint{i}", "fast", db.today(), now - 3600 + i * 400 + 120, 120, pnl, pnl / 0.1 * 100, "tp" if pnl > 0 else "sl"))
            eq = await (await http.get(f"{BASE}/api/equity")).json()
            assert len(eq["points"]) == 7 and abs(eq["points"][-1]["balance"] - (settings.paper_start_sol + 0.17)) < 1e-9
            st2 = await (await http.get(f"{BASE}/api/status")).json()
            assert st2["trade_stats"]["wins"] == 5 and st2["trade_stats"]["losses"] == 2 and st2["trade_stats"]["streak"] == 2
            # controls
            p = await (await http.post(f"{BASE}/api/pause")).json()
            assert p["paused"] is True and state.paused is True and state.can_enter is False
            p = await (await http.post(f"{BASE}/api/resume")).json()
            assert p["paused"] is False
            r = await (await http.post(f"{BASE}/api/param", json={"key": "gate_max_top10", "value": "0.1"})).json()
            assert r["ok"] and r["params"]["gate_max_top10"] == 0.1 and ctx.p("gate_max_top10") == 0.1
            bad = await http.post(f"{BASE}/api/param", json={"key": "web_token", "value": "x"})
            assert bad.status == 400
            r = await (await http.post(f"{BASE}/api/param", json={"key": "gate_max_top10", "value": ""})).json()
            assert "gate_max_top10" not in r["params"] and ctx.p("gate_max_top10") == settings.gate_max_top10
            await asyncio.sleep(1.5)
            st = await (await http.get(f"{BASE}/api/status")).json()
            assert st["counters"]["screen.pass"] >= 1
        print("api OK")
        await screenshot()
    finally:
        state.request_stop()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=15)
        await db.close()
    print("DASHBOARD TESTS PASSED")


async def screenshot() -> None:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("playwright not installed; skipping screenshot")
        return
    out = Path(os.environ.get("SHOT_DIR", tempfile.gettempdir()))
    try:
        async with async_playwright() as pw:
            b = await pw.chromium.launch()
            for name, vp in (("desktop", {"width": 1100, "height": 900}), ("mobile", {"width": 390, "height": 844})):
                pg = await b.new_page(viewport=vp, color_scheme="dark")
                await pg.goto(f"{BASE}/?token={TOKEN}", wait_until="networkidle")
                await pg.wait_for_timeout(3500)
                await pg.screenshot(path=str(out / f"dashboard-{name}.png"), full_page=(name == "mobile"))
                await pg.click(".coin")  # open candidate drawer from a coin card
                await pg.wait_for_timeout(600)
                await pg.screenshot(path=str(out / f"dashboard-{name}-drawer.png"))
                print("screenshot", out / f"dashboard-{name}.png")
            await b.close()
    except Exception as e:  # browser missing etc.
        print("screenshot skipped:", type(e).__name__, str(e)[:160])


if __name__ == "__main__":
    asyncio.run(main())
