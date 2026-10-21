"""Web dashboard: a small aiohttp server inside the agent process.

    http://<vps-ip>:8787/?token=<WEB_TOKEN>

Read endpoints (all require the token, as ?token=, cookie, or Bearer header):
    GET  /api/status            state, counters, ws, gmgn, watchlist, params
    GET  /api/candidates        ?minutes=60&lane=all|screened|drop&limit=200
    GET  /api/candidate/{id}    one row with gates + full metrics
    GET  /api/timeline          per-minute launches / passes / fails (last 120 min)
    GET  /api/frames            last raw PumpPortal frames
    GET  /api/trades            recent paper/live trades + open positions (phase 2+)
Write endpoints:
    POST /api/pause  /api/resume
    POST /api/param  {"key": "...", "value": ...}
"""
from __future__ import annotations

import json
import os
import secrets
import time
from pathlib import Path

from aiohttp import web

from ..component import Component, Context

STATIC_DIR = Path(__file__).parent / "static"
COOKIE = "tradoor_token"


class WebDashboard(Component):
    name = "web"

    def __init__(self, ctx: Context) -> None:
        super().__init__(ctx)
        self.token = ctx.settings.web_token
        self._runner: web.AppRunner | None = None

    # ------------------------------------------------------------ lifecycle

    async def ensure_token(self) -> str:
        """Use WEB_TOKEN if set; otherwise create one once and keep it in the params table."""
        if self.token:
            return self.token
        stored = await self.ctx.db.get_param("web_token")
        if not stored:
            stored = secrets.token_urlsafe(18)
            await self.ctx.db.set_param("web_token", stored, source="web:autogen")
        self.token = str(stored)
        return self.token

    def url(self) -> str:
        s = self.ctx.settings
        base = s.web_public_url.rstrip("/") if s.web_public_url else f"http://<ip-vps>:{s.web_port}"
        return f"{base}/?token={self.token}"

    async def run(self) -> None:
        s = self.ctx.settings
        await self.ensure_token()
        app = web.Application(middlewares=[self._auth_middleware])
        app.router.add_get("/", self.page)
        app.router.add_get("/api/status", self.api_status)
        app.router.add_get("/api/candidates", self.api_candidates)
        app.router.add_get("/api/candidate/{cid}", self.api_candidate)
        app.router.add_get("/api/timeline", self.api_timeline)
        app.router.add_get("/api/frames", self.api_frames)
        app.router.add_get("/api/trades", self.api_trades)
        app.router.add_get("/api/activity", self.api_activity)
        app.router.add_get("/api/funnel", self.api_funnel)
        app.router.add_get("/api/equity", self.api_equity)
        app.router.add_post("/api/pause", self.api_pause)
        app.router.add_post("/api/resume", self.api_resume)
        app.router.add_post("/api/param", self.api_param)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, s.web_host, s.web_port)
        await site.start()
        self.ctx.web_url = self.url()
        self.log.info("dashboard on http://%s:%s  (token %s…)", s.web_host, s.web_port, self.token[:4])
        try:
            while not await self.sleep(5):
                self.beat()
        finally:
            await self._runner.cleanup()

    # ------------------------------------------------------------ auth

    @web.middleware
    async def _auth_middleware(self, request: web.Request, handler):
        tok = request.query.get("token") or request.cookies.get(COOKIE) or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if not tok or not secrets.compare_digest(tok, self.token):
            if request.path.startswith("/api/"):
                return web.json_response({"error": "unauthorized"}, status=401)
            return web.Response(text=_LOGIN_HTML, content_type="text/html", status=401)
        resp = await handler(request)
        if request.query.get("token"):
            resp.set_cookie(COOKIE, self.token, max_age=30 * 86400, httponly=True, samesite="Lax")
        return resp

    # ------------------------------------------------------------ pages

    async def page(self, request: web.Request) -> web.Response:
        html = (STATIC_DIR / "dashboard.html").read_text(encoding="utf-8").replace("__PAGE_VERSION__", _version())
        return web.Response(text=html, content_type="text/html", headers={"Cache-Control": "no-store, max-age=0", "Pragma": "no-cache"})

    # ------------------------------------------------------------ api

    async def api_status(self, request: web.Request) -> web.Response:
        ctx, st, s = self.ctx, self.ctx.state, self.ctx.settings
        now = time.time()
        ingest = ctx.ingest
        gm = ctx.gmgn
        scr = ctx.screener
        ws = None
        if ingest is not None:
            ws = {
                "connected": bool(getattr(ingest, "connected", False)),
                "last_message_age_sec": int(now - ingest.last_message_at) if ingest.last_message_at else None,
                "trade_subs": len(getattr(ingest, "trade_subs", ())),
                "last_error": getattr(ingest, "last_error", ""),
                "kinds_recent": _count_kinds(getattr(ingest, "recent", ())),
            }
        gmgn = None
        if gm is not None:
            gs = gm.stats
            gmgn = {
                "requests": gs.requests, "cache_hits": gs.cache_hits, "errors": gs.errors, "rate_limited": gs.rate_limited,
                "weight_used": round(gs.weight_used), "waited_sec": round(gs.waited_sec, 1),
                "cooldown_sec": max(0, int(gs.cooldown_until - now)), "last_error": gs.last_error,
                "security_errors": gs.security_errors, "last_security_error": gs.last_security_error, "sample_keys": gs.sample_keys,
                "budget_per_sec": round(s.gmgn_rate_weight * s.gmgn_budget_fraction, 1), "by_path": gs.by_path,
            }
        watch = []
        if scr is not None:
            for e in sorted(scr.watch.values(), key=lambda e: e.first_seen, reverse=True)[:200]:
                snap = e.last_snap
                watch.append({
                    "mint": e.evt.mint, "symbol": (snap.symbol if snap and snap.symbol else e.evt.symbol),
                    "age_sec": int(now - e.first_seen), "checks": e.checks, "passes": e.passes,
                    "last_failed": e.last_failed, "migrated": e.migrated, "next_check_sec": max(0, int(e.next_check - now)),
                    "last_check_age_sec": round(now - e.last_check, 1) if e.last_check else None,
                    "candidate_id": e.last_candidate_id,
                    "mcap_usd": snap.mcap_usd if snap else None, "holders": snap.holder_count if snap else None,
                    "liquidity_usd": snap.liquidity_usd if snap else None, "top10": snap.top10_rate if snap else None,
                    "smart": snap.smart_wallets if snap else None, "snipers": snap.sniper_count if snap else None,
                    "progress": snap.launchpad_progress if snap else None,
                    "buys_1m": snap.buys_1m if snap else None, "sells_1m": snap.sells_1m if snap else None,
                })
        tstats = await ctx.db.trade_stats()
        lane_stats = await ctx.db.trade_stats_by_lane()
        dex = ctx.dexpaid
        return _json({
            "lane_stats": lane_stats,
            "dexpaid": {"enabled": bool(s.dexpaid_enabled), "last_poll_age_sec": (int(now - dex.last_poll) if dex is not None and dex.last_poll else None),
                        "last_error": getattr(dex, "last_error", "") if dex is not None else ""},
            "now": now,
            "version": _version(),
            "pid": os.getpid(),
            "paper_start_sol": s.paper_start_sol,
            "balance_sol": s.paper_start_sol + tstats["pnl_sol"],
            "trade_stats": tstats,
            "last_active": {k: round(now - v, 1) for k, v in st.last_active.items()},
            "mode": s.mode.value, "paused": st.paused, "kill_requested": st.kill_requested,
            "can_enter": st.can_enter, "uptime_sec": int(st.uptime_sec),
            "counters": dict(st.counters),
            "heartbeats_age_sec": {k: int(now - v) for k, v in st.heartbeats.items()},
            "ws": ws, "gmgn": gmgn, "watchlist": watch,
            "params": ctx.params.snapshot(),
            "settings": {k: getattr(s, k) for k in dir(s) if k.startswith(("gate_", "cheap_", "screen_", "score_", "llm_", "position_", "max_open", "daily_loss", "exit_", "paper_", "cooldown", "reentry", "entry_", "tick_", "dexpaid_")) and not callable(getattr(s, k))},
            "open_positions": await ctx.db.open_positions_count(),
            "pnl_today_sol": await ctx.db.realized_pnl_today(),
            "candidates_24h": await ctx.db.candidate_counts_today(),
        })

    async def api_candidates(self, request: web.Request) -> web.Response:
        minutes = _int(request.query.get("minutes"), 60, 1, 7 * 1440)
        limit = _int(request.query.get("limit"), 200, 1, 1000)
        lane = request.query.get("lane", "all")
        sql = "SELECT id, ts, mint, lane, reason, token_age_sec, score, breakdown_json FROM candidates WHERE ts > ?"
        args: list = [time.time() - minutes * 60]
        if lane in ("screened", "drop"):
            sql += " AND lane = ?"
            args.append(lane)
        sql += " ORDER BY ts DESC LIMIT ?"
        args.append(limit)
        rows = await self.ctx.db.fetchall(sql, args)
        out = []
        for r in rows:
            b = _loads(r["breakdown_json"])
            out.append({
                "id": r["id"], "ts": r["ts"], "mint": r["mint"], "lane": r["lane"], "reason": r["reason"],
                "score": r["score"], "route": b.get("route"),
                "age_sec": int(r["token_age_sec"] or 0), "symbol": b.get("symbol"),
                "mcap_usd": b.get("mcap_usd"), "liquidity_usd": b.get("liquidity_usd"), "holders": b.get("holder_count"),
                "top10": b.get("top10_rate"), "dev": b.get("dev_hold_rate", b.get("creator_hold_rate")),
                "bundler": b.get("bundler_rate"), "insider": b.get("insider_rate"), "snipers": b.get("sniper_count"),
                "sniper_hold": b.get("sniper_hold_rate"), "entrapment": b.get("entrapment_rate"), "created_count": b.get("creator_created_count"),
                "smart": b.get("smart_wallets"), "renowned": b.get("renowned_wallets"),
                "buys_1m": b.get("buys_1m"), "sells_1m": b.get("sells_1m"), "buys_5m": b.get("buys_5m"), "sells_5m": b.get("sells_5m"),
                "progress": b.get("launchpad_progress"), "migrated": b.get("migrated"),
            })
        return _json({"rows": out})

    async def api_candidate(self, request: web.Request) -> web.Response:
        cid = _int(request.match_info.get("cid"), 0, 1, 2**62)
        r = await self.ctx.db.fetchone("SELECT * FROM candidates WHERE id=?", (cid,))
        if not r:
            return web.json_response({"error": "not found"}, status=404)
        return _json({**{k: r[k] for k in r.keys() if not k.endswith("_json")}, "breakdown": _loads(r["breakdown_json"]), "gates": _loads(r["gates_json"], [])})

    async def api_timeline(self, request: web.Request) -> web.Response:
        minutes = _int(request.query.get("minutes"), 120, 10, 1440)
        since = time.time() - minutes * 60
        launches = await self.ctx.db.fetchall("SELECT CAST(first_seen_at/60 AS INTEGER)*60 AS m, COUNT(*) AS n FROM tokens WHERE first_seen_at > ? GROUP BY m", (since,))
        cands = await self.ctx.db.fetchall("SELECT CAST(ts/60 AS INTEGER)*60 AS m, lane, COUNT(DISTINCT mint) AS n FROM candidates WHERE ts > ? GROUP BY m, lane", (since,))
        series: dict[int, dict] = {}
        for r in launches:
            series.setdefault(int(r["m"]), {"launches": 0, "screened": 0, "drop": 0})["launches"] = r["n"]
        for r in cands:
            series.setdefault(int(r["m"]), {"launches": 0, "screened": 0, "drop": 0})[r["lane"] if r["lane"] in ("screened", "drop") else "drop"] += r["n"]
        start = int(since // 60) * 60
        points = [{"m": m, **series.get(m, {"launches": 0, "screened": 0, "drop": 0})} for m in range(start, int(time.time() // 60) * 60 + 60, 60)]
        return _json({"points": points})

    async def api_frames(self, request: web.Request) -> web.Response:
        ingest = self.ctx.ingest
        frames = list(getattr(ingest, "recent", ()))[::-1] if ingest is not None else []
        return _json({"frames": frames})

    async def api_trades(self, request: web.Request) -> web.Response:
        pos = await self.ctx.db.open_positions()
        for p in pos:
            m = p.get("meta") or {}
            p["symbol"] = m.get("symbol")
            p["score"] = m.get("score")
            p["last_change"] = m.get("last_change")
            p["peak_change"] = (float(m["peak_price_sol"]) / float(p["entry_price_sol"]) - 1.0) if m.get("peak_price_sol") and p.get("entry_price_sol") else None
            p["mcap_usd"] = m.get("mcap_usd")
            p.pop("meta", None)
        trades = await self.ctx.db.fetchall(
            "SELECT t.id, t.mint, t.lane, t.day, t.closed_at, t.holding_sec, t.pnl_sol, t.pnl_pct, t.exit_reason, p.meta_json, p.size_sol "
            "FROM trades t LEFT JOIN positions p ON p.id = t.position_id ORDER BY t.closed_at DESC LIMIT 100")
        out = []
        for r in trades:
            d = dict(r)
            m = _loads(d.pop("meta_json"))
            d["symbol"] = m.get("symbol")
            d["score"] = m.get("score")
            out.append(d)
        return _json({"positions": pos, "trades": out})

    _funnel_cache: tuple[float, dict] | None = None

    async def api_funnel(self, request: web.Request) -> web.Response:
        """Gate funnel over recent checks: which gates block, metric percentiles, best tokens seen."""
        minutes = _int(request.query.get("minutes"), 60, 5, 24 * 60)
        now = time.time()
        if self._funnel_cache and now - self._funnel_cache[0] < 45 and self._funnel_cache[1].get("minutes") == minutes:
            return _json(self._funnel_cache[1])
        rows = await self.ctx.db.fetchall(
            "SELECT id, ts, mint, lane, reason, token_age_sec, gates_json, breakdown_json FROM candidates WHERE ts > ? ORDER BY ts DESC LIMIT 25000",
            (now - minutes * 60,),
        )
        gate_eval: dict[str, int] = {}
        gate_fail: dict[str, int] = {}
        gate_na: dict[str, int] = {}
        latest: dict[str, dict] = {}          # per mint: latest check
        best: dict[str, dict] = {}            # per mint: check with max mcap
        for r in rows:
            gates = _loads(r["gates_json"], [])
            b = _loads(r["breakdown_json"])
            for g in gates:
                n = g.get("name")
                if not n:
                    continue
                if str(g.get("detail") or "").startswith("n/a"):
                    gate_na[n] = gate_na.get(n, 0) + 1
                    continue
                gate_eval[n] = gate_eval.get(n, 0) + 1
                if not g.get("passed"):
                    gate_fail[n] = gate_fail.get(n, 0) + 1
            cts = b.get("creation_ts")
            if isinstance(cts, (int, float)) and r["ts"] - cts > 3600:
                continue   # existing token with a new pool, not a launch
            rec = {"id": r["id"], "ts": r["ts"], "mint": r["mint"], "lane": r["lane"], "age": int(r["token_age_sec"] or 0),
                   "symbol": b.get("symbol"), "mcap": b.get("mcap_usd"), "holders": b.get("holder_count"), "liq": b.get("liquidity_usd"),
                   "top10": b.get("top10_rate"), "bundler": b.get("bundler_rate"), "insider": b.get("insider_rate"), "snipers": b.get("sniper_count"),
                   "sniper_hold": b.get("sniper_hold_rate"), "entrapment": b.get("entrapment_rate"),
                   "dev": b.get("dev_hold_rate", b.get("creator_hold_rate")), "smart": b.get("smart_wallets"),
                   "indexed": bool(b.get("mcap_usd")) and (b.get("holder_count") or 0) > 1,
                   "failed": [g.get("name") for g in gates if not g.get("passed")],
                   "failed_detail": [f"{g.get('name')} {str(g.get('detail') or '').split(' <= ')[0].split(' >= ')[0]}" for g in gates if not g.get("passed")]}
            if r["mint"] not in latest:
                latest[r["mint"]] = rec
            m = rec["mcap"] or 0
            if r["mint"] not in best or m > (best[r["mint"]]["mcap"] or 0):
                best[r["mint"]] = rec
        funnel = []
        for n in sorted(set(gate_eval) | set(gate_na), key=lambda k: -gate_fail.get(k, 0)):
            ev = gate_eval.get(n, 0)
            funnel.append({"gate": n, "evaluated": ev, "failed": gate_fail.get(n, 0),
                           "fail_pct": round(gate_fail.get(n, 0) / ev * 100, 1) if ev else None, "na": gate_na.get(n, 0)})

        indexed = {m: v for m, v in latest.items() if v["indexed"]}

        def pcts(key):
            vals = sorted(v[key] for v in indexed.values() if isinstance(v.get(key), (int, float)))
            if not vals:
                return None
            q = lambda f: vals[min(len(vals) - 1, int(f * (len(vals) - 1)))]  # noqa: E731
            return {"n": len(vals), "p50": q(0.5), "p90": q(0.9), "max": vals[-1]}

        metrics = {k: pcts(k) for k in ("holders", "mcap", "liq", "top10", "bundler", "sniper_hold", "entrapment", "snipers", "dev", "smart")}
        top = sorted(best.values(), key=lambda v: -(v["mcap"] or 0))[:12]
        one_away: dict[str, int] = {}
        for v in latest.values():
            if len(v["failed"]) == 1:
                one_away[v["failed"][0]] = one_away.get(v["failed"][0], 0) + 1
        out = {"minutes": minutes, "checks": len(rows), "mints": len(latest), "indexed_mints": len(indexed),
               "passed_mints": sum(1 for v in best.values() if v["lane"] == "screened"),
               "funnel": funnel, "metrics": metrics, "top_mcap": top,
               "one_gate_away": sorted(one_away.items(), key=lambda kv: -kv[1])}
        self._funnel_cache = (now, out)
        return _json(out)

    async def api_activity(self, request: web.Request) -> web.Response:
        limit = _int(request.query.get("limit"), 80, 1, 400)
        return _json({"items": list(self.ctx.state.activity)[:limit]})

    async def api_equity(self, request: web.Request) -> web.Response:
        s = self.ctx.settings
        curve = await self.ctx.db.equity_curve(s.paper_start_sol)
        return _json({"start_sol": s.paper_start_sol, "points": curve, "now": time.time()})

    async def api_pause(self, request: web.Request) -> web.Response:
        self.ctx.state.paused = True
        self.log.warning("paused from dashboard")
        return _json({"paused": True})

    async def api_resume(self, request: web.Request) -> web.Response:
        self.ctx.state.paused = False
        self.log.warning("resumed from dashboard")
        return _json({"paused": False})

    async def api_param(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
            key = str(body["key"]).strip()
            value = body["value"]
        except Exception:
            return web.json_response({"error": "body: {key, value}"}, status=400)
        if not key or key == "web_token":
            return web.json_response({"error": "bad key"}, status=400)
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
        if value is None or value == "":
            await self.ctx.db.execute("DELETE FROM params WHERE key=?", (key,))
        else:
            await self.ctx.db.set_param(key, value, source="dashboard")
        await self.ctx.params.refresh(force=True)
        self.log.warning("param %s = %r (dashboard)", key, value)
        return _json({"ok": True, "params": self.ctx.params.snapshot()})


# ---------------------------------------------------------------- helpers

def _json(data) -> web.Response:
    return web.Response(text=json.dumps(data, default=str), content_type="application/json", headers={"Cache-Control": "no-store"})


def _loads(s, default=None):
    try:
        return json.loads(s) if s else (default if default is not None else {})
    except (TypeError, json.JSONDecodeError):
        return default if default is not None else {}


def _int(v, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(v)))
    except (TypeError, ValueError):
        return default


def _count_kinds(frames) -> dict[str, int]:
    out: dict[str, int] = {}
    for f in frames:
        out[f["kind"]] = out.get(f["kind"], 0) + 1
    return out


def _version() -> str:
    from .. import __version__
    return __version__


_LOGIN_HTML = """<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>TheTradoor Agent</title><body style="font-family:system-ui;background:#0b0f14;color:#e6edf3;display:grid;place-items:center;height:100vh;margin:0">
<form onsubmit="location.href='/?token='+encodeURIComponent(t.value);return false" style="background:#111826;padding:24px;border-radius:12px;min-width:280px">
<div style="font-weight:600;margin-bottom:12px">TheTradoor Agent</div>
<input id=t placeholder="token" autofocus style="width:100%;padding:10px;border-radius:8px;border:1px solid #2a3443;background:#0b0f14;color:#e6edf3">
<button style="margin-top:10px;width:100%;padding:10px;border:0;border-radius:8px;background:#2f81f7;color:#fff;font-weight:600">masuk</button>
</form></body>"""
