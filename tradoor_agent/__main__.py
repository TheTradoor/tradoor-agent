"""Entry point: python -m tradoor_agent [--check | --smoke]"""
from __future__ import annotations

import argparse
import asyncio
import sys
import tempfile
from pathlib import Path

from . import __version__
from .config import Mode, Settings
from .supervisor import KillFilePresent, Supervisor


def _print_check(settings: Settings) -> int:
    print(f"TheTradoor Agent v{__version__} · config check")
    print("-" * 48)
    for k, v in settings.summary().items():
        print(f"{k:22} {v}")
    print("-" * 48)
    problems = settings.problems()
    if problems:
        print("MASALAH:")
        for p in problems:
            print(f"  - {p}")
        return 1
    if settings.kill_file.exists():
        print(f"PERHATIAN: KILL file ada di {settings.kill_file}; agent gak bakal start sampai dihapus.")
        return 1
    print("OK: config bisa dipakai.")
    return 0


async def _smoke() -> int:
    """Boot the whole supervisor in a temp dir, no Telegram, run 3s, shut down."""
    with tempfile.TemporaryDirectory(prefix="tradoor-smoke-") as tmp:
        settings = Settings(_env_file=None, mode=Mode.DRY_RUN, data_dir=Path(tmp), telegram_bot_token="", gmgn_api_key="", log_level="INFO")
        sup = Supervisor(settings)
        task = asyncio.create_task(sup.start())
        await asyncio.sleep(3)
        tables = await sup.db.tables()
        beats = dict(sup.state.heartbeats)
        sup.request_stop()
        await asyncio.wait_for(task, timeout=15)

    expected = {"tokens", "events", "candidates", "llm_reviews", "decisions", "orders", "fills", "positions", "trades", "params", "daily_stats"}
    missing = expected - set(tables)
    ok = not missing and {"telegram_ops", "housekeeper", "pumpportal"} <= set(beats)
    print("-" * 48)
    print(f"tables   : {len(tables)} ({'ok' if not missing else 'missing ' + ','.join(sorted(missing))})")
    print(f"heartbeat: {', '.join(sorted(beats)) or '-'}")
    print("SMOKE OK" if ok else "SMOKE FAILED")
    return 0 if ok else 1


async def _probe_ws(settings: Settings, n: int) -> int:
    """Connect to PumpPortal, print the first n raw frames, exit. Run this on the VPS."""
    import json
    import websockets
    from .ingest.pumpportal import parse_message, ws_url

    url = ws_url(settings.pumpportal_ws_url, settings.pumpportal_api_key)
    print(f"connecting {settings.pumpportal_ws_url} (key: {'yes' if settings.pumpportal_api_key else 'no'})")
    kinds: dict[str, int] = {}
    try:
        async with websockets.connect(url, open_timeout=15) as ws:
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            await ws.send(json.dumps({"method": "subscribeMigration"}))
            for i in range(n):
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
                kind, _ = parse_message(raw)
                kinds[kind] = kinds.get(kind, 0) + 1
                text = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
                print(f"[{i + 1:02d}] {kind:8} {text[:400]}")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        return 1
    print("-" * 48)
    print("kinds:", kinds)
    print("OK" if kinds.get("create") else "WARNING: no create events parsed; check field names above")
    return 0


async def _check_gmgn(settings: Settings) -> int:
    """Hit GMGN once with the configured key: user info + a token snapshot."""
    import json
    from .ingest.gmgn import GmgnError
    from .supervisor import build_gmgn

    if not settings.gmgn_api_key:
        print("GMGN_API_KEY kosong")
        return 1
    g = build_gmgn(settings)
    rc = 0
    try:
        try:
            u = await g.user_info()
            print("user/info:", json.dumps(u, default=str)[:500])
        except GmgnError as e:
            print("user/info failed:", e)
        wsol = "So11111111111111111111111111111111111111112"
        snap = await g.snapshot(wsol)
        print("token/info+security for WSOL ->", {k: v for k, v in snap.metrics().items() if k in ("price_usd", "holder_count", "liquidity_usd", "top10_rate", "renounced_mint")})
        print(f"stats: {g.stats.requests} req · {g.stats.errors} err · weight used {g.stats.weight_used:.0f}")
        print("OK" if snap.price_usd else "WARNING: no price parsed; dump raw with --dump-gmgn <mint>")
    except GmgnError as e:
        print("GMGN ERROR:", e)
        rc = 1
    finally:
        await g.close()
    return rc


async def _dump_gmgn(settings: Settings, mint: str) -> int:
    """Print raw info + security payloads for one mint (for mapping new fields)."""
    import json
    from .supervisor import build_gmgn

    g = build_gmgn(settings)
    try:
        info = await g.token_info(mint)
        sec = await g.token_security(mint)
        print("=== /v1/token/info ===")
        print(json.dumps(info, indent=1, default=str)[:6000])
        print("=== /v1/token/security ===")
        print(json.dumps(sec, indent=1, default=str)[:3000])
        print("=== normalized ===")
        from .ingest.gmgn import build_snapshot
        print(json.dumps(build_snapshot(mint, info or {}, sec or {}).metrics(), indent=1, default=str))
    finally:
        await g.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tradoor_agent", description="TheTradoor Agent")
    parser.add_argument("--check", action="store_true", help="validate config and exit")
    parser.add_argument("--smoke", action="store_true", help="boot in a temp dir for 3s and exit")
    parser.add_argument("--probe-ws", type=int, metavar="N", help="print N raw PumpPortal frames and exit")
    parser.add_argument("--check-gmgn", action="store_true", help="test the GMGN API key and field mapping")
    parser.add_argument("--dump-gmgn", metavar="MINT", help="print raw GMGN info+security for a mint")
    parser.add_argument("--version", action="version", version=f"tradoor_agent {__version__}")
    args = parser.parse_args(argv)

    if args.smoke:
        return asyncio.run(_smoke())

    settings = Settings()
    if args.check:
        return _print_check(settings)
    if args.probe_ws:
        return asyncio.run(_probe_ws(settings, args.probe_ws))
    if args.check_gmgn:
        return asyncio.run(_check_gmgn(settings))
    if args.dump_gmgn:
        return asyncio.run(_dump_gmgn(settings, args.dump_gmgn))

    problems = settings.problems()
    if problems:
        print("Config bermasalah, gak jalan:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 2

    try:
        asyncio.run(Supervisor(settings).start())
    except KillFilePresent as e:
        print(str(e), file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
