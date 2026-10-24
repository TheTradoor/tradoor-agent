"""Candidate outlook: what happened to tokens AFTER a lane passed them?

Uses nothing but the candidates table (one row per check, ~30s apart: screener
re-checks for launches, tracker rows (lane="track") for trigger lanes). For every
token's FIRST pass (lanes screened/fast/grey for launches, dexpaid/migration/trending
for triggers) we take the mcap at that moment and look at all later rows of the same mint:

  max up      best mcap afterwards vs the pass mcap   (what a perfect exit could have made)
  max down    worst mcap afterwards vs the pass mcap  (what the stop would have faced)
  at +5/+10/+30 min   mcap at that horizon vs pass mcap (what a time-based hold gets)

Run on the VPS:
    cd /opt/tradoor-agent && .venv/bin/python -m tradoor_agent.tools.outlook [--hours 48] [--db data/agent.sqlite] [--lane migration]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from collections import defaultdict
from pathlib import Path


def pct(x: float | None) -> str:
    return "   –  " if x is None else f"{x * 100:+6.0f}%"


def quantile(vals: list[float], q: float) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    return s[min(len(s) - 1, int(q * (len(s) - 1)))]


PASS_LANES = ("screened", "fast", "grey", "dexpaid", "migration", "trending")


def lane_group(lane: str) -> str:
    return "launch" if lane in ("screened", "fast", "grey") else lane


def bucket_score(score: float | None) -> str:
    if score is None or score <= 0:
        return "unscored"
    if score >= 85:
        return "85+"
    if score >= 80:
        return "80-85"
    if score >= 70:
        return "70-80"
    return "<70"


def bucket_stage(b: dict) -> str:
    if b.get("migrated") or b.get("migrated_pool"):
        return "migrated"
    p = b.get("launchpad_progress")
    if not isinstance(p, (int, float)):
        return "unknown"
    if p < 0.2:
        return "bond<20%"
    if p < 0.6:
        return "bond20-60%"
    return "bond60%+"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/agent.sqlite")
    ap.add_argument("--hours", type=float, default=48)
    ap.add_argument("--min-mcap", type=float, default=8000)
    ap.add_argument("--lane", default="all", help="all | launch | dexpaid | migration | trending")
    args = ap.parse_args()

    con = sqlite3.connect(Path(args.db))
    con.row_factory = sqlite3.Row
    since = time.time() - args.hours * 3600
    rows = con.execute(
        "SELECT mint, ts, lane, score, token_age_sec, breakdown_json FROM candidates WHERE ts > ? ORDER BY mint, ts", (since,)
    ).fetchall()
    if not rows:
        print("no candidates in range")
        return

    # group checks per mint
    by_mint: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        try:
            b = json.loads(r["breakdown_json"] or "{}")
        except json.JSONDecodeError:
            b = {}
        mcap = b.get("mcap_usd")
        by_mint[r["mint"]].append({"ts": r["ts"], "lane": r["lane"], "score": r["score"], "age": r["token_age_sec"] or 0,
                                   "mcap": float(mcap) if isinstance(mcap, (int, float)) and mcap > 0 else None, "b": b})

    samples = []
    for mint, checks in by_mint.items():
        # first pass with a price
        idx = next((i for i, c in enumerate(checks) if c["lane"] in PASS_LANES and c["mcap"] and c["mcap"] >= args.min_mcap
                    and (args.lane == "all" or lane_group(c["lane"]) == args.lane)), None)
        if idx is None:
            continue
        p0 = checks[idx]
        later = [c for c in checks[idx + 1:] if c["mcap"]]
        if not later:
            continue
        base = p0["mcap"]
        rel = [(c["ts"] - p0["ts"], c["mcap"] / base - 1.0) for c in later]
        horizon_val = {}
        for h in (300, 600, 1800):
            pts = [r for r in rel if r[0] <= h]
            horizon_val[h] = pts[-1][1] if pts else None
        samples.append({
            "mint": mint, "symbol": p0["b"].get("symbol") or mint[:6], "score": p0["score"], "stage": bucket_stage(p0["b"]), "lane": lane_group(p0["lane"]),
            "waited": p0["b"].get("trigger_waited_sec"),
            "age": p0["age"], "base": base, "tracked_sec": rel[-1][0],
            "max_up": max(r[1] for r in rel), "max_down": min(r[1] for r in rel),
            "h5": horizon_val[300], "h10": horizon_val[600], "h30": horizon_val[1800],
            "holders": p0["b"].get("holder_count"), "bundler": p0["b"].get("bundler_rate"), "top10": p0["b"].get("top10_rate"),
            "pc1": p0["b"].get("price_change_1m"), "smart": p0["b"].get("smart_wallets"),
        })

    if not samples:
        print("no passed candidates with later checks")
        return

    print(f"\n{len(samples)} tokens passed hard gates in the last {args.hours:.0f}h with >= 1 later check (min mcap ${args.min_mcap:,.0f})")
    print(f"tracked after pass: median {quantile([s['tracked_sec'] for s in samples], .5) / 60:.0f} min · p90 {quantile([s['tracked_sec'] for s in samples], .9) / 60:.0f} min\n")

    def table(label: str, groups: dict[str, list[dict]]) -> None:
        print(f"== {label}")
        print(f"{'group':12} {'n':>4}  {'≥+30%':>6} {'≥+50%':>6} {'≥+100%':>7}  {'≤-30%':>6} {'≤-50%':>6}  {'med max↑':>9} {'med max↓':>9}  {'med@5m':>7} {'med@10m':>8} {'med@30m':>8}")
        for g, ss in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            n = len(ss)
            if n == 0:
                continue
            up30 = sum(s["max_up"] >= 0.30 for s in ss) / n
            up50 = sum(s["max_up"] >= 0.50 for s in ss) / n
            up100 = sum(s["max_up"] >= 1.00 for s in ss) / n
            dn30 = sum(s["max_down"] <= -0.30 for s in ss) / n
            dn50 = sum(s["max_down"] <= -0.50 for s in ss) / n
            print(f"{g:12} {n:4d}  {up30 * 100:5.0f}% {up50 * 100:5.0f}% {up100 * 100:6.0f}%  {dn30 * 100:5.0f}% {dn50 * 100:5.0f}%  "
                  f"{pct(quantile([s['max_up'] for s in ss], .5)):>9} {pct(quantile([s['max_down'] for s in ss], .5)):>9}  "
                  f"{pct(quantile([s['h5'] for s in ss if s['h5'] is not None], .5)):>7} {pct(quantile([s['h10'] for s in ss if s['h10'] is not None], .5)):>8} "
                  f"{pct(quantile([s['h30'] for s in ss if s['h30'] is not None], .5)):>8}")
        print()

    table("all", {"all": samples})
    table("by lane (trigger source)", {k: [s for s in samples if s["lane"] == k] for k in ("launch", "dexpaid", "migration", "trending")})
    table("by score at pass", {k: [s for s in samples if bucket_score(s["score"]) == k] for k in ("85+", "80-85", "70-80", "<70", "unscored")})
    table("by stage at pass", {k: [s for s in samples if s["stage"] == k] for k in ("bond<20%", "bond20-60%", "bond60%+", "migrated", "unknown")})
    table("by holders at pass", {
        "<60": [s for s in samples if isinstance(s["holders"], int) and s["holders"] < 60],
        "60-120": [s for s in samples if isinstance(s["holders"], int) and 60 <= s["holders"] < 120],
        "120+": [s for s in samples if isinstance(s["holders"], int) and s["holders"] >= 120],
    })
    table("by 1m price change at pass", {
        "falling": [s for s in samples if isinstance(s["pc1"], (int, float)) and s["pc1"] < -0.02],
        "flat": [s for s in samples if isinstance(s["pc1"], (int, float)) and -0.02 <= s["pc1"] <= 0.10],
        "rising10-25": [s for s in samples if isinstance(s["pc1"], (int, float)) and 0.10 < s["pc1"] <= 0.25],
        "chasing>25": [s for s in samples if isinstance(s["pc1"], (int, float)) and s["pc1"] > 0.25],
    })
    table("by smart wallets at pass", {
        "0": [s for s in samples if (s["smart"] or 0) == 0],
        "1-2": [s for s in samples if 1 <= (s["smart"] or 0) <= 2],
        "3+": [s for s in samples if (s["smart"] or 0) >= 3],
    })

    # strategy simulations on the recorded path (30s granularity, so optimistic on stops; a trail also needs ticks to be honest)
    def sim(label: str, *, tp: float, sl: float, hold: int, arm: float | None = None, trail: float | None = None, flat_after: int = 0, flat_min: float = 0.0, group: list[dict] | None = None) -> str | None:
        wins, losses = [], []
        for s in (group if group is not None else samples):
            mint_checks = [c for c in by_mint[s["mint"]] if c["mcap"]]
            i0 = next(i for i, c in enumerate(mint_checks) if abs(c["mcap"] - s["base"]) < 1e-9 and c["ts"] <= mint_checks[-1]["ts"])
            base, t0 = s["base"], mint_checks[i0]["ts"]
            outcome, peak = None, 0.0
            for c in mint_checks[i0 + 1:]:
                r = c["mcap"] / base - 1.0
                peak = max(peak, r)
                if r <= -sl:
                    outcome = -sl; break
                if r >= tp:
                    outcome = tp; break
                if arm is not None and trail is not None and peak >= arm and r <= (1 + peak) * (1 - trail) - 1:
                    outcome = (1 + peak) * (1 - trail) - 1; break
                if flat_after and c["ts"] - t0 >= flat_after and peak < flat_min:
                    outcome = r; break
                if c["ts"] - t0 >= hold:
                    outcome = r; break
            if outcome is None:
                outcome = mint_checks[-1]["mcap"] / base - 1.0
            net = outcome - 0.06  # round trip: 2 x (1.5% slippage + 1% fee + 0.5% fixed on 0.1 SOL)
            (wins if net > 0 else losses).append(net)
        n = len(wins) + len(losses)
        if not n:
            return None
        aw = sum(wins) / len(wins) if wins else 0.0
        al = sum(losses) / len(losses) if losses else 0.0
        exp = (sum(wins) + sum(losses)) / n
        return f"{label:>22} {n:6d} {len(wins) / n * 100:4.0f}% {aw * 100:+7.1f}% {al * 100:+8.1f}% {exp * 100:+9.2f}% {(sum(wins) + sum(losses)) * 100:+7.0f}%"

    SIMS = (
        ("v0.8 trail10/5 sl12 15m", dict(tp=1.0, sl=0.12, hold=900, arm=0.10, trail=0.05, flat_after=300, flat_min=0.03)),
        ("trail15/8 sl12 15m", dict(tp=1.0, sl=0.12, hold=900, arm=0.15, trail=0.08)),
        ("trail20/10 sl15 30m", dict(tp=1.0, sl=0.15, hold=1800, arm=0.20, trail=0.10)),
        ("tp15/sl10/15m", dict(tp=0.15, sl=0.10, hold=900)),
        ("tp30/sl12/15m", dict(tp=0.30, sl=0.12, hold=900)),
        ("v0.7 tp50/sl15/30m", dict(tp=0.50, sl=0.15, hold=1800, arm=0.30, trail=0.15)),
    )
    print("== strategy sims on recorded paths (30s granularity; real stops and trails fill worse)")
    hdr = f"{'strategy':>22} {'trades':>6} {'win%':>5} {'avg win':>8} {'avg loss':>9} {'exp/trade':>10} {'total':>8}"
    print(hdr)
    for label, kw in SIMS:
        line = sim(label, **kw)
        if line:
            print(line)
    for lane in ("launch", "dexpaid", "migration", "trending"):
        grp = [s for s in samples if s["lane"] == lane]
        if len(grp) < 5:
            continue
        print(f"\n-- {lane} only ({len(grp)} tokens)")
        print(hdr)
        for label, kw in SIMS[:3]:
            line = sim(label, group=grp, **kw)
            if line:
                print(line)
    print("\nexp/trade = average net return per trade after 6% round-trip costs; total = sum over all passed tokens at equal size")

    # the best and worst to eyeball
    print("\n== top 10 runners after pass")
    for s in sorted(samples, key=lambda s: -s["max_up"])[:10]:
        print(f"  ${s['symbol']:12} {s['lane']:9} score {s['score'] or 0:4.0f}  {s['stage']:11} holders {s['holders'] or 0:4}  max↑ {pct(s['max_up'])}  @10m {pct(s['h10'])}  @30m {pct(s['h30'])}")


if __name__ == "__main__":
    main()
