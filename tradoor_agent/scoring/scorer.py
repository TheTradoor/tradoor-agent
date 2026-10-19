"""Scoring + routing (phase 2).

Deterministic, pure, fast. Consumes screened candidates (passed all hard gates) and
produces a 0-100 score from four component groups, each normalized to 0-1:

  momentum  buy pressure 1m, swap rate 1m, price change 1m/5m
  growth    holder velocity and mcap velocity vs the previous check of the same mint
  quality   concentration / bundle / sniper / dev / fresh-wallet / serial-launcher penalties, smart money bonus
  context   bonding progress sweet spot, socials, holder base, mcap band

Weights (SCORE_W_*) are relative and overridable live. Every component value is
written back into the candidate's breakdown["score"] so the autopsy can learn which
components predict winners. Routing:

  score >= SCORE_HIGH                       -> FAST  (deterministic entry)
  SCORE_LOW <= score < SCORE_HIGH and old enough -> GREY  (LLM review if enabled, else drop)
  otherwise                                 -> DROP
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from ..bus import TOPIC_CANDIDATE, TOPIC_SCORED
from ..component import Component, Context
from ..models import Candidate, Lane


def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


def ramp(v: float | None, zero_at: float, one_at: float) -> float | None:
    """Linear map: value at zero_at -> 0, at one_at -> 1 (direction follows the arguments)."""
    if v is None:
        return None
    if one_at == zero_at:
        return 1.0
    return clamp((v - zero_at) / (one_at - zero_at))


def wmean(parts: list[tuple[float | None, float]]) -> float:
    """Weighted mean that ignores None components (missing data is neither reward nor penalty)."""
    num = den = 0.0
    for v, w in parts:
        if v is None:
            continue
        num += v * w
        den += w
    return num / den if den else 0.5


@dataclass
class Prev:
    ts: float
    holders: int | None
    mcap: float | None


def score_candidate(b: dict, prev: Prev | None, now: float, weights: dict[str, float]) -> tuple[float, dict]:
    """b = candidate.breakdown (GMGN snapshot metrics + symbol/migrated/pool). Returns (score, components)."""
    g = lambda k: b.get(k)  # noqa: E731
    num = lambda k: (float(b[k]) if isinstance(b.get(k), (int, float)) else None)  # noqa: E731

    # ---- momentum
    buys1, sells1 = num("buys_1m") or 0.0, num("sells_1m") or 0.0
    buy_ratio = (buys1 / (buys1 + sells1)) if (buys1 + sells1) >= 3 else None     # need some trades to mean anything
    m_ratio = ramp(buy_ratio, 0.50, 0.80)
    m_swaps = ramp(num("swaps_1m") if num("swaps_1m") is not None else (buys1 + sells1), 3, 40)
    m_pc1 = ramp(num("price_change_1m"), -0.05, 0.15)
    m_pc5 = ramp(num("price_change_5m"), -0.10, 0.40)
    momentum = wmean([(m_ratio, 0.35), (m_swaps, 0.25), (m_pc1, 0.25), (m_pc5, 0.15)])

    # ---- growth (velocity vs previous check; falls back to holders-per-minute since creation)
    holders = num("holder_count")
    mcap = num("mcap_usd")
    g_hold = g_mcap = None
    if prev and prev.ts and now - prev.ts >= 10:
        dt_min = (now - prev.ts) / 60.0
        if holders is not None and prev.holders is not None:
            g_hold = ramp((holders - prev.holders) / dt_min, 0.0, 8.0)
        if mcap and prev.mcap:
            g_mcap = ramp((mcap / prev.mcap - 1.0) / dt_min, 0.0, 0.12)
    if g_hold is None and holders is not None and isinstance(g("creation_ts"), (int, float)):
        age_min = max((now - float(b["creation_ts"])) / 60.0, 0.5)
        g_hold = ramp(holders / age_min, 1.0, 20.0)
    growth = wmean([(g_hold, 0.6), (g_mcap, 0.4)])

    # ---- quality (lower concentration / bundle / sniper / dev is better; smart money is a bonus)
    q_top10 = ramp(num("top10_rate"), 0.45, 0.15)
    q_bund = ramp(num("bundler_rate"), 0.20, 0.0)
    q_snip = ramp(num("sniper_hold_rate"), 0.30, 0.0)
    dev = num("dev_hold_rate") if num("dev_hold_rate") is not None else num("creator_hold_rate")
    q_dev = ramp(dev, 0.15, 0.0)
    q_fresh = ramp(num("fresh_wallet_rate"), 0.60, 0.10)
    q_smart = ramp((num("smart_wallets") or 0.0) + 1.5 * (num("renowned_wallets") or 0.0), 0.0, 3.0)
    q_serial = ramp(num("creator_created_count"), 40.0, 3.0)
    q_trap = ramp(num("entrapment_rate"), 0.10, 0.0)
    quality = wmean([(q_top10, 0.2), (q_bund, 0.2), (q_snip, 0.15), (q_dev, 0.15), (q_fresh, 0.1), (q_smart, 0.1), (q_serial, 0.05), (q_trap, 0.05)])

    # ---- context
    prog = num("launchpad_progress")
    migrated = bool(g("migrated")) or bool(g("migrated_pool"))
    if migrated:
        c_stage = 0.7                                    # tradable on AMM, but the easy bonding run is over
    elif prog is None:
        c_stage = None
    elif prog < 0.10:
        c_stage = 0.3
    elif prog <= 0.85:
        c_stage = 1.0
    else:
        c_stage = 0.6                                    # about to migrate: price gap / volatility risk
    c_social = 1.0 if (g("twitter") or g("website") or g("telegram")) else 0.3
    c_holders = ramp(holders, 30, 150)
    if mcap is None:
        c_mcap = None
    elif mcap < 8_000:
        c_mcap = 0.2
    elif mcap <= 60_000:
        c_mcap = 1.0
    else:
        c_mcap = clamp(1.0 - (mcap - 60_000) / 190_000, 0.3, 1.0)
    context = wmean([(c_stage, 0.35), (c_social, 0.15), (c_holders, 0.25), (c_mcap, 0.25)])

    wm, wg, wq, wc = (max(weights.get(k, 0.0), 0.0) for k in ("momentum", "growth", "quality", "context"))
    total_w = (wm + wg + wq + wc) or 1.0
    score = 100.0 * (momentum * wm + growth * wg + quality * wq + context * wc) / total_w
    comps = {
        "momentum": round(momentum, 3), "growth": round(growth, 3), "quality": round(quality, 3), "context": round(context, 3),
        "m_ratio": m_ratio, "m_swaps": m_swaps, "m_pc1": m_pc1, "m_pc5": m_pc5,
        "g_hold": g_hold, "g_mcap": g_mcap,
        "q_top10": q_top10, "q_bund": q_bund, "q_snip": q_snip, "q_dev": q_dev, "q_fresh": q_fresh, "q_smart": q_smart, "q_serial": q_serial, "q_trap": q_trap,
        "c_stage": c_stage, "c_social": c_social, "c_holders": c_holders, "c_mcap": c_mcap,
    }
    return round(score, 1), {k: (round(v, 3) if isinstance(v, float) else v) for k, v in comps.items()}


class Scorer(Component):
    name = "scorer"

    def __init__(self, ctx: Context) -> None:
        super().__init__(ctx)
        self._prev: dict[str, Prev] = {}

    def route(self, score: float, token_age_sec: float) -> Lane:
        p = self.ctx.p
        if score >= p("score_high"):
            return Lane.FAST
        if score >= p("score_low") and token_age_sec >= p("grey_min_token_age_sec"):
            return Lane.GREY
        return Lane.DROP

    async def run(self) -> None:
        q = self.ctx.bus.subscribe(TOPIC_CANDIDATE, maxsize=2000)
        while not self.ctx.state.stop.is_set():
            self.beat()
            try:
                cand: Candidate = await asyncio.wait_for(q.get(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self._score(cand)
            except Exception:
                self.log.exception("scoring failed for %s", cand.mint[:8])
            # forget stale history
            if len(self._prev) > 2000:
                cutoff = time.time() - 3600
                self._prev = {m: pv for m, pv in self._prev.items() if pv.ts >= cutoff}

    async def _score(self, cand: Candidate) -> None:
        now = time.time()
        p = self.ctx.p
        weights = {"momentum": p("score_w_momentum"), "growth": p("score_w_growth"), "quality": p("score_w_quality"), "context": p("score_w_context")}
        prev = self._prev.get(cand.mint)
        score, comps = score_candidate(cand.breakdown, prev, now, weights)
        b = cand.breakdown
        self._prev[cand.mint] = Prev(ts=now, holders=b.get("holder_count") if isinstance(b.get("holder_count"), int) else None,
                                    mcap=float(b["mcap_usd"]) if isinstance(b.get("mcap_usd"), (int, float)) else None)
        lane = self.route(score, cand.token_age_sec)
        cand.score = score
        cand.lane = lane
        b["score"] = comps
        b["route"] = lane.value
        if cand.id:
            await self.ctx.db.update_candidate_score(cand.id, score, b)
        self.ctx.state.bump(f"score.{lane.value}")
        self.ctx.state.last_active["scorer"] = now
        sym = b.get("symbol") or cand.mint[:6]
        self.ctx.state.log("scorer", "info" if lane == Lane.DROP else "pass",
                           f"SCORE ${sym} {score:.0f} → {lane.value.upper()} · mom {comps['momentum']:.2f} gro {comps['growth']:.2f} qua {comps['quality']:.2f} ctx {comps['context']:.2f}",
                           cand.mint, candidate_id=cand.id)
        self.ctx.bus.publish(TOPIC_SCORED, cand)
