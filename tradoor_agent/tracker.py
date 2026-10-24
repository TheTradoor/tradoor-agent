"""Outcome tracker (v0.8): follow the price path of every token a lane decided on.

The article's step 1 is "monitor tokens from those triggers first: how many pump, how
fast, when they dump". The screener already leaves a 30s path for launches (its
re-checks). Trigger lanes have no such path, and a traded position only records
until its exit. The tracker fills that gap: for TRACK_DURATION_SEC after a lane
passes a token (traded OR observed) it writes one candidates row per
TRACK_INTERVAL_SEC with lane="track" and the mcap/price/holders at that moment.
tools/outlook.py reads those rows and grades each lane on the same footing.

Cost: token/info only (weight 1). TRACK_MAX x 1 / TRACK_INTERVAL_SEC => 80/30 = 2.7 weight/s.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from .bus import TOPIC_TRACK
from .component import Component, Context
from .ingest.gmgn import GmgnClient, GmgnError, build_snapshot
from .models import Candidate, Lane

CHECKS_PER_TICK = 6


@dataclass
class Tracked:
    mint: str
    lane: str
    symbol: str
    started: float
    next_check: float
    candidate_id: int | None = None
    base_mcap: float | None = None
    checks: int = 0
    peak: float | None = None
    trough: float | None = None
    fails: int = 0


class OutcomeTracker(Component):
    name = "tracker"

    def __init__(self, ctx: Context, gmgn: GmgnClient) -> None:
        super().__init__(ctx)
        self.gmgn = gmgn
        self.tracked: dict[str, Tracked] = {}

    async def run(self) -> None:
        q = self.ctx.bus.subscribe(TOPIC_TRACK, maxsize=2000)
        while not self.ctx.state.stop.is_set():
            self.beat()
            self._drain(q)
            if self.ctx.p("track_enabled"):
                await self._run_due()
            self._expire()
            self.ctx.state.counters["track.active"] = len(self.tracked)
            if await self.sleep(1.0):
                return

    def _drain(self, q: asyncio.Queue) -> None:
        p = self.ctx.p
        while True:
            try:
                evt = q.get_nowait()
            except asyncio.QueueEmpty:
                return
            mint = str(evt.get("mint") or "")
            if not mint or mint in self.tracked:
                continue
            if len(self.tracked) >= int(p("track_max")):
                self.ctx.state.bump("track.full")
                continue
            now = time.time()
            base = evt.get("mcap_usd")
            self.tracked[mint] = Tracked(mint=mint, lane=str(evt.get("lane") or "?"), symbol=str(evt.get("symbol") or mint[:6]), started=now,
                                         next_check=now + int(p("track_interval_sec")), candidate_id=evt.get("candidate_id"),
                                         base_mcap=float(base) if isinstance(base, (int, float)) and base > 0 else None)
            self.ctx.state.bump("track.started")

    async def _run_due(self) -> None:
        now = time.time()
        if now < self.gmgn.stats.cooldown_until:
            return
        due = sorted((t for t in self.tracked.values() if t.next_check <= now), key=lambda t: t.next_check)[:CHECKS_PER_TICK]
        if due:
            await asyncio.gather(*(self._check(t) for t in due), return_exceptions=True)

    async def _check(self, t: Tracked) -> None:
        now = time.time()
        t.next_check = now + int(self.ctx.p("track_interval_sec"))
        try:
            info = await self.gmgn.token_info(t.mint)
        except GmgnError:
            t.fails += 1
            self.ctx.state.bump("track.errors")
            return
        except Exception:
            self.log.exception("track check crashed for %s", t.mint[:8])
            return
        snap = build_snapshot(t.mint, info or {}, {})
        t.checks += 1
        mcap = snap.mcap_usd
        if mcap:
            if t.base_mcap is None:
                t.base_mcap = mcap
            rel = mcap / t.base_mcap - 1.0 if t.base_mcap else None
            if rel is not None:
                t.peak = rel if t.peak is None else max(t.peak, rel)
                t.trough = rel if t.trough is None else min(t.trough, rel)
        m = snap.metrics()
        # compact row: the outlook needs the path (mcap/price/holders/flow), not the whole snapshot, and these rows are many
        b = {k: m[k] for k in ("mcap_usd", "price_usd", "holder_count", "liquidity_usd", "buys_1m", "sells_1m", "volume_1m", "price_change_1m",
                                "launchpad_progress", "migrated_pool", "creator_status", "top10_rate") if k in m}
        b.update({"symbol": snap.symbol or t.symbol, "track_of": t.lane, "track_base_mcap": t.base_mcap, "track_origin_id": t.candidate_id,
                  "track_elapsed_sec": int(now - t.started)})
        cand = Candidate(mint=t.mint, ts=now, score=0.0, breakdown=b, gates=[], token_age_sec=(now - snap.creation_ts) if snap.creation_ts else 0.0,
                         lane=Lane.TRACK, reason=f"track:{t.lane}")
        await self.ctx.db.insert_candidate(cand)
        self.ctx.state.bump("track.checks")

    def _expire(self) -> None:
        now = time.time()
        dur = int(self.ctx.p("track_duration_sec"))
        for mint in [m for m, t in self.tracked.items() if now - t.started >= dur or t.fails >= 5]:
            t = self.tracked.pop(mint)
            self.ctx.state.bump("track.done")
            if t.peak is not None and t.trough is not None:
                self.ctx.state.log("tracker", "info", f"TRACKED ${t.symbol} · {t.lane} · {int((now - t.started) / 60)}m · max {t.peak * 100:+.0f}% · min {t.trough * 100:+.0f}%", mint,
                                   candidate_id=t.candidate_id)

    def summary(self) -> list[dict]:
        now = time.time()
        return [{"mint": t.mint, "symbol": t.symbol, "lane": t.lane, "elapsed_sec": int(now - t.started), "checks": t.checks,
                 "peak": t.peak, "trough": t.trough} for t in sorted(self.tracked.values(), key=lambda t: t.started, reverse=True)]
