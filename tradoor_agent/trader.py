"""Trader (phase 2): scored candidate -> [LLM grey review] -> risk gate -> broker.

One consumer of TOPIC_SCORED. The broker is the paper broker in dry_run; phase 3
plugs in the live broker behind the same interface.
"""
from __future__ import annotations

import asyncio
import time

from .bus import TOPIC_POSITION, TOPIC_SCORED
from .component import Component, Context
from .execution.paper import PaperBroker
from .llm_review.reviewer import LLMReviewer
from .models import Candidate, Decision, Lane, Verdict
from .risk.gate import RiskGate


class Trader(Component):
    name = "trader"

    def __init__(self, ctx: Context, broker: PaperBroker, ingest=None) -> None:
        super().__init__(ctx)
        self.broker = broker
        self.ingest = ingest
        self.risk = RiskGate(ctx)
        self.llm = LLMReviewer(ctx)
        self._lock = asyncio.Lock()      # one entry decision at a time: risk checks must see the previous fill

    async def run(self) -> None:
        q = self.ctx.bus.subscribe(TOPIC_SCORED, maxsize=1000)
        while not self.ctx.state.stop.is_set():
            self.beat()
            try:
                cand: Candidate = await asyncio.wait_for(q.get(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            try:
                async with self._lock:
                    await self._handle(cand)
            except Exception:
                self.log.exception("trader failed on %s", cand.mint[:8])

    def _entry_filter(self, b: dict) -> str | None:
        """Return a reason to skip, or None. Missing data never blocks (the gates already vetted structure)."""
        p = self.ctx.p
        num = lambda k: (float(b[k]) if isinstance(b.get(k), (int, float)) else None)  # noqa: E731
        pc1, pc5 = num("price_change_1m"), num("price_change_5m")
        if pc1 is not None and pc1 > p("entry_max_chase_1m"):
            return f"chase_1m +{pc1 * 100:.0f}%"
        if pc5 is not None and pc5 > p("entry_max_chase_5m"):
            return f"chase_5m +{pc5 * 100:.0f}%"
        holders = num("holder_count")
        if holders is not None and holders < p("entry_min_holders"):
            return f"holders {int(holders)}"
        buys, sells = num("buys_1m") or 0.0, num("sells_1m") or 0.0
        swaps = buys + sells
        if swaps < p("entry_min_swaps_1m"):
            return f"quiet {int(swaps)} swaps/1m"
        if sells / swaps > p("entry_max_sell_ratio_1m"):
            return f"distribution {sells / swaps * 100:.0f}% sells"
        return None

    async def _handle(self, cand: Candidate) -> None:
        st, p = self.ctx.state, self.ctx.p
        b = cand.breakdown
        sym = b.get("symbol") or cand.mint[:6]
        lane = cand.lane
        score = cand.score

        if lane == Lane.DROP:
            return

        # ---- grey lane: LLM may boost into FAST territory, or veto; neutral = no entry
        if lane == Lane.GREY:
            if not self.llm.enabled():
                st.bump("trader.grey_skipped")
                return
            rev = await self.llm.review(cand.mint, cand.id, b)
            if rev.verdict == Verdict.BOOST:
                score = score + min(float(p("llm_max_boost")), float(p("llm_max_boost")) * rev.confidence)
                b["llm"] = {"verdict": rev.verdict.value, "confidence": rev.confidence, "reasons": rev.reasons, "boosted_score": round(score, 1)}
                if score < p("score_high"):
                    st.log("llm", "info", f"GREY ${sym} boost → {score:.0f} still < {p('score_high')} · skip", cand.mint, candidate_id=cand.id)
                    return
                st.log("llm", "pass", f"GREY ${sym} BOOST {rev.confidence:.2f} → {score:.0f} · {'; '.join(rev.reasons)[:120]}", cand.mint, candidate_id=cand.id)
            else:
                st.log("llm", "info", f"GREY ${sym} {rev.verdict.value.upper()} {rev.confidence:.2f}{' · ' + rev.error if rev.error else ''} · skip", cand.mint, candidate_id=cand.id)
                return

        # ---- entry filters: do not chase, do not buy into distribution, do not buy what you cannot exit
        blocked = self._entry_filter(b)
        if blocked:
            st.bump(f"entry.skip.{blocked.split(' ')[0]}")
            st.log("risk", "info", f"SKIP ${sym} · score {score:.0f} · {blocked}", cand.mint, candidate_id=cand.id)
            await self.ctx.db.insert_decision(Decision(mint=cand.mint, candidate_id=cand.id, action="skip", lane=lane, size_sol=0.0, reason=f"entry:{blocked}"), {"score": score})
            return

        # ---- risk gate
        verdict = await self.risk.check(cand.mint, lane)
        decision = Decision(mint=cand.mint, candidate_id=cand.id, action="buy" if verdict.allowed else "skip", lane=lane, size_sol=verdict.size_sol,
                            reason=verdict.reason if not verdict.allowed else f"score {score:.0f} {lane.value}")
        await self.ctx.db.insert_decision(decision, verdict.checks)
        if not verdict.allowed:
            st.bump(f"risk.deny.{verdict.reason}")
            st.last_active["risk"] = time.time()
            if verdict.reason not in ("already_in_position", "reentry_cooldown"):
                st.log("risk", "warn", f"SKIP ${sym} · {verdict.reason}", cand.mint, candidate_id=cand.id)
            return
        st.last_active["risk"] = time.time()

        # ---- entry
        price_usd = b.get("price_usd")
        if not isinstance(price_usd, (int, float)) or price_usd <= 0:
            st.log("exec", "warn", f"SKIP ${sym} · no price in snapshot", cand.mint, candidate_id=cand.id)
            return
        meta = {"symbol": sym, "score": score, "score_components": b.get("score"), "candidate_id": cand.id, "lane": lane.value,
                "holders": b.get("holder_count"), "mcap_usd": b.get("mcap_usd"), "liquidity_usd": b.get("liquidity_usd"),
                "bundler_rate": b.get("bundler_rate"), "top10_rate": b.get("top10_rate"), "launchpad_progress": b.get("launchpad_progress"),
                "migrated": b.get("migrated"), "llm": b.get("llm"), "creator_status_at_entry": b.get("creator_status")}
        # subscribe ticks BEFORE the buy so the first post-entry trades are not missed
        if self.ingest and p("tick_pricing") and self.ctx.settings.pumpportal_api_key:
            try:
                await self.ingest.subscribe_trades([cand.mint])
            except Exception:
                self.log.exception("trade subscribe failed for %s", cand.mint[:8])
        pos = await self.broker.buy(cand.mint, lane, verdict.size_sol, float(price_usd), meta)
        if pos is None:
            if self.ingest and cand.mint in getattr(self.ingest, "trade_subs", ()):
                await self.ingest.unsubscribe_trades([cand.mint])
            st.log("exec", "error", f"BUY ${sym} failed · cannot price (SOL/USD unavailable?)", cand.mint, candidate_id=cand.id)
            return
        st.bump("trade.buy")
        st.last_active["exec"] = time.time()
        st.log("exec", "trade", f"BUY ${sym} · {verdict.size_sol:.3f} SOL @ {pos['entry_price_sol']:.3e} · score {score:.0f} · {lane.value}", cand.mint,
               candidate_id=cand.id, position_id=pos["id"])
        self.ctx.bus.alert(f"BUY ${sym} · {verdict.size_sol:.3f} SOL · score {score:.0f} · mcap ${(b.get('mcap_usd') or 0) / 1000:.1f}k · {self.broker.mode}", "trade")
        self.ctx.bus.publish(TOPIC_POSITION, {"event": "open", "position": pos})
