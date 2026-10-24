"""Trader: lanes -> filters -> security -> risk gate -> broker.

Two kinds of input:
  TOPIC_SCORED   scored launches from the screener/scorer (lanes fast / grey)
  TOPIC_TRIGGER  events from the trigger detectors (lanes dexpaid / migration / trending)

A trigger lane follows the same shape for every source (v0.8):
  snapshot -> structural gates (bundle, dev, snipers, honeypot, renounced...) -> fail = drop
           -> soft filters (holders, liquidity, mcap, chase, bleeding, quiet, distribution)
              fail = re-check every <lane>_recheck_sec until <lane>_window_sec runs out
           -> entry (or OBSERVE when the lane is in observe mode)
Every passed token, traded or observed, is handed to the outcome tracker so the lane
can be graded on its whole population, not just the trades the position cap allowed.

The broker is the paper broker in dry_run; phase 3 plugs in the live broker behind
the same interface.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from .bus import TOPIC_POSITION, TOPIC_SCORED, TOPIC_TRACK, TOPIC_TRIGGER
from .component import Component, Context
from .execution.paper import PaperBroker
from .gates.rules import bonding_from_metrics, failed, hard_gates
from .ingest.gmgn import GmgnClient, GmgnError
from .llm_review.reviewer import LLMReviewer
from .models import Candidate, Decision, Lane, Verdict
from .risk.gate import RiskGate

LANE_BY_SOURCE = {"dexpaid": Lane.DEXPAID, "migration": Lane.MIGRATION, "trending": Lane.TRENDING}
# gates whose failure means "wrong token", never "not yet": no re-check
STRUCTURAL = {"top10", "dev_hold", "bundler", "rat_trader", "snipers", "sniper_hold", "entrapment", "creator_created", "honeypot", "renounced_mint", "renounced_freeze", "mcap_max"}


@dataclass
class Pending:
    evt: dict
    lane: Lane
    first_ts: float
    next_ts: float
    attempts: int = 0
    last_block: str = ""
    history: list[str] = field(default_factory=list)


class Trader(Component):
    name = "trader"

    def __init__(self, ctx: Context, broker: PaperBroker, ingest=None, gmgn: GmgnClient | None = None) -> None:
        super().__init__(ctx)
        self.broker = broker
        self.ingest = ingest
        self.gmgn = gmgn
        self.risk = RiskGate(ctx)
        self.llm = LLMReviewer(ctx)
        self._lock = asyncio.Lock()      # one entry decision at a time: risk checks must see the previous fill
        self.pending: dict[str, Pending] = {}   # mint -> trigger waiting for a clean entry

    async def run(self) -> None:
        q = self.ctx.bus.subscribe(TOPIC_SCORED, maxsize=1000)
        q_trig = self.ctx.bus.subscribe(TOPIC_TRIGGER, maxsize=500)
        trig_task = asyncio.create_task(self._trigger_loop(q_trig), name="trader_triggers")
        try:
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
        finally:
            trig_task.cancel()

    # ------------------------------------------------------------ trigger lanes

    async def _trigger_loop(self, q: asyncio.Queue) -> None:
        while True:
            try:
                evt = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                evt = None
            if evt is not None:
                lane = LANE_BY_SOURCE.get(str(evt.get("source") or "dexpaid"))
                mint = str(evt.get("mint") or "")
                if lane is None or not mint:
                    continue
                if self.ctx.lane_mode(lane.value) == "off":
                    continue
                if mint in self.pending or await self.ctx.db.has_open_position(mint):
                    continue
                now = time.time()
                self.pending[mint] = Pending(evt=evt, lane=lane, first_ts=now, next_ts=now)
                self.ctx.state.counters[f"{lane.value}.triggers"] += 1
            await self._run_pending()

    async def _run_pending(self) -> None:
        now = time.time()
        due = [p for p in self.pending.values() if p.next_ts <= now]
        for pend in due:
            mint = str(pend.evt["mint"])
            try:
                async with self._lock:
                    outcome = await self._handle_trigger(pend)
            except Exception:
                self.log.exception("trigger handling failed for %s", mint[:8])
                outcome = "drop"
            if outcome == "retry":
                lane = pend.lane.value
                pend.attempts += 1
                pend.next_ts = time.time() + int(self.ctx.lane_p(lane, "recheck_sec", "trig_recheck_sec"))
                if pend.next_ts - pend.first_ts > int(self.ctx.lane_p(lane, "window_sec", "trig_window_sec")):
                    self.pending.pop(mint, None)
                    self.ctx.state.bump(f"{lane}.window_expired")
                    sym = pend.evt.get("symbol") or mint[:6]
                    self.ctx.state.log(lane, "info", f"GIVE UP ${sym} · {pend.attempts + 1} checks in {int(time.time() - pend.first_ts)}s · last: {pend.last_block}", mint)
            else:
                self.pending.pop(mint, None)

    async def _handle_trigger(self, pend: Pending) -> str:
        """One check of a pending trigger. Returns "done" (entered / observed / dropped for good) or "retry"."""
        st, p = self.ctx.state, self.ctx.p
        evt, lane = pend.evt, pend.lane
        L = lane.value
        mint = str(evt["mint"])
        if self.gmgn is None:
            return "done"
        lp = lambda key, fb: self.ctx.lane_p(L, key, fb)  # noqa: E731
        try:
            snap = await self.gmgn.snapshot(mint)
        except GmgnError as e:
            if e.rate_limited:
                return "retry"
            if pend.attempts < 2:
                return "retry"      # a token minutes old may not be indexed yet
            st.log(L, "warn", f"{L.upper()} {mint[:6]}… · no GMGN data · {str(e)[:60]}", mint)
            return "done"
        sym = snap.symbol or evt.get("symbol") or mint[:6]
        now = time.time()
        age = (now - snap.creation_ts) if snap.creation_ts else None
        b = snap.metrics()
        b.update({"symbol": sym, "trigger": L, "trigger_ts": evt.get("ts"), "trigger_kind": evt.get("kind"), "trigger_rank": evt.get("rank"),
                  "trigger_links": evt.get("links"), "trigger_attempt": pend.attempts, "dexpaid": L == "dexpaid"})
        if evt.get("migrated_ts"):
            b["migrated"] = True

        # ---- age: a trigger on an old token is not a launch play
        max_age = int(lp("max_token_age_sec", "trig_max_token_age_sec"))
        if age is not None and age > max_age:
            if pend.attempts == 0:
                st.log(L, "info", f"SKIP ${sym} · token {int(age / 3600)}h old", mint)
            st.bump(f"{L}.skip.old")
            return "done"

        # ---- gates: structural (drop for good) + growth (re-check)
        gates = hard_gates(
            snap, min_holders=int(lp("min_holders", "trig_min_holders")), min_liquidity_usd=float(lp("min_liquidity_usd", "trig_min_liquidity_usd")),
            min_mcap_usd=float(lp("min_mcap_usd", "trig_min_mcap_usd")), max_mcap_usd=float(lp("max_mcap_usd", "trig_max_mcap_usd")),
            max_top10=p("gate_max_top10"), max_dev_hold=p("gate_max_dev_hold"), max_bundler=p("gate_max_bundler"), max_rat=p("gate_max_rat"),
            max_snipers=p("gate_max_snipers"), max_sniper_hold=p("gate_max_sniper_hold"), max_entrapment=p("gate_max_entrapment"),
            require_renounced=p("gate_require_renounced"), max_creator_created=p("gate_max_creator_created"),
        )
        bad = failed(gates)
        structural = sorted(set(bad) & STRUCTURAL)
        if structural:
            cand = Candidate(mint=mint, ts=now, score=0.0, breakdown=b, gates=gates, token_age_sec=age or 0.0, lane=Lane.DROP, reason=f"{L} fail:" + ",".join(bad))
            cand.id = await self.ctx.db.insert_candidate(cand)
            st.bump(f"{L}.gate_fail")
            st.log(L, "fail", f"{L.upper()} ${sym} · gates: {','.join(bad)}", mint, candidate_id=cand.id)
            return "done"
        blocked = ", ".join(f"{g.name} {g.detail.split(' >= ')[0].split(' <= ')[0]}" for g in gates if not g.passed) if bad else self._trigger_filter(L, b)
        if blocked:
            pend.last_block = blocked
            pend.history.append(blocked)
            st.bump(f"{L}.wait")
            if pend.attempts == 0:
                st.log(L, "info", f"WAIT ${sym} · {blocked} · re-check {int(lp('recheck_sec', 'trig_recheck_sec'))}s", mint)
            return "retry"

        # ---- clean: record the pass, then enter or observe
        b["trigger_waited_sec"] = int(now - pend.first_ts)
        b["trigger_history"] = pend.history[-6:]
        cand = Candidate(mint=mint, ts=now, score=0.0, breakdown=b, gates=gates, token_age_sec=age or 0.0, lane=lane, reason=L)
        cand.id = await self.ctx.db.insert_candidate(cand)
        st.bump(f"{L}.pass")
        await self._enter(cand, lane, score=0.0, size_mult=float(lp("size_mult", "trig_size_mult")))
        return "done"

    def _trigger_filter(self, lane: str, b: dict) -> str | None:
        """Soft entry filters for a trigger lane: a reason to wait, or None. Missing data never blocks."""
        lp = lambda key, fb: self.ctx.lane_p(lane, key, fb)  # noqa: E731
        num = lambda k: (float(b[k]) if isinstance(b.get(k), (int, float)) else None)  # noqa: E731
        if lp("require_pool", "trig_require_pool") and bonding_from_metrics(b):
            prog = num("launchpad_progress")
            return f"bonding{f' {prog * 100:.0f}%' if prog is not None else ''}"
        pc1, pc5 = num("price_change_1m"), num("price_change_5m")
        if pc1 is not None and pc1 > float(lp("max_chase_1m", "trig_max_chase_1m")):
            return f"chase_1m +{pc1 * 100:.0f}%"
        if pc1 is not None and pc1 < float(lp("min_change_1m", "trig_min_change_1m")):
            return f"bleeding_1m {pc1 * 100:.0f}%"
        if pc5 is not None and pc5 > float(lp("max_chase_5m", "trig_max_chase_5m")):
            return f"chase_5m +{pc5 * 100:.0f}%"
        buys, sells = num("buys_1m") or 0.0, num("sells_1m") or 0.0
        swaps = buys + sells
        if swaps < int(lp("min_swaps_1m", "trig_min_swaps_1m")):
            return f"quiet {int(swaps)} swaps/1m"
        if swaps and sells / swaps > float(lp("max_sell_ratio_1m", "trig_max_sell_ratio_1m")):
            return f"distribution {sells / swaps * 100:.0f}% sells"
        smart = (num("smart_wallets") or 0.0) + (num("renowned_wallets") or 0.0)
        if smart < int(lp("min_smart", "trig_min_smart")):
            return f"smart {int(smart)}"
        return None

    # ------------------------------------------------------------ scored launches (fast / grey)

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
        migrated = bool(b.get("migrated")) or bool(b.get("migrated_pool"))
        prog = num("launchpad_progress")
        if not migrated and prog is not None and prog < p("entry_min_progress"):
            return f"stage bond {prog * 100:.0f}%"
        smart = (num("smart_wallets") or 0.0) + (num("renowned_wallets") or 0.0)
        if smart < p("entry_min_smart"):
            return f"smart {int(smart)}"
        return None

    async def _handle(self, cand: Candidate) -> None:
        st, p = self.ctx.state, self.ctx.p
        b = cand.breakdown
        sym = b.get("symbol") or cand.mint[:6]
        lane = cand.lane
        score = cand.score

        if lane == Lane.DROP or self.ctx.lane_mode("fast") == "off":
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

        await self._enter(cand, lane, score=score)

    # ------------------------------------------------------------ entry (all lanes)

    async def _enter(self, cand: Candidate, lane: Lane, *, score: float, size_mult: float = 1.0) -> None:
        st, p = self.ctx.state, self.ctx.p
        b = cand.breakdown
        sym = b.get("symbol") or cand.mint[:6]
        L = lane.value
        mode = self.ctx.lane_mode(L)

        # every passed token is tracked, traded or not: that is what grades the lane
        self.ctx.bus.publish(TOPIC_TRACK, {"mint": cand.mint, "lane": L, "symbol": sym, "candidate_id": cand.id, "mcap_usd": b.get("mcap_usd")})

        if mode == "observe":
            await self.ctx.db.insert_decision(Decision(mint=cand.mint, candidate_id=cand.id, action="observe", lane=lane, size_sol=0.0, reason=f"observe {L}"), {"score": score})
            st.bump(f"{L}.observe")
            st.last_active["risk"] = time.time()
            st.log("risk", "pass", f"OBSERVE ${sym} · {L} · mcap ${(b.get('mcap_usd') or 0) / 1000:.1f}k · holders {b.get('holder_count') or '?'} · 1m {b.get('buys_1m') or 0}b/{b.get('sells_1m') or 0}s", cand.mint, candidate_id=cand.id)
            return

        # ---- risk gate
        verdict = await self.risk.check(cand.mint, lane)
        if verdict.allowed and size_mult != 1.0:
            verdict.size_sol *= size_mult
        decision = Decision(mint=cand.mint, candidate_id=cand.id, action="buy" if verdict.allowed else "skip", lane=lane, size_sol=verdict.size_sol,
                            reason=verdict.reason if not verdict.allowed else (f"score {score:.0f} {L}" if score else L))
        await self.ctx.db.insert_decision(decision, verdict.checks)
        if not verdict.allowed:
            st.bump(f"risk.deny.{verdict.reason}")
            st.last_active["risk"] = time.time()
            if verdict.reason not in ("already_in_position", "reentry_cooldown"):
                st.log("risk", "warn", f"SKIP ${sym} · {L} · {verdict.reason}", cand.mint, candidate_id=cand.id)
            return
        st.last_active["risk"] = time.time()

        # ---- entry
        price_usd = b.get("price_usd")
        if not isinstance(price_usd, (int, float)) or price_usd <= 0:
            st.log("exec", "warn", f"SKIP ${sym} · no price in snapshot", cand.mint, candidate_id=cand.id)
            return
        meta = {"symbol": sym, "score": score, "score_components": b.get("score"), "candidate_id": cand.id, "lane": L,
                "holders": b.get("holder_count"), "mcap_usd": b.get("mcap_usd"), "liquidity_usd": b.get("liquidity_usd"),
                "bundler_rate": b.get("bundler_rate"), "top10_rate": b.get("top10_rate"), "launchpad_progress": b.get("launchpad_progress"),
                "migrated": b.get("migrated") or bool(b.get("migrated_pool")), "llm": b.get("llm"), "creator_status_at_entry": b.get("creator_status"),
                "dexpaid": bool(b.get("dexpaid")), "trigger": b.get("trigger"), "trigger_waited_sec": b.get("trigger_waited_sec"),
                "smart_wallets": b.get("smart_wallets"), "price_change_1m": b.get("price_change_1m"), "price_change_5m": b.get("price_change_5m")}
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
        st.bump(f"{L}.buy")
        st.last_active["exec"] = time.time()
        tag = f"score {score:.0f} · " if score else ""
        st.log("exec", "trade", f"BUY ${sym} · {verdict.size_sol:.3f} SOL @ {pos['entry_price_sol']:.3e} · {tag}{L}", cand.mint,
               candidate_id=cand.id, position_id=pos["id"])
        self.ctx.bus.alert(f"BUY ${sym} · {verdict.size_sol:.3f} SOL · {tag}{L} · mcap ${(b.get('mcap_usd') or 0) / 1000:.1f}k · {self.broker.mode}", "trade")
        self.ctx.bus.publish(TOPIC_POSITION, {"event": "open", "position": pos})
