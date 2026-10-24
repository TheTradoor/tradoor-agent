"""Trigger sources (v0.8). Each detector watches one external signal and publishes

    TOPIC_TRIGGER  {"source": "dexpaid" | "migration" | "trending", "mint", "ts", "symbol"?, ...extra}

the first time a token shows up there. The trader owns everything after that
(GMGN snapshot -> gates -> stabilise/re-check -> entry), so a detector is only
"did this token just appear in my source?".

dexpaid    DexScreener latest token profiles (+ token boosts)   60 req/min each, no auth
migration  PumpPortal subscribeMigration (already on the bus)  re-published after MIG_FIRST_CHECK_SEC
trending   GMGN /v1/market/rank                                 weight 1 per poll; new entrants into the top N

Speed matters for dexpaid (sniper bots act within seconds); the entry filters decide
whether a move is still worth joining. Migration and trending are slower by nature:
the play there is the stabilise-then-run, not the first candle.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque

import aiohttp

from ..bus import TOPIC_TOKEN_MIGRATED, TOPIC_TRIGGER
from ..component import Component, Context
from .gmgn import GmgnClient, GmgnError, rank_address

PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
BOOSTS_URL = "https://api.dexscreener.com/token-boosts/latest/v1"
ORDERS_URL = "https://api.dexscreener.com/orders/v1/solana/{mint}"


class _Seen:
    """mint -> first seen ts, bounded."""

    def __init__(self, ttl_sec: float = 86_400) -> None:
        self.ttl = ttl_sec
        self._d: dict[str, float] = {}

    def add(self, mint: str, now: float) -> bool:
        """True if new."""
        if mint in self._d:
            return False
        self._d[mint] = now
        if len(self._d) > 5000:
            cutoff = now - self.ttl
            self._d = {m: t for m, t in self._d.items() if t >= cutoff}
        return True

    def __contains__(self, mint: str) -> bool:
        return mint in self._d

    def __len__(self) -> int:
        return len(self._d)


# ---------------------------------------------------------------- dexpaid

class DexPaidDetector(Component):
    name = "dexpaid"

    def __init__(self, ctx: Context, *, session: aiohttp.ClientSession | None = None) -> None:
        super().__init__(ctx)
        self._session = session
        self._seen = _Seen()
        self._primed: set[str] = set()
        self.last_error = ""
        self.last_poll = 0.0

    async def run(self) -> None:
        if self.ctx.lane_mode("dexpaid") == "off":
            self.log.info("dexpaid lane off")
            while not await self.sleep(5):
                self.beat()
            return
        own = self._session is None
        if own:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), connector=aiohttp.TCPConnector(limit=4))
        try:
            backoff = 0.0
            feeds = deque(["profiles"] + (["boosts"] if self.ctx.p("dexpaid_boosts") else []))
            while not self.ctx.state.stop.is_set():
                self.beat()
                feed = feeds[0]
                feeds.rotate(-1)
                try:
                    await self._poll(feed)
                    backoff = 0.0
                except Exception as e:
                    self.last_error = f"{type(e).__name__}: {e}"[:200]
                    self.ctx.state.bump("dexpaid.errors")
                    backoff = min(30.0, (backoff or 2.0) * 2)
                    self.log.warning("dexpaid poll (%s) failed: %s (backoff %.0fs)", feed, self.last_error, backoff)
                if await self.sleep(backoff or float(self.ctx.p("dexpaid_poll_sec"))):
                    return
        finally:
            if own and self._session:
                await self._session.close()

    async def _poll(self, feed: str) -> None:
        assert self._session is not None
        url = PROFILES_URL if feed == "profiles" else BOOSTS_URL
        async with self._session.get(url, headers={"Accept": "application/json"}) as resp:
            if resp.status == 429:
                self.ctx.state.bump("dexpaid.429")
                raise RuntimeError("rate limited")
            resp.raise_for_status()
            items = await resp.json(content_type=None)
        self.last_poll = time.time()
        self.ctx.state.bump("dexpaid.polls")
        if not isinstance(items, list):
            return
        now = time.time()
        fresh = []
        for it in items:
            if not isinstance(it, dict) or it.get("chainId") != "solana":
                continue
            mint = str(it.get("tokenAddress") or "")
            if not mint or not self._seen.add(mint, now):
                continue
            fresh.append(it)
        if feed not in self._primed:
            # first poll of a feed: everything in it is history, not an event
            self._primed.add(feed)
            self.log.info("dexpaid %s primed with %d existing entries", feed, len(fresh))
            return
        for it in fresh:
            mint = str(it["tokenAddress"])
            self.ctx.state.bump("dexpaid.events")
            self.ctx.state.last_active["dexpaid"] = now
            links = [str(l.get("url") or "") for l in (it.get("links") or []) if isinstance(l, dict)]
            kind = "boost" if feed == "boosts" else "profile"
            amount = it.get("totalAmount") or it.get("amount")
            self.ctx.state.log("dexpaid", "watch", f"DEXPAID {mint[:6]}… · {kind}{f' {amount}' if amount else ''} · {len(links)} links", mint)
            self.ctx.bus.publish(TOPIC_TRIGGER, {"source": "dexpaid", "kind": kind, "mint": mint, "ts": now, "links": links[:6],
                                                 "description": str(it.get("description") or "")[:200], "icon": str(it.get("icon") or ""), "amount": amount})

    async def confirm(self, mint: str) -> dict | None:
        """Optional: ask /orders for the payment record. Returns the approved tokenProfile order or None."""
        if self._session is None:
            return None
        try:
            async with self._session.get(ORDERS_URL.format(mint=mint)) as resp:
                if resp.status != 200:
                    return None
                orders = await resp.json(content_type=None)
        except Exception:
            return None
        if not isinstance(orders, list):
            return None
        for o in orders:
            if isinstance(o, dict) and o.get("type") == "tokenProfile" and o.get("status") == "approved":
                return o
        return None


# ---------------------------------------------------------------- migration

class MigrationTrigger(Component):
    """token.migrated -> (wait MIG_FIRST_CHECK_SEC for the AMM pool + GMGN re-index) -> TOPIC_TRIGGER source=migration."""

    name = "migration"

    def __init__(self, ctx: Context) -> None:
        super().__init__(ctx)
        self._pending: dict[str, dict] = {}   # mint -> event
        self._seen = _Seen()

    async def run(self) -> None:
        q = self.ctx.bus.subscribe(TOPIC_TOKEN_MIGRATED, maxsize=2000)
        while not self.ctx.state.stop.is_set():
            self.beat()
            while True:
                try:
                    m = q.get_nowait()
                except asyncio.QueueEmpty:
                    break
                mint = str(m.get("mint") or "")
                if not mint or not self._seen.add(mint, time.time()):
                    continue
                self.ctx.state.bump("migration.events")
                self._pending[mint] = {"source": "migration", "mint": mint, "migrated_ts": float(m.get("ts") or time.time()),
                                       "pool": m.get("pool"), "due": time.time() + float(self.ctx.p("mig_first_check_sec"))}
            now = time.time()
            if self.ctx.lane_mode("migration") == "off":
                self._pending.clear()
            for mint in [m for m, e in self._pending.items() if e["due"] <= now]:
                e = self._pending.pop(mint)
                e.pop("due", None)
                e["ts"] = now
                self.ctx.state.last_active["migration"] = now
                self.ctx.bus.publish(TOPIC_TRIGGER, e)
            if await self.sleep(1.0):
                return


# ---------------------------------------------------------------- trending

class TrendingDetector(Component):
    """GMGN /v1/market/rank: a token entering the top N (by swaps, 1m window) for the first time is the trigger."""

    name = "trending"

    def __init__(self, ctx: Context, gmgn: GmgnClient) -> None:
        super().__init__(ctx)
        self.gmgn = gmgn
        self._seen = _Seen(ttl_sec=6 * 3600)   # a token can trend again after a few hours; that is a new event
        self._primed = False
        self.last_poll = 0.0
        self.last_error = ""
        self.last_n = 0

    async def run(self) -> None:
        if self.ctx.lane_mode("trending") == "off":
            self.log.info("trending lane off")
            while not await self.sleep(5):
                self.beat()
            return
        while not self.ctx.state.stop.is_set():
            self.beat()
            if time.time() >= self.gmgn.stats.cooldown_until:
                try:
                    await self._poll()
                except GmgnError as e:
                    self.last_error = str(e)[:200]
                    self.ctx.state.bump("trending.errors")
                except Exception as e:
                    self.last_error = f"{type(e).__name__}: {e}"[:200]
                    self.ctx.state.bump("trending.errors")
                    self.log.exception("trending poll failed")
            if await self.sleep(float(self.ctx.p("trend_poll_sec"))):
                return

    async def _poll(self) -> None:
        items = await self.gmgn.market_rank(interval=str(self.ctx.p("trend_interval")), order_by=str(self.ctx.p("trend_order_by")), limit=int(self.ctx.p("trend_limit")))
        now = time.time()
        self.last_poll = now
        self.last_n = len(items)
        self.ctx.state.bump("trending.polls")
        if not items:
            return
        fresh = []
        for rank, it in enumerate(items, start=1):
            mint = rank_address(it)
            if not mint or not self._seen.add(mint, now):
                continue
            fresh.append((rank, it))
        if not self._primed:
            self._primed = True
            self.log.info("trending primed with %d tokens in the top %d", len(fresh), len(items))
            return
        for rank, it in fresh:
            mint = rank_address(it)
            sym = str(it.get("symbol") or "")
            self.ctx.state.bump("trending.events")
            self.ctx.state.last_active["trending"] = now
            self.ctx.state.log("trending", "watch", f"TRENDING ${sym or mint[:6]} · #{rank} by {self.ctx.p('trend_order_by')}/{self.ctx.p('trend_interval')}", mint)
            self.ctx.bus.publish(TOPIC_TRIGGER, {"source": "trending", "mint": mint, "ts": now, "symbol": sym, "rank": rank,
                                                 "swaps": it.get("swaps") or it.get("swaps_1m"), "volume": it.get("volume") or it.get("volume_1m")})
