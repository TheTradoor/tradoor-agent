"""DexPaid detector: polls DexScreener's latest-token-profiles feed and emits an event the
first time a Solana token shows up there (= someone just paid for its profile).

Feed    GET https://api.dexscreener.com/token-profiles/latest/v1    (60 req/min, no auth)
Confirm GET https://api.dexscreener.com/orders/v1/solana/{mint}       -> [{type, status, paymentTimestamp}]

Speed matters here: dexpaid snipers act within seconds. Polling at 2s puts us a few
seconds behind the fastest bots; the entry filters in the trader decide whether the
move is still worth joining.

What is emitted (TOPIC_DEXPAID): {"mint", "ts", "links", "description", "icon"}.
The trader then fetches a GMGN snapshot, runs the structural gates and buys with the
dexpaid exit profile.
"""
from __future__ import annotations

import asyncio
import time

import aiohttp

from ..bus import TOPIC_DEXPAID
from ..component import Component, Context

PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
ORDERS_URL = "https://api.dexscreener.com/orders/v1/solana/{mint}"


class DexPaidDetector(Component):
    name = "dexpaid"

    def __init__(self, ctx: Context, *, session: aiohttp.ClientSession | None = None) -> None:
        super().__init__(ctx)
        self._session = session
        self._seen: dict[str, float] = {}     # mint -> first seen ts
        self._primed = False
        self.last_error = ""
        self.last_poll = 0.0

    async def run(self) -> None:
        if not self.ctx.settings.dexpaid_enabled:
            self.log.info("dexpaid lane disabled")
            while not await self.sleep(5):
                self.beat()
            return
        own = self._session is None
        if own:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), connector=aiohttp.TCPConnector(limit=4))
        try:
            backoff = 0.0
            while not self.ctx.state.stop.is_set():
                self.beat()
                try:
                    await self._poll()
                    backoff = 0.0
                except Exception as e:
                    self.last_error = f"{type(e).__name__}: {e}"[:200]
                    self.ctx.state.bump("dexpaid.errors")
                    backoff = min(30.0, (backoff or 2.0) * 2)
                    self.log.warning("dexpaid poll failed: %s (backoff %.0fs)", self.last_error, backoff)
                if await self.sleep(backoff or float(self.ctx.p("dexpaid_poll_sec"))):
                    return
                # forget old entries
                if len(self._seen) > 5000:
                    cutoff = time.time() - 86_400
                    self._seen = {m: t for m, t in self._seen.items() if t >= cutoff}
        finally:
            if own and self._session:
                await self._session.close()

    async def _poll(self) -> None:
        assert self._session is not None
        async with self._session.get(PROFILES_URL, headers={"Accept": "application/json"}) as resp:
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
            if not mint or mint in self._seen:
                continue
            self._seen[mint] = now
            fresh.append(it)
        if not self._primed:
            # first poll: everything in the feed is history, not an event
            self._primed = True
            self.log.info("dexpaid primed with %d existing profiles", len(fresh))
            return
        for it in fresh:
            mint = str(it["tokenAddress"])
            self.ctx.state.bump("dexpaid.events")
            self.ctx.state.last_active["dexpaid"] = now
            links = [str(l.get("url") or "") for l in (it.get("links") or []) if isinstance(l, dict)]
            self.ctx.state.log("dexpaid", "watch", f"DEXPAID {mint[:6]}… · profile paid · {len(links)} links", mint)
            self.ctx.bus.publish(TOPIC_DEXPAID, {"mint": mint, "ts": now, "links": links[:6], "description": str(it.get("description") or "")[:200], "icon": str(it.get("icon") or "")})

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
