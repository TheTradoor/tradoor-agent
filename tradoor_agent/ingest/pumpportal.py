"""PumpPortal real-time WebSocket ingest.

Docs (pumpportal.fun/data-api/real-time):
  url                 wss://pumpportal.fun/api/data?api-key=<key>
  free subs           {"method":"subscribeNewToken"}  {"method":"subscribeMigration"}
  paid subs           {"method":"subscribeTokenTrade","keys":[mint,...]}   (0.01 SOL / 10k events)
  rules               ONE connection at a time; flooding -> 1h ban.

Message shapes are not documented on that page; the parser keys on `txType`
("create" | "buy" | "sell" | "migrate") and tolerates missing fields. Use
`python -m tradoor_agent --probe-ws 20` on the VPS to see raw samples.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from collections.abc import AsyncIterator
from typing import Any

import websockets

from ..bus import TOPIC_TOKEN_MIGRATED, TOPIC_TOKEN_NEW, TOPIC_TRADE
from ..component import Component, Context
from ..models import Side, TokenEvent, TradeEvent

PUMP_TOTAL_SUPPLY = 1_000_000_000.0  # pump.fun fixed supply (1B, 6 decimals)


def ws_url(base: str, api_key: str) -> str:
    if not api_key:
        return base
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}api-key={api_key}"


def parse_message(raw: str | bytes) -> tuple[str, Any]:
    """Classify one WS frame.

    Returns (kind, payload) where kind is one of:
      "create" -> TokenEvent, "trade" -> TradeEvent, "migrate" -> dict,
      "ack" -> dict (server notices), "unknown" -> dict/str
    """
    try:
        msg = json.loads(raw)
    except (TypeError, ValueError):
        return "unknown", raw
    if not isinstance(msg, dict):
        return "unknown", msg

    tx = str(msg.get("txType", "")).lower()
    mint = str(msg.get("mint", "") or "")

    if tx == "create" and mint:
        return "create", TokenEvent(
            mint=mint,
            symbol=str(msg.get("symbol", "") or ""),
            name=str(msg.get("name", "") or ""),
            creator=str(msg.get("traderPublicKey", "") or ""),
            ts=time.time(),
            source="pumpportal",
            uri=str(msg.get("uri", "") or ""),
            raw=msg,
        )

    if tx in ("buy", "sell") and mint:
        sol = _f(msg.get("solAmount"))
        tokens = _f(msg.get("tokenAmount"))
        price = (sol / tokens) if (sol and tokens) else 0.0
        return "trade", TradeEvent(
            mint=mint,
            ts=time.time(),
            side=Side.BUY if tx == "buy" else Side.SELL,
            sol=sol,
            tokens=tokens,
            trader=str(msg.get("traderPublicKey", "") or ""),
            price_sol=price,
            mcap_sol=_f(msg.get("marketCapSol")) or None,
            raw=msg,
        )

    if (tx == "migrate" or "migrat" in tx) and mint:
        return "migrate", msg

    if "message" in msg or "errors" in msg or "error" in msg:
        return "ack", msg

    return "unknown", msg


def _f(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def dev_initial_buy(evt: TokenEvent) -> tuple[float, float]:
    """(dev initial buy in SOL, share of supply 0-1) from a create event."""
    sol = _f(evt.raw.get("solAmount"))
    tokens = _f(evt.raw.get("initialBuy"))
    share = tokens / PUMP_TOTAL_SUPPLY if tokens else 0.0
    return sol, min(share, 1.0)


class PumpPortalIngest(Component):
    name = "pumpportal"

    def __init__(self, ctx: Context, *, message_source: AsyncIterator[str] | None = None) -> None:
        super().__init__(ctx)
        self._source = message_source          # tests inject frames here
        self._outgoing: asyncio.Queue[dict] = asyncio.Queue()
        self.trade_subs: set[str] = set()
        self.connected = False
        self.last_message_at = 0.0
        self.last_error = ""
        self.recent: deque[dict] = deque(maxlen=60)   # last raw frames for the dashboard

    # ------------------------------------------------------------ public api

    async def subscribe_trades(self, mints: list[str]) -> None:
        new = [m for m in mints if m not in self.trade_subs]
        if not new:
            return
        self.trade_subs.update(new)
        await self._outgoing.put({"method": "subscribeTokenTrade", "keys": new})

    async def unsubscribe_trades(self, mints: list[str]) -> None:
        old = [m for m in mints if m in self.trade_subs]
        if not old:
            return
        self.trade_subs.difference_update(old)
        await self._outgoing.put({"method": "unsubscribeTokenTrade", "keys": old})

    # ------------------------------------------------------------ run loop

    async def run(self) -> None:
        if self._source is not None:
            await self._consume(self._stop_aware(self._source))
            return

        s = self.ctx.settings
        url = ws_url(s.pumpportal_ws_url, s.pumpportal_api_key)
        backoff = 5
        while not self.ctx.state.stop.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=8192, open_timeout=15) as ws:
                    self.connected = True
                    self.ctx.state.bump("ws.connects")
                    self.log.info("connected to PumpPortal")
                    self.ctx.state.log("ingest", "info", "PumpPortal connected · subscribed new tokens + migrations")
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    await ws.send(json.dumps({"method": "subscribeMigration"}))
                    if self.trade_subs:
                        await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": sorted(self.trade_subs)}))
                    sender = asyncio.create_task(self._sender(ws), name="pumpportal_sender")
                    try:
                        await self._consume(self._stop_aware(ws))
                    finally:
                        sender.cancel()
                        self.connected = False
                    backoff = 5
                if self.ctx.state.stop.is_set():
                    return
                self.log.warning("PumpPortal connection closed; reconnecting")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.connected = False
                self.ctx.state.bump("ws.errors")
                self.last_error = f"{type(e).__name__}: {e}"[:300]
                self.log.warning("PumpPortal error: %s; retry in %ss", self.last_error, backoff)
                self.ctx.state.log("ingest", "error", f"PumpPortal {self.last_error[:80]} · retry {backoff}s")
            if await self.sleep(backoff):
                return
            backoff = min(backoff * 2, 60)   # polite: PumpPortal bans connection floods

    async def _sender(self, ws) -> None:
        while True:
            cmd = await self._outgoing.get()
            try:
                await ws.send(json.dumps(cmd))
            except Exception as e:
                self.log.warning("send failed (%s); re-queue %s", e, cmd.get("method"))
                await self._outgoing.put(cmd)
                return

    async def _stop_aware(self, source: AsyncIterator[str]) -> AsyncIterator[str]:
        """Yield frames from any async iterator (a websocket or a test source),
        returning as soon as the stop event fires even if the source is blocked."""
        stop_task = asyncio.create_task(self.ctx.state.stop.wait())
        it = source.__aiter__()
        try:
            while True:
                next_task = asyncio.create_task(it.__anext__())
                done, _ = await asyncio.wait({next_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
                if stop_task in done:
                    next_task.cancel()
                    return
                try:
                    yield next_task.result()
                except StopAsyncIteration:
                    return
        finally:
            stop_task.cancel()

    async def _consume(self, frames: AsyncIterator[str]) -> None:
        unknown_logged = 0
        async for raw in frames:
            self.beat()
            self.last_message_at = time.time()
            kind, obj = parse_message(raw)
            text = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
            self.recent.append({"ts": self.last_message_at, "kind": kind, "raw": text[:600]})
            try:
                if kind == "create":
                    await self._on_create(obj)
                elif kind == "trade":
                    self.ctx.state.bump("ws.trades")
                    self.ctx.bus.publish(TOPIC_TRADE, obj)
                elif kind == "migrate":
                    await self._on_migrate(obj)
                elif kind == "ack":
                    self.log.info("PumpPortal: %s", obj.get("message") or obj.get("errors") or obj.get("error"))
                else:
                    self.ctx.state.bump("ws.unknown")
                    if unknown_logged < 5:
                        unknown_logged += 1
                        self.log.info("unknown frame: %s", str(obj)[:300])
            except Exception:
                self.log.exception("failed handling %s frame", kind)
            if self.ctx.state.stop.is_set():
                return

    async def _on_create(self, evt: TokenEvent) -> None:
        self.ctx.state.bump("ws.tokens_new")
        self.ctx.state.last_active["ingest"] = time.time()
        await self.ctx.db.upsert_token(evt)
        self.ctx.bus.publish(TOPIC_TOKEN_NEW, evt)

    async def _on_migrate(self, msg: dict) -> None:
        self.ctx.state.bump("ws.migrations")
        mint = str(msg.get("mint"))
        self.ctx.state.log("ingest", "info", f"MIGRATED {mint[:6]}… → {msg.get('pool') or 'amm'}", mint)
        await self.ctx.db.mark_migrated(mint, pool=str(msg.get("pool", "") or ""))
        self.ctx.bus.publish(TOPIC_TOKEN_MIGRATED, {"mint": mint, "pool": msg.get("pool"), "signature": msg.get("signature"), "ts": time.time()})
