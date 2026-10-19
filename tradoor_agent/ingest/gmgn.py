"""GMGN OpenAPI client (read endpoints, "exist" auth).

Verified against GMGNAI/gmgn-skills:
  host        https://openapi.gmgn.ai
  headers     X-APIKEY: <key>
  query       timestamp=<unix sec, +-5s>  client_id=<uuid, no replay within 7s>
  envelope    {"code": 0, "data": ..., "error": ..., "message": ...}
  rate limit  leaky bucket by weight; 429 / RATE_LIMIT_EXCEEDED / RATE_LIMIT_BANNED
              with x-ratelimit-reset (unix sec). Sending during cooldown extends the ban.

Weights:  token/info 1 · token/security 1 · token/pool_info 1 ·
          market/token_top_holders 5 · market/token_top_traders 5 · market/rank 1
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from ..models import TokenSnapshot

log = logging.getLogger("agent.gmgn")

WEIGHTS = {
    "/v1/token/info": 1,
    "/v1/token/security": 1,
    "/v1/token/pool_info": 1,
    "/v1/market/token_top_holders": 5,
    "/v1/market/token_top_traders": 5,
    "/v1/market/rank": 1,
    "/v1/market/token_kline": 2,
    "/v1/trenches": 3,
    "/v1/user/info": 1,
}


class GmgnError(Exception):
    def __init__(self, path: str, status: int, api_code: Any = None, api_error: str = "", message: str = "", reset_at: float | None = None):
        super().__init__(f"{path}: HTTP {status} code={api_code} error={api_error} {message}".strip())
        self.path, self.status, self.api_code, self.api_error, self.api_message, self.reset_at = path, status, api_code, api_error, message, reset_at

    @property
    def rate_limited(self) -> bool:
        return self.status == 429 or self.api_error in ("RATE_LIMIT_EXCEEDED", "RATE_LIMIT_BANNED")


class WeightBucket:
    """Client-side leaky bucket so we never hit the server limit."""

    def __init__(self, rate_per_sec: float, capacity: float | None = None) -> None:
        self.rate = max(rate_per_sec, 0.1)
        self.capacity = capacity or self.rate
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, weight: float) -> float:
        """Wait until `weight` is available. Returns seconds waited."""
        waited = 0.0
        async with self._lock:
            while True:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= weight:
                    self.tokens -= weight
                    return waited
                need = (weight - self.tokens) / self.rate
                waited += need
                await asyncio.sleep(need)


@dataclass
class _CacheEntry:
    ts: float
    value: Any


@dataclass
class GmgnStats:
    requests: int = 0
    cache_hits: int = 0
    errors: int = 0
    rate_limited: int = 0
    weight_used: float = 0.0
    waited_sec: float = 0.0
    last_error: str = ""
    cooldown_until: float = 0.0
    by_path: dict[str, int] = field(default_factory=dict)
    security_errors: int = 0
    last_security_error: str = ""
    sample_keys: dict[str, list] = field(default_factory=dict)   # field names GMGN actually returns


class GmgnClient:
    def __init__(self, api_key: str, host: str = "https://openapi.gmgn.ai", *, rate_weight: float = 50, budget_fraction: float = 0.8, cache_sec: float = 15, timeout_sec: float = 6.0, chain: str = "sol") -> None:
        self.api_key = api_key
        self.host = host.rstrip("/")
        self.chain = chain
        self.cache_sec = cache_sec
        self.timeout = aiohttp.ClientTimeout(total=timeout_sec)
        self.bucket = WeightBucket(rate_weight * budget_fraction)
        self.stats = GmgnStats()
        self._cache: dict[tuple, _CacheEntry] = {}
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------ lifecycle

    async def open(self) -> None:
        if self._session is None or self._session.closed:
            # IPv4 only: GMGN rejects IPv6 (documented), aiohttp picks v4 with family=2
            connector = aiohttp.TCPConnector(family=2, limit=20)
            self._session = aiohttp.ClientSession(timeout=self.timeout, connector=connector)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    # ------------------------------------------------------------ core request

    async def request(self, method: str, path: str, query: dict | None = None, body: Any = None, *, cache_key: tuple | None = None, cache_sec: float | None = None) -> Any:
        ttl = self.cache_sec if cache_sec is None else cache_sec
        if cache_key is not None and ttl > 0:
            hit = self._cache.get(cache_key)
            if hit and time.time() - hit.ts < ttl:
                self.stats.cache_hits += 1
                return hit.value

        now = time.time()
        if now < self.stats.cooldown_until:
            raise GmgnError(path, 429, api_error="CLIENT_COOLDOWN", message=f"cooling down {self.stats.cooldown_until - now:.0f}s", reset_at=self.stats.cooldown_until)

        await self.open()
        weight = WEIGHTS.get(path, 3)
        self.stats.waited_sec += await self.bucket.acquire(weight)
        self.stats.weight_used += weight
        self.stats.requests += 1
        self.stats.by_path[path] = self.stats.by_path.get(path, 0) + 1

        q = dict(query or {})
        q["timestamp"] = int(time.time())
        q["client_id"] = str(uuid.uuid4())
        headers = {"X-APIKEY": self.api_key, "Content-Type": "application/json", "User-Agent": "tradoor-agent/0.1"}

        try:
            assert self._session is not None
            async with self._session.request(method, self.host + path, params=q, json=body if body is not None else None, headers=headers) as resp:
                reset_raw = resp.headers.get("x-ratelimit-reset")
                reset_at = float(reset_raw) if reset_raw and reset_raw.replace(".", "", 1).isdigit() else None
                try:
                    payload = await resp.json(content_type=None)
                except Exception:
                    text = await resp.text()
                    raise GmgnError(path, resp.status, message=f"non-JSON: {text[:120]}", reset_at=reset_at)
                code = payload.get("code") if isinstance(payload, dict) else None
                if resp.status != 200 or code not in (0, None):
                    err = GmgnError(path, resp.status, api_code=code, api_error=str(payload.get("error", "")) if isinstance(payload, dict) else "", message=str(payload.get("message", "")) if isinstance(payload, dict) else "", reset_at=reset_at)
                    self._note_error(err)
                    raise err
                data = payload.get("data", payload) if isinstance(payload, dict) else payload
        except aiohttp.ClientError as e:
            err = GmgnError(path, 0, message=f"network: {e}")
            self._note_error(err)
            raise err
        except asyncio.TimeoutError:
            err = GmgnError(path, 0, message="timeout")
            self._note_error(err)
            raise err

        if cache_key is not None and ttl > 0:
            self._cache[cache_key] = _CacheEntry(time.time(), data)
            if len(self._cache) > 5000:
                cutoff = time.time() - ttl
                self._cache = {k: v for k, v in self._cache.items() if v.ts >= cutoff}
        return data

    def _note_error(self, err: GmgnError) -> None:
        self.stats.errors += 1
        self.stats.last_error = str(err)[:200]
        if err.rate_limited:
            self.stats.rate_limited += 1
            # back off until reset (or 10s), and never send during cooldown: that extends the ban
            self.stats.cooldown_until = max(self.stats.cooldown_until, err.reset_at or (time.time() + 10))
            log.warning("gmgn rate limited on %s; cooldown until +%.0fs", err.path, self.stats.cooldown_until - time.time())

    # ------------------------------------------------------------ endpoints

    async def user_info(self) -> Any:
        return await self.request("GET", "/v1/user/info", cache_key=None)

    async def token_info(self, mint: str) -> Any:
        return await self.request("GET", "/v1/token/info", {"chain": self.chain, "address": mint}, cache_key=("info", mint))

    async def token_security(self, mint: str) -> Any:
        return await self.request("GET", "/v1/token/security", {"chain": self.chain, "address": mint}, cache_key=("sec", mint), cache_sec=max(self.cache_sec, 30))

    async def top_holders(self, mint: str, limit: int = 20, tag: str | None = None) -> Any:
        q: dict[str, Any] = {"chain": self.chain, "address": mint, "limit": limit}
        if tag:
            q["tag"] = tag
        return await self.request("GET", "/v1/market/token_top_holders", q, cache_key=("holders", mint, limit, tag), cache_sec=max(self.cache_sec, 30))

    async def snapshot(self, mint: str) -> TokenSnapshot:
        """info + security in parallel -> TokenSnapshot (weight 2)."""
        info, sec = await asyncio.gather(self.token_info(mint), self.token_security(mint), return_exceptions=True)
        if isinstance(info, Exception):
            raise info
        if isinstance(sec, Exception):
            self.stats.security_errors += 1
            self.stats.last_security_error = str(sec)[:200]
            if self.stats.security_errors in (1, 10, 100) or self.stats.security_errors % 1000 == 0:
                log.warning("token/security failed (#%d) for %s: %s", self.stats.security_errors, mint, sec)
            sec = {}
        self._remember_keys(info or {}, sec or {})
        return build_snapshot(mint, info or {}, sec or {})

    def _remember_keys(self, info: Any, sec: Any) -> None:
        """Keep the key structure of the latest payloads so the dashboard can show what GMGN really returns."""
        def keys(x: Any) -> list:
            if isinstance(x, dict):
                return sorted(x.keys())[:60]
            if isinstance(x, list):
                return [f"<list len={len(x)}>"] + (keys(x[0]) if x and isinstance(x[0], dict) else [])
            return [f"<{type(x).__name__}>"]
        sk: dict[str, list] = {"info": keys(info), "security": keys(sec)}
        if isinstance(info, dict):
            for sub in ("price", "dev", "stat", "wallet_tags_stat", "link"):
                if sub in info:
                    sk[f"info.{sub}"] = keys(info[sub])
        if isinstance(sec, dict):
            for sub in sec:
                if isinstance(sec[sub], (dict, list)):
                    sk[f"security.{sub}"] = keys(sec[sub])
            sk["security VALUES (last token)"] = [f"{k}={sec[k]!r}" for k in sorted(sec) if not isinstance(sec[k], (dict, list))][:40]
        if isinstance(info, dict) and isinstance(info.get("stat"), dict):
            st = info["stat"]
            sk["info.stat VALUES (last token)"] = [f"{k}={st[k]!r}" for k in sorted(st) if not isinstance(st[k], (dict, list))][:40]
        self.stats.sample_keys = sk


# ---------------------------------------------------------------- normalization

def _g(d: Any, *path: str, default: Any = None) -> Any:
    """Nested get that tolerates missing keys and non-dict intermediates."""
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return default if cur is None else cur


def _f(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v: Any) -> int | None:
    f = _f(v)
    return None if f is None else int(f)


def _b(v: Any) -> bool | None:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    return str(v).strip().lower() in ("1", "true", "yes")


def _first(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def _chg(now: float | None, before: float | None) -> float | None:
    if now is None or not before:
        return None
    return now / before - 1.0


def _flag(v: Any) -> bool | None:
    """Strict boolean: only True/1/"1"/"true" count as set. -1, counts, None -> unknown."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return True if v == 1 else (False if v == 0 else None)
    if isinstance(v, str):
        t = v.strip().lower()
        return True if t in ("1", "true", "yes") else (False if t in ("0", "false", "no") else None)
    return None


def _honeypot(sec: dict) -> bool | None:
    """True only when GMGN explicitly flags the token as a honeypot.

    can_sell / can_not_sell are NOT used: on fresh tokens they look like counts
    (0 sells so far), and reading them as booleans flagged every launch.
    """
    for k in ("is_honeypot", "honeypot"):
        f = _flag(sec.get(k))
        if f is not None:
            return f
    return None


def build_snapshot(mint: str, info: dict, sec: dict) -> TokenSnapshot:
    """Map GMGN info + security payloads to TokenSnapshot.

    GMGN nests some fields (price.*, dev.*, stat.*, wallet_tags_stat.*) but has
    also served them flat; both are accepted. Unknown -> None, never a guess.
    """
    price = _g(info, "price", default={})
    if not isinstance(price, dict):
        price = {"price": price}
    dev = _g(info, "dev", default={}) or {}
    stat = _g(info, "stat", default={}) or {}
    tags = _g(info, "wallet_tags_stat", default={}) or {}
    link = _g(info, "link", default={}) or {}

    price_usd = _f(_first(price.get("price"), info.get("price") if not isinstance(info.get("price"), dict) else None))
    total_supply = _f(_first(info.get("total_supply"), info.get("max_supply")))
    mcap = _f(_first(info.get("market_cap"), info.get("usd_market_cap")))
    if mcap is None and price_usd is not None and total_supply:
        mcap = price_usd * total_supply

    def rate(*keys: str) -> float | None:
        for src in (sec, stat, dev, info):
            for k in keys:
                v = _f(src.get(k)) if isinstance(src, dict) else None
                if v is not None:
                    return v / 100.0 if v > 1.0 else v   # some payloads use percent
        return None

    def count(*keys: str) -> int | None:
        for src in (tags, stat, sec, info):
            for k in keys:
                v = _i(src.get(k)) if isinstance(src, dict) else None
                if v is not None:
                    return v
        return None

    return TokenSnapshot(
        mint=mint,
        ts=time.time(),
        symbol=str(_first(info.get("symbol"), "") or ""),
        name=str(_first(info.get("name"), "") or ""),
        creator=str(_first(dev.get("creator_address"), info.get("creator"), "") or ""),
        price_usd=price_usd,
        mcap_usd=mcap,
        liquidity_usd=_f(info.get("liquidity")),
        holder_count=_i(_first(info.get("holder_count"), stat.get("holder_count"))),
        total_supply=total_supply,
        launchpad=str(_first(info.get("launchpad_platform"), info.get("launchpad"), "") or ""),
        launchpad_status=_i(info.get("launchpad_status")),
        launchpad_progress=_f(info.get("launchpad_progress")),
        creation_ts=_f(_first(info.get("creation_timestamp"), info.get("open_timestamp"))),
        buys_1m=_i(price.get("buys_1m")), sells_1m=_i(price.get("sells_1m")),
        volume_1m=_f(price.get("volume_1m")), swaps_1m=_i(price.get("swaps_1m")),
        buys_5m=_i(price.get("buys_5m")), sells_5m=_i(price.get("sells_5m")),
        volume_5m=_f(price.get("volume_5m")), swaps_5m=_i(price.get("swaps_5m")),
        # price_1m / price_5m are the prices N minutes ago, so derive the change
        price_change_1m=_chg(price_usd, _f(price.get("price_1m"))),
        price_change_5m=_chg(price_usd, _f(price.get("price_5m"))),
        migrated_pool=str(info.get("migrated_pool") or ""),
        top10_rate=rate("top_10_holder_rate"),
        dev_hold_rate=rate("dev_team_hold_rate"),
        creator_hold_rate=rate("creator_hold_rate", "creator_balance_rate"),
        creator_status=str(_first(sec.get("creator_token_status"), dev.get("creator_token_status"), "") or ""),
        bundler_rate=rate("bundler_trader_amount_rate", "top_bundler_trader_percentage", "bundler_rate"),
        rat_rate=rate("rat_trader_amount_rate", "top_rat_trader_percentage"),
        insider_rate=rate("suspected_insider_hold_rate"),
        sniper_count=count("sniper_count", "sniper_wallets"),
        sniper_hold_rate=rate("top70_sniper_hold_rate"),
        entrapment_rate=rate("top_entrapment_trader_percentage"),
        private_vault_rate=rate("private_vault_hold_rate"),
        creator_created_count=count("creator_created_count"),
        rug_ratio=rate("rug_ratio"),
        wash_trading=_b(_first(sec.get("is_wash_trading"), info.get("is_wash_trading"))),
        honeypot=_honeypot(sec),
        renounced_mint=_b(_first(sec.get("renounced_mint"), info.get("renounced_mint"))),
        renounced_freeze=_b(_first(sec.get("renounced_freeze_account"), info.get("renounced_freeze_account"))),
        burn_status=str(_first(sec.get("burn_status"), info.get("burn_status"), "") or ""),
        burn_ratio=rate("burn_ratio"),
        fresh_wallet_rate=rate("fresh_wallet_rate"),
        bot_degen_count=count("bot_degen_count"),
        smart_wallets=count("smart_wallets", "smart_degen_count"),
        renowned_wallets=count("renowned_wallets", "renowned_count"),
        sniper_wallets=count("sniper_wallets"),
        bundler_wallets=count("bundler_wallets"),
        whale_wallets=count("whale_wallets"),
        fresh_wallets=count("fresh_wallets"),
        twitter=str(_first(link.get("twitter_username"), dev.get("twitter_username"), info.get("twitter_username"), "") or ""),
        website=str(_first(link.get("website"), info.get("website"), "") or ""),
        telegram=str(_first(link.get("telegram"), info.get("telegram"), "") or ""),
    )
