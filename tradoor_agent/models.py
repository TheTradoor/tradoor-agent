"""Shared data models. Every stage of the pipeline speaks these types.

Flow:  TokenEvent/TradeEvent -> (gates + scoring) -> Candidate -> (router) ->
       [LLMReview for grey lane] -> Decision -> Order -> Fill -> Position -> Trade
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


def now() -> float:
    return time.time()


class Lane(str, Enum):
    SCREENED = "screened"   # passed hard gates, waiting for scoring (phase 1 output)
    FAST = "fast"           # score >= SCORE_HIGH, deterministic only
    GREY = "grey"           # SCORE_LOW <= score < SCORE_HIGH and token old enough -> LLM review
    DROP = "drop"


class Verdict(str, Enum):
    BOOST = "boost"
    NEUTRAL = "neutral"
    VETO = "veto"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING = "pending"
    AWAITING_APPROVAL = "awaiting_approval"   # confirm mode
    SIMULATED = "simulated"
    SENT = "sent"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PositionStatus(str, Enum):
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(slots=True)
class TokenEvent:
    """A new token appeared (launch / migration)."""
    mint: str
    symbol: str
    name: str
    creator: str
    ts: float
    source: str                     # e.g. "pumpportal"
    uri: str = ""
    raw: dict = field(default_factory=dict)


@dataclass(slots=True)
class TradeEvent:
    """One swap on a token."""
    mint: str
    ts: float
    side: Side
    sol: float
    tokens: float
    trader: str
    price_sol: float                # SOL per token
    mcap_sol: float | None = None
    raw: dict = field(default_factory=dict)


@dataclass(slots=True)
class TokenSnapshot:
    """Normalized view of a token at one moment, built from GMGN info + security.

    Rates are fractions 0-1. None means the source did not report it.
    """
    mint: str
    ts: float
    symbol: str = ""
    name: str = ""
    creator: str = ""
    price_usd: float | None = None
    mcap_usd: float | None = None
    liquidity_usd: float | None = None
    holder_count: int | None = None
    total_supply: float | None = None
    launchpad: str = ""
    launchpad_status: int | None = None       # 0 not opened, 1 live (bonding), 2 migrated
    launchpad_progress: float | None = None   # 0-1
    creation_ts: float | None = None
    # activity windows
    buys_1m: int | None = None
    sells_1m: int | None = None
    volume_1m: float | None = None
    swaps_1m: int | None = None
    buys_5m: int | None = None
    sells_5m: int | None = None
    volume_5m: float | None = None
    swaps_5m: int | None = None
    price_change_1m: float | None = None
    price_change_5m: float | None = None
    # concentration / risk
    top10_rate: float | None = None
    dev_hold_rate: float | None = None
    creator_hold_rate: float | None = None
    creator_status: str = ""                  # creator_hold | creator_close | ...
    bundler_rate: float | None = None
    rat_rate: float | None = None
    insider_rate: float | None = None
    sniper_count: int | None = None
    rug_ratio: float | None = None
    wash_trading: bool | None = None
    renounced_mint: bool | None = None
    renounced_freeze: bool | None = None
    burn_status: str = ""
    fresh_wallet_rate: float | None = None
    bot_degen_count: int | None = None
    # smart money
    smart_wallets: int | None = None
    renowned_wallets: int | None = None
    sniper_wallets: int | None = None
    bundler_wallets: int | None = None
    whale_wallets: int | None = None
    fresh_wallets: int | None = None
    # socials
    twitter: str = ""
    website: str = ""
    telegram: str = ""

    def metrics(self) -> dict[str, float | int | str | bool | None]:
        """Flat dict for breakdown_json / autopsy. Skips empty values."""
        out: dict = {}
        for k in self.__slots__:  # type: ignore[attr-defined]
            if k in ("mint", "ts", "name", "symbol"):
                continue
            v = getattr(self, k)
            if v not in (None, ""):
                out[k] = v
        return out


@dataclass(slots=True)
class GateResult:
    name: str
    passed: bool
    detail: str = ""
    value: float | None = None


@dataclass(slots=True)
class Candidate:
    mint: str
    ts: float
    score: float
    breakdown: dict[str, float]
    gates: list[GateResult]
    token_age_sec: float
    lane: Lane = Lane.DROP
    reason: str = ""
    id: int | None = None           # db id once persisted

    @property
    def gates_passed(self) -> bool:
        return all(g.passed for g in self.gates)


@dataclass(slots=True)
class LLMReview:
    mint: str
    candidate_id: int | None
    verdict: Verdict
    confidence: float
    reasons: list[str]
    latency_ms: int
    model: str = ""
    error: str = ""


@dataclass(slots=True)
class Decision:
    mint: str
    candidate_id: int | None
    action: str                     # "buy" | "skip"
    lane: Lane
    size_sol: float
    reason: str
    ts: float = field(default_factory=now)


@dataclass(slots=True)
class Order:
    mint: str
    side: Side
    size_sol: float
    idempotency_key: str
    mode: str
    expected_price_sol: float | None = None
    status: OrderStatus = OrderStatus.PENDING
    tx_sig: str = ""
    error: str = ""
    ts: float = field(default_factory=now)
    id: int | None = None


@dataclass(slots=True)
class Position:
    mint: str
    lane: Lane
    entry_price_sol: float
    size_sol: float
    tokens: float
    opened_at: float
    status: PositionStatus = PositionStatus.OPEN
    closed_at: float | None = None
    id: int | None = None
