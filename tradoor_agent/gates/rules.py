"""Gate rules. Pure functions: (data, thresholds) -> list[GateResult].

Two tiers:
  cheap_gates : from the launch event alone, zero API cost. Kills obvious junk.
  hard_gates  : from a GMGN TokenSnapshot. The real filter.

A gate that cannot be evaluated (source returned None) is recorded as passed
with detail "n/a" so one missing field never blocks a token, but the autopsy
can see how often each field was missing.
"""
from __future__ import annotations

from ..ingest.pumpportal import dev_initial_buy
from ..models import GateResult, TokenEvent, TokenSnapshot


def _pct(v: float | None) -> str:
    return "n/a" if v is None else f"{v * 100:.1f}%"


def cheap_gates(evt: TokenEvent, *, max_dev_buy_sol: float, max_dev_supply_pct: float) -> list[GateResult]:
    sol, share = dev_initial_buy(evt)
    out = [
        GateResult("name_present", bool(evt.symbol or evt.name), detail=f"{evt.symbol!r}/{evt.name!r}"),
        GateResult("dev_buy_sol", sol <= max_dev_buy_sol, detail=f"{sol:.2f} SOL <= {max_dev_buy_sol}", value=sol),
        GateResult("dev_supply_pct", share <= max_dev_supply_pct, detail=f"{_pct(share)} <= {_pct(max_dev_supply_pct)}", value=share),
    ]
    return out


def hard_gates(
    snap: TokenSnapshot,
    *,
    min_holders: int,
    min_liquidity_usd: float,
    min_mcap_usd: float,
    max_mcap_usd: float,
    max_top10: float,
    max_dev_hold: float,
    max_bundler: float,
    max_rat: float,
    max_snipers: int,
    max_sniper_hold: float,
    max_entrapment: float,
    require_renounced: bool,
    max_creator_created: int = 50,
) -> list[GateResult]:
    def le(name: str, v: float | int | None, limit: float, fmt=_pct) -> GateResult:
        if v is None:
            return GateResult(name, True, detail="n/a")
        return GateResult(name, v <= limit, detail=f"{fmt(v)} <= {fmt(limit)}", value=float(v))

    def ge(name: str, v: float | int | None, limit: float, fmt=str) -> GateResult:
        if v is None:
            return GateResult(name, True, detail="n/a")
        return GateResult(name, v >= limit, detail=f"{fmt(v)} >= {fmt(limit)}", value=float(v))

    usd = lambda x: f"${x:,.0f}"  # noqa: E731
    bonding = not snap.migrated_pool and snap.launchpad_status != 2
    out = [
        ge("holders", snap.holder_count, min_holders),
        # on the bonding curve liquidity is implied by mcap; GMGN often reports 0 there. Gate it only after migration.
        GateResult("liquidity", True, detail="n/a (bonding)") if bonding else ge("liquidity", snap.liquidity_usd, min_liquidity_usd, usd),
        ge("mcap_min", snap.mcap_usd, min_mcap_usd, usd),
        le("mcap_max", snap.mcap_usd, max_mcap_usd, usd),
        le("top10", snap.top10_rate, max_top10),
        le("dev_hold", snap.dev_hold_rate if snap.dev_hold_rate is not None else snap.creator_hold_rate, max_dev_hold),
        le("bundler", snap.bundler_rate, max_bundler),
        le("rat_trader", snap.rat_rate, max_rat),
        le("snipers", snap.sniper_count, max_snipers, fmt=str),
        le("sniper_hold", snap.sniper_hold_rate, max_sniper_hold),
        le("entrapment", snap.entrapment_rate, max_entrapment),
        le("creator_created", snap.creator_created_count, max_creator_created, fmt=str),
        GateResult("honeypot", snap.honeypot is not True, detail="n/a" if snap.honeypot is None else str(snap.honeypot)),
    ]
    if require_renounced:
        # Solana: mint + freeze authority must both be renounced. None -> n/a (pass), False -> fail.
        out.append(GateResult("renounced_mint", snap.renounced_mint is not False, detail="n/a" if snap.renounced_mint is None else str(snap.renounced_mint)))
        out.append(GateResult("renounced_freeze", snap.renounced_freeze is not False, detail="n/a" if snap.renounced_freeze is None else str(snap.renounced_freeze)))
    return out


def failed(gates: list[GateResult]) -> list[str]:
    return [g.name for g in gates if not g.passed]
