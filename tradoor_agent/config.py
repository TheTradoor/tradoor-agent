"""Configuration: everything comes from environment variables / .env.

Env var names are the field names in UPPER_CASE (MODE, HELIUS_API_KEY, ...).
"""
from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict


class Mode(str, Enum):
    DRY_RUN = "dry_run"   # no wallet needed, no tx sent, PnL is virtual
    CONFIRM = "confirm"   # every entry waits for owner approval in Telegram
    LIVE = "live"         # fully automatic


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- core ---
    mode: Mode = Mode.DRY_RUN
    data_dir: Path = Path("./data")
    log_level: str = "INFO"
    timezone: str = "Asia/Jakarta"          # used for "today" in daily stats / loss cap
    i_understand_live_trading: str = ""     # must be "yes" before MODE=live is allowed to start

    # --- telegram ops ---
    telegram_bot_token: str = ""
    telegram_owner_ids: str = ""            # comma separated Telegram user ids
    telegram_alert_chat_id: str = ""        # optional; defaults to DM every owner

    # --- data / rpc ---
    helius_api_key: str = ""
    solana_rpc_url: str = ""                # overrides the Helius-derived URL
    pumpportal_ws_url: str = "wss://pumpportal.fun/api/data"
    pumpportal_api_key: str = ""            # appended as ?api-key=; needed for paid trade subs + trade API
    pumpportal_trade_subs: bool = False     # subscribeTokenTrade for watchlist (PAID: 0.01 SOL / 10k events)
    pumpportal_trade_subs_max: int = 20     # max mints subscribed at once when trade subs are on

    # --- gmgn openapi ---
    gmgn_api_key: str = ""
    gmgn_host: str = "https://openapi.gmgn.ai"
    gmgn_rate_weight: int = 50              # plan budget (weight per second). Pro = 50
    gmgn_budget_fraction: float = 0.8       # use at most this share of the budget
    gmgn_cache_sec: int = 15                # per (endpoint, mint) cache
    gmgn_timeout_sec: float = 6.0

    # --- screener (phase 1) ---
    screen_first_check_sec: int = 45        # GMGN needs ~30-40s to index a fresh launch; earlier checks return zeros
    screen_recheck_sec: int = 30            # recheck interval while on watchlist
    screen_max_age_sec: int = 1800          # drop from watchlist after this age
    screen_max_watchlist: int = 150
    screen_alert_max_per_hour: int = 20     # Telegram alerts for tokens that pass hard gates
    screen_dead_after_checks: int = 3       # evict as "dead" after this many checks if still below...
    screen_dead_max_holders: int = 10       # ...this many holders (and never passed)
    screen_stagnant_backoff_max: int = 4    # recheck interval multiplier cap for tokens that stop growing

    # --- cheap gates (from the launch event itself) ---
    cheap_max_dev_buy_sol: float = 3.0      # dev initial buy larger than this -> drop
    cheap_max_dev_supply_pct: float = 0.08  # dev initial buy as share of supply -> drop

    # --- hard gates (from GMGN snapshot); all rates are 0-1 ---
    gate_min_holders: int = 30
    gate_min_liquidity_usd: float = 3000.0
    gate_min_mcap_usd: float = 8000.0
    gate_max_mcap_usd: float = 250000.0
    gate_max_top10: float = 0.45            # early pump.fun runners routinely sit at 30-45%
    gate_max_dev_hold: float = 0.15
    gate_max_bundler: float = 0.20
    gate_max_rat: float = 0.20
    gate_max_snipers: int = 15              # wallet_tags_stat.sniper_wallets
    gate_max_sniper_hold: float = 0.30      # stat.top70_sniper_hold_rate
    gate_max_entrapment: float = 0.10       # stat.top_entrapment_trader_percentage (known trap wallets)
    gate_max_creator_created: int = 50      # stat.creator_created_count: serial launchers above this are dropped
    gate_require_renounced: bool = True

    # --- scoring (phase 2); weights are relative, components 0-1 ---
    score_w_momentum: float = 0.0           # outlook 48h: score 85+ (momentum-heavy) had 86% drawdowns > 30%. Off until proven.
    score_w_growth: float = 20.0
    score_w_quality: float = 40.0
    score_w_context: float = 40.0           # context carries stage (bond 60%+ / migrated) and smart money

    # --- entry filters (trader; evaluated after routing, before the risk gate) ---
    entry_max_chase_1m: float = 0.25        # price already up more than this in the last minute -> skip (buying the top)
    entry_max_chase_5m: float = 1.00        # same over 5 minutes
    entry_min_holders: int = 40
    entry_max_sell_ratio_1m: float = 0.55   # sells/(buys+sells) in the last minute above this -> distribution, skip
    entry_min_swaps_1m: int = 8             # too quiet to exit into
    entry_min_progress: float = 0.60        # bonding progress at entry (or migrated). <60% had median -61% @30m, 60%+ had +4%
    entry_min_smart: int = 2                # smart wallets at entry. 3+ had median +113% max / +4% @30m vs -58% for 0

    # --- lanes (v0.8: trigger -> filter -> security -> entry, one lane per trigger source) ---
    # mode per lane: trade | observe | off.  observe = run every check and record the outcome (tracker) but never buy,
    # so a trigger can be measured on hundreds of tokens without the 3-position cap censoring the data.
    fast_mode: str = "trade"                # scored launches (screener -> scorer -> FAST route)
    dexpaid_mode: str = "trade"             # DexScreener paid profile / boost
    mig_mode: str = "trade"                 # pump.fun migration (graduated to the AMM)
    trend_mode: str = "trade"               # GMGN trending rank (new entrant into the top N by swaps)

    # shared filter defaults for the trigger lanes (dexpaid / mig / trend); each lane can override any of them below.
    # these tokens already have a pool, so liquidity and holders are real requirements here (the launch lane waits for them to grow)
    trig_require_pool: bool = True          # bonding-curve tokens wait (re-check) until they migrate; v0.7 lost 0.25 SOL buying dexpaid on the curve
    trig_min_holders: int = 60
    trig_min_liquidity_usd: float = 10000.0 # thin pools = slippage; the paper fill model assumes a liquid pool
    trig_min_mcap_usd: float = 30000.0
    trig_max_mcap_usd: float = 1000000.0
    trig_max_token_age_sec: int = 86400     # older than this is not a launch play (trending majors, revived coins)
    trig_max_chase_1m: float = 0.30         # already up this much in 1m -> wait, re-check (chasing the top)
    trig_min_change_1m: float = -0.10       # bleeding this much in 1m -> wait, re-check (knife)
    trig_max_chase_5m: float = 1.00
    trig_min_swaps_1m: int = 10             # too quiet to exit into
    trig_max_sell_ratio_1m: float = 0.60    # sells/(buys+sells) in 1m above this = distribution
    trig_min_smart: int = 0
    trig_recheck_sec: int = 15              # soft-filter fail -> look again after this
    trig_window_sec: int = 120              # give up on the trigger after this long without a clean entry
    trig_size_mult: float = 1.0

    # per-lane overrides (empty = use the trig_* default)
    dexpaid_enabled: bool = True            # kept for old .env files; same as DEXPAID_MODE=off when false
    dexpaid_poll_sec: float = 2.0           # /token-profiles/latest/v1 and /token-boosts/latest/v1 are 60 req/min each
    dexpaid_boosts: bool = True             # also poll token boosts (the "dex boosted" orange icon)
    dexpaid_max_token_age_sec: int | None = None
    dexpaid_require_pool: bool | None = None
    dexpaid_min_holders: int | None = None
    dexpaid_min_liquidity_usd: float | None = None
    dexpaid_min_mcap_usd: float | None = None
    dexpaid_max_mcap_usd: float | None = None
    dexpaid_max_chase_1m: float | None = 0.40   # by the time we see the profile bots are in; tolerate a bit more
    dexpaid_min_change_1m: float | None = None
    dexpaid_min_swaps_1m: int | None = None
    dexpaid_min_smart: int | None = None
    dexpaid_recheck_sec: int | None = None
    dexpaid_window_sec: int | None = None
    dexpaid_size_mult: float | None = None

    mig_first_check_sec: int = 30           # right after migration the AMM pool is seconds old and GMGN has not re-indexed; wait
    mig_max_token_age_sec: int | None = None
    mig_require_pool: bool | None = None
    mig_min_holders: int | None = 100       # a graduated token with < 100 holders is a bundle that bonded itself
    mig_min_liquidity_usd: float | None = 15000.0
    mig_min_mcap_usd: float | None = 50000.0
    mig_max_mcap_usd: float | None = None
    mig_max_chase_1m: float | None = None
    mig_min_change_1m: float | None = -0.05 # the post-migration dump must be over: no bleeding in the last minute
    mig_min_swaps_1m: int | None = None
    mig_min_smart: int | None = None
    mig_recheck_sec: int | None = 20
    mig_window_sec: int | None = 300        # the stabilise-then-run usually happens in the first 5 minutes or not at all
    mig_size_mult: float | None = None

    trend_poll_sec: float = 10.0            # /v1/market/rank is weight 1
    trend_interval: str = "1m"              # 1m | 5m | 1h : rank window
    trend_order_by: str = "swaps"           # swaps | volume | marketcap | holder_count | change1h
    trend_limit: int = 40                   # top N; a token entering the top N for the first time is the trigger
    trend_max_token_age_sec: int | None = None
    trend_require_pool: bool | None = None
    trend_min_holders: int | None = None
    trend_min_liquidity_usd: float | None = None
    trend_min_mcap_usd: float | None = None
    trend_max_mcap_usd: float | None = None
    trend_max_chase_1m: float | None = None
    trend_min_change_1m: float | None = None
    trend_min_swaps_1m: int | None = None
    trend_min_smart: int | None = None
    trend_recheck_sec: int | None = None
    trend_window_sec: int | None = 90
    trend_size_mult: float | None = None

    # --- outcome tracker: follows every passed trigger (traded or observed) so the outlook can grade each lane ---
    track_enabled: bool = True
    track_interval_sec: int = 30            # token/info is weight 1; 30s matches the screener's resolution
    track_duration_sec: int = 1800
    track_max: int = 80                     # concurrently tracked mints (80 x 1 weight / 30s = 2.7 weight/s)

    # --- paper trading (dry_run) ---
    paper_slippage_bps: float = 150.0       # price impact, each side
    paper_fee_bps: float = 100.0            # pool/launchpad fee + PumpPortal 0.5%, each side
    paper_fixed_fee_sol: float = 0.0005     # priority fee + tx fee, each side. On a 0.1 SOL position this is 0.5% per side: size matters
    # exits (v0.8): lock small profits, cut losers, never give back a run. same for every lane unless a lane overrides.
    #   trail arms at +10% and trails 5% from the peak (a +50% run exits around +45%); hard stop -12%; time stop 15m;
    #   flat stop: after 5 minutes without reaching +3% the money moves to the next trigger.
    exit_tp_pct: float = 1.00               # take profit vs entry (the trail does the real work; this catches a vertical candle)
    exit_sl_pct: float = 0.12               # stop loss vs entry
    exit_trail_arm_pct: float = 0.10        # once up this much, trailing stop is armed
    exit_trail_pct: float = 0.05            # trail distance from peak
    exit_max_hold_sec: int = 900            # time stop
    exit_flat_after_sec: int = 300          # after this long...
    exit_flat_min_pct: float = 0.03         # ...if the peak never reached this, exit (capital is better used elsewhere); 0 = off
    exit_holders_drop_pct: float = 0.20     # holders fell this much from peak -> exit
    # per-lane exit overrides (empty = global)
    dexpaid_exit_tp_pct: float | None = None
    dexpaid_exit_sl_pct: float | None = None
    dexpaid_exit_trail_arm_pct: float | None = None
    dexpaid_exit_trail_pct: float | None = None
    dexpaid_exit_max_hold_sec: int | None = None
    dexpaid_exit_flat_after_sec: int | None = None
    mig_exit_tp_pct: float | None = None
    mig_exit_sl_pct: float | None = None
    mig_exit_trail_arm_pct: float | None = None
    mig_exit_trail_pct: float | None = None
    mig_exit_max_hold_sec: int | None = None
    mig_exit_flat_after_sec: int | None = None
    trend_exit_tp_pct: float | None = None
    trend_exit_sl_pct: float | None = None
    trend_exit_trail_arm_pct: float | None = None
    trend_exit_trail_pct: float | None = None
    trend_exit_max_hold_sec: int | None = None
    trend_exit_flat_after_sec: int | None = None
    position_check_sec: int = 5             # manager loop interval for the slow checks (holders, dev); price exits run on ticks
    tick_pricing: bool = True               # subscribe PumpPortal trade ticks for open positions (needs PUMPPORTAL_API_KEY)
    tick_stale_sec: int = 20                # no tick for this long -> fall back to GMGN price for exits
    reentry_cooldown_sec: int = 1800        # no second entry on the same mint within this window

    # --- wallet (confirm / live only) ---
    wallet_private_key: str = ""            # base58; use a dedicated hot wallet

    # --- paper / risk ---
    paper_start_sol: float = 5.0            # virtual starting balance for dry_run equity curve
    position_size_sol: float = 0.1
    max_open_positions: int = 3
    daily_loss_cap_sol: float = 0.5
    cooldown_after_loss_sec: int = 300
    llm_lane_size_mult: float = 0.5         # grey-zone entries get smaller size

    # --- routing ---
    score_high: float = 80.0                # >= high -> fast lane
    score_low: float = 50.0                 # < low -> drop ; between -> grey zone
    grey_min_token_age_sec: int = 180       # grey zone only for tokens at least this old

    # --- llm lane ---
    anthropic_api_key: str = ""
    llm_lane_enabled: bool = False
    llm_model: str = "claude-haiku-5-5"
    llm_timeout_sec: float = 2.5
    llm_verdict_cache_sec: int = 300
    llm_max_boost: float = 15.0
    llm_min_confidence: float = 0.6

    # --- web dashboard ---
    web_enabled: bool = True
    web_host: str = "0.0.0.0"
    web_port: int = 8787
    web_token: str = ""                     # empty -> generated once and stored in params table
    web_public_url: str = ""                # e.g. http://1.2.3.4:8787 ; used only in the startup message

    # --- supervisor ---
    heartbeat_stale_sec: int = 60
    watchdog_interval_sec: int = 10
    events_retention_days: int = 7
    candidates_retention_days: int = 21     # screener + tracker rows (the outlook reads these); 0 = keep forever

    # ------------------------------------------------------------------ helpers

    # lane name (models.Lane value) -> settings prefix of its overrides
    LANE_PREFIX: ClassVar[dict[str, str]] = {"fast": "fast", "grey": "fast", "dexpaid": "dexpaid", "migration": "mig", "trending": "trend"}
    LANE_MODES: ClassVar[tuple[str, ...]] = ("trade", "observe", "off")

    @classmethod
    def lane_prefix(cls, lane: str) -> str:
        return cls.LANE_PREFIX.get(lane, lane)

    @property
    def owner_ids(self) -> set[int]:
        raw = self.telegram_owner_ids.split("#", 1)[0].replace(";", ",")
        ids: set[int] = set()
        for part in raw.split(","):
            part = part.strip()
            if part.lstrip("-").isdigit():
                ids.add(int(part))
        return ids

    @property
    def rpc_url(self) -> str:
        if self.solana_rpc_url:
            return self.solana_rpc_url
        if self.helius_api_key:
            return f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"
        return "https://api.mainnet-beta.solana.com"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "agent.sqlite"

    @property
    def kill_file(self) -> Path:
        return self.data_dir / "KILL"

    def problems(self) -> list[str]:
        """Sanity check. Empty list means the config is runnable."""
        p: list[str] = []
        if self.mode in (Mode.CONFIRM, Mode.LIVE):
            p.append("MODE confirm/live belum tersedia sebelum fase 3 (eksekusi on-chain); pakai dry_run")
        if self.mode in (Mode.CONFIRM, Mode.LIVE) and not self.wallet_private_key:
            p.append("WALLET_PRIVATE_KEY kosong tapi MODE bukan dry_run")
        if self.mode == Mode.LIVE and self.i_understand_live_trading.strip().lower() != "yes":
            p.append("MODE=live butuh I_UNDERSTAND_LIVE_TRADING=yes")
        if self.telegram_bot_token and not self.owner_ids:
            p.append("TELEGRAM_BOT_TOKEN diisi tapi TELEGRAM_OWNER_IDS kosong (command Telegram bakal nonaktif)")
        if self.llm_lane_enabled and not self.anthropic_api_key:
            p.append("LLM_LANE_ENABLED=true tapi ANTHROPIC_API_KEY kosong")
        if not self.gmgn_api_key:
            p.append("GMGN_API_KEY kosong (screener butuh ini buat hard gates)")
        if self.pumpportal_trade_subs and not self.pumpportal_api_key:
            p.append("PUMPPORTAL_TRADE_SUBS=true butuh PUMPPORTAL_API_KEY")
        if not (0 < self.gmgn_budget_fraction <= 1):
            p.append("GMGN_BUDGET_FRACTION harus di antara 0 dan 1")
        if self.score_low >= self.score_high:
            p.append("SCORE_LOW harus lebih kecil dari SCORE_HIGH")
        for k in ("fast_mode", "dexpaid_mode", "mig_mode", "trend_mode"):
            if getattr(self, k) not in self.LANE_MODES:
                p.append(f"{k.upper()} harus salah satu dari {'/'.join(self.LANE_MODES)}")
        if self.trend_interval not in ("1m", "5m", "1h", "6h", "24h"):
            p.append("TREND_INTERVAL harus 1m/5m/1h/6h/24h")
        if self.exit_trail_pct <= 0 or self.exit_trail_arm_pct <= 0 or self.exit_sl_pct <= 0:
            p.append("EXIT_TRAIL_PCT / EXIT_TRAIL_ARM_PCT / EXIT_SL_PCT harus > 0")
        if self.position_size_sol <= 0:
            p.append("POSITION_SIZE_SOL harus > 0")
        if self.max_open_positions < 1:
            p.append("MAX_OPEN_POSITIONS minimal 1")
        if self.daily_loss_cap_sol <= 0:
            p.append("DAILY_LOSS_CAP_SOL harus > 0")
        try:
            from zoneinfo import ZoneInfo
            ZoneInfo(self.timezone)
        except Exception:
            p.append(f"TIMEZONE tidak dikenal: {self.timezone}")
        return p

    def summary(self) -> dict[str, str]:
        """Config overview with secrets masked, for --check and /status."""
        def mask(v: str) -> str:
            if not v:
                return "(kosong)"
            return v[:4] + "…" + v[-4:] if len(v) > 10 else "set"

        return {
            "mode": self.mode.value,
            "data_dir": str(self.data_dir.resolve()),
            "timezone": self.timezone,
            "rpc": self.rpc_url.split("?")[0],
            "helius_api_key": mask(self.helius_api_key),
            "gmgn_api_key": mask(self.gmgn_api_key),
            "gmgn_budget": f"{self.gmgn_rate_weight} x {self.gmgn_budget_fraction:.0%} weight/s",
            "pumpportal_api_key": mask(self.pumpportal_api_key),
            "pumpportal_trade_subs": str(self.pumpportal_trade_subs),
            "telegram_bot_token": mask(self.telegram_bot_token),
            "telegram_owner_ids": ",".join(map(str, sorted(self.owner_ids))) or "(kosong)",
            "wallet_private_key": mask(self.wallet_private_key),
            "anthropic_api_key": mask(self.anthropic_api_key),
            "llm_lane_enabled": str(self.llm_lane_enabled),
            "position_size_sol": str(self.position_size_sol),
            "max_open_positions": str(self.max_open_positions),
            "daily_loss_cap_sol": str(self.daily_loss_cap_sol),
            "score_low/high": f"{self.score_low}/{self.score_high}",
            "paper": f"size {self.position_size_sol} SOL · sl {self.exit_sl_pct:.0%} · trail {self.exit_trail_arm_pct:.0%}/{self.exit_trail_pct:.0%} · hold {self.exit_max_hold_sec // 60}m · max {self.max_open_positions} pos",
            "lanes": f"fast={self.fast_mode} dexpaid={self.dexpaid_mode if self.dexpaid_enabled else 'off'} migration={self.mig_mode} trending={self.trend_mode}",
        }
