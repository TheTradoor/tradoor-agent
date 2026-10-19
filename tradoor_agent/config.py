"""Configuration: everything comes from environment variables / .env.

Env var names are the field names in UPPER_CASE (MODE, HELIUS_API_KEY, ...).
"""
from __future__ import annotations

from enum import Enum
from pathlib import Path

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
    gate_require_renounced: bool = True

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

    # ------------------------------------------------------------------ helpers

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
        }
