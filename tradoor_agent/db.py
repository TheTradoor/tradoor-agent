"""SQLite persistence (aiosqlite, WAL). One file: data/agent.sqlite.

Tables are created up front for every phase so later phases only add code,
not migrations. Timestamps are unix seconds (REAL). "day" columns are the
local date in Settings.timezone.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiosqlite

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS tokens (
    mint            TEXT PRIMARY KEY,
    symbol          TEXT,
    name            TEXT,
    creator         TEXT,
    source          TEXT,
    uri             TEXT,
    first_seen_at   REAL NOT NULL,
    created_at      REAL,
    bonded_at       REAL,
    meta_json       TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    topic       TEXT NOT NULL,
    mint        TEXT,
    payload_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_mint ON events(mint);

CREATE TABLE IF NOT EXISTS candidates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              REAL NOT NULL,
    mint            TEXT NOT NULL,
    score           REAL NOT NULL,
    breakdown_json  TEXT,
    gates_json      TEXT,
    token_age_sec   REAL,
    lane            TEXT NOT NULL,
    reason          TEXT
);
CREATE INDEX IF NOT EXISTS idx_candidates_mint ON candidates(mint);
CREATE INDEX IF NOT EXISTS idx_candidates_ts ON candidates(ts);

CREATE TABLE IF NOT EXISTS llm_reviews (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              REAL NOT NULL,
    mint            TEXT NOT NULL,
    candidate_id    INTEGER,
    model           TEXT,
    input_json      TEXT,
    output_json     TEXT,
    verdict         TEXT,
    confidence      REAL,
    latency_ms      INTEGER,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_llm_reviews_mint ON llm_reviews(mint);

CREATE TABLE IF NOT EXISTS decisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              REAL NOT NULL,
    mint            TEXT NOT NULL,
    candidate_id    INTEGER,
    action          TEXT NOT NULL,
    lane            TEXT NOT NULL,
    size_sol        REAL,
    reason          TEXT,
    risk_json       TEXT
);
CREATE INDEX IF NOT EXISTS idx_decisions_ts ON decisions(ts);

CREATE TABLE IF NOT EXISTS orders (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                  REAL NOT NULL,
    mint                TEXT NOT NULL,
    side                TEXT NOT NULL,
    mode                TEXT NOT NULL,
    size_sol            REAL NOT NULL,
    expected_price_sol  REAL,
    status              TEXT NOT NULL,
    tx_sig              TEXT,
    error               TEXT,
    idempotency_key     TEXT NOT NULL UNIQUE,
    position_id         INTEGER
);
CREATE INDEX IF NOT EXISTS idx_orders_mint ON orders(mint);

CREATE TABLE IF NOT EXISTS fills (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    INTEGER NOT NULL,
    ts          REAL NOT NULL,
    tokens      REAL NOT NULL,
    sol         REAL NOT NULL,
    price_sol   REAL NOT NULL,
    fee_sol     REAL DEFAULT 0,
    slippage_bps REAL
);

CREATE TABLE IF NOT EXISTS positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mint            TEXT NOT NULL,
    lane            TEXT NOT NULL,
    opened_at       REAL NOT NULL,
    closed_at       REAL,
    entry_price_sol REAL NOT NULL,
    size_sol        REAL NOT NULL,
    tokens          REAL NOT NULL,
    status          TEXT NOT NULL,
    meta_json       TEXT
);
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);

CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id     INTEGER NOT NULL,
    mint            TEXT NOT NULL,
    lane            TEXT NOT NULL,
    day             TEXT NOT NULL,
    closed_at       REAL NOT NULL,
    holding_sec     REAL,
    pnl_sol         REAL NOT NULL,
    pnl_pct         REAL,
    exit_reason     TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_day ON trades(day);

CREATE TABLE IF NOT EXISTS params (
    key         TEXT PRIMARY KEY,
    value_json  TEXT NOT NULL,
    updated_at  REAL NOT NULL,
    source      TEXT
);

CREATE TABLE IF NOT EXISTS daily_stats (
    day                 TEXT PRIMARY KEY,
    trades              INTEGER DEFAULT 0,
    wins                INTEGER DEFAULT 0,
    losses              INTEGER DEFAULT 0,
    realized_pnl_sol    REAL DEFAULT 0,
    updated_at          REAL
);
"""


class Database:
    def __init__(self, path: Path, timezone: str = "UTC") -> None:
        self.path = path
        self.tz = ZoneInfo(timezone)
        self._conn: aiosqlite.Connection | None = None

    # ------------------------------------------------------------ lifecycle

    async def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()
        log.info("db ready at %s", self.path)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.commit()
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("database not open")
        return self._conn

    # ------------------------------------------------------------ primitives

    async def execute(self, sql: str, params: tuple | list = ()) -> int:
        """Run a write. Returns lastrowid."""
        cur = await self.conn.execute(sql, params)
        await self.conn.commit()
        return cur.lastrowid or 0

    async def fetchone(self, sql: str, params: tuple | list = ()) -> aiosqlite.Row | None:
        cur = await self.conn.execute(sql, params)
        return await cur.fetchone()

    async def fetchall(self, sql: str, params: tuple | list = ()) -> list[aiosqlite.Row]:
        cur = await self.conn.execute(sql, params)
        return list(await cur.fetchall())

    async def tables(self) -> list[str]:
        rows = await self.fetchall("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [r["name"] for r in rows]

    # ------------------------------------------------------------ time helpers

    def today(self) -> str:
        return datetime.now(self.tz).date().isoformat()

    # ------------------------------------------------------------ params

    async def get_param(self, key: str, default: Any = None) -> Any:
        row = await self.fetchone("SELECT value_json FROM params WHERE key=?", (key,))
        return json.loads(row["value_json"]) if row else default

    async def set_param(self, key: str, value: Any, source: str = "manual") -> None:
        import time
        await self.execute(
            "INSERT INTO params(key, value_json, updated_at, source) VALUES(?,?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, "
            "updated_at=excluded.updated_at, source=excluded.source",
            (key, json.dumps(value), time.time(), source),
        )

    async def all_params(self) -> dict[str, Any]:
        rows = await self.fetchall("SELECT key, value_json FROM params ORDER BY key")
        return {r["key"]: json.loads(r["value_json"]) for r in rows}

    # ------------------------------------------------------------ events

    async def log_event(self, topic: str, mint: str | None, payload: dict | None = None) -> int:
        import time
        return await self.execute(
            "INSERT INTO events(ts, topic, mint, payload_json) VALUES(?,?,?,?)",
            (time.time(), topic, mint, json.dumps(payload or {}, default=str)),
        )

    async def prune_events(self, older_than_days: int) -> int:
        import time
        cutoff = time.time() - older_than_days * 86_400
        cur = await self.conn.execute("DELETE FROM events WHERE ts < ?", (cutoff,))
        await self.conn.commit()
        return cur.rowcount or 0

    # ------------------------------------------------------------ tokens / candidates

    async def upsert_token(self, evt) -> None:
        """evt: models.TokenEvent"""
        import time
        raw = evt.raw or {}
        meta = {k: raw.get(k) for k in ("pool", "initialBuy", "solAmount", "marketCapSol", "bondingCurveKey", "signature") if k in raw}
        await self.execute(
            "INSERT INTO tokens(mint, symbol, name, creator, source, uri, first_seen_at, created_at, meta_json) "
            "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(mint) DO UPDATE SET "
            "symbol=COALESCE(NULLIF(excluded.symbol,''), tokens.symbol), "
            "name=COALESCE(NULLIF(excluded.name,''), tokens.name), "
            "creator=COALESCE(NULLIF(excluded.creator,''), tokens.creator)",
            (evt.mint, evt.symbol, evt.name, evt.creator, evt.source, evt.uri, time.time(), evt.ts, json.dumps(meta, default=str)),
        )

    async def mark_migrated(self, mint: str, pool: str = "") -> None:
        import time
        now = time.time()
        await self.execute(
            "INSERT INTO tokens(mint, source, first_seen_at, bonded_at, meta_json) VALUES(?,?,?,?,?) "
            "ON CONFLICT(mint) DO UPDATE SET bonded_at=excluded.bonded_at",
            (mint, "pumpportal", now, now, json.dumps({"pool": pool})),
        )

    async def insert_candidate(self, c) -> int:
        """c: models.Candidate -> row id"""
        from dataclasses import asdict
        return await self.execute(
            "INSERT INTO candidates(ts, mint, score, breakdown_json, gates_json, token_age_sec, lane, reason) VALUES(?,?,?,?,?,?,?,?)",
            (c.ts, c.mint, c.score, json.dumps(c.breakdown, default=str), json.dumps([asdict(g) for g in c.gates], default=str), c.token_age_sec, c.lane.value, c.reason),
        )

    async def candidate_counts_today(self) -> dict[str, int]:
        import time
        rows = await self.fetchall(
            "SELECT lane, COUNT(*) AS n FROM candidates WHERE ts > ? GROUP BY lane", (time.time() - 86_400,)
        )
        return {r["lane"]: int(r["n"]) for r in rows}

    # ------------------------------------------------------------ stats used by ops/risk

    async def open_positions_count(self) -> int:
        row = await self.fetchone("SELECT COUNT(*) AS n FROM positions WHERE status IN ('open','closing')")
        return int(row["n"]) if row else 0

    async def realized_pnl_today(self) -> float:
        row = await self.fetchone(
            "SELECT COALESCE(SUM(pnl_sol),0) AS pnl FROM trades WHERE day=?", (self.today(),)
        )
        return float(row["pnl"]) if row else 0.0

    async def ensure_daily_stats_row(self) -> None:
        import time
        await self.execute(
            "INSERT OR IGNORE INTO daily_stats(day, updated_at) VALUES(?,?)",
            (self.today(), time.time()),
        )

    async def refresh_daily_stats(self) -> None:
        """Recompute today's row from trades (idempotent)."""
        import time
        day = self.today()
        row = await self.fetchone(
            "SELECT COUNT(*) AS n, "
            "SUM(CASE WHEN pnl_sol > 0 THEN 1 ELSE 0 END) AS wins, "
            "SUM(CASE WHEN pnl_sol <= 0 THEN 1 ELSE 0 END) AS losses, "
            "COALESCE(SUM(pnl_sol),0) AS pnl FROM trades WHERE day=?",
            (day,),
        )
        await self.execute(
            "INSERT INTO daily_stats(day, trades, wins, losses, realized_pnl_sol, updated_at) "
            "VALUES(?,?,?,?,?,?) ON CONFLICT(day) DO UPDATE SET trades=excluded.trades, "
            "wins=excluded.wins, losses=excluded.losses, realized_pnl_sol=excluded.realized_pnl_sol, "
            "updated_at=excluded.updated_at",
            (day, row["n"] or 0, row["wins"] or 0, row["losses"] or 0, row["pnl"] or 0.0, time.time()),
        )
