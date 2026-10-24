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

    async def prune_candidates(self, older_than_days: int) -> int:
        """Drop old screener / tracker rows (the outlook only needs recent paths; trades keep their own copy of the entry breakdown)."""
        import time
        if older_than_days <= 0:
            return 0
        cutoff = time.time() - older_than_days * 86_400
        cur = await self.conn.execute("DELETE FROM candidates WHERE ts < ?", (cutoff,))
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

    async def update_candidate_score(self, cid: int, score: float, breakdown: dict) -> None:
        await self.execute("UPDATE candidates SET score=?, breakdown_json=? WHERE id=?", (score, json.dumps(breakdown, default=str), cid))

    # ------------------------------------------------------------ trading (paper + live share these)

    async def insert_decision(self, d, risk: dict | None = None) -> int:
        """d: models.Decision"""
        return await self.execute(
            "INSERT INTO decisions(ts, mint, candidate_id, action, lane, size_sol, reason, risk_json) VALUES(?,?,?,?,?,?,?,?)",
            (d.ts, d.mint, d.candidate_id, d.action, d.lane.value, d.size_sol, d.reason, json.dumps(risk or {}, default=str)),
        )

    async def insert_order(self, o, position_id: int | None = None) -> int:
        """o: models.Order"""
        return await self.execute(
            "INSERT INTO orders(ts, mint, side, mode, size_sol, expected_price_sol, status, tx_sig, error, idempotency_key, position_id) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (o.ts, o.mint, o.side.value, o.mode, o.size_sol, o.expected_price_sol, o.status.value, o.tx_sig, o.error, o.idempotency_key, position_id),
        )

    async def update_order(self, oid: int, status: str, tx_sig: str = "", error: str = "", position_id: int | None = None) -> None:
        await self.execute(
            "UPDATE orders SET status=?, tx_sig=COALESCE(NULLIF(?, ''), tx_sig), error=?, position_id=COALESCE(?, position_id) WHERE id=?",
            (status, tx_sig, error, position_id, oid),
        )

    async def insert_fill(self, order_id: int, tokens: float, sol: float, price_sol: float, fee_sol: float, slippage_bps: float) -> int:
        import time
        return await self.execute(
            "INSERT INTO fills(order_id, ts, tokens, sol, price_sol, fee_sol, slippage_bps) VALUES(?,?,?,?,?,?,?)",
            (order_id, time.time(), tokens, sol, price_sol, fee_sol, slippage_bps),
        )

    async def insert_position(self, p, meta: dict | None = None) -> int:
        """p: models.Position"""
        return await self.execute(
            "INSERT INTO positions(mint, lane, opened_at, entry_price_sol, size_sol, tokens, status, meta_json) VALUES(?,?,?,?,?,?,?,?)",
            (p.mint, p.lane.value, p.opened_at, p.entry_price_sol, p.size_sol, p.tokens, p.status.value, json.dumps(meta or {}, default=str)),
        )

    async def update_position_meta(self, pid: int, meta: dict, status: str | None = None) -> None:
        if status:
            await self.execute("UPDATE positions SET meta_json=?, status=? WHERE id=?", (json.dumps(meta, default=str), status, pid))
        else:
            await self.execute("UPDATE positions SET meta_json=? WHERE id=?", (json.dumps(meta, default=str), pid))

    async def close_position(self, pid: int, closed_at: float, meta: dict) -> None:
        await self.execute("UPDATE positions SET status='closed', closed_at=?, meta_json=? WHERE id=?", (closed_at, json.dumps(meta, default=str), pid))

    async def insert_trade(self, position_id: int, mint: str, lane: str, closed_at: float, holding_sec: float, pnl_sol: float, pnl_pct: float, exit_reason: str) -> int:
        tid = await self.execute(
            "INSERT INTO trades(position_id, mint, lane, day, closed_at, holding_sec, pnl_sol, pnl_pct, exit_reason) VALUES(?,?,?,?,?,?,?,?,?)",
            (position_id, mint, lane, self.today(), closed_at, holding_sec, pnl_sol, pnl_pct, exit_reason),
        )
        await self.refresh_daily_stats()
        return tid

    async def open_positions(self) -> list[dict]:
        rows = await self.fetchall("SELECT * FROM positions WHERE status IN ('open','closing') ORDER BY opened_at")
        out = []
        for r in rows:
            d = dict(r)
            try:
                d["meta"] = json.loads(d.pop("meta_json") or "{}")
            except json.JSONDecodeError:
                d["meta"] = {}
            out.append(d)
        return out

    async def has_open_position(self, mint: str) -> bool:
        row = await self.fetchone("SELECT 1 FROM positions WHERE mint=? AND status IN ('open','closing') LIMIT 1", (mint,))
        return row is not None

    async def last_trade_ts_for_mint(self, mint: str) -> float | None:
        row = await self.fetchone("SELECT MAX(closed_at) AS t FROM trades WHERE mint=?", (mint,))
        return float(row["t"]) if row and row["t"] is not None else None

    async def last_loss_ts(self) -> float | None:
        row = await self.fetchone("SELECT MAX(closed_at) AS t FROM trades WHERE pnl_sol < 0")
        return float(row["t"]) if row and row["t"] is not None else None

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

    async def trade_stats(self) -> dict:
        """All-time: realized pnl, wins, losses, current streak (+n wins / -n losses), count."""
        row = await self.fetchone(
            "SELECT COUNT(*) AS n, COALESCE(SUM(pnl_sol),0) AS pnl, "
            "SUM(CASE WHEN pnl_sol > 0 THEN 1 ELSE 0 END) AS wins, "
            "SUM(CASE WHEN pnl_sol <= 0 THEN 1 ELSE 0 END) AS losses FROM trades"
        )
        recent = await self.fetchall("SELECT pnl_sol FROM trades ORDER BY closed_at DESC LIMIT 200")
        streak = 0
        if recent:
            sign = recent[0]["pnl_sol"] > 0
            for r in recent:
                if (r["pnl_sol"] > 0) == sign:
                    streak += 1
                else:
                    break
            if not sign:
                streak = -streak
        return {"trades": int(row["n"] or 0), "pnl_sol": float(row["pnl"] or 0), "wins": int(row["wins"] or 0), "losses": int(row["losses"] or 0), "streak": streak}

    async def trade_stats_by_lane(self) -> dict[str, dict]:
        rows = await self.fetchall(
            "SELECT lane, COUNT(*) AS n, COALESCE(SUM(pnl_sol),0) AS pnl, "
            "SUM(CASE WHEN pnl_sol > 0 THEN 1 ELSE 0 END) AS wins, "
            "AVG(CASE WHEN pnl_sol > 0 THEN pnl_pct END) AS avg_win_pct, AVG(CASE WHEN pnl_sol <= 0 THEN pnl_pct END) AS avg_loss_pct, "
            "AVG(holding_sec) AS avg_hold FROM trades GROUP BY lane"
        )
        return {r["lane"]: {"trades": int(r["n"]), "pnl_sol": float(r["pnl"] or 0), "wins": int(r["wins"] or 0),
                            "avg_win_pct": r["avg_win_pct"], "avg_loss_pct": r["avg_loss_pct"], "avg_hold_sec": r["avg_hold"]} for r in rows}

    async def equity_curve(self, start_sol: float, limit: int = 2000) -> list[dict]:
        """Cumulative balance after each closed trade, oldest first, with markers."""
        rows = await self.fetchall(
            "SELECT t.closed_at, t.pnl_sol, t.mint, t.exit_reason, t.lane, p.opened_at, p.size_sol "
            "FROM trades t LEFT JOIN positions p ON p.id = t.position_id ORDER BY t.closed_at ASC LIMIT ?", (limit,)
        )
        bal = start_sol
        out = []
        for r in rows:
            bal += float(r["pnl_sol"] or 0)
            out.append({"ts": r["closed_at"], "balance": bal, "pnl": r["pnl_sol"], "mint": r["mint"], "exit": r["exit_reason"], "lane": r["lane"], "opened_at": r["opened_at"], "size_sol": r["size_sol"]})
        return out

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
