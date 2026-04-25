"""
SQLite persistence layer.
Tables: signals, trades, positions, daily_stats, events.
"""

import sqlite3
import logging
from datetime import datetime, date
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        TEXT    NOT NULL,
    market_id TEXT    NOT NULL,
    question  TEXT,
    side      TEXT    NOT NULL,
    price     REAL    NOT NULL,
    edge      REAL    NOT NULL,
    prob      REAL    NOT NULL,
    size      REAL    NOT NULL,
    traded    INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS trades (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL,
    order_id    TEXT,
    market_id   TEXT    NOT NULL,
    question    TEXT,
    side        TEXT    NOT NULL,
    price       REAL    NOT NULL,
    size_usdc   REAL    NOT NULL,
    shares      REAL    NOT NULL,
    stop_price  REAL    NOT NULL,
    status      TEXT    NOT NULL,
    pnl         REAL,
    closed_ts   TEXT,
    error       TEXT
);

CREATE TABLE IF NOT EXISTS positions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id    TEXT    UNIQUE,
    market_id   TEXT    NOT NULL,
    question    TEXT,
    token_id    TEXT    NOT NULL,
    side        TEXT    NOT NULL,
    entry_price REAL    NOT NULL,
    shares      REAL    NOT NULL,
    size_usdc   REAL    NOT NULL,
    stop_price  REAL    NOT NULL,
    status      TEXT    NOT NULL DEFAULT 'OPEN',
    opened_ts   TEXT    NOT NULL,
    closed_ts   TEXT
);

CREATE TABLE IF NOT EXISTS daily_stats (
    date                TEXT PRIMARY KEY,
    trades              INTEGER DEFAULT 0,
    wins                INTEGER DEFAULT 0,
    realized_pnl        REAL    DEFAULT 0.0,
    consecutive_losses  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    ts   TEXT NOT NULL,
    type TEXT NOT NULL,
    msg  TEXT
);
"""


@dataclass
class PositionRow:
    order_id: str
    market_id: str
    question: str
    token_id: str
    side: str
    entry_price: float
    shares: float
    size_usdc: float
    stop_price: float
    status: str
    opened_ts: str


class Database:
    def __init__(self, path: str = "polybot.db"):
        self.path = path
        self._init_schema()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self):
        with self._conn() as conn:
            conn.executescript(_SCHEMA)

    # ── Signals ───────────────────────────────────────────────────────────────

    def log_signal(self, signal, market: dict, traded: bool = False):
        ts = datetime.utcnow().isoformat()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO signals "
                "(ts,market_id,question,side,price,edge,prob,size,traded) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    ts, market["id"], market.get("question", ""),
                    signal.side, signal.price, signal.edge,
                    signal.probability, signal.size, int(traded),
                ),
            )

    # ── Trades ────────────────────────────────────────────────────────────────

    def log_trade(self, order_result, market: dict, signal, stop_price: float):
        ts = datetime.utcnow().isoformat()
        shares = round(signal.size / signal.price, 4)
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO trades "
                "(ts,order_id,market_id,question,side,price,size_usdc,"
                " shares,stop_price,status,error) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    ts, order_result.order_id, market["id"],
                    market.get("question", ""), signal.side, signal.price,
                    signal.size, shares, stop_price,
                    "PLACED", order_result.error,
                ),
            )
        self._update_daily(trades_delta=1)

    def settle_trade(self, order_id: str, pnl: float):
        """Record final P&L when a market resolves."""
        ts = datetime.utcnow().isoformat()
        with self._conn() as conn:
            conn.execute(
                "UPDATE trades SET pnl=?, status='SETTLED', closed_ts=? "
                "WHERE order_id=?",
                (pnl, ts, order_id),
            )
        won = int(pnl > 0)
        self._update_daily(pnl_delta=pnl, win_delta=won, loss_if_lost=int(pnl <= 0))

    # ── Positions ─────────────────────────────────────────────────────────────

    def open_position(self, order_result, market: dict, signal, stop_price: float):
        ts = datetime.utcnow().isoformat()
        shares = round(signal.size / signal.price, 4)
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO positions "
                "(order_id,market_id,question,token_id,side,entry_price,"
                " shares,size_usdc,stop_price,status,opened_ts) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    order_result.order_id, market["id"],
                    market.get("question", ""), signal.token_id,
                    signal.side, signal.price, shares,
                    signal.size, stop_price, "OPEN", ts,
                ),
            )

    def close_position(self, order_id: str, reason: str = "STOP_LOSS"):
        ts = datetime.utcnow().isoformat()
        with self._conn() as conn:
            conn.execute(
                "UPDATE positions SET status=?, closed_ts=? WHERE order_id=?",
                (reason, ts, order_id),
            )

    def get_open_positions(self) -> list[PositionRow]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM positions WHERE status='OPEN'"
            ).fetchall()
        return [
            PositionRow(
                order_id=r["order_id"], market_id=r["market_id"],
                question=r["question"], token_id=r["token_id"],
                side=r["side"], entry_price=r["entry_price"],
                shares=r["shares"], size_usdc=r["size_usdc"],
                stop_price=r["stop_price"], status=r["status"],
                opened_ts=r["opened_ts"],
            )
            for r in rows
        ]

    # ── Daily stats ───────────────────────────────────────────────────────────

    def get_today_stats(self) -> dict:
        today = date.today().isoformat()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT trades,wins,realized_pnl,consecutive_losses "
                "FROM daily_stats WHERE date=?",
                (today,),
            ).fetchone()
        if row:
            return {
                "trades": row["trades"],
                "wins": row["wins"],
                "realized_pnl": row["realized_pnl"],
                "consecutive_losses": row["consecutive_losses"],
            }
        return {"trades": 0, "wins": 0, "realized_pnl": 0.0, "consecutive_losses": 0}

    def _update_daily(
        self,
        trades_delta: int = 0,
        pnl_delta: float = 0.0,
        win_delta: int = 0,
        loss_if_lost: int = 0,
    ):
        today = date.today().isoformat()
        stats = self.get_today_stats()
        new_consec = (
            0 if win_delta else stats["consecutive_losses"] + loss_if_lost
        )
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO daily_stats (date,trades,wins,realized_pnl,consecutive_losses)
                VALUES (?,?,?,?,?)
                ON CONFLICT(date) DO UPDATE SET
                    trades             = trades + ?,
                    wins               = wins + ?,
                    realized_pnl       = realized_pnl + ?,
                    consecutive_losses = ?
                """,
                (
                    today, trades_delta, win_delta, pnl_delta, new_consec,
                    trades_delta, win_delta, pnl_delta, new_consec,
                ),
            )

    # ── Events ────────────────────────────────────────────────────────────────

    def log_event(self, event_type: str, msg: str):
        ts = datetime.utcnow().isoformat()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO events (ts,type,msg) VALUES (?,?,?)",
                (ts, event_type, msg),
            )
