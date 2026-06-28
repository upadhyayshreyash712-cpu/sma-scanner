import sqlite3
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self):
        conn = self._get_conn()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS signal_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    entry_price REAL,
                    sma_fast REAL,
                    sma_slow REAL,
                    stop_loss REAL,
                    take_profit REAL,
                    candle_timestamp INTEGER,
                    data_source TEXT DEFAULT 'Binance',
                    detected_at TEXT DEFAULT (datetime('now')),
                    hash_id TEXT UNIQUE
                );

                CREATE TABLE IF NOT EXISTS watchlist (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    market_cap REAL,
                    last_updated TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS sma_state (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    sma_fast REAL,
                    sma_slow REAL,
                    prev_sma_fast REAL,
                    prev_sma_slow REAL,
                    last_candle_timestamp INTEGER,
                    last_signal_timestamp INTEGER,
                    last_signal_type TEXT,
                    updated_at TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (symbol, timeframe)
                );

                CREATE TABLE IF NOT EXISTS processed_candles (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    PRIMARY KEY (symbol, timeframe, timestamp)
                );
            """)
            conn.commit()
        finally:
            conn.close()

    def _hash_id(self, symbol: str, timeframe: str, candle_ts: int, signal_type: str) -> str:
        return f"{symbol}|{timeframe}|{candle_ts}|{signal_type}"

    def signal_exists(self, symbol: str, timeframe: str, candle_timestamp: int, signal_type: str) -> bool:
        hid = self._hash_id(symbol, timeframe, candle_timestamp, signal_type)
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT 1 FROM signal_history WHERE hash_id=?", (hid,)).fetchone()
            return row is not None
        finally:
            conn.close()

    def record_signal(self, symbol: str, timeframe: str, signal_type: str,
                      entry_price: float, sma_fast: float, sma_slow: float,
                      stop_loss: float, take_profit: float, candle_timestamp: int,
                      data_source: str = "Binance"):
        hid = self._hash_id(symbol, timeframe, candle_timestamp, signal_type)
        conn = self._get_conn()
        try:
            conn.execute(
                """INSERT OR IGNORE INTO signal_history
                   (symbol, timeframe, signal_type, entry_price, sma_fast, sma_slow,
                    stop_loss, take_profit, candle_timestamp, data_source, hash_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, timeframe, signal_type, entry_price, sma_fast, sma_slow,
                 stop_loss, take_profit, candle_timestamp, data_source, hid)
            )
            conn.commit()
        finally:
            conn.close()

    def get_watchlist(self) -> list:
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT symbol FROM watchlist ORDER BY market_cap DESC").fetchall()
            return [r["symbol"] for r in rows]
        finally:
            conn.close()

    def update_watchlist(self, coins: list):
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM watchlist")
            for c in coins:
                conn.execute(
                    "INSERT INTO watchlist (symbol, name, market_cap) VALUES (?,?,?)",
                    (c["symbol"], c.get("name", ""), c.get("market_cap", 0))
                )
            conn.commit()
        finally:
            conn.close()

    def get_sma_state(self, symbol: str, timeframe: str) -> Optional[dict]:
        conn = self._get_conn()
        try:
            row = conn.execute(
                """SELECT sma_fast, sma_slow, prev_sma_fast, prev_sma_slow,
                          last_candle_timestamp, last_signal_timestamp, last_signal_type
                   FROM sma_state WHERE symbol=? AND timeframe=?""",
                (symbol, timeframe)
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def update_sma_state(self, symbol: str, timeframe: str, sma_fast: float, sma_slow: float,
                         prev_sma_fast: float, prev_sma_slow: float,
                         last_candle_timestamp: int, last_signal_timestamp=None,
                         last_signal_type=None):
        conn = self._get_conn()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO sma_state
                   (symbol, timeframe, sma_fast, sma_slow, prev_sma_fast, prev_sma_slow,
                    last_candle_timestamp, last_signal_timestamp, last_signal_type, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))""",
                (symbol, timeframe, sma_fast, sma_slow, prev_sma_fast, prev_sma_slow,
                 last_candle_timestamp, last_signal_timestamp, last_signal_type)
            )
            conn.commit()
        finally:
            conn.close()

    def is_candle_processed(self, symbol: str, timeframe: str, timestamp: int) -> bool:
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT 1 FROM processed_candles WHERE symbol=? AND timeframe=? AND timestamp=?",
                (symbol, timeframe, timestamp)
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def mark_candle_processed(self, symbol: str, timeframe: str, timestamp: int):
        conn = self._get_conn()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO processed_candles (symbol, timeframe, timestamp) VALUES (?,?,?)",
                (symbol, timeframe, timestamp)
            )
            conn.commit()
        finally:
            conn.close()

    def get_all_signals(self) -> list:
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT * FROM signal_history ORDER BY detected_at DESC").fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def export_signals_csv(self, path: str):
        import csv
        signals = self.get_all_signals()
        if not signals:
            return
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=signals[0].keys())
            w.writeheader()
            w.writerows(signals)
