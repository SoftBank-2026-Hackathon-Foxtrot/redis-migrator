"""SQLite stands in for the RDB (source of truth). One file shared by the app and the workers."""
import json
import os
import sqlite3
import threading

DB_PATH = os.environ.get("APP_DB", "/data/app.db")
_local = threading.local()

SCHEMA = """
CREATE TABLE IF NOT EXISTS groupbuys(id INTEGER PRIMARY KEY, title TEXT, price INTEGER, stock INTEGER,
                                     initial INTEGER, closed_ms INTEGER, close_at_ms INTEGER);
CREATE TABLE IF NOT EXISTS join_requests(request_id TEXT PRIMARY KEY, groupbuy_id INTEGER, user_id TEXT,
                                         qty INTEGER, status TEXT, created_ms INTEGER, enqueued_ms INTEGER,
                                         processed_ms INTEGER, requeued INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS orders(request_id TEXT PRIMARY KEY, groupbuy_id INTEGER, user_id TEXT, qty INTEGER);
CREATE TABLE IF NOT EXISTS notifications(ref TEXT PRIMARY KEY, kind TEXT, sent_ms INTEGER);
CREATE TABLE IF NOT EXISTS stats(name TEXT PRIMARY KEY, value INTEGER);
CREATE INDEX IF NOT EXISTS jr_status ON join_requests(status, enqueued_ms);
"""


def close_job(groupbuy_id: int) -> str:
    """The q:delayed member for a group-buy's closing: one job per group-buy, however often it is added."""
    return json.dumps({"type": "close", "groupbuy_id": groupbuy_id})


def connect() -> sqlite3.Connection:
    """One connection per thread, reused (the app's endpoints run in a thread pool)."""
    db = getattr(_local, "db", None)
    if db is None:
        db = sqlite3.connect(DB_PATH, timeout=30, isolation_level="DEFERRED")
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("PRAGMA synchronous=NORMAL")
        _local.db = db
    return db


def init_schema(db: sqlite3.Connection, reset: bool = False):
    db.execute("PRAGMA journal_mode=WAL")   # persistent in the file; set once here
    if reset:
        for t in ("groupbuys", "join_requests", "orders", "notifications", "stats"):
            db.execute(f"DROP TABLE IF EXISTS {t}")
    db.executescript(SCHEMA)
    db.commit()


def bump(db: sqlite3.Connection, name: str, n: int = 1):
    db.execute("INSERT INTO stats(name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value=value+?",
               (name, n, n))
