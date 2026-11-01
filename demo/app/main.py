"""치이카와 공구앱 (demo): the app being migrated. It only knows REDIS_URL (the central proxy).

Source of truth is the RDB (SQLite here). Redis holds what the design moves:
  session:{sid}          login session, TTL 30 min (must survive a cutover)
  cache:groupbuy:{id}    cache-aside group-buy detail, TTL 5 min
  rl:{uid}:{minute}      join rate limit, TTL 1 min (not migrated)
  q:join, q:notify       Streams with consumer group "workers"
  q:delayed              ZSET of scheduled jobs (score = due time)

Join flow: INSERT join_requests(status=QUEUED) → XADD q:join → "accepted".
The worker decides CONFIRMED / SOLD_OUT inside one DB transaction.
"""
import json
import os
import time
import uuid

import redis
from fastapi import FastAPI, Header, HTTPException
from redis.backoff import NoBackoff
from redis.retry import Retry

from .db import close_job, connect, init_schema

SESSION_TTL, CACHE_TTL, RL_WINDOW, RL_LIMIT = 1800, 300, 60, 1000

# No client-side retries: any error the proxy lets through must show up in the load test.
r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=True,
                         retry=Retry(NoBackoff(), 0), max_connections=200, socket_timeout=10)
app = FastAPI(title="chiikawa group-buy")


def user_of(sid: str | None) -> str:
    uid = r.get(f"session:{sid}") if sid else None
    if uid is None:
        raise HTTPException(401, "login first")
    r.expire(f"session:{sid}", SESSION_TTL)
    return uid


@app.post("/seed")
def seed(groupbuys: int = 10, stock: int = 1_000_000, close_in_s: int = 30):
    db = connect()
    init_schema(db, reset=True)
    now = int(time.time() * 1000)
    closes = {i: now + close_in_s * 1000 + i * 1000 for i in range(groupbuys)}
    db.executemany("INSERT INTO groupbuys(id, title, price, stock, initial, close_at_ms) VALUES (?,?,?,?,?,?)",
                   [(i, f"치이카와 굿즈 #{i}", 12000 + i * 100, stock, stock, closes[i]) for i in range(groupbuys)])
    db.commit()
    r.zadd("q:delayed", {close_job(i): at for i, at in closes.items()})
    return {"groupbuys": groupbuys, "stock": stock}


@app.post("/login/{uid}")
def login(uid: str):
    sid = uuid.uuid4().hex
    r.set(f"session:{sid}", uid, ex=SESSION_TTL)
    return {"sid": sid}


@app.post("/logout")
def logout(x_session: str | None = Header(None)):
    r.delete(f"session:{x_session}")
    return {"ok": True}


@app.get("/me")
def me(x_session: str | None = Header(None)):
    return {"uid": user_of(x_session)}


@app.get("/groupbuys/{gb}")
def get_gb(gb: int, x_session: str | None = Header(None)):
    user_of(x_session)
    key = f"cache:groupbuy:{gb}"
    if (hit := r.get(key)) is not None:
        return json.loads(hit) | {"cache": "hit"}
    row = connect().execute("SELECT id, title, price, stock FROM groupbuys WHERE id=?", (gb,)).fetchone()
    if row is None:
        raise HTTPException(404)
    val = {"id": row[0], "title": row[1], "price": row[2]}
    r.set(key, json.dumps(val), ex=CACHE_TTL)
    return val | {"cache": "miss"}


@app.post("/groupbuys/{gb}/price")
def set_price(gb: int, price: int):
    db = connect()
    db.execute("UPDATE groupbuys SET price=? WHERE id=?", (price, gb))
    db.commit()
    r.delete(f"cache:groupbuy:{gb}")   # cache-aside: invalidate after commit
    return {"ok": True}


@app.post("/groupbuys/{gb}/join")
def join(gb: int, qty: int = 1, x_session: str | None = Header(None)):
    uid = user_of(x_session)
    rl = f"rl:{uid}:{int(time.time()) // RL_WINDOW}"
    with r.pipeline(transaction=True) as p:     # MULTI/EXEC through the proxy
        p.incr(rl)
        p.expire(rl, RL_WINDOW)
        n, _ = p.execute()
    if n > RL_LIMIT:
        raise HTTPException(429, "slow down")
    rid = uuid.uuid4().hex
    now = int(time.time() * 1000)
    db = connect()
    db.execute("INSERT INTO join_requests(request_id, groupbuy_id, user_id, qty, status, created_ms) "
               "VALUES (?,?,?,?, 'QUEUED', ?)", (rid, gb, uid, qty, now))
    db.commit()
    r.xadd("q:join", {"request_id": rid})
    db.execute("UPDATE join_requests SET enqueued_ms=? WHERE request_id=?", (int(time.time() * 1000), rid))
    db.commit()
    return {"request_id": rid, "status": "QUEUED"}


@app.get("/report")
def report():
    db = connect()
    rows = db.execute("SELECT request_id, status, qty, groupbuy_id, enqueued_ms, processed_ms, requeued "
                      "FROM join_requests").fetchall()
    gbs = db.execute("SELECT id, initial, stock FROM groupbuys").fetchall()
    orders = db.execute("SELECT groupbuy_id, COUNT(*), SUM(qty) FROM orders GROUP BY groupbuy_id").fetchall()
    stats = dict(db.execute("SELECT name, value FROM stats").fetchall())
    notified = db.execute("SELECT kind, COUNT(*) FROM notifications GROUP BY kind").fetchall()
    return {"requests": [list(x) for x in rows], "groupbuys": [list(x) for x in gbs],
            "orders": [list(x) for x in orders], "stats": stats, "notifications": dict(notified)}


@app.get("/cache-check")
def cache_check():
    """Every cached group-buy must equal the DB row (cache-aside invariant)."""
    db = connect()
    bad, checked = [], 0
    for gb, title, price in db.execute("SELECT id, title, price FROM groupbuys"):
        hit = r.get(f"cache:groupbuy:{gb}")
        if hit is None:
            continue
        checked += 1
        v = json.loads(hit)
        if v["price"] != price or v["title"] != title:
            bad.append({"gb": gb, "cache": v, "db_price": price})
    return {"checked": checked, "mismatched": bad, "ok": not bad}
