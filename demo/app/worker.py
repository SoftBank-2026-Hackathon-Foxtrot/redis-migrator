"""Queue worker for the group-buy app. Talks to Redis only through the central proxy.

  q:join     confirm or sell out, inside one DB transaction guarded by status=QUEUED,
             so a message delivered twice changes stock once
  q:notify   record each notification once (ref is the primary key)
  q:delayed  due jobs are moved to q:notify by one Lua script (atomic, safe with many workers)
  pending    messages a dead worker left un-acked are taken over with XAUTOCLAIM
  dead       a message that keeps failing moves to <stream>:dead, so it cannot block a drain
  reconciler (--reconciler) re-enqueues requests that stayed QUEUED too long: covers an app
             crash between INSERT and XADD, and a queue lost with a failed Redis. It also
             puts back q:delayed jobs of group-buys that are still open

  python -m demo.app.worker --name w1 [--reconciler]
"""
import argparse
import json
import logging
import os
import time

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from .db import bump, close_job, connect

log = logging.getLogger("worker")
GROUP = "workers"
REQUEUE_AFTER_MS = 10_000
MAX_DELIVERIES = 3

MOVE_DUE = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, 50)
for _, job in ipairs(due) do
  redis.call('XADD', KEYS[2], '*', 'job', job)
  redis.call('ZREM', KEYS[1], job)
end
return #due
"""


def now_ms() -> int:
    return int(time.time() * 1000)


class Worker:
    def __init__(self, name: str, reconciler: bool):
        self.name, self.reconciler = name, reconciler
        self.r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=True,
                                      retry=Retry(NoBackoff(), 0), socket_timeout=10)
        self.move_due = self.r.register_script(MOVE_DUE)
        self.db = connect()

    def handle_join(self, rid: str) -> bool:
        db = self.db
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT status, groupbuy_id, user_id, qty FROM join_requests WHERE request_id=?",
                         (rid,)).fetchone()
        if row is None or row[0] != "QUEUED":
            db.rollback()
            if row is not None:
                bump(db, "duplicate_deliveries")
                db.commit()
            return False
        _, gb, uid, qty = row
        cur = db.execute("UPDATE groupbuys SET stock=stock-? WHERE id=? AND stock>=? AND closed_ms IS NULL",
                         (qty, gb, qty))
        status = "CONFIRMED" if cur.rowcount == 1 else "SOLD_OUT"
        if status == "CONFIRMED":
            db.execute("INSERT INTO orders(request_id, groupbuy_id, user_id, qty) VALUES (?,?,?,?)",
                       (rid, gb, uid, qty))
        db.execute("UPDATE join_requests SET status=?, processed_ms=? WHERE request_id=? AND status='QUEUED'",
                   (status, now_ms(), rid))
        db.commit()
        self.r.xadd("q:notify", {"job": json.dumps({"type": status.lower(), "ref": rid})})
        return True

    def handle_notify(self, job: dict):
        ref = job.get("ref") or f"{job['type']}:{job.get('groupbuy_id')}"
        db = self.db
        if job["type"] == "close":
            db.execute("UPDATE groupbuys SET closed_ms=? WHERE id=? AND closed_ms IS NULL",
                       (now_ms(), job["groupbuy_id"]))
        cur = db.execute("INSERT OR IGNORE INTO notifications(ref, kind, sent_ms) VALUES (?,?,?)",
                         (ref, job["type"], now_ms()))
        if cur.rowcount == 0:
            bump(db, "duplicate_notifications")
        db.commit()

    def consume(self, stream: str, msgs, reclaimed: bool = False):
        for mid, fields in msgs:
            if reclaimed and self.delivered(stream, mid) > MAX_DELIVERIES:
                self.bury(stream, mid, fields)
                continue
            try:
                if stream == "q:join":
                    self.handle_join(fields["request_id"])
                else:
                    self.handle_notify(json.loads(fields["job"]))
            except (redis.RedisError, OSError):
                raise
            except Exception as e:  # noqa: BLE001  left pending: XAUTOCLAIM retries it
                self.db.rollback()
                log.warning("message %s on %s failed: %r", mid, stream, e)
                continue
            self.r.xack(stream, GROUP, mid)

    def delivered(self, stream: str, mid: str) -> int:
        rows = self.r.xpending_range(stream, GROUP, min=mid, max=mid, count=1)
        return rows[0]["times_delivered"] if rows else 0

    def bury(self, stream: str, mid: str, fields: dict):
        self.r.xadd(f"{stream}:dead", {**fields, "source_id": mid})
        self.r.xack(stream, GROUP, mid)
        bump(self.db, "dead_letters")
        self.db.commit()
        log.warning("message %s on %s moved to %s:dead", mid, stream, stream)

    def requeue_stale(self):
        cutoff = now_ms() - REQUEUE_AFTER_MS
        rows = self.db.execute("SELECT request_id FROM join_requests WHERE status='QUEUED' "
                               "AND COALESCE(enqueued_ms, created_ms) < ? LIMIT 500", (cutoff,)).fetchall()
        for (rid,) in rows:
            self.r.xadd("q:join", {"request_id": rid})
            self.db.execute("UPDATE join_requests SET enqueued_ms=?, requeued=requeued+1 WHERE request_id=?",
                            (now_ms(), rid))
        if rows:
            bump(self.db, "requeued", len(rows))
            log.info("requeued %d stale requests", len(rows))
        self.db.commit()

    def restore_scheduled(self):
        """The DB owns closing times: a job lost with a Redis comes back (NX keeps existing ones)."""
        rows = self.db.execute("SELECT id, close_at_ms FROM groupbuys WHERE closed_ms IS NULL "
                               "AND close_at_ms IS NOT NULL").fetchall()
        self.db.commit()
        if rows:
            self.r.zadd("q:delayed", {close_job(gb): at for gb, at in rows}, nx=True)

    def ensure_groups(self):
        for stream in ("q:join", "q:notify"):
            try:
                self.r.xgroup_create(stream, GROUP, id="0", mkstream=True)
            except redis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

    def run(self):
        last_claim = last_due = last_requeue = last_restore = 0.0
        self.ensure_groups()
        while True:
            try:
                for stream in ("q:join", "q:notify"):   # one stream per call: the proxy routes per command
                    res = self.r.xreadgroup(GROUP, self.name, {stream: ">"}, count=50, block=500)
                    for s, msgs in res or []:
                        self.consume(s, msgs)
                t = time.time()
                if t - last_claim > 5:
                    last_claim = t
                    for stream in ("q:join", "q:notify"):
                        _, msgs, _ = self.r.xautoclaim(stream, GROUP, self.name, min_idle_time=5000, count=50)
                        self.consume(stream, msgs, reclaimed=True)
                if t - last_due > 1:
                    last_due = t
                    self.move_due(keys=["q:delayed", "q:notify"], args=[now_ms()])
                if self.reconciler and t - last_requeue > 3:
                    last_requeue = t
                    self.requeue_stale()
                if self.reconciler and t - last_restore > 5:
                    last_restore = t
                    self.restore_scheduled()
            except (redis.RedisError, OSError) as e:
                log.warning("redis error: %s", e)
                try:
                    bump(self.db, "worker_redis_errors")
                    self.db.commit()
                except Exception:  # noqa: BLE001
                    self.db.rollback()
                time.sleep(0.2)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--reconciler", action="store_true")
    a = ap.parse_args()
    for _ in range(60):   # wait for /seed to create the schema
        try:
            connect().execute("SELECT 1 FROM join_requests LIMIT 1")
            break
        except Exception:  # noqa: BLE001
            time.sleep(1)
    Worker(a.name, a.reconciler).run()


if __name__ == "__main__":
    main()
