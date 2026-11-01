"""End-to-end check of the design under continuous load (run on the host).

  normal:  aws → gcp cutover, normal rollback gcp → aws, aws → gcp again, commit
  failure: aws → gcp cutover, then the gcp Redis container is stopped and a
           failure rollback to aws is run while the load keeps going

Load = users who log in once and keep their session (GET /me, group-buy detail
through the cache, joins through the queue), session churn (login + logout) and
an admin changing prices (cache invalidation).

  python demo/scenario.py [--mode normal|failure] [--users 60]
"""
import argparse
import asyncio
import json
import os
import random
import subprocess
import time
from pathlib import Path

import httpx
import redis.asyncio as aioredis

APP, CTL = os.environ.get("APP_URL", "http://localhost:18000"), "http://localhost:18090"
ADMIN_TOKEN = os.environ.get("STATEMIG_ADMIN_TOKEN", "demo-admin-token")
ADMIN_HEADERS = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
ADMIN = {"aws": "redis://default:adminpw@localhost:16379", "gcp": "redis://default:adminpw@localhost:26379"}
APPUSER = {"aws": "redis://app:apppw@localhost:16379", "gcp": "redis://app:apppw@localhost:26379"}
COMPOSE_DIR = Path(__file__).parent
GROUPBUYS = 10


class Load:
    def __init__(self, users: int, think_ms: float):
        self.users, self.think = users, think_ms / 1000
        self.ok = self.failed = self.session_lost = 0
        self.errors: dict[str, int] = {}
        self.lat: list[tuple[float, float]] = []   # (t, ms)
        self.failures: list[float] = []
        self.failure_log: list[tuple[float, str]] = []
        self.stop = asyncio.Event()

    def _fail(self, what: str, started: float | None = None):
        """Failures and latencies are filed under the time the request was *sent*."""
        self.failed += 1
        self.failures.append(started or time.time())
        self.failure_log.append((round(started or time.time(), 3), what))
        self.errors[what] = self.errors.get(what, 0) + 1

    async def user(self, cli: httpx.AsyncClient, uid: str):
        sid = (await cli.post(f"/login/{uid}")).json()["sid"]
        while not self.stop.is_set():
            x = random.random()
            if x < 0.35:
                method, path = "GET", "/me"
            elif x < 0.70:
                method, path = "GET", f"/groupbuys/{random.randrange(GROUPBUYS)}"
            else:
                method, path = "POST", f"/groupbuys/{random.randrange(GROUPBUYS)}/join"
            t0, sent = time.monotonic(), time.time()
            try:
                resp = await cli.request(method, path, headers={"x-session": sid})
                if resp.status_code == 401:     # the session this user got at login is gone
                    self.session_lost += 1
                    sid = (await cli.post(f"/login/{uid}")).json()["sid"]
                elif resp.status_code >= 500:
                    self._fail(f"{resp.status_code} {resp.text[:80]}", sent)
                else:
                    self.ok += 1
            except Exception as e:  # noqa: BLE001
                self._fail(type(e).__name__ + ": " + str(e)[:80], sent)
            self.lat.append((sent, (time.monotonic() - t0) * 1000))
            await asyncio.sleep(random.uniform(0.5, 1.5) * self.think)

    async def churn(self, cli: httpx.AsyncClient):
        """Short-lived sessions: login then logout. Exercises deletes in the sync."""
        i = 0
        while not self.stop.is_set():
            i += 1
            sent = time.time()
            try:
                sid = (await cli.post(f"/login/tmp{i}")).json()["sid"]
                await cli.post("/logout", headers={"x-session": sid})
            except Exception as e:  # noqa: BLE001
                self._fail("churn " + type(e).__name__, sent)
            await asyncio.sleep(0.05)

    async def prices(self, cli: httpx.AsyncClient):
        while not self.stop.is_set():
            sent = time.time()
            try:
                r = await cli.post(f"/groupbuys/{random.randrange(GROUPBUYS)}/price",
                                   params={"price": random.randrange(10000, 20000)})
                if r.status_code >= 500:
                    self._fail(f"price {r.status_code}", sent)
            except Exception as e:  # noqa: BLE001
                self._fail("price " + type(e).__name__, sent)
            await asyncio.sleep(0.3)

    async def run(self):
        limits = httpx.Limits(max_connections=self.users + 10)
        async with httpx.AsyncClient(base_url=APP, timeout=10, limits=limits) as cli:
            await asyncio.gather(*(self.user(cli, f"u{i}") for i in range(self.users)),
                                 self.churn(cli), self.prices(cli))

    def summary(self, since: float = 0, until: float = 1e18):
        xs = sorted(ms for t, ms in self.lat if since <= t <= until) or [0]
        return {"requests": len(xs), "failed": sum(1 for t in self.failures if since <= t <= until),
                "p50_ms": round(xs[len(xs) // 2], 1), "p99_ms": round(xs[int(len(xs) * .99)], 1),
                "max_ms": round(xs[-1], 1)}


async def post(cli, path, body=None):
    r = await cli.post(path, json=body or {})
    if r.status_code >= 400:
        raise SystemExit(f"{path} → {r.status_code} {r.text}")
    return r.json()


async def wait_state(ctl, states, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        m = (await ctl.get("/status")).json()["migration"]
        if m and m["state"] in states:
            return m
        await asyncio.sleep(0.2)
    raise SystemExit(f"timed out waiting for {states}")


async def wait_ready(ctl, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if (await ctl.get("/migrations/current/ready")).json()["ready"]:
            return
        await asyncio.sleep(0.2)
    raise SystemExit("timed out waiting for READY")


async def app_can_write(env: str) -> str:
    c = aioredis.from_url(APPUSER[env])
    try:
        await c.set("__fence_probe", 1, px=1000)
        return "write allowed"
    except Exception as e:  # noqa: BLE001  redis-py drops the NOPERM prefix from the message
        return "NOPERM" if "no permissions" in str(e) else str(e)[:60]
    finally:
        await c.aclose()


async def count(env: str, pattern: str) -> int:
    c = aioredis.from_url(ADMIN[env])
    try:
        return len([k async for k in c.scan_iter(match=pattern, count=1000)])
    finally:
        await c.aclose()


async def deleted_probe(app: httpx.AsyncClient, n=30) -> list[str]:
    """Sessions created and logged out while syncing: must not exist on the target after cutover."""
    sids = []
    for i in range(n):
        sid = (await app.post(f"/login/probe{i}")).json()["sid"]
        await app.post("/logout", headers={"x-session": sid})
        sids.append(sid)
    return sids


async def absent(env: str, sids: list[str]) -> int:
    c = aioredis.from_url(ADMIN[env])
    try:
        return sum([await c.exists(f"session:{s}") for s in sids])
    finally:
        await c.aclose()


SYNC_MODE = "delta"


async def cutover(ctl, app, src, dst, checks, label):
    print(f"\n== {label}: {src} → {dst}")
    await post(ctl, "/migrations", {"target": dst, "mode": SYNC_MODE})
    await wait_ready(ctl)
    probe = await deleted_probe(app)
    await asyncio.sleep(2)
    res = await post(ctl, "/migrations/current/cutover", {"budget_ms": 3000})
    print(f"   cutover  paused={res['paused_ms']}ms (pause wait {res['pause_wait_ms']}ms, reconcile "
          f"{res['reconcile_ms']}ms / {res['reconciled_keys']} keys, verify {res['verify_ms']}ms) gen={res['gen']}")
    fence = await app_can_write(src)
    left = await absent(dst, probe)
    print(f"   fence    app user on {src}: {fence}")
    print(f"   deletes  logged-out probe sessions on {dst}: {left}/{len(probe)}")
    m = await wait_state(ctl, ["CUTOVER"])
    drained = [e for e in m["events"] if e["msg"] == "queue drain done"][-1]
    print(f"   drain    done {round((drained['t'] - res['switched_at']) * 1000, 1)}ms after resume")
    cc = (await app.get("/cache-check")).json()
    print(f"   cache    checked={cc['checked']} mismatched={len(cc['mismatched'])}")
    end = res["switched_at"]
    start = end - res["total_ms"] / 1000
    checks.append({"label": label, "fence_ok": fence == "NOPERM", "deletes_ok": left == 0, "cache_ok": cc["ok"],
                   "window": (start, end), "paused_ms": res["paused_ms"]})
    return res


def boundary_ok(requests, window) -> tuple[bool, dict]:
    """Requests enqueued before the pause are all processed before any request enqueued after resume."""
    start, end = window[0] * 1000, window[1] * 1000
    pre = [p for _, s, _, _, enq, p, rq in requests if enq and enq < start and p and rq == 0]
    post_ = [p for _, s, _, _, enq, p, rq in requests if enq and enq > end and p and rq == 0]
    if not pre or not post_:
        return True, {"pre": len(pre), "post": len(post_)}
    return max(pre) <= min(post_), {"pre": len(pre), "post": len(post_), "last_pre_ms": max(pre),
                                    "first_post_ms": min(post_)}


async def wait_processed(app, timeout=90):
    t0 = time.time()
    while time.time() - t0 < timeout:
        rep = (await app.get("/report")).json()
        queued = sum(1 for r in rep["requests"] if r[1] == "QUEUED")
        if queued == 0:
            return rep
        await asyncio.sleep(1)
    return rep


def invariants(rep) -> dict:
    confirmed = {}
    for _, status, qty, gb, *_ in rep["requests"]:
        if status == "CONFIRMED":
            confirmed[gb] = confirmed.get(gb, 0) + qty
    orders = {gb: q for gb, _, q in rep["orders"]}
    bad = [gb for gb, init, stock in rep["groupbuys"]
           if not (init - stock == confirmed.get(gb, 0) == orders.get(gb, 0))]
    statuses = {}
    for r in rep["requests"]:
        statuses[r[1]] = statuses.get(r[1], 0) + 1
    return {"ok": not bad, "violations": bad, "statuses": statuses, "stats": rep["stats"],
            "notifications": rep["notifications"]}


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=COMPOSE_DIR, check=True, capture_output=True)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["normal", "failure"], default="normal")
    ap.add_argument("--users", type=int, default=60)
    ap.add_argument("--think-ms", type=float, default=12, help="mean pause between one user's requests")
    ap.add_argument("--sync-mode", choices=["full", "delta"], default="delta", help="where the full compare runs")
    a = ap.parse_args()
    global SYNC_MODE
    SYNC_MODE = a.sync_mode

    async with httpx.AsyncClient(base_url=CTL, timeout=60, headers=ADMIN_HEADERS) as ctl, \
            httpx.AsyncClient(base_url=APP, timeout=30) as app:
        (await app.post("/seed", params={"groupbuys": GROUPBUYS, "close_in_s": 30})).raise_for_status()
        load = Load(a.users, a.think_ms)
        runner = asyncio.create_task(load.run())
        await asyncio.sleep(5)
        checks: list[dict] = []
        result: dict = {"mode": a.mode}

        await cutover(ctl, app, "aws", "gcp", checks, "cutover 1")
        await asyncio.sleep(5)

        if a.mode == "normal":
            lost_before = load.session_lost
            t_rb = time.time()
            rb = await post(ctl, "/migrations/current/rollback", {"mode": "normal"})
            await wait_state(ctl, ["ROLLED_BACK"])
            fence = await app_can_write("gcp")
            print(f"\n== normal rollback gcp → aws: paused={rb['paused_ms']}ms, fence on gcp: {fence}")
            checks.append({"label": "normal rollback", "fence_ok": fence == "NOPERM",
                           "window": (rb["switched_at"] - rb["total_ms"] / 1000, rb["switched_at"]),
                           "paused_ms": rb["paused_ms"]})
            await asyncio.sleep(5)
            result["sessions_lost_in_normal_rollback"] = load.session_lost - lost_before
            await cutover(ctl, app, "aws", "gcp", checks, "cutover 2")
            await post(ctl, "/migrations/current/commit")
            await asyncio.sleep(5)
            result["rollback_window"] = load.summary(t_rb - 1, t_rb + 5)
        else:
            lost_before = load.session_lost
            print("\n== failure: stopping redis-gcp under load")
            t_kill0 = time.time()
            compose("kill", "redis-gcp")
            t_kill = time.time()           # the container is gone from here on
            rb = await post(ctl, "/migrations/current/rollback", {"mode": "failure", "budget_ms": 2000})
            t_rb = time.time()
            fence = await app_can_write("aws")
            await asyncio.sleep(1)
            cc = (await app.get("/cache-check")).json()   # refilled entries must match the DB
            print(f"   rollback call {round((t_rb - t_kill) * 1000)}ms (writes paused {rb['paused_ms']}ms); "
                  f"reverse backlog at failure={rb.get('reverse_backlog_at_failure')} "
                  f"(oldest {rb.get('reverse_oldest_dirty_age_ms')}ms); cache dropped={rb['cache_keys_dropped']}, "
                  f"refilled cache vs DB mismatched={len(cc['mismatched'])}; app on aws: {fence}")
            await asyncio.sleep(10)
            result["failure_times"] = {"kill_start": t_kill0, "killed": t_kill, "rollback_done": t_rb}
            result["failure"] = {"rollback_call_ms": round((t_rb - t_kill) * 1000), "paused_ms": rb["paused_ms"],
                                 "reverse_backlog_at_failure": rb.get("reverse_backlog_at_failure"),
                                 "sessions_lost": load.session_lost - lost_before,
                                 "failed_requests_kill_to_rollback": load.summary(t_kill0, t_rb)["failed"],
                                 "failed_requests_after_rollback": load.summary(t_rb + 0.5)["failed"],
                                 "cache_ok_after_rollback": cc["ok"], "app_write_on_aws": fence}
            compose("start", "redis-gcp")

        load.stop.set()
        await runner
        rep = await wait_processed(app)
        inv = invariants(rep)
        order = {c["label"]: boundary_ok(rep["requests"], c["window"]) for c in checks}
        px = (await ctl.get("/status")).json()["proxy"]
        s = load.summary()

    print("\n== result")
    print(f"   load      requests={s['requests']} failed={load.failed} p50={s['p50_ms']}ms p99={s['p99_ms']}ms "
          f"max={s['max_ms']}ms session_lost={load.session_lost}")
    if load.errors:
        print(f"   errors    {load.errors}")
    print(f"   queue     {inv['statuses']} stats={inv['stats']} notifications={inv['notifications']}")
    print(f"   stock     ok={inv['ok']} violations={inv['violations']}")
    for label, (ok, d) in order.items():
        print(f"   order     {label}: ok={ok} {d}")
    for c in checks:
        w = load.summary(c["window"][0] - 0.2, c["window"][1] + 1.0)   # requests sent around the pause
        c["latency_around_pause"] = w
        print(f"   {c['label']:<16} " + " ".join(f"{k}={v}" for k, v in c.items() if k.endswith("_ok"))
              + f" | pause {c['paused_ms']}ms, sent in pause±: n={w['requests']} p50={w['p50_ms']}ms "
                f"p99={w['p99_ms']}ms max={w['max_ms']}ms failed={w['failed']}")
    print(f"   proxy     switches={px['switches']} max_pause={px['max_pause_ms']}ms denied={px['denied']}")

    ok = inv["ok"] and all(o for o, _ in order.values()) and all(all(v for k, v in c.items() if k.endswith("_ok"))
                                                                   for c in checks)
    if a.mode == "normal":
        ok = ok and load.failed == 0 and load.session_lost == 0
    else:
        f = result["failure"]
        ok = (ok and f["failed_requests_after_rollback"] == 0 and f["app_write_on_aws"] == "write allowed"
              and f["cache_ok_after_rollback"])
        print(f"   failure   {f}")
    result.update(failure_log=load.failure_log, load=s, failed=load.failed, session_lost=load.session_lost, invariants=inv,
                  order={k: v[1] | {"ok": v[0]} for k, v in order.items()}, checks=checks, pass_=ok)
    out = COMPOSE_DIR / f"result-{a.mode}.json"
    out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print("PASS" if ok else "FAIL", f"(details: {out.name})")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
