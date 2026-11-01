"""Measures one cutover with N session keys under load, with latency to the source Redis.

  python demo/bench.py --keys 1000 --mode full --label A
  python demo/bench.py --keys 100000 --mode delta --label B

Appends one line to demo/bench-results.jsonl. Host load: 12 users (scenario.py's Load).
"""
import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
import redis.asyncio as aioredis

from scenario import ADMIN, ADMIN_HEADERS, APP, COMPOSE_DIR, CTL, GROUPBUYS, Load

FILES = ["-f", "docker-compose.yml", "-f", "bench.compose.yml"]
SEED_TTL_S = 7200


def compose(*args, env=None):
    subprocess.run(["docker", "compose", *FILES, *args], cwd=COMPOSE_DIR, check=True, capture_output=True, env=env)


async def seed_keys(n: int):
    c = aioredis.from_url(ADMIN["aws"])
    for start in range(0, n, 5000):
        p = c.pipeline(transaction=False)
        for i in range(start, min(start + 5000, n)):
            p.set(f"session:bench:{i}", f"u{i}", ex=SEED_TTL_S)
        await p.execute()
    await c.aclose()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", type=int, required=True)
    ap.add_argument("--mode", choices=["full", "delta"], default="delta")
    ap.add_argument("--label", required=True)
    ap.add_argument("--rtt-ms", type=int, default=30)
    ap.add_argument("--budget-ms", type=int, default=1_800_000)
    a = ap.parse_args()

    env = {**os.environ, "BENCH_DELAY_MS": str(a.rtt_ms // 2)}
    compose("down", "-v", "--remove-orphans", env=env)
    compose("up", "-d", "--build", env=env)
    ctl = httpx.AsyncClient(base_url=CTL, timeout=a.budget_ms / 1000 + 60, headers=ADMIN_HEADERS)
    web = httpx.AsyncClient(base_url=APP, timeout=30)
    for _ in range(60):
        try:
            if (await web.post("/seed", params={"groupbuys": GROUPBUYS, "close_in_s": 3600})).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        await asyncio.sleep(1)
    else:
        raise SystemExit("seed failed")
    await seed_keys(a.keys)

    load = Load(12, 150)
    runner = asyncio.create_task(load.run())
    await asyncio.sleep(5)

    t0 = time.time()
    r = await ctl.post("/migrations", json={"target": "gcp", "budget_ms": a.budget_ms, "mode": a.mode})
    if r.status_code >= 400:
        raise SystemExit(f"start: {r.text}")
    while not (await ctl.get("/migrations/current/ready")).json()["ready"]:
        await asyncio.sleep(0.2)
    ready_s = round(time.time() - t0, 1)
    await asyncio.sleep(3)          # a few seconds of ordinary changes before the pause

    r = await ctl.post("/migrations/current/cutover", json={"budget_ms": a.budget_ms, "mode": a.mode})
    if r.status_code >= 400:
        raise SystemExit(f"cutover: {r.text}")
    res = r.json()
    await asyncio.sleep(4)
    load.stop.set()
    await runner

    win = (res["switched_at"] - res["total_ms"] / 1000, res["switched_at"])
    near = load.summary(win[0] - 0.2, win[1] + 1)
    out = {"label": a.label, "mode": a.mode, "keys": a.keys, "rtt_ms": a.rtt_ms, "until_ready_s": ready_s,
           "paused_ms": res["paused_ms"], "reconcile_ms": res["reconcile_ms"], "verify_ms": res["verify_ms"],
           "reconciled_keys": res["reconciled_keys"], "failed": load.failed, "session_lost": load.session_lost,
           "max_ms_near_pause": near["max_ms"], "errors": dict(list(load.errors.items())[:3])}
    print(json.dumps(out, ensure_ascii=False))
    with (Path(__file__).parent / "bench-results.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(out, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    asyncio.run(main())
