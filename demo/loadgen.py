"""Load generator for the demo console. Runs inside the compose network: Docker Desktop's
port forwarding saturates near 96 rps on Windows and would hide the app's real latency.

  uvicorn demo.loadgen:app --port 8100
"""
import asyncio
import time

from fastapi import FastAPI

from .scenario import Load

USERS, THINK_MS, CHART_SECONDS = 12, 150, 120

app = FastAPI(title="demo load generator")
load: Load | None = None
runner: asyncio.Task | None = None


@app.post("/start")
async def start():
    global load, runner
    if not (runner and not runner.done()):
        load = Load(USERS, THINK_MS)
        runner = asyncio.create_task(load.run())
    return {"users": USERS}


@app.post("/stop")
async def stop():
    if load:
        load.stop.set()
    if runner:
        await asyncio.gather(runner, return_exceptions=True)
    return {"ok": True}


def series(now: float) -> list[dict]:
    per: dict[int, list[float]] = {}
    for t, ms in reversed(load.lat):
        if t < now - CHART_SECONDS:
            break
        per.setdefault(int(t), []).append(ms)
    fails: dict[int, int] = {}
    for t in reversed(load.failures):
        if t < now - CHART_SECONDS:
            break
        fails[int(t)] = fails.get(int(t), 0) + 1
    out = []
    for sec in sorted(per):
        if sec >= int(now):     # this second is still filling up
            continue
        xs = sorted(per[sec])
        out.append({"t": sec, "n": len(xs), "p50": round(xs[len(xs) // 2], 1),
                    "p99": round(xs[int(len(xs) * .99)], 1), "failed": fails.get(sec, 0)})
    return out


@app.get("/stats")
async def stats():
    now = time.time()
    running = bool(runner and not runner.done())
    error = None
    if runner and runner.done() and not runner.cancelled() and runner.exception():
        error = repr(runner.exception())
    if load is None:
        return {"now": now, "running": False, "error": None, "users": USERS, "ok": 0, "failed": 0,
                "session_lost": 0, "errors": {}, "series": []}
    return {"now": now, "running": running, "error": error, "users": USERS, "ok": load.ok, "failed": load.failed,
            "session_lost": load.session_lost, "series": series(now),
            "errors": dict(sorted(load.errors.items(), key=lambda x: -x[1])[:3])}


@app.get("/summary")
async def summary(since: float = 0, until: float = 1e18):
    return load.summary(since, until) if load else {"requests": 0, "failed": 0, "p50_ms": 0, "p99_ms": 0, "max_ms": 0}
