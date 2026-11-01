"""Demo console for a screen share (run on the host, next to docker compose).

Runs the same steps and checks as scenario.py, one button at a time, and shows
the proxy, the Sync Worker and both Redis instances live. The load itself comes
from the loadgen container.

  python demo/console.py     # then open http://localhost:18888
"""
import asyncio
import subprocess
import sys
import time
from pathlib import Path

import httpx
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).parent))
from scenario import (ADMIN, ADMIN_HEADERS, APP, COMPOSE_DIR, CTL, GROUPBUYS, absent, app_can_write,  # noqa: E402
                      boundary_ok, deleted_probe, invariants, wait_processed)

LOADGEN = "http://localhost:18100"
CLOSE_IN_S = 900               # keep q:delayed non-empty during the demo
CHART_SECONDS = 120
HISTORY_KEYS = ["session:", "cache:", "rl:"]
STREAMS = ["q:join", "q:notify"]

app = FastAPI(title="Redis migration demo console")
ctl = httpx.AsyncClient(base_url=CTL, timeout=60, headers=ADMIN_HEADERS)
web = httpx.AsyncClient(base_url=APP, timeout=30)
gen = httpx.AsyncClient(base_url=LOADGEN, timeout=30)


class Demo:
    def __init__(self):
        self.load: dict = {}             # latest stats from the loadgen container
        self.busy: str | None = None
        self.notes: list[dict] = []      # chart markers and the action log
        self.windows: list[dict] = []    # pause windows, for the checks
        self.probe: list[str] = []       # sessions logged out while syncing
        self.probe_left: int | None = None
        self.probe_side: str | None = None
        self.killed: str | None = None
        self.failure_rollback_at: float | None = None
        self.last_result: dict | None = None
        self.checks: list[dict] | None = None
        self.status: dict = {}
        self.ready = False
        self.sides: dict = {}

    def note(self, label: str, kind: str = "info", marker: bool = False):
        self.notes.append({"t": time.time(), "label": label, "kind": kind, "marker": marker})


demo = Demo()


async def side_stats(name: str, url: str) -> dict:
    c = aioredis.from_url(url, socket_timeout=0.5, socket_connect_timeout=0.5, decode_responses=True)
    try:
        counts = dict.fromkeys(HISTORY_KEYS, 0)
        async for k in c.scan_iter(count=1000):
            for p in HISTORY_KEYS:
                if k.startswith(p):
                    counts[p] += 1
        out = {"up": True, "session": counts["session:"], "cache": counts["cache:"], "rl": counts["rl:"],
               "delayed": await c.zcard("q:delayed"), "streams": {}}
        for s in STREAMS:
            info = {"len": await c.xlen(s), "lag": None, "pending": None}
            try:
                for g in await c.xinfo_groups(s):
                    if g["name"] == "workers":
                        info["lag"], info["pending"] = g.get("lag"), g.get("pending")
            except aioredis.ResponseError:
                pass
            out["streams"][s] = info
        return out
    except Exception as e:  # noqa: BLE001
        return {"up": False, "error": type(e).__name__}
    finally:
        await c.aclose()


async def poller():
    """Status is collected here so a dead Redis never stalls the page."""
    while True:
        try:
            demo.status = (await ctl.get("/status", timeout=3)).json()
            m = demo.status.get("migration")
            demo.ready = bool(m and m["state"] == "SYNCING"
                              and (await ctl.get("/migrations/current/ready", timeout=3)).json()["ready"])
        except Exception as e:  # noqa: BLE001
            demo.status, demo.ready = {"error": type(e).__name__}, False
        names = await asyncio.gather(*(side_stats(n, u) for n, u in ADMIN.items()))
        demo.sides = dict(zip(ADMIN, names))
        try:
            demo.load = (await gen.get("/stats", timeout=3)).json()
        except Exception:  # noqa: BLE001
            demo.load = {}
        await asyncio.sleep(0.5)


@app.on_event("startup")
async def startup():
    asyncio.create_task(poller())


@app.get("/api/state")
async def state():
    load = demo.load
    pts = load.get("series", [])
    recent = pts[-5:]
    return {"now": load.get("now", time.time()), "busy": demo.busy, "ready": demo.ready, "status": demo.status,
            "sides": demo.sides, "killed": demo.killed, "notes": demo.notes[-40:], "series": pts,
            "last_result": demo.last_result, "checks": demo.checks, "chart_seconds": CHART_SECONDS,
            "probe_left": demo.probe_left, "probe_total": len(demo.probe), "probe_side": demo.probe_side,
            "load": {"running": load.get("running", False), "error": load.get("error"), "users": load.get("users", 0),
                     "ok": load.get("ok", 0), "failed": load.get("failed", 0),
                     "session_lost": load.get("session_lost", 0),
                     "rps": round(sum(p["n"] for p in recent) / len(recent), 1) if recent else 0,
                     "p50": recent[-1]["p50"] if recent else None, "errors": load.get("errors", {})}}


async def control(method: str, path: str, body: dict | None = None) -> dict:
    r = await ctl.request(method, path, json=body)
    if r.status_code >= 400:
        raise HTTPException(409, r.json().get("detail", r.text))
    return r.json()


def exclusive(name: str):
    """One action at a time; the page greys out the buttons meanwhile."""
    def wrap(fn):
        async def run(*a, **kw):
            if demo.busy:
                raise HTTPException(409, f"{demo.busy} 진행 중")
            demo.busy = name
            try:
                return await fn(*a, **kw)
            finally:
                demo.busy = None
        run.__name__ = fn.__name__
        run.__signature__ = __import__("inspect").signature(fn)
        return run
    return wrap


def compose(*args):
    p = subprocess.run(["docker", "compose", *args], cwd=COMPOSE_DIR, capture_output=True, text=True)
    if p.returncode:
        raise HTTPException(500, p.stderr[-400:])


async def stop_load():
    try:
        await gen.post("/stop")
    except httpx.HTTPError:
        pass                    # nothing is running yet (first reset)


async def summary(since: float = 0, until: float = 1e18) -> dict:
    return (await gen.get("/summary", params={"since": since, "until": until})).json()


@app.post("/api/reset")
@exclusive("초기화")
async def reset():
    await stop_load()
    await asyncio.to_thread(compose, "down", "-v", "--remove-orphans")
    await asyncio.to_thread(compose, "up", "-d", "--build")
    for _ in range(60):
        try:
            r = await web.post("/seed", params={"groupbuys": GROUPBUYS, "close_in_s": CLOSE_IN_S})
            if r.status_code == 200:
                break
        except httpx.HTTPError:
            pass
        await asyncio.sleep(1)
    else:
        raise HTTPException(500, "앱이 준비되지 않았습니다")
    fresh = Demo()
    fresh.busy, fresh.status, fresh.sides = demo.busy, demo.status, demo.sides
    demo.__dict__.update(fresh.__dict__)
    demo.note("초기화 완료: 활성 Redis = aws")
    return {"ok": True}


@app.post("/api/load/start")
@exclusive("부하 시작")
async def load_start():
    res = (await gen.post("/start")).json()
    demo.note(f"부하 시작: 사용자 {res['users']}명")
    return {"ok": True}


@app.post("/api/load/stop")
@exclusive("부하 중지")
async def load_stop():
    await stop_load()
    demo.note("부하 중지")
    return {"ok": True}


class StartReq(BaseModel):
    target: str


@app.post("/api/sync")
@exclusive("동기화 시작")
async def sync_start(req: StartReq):
    res = await control("POST", "/migrations", {"target": req.target})
    demo.last_result = None
    demo.checks = None
    demo.note(f"동기화 시작: {res['source']} → {res['target']}")
    return res


@app.post("/api/cutover")
@exclusive("컷오버")
async def cutover():
    m = demo.status["migration"]
    src, dst = m["source"], m["target"]
    demo.probe = await deleted_probe(web)   # logged out before the cutover: must not reach the target
    await asyncio.sleep(1)
    res = await control("POST", "/migrations/current/cutover", {"budget_ms": 3000})
    demo.probe_left, demo.probe_side = await absent(dst, demo.probe), dst
    fence = await app_can_write(src)
    demo.windows.append({"label": f"{src} → {dst} 컷오버", "window": (res["switched_at"] - res["total_ms"] / 1000,
                                                                     res["switched_at"]),
                         "paused_ms": res["paused_ms"]})
    demo.last_result = {"kind": "cutover", "src": src, "dst": dst, "probe": fence, **res}
    demo.note(f"컷오버 {src} → {dst}: 쓰기 정지 {res['paused_ms']:.0f}ms", "good", marker=True)
    return res


@app.post("/api/rollback/normal")
@exclusive("정상 롤백")
async def rollback_normal():
    m = demo.status["migration"]
    res = await control("POST", "/migrations/current/rollback", {"mode": "normal"})
    fence = await app_can_write(m["target"])
    demo.windows.append({"label": f"{m['target']} → {m['source']} 정상 롤백",
                         "window": (res["switched_at"] - res["total_ms"] / 1000, res["switched_at"]),
                         "paused_ms": res["paused_ms"]})
    demo.last_result = {"kind": "rollback", "src": m["target"], "dst": m["source"], "probe": fence, **res}
    demo.note(f"정상 롤백 {m['target']} → {m['source']}: 쓰기 정지 {res['paused_ms']:.0f}ms", "good", marker=True)
    return res


@app.post("/api/commit")
@exclusive("커밋")
async def commit():
    res = await control("POST", "/migrations/current/commit")
    demo.note("커밋: 역방향 동기화 중지, 원본 정리 가능")
    return res


@app.post("/api/kill")
@exclusive("장애 주입")
async def kill():
    target = demo.status["migration"]["target"]
    await asyncio.to_thread(compose, "kill", f"redis-{target}")
    demo.killed = target
    demo.note(f"장애 주입: redis-{target} kill", "bad", marker=True)
    return {"ok": True}


@app.post("/api/rollback/failure")
@exclusive("장애 롤백")
async def rollback_failure():
    res = await control("POST", "/migrations/current/rollback", {"mode": "failure", "budget_ms": 2000})
    demo.failure_rollback_at = time.time()
    demo.last_result = {"kind": "failure", **res}
    demo.note(f"장애 롤백: 쓰기 정지 {res['paused_ms']:.0f}ms, 캐시 {res['cache_keys_dropped']}개 삭제", "good", marker=True)
    return res


@app.post("/api/recover")
@exclusive("대상 복구")
async def recover():
    await asyncio.to_thread(compose, "start", f"redis-{demo.killed}")
    demo.note(f"redis-{demo.killed} 재시작 (빈 상태)")
    demo.killed = None
    return {"ok": True}


@app.post("/api/verify")
@exclusive("검증")
async def verify():
    """Stops the load first: the stock check reads several tables, which only agree when workers are idle."""
    await stop_load()
    demo.note("부하 중지 후 검증")
    rep = await wait_processed(web)
    inv = invariants(rep)
    cc = (await web.get("/cache-check")).json()
    stats = (await gen.get("/stats")).json()
    rows: list[dict] = []

    def row(name, ok, detail):
        rows.append({"name": name, "ok": ok, "detail": detail})

    failed, ok_n, lost = stats["failed"], stats["ok"], stats["session_lost"]
    if ok_n + failed:
        since = demo.failure_rollback_at
        if since:
            after = (await summary(since + 0.5))["failed"]
            row("롤백 뒤 요청 실패", after == 0, f"롤백 뒤 {after}건 (장애~롤백 구간 포함 전체 {failed}건)")
        else:
            row("요청 실패", failed == 0, f"{failed}건 / 전체 {ok_n + failed}건")
            row("세션 유실", lost == 0, f"{lost}건")
    active = demo.status.get("proxy", {}).get("active")
    for side in ADMIN:
        if side != active and demo.sides.get(side, {}).get("up") and demo.windows:
            fence = await app_can_write(side)
            row(f"구 Redis({side}) 쓰기 차단", fence == "NOPERM", f"앱 사용자로 쓰기 → {fence}")
    if demo.probe_left is not None:
        row("삭제 반영", demo.probe_left == 0,
            f"로그아웃한 세션 {demo.probe_left}/{len(demo.probe)}개가 {demo.probe_side}에 남음")
    row("캐시 = DB", cc["ok"], f"{cc['checked']}개 비교, 불일치 {len(cc['mismatched'])}개")
    row("재고 = 확정 수량 = 주문 수량", inv["ok"], f"위반 공구 {len(inv['violations'])}개")
    queued = inv["statuses"].get("QUEUED", 0)
    row("큐 요청 유실 없음", queued == 0, " · ".join(f"{k} {v}" for k, v in inv["statuses"].items()) + f" · QUEUED {queued}")
    dup = inv["stats"].get("duplicate_deliveries", 0)
    row("중복 전달의 재고 영향 없음", inv["ok"], f"중복 전달 {dup}건, 재고는 1번만 차감")
    for w in demo.windows:
        ok, d = boundary_ok(rep["requests"], w["window"])
        row(f"처리 순서: {w['label']}", ok, f"전환 전 요청 {d.get('pre', 0)}건이 전환 후 요청 {d.get('post', 0)}건보다 먼저 처리")
        if ok_n + failed:
            near = await summary(w["window"][0] - 0.2, w["window"][1] + 1)
            base = await summary(w["window"][0] - 10, w["window"][0] - 1)
            if not near["requests"]:    # the load was restarted after this window
                continue
            row(f"지연: {w['label']}", near["failed"] == 0,
                f"쓰기 정지 {w['paused_ms']:.0f}ms. 전후 요청 p50 {near['p50_ms']}ms (평소 {base['p50_ms']}ms), "
                f"최대 {near['max_ms']}ms, 실패 {near['failed']}건")
    demo.checks = rows
    good = all(r["ok"] for r in rows)
    demo.note(f"검증 {'통과' if good else '실패 있음'}: {sum(r['ok'] for r in rows)}/{len(rows)}", "good" if good else "bad")
    return {"rows": rows}


@app.get("/", response_class=HTMLResponse)
async def page():
    return (Path(__file__).parent / "console.html").read_text(encoding="utf-8")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=18888, log_level="warning")
