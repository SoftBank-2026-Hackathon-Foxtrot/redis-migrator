"""Keys in several dbs keep their db number through a move and back (run on the host).

  python demo/multidb_check.py

Fresh stack without the app. Seeds db 0, 1, 2 and 3 through the proxy, changes keys while the sync
runs (db 5 is used for the first time then), cuts over to gcp, writes there, rolls back normally,
and compares both sides db by db after each step. Also checks that a delete held during a pause
makes only its own db's cache key a miss.
"""
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import redis

COMPOSE_DIR = Path(__file__).parent
SERVICES = ["redis-aws", "redis-gcp", "proxy", "syncworker", "statemig"]
AUTH = {"Authorization": "Bearer demo-admin-token"}
API = httpx.Client(base_url="http://localhost:18090", headers=AUTH, timeout=60)
PROXY = httpx.Client(base_url="http://localhost:17000", headers=AUTH, timeout=5)
PORT = {"aws": 16379, "gcp": 26379}
DBS = [0, 1, 2, 3, 5]
results: list[bool] = []


def report(name: str, ok: bool, detail=""):
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=COMPOSE_DIR, capture_output=True, check=True)


def app(db: int) -> redis.Redis:
    return redis.Redis(host="localhost", port=16380, username="app", password="app-proxy-pw", db=db,
                       decode_responses=True, socket_timeout=10)


def admin(side: str, db: int) -> redis.Redis:
    return redis.Redis(host="localhost", port=PORT[side], username="default", password="adminpw", db=db,
                       socket_timeout=10)


def until(fn, timeout=30.0, what="condition"):
    end, last = time.time() + timeout, None
    while time.time() < end:
        try:
            if v := fn():
                return v
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(0.2)
    raise TimeoutError(f"{what} ({last})")


def phase() -> str:
    return API.get("/internal/status").json()["phase"]


def snapshot(side: str, db: int) -> dict[bytes, bytes]:
    """Every migrated key of one db with its DUMP payload (same engine on both sides)."""
    c = admin(side, db)
    out = {}
    for k in c.scan_iter(count=1000):
        if k.startswith((b"session:", b"cache:", b"q:delayed")):
            out[k] = c.dump(k)
    return out


def same_everywhere(label: str):
    diff = {}
    for db in DBS:
        a, g = snapshot("aws", db), snapshot("gcp", db)
        if a != g:
            diff[db] = {"only_aws": len(a.keys() - g.keys()), "only_gcp": len(g.keys() - a.keys()),
                        "value": sum(1 for k in a.keys() & g.keys() if a[k] != g[k])}
    counts = {db: len(snapshot("aws", db)) for db in DBS}
    report(f"{label}: aws와 gcp가 db마다 같음", not diff, diff or counts)


def pending_cache_is_per_db():
    app(1).set("cache:shared", "in-db1")
    app(2).set("cache:shared", "in-db2")
    PROXY.post("/pause", json={"max_wait_ms": 1000})
    held = threading.Thread(target=lambda: app(1).delete("cache:shared"))
    try:
        held.start()
        time.sleep(0.3)        # the DEL is now waiting at the gate
        db1, db2 = app(1).get("cache:shared"), app(2).get("cache:shared")
    finally:
        PROXY.post("/resume")
        held.join(5)
    after1, after2 = app(1).get("cache:shared"), app(2).get("cache:shared")
    report("PAUSE 중 삭제 대기: 같은 db의 키만 miss, 다른 db의 같은 이름은 응답",
           db1 is None and db2 == "in-db2" and after1 is None and after2 == "in-db2",
           f"db1={db1}, db2={db2}, RESUME 뒤 db1={after1}, db2={after2}")


def main():
    print("fresh stack...", flush=True)
    compose("down", "-v", "-t", "0", "--remove-orphans")
    compose("up", "-d", "--build", *SERVICES)
    until(lambda: API.get("/internal/status", timeout=2).status_code == 200 and app(0).ping(), 90, "stack up")
    for s in ("q:join", "q:notify"):    # what the workers create at startup; they are not in this stack
        admin("aws", 0).xgroup_create(s, "workers", id="0", mkstream=True)

    for db in (0, 1, 3):
        p = app(db).pipeline(transaction=False)
        for i in range(300):
            p.set(f"session:d{db}:{i}", f"v{db}-{i}", ex=1800)
        p.execute()
    app(2).set("cache:groupbuy:1", "detail", ex=300)
    app(3).zadd("q:delayed", {"job-a": 1, "job-b": 2})
    pending_cache_is_per_db()

    API.post("/internal/prepare", json={"target": "gcp"})
    until(lambda: phase() == "ready", 60, "ready")
    app(1).set("session:d1:0", "changed during sync")
    app(0).delete("session:d0:1")
    app(3).set("session:d3:new", "added during sync")
    app(5).set("session:d5:first", "a db used for the first time during sync")
    until(lambda: admin("gcp", 5).get("session:d5:first") is not None, 10, "db 5 reaches gcp")
    report("동기화 중 처음 쓴 db 5도 대상에 복사", True)
    until(lambda: phase() == "ready", 60, "ready again")

    API.post("/internal/execute")
    st = until(lambda: (s := API.get("/internal/status").json())["phase"] in ("executed", "failed", "ready") and s, 60,
               "cutover")
    report("컷오버 성공", st["phase"] == "executed", st.get("operation"))
    px = PROXY.get("/status").json()["proxy"]
    report("프록시가 gcp로 전환", px["active"] == "gcp", px["active"])
    same_everywhere("컷오버 직후")
    report("동기화 중 삭제한 db 0 키는 대상에도 없음", admin("gcp", 0).get("session:d0:1") is None)
    report("db 번호 유지: db 1 값은 gcp db 1에만", admin("gcp", 1).get("session:d1:0") == b"changed during sync"
           and admin("gcp", 0).get("session:d1:0") is None)

    app(1).set("session:d1:after", "written on gcp")
    until(lambda: admin("aws", 1).get("session:d1:after") is not None, 10, "reverse sync to aws db 1")
    report("전환 후 db 1 쓰기가 역방향으로 aws db 1에 도착", True)

    API.post("/internal/rollback", json={"mode": "normal"})
    st = until(lambda: (s := API.get("/internal/status").json())["phase"] in ("rolled_back", "failed", "executed")
               and s["operation"]["state"] != "running" and s, 60, "rollback")
    report("정상 롤백 성공", st["phase"] == "rolled_back", st.get("operation"))
    report("프록시가 aws로 복귀", PROXY.get("/status").json()["proxy"]["active"] == "aws")
    same_everywhere("롤백 직후")
    report("롤백 후 앱이 db 1 값을 그대로 읽음", app(1).get("session:d1:after") == "written on gcp")

    failed = results.count(False)
    print(f"\n{len(results) - failed}/{len(results)} passed", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
