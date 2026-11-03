"""What happens when a process or a Redis really dies in the middle of a move (run on the host).

  python demo/recovery_check.py

Starts from a fresh stack for every case, without the app: one writer through the proxy, whose
acknowledged keys are the ground truth. statemig dies at every step of the failure rollback through
demo/crashhook.py. Prints PASS or FAIL per check.
"""
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import httpx
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

COMPOSE_DIR = Path(__file__).parent
FILES = ["-f", "docker-compose.yml", "-f", "crash.compose.yml"]
SERVICES = ["redis-aws", "redis-gcp", "proxy", "syncworker", "statemig"]
AUTH = {"Authorization": "Bearer demo-admin-token"}
API = httpx.Client(base_url="http://localhost:18090", headers=AUTH, timeout=60)
PROXY = httpx.Client(base_url="http://localhost:17000", headers=AUTH, timeout=3)
SYNC = httpx.Client(base_url="http://localhost:17100", headers=AUTH, timeout=3)
PORT = {"aws": 16379, "gcp": 26379}
KEYS = 1000
results: list[bool] = []


def report(name: str, ok: bool, detail=""):
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)


def compose(*args):
    subprocess.run(["docker", "compose", *FILES, *args], cwd=COMPOSE_DIR, capture_output=True, check=True)


def container_of(service: str) -> str:
    out = subprocess.run(["docker", "compose", *FILES, "ps", "-a", "-q", service], cwd=COMPOSE_DIR,
                         capture_output=True, text=True, check=True).stdout.strip()
    return out


def start(service: str):
    """`docker start`: `compose start` would also bring back the dependencies that are meant to stay dead."""
    subprocess.run(["docker", "start", container_of(service)], capture_output=True, check=True)


def until(fn, timeout=20.0, what="condition"):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            if v := fn():
                return v
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(0.1)
    raise TimeoutError(f"{what} ({last})")


def maybe(fn, default=None):
    try:
        return fn()
    except Exception:  # noqa: BLE001
        return default


def app_client(timeout=2.0):
    return redis.Redis(host="localhost", port=16380, username="app", password="app-proxy-pw", decode_responses=True,
                       socket_timeout=timeout, socket_connect_timeout=timeout, retry=Retry(NoBackoff(), 0))


def admin(side: str):
    return redis.Redis(host="localhost", port=PORT[side], username="default", password="adminpw",
                       decode_responses=True, socket_timeout=5)


def count(side: str) -> int:
    return sum(1 for _ in admin(side).scan_iter(match="session:base:*", count=1000))


def status() -> dict:
    return API.get("/internal/status", timeout=10).json()


def proxy_status() -> dict:
    return PROXY.get("/status").json()["proxy"]


def hook() -> dict:
    return API.get("/__hook/state").json()


def api_up() -> bool:
    return maybe(lambda: API.get("/internal/status", timeout=2).status_code == 200, False)


def wait_phase(*phases, timeout=30.0) -> dict:
    end = time.time() + timeout
    st = {}
    while time.time() < end:
        st = maybe(status, st)
        if st.get("phase") in phases:
            return st
        time.sleep(0.1)
    return st


class Writer(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.acked: list[tuple[str, float]] = []
        self.halt = threading.Event()
        self.last_error = ""

    def run(self):
        c, seq = app_client(), 0
        while not self.halt.is_set():
            seq += 1
            try:
                c.set(f"session:w:{seq}", seq)
                self.acked.append((f"session:w:{seq}", time.time()))
            except Exception as e:  # noqa: BLE001
                self.last_error = f"{type(e).__name__}: {e}"[:100]
                time.sleep(0.05)
            time.sleep(0.01)

    def acks_since(self, t: float) -> int:
        return sum(1 for _, at in self.acked if at >= t)

    def stop(self):
        self.halt.set()
        self.join(5)


def fresh() -> Writer:
    compose("down", "-v", "-t", "0", "--remove-orphans")
    compose("up", "-d", *SERVICES)
    until(lambda: api_up() and app_client().ping(), 60, "stack up")
    for s in ("q:join", "q:notify"):    # what the workers create at startup; they are not in this stack
        admin("aws").xgroup_create(s, "workers", id="0", mkstream=True)
    pipe = app_client(10).pipeline(transaction=False)
    for i in range(KEYS):
        pipe.set(f"session:base:{i}", "v")
    pipe.execute()
    pipe = None
    app_client().set("cache:seed", "x")
    w = Writer()
    w.start()
    return w


def to_ready():
    API.post("/internal/prepare", json={"target": "gcp"})
    assert wait_phase("ready")["phase"] == "ready"


def to_executed():
    to_ready()
    API.post("/internal/execute")
    assert wait_phase("executed")["phase"] == "executed"
    until(lambda: count("aws") == KEYS, 10, "old side still has the seed")


def missing(side: str, keys: list[str]) -> list[str]:
    c, out = admin(side), []
    for i in range(0, len(keys), 500):
        part = keys[i:i + 500]
        out += [k for k, v in zip(part, c.mget(part)) if v is None]
    return out


def flowing(w: Writer, seconds=2.0) -> int:
    t = time.time()
    time.sleep(seconds)
    return w.acks_since(t)


# ---- a Redis comes back empty -------------------------------------------------------------
def target_back_empty():
    w = fresh()
    to_executed()
    compose("kill", "redis-gcp")
    time.sleep(1)
    start("redis-gcp")
    until(lambda: admin("gcp").ping(), 15, "gcp back")
    time.sleep(4)
    kept = count("aws")
    view = status()["sync"]
    r = API.post("/migrations/current/rollback", json={"mode": "failure"})
    body = r.json()
    time.sleep(1)
    w.stop()
    report("대상이 빈 채로 재시작 → 원본 유지, 동기화 중단", kept == KEYS and bool(view["halted"]),
           f"aws 키 {kept}/{KEYS}, {view['halted']}")
    report("이어서 장애 롤백: 데이터 유지, 유실 범위는 모름으로 보고",
           r.status_code == 200 and count("aws") == KEYS and "unknown" in str(body.get("reverse_backlog_at_failure")),
           body.get("reverse_backlog_at_failure"))


def source_back_empty():
    w = fresh()
    to_ready()
    until(lambda: count("gcp") == KEYS, 10, "gcp has the seed")
    compose("kill", "redis-aws")
    time.sleep(1)
    start("redis-aws")
    until(lambda: admin("aws").ping(), 15, "aws back")
    time.sleep(4)
    kept, st = count("gcp"), status()
    w.stop()
    r = API.post("/internal/execute")
    time.sleep(1)
    report("동기화 중 원본이 빈 채로 재시작 → 대상 유지, 컷오버 거부",
           kept == KEYS and st["phase"] != "ready" and bool(st["sync"]["halted"])
           and status()["operation"]["state"] == "failed" and proxy_status()["active"] == "aws",
           f"gcp 키 {kept}/{KEYS}, phase={st['phase']}, {r.status_code} {status()['operation']['error']}")
    r = API.post("/internal/prepare", json={"target": "gcp"})
    report("중단된 동기화는 prepare로 다시 시작할 수 있음", r.status_code == 202, r.status_code)


# ---- the proxy dies inside a switch ---------------------------------------------------------
def proxy_dies(point: str, down_s: float, want_active: str):
    w = fresh()
    to_ready()
    API.post("/__hook/arm", json={"points": {point: "hold"}})
    API.post("/internal/execute")
    until(lambda: hook()["held"], 10, "held")
    compose("kill", "proxy")
    API.post("/__hook/release")
    time.sleep(down_s)
    start("proxy")
    until(proxy_status, 40, "proxy back")
    until(lambda: not hook()["switching"] and not proxy_status()["paused"], 30, "converged")
    time.sleep(STALE_WAIT)
    px, rec = proxy_status(), hook()["record"]
    acks = flowing(w)
    w.stop()
    time.sleep(0.5)
    lost = missing(px["active"], [k for k, _ in w.acked])
    report(f"프록시가 {point}에서 {down_s}초 죽음 → {want_active}로 수렴",
           px["gen"] == rec["gen"] and px["active"] == rec["active"] == want_active and not px["paused"]
           and acks > 0 and not lost, f"gen {px['gen']}/{rec['gen']}, {px['active']}, 쓰기 {acks}건, 유실 {len(lost)}건")


STALE_WAIT = 4


def proxy_restarts_target_dead():
    w = fresh()
    to_executed()
    compose("kill", "redis-gcp")
    compose("kill", "proxy")
    start("proxy")
    up = maybe(lambda: until(proxy_status, 15, "proxy back"), False)
    r = API.post("/internal/rollback", json={"mode": "failure"})
    st = wait_phase("rolled_back")
    acks = flowing(w)
    w.stop()
    report("활성 Redis가 죽은 채 프록시 재시작 → 뜨고, 장애 롤백 가능",
           bool(up) and r.status_code == 202 and st.get("phase") == "rolled_back" and acks > 0,
           f"phase={st.get('phase')}, 쓰기 {acks}건, {w.last_error if not acks else ''}")


# ---- the Sync Worker restarts ---------------------------------------------------------------
def sync_worker_restarts_syncing():
    w = fresh()
    to_ready()
    compose("kill", "syncworker")
    start("syncworker")
    st = until(lambda: (lambda s: s if s["phase"] == "ready" else None)(status()), 30, "ready again")
    r = API.post("/internal/execute")
    done = wait_phase("executed")
    w.stop()
    report("동기화 중 Sync Worker 재시작 → 저절로 ready, 컷오버 가능",
           st["sync"]["running"] and r.status_code == 202 and done.get("phase") == "executed", done.get("phase"))


def sync_worker_restarts_executed():
    w = fresh()
    to_executed()
    compose("kill", "syncworker")
    time.sleep(1)
    start("syncworker")
    until(lambda: status()["sync"]["running"], 20, "reverse sync back")
    pipe = app_client(10).pipeline(transaction=False)
    for i in range(500):
        pipe.set(f"session:late:{i}", "x")
    pipe.execute()
    late = [f"session:late:{i}" for i in range(500)]
    arrived = maybe(lambda: until(lambda: not missing("aws", late), 15, "late keys"), False)
    until(lambda: maybe(lambda: SYNC.get("/status").json()["sync"]["compare_valid"]), 20, "compare")
    r = API.post("/internal/rollback", json={"mode": "normal"})
    st = wait_phase("rolled_back")
    w.stop()
    report("컷오버 후 Sync Worker 재시작 → 역방향 재개, 정상 롤백 가능",
           bool(arrived) and st.get("phase") == "rolled_back" and proxy_status()["active"] == "aws",
           f"재개 뒤 쓰기 도착={bool(arrived)}, phase={st.get('phase')}, {(st.get('operation') or {}).get('error')}")


def sync_worker_hung():
    w = fresh()
    to_executed()
    compose("pause", "syncworker")
    compose("kill", "redis-gcp")
    t0 = time.time()
    r = API.post("/migrations/current/rollback", json={"mode": "failure"})
    ms = (time.time() - t0) * 1000
    body = r.json()
    compose("unpause", "syncworker")
    w.stop()
    report("Sync Worker가 멈춘 채 장애 롤백 → 기다리지 않고 진행, 유실 범위는 모름",
           r.status_code == 200 and ms < 3000 and "unknown" in str(body.get("reverse_backlog_at_failure")),
           f"{ms:.0f}ms, {body.get('reverse_backlog_at_failure')}")


# ---- the write-pause budget -----------------------------------------------------------------
def budget_refused_without_pause():
    w = fresh()
    to_ready()
    for attempt in range(10):   # a burst leaves changed keys until the next rolling compare takes them
        until(lambda: SYNC.get("/status").json()["sync"]["compare_rate"] > 0, 10, "compare rate")
        pipe = app_client(10).pipeline(transaction=False)
        for i in range(3000):
            pipe.set(f"session:burst:{attempt}:{i}", "x")
        pipe.execute()
        paused_before = proxy_status()["last_pause_ms"]   # an earlier try may have paused and given up
        r = API.post("/migrations/current/cutover", json={"budget_ms": 1})
        if "not started" in r.text or r.status_code == 200:
            break
        time.sleep(1)
    px = proxy_status()
    acks = flowing(w)
    w.stop()
    report("검증이 예산을 넘을 게 뻔함 → 쓰기를 멈추지 않고 거부",
           r.status_code == 409 and "not started" in r.text and px["active"] == "aws" and px["last_pause_ms"] == paused_before
           and acks > 0, f"{r.status_code}, {r.json().get('detail')}, 이번 시도의 쓰기 정지 없음={px['last_pause_ms'] == paused_before}")


def budget_exceeded_in_pause():
    w = fresh()
    to_ready()
    budget = 500
    # #1 is the key count before the pause, #2 the verify inside it
    API.post("/__hook/arm", json={"points": {"before:sync:/verify#2": "hold"}})
    API.post("/internal/execute", json={"budget_ms": budget})
    until(lambda: hook()["held"], 10, "held")
    time.sleep(budget / 1000 + 0.2)     # the budget runs out while writes are paused
    API.post("/__hook/release")
    st = until(lambda: (lambda s: s if s["operation"]["state"] != "running" else None)(status()), 15, "finished")
    px, rec = proxy_status(), hook()["record"]
    acks = flowing(w)
    w.stop()
    time.sleep(0.5)
    lost = missing("aws", [k for k, _ in w.acked])
    report("쓰기 정지 중 예산 초과 → 전환 없이 취소, 원본에서 재개",
           st["operation"]["state"] == "failed" and px["active"] == rec["active"] == "aws" and px["gen"] == 0
           and not px["paused"] and acks > 0 and not lost,
           f"{st['operation']['error']}, 쓰기 {acks}건, 유실 {len(lost)}건")


# ---- giving up before a cutover -------------------------------------------------------------
def abort_before_cutover():
    w = fresh()
    to_ready()
    r = API.post("/internal/abort")
    st = wait_phase("aborted")
    running = SYNC.get("/status").json()["running"]
    acks = flowing(w)
    px = proxy_status()
    refused = API.post("/internal/execute")
    time.sleep(0.5)
    op = status()["operation"]
    report("컷오버 전 취소 → 동기화 중지, 앱은 원본 유지, 컷오버 거부",
           r.status_code == 202 and st.get("phase") == "aborted" and not running and px["active"] == "aws"
           and not px["paused"] and acks > 0 and op["name"] == "execute" and op["state"] == "failed",
           f"phase={st.get('phase')}, 쓰기 {acks}건, {refused.status_code} {op['error']}")
    API.post("/internal/prepare", json={"target": "gcp"})
    again = wait_phase("ready")
    API.post("/internal/execute")
    done = wait_phase("executed")
    w.stop()
    report("취소 뒤 prepare → ready → 컷오버 가능", again.get("phase") == "ready" and done.get("phase") == "executed",
           done.get("phase"))
    API.post("/internal/abort")
    time.sleep(0.5)
    st = status()
    report("컷오버 뒤에는 취소 거부", st["operation"]["state"] == "failed" and st["phase"] == "executed"
           and proxy_status()["active"] == "gcp", st["operation"]["error"])


def prepare_times_out():
    w = fresh()
    API.post("/internal/prepare", json={"target": "gcp", "timeout_s": 1})     # ready needs 3s of keeping up
    st = wait_phase("aborted", timeout=15)
    acks = flowing(w)
    w.stop()
    report("제한 시간 안에 ready가 안 됨 → 이전 취소, 이유 표시",
           st.get("phase") == "aborted" and "not ready in time" in str(st.get("abort_reason"))
           and proxy_status()["active"] == "aws" and acks > 0, st.get("abort_reason"))


def statemig_dies_in_abort():
    w = fresh()
    to_ready()
    API.post("/__hook/arm", json={"points": {"before:sync:/stop#1": "crash"}})
    API.post("/internal/abort")
    until(lambda: not api_up(), 15, "statemig died")
    start("statemig")
    until(api_up, 30, "statemig back")
    back = wait_phase("ready", timeout=20)
    API.post("/internal/abort")
    st = wait_phase("aborted")
    w.stop()
    report("취소 중 제어부 사망 → 이전은 그대로, 다시 취소 가능",
           back.get("phase") == "ready" and st.get("phase") == "aborted", f"{back.get('phase')} → {st.get('phase')}")


# ---- the common API across a restart --------------------------------------------------------
def adapter_dies_during_common_command():
    w = fresh()
    plan = {"workflowId": "wf", "resourceId": "redis", "kind": "REDIS", "profile": "redis-proxy-sync-v1",
            "sourceRef": "resource:aws", "targetRef": "resource:gcp", "bindingRef": "binding:redis-proxy"}
    tx = {"transitionId": "tx-1", "quiesceRef": "q:1"}
    key = lambda: {"Idempotency-Key": uuid.uuid4().hex}  # noqa: E731
    mid = API.post("/v1/migrations", headers=key(), json=plan).json()["migrationId"]
    view = lambda: API.get(f"/v1/migrations/{mid}", timeout=10).json()  # noqa: E731

    def act(action, **extra):
        r = API.post(f"/v1/migrations/{mid}/actions", headers=key(),
                     json={"action": action, "expectedRevision": view()["revision"], **extra})
        return r

    def done(r):
        assert r.status_code == 202, r.text
        return until(lambda: (lambda o: o if o["status"] not in ("QUEUED", "RUNNING") else None)(
            API.get(r.json()["statusUrl"]).json()), 60, "operation")

    done(act("PREPARE"))
    done(act("START_SYNC"))
    until(lambda: view()["lifecycle"] == "READY", 30, "ready")
    done(act("QUIESCE", **tx))
    API.post("/__hook/arm", json={"points": {"after:proxy:/switch#1": "crash"}})
    op_id = act("CUTOVER", **tx).json()["operationId"]
    until(lambda: not api_up(), 15, "adapter died")
    start("statemig")
    until(api_up, 30, "adapter back")
    op = API.get(f"/v1/operations/{op_id}").json()
    st = until(lambda: (lambda s: s if s["observed"]["route"] == "TARGET" and s["observed"]["businessWriteAccess"] == "TARGET_ONLY" else None)(view()),
               20, "converged")
    refused = API.post(f"/v1/migrations/{mid}/actions", headers=key(),
                       json={"action": "RESUME", "expectedRevision": st["revision"], **tx, "expectedLocation": "TARGET"})
    report("공통 API 명령 중 어댑터 사망 → UNKNOWN, RECONCILE 전 변경 거부",
           op["status"] == "UNKNOWN" and st["health"] == "UNKNOWN" and st["allowedActions"] == ["RECONCILE"]
           and refused.status_code == 409 and refused.json()["code"] == "UNKNOWN_STATE",
           f"{op['status']}, 허용 {st['allowedActions']}, {refused.json().get('code')}")
    rec = done(act("RECONCILE"))
    st = view()
    resumed = done(act("RESUME", **tx, expectedLocation="TARGET"))
    w.stop()
    report("RECONCILE 뒤 실제 상태로 복귀, 다음 명령 가능",
           rec["status"] == "SUCCEEDED" and st["lifecycle"] == "SWITCHED" and st["health"] == "OK"
           and resumed["status"] == "SUCCEEDED" and view()["lifecycle"] == "ACTIVE",
           f"{st['lifecycle']} → {view()['lifecycle']}")


# ---- statemig dies inside a failure rollback ---------------------------------------------------
def failure_steps() -> list[str]:
    w = fresh()
    to_executed()
    compose("kill", "redis-gcp")
    API.post("/__hook/arm", json={"points": {}})
    API.post("/internal/rollback", json={"mode": "failure"})
    assert wait_phase("rolled_back")["phase"] == "rolled_back"
    time.sleep(0.5)
    w.stop()
    trace = hook()["trace"]
    return [p for i, p in enumerate(trace) if i == 0 or p.startswith("after:")]


def statemig_dies_in_failure_rollback(point: str):
    w = fresh()
    to_executed()
    compose("kill", "redis-gcp")
    API.post("/__hook/arm", json={"points": {point: "crash"}})
    API.post("/internal/rollback", json={"mode": "failure"})
    until(lambda: not api_up(), 15, "statemig died")
    start("statemig")
    until(api_up, 30, "statemig back")
    st = wait_phase("rolled_back", timeout=15)
    px = maybe(proxy_status, {})
    acks = flowing(w)
    w.stop()
    time.sleep(0.5)
    report(f"장애 롤백 중 제어부 사망 @ {point} → 저절로 완료",
           st.get("phase") == "rolled_back" and px.get("active") == "aws" and not px.get("paused") and acks > 0
           and count("aws") == KEYS and not admin("aws").exists("cache:seed"),
           f"phase={st.get('phase')}, proxy={px.get('active')}, 쓰기 {acks}건, aws 키 {count('aws')}/{KEYS}")


def safe(fn, *args):
    try:
        fn(*args)
    except Exception as e:  # noqa: BLE001
        report(f"{fn.__name__}{args}", False, f"{type(e).__name__}: {e}"[:300])
        subprocess.run(["docker", "compose", *FILES, "unpause", "syncworker"], cwd=COMPOSE_DIR, capture_output=True)


def main():
    print("fresh stack...", flush=True)
    compose("up", "-d", "--build", *SERVICES)
    safe(target_back_empty)
    safe(source_back_empty)
    safe(proxy_dies, "before:proxy:/resume#1", 1, "gcp")
    safe(proxy_dies, "before:proxy:/resume#1", 12, "gcp")
    safe(proxy_dies, "before:proxy:/switch#1", 12, "aws")
    safe(proxy_dies, "before:sync:/reconcile#1", 1, "gcp")
    safe(proxy_restarts_target_dead)
    safe(sync_worker_restarts_syncing)
    safe(sync_worker_restarts_executed)
    safe(sync_worker_hung)
    safe(budget_refused_without_pause)
    safe(budget_exceeded_in_pause)
    safe(abort_before_cutover)
    safe(prepare_times_out)
    safe(statemig_dies_in_abort)
    safe(adapter_dies_during_common_command)
    for point in failure_steps():
        safe(statemig_dies_in_failure_rollback, point)
    compose("down", "-v", "-t", "0", "--remove-orphans")
    print(f"\n{sum(results)}/{len(results)} passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
