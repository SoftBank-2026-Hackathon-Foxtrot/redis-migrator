"""Failure paths of the design, each caused on purpose under load (run on the host).

  python demo/faults.py

Starts from a fresh stack. Every case prints PASS or FAIL with what it saw.
"""
import asyncio
import json
import subprocess
import sys
import time

import httpx
import redis.asyncio as aioredis
from redis.exceptions import AuthenticationError, RedisError

from scenario import (ADMIN, ADMIN_HEADERS, APP, COMPOSE_DIR, COMPOSE_FILES, CTL, GROUPBUYS, Load, app_can_write,
                      invariants, wait_processed)

PROXY_ADMIN, SYNC_ADMIN = "http://localhost:17000", "http://localhost:17100"
PROXY_URL = "redis://{}localhost:16380"
results: list[tuple[str, bool, str]] = []


def compose(*args, check=True) -> str:
    p = subprocess.run(["docker", "compose", *COMPOSE_FILES, *args], cwd=COMPOSE_DIR, capture_output=True, text=True,
                       check=check)
    return p.stdout


def report(name: str, ok: bool, detail=""):
    results.append((name, ok, str(detail)))
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)


def admin(side: str):
    return aioredis.from_url(ADMIN[side], decode_responses=True)


async def acl(side: str, *rules):
    """Changes what the control plane's user may do, through a separate test account."""
    url = ADMIN[side].replace("default:adminpw", "faults:faultspw")
    c = aioredis.from_url(url, decode_responses=True)
    try:
        await c.execute_command("ACL", "SETUSER", "default", *rules)
    finally:
        await c.aclose()


async def add_test_account():
    for side in ADMIN:
        c = admin(side)
        await c.execute_command("ACL", "SETUSER", "faults", "on", ">faultspw", "~*", "&*", "+@all")
        await c.aclose()


class Ctl:
    def __init__(self):
        self.h = httpx.AsyncClient(base_url=CTL, timeout=60, headers=ADMIN_HEADERS)

    async def status(self) -> dict:
        return (await self.h.get("/status")).json()

    async def migration(self) -> dict:
        return (await self.status())["migration"]

    async def post(self, path: str, body: dict | None = None) -> httpx.Response:
        return await self.h.post(path, json=body or {})

    async def wait(self, pred, what: str, timeout=30):
        t0 = time.time()
        st = None
        while time.time() - t0 < timeout:
            try:
                st = await self.status()
                if pred(st):
                    return st
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.2)
        raise SystemExit(f"timed out waiting for {what}: {st}")

    async def sync_ready(self, target="gcp", **body):
        r = await self.post("/migrations", {"target": target, **body})
        if r.status_code >= 400:
            raise SystemExit(f"sync start failed: {r.text}")
        t0 = time.time()
        while not (await self.h.get("/migrations/current/ready")).json()["ready"]:
            if time.time() - t0 > 60:
                raise SystemExit("sync never became ready")
            await asyncio.sleep(0.2)

    async def drained(self):
        await self.wait(lambda s: s["migration"]["state"] == "CUTOVER", "drain")

    async def back_to_aws(self):
        """Normal rollback, so the next case starts with aws active."""
        r = await self.post("/migrations/current/rollback", {"mode": "normal"})
        if r.status_code >= 400:
            raise SystemExit(f"rollback failed: {r.text}")
        await self.wait(lambda s: s["migration"]["state"] == "ROLLED_BACK", "rollback drain")


async def case_reverse_subscribe_fails(ctl: Ctl, load: Load):
    before = load.failed
    await ctl.sync_ready()
    await acl("gcp", "-psubscribe")
    t0 = time.time()
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 1500})
    took = time.time() - t0
    await acl("gcp", "+psubscribe")
    st = await ctl.status()
    px, m = st["proxy"], st["migration"]
    await asyncio.sleep(2)
    ok = (r.status_code == 409 and px["active"] == "aws" and px["consume"] == "aws" and not px["paused"]
          and m["state"] == "ABORTED" and load.failed == before and await app_can_write("aws") == "write allowed")
    report("역방향 구독 실패 → 전환 되돌리기", ok,
           f"{took:.1f}s 뒤 active={px['active']} paused={px['paused']} state={m['state']} "
           f"gen={px['gen']} 실패={load.failed - before}")


async def case_fence_fails(ctl: Ctl, load: Load):
    before = load.failed
    await ctl.wait(lambda s: not s["sync"].get("running"), "previous sync stopped")
    await ctl.sync_ready()
    await acl("aws", "-acl")
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    res = r.json()
    px = (await ctl.status())["proxy"]
    write_before = await app_can_write("aws")
    await asyncio.sleep(1)
    await acl("aws", "+acl")
    await asyncio.sleep(2.5)
    write_after = await app_can_write("aws")
    fence_now = (await ctl.migration())["last_cutover"]["fence"]
    ok = (r.status_code == 200 and res["fence"] == "isolated (retrying)" and px["active"] == "gcp" and not px["paused"]
          and write_before == "write allowed" and write_after == "NOPERM" and fence_now == "acl"
          and load.failed == before)
    report("원본 권한 회수 실패 → 격리로 진행, 나중에 회수", ok,
           f"fence={res.get('fence')} active={px['active']} 복구 전 aws 쓰기={write_before}, 복구 뒤={write_after}, "
           f"실패={load.failed - before}")
    await ctl.drained()
    await ctl.back_to_aws()


async def case_group_missing(ctl: Ctl, load: Load):
    before = load.failed
    await ctl.sync_ready()
    c = admin("gcp")
    await c.xgroup_destroy("q:join", "workers")
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    st = await ctl.status()
    await c.xgroup_create("q:join", "workers", id="0", mkstream=True)
    await c.aclose()
    aborted = (r.status_code == 409 and "queue group" in r.text and st["proxy"]["active"] == "aws"
               and not st["proxy"]["paused"] and st["migration"]["state"] == "SYNCING")
    r2 = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    report("VERIFY: 대상 group 없음 → 취소, 복구 뒤 재시도 성공", aborted and r2.status_code == 200
           and load.failed == before, f"1차 {r.status_code}, 2차 {r2.status_code}, 실패={load.failed - before}")
    await ctl.drained()
    await ctl.back_to_aws()


async def case_budget_too_small(ctl: Ctl):
    r = await ctl.post("/migrations", {"target": "gcp", "budget_ms": 0})
    m = await ctl.migration()
    report("사전 확인: 예상 시간 > 예산 → 시작 안 함", r.status_code == 409 and "estimated" in r.text
           and m["state"] == "FAILED", m["precheck"].get("estimate"))


async def case_cache_count(ctl: Ctl):
    await ctl.sync_ready()
    c = admin("gcp")
    stale = [f"cache:stale:{i}" for i in range(30)]
    for k in stale:
        await c.set(k, "old")
    async with httpx.AsyncClient(base_url=SYNC_ADMIN, timeout=30, headers=ADMIN_HEADERS) as sw:
        v = (await sw.post("/verify", json={"sample": {"cache:": 200}})).json()
    await c.delete(*stale)
    await c.aclose()
    report("VERIFY: 대상에만 남은 캐시 → 키 수 불일치", not v["ok"] and v["prefixes"]["cache:"]["count_diff"] >= 30,
           v["prefixes"]["cache:"].get("count_diff"))


async def case_restart_syncing(ctl: Ctl, load: Load):
    before = load.failed
    compose("restart", "statemig")
    st = await ctl.wait(lambda s: s["migration"] and s["migration"]["state"] == "SYNCING", "state after restart")
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    report("제어부 재시작(동기화 중) → 이어서 컷오버", st["proxy"]["active"] == "aws" and r.status_code == 200
           and load.failed == before, f"cutover {r.status_code}, 실패={load.failed - before}")
    await ctl.drained()
    await ctl.back_to_aws()


FAKE_CRASH = """
import json
rec = json.load(open('/state/control.json'))
{record_change}
json.dump(rec, open('/state/control.json', 'w'))
m = json.load(open('/state/migration.json'))
m['switching'] = {{'old': 'aws', 'new': 'gcp', 'kind': 'cutover'}}
json.dump(m, open('/state/migration.json', 'w'))
"""


async def crash_mid_switch(ctl: Ctl, record_change: str):
    """Leave the files and the proxy as a crash during the switch would."""
    compose("exec", "-T", "statemig", "python", "-c", FAKE_CRASH.format(record_change=record_change))
    async with httpx.AsyncClient(base_url=PROXY_ADMIN, timeout=30, headers=ADMIN_HEADERS) as px:
        await px.post("/pause", json={"max_wait_ms": 1000})
    compose("restart", "statemig")


async def case_crash_before_record(ctl: Ctl, load: Load):
    before = load.failed
    await ctl.sync_ready()
    await crash_mid_switch(ctl, "rec['paused'] = True")
    st = await ctl.wait(lambda s: s["migration"] and not s["proxy"]["paused"], "resume after restart")
    await asyncio.sleep(2)
    px, m = st["proxy"], st["migration"]
    report("전환 기록 전 크래시 → 취소하고 재개", px["active"] == "aws" and m["state"] == "SYNCING"
           and load.failed == before, f"active={px['active']} state={m['state']} 실패={load.failed - before}")


async def case_crash_after_record(ctl: Ctl, load: Load):
    before = load.failed
    gen = (await ctl.status())["record"]["gen"] + 1
    await crash_mid_switch(ctl, f"rec.update(gen={gen}, active='gcp', consume='aws', paused=True)")
    st = await ctl.wait(lambda s: s["migration"] and not s["proxy"]["paused"] and s["proxy"]["active"] == "gcp",
                        "converge after restart")
    await ctl.drained()
    await asyncio.sleep(2)
    fence = await app_can_write("aws")
    report("전환 기록 후 크래시 → 대상으로 수렴", st["proxy"]["gen"] == gen and fence == "NOPERM"
           and load.failed == before, f"gen={st['proxy']['gen']} aws 쓰기={fence} 실패={load.failed - before}")
    await ctl.back_to_aws()


async def case_managed_like(ctl: Ctl, load: Load):
    """CONFIG and ACL refused, as on managed Redis; notifications were set beforehand (IaC)."""
    before = load.failed
    # such a Redis was never fenced: undo what the earlier rollbacks left on the app user
    c = admin("gcp")
    await c.execute_command("ACL", "SETUSER", "app", "reset", "on", ">apppw", "~*", "&*", "+@all")
    await c.aclose()
    for side in ADMIN:
        await acl(side, "-config", "-acl")
    await ctl.sync_ready()
    m = await ctl.migration()
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    res = r.json()
    await ctl.drained()
    r2 = await ctl.post("/migrations/current/rollback", {"mode": "normal"})
    await ctl.wait(lambda s: s["migration"]["state"] == "ROLLED_BACK", "rollback drain")
    ok = (m["fence_mode"] == {"aws": "isolated", "gcp": "isolated"} and r.status_code == 200
          and res["fence"] == "isolated" and r2.status_code == 200 and load.failed == before)
    report("CONFIG·ACL 막힌 Redis → 알림 실측, 격리로 전환·롤백", ok,
           f"fence_mode={m['fence_mode']} 경고 {len(m['precheck']['warnings'])}건 실패={load.failed - before}")
    # notifications really off and not switchable: must not start
    for side in ADMIN:
        await acl(side, "+config")
    c = admin("gcp")
    await c.config_set("notify-keyspace-events", "")
    await c.aclose()
    await acl("gcp", "-config")
    r = await ctl.post("/migrations", {"target": "gcp"})
    report("알림이 안 오는 Redis → 시작 안 함", r.status_code == 409 and "notifications" in r.text, r.json().get("detail"))
    for side in ADMIN:
        await acl(side, "+config", "+acl")


async def case_full_mode(ctl: Ctl, load: Load):
    """The older mode stays available: everything is copied and compared inside the pause."""
    before = load.failed
    await ctl.sync_ready(mode="full")
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    await ctl.drained()
    await ctl.back_to_aws()
    report("A 방식(mode=full)도 동작", r.status_code == 200 and load.failed == before,
           f"쓰기 정지 {r.json().get('paused_ms')}ms, 실패={load.failed - before}")


async def case_delta_flow(ctl: Ctl, load: Load):
    before = load.failed
    await ctl.sync_ready()
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    await ctl.drained()
    fence = await app_can_write("aws")
    r2 = await ctl.post("/migrations/current/rollback", {"mode": "normal"})
    await ctl.wait(lambda s: s["migration"]["state"] == "ROLLED_BACK", "rollback drain")
    report("B: 컷오버 → 정상 롤백", r.status_code == 200 and r2.status_code == 200 and fence == "NOPERM"
           and load.failed == before, f"쓰기 정지 {r.json().get('paused_ms')}ms, {r2.json().get('paused_ms')}ms, "
           f"변경 키 {r.json().get('reconciled_keys')}개, 실패={load.failed - before}")


async def case_delta_subscription_drop(ctl: Ctl, load: Load):
    """Changes made while the subscription is down must still reach the target."""
    before = load.failed
    await ctl.sync_ready()
    c, g = admin("aws"), admin("gcp")
    await c.client_kill_filter(_type="pubsub")
    await c.set("session:written-during-drop", "1", ex=600)
    st = await ctl.wait(lambda s: s["sync"]["sync"]["resyncs"] >= 1 and s["sync"]["sync"]["compare_valid"],
                        "compare again after the drop")
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    reached = await g.get("session:written-during-drop")
    await c.aclose()
    await g.aclose()
    report("B: 구독 끊김 → 전체 비교 다시, 끊긴 동안의 쓰기도 도착", r.status_code == 200 and reached == "1"
           and load.failed == before, f"resyncs={st['sync']['sync']['resyncs']} 대상 값={reached} 실패={load.failed - before}")
    await ctl.drained()
    await ctl.back_to_aws()


async def case_delta_stale_keys(ctl: Ctl, load: Load):
    before = load.failed
    await ctl.sync_ready()
    g = admin("gcp")
    stale = [f"session:stale:{i}" for i in range(30)]
    for k in stale:
        await g.set(k, "old", ex=600)
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    px = (await ctl.status())["proxy"]
    await g.delete(*stale)
    await g.aclose()
    report("B: 대상에만 키가 생김 → 키 수 불일치로 취소", r.status_code == 409 and "verify failed" in r.text
           and px["active"] == "aws" and not px["paused"] and load.failed == before, f"{r.status_code}, active={px['active']}")
    await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    await ctl.drained()
    await ctl.back_to_aws()


async def case_delta_silent_drift(ctl: Ctl):
    """B's known blind spot: a target value changed without a notification on the source."""
    a, g = admin("aws"), admin("gcp")
    for i in range(1000):
        await a.set(f"session:drift:{i}", "right", ex=900)
    await ctl.sync_ready()
    await g.set("session:drift:7", "wrong", ex=900)
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    value = await g.get("session:drift:7")
    if r.status_code == 200:
        outcome = f"샘플에 안 걸려 전환됨, 대상 값 = {value}" if value == "wrong" else f"값 = {value}"
    else:
        outcome = "샘플에 걸려 취소됨"
    report("B의 약점 확인 (정보): 알림 없이 대상 값만 바뀜", True, outcome)
    await a.aclose()
    await g.aclose()


async def case_cache_during_pause(ctl: Ctl):
    """A commit during PAUSE: its DEL is held. No fill may land after it, no read may see the old value.
    Keys with no held write keep answering from Redis."""
    active = (await ctl.status())["proxy"]["active"]
    app_c = aioredis.from_url(PROXY_URL.format("app:app-proxy-pw@"), decode_responses=True)
    await app_c.set("cache:race:committed", "old", ex=60)
    await app_c.set("cache:race:other", "kept", ex=60)
    delete = None
    async with httpx.AsyncClient(base_url=PROXY_ADMIN, timeout=30, headers=ADMIN_HEADERS) as px:
        await px.post("/pause", json={"max_wait_ms": 1000})
        try:
            before = await asyncio.wait_for(app_c.get("cache:race:committed"), 2)
            delete = asyncio.create_task(app_c.delete("cache:race:committed"))
            read = before
            for _ in range(200):    # the DEL takes a moment to reach the gate
                read = await asyncio.wait_for(app_c.get("cache:race:committed"), 2)
                if read is None:
                    break
                await asyncio.sleep(0.01)
            other = await asyncio.wait_for(app_c.get("cache:race:other"), 2)
            await asyncio.wait_for(app_c.set("cache:race:committed", "stale", ex=60), 2)    # answered, not held
            await asyncio.wait_for(app_c.set("cache:race:other", "stale", ex=60), 2)
            del_held = not delete.done()
        finally:
            await px.post("/resume")
    await delete
    await asyncio.sleep(0.2)
    await app_c.aclose()
    c = admin(active)
    left, other_left = await c.get("cache:race:committed"), await c.get("cache:race:other")
    await c.delete("cache:race:other")
    await c.aclose()
    report("PAUSE 중 캐시: 삭제 대기 키만 미스, 나머지는 응답, 채우기는 버림",
           before == "old" and read is None and other == "kept" and del_held and left is None and other_left == "kept",
           f"대기 전={before}, 대기 중={read}, 다른 키={other}, 삭제 보류={del_held}, RESUME 뒤={left}/{other_left}")


async def case_poison_message(ctl: Ctl):
    active = (await ctl.status())["proxy"]["active"]
    c = admin(active)
    await c.xadd("q:join", {"bad": "no request_id"})
    t0 = time.time()
    dead = 0
    while time.time() - t0 < 60 and not dead:
        dead = await c.xlen("q:join:dead")
        await asyncio.sleep(0.5)
    pending = 0
    for g in await c.xinfo_groups("q:join"):
        if g["name"] == "workers":
            pending = g["pending"]
    await c.aclose()
    report("처리 실패 메시지 → q:join:dead, pending 안 남음", dead >= 1, f"{time.time() - t0:.0f}s, dead={dead}, pending={pending}")


async def case_delayed_rebuilt(ctl: Ctl):
    active = (await ctl.status())["proxy"]["active"]
    c = admin(active)
    job = json.dumps({"type": "close", "groupbuy_id": 3})
    removed = await c.zrem("q:delayed", job)
    t0 = time.time()
    back = None
    while time.time() - t0 < 15 and back is None:
        back = await c.zscore("q:delayed", job)
        await asyncio.sleep(0.5)
    await c.aclose()
    report("q:delayed 유실 → DB에서 복구", removed == 1 and back is not None, f"{time.time() - t0:.0f}s")


async def expect_denied(url: str, *cmd) -> str:
    c = aioredis.from_url(url)
    try:
        await c.execute_command(*cmd)
        return "allowed"
    except AuthenticationError as e:
        return f"auth: {e}"
    except RedisError as e:
        return str(e)
    finally:
        await c.aclose()


async def case_permissions():
    app_u = PROXY_URL.format("app:app-proxy-pw@")
    wk_u = PROXY_URL.format("worker:worker-proxy-pw@")
    anon = PROXY_URL.format("")
    cases = [
        ("인증 없음", anon, ("GET", "session:x"), "authentication|authenticated"),
        ("틀린 비밀번호", PROXY_URL.format("app:wrong@"), ("GET", "session:x"), "auth"),
        ("앱 XREADGROUP", app_u, ("XREADGROUP", "GROUP", "workers", "x", "STREAMS", "q:join", ">"), "no permissions|the app may not"),
        ("앱 XDEL", app_u, ("XDEL", "q:join", "1-1"), "the app may not"),
        ("앱 DEL 큐", app_u, ("DEL", "q:join"), "only XADD"),
        ("앱 다른 키", app_u, ("SET", "other", "1"), "may not touch"),
        ("worker DEL", wk_u, ("DEL", "q:join"), "may not run"),
        ("worker XTRIM", wk_u, ("XTRIM", "q:join", "MAXLEN", "0"), "may not run"),
        ("worker GROUP 삭제", wk_u, ("XGROUP", "DESTROY", "q:join", "workers"), "may not run"),
        ("worker 세션 읽기", wk_u, ("GET", "session:x"), "only touch"),
    ]
    bad = []
    for name, url, cmd, want in cases:
        got = (await expect_denied(url, *cmd)).lower()
        if not any(w in got for w in want.lower().split("|")):
            bad.append(f"{name}: {got}")
    report(f"프록시 역할별 명령 제한 ({len(cases)}건)", not bad, bad or "")
    allowed = await expect_denied(app_u, "SET", "session:perm-probe", "1")
    report("앱 정상 명령은 허용", allowed == "allowed", allowed)
    select1 = await expect_denied(app_u, "SELECT", "1")
    report("SELECT 1은 허용 (db 번호 보존)", select1 == "allowed", select1)
    codes = {}
    async with httpx.AsyncClient(timeout=10) as h:
        for name, url in (("proxy", PROXY_ADMIN), ("sync", SYNC_ADMIN), ("control", CTL)):
            codes[name] = (await h.get(url + "/status")).status_code
    report("관리 API 토큰 없으면 거부", set(codes.values()) == {401}, codes)


async def main():
    print("fresh stack...", flush=True)
    compose("down", "-v", "--remove-orphans")
    compose("up", "-d", "--build")
    ctl = Ctl()
    await add_test_account()
    async with httpx.AsyncClient(base_url=APP, timeout=30) as web:
        for _ in range(60):
            try:
                if (await web.post("/seed", params={"groupbuys": GROUPBUYS, "close_in_s": 1800})).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(1)
        else:
            raise SystemExit("seed failed: the app never came up")
        load = Load(12, 150)
        runner = asyncio.create_task(load.run())
        await asyncio.sleep(5)

        await case_permissions()
        await case_cache_during_pause(ctl)
        await case_poison_message(ctl)
        await case_delayed_rebuilt(ctl)
        await case_budget_too_small(ctl)
        await case_reverse_subscribe_fails(ctl, load)
        await case_fence_fails(ctl, load)
        await case_group_missing(ctl, load)
        await case_cache_count(ctl)
        await case_restart_syncing(ctl, load)
        await case_crash_before_record(ctl, load)
        await case_crash_after_record(ctl, load)
        await case_managed_like(ctl, load)
        await case_full_mode(ctl, load)
        await case_delta_flow(ctl, load)
        await case_delta_subscription_drop(ctl, load)
        await case_delta_stale_keys(ctl, load)

        load.stop.set()
        await runner
        inv = invariants(await wait_processed(web))
        report("전체: 재고 불변식, 큐 유실 없음", inv["ok"] and inv["statuses"].get("QUEUED", 0) == 0, inv["statuses"])
        report("전체: 요청 실패·세션 유실 0", load.failed == 0 and load.session_lost == 0,
               f"실패 {load.failed}, 세션 유실 {load.session_lost}, 요청 {load.ok}, {dict(list(load.errors.items())[:3])}")
        await case_delta_silent_drift(ctl)

    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    asyncio.run(main())
