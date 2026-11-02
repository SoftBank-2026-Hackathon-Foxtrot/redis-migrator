"""The on-prem Valkey as a real endpoint, restarts included, under load (run on the host).

  python demo/onprem_check.py

Starts from a fresh stack: aws → onprem, normal rollback, kill the fenced on-prem Valkey,
aws → onprem again, kill it while it is active, normal rollback. Prints PASS or FAIL per check.
"""
import asyncio
import subprocess
import sys
import time

import httpx
import redis.asyncio as aioredis

import scenario
from faults import Ctl, report, results
from scenario import APP, COMPOSE_DIR, COMPOSE_FILES, GROUPBUYS, Load, app_can_write, invariants, wait_processed

ONPREM_ADMIN = "redis://default:onprem-admin-pw@localhost:36379"
scenario.ADMIN["onprem"] = ONPREM_ADMIN
scenario.APPUSER["onprem"] = "redis://app:onprem-app-pw@localhost:36379"
SENTINEL = "onprem:sentinel"     # outside the synced prefixes: only a restart can touch it
COMPOSE = ["docker", "compose", *COMPOSE_FILES, "-f", "onprem.compose.yml", "--env-file", "onprem.env"]


def compose(*args):
    subprocess.run([*COMPOSE, *args], cwd=COMPOSE_DIR, capture_output=True, check=True)


def admin(side: str = "onprem"):
    return aioredis.from_url(scenario.ADMIN[side], decode_responses=True)


async def kill_onprem():
    """SIGKILL, then wait until it has loaded its AOF. Returns when it answers again."""
    compose("kill", "redis-onprem")
    compose("start", "redis-onprem")
    c = admin()
    try:
        for _ in range(120):
            try:
                await c.get(SENTINEL)
                return
            except Exception:  # noqa: BLE001  connection refused, then LOADING
                await asyncio.sleep(0.5)
        raise SystemExit("on-prem Valkey did not come back")
    finally:
        await c.aclose()


async def rollback_to_aws(ctl: Ctl) -> httpx.Response:
    r = await ctl.post("/migrations/current/rollback", {"mode": "normal"})
    if r.status_code == 200:
        await ctl.wait(lambda s: s["migration"]["state"] == "ROLLED_BACK", "rollback drain")
    return r


async def case_precheck_and_cutover(ctl: Ctl, load: Load, web: httpx.AsyncClient):
    before = load.failed
    await ctl.sync_ready(target="onprem")
    m = await ctl.migration()
    pc = m["precheck"]
    target_risks = [w for w in pc["warnings"] if w.endswith("on target")]
    report("사전 확인: 온프레 Valkey 8.0, DUMP 복사, ACL 차단, 재시작 위험 없음",
           pc["target_version"].startswith("valkey 8.0") and pc["copy_mode"] == "dump"
           and m["fence_mode"]["onprem"] == "acl" and not target_risks,
           f"{pc['target_version']} copy={pc['copy_mode']} fence={m['fence_mode']} 위험={target_risks}")
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    await ctl.drained()
    cc = (await web.get("/cache-check")).json()
    fence, write = await app_can_write("aws"), await app_can_write("onprem")
    report("aws → onprem 컷오버 (앱 비밀번호가 달라도 동작)",
           r.status_code == 200 and fence == "NOPERM" and write == "write allowed" and cc["ok"]
           and load.failed == before and load.session_lost == 0,
           f"쓰기 정지 {r.json().get('paused_ms')}ms, aws 쓰기={fence}, onprem 쓰기={write}, "
           f"캐시 불일치={len(cc['mismatched'])}, 실패={load.failed - before}, 세션 유실={load.session_lost}")


async def case_rollback_then_restart_fenced(ctl: Ctl, load: Load):
    before = load.failed
    r = await rollback_to_aws(ctl)
    fence = await app_can_write("onprem")
    report("onprem → aws 정상 롤백", r.status_code == 200 and fence == "NOPERM" and load.failed == before,
           f"쓰기 정지 {r.json().get('paused_ms')}ms, onprem 쓰기={fence}, 실패={load.failed - before}")
    c = admin()
    await c.set(SENTINEL, "kept", ex=3600)
    await c.aclose()
    await asyncio.sleep(1.5)      # appendfsync everysec
    await kill_onprem()
    c = admin()
    value, ttl = await c.get(SENTINEL), await c.ttl(SENTINEL)
    notify = (await c.config_get("notify-keyspace-events"))["notify-keyspace-events"]
    maxmem = int((await c.config_get("maxmemory"))["maxmemory"])
    await c.aclose()
    report("재시작 후 데이터 유지 (AOF)", value == "kept" and 0 < ttl <= 3600, f"값={value} TTL={ttl}")
    report("재시작 후 알림 설정 유지", {"K", "A"} <= set(notify), notify)
    report("재시작 후 메모리 제한 유지", maxmem == 1 << 30, maxmem)
    fence = await app_can_write("onprem")
    report("재시작 후 쓰기 차단 유지", fence == "NOPERM", fence)


async def case_restart_active(ctl: Ctl, load: Load, web: httpx.AsyncClient):
    await ctl.sync_ready(target="onprem")
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    await ctl.drained()
    report("다시 aws → onprem 컷오버 (차단이 풀림)", r.status_code == 200 and await app_can_write("onprem") == "write allowed",
           f"쓰기 정지 {r.json().get('paused_ms')}ms")

    failed, lost, t_kill = load.failed, load.session_lost, time.time()
    resyncs = (await ctl.status())["sync"]["sync"]["resyncs"]
    await kill_onprem()
    t_up = time.time()
    st = await ctl.wait(lambda s: s["sync"]["sync"]["resyncs"] > resyncs and s["sync"]["sync"]["compare_valid"]
                        and s["sync"]["sync"]["backlog"] < 200, "resubscribe and compare after the restart", timeout=60)
    report("활성 온프레 강제 종료: 복구 시간과 영향 (정보)", True,
           f"{t_up - t_kill:.1f}s, 요청 실패 {load.failed - failed}건, 세션 유실 {load.session_lost - lost}건")

    # this key can only reach aws through a notification: the rescan after the restart is over
    await asyncio.sleep(2)
    c, a = admin(), admin("aws")
    await c.set("session:after-restart", "1", ex=600)
    reached = None
    for _ in range(50):
        reached = await a.get("session:after-restart")
        if reached:
            break
        await asyncio.sleep(0.2)
    await c.aclose()
    await a.aclose()
    report("재시작 후 역방향 동기화가 알림으로 계속 동작", reached == "1", f"aws 값={reached}, resyncs={st['sync']['sync']['resyncs']}")
    write, fence = await app_can_write("onprem"), await app_can_write("aws")
    report("재시작 후 쓰기 대상은 온프레 하나", write == "write allowed" and fence == "NOPERM", f"onprem={write}, aws={fence}")
    await asyncio.sleep(3)
    after = load.failed
    await asyncio.sleep(5)
    report("복구 뒤 요청 실패 없음", load.failed == after, f"5초 동안 {load.failed - after}건")

    r = await rollback_to_aws(ctl)
    report("재시작 뒤 정상 롤백 (표시 키 알림 통과)", r.status_code == 200 and await app_can_write("onprem") == "NOPERM",
           r.status_code if r.status_code != 200 else f"쓰기 정지 {r.json().get('paused_ms')}ms")


async def main():
    print("fresh stack...", flush=True)
    compose("down", "-v", "--remove-orphans")
    compose("up", "-d", "--build")
    ctl = Ctl()
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

        await case_precheck_and_cutover(ctl, load, web)
        await case_rollback_then_restart_fenced(ctl, load)
        await case_restart_active(ctl, load, web)

        load.stop.set()
        await runner
        inv = invariants(await wait_processed(web))
        report("전체: 재고 불변식, 큐 유실 없음", inv["ok"] and inv["statuses"].get("QUEUED", 0) == 0, inv["statuses"])

    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    asyncio.run(main())
