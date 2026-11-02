"""Real ElastiCache as the source, under load (run on a host in its VPC).

  AWS_ENDPOINT=... AWS_ADMIN_PW=... AWS_APP_PW=... python demo/aws_check.py

Fresh stack: precheck against the real service, ElastiCache → Valkey cutover, reverse sync,
normal rollback, cutover again, then the Valkey is killed and rolled back from.
"""
import asyncio
import json
import os
import subprocess
import sys
import time

import httpx
import redis.asyncio as aioredis

import scenario
from faults import Ctl, report, results
from scenario import (APP, COMPOSE_DIR, COMPOSE_FILES, GROUPBUYS, ONPREM_FILES, Load, add_onprem, app_can_write,
                      invariants, wait_processed)

add_onprem()
ENDPOINT, ADMIN_PW, APP_PW = os.environ["AWS_ENDPOINT"], os.environ["AWS_ADMIN_PW"], os.environ["AWS_APP_PW"]
scenario.ADMIN["aws"] = f"rediss://default:{ADMIN_PW}@{ENDPOINT}:6379"
scenario.APPUSER["aws"] = f"rediss://app:{APP_PW}@{ENDPOINT}:6379"
COMPOSE = ["docker", "compose", *COMPOSE_FILES, *ONPREM_FILES, "-f", "aws.compose.yml", "--env-file", "aws.env"]


def compose(*args):
    subprocess.run([*COMPOSE, *args], cwd=COMPOSE_DIR, capture_output=True, check=True)


def admin(side: str):
    return aioredis.from_url(scenario.ADMIN[side], decode_responses=True)


async def arrives(side: str, key: str, want: str, timeout=10) -> bool:
    c = admin(side)
    try:
        for _ in range(int(timeout / 0.2)):
            if await c.get(key) == want:
                return True
            await asyncio.sleep(0.2)
        return False
    finally:
        await c.aclose()


async def case_cutover(ctl: Ctl, load: Load, web: httpx.AsyncClient):
    t0 = time.time()
    await ctl.sync_ready(target="onprem")
    m = await ctl.migration()
    pc = m["precheck"]
    print(json.dumps({"precheck": pc, "fence_mode": m["fence_mode"], "ready_s": round(time.time() - t0, 1)},
                     ensure_ascii=False, indent=1), flush=True)
    report("사전 확인: 실제 ElastiCache에서 알림, DUMP 복사 통과", not pc["errors"] and pc["copy_mode"] == "dump",
           f"{pc['source_version']} → {pc['target_version']}, copy={pc['copy_mode']}, 왕복 {pc['estimate']['rtt_ms']}ms")
    report("쓰기 차단 방식 (정보)", True, m["fence_mode"])
    before, lost = load.failed, load.session_lost
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    if r.status_code != 200:
        raise SystemExit(f"cutover: {r.text}")
    await ctl.drained()
    cc = (await web.get("/cache-check")).json()
    px = (await ctl.status())["proxy"]
    report("ElastiCache → Valkey 컷오버", px["active"] == "onprem" and cc["ok"] and load.failed == before
           and load.session_lost == lost,
           f"쓰기 정지 {r.json()['paused_ms']}ms, fence={r.json()['fence']}, 캐시 불일치={len(cc['mismatched'])}, "
           f"실패={load.failed - before}, 세션 유실={load.session_lost - lost}")
    report("원본 앱 사용자 직접 쓰기 (정보)", True, await app_can_write("aws"))
    c = admin("onprem")
    await c.set("session:reverse-probe", "1", ex=600)
    await c.aclose()
    report("역방향 동기화가 ElastiCache까지 도착", await arrives("aws", "session:reverse-probe", "1"), "")


async def case_normal_rollback(ctl: Ctl, load: Load):
    before, lost = load.failed, load.session_lost
    r = await ctl.post("/migrations/current/rollback", {"mode": "normal"})
    if r.status_code != 200:
        raise SystemExit(f"rollback: {r.text}")
    await ctl.wait(lambda s: s["migration"]["state"] == "ROLLED_BACK", "rollback drain")
    fence = await app_can_write("onprem")
    report("Valkey → ElastiCache 정상 롤백", fence == "NOPERM" and load.failed == before and load.session_lost == lost,
           f"쓰기 정지 {r.json()['paused_ms']}ms, Valkey 쓰기={fence}, 실패={load.failed - before}, "
           f"세션 유실={load.session_lost - lost}")


async def case_failure_rollback(ctl: Ctl, load: Load):
    await ctl.sync_ready(target="onprem")
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    if r.status_code != 200:
        raise SystemExit(f"cutover: {r.text}")
    await ctl.drained()
    compose("kill", "redis-onprem")
    await asyncio.sleep(5)
    r = await ctl.post("/migrations/current/rollback", {"mode": "failure", "budget_ms": 2000})
    t_rb = time.time()
    await asyncio.sleep(10)
    after = load.summary(t_rb + 0.5)
    px = (await ctl.status())["proxy"]
    res = r.json()
    report("Valkey 강제 종료 후 ElastiCache로 장애 롤백",
           r.status_code == 200 and px["active"] == "aws" and after["requests"] > 0 and after["failed"] == 0,
           f"쓰기 정지 {res.get('paused_ms')}ms, 캐시 {res.get('cache_keys_dropped')}개 삭제, "
           f"역방향 미처리 {res.get('reverse_backlog_at_failure')}개, 롤백 뒤 요청 {after['requests']}건 중 실패 {after['failed']}건")
    compose("start", "redis-onprem")


async def main():
    print("fresh stack...", flush=True)
    compose("down", "-v", "--remove-orphans")
    compose("up", "-d", "--build")
    ctl = Ctl()
    async with httpx.AsyncClient(base_url=APP, timeout=30) as web:
        for _ in range(90):
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

        await case_cutover(ctl, load, web)
        await case_normal_rollback(ctl, load)
        await case_failure_rollback(ctl, load)

        load.stop.set()
        await runner
        inv = invariants(await wait_processed(web))
        report("전체: 재고 불변식, 큐 유실 없음", inv["ok"] and inv["statuses"].get("QUEUED", 0) == 0, inv["statuses"])
        print(f"요청 성공 {load.ok}, 실패 {load.failed}, 세션 유실 {load.session_lost}, {dict(list(load.errors.items())[:3])}")

    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    asyncio.run(main())
