"""aws → onprem → gcp in one run under load, with the same proxy and apps (run on the host).

  python demo/chain_check.py

Starts from a fresh stack. Each hop is a cutover and a commit; the second hop is also
rolled back to the on-prem Valkey and cut over again. Prints PASS or FAIL per check.
"""
import asyncio
import sys

import httpx

from faults import Ctl, report, results
from onprem_check import admin, compose
from scenario import APP, GROUPBUYS, Load, app_can_write, invariants, wait_processed


async def session_alive(web: httpx.AsyncClient, sid: str) -> bool:
    return (await web.get("/me", headers={"x-session": sid})).status_code == 200


async def cutover(ctl: Ctl, target: str) -> httpx.Response:
    await ctl.sync_ready(target=target)
    r = await ctl.post("/migrations/current/cutover", {"budget_ms": 3000})
    await ctl.drained()
    return r


async def hop(ctl: Ctl, load: Load, web: httpx.AsyncClient, src: str, dst: str, sid: str):
    """One cutover and commit. The old side must be fenced and out of the sync afterwards."""
    before, lost = load.failed, load.session_lost
    r = await cutover(ctl, dst)
    px = (await ctl.status())["proxy"]
    cc = (await web.get("/cache-check")).json()
    fence, write = await app_can_write(src), await app_can_write(dst)
    alive = await session_alive(web, sid)
    report(f"{src} → {dst} 컷오버",
           r.status_code == 200 and px["active"] == dst and px["consume"] == dst and fence == "NOPERM"
           and write == "write allowed" and cc["ok"] and alive and load.failed == before and load.session_lost == lost,
           f"쓰기 정지 {r.json().get('paused_ms')}ms, {src} 쓰기={fence}, {dst} 쓰기={write}, 첫 세션 유지={alive}, "
           f"캐시 불일치={len(cc['mismatched'])}, 실패={load.failed - before}, 세션 유실={load.session_lost - lost}")

    r = await ctl.post("/migrations/current/commit")
    old, new = admin(src), admin(dst)
    await old.set("session:after-commit", src, ex=600)
    await asyncio.sleep(1)
    leaked = await new.get("session:after-commit")
    await old.delete("session:after-commit")
    await old.aclose()
    await new.aclose()
    st = await ctl.status()
    report(f"{src} → {dst} commit: 동기화 중지", r.status_code == 200 and leaked is None and not st["sync"]["running"],
           f"{r.status_code}, {src}의 새 쓰기가 {dst}에 도착={leaked is not None}")


async def case_rollback_last_hop(ctl: Ctl, load: Load, web: httpx.AsyncClient, sid: str):
    before, lost = load.failed, load.session_lost
    r1 = await cutover(ctl, "gcp")
    r2 = await ctl.post("/migrations/current/rollback", {"mode": "normal"})
    if r2.status_code == 200:
        await ctl.wait(lambda s: s["migration"]["state"] == "ROLLED_BACK", "rollback drain")
    px = (await ctl.status())["proxy"]
    fence, write = await app_can_write("gcp"), await app_can_write("onprem")
    alive = await session_alive(web, sid)
    report("onprem → gcp 컷오버 후 정상 롤백",
           r1.status_code == 200 and r2.status_code == 200 and px["active"] == "onprem" and fence == "NOPERM"
           and write == "write allowed" and alive and load.failed == before and load.session_lost == lost,
           f"쓰기 정지 {r1.json().get('paused_ms')}ms, {r2.json().get('paused_ms')}ms, gcp 쓰기={fence}, "
           f"onprem 쓰기={write}, 첫 세션 유지={alive}, 실패={load.failed - before}, 세션 유실={load.session_lost - lost}")


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
        sid = (await web.post("/login/chain")).json()["sid"]
        load = Load(12, 150)
        runner = asyncio.create_task(load.run())
        await asyncio.sleep(5)

        await hop(ctl, load, web, "aws", "onprem", sid)
        await case_rollback_last_hop(ctl, load, web, sid)
        await hop(ctl, load, web, "onprem", "gcp", sid)

        await asyncio.sleep(3)
        load.stop.set()
        await runner
        c = admin("gcp")
        delayed = await c.zcard("q:delayed")
        await c.aclose()
        report("예약 작업이 최종 환경에 있음", delayed > 0, f"gcp q:delayed={delayed}")
        alive = await session_alive(web, sid)
        report("첫 세션이 최종 환경에서 유지", alive, "aws에서 만든 세션으로 gcp에서 인증")
        inv = invariants(await wait_processed(web))
        report("전체: 재고 불변식, 큐 유실 없음", inv["ok"] and inv["statuses"].get("QUEUED", 0) == 0, inv["statuses"])
        report("전체: 요청 실패·세션 유실 0", load.failed == 0 and load.session_lost == 0,
               f"요청 {load.ok}, 실패 {load.failed}, 세션 유실 {load.session_lost}, {dict(list(load.errors.items())[:3])}")

    failed = [n for n, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed", flush=True)
    compose("down", "-v", "--remove-orphans")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    asyncio.run(main())
