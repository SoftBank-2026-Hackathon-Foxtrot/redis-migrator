"""Proxy entry (design 3-8), run on the host: the precheck refuses a source that an application still
reaches directly, shuts the direct account on the source once nobody uses it, and opens it on the
side that stays when the migration ends.

  python demo/direct_check.py

Starts two throwaway Valkey containers of its own (no stack needed) and calls statemig directly.
"""
import asyncio
import subprocess
import sys
import tempfile
from pathlib import Path

import redis.asyncio as aioredis
from redis.exceptions import AuthenticationError, ResponseError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from statemig.backend import Endpoint  # noqa: E402
from statemig.migration import ControlRecord, Migration  # noqa: E402

IMAGE = "valkey/valkey:8"
NODES = {"src": 16401, "dst": 16402}
DIRECT_USER, DIRECT_PW = "app-direct", "direct-pw"
results: list[bool] = []


def report(name: str, ok: bool, detail=""):
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def start():
    for name, port in NODES.items():
        docker("rm", "-f", f"direct-{name}")
        p = docker("run", "-d", "--name", f"direct-{name}", "-p", f"127.0.0.1:{port}:6379", IMAGE, "valkey-server")
        if p.returncode:
            raise SystemExit(p.stderr)


def stop():
    for name in NODES:
        docker("rm", "-f", f"direct-{name}")


def ep(name: str) -> Endpoint:
    return Endpoint(name, f"redis://127.0.0.1:{NODES[name]}")


def app_client(name: str, client_name: str | None, user: str = DIRECT_USER) -> aioredis.Redis:
    return aioredis.Redis(host="127.0.0.1", port=NODES[name], username=user, password=DIRECT_PW,
                          client_name=client_name)


async def setup():
    for name in NODES:
        c = ep(name).client()
        for _ in range(50):
            try:
                await c.ping()
                break
            except Exception:  # noqa: BLE001
                await asyncio.sleep(0.2)
        await c.execute_command("ACL", "SETUSER", DIRECT_USER, "on", f">{DIRECT_PW}", "~*", "&*", "+@all")
        await c.aclose()


def migration(record: ControlRecord) -> Migration:
    return Migration(None, None, record, ep("src"), ep("dst"), "default", direct_user=DIRECT_USER)


async def direct_errors(m: Migration) -> list[str]:
    r = await m.precheck()
    return [e for e in r["errors"] if "direct" in e or "CLIENT LIST" in e]


async def can_login(name: str) -> bool:
    c = app_client(name, None)
    try:
        await c.ping()
        return True
    except (AuthenticationError, ResponseError):
        return False
    finally:
        await c.aclose()


async def main():
    start()
    try:
        await setup()
        record = ControlRecord(str(Path(tempfile.mkdtemp()) / "control.json"))
        m = migration(record)

        admin = aioredis.Redis(host="127.0.0.1", port=NODES["src"])   # unnamed, but not the direct account
        await admin.ping()
        errs = await direct_errors(m)
        report("이름 없는 관리 연결만 있음 → 거부 없음", errs == [], errs)

        named = app_client("src", "direct:goods-aws-api")
        await named.ping()
        errs = await direct_errors(m)
        report("direct:<actor> 이름의 연결 → 거부", len(errs) == 1 and "1 direct connections" in errs[0], errs)
        await named.aclose()

        unnamed = app_client("src", None)
        await unnamed.ping()
        errs = await direct_errors(m)
        report("이름 없이 직접 연결 계정으로 붙은 연결 → 거부", len(errs) == 1 and "(unnamed)" in errs[0], errs)
        await unnamed.aclose()

        through_proxy = app_client("src", "statemig-proxy", user="default")
        await through_proxy.ping()
        errs = await direct_errors(m)
        report("프록시 업스트림 이름의 연결 → 거부 없음", errs == [], errs)
        await through_proxy.aclose()

        await admin.aclose()
        await m.check()
        report("사전 점검 통과 → 원본의 직접 연결 계정을 닫음",
               not await can_login("src") and m.direct_fence.get("src") == "acl", m.direct_fence)
        report("대상의 직접 연결 계정은 그대로", await can_login("dst"))

        record.write(gen=1, active="dst", consume="dst", paused=False)
        await m._open_direct_on_active()
        report("대상에 남으면 대상 계정을 엶 (원본은 닫힌 채)",
               await can_login("dst") and not await can_login("src"), m.direct_fence)

        record.write(gen=2, active="src", consume="src", paused=False)
        await m._open_direct_on_active()
        report("원본으로 돌아오면 원본 계정을 다시 엶", await can_login("src"), m.direct_fence)
    finally:
        stop()
    failed = results.count(False)
    print(f"\n{len(results) - failed}/{len(results)} passed", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    asyncio.run(main())
