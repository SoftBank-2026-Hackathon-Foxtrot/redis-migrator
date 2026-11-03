"""The precheck refuses what it cannot move whole (run on the host): a cluster, a db the other side
does not have (each db keeps its number), and queue streams outside db 0.

  python demo/topology_check.py

Starts throwaway Valkey containers of its own (no stack needed) and calls the precheck directly.
"""
import asyncio
import subprocess
import sys
from pathlib import Path

import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from statemig.backend import Endpoint  # noqa: E402
from statemig.migration import Migration  # noqa: E402

IMAGE = "valkey/valkey:8"
NODES = {"plain-a": (16391, []), "plain-b": (16392, []), "cluster": (16393, ["--cluster-enabled", "yes"]),
         "two-dbs": (16394, ["--databases", "2"]),
         "evicting": (16395, ["--maxmemory", "64mb", "--maxmemory-policy", "allkeys-lru"]),
         "lru-no-limit": (16396, ["--maxmemory-policy", "allkeys-lru"])}
results: list[bool] = []


def report(name: str, ok: bool, detail=""):
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def start():
    for name, (port, extra) in NODES.items():
        docker("rm", "-f", f"topo-{name}")
        p = docker("run", "-d", "--name", f"topo-{name}", "-p", f"127.0.0.1:{port}:6379", IMAGE, "valkey-server", *extra)
        if p.returncode:
            raise SystemExit(p.stderr)


def stop():
    for name in NODES:
        docker("rm", "-f", f"topo-{name}")


def ep(name: str) -> Endpoint:
    return Endpoint(name, f"redis://127.0.0.1:{NODES[name][0]}")


async def wait_up():
    for name in NODES:
        c = ep(name).client()
        for _ in range(50):
            try:
                await c.ping()
                break
            except Exception:  # noqa: BLE001
                await asyncio.sleep(0.2)
        await c.aclose()


async def topology_errors(src: str, dst: str) -> list[str]:
    m = Migration(None, None, None, ep(src), ep(dst), "default")
    r = await m.precheck()
    return [e for e in r["errors"] if "cluster mode" in e or " db " in e or "outside db 0" in e or "evict" in e]


async def put(name: str, db: int, *cmd):
    c = aioredis.Redis(host="127.0.0.1", port=NODES[name][0], db=db)
    await c.execute_command(*cmd)
    await c.aclose()


async def main():
    start()
    try:
        await wait_up()
        errs = await topology_errors("plain-a", "plain-b")
        report("단일 노드, db 0만 → 거부 없음", errs == [], errs)

        errs = await topology_errors("plain-a", "cluster")
        report("대상이 cluster 모드 → 거부", errs == ["cluster mode is not supported on target"], errs)
        errs = await topology_errors("cluster", "plain-b")
        report("원본이 cluster 모드 → 거부", errs == ["cluster mode is not supported on source"], errs)

        errs = await topology_errors("plain-a", "evicting")
        report("대상이 maxmemory + allkeys-lru → 거부", errs == ["maxmemory-policy allkeys-lru on target can evict keys:"
                                                              " set noeviction"], errs)
        errs = await topology_errors("evicting", "plain-b")
        report("원본이 축출 정책 → 거부 (역방향 동기화 때 대상이 됨)", len(errs) == 1 and "on source" in errs[0], errs)
        errs = await topology_errors("plain-a", "lru-no-limit")
        report("maxmemory 없음 → 축출 정책이 있어도 거부 안 함", errs == [], errs)

        await put("plain-a", 5, "SET", "session:in-db5", "1")
        errs = await topology_errors("plain-a", "plain-b")
        report("원본 db 5에 키, 대상도 db 5 있음 → 거부 없음", errs == [], errs)
        errs = await topology_errors("plain-a", "two-dbs")
        report("원본 db 5에 키, 대상은 db 2개 → 거부", errs == ["source has keys in db 5, target has no db 5"], errs)
        errs = await topology_errors("two-dbs", "plain-a")
        report("대상 db 5에 키, 원본은 db 2개 → 거부 (역방향)", errs == ["target has keys in db 5, source has no db 5"], errs)

        await put("plain-b", 3, "XADD", "q:join", "*", "request_id", "x")
        errs = await topology_errors("plain-a", "plain-b")
        report("큐 스트림이 db 3에 있음 → 거부", errs == ["queue streams outside db 0 on target: ['db3/q:join']"], errs)
    finally:
        stop()
    failed = results.count(False)
    print(f"\n{len(results) - failed}/{len(results)} passed", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    asyncio.run(main())
