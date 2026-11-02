"""Measures the Go and the Python data plane the same way, in turns (run on the host).

  python demo/compare_dataplanes.py [--rounds 3] [--bench-rounds 2]

Appends one JSON line per measurement to demo/compare-results.jsonl. Summary: PERFORMANCE.md.
  throughput  valkey-benchmark from inside the compose network: through the proxy, and directly
  scenario    scenario.py --mode normal: pause and app latency under load
  bench       bench.py --mode delta: until ready and pause with N keys, 30ms to the source
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx

DEMO = Path(__file__).parent
OUT = DEMO / "compare-results.jsonl"
IMPLS = ("go", "python")
BENCH_FIELDS = ("rps", "avg_ms", "min_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms")
TARGETS = {"proxy": ["-h", "proxy", "--user", "app", "-a", "app-proxy-pw"],
           "direct": ["-h", "redis-aws", "--user", "app", "-a", "apppw"]}
RESTORE = ["bench-results.jsonl", "result-normal.json"]     # the scripts below overwrite these


def env_of(impl: str) -> dict:
    return {**os.environ, "DATAPLANE": impl, "PYTHONIOENCODING": "utf-8"}


def compose_cmd(impl: str, *args) -> list[str]:
    go = ["-f", "go.compose.yml"] if impl == "go" else []
    return ["docker", "compose", "-f", "docker-compose.yml", *go, *args]


def compose(impl: str, *args) -> str:
    p = subprocess.run(compose_cmd(impl, *args), cwd=DEMO, capture_output=True, check=True, env=env_of(impl))
    return p.stdout.decode(errors="replace")


def fresh(impl: str):
    compose(impl, "down", "-v", "--remove-orphans")
    for attempt in range(3):
        p = subprocess.run(compose_cmd(impl, "up", "-d", "--build"), cwd=DEMO, capture_output=True, env=env_of(impl))
        if p.returncode == 0:
            break
        print(f"compose up failed: {p.stderr.decode(errors='replace')[-500:]}", flush=True)
        if attempt == 2:
            raise SystemExit("compose up kept failing")
        compose(impl, "down", "-v", "--remove-orphans")
        time.sleep(10)
    for _ in range(60):
        try:
            if httpx.post("http://localhost:18000/seed", params={"groupbuys": 10, "close_in_s": 30}).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(2)
    raise SystemExit("the app never came up")


def record(**row):
    print(json.dumps(row, ensure_ascii=False), flush=True)
    with OUT.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def benchmark_cmd(impl: str, target: str, clients: int, n: int, command: str) -> list[str]:
    return compose_cmd(impl, "exec", "-T", "redis-aws", "valkey-benchmark", *TARGETS[target], "-n", str(n),
                       "-c", str(clients), "-r", "100000", "--csv", *command.split())


def parse_csv(text: str) -> dict:
    row = list(csv.reader(text.strip().splitlines()))[-1]
    return dict(zip(BENCH_FIELDS, map(float, row[1:])))


def stats(impl: str) -> dict:
    """CPU and memory of the data plane containers right now."""
    ids = compose(impl, "ps", "-q", "proxy", "syncworker").split()
    p = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids], capture_output=True, check=True)
    rows = [json.loads(line) for line in p.stdout.decode().splitlines()]
    return {("proxy" if "proxy" in r["Name"] else "syncworker"): {"cpu": r["CPUPerc"], "mem": r["MemUsage"].split(" /")[0]}
            for r in rows}


def throughput(impl: str, round_no: int):
    fresh(impl)
    time.sleep(3)
    record(kind="idle", impl=impl, round=round_no, stats=stats(impl))
    for target in TARGETS:
        for clients in (1, 50):
            for command in ("SET session:__rand_int__ x", "GET session:__rand_int__"):
                p = subprocess.run(benchmark_cmd(impl, target, clients, 30000, command), cwd=DEMO, capture_output=True,
                                   check=True, env=env_of(impl))
                record(kind="throughput", impl=impl, round=round_no, target=target, clients=clients,
                       command=command.split()[0], **parse_csv(p.stdout.decode()))
    # CPU while the proxy is saturated
    b = subprocess.Popen(benchmark_cmd(impl, "proxy", 50, 150000, "GET session:__rand_int__"), cwd=DEMO,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env_of(impl))
    time.sleep(1)
    record(kind="saturated", impl=impl, round=round_no, stats=stats(impl))
    b.wait()


def scenario(impl: str, round_no: int, users: int, think_ms: int):
    fresh(impl)
    p = subprocess.run([sys.executable, str(DEMO / "scenario.py"), "--mode", "normal", "--users", str(users),
                        "--think-ms", str(think_ms)], cwd=DEMO, capture_output=True, env=env_of(impl))
    res = json.loads((DEMO / "result-normal.json").read_text(encoding="utf-8"))
    record(kind="scenario", impl=impl, round=round_no, users=users, passed=p.returncode == 0,
           paused_ms=[c["paused_ms"] for c in res["checks"]], load=res["load"], failed=res["failed"],
           session_lost=res["session_lost"])


def bench(impl: str, round_no: int, keys: int):
    p = subprocess.run([sys.executable, str(DEMO / "bench.py"), "--keys", str(keys), "--mode", "delta", "--label", impl],
                       cwd=DEMO, capture_output=True, env=env_of(impl))
    if p.returncode:
        record(kind="bench", impl=impl, round=round_no, keys=keys, error=p.stderr.decode(errors="replace")[-300:])
        return
    row = json.loads(p.stdout.decode().strip().splitlines()[-1])
    record(kind="bench", impl=impl, round=round_no, **{k: v for k, v in row.items() if k != "label"})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--bench-rounds", type=int, default=2)
    ap.add_argument("--stages", default="throughput,scenario,bench", help="comma-separated, to continue a stopped run")
    ap.add_argument("--first-round", type=int, default=0, help="for the scenario stage, to continue a stopped run")
    a = ap.parse_args()
    stages = a.stages.split(",")
    saved = {name: (DEMO / name).read_bytes() for name in RESTORE}
    try:
        if "throughput" in stages:
            for r in range(a.rounds):
                for impl in IMPLS:
                    throughput(impl, r)
        if "scenario" in stages:
            for r in range(a.first_round, a.rounds):
                for impl in IMPLS:
                    scenario(impl, r, 12, 150)
                    scenario(impl, r, 60, 12)
        if "bench" in stages:
            for r in range(a.bench_rounds):
                for keys in (10000, 100000):
                    for impl in IMPLS:
                        bench(impl, r, keys)
    finally:
        for name, data in saved.items():
            (DEMO / name).write_bytes(data)
        compose("python", "-f", "bench.compose.yml", "down", "-v", "--remove-orphans")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
