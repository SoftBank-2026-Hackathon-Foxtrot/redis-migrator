"""A lost TCP SYN towards the source must not stretch a failure rollback's write pause (run on the
host, as root on Linux: it adds an iptables rule inside the Redis container's network namespace).

  sudo python demo/syn_loss_check.py

Found on the real environments: the failure rollback held writes for about 4.1 s in three runs out
of four. Each step that opened a new connection inside the pause waited on the kernel's SYN retry
(1 s, then 3 s) whenever a SYN was lost. Every other SYN to the source's port 6379 is dropped
here, and the pause must stay under 1 s: with connections opened before it, and with new ones
retried after 0.3 s instead.
"""
import os
import subprocess
import sys

import recovery_check as rc

CACHE_KEYS = 5000
RULE = ["INPUT", "-p", "tcp", "--dport", "6379", "--syn", "-m", "statistic", "--mode", "nth",
        "--every", "2", "--packet", "0", "-j", "DROP"]


def netns_iptables(service: str, *args):
    pid = subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", rc.container_of(service)],
                         capture_output=True, text=True, check=True).stdout.strip()
    subprocess.run(["nsenter", "-t", pid, "-n", "iptables", *args], capture_output=True, check=True)


def failure_rollback(label: str, lose_syns: bool, restart_statemig: bool):
    w = rc.fresh()
    p = rc.admin("aws").pipeline(transaction=False)
    for i in range(CACHE_KEYS):
        p.set(f"cache:item:{i}", "x" * 50)
    p.execute()
    rc.to_executed()
    if restart_statemig:   # nothing opened yet: the connections are made during the rollback
        rc.compose("restart", "statemig")
        rc.until(rc.api_up, 30, "statemig back")
        rc.until(lambda: rc.status().get("phase") == "executed", 30, "recovered")
    if lose_syns:
        netns_iptables("redis-aws", "-I", *RULE)
    try:
        rc.compose("kill", "redis-gcp")
        rc.API.post("/internal/rollback", json={"mode": "failure"})
        st = rc.wait_phase("rolled_back", "failed", timeout=60)
    finally:
        if lose_syns:
            netns_iptables("redis-aws", "-D", *RULE)
    w.stop()
    res = (st.get("operation") or {}).get("result") or {}
    paused = res.get("paused_ms")
    rc.report(label, st.get("phase") == "rolled_back" and paused is not None and paused < 1000
              and res.get("cache_keys_dropped", 0) >= CACHE_KEYS,
              f"쓰기 정지 {paused}ms, 단계 {res.get('steps_ms')}")


def main():
    if os.geteuid() != 0:
        raise SystemExit("needs root: iptables in the container's network namespace")
    print("fresh stack...", flush=True)
    rc.compose("up", "-d", "--build", *rc.SERVICES)
    rc.safe(failure_rollback, "장애 롤백, SYN 손실 없음 → 쓰기 정지 1초 미만", False, False)
    rc.safe(failure_rollback, "장애 롤백, 원본으로 가는 SYN 절반 손실 → 1초 미만", True, False)
    rc.safe(failure_rollback, "같은 조건, statemig 재시작 뒤라 연결이 하나도 없음 → 1초 미만", True, True)
    rc.compose("down", "-v", "-t", "0", "--remove-orphans")
    print(f"\n{sum(rc.results)}/{len(rc.results)} passed")
    sys.exit(0 if all(rc.results) else 1)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
