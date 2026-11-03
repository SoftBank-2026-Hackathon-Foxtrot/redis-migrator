"""What a failure rollback reports as lost, against what it can know (run on the host).

  python demo/loss_report_check.py

After a cutover the reverse sync copies the target's changes back. When the target dies, the
rollback reports that backlog. It is exact only if every change was seen: this kills the target
once with the reverse subscription up (exact), and once after the target kept taking writes while
the subscription was refused (reported as a minimum). In between, the flag must clear again once
the subscription is back and the rescan has run.
"""
import sys
import time

import recovery_check as rc

SYNC = rc.SYNC


def sync_status() -> dict:
    return SYNC.get("/status").json().get("sync") or {}


def refuse_subscription(refuse: bool):
    gcp = rc.admin("gcp")
    gcp.execute_command("ACL", "SETUSER", "default", "-psubscribe" if refuse else "+psubscribe")
    if refuse:
        gcp.execute_command("CLIENT", "KILL", "TYPE", "pubsub")


def failure_rollback() -> dict:
    rc.compose("kill", "redis-gcp")
    rc.API.post("/internal/rollback", json={"mode": "failure"})
    st = rc.wait_phase("rolled_back", timeout=30)
    return (st.get("operation") or {}).get("result") or {}


def exact_case():
    w = rc.fresh()
    rc.to_executed()
    res = failure_rollback()
    w.stop()
    rc.report("구독이 살아 있던 대상이 죽음 → 유실 보고는 정확값", res.get("reverse_backlog_is_minimum") is False,
              {k: res.get(k) for k in ("reverse_backlog_at_failure", "reverse_backlog_is_minimum",
                                       "reverse_subscription_down_ms")})


def unseen_writes_case():
    w = rc.fresh()
    rc.to_executed()
    refuse_subscription(True)
    rc.until(lambda: sync_status().get("written_while_unsubscribed"), 10, "write seen while unsubscribed")
    refuse_subscription(False)
    cleared = rc.maybe(lambda: rc.until(lambda: not sync_status().get("written_while_unsubscribed")
                                        and not sync_status().get("rescanning"), 15, "cleared by the rescan"), False)
    rc.report("구독이 돌아와 다시 스캔하면 표시가 풀림", bool(cleared), sync_status().get("resyncs"))
    refuse_subscription(True)
    rc.until(lambda: sync_status().get("written_while_unsubscribed"), 10, "write seen while unsubscribed again")
    time.sleep(1)   # the writer keeps writing to gcp, none of it notified
    res = failure_rollback()
    w.stop()
    rc.report("구독이 끊긴 채 대상이 쓰기를 받다 죽음 → 유실 보고는 최소값", res.get("reverse_backlog_is_minimum") is True,
              {k: res.get(k) for k in ("reverse_backlog_at_failure", "reverse_backlog_is_minimum",
                                       "reverse_subscription_down_ms")})


def main():
    print("fresh stack...", flush=True)
    rc.compose("up", "-d", "--build", *rc.SERVICES)
    rc.safe(exact_case)
    rc.safe(unseen_writes_case)
    rc.compose("down", "-v", "-t", "0", "--remove-orphans")
    print(f"\n{sum(rc.results)}/{len(rc.results)} passed")
    sys.exit(0 if all(rc.results) else 1)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
