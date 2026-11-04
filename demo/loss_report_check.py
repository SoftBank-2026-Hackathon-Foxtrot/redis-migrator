"""What a failure rollback reports as lost, against what it can know (run on the host).

  python demo/loss_report_check.py

After a cutover the reverse sync copies the target's changes back. When the target dies, the
rollback reports that backlog. It is exact only if every change was seen: this kills the target
once with the reverse subscription up (exact), and once after the target kept taking writes while
the subscription was refused (reported as a minimum). In between, the flag must clear again once
the subscription is back and the rescan has run. Last, notifications turned off on a live
subscription: during the sync (noticed, put back, nothing lost), and on the target after a cutover
where they cannot be put back (the loss report is a minimum).
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


def silence_heals_case():
    """Notifications turned off on the source during the sync: noticed, put back, nothing lost."""
    w = rc.fresh()
    rc.to_ready()
    aws = rc.admin("aws")
    aws.config_set("notify-keyspace-events", "")
    t0 = time.time()
    for i in range(50):        # written while nobody is told
        rc.app_client().set(f"session:unseen:{i}", i)
    noticed = rc.maybe(lambda: rc.until(lambda: sync_status().get("notification_silences", 0) >= 1, 15, "noticed"), False)
    healed = rc.maybe(lambda: rc.until(lambda: rc.status().get("ready"), 60, "ready again"), False)
    gcp = rc.admin("gcp")
    copied = sum(1 for i in range(50) if gcp.get(f"session:unseen:{i}") is not None)
    w.stop()
    rc.report("동기화 중 원본 알림이 꺼짐 → 감지, 다시 켬, 그사이 쓴 키도 복사",
              bool(noticed) and bool(healed) and copied == 50 and "K" in aws.config_get("notify-keyspace-events")[
                  "notify-keyspace-events"],
              f"감지·복구·ready까지 {time.time() - t0:.1f}s, 복사 {copied}/50, 알림 설정 "
              f"{aws.config_get('notify-keyspace-events')['notify-keyspace-events']!r}")


def silence_retry_case():
    """The first try to turn notifications back on fails (CONFIG refused at that moment): they are
    tried again until they come back, not given up after one try."""
    w = rc.fresh()
    rc.to_ready()
    aws = rc.admin("aws")
    aws.execute_command("ACL", "SETUSER", "default", "-config")
    aws.execute_command("ACL", "SETUSER", "admin-tmp", "on", ">tmppw", "~*", "&*", "+@all")
    tmp = rc.redis.Redis(host="localhost", port=rc.PORT["aws"], username="admin-tmp", password="tmppw",
                         decode_responses=True)
    tmp.config_set("notify-keyspace-events", "")
    for i in range(50):
        rc.app_client().set(f"session:unseen2:{i}", i)
    noticed = rc.maybe(lambda: rc.until(lambda: sync_status().get("notification_silences", 0) >= 1, 15, "noticed"), False)
    time.sleep(2)             # the first try has failed by now
    tmp.execute_command("ACL", "SETUSER", "default", "+config")
    t0 = time.time()
    healed = rc.maybe(lambda: rc.until(lambda: rc.status().get("ready"), 60, "ready again"), False)
    gcp = rc.admin("gcp")
    copied = sum(1 for i in range(50) if gcp.get(f"session:unseen2:{i}") is not None)
    conf = tmp.config_get("notify-keyspace-events")["notify-keyspace-events"]
    tmp.execute_command("ACL", "DELUSER", "admin-tmp")
    w.stop()
    rc.report("알림 다시 켜기가 처음엔 막혀도 재시도로 복구, 그사이 쓴 키도 복사",
              bool(noticed) and bool(healed) and copied == 50 and "K" in conf,
              f"허용 후 ready까지 {time.time() - t0:.1f}s, 복사 {copied}/50, 알림 설정 {conf!r}")


def silence_then_failure_case():
    """After a cutover the target's notifications go off for good, then the target dies."""
    w = rc.fresh()
    rc.to_executed()
    gcp = rc.admin("gcp")
    gcp.config_set("notify-keyspace-events", "")
    gcp.execute_command("ACL", "SETUSER", "default", "-config")    # the Sync Worker cannot put them back
    silent = rc.maybe(lambda: rc.until(lambda: sync_status().get("notifications_silent"), 15, "silent"), False)
    time.sleep(1)   # the writer keeps writing to gcp, none of it notified
    res = failure_rollback()
    w.stop()
    rc.report("역방향 알림이 꺼진 채 대상이 죽음 → 유실 보고는 최소값", bool(silent)
              and res.get("reverse_backlog_is_minimum") is True,
              {k: res.get(k) for k in ("reverse_backlog_at_failure", "reverse_backlog_is_minimum")})


def main():
    print("fresh stack...", flush=True)
    rc.compose("up", "-d", "--build", *rc.SERVICES)
    rc.safe(exact_case)
    rc.safe(unseen_writes_case)
    rc.safe(silence_heals_case)
    rc.safe(silence_retry_case)
    rc.safe(silence_then_failure_case)
    rc.compose("down", "-v", "-t", "0", "--remove-orphans")
    print(f"\n{sum(rc.results)}/{len(rc.results)} passed")
    sys.exit(0 if all(rc.results) else 1)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
