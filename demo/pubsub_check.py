"""Subscriptions through the proxy survive a move: no reconnect, no message lost or doubled (run on the host).

  python demo/pubsub_check.py

Fresh stack without the app. Three subscribers on raw sockets, so a dropped connection cannot be
hidden by a client library's reconnect: a RESP2 channel subscriber, a RESP2 pattern subscriber
and a RESP3 channel subscriber. A publisher sends a numbered message every 5 ms through the proxy.
Then: a cutover the target refuses to subscribe for (called off), a cutover, a normal rollback,
another cutover and a failure rollback. Every message the proxy acknowledged must arrive exactly
once, except on the failure rollback, where only what the dead target had not delivered may miss.
"""
import socket
import sys
import threading
import time

import recovery_check as rc

CHANNEL = "news"


def read_frame(f):
    """One RESP2/RESP3 frame from a socket file, as Python values."""
    line = f.readline()
    if not line:
        raise EOFError
    t, body = line[:1], line[1:-2]
    if t in (b"+", b"-", b":", b"_", b"#", b",", b"("):
        return body.decode()
    if t in (b"$", b"=", b"!"):
        n = int(body)
        return None if n < 0 else f.read(n + 2)[:-2].decode()
    n = int(body)
    if t == b"%":
        return [read_frame(f) for _ in range(2 * n)]
    return [read_frame(f) for _ in range(n)]


def cmd(*args) -> bytes:
    out = [f"*{len(args)}\r\n".encode()]
    for a in args:
        a = str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class Subscriber(threading.Thread):
    def __init__(self, name: str, resp3: bool, pattern: bool):
        super().__init__(daemon=True)
        self.name, self.got, self.dropped = name, [], None
        self.sock = socket.create_connection(("localhost", 16380))
        self.f = self.sock.makefile("rb")
        if resp3:
            self.sock.sendall(cmd("HELLO", 3, "AUTH", "app", "app-proxy-pw"))
        else:
            self.sock.sendall(cmd("AUTH", "app", "app-proxy-pw"))
        read_frame(self.f)
        self.sock.sendall(cmd("PSUBSCRIBE", CHANNEL + "*") if pattern else cmd("SUBSCRIBE", CHANNEL))
        read_frame(self.f)

    def run(self):
        try:
            while True:
                fr = read_frame(self.f)
                if isinstance(fr, list) and fr and fr[0] in ("message", "pmessage"):
                    self.got.append(int(fr[-1]))
        except (EOFError, OSError) as e:
            self.dropped = f"{type(e).__name__} at {time.strftime('%X')}"


class Publisher(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.acked, self.failed, self.seq, self.halt = [], 0, 0, threading.Event()

    def run(self):
        c = rc.app_client(timeout=5)
        while not self.halt.is_set():
            self.seq += 1
            try:
                c.publish(CHANNEL, self.seq)
                self.acked.append((self.seq, time.time()))
            except Exception:  # noqa: BLE001  the dead target during a failure rollback
                self.failed += 1
            time.sleep(0.005)

    def stop(self):
        self.halt.set()
        self.join(5)


def check(subs, pub, label, since=0.0, until=1e18, may_miss_between=None):
    # Only what was acknowledged a while ago: the latest publish may still be on its way to a
    # subscriber while the publisher keeps going, which is not a loss.
    until = min(until, time.time() - 0.5)
    want = [s for s, t in pub.acked if since <= t <= until]
    for s in subs:
        got = [x for x in s.got if want and want[0] <= x <= want[-1]]
        dup = len(got) - len(set(got))
        missing = sorted(set(want) - set(got))
        excused = [m for m in missing if may_miss_between and may_miss_between[0] <= m <= may_miss_between[1]]
        lost = [m for m in missing if m not in excused]
        rc.report(f"{label}: {s.name} 유실·중복 없음, 연결 유지", not lost and not dup and s.dropped is None,
                  f"보낸 {len(want)}, 받은 {len(got)}, 유실 {len(lost)}, 중복 {dup}, 장애 구간 유실 {len(excused)}, "
                  f"끊김 {s.dropped}" + (f", 빠진 번호 {lost[:5]} (마지막 발행 {want[-1]})" if lost else ""))


def proxy_stats() -> dict:
    px = rc.proxy_status()
    return {k: px[k] for k in ("pubsub_sessions", "pubsub_moved", "pubsub_cut", "pubsub_dropped", "pubsub_repaired")}


def main():
    print("fresh stack...", flush=True)
    rc.compose("up", "-d", "--build", *rc.SERVICES)
    w = rc.fresh()
    w.stop()   # the key writer of the recovery checks is not needed here
    subs = [Subscriber("RESP2 채널", False, False), Subscriber("RESP2 패턴", False, True),
            Subscriber("RESP3 채널", True, False)]
    for s in subs:
        s.start()
    pub = Publisher()
    pub.start()
    rc.to_ready()

    gcp = rc.admin("gcp")
    gcp.execute_command("ACL", "SETUSER", "app", "-subscribe", "-psubscribe")
    t0 = time.time()
    rc.API.post("/internal/execute")
    st = rc.wait_phase("ready", "executed", "failed", timeout=30)
    gcp.execute_command("ACL", "SETUSER", "app", "+subscribe", "+psubscribe")
    rc.report("대상에 구독할 수 없으면 컷오버 취소, aws 유지",
              rc.proxy_status()["active"] == "aws" and "presubscribe" in str((st.get("operation") or {}).get("error")),
              (st.get("operation") or {}).get("error"))
    time.sleep(1)
    check(subs, pub, "취소된 컷오버", since=t0, until=time.time())

    t0 = time.time()
    rc.API.post("/internal/execute")
    rc.report("컷오버", rc.wait_phase("executed", timeout=60).get("phase") == "executed", proxy_stats())
    time.sleep(1)
    check(subs, pub, "컷오버", since=t0, until=time.time())

    t0 = time.time()
    rc.API.post("/internal/rollback", json={"mode": "normal"})
    rc.report("정상 롤백", rc.wait_phase("rolled_back", timeout=60).get("phase") == "rolled_back", proxy_stats())
    time.sleep(1)
    check(subs, pub, "정상 롤백", since=t0, until=time.time())

    rc.to_executed()
    time.sleep(1)
    t_kill = time.time()
    rc.compose("kill", "redis-gcp")
    rc.API.post("/internal/rollback", json={"mode": "failure"})
    rc.report("장애 롤백", rc.wait_phase("rolled_back", timeout=30).get("phase") == "rolled_back", proxy_stats())
    time.sleep(2)
    pub.stop()
    time.sleep(0.5)
    around = [s for s, t in pub.acked if t_kill - 1 <= t <= t_kill + 1]
    check(subs, pub, "장애 롤백 전후", since=t_kill - 5,
          may_miss_between=(around[0], around[-1]) if around else None)
    px = proxy_stats()
    # pubsub_cut counts the failure rollback's moves: the dead side cannot answer the barrier.
    rc.report("세션 모두 유지, 끊어서 옮긴 세션 없음", px["pubsub_sessions"] == len(subs) and px["pubsub_dropped"] == 0,
              px)

    rc.compose("down", "-v", "-t", "0", "--remove-orphans")
    print(f"\n{sum(rc.results)}/{len(rc.results)} passed")
    sys.exit(0 if all(rc.results) else 1)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
