"""The common migration API, called the way the orchestrator calls it (run on the host, stack up).

  prepare → execute → rollback, then prepare → execute → commit, with a session kept throughout.
"""
import sys
import time

import httpx

API = httpx.Client(base_url="http://localhost:18090", headers={"Authorization": "Bearer demo-admin-token"}, timeout=30)
APP = httpx.Client(base_url="http://localhost:18000", timeout=30)
failed = 0


def check(name: str, ok: bool, detail=""):
    global failed
    failed += not ok
    print(f"{'PASS' if ok else 'FAIL'}  {name} {detail}")


def wait_phase(*phases: str, timeout: float = 90) -> dict:
    end = time.time() + timeout
    while time.time() < end:
        st = API.get("/v1/status").json()
        if st["phase"] in phases:
            return st
        if st["operation"] and st["operation"]["state"] == "failed":
            return st
        time.sleep(0.3)
    return st


def session_alive(sid: str) -> bool:
    return APP.get("/me", headers={"x-session": sid}).status_code == 200


def main():
    APP.post("/seed")
    sid = APP.post("/login/adapter").json()["sid"]

    check("no token is refused", httpx.get("http://localhost:18090/v1/status").status_code == 401)
    check("idle before prepare", API.get("/v1/status").json()["phase"] == "idle")
    check("execute before prepare is refused", API.post("/v1/execute").status_code == 404)
    check("unknown target is refused", API.post("/v1/prepare", json={"target": "nope"}).status_code == 400)
    check("unknown rollback mode is refused", API.post("/v1/rollback", json={"mode": "nope"}).status_code == 422)

    r = API.post("/v1/prepare", json={"target": "gcp"})
    check("prepare answers 202", r.status_code == 202, r.json())
    st = wait_phase("ready")
    check("prepare reaches ready", st["phase"] == "ready" and st["ready"], st["operation"])

    r = API.post("/v1/execute")
    check("execute answers 202", r.status_code == 202)
    check("a second step while one runs is refused", API.post("/v1/execute").status_code == 409)
    st = wait_phase("executed")
    check("execute reaches executed on gcp", st["phase"] == "executed" and st["active"] == "gcp", st["operation"])
    check("session survives the cutover", session_alive(sid))
    check("write pause is reported", (st["last_cutover"] or {}).get("paused_ms") is not None, st["last_cutover"])

    API.post("/v1/rollback", json={"mode": "normal"})
    st = wait_phase("rolled_back")
    check("rollback returns to aws", st["phase"] == "rolled_back" and st["active"] == "aws", st["operation"])
    check("session survives the rollback", session_alive(sid))

    API.post("/v1/prepare", json={"target": "gcp"})
    wait_phase("ready")
    API.post("/v1/execute")
    wait_phase("executed")
    API.post("/v1/commit")
    st = wait_phase("committed")
    check("commit finishes the move", st["phase"] == "committed" and st["active"] == "gcp", st["operation"])
    check("session survives the commit", session_alive(sid))

    print("ALL OK" if not failed else f"{failed} FAILED")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
