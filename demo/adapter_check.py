"""The common migration API, called the way the orchestrator calls it (run on the host, stack up).

  plan → PREPARE → START_SYNC → QUIESCE → CUTOVER → RESUME TARGET → QUIESCE → ROLLBACK → RESUME SOURCE
  → FINALIZE, then a second plan through CANCEL and a third through FINALIZE, with a session kept throughout.
"""
import sys
import time
import uuid

import httpx

API = httpx.Client(base_url="http://localhost:18090", headers={"Authorization": "Bearer demo-admin-token"}, timeout=30)
APP = httpx.Client(base_url="http://localhost:18000", timeout=30)
TX = {"transitionId": "tx-001", "quiesceRef": "quiesce:redis-main:tx-001:1"}
failed = 0


def check(name: str, ok: bool, detail=""):
    global failed
    failed += not ok
    print(f"{'PASS' if ok else 'FAIL'}  {name} {detail}")


def key() -> dict:
    return {"Idempotency-Key": uuid.uuid4().hex}


def plan(**over) -> dict:
    return {"workflowId": "wf-001", "resourceId": "redis-main", "kind": "REDIS", "profile": "redis-proxy-sync-v1",
            "sourceRef": "resource:aws", "targetRef": "resource:gcp", "bindingRef": "binding:redis-proxy",
            "policy": {"rollbackMode": "REVERSE_REQUIRED", "maxQuiesceSeconds": 1.8}, **over}


def status(mid: str) -> dict:
    return API.get(f"/v1/migrations/{mid}").json()


def act(mid: str, action: str, **extra) -> dict:
    """Sends a command and waits for its operation. Returns the operation."""
    st = status(mid)
    r = API.post(f"/v1/migrations/{mid}/actions", headers=key(),
                 json={"action": action, "expectedRevision": st["revision"], **extra})
    assert r.status_code == 202, r.text
    end = time.time() + 60
    while time.time() < end:
        op = API.get(r.json()["statusUrl"]).json()
        if op["status"] not in ("QUEUED", "RUNNING"):
            return op
        time.sleep(0.1)
    return op


def wait(mid: str, lifecycle: str, timeout: float = 60) -> dict:
    end = time.time() + timeout
    while time.time() < end:
        st = status(mid)
        if st["lifecycle"] == lifecycle:
            return st
        time.sleep(0.2)
    return st


def session_alive(sid: str) -> bool:
    return APP.get("/me", headers={"x-session": sid}).status_code == 200


def main():
    APP.post("/seed")
    sid = APP.post("/login/adapter").json()["sid"]

    check("no token is refused", httpx.get("http://localhost:18090/v1/capabilities").status_code == 401)
    caps = API.get("/v1/capabilities").json()
    check("capabilities list the profile", caps["profiles"][0]["profile"] == "redis-proxy-sync-v1"
          and "resource:gcp" in caps["profiles"][0]["resources"])
    check("a plan needs an Idempotency-Key", API.post("/v1/migrations", json=plan()).status_code == 400)
    r = API.post("/v1/migrations", headers=key(), json=plan(profile="nope"))
    check("an unknown profile is refused", r.status_code == 422 and r.json()["code"] == "UNSUPPORTED_PROFILE")
    r = API.post("/v1/migrations", headers=key(), json=plan(targetRef="resource:nope"))
    check("an unknown resource is refused", r.status_code == 422 and r.json()["code"] == "PRECONDITION_FAILED")

    k = key()
    r = API.post("/v1/migrations", headers=k, json=plan())
    mid = r.json()["migrationId"]
    check("a plan answers 201 with a Location", r.status_code == 201 and r.headers["location"].endswith(mid))
    again = API.post("/v1/migrations", headers=k, json=plan())
    check("the same key and body return the same plan", again.status_code == 201 and again.json()["migrationId"] == mid)
    r = API.post("/v1/migrations", headers=k, json=plan(workflowId="other"))
    check("the same key with another body is refused", r.status_code == 409 and r.json()["code"] == "IDEMPOTENCY_CONFLICT")
    r = API.post("/v1/migrations", headers=key(), json=plan())
    check("a second plan is refused while one runs", r.status_code == 409 and r.json()["code"] == "RESOURCE_BUSY")
    check("a new plan touches nothing", status(mid)["lifecycle"] == "CREATED" and status(mid)["observed"]["route"] == "SOURCE")

    r = API.post(f"/v1/migrations/{mid}/actions", headers=key(), json={"action": "PREPARE", "expectedRevision": 99})
    check("a stale revision is refused", r.status_code == 409 and r.json()["code"] == "REVISION_CONFLICT")
    r = API.post(f"/v1/migrations/{mid}/actions", headers=key(),
                 json={"action": "CUTOVER", "expectedRevision": status(mid)["revision"], **TX})
    check("a command out of order is refused", r.status_code == 409 and r.json()["code"] == "INVALID_STATE")

    k = key()
    body = {"action": "PREPARE", "expectedRevision": status(mid)["revision"]}
    r1 = API.post(f"/v1/migrations/{mid}/actions", headers=k, json=body)
    r2 = API.post(f"/v1/migrations/{mid}/actions", headers=k, json=body)
    check("a command answers 202; a retry gets the same operation", r1.status_code == 202 and r2.status_code == 202
          and r1.json()["operationId"] == r2.json()["operationId"] and "location" in r1.headers)
    wait(mid, "PREPARED")
    check("PREPARE reaches PREPARED", status(mid)["lifecycle"] == "PREPARED")

    op = act(mid, "START_SYNC")
    st = wait(mid, "READY")
    check("START_SYNC reaches READY", op["status"] == "SUCCEEDED" and st["lifecycle"] == "READY", st["readiness"]["cutover"])

    r = API.post(f"/v1/migrations/{mid}/actions", headers=key(), json={"action": "QUIESCE", "expectedRevision": st["revision"]})
    check("QUIESCE needs a transitionId and quiesceRef", r.status_code == 400)
    op = act(mid, "QUIESCE", **TX)
    st = status(mid)
    check("QUIESCE only checks: writes stay open", op["status"] == "SUCCEEDED" and st["lifecycle"] == "QUIESCED"
          and st["observed"]["businessWriteAccess"] == "SOURCE_ONLY" and st["transitionState"] == "OPEN")
    r = API.post(f"/v1/migrations/{mid}/actions", headers=key(),
                 json={"action": "CUTOVER", "expectedRevision": st["revision"], **TX, "transitionId": "tx-other"})
    check("another transitionId is refused", r.status_code == 409)

    op = act(mid, "CUTOVER", **TX)
    st = status(mid)
    check("CUTOVER reaches SWITCHED on the target", op["status"] == "SUCCEEDED" and st["lifecycle"] == "SWITCHED"
          and st["observed"]["route"] == "TARGET" and st["observed"]["targetWritesPossible"], op["result"])
    check("session survives the cutover", session_alive(sid))
    r = API.post(f"/v1/migrations/{mid}/actions", headers=key(),
                 json={"action": "RESUME", "expectedRevision": st["revision"], **TX, "expectedLocation": "SOURCE"})
    check("RESUME with the wrong location is refused", r.status_code == 409)
    op = act(mid, "RESUME", **TX, expectedLocation="TARGET")
    st = status(mid)
    check("RESUME TARGET reaches ACTIVE", op["status"] == "SUCCEEDED" and st["lifecycle"] == "ACTIVE")
    codes = {c["code"]: c for c in st["checks"]}
    check("the required checks are all there", {"QUIESCE_COMPLETE", "CUTOVER_SAFE", "RESUME_SAFE", "ROLLBACK_SAFE",
                                                "RESOURCE_HEALTH", "ROUTE_MATCH", "WORKLOAD_RUNNING", "BUSINESS_PROBE",
                                                "FOLLOWUP_COMPLETE", "STABILITY_WINDOW"} <= set(codes)
          and all(c["evidenceRef"] and c["validUntil"] for c in codes.values()))
    check("advance is not PASS without the orchestrator's evidence", st["readiness"]["advance"]["result"] != "PASS",
          st["readiness"]["advance"])
    end = time.time() + 30
    while time.time() < end and status(mid)["details"]["redis"]["drainStatus"] != "SUCCEEDED":
        time.sleep(0.2)
    st = status(mid)
    check("details.redis follows the drain", st["details"]["redis"]["drainStatus"] == "SUCCEEDED"
          and st["details"]["redis"]["consumerRoute"] == "TARGET")
    check("rollback is ready after the drain", st["readiness"]["rollback"]["result"] == "PASS"
          and st["rollback"]["method"] == "REVERSE_SYNC", st["readiness"]["rollback"])

    op = act(mid, "RECONCILE")
    st2 = status(mid)
    check("RECONCILE changes nothing", op["status"] == "SUCCEEDED" and st2["lifecycle"] == "ACTIVE"
          and st2["observed"]["route"] == "TARGET")

    act(mid, "QUIESCE", **TX)
    op = act(mid, "ROLLBACK", **TX)
    st = status(mid)
    check("ROLLBACK returns to the source", op["status"] == "SUCCEEDED" and st["lifecycle"] == "ROLLED_BACK_HELD"
          and st["observed"]["route"] == "SOURCE", op["error"])
    check("session survives the rollback", session_alive(sid))
    op = act(mid, "RESUME", **TX, expectedLocation="SOURCE")
    check("RESUME SOURCE reaches RESTORED", op["status"] == "SUCCEEDED" and status(mid)["lifecycle"] == "RESTORED")
    op = act(mid, "FINALIZE")
    st = status(mid)
    check("FINALIZE completes the plan", op["status"] == "SUCCEEDED" and st["lifecycle"] == "COMPLETED"
          and st["transitionState"] == "CLOSED", op["error"])

    mid = API.post("/v1/migrations", headers=key(), json=plan(workflowId="wf-002")).json()["migrationId"]
    act(mid, "PREPARE")
    act(mid, "START_SYNC")
    op = act(mid, "CANCEL")
    st = status(mid)
    check("CANCEL stops the sync and keeps the source", op["status"] == "SUCCEEDED" and st["lifecycle"] == "CANCELLED"
          and st["observed"]["route"] == "SOURCE")
    r = API.post(f"/v1/migrations/{mid}/actions", headers=key(), json={"action": "START_SYNC", "expectedRevision": st["revision"]})
    check("a cancelled plan takes no more commands", r.status_code == 409)

    mid = API.post("/v1/migrations", headers=key(), json=plan(workflowId="wf-003")).json()["migrationId"]
    act(mid, "PREPARE")
    act(mid, "START_SYNC")
    wait(mid, "READY")
    act(mid, "QUIESCE", **{**TX, "transitionId": "tx-003"})
    act(mid, "CUTOVER", **{**TX, "transitionId": "tx-003"})
    act(mid, "RESUME", **{**TX, "transitionId": "tx-003"}, expectedLocation="TARGET")
    end = time.time() + 30
    while time.time() < end and status(mid)["details"]["redis"]["drainStatus"] != "SUCCEEDED":
        time.sleep(0.2)
    op = act(mid, "FINALIZE")
    st = status(mid)
    check("FINALIZE on the target finishes the move", op["status"] == "SUCCEEDED" and st["lifecycle"] == "COMPLETED"
          and st["observed"]["route"] == "TARGET", op["error"])
    check("session survives the whole move", session_alive(sid))

    print("ALL OK" if not failed else f"{failed} FAILED")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
