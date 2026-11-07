"""Control plane API. The Go proxy and Sync Worker are the data plane.

Config (env):
  STATEMIG_ENDPOINTS     admin URLs of every Redis ("aws=redis://default:pw@redis-aws:6379,gcp=...")
  STATEMIG_PROXY         admin URL of the central proxy (default http://localhost:7000)
  STATEMIG_SYNCWORKER    admin URL of the Sync Worker (default http://localhost:7100)
  STATEMIG_CONTROL_FILE  control record shared with the proxy (stand-in for the management DB)
  STATEMIG_APP_USER      ACL user the proxy uses towards Redis (fenced on the old side)
  STATEMIG_DIRECT_USER   account applications use to reach Redis directly outside a migration
                         (optional). Shut on the source once every application goes through the
                         proxy, opened on the side that stays when the migration ends
  STATEMIG_ADMIN_TOKEN   bearer token for this API, the proxy's and the Sync Worker's
"""
import asyncio
import hmac
import logging
import os
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .telemetry import TraceMiddleware, get_telemetry, publish_loop
from .backend import Endpoint
from .common import CommonApi, build_router
from .migration import Admin, ControlRecord, Migration, MigrationError, MigrationStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

ENDPOINTS = {
    name: Endpoint(name, url)
    for name, url in (item.split("=", 1) for item in os.environ.get(
        "STATEMIG_ENDPOINTS", "local=redis://localhost:6379").split(","))
}
TOKEN = os.environ["STATEMIG_ADMIN_TOKEN"]
proxy = Admin(os.environ.get("STATEMIG_PROXY", "http://localhost:7000"), TOKEN)
sync = Admin(os.environ.get("STATEMIG_SYNCWORKER", "http://localhost:7100"), TOKEN)
record = ControlRecord(os.environ.get("STATEMIG_CONTROL_FILE", "control.json"))
store = MigrationStore(str(record.path.with_name("migration.json")))
APP_USER = os.environ.get("STATEMIG_APP_USER", "app")
DIRECT_USER = os.environ.get("STATEMIG_DIRECT_USER") or None
if DIRECT_USER == APP_USER:
    raise RuntimeError("STATEMIG_DIRECT_USER must differ from STATEMIG_APP_USER: shutting it would cut the proxy off")
current: Migration | None = None
history: list[dict] = []


def require_token(authorization: str = Header("")):
    if not hmac.compare_digest(authorization, f"Bearer {TOKEN}"):
        raise HTTPException(401, "admin token required")


app = FastAPI(title="statemig – Redis zero-downtime migration", dependencies=[Depends(require_token)])


telemetry = get_telemetry("redis")
app.add_middleware(TraceMiddleware, telemetry=telemetry)


@app.exception_handler(RequestValidationError)
async def invalid_request(request: Request, exc: RequestValidationError):
    """The common API answers a malformed body with 400 INVALID_REQUEST in its error shape."""
    if not request.url.path.startswith("/v1/"):
        return await request_validation_exception_handler(request, exc)
    fields = sorted({".".join(str(p) for p in e["loc"][1:]) for e in exc.errors()})
    return JSONResponse({"code": "INVALID_REQUEST", "message": f"invalid or missing: {', '.join(fields)}",
                         "operationId": None, "blockers": fields}, status_code=400)


@app.on_event("startup")
async def recover_saved_migration():
    global current
    current = Migration.load(proxy, sync, record, store, ENDPOINTS, APP_USER, DIRECT_USER)
    if current:
        asyncio.create_task(current.recover())
    app.state.watch_task = asyncio.create_task(_watch())
    app.state.telemetry_task = asyncio.create_task(publish_loop(telemetry, collect_observations))


async def collect_observations():
    rows = []
    for mid, mig in list(common.data["migrations"].items())[:256]:
        try:
            rows.append(("REDIS", await common.view(mid)))
        except Exception:
            telemetry.snapshot("REDIS", {"workflowId": mig["workflowId"], "migrationId": mid},
                               error="OBSERVATION_UNAVAILABLE")
    return rows


@app.on_event("shutdown")
async def stop_observations():
    tasks = [app.state.watch_task, app.state.telemetry_task]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.to_thread(telemetry.close)


async def _watch():
    while True:
        await asyncio.sleep(1)
        common.tick()
        try:
            if current:
                await current.watch_once()
        except Exception:  # noqa: BLE001  the next round tries again
            logging.getLogger("statemig.api").exception("watch")


class StartReq(BaseModel):
    target: str
    budget_ms: int = 1800
    mode: str = "delta"    # delta | full
    timeout_s: int | None = Field(None, gt=0)    # abort if not ready by then; None = wait


class CutoverReq(BaseModel):
    budget_ms: int = 1800


class RollbackReq(BaseModel):
    mode: Literal["normal", "failure"] = "normal"
    budget_ms: int = 1800


def _need_current() -> Migration:
    if current is None:
        raise HTTPException(404, "no migration")
    return current


async def _run(coro):
    try:
        return await coro
    except MigrationError as e:
        raise HTTPException(409, str(e)) from e


@app.get("/status")
async def status():
    px = await proxy.call("GET", "/status")
    sw = await sync.call("GET", "/status")
    return {"proxy": px["proxy"], "sync": sw, "record": record.read(),
            "migration": current.status() if current else None, "history": history[-10:]}


async def _new_migration(req: StartReq) -> Migration:
    global current
    if req.target not in ENDPOINTS:
        raise HTTPException(400, f"unknown endpoint {req.target}")
    px = (await proxy.call("GET", "/status"))["proxy"]
    if req.target == px["active"]:
        raise HTTPException(400, "target is already active")
    if px["active"] != px["consume"]:
        raise HTTPException(409, "queue drain of the previous move is still running")
    if current and current.state in ("PRECHECK", "SYNCING", "DRAINING", "ROLLING_BACK_DRAIN"):
        view = await current.sync_view()
        restartable = current.state == "SYNCING" and (not view["running"] or view["halted"])
        if not restartable:     # a sync that is gone or halted may be prepared again; nothing was switched yet
            raise HTTPException(409, f"migration {current.source.name}→{current.target.name} is {current.state}")
    if record.read() is None:   # first run: record what the proxy is serving now
        record.write(gen=px["gen"], active=px["active"], consume=px["consume"], paused=False)
    if current:
        history.append(current.status())
        current.retire()
    current = Migration(proxy, sync, record, ENDPOINTS[px["active"]], ENDPOINTS[req.target], APP_USER, store, req.mode,
                        DIRECT_USER)
    return current


@app.post("/migrations")
async def start(req: StartReq):
    m = await _new_migration(req)
    await _run(m.start(req.budget_ms, req.timeout_s))
    return m.status()


@app.get("/migrations/current/ready")
async def ready():
    return {"ready": await _need_current().ready()}


@app.post("/migrations/current/cutover")
async def cutover(req: CutoverReq = CutoverReq()):
    return await _run(_need_current().cutover(req.budget_ms))


@app.post("/migrations/current/rollback")
async def rollback(req: RollbackReq = RollbackReq()):
    return await _run(_need_current().rollback(req.mode, req.budget_ms))


@app.post("/migrations/current/commit")
async def commit():
    await _run(_need_current().commit())
    return current.status()


# ---- common migration API, what the orchestrator calls: see common.py ------------------
async def _new_core(target: str, budget_ms: int) -> Migration:
    try:
        return await _new_migration(StartReq(target=target, budget_ms=budget_ms))
    except HTTPException as e:
        raise MigrationError(str(e.detail)) from e


common = CommonApi(proxy, sync, record, ENDPOINTS, str(record.path.with_name("common.json")), lambda: current, _new_core)
app.include_router(build_router(common))


# ---- internal shortcuts for the checks and the demo console ------------------------------
# Long steps answer 202 at once. Poll GET /internal/status.
PHASES = {"PRECHECK": "preparing", "SYNCING": "preparing", "DRAINING": "executing", "CUTOVER": "executed",
          "ROLLING_BACK_DRAIN": "rolling_back", "ROLLED_BACK": "rolled_back", "COMMITTED": "committed",
          "ABORTED": "aborted", "FAILED": "failed"}
RUNNING_PHASES = {"prepare": "preparing", "execute": "executing", "rollback": "rolling_back", "commit": "committing",
                  "abort": "aborting"}
operation: dict | None = None
operation_task: asyncio.Task | None = None


async def _track(op: dict, coro):
    try:
        op["result"] = await coro
        op["state"] = "succeeded"
    except Exception as e:  # noqa: BLE001  reported through /v1/status
        op["state"], op["error"] = "failed", str(e) or type(e).__name__    # httpx timeouts have no message


def _begin(name: str, coro) -> dict:
    global operation, operation_task
    operation = {"name": name, "state": "running", "error": None, "result": None}
    operation_task = asyncio.create_task(_track(operation, coro))
    return {"operation": name, "state": "running"}


def _need_idle():
    if operation and operation["state"] == "running":
        raise HTTPException(409, f"{operation['name']} is still running")


@app.post("/internal/prepare", status_code=202)
async def internal_prepare(req: StartReq):
    _need_idle()
    m = await _new_migration(req)
    return _begin("prepare", m.start(req.budget_ms, req.timeout_s))


@app.post("/internal/execute", status_code=202)
async def internal_execute(req: CutoverReq = CutoverReq()):
    _need_idle()
    return _begin("execute", _need_current().cutover(req.budget_ms))


@app.post("/internal/rollback", status_code=202)
async def internal_rollback(req: RollbackReq = RollbackReq()):
    _need_idle()
    return _begin("rollback", _need_current().rollback(req.mode, req.budget_ms))


@app.post("/internal/commit", status_code=202)
async def internal_commit():
    _need_idle()
    return _begin("commit", _need_current().commit())


@app.post("/internal/abort", status_code=202)
async def internal_abort():
    _need_idle()
    return _begin("abort", _need_current().abort())


@app.get("/internal/status")
async def internal_status():
    running = operation and operation["state"] == "running"
    m = current
    ready = False
    if m and not running:
        try:
            ready = await m.ready()
        except Exception:  # noqa: BLE001  the Sync Worker is down: not ready, and status still answers
            pass
    phase = RUNNING_PHASES[operation["name"]] if running else "ready" if ready else PHASES[m.state] if m else "idle"
    return {"resource": "redis", "phase": phase, "ready": ready, "source": m.source.name if m else None,
            "target": m.target.name if m else None, "active": (record.read() or {}).get("active"),
            "operation": operation, "last_cutover": m.last_cutover if m else None,
            "sync": await m.sync_view() if m else None, "abort_reason": m.abort_reason if m else None,
            "events": m.events[-20:] if m else []}
