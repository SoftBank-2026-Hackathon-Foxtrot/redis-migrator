"""Control plane API. The Go proxy and Sync Worker are the data plane.

Config (env):
  STATEMIG_ENDPOINTS     admin URLs of every Redis ("aws=redis://default:pw@redis-aws:6379,gcp=...")
  STATEMIG_PROXY         admin URL of the central proxy (default http://localhost:7000)
  STATEMIG_SYNCWORKER    admin URL of the Sync Worker (default http://localhost:7100)
  STATEMIG_CONTROL_FILE  control record shared with the proxy (stand-in for the management DB)
  STATEMIG_APP_USER      ACL user the proxy uses towards Redis (fenced on the old side)
  STATEMIG_ADMIN_TOKEN   bearer token for this API, the proxy's and the Sync Worker's
"""
import asyncio
import hmac
import logging
import os

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from .backend import Endpoint
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
current: Migration | None = None
history: list[dict] = []


def require_token(authorization: str = Header("")):
    if not hmac.compare_digest(authorization, f"Bearer {TOKEN}"):
        raise HTTPException(401, "admin token required")


app = FastAPI(title="statemig – Redis zero-downtime migration", dependencies=[Depends(require_token)])


@app.on_event("startup")
async def recover_saved_migration():
    global current
    current = Migration.load(proxy, sync, record, store, ENDPOINTS, APP_USER)
    if current:
        asyncio.create_task(current.recover())


class StartReq(BaseModel):
    target: str
    budget_ms: int = 1800
    mode: str = "delta"    # delta | full


class CutoverReq(BaseModel):
    budget_ms: int = 1800


class RollbackReq(BaseModel):
    mode: str = "normal"   # normal | failure
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


@app.post("/migrations")
async def start(req: StartReq):
    global current
    if req.target not in ENDPOINTS:
        raise HTTPException(400, f"unknown endpoint {req.target}")
    px = (await proxy.call("GET", "/status"))["proxy"]
    if req.target == px["active"]:
        raise HTTPException(400, "target is already active")
    if px["active"] != px["consume"]:
        raise HTTPException(409, "queue drain of the previous move is still running")
    if current and current.state in ("PRECHECK", "SYNCING", "DRAINING", "ROLLING_BACK_DRAIN"):
        raise HTTPException(409, f"migration {current.source.name}→{current.target.name} is {current.state}")
    if record.read() is None:   # first run: record what the proxy is serving now
        record.write(gen=px["gen"], active=px["active"], consume=px["consume"], paused=False)
    if current:
        history.append(current.status())
    current = Migration(proxy, sync, record, ENDPOINTS[px["active"]], ENDPOINTS[req.target], APP_USER, store, req.mode)
    await _run(current.start(req.budget_ms))
    return current.status()


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
