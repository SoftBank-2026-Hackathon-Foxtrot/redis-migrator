"""The migration API shared by every resource adapter (common design doc, draft 0.2).

This module only translates: plans, commands, retries and the status format. The move itself
stays in Migration. Where the draft and the Redis design differ, Redis behaviour is unchanged:
  QUIESCE   checks only; the proxy holds writes inside CUTOVER, not across separate calls
  CUTOVER   runs the whole switch, resume included
  RESUME    confirms the location; the writes already flow there
  ROLLBACK  resumes inside it too. mode=failure is the rollback that accepts loss

Plans, commands and retries are kept in a JSON file next to the control record, standing in
for the management DB. quiesceRef is required but not verified: that needs the shared DB.
"""
import asyncio
import copy
import hashlib
import json
import logging
import time
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Header, Response
from fastapi.responses import JSONResponse
from opentelemetry.trace import Status, StatusCode
from pydantic import BaseModel, Field, model_validator

from .bindings import BindingError, BindingReq, EndpointInput
from .migration import ControlRecord, MigrationError, _why
from .telemetry import get_telemetry

log = logging.getLogger("statemig.common")

PROFILE = "redis-proxy-sync-v1"
BINDING = "binding:redis-proxy"
ACTIONS = ["PREPARE", "START_SYNC", "QUIESCE", "CUTOVER", "RESUME", "ROLLBACK", "CANCEL", "RECONCILE", "FINALIZE"]
NEEDS_TRANSITION = {"QUIESCE", "CUTOVER", "RESUME", "ROLLBACK"}
ALLOWED = {
    "CREATED": ["PREPARE", "CANCEL"],
    "PREPARED": ["START_SYNC", "CANCEL"],
    "SYNCING": ["CANCEL"],
    "READY": ["QUIESCE", "CANCEL"],
    "QUIESCED": ["CUTOVER", "CANCEL"],
    "SWITCHED": ["RESUME", "ROLLBACK"],
    "ACTIVE": ["QUIESCE", "ROLLBACK", "FINALIZE"],
    "ROLLED_BACK_HELD": ["RESUME"],
    "RESTORED": ["FINALIZE"],
    "FAILED": ["CANCEL"],
    "CANCELLED": [],
    "COMPLETED": [],
}
TERMINAL = {"CANCELLED", "COMPLETED"}
EVIDENCE_TTL_S = 30
STABILITY_WINDOW_S = 30
DEFAULT_QUIESCE_S = 1.8


class Policy(BaseModel):
    rollbackMode: str = "REVERSE_REQUIRED"
    maxQuiesceSeconds: float = Field(DEFAULT_QUIESCE_S, gt=0)
    readyTimeoutSeconds: int | None = Field(None, gt=0)


class CreateReq(BaseModel):
    workflowId: str
    resourceId: str
    kind: str = "REDIS"
    profile: str = PROFILE
    sourceRef: str | None = None
    targetRef: str | None = None
    bindingRef: str | None = None
    source: EndpointInput | None = None
    target: EndpointInput | None = None
    workloadScopeRef: str
    policy: Policy = Policy()

    @model_validator(mode="after")
    def endpoints_present(self):
        if self.source is not None or self.target is not None:
            if self.source is None or self.target is None:
                raise ValueError("source and target must be supplied together")
            if self.sourceRef is not None or self.targetRef is not None:
                raise ValueError("source/target objects cannot be mixed with sourceRef/targetRef")
        elif not self.sourceRef or not self.targetRef:
            raise ValueError("source and target are required")
        return self


class ActionReq(BaseModel):
    action: str
    expectedRevision: int
    transitionId: str | None = None
    quiesceRef: str | None = None
    expectedLocation: str | None = None
    mode: str = "normal"    # ROLLBACK: normal | failure


class Refused(Exception):
    def __init__(self, status: int, code: str, message: str, blockers: list[str] | None = None):
        super().__init__(message)
        self.status, self.code, self.message, self.blockers = status, code, message, blockers or []


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _error(e: Refused) -> JSONResponse:
    return JSONResponse({"code": e.code, "message": e.message, "operationId": None, "blockers": e.blockers},
                        status_code=e.status)


def _decision(result: str, *reasons: str) -> dict:
    return {"result": result, "reasons": list(reasons)}


class Store(ControlRecord):
    """Migrations, operations and idempotency records in one atomically replaced file."""

    def load(self) -> dict:
        return self.read() or {"migrations": {}, "operations": {}, "idempotency": {}, "core": None}

    def save(self, data: dict):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data))
        tmp.replace(self.path)


class CommonApi:
    def __init__(self, proxy, sync, record, endpoints, store_path, get_current, new_core, bindings=None):
        self.proxy, self.sync, self.record, self.endpoints = proxy, sync, record, endpoints
        self.get_current, self.new_core = get_current, new_core
        self.bindings = bindings
        self.mutation_lock = asyncio.Lock()
        self.store = Store(store_path)
        self.data = self.store.load()
        self.tasks: set[asyncio.Task] = set()
        # A command that was running when the process died: its effect is unknown until RECONCILE.
        for op in self.data["operations"].values():
            if op["status"] in ("QUEUED", "RUNNING"):
                op["status"] = "UNKNOWN"
                op["error"] = self._op_error(op, "UNKNOWN_STATE", "the adapter restarted during this command")
                self.data["migrations"][op["migrationId"]]["activeOperationId"] = None
                self.data["migrations"][op["migrationId"]]["unknownOperationId"] = op["operationId"]
        self.store.save(self.data)

    # ---- lookups ---------------------------------------------------------------------
    def _mig(self, migration_id: str) -> dict:
        mig = self.data["migrations"].get(migration_id)
        if not mig:
            raise Refused(404, "NOT_FOUND", f"no migration {migration_id}")
        return mig

    def _core(self, mig: dict):
        return self.get_current() if self.data.get("core") == mig["migrationId"] else None

    def _bump(self, mig: dict, lifecycle: str | None = None):
        if lifecycle:
            mig["lifecycle"] = lifecycle
        mig["revision"] += 1
        self.store.save(self.data)

    def _endpoint(self, ref: str) -> str:
        name = ref.removeprefix("resource:")
        if not ref.startswith("resource:") or name not in self.endpoints:
            raise Refused(422, "PRECONDITION_FAILED", f"unknown resource {ref}")
        return name

    # ---- plans -----------------------------------------------------------------------
    def capabilities(self) -> dict:
        # What PREPARE refuses on either side, since each side is written by the sync in turn.
        conditions = ["Redis 6+ or Valkey 7+, same family on both sides (DUMP/RESTORE, else per-type copy)",
                      "single node or primary/replica: cluster mode off", "keyspace notifications on (KEA)",
                      "maxmemory-policy noeviction when maxmemory is set",
                      f"registered resource: {', '.join(f'resource:{n}' for n in self.endpoints)}"]
        return {"apiVersion": "v1", "bindingRegistration": "/v1/bindings", "profiles": [{
            "kind": "REDIS", "profile": PROFILE, "sourceConditions": conditions, "targetConditions": conditions,
            "actions": ACTIONS, "rollbackModes": ["REVERSE_REQUIRED"],
            "actionSemantics": {"QUIESCE": "CHECKS_ONLY", "CUTOVER": "PAUSE_SWITCH_RESUME",
                                "RESUME": "CONFIRM_LOCATION", "globalHoldOwner": "ORCHESTRATOR",
                                "failureRollbackAcceptsDataLoss": True},
            "constraints": ["no Redis Cluster", "each db keeps its number; queues in db 0", "keyspace notifications required", "same engine family", "database input must be string 0; all logical DBs keep their numbers",
                            "QUIESCE checks only; CUTOVER holds, switches and resumes writes in one command",
                            "ROLLBACK mode=failure accepts the loss of what the reverse sync had not copied"],
        }]}

    def ensure_available(self):
        for mig in self.data["migrations"].values():
            if mig.get("unknownOperationId"):
                raise Refused(409, "UNKNOWN_STATE", "reconcile the previous operation first")
            if mig["lifecycle"] not in TERMINAL or mig.get("activeOperationId"):
                raise Refused(409, "RESOURCE_BUSY", "the previous migration is unfinished")

    async def create_plan(self, req: CreateReq) -> dict:
        self.ensure_available()
        if req.kind != "REDIS" or req.profile != PROFILE or req.policy.rollbackMode != "REVERSE_REQUIRED":
            raise Refused(422, "UNSUPPORTED_PROFILE", f"supported: REDIS, {PROFILE}, REVERSE_REQUIRED")
        if req.source is not None:
            if not self.bindings:
                raise Refused(422, "PRECONDITION_FAILED", "binding registration is unavailable")
            binding = self.bindings.data["binding"]
            if req.bindingRef and (not binding or req.bindingRef != binding["bindingRef"]):
                raise Refused(422, "PRECONDITION_FAILED", "binding does not belong to this application")
            try:
                source, target, binding = await self.bindings.prepare(req.source, req.target, req.resourceId)
            except BindingError as e:
                raise Refused(e.status, e.code, str(e)) from None
            req = req.model_copy(update={"sourceRef": source, "targetRef": target, "bindingRef": binding})
        else:
            req = req.model_copy(update={"bindingRef": req.bindingRef or BINDING})
            px = (await self.proxy.call("GET", "/status"))["proxy"]
            if px.get("paused") or px.get("active") != px.get("consume"):
                raise Refused(409, "RESOURCE_BUSY", "proxy is paused or queue drain is unfinished")
            if px.get("active") != self._endpoint(req.sourceRef):
                raise Refused(409, "SOURCE_MISMATCH", "source is not the proxy's active Redis")
        return self.create(req)

    async def binding_view(self, binding_id: str) -> dict:
        if not self.bindings:
            raise BindingError("binding was not found", "NOT_FOUND", 404)
        result = await self.bindings.view(binding_id)
        migrations = [m for m in self.data["migrations"].values() if m["bindingRef"] == result["bindingRef"]]
        result["lastMigrationId"] = migrations[-1]["migrationId"] if migrations else None
        return result

    def create(self, req: CreateReq) -> dict:
        if req.kind != "REDIS" or req.profile != PROFILE or req.policy.rollbackMode != "REVERSE_REQUIRED":
            raise Refused(422, "UNSUPPORTED_PROFILE", f"supported: REDIS, {PROFILE}, REVERSE_REQUIRED")
        binding = self.bindings.data["binding"] if self.bindings else None
        if req.bindingRef not in (BINDING, binding["bindingRef"] if binding else None):
            raise Refused(422, "PRECONDITION_FAILED", f"unknown binding {req.bindingRef}")
        source, target = self._endpoint(req.sourceRef), self._endpoint(req.targetRef)
        if source == target:
            raise Refused(422, "PRECONDITION_FAILED", "source and target are the same resource")
        self.ensure_available()
        migration_id = f"mig-{uuid.uuid4().hex[:12]}"
        self.data["migrations"][migration_id] = {
            **req.model_dump(), "migrationId": migration_id, "source": source, "target": target, "revision": 1,
            "lifecycle": "CREATED", "health": "OK", "activeOperationId": None, "unknownOperationId": None,
            "transitionId": None, "transitionState": "NONE", "targetWritesPossible": False, "cutOver": False,
            "stableSince": None,
        }
        self.store.save(self.data)
        return self.data["migrations"][migration_id]

    # ---- commands --------------------------------------------------------------------
    def _allowed(self, mig: dict, lifecycle: str) -> list[str]:
        if lifecycle == "QUIESCED" and mig.get("returning"):    # on the target, on the way back
            return ["ROLLBACK"]
        return ALLOWED[lifecycle]

    def _check_action(self, mig: dict, req: ActionReq):
        if mig["lifecycle"] in TERMINAL:
            raise Refused(409, "INVALID_STATE", "completed migration history cannot be changed")
        if req.action not in ACTIONS:
            raise Refused(400, "INVALID_REQUEST", f"unknown action {req.action}")
        if mig["activeOperationId"]:
            raise Refused(409, "RESOURCE_BUSY", f"{mig['activeOperationId']} is running")
        if mig["revision"] != req.expectedRevision:
            raise Refused(409, "REVISION_CONFLICT", f"revision is {mig['revision']}, not {req.expectedRevision}")
        if req.action == "RECONCILE":
            return
        if mig["unknownOperationId"]:
            raise Refused(409, "UNKNOWN_STATE", f"{mig['unknownOperationId']} is unknown: RECONCILE first")
        lifecycle = self._lifecycle(mig)
        if req.action not in self._allowed(mig, lifecycle):
            raise Refused(409, "INVALID_STATE", f"{req.action} is not allowed in {lifecycle}")
        if req.action in NEEDS_TRANSITION:
            if not req.transitionId or not req.quiesceRef:
                raise Refused(400, "INVALID_REQUEST", f"{req.action} needs transitionId and quiesceRef")
            if mig["transitionId"] and mig["transitionId"] != req.transitionId:
                raise Refused(409, "INVALID_STATE", f"transition is {mig['transitionId']}")
            if not mig["transitionId"] and req.action != "QUIESCE":
                raise Refused(409, "INVALID_STATE", "no transition yet: QUIESCE first")
        if req.action == "RESUME":
            want = "TARGET" if lifecycle == "SWITCHED" else "SOURCE"
            if req.expectedLocation != want:
                raise Refused(409, "INVALID_STATE", f"expectedLocation must be {want}")
        if req.action == "ROLLBACK" and req.mode not in ("normal", "failure"):
            raise Refused(400, "INVALID_REQUEST", "mode must be normal or failure")

    def accept(self, migration_id: str, req: ActionReq) -> dict:
        mig = self._mig(migration_id)
        self._check_action(mig, req)
        op_id = f"op-{uuid.uuid4().hex[:12]}"
        op = {"operationId": op_id, "migrationId": migration_id, "action": req.action, "status": "QUEUED",
              "statusUrl": f"/v1/operations/{op_id}", "result": None, "error": None, "createdAt": _now()}
        self.data["operations"][op_id] = op
        mig["activeOperationId"] = op_id
        self.store.save(self.data)
        task = asyncio.create_task(self._execute(mig, op, req))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return op

    async def _execute(self, mig: dict, op: dict, req: ActionReq):
        telemetry = get_telemetry("redis")
        with telemetry.operation(req.action, workflow=mig["workflowId"],
                                 migration=mig["migrationId"], operation=op["operationId"]) as span:
            await self._execute_work(mig, op, req)
            if op["status"] != "SUCCEEDED":
                span.set_status(Status(StatusCode.ERROR, op["status"]))
            try:
                telemetry.snapshot("REDIS", await self.view(mig["migrationId"]))
            except Exception:
                log.warning("operation observation unavailable")

    async def _execute_work(self, mig: dict, op: dict, req: ActionReq):
        op["status"] = "RUNNING"
        op["startedAt"] = _now()
        self.store.save(self.data)
        before = await self._observe(mig)
        try:
            op["result"] = await getattr(self, f"_do_{req.action.lower()}")(mig, req)
            op["status"] = "SUCCEEDED"
        except MigrationError as e:
            op["status"], op["error"] = "FAILED", self._op_error(op, "PRECONDITION_FAILED", str(e))
        except Refused as e:
            op["status"], op["error"] = "FAILED", self._op_error(op, e.code, e.message, e.blockers)
        except Exception as e:  # noqa: BLE001  a call that may or may not have taken effect
            log.exception("%s", req.action)
            op["status"], op["error"] = "UNKNOWN", self._op_error(op, "UNKNOWN_STATE", _why(e))
            mig["unknownOperationId"] = op["operationId"]
        op["finishedAt"] = _now()
        op["confirmedChanges"] = _changes(req.action, op, before, await self._observe(mig))
        mig["activeOperationId"] = None
        mig["revision"] += 1
        if mig["lifecycle"] in TERMINAL and not mig.get("unknownOperationId"):
            await self.view(mig["migrationId"])
        self.store.save(self.data)

    @staticmethod
    def _op_error(op: dict, code: str, message: str, blockers: list[str] | None = None) -> dict:
        return {"code": code, "message": message, "operationId": op["operationId"], "blockers": blockers or []}

    def _budget_ms(self, mig: dict) -> int:
        return int(mig["policy"]["maxQuiesceSeconds"] * 1000)

    async def _do_prepare(self, mig: dict, req: ActionReq) -> dict:
        px = (await self.proxy.call("GET", "/status"))["proxy"]
        if px["active"] != mig["source"]:
            raise Refused(422, "PRECONDITION_FAILED", f"the proxy is on {px['active']}, not {mig['source']}")
        core = await self.new_core(mig["target"], self._budget_ms(mig))
        self.data["core"] = mig["migrationId"]
        report = await core.check(self._budget_ms(mig))
        mig["lifecycle"] = "PREPARED"
        return {"precheck": report}

    async def _do_start_sync(self, mig: dict, req: ActionReq) -> dict:
        core = self._core(mig)
        await core.start_sync(mig["policy"].get("readyTimeoutSeconds"))
        mig["lifecycle"] = "SYNCING"
        return {"started": True}

    async def _do_quiesce(self, mig: dict, req: ActionReq) -> dict:
        core = self._core(mig)
        if mig["lifecycle"] != "ACTIVE":
            if why := await core.not_ready():
                raise MigrationError(f"not ready ({why})")
            await core.before_pause(self._budget_ms(mig))
        mig["transitionId"], mig["transitionState"] = req.transitionId, "OPEN"
        mig["returning"] = mig["lifecycle"] == "ACTIVE"
        mig["lifecycle"] = "QUIESCED"
        return {"checked": True, "writesHeld": False}

    async def _do_cutover(self, mig: dict, req: ActionReq) -> dict:
        # The intent goes on record first: from here the target may take writes, even if the
        # answer is lost. It never goes back to false for this plan.
        mig["targetWritesPossible"] = True
        self.store.save(self.data)
        res = await self._core(mig).cutover(self._budget_ms(mig))
        mig["cutOver"], mig["targetWritesPossible"] = True, True
        mig["lifecycle"] = "SWITCHED"
        return res

    async def _do_resume(self, mig: dict, req: ActionReq) -> dict:
        px = (await self.proxy.call("GET", "/status"))["proxy"]
        want = mig["target"] if req.expectedLocation == "TARGET" else mig["source"]
        if px["active"] != want or px["paused"]:
            raise MigrationError(f"expected writes on {want}, proxy is on {px['active']} paused={px['paused']}")
        if req.expectedLocation == "TARGET":
            mig["lifecycle"] = "ACTIVE"
        else:
            mig["lifecycle"], mig["transitionState"] = "RESTORED", "CLOSED"
            mig["rolledBack"] = True
        return {"location": req.expectedLocation, "alreadyResumed": True}

    async def _do_rollback(self, mig: dict, req: ActionReq) -> dict:
        res = await self._core(mig).rollback(req.mode, self._budget_ms(mig))
        mig["cutOver"], mig["returning"] = False, False
        mig["rolledBack"] = True
        mig["lifecycle"] = "ROLLED_BACK_HELD"
        return res

    async def _do_cancel(self, mig: dict, req: ActionReq) -> dict:
        core = self._core(mig)
        if core and core.state in ("PRECHECK", "SYNCING"):
            await core.abort("cancelled")
        elif core and core.state not in ("ABORTED", "FAILED"):
            raise MigrationError(f"cancel only before a cutover (state={core.state})")
        mig["lifecycle"], mig["transitionState"] = "CANCELLED", "CLOSED" if mig["transitionId"] else "NONE"
        return {"targetDataKept": True}

    async def _do_finalize(self, mig: dict, req: ActionReq) -> dict:
        if self.bindings and self.bindings.data["binding"]:
            try:
                await self.bindings.stable_route()
            except BindingError as e:
                raise Refused(e.status, e.code, str(e)) from None
        core = self._core(mig)
        if mig["lifecycle"] == "ACTIVE":
            await core.commit()
        else:
            await core.stop_sync()
        mig["lifecycle"], mig["transitionState"] = "COMPLETED", "CLOSED"
        return {"sourceDeleted": False}

    async def _do_reconcile(self, mig: dict, req: ActionReq) -> dict:
        """Compares what is recorded with what is running. Changes nothing outside this file."""
        observed = await self._observe(mig)
        mig["unknownOperationId"] = None
        core = self._core(mig)
        if core:
            mig["cutOver"] = observed["route"] == "TARGET"
            mig["targetWritesPossible"] = mig["targetWritesPossible"] or mig["cutOver"]
            mig["lifecycle"] = self._from_core(mig, core)
        mig["health"] = "UNKNOWN" if observed["route"] == "UNKNOWN" else "OK"
        return {"observed": observed, "lifecycle": mig["lifecycle"]}

    def _from_core(self, mig: dict, core) -> str:
        """What the Migration has reached: it can move on its own after a restart."""
        state, lifecycle = core.state, mig["lifecycle"]
        if state == "PRECHECK":
            return lifecycle
        if state == "SYNCING":
            return "QUIESCED" if lifecycle == "QUIESCED" else "SYNCING"
        if state in ("DRAINING", "CUTOVER"):
            return lifecycle if lifecycle == "ACTIVE" or (lifecycle == "QUIESCED" and mig.get("returning")) else "SWITCHED"
        if state in ("ROLLING_BACK_DRAIN", "ROLLED_BACK"):
            return "RESTORED" if lifecycle == "RESTORED" else "ROLLED_BACK_HELD"
        if state == "COMMITTED":
            return "COMPLETED"
        if state == "REPLACED":
            return lifecycle
        return "CANCELLED" if lifecycle == "CANCELLED" else "FAILED"       # ABORTED, FAILED

    def _lifecycle(self, mig: dict) -> str:
        """SYNCING ↔ READY only follows the sync, without a new revision: it can change every moment."""
        lifecycle = mig["lifecycle"]
        if lifecycle in TERMINAL or lifecycle in ("CREATED", "PREPARED") or mig["activeOperationId"]:
            return lifecycle
        core = self._core(mig)
        if not core:
            return lifecycle
        current = self._from_core(mig, core)
        if current == "SYNCING" and mig.get("isReady"):
            return "READY"
        return current

    # ---- status ----------------------------------------------------------------------
    async def _observe(self, mig: dict) -> dict:
        try:
            px = (await self.proxy.call("GET", "/status", timeout=2))["proxy"]
        except Exception:  # noqa: BLE001
            return {"observedAt": _now(), "route": "UNKNOWN", "businessWriteAccess": "UNKNOWN",
                    "targetWritesPossible": mig["targetWritesPossible"], "proxy": None}
        route = {mig["source"]: "SOURCE", mig["target"]: "TARGET"}.get(px["active"], "UNKNOWN")
        access = "BLOCKED" if px["paused"] else {"SOURCE": "SOURCE_ONLY", "TARGET": "TARGET_ONLY"}.get(route, "UNKNOWN")
        return {"observedAt": _now(), "route": route, "businessWriteAccess": access,
                "targetWritesPossible": mig["targetWritesPossible"], "proxy": px}

    def _details(self, mig: dict, observed: dict, core) -> dict:
        px = observed.get("proxy") or {}
        names = {mig["source"]: "SOURCE", mig["target"]: "TARGET"}
        consumer = names.get(px.get("consume"), "UNKNOWN")
        route = observed["route"]
        returned = mig.get("rolledBack") or mig["lifecycle"] in ("ROLLED_BACK_HELD", "RESTORED")
        if returned and route == "SOURCE":
            drain = "SUCCEEDED" if consumer == "SOURCE" else "RUNNING" if consumer == "TARGET" else "UNKNOWN"
        elif not mig["cutOver"] and route == "SOURCE":
            drain = "NOT_STARTED"
        elif consumer == route and route != "UNKNOWN":
            drain = "SUCCEEDED"
        elif route == "UNKNOWN":
            drain = "UNKNOWN"
        else:
            drain = "RUNNING"
        return {"dataRoute": route, "producerRoute": route, "consumerRoute": consumer,
                "sourceConsumerAccess": "ALLOWED" if route == "SOURCE" else _source_consumer_access(drain),
                "drainStatus": drain}

    def _check(self, mig: dict, code: str, stage: str, decision: dict, observed: dict) -> dict:
        at = observed["observedAt"]
        until = datetime.fromtimestamp(time.time() + EVIDENCE_TTL_S, timezone.utc).isoformat(timespec="seconds")
        return {"code": code, "stage": stage, "required": True, "decision": decision,
                "evidenceRef": f"evidence:{mig['migrationId']}:{code}:{at}", "observedAt": at,
                "validUntil": until.replace("+00:00", "Z"), "transitionId": mig["transitionId"],
                "topologyRevision": self._topology(mig, observed)}

    @staticmethod
    def _topology(mig: dict, observed: dict) -> int:
        """The proxy's route generation; the last one seen while the proxy does not answer."""
        if (gen := (observed.get("proxy") or {}).get("gen")) is not None:
            mig["topologyRevision"] = gen
        return mig.get("topologyRevision") or 0

    async def view(self, migration_id: str) -> dict:
        mig = self._mig(migration_id)
        if (mig["lifecycle"] in TERMINAL and mig.get("terminalView")
                and not mig.get("unknownOperationId") and not mig.get("activeOperationId")):
            return copy.deepcopy(mig["terminalView"])
        core = self._core(mig)
        observed = await self._observe(mig)
        why_not_ready = None
        sync_view: dict = {}
        if core:
            sync_view = await core.sync_view()
            why_not_ready = sync_view.get("not_ready")
            mig["isReady"] = core.state == "SYNCING" and not why_not_ready
        lifecycle = self._lifecycle(mig)
        details = self._details(mig, observed, core)
        details["sync"] = sync_view
        px = observed.get("proxy") or {}
        details["proxy"] = {key: px.get(key) for key in (
            "gen", "paused", "inflight_writes", "open_tx", "last_pause_ms",
            "pubsub_sessions", "pubsub_moved", "pubsub_cut", "pubsub_dropped")}
        details["proxy"]["observedAt"] = observed["observedAt"] if px else None
        route, paused = observed["route"], (observed.get("proxy") or {}).get("paused")

        if lifecycle in ("READY", "QUIESCED"):
            cutover = _decision("PASS") if not why_not_ready else _decision("FAIL", "NOT_READY")
        else:
            cutover = _decision("FAIL", f"LIFECYCLE_{lifecycle}")
        if route == "UNKNOWN":
            resume = _decision("UNKNOWN", "ROUTE_UNKNOWN")
        elif lifecycle == "SWITCHED":
            resume = _decision("PASS") if route == "TARGET" and not paused else _decision("FAIL", "TARGET_ROUTE_NOT_VERIFIED")
        elif lifecycle == "ROLLED_BACK_HELD":
            resume = _decision("PASS") if route == "SOURCE" and not paused else _decision("FAIL", "SOURCE_ROUTE_NOT_VERIFIED")
        else:
            resume = _decision("FAIL", f"LIFECYCLE_{lifecycle}")

        reverse_on = bool(core) and core.state == "CUTOVER" and sync_view.get("running") and not sync_view.get("halted")
        if lifecycle in ("SWITCHED", "ACTIVE", "QUIESCED") and mig["cutOver"]:
            if details["drainStatus"] != "SUCCEEDED":
                rollback = _decision("FAIL", "QUEUE_DRAIN_NOT_DONE")
            elif not reverse_on:
                rollback = _decision("FAIL", "REVERSE_SYNC_NOT_RUNNING")
            else:
                rollback = _decision("PASS")
        else:
            rollback = _decision("FAIL", f"LIFECYCLE_{lifecycle}")

        health_ok = mig["health"] == "OK" and not mig["unknownOperationId"]
        route_ok = (lifecycle == "ACTIVE" and route == "TARGET") or (lifecycle == "RESTORED" and route == "SOURCE")
        followup = details["drainStatus"] == "SUCCEEDED"
        stable_since = mig.get("stableSince")
        stable_for = time.time() - stable_since if stable_since else 0
        stable = stable_for >= STABILITY_WINDOW_S
        # The adapter cannot see these two: they belong to the orchestrator's evidence.
        unseen = _decision("UNKNOWN", "ORCHESTRATOR_EVIDENCE_REQUIRED")
        checks = [
            self._check(mig, "QUIESCE_COMPLETE", "QUIESCE",
                        _decision("PASS") if lifecycle == "QUIESCED" else _decision("FAIL", f"LIFECYCLE_{lifecycle}"), observed),
            self._check(mig, "CUTOVER_SAFE", "CUTOVER", cutover, observed),
            self._check(mig, "RESUME_SAFE", "RESUME", resume, observed),
            self._check(mig, "ROLLBACK_SAFE", "ROLLBACK", rollback, observed),
            self._check(mig, "RESOURCE_HEALTH", "ADVANCE", _decision("PASS") if health_ok else _decision("FAIL", "UNHEALTHY"), observed),
            self._check(mig, "ROUTE_MATCH", "ADVANCE", _decision("PASS") if route_ok else _decision("FAIL", "ROUTE_MISMATCH"), observed),
            self._check(mig, "WORKLOAD_RUNNING", "ADVANCE", unseen, observed),
            self._check(mig, "BUSINESS_PROBE", "ADVANCE", unseen, observed),
            self._check(mig, "FOLLOWUP_COMPLETE", "ADVANCE", _decision("PASS") if followup else _decision("FAIL", "QUEUE_DRAIN_NOT_DONE"), observed),
            self._check(mig, "STABILITY_WINDOW", "ADVANCE", _decision("PASS") if stable else _decision("FAIL", "NOT_STABLE_YET"), observed),
        ]
        advance_checks = [c["decision"] for c in checks if c["stage"] == "ADVANCE"]
        fails = [r for d in advance_checks if d["result"] == "FAIL" for r in d["reasons"]]
        if fails:
            advance = _decision("FAIL", *fails)
        else:
            advance = _decision("UNKNOWN", *[r for d in advance_checks if d["result"] == "UNKNOWN" for r in d["reasons"]])

        method = "REVERSE_SYNC" if rollback["result"] == "PASS" else "DIRECT" if not mig["targetWritesPossible"] else "UNAVAILABLE"
        health = "UNKNOWN" if mig["unknownOperationId"] or route == "UNKNOWN" else "FAILED" if lifecycle == "FAILED" else "OK"
        allowed = ([] if mig["activeOperationId"] or mig["lifecycle"] in TERMINAL
                   else ["RECONCILE"] if mig["unknownOperationId"] else self._allowed(mig, lifecycle) + ["RECONCILE"])
        public_observed = {k: v for k, v in observed.items() if k != "proxy"}
        result = {
            "migrationId": mig["migrationId"], "workflowId": mig["workflowId"], "resourceId": mig["resourceId"],
            "kind": mig["kind"], "revision": mig["revision"], "lifecycle": lifecycle,
            "health": health, "phase": f"redis.{core.state.lower()}" if core else "redis.none",
            "activeOperationId": mig["activeOperationId"], "transitionId": mig["transitionId"],
            "transitionState": mig["transitionState"], "topologyRevision": self._topology(mig, observed),
            "allowedActions": allowed, "observed": public_observed,
            "readiness": {"cutover": cutover, "resume": resume, "rollback": rollback, "advance": advance},
            "rollback": {"method": method, "reasons": rollback["reasons"]},
            "progress": {"completed": None, "total": None, "unit": "keys"},
            "checks": checks,
            "details": {"redis": details},
        }
        if mig["lifecycle"] in TERMINAL and not mig["activeOperationId"] and not mig.get("unknownOperationId"):
            mig["terminalView"] = copy.deepcopy(result)
            self.store.save(self.data)
        return result

    def tick(self):
        """Once a second: how long has the active route been up without a change?"""
        for mig in self.data["migrations"].values():
            if mig["lifecycle"] in ("ACTIVE", "RESTORED"):
                mig["stableSince"] = mig.get("stableSince") or time.time()
            else:
                mig["stableSince"] = None

    # ---- retries ---------------------------------------------------------------------
    def replay(self, scope: str, key: str | None, body: dict) -> tuple[str, JSONResponse | None]:
        """Same key and body: the original answer, before any revision check."""
        if not key:
            raise Refused(400, "INVALID_REQUEST", "Idempotency-Key is required")
        record_key = f"{scope}:{key}"
        digest = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
        seen = self.data["idempotency"].get(record_key)
        if seen:
            if seen["digest"] != digest:
                raise Refused(409, "IDEMPOTENCY_CONFLICT", "this key was used with another body")
            return record_key, JSONResponse(seen["body"], status_code=seen["status"], headers=seen["headers"])
        return record_key, None

    def remember(self, record_key: str, body: dict, status: int, payload: dict, headers: dict):
        digest = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
        self.data["idempotency"][record_key] = {"digest": digest, "status": status, "body": payload, "headers": headers}
        self.store.save(self.data)


def _source_consumer_access(drain: str) -> str:
    """Before the drain is done, consumers still read and ACK on the source. After it, the proxy
    sends them to the target; the source is reached only through the proxy's credentials."""
    return {"NOT_STARTED": "ALLOWED", "RUNNING": "ALLOWED", "SUCCEEDED": "BLOCKED"}.get(drain, "UNKNOWN")


def _changes(action: str, op: dict, before: dict, after: dict) -> list[str]:
    """The external changes this command is known to have made: what the proxy showed before and
    after, and what the step itself reported. Nothing is listed for what was not seen."""
    out = []
    if before["route"] != after["route"]:
        out.append(f"route {before['route']} -> {after['route']}")
    b, a = before.get("proxy") or {}, after.get("proxy") or {}
    if b.get("consume") and a.get("consume") and b["consume"] != a["consume"]:
        out.append(f"queue consumers {b['consume']} -> {a['consume']}")
    res = op.get("result") or {}
    if op["status"] == "SUCCEEDED":
        if action == "START_SYNC":
            out.append("sync started; queue groups ensured on the target")
        if res.get("fence"):
            out.append(f"old side app writes fenced ({res['fence']})")
        if res.get("paused_ms") is not None:
            out.append(f"writes held {res['paused_ms']}ms, resumed on {after['route']}")
        if res.get("cache_keys_dropped") is not None:
            out.append(f"cache keys dropped on the source: {res['cache_keys_dropped']}")
        if action in ("CANCEL", "FINALIZE"):
            out.append("sync stopped")
    elif action in ("CUTOVER", "ROLLBACK") and before["route"] == after["route"] and after["route"] != "UNKNOWN":
        out.append(f"route unchanged ({after['route']})")
    return out


def build_router(api: CommonApi) -> APIRouter:
    router = APIRouter(prefix="/v1")

    @router.get("/capabilities")
    async def capabilities():
        return api.capabilities()

    @router.post("/bindings", status_code=201)
    async def create_binding(req: BindingReq, idempotency_key: str | None = Header(None)):
        body = req.model_dump(exclude_none=True)
        async with api.mutation_lock:
            try:
                record_key, replayed = api.replay("POST /v1/bindings", idempotency_key, body)
                if replayed:
                    return replayed
                api.ensure_available()
                if not api.bindings:
                    raise Refused(422, "PRECONDITION_FAILED", "binding registration is unavailable")
                payload = await api.bindings.create(req, record_key)
                payload["lastMigrationId"] = None
                headers = {"Location": f"/v1/bindings/{payload['bindingId']}"}
                api.remember(record_key, body, 201, payload, headers)
                return JSONResponse(payload, status_code=201, headers=headers)
            except BindingError as e:
                return _error(Refused(e.status, e.code, str(e)))
            except Refused as e:
                return _error(e)

    @router.get("/bindings/{binding_id}")
    async def binding_status(binding_id: str):
        try:
            return await api.binding_view(binding_id)
        except BindingError as e:
            return _error(Refused(e.status, e.code, str(e)))

    @router.post("/migrations", status_code=201)
    async def create(req: CreateReq, idempotency_key: str | None = Header(None)):
        body = req.model_dump()
        async with api.mutation_lock:
            try:
                record_key, replayed = api.replay("POST /v1/migrations", idempotency_key, body)
                if replayed:
                    return replayed
                mig = await api.create_plan(req)
            except Refused as e:
                return _error(e)
            payload = await api.view(mig["migrationId"])
            headers = {"Location": f"/v1/migrations/{mig['migrationId']}"}
            api.remember(record_key, body, 201, payload, headers)
            return JSONResponse(payload, status_code=201, headers=headers)

    @router.get("/migrations/{migration_id}")
    async def status(migration_id: str):
        try:
            return await api.view(migration_id)
        except Refused as e:
            return _error(e)

    @router.post("/migrations/{migration_id}/actions", status_code=202)
    async def action(migration_id: str, req: ActionReq, idempotency_key: str | None = Header(None)):
        body = req.model_dump()
        try:
            record_key, replayed = api.replay(f"POST /v1/migrations/{migration_id}/actions", idempotency_key, body)
            if replayed:
                return replayed
            await api.view(migration_id)        # refreshes READY before the state check
            op = api.accept(migration_id, req)
        except Refused as e:
            return _error(e)
        payload = {k: op[k] for k in ("operationId", "migrationId", "action", "status", "statusUrl")}
        headers = {"Location": op["statusUrl"], "Retry-After": "1"}
        api.remember(record_key, body, 202, payload, headers)
        return JSONResponse(payload, status_code=202, headers=headers)

    @router.get("/operations/{operation_id}")
    async def operation(operation_id: str, response: Response):
        op = api.data["operations"].get(operation_id)
        if not op:
            return _error(Refused(404, "NOT_FOUND", f"no operation {operation_id}"))
        if op["status"] in ("QUEUED", "RUNNING"):
            response.headers["Retry-After"] = "1"
        return {"operationId": op["operationId"], "migrationId": op["migrationId"], "action": op["action"],
                "status": op["status"], "error": op["error"], "confirmedChanges": op.get("confirmedChanges", [])}

    return router
