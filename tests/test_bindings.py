import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI

from statemig.bindings import BindingError, BindingReq, Bindings, EndpointInput
from statemig.common import ActionReq, CommonApi, CreateReq, Refused, build_router
from statemig.migration import ControlRecord, Migration


class AdminStub:
    def __init__(self, record, proxy=False):
        self.record, self.proxy = record, proxy
        self.backends = {}
        self.route = {"active": "", "consume": "", "gen": 0, "paused": False}
        self.stops = 0

    async def call(self, method, path, body=None, **_):
        if path == "/backends":
            self.backends[body["name"]] = body["url"]
            return {}
        if path == "/bind":
            if not self.route["active"]:
                self.route = dict(self.record.read())
            return self.route
        if path == "/status":
            return {"proxy": dict(self.route)} if self.proxy else {"running": False}
        if path == "/stop":
            self.stops += 1
            return {"running": False}
        raise AssertionError(path)


class BindingsTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.record = ControlRecord(str(self.path / "control.json"))
        self.proxy, self.sync = AdminStub(self.record, True), AdminStub(self.record)
        self.secrets = {name: {"url": f"redis://default:private-{name}@{name}:6379"} for name in ("a", "b", "c")}
        self.resolved = []

        def resolver(ref):
            self.resolved.append(ref)
            return self.secrets[ref]

        self.resolver = resolver
        self.registry = Bindings(str(self.path / "bindings.json"), self.proxy, self.sync, self.record, {}, resolver)
        self.current = None
        self.api = CommonApi(self.proxy, self.sync, self.record, self.registry.endpoints,
                             str(self.path / "common.json"), lambda: self.current, AsyncMock(), self.registry)
        app = FastAPI()
        app.include_router(build_router(self.api))
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
        self.addAsyncCleanup(self.client.aclose)

    def endpoint(self, name):
        return {"host": name, "port": 6379, "database": "0", "secretRef": name}

    async def post(self, path, body, key="key"):
        return await self.client.post(path, json=body, headers={"Idempotency-Key": key})

    async def bind(self):
        result = await self.post("/v1/bindings", {"resourceId": "redis-main", "source": self.endpoint("a")})
        self.assertEqual(result.status_code, 201, result.text)
        return result.json()

    def plan(self, source, target):
        return {"workflowId": "wf", "resourceId": "redis-main", "source": self.endpoint(source),
                "target": self.endpoint(target), "workloadScopeRef": "workload:app"}

    async def test_binding_idempotency_duplicate_and_no_secret_persistence(self):
        first = await self.bind()
        again = await self.post("/v1/bindings", {"resourceId": "redis-main", "source": self.endpoint("a")})
        self.assertEqual(again.json(), first)
        duplicate = await self.post("/v1/bindings", {"resourceId": "redis-main", "source": self.endpoint("a")}, "other")
        self.assertEqual(duplicate.status_code, 409)
        for path in self.path.glob("*.json"):
            self.assertNotIn("private-", path.read_text())
            self.assertNotIn("redis://", path.read_text())
        self.assertNotIn("private-", json.dumps(first))
        self.assertEqual(first["source"]["host"], "a")

    async def test_finalize_allows_new_ids_current_source_and_new_targets(self):
        binding = await self.bind()
        ids = set()
        snapshots = {}
        for source, target in (("a", "b"), ("b", "c"), ("c", "b"), ("b", "a")):
            result = await self.post("/v1/migrations", self.plan(source, target), f"{source}-{target}")
            self.assertEqual(result.status_code, 201, result.text)
            mid = result.json()["migrationId"]
            self.assertNotIn(mid, ids)
            ids.add(mid)
            mig = self.api.data["migrations"][mid]
            name = EndpointInput.model_validate(self.endpoint(target)).endpoint_name()
            route = {"gen": self.proxy.route["gen"] + 1, "active": name, "consume": name, "paused": False}
            self.proxy.route = route
            self.record.write(**route)
            self.current = SimpleNamespace(commit=AsyncMock(), stop_sync=AsyncMock(), state="COMMITTED",
                                           sync_view=AsyncMock(return_value={}))
            self.api.data["core"] = mid
            mig["lifecycle"] = "ACTIVE"
            await self.api._do_finalize(mig, None)
            self.current.commit.assert_awaited_once()
            self.assertEqual(self.proxy.route, route)
            response = await self.client.get(f"/v1/bindings/{binding['bindingId']}")
            self.assertEqual(response.json()["source"]["host"], target)
            self.assertEqual(response.json()["lastMigrationId"], mid)
            snapshots[mid] = await self.api.view(mid)
            self.assertEqual(snapshots[mid]["allowedActions"], [])
            for old_id, snapshot in snapshots.items():
                self.assertEqual(await self.api.view(old_id), snapshot)
            with self.assertRaises(Refused):
                self.api.accept(mid, ActionReq(action="RECONCILE", expectedRevision=mig["revision"]))

    async def test_restart_does_not_resolve_retired_backend(self):
        await self.bind()
        await self.registry.prepare(EndpointInput(**self.endpoint("a")), EndpointInput(**self.endpoint("b")), "redis-main")
        name = EndpointInput(**self.endpoint("b")).endpoint_name()
        self.record.write(gen=4, active=name, consume=name, paused=False)
        del self.secrets["a"]
        self.resolved.clear()
        proxy, sync = AdminStub(self.record, True), AdminStub(self.record)
        restored = Bindings(str(self.path / "bindings.json"), proxy, sync, self.record, {}, self.resolver)
        await restored.restore()
        self.assertEqual(self.resolved, ["b"])
        self.assertEqual(proxy.route["active"], name)
        self.assertEqual(len(proxy.backends), 1)

    async def test_initial_binding_intent_recovers_between_metadata_and_control_writes(self):
        req = BindingReq(resourceId="redis-main", source=EndpointInput(**self.endpoint("a")))
        with patch.object(self.record, "write", side_effect=OSError("interrupted")):
            with self.assertRaises(OSError):
                await self.registry.create(req, "initial-key")
        bid = self.registry.data["binding"]["bindingId"]
        restored = Bindings(str(self.path / "bindings.json"), self.proxy, self.sync, self.record, {}, self.resolver)
        self.assertEqual(restored.data["intent"]["status"], "PENDING")
        with self.assertRaises(BindingError):
            await restored.create(BindingReq(resourceId="redis-main", source=EndpointInput(**self.endpoint("b"))), "initial-key")
        result = await restored.create(req, "initial-key")
        self.assertEqual(result["bindingId"], bid)
        self.assertEqual(result["status"], "ACTIVE")
        self.assertEqual(restored.data["intent"]["status"], "ACTIVE")

    async def test_initial_binding_replays_after_common_response_record_was_not_saved(self):
        body = {"resourceId": "redis-main", "source": self.endpoint("a")}
        with patch.object(self.api, "remember", side_effect=OSError("interrupted")):
            with self.assertRaises(OSError):
                await self.post("/v1/bindings", body)
        bid = self.registry.data["binding"]["bindingId"]
        self.assertFalse(self.api.data["idempotency"])
        result = await self.post("/v1/bindings", body)
        self.assertEqual(result.status_code, 201, result.text)
        self.assertEqual(result.json()["bindingId"], bid)

    async def test_active_binding_with_missing_control_never_resets_to_initial_source(self):
        await self.bind()
        self.record.path.unlink()
        with self.assertRaises(BindingError) as raised:
            await self.registry.restore()
        self.assertEqual(raised.exception.code, "UNKNOWN_STATE")
        self.assertFalse(self.record.path.exists())

    async def test_source_mismatch_busy_unknown_and_drain_block_before_registration(self):
        await self.bind()
        mixed = await self.post("/v1/migrations", {**self.plan("a", "b"), "sourceRef": "resource:manual"}, "mixed")
        self.assertEqual(mixed.status_code, 422)
        self.assertEqual(len(self.proxy.backends), 1)
        unsupported = await self.post("/v1/migrations", {**self.plan("a", "b"), "profile": "unsupported"}, "unsupported")
        self.assertEqual(unsupported.status_code, 422)
        self.assertEqual(len(self.proxy.backends), 1)
        wrong = await self.post("/v1/migrations", self.plan("b", "c"), "wrong")
        self.assertEqual(wrong.status_code, 409)
        self.assertEqual(wrong.json()["code"], "SOURCE_MISMATCH")
        self.assertEqual(len(self.proxy.backends), 1)
        self.proxy.route["paused"] = True
        paused = await self.post("/v1/migrations", self.plan("a", "b"), "paused")
        self.assertEqual(paused.status_code, 409)
        self.proxy.route["paused"] = False
        result = await self.post("/v1/migrations", self.plan("a", "b"), "first")
        self.assertEqual(result.status_code, 201, result.text)
        other = await self.post("/v1/migrations", self.plan("a", "c"), "second")
        self.assertEqual(other.status_code, 409)
        self.assertNotIn("c", self.resolved)
        mig = self.api.data["migrations"][result.json()["migrationId"]]
        mig["lifecycle"], mig["unknownOperationId"] = "COMPLETED", "operation-lost"
        with self.assertRaises(Refused) as refused:
            await self.api.create_plan(CreateReq(**self.plan("a", "c")))
        self.assertEqual(refused.exception.code, "UNKNOWN_STATE")
        mig["unknownOperationId"] = None
        self.proxy.route["consume"] = "unfinished-drain"
        blocked = await self.post("/v1/migrations", self.plan("a", "c"), "drain")
        self.assertEqual(blocked.status_code, 409)

    async def test_unknown_or_running_operation_is_not_frozen_as_a_terminal_result(self):
        await self.bind()
        result = await self.post("/v1/migrations", self.plan("a", "b"), "first")
        mid = result.json()["migrationId"]
        mig = self.api.data["migrations"][mid]
        mig["lifecycle"] = "COMPLETED"
        snapshot = await self.api.view(mid)
        mig["unknownOperationId"] = "lost-operation"
        unknown = await self.api.view(mid)
        self.assertEqual(unknown["health"], "UNKNOWN")
        self.assertEqual(mig["terminalView"], snapshot)
        mig["unknownOperationId"] = None
        mig["activeOperationId"] = "running-operation"
        running = await self.api.view(mid)
        self.assertEqual(running["activeOperationId"], "running-operation")
        self.assertEqual(mig["terminalView"], snapshot)

    def test_returned_source_reports_completed_drain_without_changing_initial_state(self):
        mig = {"source": "a", "target": "b", "cutOver": False, "lifecycle": "CREATED"}
        observed = {"route": "SOURCE", "proxy": {"consume": "a"}}
        self.assertEqual(self.api._details(mig, observed, None)["drainStatus"], "NOT_STARTED")
        mig["lifecycle"] = "RESTORED"
        details = self.api._details(mig, observed, None)
        self.assertEqual(details["drainStatus"], "SUCCEEDED")
        self.assertEqual(details["sourceConsumerAccess"], "ALLOWED")
        observed["proxy"]["consume"] = "b"
        self.assertEqual(self.api._details(mig, observed, None)["drainStatus"], "RUNNING")

    async def test_secret_failure_is_sanitized_and_fixed_proxy_keeps_accounts(self):
        self.secrets["a"] = {"url": "redis://default:private-a@wrong-host:6379"}
        with self.assertRaises(BindingError) as raised:
            await self.registry.resolve(EndpointInput(**self.endpoint("a")))
        self.assertNotIn("private", str(raised.exception))
        migration = object.__new__(Migration)
        migration.persistent_proxy = True
        migration._set_direct = AsyncMock()
        await migration._open_direct_on_active()
        migration._set_direct.assert_not_called()


if __name__ == "__main__":
    unittest.main()
