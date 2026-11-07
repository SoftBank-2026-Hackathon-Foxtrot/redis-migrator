"""Opt in with STATEMIG_RUNTIME_TESTS=1; uses isolated local Docker Redis instances."""
import asyncio
import importlib
import json
import logging
import os
import socket
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import httpx
import redis

from statemig.migration import _ensure_groups


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@unittest.skipUnless(os.environ.get("STATEMIG_RUNTIME_TESTS") == "1", "local runtime tests are opt-in")
class FixedBindingRuntime(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_moves_and_restart_without_retired_source(self):
        root = Path(__file__).resolve().parents[1]
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("statemig.migration").setLevel(logging.WARNING)
        with tempfile.TemporaryDirectory(prefix="statemig-fixed-") as temp:
            temp = Path(temp)
            containers, children, logs = [], [], []
            client, api = None, None
            suffix = uuid.uuid4().hex[:10]
            ports = {name: unused_port() for name in ("proxy", "proxy_admin", "sync_admin")}
            state = temp / "state"
            state.mkdir()
            env = {**os.environ, "STATEMIG_ADMIN_TOKEN": "isolated-test-token",
                   "STATEMIG_CLIENTS": "app:isolated-proxy-pw:app", "STATEMIG_APP_USER": "app",
                   "STATEMIG_DIRECT_USER": "isolated-direct",
                   "STATEMIG_CONTROL_FILE": str(state / "control.json"),
                   "STATEMIG_PROXY": f"http://127.0.0.1:{ports['proxy_admin']}",
                   "STATEMIG_SYNCWORKER": f"http://127.0.0.1:{ports['sync_admin']}",
                   "STATEMIG_PROXY_PUBLIC_HOST": "127.0.0.1", "STATEMIG_PROXY_PUBLIC_PORT": str(ports["proxy"])}
            env.pop("STATEMIG_ENDPOINTS", None)
            endpoints, secrets = {}, {}
            terminal_history = {}

            def command(*args):
                return subprocess.run(args, cwd=root, check=True, capture_output=True, text=True).stdout.strip()

            def launch(binary, **over):
                log = open(temp / f"{binary}-{len(logs)}.log", "w+")
                logs.append(log)
                proc = subprocess.Popen([str(temp / binary)], env={**env, **over}, stdout=log, stderr=log, cwd=root)
                children.append(proc)
                return proc

            async def wait_admin(port):
                async with httpx.AsyncClient() as http:
                    for _ in range(100):
                        try:
                            response = await http.get(f"http://127.0.0.1:{port}/status",
                                                      headers={"Authorization": "Bearer isolated-test-token"})
                            if response.status_code == 200:
                                return
                        except httpx.HTTPError:
                            pass
                        await asyncio.sleep(0.05)
                self.fail("Go admin API did not start")

            async def load_api():
                nonlocal api, client
                if client:
                    await client.aclose()
                if api:
                    await api.stop_observations()
                    await api.proxy.http.aclose()
                    await api.sync.http.aclose()
                with patch.dict(os.environ, env):
                    os.environ.pop("STATEMIG_ENDPOINTS", None)
                    module = importlib.import_module("statemig.api")
                    api = importlib.reload(module)
                api.bindings.resolver = lambda ref: secrets[ref]
                await api.recover_saved_migration()
                client = httpx.AsyncClient(transport=httpx.ASGITransport(app=api.app), base_url="http://test",
                                           headers={"Authorization": "Bearer isolated-test-token"})

            async def post(path, body, expected=201):
                response = await client.post(path, json=body, headers={"Idempotency-Key": uuid.uuid4().hex})
                self.assertEqual(response.status_code, expected, response.text)
                return response.json()

            async def action(mid, name, **extra):
                status = (await client.get(f"/v1/migrations/{mid}")).json()
                result = await post(f"/v1/migrations/{mid}/actions",
                                    {"action": name, "expectedRevision": status["revision"], **extra}, 202)
                for _ in range(600):
                    operation = (await client.get(result["statusUrl"])).json()
                    if operation["status"] not in ("QUEUED", "RUNNING"):
                        self.assertEqual(operation["status"], "SUCCEEDED", operation)
                        return
                    await asyncio.sleep(0.05)
                self.fail(f"{name} operation did not finish")

            async def wait_state(mid, predicate):
                for _ in range(600):
                    status = (await client.get(f"/v1/migrations/{mid}")).json()
                    if predicate(status):
                        return status
                    await asyncio.sleep(0.05)
                self.fail(f"migration did not reach expected state: {status}")

            async def move(source, target, rollback=False):
                result = await post("/v1/migrations", {"workflowId": uuid.uuid4().hex, "resourceId": "redis-main",
                                    "workloadScopeRef": "workload:test", "source": endpoints[source],
                                    "target": endpoints[target], "policy": {"maxQuiesceSeconds": 5}})
                mid = result["migrationId"]
                await action(mid, "PREPARE")
                await action(mid, "START_SYNC")
                await wait_state(mid, lambda s: s["lifecycle"] == "READY")
                tx = {"transitionId": "tx-" + mid, "quiesceRef": "quiesce:" + mid}
                await action(mid, "QUIESCE", **tx)
                await action(mid, "CUTOVER", **tx)
                await action(mid, "RESUME", **tx, expectedLocation="TARGET")
                await wait_state(mid, lambda s: s["details"]["redis"]["drainStatus"] == "SUCCEEDED")
                active = target
                if rollback:
                    await action(mid, "QUIESCE", **tx)
                    await action(mid, "ROLLBACK", **tx, mode="normal")
                    await action(mid, "RESUME", **tx, expectedLocation="SOURCE")
                    restored = await wait_state(mid, lambda s: s["lifecycle"] == "RESTORED"
                                                and s["details"]["redis"]["drainStatus"] == "SUCCEEDED")
                    followup = next(c for c in restored["checks"] if c["code"] == "FOLLOWUP_COMPLETE")
                    self.assertEqual(followup["decision"]["result"], "PASS")
                    active = source
                admin = redis.Redis.from_url(secrets[f"secret-{active}"]["url"], client_name="statemig-admin")
                accounts_before = admin.acl_getuser("isolated-direct")
                route_before = api.record.read()
                await action(mid, "FINALIZE")
                self.assertEqual(api.record.read(), route_before)
                self.assertEqual(admin.acl_getuser("isolated-direct"), accounts_before)
                self.assertIn("off", accounts_before["flags"])
                admin.close()
                self.assertFalse((await api.sync.call("GET", "/status"))["running"])
                self.assertEqual(app_redis.get("session:fixed"), b"preserved")
                self.assertGreater(app_redis.pttl("session:fixed"), 0)
                binding_view = (await client.get(f"/v1/bindings/{binding['bindingId']}")).json()
                self.assertEqual(binding_view["source"]["port"], endpoints[active]["port"])
                self.assertEqual(binding_view["lastMigrationId"], mid)
                self.assertEqual(binding_view["status"], "ACTIVE")
                self.assertTrue(all(p.poll() is None for p in live_children))
                terminal_history[mid] = (await client.get(f"/v1/migrations/{mid}")).json()
                for old_id, snapshot in terminal_history.items():
                    self.assertEqual((await client.get(f"/v1/migrations/{old_id}")).json(), snapshot)
                return mid

            try:
                for binary, package in (("proxy", "./cmd/proxy"), ("syncworker", "./cmd/syncworker")):
                    subprocess.run(["go", "build", "-o", str(temp / binary), package], cwd=root / "proxy", check=True,
                                   capture_output=True)
                for name in ("a", "b", "c"):
                    container = f"statemig-fixed-{suffix}-{name}"
                    containers.append(container)
                    backend_port = unused_port()
                    command("docker", "run", "-d", "--name", container, "-p", f"127.0.0.1:{backend_port}:6379", "valkey/valkey:8",
                            "valkey-server", "--requirepass", "isolated-admin-pw", "--user", "app", "on",
                            ">isolated-app-pw", "~*", "&*", "+@all", "--user", "isolated-direct", "off",
                            ">isolated-direct-pw", "~*", "&*", "+@all", "--notify-keyspace-events", "KEA")
                    port = int(command("docker", "port", container, "6379").rsplit(":", 1)[1])
                    endpoints[name] = {"host": "127.0.0.1", "port": port, "database": "0", "secretRef": f"secret-{name}"}
                    secrets[f"secret-{name}"] = {"url": f"redis://default:isolated-admin-pw@127.0.0.1:{port}",
                                                "proxyUrl": f"redis://app:isolated-app-pw@127.0.0.1:{port}"}
                proxy = launch("proxy", STATEMIG_LISTEN=f"127.0.0.1:{ports['proxy']}",
                               STATEMIG_ADMIN=f"127.0.0.1:{ports['proxy_admin']}")
                worker = launch("syncworker", STATEMIG_ADMIN=f"127.0.0.1:{ports['sync_admin']}")
                live_children = [proxy, worker]
                await wait_admin(ports["proxy_admin"])
                await wait_admin(ports["sync_admin"])
                await load_api()
                binding = await post("/v1/bindings", {"resourceId": "redis-main", "source": endpoints["a"]})
                await _ensure_groups(api.ENDPOINTS[api.record.read()["active"]])
                app_redis = redis.Redis(host="127.0.0.1", port=ports["proxy"], username="app", password="isolated-proxy-pw")
                self.addCleanup(app_redis.close)
                app_redis.set("session:fixed", "preserved", ex=120)
                mids = {await move("a", "b", rollback=True), await move("a", "b")}
                mismatch = await post("/v1/migrations", {"workflowId": "wrong-source", "resourceId": "redis-main",
                                      "workloadScopeRef": "workload:test", "source": endpoints["a"], "target": endpoints["c"]}, 409)
                self.assertEqual(mismatch["code"], "SOURCE_MISMATCH")

                # A retired source and its secret may disappear after FINALIZE.
                command("docker", "stop", "-t", "1", containers[0])
                secret_a = secrets.pop("secret-a")
                for proc in (proxy, worker):
                    proc.terminate()
                    proc.wait(timeout=10)
                proxy = launch("proxy", STATEMIG_LISTEN=f"127.0.0.1:{ports['proxy']}",
                               STATEMIG_ADMIN=f"127.0.0.1:{ports['proxy_admin']}")
                worker = launch("syncworker", STATEMIG_ADMIN=f"127.0.0.1:{ports['sync_admin']}")
                live_children = [proxy, worker]
                await wait_admin(ports["proxy_admin"])
                await wait_admin(ports["sync_admin"])
                await load_api()
                self.assertEqual(app_redis.get("session:fixed"), b"preserved")
                self.assertEqual(set(api.bindings.resolved), {api.record.read()["active"]})
                command("docker", "start", containers[0])
                self.assertEqual(int(command("docker", "port", containers[0], "6379").rsplit(":", 1)[1]), endpoints["a"]["port"])
                secrets["secret-a"] = secret_a
                for source, target in (("b", "c"), ("c", "b"), ("b", "a")):
                    mid = await move(source, target)
                    self.assertNotIn(mid, mids)
                    mids.add(mid)
                for path in state.glob("*.json"):
                    self.assertNotIn("isolated-admin-pw", path.read_text())
                    self.assertNotIn("isolated-app-pw", path.read_text())
                    self.assertNotIn("redis://", path.read_text())
            finally:
                if client:
                    await client.aclose()
                if api:
                    await api.stop_observations()
                    await api.proxy.http.aclose()
                    await api.sync.http.aclose()
                for proc in children:
                    if proc.poll() is None:
                        proc.terminate()
                        proc.wait(timeout=10)
                for name in containers:
                    subprocess.run(["docker", "rm", "-f", name], cwd=root, capture_output=True)
                for log in logs:
                    log.close()
