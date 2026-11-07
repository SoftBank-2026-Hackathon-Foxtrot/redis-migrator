"""Redis migration steps for one move (source → target).

  start:    precheck → keyspace notifications on → target queue groups → Sync Worker (src → dst)
            → full compare while writes continue (mode "delta")
  cutover:  PAUSE → FINAL RECONCILE → VERIFY → PRESUBSCRIBE → SWITCH (record gen first) → FENCE SOURCE
            → REVERSE SYNC (subscription confirmed) → RESUME → QUEUE DRAIN (background)
            mode "delta" (default): reconcile and verify only what changed since the compare,
            plus key counts and a sample. mode "full": copy and compare everything in the pause
  revert:   a failure after SWITCH but before RESUME: nothing reached the new side yet,
            so route and permissions go back and writes resume on the old side
  rollback: normal  = the same cutover in the other direction (target still alive)
            failure = target is gone: switch back without reconcile, drop cache:*,
                      queues are rebuilt by the app's reconciler from the DB
  commit:   stop reverse sync; the old source can be decommissioned
  watch:    once a second outside any step: finish a step left half done, put the proxy back on
            the record, and pick the sync up again after a Sync Worker restart

The data plane is Go: the central proxy and the Sync Worker. This module only
decides *when* to do what. The control record (gen, active, consume, paused)
is written here before every proxy call; the proxy refuses a switch to any
generation that is not in the record and restarts from it. The steps themselves
are saved too (MigrationStore), so a restart of this process converges from there.
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from redis.exceptions import ResponseError

from .backend import Endpoint

log = logging.getLogger("statemig.migration")

# Dead-letter streams are copied like any key: no consumer group reads them, so there is nothing
# to drain, and they must not stay behind on the old Redis once that is decommissioned.
DEAD_STREAMS = ["q:join:dead", "q:notify:dead"]
SYNC_PREFIXES = ["session:", "cache:", "q:delayed", *DEAD_STREAMS]
VERIFY = {"full": ["session:", "q:delayed", *DEAD_STREAMS], "sample": {"cache:": 200}}
VERIFY_DELTA = {"mode": "delta", "sample": {"session:": 200, "cache:": 200, "q:delayed": 200,
                                            **{d: 200 for d in DEAD_STREAMS}}}
STREAMS = ["q:join", "q:notify"]
GROUP = "workers"
CONSUME_CMDS = "+xreadgroup +xack +xautoclaim +xclaim"
READY_BACKLOG = 200             # what the precheck assumes is left at the pause
READY_STABLE_MS = 3000          # how long the sync must have kept up before a cutover
READY_VERIFY_MS = 1000          # the most a cutover may expect to spend comparing changed keys
SYNC_WORKERS = 16               # the Sync Worker's default
VERIFY_SAMPLE = 200
COPY_BATCH = 100                # keys per round trip, per worker
VERIFY_BATCH = 200              # keys per round trip
SCAN_COUNT = 10000
PROBE_KEY = b"__statemig:probe"
PROBE_USER = "__statemig_probe"
STALE_PROXY_S = 3               # the proxy may differ from the record this long before it is repaired
SYNC_RETRY_S = 5
FAILURE_STATUS_TIMEOUT_S = 1    # the target is dead: a hung Sync Worker must not hold up the rollback


class MigrationError(Exception):
    pass


class Admin:
    """Thin client for a Go admin API (proxy or Sync Worker)."""

    def __init__(self, base_url: str, token: str = ""):
        self.http = httpx.AsyncClient(base_url=base_url, timeout=30, headers={"Authorization": f"Bearer {token}"})

    async def call(self, method: str, path: str, body: dict | None = None, timeout: float = 30) -> dict:
        r = await self.http.request(method, path, json=body, timeout=timeout)
        try:
            data = r.json()
        except ValueError:
            data = {"error": r.text}
        if r.status_code >= 400:
            raise MigrationError(f"{path}: {data.get('error', r.text)}")
        return data


class ControlRecord:
    """Stand-in for the management DB row. Atomic replace, read by the proxy."""

    def __init__(self, path: str):
        self.path = Path(path)

    def read(self) -> dict | None:
        try:
            return json.loads(self.path.read_text())
        except FileNotFoundError:
            return None

    def write(self, **rec) -> dict:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(rec))
        os.replace(tmp, self.path)
        return rec


class MigrationStore(ControlRecord):
    """The current move's steps, in the same place as the control record."""

    def clear(self):
        self.path.unlink(missing_ok=True)


class Migration:
    def __init__(self, proxy: Admin, sync: Admin, record: ControlRecord, source: Endpoint, target: Endpoint,
                 app_user: str, store: MigrationStore | None = None, mode: str = "delta",
                 direct_user: str | None = None):
        self.proxy, self.sync, self.record, self.store = proxy, sync, record, store
        self.source, self.target = source, target
        self.app_user = app_user
        # The account applications use when they connect to Redis directly, outside a migration.
        # It is shut on the source once every application goes through the proxy, and opened on
        # the side that stays active when the migration ends.
        self.direct_user = direct_user
        self.direct_fence: dict[str, str] = {}   # endpoint → "acl" | "manual" | "open"
        self.mode = mode           # "full": compare everything inside the pause; "delta": before it
        self.compare_task: asyncio.Task | None = None
        self._state = "PRECHECK"
        self.precheck_report: dict = {}
        self.fence_mode: dict[str, str] = {}   # endpoint → "acl" | "isolated"
        self.switching: dict | None = None      # {"old", "new", "kind"} from PAUSE until RESUME
        self.last_cutover: dict | None = None
        self.events: list[dict] = []
        self.drain_task: asyncio.Task | None = None
        self.fence_task: asyncio.Task | None = None
        self.sync_error: str | None = None      # why the sync could not be picked up again
        self._sync_retry_at = 0.0
        self._stale_since: float | None = None
        self._watch_error: str | None = None
        self.ready_by: float | None = None      # unix time: not ready by then means give up
        self.abort_reason: str | None = None
        self._lock = asyncio.Lock()
        self._admins: dict = {}     # (endpoint, db) -> admin connection used inside write pauses
        self._spublish: dict = {}   # endpoint -> whether it knows SPUBLISH, asked before a pause

    def retire(self):
        """A newer migration replaces this one: its background loops end, its saved file is reused."""
        self._state = "REPLACED"
        for c in self._admins.values():
            asyncio.create_task(c.aclose())
        self._admins.clear()

    # ---- connections used inside a write pause -----------------------------------------
    def _admin(self, ep: Endpoint, db: int = 0):
        """A pause must not wait on a new TCP connection: these are opened before it and reused."""
        key = (ep.name, db)
        if key not in self._admins:
            self._admins[key] = ep.admin_client(db)
        return self._admins[key]

    async def _warm(self, *eps: Endpoint, sync: bool = False) -> dict:
        """Opens (or checks) the admin connections of eps, for every db that holds keys, and the
        Sync Worker's if sync. A side that does not answer is skipped: in a failure rollback the target
        is dead."""
        out = {}

        async def one(ep: Endpoint):
            try:
                c = self._admin(ep)
                await c.ping()
                self._spublish[ep.name] = await _has_spublish(c)
                await asyncio.gather(*(self._admin(ep, db).ping() for db in _dbs(await c.info("keyspace"))))
                out[ep.name] = "ok"
            except Exception as e:  # noqa: BLE001
                out[ep.name] = _why(e)

        async def sync_pool():
            if not sync:
                return
            try:
                # delta mode: the verify's sample keys are picked now too, the pause only compares
                await self.sync.call("POST", "/warm", {"sample": VERIFY_DELTA["sample"] if self.mode == "delta" else {}},
                                     timeout=10)
            except Exception as e:  # noqa: BLE001  it dials inside the pause instead
                out["sync"] = _why(e)
        await asyncio.gather(sync_pool(), *(one(ep) for ep in eps))
        return out

    @property
    def state(self) -> str:
        return self._state

    @state.setter
    def state(self, value: str):
        self._state = value
        self._save()

    def _save(self):
        if self.store:
            self.store.write(source=self.source.name, target=self.target.name, state=self._state, mode=self.mode,
                             fence_mode=self.fence_mode, direct_fence=self.direct_fence,
                             switching=self.switching, last_cutover=self.last_cutover,
                             precheck=self.precheck_report, events=self.events[-40:], ready_by=self.ready_by,
                             abort_reason=self.abort_reason)

    def _event(self, msg: str, **extra):
        self.events.append({"t": round(time.time(), 3), "state": self.state, "msg": msg, **extra})
        log.info("[%s→%s] %s %s", self.source.name, self.target.name, msg, extra or "")

    def status(self) -> dict:
        return {"source": self.source.name, "target": self.target.name, "state": self.state, "mode": self.mode,
                "precheck": self.precheck_report, "fence_mode": self.fence_mode, "direct_fence": self.direct_fence,
                "last_cutover": self.last_cutover,
                "record": self.record.read(), "events": self.events[-40:]}

    # ---- start ----------------------------------------------------------------------
    async def precheck(self, budget_ms: int = 1800) -> dict:
        """Decided by what each Redis actually does, never by the name of the service."""
        src, dst = self.source.client(), self.target.client()
        r: dict = {"warnings": [], "errors": [], "fence": {}}
        try:
            for name, c in (("source", src), ("target", dst)):
                try:
                    await c.ping()
                except Exception as e:  # noqa: BLE001
                    r["errors"].append(f"{name} unreachable: {e}")
            if r["errors"]:
                return r
            # Every application must go through the proxy before anything moves: a direct writer
            # would keep writing to the source after the cutover, and a direct publisher would
            # reach no subscriber. The orchestrator moves them first (design 3-8).
            direct = await _direct_clients(src, self.direct_user)
            r["direct_clients"] = direct
            if direct is None:
                r["errors"].append("CLIENT LIST refused on source: cannot prove no application is connected directly")
            elif direct:
                r["errors"].append(f"{len(direct)} direct connections on source: {direct[:5]}")
            if r["errors"]:
                return r
            si, ti = await src.info(), await dst.info()
            r["source_version"], r["target_version"] = _version(si), _version(ti)
            for name, info in (("source", si), ("target", ti)):
                if info.get("cluster_enabled"):
                    r["errors"].append(f"cluster mode is not supported on {name}")
            if r["errors"]:
                return r
            # A db keeps its number on the other side, in both directions (the reverse sync).
            for (a, a_info), (b, b_ep) in ((("source", si), ("target", self.target)),
                                           (("target", ti), ("source", self.source))):
                if (top := max(_dbs(a_info), default=0)) and not await _has_db(b_ep, top):
                    r["errors"].append(f"{a} has keys in db {top}, {b} has no db {top}")
            for name, ep, info in (("source", self.source, si), ("target", self.target, ti)):
                if found := await _queues_outside_db0(ep, _dbs(info)):
                    r["errors"].append(f"queue streams outside db 0 on {name}: {found}")
            if r["errors"]:
                return r
            if ti.get("maxmemory") and ti["maxmemory"] < si.get("used_memory", 0):
                r["errors"].append("target maxmemory < source used_memory")
            # An evicted key is gone from that side only: compares fail and a rollback misses it.
            # Both sides count, since the source is written by the reverse sync after a cutover.
            for name, info in (("source", si), ("target", ti)):
                if info.get("maxmemory") and info.get("maxmemory_policy", "noeviction") != "noeviction":
                    r["errors"].append(f"maxmemory-policy {info['maxmemory_policy']} on {name} can evict keys:"
                                       " set noeviction")
            for ep, name, c in ((self.source, "source", src), (self.target, "target", dst)):
                r["warnings"] += [f"{w} on {name}" for w in await _restart_risks(c)]
                try:
                    await c.config_set("notify-keyspace-events", "KA")
                except Exception as e:  # noqa: BLE001  managed Redis: notifications are set by IaC
                    r["warnings"].append(f"CONFIG refused on {name} ({e}); expecting notifications set by IaC")
                if why := await _notifications_work(c):
                    r["errors"].append(f"{why} on {name}")
                r["fence"][ep.name] = await _fence_mode(c, self.app_user)
                if r["fence"][ep.name] == "isolated":
                    r["warnings"].append(f"ACL refused on {name}: only credential isolation fences it")
            r["copy_mode"] = await _probe_dump(src, dst)
            r["estimate"] = await _estimate(self.source, _dbs(si), src, dst, budget_ms, self.mode)
            if r["estimate"]["ms"] > budget_ms:
                r["errors"].append(f"estimated final reconcile {r['estimate']['ms']}ms > budget {budget_ms}ms")
        finally:
            await src.aclose()
            await dst.aclose()
        return r

    async def start(self, budget_ms: int = 1800, timeout_s: int | None = None) -> None:
        await self.check(budget_ms)
        await self.start_sync(timeout_s)

    async def check(self, budget_ms: int = 1800) -> dict:
        async with self._lock:
            self.precheck_report = await self.precheck(budget_ms)
            self.fence_mode = self.precheck_report.get("fence", {})
            self._event("precheck", **{k: v for k, v in self.precheck_report.items() if k != "warnings"})
            if self.precheck_report["errors"]:
                self.state = "FAILED"
                raise MigrationError("; ".join(self.precheck_report["errors"]))
            # Nobody connects directly any more: close the way, so a process that was missed or
            # starts later with old settings cannot write to the source behind the proxy.
            await self._set_direct(self.source, False)
            self._save()
            return self.precheck_report

    async def start_sync(self, timeout_s: int | None = None) -> None:
        async with self._lock:
            self.ready_by = time.time() + timeout_s if timeout_s else None
            await _ensure_groups(self.target)
            await self._set_write(self.target, True)
            await self.sync.call("POST", "/start", {"src": self.source.name, "dst": self.target.name,
                                                   "include": SYNC_PREFIXES})
            self.state = "SYNCING"
            self._event("sync started (subscription confirmed, two-sided scan queued)")
            self._watch_compare()

    def _watch_compare(self):
        if self.mode == "delta" and not self.compare_task:
            self.compare_task = asyncio.create_task(self._keep_compared())

    async def _keep_compared(self):
        """Delta mode: keep a valid full compare. It lapses when the subscription drops."""
        while self.state in ("SYNCING", "DRAINING", "CUTOVER"):
            try:
                st = (await self.sync.call("GET", "/status")).get("sync", {})
                if st and not st.get("halted") and not st["compare_valid"] and not st["comparing"]:
                    res = await self.sync.call("POST", "/precompare", {"deadline_ms": 3_600_000}, timeout=3700)
                    self._event("precompare done", **res)
            except Exception as e:  # noqa: BLE001
                self._event("precompare failed", error=str(e))
            await asyncio.sleep(1)
        self.compare_task = None

    async def not_ready(self) -> str | None:
        """Why a cutover should not start now, or None. Lag is judged in time, not in keys:
        a high write rate always has keys on the way, however healthy the sync is."""
        st = await self.sync.call("GET", "/status", timeout=2)
        s = st.get("sync") or {}
        if self.state != "SYNCING":
            return f"state={self.state}"
        if not st.get("running"):
            return "no sync running"
        if s.get("halted"):
            return f"sync halted: {s['halted']}"
        if s.get("notifications_silent"):
            return f"keyspace notifications stopped arriving from {s['src']}"
        if self.mode == "delta" and not s["compare_valid"]:
            return "full compare not finished"
        if s["stable_ms"] < READY_STABLE_MS:
            return (f"sync not keeping up for {READY_STABLE_MS}ms yet (lag {s['lag_ms']}ms, backlog {s['backlog']}, "
                    f"oldest waiting {s['oldest_dirty_age_ms']}ms)")
        if s["est_verify_ms"] > READY_VERIFY_MS:
            return f"{s['delta_keys']} changed keys still to compare (about {s['est_verify_ms']}ms)"
        return None

    async def ready(self) -> bool:
        return await self.not_ready() is None

    # ---- cutover --------------------------------------------------------------------
    async def cutover(self, budget_ms: int = 1800) -> dict:
        async with self._lock:
            if why := await self.not_ready():
                raise MigrationError(f"not ready ({why})")
            res = await self._switch_over(self.source, self.target, budget_ms, "cutover")
            self._after_switch(res, "cutover")
            return res

    def _after_switch(self, res: dict, kind: str):
        self.switching = None
        self.last_cutover = res
        if kind == "cutover":
            self.state = "DRAINING"
            self._event("cutover done", **res)
            self.drain_task = asyncio.create_task(self._drain(self.source, res["gen"], "CUTOVER"))
        else:
            self.state = "ROLLING_BACK_DRAIN"
            self._event("normal rollback done", **res)
            self.drain_task = asyncio.create_task(self._drain(self.target, res["gen"], "ROLLED_BACK"))

    async def before_pause(self, budget_ms: int):
        """What can be judged while writes still flow, so a hopeless cutover costs no pause."""
        try:
            st = (await self.sync.call("GET", "/status", timeout=5)).get("sync") or {}
            if st.get("est_verify_ms", 0) > budget_ms:
                raise MigrationError(f"not started: comparing {st['delta_keys']} changed keys would take "
                                     f"{st['est_verify_ms']}ms, budget {budget_ms}ms")
            c = await self.sync.call("POST", "/verify", {"mode": "count", "sample": VERIFY_DELTA["sample"]}, timeout=300)
        except httpx.HTTPError as e:
            raise MigrationError(f"not started: {_why(e)}") from e
        if not c["ok"]:
            raise MigrationError(f"verify failed: {c['prefixes']}")

    async def _switch_over(self, old: Endpoint, new: Endpoint, budget_ms: int, kind: str) -> dict:
        """PAUSE … RESUME, shared by cutover and normal rollback."""
        if self.mode == "delta":
            await self.before_pause(budget_ms)
        await self._warm(old, new, sync=True)      # writes still flow: connection setup costs no pause
        t0 = time.monotonic()
        spent = lambda: (time.monotonic() - t0) * 1000  # noqa: E731
        left = lambda: max(100, int(budget_ms - spent()))  # noqa: E731
        rec = self.record.read()
        timings: dict = {}
        steps: dict = {}
        mark = [t0]

        def lap(name: str):
            now = time.monotonic()
            steps[name] = round((now - mark[0]) * 1000, 1)
            mark[0] = now
        timings["steps_ms"] = steps

        # 1. PAUSE
        self.switching = {"old": old.name, "new": new.name, "kind": kind, "gen0": rec["gen"]}
        self.record.write(**{**rec, "paused": True})
        self._save()
        try:
            p = await self.proxy.call("POST", "/pause", {"max_wait_ms": left()})
            timings["pause_wait_ms"] = p["waited_ms"]
            lap("pause")
            # 2. FINAL RECONCILE
            rc = await self.sync.call("POST", "/reconcile", {"deadline_ms": left(), "mode": self.mode},
                                      timeout=left() / 1000 + 1)
            timings["reconcile_ms"], timings["reconciled_keys"] = rc["ms"], rc["scanned"]
            lap("reconcile")
            # 3. VERIFY
            v = await self.sync.call("POST", "/verify",
                                     {**(VERIFY_DELTA if self.mode == "delta" else VERIFY), "deadline_ms": left()},
                                     timeout=left() / 1000 + 1)
            timings["verify_ms"] = v["ms"]
            if not v["ok"]:
                raise MigrationError(f"verify failed: {v['prefixes']}")
            lap("verify")
            if missing := await _missing_groups(new, self._admin(new)):
                raise MigrationError(f"verify failed: no queue group on {new.name}: {missing}")
            lap("groups")
            # PRESUBSCRIBE: every subscribed client gets the same subscriptions on the new side now,
            # while nothing is published; one that cannot calls the cutover off with nothing changed.
            timings["presubscribed"] = await self._presubscribe(new, left)
            lap("presubscribe")
            if spent() > budget_ms:     # the steps left are quick; past the budget, writes come first
                raise MigrationError(f"budget exceeded: {round(spent())}ms > {budget_ms}ms before the switch")
        except (MigrationError, httpx.HTTPError) as e:
            try:
                await self._abort(rec, str(e))
            except Exception as e2:  # noqa: BLE001  switching stays set: the watch finishes it
                self._event("abort unfinished, the watch retries", error=_why(e2))
            raise MigrationError(_why(e)) from e

        # 4. SWITCH: record first; from here on forward, or a full revert
        gen = rec["gen"] + 1
        self.record.write(gen=gen, active=new.name, consume=old.name, paused=True)
        lap("record")
        try:
            await self._finish_switch(old, new, gen, timings, left, lap)
        except Exception as e:  # noqa: BLE001
            try:
                await self._revert(old, new, gen, kind, _why(e))
            except Exception as e2:  # noqa: BLE001  switching stays set: the watch finishes it
                self._event("revert unfinished, the watch retries", error=_why(e2))
                raise MigrationError(f"switch reverting to {old.name}: {_why(e)}") from e
            raise MigrationError(f"switch reverted to {old.name}: {_why(e)}") from e
        timings["total_ms"] = round((time.monotonic() - t0) * 1000, 3)
        timings["switched_at"] = time.time()
        return timings

    async def _presubscribe(self, new: Endpoint, left) -> int:
        """A refusal calls the cutover off. An unreachable proxy is retried like the switch: one
        that restarted has no subscribed session left, so there is nothing to lose by waiting."""
        while True:
            try:
                ps = await self.proxy.call("POST", "/presubscribe", {"target": new.name, "max_wait_ms": left()},
                                           timeout=left() / 1000 + 1)
                return ps["sessions"]
            except httpx.HTTPError:
                if left() <= 100:
                    raise
                await asyncio.sleep(0.2)

    async def _finish_switch(self, old: Endpoint, new: Endpoint, gen: int, timings: dict, left, lap=lambda _: None):
        """Steps 4–7. Safe to run again: a restart converges by calling this once more."""
        while True:
            try:
                # 100ms less: the proxy may wait that long more to close a client that does not read
                await self.proxy.call("POST", "/switch", {"gen": gen, "max_wait_ms": max(100, left() - 100)})
                break
            except Exception as e:  # noqa: BLE001
                try:
                    st = (await self.proxy.call("GET", "/status"))["proxy"]
                    if st["gen"] == gen:   # applied, only the reply was lost
                        break
                    if not st["paused"]:    # it restarted before the record said paused
                        await self._close_gate()
                except Exception:  # noqa: BLE001  still down
                    pass
                if left() <= 100:
                    raise MigrationError(f"switch to gen {gen} not applied: {e}") from e
                await asyncio.sleep(0.2)
        lap("switch")
        # 5. FENCE SOURCE (the app user may only consume queues on the old side), open the new
        # side, 6. REVERSE SYNC (subscription confirmed before any write reaches the new side).
        # Independent of each other and all done before RESUME: run at once, a round trip for all.
        fence, unfence, reverse = await asyncio.gather(
            self._fence_old(old), self._set_write(new, True),
            self.sync.call("POST", "/reverse", {"src": new.name, "deadline_ms": max(1000, left())}),
            return_exceptions=True)
        for e in (unfence, reverse):   # all finished: a revert does not race a step still running
            if isinstance(e, BaseException):
                raise e
        timings["fence"] = fence if not isinstance(fence, BaseException) else _why(fence)
        lap("fence_unfence_reverse")
        # 7. RESUME, after checking the proxy really is on the recorded generation
        st = (await self.proxy.call("GET", "/status"))["proxy"]
        if st["gen"] != gen or st["active"] != new.name:
            raise MigrationError(f"proxy not on recorded gen {gen}: {st}")
        lap("status")
        self.record.write(gen=gen, active=new.name, consume=old.name, paused=False)
        timings["gen"] = gen
        try:
            r = await self.proxy.call("POST", "/resume")
            lap("resume")
            timings["paused_ms"] = r["paused_ms"]
        except Exception as e:  # noqa: BLE001  the record already says resumed: no way back from here
            timings["paused_ms"] = None    # a restarted proxy starts unpaused, the watch covers the rest
            self._event("resume unanswered, going forward", error=_why(e))

    async def _fence_old(self, old: Endpoint) -> str:
        """A failed fence does not stop the switch: only the proxy holds the credentials."""
        if self.fence_mode.get(old.name) == "isolated":
            return "isolated"
        try:
            await self._set_write(old, False)
            return "acl"
        except Exception as e:  # noqa: BLE001
            self._event("fence failed; credential isolation only, retrying", endpoint=old.name, error=str(e))
            self.fence_task = asyncio.create_task(self._retry_fence(old))
            return "isolated (retrying)"

    async def _retry_fence(self, old: Endpoint):
        while True:
            await asyncio.sleep(1)
            if (self.record.read() or {}).get("active") == old.name:
                return          # rolled back: this side must stay writable
            try:
                await self._set_write(old, False)
            except Exception:  # noqa: BLE001
                continue
            if self.last_cutover:
                self.last_cutover["fence"] = "acl"
            self._event("fence applied on retry", endpoint=old.name)
            self._save()
            return

    async def _revert(self, old: Endpoint, new: Endpoint, gen: int, kind: str, reason: str):
        """Nothing reached the new side yet (no RESUME), so going back loses nothing."""
        back = gen + 1
        self.record.write(gen=back, active=old.name, consume=old.name, paused=True)
        await self._finish_revert(old, new, back, kind, reason)

    async def _finish_revert(self, old: Endpoint, new: Endpoint, back: int, kind: str, reason: str):
        """Safe to run again: the record already says where this ends."""
        if self.fence_task:
            self.fence_task.cancel()
        await self._apply_route(back)
        await self._set_write(old, True)
        self.record.write(gen=back, active=old.name, consume=old.name, paused=False)
        await self.proxy.call("POST", "/resume")
        self.switching = None
        self._event("switch reverted, writes resumed on the old side", reason=reason, gen=back)
        if kind == "cutover":
            self.state = "ABORTED"
            asyncio.create_task(self._stop_sync_quietly())
        else:           # a failed rollback: still cut over, keep the reverse sync
            self.state = "CUTOVER"
            asyncio.create_task(self._restore_direction(old, new))

    async def _close_gate(self):
        """Only closes it: SWITCH needs the gate shut, not the writes in flight finished."""
        try:
            await self.proxy.call("POST", "/pause", {"max_wait_ms": 1})
        except MigrationError:
            pass

    async def _apply_route(self, gen: int):
        """Puts the proxy on the generation in the record."""
        st = (await self.proxy.call("GET", "/status"))["proxy"]
        if st["gen"] < gen:
            await self._close_gate()
            await self.proxy.call("POST", "/switch", {"gen": gen})

    async def _restore_direction(self, active: Endpoint, other: Endpoint):
        try:
            await self._set_write(other, False)
            await self.sync.call("POST", "/reverse", {"src": active.name, "deadline_ms": 5000})
        except Exception as e:  # noqa: BLE001
            self._event("could not restore the reverse sync", error=str(e))

    async def _abort(self, rec: dict, reason: str):
        self.record.write(**{**rec, "paused": False})
        r = await self.proxy.call("POST", "/resume")
        self.switching = None
        self._save()
        self._event("cutover aborted, old side still active", reason=reason, paused_ms=r["paused_ms"])

    async def _drain(self, old: Endpoint, gen: int, final_state: str):
        """QUEUE DRAIN: once the old side's queues are empty, consumers follow the writers."""
        cli = old.client()
        try:
            while True:
                if await _queues_empty(cli):
                    break
                await asyncio.sleep(0.2)
            rec = self.record.read()
            self.record.write(**{**rec, "consume": rec["active"]})
            await self.proxy.call("POST", "/drain-done", {"gen": gen})
            self.state = final_state
            self._event("queue drain done", consume=rec["active"])
        except Exception as e:  # noqa: BLE001
            self._event("drain monitor stopped", error=str(e))
        finally:
            await cli.aclose()

    # ---- abort ------------------------------------------------------------------------
    async def abort(self, reason: str = "requested") -> None:
        """Give up before any cutover. The app never left the source, so only the sync stops.
        What was copied stays on the target; the next prepare compares it again."""
        async with self._lock:
            if self.state not in ("PRECHECK", "SYNCING") or self.switching:
                raise MigrationError(f"abort only before a cutover (state={self.state})")
            try:
                await self.sync.call("POST", "/stop")
            except Exception as e:  # noqa: BLE001  it comes back with no sync, or the next prepare replaces it
                self._event("sync stop failed", error=_why(e))
            self.ready_by, self.abort_reason = None, reason
            self.state = "ABORTED"
            self._event("migration aborted before cutover", reason=reason)
            await self._open_direct_on_active()

    async def stop_sync(self) -> None:
        async with self._lock:
            await self.sync.call("POST", "/stop")
            await self._open_direct_on_active()

    async def _give_up_if_late(self):
        if self.state != "SYNCING" or not self.ready_by or time.time() < self.ready_by:
            return
        try:
            why = await self.not_ready()
        except Exception as e:  # noqa: BLE001
            why = f"Sync Worker unreachable: {_why(e)}"
        if why:
            await self.abort(f"not ready in time: {why}")
        else:       # ready: from here on it is the orchestrator's call
            self.ready_by = None
            self._save()

    # ---- rollback / commit ------------------------------------------------------------
    async def rollback(self, mode: str = "normal", budget_ms: int = 1800) -> dict:
        async with self._lock:
            if self.state not in ("CUTOVER", "DRAINING"):
                raise MigrationError(f"rollback only after cutover (state={self.state})")
            if mode == "normal":
                if self.state != "CUTOVER":
                    raise MigrationError("queue drain still running; normal rollback waits for DRAIN_DONE")
                res = await self._switch_over(self.target, self.source, budget_ms, "rollback")
                self._after_switch(res, "rollback")
                return res
            return await self._failure_rollback(budget_ms)

    async def _failure_rollback(self, budget_ms: int) -> dict:
        """Target Redis is gone. Loss = what the reverse sync had not copied yet.
        Safe to run again: a restart or the watch finishes it from the saved switching."""
        t0 = time.monotonic()
        res: dict = {"mode": "failure"}
        steps = res["steps_ms"] = {}    # where the time went, for the write pause in particular
        mark = [t0]

        def lap(name: str):
            now = time.monotonic()
            steps[name] = round((now - mark[0]) * 1000, 1)
            mark[0] = now
        self.switching = {"old": self.target.name, "new": self.source.name, "kind": "failure"}
        self._save()
        # Connect to the source alongside the first steps, not inside the pause after them.
        warm = asyncio.create_task(self._warm(self.source))
        try:
            status = await self.sync.call("GET", "/status", timeout=FAILURE_STATUS_TIMEOUT_S)
            st = status.get("sync", {})
            if not status.get("running"):
                res["reverse_backlog_at_failure"] = "unknown: no sync running"
            elif st.get("halted"):
                res["reverse_backlog_at_failure"] = f"unknown: sync halted: {st['halted']}"
            else:
                res["reverse_backlog_at_failure"] = st.get("backlog")
                res["reverse_oldest_dirty_age_ms"] = st.get("oldest_dirty_age_ms")
                # The backlog is all that is missing only if every change was seen: not while a
                # rescan after a dropped subscription is running, nor when the target still took
                # writes while the subscription was down (a target that died drops it, harmlessly).
                res["reverse_backlog_is_minimum"] = bool(st.get("rescanning") or st.get("written_while_unsubscribed")
                                                         or st.get("notifications_silent"))
                res["reverse_subscription_down_ms"] = st.get("subscription_down_ms")
        except Exception as e:  # noqa: BLE001
            res["reverse_backlog_at_failure"] = f"unknown: {_why(e)}"
        lap("sync_status")
        rec = self.record.read()
        gen = rec["gen"]
        if rec["active"] != self.source.name:
            self.record.write(**{**rec, "paused": True})
            try:   # writes in flight to the dead backend fail on their own: do not wait for them
                await self.proxy.call("POST", "/pause", {"max_wait_ms": min(budget_ms, 300)})
            except MigrationError as e:
                res["pause_note"] = str(e)
            gen += 1
        lap("pause")
        self.record.write(gen=gen, active=self.source.name, consume=self.source.name, paused=True)
        await self._apply_route(gen)
        lap("switch")
        res["warm"] = await warm
        lap("warm")
        await self._set_write(self.source, True)
        lap("unfence")
        res["cache_keys_dropped"] = await _drop_prefix(self.source, "cache:", steps,
                                                       lambda db: self._admin(self.source, db))
        lap("drop_cache")
        self.record.write(gen=gen, active=self.source.name, consume=self.source.name, paused=False)
        r = await self.proxy.call("POST", "/resume")
        lap("resume")
        if self.drain_task:
            self.drain_task.cancel()
        # outside the pause: its workers may still be timing out against the dead target
        asyncio.create_task(self._stop_sync_quietly())
        res.update(gen=gen, paused_ms=r["paused_ms"], total_ms=round((time.monotonic() - t0) * 1000, 3))
        self.switching = None
        self.state = "ROLLED_BACK"
        self._event("failure rollback done", **res)
        return res

    async def _stop_sync_quietly(self):
        try:
            await self.sync.call("POST", "/stop")
        except Exception as e:  # noqa: BLE001
            self._event("sync stop failed", error=str(e))

    async def commit(self) -> None:
        async with self._lock:
            if self.state != "CUTOVER":
                raise MigrationError(f"commit only after queue drain (state={self.state})")
            await self.sync.call("POST", "/stop")
            self.state = "COMMITTED"
            self._event("committed; reverse sync stopped, old side can be decommissioned")
            await self._open_direct_on_active()

    # ---- restart ----------------------------------------------------------------------
    @classmethod
    def load(cls, proxy: Admin, sync: Admin, record: ControlRecord, store: MigrationStore,
             endpoints: dict[str, Endpoint], app_user: str, direct_user: str | None = None) -> "Migration | None":
        saved = store.read()
        if not saved:
            return None
        m = cls(proxy, sync, record, endpoints[saved["source"]], endpoints[saved["target"]], app_user, store,
                saved.get("mode", "delta"), direct_user)
        m._state = saved["state"]
        m.fence_mode = saved.get("fence_mode", {})
        m.direct_fence = saved.get("direct_fence", {})
        m.switching = saved.get("switching")
        m.last_cutover = saved.get("last_cutover")
        m.precheck_report = saved.get("precheck", {})
        m.events = saved.get("events", [])
        m.ready_by = saved.get("ready_by")
        m.abort_reason = saved.get("abort_reason")
        return m

    async def recover(self):
        """Converge after a restart of this process: the control record says how far the switch got."""
        async with self._lock:
            for attempt in range(60):
                try:
                    await self.proxy.call("GET", "/status")
                    break
                except Exception:  # noqa: BLE001  the proxy may be restarting too
                    if attempt == 59:
                        raise
                    await asyncio.sleep(1)
            await self._converge()

    async def _converge(self):
        rec = self.record.read()
        sw = self.switching
        self._event("recovering", state=self.state, switching=sw, record=rec)
        if sw and sw["kind"] == "failure":        # the decision was made: the target is gone
            await self._failure_rollback(1800)
        elif sw and rec["active"] == sw["new"]:   # gen N+1 is recorded: finish going forward
            await self._recover_forward(sw, rec)
        elif sw:                                  # not recorded, or already going back
            await self._recover_back(sw, rec)
        else:
            await self._apply_route(rec["gen"])
            px = (await self.proxy.call("GET", "/status"))["proxy"]
            if rec["paused"] or px["paused"]:
                self.record.write(**{**rec, "paused": False})
                await self.proxy.call("POST", "/resume")
        rec = self.record.read()
        self._watch_compare()
        if not self.drain_task or self.drain_task.done():
            if self.state == "DRAINING":
                self.drain_task = asyncio.create_task(self._drain(self.source, rec["gen"], "CUTOVER"))
            elif self.state == "ROLLING_BACK_DRAIN":
                self.drain_task = asyncio.create_task(self._drain(self.target, rec["gen"], "ROLLED_BACK"))
        if self.state == "ROLLED_BACK":     # a failure rollback that died before stopping the sync
            st = await self.sync.call("GET", "/status", timeout=2)
            if st.get("running") and st["sync"]["src"] != rec["active"]:
                await self._stop_sync_quietly()

    async def _recover_back(self, sw: dict, rec: dict):
        eps = {self.source.name: self.source, self.target.name: self.target}
        old, new = eps[sw["old"]], eps[sw["new"]]
        if rec["gen"] > sw.get("gen0", rec["gen"]):    # a revert got as far as the record
            await self._finish_revert(old, new, rec["gen"], sw["kind"], "finished after a restart")
        else:
            await self._abort(rec, "restarted before the switch was recorded")

    # ---- watch ----------------------------------------------------------------------
    async def watch_once(self):
        """Outside any step (they hold the lock), the record must describe what is running."""
        if self._lock.locked():
            return
        await self._give_up_if_late()
        try:
            px = (await self.proxy.call("GET", "/status", timeout=2))["proxy"]
        except Exception:  # noqa: BLE001  down: a restart reads the record, then this repairs the rest
            return
        rec = self.record.read()
        if not rec:
            return
        stale = (px["gen"] < rec["gen"] or (px["gen"] == rec["gen"] and px["active"] != rec["active"])
                 or px["paused"] or rec["paused"])
        self._stale_since = (self._stale_since or time.monotonic()) if stale else None
        if self.switching or (stale and time.monotonic() - self._stale_since >= STALE_PROXY_S):
            try:
                await self.recover()
                self._watch_error, self._stale_since = None, None
            except Exception as e:  # noqa: BLE001
                if _why(e) != self._watch_error:
                    self._watch_error = _why(e)
                    self._event("repair failed, retrying", error=_why(e))
                return
        await self._ensure_sync()

    async def _ensure_sync(self):
        """The Sync Worker keeps nothing across a restart: start it again in the right direction."""
        if self.state == "SYNCING":
            src, dst = self.source, self.target
        elif self.state in ("DRAINING", "CUTOVER"):
            src, dst = self.target, self.source
        else:
            return
        try:
            if (await self.sync.call("GET", "/status", timeout=2)).get("running"):
                self.sync_error = None
                return
        except Exception:  # noqa: BLE001
            return
        if time.monotonic() < self._sync_retry_at:
            return
        async with self._lock:
            if self.switching:
                return
            try:
                await self.sync.call("POST", "/start", {"src": src.name, "dst": dst.name, "include": SYNC_PREFIXES,
                                                       "expect_synced": True}, timeout=SYNC_RETRY_S + 1)
                self.sync_error = None
                self._event("sync picked up again after a Sync Worker restart", src=src.name, dst=dst.name)
                self._watch_compare()
            except Exception as e:  # noqa: BLE001
                self._sync_retry_at = time.monotonic() + SYNC_RETRY_S
                if _why(e) != self.sync_error:
                    self.sync_error = _why(e)
                    self._event("could not pick the sync up again", error=_why(e))

    async def sync_view(self) -> dict:
        """What the orchestrator needs to see: is anything copying right now?"""
        try:
            st = await self.sync.call("GET", "/status", timeout=1)
            not_ready = await self.not_ready() if self.state == "SYNCING" else None
        except Exception:  # noqa: BLE001
            return {"running": False, "halted": None, "backlog": None, "lag_ms": None, "changed_keys": None,
                    "error": "SYNC_WORKER_UNREACHABLE", "not_ready": "Sync Worker unreachable", "observedAt": None}
        s = st.get("sync") or {}
        comparison = s.get("last_compare") or {}
        return {"running": bool(st.get("running")), "halted": bool(s.get("halted")), "backlog": s.get("backlog"),
                "lag_ms": s.get("lag_ms"), "changed_keys": s.get("delta_keys"),
                "error": "SYNC_ERROR" if self.sync_error else None, "not_ready": not_ready,
                "observedAt": datetime.now(timezone.utc).isoformat(),
                "direction": "FORWARD" if s.get("src") == self.source.name else
                             "REVERSE" if s.get("src") == self.target.name else "UNKNOWN",
                "copied": s.get("copied"), "oldestDirtyAgeMs": s.get("oldest_dirty_age_ms"),
                "epoch": s.get("epoch"),
                "verification": {"method": comparison.get("method"), "scope": "SYNC_PREFIXES",
                    "valid": bool(s.get("compare_valid")), "observedAt": comparison.get("observed_at"),
                    "checkedCount": comparison.get("compared"),
                    "mismatchCount": comparison.get("differed"),
                    "comparedBuckets": comparison.get("buckets"),
                    "mismatchedBuckets": comparison.get("buckets_differed")}}

    async def _recover_forward(self, sw: dict, rec: dict):
        eps = {self.source.name: self.source, self.target.name: self.target}
        old, new = eps[sw["old"]], eps[sw["new"]]
        timings: dict = {"recovered": True}
        deadline = time.monotonic() + 5
        try:
            await self.proxy.call("POST", "/pause", {"max_wait_ms": 1000})
            await self._finish_switch(old, new, rec["gen"], timings, lambda: int((deadline - time.monotonic()) * 1000))
        except Exception as e:  # noqa: BLE001
            await self._revert(old, new, rec["gen"], sw["kind"], f"after restart: {e}")
            return
        timings["switched_at"] = time.time()
        timings["total_ms"] = 0
        self._after_switch(timings, sw["kind"])

    # ---- permissions ------------------------------------------------------------------
    async def _set_direct(self, ep: Endpoint, enabled: bool):
        """Opens or shuts the applications' direct account on ep. Never inside a write pause.
        Managed Redis refuses ACL: the account is then rotated through the provider, and the
        status says so ("manual") instead of failing the migration."""
        if not self.direct_user:
            return
        c = ep.client()
        try:
            await c.execute_command("ACL", "SETUSER", self.direct_user, "on" if enabled else "off")
            if not enabled:
                await c.execute_command("CLIENT", "KILL", "USER", self.direct_user)
            try:
                await c.execute_command("ACL", "SAVE")
            except ResponseError as e:
                if "not configured" not in str(e):   # no aclfile: the precheck warned already
                    raise
            self.direct_fence[ep.name] = "open" if enabled else "acl"
            self._event("direct account " + ("opened" if enabled else "shut"), endpoint=ep.name, user=self.direct_user)
        except Exception as e:  # noqa: BLE001
            self.direct_fence[ep.name] = "manual"
            self._event("direct account must be " + ("opened" if enabled else "shut") + " through the provider",
                        endpoint=ep.name, user=self.direct_user, error=_why(e))
        finally:
            await c.aclose()
            self._save()

    async def _open_direct_on_active(self):
        """The migration ended: applications go back to a direct connection on the side that stays."""
        rec = self.record.read() or {}
        eps = {self.source.name: self.source, self.target.name: self.target}
        if (active := eps.get(rec.get("active"))) is not None:
            await self._set_direct(active, True)

    async def _set_write(self, ep: Endpoint, allowed: bool):
        """FENCE: take data writes away from the app user, keep queue consumption.
        The user's password is left alone: each Redis has its own. Runs inside a write pause, on
        the admin connection opened before it."""
        if self.fence_mode.get(ep.name) == "isolated":
            return
        c = self._admin(ep)
        # PUBLISH is not in @write: it is taken away too, or a client off the proxy could still
        # publish to a side nobody subscribes to any more.
        rules = ["on", "~*", "&*", "+@all"]
        if not allowed:
            rules += ["-@write", "-publish", *CONSUME_CMDS.split()]
            if self._spublish.get(ep.name) is None:
                self._spublish[ep.name] = await _has_spublish(c)
            if self._spublish[ep.name]:
                rules.append("-spublish")
        async with c.pipeline(transaction=False) as p:   # one round trip
            p.execute_command("ACL", "SETUSER", self.app_user, *rules)
            p.execute_command("ACL", "SAVE")
            setuser, save = await p.execute(raise_on_error=False)
        if isinstance(setuser, Exception):
            raise setuser
        if isinstance(save, Exception) and "not configured" not in str(save):   # no aclfile: precheck warned
            raise save


async def _has_spublish(c) -> bool:
    """SPUBLISH came with Redis 7, and ACL SETUSER refuses all its rules over a command it does not know.
    Valkey reports the Redis version it is compatible with here."""
    return int((await c.info("server"))["redis_version"].split(".")[0]) >= 7


def _why(e: Exception) -> str:
    """httpx timeouts have an empty message."""
    return str(e) or type(e).__name__


def _dbs(info: dict) -> list[int]:
    """The dbs that hold keys, from INFO keyspace."""
    return sorted(int(k[2:]) for k in info if k.startswith("db") and k[2:].isdigit())


async def _has_db(ep: Endpoint, db: int) -> bool:
    """Whether this Redis has that db: SELECT fails past its `databases` (CONFIG may be refused)."""
    c = ep.client(db)
    try:
        await c.ping()
        return True
    except ResponseError:
        return False
    finally:
        await c.aclose()


async def _queues_outside_db0(ep: Endpoint, dbs: list[int]) -> list[str]:
    """The queue drain and the group checks work on db 0: streams elsewhere would be left behind."""
    found = []
    for db in dbs:
        if db == 0:
            continue
        c = ep.client(db)
        try:
            for s in STREAMS:
                if await c.exists(s):
                    found.append(f"db{db}/{s}")
        finally:
            await c.aclose()
    return found


def _version(info: dict) -> str:
    if "valkey_version" in info:
        return f"valkey {info['valkey_version']}"
    return f"redis {info.get('redis_version')}"


async def _restart_risks(c) -> list[str]:
    """What a restart would undo, as far as CONFIG shows it. Call before turning notifications on."""
    try:
        notify = (await c.config_get("notify-keyspace-events")).get("notify-keyspace-events", "")
        aclfile = (await c.config_get("aclfile")).get("aclfile", "")
    except Exception:  # noqa: BLE001  managed Redis: the service keeps its own settings
        return []
    risks = []
    if not {"K", "A"} <= set(notify):
        risks.append("notifications are not in the config file: a restart turns them off")
    if not aclfile:
        risks.append("no aclfile: a restart undoes the fence")
    return risks


async def _notifications_work(c) -> str | None:
    """Real probes, since CONFIG is refused on managed Redis: a write must produce a keyspace event,
    and so must an expiry (an expired key that sent nothing would stay on the other side). Returns
    what is missing, or None."""
    p = c.pubsub()
    written, expiring = PROBE_KEY, PROBE_KEY + b":ttl"
    try:
        await p.psubscribe(b"__keyspace@0__:" + PROBE_KEY + b"*")
        await p.get_message(timeout=1)      # the subscribe confirmation
        seen = set()

        async def wait_for(event: bytes) -> bool:
            end = time.monotonic() + 2
            while time.monotonic() < end and event not in seen:
                m = await p.get_message(ignore_subscribe_messages=True, timeout=0.2)
                if m:
                    seen.add(m["data"] if isinstance(m["data"], bytes) else str(m["data"]).encode())
            return event in seen
        await c.set(written, b"1", px=2000)
        if not await wait_for(b"set"):
            return "keyspace notifications do not arrive"
        await c.set(expiring, b"1", px=50)
        await asyncio.sleep(0.1)
        await c.exists(expiring)            # expires it now if the active cycle has not yet
        if not await wait_for(b"expired"):
            return "expired-key notifications do not arrive (notify-keyspace-events needs K and A, or Kx with the types)"
        return None
    except Exception as e:  # noqa: BLE001
        return f"keyspace notifications could not be checked ({_why(e)})"
    finally:
        await c.delete(written, expiring)
        await p.aclose()


async def _direct_clients(c, direct_user: str | None) -> list[str] | None:
    """Connections of applications that bypass the proxy, or None if CLIENT LIST is refused.
    An application names its direct connections "direct:<actor>". An unnamed connection counts too
    when it logged in with the direct account: a process that forgot the name is still found."""
    try:
        clients = await c.client_list()
    except Exception:  # noqa: BLE001
        return None
    found = []
    for cl in clients:
        name, user = _text(cl.get("name")), _text(cl.get("user"))
        if name.startswith("direct:") or (not name and direct_user and user == direct_user):
            found.append(f"{name or '(unnamed)'}@{_text(cl.get('addr'))}")
    return found


def _text(v) -> str:
    return v.decode() if isinstance(v, bytes) else str(v or "")


async def _fence_mode(c, app_user: str) -> str:
    """Can this Redis take writes away from a user? Tried on a disabled throwaway user."""
    try:
        await c.execute_command("ACL", "SETUSER", PROBE_USER, "off", "-@all")
        await c.execute_command("ACL", "DELUSER", PROBE_USER)
        await c.acl_getuser(app_user)
        return "acl"
    except Exception:  # noqa: BLE001
        return "isolated"


async def _probe_dump(src, dst) -> str:
    """One real DUMP → RESTORE with a throwaway key."""
    try:
        await src.set(PROBE_KEY, b"1", px=5000)
        payload = await src.dump(PROBE_KEY)
        await dst.restore(PROBE_KEY, 1000, payload, replace=True)
        await dst.delete(PROBE_KEY)
        await src.delete(PROBE_KEY)
        return "dump"
    except Exception:  # noqa: BLE001
        return "logical"


async def _rtt_ms(c, n: int = 5) -> float:
    t0 = time.perf_counter()
    for _ in range(n):
        await c.ping()
    return (time.perf_counter() - t0) * 1000 / n


async def _estimate(src_ep: Endpoint, dbs: list[int], src, dst, budget_ms: int, mode: str) -> dict:
    """Final reconcile + VERIFY, from the key counts and measured round trips (a batch per round trip)."""
    counts = {p: 0 for p in SYNC_PREFIXES}
    for db in dbs or [0]:
        c = src_ep.client(db)
        try:
            for p in SYNC_PREFIXES:
                async for _ in c.scan_iter(match=p + "*", count=1000):
                    counts[p] += 1
        finally:
            await c.aclose()
    rs, rd = await _rtt_ms(src), await _rtt_ms(dst)
    keys = sum(counts.values())
    if mode == "delta":     # what is left at the pause, a sample per prefix, and counting keys
        left, sampled = READY_BACKLOG, min(keys, VERIFY_SAMPLE * len(SYNC_PREFIXES))
        copy_ms = left / COPY_BATCH * (rs + rd) / SYNC_WORKERS
        verify_ms = ((left + sampled) / VERIFY_BATCH + len(SYNC_PREFIXES)) * (rs + rd)  # key counts: before the pause
    else:
        copy_ms = keys / COPY_BATCH * (rs + rd) / SYNC_WORKERS
        compared = counts["session:"] + counts["q:delayed"] + min(counts["cache:"], VERIFY_SAMPLE)
        verify_ms = (compared / VERIFY_BATCH + keys / SCAN_COUNT + len(SYNC_PREFIXES)) * (rs + rd)
    return {"keys": keys, "rtt_ms": {"source": round(rs, 3), "target": round(rd, 3)},
            "ms": round(copy_ms + verify_ms, 1), "budget_ms": budget_ms}


async def _ensure_groups(ep: Endpoint):
    c = ep.client()
    try:
        for s in STREAMS:
            try:
                await c.xgroup_create(s, GROUP, id="0", mkstream=True)
            except Exception as e:  # noqa: BLE001
                if "BUSYGROUP" not in str(e):
                    raise
    finally:
        await c.aclose()


async def _missing_groups(ep: Endpoint, client=None) -> list[str]:
    c = client or ep.client()
    missing = []
    try:
        for s in STREAMS:
            try:
                names = [g["name"] for g in await c.xinfo_groups(s)]
            except Exception:  # noqa: BLE001  no such stream
                names = []
            if GROUP.encode() not in names and GROUP not in names:
                missing.append(s)
    finally:
        if client is None:
            await c.aclose()
    return missing


async def _queues_empty(c) -> bool:
    for s in STREAMS:
        try:
            groups = await c.xinfo_groups(s)
        except Exception:  # noqa: BLE001  stream never created on this side
            continue
        for g in groups:
            name = g["name"].decode() if isinstance(g["name"], bytes) else g["name"]
            if name != GROUP:
                continue
            if g.get("pending", 0) != 0:
                return False
            lag = g.get("lag")
            if lag is None:  # lag unknown: compare last delivered id with the stream tip
                tip = (await c.xinfo_stream(s))["last-generated-id"]
                if g["last-delivered-id"] != tip:
                    return False
            elif lag != 0:
                return False
    return True


async def _drop_prefix(ep: Endpoint, prefix: str, steps: dict | None = None, client_for=None) -> int:
    """In every db: a db keeps its number across a migration, and so does its cache. client_for(db)
    gives connections opened before the write pause; without it, new ones are opened and closed."""
    steps = {} if steps is None else steps
    owned = client_for is None
    client_for = client_for or ep.client
    t = time.monotonic()
    c = client_for(0)
    try:
        dbs = _dbs(await c.info("keyspace"))
    finally:
        if owned:
            await c.aclose()
    steps["drop_info"] = round((time.monotonic() - t) * 1000, 1)
    n = 0
    for db in dbs:
        c = client_for(db)
        try:
            t = time.monotonic()
            batch = []      # a failure rollback does this with writes paused: one round trip per key adds up
            async for k in c.scan_iter(match=prefix + "*", count=1000):
                batch.append(k)
                if len(batch) >= 1000:
                    n += await c.unlink(*batch)
                    batch.clear()
            if batch:
                n += await c.unlink(*batch)
            steps[f"drop_db{db}"] = round((time.monotonic() - t) * 1000, 1)
        finally:
            if owned:
                await c.aclose()
    return n
