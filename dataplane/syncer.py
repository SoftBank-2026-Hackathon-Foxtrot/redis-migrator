"""One-directional sync src → dst for the keys under the included prefixes.

  - Change capture: keyspace notifications (PSUBSCRIBE __keyspace@0__:*) on src.
    A notification only carries the key name; the worker always reads the key's
    *current* value from src, so handling a notification twice is harmless.
  - Start order: subscribe (confirmed) → enqueue a full scan of both sides. Any write
    after the subscription is confirmed produces a notification, so no change can
    fall between the scan and the subscription.
  - Per-key ordering: keys are sharded by hash and each shard has one worker, so two
    copies of the same key never run concurrently and the last processed change
    always decides the result.
  - Lost notifications (Pub/Sub has no replay): after every reconnect, and in the
    final reconcile, both sides are scanned. Keys present in src are copied, keys
    only present in dst are deleted.
  - Precompare (delta mode): both sides are compared while writes continue and every
    key marked from then on is remembered. As long as the subscription never dropped,
    a key outside that set still equals what was compared, so the cutover only has to
    finish and check those keys. A barrier key written after the pause proves every
    earlier notification has arrived.
"""
import asyncio
import itertools
import logging
import time
import zlib

from .admin import message
from .resp import Conn, Endpoint, ReplyError, read_value
from .verify import VERIFY_BATCH, compare_keys, read_cmd, scan_batches

log = logging.getLogger("statemig.sync")

NOTIFY_PREFIX = b"__keyspace@0__:"
BARRIER_KEY = b"__statemig:barrier"
COPY_BATCH = 100        # keys per round trip, per worker


class SyncError(Exception):
    pass


class Shard:
    def __init__(self):
        self.dirty: dict[bytes, int] = {}   # key → unix ms when it first became dirty
        self.copying = 0
        self.wake = asyncio.Event()


class Syncer:
    def __init__(self, src: Endpoint, dst: Endpoint, include: list[str], workers: int):
        self.include = [p.encode() for p in include]
        self.prefixes = tuple(self.include)
        self.src, self.dst = src, dst
        self.dir_epoch = 0              # bumped on reverse; workers reopen their connections
        self.stopped = False
        self.sub: Conn | None = None    # current subscription connection
        self.shards = [Shard() for _ in range(workers)]
        self.tasks: set[asyncio.Task] = set()
        self.confirmed = asyncio.Event()

        self.logical = False            # DUMP payload rejected by dst → per-type copy
        self.subscribed = False
        self.copied = self.deleted = self.errs = self.marked = self.notifications = self.resyncs = 0
        self.last_err = ""
        self.last_full_scan = 0         # unix ms

        self.tracking = False           # remember marked keys since the last precompare
        self.delta: set[bytes] = set()  # keys marked since the last precompare started
        self.compared_resync = -1       # resyncs at the last finished precompare; -1 = none
        self.comparing = False
        self.last_compare: dict = {}
        self.barrier_seen: asyncio.Event | None = None

    def _spawn(self, coro) -> asyncio.Task:
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)
        return t

    def mark(self, key: bytes) -> None:
        if self.tracking:
            self.delta.add(key)
        sh = self.shards[zlib.crc32(key) % len(self.shards)]
        sh.dirty.setdefault(key, int(time.time() * 1000))
        sh.wake.set()
        self.marked += 1

    async def start(self, timeout: float) -> None:
        """Subscribes to src, waits for the subscription to be confirmed, then enqueues a full two-sided scan."""
        for sh in self.shards:
            self._spawn(self._worker(sh))
        await self._subscribe(timeout)
        await self.full_scan()

    async def _subscribe(self, timeout: float) -> None:
        self.confirmed = asyncio.Event()
        self._spawn(self._subscriber(self.src, self.dir_epoch, self.confirmed))
        await _wait_confirmed(self.confirmed, timeout)

    async def _subscriber(self, src: Endpoint, epoch: int, confirmed: asyncio.Event) -> None:
        first = True
        while not self.stopped and self.dir_epoch == epoch:
            c = None
            try:
                c = await src.dial()
                await c.send([b"PSUBSCRIBE", NOTIFY_PREFIX + b"*"])
                v = await read_value(c.reader)      # ["psubscribe", pattern, 1]
                if isinstance(v, ReplyError):
                    raise v
            except Exception as e:  # noqa: BLE001
                self.last_err = f"subscribe {src.name}: {message(e)}"
                if c:
                    c.close()
                await asyncio.sleep(0.5)
                continue
            if self.stopped or self.dir_epoch != epoch:
                c.close()
                return
            self.sub = c
            self.subscribed = True
            if first:
                first = False
                confirmed.set()
            else:
                # notifications sent while we were disconnected are gone
                self.resyncs += 1
                log.info("resubscribed to %s: full two-sided rescan", src.name)
                self._spawn(self._quiet_full_scan())
            try:
                while True:
                    v = await read_value(c.reader)
                    if isinstance(v, list) and len(v) == 4 and v[0] == b"pmessage":
                        key = v[2].removeprefix(NOTIFY_PREFIX)
                        self.notifications += 1
                        if key == BARRIER_KEY:
                            self._barrier_arrived()
                        elif key.startswith(self.prefixes):
                            self.mark(key)
            except Exception:  # noqa: BLE001  the connection dropped: reconnect
                pass
            self.subscribed = False
            c.close()

    def _close_sub(self) -> None:
        if self.sub:
            self.sub.close()
            self.sub = None

    async def _quiet_full_scan(self) -> None:
        try:
            await self.full_scan()
        except Exception as e:  # noqa: BLE001
            self.last_err = f"full scan: {message(e)}"

    async def full_scan(self) -> int:
        """Enqueues every included key of both sides."""
        n = 0
        for ep in (self.src, self.dst):
            c = await ep.dial()
            try:
                for p in self.include:
                    cursor = b"0"
                    while True:
                        cursor, found = await c.do(b"SCAN", cursor, b"MATCH", p + b"*", b"COUNT", 1000)
                        for k in found:
                            self.mark(k)
                        n += len(found)
                        if cursor == b"0":
                            break
            finally:
                c.close()
        self.last_full_scan = int(time.time() * 1000)
        return n

    def backlog(self) -> int:
        return sum(len(sh.dirty) + sh.copying for sh in self.shards)

    def _oldest_age(self) -> int:
        oldest = min((min(sh.dirty.values()) for sh in self.shards if sh.dirty), default=0)
        return int(time.time() * 1000) - oldest if oldest else 0

    async def drain(self, deadline: float) -> bool:
        while self.backlog():
            if time.monotonic() > deadline:
                return False
            await asyncio.sleep(0.0005)
        return True

    async def reconcile(self, deadline: float) -> int:
        """The final reconcile: drain, scan both sides, drain again. Called while writes
        are paused, so afterwards dst equals src for every included key."""
        if not await self.drain(deadline):
            raise SyncError(f"drain timeout (backlog {self.backlog()})")
        n = await self.full_scan()
        if not await self.drain(deadline):
            raise SyncError(f"drain timeout after scan (backlog {self.backlog()})")
        return n

    async def reverse(self, new_src: str, deadline: float) -> None:
        """Makes new_src the source of changes and waits until the subscription on it is
        confirmed. Safe to call again after a timeout: the direction is already flipped
        then and the subscriber is still retrying, so it only waits."""
        if self.src.name == new_src:
            await _wait_confirmed(self.confirmed, deadline - time.monotonic())
            return
        if not await self.drain(deadline):
            raise SyncError("forward backlog not empty")
        self.src, self.dst = self.dst, self.src
        self.dir_epoch += 1
        # Writes are paused and both sides were just verified: tracking restarts from equal.
        self._start_tracking()
        if self.compared_resync >= 0:
            self.compared_resync = self.resyncs
        self._close_sub()
        await self._subscribe(deadline - time.monotonic())

    async def stop(self) -> None:
        self.stopped = True
        self._close_sub()
        tasks = list(self.tasks)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def status(self) -> dict:
        return {"src": self.src.name, "dst": self.dst.name, "include": [p.decode() for p in self.include],
                "subscribed": self.subscribed, "backlog": self.backlog(), "oldest_dirty_age_ms": self._oldest_age(),
                "copy_mode": "logical" if self.logical else "dump", "copied": self.copied, "deleted": self.deleted,
                "errors": self.errs, "marked": self.marked, "notifications": self.notifications,
                "resyncs": self.resyncs, "last_full_scan_ms": self.last_full_scan, "last_error": self.last_err,
                "compare_valid": self.compare_valid(), "comparing": self.comparing, "delta_keys": len(self.delta),
                "last_compare": self.last_compare}

    async def _worker(self, sh: Shard) -> None:
        cp = _Copier(self)
        try:
            while not self.stopped:
                if not sh.dirty:
                    sh.wake.clear()
                    await sh.wake.wait()
                    continue
                batch = {}
                for k in list(itertools.islice(sh.dirty, COPY_BATCH)):
                    batch[k] = sh.dirty.pop(k)      # before reading src: a write after this point marks it again
                sh.copying += len(batch)
                try:
                    copied, deleted = await cp.copy_many(list(batch))
                except Exception as e:  # noqa: BLE001
                    for k, since in batch.items():
                        sh.dirty.setdefault(k, since)   # retry, keep the original age
                    self.errs += 1
                    self.last_err = message(e)
                    cp.reset()
                    await asyncio.sleep(0.05)
                else:
                    self.copied += copied
                    self.deleted += deleted
                finally:
                    sh.copying -= len(batch)
        finally:
            cp.reset()

    # ---- precompare ------------------------------------------------------------------

    def _start_tracking(self) -> None:
        self.tracking, self.delta = True, set()

    def compare_valid(self) -> bool:
        return 0 <= self.compared_resync == self.resyncs

    async def precompare(self, deadline: float) -> dict:
        """Compares both sides while writes continue. A key that differs is marked and copied
        again; a key changing during the pass is marked by its notification. Valid until the
        subscription drops."""
        if self.comparing:
            raise SyncError("precompare already running")
        self.comparing = True
        try:
            return await self._precompare(deadline)
        finally:
            self.comparing = False

    async def _precompare(self, deadline: float) -> dict:
        t0 = time.monotonic()
        base = self.resyncs
        self.compared_resync = -1
        self._start_tracking()
        if not await self.drain(deadline):
            raise SyncError("precompare: backlog not drained in time")
        src = await self.src.dial()
        try:
            dst = await self.dst.dial()
        except BaseException:
            src.close()
            raise
        compared = differed = 0
        try:
            for p in self.include:
                async for keys in scan_batches(src, p, VERIFY_BATCH):
                    bad = await compare_keys(src, dst, keys, self.logical)
                    compared += len(keys)
                    differed += len(bad)
                    for k in bad:
                        self.mark(k)
                async for keys in scan_batches(dst, p, VERIFY_BATCH):   # keys only on dst
                    exists = await src.pipeline([[b"EXISTS", k] for k in keys])
                    for k, v in zip(keys, exists):
                        if v == 0:
                            differed += 1
                            self.mark(k)
        finally:
            src.close()
            dst.close()
        if not await self.drain(deadline):
            raise SyncError("precompare: differences not copied in time")
        res = {"compared": compared, "differed": differed, "ms": round((time.monotonic() - t0) * 1000, 3)}
        self.last_compare = res
        if self.resyncs != base:
            raise SyncError("precompare: subscription dropped during the pass")
        self.compared_resync = base
        return res

    def _barrier_arrived(self) -> None:
        if self.barrier_seen:
            self.barrier_seen.set()
            self.barrier_seen = None

    async def reconcile_delta(self, deadline: float) -> int:
        """The final reconcile of delta mode. Writes are paused: once the barrier's
        notification arrives, every earlier change's notification has too."""
        if not self.compare_valid():
            raise SyncError("no valid precompare: the subscription dropped or the compare has not finished")
        seen = self.barrier_seen = asyncio.Event()
        src = await self.src.dial()
        try:
            await src.do(b"SET", BARRIER_KEY, time.time_ns(), b"PX", 10000)
        finally:
            src.close()
        try:
            await asyncio.wait_for(seen.wait(), deadline - time.monotonic())
        except TimeoutError as e:
            raise SyncError("barrier notification did not arrive in time") from e
        if not self.compare_valid():
            raise SyncError("the subscription dropped during the pause")
        if not await self.drain(deadline):
            raise SyncError(f"drain timeout (backlog {self.backlog()})")
        return len(self.delta)


async def _wait_confirmed(confirmed: asyncio.Event, timeout: float) -> None:
    try:
        await asyncio.wait_for(confirmed.wait(), timeout)
    except TimeoutError as e:
        raise SyncError("keyspace subscription not confirmed in time") from e


# ---- copier ----------------------------------------------------------------------

class _Copier:
    def __init__(self, s: Syncer):
        self.s = s
        self.src: Conn | None = None
        self.dst: Conn | None = None
        self.epoch = 0

    async def _conns(self) -> tuple[Conn, Conn]:
        if self.s.dir_epoch != self.epoch:
            self.reset()
            self.epoch = self.s.dir_epoch
        if self.src is None:
            self.src = await self.s.src.dial()
        if self.dst is None:
            self.dst = await self.s.dst.dial()
        return self.src, self.dst

    def reset(self) -> None:
        for c in (self.src, self.dst):
            if c:
                c.close()
        self.src = self.dst = None

    async def copy_many(self, keys: list[bytes]) -> tuple[int, int]:
        """Copies keys in as few round trips as it can."""
        src, dst = await self._conns()
        if not self.s.logical:
            try:
                return await _copy_dump_many(src, dst, keys)
            except ReplyError as e:
                if not _is_payload_err(e):
                    raise
                log.warning("dst rejects DUMP payload (%s) → logical copy", e)
                self.s.logical = True
        copied = deleted = 0
        for k in keys:
            if await _copy_logical(src, dst, k) == "deleted":
                deleted += 1
            else:
                copied += 1
        return copied, deleted


def _is_payload_err(e: Exception) -> bool:
    m = str(e).lower()
    return "payload version" in m or "bad data format" in m or "unknown command" in m


async def _copy_dump_many(src: Conn, dst: Conn, keys: list[bytes]) -> tuple[int, int]:
    """DUMP+PTTL atomically on the source, RESTORE ... REPLACE on the target."""
    r = await src.tx([c for k in keys for c in ([b"DUMP", k], [b"PTTL", k])])
    write, copied, deleted = [], 0, 0
    for i, k in enumerate(keys):
        dump, ttl = r[i * 2], r[i * 2 + 1]
        if dump is None:
            write.append([b"DEL", k])
            deleted += 1
        else:
            write.append([b"RESTORE", k, str(max(ttl, 0)).encode(), dump, b"REPLACE"])
            copied += 1
    for v in await dst.pipeline(write):
        if isinstance(v, ReplyError):
            raise v
    return copied, deleted


async def _copy_logical(src: Conn, dst: Conn, key: bytes) -> str:
    """Per-type read on the source (in MULTI, with TYPE re-checked) and DEL + rebuild + PEXPIRE
    in one MULTI on the target."""
    typ = (await src.do(b"TYPE", key)).decode()
    if typ == "none":
        await dst.do(b"DEL", key)
        return "deleted"
    read = read_cmd(typ, key)
    if read is None:
        raise SyncError("unsupported type")
    r = await src.tx([[b"TYPE", key], read, [b"PTTL", key]])
    if r[0].decode() != typ:
        raise SyncError("type changed during copy")     # retried
    val, pttl = r[1], r[2]

    cmds = [[b"DEL", key]]
    if typ == "string":
        if val is not None:
            cmds.append([b"SET", key, val])
    elif val:
        if typ == "hash":
            cmds.append([b"HSET", key, *val])
        elif typ == "list":
            cmds.append([b"RPUSH", key, *val])
        elif typ == "set":
            cmds.append([b"SADD", key, *val])
        elif typ == "zset":
            members = [x for i in range(0, len(val) - 1, 2) for x in (val[i + 1], val[i])]   # score member
            cmds.append([b"ZADD", key, *members])
        elif typ == "stream":
            cmds += [[b"XADD", key, entry_id, *fields] for entry_id, fields in val]
    if pttl > 0:
        cmds.append([b"PEXPIRE", key, str(pttl).encode()])
    await dst.tx(cmds)
    return "copied"
