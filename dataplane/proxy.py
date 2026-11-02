"""Central RESP proxy. Every app talks to this one process at a fixed address.

  1. Every command goes to the *active* backend, except queue consumer commands,
     which go to the *consume* backend (the old one until its queue is drained).
  2. PAUSE holds write commands (and new MULTI/WATCH) at the gate; reads keep flowing.
     It completes once no held-type command is in flight and no transaction is open.
  3. SWITCH changes the route inside this one process, so there is no state where part
     of the fleet writes to the old Redis. RESUME releases the held commands to the new route.

It must stay one process: that is what makes the switch atomic. All state below is
only touched from its one event loop, so nothing changes between two awaits.

env:
  STATEMIG_ENDPOINTS      aws=redis://app:pw@redis-aws:6379,gcp=... (app ACL user)
  STATEMIG_ACTIVE         backend to start on if no control record exists
  STATEMIG_CONTROL_FILE   control record written by the control plane (stand-in
                          for the management DB): {"gen","active","consume","paused"}
  STATEMIG_LISTEN         RESP listen addr (default :6379)
  STATEMIG_ADMIN          admin HTTP addr (default :7000)
  STATEMIG_CLIENTS        accounts clients AUTH with: app:pw:app,worker:pw:worker
                          (user:password:role); empty means no auth and no role limits
  STATEMIG_ADMIN_TOKEN    bearer token required on the admin API

The control plane writes the record first and then calls the admin API; the proxy only
accepts a switch to the generation that is in the record. On restart the proxy starts
from the record, paused if a cutover was in progress.
"""
import asyncio
import contextlib
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, replace

from .admin import ApiError, admin_app, message, require_token, serve, split_addr
from .keys import CONSUME_CMDS, DENIED_CMDS, PUBSUB_CMDS, ROLE_APP, ROLE_WORKER, CommandTable, deny, load_command_table
from .resp import (READ_LIMIT, Conn, Endpoint, encode_command, error_reply, parse_endpoints, read_command,
                   read_raw_reply, simple_reply)

log = logging.getLogger("statemig.proxy")

BLOCK_CAP_MS = 200              # caps XREADGROUP BLOCK while consume != active
UPSTREAM_TIMEOUT = 3.0          # per-command I/O deadline towards a backend


class StateError(ApiError):
    def __init__(self, msg: str):
        super().__init__(409, f"bad state: {msg}")


@dataclass
class ProxyRoute:
    """The proxy's view of the control record for one generation."""
    gen: int
    active: Endpoint            # reads, writes, produce
    consume: Endpoint           # queue consumer commands


class Proxy:
    def __init__(self, route: ProxyRoute, paused: bool, clients: dict[str, tuple[str, str]], table: CommandTable):
        self.clients, self.table = clients, table   # clients empty: no auth, no role limits
        self.route = route
        self.epoch = 0                  # bumped on every route change; sessions drop stale upstreams
        self.paused = paused
        self.paused_at = time.monotonic()
        self.released = asyncio.Event()
        if not paused:
            self.released.set()
        self.inflight = 0               # held-type commands currently at a backend
        self.tx = 0                     # sessions inside MULTI/WATCH
        self.sessions: set[Session] = set()
        self.latency: deque[float] = deque(maxlen=20000)
        self.commands = self.held_commands = self.error_replies = 0
        self.upstream_errors = self.denied = self.switches = 0
        self.last_pause_ms = self.max_pause_ms = 0.0

    async def serve_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        await Session(self, reader, writer).run()

    async def enter(self, name: str, in_tx: bool) -> tuple[ProxyRoute, int, bool]:
        """Waits at the gate for held-type commands while paused and counts them as in flight.
        Commands inside an open transaction pass, so it can finish. Queue consumer commands
        are neither held nor counted: SWITCH does not move them, and a blocking XREADGROUP
        would otherwise stretch every PAUSE up to its BLOCK timeout."""
        held = not in_tx and name not in CONSUME_CMDS and self.table.is_held(name)
        if held and self.paused:
            self.held_commands += 1
            while self.paused:
                await self.released.wait()
        if held:
            self.inflight += 1
        return self.route, self.epoch, held

    async def pause(self, max_wait: float) -> float:
        """Closes the gate for writes and waits until no write and no transaction is
        in flight. On timeout the gate stays closed; the caller decides to resume."""
        t0 = time.monotonic()
        if not self.paused:
            self.paused = True
            self.paused_at = t0
            self.released.clear()
        while self.inflight or self.tx:
            if time.monotonic() - t0 > max_wait:
                raise StateError(f"quiesce timeout (inflight={self.inflight}, tx={self.tx})")
            await asyncio.sleep(0.0002)
        return _ms_since(t0)

    def switch(self, r: ProxyRoute) -> None:
        """Changes the route. Only allowed while paused and only to a newer generation."""
        if not self.paused:
            raise StateError("switch requires PAUSE")
        if r.gen <= self.route.gen:
            raise StateError(f"gen {r.gen} is not newer than {self.route.gen}")
        old, self.route = self.route, r
        self.epoch += 1
        for s in self.sessions:
            if s.pubsub:    # subscriptions live on the old server: make clients resubscribe
                s.writer.close()
        self.switches += 1
        log.info("switch gen %d→%d active %s→%s consume %s→%s", old.gen, r.gen, old.active.name, r.active.name,
                 old.consume.name, r.consume.name)

    def drain_done(self, gen: int, consume: Endpoint) -> None:
        """Moves queue consumers to a new backend within the same generation."""
        if gen != self.route.gen:
            raise StateError(f"gen {gen}, proxy is at {self.route.gen}")
        log.info("drain done gen %d: consume %s→%s", gen, self.route.consume.name, consume.name)
        self.route = replace(self.route, consume=consume)
        self.epoch += 1

    def resume(self) -> float:
        if not self.paused:
            return 0.0
        self.paused = False
        ms = _ms_since(self.paused_at)
        self.released.set()
        self.last_pause_ms = ms
        self.max_pause_ms = max(self.max_pause_ms, ms)
        log.info("resume (paused %.2fms)", ms)
        return ms

    def status(self) -> dict:
        return {"gen": self.route.gen, "active": self.route.active.name, "consume": self.route.consume.name,
                "paused": self.paused, "inflight_writes": self.inflight, "open_tx": self.tx,
                "clients": len(self.sessions), "commands": self.commands, "held_commands": self.held_commands,
                "error_replies": self.error_replies, "upstream_errors": self.upstream_errors, "denied": self.denied,
                "switches": self.switches, "last_pause_ms": self.last_pause_ms, "max_pause_ms": self.max_pause_ms,
                "latency_ms": self._latency_summary()}

    def _latency_summary(self) -> dict:
        xs = sorted(self.latency)
        if not xs:
            return {}
        return {"p50": xs[len(xs) // 2], "p99": xs[min(int(.99 * len(xs)), len(xs) - 1)], "max": xs[-1]}


def _ms_since(t: float) -> float:
    return round((time.monotonic() - t) * 1000, 3)


# ---- session -----------------------------------------------------------------

class Session:
    def __init__(self, p: Proxy, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.p, self.reader, self.writer = p, reader, writer
        self.authed = not p.clients
        self.role: str | None = None
        self.db = 0
        self.ups: dict[str, Conn] = {}      # one upstream per backend
        self.up_epoch = 0
        self.multi = self.watching = False
        self.tx_ep: Endpoint | None = None  # a transaction stays on the backend it started on
        self.hello: list[bytes] | None = None   # HELLO (protocol version, SETNAME) replayed on reconnect
        self.pubsub = False

    def login(self, user: bytes, password: bytes) -> bool:
        """Checks an account and remembers its role."""
        if not self.p.clients:
            self.authed = True
            return True
        c = self.p.clients.get(user.decode(errors="replace"))
        if not c or c[0].encode() != password:
            return False
        self.authed, self.role = True, c[1]
        return True

    def strip_hello_auth(self, args: list[bytes]) -> list[bytes] | None:
        out, i = [args[0]], 1
        while i < len(args):
            if args[i].lower() == b"auth" and i + 2 < len(args):
                if not self.login(args[i + 1], args[i + 2]):
                    return None
                i += 3
                continue
            out.append(args[i])
            i += 1
        return out

    @property
    def in_tx(self) -> bool:
        return self.multi or self.watching

    def close(self):
        p = self.p
        p.sessions.discard(self)
        if self.in_tx:
            p.tx -= 1
        self.multi = self.watching = False
        for c in self.ups.values():
            c.close()
        self.writer.close()

    async def run(self):
        self.p.sessions.add(self)
        try:
            await self._loop()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self.close()

    async def _loop(self):
        p, w = self.p, self.writer
        while True:
            try:
                args = await read_command(self.reader)
            except (ConnectionError, asyncio.IncompleteReadError):
                return
            except Exception as e:  # noqa: BLE001
                w.write(error_reply(f"ERR {message(e)}"))
                return
            name = args[0].decode(errors="replace").lower()

            # answered by the proxy itself
            if name == "auth":
                user = args[1] if len(args) >= 3 else b"default"
                ok = self.login(user, args[-1])
                w.write(simple_reply("OK") if ok else error_reply("WRONGPASS invalid username-password pair"))
                await w.drain()
                continue
            if not self.authed and name != "hello":     # HELLO may carry AUTH, checked below
                w.write(error_reply("NOAUTH Authentication required."))
                await w.drain()
                continue
            if name == "quit":
                w.write(simple_reply("OK"))
                return
            if name in DENIED_CMDS:
                p.denied += 1
                w.write(error_reply(f"ERR {name} is not allowed through the proxy"))
                await w.drain()
                continue
            if name == "hello":
                # Backend auth is the proxy's job: check AUTH against the proxy password,
                # strip it, and remember the rest so it's replayed on every new upstream.
                stripped = self.strip_hello_auth(args)
                if stripped is None:
                    w.write(error_reply("WRONGPASS invalid username-password pair"))
                    await w.drain()
                    continue
                args = stripped
                if not self.authed:
                    w.write(error_reply("NOAUTH HELLO must be called with the client already authenticated"))
                    await w.drain()
                    continue
            why = deny(self.role, name, args, p.table)
            if why:
                p.denied += 1
                w.write(error_reply(f"NOPERM {why}"))
                await w.drain()
                continue

            t0 = time.monotonic()   # includes time at the gate: the pause shows up in latency honestly
            was_tx = self.in_tx
            route, epoch, held = await p.enter(name, was_tx)
            try:
                ep = route.active
                if was_tx:
                    ep = self.tx_ep
                elif name in CONSUME_CMDS:
                    ep = route.consume
                    if name == "xreadgroup" and route.consume is not route.active:
                        _cap_block(args, BLOCK_CAP_MS)  # blocked readers come back soon and re-route after DRAIN_DONE
                try:
                    reply = await self._forward(ep, route, epoch, args)
                except Exception as e:  # noqa: BLE001
                    p.upstream_errors += 1
                    reply = error_reply(f"ERR upstream unavailable: {message(e)}")
                self._track(name, args, reply, ep)
                if self.in_tx and not was_tx:
                    p.tx += 1
                elif was_tx and not self.in_tx:
                    p.tx -= 1
            finally:
                if held:
                    p.inflight -= 1

            p.commands += 1
            p.latency.append((time.monotonic() - t0) * 1000)
            is_err = reply[:1] == b"-"
            if is_err:
                p.error_replies += 1
            w.write(reply)
            await w.drain()

            if name in PUBSUB_CMDS and not is_err:
                self.pubsub = True
                await self._pipe(self.ups[ep.name])
                return

    async def _forward(self, ep: Endpoint, route: ProxyRoute, epoch: int, args: list[bytes]) -> bytes | bytearray:
        """Sends one command to ep. After a route change, upstreams to backends that are
        no longer in the route are closed (the old writer connection goes away)."""
        if self.up_epoch != epoch:
            for name in list(self.ups):
                if name not in (route.active.name, route.consume.name) and (not self.tx_ep or name != self.tx_ep.name):
                    self.ups.pop(name).close()
            self.up_epoch = epoch
        up = self.ups.get(ep.name)
        if up is None:
            up = await ep.dial(self.db)
            if self.hello is not None and args[0].lower() != b"hello":
                try:
                    await up.send(self.hello)
                    await read_raw_reply(up.reader, bytearray())
                except BaseException:
                    up.close()
                    raise
            self.ups[ep.name] = up
        # A dead backend must fail the command, not hang it: a hung write would keep
        # PAUSE waiting and the app request open.
        reply = bytearray()
        try:
            async with asyncio.timeout(_deadline(args)):
                up.writer.write(encode_command(args))
                await up.writer.drain()
                await read_raw_reply(up.reader, reply)
        except BaseException:
            up.close()
            self.ups.pop(ep.name, None)
            raise
        return reply

    def _track(self, name: str, args: list[bytes], reply, ep: Endpoint):
        is_err = reply[:1] == b"-"
        if name == "select" and not is_err:
            self.db = int(args[1])
            for n in list(self.ups):    # other backends' connections are on the old db
                if n != ep.name:
                    self.ups.pop(n).close()
        elif name == "hello" and not is_err:
            self.hello = args
        elif name == "watch" and not is_err:
            self.watching, self.tx_ep = True, ep
        elif name == "multi" and not is_err:
            if not self.watching:
                self.tx_ep = ep
            self.multi = True
        elif name in ("exec", "discard"):
            self.multi, self.watching, self.tx_ep = False, False, None
        elif name == "unwatch" and not self.multi:
            self.watching, self.tx_ep = False, None

    async def _pipe(self, up: Conn):
        """Shovels bytes both ways for Pub/Sub / MONITOR until either side closes."""
        async def copy(r: asyncio.StreamReader, w: asyncio.StreamWriter):
            while data := await r.read(READ_LIMIT):
                w.write(data)
                await w.drain()

        tasks = [asyncio.create_task(copy(self.reader, up.writer)), asyncio.create_task(copy(up.reader, self.writer))]
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _block_ms(args: list[bytes]) -> int | None:
    """A BLOCK argument (XREAD / XREADGROUP) before STREAMS."""
    for i in range(1, len(args) - 1):
        a = args[i].lower()
        if a == b"streams":
            break
        if a == b"block":
            with contextlib.suppress(ValueError):
                return int(args[i + 1])
            return None
    return None


def _deadline(args: list[bytes]) -> float | None:
    """The upstream I/O timeout for one command; None = none (BLOCK 0 waits forever)."""
    ms = _block_ms(args)
    if ms is None:
        return UPSTREAM_TIMEOUT
    return None if ms == 0 else ms / 1000 + UPSTREAM_TIMEOUT


def _cap_block(args: list[bytes], cap_ms: int) -> None:
    """Lowers an XREADGROUP BLOCK timeout in place."""
    ms = _block_ms(args)
    if ms is not None and (ms == 0 or ms > cap_ms):
        for i in range(1, len(args) - 1):
            if args[i].lower() == b"block":
                args[i + 1] = str(cap_ms).encode()
                return


# ---- process -------------------------------------------------------------------

def _read_record(path: str) -> dict | None:
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _parse_clients(spec: str) -> dict[str, tuple[str, str]]:
    out = {}
    for item in filter(None, spec.split(",")):
        f = item.split(":")
        if len(f) != 3 or f[2] not in (ROLE_APP, ROLE_WORKER):
            raise SystemExit(f"bad client {f[0]!r}, want user:password:app|worker")
        out[f[0]] = (f[1], f[2])
    return out


def build_app(endpoints: dict[str, Endpoint], control_file: str, token: str, listen: str):
    clients = _parse_clients(os.environ.get("STATEMIG_CLIENTS", ""))
    proxy: Proxy | None = None

    def route_of(rec: dict) -> ProxyRoute:
        a, c = endpoints.get(rec["active"]), endpoints.get(rec["consume"])
        if not a or not c:
            raise ApiError(500, f"control record names unknown backend ({rec['active']!r}/{rec['consume']!r})")
        return ProxyRoute(rec["gen"], a, c)

    def record_for(gen: int) -> ProxyRoute:
        """Loads the control record and checks it is for the requested generation."""
        rec = _read_record(control_file)
        if rec is None:
            raise StateError("control record: not readable")
        if rec["gen"] != gen:
            raise StateError(f"control record is at gen {rec['gen']}, not {gen}")
        return route_of(rec)

    @contextlib.asynccontextmanager
    async def lifespan(_app):
        nonlocal proxy
        rec = _read_record(control_file)
        if rec is None:
            name = os.environ.get("STATEMIG_ACTIVE") or next(iter(endpoints))
            rec = {"gen": 0, "active": name, "consume": name, "paused": False}
            log.info("no control record: starting on %s", name)
        route = route_of(rec)
        for attempt in range(31):       # backends may still be starting
            try:
                c = await route.active.dial()
                try:
                    table = await load_command_table(c)
                finally:
                    c.close()
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 30:
                    raise SystemExit(f"connect {route.active.name}: {message(e)}") from e
                await asyncio.sleep(1)
        proxy = Proxy(route, rec.get("paused", False), clients, table)
        host, port = split_addr(listen)
        server = await asyncio.start_server(proxy.serve_client, host, port, limit=READ_LIMIT)
        log.info("RESP proxy on %s gen=%d active=%s consume=%s paused=%s", listen, route.gen, route.active.name,
                 route.consume.name, proxy.paused)
        yield
        server.close()

    async def status(_body):
        return {"proxy": proxy.status(), "endpoints": [ep.public() for ep in endpoints.values()]}

    async def pause(body):
        max_wait = body.get("max_wait_ms") or 2000
        return {"paused": True, "waited_ms": await proxy.pause(max_wait / 1000)}

    async def switch(body):
        proxy.switch(record_for(body.get("gen", 0)))
        return proxy.status()

    async def resume(_body):
        return {"paused_ms": proxy.resume(), "proxy": proxy.status()}

    async def drain_done(body):
        gen = body.get("gen", 0)
        proxy.drain_done(gen, record_for(gen).consume)
        return proxy.status()

    return admin_app(token, [("GET", "/status", status), ("POST", "/pause", pause), ("POST", "/switch", switch),
                             ("POST", "/resume", resume), ("POST", "/drain-done", drain_done)], lifespan)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    endpoints = parse_endpoints(os.environ.get("STATEMIG_ENDPOINTS", "local=redis://localhost:6379"))
    token = require_token()
    app = build_app(endpoints, os.environ.get("STATEMIG_CONTROL_FILE", "control.json"), token,
                    os.environ.get("STATEMIG_LISTEN", ":6379"))
    serve(app, os.environ.get("STATEMIG_ADMIN", ":7000"))


if __name__ == "__main__":
    main()
