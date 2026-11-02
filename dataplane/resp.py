"""Minimal RESP codec and Redis connection, shared by the proxy and the Sync Worker.

RESP2, plus RESP3 frame boundaries for relaying. The proxy relays reply frames as
raw bytes; the Sync Worker uses the parsed form: bytes, int, list or None, and a
ReplyError instance for an error reply (raised by Conn.do, left in place in a pipeline).
"""
import asyncio
import socket
import ssl
from dataclasses import dataclass, field
from urllib.parse import unquote, urlparse

CRLF = b"\r\n"
READ_LIMIT = 1 << 16


class ProtocolError(Exception):
    pass


class ReplyError(Exception):
    pass


async def _read_line(r: asyncio.StreamReader) -> bytes:
    line = await r.readline()
    if not line:
        raise ConnectionError("connection closed")
    if not line.endswith(CRLF):
        raise ProtocolError("resp protocol error")
    return line


async def read_command(r: asyncio.StreamReader) -> list[bytes]:
    """One client command (multibulk or inline)."""
    while True:
        line = await _read_line(r)
        if line[0] != 42:   # '*'
            args = line.split()
            if args:
                return args
            continue
        try:
            n = int(line[1:-2])
            args = []
            for _ in range(n):
                hdr = await _read_line(r)
                if hdr[0] != 36:   # '$'
                    raise ProtocolError("resp protocol error")
                size = int(hdr[1:-2])
                if size < 0:
                    raise ProtocolError("resp protocol error")
                args.append((await r.readexactly(size + 2))[:size])
        except ValueError as e:
            raise ProtocolError("resp protocol error") from e
        if args:
            return args


def encode_command(args) -> bytes:
    parts = [b"*%d\r\n" % len(args)]
    for a in args:
        parts += (b"$%d\r\n" % len(a), a, CRLF)
    return b"".join(parts)


async def read_raw_reply(r: asyncio.StreamReader, out: bytearray) -> None:
    """Appends one complete reply frame to out."""
    line = await _read_line(r)
    out += line
    kind = line[:1]
    if kind in b"+-:_#,(":   # RESP3 null/bool/double/bignum are one line too
        return
    try:
        n = int(line[1:-2])
    except ValueError as e:
        raise ProtocolError("resp protocol error") from e
    if kind in b"$!=":        # bulk string, RESP3 blob error, verbatim string
        if n >= 0:
            out += await r.readexactly(n + 2)
        return
    if kind in b"*~>%|":      # array, RESP3 set/push, map/attribute (2 frames per entry)
        if kind in b"%|":
            n *= 2
        if kind == b"|":      # an attribute precedes the actual reply
            n += 1
        for _ in range(n):
            await read_raw_reply(r, out)
        return
    raise ProtocolError(f"unsupported reply type {kind!r}")


async def read_value(r: asyncio.StreamReader):
    """Parses one reply frame."""
    line = await _read_line(r)
    kind, body = line[:1], line[1:-2]
    if kind == b"+":
        return body
    if kind == b"-":
        return ReplyError(body.decode(errors="replace"))
    if kind == b":":
        return int(body)
    if kind == b"$":
        n = int(body)
        return None if n < 0 else (await r.readexactly(n + 2))[:n]
    if kind == b"*":
        n = int(body)
        return None if n < 0 else [await read_value(r) for _ in range(n)]
    raise ProtocolError(f"unsupported reply type {kind!r}")


def simple_reply(s: str) -> bytes:
    return b"+" + s.encode() + CRLF


def error_reply(s: str) -> bytes:
    return b"-" + s.encode(errors="replace") + CRLF


@dataclass(eq=False)
class Endpoint:
    """One Redis deployment (ElastiCache, Memorystore, on-prem...)."""
    name: str
    host: str
    port: int
    tls: bool
    username: str | None = None
    password: str | None = None
    _lookup: asyncio.Future | None = field(default=None, init=False, repr=False)

    def public(self) -> dict:
        return {"name": self.name, "addr": f"{self.host}:{self.port}", "tls": self.tls}

    async def _resolve(self) -> str:
        """One lookup at a time per endpoint, shared by every dial. The event loop has few
        resolver threads and a dead host's lookup holds one for seconds: without this, apps
        dialing a dead Redis starve the lookups of the live one, even after a rollback."""
        if self._lookup is None:
            self._lookup = asyncio.ensure_future(
                asyncio.get_running_loop().getaddrinfo(self.host, self.port, type=socket.SOCK_STREAM))
            self._lookup.add_done_callback(self._lookup_done)
        return (await asyncio.shield(self._lookup))[0][4][0]

    def _lookup_done(self, f: asyncio.Future) -> None:
        self._lookup = None
        if not f.cancelled():
            f.exception()       # read here: every waiter may already have timed out

    async def dial(self, db: int = 0) -> "Conn":
        ctx = None
        if self.tls:    # managed Redis certs are not in the image trust store
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        async with asyncio.timeout(5):
            addr = await self._resolve()
            r, w = await asyncio.open_connection(addr, self.port, ssl=ctx, server_hostname=self.host if ctx else None,
                                                 limit=READ_LIMIT)
            w.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            c = Conn(r, w)
            try:
                if self.password:
                    args = [b"AUTH", self.password.encode()]
                    if self.username:
                        args.insert(1, self.username.encode())
                    try:
                        await c.do(*args)
                    except ReplyError as e:
                        raise ReplyError(f"auth {self.name}: {e}") from e
                if db:
                    await c.do(b"SELECT", str(db).encode())
            except BaseException:
                c.close()
                raise
            return c


def parse_endpoints(spec: str) -> dict[str, Endpoint]:
    """"aws=redis://host:6379,gcp=rediss://:pw@host:6378", in order."""
    eps = {}
    for item in spec.split(","):
        name, sep, raw = item.strip().partition("=")
        if not sep:
            raise ValueError(f"bad endpoint {item!r} (want name=url)")
        u = urlparse(raw)
        if u.scheme not in ("redis", "rediss"):
            raise ValueError(f"unsupported scheme {u.scheme!r}")
        eps[name] = Endpoint(name, u.hostname or "localhost", u.port or 6379, u.scheme == "rediss",
                             unquote(u.username) if u.username else None,
                             unquote(u.password) if u.password else None)
    return eps


class Conn:
    """A plain RESP connection: one command or pipeline at a time."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader, self.writer = reader, writer

    def close(self):
        self.writer.close()

    async def send(self, *cmds) -> None:
        self.writer.write(b"".join(encode_command(c) for c in cmds))
        await self.writer.drain()

    async def do(self, *args):
        await self.send(_as_bytes(args))
        v = await read_value(self.reader)
        if isinstance(v, ReplyError):
            raise v
        return v

    async def pipeline(self, cmds: list) -> list:
        """cmds in one round trip. A Redis error reply stays in its place."""
        await self.send(*cmds)
        return [await read_value(self.reader) for _ in cmds]

    async def tx(self, cmds: list) -> list:
        """cmds inside MULTI/EXEC; returns the EXEC array."""
        await self.send([b"MULTI"], *cmds, [b"EXEC"])
        first_err = None
        for _ in range(len(cmds) + 1):     # +OK for MULTI, +QUEUED per command
            v = await read_value(self.reader)
            if isinstance(v, ReplyError) and first_err is None:
                first_err = v
        v = await read_value(self.reader)
        if first_err is not None:
            raise first_err
        if isinstance(v, ReplyError):
            raise v
        for item in v:
            if isinstance(item, ReplyError):
                raise item
        return v


def _as_bytes(args) -> list[bytes]:
    return [a if isinstance(a, bytes) else str(a).encode() for a in args]
