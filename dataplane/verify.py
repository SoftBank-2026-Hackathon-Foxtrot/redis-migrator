"""VERIFY: compare src and dst while writes are paused.

  delta mode:      only the keys marked since the precompare, then key counts and a sample
  full prefixes:   key sets of both sides must match, then every value and TTL
  sample prefixes: key counts must agree (TTL expiry may differ a little),
                   then up to N keys taken from src are compared
"""
from .resp import Conn, ReplyError

VERIFY_BATCH = 200      # keys per round trip
MAX_LISTED = 20


def read_cmd(typ: str, key: bytes) -> list[bytes] | None:
    return {"string": [b"GET", key],
            "hash": [b"HGETALL", key],
            "list": [b"LRANGE", key, b"0", b"-1"],
            "set": [b"SMEMBERS", key],
            "zset": [b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"],
            "stream": [b"XRANGE", key, b"-", b"+"]}.get(typ)


async def scan_batches(c: Conn, prefix: bytes, n: int):
    """The keys under a prefix in groups of about n."""
    group, cursor = [], b"0"
    while True:
        cursor, found = await c.do(b"SCAN", cursor, b"MATCH", prefix + b"*", b"COUNT", n)
        group += found
        if len(group) >= n or (cursor == b"0" and group):
            yield group
            group = []
        if cursor == b"0":
            return


async def scan_keys(c: Conn, prefix: bytes) -> list[bytes]:
    out, cursor = [], b"0"
    while True:
        cursor, found = await c.do(b"SCAN", cursor, b"MATCH", prefix + b"*", b"COUNT", 10000)
        out += found
        if cursor == b"0":
            return out


async def _snapshot(c: Conn, key: bytes):
    """A key's type, value (normalised) and TTL."""
    typ = (await c.do(b"TYPE", key)).decode()
    if typ == "none":
        return typ, None, -2
    read = read_cmd(typ, key)
    if read is None:
        raise ReplyError(f"unsupported type {typ}")
    v = await c.do(*read)
    return typ, _normalise(typ, v), await c.do(b"PTTL", key)


def _normalise(typ: str, v):
    if typ == "string":
        return v
    if typ == "hash":       # field/value pairs in any order
        parts = sorted(v[i] + b"\0" + v[i + 1] for i in range(0, len(v) - 1, 2))
    elif typ == "set":
        parts = sorted(v)
    elif typ == "stream":
        parts = [p for entry_id, fields in v for p in (entry_id, *fields)]
    else:                   # list, zset: order matters
        parts = v
    return b"\x01".join(parts)


async def _same_key(a: Conn, d: Conn, key: bytes) -> bool:
    ta, va, la = await _snapshot(a, key)
    td, vd, ld = await _snapshot(d, key)
    if ta != td or va != vd:
        return False
    # TTL: both persistent, or both expiring within 2s of each other
    return (la < 0) == (ld < 0) and (la < 0 or abs(la - ld) <= 2000)


async def compare_keys(src: Conn, dst: Conn, keys: list[bytes], logical: bool) -> list[bytes]:
    """The keys that differ, a batch per round trip. Equal DUMP bytes settle it; anything
    else gets the per-type comparison, since the same value can have a different encoding."""
    if logical:
        return [k for k in keys if not await _same_key(src, dst, k)]
    cmds = [c for k in keys for c in ([b"DUMP", k], [b"PTTL", k])]
    sr, dr = await src.pipeline(cmds), await dst.pipeline(cmds)
    bad = []
    for i, k in enumerate(keys):
        sv, dv, st, dt = sr[i * 2], dr[i * 2], sr[i * 2 + 1], dr[i * 2 + 1]
        if sv is None and dv is None:
            continue
        if (isinstance(sv, bytes) and sv == dv and isinstance(st, int) and isinstance(dt, int)
                and (st < 0) == (dt < 0) and abs(st - dt) <= 2000):
            continue
        if not await _same_key(src, dst, k):
            bad.append(k)
    return bad


def _text(k: bytes) -> str:
    return k.decode(errors="replace")


def _report(mode: str, src_keys=0, dst_keys=0) -> dict:
    return {"mode": mode, "src_keys": src_keys, "dst_keys": dst_keys, "compared": 0,
            "mismatched": [], "only_in_src": [], "only_in_dst": []}


async def _compare_into(r: dict, src: Conn, dst: Conn, keys: list[bytes], logical: bool) -> bool:
    ok = True
    for i in range(0, len(keys), VERIFY_BATCH):
        group = keys[i:i + VERIFY_BATCH]
        bad = await compare_keys(src, dst, group, logical)
        r["compared"] += len(group)
        if bad:
            ok = False
            r["mismatched"] += [_text(k) for k in bad[:MAX_LISTED - len(r["mismatched"])]]
    return ok


async def _verify_delta(src: Conn, dst: Conn, delta: list[bytes], sample: dict, logical: bool) -> tuple[dict, bool]:
    r = _report("delta", len(delta))
    ok = await _compare_into(r, src, dst, delta, logical)
    out = {"delta": r}
    for p, n in sample.items():
        sk = await scan_keys(src, p.encode())
        dk = await scan_keys(dst, p.encode())
        pr = _report(f"count + sample {n}", len(sk), len(dk))
        if abs(len(sk) - len(dk)) > max(2, len(sk) // 1000):
            pr["count_diff"] = len(dk) - len(sk)
            ok = False
        chosen = sk[::max(1, len(sk) // max(1, n))][:n]
        ok = await _compare_into(pr, src, dst, chosen, logical) and ok
        out[p] = pr
    return out, ok


async def verify(src: Conn, dst: Conn, req: dict, delta: list[bytes], logical: bool) -> tuple[dict, bool]:
    sample = req.get("sample") or {}
    if req.get("mode") == "delta":
        return await _verify_delta(src, dst, delta, sample, logical)
    out, ok = {}, True
    for p in req.get("full") or []:
        sk = await scan_keys(src, p.encode())
        dk = await scan_keys(dst, p.encode())
        r = _report("full", len(sk), len(dk))
        only_dst = set(dk) - set(sk)
        if only_dst:
            r["only_in_dst"] = [_text(k) for k in list(only_dst)[:MAX_LISTED]]
            ok = False
        ok = await _compare_into(r, src, dst, sk, logical) and ok
        out[p] = r
    for p, n in sample.items():
        sk = await scan_keys(src, p.encode())
        dk = await scan_keys(dst, p.encode())
        r = _report(f"sample {n}", len(sk), len(dk))
        if abs(len(sk) - len(dk)) > max(2, len(sk) // 100):
            r["count_diff"] = len(dk) - len(sk)
            ok = False
        ok = await _compare_into(r, src, dst, sk[:n], logical) and ok
        out[p] = r
    return out, ok
