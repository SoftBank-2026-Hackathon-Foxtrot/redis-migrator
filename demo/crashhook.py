"""statemig for the recovery checks: dies (os._exit) or holds at an exact step.

The migration code is not changed. Admin.call, ControlRecord.write, _set_write and _drop_prefix
are wrapped so each reports a named point before and after it runs. Started by crash.compose.yml.
"""
import asyncio
import os

import uvicorn

import statemig.migration as mig

armed: dict[str, str] = {}
counts: dict[str, int] = {}
trace: list[str] = []
held: str | None = None
release = asyncio.Event()


def _next(name: str) -> str:
    counts[name] = counts.get(name, 0) + 1
    pt = f"{name}#{counts[name]}"
    trace.append(pt)
    return pt


def hit_sync(name: str):
    if armed.get(_next(name)) == "crash":
        os._exit(137)


async def hit(name: str):
    global held
    pt = _next(name)
    action = armed.get(pt)
    if action == "crash":
        os._exit(137)
    if action == "hold":
        held = pt
        release.clear()
        await release.wait()
        held = None


_call, _write, _set_write, _drop = mig.Admin.call, mig.ControlRecord.write, mig.Migration._set_write, mig._drop_prefix


async def call(self, method, path, body=None, timeout=30):
    who = "proxy" if ":7000" in str(self.http.base_url) else "sync"
    if method == "GET":   # polled all the time, not a step
        return await _call(self, method, path, body, timeout)
    tag = f"{who}:{path}"
    await hit("before:" + tag)
    try:
        return await _call(self, method, path, body, timeout)
    finally:   # an answered error is a finished call too: the point must not depend on the outcome
        await hit("after:" + tag)


def write(self, **rec):
    if type(self) is not mig.ControlRecord:
        return _write(self, **rec)
    tag = f"rec:active={rec['active']},consume={rec['consume']},paused={rec['paused']}"
    hit_sync("before:" + tag)
    res = _write(self, **rec)
    hit_sync("after:" + tag)
    return res


async def set_write(self, ep, allowed):
    tag = f"acl:{ep.name}={'rw' if allowed else 'ro'}"
    await hit("before:" + tag)
    await _set_write(self, ep, allowed)
    await hit("after:" + tag)


async def drop_prefix(ep, prefix, *args):
    await hit("before:dropcache")
    res = await _drop(ep, prefix, *args)
    await hit("after:dropcache")
    return res


mig.Admin.call, mig.ControlRecord.write, mig.Migration._set_write, mig._drop_prefix = call, write, set_write, drop_prefix

from statemig import api  # noqa: E402  must see the wrapped classes


@api.app.post("/__hook/arm")
async def arm(body: dict):
    armed.clear()
    counts.clear()
    trace.clear()
    armed.update(body.get("points", {}))
    return {"ok": True}


@api.app.get("/__hook/state")
async def state():
    m = api.current
    return {"trace": trace, "held": held, "record": api.record.read(),
            "mig_state": m.state if m else None, "switching": m.switching if m else None,
            "operation": api.operation}


@api.app.post("/__hook/release")
async def rel():
    release.set()
    return {"ok": True}


if __name__ == "__main__":
    uvicorn.run(api.app, host="0.0.0.0", port=8090, timeout_keep_alive=120)
