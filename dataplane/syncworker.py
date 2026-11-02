"""Sync Worker: keeps the target Redis in step with the source during a migration, and
the old source in step with the new one afterwards. It is not on the request path; it
talks to both Redis servers directly.

env:
  STATEMIG_ENDPOINTS    aws=redis://admin:pw@redis-aws:6379,gcp=... (migration ACL user)
  STATEMIG_ADMIN_TOKEN  bearer token required on the admin API
  STATEMIG_ADMIN        admin HTTP addr (default :7100)
"""
import logging
import os
import time

from .admin import ApiError, admin_app, message, require_token, serve
from .resp import Endpoint, parse_endpoints
from .syncer import Syncer
from .verify import verify as verify_keys

log = logging.getLogger("statemig.syncworker")


def _deadline(body: dict) -> float:
    return time.monotonic() + (body.get("deadline_ms") or 5000) / 1000


def _ms_since(t: float) -> float:
    return round((time.monotonic() - t) * 1000, 3)


def build_app(endpoints: dict[str, Endpoint], token: str):
    sy: Syncer | None = None

    def current() -> Syncer:
        if sy is None:
            raise ApiError(409, "no sync running")
        return sy

    async def status(_body):
        return {"running": True, "sync": sy.status()} if sy else {"running": False}

    async def start(body):
        nonlocal sy
        src, dst = endpoints.get(body.get("src")), endpoints.get(body.get("dst"))
        include = body.get("include") or []
        if not src or not dst or src is dst or not include:
            raise ApiError(400, "need distinct known src/dst and at least one include prefix")
        old, sy = sy, None
        if old:
            await old.stop()
        new = Syncer(src, dst, include, body.get("workers") or 16)
        try:
            await new.start(10)
        except Exception as e:
            await new.stop()
            raise ApiError(500, message(e)) from e
        sy = new
        log.info("sync %s → %s started for %s", src.name, dst.name, include)
        return 201, new.status()

    async def reconcile(body):
        s, t0, deadline = current(), time.monotonic(), _deadline(body)
        try:
            n = await (s.reconcile_delta(deadline) if body.get("mode") == "delta" else s.reconcile(deadline))
        except Exception as e:
            raise ApiError(409, message(e)) from e
        return {"scanned": n, "ms": _ms_since(t0)}

    async def precompare(body):
        s, deadline = current(), _deadline(body)
        try:
            return await s.precompare(deadline)
        except Exception as e:
            raise ApiError(409, message(e)) from e

    async def verify(body):
        s, t0 = current(), time.monotonic()
        src = await s.src.dial()
        try:
            dst = await s.dst.dial()
            try:
                report, ok = await verify_keys(src, dst, body, list(s.delta), s.logical)
            finally:
                dst.close()
        finally:
            src.close()
        return {"ok": ok, "prefixes": report, "ms": _ms_since(t0)}

    async def reverse(body):
        s, deadline = current(), _deadline(body)
        if not body.get("src"):
            raise ApiError(400, "src is required")
        try:
            await s.reverse(body["src"], deadline)
        except Exception as e:
            raise ApiError(409, message(e)) from e
        log.info("reversed: now %s → %s", s.src.name, s.dst.name)
        return s.status()

    async def stop(_body):
        nonlocal sy
        old, sy = sy, None
        if old is None:
            return {"running": False}
        st = old.status()
        await old.stop()
        return {"stopped": st}

    return admin_app(token, [("GET", "/status", status), ("POST", "/start", start), ("POST", "/precompare", precompare),
                             ("POST", "/reconcile", reconcile), ("POST", "/verify", verify),
                             ("POST", "/reverse", reverse), ("POST", "/stop", stop)])


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    endpoints = parse_endpoints(os.environ.get("STATEMIG_ENDPOINTS", "local=redis://localhost:6379"))
    serve(build_app(endpoints, require_token()), os.environ.get("STATEMIG_ADMIN", ":7100"))


if __name__ == "__main__":
    main()
