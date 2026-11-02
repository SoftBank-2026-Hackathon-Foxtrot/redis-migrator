"""The admin HTTP API of the proxy and the Sync Worker: bearer token, JSON errors."""
import hmac
import logging
import os

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

log = logging.getLogger("statemig.admin")


class ApiError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


def message(e: BaseException) -> str:
    return str(e) or type(e).__name__


def require_token() -> str:
    token = os.environ.get("STATEMIG_ADMIN_TOKEN", "")
    if not token:
        raise SystemExit("STATEMIG_ADMIN_TOKEN is required")
    return token


def _guarded(token: str, handler):
    want = f"Bearer {token}".encode()

    async def endpoint(request: Request) -> JSONResponse:
        if not hmac.compare_digest(request.headers.get("authorization", "").encode(), want):
            return JSONResponse({"error": "admin token required"}, 401)
        try:
            body = await request.json() if await request.body() else {}
            result = await handler(body)
        except ApiError as e:
            return JSONResponse({"error": str(e)}, e.code)
        except Exception as e:  # noqa: BLE001
            log.exception("%s failed", request.url.path)
            return JSONResponse({"error": message(e)}, 500)
        code, data = result if isinstance(result, tuple) else (200, result)
        return JSONResponse(data, code)

    return endpoint


def admin_app(token: str, routes: list[tuple[str, str, object]], lifespan=None) -> Starlette:
    return Starlette(routes=[Route(path, _guarded(token, h), methods=[method]) for method, path, h in routes],
                     lifespan=lifespan)


def split_addr(addr: str) -> tuple[str, int]:
    host, _, port = addr.rpartition(":")
    return host or "0.0.0.0", int(port)


def serve(app: Starlette, addr: str) -> None:
    host, port = split_addr(addr)
    log.info("admin API on %s", addr)
    uvicorn.run(app, host=host, port=port, log_level="warning", access_log=False)
