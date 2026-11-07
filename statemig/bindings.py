"""One application's persistent binding; resolved credentials remain in memory only."""
import asyncio
import hashlib
import json
import os
import re
import uuid
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .backend import Endpoint
from .migration import ControlRecord


class BindingError(Exception):
    def __init__(self, message: str, code: str = "PRECONDITION_FAILED", status: int = 422):
        super().__init__(message)
        self.code, self.status = code, status


class RedisOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tls: bool | None = None


class EndpointInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    host: str = Field(min_length=1)
    port: int = Field(6379, ge=1, le=65535)
    # The existing engine preserves every logical DB. DB 0 is the application default.
    database: Literal["0"] = "0"
    secretRef: str = Field(min_length=1)
    options: RedisOptions = Field(default_factory=RedisOptions)

    @field_validator("host")
    @classmethod
    def valid_host(cls, value: str) -> str:
        if any(c.isspace() for c in value) or any(c in value for c in "/@?#"):
            raise ValueError("host must be a hostname or IP address")
        return value.lower()

    @field_validator("secretRef")
    @classmethod
    def valid_secret_ref(cls, value: str) -> str:
        pattern = r"(?:arn:[^:]+:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:)?[A-Za-z0-9/_+=.@-]+"
        if not re.fullmatch(pattern, value):
            raise ValueError("secretRef must be an AWS Secrets Manager name or ARN")
        return value

    def endpoint_name(self) -> str:
        identity = f"{self.host}:{self.port}/{self.database}"
        return "ep-" + hashlib.sha256(identity.encode()).hexdigest()[:20]


class BindingReq(BaseModel):
    model_config = ConfigDict(extra="forbid")
    resourceId: str = Field(min_length=1)
    kind: Literal["REDIS"] = "REDIS"
    profile: Literal["redis-proxy-sync-v1"] = "redis-proxy-sync-v1"
    source: EndpointInput


def read_secret(secret_ref: str) -> dict:
    """AWS task-role credentials are used. Never propagate SDK errors or secret values."""
    import boto3

    parts = secret_ref.split(":")
    region = (parts[3] if len(parts) > 4 and parts[0] == "arn" and parts[2] == "secretsmanager"
              else os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
    value = boto3.client("secretsmanager", region_name=region).get_secret_value(SecretId=secret_ref)
    return json.loads(value["SecretString"])


class Bindings:
    def __init__(self, path, proxy, sync, record, endpoints, resolver=None):
        self.store = ControlRecord(path)
        self.data = self.store.read() or {"binding": None, "endpoints": {}}
        self.proxy, self.sync, self.record, self.endpoints = proxy, sync, record, endpoints
        self.resolver = resolver or read_secret
        self.resolved: dict[str, tuple[dict, str, str]] = {}
        self.restore_lock = asyncio.Lock()

    def save(self):
        self.store.path.parent.mkdir(parents=True, exist_ok=True)
        self.store.write(**self.data)

    async def resolve(self, spec: EndpointInput) -> tuple[str, str]:
        name = spec.endpoint_name()
        public = spec.model_dump(exclude_none=True)
        cached = self.resolved.get(name)
        if cached and cached[0] == public:
            return cached[1], cached[2]
        try:
            secret = await asyncio.to_thread(self.resolver, spec.secretRef)
            admin_url = secret["url"]
            proxy_url = secret.get("proxyUrl", admin_url)
            for raw in (admin_url, proxy_url):
                parsed = urlsplit(raw)
                if (parsed.scheme not in ("redis", "rediss") or parsed.hostname != spec.host
                        or (parsed.port or 6379) != spec.port or parsed.path not in ("", "/", "/0")
                        or parsed.query or parsed.fragment):
                    raise ValueError("secret endpoint mismatch")
                if spec.options.tls is not None and spec.options.tls != (parsed.scheme == "rediss"):
                    raise ValueError("TLS mismatch")
        except Exception:
            raise BindingError("Redis secret could not be resolved or does not match the endpoint") from None
        self.resolved[name] = (public, admin_url, proxy_url)
        return admin_url, proxy_url

    async def register(self, spec: EndpointInput):
        name = spec.endpoint_name()
        admin_url, proxy_url = await self.resolve(spec)
        try:
            await self.proxy.call("POST", "/backends", {"name": name, "url": proxy_url})
            await self.sync.call("POST", "/backends", {"name": name, "url": admin_url})
        except Exception:
            raise BindingError("Redis backend registration failed; active route was retained") from None
        self.endpoints[name] = Endpoint(name, admin_url)

    async def restore(self, required=()):
        async with self.restore_lock:
            await self._restore(required)

    async def _restore(self, required):
        if not self.data["binding"]:
            return
        rec = self.record.read()
        if rec is None:
            intent = self.data.get("intent") or {}
            if intent.get("status") != "PENDING":
                raise BindingError("binding control record is missing", "UNKNOWN_STATE", 409)
            state = (await self.proxy.call("GET", "/status"))["proxy"]
            name = intent["source"]
            if state.get("active"):
                raise BindingError("initial binding route is not confirmed", "UNKNOWN_STATE", 409)
            self.record.write(gen=0, active=name, consume=name, paused=False)
            rec = self.record.read()
        names = {rec["active"], rec["consume"], *required}
        for name in names:
            if name not in self.data["endpoints"]:
                raise BindingError("required backend metadata is missing", "UNKNOWN_STATE", 409)
            await self.register(EndpointInput.model_validate(self.data["endpoints"][name]))
        try:
            await self.proxy.call("POST", "/bind")
        except Exception:
            raise BindingError("binding route restoration is not confirmed", "UNKNOWN_STATE", 409) from None
        intent = self.data.get("intent") or {}
        if intent.get("status") == "PENDING":
            state = await self.stable_route()
            if state["active"] != intent["source"]:
                raise BindingError("initial binding source changed", "UNKNOWN_STATE", 409)
            intent["status"] = "ACTIVE"
            self.save()

    async def stable_route(self) -> dict:
        try:
            state = (await self.proxy.call("GET", "/status"))["proxy"]
        except Exception:
            raise BindingError("proxy route is unavailable", "UNKNOWN_STATE", 409) from None
        rec = self.record.read()
        if (not state.get("active") or not rec or state["gen"] != rec["gen"]
                or state["active"] != rec["active"] or state["consume"] != rec["consume"]):
            raise BindingError("proxy route does not match its control record", "UNKNOWN_STATE", 409)
        if state["paused"] or rec["paused"] or state["active"] != state["consume"]:
            raise BindingError("proxy is paused or queue drain is unfinished", "RESOURCE_BUSY", 409)
        return state

    async def create(self, req: BindingReq, request_key: str) -> dict:
        digest = hashlib.sha256(json.dumps(req.model_dump(exclude_none=True), sort_keys=True).encode()).hexdigest()
        if self.data["binding"]:
            intent = self.data.get("intent") or {}
            if intent.get("key") != request_key or intent.get("digest") != digest:
                raise BindingError("this application already has another binding intent", "RESOURCE_BUSY", 409)
            await self.restore()
            return await self.view(self.data["binding"]["bindingId"])
        try:
            state = (await self.proxy.call("GET", "/status"))["proxy"]
        except Exception:
            raise BindingError("proxy is unavailable", "UNKNOWN_STATE", 409) from None
        if state.get("active") or self.record.read():
            raise BindingError("proxy is already bound", "RESOURCE_BUSY", 409)
        name = req.source.endpoint_name()
        bid = "binding-" + uuid.uuid4().hex[:12]
        self.data["binding"] = {"bindingId": bid, "bindingRef": "binding:" + bid,
                                "resourceId": req.resourceId, "kind": req.kind, "profile": req.profile}
        self.data["endpoints"][name] = req.source.model_dump(exclude_none=True)
        self.data["intent"] = {"key": request_key, "digest": digest, "source": name, "status": "PENDING"}
        self.save()
        await self.restore()
        return await self.view(bid)

    async def view(self, binding_id: str | None = None) -> dict:
        binding = self.data["binding"]
        if not binding or (binding_id and binding_id not in (binding["bindingId"], binding["bindingRef"])):
            raise BindingError("binding was not found", "NOT_FOUND", 404)
        state = (await self.proxy.call("GET", "/status"))["proxy"]
        source = self.data["endpoints"].get(state.get("active"))
        try:
            await self.stable_route()
            status = "ACTIVE" if source else "UNKNOWN"
        except BindingError:
            status = "UNKNOWN"
        if (self.data.get("intent") or {}).get("status") == "PENDING":
            status = "PENDING"
        return {**binding, "source": source, "status": status,
                "proxy": {"host": os.environ.get("STATEMIG_PROXY_PUBLIC_HOST", "localhost"),
                          "port": int(os.environ.get("STATEMIG_PROXY_PUBLIC_PORT", "6379")),
                          "database": "0", "protocol": "redis"}, "generation": state["gen"]}

    async def prepare(self, source: EndpointInput, target: EndpointInput, resource_id: str):
        binding = self.data["binding"]
        if not binding or binding["resourceId"] != resource_id:
            raise BindingError("register this application's source binding first")
        if (self.data.get("intent") or {}).get("status") != "ACTIVE":
            raise BindingError("initial binding registration is unfinished", "UNKNOWN_STATE", 409)
        await self.restore()
        state = await self.stable_route()
        if source.endpoint_name() != state["active"]:
            raise BindingError("source is not the proxy's active Redis", "SOURCE_MISMATCH", 409)
        if source.endpoint_name() == target.endpoint_name():
            raise BindingError("source and target are the same Redis")
        for spec in (source, target):
            await self.register(spec)
            self.data["endpoints"][spec.endpoint_name()] = spec.model_dump(exclude_none=True)
        self.save()
        return "resource:" + source.endpoint_name(), "resource:" + target.endpoint_name(), binding["bindingRef"]
