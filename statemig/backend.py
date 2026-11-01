"""Redis endpoints (ElastiCache / Memorystore / self-hosted all look the same here)."""
from dataclasses import dataclass, field
from urllib.parse import unquote, urlparse

import redis.asyncio as aioredis


@dataclass
class Endpoint:
    name: str          # e.g. "aws", "gcp", "onprem"
    url: str           # redis://[user:pass@]host:port  or rediss:// for TLS
    host: str = field(init=False)
    port: int = field(init=False)
    username: str | None = field(init=False)
    password: str | None = field(init=False)
    tls: bool = field(init=False)

    def __post_init__(self):
        u = urlparse(self.url)
        if u.scheme not in ("redis", "rediss"):
            raise ValueError(f"unsupported scheme {u.scheme!r}")
        self.host = u.hostname or "localhost"
        self.port = u.port or 6379
        self.username = unquote(u.username) if u.username else None
        self.password = unquote(u.password) if u.password else None
        self.tls = u.scheme == "rediss"

    def public(self) -> dict:
        return {"name": self.name, "host": self.host, "port": self.port, "tls": self.tls}

    def client(self, db: int = 0) -> aioredis.Redis:
        """redis-py client for control-plane checks (precheck, verify)."""
        return aioredis.Redis(
            host=self.host, port=self.port, db=db,
            username=self.username, password=self.password,
            ssl=self.tls, ssl_cert_reqs="none" if self.tls else None,
            socket_timeout=10, decode_responses=False,
        )
