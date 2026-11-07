"""Redis endpoints (ElastiCache / Memorystore / self-hosted all look the same here)."""
from dataclasses import dataclass, field
from urllib.parse import unquote, urlparse

import redis.asyncio as aioredis
from redis.backoff import NoBackoff
from redis.exceptions import ResponseError
from redis.retry import Retry

# CLIENT LIST tells the migration's own connections from an application's direct ones by name.
CLIENT_NAME = "statemig-admin"


async def _connect_named(conn) -> None:
    """Connect, then name the connection if the server allows it. Memorystore refuses every CLIENT
    command; the connection then stays unnamed instead of failing, as in the Go proxy
    (redisx.Endpoint.dial). redis-py's own client_name fails the connection on a refusal."""
    await conn.on_connect()
    try:
        await conn.send_command("CLIENT", "SETNAME", CLIENT_NAME)
        await conn.read_response()
    except ResponseError:
        pass


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

    def admin_client(self, db: int = 0) -> aioredis.Redis:
        """For the steps inside a write pause. Opened and checked before it (Migration._warm), then
        reused. A connect that does not answer is retried after 0.3s instead of waiting on the
        kernel's SYN retry (1s, then 3s): one lost SYN held every write for 1 to 3s otherwise."""
        return aioredis.Redis(
            host=self.host, port=self.port, db=db,
            username=self.username, password=self.password,
            ssl=self.tls, ssl_cert_reqs="none" if self.tls else None,
            socket_connect_timeout=0.3, socket_timeout=10, health_check_interval=5,
            retry=Retry(NoBackoff(), 6), decode_responses=False, redis_connect_func=_connect_named,
        )

    def client(self, db: int = 0) -> aioredis.Redis:
        """redis-py client for control-plane checks (precheck, verify)."""
        return aioredis.Redis(
            host=self.host, port=self.port, db=db,
            username=self.username, password=self.password,
            ssl=self.tls, ssl_cert_reqs="none" if self.tls else None,
            socket_timeout=10, decode_responses=False, redis_connect_func=_connect_named,
        )
