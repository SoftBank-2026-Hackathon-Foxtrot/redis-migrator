"""Which commands write, where their keys are, and what each role may send.

Uses the backend's own COMMAND table ("write" flag), so new commands and Valkey/Redis
differences are handled without a hardcoded list.
"""
from .resp import Conn

ROLE_APP, ROLE_WORKER = "app", "worker"

QUEUE_ADMIN_CMDS = {"xdel", "xtrim", "xsetid", "del", "unlink"}
APP_DENIED_CMDS = {"xgroup", "eval", "evalsha", "fcall", "script", "function"}
# Scripts may write anything; the COMMAND table doesn't flag them as writes.
SCRIPT_CMDS = {"eval", "evalsha", "fcall"}
# Starting a transaction is held during PAUSE like a write, so no new
# transaction opens while the proxy waits for the open ones to finish.
TX_START_CMDS = {"multi", "watch"}
# Queue consumer commands: routed to the consume backend until DRAIN_DONE.
CONSUME_CMDS = {"xreadgroup", "xack", "xautoclaim", "xclaim", "xpending", "xinfo"}
# Always rejected: whole-db / admin commands the app has no business sending.
DENIED_CMDS = {"flushall", "flushdb", "keys", "config", "swapdb", "move"}
PUBSUB_CMDS = {"subscribe", "psubscribe", "ssubscribe", "monitor"}


class CommandTable:
    def __init__(self, held: set[str], keys: dict[str, tuple[int, int, int]]):
        self.held, self.key_specs = held, keys

    def is_held(self, name: str) -> bool:
        """Is the command held at the gate during PAUSE?"""
        return name in self.held

    def keys_of(self, name: str, args: list[bytes]) -> list[bytes]:
        spec = self.key_specs.get(name)
        if not spec or spec[0] <= 0 or spec[2] <= 0:    # no keys, or movable keys
            return []
        first, last, step = spec
        if last < 0:
            last += len(args)
        return args[first:min(last + 1, len(args)):step]


async def load_command_table(c: Conn) -> CommandTable:
    held, keys = set(SCRIPT_CMDS | TX_START_CMDS), {}
    for e in await c.do(b"COMMAND"):
        if len(e) < 3:
            continue
        cmd = e[0].decode().lower()
        if b"write" in e[2]:
            held.add(cmd)
        if len(e) >= 6:
            keys[cmd] = (e[3], e[4], e[5])
    return CommandTable(held, keys)


def deny(role: str | None, name: str, args: list[bytes], table: CommandTable) -> str:
    """Why the role may not send this command, or "" if it may."""
    if not role:
        return ""
    keys = table.keys_of(name, args)
    if role == ROLE_APP:
        if name in CONSUME_CMDS or name in APP_DENIED_CMDS or name in ("xdel", "xtrim"):
            return f"the app may not run {name}"
        for k in keys:
            if k.startswith(b"q:"):
                if name != "xadd" and not (name == "zadd" and k == b"q:delayed"):
                    return "the app may only XADD to queues and schedule on q:delayed"
            elif not k.startswith((b"session:", b"cache:", b"rl:")):
                return f"the app may not touch key {k.decode(errors='replace')}"
    elif role == ROLE_WORKER:
        if name in QUEUE_ADMIN_CMDS:
            return f"a worker may not run {name}"
        if name == "xgroup" and len(args) > 1:
            sub = args[1].decode(errors="replace").lower()
            if sub not in ("create", "createconsumer"):
                return f"a worker may not run xgroup {sub}"
        for k in keys:
            if not k.startswith(b"q:"):
                return "a worker may only touch q:* keys"
    return ""
