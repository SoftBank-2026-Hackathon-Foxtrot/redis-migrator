"""A Redis that refuses CLIENT (Memorystore) still connects; one that allows it gets the name."""
import asyncio
import unittest

from statemig.backend import CLIENT_NAME, Endpoint


async def _fake_redis(allow_client: bool, seen: list):
    """A minimal RESP server: PING, CLIENT SETNAME (or Memorystore's refusal), anything else +OK."""

    async def handle(reader, writer):
        while True:
            line = await reader.readline()
            if not line:
                break
            if not line.startswith(b"*"):
                continue
            args = []
            for _ in range(int(line[1:])):
                size = int((await reader.readline())[1:])
                args.append((await reader.readexactly(size + 2))[:-2].decode())
            seen.append(args)
            name = args[0].upper()
            if name == "PING":
                writer.write(b"+PONG\r\n")
            elif name == "CLIENT" and not allow_client:
                writer.write(b"-ERR unknown command 'CLIENT', with args beginning with: 'SETNAME'\r\n")
            else:
                writer.write(b"+OK\r\n")
            await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


class ClientNameTest(unittest.IsolatedAsyncioTestCase):
    async def test_connects_whether_or_not_client_is_allowed(self):
        for allow_client in (False, True):
            with self.subTest(allow_client=allow_client):
                seen: list = []
                server, port = await _fake_redis(allow_client, seen)
                async with server:
                    ep = Endpoint("gcp", f"redis://127.0.0.1:{port}")
                    for client in (ep.client(), ep.admin_client()):
                        self.assertTrue(await client.ping())
                        await client.aclose()
                self.assertIn(["CLIENT", "SETNAME", CLIENT_NAME], seen)
