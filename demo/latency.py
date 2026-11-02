"""TCP forwarder that adds a fixed one-way delay, to measure a cutover as if the source
Redis were in another cloud.

env: LISTEN (default :6379), TARGET (host:port), DELAY_MS (one way)
"""
import asyncio
import logging
import os
import time

log = logging.getLogger("statemig.latency")


def split_addr(addr: str) -> tuple[str, int]:
    host, _, port = addr.rpartition(":")
    return host or "0.0.0.0", int(port)


async def relay(r: asyncio.StreamReader, w: asyncio.StreamWriter, delay: float) -> None:
    """Copies r → w, delaying every chunk without reordering or throttling."""
    q: asyncio.Queue = asyncio.Queue()

    async def send():
        while (item := await q.get()) is not None:
            data, due = item
            await asyncio.sleep(due - time.monotonic())
            w.write(data)
            await w.drain()

    sender = asyncio.create_task(send())
    try:
        while data := await r.read(1 << 16):
            q.put_nowait((data, time.monotonic() + delay))
    except ConnectionError:
        pass
    q.put_nowait(None)
    try:
        await sender
    except ConnectionError:
        pass
    w.close()


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    listen, target = os.environ.get("LISTEN", ":6379"), os.environ.get("TARGET", "")
    if not target or not os.environ.get("DELAY_MS"):
        raise SystemExit("TARGET and DELAY_MS are required")
    delay = int(os.environ["DELAY_MS"]) / 1000
    target_host, target_port = split_addr(target)

    async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter):
        try:
            up_r, up_w = await asyncio.open_connection(target_host, target_port)
        except OSError as e:
            log.warning("%s", e)
            w.close()
            return
        await asyncio.gather(relay(r, up_w, delay), relay(up_r, w, delay))

    host, port = split_addr(listen)
    server = await asyncio.start_server(handle, host, port)
    log.info("%s → %s, %dms each way", listen, target, delay * 1000)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
