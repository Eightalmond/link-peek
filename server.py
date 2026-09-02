"""linkpeek — an asyncio TCP server that will eventually fetch link previews.

Right now it speaks a tiny line-based protocol over TCP:

    PREVIEW <url>   ->  ECHO: <url>
    <anything else> ->  ERROR: unknown command

Run it with:  python server.py
"""

import asyncio
import logging

HOST = "127.0.0.1"
PORT = 8888

# Max bytes we will buffer while waiting for a newline, so a client that never
# sends one cannot make the server grow without bound.
MAX_LINE_BYTES = 8192

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("linkpeek")

# Every accepted connection gets the next number from this counter, so log
# lines from concurrent clients can be told apart.
_next_client_id = 0


# Hands out the next client ID. Because asyncio runs all callbacks on a single
# thread, a plain counter is safe here — no lock needed.
def next_client_id() -> int:
    global _next_client_id
    _next_client_id += 1
    return _next_client_id


# Turns one line of text from a client into the line we should send back.
# This is where the protocol lives; keeping it separate from the socket code
# makes it easy to test and easy to swap in real fetching later.
def handle_command(line: str) -> str:
    parts = line.split(maxsplit=1)

    if len(parts) == 2 and parts[0].upper() == "PREVIEW":
        url = parts[1].strip()
        if url:
            return f"ECHO: {url}"

    return "ERROR: unknown command"


# Runs for the lifetime of a single client connection: logs the connect, reads
# newline-terminated commands in a loop until the client goes away, and always
# logs the disconnect and closes the socket on the way out.
async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    client_id = next_client_id()
    peer = writer.get_extra_info("peername")
    log.info("Client %d connected from %s", client_id, peer)

    try:
        while True:
            # readline() returns b"" at EOF, which is how we learn the client
            # hung up cleanly.
            try:
                data = await reader.readline()
            except asyncio.LimitOverrunError:
                log.warning("Client %d sent an over-long line; closing", client_id)
                break

            if not data:
                break

            if len(data) > MAX_LINE_BYTES:
                log.warning("Client %d sent an over-long line; closing", client_id)
                break

            line = data.decode("utf-8", errors="replace").rstrip("\r\n")

            log.info("Client %d -> %r", client_id, line)
            response = handle_command(line)

            writer.write((response + "\n").encode("utf-8"))
            await writer.drain()

    except (ConnectionResetError, BrokenPipeError):
        # The client vanished mid-conversation. Normal on the internet; not an
        # error worth a traceback, and definitely not worth killing the server.
        log.info("Client %d dropped the connection", client_id)
    except asyncio.CancelledError:
        # Server is shutting down. Re-raise so asyncio can finish tearing down.
        raise
    except Exception:
        log.exception("Client %d handler failed", client_id)
    finally:
        log.info("Client %d disconnected", client_id)
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            pass


# Starts the listening socket and serves forever. asyncio gives each accepted
# connection its own task running handle_client(), which is what lets many
# clients be served concurrently.
async def main() -> None:
    server = await asyncio.start_server(handle_client, HOST, PORT)

    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    log.info("linkpeek listening on %s", addrs)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down")
