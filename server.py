"""linkpeek — an asyncio TCP server that fetches link previews.

It speaks a tiny line-based protocol over TCP:

    PREVIEW <url>   ->  OK title=<title> desc=<description>
                        ERROR: fetch timed out
                        ERROR: could not fetch url
                        ERROR: bad status <status_code>
    <anything else> ->  ERROR: unknown command

Run it with:  python server.py
"""

import asyncio
import functools
import logging

import aiohttp
from bs4 import BeautifulSoup

HOST = "127.0.0.1"
PORT = 8888

# Max bytes we will buffer while waiting for a newline, so a client that never
# sends one cannot make the server grow without bound.
MAX_LINE_BYTES = 8192

# How long a single PREVIEW fetch may take, end to end: connect, send, and read
# the whole body.
FETCH_TIMEOUT_SECONDS = 5

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


# Squashes a scraped string onto a single line. Our protocol puts one response
# per line, so a title containing a newline would look like a second response
# to the client and desync the conversation.
def one_line(text: str) -> str:
    return " ".join(text.split())


# Pulls the two fields we care about out of a page's HTML: the <title> text and
# the content of <meta name="description">. Either can be missing, in which case
# we return an empty string rather than failing the whole preview.
def extract_preview(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")

    title = one_line(soup.title.get_text()) if soup.title else ""

    description = ""
    # attrs= form (rather than name=) because `name` is BeautifulSoup's own
    # keyword for the tag name, so it cannot be used for the name attribute.
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        description = one_line(meta["content"])

    return title, description


# Fetches one URL and turns it into a single protocol response line. Every
# failure mode the client can trigger — timeout, bad host, non-200 — is caught
# here and reported as an ERROR line, so one bad URL never takes down the
# connection or the server.
async def fetch_preview(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url) as response:
            if response.status != 200:
                return f"ERROR: bad status {response.status}"
            html = await response.text()
    except asyncio.TimeoutError:
        # Covers the whole request; aiohttp raises this when ClientTimeout fires.
        return "ERROR: fetch timed out"
    except (aiohttp.ClientError, ValueError, UnicodeError) as exc:
        # ClientError covers DNS failures, refused connections, TLS errors and
        # malformed URLs (aiohttp.InvalidURL). ValueError/UnicodeError catch the
        # things that never reach the network at all, like a missing scheme or
        # a hostname that will not encode.
        log.info("Fetch failed for %r: %s", url, exc)
        return "ERROR: could not fetch url"

    title, description = extract_preview(html)
    return f"OK title={title} desc={description}"


# Turns one line of text from a client into the line we should send back.
# This is where the protocol lives; keeping it separate from the socket code
# makes it easy to test.
async def handle_command(session: aiohttp.ClientSession, line: str) -> str:
    parts = line.split(maxsplit=1)

    if len(parts) == 2 and parts[0].upper() == "PREVIEW":
        url = parts[1].strip()
        if url:
            return await fetch_preview(session, url)

    return "ERROR: unknown command"


# Runs for the lifetime of a single client connection: logs the connect, reads
# newline-terminated commands in a loop until the client goes away, and always
# logs the disconnect and closes the socket on the way out.
async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    session: aiohttp.ClientSession,
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
            response = await handle_command(session, line)

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
    timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT_SECONDS)

    # One ClientSession for the whole server, shared by every connection and
    # every request, rather than one per PREVIEW.
    #
    # A ClientSession is not a lightweight request helper — it owns a connection
    # pool, a DNS cache, and cookie state. Sharing one means a second PREVIEW of
    # the same host reuses the already-open TCP+TLS connection and the cached DNS
    # answer instead of paying for a fresh handshake, which is most of the
    # latency of a small fetch.
    #
    # Creating one per request would also leak: a session must be closed to
    # release its connector, and a per-request session that is abandoned when a
    # client disconnects mid-fetch leaves sockets in the pool until the garbage
    # collector notices. Under concurrent clients that adds up fast.
    #
    # The session must be created inside a running event loop — that is why it
    # lives here in main() and not at module import time.
    async with aiohttp.ClientSession(timeout=timeout) as session:
        server = await asyncio.start_server(
            functools.partial(handle_client, session=session), HOST, PORT
        )

        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        log.info("linkpeek listening on %s", addrs)

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down")
