"""A TCP proxy that shifts the Redis server's clock for ONE client.

The wire-level twin of :class:`tests._clock_skew.SkewedClock`: where
``SkewedClock`` offsets the injected Python clock, this proxy offsets the
STORE clock a client sees, by rewriting the reply to every ``TIME``
command with a fixed offset. Everything else is forwarded byte-for-byte.

What it can and cannot skew (this bounds what a test may claim):

* CAN skew: the client-visible ``TIME`` reply -- the only store clock the
  non-script paths read (``taskq.ratelimit._redis_utils.redis_time_seconds``,
  the three peek paths).
* CANNOT skew: ``redis.call('TIME')`` executed INSIDE a Lua script -- that
  runs on the server against the server's own clock and never touches the
  wire. The acquire scripts therefore always measure with the real server
  clock through the proxy, which is exactly the property the crossing pins
  exercise: a client whose view of the store clock is shifted must not move
  admission math.

Cross-domain STAMPS (a ts written by the OTHER store's clock, the
hypothesized outage-fallback residue) are induced in tests by direct state
writes; no code path produces them, and the pins prove the acquire/refund
math ignores them.

Wire discipline: RESP2 both directions (redis-py's default protocol), a
FIFO of outstanding commands per connection marks which reply belongs to
``TIME`` (pipelining keeps request/response order per connection), and a
reply is rewritten only when its marker says TIME and its shape is the
exact ``*2\\r\\n$<n>\\r\\n<digits>\\r\\n$<n>\\r\\n<digits>\\r\\n`` TIME
shape, so an HMGET reply that happens to carry two numeric-looking strings
(an ``HMGET key tokens ts`` reply is structurally identical) can never be
rewritten by mistake.
"""

import asyncio
import contextlib
from collections import deque

__all__ = ["RedisTimeSkewProxy"]


class _Reply:
    """One parsed RESP2 reply: a kind tag plus its raw payload."""

    __slots__ = ("kind", "value")

    def __init__(self, kind: str, value: bytes | list["_Reply"] | None) -> None:
        self.kind = kind
        self.value = value


def _parse_reply(buf: bytearray, pos: int) -> tuple[_Reply, int] | None:
    """Parse one complete RESP2 reply from *buf* at *pos*.

    Returns ``(reply, next_pos)``, or ``None`` when the buffer does not
    yet hold a complete reply (the caller keeps the bytes and waits).
    """
    if pos >= len(buf):
        return None
    kind = chr(buf[pos])
    pos += 1
    line_end = buf.find(b"\r\n", pos)
    if line_end < 0:
        return None
    payload = bytes(buf[pos:line_end])
    pos = line_end + 2

    if kind in "+-:":
        return _Reply(kind, payload), pos
    if kind == "_":  # RESP3 null
        return _Reply("_", None), pos
    if kind in "#,":  # RESP3 boolean / double: one line payload
        return _Reply(kind, payload), pos
    if kind == "(":  # RESP3 big number: line payload
        return _Reply("(", payload), pos
    if kind == "$":
        if payload == b"-1":
            return _Reply("$", None), pos
        n = int(payload)
        if len(buf) < pos + n + 2:
            return None
        data = bytes(buf[pos : pos + n])
        pos += n + 2  # the bulk's own trailing CRLF
        return _Reply("$", data), pos
    if kind == "*":
        if payload == b"-1":
            return _Reply("*", None), pos
        n = int(payload)
        items: list[_Reply] = []
        for _ in range(n):
            parsed = _parse_reply(buf, pos)
            if parsed is None:
                return None
            item, pos = parsed
            items.append(item)
        return _Reply("*", items), pos
    if kind in "%~>":  # RESP3 map (N pairs) / set / push: 2N or N sub-replies
        n = int(payload)
        if n < 0:
            return _Reply(kind, None), pos
        count = n * 2 if kind == "%" else n
        items = []
        for _ in range(count):
            parsed = _parse_reply(buf, pos)
            if parsed is None:
                return None
            item, pos = parsed
            items.append(item)
        return _Reply(kind, items), pos
    if kind in "!=":  # verbatim string / verbatim error: like a bulk with a 3-byte type prefix
        if payload == b"-1":
            return _Reply(kind, None), pos
        n = int(payload)
        if len(buf) < pos + n + 2:
            return None
        data = bytes(buf[pos : pos + n])
        pos += n + 2
        return _Reply(kind, data), pos
    raise ValueError(f"unexpected RESP2/RESP3 reply tag {kind!r} at byte {pos - 1}")


def _dump_reply(reply: _Reply) -> bytes:
    """Re-serialize a parsed reply (used only for rewritten TIME replies)."""
    kind, value = reply.kind, reply.value
    if kind in ("+", "-", ":", "#", ",", "("):
        return kind.encode() + value + b"\r\n"  # type: ignore[operator]  # Why: line replies always carry bytes; the tag guards above guarantee it.
    if kind == "_":
        return b"_\r\n"
    if kind in ("$", "!", "="):
        if value is None:
            return kind.encode() + b"-1\r\n"
        return kind.encode() + b"%d\r\n%s\r\n" % (len(value), value)  # type: ignore[operator]  # Why: bulk replies always carry bytes.
    if value is None:
        return kind.encode() + b"-1\r\n"
    if kind == "%":
        return b"%%%d\r\n" % (len(value) // 2) + b"".join(_dump_reply(item) for item in value)  # type: ignore[operator]  # Why: map replies always carry a list.
    return kind.encode() + b"%d\r\n" % len(value) + b"".join(_dump_reply(item) for item in value)  # type: ignore[operator]  # Why: array/set/push replies always carry a list.


def _parse_command(buf: bytearray) -> tuple[bytes, int] | None:
    """Parse one client request (an array of bulk strings); return its
    upper-cased first token and the consumed length, or ``None`` when
    incomplete. Raises on a non-array request: the proxy only speaks to
    redis-py clients, which always send the array-of-bulks shape."""
    if not buf or chr(buf[0]) != "*":
        raise ValueError("client stream is not an array of bulk strings")
    line_end = buf.find(b"\r\n")
    if line_end < 0:
        return None
    n = int(buf[1:line_end])
    pos = line_end + 2
    first: bytes | None = None
    for _ in range(n):
        if pos >= len(buf) or chr(buf[pos]) != "$":
            raise ValueError("client stream is not an array of bulk strings")
        length_end = buf.find(b"\r\n", pos)
        if length_end < 0:
            return None
        length = int(buf[pos + 1 : length_end])
        pos = length_end + 2
        if len(buf) < pos + length + 2:
            return None
        if first is None:
            first = bytes(buf[pos : pos + length])
        pos += length + 2
    assert first is not None
    return first.upper(), pos


def _is_time_reply_shape(reply: _Reply) -> bool:
    """The exact TIME reply shape: a 2-array of two numeric strings.

    Redis answers with bulk strings; Dragonfly (the test stack's
    Redis-compatible store) answers with RESP integers. Both are a
    2-array whose members are numeric bytes.
    """
    if reply.kind != "*" or not isinstance(reply.value, list) or len(reply.value) != 2:
        return False
    return all(
        item.kind in ("$", ":") and isinstance(item.value, bytes) and item.value.isdigit()
        for item in reply.value
    )


def _skew_time_reply(reply: _Reply, skew_seconds: int) -> _Reply:
    """Shift the seconds member of a parsed TIME reply by *skew_seconds*.

    TIME replies as ``[seconds, microseconds]``; the microseconds pass
    through, the seconds carry the whole offset (integer skew), so the
    skewed clock stays well-formed.
    """
    assert reply.kind == "*" and isinstance(reply.value, list)
    seconds = reply.value[0]
    assert isinstance(seconds.value, bytes)
    reply.value[0] = _Reply("$", str(int(seconds.value) + skew_seconds).encode())
    return reply


class _ConnectionPipe:
    """One client connection's bidirectional pipe with TIME tracking."""

    __slots__ = (
        "_client_reader",
        "_client_writer",
        "_pending",
        "_skew_seconds",
        "_upstream_reader",
        "_upstream_writer",
    )

    def __init__(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        upstream: tuple[asyncio.StreamReader, asyncio.StreamWriter],
        skew_seconds: int,
    ) -> None:
        self._client_reader = client_reader
        self._client_writer = client_writer
        self._upstream_reader, self._upstream_writer = upstream
        self._skew_seconds = skew_seconds
        self._pending: deque[bool] = deque()  # True when that reply belongs to TIME

    async def run(self) -> None:
        c2s = asyncio.create_task(self._pipe_client_to_server())
        s2c = asyncio.create_task(self._pipe_server_to_client())
        try:
            await asyncio.wait({c2s, s2c}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (c2s, s2c):
                task.cancel()
            await asyncio.gather(c2s, s2c, return_exceptions=True)
            self._upstream_writer.close()
            self._client_writer.close()
            for writer in (self._upstream_writer, self._client_writer):
                with contextlib.suppress(ConnectionError, OSError):
                    await writer.wait_closed()

    async def _pipe_client_to_server(self) -> None:
        # Requests always pass through untouched; the parse only TAGS
        # which requests are TIME so the reply pipe knows what to skew.
        parse_buf = bytearray()
        while True:
            chunk = await self._client_reader.read(65536)
            if not chunk:
                return
            self._upstream_writer.write(chunk)
            await self._upstream_writer.drain()
            parse_buf.extend(chunk)
            try:
                while parse_buf:
                    parsed = _parse_command(parse_buf)
                    if parsed is None:
                        break
                    name, consumed = parsed
                    self._pending.append(name == b"TIME")
                    del parse_buf[:consumed]
            except ValueError:
                # Not redis-py's request shape: forward raw and stop
                # tracking (the skew then simply never fires).
                self._pending.clear()

    async def _pipe_server_to_client(self) -> None:
        buf = bytearray()
        while True:
            chunk = await self._upstream_reader.read(65536)
            if not chunk:
                return
            buf.extend(chunk)
            while buf:
                parsed = _parse_reply(buf, 0)
                if parsed is None:
                    break
                reply, consumed = parsed
                raw = bytes(buf[:consumed])
                del buf[:consumed]
                if reply.kind == ">":
                    # RESP3 out-of-band PUSH (the maintenance-notification
                    # channel a redis-py 8.x pool enables on every live
                    # connection): not a reply to any queued request, so
                    # it must not consume a marker.
                    self._client_writer.write(raw)
                    await self._client_writer.drain()
                    continue
                # One reply per request, in order: every parsed reply pops
                # its request's marker, TIME or not.
                is_time = self._pending.popleft() if self._pending else False
                if is_time and _is_time_reply_shape(reply):
                    self._client_writer.write(
                        _dump_reply(_skew_time_reply(reply, self._skew_seconds))
                    )
                    await self._client_writer.drain()
                    continue
                self._client_writer.write(raw)
                await self._client_writer.drain()


class RedisTimeSkewProxy:
    """Run a listener that proxies to Redis, skewing TIME replies.

    ``skew_seconds > 0`` moves the client-visible store clock AHEAD,
    ``< 0`` BEHIND. ``url()`` returns a redis:// URL bound to the
    listener, ready for ``redis_async.from_url``.
    """

    def __init__(
        self, upstream_host: str, upstream_port: int, *, skew_seconds: int, upstream_db: int = 0
    ) -> None:
        self._upstream_host = upstream_host
        self._upstream_port = upstream_port
        self._upstream_db = upstream_db
        self._skew_seconds = skew_seconds
        self._server: asyncio.Server | None = None
        self._handlers: set[asyncio.Task[None]] = set()
        self._host: str = "127.0.0.1"
        self._port: int = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        sockets = self._server.sockets or ()
        assert sockets, "proxy listener produced no socket"
        host, port = sockets[0].getsockname()[:2]
        self._host, self._port = str(host), int(port)

    async def stop(self) -> None:
        if self._server is None:
            return
        server, self._server = self._server, None
        server.close()
        # Cancel the live connection pipes explicitly rather than
        # ``wait_closed()``-ing for them: a redis-py pool's maintenance-
        # notification push connection outlives ``aclose()`` (its
        # proactive-reconnect handler reopens), so waiting for handlers
        # to drain on their own never returns.
        for task in list(self._handlers):
            task.cancel()
        await asyncio.gather(*self._handlers, return_exceptions=True)
        self._handlers.clear()

    def url(self) -> str:
        """A redis:// URL pointing at the skewed listener, same db as the
        upstream URL the caller passed (a proxied client must land in the
        SAME database as the direct one, or the pins measure nothing)."""
        return f"redis://{self._host}:{self._port}/{self._upstream_db}"

    def _accept(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.create_task(self._handle(client_reader, client_writer))
        self._handlers.add(task)
        task.add_done_callback(self._handlers.discard)

    async def _handle(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ) -> None:
        try:
            upstream = await asyncio.open_connection(self._upstream_host, self._upstream_port)
        except (ConnectionError, OSError):
            client_writer.close()
            return
        pipe = _ConnectionPipe(client_reader, client_writer, upstream, self._skew_seconds)
        await pipe.run()
