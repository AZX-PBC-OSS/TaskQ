"""A local wire proxy that drops the first COMMIT ReadyForQuery after arming."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit


@dataclass(slots=True)
class LostCommitProxy:
    """Real PostgreSQL commits; the proxy disconnects before the client's final ack."""

    upstream_dsn: str
    server: asyncio.Server | None = None
    _tasks: set[asyncio.Task[None]] = field(default_factory=set)
    _writers: set[asyncio.StreamWriter] = field(default_factory=set)
    armed: bool = False
    disconnects: int = 0

    async def start(self) -> str:
        address = urlsplit(self.upstream_dsn)
        assert address.hostname is not None and address.port is not None
        self.server = await asyncio.start_server(self._accept, host="127.0.0.1", port=0)
        socket = self.server.sockets[0]
        host, port = socket.getsockname()[:2]
        return urlunsplit(
            address._replace(netloc=f"{address.username}:{address.password}@{host}:{port}")
        )

    @property
    def open_resource_count(self) -> int:
        return len(self._tasks) + len(self._writers)

    def arm(self) -> None:
        assert not self.armed
        self.armed = True

    async def close(self) -> None:
        for writer in list(self._writers):
            writer.close()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert isinstance(task, asyncio.Task)
        self._tasks.add(task)
        self._writers.add(writer)
        address = urlsplit(self.upstream_dsn)
        assert address.hostname is not None and address.port is not None
        upstream_writer: asyncio.StreamWriter | None = None
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                address.hostname, address.port
            )
            self._writers.add(upstream_writer)
            await asyncio.gather(
                self._copy_client(reader, upstream_writer),
                self._copy_server(upstream_reader, writer),
            )
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError):
            pass
        finally:
            self._writers.discard(writer)
            if upstream_writer is not None:
                self._writers.discard(upstream_writer)
            writer.close()
            if upstream_writer is not None:
                upstream_writer.close()
            self._tasks.discard(task)

    async def _copy_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        while True:
            data = await reader.read(65536)
            if not data:
                return
            writer.write(data)
            await writer.drain()

    async def _copy_server(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        buffered = b""
        committed = False
        while True:
            buffered += await reader.read(65536)
            # SSLRequest's answer is a lone 'S'/'N', without a normal message header.
            if len(buffered) == 1 and buffered in (b"S", b"N"):
                writer.write(buffered)
                buffered = b""
                await writer.drain()
                continue
            while len(buffered) >= 5:
                length = int.from_bytes(buffered[1:5], "big")
                if len(buffered) < length + 1:
                    break
                message, buffered = buffered[: length + 1], buffered[length + 1 :]
                kind, payload = message[:1], message[5:]
                if kind == b"C" and payload == b"COMMIT\x00" and self.armed:
                    committed = True
                if kind == b"Z" and committed:
                    self.disconnects += 1
                    self.armed = False
                    writer.close()
                    return
                writer.write(message)
                await writer.drain()
            if not buffered and reader.at_eof():
                return
