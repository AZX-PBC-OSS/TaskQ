"""Admin rate-limits page Redis state fetch: one round trip, not one per bucket.

``_fetch_redis_rl_state`` (taskq.web.admin.ops) reads live Redis state for
every configured/materialized rate-limit bucket. Its name set is UNBOUNDED
by construction: the page unions the in-process registry with every
``rate_limit_buckets`` PG row — keyed buckets are published there on first
acquisition and never removed — so the set grows with distinct keys ever
seen. One awaited Redis call per name (the pre-fix shape) is the
per-row-round-trip class: at 10k keyed buckets that is 10k sequential
awaits (~seconds at ~0.3 ms each) inside one request handler, on a page
the admin UI re-polls every ``admin_ui_polling_interval_seconds``.

The fix is a single Redis pipeline: N commands queued, ONE ``execute()``
round trip, identical per-key semantics. This file pins that contract
and the decode/mapping behavior.
"""

from __future__ import annotations

import pytest

from taskq.web.admin.ops import (
    _fetch_redis_rl_state,  # pyright: ignore[reportPrivateUsage]  # Why: pinning the production fetch path, not a copy.
)


class _RecordingPipeline:
    """Queue-record stand-in for a redis-py pipeline.

    The behavior under test is the round-trip count on the wire, so the
    seam records queued commands and counts ``execute()`` calls; the
    canned results let the decode/mapping assertions run against real
    return shapes (bytes dict / bytes / int).
    """

    def __init__(self, results: list[object]) -> None:
        self._results = results
        self.commands: list[tuple[str, str]] = []
        self.executes = 0

    def hgetall(self, key: str) -> _RecordingPipeline:
        self.commands.append(("hgetall", key))
        return self

    def get(self, key: str) -> _RecordingPipeline:
        self.commands.append(("get", key))
        return self

    def zcard(self, key: str) -> _RecordingPipeline:
        self.commands.append(("zcard", key))
        return self

    async def execute(self) -> list[object]:
        self.executes += 1
        return list(self._results)


class _RecordingRedis:
    """Redis client stand-in recording direct (unbatched) calls."""

    def __init__(self, pipeline_results: list[object]) -> None:
        self._pipeline_results = pipeline_results
        self.pipelines: list[_RecordingPipeline] = []
        self.direct_calls: list[tuple[str, str]] = []

    def pipeline(self) -> _RecordingPipeline:
        pipe = _RecordingPipeline(self._pipeline_results)
        self.pipelines.append(pipe)
        return pipe

    async def hgetall(self, key: str) -> dict[bytes, bytes]:
        self.direct_calls.append(("hgetall", key))
        return {b"tokens": b"9.5", b"ts": b"100.25"}

    async def get(self, key: str) -> bytes:
        self.direct_calls.append(("get", key))
        return b"123.5"

    async def zcard(self, key: str) -> int:
        self.direct_calls.append(("zcard", key))
        return 7


async def test_fetch_redis_state_is_one_pipelined_round_trip() -> None:
    """N buckets must cost ONE execute() call, zero direct per-key calls."""
    names = [
        ("tb_a", "token_bucket"),
        ("tb_b", "token_bucket"),
        ("gcra_a", "sliding_window_gcra"),
        ("log_a", "sliding_window_log"),
    ]
    # Canned results in command order: two HGETALLs, one GET, one ZCARD.
    client = _RecordingRedis(
        [
            {b"tokens": b"9.5", b"ts": b"100.25"},
            {b"tokens": b"3.0", b"ts": b"101.5"},
            b"123.5",
            7,
        ]
    )

    result = await _fetch_redis_rl_state(client, "sweepaudit", names)  # type: ignore[arg-type]  # Why: recording stand-in for redis.asyncio.Redis at the module's Any erasure boundary.

    # Exactly one pipeline, executed exactly once — N commands, 1 round trip.
    assert len(client.pipelines) == 1, (
        f"expected exactly one pipeline, got {len(client.pipelines)} "
        f"(direct calls: {client.direct_calls})"
    )
    pipe = client.pipelines[0]
    assert pipe.executes == 1, f"expected one execute() round trip, got {pipe.executes}"
    assert pipe.commands == [
        ("hgetall", "taskq:sweepaudit:rl:tb:{tb_a}"),
        ("hgetall", "taskq:sweepaudit:rl:tb:{tb_b}"),
        ("get", "taskq:sweepaudit:sw_gcra:{gcra_a}"),
        ("zcard", "taskq:sweepaudit:sw:{log_a}"),
    ], f"pipeline commands must be the per-bucket reads in order: {pipe.commands}"
    # No per-key direct (unbatched) calls may remain.
    assert client.direct_calls == [], f"unbatched calls remained: {client.direct_calls}"

    # Decode/mapping: bytes dict → str dict; GET → tat str; ZCARD → count str.
    assert result == {
        "tb_a": {"tokens": "9.5", "ts": "100.25"},
        "tb_b": {"tokens": "3.0", "ts": "101.5"},
        "gcra_a": {"tat": "123.5"},
        "log_a": {"count": "7"},
    }


async def test_fetch_redis_state_empty_names_is_no_round_trip() -> None:
    """No buckets → no pipeline, no round trip, empty result."""
    client = _RecordingRedis([])
    result = await _fetch_redis_rl_state(client, "sweepaudit", [])  # type: ignore[arg-type]  # Why: same erasure boundary as above.
    assert result == {}
    assert client.pipelines == []
    assert client.direct_calls == []


async def test_fetch_redis_state_degrades_to_none_on_failure() -> None:
    """A failed pipeline must degrade to None (page renders without live
    state), never raise into the request handler."""

    class _FailingPipeline(_RecordingPipeline):
        async def execute(self) -> list[object]:
            raise ConnectionError("redis gone")

    class _FailingRedis(_RecordingRedis):
        def pipeline(self) -> _RecordingPipeline:
            pipe = _FailingPipeline([])
            self.pipelines.append(pipe)
            return pipe

    client = _FailingRedis([])
    result = await _fetch_redis_rl_state(client, "sweepaudit", [("tb_a", "token_bucket")])  # type: ignore[arg-type]  # Why: same erasure boundary as above.
    assert result is None


async def test_fetch_redis_state_against_fakeredis() -> None:
    """End-to-end against a real Redis implementation: the pipelined path
    reads the same values the direct path would (keys seeded per the
    module's documented conventions).
    """
    fakeredis = pytest.importorskip("fakeredis.aioredis")
    client = fakeredis.FakeRedis()
    await client.hset("taskq:sweepaudit:rl:tb:{tb_live}", mapping={"tokens": "9.5", "ts": "100.25"})
    await client.set("taskq:sweepaudit:sw_gcra:{gcra_live}", "123.5")
    await client.zadd("taskq:sweepaudit:sw:{log_live}", {"r1": 1.0, "r2": 2.0})

    result = await _fetch_redis_rl_state(
        client,  # type: ignore[arg-type]  # Why: fakeredis satisfies the runtime protocol at the Any erasure boundary.
        "sweepaudit",
        [
            ("tb_live", "token_bucket"),
            ("gcra_live", "sliding_window_gcra"),
            ("log_live", "sliding_window_log"),
            ("missing", "token_bucket"),
        ],
    )
    assert result is not None
    assert result == {
        "tb_live": {"tokens": "9.5", "ts": "100.25"},
        "gcra_live": {"tat": "123.5"},
        "log_live": {"count": "2"},
    }
    # A bucket with no Redis state (expired or never acquired) is omitted.
    assert "missing" not in result
    await client.aclose()
