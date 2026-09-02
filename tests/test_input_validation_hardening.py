"""Input-validation hardening at the library's caller-facing chokepoints.

Each section pins a gap found in the integ-merge-v2 audit where untrusted
caller text sailed past a validator that existed but never ran, or past a
boundary that had none at all:

- queue names are actually validated where they enter (enqueue and actor
  declaration) — a malformed name used to be silently accepted, stranding
  jobs on a queue no worker's ``queue = ANY($1)`` ever matches;
- the InMemory enqueue mirror rejects a NUL in payload/metadata the same
  way PG's bind-time jsonb guard does, so an app validated against
  InMemory cannot pass a payload the first real PG enqueue rejects;
- ``JobFilter`` text predicates reject a NUL before they reach a backend
  (``list_jobs`` / ``cancel_where``);
- ``BatchFilter.limit`` is bounded above (``list_batches`` runs a
  per-batch count join with no cursor pagination);
- ``ScheduleCreateArgs`` caller text rejects a NUL before the bind;
- the capacity cache reports ``has_snapshot`` from "has a refresh ever
  succeeded", not from the row count of the last snapshot.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
import structlog.testing
from pydantic import BaseModel

from taskq import actor
from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import (
    BatchFilter,
    IdentityKey,
    JobFilter,
    ScheduleCreateArgs,
)
from taskq.client._args import build_enqueue_args
from taskq.client._capacity import ActorCapacityCache
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int = 1


@actor(name="validation_hardening_actor")
async def _hardening_actor(_payload: _Payload) -> None:
    pass


# ── Queue names are validated where they enter ──────────────────────────
#
# ``QueueName``'s AfterValidator only runs inside pydantic model
# validation; its only references were static function-parameter
# annotations, so ``enqueue(..., queue="deafult ")`` or
# ``@actor(queue=...)`` with a malformed name was silently accepted.
# The job then landed on a queue no worker's ``queue = ANY($1)`` ever
# matches — stranded pending forever, with no error anywhere.


@pytest.mark.parametrize(
    "bad_queue",
    [
        "deafult ",  # trailing space — the classic typo class
        "bad name",  # interior space
        "bad\nname",  # interior newline
        "bad\tname",  # interior tab
        "1queue",  # leading digit
        "queue!",  # character outside the allowed set
        "",  # empty
    ],
)
def test_build_enqueue_args_rejects_invalid_queue_name(bad_queue: str) -> None:
    with pytest.raises(ValueError, match="invalid queue name") as excinfo:
        build_enqueue_args(_hardening_actor, _Payload(), queue=bad_queue)
    message = str(excinfo.value)
    # repr() escapes control characters, so accept either rendering.
    assert bad_queue in message or repr(bad_queue) in message, "the error must name the queue"


def test_build_enqueue_args_rejects_invalid_actor_declared_queue() -> None:
    """The actor-declared default is validated too, not just the per-call
    override — a ref whose queue was never checked strands every job.

    After the fix an invalid queue cannot get onto a ref through the
    decorator at all, so simulate the unchecked-ref state directly.
    """

    async def handler(payload: _Payload) -> None:
        pass

    ref = actor(name="unchecked_default_queue_actor", queue="default")(handler)
    ref.queue = "bad name"  # simulate a ref whose declared queue was never validated
    with pytest.raises(ValueError, match="invalid queue name") as excinfo:
        build_enqueue_args(ref, _Payload())
    assert "bad name" in str(excinfo.value)


def test_actor_declaration_rejects_invalid_queue_name() -> None:
    """``@actor(queue=...)`` fails at decoration time — import time in the
    common case — instead of stranding jobs at enqueue time."""

    async def handler(payload: _Payload) -> None:
        pass

    with pytest.raises(ValueError, match="invalid queue name") as excinfo:
        actor(name="bad_queue_actor", queue="bad name")(handler)
    assert "bad name" in str(excinfo.value)


@pytest.mark.parametrize(
    "good_queue",
    [
        "default",
        "priority",
        "q1",
        "my-queue",  # hyphen: allowed after the first char
        "my.queue",  # dot: allowed after the first char
        "_internal",  # leading underscore
        "Q_2.x",
    ],
)
def test_valid_queue_names_still_pass(good_queue: str) -> None:
    args = build_enqueue_args(_hardening_actor, _Payload(), queue=good_queue)
    assert args.queue == good_queue


async def test_valid_queue_name_enqueues_end_to_end_on_in_memory() -> None:
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    args = build_enqueue_args(_hardening_actor, _Payload(), queue="my-queue")
    row = await backend.enqueue(args)
    assert row.queue == "my-queue"


# ── InMemory enqueue mirrors PG's bind-time NUL guard for jsonb ─────────
#
# ``EnqueueArgs._check_no_nul_text`` deliberately skips payload/metadata:
# on PG they transit jsonb via ``jsonb_param`` → ``dumps_jsonb_str``, which
# rejects a NUL at bind time. The InMemory mirror never called
# ``jsonb_param``, so a NUL payload passed on InMemory and raised
# ValueError on the first real PG enqueue — an app validated against
# InMemory broke in production.


async def test_in_memory_enqueue_rejects_nul_in_payload_value() -> None:
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    payload = {"value": "a\x00b"}
    args = make_enqueue_args(payload=payload)

    with pytest.raises(ValueError) as mem_excinfo:
        await backend.enqueue(args)

    # The rejection must equal the PG bind path's for the same value —
    # the exact guard jsonb_param runs, not a lookalike message.
    with pytest.raises(ValueError) as pg_excinfo:
        dumps_jsonb_str(payload)
    assert str(mem_excinfo.value) == str(pg_excinfo.value)


async def test_in_memory_enqueue_rejects_nul_in_metadata() -> None:
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    metadata = {"note": "a\x00b"}
    args = make_enqueue_args(metadata=metadata)

    with pytest.raises(ValueError) as mem_excinfo:
        await backend.enqueue(args)

    with pytest.raises(ValueError) as pg_excinfo:
        dumps_jsonb_str(metadata)
    assert str(mem_excinfo.value) == str(pg_excinfo.value)


async def test_in_memory_enqueue_accepts_clean_payload_and_metadata() -> None:
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    args = make_enqueue_args(payload={"value": "clean"}, metadata={"note": "clean"})

    row = await backend.enqueue(args)

    assert row.payload == {"value": "clean"}
    assert row.metadata == {"note": "clean"}
    # Parity cuts both ways: the PG-side guard passes the same values.
    assert dumps_jsonb_str({"value": "clean"}) is not None
    assert dumps_jsonb_str({"note": "clean"}) is not None


# ── The trailing-newline trap, at the real chokepoints ──────────────────
#
# `^...$` let a single trailing newline past the queue-name and tag
# validators (see tests/test_ident_re_anchoring.py for the regex-level
# pins, and that file's docstring for why this matters).


def test_build_enqueue_args_rejects_queue_name_with_trailing_newline() -> None:
    with pytest.raises(ValueError, match="invalid queue name"):
        build_enqueue_args(_hardening_actor, _Payload(), queue="default\n")


def test_build_enqueue_args_rejects_tag_with_trailing_newline() -> None:
    with pytest.raises(ValueError, match="invalid tag"):
        build_enqueue_args(_hardening_actor, _Payload(), tags=["tag\n"])


# ── JobFilter text predicates reject a NUL before reaching a backend ────
#
# ``JobFilter.__post_init__`` validated limit/cursor/status but not NUL in
# queue/actor/identity_key/tags: a direct backend ``cancel_where`` /
# ``list_jobs`` call with a NUL surfaced a raw asyncpg
# CharacterNotInRepertoireError (SQLSTATE 22021) instead of the clean
# ValueError the enqueue path raises. The bulk-cancel feature extended
# this exposure to a write path.


def test_job_filter_rejects_nul_in_queue() -> None:
    with pytest.raises(ValueError, match="queue contains a NUL"):
        JobFilter(queue="a\x00b")


def test_job_filter_rejects_nul_in_actor() -> None:
    with pytest.raises(ValueError, match="actor contains a NUL"):
        JobFilter(actor="a\x00b")


def test_job_filter_rejects_nul_in_identity_key() -> None:
    with pytest.raises(ValueError, match="identity_key contains a NUL"):
        JobFilter(identity_key=IdentityKey("a\x00b"))


def test_job_filter_rejects_nul_in_tags() -> None:
    with pytest.raises(ValueError, match="tag contains a NUL"):
        JobFilter(tags=("a\x00b",))


def test_job_filter_clean_text_predicates_still_construct() -> None:
    """Valid predicates are unaffected — the guard is a pure prefilter."""
    f = JobFilter(
        queue="default", actor="my_actor", identity_key=IdentityKey("tenant-1"), tags=("t1",)
    )
    assert f.queue == "default"
    assert f.tags == ("t1",)


# ── BatchFilter.limit has no upper bound ────────────────────────────────
#
# ``list_batches`` has neither a cursor nor an offset, so the limit is the
# only way to reach a batch at all: any upper bound makes the batches past
# it unreachable by any means. Only ``limit >= 0`` is validated.


def test_batch_filter_accepts_a_limit_past_any_page_size() -> None:
    """A limit large enough to reach the whole table is a legitimate request:
    without a cursor it is the only way to see a batch beyond the first page."""
    assert BatchFilter(limit=10_000).limit == 10_000


def test_batch_filter_limit_boundaries() -> None:
    """Zero stays allowed (it means 'no rows' — codified in
    tests/test_batch_protocol.py); negatives stay rejected."""
    assert BatchFilter(limit=0).limit == 0
    with pytest.raises(ValueError, match="limit must be >= 0"):
        BatchFilter(limit=-1)


# ── ScheduleCreateArgs caller text rejects a NUL before the bind ────────
#
# ``__post_init__`` validated only ``cron_expr``; ``actor``/``name``/
# ``timezone``/``payload_factory``/``identity_key`` are caller text bound
# directly to text columns in the create_schedule INSERT, so a NUL
# surfaced as an opaque asyncpg 22021 instead of the clean ValueError the
# enqueue path raises.


def test_schedule_create_args_rejects_nul_in_actor() -> None:
    with pytest.raises(ValueError, match="actor contains a NUL"):
        ScheduleCreateArgs(
            actor="a\x00b", cron_expr="*/5 * * * *", timezone="UTC", next_fire_at=_START
        )


def test_schedule_create_args_rejects_nul_in_name() -> None:
    with pytest.raises(ValueError, match="name contains a NUL"):
        ScheduleCreateArgs(
            actor="a", cron_expr="*/5 * * * *", timezone="UTC", next_fire_at=_START, name="a\x00b"
        )


def test_schedule_create_args_rejects_nul_in_timezone() -> None:
    with pytest.raises(ValueError, match="timezone contains a NUL"):
        ScheduleCreateArgs(
            actor="a", cron_expr="*/5 * * * *", timezone="a\x00b", next_fire_at=_START
        )


def test_schedule_create_args_rejects_nul_in_payload_factory() -> None:
    with pytest.raises(ValueError, match="payload_factory contains a NUL"):
        ScheduleCreateArgs(
            actor="a",
            cron_expr="*/5 * * * *",
            timezone="UTC",
            next_fire_at=_START,
            payload_factory="a\x00b",
        )


def test_schedule_create_args_rejects_nul_in_identity_key() -> None:
    with pytest.raises(ValueError, match="identity_key contains a NUL"):
        ScheduleCreateArgs(
            actor="a",
            cron_expr="*/5 * * * *",
            timezone="UTC",
            next_fire_at=_START,
            identity_key=IdentityKey("a\x00b"),
        )


def test_schedule_create_args_rejects_nul_in_cron_expr_via_croniter() -> None:
    """cron_expr needs no dedicated NUL guard: croniter.is_valid already
    rejects it as an invalid expression."""
    with pytest.raises(ValueError, match="Invalid cron expression"):
        ScheduleCreateArgs(
            actor="a", cron_expr="0\x00 * * * *", timezone="UTC", next_fire_at=_START
        )


def test_schedule_create_args_rejects_bogus_dst_strategy() -> None:
    """``dst_strategy`` was an inert Literal on a dataclass: a bogus value
    constructed fine and only died later as a raw asyncpg
    CheckViolationError from the INSERT — instead of the clean ValueError
    its sibling fields raise."""
    with pytest.raises(ValueError, match="Invalid dst_strategy"):
        ScheduleCreateArgs(
            actor="a",
            cron_expr="*/5 * * * *",
            timezone="UTC",
            next_fire_at=_START,
            dst_strategy="bogus",  # type: ignore[arg-type]  # Why: deliberately invalid — the constructor must reject it at parse time.
        )


def test_schedule_create_args_clean_text_still_constructs() -> None:
    args = ScheduleCreateArgs(
        actor="report_actor",
        name="nightly",
        cron_expr="*/5 * * * *",
        timezone="UTC",
        next_fire_at=_START,
        identity_key=IdentityKey("tenant-1"),
        payload_factory="make_report",
    )
    assert args.name == "nightly"
    assert args.payload_factory == "make_report"


# ── The capacity cache reports snapshot-ness from refresh success ───────
#
# ``has_snapshot = bool(self._rows)`` misread a SUCCESSFUL refresh of an
# empty actor_config table as "no snapshot": the next refresh failure
# then reported ``has_snapshot=False`` / ``degraded_to_literal=True`` — a
# false alarm on the metric built to be alertable, and the wrong
# stale-vs-literal distinction (empty-but-healthy is stale-serving, not
# degraded).


class _CapacityBackend:
    """Backend double whose capacity read can fail on demand."""

    def __init__(self, rows: dict[str, int] | None = None) -> None:
        self.rows = rows or {}
        self.fail = False

    async def get_actor_max_pending(self) -> dict[str, int]:
        if self.fail:
            raise ConnectionError("pg unreachable")
        return dict(self.rows)


async def test_capacity_cache_empty_successful_snapshot_then_failure_reports_snapshot() -> None:
    """A successful refresh of an empty actor_config table is still a
    snapshot: the next refresh failure must report stale-serving, not the
    materially-worse degraded-to-literal mode."""
    backend = _CapacityBackend()  # healthy read that returns zero rows
    cache = ActorCapacityCache(backend, ttl=0.01)

    assert await cache.effective_max_pending("a", literal=10) == 10

    backend.fail = True
    await asyncio.sleep(0.02)  # let the TTL expire
    with structlog.testing.capture_logs() as logs:
        assert await cache.effective_max_pending("a", literal=10) == 10

    entry = next(e for e in logs if e["event"] == "actor-capacity-cache-refresh-failed")
    assert entry["has_snapshot"] is True
    assert entry["degraded_to_literal"] is False
