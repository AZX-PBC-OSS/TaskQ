"""Lock-timeout refusals are counted, not just logged — closing the counting asymmetry.

The capacity arm of enqueue identity serialization has always counted its
lock-budget refusals (``record_backpressure_error(actor,
kind="max_pending_lock_timeout")`` beside the warning log). The unique_for
and idempotency arms raised the same class of typed refusal with only a log
line — a refusal an operator's alerting cannot see, in exactly the
contention storms where logs drown. These pins drive both remaining arms
through the real emitters and assert the ``taskq.backpressure.errors``
datapoint lands with its own bounded kind and the actor label: the same
shape as the capacity arm, never a capacity kind, never uncounted.

The error TYPES are unchanged (neither is a ``BackpressureError``; their
retry-yields-dedup guidance is the caller's correct response) — pinned in
tests/test_postgres_enqueue_unique_for_lock.py; what changed is that the
refusal is also counted.
"""

from datetime import UTC, datetime

import asyncpg
import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.backend._enqueue import _enqueue_on_conn
from taskq.backend._sql_templates import render
from taskq.exceptions import IdempotencyKeyLockTimeoutError, UniqueForLockTimeoutError
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_enqueue_args
from tests.test_postgres_enqueue_unique_for_lock import (  # pyright: ignore[reportPrivateUsage]  # Why: the contended-conn doubles are the established fakes for these arms; redefining them here would drift from the shapes the bounded-wait pins drive.
    _ContendedFakeConn,
    _unique_for_args,
)

_START = datetime(2025, 1, 1, tzinfo=UTC)

_UNIQUE_FOR_ACTOR = "test_actor"
_IDEMPOTENCY_ACTOR = "lock_counter_actor"
_IDEMPOTENCY_KEY = "lock-counter-137"


class _IdempotencyContendedFakeConn(_ContendedFakeConn):
    """The idempotency arm's contended stand-in: the contention surfaces at
    the token INSERT itself — the server-side ``lock_timeout`` the bounded
    speculative wait set fires on the uncommitted same-pair row (55P03),
    the arm's documented exhaustion path."""

    async def fetchrow(self, sql: str, *params: object) -> dict[str, object] | None:
        if "INSERT" in sql:
            raise asyncpg.LockNotAvailableError("simulated server lock_timeout")
        return await super().fetchrow(sql, *params)


@pytest.fixture
def otel_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation with the backpressure counter rebound.

    ``taskq.backpressure.errors`` is a module-level instrument singleton
    (unconditional, not lazy), so isolating it means rebinding the
    singleton onto a fresh meter — the ``tests/test_obs.py``
    ``_patch_instruments`` convention.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())  # pyright: ignore[reportPrivateUsage]  # Why: mirrors tests/test_obs.py's otel_reader fixture, which reads the same private version helper.
    monkeypatch.setattr(otel_mod, "get_meter", lambda: new_meter)
    monkeypatch.setattr(
        otel_mod, "_backpressure_errors", new_meter.create_counter("taskq.backpressure.errors")
    )  # pyright: ignore[reportPrivateUsage]  # Why: the singleton-rebind convention from tests/test_obs.py::_patch_instruments.
    return reader


def _backpressure_points(reader: InMemoryMetricReader) -> dict[tuple[str, str], int]:
    """The backpressure counter's value per (actor, kind) pair."""
    from taskq.testing.otel import counter_data_points

    points = counter_data_points(reader, "taskq.backpressure.errors")
    return {
        (
            str(dict(p.attributes or {}).get("actor")),
            str(dict(p.attributes or {}).get("kind")),
        ): int(p.value)
        for p in points
    }


async def test_unique_for_lock_timeout_counts_on_backpressure_counter(
    otel_reader: InMemoryMetricReader,
) -> None:
    """A unique_for lock-budget refusal bumps ``taskq.backpressure.errors``
    once, actor-labeled under the bounded kind ``unique_for_lock_timeout`` —
    the same shape as the capacity arm, which has always counted."""
    conn = _ContendedFakeConn(try_lock_result=False, blocking_times_out=True)

    with pytest.raises(UniqueForLockTimeoutError):
        await _enqueue_on_conn(
            conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
            render("taskq"),
            "taskq",
            FakeClock(_START),
            _unique_for_args(),  # type: ignore[arg-type]  # Why: dataclasses.replace keeps the EnqueueArgs type
            unique_for_lock_timeout_ms=100.0,
        )

    assert _backpressure_points(otel_reader) == {
        (_UNIQUE_FOR_ACTOR, "unique_for_lock_timeout"): 1
    }, (
        "a unique_for lock-timeout refusal must land on taskq.backpressure.errors "
        "with kind='unique_for_lock_timeout' — log-only refusals are invisible to "
        "alerting in exactly the contention storms where logs drown"
    )


async def test_idempotency_lock_timeout_counts_on_backpressure_counter(
    otel_reader: InMemoryMetricReader,
) -> None:
    """An idempotency speculative-token timeout bumps the same counter
    under ``idempotency_lock_timeout`` — all three identity-serialization
    arms count their refusals; no arm may be the log-only one."""
    conn = _IdempotencyContendedFakeConn(try_lock_result=True)
    args = make_enqueue_args(actor=_IDEMPOTENCY_ACTOR, idempotency_key=_IDEMPOTENCY_KEY)

    with pytest.raises(IdempotencyKeyLockTimeoutError):
        await _enqueue_on_conn(
            conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
            render("taskq"),
            "taskq",
            FakeClock(_START),
            args,
            idempotency_lock_timeout_ms=100.0,
        )

    assert _backpressure_points(otel_reader) == {
        (_IDEMPOTENCY_ACTOR, "idempotency_lock_timeout"): 1
    }, (
        "an idempotency lock-timeout refusal must land on taskq.backpressure.errors "
        "with kind='idempotency_lock_timeout' — the third arm of the identity "
        "serialization family may not be the uncounted one"
    )


async def test_successful_enqueue_touches_no_backpressure_counter(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The happy path stays off the counter — a refusal counter that also
    counts successes cannot serve as a refusal signal."""
    conn = _ContendedFakeConn(try_lock_result=True)
    row = await _enqueue_on_conn(
        conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in
        render("taskq"),
        "taskq",
        FakeClock(_START),
        make_enqueue_args(),
    )

    assert row.actor
    assert _backpressure_points(otel_reader) == {}, (
        "a fresh uncontended enqueue must not touch taskq.backpressure.errors"
    )
