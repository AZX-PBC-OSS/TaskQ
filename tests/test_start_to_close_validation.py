"""Regression tests for start_to_close validation across all precedence levels.

Covers:
- S1: Negative/zero ``default_start_to_close`` (worker fallback) raises ValueError.
- S1: Negative/zero ``start_to_close`` on ``@actor`` (actor default) raises ValueError.
- S1: Negative/zero ``start_to_close`` on enqueue (per-call) raises ValueError.
- S2: ``EnqueueItem`` with ``start_to_close`` passes through to EnqueueArgs.
- S2: ``EnqueueItem`` with negative ``start_to_close`` raises ValueError.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq.actor import actor
from taskq.batch import EnqueueItem
from taskq.client._args import build_batch_args, build_enqueue_args
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int = 0


@actor(name="_stc_validation_actor")
async def _test_actor(_payload: _Payload) -> None:
    pass


def _load_settings(**overrides: str) -> WorkerSettings:
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


# ── S1: Worker fallback — default_start_to_close validation ────────────────


class TestDefaultStartToCloseSettings:
    """WorkerSettings._post_load rejects non-positive default_start_to_close."""

    def test_negative_raises(self) -> None:
        s = _load_settings()
        object.__setattr__(s, "default_start_to_close", timedelta(seconds=-1))
        with pytest.raises(ValueError, match=r"default_start_to_close must be > 0"):
            s._post_load()

    def test_zero_raises(self) -> None:
        s = _load_settings()
        object.__setattr__(s, "default_start_to_close", timedelta(seconds=0))
        with pytest.raises(ValueError, match=r"default_start_to_close must be > 0"):
            s._post_load()

    def test_positive_accepted(self) -> None:
        s = _load_settings()
        object.__setattr__(s, "default_start_to_close", timedelta(seconds=30))
        s._post_load()
        assert s.default_start_to_close == timedelta(seconds=30)

    def test_none_accepted(self) -> None:
        s = _load_settings()
        object.__setattr__(s, "default_start_to_close", None)
        s._post_load()
        assert s.default_start_to_close is None


# ── S1: Actor default — @actor(start_to_close=...) validation ──────────────


class TestActorStartToClose:
    """The @actor decorator rejects non-positive start_to_close at decoration time."""

    def test_negative_raises(self) -> None:
        async def _handler(_payload: _Payload) -> None:
            pass

        with pytest.raises(ValueError, match=r"start_to_close must be > 0"):
            actor(name="_stc_neg_actor", start_to_close=timedelta(seconds=-1))(_handler)

    def test_zero_raises(self) -> None:
        async def _handler(_payload: _Payload) -> None:
            pass

        with pytest.raises(ValueError, match=r"start_to_close must be > 0"):
            actor(name="_stc_zero_actor", start_to_close=timedelta(seconds=0))(_handler)

    def test_positive_accepted(self) -> None:
        @actor(name="_stc_pos_actor", start_to_close=timedelta(seconds=30))
        async def _pos_actor(_payload: _Payload) -> None:
            pass

        assert _pos_actor.start_to_close == timedelta(seconds=30)


# ── S1: Per-enqueue — build_enqueue_args validation ────────────────────────


class TestEnqueueStartToClose:
    """build_enqueue_args rejects non-positive per-call start_to_close."""

    def test_negative_raises(self) -> None:
        clock = FakeClock(_START)
        with pytest.raises(ValueError, match=r"start_to_close must be > 0"):
            build_enqueue_args(
                _test_actor,
                _Payload(),
                start_to_close=timedelta(seconds=-1),
                clock=clock,
            )

    def test_zero_raises(self) -> None:
        clock = FakeClock(_START)
        with pytest.raises(ValueError, match=r"start_to_close must be > 0"):
            build_enqueue_args(
                _test_actor,
                _Payload(),
                start_to_close=timedelta(seconds=0),
                clock=clock,
            )

    def test_positive_accepted(self) -> None:
        clock = FakeClock(_START)
        args = build_enqueue_args(
            _test_actor,
            _Payload(),
            start_to_close=timedelta(seconds=30),
            clock=clock,
        )
        assert args.start_to_close == timedelta(seconds=30)


# ── S2: Batch per-item start_to_close pass-through ─────────────────────────


class TestBatchStartToClose:
    """EnqueueItem.start_to_close passes through to EnqueueArgs via build_batch_args."""

    def test_start_to_close_passes_through(self) -> None:
        clock = FakeClock(_START)
        batch_id = UUID("12345678-1234-5678-1234-567812345678")
        items = [
            EnqueueItem(
                actor_ref=_test_actor,
                payload=_Payload(value=1),
                start_to_close=timedelta(seconds=45),
            ),
        ]

        args_list = build_batch_args(items, batch_id, clock)

        assert len(args_list) == 1
        assert args_list[0].start_to_close == timedelta(seconds=45)

    def test_none_default_passes_through(self) -> None:
        clock = FakeClock(_START)
        batch_id = UUID("12345678-1234-5678-1234-567812345678")
        items = [EnqueueItem(actor_ref=_test_actor, payload=_Payload(value=1))]

        args_list = build_batch_args(items, batch_id, clock)

        assert args_list[0].start_to_close is None

    def test_negative_raises(self) -> None:
        clock = FakeClock(_START)
        batch_id = UUID("12345678-1234-5678-1234-567812345678")
        items = [
            EnqueueItem(
                actor_ref=_test_actor,
                payload=_Payload(value=1),
                start_to_close=timedelta(seconds=-1),
            ),
        ]

        with pytest.raises(ValueError, match=r"start_to_close must be > 0"):
            build_batch_args(items, batch_id, clock)

    def test_zero_raises(self) -> None:
        clock = FakeClock(_START)
        batch_id = UUID("12345678-1234-5678-1234-567812345678")
        items = [
            EnqueueItem(
                actor_ref=_test_actor,
                payload=_Payload(value=1),
                start_to_close=timedelta(seconds=0),
            ),
        ]

        with pytest.raises(ValueError, match=r"start_to_close must be > 0"):
            build_batch_args(items, batch_id, clock)
