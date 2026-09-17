"""Unit tests for the StallAttributionTally: the shared holder between the
lag watchdog's daemon thread (writer) and the heartbeat loop (reader).

The tally's whole job is to survive that two-thread seam and to stay
bounded: the heartbeat merges its value into the workers row metadata
every tick, so an unbounded tally would grow a hot-path row forever.
"""

import threading

from taskq.worker._stall_tally import (
    STALL_TALLY_MAX_ACTORS,
    STALL_TALLY_UNATTRIBUTED,
    StallAttributionTally,
    remedy_for_kind,
)


def test_record_buckets_by_actor_and_kind() -> None:
    tally = StallAttributionTally()
    tally.record("send_email", kind="blocking_call")
    tally.record("send_email", kind="gil_held")
    tally.record("send_email", kind="gil_held")
    tally.record("resize_image", kind="blocking_call")

    assert tally.snapshot() == {
        "send_email": {"blocking_call": 1, "gil_held": 2},
        "resize_image": {"blocking_call": 1},
    }


def test_record_without_actor_lands_under_the_unattributed_key() -> None:
    """A stall whose sampled stack named no registered actor still counts:
    the tally's total stays honest about how often the loop stalled."""
    tally = StallAttributionTally()
    tally.record(None, kind="blocking_call")

    assert tally.snapshot() == {STALL_TALLY_UNATTRIBUTED: {"blocking_call": 1}}


def test_tally_is_bounded_at_the_top_twenty_actors() -> None:
    tally = StallAttributionTally()
    for i in range(STALL_TALLY_MAX_ACTORS + 10):
        tally.record(f"actor_{i:03d}", kind="blocking_call")

    snapshot = tally.snapshot()
    assert len(snapshot) == STALL_TALLY_MAX_ACTORS
    # The hottest actors survive the bound: each evicted actor had the
    # smallest total, so the single-count tail is what got dropped.
    assert "actor_000" not in snapshot
    assert "actor_009" not in snapshot
    assert "actor_029" in snapshot


def test_metadata_value_empty_while_nothing_attributed() -> None:
    """An empty tally merges a no-op: the heartbeat's jsonb concat of an
    empty object leaves the registered metadata keys untouched."""
    assert StallAttributionTally().metadata_value() == {}


def test_metadata_value_carries_the_tally_under_one_key() -> None:
    tally = StallAttributionTally()
    tally.record("send_email", kind="gil_held")

    assert tally.metadata_value() == {
        "loop_stalls": {"send_email": {"gil_held": 1}},
    }


def test_snapshot_is_a_copy_the_caller_cannot_corrupt() -> None:
    tally = StallAttributionTally()
    tally.record("send_email", kind="gil_held")

    snapshot = tally.snapshot()
    snapshot["send_email"]["gil_held"] = 999
    snapshot["injected"] = {}

    assert tally.snapshot() == {"send_email": {"gil_held": 1}}


def test_record_is_safe_from_two_threads() -> None:
    """The watchdog thread records while the heartbeat thread snapshots:
    concurrent access must neither raise nor lose counts."""
    tally = StallAttributionTally()
    errors: list[BaseException] = []

    def _hammer() -> None:
        try:
            for _ in range(2_000):
                tally.record("send_email", kind="blocking_call")
                tally.snapshot()
        except BaseException as exc:  # Why: the thread records the failure for the assertion instead of dying silently.
            errors.append(exc)

    threads = [threading.Thread(target=_hammer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert tally.snapshot()["send_email"]["blocking_call"] == 8_000


def test_remedy_for_kind_names_both_shapes() -> None:
    assert "asyncio.to_thread" in remedy_for_kind("blocking_call")
    assert "chunk" in remedy_for_kind("gil_held")
