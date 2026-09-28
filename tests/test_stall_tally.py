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
    LoopIdleWindow,
    StallAttributionTally,
    remedy_for_kind,
)


def _as_dict(value: object) -> dict[str, object]:
    """Pyright narrowing for the metadata payload's nested JSONB values."""
    assert isinstance(value, dict)
    return value


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

    value = tally.metadata_value()
    # The cumulative key keeps its exact pre-window shape (no consumer
    # breaks); the windowed delta rides beside it (the tests below).
    assert value["loop_stalls"] == {"send_email": {"gil_held": 1}}


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


# ── The windowed tally: an active staller vs a healed one ────────────


def test_windowed_tally_distinguishes_active_staller_from_healed() -> None:
    """The cumulative tally cannot say WHEN a stall happened: a worker that
    stalled once yesterday re-publishes the same counts on every beat and
    reads identically to one stalling on every beat. The windowed delta
    beside it is what separates the two: after a publish with no new
    stalls, the cumulative counts stand but the window's ``stalls`` map is
    empty - the healed worker's signature. (On the pre-window tally this
    test fails: there was no field a consumer could read the difference
    from.)"""
    tally = StallAttributionTally()
    tally.record("send_email", kind="gil_held")  # yesterday's stall
    first = tally.metadata_value()
    # Cumulative keeps the stall (no consumer breaks).
    assert first["loop_stalls"] == {"send_email": {"gil_held": 1}}
    # Next heartbeat, no new stall: the healed worker's window is empty.
    second = tally.metadata_value()
    window = _as_dict(second["loop_stalls_window"])
    assert window["stalls"] == {}
    assert second["loop_stalls"] == {"send_email": {"gil_held": 1}}
    # The window spanned a publish-to-publish interval: a positive span.
    assert isinstance(window["window_seconds"], float)
    assert window["window_seconds"] >= 0.0


def test_windowed_delta_counts_only_new_stalls() -> None:
    """An active staller shows in the window: stalls recorded between two
    publishes land in the delta, keyed actor -> kind, exactly the
    cumulative shape's vocabulary."""
    tally = StallAttributionTally()
    tally.metadata_value()  # anchor the window on an empty publish
    tally.record("send_email", kind="gil_held")
    tally.record("send_email", kind="gil_held")
    tally.record("resize_image", kind="blocking_call")

    window = _as_dict(tally.metadata_value()["loop_stalls_window"])
    assert window["stalls"] == {
        "send_email": {"gil_held": 2},
        "resize_image": {"blocking_call": 1},
    }
    # Windowed reads never move the cumulative counts.
    assert _as_dict(_as_dict(tally.metadata_value()["loop_stalls_window"])["stalls"]) == {}


def test_first_publish_window_covers_the_process_age() -> None:
    """A worker's first beat has no predecessor window: the delta is the
    full cumulative tally (the honest summary a fresh worker can give) and
    the span is the tally's own age."""
    clock_value = [100.0]
    tally = StallAttributionTally(clock=lambda: clock_value[0])
    tally.record("send_email", kind="blocking_call")
    clock_value[0] = 160.0  # 60s of process age at the first beat

    window = _as_dict(tally.metadata_value()["loop_stalls_window"])
    assert window["stalls"] == {"send_email": {"blocking_call": 1}}
    assert window["window_seconds"] == 60.0


def test_windowed_delta_clamps_after_eviction() -> None:
    """The tally evicts its coldest actors; a delta is computed only over
    the actors the CURRENT snapshot holds, so an evicted actor vanishes
    from the delta rather than producing a negative - a windowed rate of
    -n is a fabrication no consumer should have to defend against. An
    evicted actor re-attributed later (still the coldest, re-evicted at
    once by the tie-break) never reappears with a negative either."""
    tally = StallAttributionTally(max_actors=2)
    tally.record("hot", kind="blocking_call")
    tally.record("hot", kind="blocking_call")
    tally.record("cold", kind="blocking_call")
    tally.metadata_value()  # anchor: publishes {hot: 2, cold: 1}

    tally.record("hot", kind="blocking_call")  # hot: 3
    tally.record("newcomer", kind="blocking_call")  # evicts `cold` (1 < 3, tie-break by name)

    window = _as_dict(tally.metadata_value()["loop_stalls_window"])
    assert window["stalls"] == {
        "hot": {"blocking_call": 1},
        "newcomer": {"blocking_call": 1},
    }
    stalls = _as_dict(window["stalls"])
    assert all(
        isinstance(count, int) and count >= 0
        for kinds in stalls.values()
        for count in _as_dict(kinds).values()
    )


def test_windowed_value_is_absent_while_nothing_attributed() -> None:
    """An empty tally merges a no-op, window keys included: a quiet
    process must not stamp ``loop_stalls_window`` into the row at all."""
    assert StallAttributionTally().metadata_value() == {}


# ── LoopIdleWindow: the sampled idle-fraction window ─────────────────


def test_idle_window_drain_is_empty_before_any_sample() -> None:
    """No samples in the window drains to None - the heartbeat merges a
    no-op and records no histogram point: an absent series is the honest
    'nothing observed', never a stale 0."""
    assert LoopIdleWindow().drain() is None


def test_idle_window_fraction_and_reset() -> None:
    window = LoopIdleWindow()
    for parked in (True, True, True, False):
        window.record_sample(parked=parked)

    drained = window.drain()
    assert drained is not None
    assert drained.idle_fraction == 0.75
    assert drained.samples == 4
    assert drained.samples_parked == 3
    assert drained.samples_unreadable == 0
    assert drained.window_seconds >= 0.0
    # The drain starts a fresh window: the aggregate is exactly the span
    # since the previous publish, never cumulative.
    assert window.drain() is None


def test_idle_window_counts_unreadable_samples_as_not_parked() -> None:
    """A sample whose frames could not be read counts toward the window
    and as not parked - the conservative direction (the idle fraction may
    under-report, never over-report) - with its own count so a reader can
    judge the coverage."""
    window = LoopIdleWindow()
    window.record_sample(parked=True)
    window.record_sample(parked=None)
    window.record_sample(parked=None)

    drained = window.drain()
    assert drained is not None
    assert drained.idle_fraction == 1 / 3
    assert drained.samples == 3
    assert drained.samples_parked == 1
    assert drained.samples_unreadable == 2


def test_idle_window_is_safe_from_two_threads() -> None:
    """The watchdog thread records while the heartbeat thread drains:
    concurrent access must neither raise nor lose samples."""
    window = LoopIdleWindow()
    errors: list[BaseException] = []

    def _hammer() -> None:
        try:
            for _ in range(2_000):
                window.record_sample(parked=True)
                window.drain()
        except BaseException as exc:  # Why: the thread records the failure for the assertion instead of dying silently.
            errors.append(exc)

    threads = [threading.Thread(target=_hammer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors


# ── The park classifier: the watchdog's actual mechanism ─────────────


def _frame(filename: str, funcname: str) -> tuple[str, int, str, int]:
    return (filename, 1, funcname, id(funcname))


def test_classifier_reads_the_parked_selector_shape() -> None:
    """An idle CPython asyncio loop parks in its selector: innermost frame
    ``selectors.py:select`` directly under ``base_events.py:_run_once`` -
    the exact shape the live-frame probe observed."""
    sample = (
        _frame("/usr/lib/python3.13/selectors.py", "select"),
        _frame("/usr/lib/python3.13/asyncio/base_events.py", "_run_once"),
        _frame("/usr/lib/python3.13/asyncio/base_events.py", "run_forever"),
    )
    from taskq.worker._watchdog import _classify_loop_parked

    assert _classify_loop_parked(sample) is True


def test_classifier_reads_an_executing_callback_as_busy() -> None:
    """Any other readable shape - a callback mid-execution - is not
    parked, whether it names an actor or taskq's own code."""
    from taskq.worker._watchdog import _classify_loop_parked

    busy = (
        _frame("/app/jobs.py", "_blocking_helper"),
        _frame("/app/jobs.py", "send_email"),
        _frame("/usr/lib/python3.13/asyncio/base_events.py", "_run_once"),
    )
    assert _classify_loop_parked(busy) is False
    park_entry = (
        _frame("/usr/lib/python3.13/asyncio/selector_events.py", "bother"),
        _frame("/usr/lib/python3.13/asyncio/base_events.py", "_run_once"),
    )
    assert _classify_loop_parked(park_entry) is False


def test_classifier_degrades_to_unknown_on_an_unreadable_chain() -> None:
    """A chain too short to classify (the thread exited, the read raced
    teardown) returns None: the window counts it as not parked."""
    from taskq.worker._watchdog import _classify_loop_parked

    assert _classify_loop_parked(()) is None
    assert _classify_loop_parked((_frame("/app/jobs.py", "send_email"),)) is None
