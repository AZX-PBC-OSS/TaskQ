"""Cross-test isolation pins for the ``obs/_otel.py`` process-global caches.

The flake this file pins
------------------------
``tests/test_rt_worker_metric_cardinality.py`` failed once in a full xdist
suite (green isolated, green on the re-run) — the order-dependent signature
of process-global residue.  The mechanism, source-verified:

* The production prune family stamps the batch-size gauges on EVERY batch,
  including an empty one (``worker/_leader_shared.py``:
  ``_record_prune_batch_size("prune"|"archive_expiry", ...)`` runs before
  the fetch), so any test that drives the real ``prune_terminal_jobs`` /
  ``archive_expiry_sweep`` path — or a leader whose ``_prune_loop`` /
  ``_archive_expiry_loop`` fired — writes ``"prune"`` / ``"archive_expiry"``
  into the process-global ``_sweep_batch_size_cache`` and leaves it there
  forever: nothing reset that cache between tests.
* The victim's batch-size cardinality test MERGES its eight production
  sweep names into that cache (``update_sweep_batch_size_cache`` merges,
  never replaces) and asserts strict key-set equality — a ninth residue
  key fails it.  Its ``sweep_success`` sibling pins its own cache per test;
  the batch-size test relied on nothing, and under xdist only the workers
  whose schedule put a prune-writing module first ever saw the failure.

The fix is the suite's established isolation doctrine (the autouse
``_reset_web_admin_caches`` family): every process-global in
``obs/_otel.py`` that a gauge observer reads is reset to construction
state before AND after each test.  The polluter test below performs the
exact production writes; the entry-state test is the "next reader sees
clean state" pin; the reset-coverage test holds the reset function to
construction state for every gauge-read global.
"""

from __future__ import annotations

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.testing.otel import reset_otel_gauge_caches


async def test_prune_family_batch_size_stamps_are_process_global() -> None:
    """The polluter half: one real prune-family write lands in the
    process-global batch-size cache.

    These are the exact emitter calls ``prune_terminal_jobs`` /
    ``archive_expiry_sweep`` make via ``_record_prune_batch_size`` — a
    stamp is recorded before every batch, empty or not, so a single
    prune-driving test leaves both names in the cache for the rest of the
    process.  Asserting the write landed keeps this half honest about
    being the real write path, not a lookalike.
    """
    obs_mod.record_sweep_batch_size("prune", 250)
    obs_mod.record_sweep_batch_size("archive_expiry", 125)

    assert otel_mod._sweep_batch_size_cache["prune"] == 250  # pyright: ignore[reportPrivateUsage]  # Why: the cache IS the state under test — same seam the cardinality tests read.
    assert otel_mod._sweep_batch_size_cache["archive_expiry"] == 125  # pyright: ignore[reportPrivateUsage]


def test_gauge_caches_are_at_construction_state_for_each_reader() -> None:
    """The victim half: whatever ran before this test, every process-global
    a gauge observer reads must be back at construction state when the
    next test starts.

    This is the flaky cardinality test's precondition made explicit and
    total — the batch-size cache specifically (the one the flake hit, via
    a merge-then-assert-equality read) and every sibling cache the same
    writer population can dirty.  It passes only if the between-tests
    reset exists.
    """
    assert otel_mod._queue_depth_cache == {}  # pyright: ignore[reportPrivateUsage]  # Why: the isolation invariant IS the point of the test.
    assert otel_mod._stranded_jobs_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._reservation_slots_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._keyed_reclaim_pending == 0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._sweep_success_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._sweep_batch_size_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._sweep_batch_size_configured_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._leader_lease_expires_in_seconds_cache is None  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._jobs_by_status_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._actor_backlog_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._actor_oldest_pending_age_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._oldest_due_age_seconds == 0.0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._scheduled_count == 0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._running_lease_expired_count == 0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._heartbeat_consecutive_failures_count == 0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._disabled_schedules_count == 0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._slot_pool_occupancy_source is None  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._queue_label_values == set()  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._cron_actor_label_values == set()  # pyright: ignore[reportPrivateUsage]
    assert len(otel_mod._cron_failure_levels) == 0  # pyright: ignore[reportPrivateUsage]


def test_reset_otel_gauge_caches_restores_construction_state() -> None:
    """The reset covers every process-global a gauge observer reads.

    Dirties each one through its public writer (the same call shape the
    loops use), resets, and asserts construction state — so a gauge cache
    added to ``obs/_otel.py`` without a reset entry fails here instead of
    resurfacing as the next order-dependent flake.
    """
    obs_mod.update_queue_depth_cache({"default": 5})
    obs_mod.update_stranded_jobs_cache({("orphan_actor", "no_actor_config"): 3})
    obs_mod.update_reservation_slots_cache({"bucket_a": 2})
    obs_mod.update_keyed_reclaim_pending(7)
    obs_mod.record_sweep_success("expired_locks")
    obs_mod.record_sweep_batch_size("prune", 250)
    obs_mod.record_sweep_batch_size_configured("prune", 1000)
    obs_mod.record_leader_lease_expires_in_seconds("w1", 30.0)
    obs_mod.update_jobs_by_status_cache({"pending": 4})
    obs_mod.update_actor_backlog_cache({("actor_a", "default"): 9})
    obs_mod.update_actor_oldest_pending_age_cache({("actor_a", "default"): 12.0})
    obs_mod.update_oldest_due_age_cache(11.0)
    obs_mod.update_scheduled_count_cache(6)
    obs_mod.update_running_lease_expired_cache(2)
    obs_mod.update_heartbeat_consecutive_failures("w1", 3)
    obs_mod.update_disabled_schedules_count(1)
    otel_mod.set_slot_pool_occupancy_source(
        _NeverPool()  # pyright: ignore[reportArgumentType]  # Why: the gauge reads the source structurally; any object with the two read methods satisfies it.
    )
    # The emitter-side process globals the cardinality tests pin per test
    # today — the same order-dependent class (a prior test's admissions
    # widen a later test's boundary), so they reset too.
    obs_mod.record_published_message("actor_a", "queue_a")
    obs_mod.record_cron_failure("actor_a", 1)

    reset_otel_gauge_caches()

    assert otel_mod._queue_depth_cache == {}  # pyright: ignore[reportPrivateUsage]  # Why: the reset's completeness is the point of the test.
    assert otel_mod._stranded_jobs_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._reservation_slots_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._keyed_reclaim_pending == 0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._sweep_success_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._sweep_batch_size_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._sweep_batch_size_configured_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._leader_lease_expires_in_seconds_cache is None  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._jobs_by_status_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._actor_backlog_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._actor_oldest_pending_age_cache == {}  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._oldest_due_age_seconds == 0.0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._scheduled_count == 0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._running_lease_expired_count == 0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._heartbeat_consecutive_failures_count == 0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._disabled_schedules_count == 0  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._slot_pool_occupancy_source is None  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._queue_label_values == set()  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._cron_actor_label_values == set()  # pyright: ignore[reportPrivateUsage]
    assert len(otel_mod._cron_failure_levels) == 0  # pyright: ignore[reportPrivateUsage]


class _NeverPool:
    """Structural stand-in for the occupancy gauge's pool source."""

    def get_size(self) -> int:
        return 1

    def get_idle_size(self) -> int:
        return 0
