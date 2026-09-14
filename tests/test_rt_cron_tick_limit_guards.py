"""Red-team attacks on two boundary gaps the bounded-batch fix left open.

Both target the fix's own stated invariant — a degenerate row cap must
fail loudly at the typed boundary, before any SQL runs — at the two
sites the fix's boundary tests do not reach:

* ``tick_cron``'s ``limit`` (``src/taskq/worker/cron_loop.py:358``)
  feeds ``LIMIT $1`` (``:439-440``) with no function-level validation:
  the function validates only the schema identifier (``:391-392``).
  Every sibling bound — the sweeps (``_sweeps.py:676-677,795-796,
  908-909``), bulk cancel (``_cancel_bulk.py:167-168``), force
  deregistration (``actor_config_ops.py:523-524``) — rejects 0 and
  negatives with ``ValueError`` before touching the database, for the
  documented reason that ``LIMIT 0`` is a legal rowless query: a silent
  drain stall that reports success on zero rows forever while eligible
  work sits waiting. ``tick_cron(limit=0)`` is that shape exactly: it
  returns 0, indistinguishable from "nothing due", while due schedules
  sit unfired — the cron half of the silent stall the settings layer
  already names (``TASKQ_CRON_TICK_LIMIT`` is ``ge=1`` with the comment
  "0 schedules per tick is a silent cron stall",
  ``test_worker_settings_event_writer.py:103-105``). A negative limit
  is the companion edge: a negative LIMIT *parameter* is a server-side
  data error (SQLSTATE 2201W), outside the leader's transient-error
  classification, so it burns the unexpected-error budget instead of
  being caught where it belongs. ``tick_cron`` is not an internal
  helper behind a settings-only caller — its cap is an exercised
  contract (``test_cron_tick_bounded.py`` drives explicit caps) — so
  the coverage note's "settings layer is the guard" design line, which
  names only ``complete_stale_batches``/``cleanup_stale_workers``, does
  not cover it.

* ``SweepBatchSizer`` with ``divisor=1`` (``_sweeps.py:489-519``)
  constructs a breaker that latches but never degrades:
  ``effective_size()`` answers ``max(1, default // 1) == default`` in
  the latched tier. The settings layer already declares 1 degenerate
  (``event_writer_reduced_batch_divisor`` is ``ge=2`` with the comment
  "A divisor of 1 makes the breaker's reduced tier equal to the normal
  tier: no degradation when the database is falling over",
  ``test_worker_settings_event_writer.py:92-94``), and the existing
  constructor-boundary test (``test_rt_sweeps_boundary.py:212-241``)
  rejects 0 and -2 but not 1 — the gap sits exactly on the edge between
  tested and untested. The constructor's own comment misstates the
  boundary it enforces ("divisor < 1 either never reduces or divides
  by zero" at ``_sweeps.py:498-504``): ``divisor == 1`` also never
  reduces, so either the validation is off by one or the comment is
  wrong about what 1 means.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.backend._sweeps import SweepBatchSizer
from taskq.settings import WorkerSettings
from taskq.worker.cron_loop import tick_cron

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


class _TouchRaisesConn:
    """A connection stand-in whose every use is a test failure.

    The degenerate bound must be rejected at the typed boundary BEFORE
    any database access, so any method call reaching this object means
    validation did not run first. Mirrors the ``_TouchRaisesConn`` /
    ``_TouchRaisesPool`` stand-ins in
    ``tests/test_rt_cancel_batch_guards.py``.
    """

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(
            "boundary validation must reject a degenerate cron limit before "
            f"the connection is used; got a call to {name!r}"
        )


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _DSN})


@pytest.mark.parametrize("limit", [0, -1, -100])
async def test_tick_cron_rejects_degenerate_limit_before_any_sql(limit: int) -> None:
    """``tick_cron`` must reject a degenerate ``limit`` with ``ValueError``.

    ``limit=0`` selects zero due schedules and returns 0 forever while
    schedules sit due — a silent cron stall wearing "nothing due"
    clothing, the exact failure shape the bounded-sweep boundary
    contract exists to remove. A negative limit is a server-side data
    error (SQLSTATE 2201W) the transient classification does not catch.
    Both are caller configuration bugs and belong at the boundary,
    before the advisory-lock acquisition — the backend argument is
    never reached, so a dummy stands in for it.
    """
    with pytest.raises(ValueError, match="limit"):
        await tick_cron(
            cast(Any, _TouchRaisesConn()),
            _settings(),
            cast("Backend", None),
            "taskq",
            new_uuid(),
            limit=limit,
        )


def test_sizer_constructor_rejects_noop_divisor() -> None:
    """A ``divisor`` of 1 must fail at construction, not build a no-op breaker.

    With ``divisor=1`` the latched tier equals the normal tier, so a
    database that keeps aborting full-size batches gets full-size
    batches forever — the degradation tier exists silently without ever
    engaging. The settings layer already rejects 1 for this reason;
    the shared constructor must enforce the same boundary, since it is
    constructed directly (every backend sweep call mints its tier from
    it, and the existing boundary test constructs it directly too).
    """
    with pytest.raises(ValueError, match="divisor"):
        SweepBatchSizer(100, 1, 3, 600.0)
