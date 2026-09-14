"""Adversarial pins: sweep-health cache publication must be snapshot-safe.

The sweep-health caches feed TWO cross-thread readers:

* the OTel SDK invokes the observable-gauge callbacks
  (``_observe_sweep_success`` / ``_observe_sweep_batch_size`` /
  ``_observe_sweep_batch_size_configured``) on a reader thread while the
  worker's event-loop thread publishes stamps — the identical race the
  module already warns about in ``leader.py``'s ``_active_leaders_lock``
  ("the OTel SDK reader thread invokes … while the event-loop thread
  mutates … Unsynchronized iteration raises RuntimeError: Set changed
  size during iteration");
* ``maintenance_health`` reads them on the event-loop thread.

A writer that mutates a cache dict IN PLACE while a reader's iterator is
open raises ``RuntimeError: dictionary changed size during iteration`` —
and the size changes land exactly where they matter most: on the first
success after startup, and after every ``clear_sweep_health_caches()``
demotion re-populates the cache. The pins below hold a reader generator
open across such an insert and assert it never raises: publication must
rebind (like ``_queue_depth_cache`` / ``_jobs_by_status_cache`` already
do in this module), never mutate in place.
"""

from __future__ import annotations

import pytest
from opentelemetry.metrics import CallbackOptions

import taskq.obs._otel as otel_mod


@pytest.fixture
def fresh_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Swap the process-global caches for empty dicts, restored on teardown.

    A rebinding writer replaces the module global mid-test; monkeypatch
    restores the original object regardless, so no stamp leaks into
    other tests.
    """
    monkeypatch.setattr(otel_mod, "_sweep_success_cache", {})
    monkeypatch.setattr(otel_mod, "_sweep_batch_size_cache", {})
    monkeypatch.setattr(otel_mod, "_sweep_batch_size_configured_cache", {})
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)


def test_success_cache_reader_survives_concurrent_first_success(
    fresh_caches: None,
) -> None:
    """Holding the reader open across a first-success insert must not raise.

    ``next(reader)`` opens the dict iterator exactly as the OTel SDK's
    collection does; the insert that follows is the startup / re-election
    shape (a NEW sweep name arriving in a just-emptied cache). Draining
    the reader after the insert is the SDK's ``for obs in callback(...)``.
    """
    otel_mod.record_sweep_success("scheduled_to_pending")

    reader = otel_mod._observe_sweep_success(CallbackOptions())  # pyright: ignore[reportPrivateUsage]  # Why: the callback is the exact reader the SDK invokes; driving it directly reproduces the reader thread's iteration.
    admission = next(reader)
    assert admission.value is not None

    # The event-loop thread publishes another sweep's first stamp while
    # the reader's iterator is still open.
    otel_mod.record_sweep_success("deadline_exceeded")

    # Draining the open iterator must not raise — publication is a rebind.
    remaining = list(reader)
    assert {dict(o.attributes or {})["sweep_name"] for o in remaining} <= {
        "scheduled_to_pending",
        "deadline_exceeded",
    }


def test_batch_size_cache_reader_survives_concurrent_first_write(
    fresh_caches: None,
) -> None:
    """The batch-size cache reader must survive a first-write insert, for
    the same reason: ``maintenance_health`` and the degraded-batch gauge
    both read it, and a RuntimeError in the SDK's collection drops the
    series that reports the reduced tier."""
    otel_mod.update_sweep_batch_size_cache("scheduled_to_pending", 100)

    reader = otel_mod._observe_sweep_batch_size(CallbackOptions())  # pyright: ignore[reportPrivateUsage]  # Why: the SDK's exact reader, driven directly.
    assert next(reader).value == 100

    otel_mod.update_sweep_batch_size_cache("deadline_exceeded", 25)

    remaining = list(reader)
    assert {dict(o.attributes or {})["sweep_name"] for o in remaining} <= {
        "scheduled_to_pending",
        "deadline_exceeded",
    }


def test_batch_size_configured_cache_reader_survives_concurrent_first_write(
    fresh_caches: None,
) -> None:
    """The configured-size cache has the same publication discipline — its
    gauge and the used-size gauge must stay label-matched, and a crashed
    collection on one would desynchronise the sweep-degraded comparison."""
    otel_mod.record_sweep_batch_size_configured("scheduled_to_pending", 100)

    reader = otel_mod._observe_sweep_batch_size_configured(CallbackOptions())  # pyright: ignore[reportPrivateUsage]  # Why: the SDK's exact reader, driven directly.
    assert next(reader).value == 100

    otel_mod.record_sweep_batch_size_configured("deadline_exceeded", 100)

    remaining = list(reader)
    assert {dict(o.attributes or {})["sweep_name"] for o in remaining} <= {
        "scheduled_to_pending",
        "deadline_exceeded",
    }
