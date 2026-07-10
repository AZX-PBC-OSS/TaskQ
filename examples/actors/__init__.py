"""Toy actor definitions exercising M1-M5+ TaskQ features.

All actors are registered on the ``"examples"`` queue and imported by both
the worker entrypoint (``worker.py``) and the trigger app (``app.py``).
Rate-limit primitives are registered on the ``ratelimit`` submodule's
``registry`` singleton at import time.

Actors are organized by feature domain:

- :mod:`basic` — long-running cancellable jobs and deferred scheduling.
- :mod:`failure` — retry, snooze, and simulated errors.
- :mod:`ratelimit` — sliding windows, token buckets, and concurrency reservations.
- :mod:`chained` — actor chaining and fan-out via ``ctx.jobs.enqueue()`` /
  ``ctx.jobs.enqueue_batch()``.
- :mod:`di` — dependency injection with LOOP-scope and TRANSIENT-scope providers.
- :mod:`batch` — ``enqueue_batch`` and fan-out-then-finalize via ``wait_for_batch``.
- :mod:`advanced` — singleton, max_concurrent, unique_for, and result_ttl.
- :mod:`ticker` — cron-scheduled periodic actor.
- :mod:`progress` — ctx.progress() and JobHandle.progress_stream() (M5).
- :mod:`tags_demo` — job tagging and tag-based filtering.
- :mod:`sync_demo` — plain ``def`` actor dispatched via ``asyncio.to_thread``.
"""

from examples.actors.advanced import capped_job, deduplicated, singleton_job, summer
from examples.actors.basic import counter, deferred
from examples.actors.batch import batch_counter, batch_finalizer
from examples.actors.chained import fan_out, step_one, step_two
from examples.actors.di import db_lookup_actor, fetch_actor
from examples.actors.failure import flaky, snoozer
from examples.actors.progress import file_processor
from examples.actors.ratelimit import (
    inmemory_rate_limited,
    reserved,
    token_rate_limited,
    window_rate_limited,
)
from examples.actors.sync_demo import count_words
from examples.actors.tags_demo import tagged_lower, tagged_upper
from examples.actors.ticker import ticker

__all__ = [
    "batch_counter",
    "batch_finalizer",
    "capped_job",
    "count_words",
    "counter",
    "db_lookup_actor",
    "deduplicated",
    "deferred",
    "fan_out",
    "fetch_actor",
    "file_processor",
    "flaky",
    "inmemory_rate_limited",
    "reserved",
    "singleton_job",
    "snoozer",
    "step_one",
    "step_two",
    "summer",
    "tagged_lower",
    "tagged_upper",
    "ticker",
    "token_rate_limited",
    "window_rate_limited",
]
