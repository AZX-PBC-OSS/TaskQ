"""Worker entrypoint for the example application.

Runs ``apply_pending_locked`` at startup so two worker replicas starting
simultaneously do not race, then hands off to ``worker_main``.  All
configuration flows through ``WorkerSettings.load()``; ``TASKQ_QUEUES=examples``
is set in docker-compose, not here.

A custom DI registry is built via :func:`examples.actors.di.build_registry`
and passed to ``worker_main`` so that ``FakeHttpClient`` (LOOP scope) and
``FakeDb`` (TRANSIENT scope) are available for injection into actors.
"""

import asyncio
import sys
from typing import Any

from examples.actors import (
    batch_counter,
    batch_finalizer,
    capped_job,
    count_words,
    counter,
    db_lookup_actor,
    deduplicated,
    deferred,
    fan_out,
    fetch_actor,
    file_processor,
    flaky,
    inmemory_rate_limited,
    reserved,
    singleton_job,
    snoozer,
    step_one,
    step_two,
    summer,
    tagged_lower,
    tagged_upper,
    ticker,
    token_rate_limited,
    window_rate_limited,
)
from examples.actors.di import build_registry
from examples.fastapi_app.actors import process_item
from taskq import ActorRef
from taskq.migrate import apply_pending_locked
from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main

ACTORS: dict[str, ActorRef[Any, Any]] = {
    "counter": counter,
    "flaky": flaky,
    "snoozer": snoozer,
    "deferred": deferred,
    "window_rate_limited": window_rate_limited,
    "token_rate_limited": token_rate_limited,
    "inmemory_rate_limited": inmemory_rate_limited,
    "reserved": reserved,
    # chained actors
    "step_one": step_one,
    "step_two": step_two,
    "fan_out": fan_out,
    # batch actors
    "batch_counter": batch_counter,
    "batch_finalizer": batch_finalizer,
    # DI actors
    "fetch": fetch_actor,
    "db_lookup": db_lookup_actor,
    # advanced actors
    "singleton_job": singleton_job,
    "capped_job": capped_job,
    "deduplicated": deduplicated,
    "summer": summer,
    # cron actors
    "ticker": ticker,
    # progress actors
    "file_processor": file_processor,
    # tagged actors
    "tagged_lower": tagged_lower,
    "tagged_upper": tagged_upper,
    # sync actor
    "count_words": count_words,
    # fastapi_app demo actor
    "process_item": process_item,
}

if __name__ == "__main__":
    settings = WorkerSettings.load()

    asyncio.run(
        apply_pending_locked(str(settings.resolved_pg_dsn_direct), schema=settings.schema_name)
    )
    sys.exit(worker_main(settings, actor_registry=ACTORS, di_registry=build_registry()))
