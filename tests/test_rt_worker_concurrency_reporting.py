"""Red-team pin: the workers row must expose the worker's effective
binding concurrency.

``run.py`` binds the worker's whole dispatch capacity to
``settings.max_concurrency`` (``local_queue`` maxsize, the available-
slot computation) — yet ``register_worker`` writes only hostname, pid,
queues, label, instance, and ``{"notify_enabled": ...}`` into the
``workers`` row. The single most load-bearing number in a capacity
incident — how many jobs each worker can actually run — is invisible
fleet-wide, which is issue #141's production-evidenced complaint: a
misconfigured fleet cannot be told apart from a correctly-sized one
from the database.

Every vendored system that reports worker state reports this number:
good_job's process rows carry the schedulers' stats including
``max_threads`` / ``available_threads``
(``vendor/good_job/app/models/good_job/process.rb`` —
``state: process_state`` with ``GoodJob::Scheduler.instances.map(&:stats)``);
sidekiq heartbeats ``concurrency`` and ``busy`` into its process entry
every 5 seconds (``vendor/sidekiq/lib/sidekiq/launcher.rb`` —
``transaction.hset(key, ..., "concurrency", @config.total_concurrency,
"busy", curstate.size, ...)``). The pin holds the minimum of that
contract: the ``register_worker`` write carries the effective binding
concurrency, in the row's metadata where the schema already has a
place for worker facts. Red today — only ``notify_enabled`` is written.
"""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from taskq.settings import WorkerSettings
from taskq.worker.run import register_worker

_TEST_DSN = "postgresql://localhost:5432/rt_worker_reporting"


def _mock_pool() -> tuple[MagicMock, AsyncMock]:
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    return mock_pool, mock_conn


async def test_register_worker_reports_effective_binding_concurrency() -> None:
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": _TEST_DSN, "schema_name": "taskq", "max_concurrency": 7},
        validate=False,
    )
    mock_pool, mock_conn = _mock_pool()

    worker_id = await register_worker(mock_pool, settings)

    assert worker_id is not None
    mock_conn.execute.assert_called_once()
    params: tuple[Any, ...] = mock_conn.execute.call_args[0][1:]

    metadata_json = params[6]
    metadata = json.loads(metadata_json)
    assert metadata.get("max_concurrency") == 7, (
        "the workers row must carry the worker's effective binding concurrency — "
        "the number that sizes local_queue and bounds every dispatch — so a "
        "capacity incident is diagnosable from the database. good_job reports "
        "max_threads in its process rows and sidekiq heartbeats concurrency "
        f"every 5s; ours wrote only {sorted(metadata)}"
    )
