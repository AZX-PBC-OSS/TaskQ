"""Red-team pin: the workers row must expose the worker's effective
binding concurrency.

``run.py`` binds the worker's whole dispatch capacity to
``settings.max_concurrency`` (``local_queue`` maxsize, the available-
slot computation) — yet ``register_worker`` writes only hostname, pid,
queues, label, instance, and ``{"notify_enabled": ...}`` into the
``workers`` row. The single most load-bearing number in a capacity
incident — how many jobs each worker can actually run — is invisible
fleet-wide, which is the production-evidenced complaint: a
misconfigured fleet cannot be told apart from a correctly-sized one
from the database.

Worker state systems report the effective binding concurrency to make
fleet capacity diagnosable: knowing the max thread / concurrency count
per worker, an operator can see at a glance whether the fleet is
correctly sized or whether a capacity incident points to
misconfiguration. The metadata schema already has a place for worker
facts — the write that carries effective binding concurrency lives
there, alongside the other row attributes. Red today — only
``notify_enabled`` is written.
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
        "capacity incident is diagnosable from the database without reading "
        "the worker's runtime state. Metadata carries: "
        f"{sorted(metadata)}"
    )
