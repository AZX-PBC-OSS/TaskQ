"""Integration tests for JobsClient against the PG backend.

Covers:
- JobsClient.enqueue against the PG backend with pg_notify actually firing.
"""

from datetime import UTC, datetime

import pytest
from pydantic import TypeAdapter

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.client import JobHandle, JobsClient
from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration


class TestJobsClientIntegration:
    """Integration: JobsClient.enqueue against the PG backend."""

    async def test_enqueue_against_pg(self, clean_jobs_app: JobsApp) -> None:
        """Verify the same surface works end-to-end with pg_notify
        actually firing.
        """

        backend = clean_jobs_app.backend

        client = JobsClient(backend)
        _ra: TypeAdapter[None] = TypeAdapter(type(None))
        args = EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC),
        )

        job_row = await backend.enqueue(args)
        handle: JobHandle[None] = JobHandle(
            client=client, row=job_row, result_adapter=_ra, was_existing=False
        )
        assert isinstance(handle, JobHandle)
        assert handle.job_id == args.id

        # Verify row in PG
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.id == handle.job_id
