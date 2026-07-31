"""Sub-job tag inheritance and merge e2e — real pipeline in a worker container.

Scenario:
parent job enqueued with a tag enqueues sub-jobs via ``ctx.jobs.enqueue()``.
Sub-jobs with no explicit tags inherit the parent tag; sub-jobs with explicit
tags carry both the inherited parent tag and the explicit tag.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from taskq import JobFilter

from ._assertions import wait_for_effects
from .actors import (
    PipelineStagePayload,
    TaggedPipelineStagePayload,
    pipeline_stage,
    tagged_pipeline_stage,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def test_sub_job_inherits_tags_in_pipeline(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Sub-jobs enqueued via ctx.jobs.enqueue() inherit parent tags.

    A pipeline of 3 stages is enqueued with tag "run-{run_id}". Each stage
    enqueues the next via ctx.jobs.enqueue() with no explicit tags. All 3
    jobs should be findable by the tag filter.
    """
    tag = f"run-{run_id[:8]}"
    await e2e_client.enqueue(
        pipeline_stage,
        PipelineStagePayload(run_id=run_id, stage=1, total_stages=3),
        tags=[tag],
    )

    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="stage",
        min_count=3,
        timeout=30,
    )

    page = await e2e_client.list(JobFilter(tags=(tag,)))
    assert len(page.jobs) == 3, (
        f"Expected 3 jobs with tag {tag!r}, found {len(page.jobs)}: {[j.id for j in page.jobs]}"
    )


async def test_sub_job_explicit_tags_merge_with_parent(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Explicit tags on sub-job merge with inherited parent tags.

    Stage 1 is enqueued with parent tag "run-{run_id}". Each stage enqueues
    the next with an explicit stage tag (e.g. "stage-2"). The sub-job should
    carry both the inherited run tag and the explicit stage tag.
    """
    parent_tag = f"run-{run_id[:8]}"

    await e2e_client.enqueue(
        tagged_pipeline_stage,
        TaggedPipelineStagePayload(run_id=run_id, stage=1, total_stages=3),
        tags=[parent_tag],
    )

    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="stage",
        min_count=3,
        timeout=30,
    )

    page = await e2e_client.list(JobFilter(tags=(parent_tag,)))
    assert len(page.jobs) == 3, (
        f"Expected 3 jobs with parent tag {parent_tag!r}, found {len(page.jobs)}"
    )

    stage2 = await e2e_client.list(JobFilter(tags=("stage-2",)))
    assert len(stage2.jobs) == 2, (
        f"Expected 2 jobs with stage-2 tag (stage 2 + stage 3 inherited it), found {len(stage2.jobs)}"
    )
    stage2_only = [j for j in stage2.jobs if "stage-3" not in j.tags]
    assert len(stage2_only) == 1, "Expected exactly one job with stage-2 but not stage-3"
    assert parent_tag in stage2_only[0].tags
    assert "stage-2" in stage2_only[0].tags
