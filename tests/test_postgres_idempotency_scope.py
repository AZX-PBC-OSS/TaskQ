"""Integration tests for PostgresBackend idempotency_scope handling.

Covers:
- Regression: unscoped idempotency_key (default "" or explicit "") still
  dedupes globally exactly as before.
- Same idempotency_key with DIFFERENT idempotency_scope values → both
  enqueue as distinct jobs.
- Same idempotency_key AND same idempotency_scope → dedupes.
- Concurrent enqueue: asyncio.gather fires 2+ enqueue calls with same
  actor/scope/key; exactly one job is created (both return same id).
"""

import asyncio

import pytest

from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration


# ── Regression: unscoped key still dedupes globally ────────────


class TestUnscopedKeyDedupesGlobally:
    """idempotency_key with default scope ("" or explicit "") dedupes
    exactly as before the idempotency_scope feature was added."""

    async def test_default_scope_dedupes(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        key = "scope-regression-default"
        args1 = make_enqueue_args(idempotency_key=key)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key, payload={"v": 2})
        row2 = await backend.enqueue(args2)

        assert row1.id == row2.id
        assert row1.idempotency_key == key

    async def test_explicit_empty_scope_dedupes(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        key = "scope-regression-explicit-empty"
        args1 = make_enqueue_args(idempotency_key=key, idempotency_scope="")
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key, idempotency_scope="", payload={"v": 2})
        row2 = await backend.enqueue(args2)

        assert row1.id == row2.id

    async def test_default_equals_explicit_empty(self, clean_jobs_app: JobsApp) -> None:
        """Default scope ("") and explicit "" are the same scope."""
        backend = clean_jobs_app.backend

        key = "scope-regression-default-eq-empty"
        args1 = make_enqueue_args(idempotency_key=key)
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key, idempotency_scope="")
        row2 = await backend.enqueue(args2)

        assert row1.id == row2.id

    async def test_unscoped_dedup_preserves_payload(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        key = "scope-regression-payload"
        args1 = make_enqueue_args(idempotency_key=key, payload={"v": 1})
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key, payload={"v": 2})
        row2 = await backend.enqueue(args2)

        assert row2.id == row1.id
        assert row2.payload == {"v": 1}


# ── Same key, different scope → distinct jobs ──────────────────


class TestSameKeyDifferentScope:
    """Same idempotency_key with different idempotency_scope values
    should both enqueue as distinct jobs."""

    async def test_different_scopes_create_distinct_jobs(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        key = "scope-distinct-key"
        args1 = make_enqueue_args(idempotency_key=key, idempotency_scope="run-A")
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key, idempotency_scope="run-B")
        row2 = await backend.enqueue(args2)

        assert row1.id != row2.id

    async def test_scoped_vs_unscoped_distinct(self, clean_jobs_app: JobsApp) -> None:
        """A scoped key and an unscoped (default "") key with the same
        idempotency_key value are distinct jobs."""
        backend = clean_jobs_app.backend

        key = "scope-vs-unscoped"
        args1 = make_enqueue_args(idempotency_key=key, idempotency_scope="run-A")
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key)
        row2 = await backend.enqueue(args2)

        assert row1.id != row2.id


# ── Same key AND same scope → dedupes ──────────────────────────


class TestSameKeySameScope:
    """Same idempotency_key AND same idempotency_scope → dedupes
    (second returns first job)."""

    async def test_same_scope_dedupes(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        key = "scope-same-dedup"
        args1 = make_enqueue_args(idempotency_key=key, idempotency_scope="run-A")
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key, idempotency_scope="run-A", payload={"v": 2})
        row2 = await backend.enqueue(args2)

        assert row1.id == row2.id

    async def test_same_scope_preserves_payload(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        key = "scope-same-payload"
        args1 = make_enqueue_args(idempotency_key=key, idempotency_scope="run-A", payload={"v": 1})
        row1 = await backend.enqueue(args1)

        args2 = make_enqueue_args(idempotency_key=key, idempotency_scope="run-A", payload={"v": 2})
        row2 = await backend.enqueue(args2)

        assert row2.id == row1.id
        assert row2.payload == {"v": 1}

    async def test_scope_isolation(self, clean_jobs_app: JobsApp) -> None:
        """Within scope-A the key dedupes; within scope-B the same key
        dedupes independently; the two scopes are isolated."""
        backend = clean_jobs_app.backend

        key = "scope-isolation"

        args_a1 = make_enqueue_args(idempotency_key=key, idempotency_scope="A")
        row_a1 = await backend.enqueue(args_a1)

        args_a2 = make_enqueue_args(idempotency_key=key, idempotency_scope="A")
        row_a2 = await backend.enqueue(args_a2)

        args_b1 = make_enqueue_args(idempotency_key=key, idempotency_scope="B")
        row_b1 = await backend.enqueue(args_b1)

        args_b2 = make_enqueue_args(idempotency_key=key, idempotency_scope="B")
        row_b2 = await backend.enqueue(args_b2)

        assert row_a1.id == row_a2.id
        assert row_b1.id == row_b2.id
        assert row_a1.id != row_b1.id


# ── Concurrent enqueue: real ON CONFLICT race ──────────────────


class TestConcurrentEnqueueSameScope:
    """Fire 2+ concurrent PostgresBackend.enqueue() calls with the same
    actor, idempotency_scope, and idempotency_key.  Exactly one job should
    be created — both calls return the same job id.

    This exercises the real ON CONFLICT race in Postgres, not a mock.
    """

    async def test_two_concurrent_same_scope_key(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        key = "concurrent-same-scope"
        scope = "concurrent-run"
        args1 = make_enqueue_args(idempotency_key=key, idempotency_scope=scope)
        args2 = make_enqueue_args(idempotency_key=key, idempotency_scope=scope)

        row1, row2 = await asyncio.gather(
            backend.enqueue(args1),
            backend.enqueue(args2),
        )

        assert row1.id == row2.id

    async def test_four_concurrent_same_scope_key(self, clean_jobs_app: JobsApp) -> None:
        backend = clean_jobs_app.backend

        key = "concurrent-four-same-scope"
        scope = "concurrent-run-4"
        args_list = [
            make_enqueue_args(idempotency_key=key, idempotency_scope=scope) for _ in range(4)
        ]

        rows = await asyncio.gather(*[backend.enqueue(a) for a in args_list])

        ids = {str(r.id) for r in rows}
        assert len(ids) == 1

    async def test_concurrent_different_scopes_distinct(self, clean_jobs_app: JobsApp) -> None:
        """Concurrent enqueues with same key but different scopes should
        all succeed as distinct jobs."""
        backend = clean_jobs_app.backend

        key = "concurrent-diff-scopes"
        args_list = [
            make_enqueue_args(idempotency_key=key, idempotency_scope=f"scope-{i}") for i in range(4)
        ]

        rows = await asyncio.gather(*[backend.enqueue(a) for a in args_list])

        ids = {str(r.id) for r in rows}
        assert len(ids) == 4
