"""A fleet of real worker pods against one real Postgres schema.

Every test in the multi-pod suite needs the same thing: several
independent workers, each with its own connection pools, its own worker
row, and its own identity, all polling one shared schema - the shape a
Kubernetes deployment has and a single-process test never does. This
module builds that once so the scenarios can spend their lines on the
operational event under test rather than on bootstrap.

A pod here is a real ``WorkerDeps`` plus a real ``PostgresBackend``, the
same pair ``_main`` constructs, opened through the production
``open_worker_deps`` context manager. What it deliberately is *not* is a
full ``_main``: that coroutine owns its shutdown event privately and
installs process-global signal handlers, so N of them in one process
would share one SIGTERM and one health socket. The pods here own their
shutdown events, which is what lets a test stop pod A while pod B keeps
running - the rolling deploy the fleet actually experiences.

Claiming goes through ``backend.dispatch_batch``, the production claim
round, and execution through ``consume_one_job``, the production attempt
path, so the seam under test is the real one: two pods contending for
rows in the same table, with Postgres arbitrating.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId, JobRow
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import StubActorConfig
from taskq.testing.pg import create_worker
from taskq.worker._consumer import consume_one_job
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.shutdown import ShutdownPhase

# A lease short enough that a test can watch an abandoned pod's rows
# become reclaimable without waiting on production timings, and long
# enough that a pod which is merely slow is never reclaimed mid-test.
FLEET_LOCK_LEASE = timedelta(seconds=30)


class FleetPayload(BaseModel):
    """The payload every fleet actor takes: a job's own identity."""

    marker: str = ""


@dataclass(slots=True)
class Pod:
    """One worker process's worth of state, as a test can drive it.

    ``deps`` and ``backend`` are the production objects; ``worker_id`` is
    the identity its claims are written under, so a row's
    ``locked_by_worker`` names the pod that holds it. ``shutdown_event``
    is owned here rather than inside a ``_main`` so a scenario can stop
    this pod alone.
    """

    name: str
    deps: WorkerDeps
    backend: PostgresBackend
    worker_id: UUID
    shutdown_event: asyncio.Event
    stack: AsyncExitStack

    @property
    def schema(self) -> str:
        return self.deps.settings.schema_name

    async def claim(self, queues: Sequence[str], limit: int) -> list[JobRow]:
        """Run one production claim round for this pod.

        The rows come back locked to ``worker_id`` exactly as they would
        in the producer loop; nothing here pre-selects or filters them,
        so what a test sees is what Postgres gave this pod when another
        pod was reaching for the same table.
        """
        return await self.backend.dispatch_batch(
            self.worker_id, list(queues), limit, FLEET_LOCK_LEASE
        )

    async def run(
        self,
        job: JobRow,
        handler: Callable[[FleetPayload, Any], Awaitable[object]],
        *,
        actor_config: StubActorConfig,
    ) -> object:
        """Execute one claimed job through the production attempt path."""

        async def _run_actor(_row: JobRow, ctx: Any) -> object:
            payload = FleetPayload.model_validate(job.payload)
            return await handler(payload, ctx)

        return await consume_one_job(
            self.backend,
            job,
            self.worker_id,
            deps=self.deps,
            run_actor=_run_actor,
            actor_config=actor_config,
            payload_type=FleetPayload,
            clock=SystemClock(),
            active_jobs=self.deps.active_jobs,
        )

    def start(
        self,
        job: JobRow,
        handler: Callable[[FleetPayload, Any], Awaitable[object]],
        *,
        actor_config: StubActorConfig,
    ) -> asyncio.Task[object]:
        """Start one claimed job through the production attempt path WITHOUT
        awaiting it to completion.

        ``Pod.run`` awaits ``consume_one_job``, so no scenario built on it
        can have a job mid-execution when a pod stops. ``start`` schedules
        the same call as a task: the job registers in
        ``deps.active_jobs`` (``consume_one_job``'s own registration, the
        same entry ``di_consumer_loop`` produces in production) while the
        caller drives on - which is the only way the shutdown
        orchestration's CANCELLING / FORCING / RELEASING phases ever see a
        running actor in a fleet scenario. The caller owns the task's
        lifecycle: join or cancel it once the scenario's pod is stopped.
        """
        return asyncio.create_task(self.run(job, handler, actor_config=actor_config))


@dataclass(slots=True)
class Fleet:
    """The pods a scenario is running, plus the schema they share."""

    schema: str
    dsn: str
    settings: WorkerSettings
    pods: list[Pod] = field(default_factory=list)
    _stack: AsyncExitStack | None = None

    def pod(self, name: str) -> Pod:
        for pod in self.pods:
            if pod.name == name:
                return pod
        raise KeyError(f"no pod named {name!r} in this fleet")

    def any_pod(self) -> Pod:
        """A live pod to run fleet-level queries through.

        Enqueueing and reading are things the fleet does, not things a
        particular pod does, but they still need a live connection. Any
        running pod's pool serves, and scenarios routinely stop the pod
        that happened to be first, so this never pins one.
        """
        if not self.pods:
            raise RuntimeError(
                "this fleet has no running pods, so it has no connection to work "
                "through; start a pod before enqueueing or reading."
            )
        return self.pods[0]

    async def start_pod(self, name: str) -> Pod:
        """Bring a new pod up against the shared schema.

        This is the scale-up and the rolling-deploy primitive: the pod
        opens its own pools and registers its own worker row, exactly as
        a freshly scheduled container does, and begins contending for the
        same rows as everyone already running.
        """
        assert self._stack is not None, "fleet used outside its context manager"
        stack = AsyncExitStack()
        deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(self.settings))
        try:
            backend = PostgresBackend(
                deps,
                clock=SystemClock(),
                cancellation_grace_period=timedelta(seconds=0),
                cleanup_grace_period=timedelta(seconds=0),
            )
            worker_id = new_uuid()
            async with deps.dispatcher_pool.acquire() as conn:
                await create_worker(conn, self.schema, worker_id)
        except BaseException:
            await stack.aclose()
            raise

        pod = Pod(
            name=name,
            deps=deps,
            backend=backend,
            worker_id=worker_id,
            shutdown_event=asyncio.Event(),
            stack=stack,
        )
        self.pods.append(pod)
        return pod

    async def stop_pod(self, name: str, *, graceful: bool = True) -> int:
        """Take a pod out of the fleet.

        ``graceful`` runs the production shutdown orchestration - the
        SIGTERM path, with its draining, cancelling and forcing phases -
        and returns its exit code. Without it the pod's pools
        close under it with its claims still held, which is what a
        ``SIGKILL``, an OOM kill, or a node loss looks like to the rest
        of the fleet: no hand-back, nothing but an expiring lease.
        """
        from taskq.worker.shutdown import orchestrate_shutdown

        pod = self.pod(name)
        exit_code = 0
        if graceful:
            pod.deps.producer_stop_event = asyncio.Event()
            pod.deps.shutdown_phase = ShutdownPhase.NONE
            pod.deps.shutdown_started_at = None
            exit_code = await orchestrate_shutdown(
                pod.deps,
                pod.deps.settings,
                pod.worker_id,
                pod.shutdown_event,
                None,
                backend=pod.backend,
            )
        await pod.stack.aclose()
        self.pods.remove(pod)
        return exit_code

    async def enqueue(
        self,
        count: int,
        *,
        actor: str,
        queue: str,
        max_attempts: int = 3,
        fairness_key: str | None = None,
        heartbeat_timeout: timedelta | None = None,
        start_to_close: timedelta | None = None,
    ) -> list[JobId]:
        """Put *count* due jobs on the queue, as a producer would."""
        pod = self.any_pod()
        ids: list[JobId] = []
        for index in range(count):
            job_id = JobId(new_uuid())
            await pod.backend.enqueue(
                EnqueueArgs(
                    id=job_id,
                    actor=actor,
                    queue=queue,
                    payload={"marker": f"{actor}-{index}"},
                    max_attempts=max_attempts,
                    retry_kind="transient",
                    scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
                    fairness_key=fairness_key,
                    heartbeat_timeout=heartbeat_timeout,
                    start_to_close=start_to_close,
                )
            )
            ids.append(job_id)
        return ids

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        """Read the fleet's shared schema the way an operator would."""
        pod = self.any_pod()
        async with pod.deps.worker_pool.acquire() as conn:
            return await conn.fetch(sql.format(schema=self.schema), *args)

    async def job_states(self) -> dict[UUID, str]:
        rows = await self.fetch('SELECT id, status FROM "{schema}".jobs')
        return {row["id"]: row["status"] for row in rows}

    async def rows_locked_by(self, worker_id: UUID) -> list[UUID]:
        rows = await self.fetch(
            "SELECT id FROM \"{schema}\".jobs WHERE status = 'running' AND locked_by_worker = $1",
            worker_id,
        )
        return [row["id"] for row in rows]


@asynccontextmanager
async def open_fleet(
    pg_dsn: str,
    *,
    schema: str,
    pods: Sequence[str],
    actors: Sequence[tuple[str, str]],
    settings_overrides: dict[str, str] | None = None,
    migrate: bool = True,
) -> AsyncGenerator[Fleet, None]:
    """Open a migrated schema with *actors* registered and *pods* running.

    ``actors`` is a sequence of ``(actor, queue)`` pairs; every actor a
    fleet dispatches needs its ``actor_config`` row, uncapped or not.
    Teardown closes each pod's pools and drops the schema, so a scenario
    that leaves a pod deliberately abandoned still cleans up.

    ``migrate=False`` leaves the schema exactly as the caller prepared
    it - for rollout scenarios that need a deliberately part-migrated
    schema, which this helper must not quietly complete.
    """
    from taskq.migrate import apply_pending
    from taskq.testing.settings import make_integration_settings_dict

    overrides = dict(settings_overrides or {})
    overrides["schema_name"] = schema
    settings = WorkerSettings.load_from_dict(make_integration_settings_dict(pg_dsn, **overrides))
    settings.schema_name = schema

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        if migrate:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await apply_pending(conn, schema=schema)
        for actor, queue in actors:
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config (actor, queue) '  # noqa: S608  # Why: schema is this module's own generated identifier; values are $-bound.
                "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
                actor,
                queue,
            )
    finally:
        await conn.close()

    fleet = Fleet(schema=schema, dsn=pg_dsn, settings=settings)
    stack = AsyncExitStack()
    fleet._stack = stack  # Why: the fleet owns this stack; the attribute is private to the pair.
    try:
        for name in pods:
            await fleet.start_pod(name)
        yield fleet
    finally:
        for pod in list(fleet.pods):
            await pod.stack.aclose()
        fleet.pods.clear()
        await stack.aclose()
        cleanup = await asyncpg.connect(str(settings.pg_dsn))
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()


def fleet_actor_config(*, max_attempts: int = 3) -> StubActorConfig:
    """The registration record a pod runs a fleet job under.

    Jitter is zero so a retry's backoff is a function of the attempt
    number alone: a scenario that asserts a job came back to the fleet
    can do so on the row, with no timing window to lose a race in.
    """
    return StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=max_attempts, jitter=0.0)
    )
