"""TaskQ e2e bridge prototype — asyncio mini-worker.

A standalone prototype that mimics the real worker's dispatch-loop
responsibilities against taskq's InMemoryBackend, with foreign (TypeScript)
actors executing in a warm Node runtime speaking NDJSON over stdio.

Responsibilities proven here (all durability stays Python-side):
  * runtime handshake + manifest validation (schema-hash vs pre-registered
    registry — mismatch fails loudly and quarantines the actor)
  * a merged actor registry: native Python ActorRef entries AND foreign
    runtime-hosted entries coexist in one dispatch table
  * dispatch loop over InMemoryBackend.dispatch_batch (same call shape the
    real worker uses)
  * payload validation against codegen'd Pydantic models; result validation
    through TypeAdapter on the way back into JSONB
  * progress/log events bound into structlog output per job
  * buffered sub-enqueue (mirror of SubJobEnqueuer): buffer sub-enqueue
    requests, flush on parent success, discard on failure/crash
  * terminal writes: mark_succeeded / mark_failed_or_retry / mark_cancelled
  * cooperative cancel: poll_cancel_flags -> {op:"cancel"} advisory message
  * forced cancel: 2s grace after advisory, then SIGKILL of the runtime
    process group, job failed with a WorkerCancelled-class ErrorInfo
  * fail-fast crash recovery: runtime EOF -> in-flight jobs immediately
    failed-or-retryable (measured against the reclaim_expired_locks sweep)
  * unknown/not-hosted actors: snooze-and-release (10s)

Everything the real worker would need that this prototype fakes is listed
in the README ("what the mini-worker had to fake").
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID

import structlog
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import (
    EnqueueArgs,
    ErrorInfo,
    JobId,
    JobRow,
    RetryKind,
)
from taskq.backend.clock import Clock
from taskq.context import JobContext
from taskq.testing.in_memory import InMemoryBackend

if TYPE_CHECKING:
    from taskq.actor import ActorRef

SPIKE_DIR = Path(__file__).resolve().parent

# ── structlog: same shape the real worker logs with ───────────────────

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.dev.ConsoleRenderer(colors=False),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
)
log: structlog.stdlib.BoundLogger = structlog.get_logger("miniworker")


# ── Protocol plumbing: the warm Node runtime ──────────────────────────


class RuntimeProc:
    """One warm Node actor-host process speaking NDJSON over stdio."""

    def __init__(
        self,
        actors_module: str,
        *,
        env_extra: dict[str, str] | None = None,
        node_bin: str = "node",
    ) -> None:
        self.actors_module = actors_module
        self.env_extra = env_extra or {}
        self.node_bin = node_bin
        self.proc: asyncio.subprocess.Process | None = None
        self.manifest: dict[str, Any] | None = None
        # Consumed by MiniWorker._pump_events (single consumer).
        self.lines: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self.stderr_tail: list[str] = []

    async def start(self, handshake_timeout: float = 10.0) -> dict[str, Any]:
        env = {**os.environ, **self.env_extra}
        cmd = [
            self.node_bin,
            "--import",
            "tsx/esm",
            str(SPIKE_DIR / "runtime.js"),
            self.actors_module,
        ]
        # start_new_session: the runtime gets its own process group so the
        # forced-cancel path can SIGKILL the whole group, node and tsx.
        self.proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=SPIKE_DIR,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        asyncio.create_task(self._read_stdout())
        asyncio.create_task(self._read_stderr())
        first = await asyncio.wait_for(self.lines.get(), handshake_timeout)
        if first is None or first.get("op") != "manifest":
            raise RuntimeError(f"runtime handshake failed: {first!r}")
        self.manifest = first
        return first

    async def _read_stdout(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        async for raw in self.proc.stdout:
            line = raw.decode().strip()
            if not line:
                continue
            try:
                self.lines.put_nowait(json.loads(line))
            except json.JSONDecodeError:
                self.stderr_tail.append(f"unparseable stdout: {line[:200]}")
        self.lines.put_nowait(None)  # EOF sentinel

    async def _read_stderr(self) -> None:
        assert self.proc is not None and self.proc.stderr is not None
        async for raw in self.proc.stderr:
            line = raw.decode(errors="replace").rstrip()
            if line:
                self.stderr_tail.append(line[:500])

    def send(self, obj: dict[str, Any]) -> None:
        assert self.proc is not None and self.proc.stdin is not None
        self.proc.stdin.write((json.dumps(obj) + "\n").encode())

    async def drain(self) -> None:
        assert self.proc is not None and self.proc.stdin is not None
        await self.proc.stdin.drain()

    def kill_group(self, sig: int = signal.SIGKILL) -> None:
        """Kill the runtime's whole process group (node + tsx)."""
        assert self.proc is not None
        try:
            os.killpg(os.getpgid(self.proc.pid), sig)
        except ProcessLookupError:
            pass

    async def stop(self, timeout: float = 3.0) -> None:
        """Graceful: close stdin (runtime exits 0 on EOF)."""
        if self.proc is None or self.proc.returncode is not None:
            return
        try:
            if self.proc.stdin is not None:
                self.proc.stdin.close()
            await asyncio.wait_for(self.proc.wait(), timeout)
        except asyncio.TimeoutError:
            self.kill_group()
            await self.proc.wait()


# ── Registry: pre-registered expected schemas (codegen stand-in) ──────


def canonical_hash(schema: dict[str, Any]) -> str:
    """sha256 over the canonical JSON dump (sorted keys, compact)."""
    return hashlib.sha256(
        json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


# Pydantic payload/result models — the Python-side stand-in for the output
# of the schema-pipeline codegen step (pydantic -> canonical JSON Schema is
# what the registry table stores; these models are what the worker imports).
class EtlSmallPayload(BaseModel):
    rows: int = Field(ge=1, le=1_000_000)
    label: str


class EtlSmallResult(BaseModel):
    rows_processed: int
    label: str
    duration_ms: int


class FanoutPayload(BaseModel):
    children: int = Field(ge=1, le=100)
    hold_ms: int = Field(ge=0, le=60_000)


class FanoutResult(BaseModel):
    requested: int


class SpinnyPayload(BaseModel):
    duration_ms: int = Field(ge=1, le=120_000)


class SpinnyResult(BaseModel):
    iterations: int
    observed_cancel: bool


class ForeignElsewherePayload(BaseModel):
    n: int


def _z(schema: dict[str, Any]) -> dict[str, Any]:
    return {"$schema": "https://json-schema.org/draft/2020-12/schema", **schema}


_INT = {"type": "integer", "minimum": -9007199254740991, "maximum": 9007199254740991}
# Schemas below mirror z.toJSONSchema output for the Zod schemas in
# actors/*.ts — in the real system the codegen pipeline writes these into
# the registry table, and this dict is its read model.


@dataclass
class RegisteredSchema:
    """One pre-registered row of the (simulated) schema registry table."""

    actor: str
    queue: str
    payload_schema: dict[str, Any]
    result_schema: dict[str, Any]
    payload_model: type[BaseModel]
    result_adapter: TypeAdapter[Any]
    max_attempts: int = 3
    retry_kind: RetryKind = "transient"
    rate_limits: list[dict[str, Any]] = field(default_factory=list)

    @property
    def payload_hash(self) -> str:
        return canonical_hash(self.payload_schema)


FOREIGN_SCHEMAS: dict[str, RegisteredSchema] = {
    e.actor: e
    for e in [
        RegisteredSchema(
            actor="etl_small",
            queue="etl",
            payload_schema=_z(
                {
                    "type": "object",
                    "properties": {
                        "rows": {"type": "integer", "minimum": 1, "maximum": 1000000},
                        "label": {"type": "string"},
                    },
                    "required": ["rows", "label"],
                    "additionalProperties": False,
                }
            ),
            result_schema=_z(
                {
                    "type": "object",
                    "properties": {
                        "rows_processed": _INT,
                        "label": {"type": "string"},
                        "duration_ms": _INT,
                    },
                    "required": ["rows_processed", "label", "duration_ms"],
                    "additionalProperties": False,
                }
            ),
            payload_model=EtlSmallPayload,
            result_adapter=TypeAdapter(EtlSmallResult),
            rate_limits=[
                {"kind": "token_bucket", "name": "etl_small_global", "capacity": 10, "refill_per_sec": 5}
            ],
        ),
        RegisteredSchema(
            actor="fanout",
            queue="etl",
            payload_schema=_z(
                {
                    "type": "object",
                    "properties": {
                        "children": {"type": "integer", "minimum": 1, "maximum": 100},
                        "hold_ms": {"default": 300, "type": "integer", "minimum": 0, "maximum": 60000},
                    },
                    "required": ["children", "hold_ms"],
                    "additionalProperties": False,
                }
            ),
            result_schema=_z(
                {
                    "type": "object",
                    "properties": {"requested": _INT},
                    "required": ["requested"],
                    "additionalProperties": False,
                }
            ),
            payload_model=FanoutPayload,
            result_adapter=TypeAdapter(FanoutResult),
        ),
        RegisteredSchema(
            actor="spinny",
            queue="cpu",
            payload_schema=_z(
                {
                    "type": "object",
                    "properties": {"duration_ms": {"type": "integer", "minimum": 1, "maximum": 120000}},
                    "required": ["duration_ms"],
                    "additionalProperties": False,
                }
            ),
            result_schema=_z(
                {
                    "type": "object",
                    "properties": {"iterations": _INT, "observed_cancel": {"type": "boolean"}},
                    "required": ["iterations", "observed_cancel"],
                    "additionalProperties": False,
                }
            ),
            payload_model=SpinnyPayload,
            result_adapter=TypeAdapter(SpinnyResult),
            max_attempts=1,
        ),
        # Registered fleet-wide but hosted by ANOTHER worker's runtime —
        # used by the mixed-fleet scenario (snooze-and-release here).
        RegisteredSchema(
            actor="foreign_elsewhere",
            queue="etl",
            payload_schema=_z(
                {
                    "type": "object",
                    "properties": {"n": _INT},
                    "required": ["n"],
                    "additionalProperties": False,
                }
            ),
            result_schema=_z({"type": "object", "properties": {}, "additionalProperties": True}),
            payload_model=ForeignElsewherePayload,
            result_adapter=TypeAdapter(dict),
        ),
    ]
}


class SchemaDriftError(RuntimeError):
    """Manifest schema hash does not match the pre-registered registry."""


# ── The two registry entry kinds ──────────────────────────────────────


class RegistryEntry(Protocol):
    name: str
    queue: str


@dataclass
class NativeEntry:
    """Registry entry kind 1 — a native Python actor (real ActorRef)."""

    ref: ActorRef[Any, Any]

    @property
    def name(self) -> str:
        return self.ref.name

    @property
    def queue(self) -> str:
        return self.ref.queue


@dataclass
class ForeignEntry:
    """Registry entry kind 2 — a TS actor hosted by the Node runtime.

    Built by merging the runtime's manifest entry with the pre-registered
    schema row (hash verified at startup).
    """

    manifest_entry: dict[str, Any]
    registered: RegisteredSchema

    @property
    def name(self) -> str:
        return self.registered.actor

    @property
    def queue(self) -> str:
        return self.registered.queue

    @property
    def max_attempts(self) -> int:
        return self.registered.max_attempts

    @property
    def retry_kind(self) -> RetryKind:
        return self.registered.retry_kind


# ── Sub-enqueue buffer (SubJobEnqueuer mirror) ────────────────────────


@dataclass
class SubJobRequest:
    actor: str
    payload: dict[str, Any]


class SubJobBuffer:
    """Mirror of SubJobEnqueuer's transactional buffer semantics.

    Sub-enqueue requests arriving from the runtime are buffered, not
    written. On parent success the buffer is flushed to the backend
    (each entry pre-validated against the target actor's payload model);
    on failure or crash the buffer is discarded — sub-jobs the parent
    never successfully completed are never enqueued.
    """

    def __init__(
        self, backend: InMemoryBackend, clock: Clock, entries: dict[str, RegistryEntry]
    ) -> None:
        self._backend = backend
        self._clock = clock
        self._entries = entries
        self._buffered: list[EnqueueArgs] = []
        self.discarded: int = 0

    def buffer(self, job_id: JobId, requests: list[SubJobRequest]) -> None:
        for req in requests:
            entry = self._entries.get(req.actor)
            if entry is None:
                raise LookupError(f"sub-job target actor not registered: {req.actor}")
            if isinstance(entry, ForeignEntry):
                # build_enqueue_args validates at flush time in the real
                # SubJobEnqueuer; validate now so a bad sub-payload poisons
                # the buffer immediately.
                entry.registered.payload_model.model_validate(req.payload)
                max_attempts = entry.max_attempts
                retry_kind = entry.retry_kind
            else:  # native entry
                entry.ref.payload_type.model_validate(req.payload)
                max_attempts = entry.ref.retry.max_attempts
                retry_kind = "transient"
            self._buffered.append(
                EnqueueArgs(
                    id=new_job_id(),
                    actor=req.actor,
                    queue=entry.queue,
                    payload=req.payload,
                    max_attempts=max_attempts,
                    retry_kind=retry_kind,
                    scheduled_at=self._clock.now(),
                    metadata={"parent_job_id": str(job_id)},
                )
            )

    async def flush(self) -> int:
        """Write buffered sub-jobs. Mirrors SubJobEnqueuer.flush_buffer:
        per-item failures are collected and surfaced as SubEnqueueError
        after the loop (the parent stays succeeded — that is the real
        semantic being demonstrated)."""
        snapshot, self._buffered = self._buffered, []
        failed: list[tuple[EnqueueArgs, Exception]] = []
        for args in snapshot:
            try:
                await self._backend.enqueue(args)
            except Exception as exc:  # noqa: BLE001 — mirror SubJobEnqueuer.flush_buffer
                failed.append((args, exc))
        if failed:
            from taskq.exceptions import SubEnqueueError

            raise SubEnqueueError(failed_items=failed)
        return len(snapshot)

    def discard(self) -> None:
        self.discarded += len(self._buffered)
        self._buffered = []


# ── The mini-worker ───────────────────────────────────────────────────


@dataclass
class InFlight:
    job: JobRow
    started_perf: float
    fut: asyncio.Future[dict[str, Any]]
    events: list[dict[str, Any]] = field(default_factory=list)
    progress_seq: int = 0
    progress_state: dict[str, Any] = field(default_factory=dict)
    subenqueue_seen: bool = False
    cancel_sent_at: float | None = None
    cancel_requested_at: float | None = None  # perf_counter when flag observed


@dataclass
class CrashRecovery:
    """Measured fail-fast recovery numbers for the last runtime death."""

    jobs_requeued: int = 0
    time_to_requeue_ms: float | None = None
    killed_at_perf: float | None = None
    signal_to_reaped_ms: float | None = None


class MiniWorker:
    def __init__(
        self,
        *,
        backend: InMemoryBackend,
        clock: Clock,
        runtime: RuntimeProc,
        native_entries: list[NativeEntry],
        queues: list[str],
        worker_id: UUID | None = None,
        dispatch_limit: int = 8,
        lock_lease: timedelta = timedelta(seconds=30),
        cancel_grace: timedelta = timedelta(seconds=2),
        trace: list[dict[str, Any]] | None = None,
    ) -> None:
        self.backend = backend
        self.clock = clock
        self.runtime = runtime
        self.queues = queues
        self.worker_id = worker_id or new_uuid()
        self.dispatch_limit = dispatch_limit
        self.lock_lease = lock_lease
        self.cancel_grace = cancel_grace
        self.trace = trace if trace is not None else []

        self.entries: dict[str, NativeEntry | ForeignEntry] = {
            e.name: e for e in native_entries
        }
        # Actor names whose manifest hash failed validation — dispatch
        # refuses them (quarantine) after the loud startup error.
        self.quarantined: set[str] = set()
        self.drift_report: dict[str, list[str]] = {}

        self._inflight: dict[JobId, InFlight] = {}
        self._buffers: dict[JobId, SubJobBuffer] = {}
        self._escalating: set[JobId] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._running = False
        self.crash = CrashRecovery()
        self.runtime_died = asyncio.Event()
        global _current_backend, _current_clock
        _current_backend = backend
        _current_clock = clock

    # ── startup: handshake + manifest validation ──

    async def start(self) -> dict[str, Any]:
        manifest = await self.runtime.start()
        hosted: dict[str, dict[str, Any]] = {a["name"]: a for a in manifest["actors"]}

        for name, registered in FOREIGN_SCHEMAS.items():
            if name not in hosted:
                continue  # not hosted by THIS runtime — fine (snooze-and-release)
            m = hosted[name]
            drift: list[str] = []
            if canonical_hash(m["payload_schema"]) != registered.payload_hash:
                drift.append(
                    f"payload_schema sha256 {canonical_hash(m['payload_schema'])[:12]}… "
                    f"!= registered {registered.payload_hash[:12]}…"
                )
            if canonical_hash(m["result_schema"]) != canonical_hash(registered.result_schema):
                drift.append("result_schema hash mismatch")
            if drift:
                # Loud startup failure for THIS actor: quarantine it; the
                # worker refuses to dispatch it. (A real worker could abort
                # outright or quarantine per-actor; quarantining keeps the
                # rest of the fleet moving while making the error unmissable.)
                self.quarantined.add(name)
                self.drift_report[name] = drift
                log.error(
                    "schema_drift_detected",
                    actor=name,
                    runtime=manifest["runtime_version"],
                    detail="; ".join(drift),
                    action="actor quarantined — dispatch will refuse it",
                )
                continue
            self.entries[name] = ForeignEntry(manifest_entry=m, registered=registered)

        hosted_but_unregistered = [n for n in hosted if n not in FOREIGN_SCHEMAS]
        if hosted_but_unregistered:
            raise SchemaDriftError(
                f"runtime hosts actors absent from the registry table: "
                f"{hosted_but_unregistered} — refusing to start"
            )

        self._running = True
        asyncio.create_task(self._pump_events())
        asyncio.create_task(self._cancel_poll_loop())
        asyncio.create_task(self._heartbeat_loop())
        log.info(
            "miniworker_started",
            worker_id=str(self.worker_id),
            native=[e.name for e in self.entries.values() if isinstance(e, NativeEntry)],
            foreign=[e.name for e in self.entries.values() if isinstance(e, ForeignEntry)],
            quarantined=sorted(self.quarantined),
            runtime_version=manifest["runtime_version"],
        )
        return manifest

    async def stop(self) -> None:
        self._running = False
        await self.runtime.stop()
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    # ── dispatch loop ──

    async def run_forever(self) -> None:
        """Mimic the real dispatch loop: pull, lock, execute, repeat."""
        while self._running:
            if self.runtime.proc is not None and self.runtime.proc.returncode is not None:
                # Fail-fast halts dispatch until the runtime is respawned
                # (respawn not implemented in the prototype).
                log.error("dispatch_halted_runtime_dead")
                self._trace("dispatch_halted_runtime_dead")
                return
            await self.dispatch_round()
            await asyncio.sleep(0.05)
            await self.backend.scheduled_to_pending()

    def _runtime_alive(self) -> bool:
        return self.runtime.proc is not None and self.runtime.proc.returncode is None

    async def dispatch_round(self) -> None:
        if not self._runtime_alive() and any(
            isinstance(e, ForeignEntry) for e in self.entries.values()
        ):
            return  # fail-fast has halted dispatch; respawn not implemented
        batch = await self.backend.dispatch_batch(
            self.worker_id, self.queues, self.dispatch_limit, self.lock_lease
        )
        for job in batch:
            if job.actor in self.quarantined:
                # Refuse dispatch for a drifted actor: release the job for
                # another (honest) worker via snooze.
                await self.backend.mark_snoozed(job.id, self.worker_id, timedelta(seconds=10))
                self._trace("quarantined_snoozed", job_id=str(job.id), actor=job.actor)
                continue
            self._spawn(self._run_job(job))

    def _spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _trace(self, kind: str, **data: Any) -> None:
        self.trace.append({"kind": kind, "t_ms": round(time.perf_counter() * 1000, 3), **data})

    # ── protocol pump: route runtime lines to per-job state ──

    async def _pump_events(self) -> None:
        while True:
            msg = await self.runtime.lines.get()
            if msg is None:
                await self._on_runtime_died()
                return
            op = msg.get("op")
            job_id_raw = msg.get("job_id")
            inflight = (
                self._inflight.get(_as_job_id(job_id_raw)) if job_id_raw is not None else None
            )
            if inflight is not None:
                inflight.events.append(msg)
            if op == "log":
                with structlog.contextvars.bound_contextvars(
                    job_id=str(job_id_raw), runtime="node"
                ):
                    log.info("runtime_log", message=msg.get("message"))
            elif op == "progress":
                if inflight is not None:
                    inflight.progress_seq += 1
                    inflight.progress_state.update(
                        {"step": msg.get("step"), "percent": msg.get("percent")}
                    )
            elif op == "subenqueue":
                if inflight is not None:
                    inflight.subenqueue_seen = True
                    buf = self._buffers.get(_as_job_id(job_id_raw))
                    if buf is not None:
                        try:
                            buf.buffer(
                                _as_job_id(job_id_raw),
                                [SubJobRequest(**j) for j in msg.get("jobs", [])],
                            )
                            self._trace(
                                "subenqueue_buffered",
                                job_id=str(job_id_raw),
                                count=len(msg.get("jobs", [])),
                            )
                        except (LookupError, ValidationError, TypeError) as exc:
                            # A bad sub-request must not wedge the run:
                            # surface as the run's failure.
                            inflight.fut.set_exception(exc)
            elif op == "done":
                if inflight is not None and not inflight.fut.done():
                    inflight.fut.set_result(msg)
            else:
                log.warning("runtime_protocol_error", line=msg)

    # ── per-job execution ──

    async def _run_job(self, job: JobRow) -> None:
        entry = self.entries.get(job.actor)
        if entry is None:
            # Actor known to actor_config but no handler in this worker:
            # snooze-and-release (TaskQ's unknown-actor pattern).
            await self.backend.mark_snoozed(job.id, self.worker_id, timedelta(seconds=10))
            self._trace("unknown_actor_snoozed", job_id=str(job.id), actor=job.actor)
            log.info("unknown_actor_snoozed", job_id=str(job.id), actor=job.actor, release_s=10)
            return

        with structlog.contextvars.bound_contextvars(
            job_id=str(job.id), actor=job.actor, attempt=job.attempt
        ):
            # Payload validation against the codegen'd Pydantic model.
            payload_model = (
                entry.registered.payload_model
                if isinstance(entry, ForeignEntry)
                else entry.ref.payload_type
            )
            try:
                payload = payload_model.model_validate(job.payload)
            except ValidationError as exc:
                info = ErrorInfo(
                    error_class="PayloadValidationError",
                    error_message=str(exc.errors()[:3]),
                    error_traceback=None,
                )
                await self._finish_failed(job, None, info, retryable=False)
                return

            inflight = InFlight(
                job=job,
                started_perf=time.perf_counter(),
                fut=asyncio.get_running_loop().create_future(),
            )
            buf = SubJobBuffer(self.backend, self.clock, self.entries)
            self._inflight[job.id] = inflight
            self._buffers[job.id] = buf
            try:
                if isinstance(entry, NativeEntry):
                    result_raw = await _call_native(entry.ref, payload, job)
                    done_msg: dict[str, Any] = {
                        "op": "done",
                        "ok": {"result": result_raw.model_dump(mode="json")},
                    }
                else:
                    if not self._runtime_alive():
                        # Runtime died between dispatch and send — leave the
                        # job pending; fail-fast recovery has already handled
                        # its in-flight peers. (Respawn is not implemented.)
                        self._trace("send_aborted_runtime_dead", job_id=str(job.id))
                        return
                    self.runtime.send(
                        {
                            "op": "run",
                            "job_id": str(job.id),
                            "attempt": job.attempt,
                            "actor": job.actor,
                            "payload": job.payload,
                        }
                    )
                    await self.runtime.drain()
                    done_msg = await inflight.fut
                await self._on_done(job, entry, inflight, buf, done_msg)
            except asyncio.InvalidStateError:
                pass  # future already resolved/cancelled (e.g. fail-fast won the race)
            except Exception as exc:  # noqa: BLE001
                info = ErrorInfo(
                    error_class=type(exc).__name__,
                    error_message=str(exc),
                    error_traceback=None,
                )
                await self._finish_failed(job, inflight, info, retryable=True)
            finally:
                self._inflight.pop(job.id, None)
                self._buffers.pop(job.id, None)

    async def _on_done(
        self,
        job: JobRow,
        entry: RegistryEntry,
        inflight: InFlight,
        buf: SubJobBuffer,
        done_msg: dict[str, Any],
    ) -> None:
        err = done_msg.get("err")
        if err is not None:
            info = ErrorInfo(
                error_class=str(err.get("errtype", "RuntimeError")),
                error_message=str(err.get("message", "")),
                error_traceback=err.get("backtrace"),
            )
            await self._finish_failed(job, inflight, info, retryable=bool(err.get("retryable", True)))
            return

        if done_msg.get("cancelled"):
            # Cooperative cancel observed by the runtime — clean cancel.
            await self.backend.mark_cancelled(
                job.id, self.worker_id, inflight.progress_seq, inflight.progress_state
            )
            self._trace(
                "job_cancelled",
                job_id=str(job.id),
                actor=job.actor,
                runtime_result=done_msg.get("result"),
            )
            log.info("job_cancelled", job_id=str(job.id), actor=job.actor)
            return

        # Validate the result back through the TypeAdapter before JSONB.
        result_adapter = (
            entry.registered.result_adapter if isinstance(entry, ForeignEntry) else entry.ref.result_adapter
        )
        raw_result = (done_msg.get("ok") or {}).get("result")
        validated = result_adapter.validate_python(raw_result)
        if isinstance(validated, BaseModel):
            result_json: dict[str, Any] = validated.model_dump(mode="json")
        elif isinstance(validated, dict):
            result_json = validated
        else:
            result_json = {"value": validated}

        ok = await self.backend.mark_succeeded(
            job.id, self.worker_id, result_json, inflight.progress_seq, inflight.progress_state
        )
        duration_ms = round((time.perf_counter() - inflight.started_perf) * 1000, 3)
        self._trace(
            "job_succeeded",
            job_id=str(job.id),
            actor=job.actor,
            duration_ms=duration_ms,
            subenqueue_seen=inflight.subenqueue_seen,
        )
        log.info("job_succeeded", job_id=str(job.id), actor=job.actor, duration_ms=duration_ms)
        if not ok:
            return
        # Flush the sub-enqueue buffer AFTER the parent's success write —
        # exactly SubJobEnqueuer.flush_buffer's position.
        try:
            flushed = await buf.flush()
            if flushed:
                self._trace("subenqueue_flushed", job_id=str(job.id), count=flushed)
        except Exception as exc:  # SubEnqueueError — parent stays succeeded
            log.error("sub_enqueue_flush_error", job_id=str(job.id), error=str(exc))

    async def _finish_failed(
        self, job: JobRow, inflight: InFlight | None, info: ErrorInfo, retryable: bool
    ) -> None:
        seq = inflight.progress_seq if inflight else 0
        state = inflight.progress_state if inflight else {}
        will_retry = (
            retryable and job.attempt < job.max_attempts and job.retry_kind != "non_retryable"
        )
        retry_delay = timedelta(0) if will_retry else None
        await self.backend.mark_failed_or_retry(
            job.id, self.worker_id, info, retry_delay, seq, state
        )
        self._trace(
            "job_failed",
            job_id=str(job.id),
            actor=job.actor,
            error_class=info.error_class,
            will_retry=will_retry,
        )
        log.warning(
            "job_failed",
            job_id=str(job.id),
            actor=job.actor,
            error_class=info.error_class,
            will_retry=will_retry,
        )

    # ── cancellation ──

    async def _cancel_poll_loop(self) -> None:
        """Mimic the real worker's cancel-poll: notice phase-1 flags for
        running jobs, forward the advisory to the runtime, escalate after
        the grace period."""
        while True:
            await asyncio.sleep(0.05)
            if not self._inflight:
                continue
            flags = await self.backend.poll_cancel_flags(self.worker_id)
            now = time.perf_counter()
            for flag in flags:
                inflight = self._inflight.get(flag.job_id)
                if inflight is None:
                    continue
                if inflight.cancel_requested_at is None:
                    inflight.cancel_requested_at = now
                    inflight.cancel_sent_at = now
                    self.runtime.send({"op": "cancel", "job_id": str(flag.job_id)})
                    self._trace("cancel_forwarded", job_id=str(flag.job_id))
                elif now - inflight.cancel_requested_at > self.cancel_grace.total_seconds():
                    if flag.job_id in self._escalating:
                        continue
                    self._escalating.add(flag.job_id)
                    await self.backend.write_cancel_escalation(flag.job_id, self.worker_id, 2)
                    self._trace("cancel_escalated", job_id=str(flag.job_id))
                    log.warning("cancel_escalated_kill_runtime", job_id=str(flag.job_id))
                    self.crash.killed_at_perf = time.perf_counter()
                    self.runtime.kill_group(signal.SIGKILL)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            if self._inflight:
                await self.backend.heartbeat_jobs(self.worker_id, self.lock_lease)

    # ── fail-fast crash recovery ──

    async def _on_runtime_died(self) -> None:
        """Runtime stdout EOF — mark every in-flight job failed-or-retryable
        immediately (fail-fast), instead of waiting for the lock-lease
        reclaim sweep."""
        t0 = time.perf_counter()
        log.error("runtime_died", in_flight=len(self._inflight))
        self._trace("runtime_died", in_flight=len(self._inflight))
        requeued = 0
        latencies: list[float] = []
        for job_id, inflight in list(self._inflight.items()):
            job = inflight.job
            kill_ref = self.crash.killed_at_perf or t0
            if job_id in self._escalating:
                info = ErrorInfo(
                    error_class="WorkerCancelled",
                    error_message=(
                        "cooperative cancel ignored by actor; forced via runtime "
                        f"process-group SIGKILL after {self.cancel_grace.total_seconds():.0f}s grace"
                    ),
                    error_traceback=None,
                )
                await self.backend.mark_failed_or_retry(
                    job_id, self.worker_id, info, None,
                    inflight.progress_seq, inflight.progress_state,
                )
                self._trace("job_failed_forced_cancel", job_id=str(job_id))
            elif job.attempt < job.max_attempts and job.retry_kind != "non_retryable":
                info = ErrorInfo(
                    error_class="WorkerCrashed",
                    error_message="runtime process died with job in flight (fail-fast)",
                    error_traceback=None,
                )
                await self.backend.mark_failed_or_retry(
                    job_id, self.worker_id, info, timedelta(0),
                    inflight.progress_seq, inflight.progress_state,
                )
                requeued += 1
            else:
                info = ErrorInfo(
                    error_class="WorkerCrashed",
                    error_message="runtime process died; retry budget exhausted",
                    error_traceback=None,
                )
                await self.backend.mark_failed_or_retry(
                    job_id, self.worker_id, info, None,
                    inflight.progress_seq, inflight.progress_state,
                )
            latencies.append((time.perf_counter() - kill_ref) * 1000)
            buf = self._buffers.pop(job_id, None)
            if buf is not None and buf._buffered:
                # Crash discards unflushed sub-enqueues — buffered-bridge
                # semantics: never durable unless the parent succeeded.
                count = len(buf._buffered)
                buf.discard()
                self._trace("subenqueue_discarded", job_id=str(job_id), count=count)
            if not inflight.fut.done():
                inflight.fut.cancel()
            self._inflight.pop(job_id, None)
        elapsed = (time.perf_counter() - t0) * 1000
        self.crash.jobs_requeued = requeued
        self.crash.time_to_requeue_ms = elapsed
        self.crash.signal_to_reaped_ms = max(latencies) if latencies else elapsed
        self._trace(
            "runtime_died_recovered",
            requeued=requeued,
            time_to_requeue_ms=round(elapsed, 3),
        )
        log.error("runtime_died_recovered", requeued=requeued, time_to_requeue_ms=round(elapsed, 3))
        self.runtime_died.set()


def _as_job_id(raw: str) -> JobId:
    return JobId(UUID(raw))


async def _call_native(ref: ActorRef[Any, Any], payload: BaseModel, job: JobRow) -> Any:
    """Invoke a native ActorRef the way the real dispatcher would —
    constructing a JobContext when the handler declared one."""
    if not ref.wants_ctx:
        return await ref(payload)
    from taskq.client._enqueuer import SubJobEnqueuer

    ctx = JobContext(
        job_id=job.id,
        actor=job.actor,
        queue=job.queue,
        attempt=job.attempt,
        worker_id=ref.__self__.worker_id if hasattr(ref, "__self__") else new_uuid(),
        payload=payload,
        jobs=SubJobEnqueuer(None, None, _current_backend, clock=_current_clock),
        log=structlog.get_logger("native_actor"),
    )
    return await ref(payload, ctx)


# Module-level current backend/clock — a DI-shaped wart the prototype
# accepts to keep _call_native simple (see README frictions).
_current_backend: InMemoryBackend | None = None
_current_clock: Clock | None = None


# ── Client stand-in: typed enqueue the way JobsClient does ────────────


async def enqueue_typed(
    backend: InMemoryBackend,
    *,
    actor: str,
    queue: str,
    payload: BaseModel,
    max_attempts: int = 3,
    retry_kind: RetryKind = "transient",
    clock: Clock,
) -> JobRow:
    """Mimic JobsClient.enqueue: validate, dump to a JSONB dict, EnqueueArgs."""
    payload_model = type(payload).model_validate(payload)  # client-side validation
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload=payload_model.model_dump(mode="json"),
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        scheduled_at=clock.now(),
    )
    return await backend.enqueue(args)


def now_utc() -> datetime:
    return datetime.now(tz=UTC)
