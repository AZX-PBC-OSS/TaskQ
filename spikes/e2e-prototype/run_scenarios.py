#!/usr/bin/env python
"""Scenario driver for the TaskQ e2e bridge prototype.

Runs all 8 scenarios against InMemoryBackend + the warm Node runtime and
writes one JSON trace per scenario into traces/. Every number in the
README comes from a real run of this script.

    uv run python spikes/e2e-prototype/run_scenarios.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, JobFilter, JobStatus
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

import miniworker as mw
from miniworker import (
    EtlSmallPayload,
    FanoutPayload,
    FOREIGN_SCHEMAS,
    ForeignEntry,
    MiniWorker,
    NativeEntry,
    RuntimeProc,
    SpinnyPayload,
    enqueue_typed,
)
from native_actor import native_wordcount

START = datetime(2026, 1, 1, tzinfo=UTC)
TERMINAL: set[JobStatus] = {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
TRACES_DIR = Path(__file__).resolve().parent / "traces"

# Registry actors to admit into dispatch (the actor_config table read model)
_ACTOR_NAMES = ["etl_small", "fanout", "spinny", "native_wordcount", "foreign_elsewhere"]


class Rig:
    """One scenario's isolated stack: backend + clock + runtime + worker."""

    def __init__(
        self,
        name: str,
        *,
        actors_module: str = "./actors/index.ts",
        env_extra: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self.clock = FakeClock(START)
        self.backend = InMemoryBackend(clock=self.clock)
        for actor_name in _ACTOR_NAMES:
            self.backend.register_actor_config(actor=actor_name)
        self.runtime = RuntimeProc(actors_module, env_extra=env_extra)
        self.worker = MiniWorker(
            backend=self.backend,
            clock=self.clock,
            runtime=self.runtime,
            native_entries=[NativeEntry(ref=native_wordcount)],
            queues=["default", "etl", "cpu", "native"],
            trace=[],
        )
        self.loop_task: asyncio.Task[None] | None = None

    async def start(self) -> dict[str, Any]:
        manifest = await self.worker.start()
        self.loop_task = asyncio.create_task(self.worker.run_forever())
        return manifest

    async def stop(self) -> None:
        await self.worker.stop()
        if self.loop_task is not None:
            self.loop_task.cancel()
            try:
                await self.loop_task
            except asyncio.CancelledError:
                pass

    async def wait_terminal(self, job_id: Any, timeout: float = 10.0) -> dict[str, Any]:
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < timeout:
            row = await self.backend.get(job_id)
            if row is not None and row.status in TERMINAL:
                return {"status": row.status, "ms": round((time.perf_counter() - t0) * 1000, 3)}
            await asyncio.sleep(0.02)
        row = await self.backend.get(job_id)
        raise TimeoutError(f"job {job_id} not terminal within {timeout}s (status={row and row.status})")

    async def wait_status(self, job_id: Any, status: str, timeout: float = 10.0) -> None:
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < timeout:
            row = await self.backend.get(job_id)
            if row is not None and row.status == status:
                return
            await asyncio.sleep(0.02)
        row = await self.backend.get(job_id)
        raise TimeoutError(f"job {job_id} never reached {status} (status={row and row.status})")

    async def snapshot_jobs(self) -> list[dict[str, Any]]:
        rows = await self.backend.list_jobs(JobFilter(limit=1000))
        return [
            {
                "job_id": str(r.id),
                "actor": r.actor,
                "queue": r.queue,
                "status": r.status,
                "attempt": r.attempt,
                "max_attempts": r.max_attempts,
                "error_class": r.error_class,
                "error_message": r.error_message,
                "result": r.result,
                "result_size_bytes": r.result_size_bytes,
                "progress_seq": r.progress_seq,
                "progress_state": r.progress_state,
                "cancel_phase": int(r.cancel_phase),
                "scheduled_at": r.scheduled_at.isoformat(),
                "metadata": r.metadata,
            }
            for r in rows
        ]


async def save_trace(name: str, description: str, rig: Rig, metrics: dict[str, Any]) -> None:
    TRACES_DIR.mkdir(exist_ok=True)
    doc = {
        "scenario": name,
        "description": description,
        "generated_at": datetime.now(UTC).isoformat(),
        "metrics": metrics,
        "backend_final": await rig.snapshot_jobs(),
        "worker_trace": rig.worker.trace,
        "runtime_stderr_tail": rig.runtime.stderr_tail[-10:],
    }
    out = TRACES_DIR / f"{name}.json"
    out.write_text(json.dumps(doc, indent=2, default=str))
    print(f"  trace → {out.relative_to(Path.cwd())}")


# ── Scenario 1: happy path ────────────────────────────────────────────


async def scenario_1() -> None:
    print("1. happy path: etl_small via typed Python payload → TS runtime → typed result")
    rig = Rig("happy_path")
    await rig.start()
    try:
        t0 = time.perf_counter()
        row = await enqueue_typed(
            rig.backend,
            actor="etl_small",
            queue="etl",
            payload=EtlSmallPayload(rows=1200, label="happy-path"),
            clock=rig.clock,
        )
        t_enqueue = time.perf_counter() - t0
        await rig.wait_terminal(row.id, timeout=15)
        final = await rig.backend.get(row.id)
        assert final is not None and final.status == "succeeded"

        # Client-side result validation through the TypeAdapter (the
        # JobHandle.result round-trip the real client does).
        adapter = FOREIGN_SCHEMAS["etl_small"].result_adapter
        validated = adapter.validate_python(final.result)

        events = await rig.backend.get_events(row.id)
        attempts = await rig.backend.get_attempts(row.id)
        await save_trace(
            "01_happy_path",
            "enqueue etl_small (typed payload) → dispatched to Node runtime → "
            "progress/log events streamed → result re-validated via Pydantic "
            "TypeAdapter → mark_succeeded with JSONB result",
            rig,
            {
                "enqueue_latency_ms": round(t_enqueue * 1000, 3),
                "job_status": final.status,
                "attempt": final.attempt,
                "result_validated": True,
                "result": final.result,
                "result_validated_object": validated.model_dump(),
                "result_size_bytes": final.result_size_bytes,
                "progress_seq": final.progress_seq,
                "progress_state": final.progress_state,
                "state_change_events": len(events),
                "attempt_rows": len(attempts),
                "attempt_outcomes": [a.outcome for a in attempts],
            },
        )
    finally:
        await rig.stop()


# ── Scenario 2: fanout success ────────────────────────────────────────


async def scenario_2() -> None:
    print("2. fanout success: 5 sub-enqueues buffered, flushed exactly on parent success")
    rig = Rig("fanout_success")
    await rig.start()
    try:
        row = await enqueue_typed(
            rig.backend,
            actor="fanout",
            queue="etl",
            payload=FanoutPayload(children=5, hold_ms=100),
            clock=rig.clock,
        )
        await rig.wait_terminal(row.id, timeout=15)
        # Drain the flushed children to completion.
        children = await rig.backend.list_jobs(JobFilter(actor="etl_small", limit=100))
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < 15:
            rows = [await rig.backend.get(c.id) for c in children]
            if all(r is not None and r.status in TERMINAL for r in rows):
                break
            await asyncio.sleep(0.02)
        children_final = [await rig.backend.get(c.id) for c in children]
        flushed = [t for t in rig.worker.trace if t["kind"] == "subenqueue_flushed"]
        await save_trace(
            "02_fanout_success",
            "fanout actor requests 5 sub-enqueues via ctx.subenqueue → buffer "
            "held in Python → parent succeeded → flush enqueued exactly 5 → "
            "children drained to succeeded",
            rig,
            {
                "parent_status": (await rig.backend.get(row.id)).status,
                "subenqueue_buffered_events": [
                    t for t in rig.worker.trace if t["kind"] == "subenqueue_buffered"
                ],
                "flushed": flushed,
                "children_enqueued": len(children),
                "children_succeeded": sum(1 for c in children_final if c.status == "succeeded"),
                "children_have_parent_tag": all(
                    c.metadata.get("parent_job_id") == str(row.id) for c in children_final
                ),
            },
        )
        assert len(children) == 5, f"expected exactly 5 sub-jobs, got {len(children)}"
    finally:
        await rig.stop()


# ── Scenario 3: fanout crash ──────────────────────────────────────────


async def scenario_3() -> None:
    print("3. fanout crash: runtime killed after buffering, before done → 0 sub-jobs")
    rig = Rig("fanout_crash")
    await rig.start()
    try:
        row = await enqueue_typed(
            rig.backend,
            actor="fanout",
            queue="etl",
            payload=FanoutPayload(children=5, hold_ms=3000),
            clock=rig.clock,
        )
        # Wait until the runtime's subenqueue frame is buffered (not flushed).
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < 10:
            if any(t["kind"] == "subenqueue_buffered" for t in rig.worker.trace):
                break
            await asyncio.sleep(0.02)
        t_kill = time.perf_counter()
        rig.runtime.kill_group()
        await asyncio.wait_for(rig.worker.runtime_died.wait(), 5)
        kill_to_requeued_ms = (time.perf_counter() - t_kill) * 1000
        await asyncio.sleep(0.2)  # settle
        children = await rig.backend.list_jobs(JobFilter(actor="etl_small", limit=100))
        parent = await rig.backend.get(row.id)
        await save_trace(
            "03_fanout_crash",
            "runtime SIGKILLed after subenqueue buffered but before done → "
            "buffer discarded (0 sub-jobs durable) → parent fail-fast requeued",
            rig,
            {
                "parent_status": parent.status,
                "parent_error_class": parent.error_class,
                "parent_attempt": parent.attempt,
                "sub_jobs_enqueued": len(children),
                "buffer_discarded_events": [
                    t for t in rig.worker.trace if t["kind"] == "subenqueue_discarded"
                ],
                "kill_to_requeued_ms": round(kill_to_requeued_ms, 3),
                "fail_fast_internal_ms": rig.worker.crash.time_to_requeue_ms,
            },
        )
        assert len(children) == 0, f"expected 0 sub-jobs, got {len(children)}"
        assert parent.status == "pending", f"expected requeued pending, got {parent.status}"
    finally:
        await rig.stop()


# ── Scenario 4: cooperative cancel ────────────────────────────────────


async def scenario_4() -> None:
    print("4. cooperative cancel: spinny observes ctx.cancelled, returns early")
    rig = Rig("coop_cancel")
    await rig.start()
    try:
        row = await enqueue_typed(
            rig.backend,
            actor="spinny",
            queue="cpu",
            payload=SpinnyPayload(duration_ms=10_000),
            max_attempts=1,
            clock=rig.clock,
        )
        await rig.wait_status(row.id, "running")
        t_cancel = time.perf_counter()
        await rig.backend.write_cancel_request(row.id, "scenario 4: cooperative cancel")
        # cancel-poll loop forwards {op:"cancel"}; spinny checks every ~100ms.
        await rig.wait_terminal(row.id, timeout=5)
        cancel_to_terminal_ms = (time.perf_counter() - t_cancel) * 1000
        final = await rig.backend.get(row.id)
        cancelled_trace = next(
            (t for t in rig.worker.trace if t["kind"] == "job_cancelled"), {}
        )
        runtime_result = cancelled_trace.get("runtime_result")
        await save_trace(
            "04_cooperative_cancel",
            "cancel requested mid-run → worker cancel-poll loop forwards "
            "{op:'cancel'} advisory → spinny observes ctx.cancelled() within a "
            "~100ms chunk, returns early → runtime classifies run cancelled → "
            "mark_cancelled",
            rig,
            {
                "cancel_to_terminal_ms": round(cancel_to_terminal_ms, 3),
                "job_status": final.status,
                "runtime_reported": runtime_result,
                "cancel_phase_final": int(final.cancel_phase),
                "cancel_forwarded_events": [
                    t for t in rig.worker.trace if t["kind"] == "cancel_forwarded"
                ],
            },
        )
        assert final.status == "cancelled"
        assert runtime_result and runtime_result.get("observed_cancel") is True
    finally:
        await rig.stop()


# ── Scenario 5: forced cancel ─────────────────────────────────────────


async def scenario_5() -> None:
    print("5. forced cancel: spinny --no-cooperate ignores advisory → 2s grace → SIGKILL")
    rig = Rig("forced_cancel", env_extra={"SPINNY_NO_COOPERATE": "1"})
    await rig.start()
    try:
        row = await enqueue_typed(
            rig.backend,
            actor="spinny",
            queue="cpu",
            payload=SpinnyPayload(duration_ms=30_000),
            max_attempts=1,
            clock=rig.clock,
        )
        await rig.wait_status(row.id, "running")
        t_cancel = time.perf_counter()
        await rig.backend.write_cancel_request(row.id, "scenario 5: forced cancel")
        await asyncio.wait_for(rig.worker.runtime_died.wait(), 10)
        # signal→reaped is measured inside the fail-fast path; cancel→terminal
        # derived from wall clock around the cancel request.
        await asyncio.sleep(0.1)
        final = await rig.backend.get(row.id)
        cancel_to_terminal_ms = (time.perf_counter() - t_cancel) * 1000
        await save_trace(
            "05_forced_cancel",
            "spinny --no-cooperate pins the event loop so the cancel line is "
            "never read → advisory ignored → 2s grace → cancel-poll escalates "
            "(write_cancel_escalation phase 2) → SIGKILL process group → "
            "fail-fast marks job failed with WorkerCancelled ErrorInfo",
            rig,
            {
                "cancel_to_terminal_ms": round(cancel_to_terminal_ms, 3),
                "job_status": final.status,
                "error_class": final.error_class,
                "error_message": final.error_message,
                "cancel_phase_final": int(final.cancel_phase),
                "cancel_escalated_events": [
                    t for t in rig.worker.trace if t["kind"] == "cancel_escalated"
                ],
                "signal_to_reaped_ms": rig.worker.crash.signal_to_reaped_ms,
            },
        )
        assert final.status == "failed"
        assert final.error_class == "WorkerCancelled"
        assert int(final.cancel_phase) == 2
    finally:
        await rig.stop()


# ── Scenario 6: crash recovery, fail-fast vs reclaim sweep ────────────


async def scenario_6() -> None:
    print("6. crash recovery: kill runtime with 3 in-flight → fail-fast vs reclaim sweep")
    rig = Rig("crash_recovery")
    await rig.start()
    try:
        jobs = [
            await enqueue_typed(
                rig.backend,
                actor="etl_small",
                queue="etl",
                payload=EtlSmallPayload(rows=500_000, label=f"slow-{i}"),
                clock=rig.clock,
            )
            for i in range(3)
        ]
        for j in jobs:
            await rig.wait_status(j.id, "running")
        t_kill = time.perf_counter()
        rig.runtime.kill_group()
        await asyncio.wait_for(rig.worker.runtime_died.wait(), 5)
        kill_to_requeued_ms = (time.perf_counter() - t_kill) * 1000
        rows = [await rig.backend.get(j.id) for j in jobs]

        # Fallback comparison: the reclaim_expired_locks sweep path. Fresh
        # running rows (same lock lease), clock advanced past expiry, then
        # time one sweep call.
        fb_backend = InMemoryBackend(clock=FakeClock(START))
        for actor_name in _ACTOR_NAMES:
            fb_backend.register_actor_config(actor=actor_name)
        fb_args = [
            EnqueueArgs(
                id=new_job_id(),
                actor="etl_small",
                queue="etl",
                payload={"rows": 1, "label": f"fb-{i}"},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=START,
            )
            for i in range(3)
        ]
        for a in fb_args:
            await fb_backend.enqueue(a)
        worker_id = fb_backend._worker_id  # test-only inspection, documented
        dispatched = await fb_backend.dispatch_batch(
            worker_id, ["etl"], 3, timedelta(seconds=30)
        )
        assert len(dispatched) == 3
        fb_backend.advance_clock_to(START + timedelta(seconds=31))  # past lock lease
        t_sweep = time.perf_counter()
        reclaimed = await fb_backend.reclaim_expired_locks(
            timedelta(0), timedelta(0), batch_size=100
        )
        sweep_ms = (time.perf_counter() - t_sweep) * 1000

        await save_trace(
            "06_crash_recovery",
            "runtime SIGKILLed with 3 jobs in flight → stdout EOF → fail-fast "
            "marks all 3 failed-or-retryable immediately (status pending, "
            "WorkerCrashed). Fallback: reclaim_expired_locks sweep measured "
            "separately (lock lease 30s, graces 0).",
            rig,
            {
                "fail_fast": {
                    "kill_to_requeued_ms": round(kill_to_requeued_ms, 3),
                    "internal_recovery_ms": rig.worker.crash.time_to_requeue_ms,
                    "jobs_requeued": sum(1 for r in rows if r.status == "pending"),
                    "statuses": [r.status for r in rows],
                    "error_classes": [r.error_class for r in rows],
                    "attempts": [r.attempt for r in rows],
                },
                "fallback_reclaim_sweep": {
                    "rows_reclaimed": reclaimed,
                    "sweep_execution_ms": round(sweep_ms, 3),
                    "lock_lease_s": 30,
                    "note": (
                        "time-to-requeue via sweep-only ≈ remaining lock lease "
                        "(≤ lock_lease) + sweep poll interval + sweep execution; "
                        "the sweep's own execution was "
                        f"{sweep_ms:.3f} ms for 3 rows. Sweep also requeues at "
                        "now+5s (extra dispatch delay)."
                    ),
                },
            },
        )
        assert all(r.status == "pending" for r in rows), [r.status for r in rows]
    finally:
        await rig.stop()


# ── Scenario 7: schema drift ──────────────────────────────────────────


async def scenario_7() -> None:
    print("7. schema drift: manifest hash mismatch → loud error, actor quarantined")
    rig = Rig("schema_drift", actors_module="./actors/drifted.ts")
    manifest = await rig.start()
    try:
        row = await enqueue_typed(
            rig.backend,
            actor="etl_small",
            queue="etl",
            payload=EtlSmallPayload(rows=10, label="drift"),
            clock=rig.clock,
        )
        await asyncio.sleep(0.3)  # let the loop refuse + snooze it
        final = await rig.backend.get(row.id)
        executed = any(
            t.get("kind") == "job_succeeded" and t.get("actor") == "etl_small"
            for t in rig.worker.trace
        )
        drifted_schema = manifest["actors"][0]["payload_schema"]
        registered_schema = FOREIGN_SCHEMAS["etl_small"].payload_schema
        await save_trace(
            "07_schema_drift",
            "runtime manifest carries a drifted payload schema for etl_small "
            "(label required + rows upper bound removed) → canonical-JSON "
            "sha256 mismatch vs the pre-registered registry row → loud "
            "schema_drift_detected error, actor quarantined, dispatch refuses "
            "its jobs (snooze-and-release)",
            rig,
            {
                "startup_quarantined": sorted(rig.worker.quarantined),
                "drift_report": rig.worker.drift_report,
                "registered_hash": FOREIGN_SCHEMAS["etl_small"].payload_hash[:16],
                "manifest_hash": mw.canonical_hash(drifted_schema)[:16],
                "registered_schema": registered_schema,
                "manifest_schema": drifted_schema,
                "job_status": final.status,
                "job_executed": executed,
                "quarantined_snooze_events": [
                    t for t in rig.worker.trace if t["kind"] == "quarantined_snoozed"
                ],
            },
        )
        assert rig.worker.quarantined == {"etl_small"}
        assert not executed
        assert final.status == "scheduled"  # snoozed 10s, never ran
    finally:
        await rig.stop()


# ── Scenario 8: mixed fleet ───────────────────────────────────────────


async def scenario_8() -> None:
    print("8. mixed fleet: native + foreign-hosted + not-hosted actors coexist")
    rig = Rig("mixed_fleet")
    await rig.start()
    try:
        native_job = await enqueue_typed(
            rig.backend,
            actor="native_wordcount",
            queue="native",
            payload=native_wordcount.payload_type(text="the quick brown fox"),
            clock=rig.clock,
        )
        foreign_job = await enqueue_typed(
            rig.backend,
            actor="etl_small",
            queue="etl",
            payload=EtlSmallPayload(rows=42, label="foreign"),
            clock=rig.clock,
        )
        elsewhere_job = await enqueue_typed(
            rig.backend,
            actor="foreign_elsewhere",
            queue="etl",
            payload=FOREIGN_SCHEMAS["foreign_elsewhere"].payload_model(n=1),
            clock=rig.clock,
        )
        await rig.wait_terminal(native_job.id, timeout=10)
        await rig.wait_terminal(foreign_job.id, timeout=10)
        await asyncio.sleep(0.3)  # let the elsewhere job get snoozed once
        first = await rig.backend.get(elsewhere_job.id)
        release_1 = (first.scheduled_at - START).total_seconds()
        # Advance past the release; the run_forever loop promotes it back to
        # pending (scheduled_to_pending tick) and snoozes it again.
        rig.clock.advance(timedelta(seconds=10.5))
        await asyncio.sleep(0.3)
        second = await rig.backend.get(elsewhere_job.id)
        native_final = await rig.backend.get(native_job.id)
        foreign_final = await rig.backend.get(foreign_job.id)
        await save_trace(
            "08_mixed_fleet",
            "one dispatch fleet, two registry entry kinds: native_wordcount "
            "(@actor ActorRef, executed in-process) and etl_small (foreign, "
            "executed on the Node runtime) both succeed; foreign_elsewhere is "
            "registered fleet-wide but not hosted by this runtime → "
            "snooze-and-release 10s, twice",
            rig,
            {
                "native_status": native_final.status,
                "native_result": native_final.result,
                "foreign_status": foreign_final.status,
                "foreign_result": foreign_final.result,
                "not_hosted_first_status": first.status,
                "not_hosted_release_s": release_1,
                "not_hosted_second_status": second.status,
                "unknown_actor_snooze_events": [
                    t for t in rig.worker.trace if t["kind"] == "unknown_actor_snoozed"
                ],
            },
        )
        assert native_final.status == "succeeded"
        assert foreign_final.status == "succeeded"
        assert first.status == "scheduled" and release_1 == 10.0
    finally:
        await rig.stop()


async def main() -> None:
    scenarios = [
        ("01", scenario_1),
        ("02", scenario_2),
        ("03", scenario_3),
        ("04", scenario_4),
        ("05", scenario_5),
        ("06", scenario_6),
        ("07", scenario_7),
        ("08", scenario_8),
    ]
    only = sys.argv[1] if len(sys.argv) > 1 else None
    for tag, fn in scenarios:
        if only and not fn.__name__.endswith(only):
            continue
        await fn()
    print("done.")


if __name__ == "__main__":
    asyncio.run(main())
