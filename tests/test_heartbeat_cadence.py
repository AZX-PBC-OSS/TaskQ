"""THE HEARTBEAT-CADENCE PIN (the HB-deltas instrument, permanent).

The live finding this pin settles: "every task sits idle yet timers fire
7x late, ALTERNATING". The 2026-10-08 investigation measured the cadence
in FIVE regimes on the probe box and could not reproduce the pathology —
the numbers, every one captured in ``.measurements/``:

| regime | HB max delta | bare-timer max lateness |
|---|---|---|
| quiet, independent process, idle tasks | — | 1ms |
| 32 CPU hogs (full co-tenancy), independent | — | 4ms |
| beside a real -n 8 PG-heavy battery | — | 2ms |
| in-process ``_main``, solo | 1.006x interval | 1.7ms |
| in-process ``_main``, during the battery | 1.006x interval | 1.4ms |
| contended single core (runner+PG+hog on core 0) | 1.01x interval | 1.7ms |

The structural suspects were each probed and cleared:

- THE SCHEDULING MATH: the wait anchors to ``tick_start`` —
  ``remaining = interval - (monotonic() - tick_start)`` — the FOLLOW form
  (each beat's anchor is the last tick's start). A beat can never
  accumulate drift: an overrun tick waits zero and re-enters, and the
  follow-form is the SAFE one for a lease model — a blocked beat never
  produces a compensating double-fire. The no-drift law is pinned below
  by injecting ONE sync block on the loop and asserting the next beat
  returns to ~interval: the block shows up as ONE long delta, never a
  run of shifted ones.
- THE SYNC-PG-CALL SUSPECT: every statement in the tick path is an
  ``await`` (BEGIN, the liveness write, the gated renewal, the
  reservation-lease write, the probes, the hook, COMMIT, the post-tx
  drain) — no sync round trip exists on the loop to block it.
- THE EXPORTER SUSPECT: the OTel wiring goes through the SDK's
  ``_initialize_components`` — batch processors, off-loop threads.
- THE CO-TENANCY SUSPECT: the bare-timer probe beside a real battery and
  under a full-core CPU storm stays within 4ms — the scheduler absorbs
  the co-tenancy; it does not reach an asyncio timer's fire time.

THE INSTRUMENT: the HB records' deltas are tapped at
``record_lock_expires_in_seconds`` — called exactly once per successful
tick after the first (the same samples the lock-expiry alert reads), so
its call times ARE the heartbeat's beat times. The deltas' distribution
answers bimodal/alternating directly.

THE BOUND: the measured envelope's worst beat across all five regimes is
1.01x interval; the pin's bound is 1.2x — 20% headroom over the WORST
measured beat under load, still 5.8x below the reported 7x pathology.
THE ALTERNATING CHECK: a beat past the bound OUTSIDE the pinned block's
window would be the reported pathology's signature; the raw deltas ride
the failure message, so a red names the distribution, not just the
count. The pin carries the load_sensitive marker: its subject is a
wall-clock cadence, so it runs in the exclusive lane (BUILD-PROTOCOL
§7b) where its measurement is trustworthy — exactly where the finding
was made.
"""

import asyncio
import contextlib
import time

from pydantic import BaseModel

from taskq.actor import actor
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker import heartbeat as hb_mod
from taskq.worker._bootstrap import _main

#: The probe interval: fast enough for ~16 beats in the run window, slow
#: enough that a PG round trip (a few ms quiet) stays far under the bound.
_INTERVAL = 0.25
_RUN_SECS = 4.0
#: The cadence bound: 1.2x interval (see the module docstring's sizing).
_CADENCE_BOUND_FRACTION = 1.2
#: The injected block: ONE second of sync work ON the loop (the GIL-held
#: model) — unmistakable at a 0.25s interval, small in the pin's wall clock.
_INJECTED_BLOCK_SECS = 1.0


async def test_heartbeat_cadence_holds_the_bound_and_does_not_drift(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """THE CADENCE BOUND + THE NO-DRIFT LAW, measured on the real loop.

    Two assertions over the same beat record:

    1. THE BOUND: every inter-beat delta outside the injected block's
       window is within 1.2x the interval — red on the 7x-late pathology,
       with the raw deltas in the failure message (the distribution, not
       just the count).
    2. NO DRIFT ACCUMULATION: the beat immediately after the block-widened
       gap is back inside the bound. The follow-form anchor guarantees
       it; a scheduler that accumulated drift (next-fire anchored to the
       WRONG instant, or a catch-up burst) would show a run of shifted
       deltas — exactly the alternating signature — and reds here.
    """
    schema = module_pg_schema.schema_name

    hb_beat_times: list[float] = []
    real_record = hb_mod.record_lock_expires_in_seconds  # pyright: ignore[reportPrivateImportUsage]  # Why: the tap patches the heartbeat module's own binding of the obs instrument - the call site's name, the seam the HB deltas flow through.

    def _tap(worker_id: str, remaining: float) -> None:
        hb_beat_times.append(time.monotonic())
        real_record(worker_id, remaining)

    hb_mod.record_lock_expires_in_seconds = _tap  # pyright: ignore[reportPrivateImportUsage]
    try:

        class _Idle(BaseModel):
            pass

        @actor(name="cadence_probe_actor")
        async def cadence_probe_actor(payload: _Idle) -> None:
            await asyncio.sleep(0)

        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": pg_dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_HEARTBEAT_INTERVAL": str(_INTERVAL),
                "TASKQ_LOCK_LEASE": "60",
                # The loop-lag watchdog is the CURE's tripwire, not this
                # pin's instrument: its budgets stay at their defaults in
                # production. Here the injected block would trip a tight
                # tier-2 into a force exit mid-measurement, and a tier-1
                # dump would drown the probe's own signal — both silenced
                # for the measurement window only.
                "TASKQ_LOOP_LAG_WARN_BUDGET": "9999",
                "TASKQ_LOOP_LAG_BUDGET": "9999",
            },
            validate=False,
        )

        async def _runner() -> None:
            with contextlib.suppress(asyncio.CancelledError):
                await _main(settings, actor_registry={"cadence_probe_actor": cadence_probe_actor})

        # The block's window, in wall clock, recorded BY the blocker.
        block_window: list[float] = []

        async def _probe_loop() -> None:
            # The block lands mid-run: ONE second of sync work in THIS
            # coroutine body — ON the loop, in the loop's own thread. That
            # is the GIL-held model: the loop cannot schedule anything
            # (the heartbeat's timer included) until the block releases.
            await asyncio.sleep(_RUN_SECS / 2)
            start = time.monotonic()
            block_window.append(start)
            deadline = start + _INJECTED_BLOCK_SECS
            while time.monotonic() < deadline:
                pass
            block_window.append(time.monotonic())

        worker_task = asyncio.create_task(_runner())
        noise_task = asyncio.create_task(_probe_loop())
        await asyncio.sleep(_RUN_SECS)
        noise_task.cancel()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
    finally:
        hb_mod.record_lock_expires_in_seconds = real_record  # pyright: ignore[reportPrivateImportUsage]

    deltas = [hb_beat_times[i + 1] - hb_beat_times[i] for i in range(len(hb_beat_times) - 1)]
    assert len(deltas) >= 8, (
        f"the HB tap saw {len(hb_beat_times)} beats in {_RUN_SECS}s at "
        f"{_INTERVAL}s interval — the loop's own cadence collapsed; "
        f"deltas: {deltas}"
    )

    # A delta is BLOCK-OWNED when the beat it spans overlaps the block's
    # recorded window (the tap time pair brackets it).
    block_start = block_window[0] - _INTERVAL if block_window else float("inf")
    block_end = block_window[-1] + 0.25 if block_window else float("-inf")

    def _in_block_window(idx: int) -> bool:
        beat_from = hb_beat_times[idx]
        beat_to = hb_beat_times[idx + 1]
        return beat_to >= block_start and beat_from <= block_end

    bound = _INTERVAL * _CADENCE_BOUND_FRACTION
    late = [(i, d) for i, d in enumerate(deltas) if d > bound]
    late_outside_block = [(i, round(d / _INTERVAL, 2)) for i, d in late if not _in_block_window(i)]
    assert not late_outside_block, (
        f"heartbeat cadence degraded: {len(late_outside_block)} beat(s) beyond "
        f"{_CADENCE_BOUND_FRACTION}x interval OUTSIDE the injected block "
        f"({[x for _, x in late_outside_block]}x) — the reported "
        f"7x-late-alternating class. Raw deltas (x interval): "
        f"{[round(d / _INTERVAL, 2) for d in deltas]}"
    )
    # THE NO-DRIFT LAW: the beat immediately after the block-widened gap
    # is back inside the bound (the anchor followed the tick, the schedule
    # did not stay shifted).
    if late:
        last_late_idx = max(i for i, _ in late)
        if last_late_idx + 1 < len(deltas):
            following = deltas[last_late_idx + 1]
            assert following <= bound, (
                f"drift accumulated: the beat after the {deltas[last_late_idx]:.2f}s "
                f"gap ran at {following / _INTERVAL:.2f}x interval "
                f"(>{_CADENCE_BOUND_FRACTION}x) — the schedule stayed shifted, "
                f"the anchor did not follow the tick. Deltas (x interval): "
                f"{[round(d / _INTERVAL, 2) for d in deltas]}"
            )
