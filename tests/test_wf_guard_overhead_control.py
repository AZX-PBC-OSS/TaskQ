"""The contract §9 control 15: the SHIPPED exact-park guard trigger's
overhead through the real ``sweep_scheduled_to_pending`` on identical
vanilla scheduled-row populations, paired alternating base/guard order on
the same real-PG instance.

No post-hoc wall-clock threshold and no selected best run: per-shape raw
samples, median, p95, min/max, ms/call, µs/row, and the guard-minus-base
delta are printed; the shapes' variance is reported, never summarized as a
universal percentage. The same control asserts the guard's BEHAVIOR: an
exact-park old row without a fitted reservation physically refuses
`scheduled -> pending`, and a winning reserved promotion succeeds."""

from __future__ import annotations

import json as _json
import statistics

import pytest

pytestmark = pytest.mark.integration

_SHAPES = (1_000, 10_000, 50_000)
_PAIRS = {1_000: 9, 10_000: 7, 50_000: 5}


def new_uuid_str() -> str:
    from taskq._ids import new_uuid

    return str(new_uuid())


async def _migrated_schema(pg_dsn: str) -> str:
    import asyncpg

    from taskq.migrate import apply_pending

    schema = f"c15_{new_uuid_str().replace('-', '')[:16]}"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    return schema


async def _drop_schema(pg_dsn: str, schema: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


async def _seed_vanilla_scheduled(pg_dsn: str, schema: str, count: int) -> None:
    import asyncio

    import asyncpg

    conn = await asyncpg.connect(pg_dsn)
    try:
        rows = [(new_uuid_str(), f"c15-{index}") for index in range(count)]
        await conn.executemany(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, max_attempts, retry_kind, "
            "step_key, metadata, scheduled_at, fairness_key) "
            "VALUES ($1, 'c15-actor', 'c15-queue', '{}', 'scheduled', 3, "
            "'transient', 'work', $2::jsonb, clock_timestamp(), $3)",
            [(job_id, _json.dumps({}), flow) for job_id, flow in rows],
        )
        await conn.execute(f'ANALYZE "{schema}".jobs')
    finally:
        await conn.close()
    _ = asyncio


async def _run_sweep_once(pg_dsn: str, schema: str, batch_size: int) -> float:
    import time

    import asyncpg

    from taskq.backend._sweeps import sweep_scheduled_to_pending

    conn = await asyncpg.connect(pg_dsn)
    start = time.monotonic()
    try:
        promoted = await sweep_scheduled_to_pending(
            conn, schema=schema, batch_size=batch_size, statement_timeout_ms=120_000
        )
    finally:
        await conn.close()
    elapsed = (time.monotonic() - start) * 1000.0
    assert promoted >= 0
    return elapsed


async def _reset_rows(pg_dsn: str, schema: str, batch_size: int) -> None:
    """Reset every promoted row back to scheduled (the paired alternation's
    own reset)."""
    import asyncpg

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'scheduled' WHERE status = 'pending'"
        )
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.load_sensitive
async def test_shipped_guard_overhead_through_the_real_sweep(pg_dsn: str) -> None:
    """Paired alternating base/guard order on the same instance: the guard
    schema adds the SHIPPED 42_01 trigger (every migrated schema carries
    it); the BASE pair is the same schema with the trigger dropped. Per
    shape: a discarded warmup, then the paired samples; raw values,
    median, p95, min/max, ms/call and µs/row, and the guard-minus-base
    delta."""
    base_schema = await _migrated_schema(pg_dsn)
    guard_schema = await _migrated_schema(pg_dsn)
    try:
        # The BASE pair: drop the shipped guard (the schema without the
        # trigger is the base).
        import asyncpg

        conn = await asyncpg.connect(pg_dsn)
        await conn.execute(f'DROP TRIGGER IF EXISTS wf_emit_park_guard ON "{base_schema}".jobs')
        await conn.close()

        for shape in _SHAPES:
            await _seed_vanilla_scheduled(pg_dsn, base_schema, shape)
            await _seed_vanilla_scheduled(pg_dsn, guard_schema, shape)
            base_samples: list[float] = []
            guard_samples: list[float] = []
            # One discarded warmup per shape.
            await _run_sweep_once(pg_dsn, base_schema, shape)
            await _reset_rows(pg_dsn, base_schema, shape)
            await _run_sweep_once(pg_dsn, guard_schema, shape)
            await _reset_rows(pg_dsn, guard_schema, shape)
            for _ in range(_PAIRS[shape]):
                base_samples.append(await _run_sweep_once(pg_dsn, base_schema, shape))
                await _reset_rows(pg_dsn, base_schema, shape)
                guard_samples.append(await _run_sweep_once(pg_dsn, guard_schema, shape))
                await _reset_rows(pg_dsn, guard_schema, shape)

            def summarize(samples: list[float], shape: int = shape) -> dict[str, float]:
                ordered = sorted(samples)
                p95_index = min(len(ordered) - 1, int(0.95 * len(ordered)))
                return {
                    "median_ms": statistics.median(ordered),
                    "p95_ms": ordered[p95_index],
                    "min_ms": ordered[0],
                    "max_ms": ordered[-1],
                    "ms_per_call": statistics.mean(ordered),
                    "us_per_row": statistics.mean(ordered) * 1000.0 / shape,
                }

            base_summary = summarize(base_samples)
            guard_summary = summarize(guard_samples)
            delta_median = guard_summary["median_ms"] - base_summary["median_ms"]
            report = {
                "shape": shape,
                "pairs": _PAIRS[shape],
                "base": base_summary,
                "guard": guard_summary,
                "delta_median_ms": delta_median,
                "delta_us_per_row": delta_median * 1000.0 / shape,
            }
            print("GUARD-OVERHEAD " + _json.dumps(report, sort_keys=True))
            # The reset's own law: every promoted row returned to scheduled.
            import asyncpg

            conn = await asyncpg.connect(pg_dsn)
            pend = await conn.fetchval(
                f"SELECT count(*) FROM \"{guard_schema}\".jobs WHERE status = 'pending'"
            )
            await conn.close()
            assert int(pend) == 0, pend

        # ── THE GUARD'S BEHAVIOR (a cheaper no-op guard cannot pass) ──
        conn = await asyncpg.connect(pg_dsn)
        # An exact-park old row WITHOUT a fitted reservation refuses the
        # scheduled -> pending transition physically.
        park_id = new_uuid_str()
        flow = new_uuid_str()
        await conn.execute(
            f'INSERT INTO "{guard_schema}".jobs '
            "(id, actor, queue, payload, status, max_attempts, retry_kind, "
            "step_key, metadata, scheduled_at, fairness_key) "
            "VALUES ($1, 'a', 'q', '{}', 'scheduled', 3, 'transient', 'src', "
            "$2::jsonb, clock_timestamp(), $3)",
            park_id,
            _json.dumps(
                {
                    "flow_id": flow,
                    "blocking_reason": "backpressure",
                    "wf_deferral": {
                        "signal": "EmitBackpressure",
                        "consume_budget": False,
                        "attempt": 0,
                    },
                    "wf_emit_backpressure": {"version": 1, "page_width": 1, "bound": 2},
                }
            ),
            flow,
        )
        with pytest.raises(Exception) as excinfo:
            await conn.execute(
                f"UPDATE \"{guard_schema}\".jobs SET status = 'pending' WHERE id = $1",
                park_id,
            )
        assert "parked" in str(excinfo.value) or "WF002" in str(excinfo.value), excinfo.value
        still = await conn.fetchval(
            f'SELECT status::text FROM "{guard_schema}".jobs WHERE id = $1', park_id
        )
        assert still == "scheduled", still
        # The winning reserved promotion (the same UPDATE carrying the
        # reservation) succeeds.
        await conn.execute(
            f"UPDATE \"{guard_schema}\".jobs SET status = 'pending', "
            "metadata = (metadata - 'blocking_reason') "
            "|| $2::jsonb WHERE id = $1",
            park_id,
            _json.dumps({"wf_emit_reservation": {"version": 1, "width": 1}}),
        )
        promoted = await conn.fetchval(
            f'SELECT status::text FROM "{guard_schema}".jobs WHERE id = $1', park_id
        )
        assert promoted == "pending", promoted
        await conn.close()
    finally:
        await _drop_schema(pg_dsn, base_schema)
        await _drop_schema(pg_dsn, guard_schema)
