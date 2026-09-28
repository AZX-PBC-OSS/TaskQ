"""Metrics review: cardinality under fleet scale.

The documented contract (observability.md, "Dimension cardinality"): the
queue/bucket/cron-actor label caps admit the first 100 distinct values a
process sees and collapse every later value onto the fixed ``_other_``
label, so the exposition's series count is hard-bounded at 101 per
capped metric no matter how many names the deployment mints; the
leader-sampled queue gauges rank by VALUE and collapse everything past
the 100 deepest onto one ``_other_`` observation carrying the SUMMED
overflow, so the fleet-wide total stays exact.

This suite drives the real emitters at a 50-queue/100-actor fleet shape
(plus the past-the-cap 120-queue / 120-bucket / 120-cron-actor shapes)
through a real provider→bridge→exposition path and asserts the contract
on the SERVED TEXT: bounded series counts, the ``_other_`` label present,
and the gauge overflow carrying the summed depth.
"""

from __future__ import annotations

import subprocess
import sys
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("opentelemetry.exporter.prometheus")

from tests._prom_review import (  # pyright: ignore[reportPrivateUsage]  # Why: the migration helper is the review harness's own; importing it keeps one migration path.
    _migrate_schema,
    parse_exposition,
    probe_env,
)

pytestmark = [pytest.mark.integration, pytest.mark.otel]

_SUBPROCESS = '''
"""Fleet-shape cardinality probe: drive the real capped emitters at
50-queue/100-actor scale and past the cap, then dump the bridge's
exposition text."""

import asyncio
import os
import sys

PROBE_DIR = os.environ["PROBE_DIR"]
sys.path.insert(0, PROBE_DIR)

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import CollectorRegistry, generate_latest

# The cap contract is per-PROCESS state: an isolated registry + provider,
# wired before anything records (the same wiring the bridge router
# performs; here explicit because the probe drives the emitters directly).
REGISTRY = CollectorRegistry()
READER = PrometheusMetricReader(registry=REGISTRY)
metrics.set_meter_provider(MeterProvider(metric_readers=[READER]))

from taskq.obs._otel import (  # noqa: E402
    _bounded_bucket,
    _bounded_cron_actor,
    _bounded_queue,
    record_consumed_message,
    record_cron_failure,
    record_dispatch_duration,
    record_published_message,
    record_ratelimit_refund_failure,
    update_queue_depth_cache,
    update_queue_live_workers_cache,
)


async def _run() -> None:
    # The fleet shape: 50 queues, 100 actors, and - past the cap - 120
    # distinct queue / bucket / cron-actor names to force the overflow.
    for q in range(50):
        for a in range(2):
            record_published_message(f"fleet_actor_{a:03d}", f"fleet_queue_{q:03d}")
    for q in range(50):
        record_dispatch_duration(f"fleet_queue_{q:03d}", 0.01)
        for outcome in ("succeeded", "failed", "scheduled"):
            record_consumed_message(
                f"fleet_actor_{q % 100:03d}", f"fleet_queue_{q:03d}", outcome=outcome
            )

    # Past the cap: 120 distinct queue names through the capped emitters.
    for q in range(120):
        record_published_message("overflow_actor", f"overflow_queue_{q:03d}")
        record_dispatch_duration(f"overflow_queue_{q:03d}", 0.02)
        record_consumed_message(
            "overflow_actor", f"overflow_queue_{q:03d}", outcome="succeeded"
        )

    # Past the cap on the other capped labels: refund-failure buckets and
    # cron actors.
    for b in range(120):
        record_ratelimit_refund_failure(
            bucket=f"bucket_{b:03d}", backend="redis", error_type="ConnectionError"
        )
    for a in range(120):
        record_cron_failure(f"cron_actor_{a:03d}", 1)

    # The gauge cap: the leader's cache with 150 queues (the sampler
    # ranks by VALUE, keeps the 100 deepest, sums the overflow onto
    # `_other_`), then the live-workers cache with the same shape.
    depths = {f"gauge_queue_{i:03d}": (i % 10) + 1 for i in range(150)}
    update_queue_depth_cache(depths)
    update_queue_live_workers_cache({name: 1 for name in list(depths)[:40]})

    # Sanity the caps expose the real admitted/overflow partition at the
    # label-mapping layer (the served-text assertions below audit the
    # exposition itself):
    assert _bounded_queue("overflow_queue_000") == "overflow_queue_000"  # first 100 keep names
    assert _bounded_queue("overflow_queue_999") == "_other_"
    assert _bounded_bucket("bucket_999") == "_other_"
    assert _bounded_cron_actor("cron_actor_999") == "_other_"

    text = generate_latest(REGISTRY).decode()
    with open(os.environ["PROBE_SCRAPE_PATH"], "w") as fh:
        fh.write(text)
    print("FLEET_SCRAPE_BYTES:", len(text), flush=True)


asyncio.run(_run())
'''


@pytest.fixture(scope="module")
def fleet_exposition(tmp_path_factory: Any) -> Any:
    import subprocess

    workdir = tmp_path_factory.mktemp("prom_fleet_probe")
    script = workdir / "probe_fleet.py"
    script.write_text(_SUBPROCESS)
    scrape_path = workdir / "fleet_scrape.txt"
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell.
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=120,
        env=probe_env(PROBE_DIR=str(workdir), PROBE_SCRAPE_PATH=str(scrape_path)),
    )
    assert result.returncode == 0, (
        f"fleet probe failed:\nstdout={result.stdout[-3000:]}\nstderr={result.stderr[-3000:]}"
    )
    return parse_exposition(scrape_path.read_text())


def _queue_series(exp: Any, name: str) -> dict[str, float]:
    return {
        sample.labels.get("queue", ""): sample.value
        for sample in exp.series(name)
        if "queue" in sample.labels
    }


class TestJobSideCapContract:
    """The job-side emitters: first 100 queue names keep series, the rest
    collapse onto `_other_` - hard-bounded at 101 per metric."""

    def test_published_series_bounded_at_101_with_overflow(self, fleet_exposition: Any) -> None:
        published = fleet_exposition.series("messaging_client_published_messages_total")
        assert published, "published counter absent from the fleet scrape"
        queues = _queue_series(fleet_exposition, "messaging_client_published_messages_total")
        # 50 fleet + 100 admitted overflow + _other_ ... the cap admits
        # the FIRST 100 distinct names the process saw: the 50 fleet
        # queues plus the first 50 overflow queues; overflow_queue_050+
        # collapse onto `_other_`.
        assert len(queues) <= 101, (
            f"the documented hard bound is 101 queue series per metric, got {len(queues)}"
        )
        assert "_other_" in queues, (
            "past the cap the overflow must collapse onto the fixed _other_ "
            f"label: {sorted(queues)[:5]}..."
        )
        assert queues["_other_"] == 70.0, (
            "the _other_ series must carry the summed overflow (70 collapsed "
            f"names x 1 publish each): {queues['_other_']}"
        )
        assert queues.get("overflow_queue_049") == 1.0, (
            "the 100th admitted name must keep its own series"
        )
        assert "overflow_queue_050" not in queues, (
            "the 101st distinct name must NOT keep its own series"
        )

    def test_dispatch_duration_bounded_at_101(self, fleet_exposition: Any) -> None:
        queues = _queue_series(fleet_exposition, "taskq_dispatch_duration_seconds_sum")
        assert queues, "dispatch duration absent from the fleet scrape"
        assert len(queues) <= 101, f"dispatch duration exceeds the 101-series bound: {len(queues)}"
        assert "_other_" in queues

    def test_consumed_messages_bounded_at_101(self, fleet_exposition: Any) -> None:
        # consumed carries outcome too: 101 queues x 3 outcomes is the
        # documented bound's product, but the QUEUE dimension itself must
        # stay at <= 101 distinct values.
        distinct = {
            sample.labels.get("queue", "")
            for sample in fleet_exposition.series("messaging_client_consumed_messages_total")
        }
        assert len(distinct) <= 101, f"queue cardinality blew the cap: {len(distinct)}"
        assert "_other_" in distinct


class TestOtherCappedLabels:
    def test_refund_failure_buckets_collapse_onto_other(self, fleet_exposition: Any) -> None:
        series = fleet_exposition.series("taskq_ratelimit_refund_failures_total")
        assert series, "refund-failures counter absent"
        buckets = {sample.labels.get("bucket") for sample in series}
        assert "_other_" in buckets, buckets
        assert len(buckets) <= 101, len(buckets)

    def test_cron_failure_actors_collapse_onto_other(self, fleet_exposition: Any) -> None:
        # The up-down counter serves without a _total suffix.
        series = fleet_exposition.series("taskq_cron_consecutive_failures")
        assert series, "the cron consecutive-failures up-down counter is absent"
        actors = {sample.labels.get("actor") for sample in series}
        assert "_other_" in actors, actors
        assert len(actors) <= 101, len(actors)


class TestGaugeCapContract:
    """The leader-sampled queue gauges rank by VALUE: the 100 deepest
    queues keep series, the overflow sums onto one `_other_`."""

    def test_depth_gauge_bounded_and_overflow_summed(self, fleet_exposition: Any) -> None:
        queues = _queue_series(fleet_exposition, "taskq_queue_depth")
        assert queues, "queue depth gauge absent from the fleet scrape"
        assert len(queues) <= 101, (
            f"the depth gauge must stay bounded at 101 series (100 deepest + "
            f"_other_), got {len(queues)}"
        )
        assert "_other_" in queues
        # The documented partition, recomputed from the fed cache shape:
        # the 100 DEEPEST queues keep their series (ties broken by queue
        # name), the 50 shallowest collapse onto one _other_ observation
        # carrying their SUMMED depth, so the fleet total stays exact.
        depths = {f"gauge_queue_{i:03d}": (i % 10) + 1 for i in range(150)}
        ranked = sorted(depths.items(), key=lambda kv: (-kv[1], kv[0]))
        expected_overflow = sum(v for _, v in ranked[100:])
        assert queues["_other_"] == expected_overflow, (
            f"_other_ must carry the summed overflow depth ({expected_overflow}), "
            f"got {queues['_other_']}"
        )
        named = {q: v for q, v in queues.items() if q != "_other_"}
        assert named == dict(ranked[:100]), (
            "the gauge cap must keep the 100 deepest queues (ties by name) individually visible"
        )

    def test_live_workers_gauge_same_cap_same_partition(self, fleet_exposition: Any) -> None:
        queues = _queue_series(fleet_exposition, "taskq_queue_live_workers")
        assert queues, "live-workers gauge absent"
        assert len(queues) <= 101, len(queues)
        # 40 queues x 1 worker, all below the cap: no overflow bucket.
        assert "_other_" not in queues, (
            "an under-cap cache must not mint an _other_ series: every "
            "member keeps its name and the total is exact without it"
        )
        assert sum(queues.values()) == 40.0

    def test_fleet_shape_series_count_is_bounded(self, fleet_exposition: Any) -> None:
        """The whole-exposition contract at the 50-queue/100-actor shape
        plus past-cap stress: no taskq/messaging family carries more than
        101 distinct queue/bucket/actor label values (the documented cap)
        or 101 series for the capped gauge families, so a fleet of any
        size scrapes a bounded page."""
        for name in fleet_exposition.names():
            if not name.startswith(("taskq_", "messaging_")):
                continue
            samples = fleet_exposition.series(name)
            for label in ("queue", "bucket", "actor"):
                values = {s.labels[label] for s in samples if label in s.labels}
                assert len(values) <= 101, (
                    f"family {name} serves {len(values)} distinct {label} "
                    "values at fleet shape - past the documented cap of 100 "
                    "admitted names plus the fixed _other_"
                )
            if "_bucket" not in name and "_sum" not in name and "_count" not in name:
                # The gauge families (queue-labeled, no actor dimension)
                # are hard-bounded at 101 series. The job-side counters
                # multiply the queue cap by the ACTOR dimension, which the
                # docs keep unbounded on those emitters ("actor remains
                # user-defined and unbounded: keep actor names a bounded
                # enum") - the enforced contract is the queue-value cap.
                has_actor = any("actor" in s.labels for s in samples)
                if not has_actor:
                    assert len(samples) <= 101, (
                        f"gauge family {name} serves {len(samples)} series "
                        "at fleet shape - past the bounded-cardinality contract"
                    )


# ── the REAL sampler, end to end ────────────────────────────────────
#
# The capped-gauge contract above feeds the cache by hand. The sampler the
# leader actually runs is ``_queue_depth_loop``: a SQL GROUP BY over the
# jobs table, fed through ``update_queue_depth_cache`` every tick. This
# leg drives that REAL read - the shipped SQL template over a real
# migrated schema seeded with 150 distinct queue values - into the real
# cache write and asserts the SERVED exposition's series count, so the
# SQL's row shape (queue names straight out of the jobs table) is proven
# to enter the cap unchanged.

_SAMPLER_SUBPROCESS = '''
"""Feeds the REAL sampler's read result through the real cache write and
dumps the bridge's served exposition."""

import asyncio
import json
import os
import sys

PROBE_DIR = os.environ["PROBE_DIR"]
SAMPLER_CACHE_PATH = os.environ["SAMPLER_CACHE_PATH"]
sys.path.insert(0, PROBE_DIR)

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import CollectorRegistry, generate_latest

REGISTRY = CollectorRegistry()
READER = PrometheusMetricReader(registry=REGISTRY)
metrics.set_meter_provider(MeterProvider(metric_readers=[READER]))

from taskq.obs._otel import update_queue_depth_cache  # noqa: E402


async def _run() -> None:
    with open(SAMPLER_CACHE_PATH) as fh:
        sampled = json.load(fh)
    # The cache write is exactly what _queue_depth_loop does with its rows.
    update_queue_depth_cache(sampled)
    text = generate_latest(REGISTRY).decode()
    with open(os.environ["PROBE_SCRAPE_PATH"], "w") as fh:
        fh.write(text)
    print("SAMPLER_SCRAPE_BYTES:", len(text), flush=True)


asyncio.run(_run())
'''


def test_real_queue_depth_sampler_serves_a_bounded_exposition(
    pg_dsn: str, module_pg_schema: Any, tmp_path_factory: Any
) -> None:
    """150 distinct queues through the leader's REAL depth-sampler read
    (the shipped SQL over a migrated schema) → the real cache write → the
    SERVED exposition: exactly 101 series (the 100 deepest + `_other_`),
    `_other_` carrying the summed overflow, the fleet total exact."""
    import asyncio
    import json

    from taskq._ids import new_uuid

    schema = module_pg_schema.schema_name
    _migrate_schema(pg_dsn, schema)

    # Seed 150 distinct queue values straight into the jobs table - the
    # sampler's GROUP BY sees exactly what production sees: queue names
    # out of live rows. Queue i gets (i % 10) + 1 pending rows.
    depths = {f"sampler_queue_{i:03d}": (i % 10) + 1 for i in range(150)}
    workdir = tmp_path_factory.mktemp("prom_sampler_probe")
    seed_rows = [
        [str(new_uuid()), "sampler_actor", queue, str(count)]
        for queue, count in depths.items()
        for _ in range(count)
    ]
    seed_path = workdir / "sampler_seed.json"
    seed_path.write_text(json.dumps(seed_rows))
    seed_script = workdir / "probe_seed.py"
    seed_script.write_text(
        '''
"""Seeds the sampler's fleet: 150 distinct queue values as pending jobs rows."""

import asyncio
import json
import os
import sys

sys.path.insert(0, os.environ["PROBE_DIR"])


async def _run() -> None:
    import asyncpg

    with open(os.environ["SAMPLER_SEED_PATH"]) as fh:
        rows = json.load(fh)
    conn = await asyncpg.connect(os.environ["PROBE_PG_DSN"])
    try:
        await conn.executemany(
            f'INSERT INTO "{os.environ["PROBE_SCHEMA"]}".jobs '
            "(id, actor, queue, payload, max_attempts, retry_kind) "
            "VALUES ($1, $2, $3, '{}'::jsonb, 1, 'transient')",
            [(r[0], r[1], r[2]) for r in rows],
        )
    finally:
        await conn.close()
    print("SEEDED:", len(rows), flush=True)


asyncio.run(_run())
'''
    )
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell.
        [sys.executable, str(seed_script)],
        capture_output=True,
        text=True,
        timeout=120,
        env=probe_env(
            PROBE_DIR=str(workdir),
            PROBE_PG_DSN=pg_dsn,
            PROBE_SCHEMA=schema,
            SAMPLER_SEED_PATH=str(seed_path),
        ),
    )
    assert result.returncode == 0, f"seeding failed: {result.stderr[-2000:]}"

    # The REAL sampler read: the shipped SQL template, exactly as
    # _queue_depth_loop runs it every tick.
    from taskq.worker._leader_shared import _QUERY_QUEUE_DEPTH_SQL_TEMPLATE

    async def _sample() -> dict[str, int]:
        import asyncpg

        conn = await asyncpg.connect(pg_dsn)
        try:
            db_rows = await conn.fetch(_QUERY_QUEUE_DEPTH_SQL_TEMPLATE.format(schema=schema))
        finally:
            await conn.close()
        return {row["queue"]: row["count"] for row in db_rows}

    sampled = asyncio.run(_sample())
    assert len(sampled) == 150, (
        f"the sampler read saw {len(sampled)} queues, expected 150: the seed "
        "or the shipped SQL drifted"
    )

    script = workdir / "probe_sampler.py"
    script.write_text(_SAMPLER_SUBPROCESS)
    cache_path = workdir / "sampler_cache.json"
    cache_path.write_text(json.dumps(sampled))
    scrape_path = workdir / "sampler_scrape.txt"
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell.
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=120,
        env=probe_env(
            PROBE_DIR=str(workdir),
            SAMPLER_CACHE_PATH=str(cache_path),
            PROBE_SCRAPE_PATH=str(scrape_path),
        ),
    )
    assert result.returncode == 0, (
        f"sampler probe failed:\nstdout={result.stdout[-3000:]}\nstderr={result.stderr[-3000:]}"
    )
    served = parse_exposition(scrape_path.read_text())
    queues = _queue_series(served, "taskq_queue_depth")
    assert len(queues) == 101, (
        f"150 real queues served {len(queues)} depth series - the served "
        "exposition must be hard-bounded at 101 (100 deepest + _other_)"
    )
    ranked = sorted(depths.items(), key=lambda kv: (-kv[1], kv[0]))
    named = {q: v for q, v in queues.items() if q != "_other_"}
    assert named == dict(ranked[:100]), (
        "the served exposition must keep exactly the 100 deepest queues "
        "(ties by name) the REAL sampler read"
    )
    expected_overflow = sum(v for _, v in ranked[100:])
    assert queues["_other_"] == expected_overflow, (
        f"_other_ must carry the summed overflow of the 50 shallowest queues "
        f"({expected_overflow}), got {queues['_other_']}"
    )
    assert sum(queues.values()) == sum(depths.values()), (
        "the fleet total must stay exact through the cap"
    )
