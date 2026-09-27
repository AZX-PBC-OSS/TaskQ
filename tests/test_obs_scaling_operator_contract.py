# Why: schema is a fixture-derived test identifier, not user input; the
# rendered statements are $-bound and never interpolate caller data.
# ruff: noqa: S608
"""The scaling-operator metric contract, pinned.

A future dynamic worker-scaling operator scrapes the OTel surface at fleet
scale and makes scaling decisions off it, so the series it reads are a
CONTRACT, not an implementation detail: the gauge set it needs, the label
vocabulary the series share, and the cardinality bounds that keep a scrape
bounded no matter how many queues a deployment mints.

Five pins live here:

1. ``taskq.queue.utilization`` — the one series an operator would otherwise
   COMPUTE client-side from depth ÷ live_workers ÷ actor capacity. The
   computation needs ``actor_config`` (a table the scraper does not have),
   so the leader sampler publishes the ratio as a first-class gauge,
   sampled in the same tick as ``taskq.queue.depth`` and
   ``taskq.queue.live_workers`` so the three join on ``queue`` without
   describing different moments.
2. The label-cardinality bounds as constants: the ``queue`` /
   cron-``actor`` / ``bucket`` caps and their shared ``_other_`` overflow.
   A future label addition that mints unbounded series fails here first.
3. The per-queue leader gauges' boundedness behaviour: fed more distinct
   queues than the cap, each emits exactly ``cap + 1`` series and the
   ``_other_`` series carries the summed remainder (the reported total
   always equals the true total).
4. The sampler's utilization MATH: the exact pathological inputs a fleet
   produces — multiple actors routed to one queue (the capacity sum), a
   worker whose queue has no routed actor (the stranded class), zero live
   workers under positive depth, and a worker FLAPPING live → dead → live
   across ticks (the series must churn, never freeze stale).
5. The sampler's SQL: the utilization gauge's NUMERATOR is the DUE-NOW
   population (``status = 'pending' AND scheduled_at <= statement_timestamp()``),
   NOT the depth gauge's wider pending+scheduled population — a
   future-armed wave must not read as starvation. Both sampler templates
   are pinned textually (no container) and against
   ``fetch_queue_imbalance``'s own ``due``/``cap`` CTEs on a real migrated
   schema (the integration pins): the gauge and the SQL twin must report
   the SAME ratio for the same fleet, omission-for-omission.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from opentelemetry.metrics import CallbackOptions
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq._ids import new_uuid
from taskq.migrate import apply_pending
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.testing.otel import collect_metrics
from taskq.worker._leader_shared import (  # pyright: ignore[reportPrivateUsage]  # Why: the tests pin the sampler's private SQL templates — restating the SQL would let the pin drift from the thing it guards.
    _QUERY_QUEUE_DEPTH_SQL_TEMPLATE,
    _QUERY_QUEUE_DUE_DEPTH_SQL_TEMPLATE,
)
from taskq.worker._leader_sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: as above, for the templates the sweeps module owns.
    _QUERY_QUEUE_ACTOR_CAPACITY_SQL_TEMPLATE,
    _QUERY_QUEUE_LIVE_WORKERS_SQL_TEMPLATE,
    _queue_utilization,
)

_PG_IMAGE = "postgres:18"

_CAP = otel_mod._MAX_QUEUE_LABEL_VALUES  # pyright: ignore[reportPrivateUsage]  # Why: the tests assert behaviour AT the cap; duplicating the constant would let it drift from the thing it guards.


@pytest.fixture
def utilization_reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME)
    monkeypatch.setattr(
        otel_mod,
        "_queue_utilization_gauge",
        meter.create_observable_gauge(
            "taskq.queue.utilization",
            callbacks=[otel_mod._observe_queue_utilization],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback is the point of the test.
        ),
    )
    yield reader
    obs_mod.update_queue_utilization_cache({})


def _points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


# ── 1. taskq.queue.utilization ──────────────────────────────────────────


def test_utilization_gauge_reports_one_series_per_queue(
    utilization_reader: InMemoryMetricReader,
) -> None:
    obs_mod.update_queue_utilization_cache({"default": 0.5, "reports": 1.25})
    reported = {
        str(dp.attributes["queue"]): float(dp.value)
        for dp in _points(utilization_reader, "taskq.queue.utilization")
        if dp.attributes
    }
    assert reported == {"default": 0.5, "reports": 1.25}


def test_utilization_gauge_clears_on_an_empty_sample(
    utilization_reader: InMemoryMetricReader,
) -> None:
    """A queue with zero effective capacity reports NO utilization series
    (the sampler omits it), so an empty cache must clear the series rather
    than freeze it at the last ratio."""
    obs_mod.update_queue_utilization_cache({"default": 0.5})
    assert _points(utilization_reader, "taskq.queue.utilization")
    obs_mod.update_queue_utilization_cache({})
    assert _points(utilization_reader, "taskq.queue.utilization") == []


def test_utilization_gauge_is_capped_like_the_depth_gauge(
    utilization_reader: InMemoryMetricReader,
) -> None:
    """Same partition as taskq.queue.depth / taskq.queue.live_workers: the
    largest ``cap`` queues keep their series, the rest collapse onto ONE
    ``_other_`` series carrying their summed utilization, so the three
    gauges share a label vocabulary and a bound and a PromQL join on
    ``queue`` cannot fan out."""
    ratios = {f"q-{i}": 1.0 for i in range(_CAP + 30)}
    ratios["busy"] = 2.5
    obs_mod.update_queue_utilization_cache(ratios)

    points = _points(utilization_reader, "taskq.queue.utilization")
    reported = {str(dp.attributes["queue"]): float(dp.value) for dp in points if dp.attributes}
    assert len(points) == _CAP + 1
    assert reported["busy"] == 2.5
    assert reported["_other_"] == 31.0  # 131 queues fed, cap admitted (busy among them)
    assert sum(reported.values()) == sum(ratios.values())


# ── 2. The cardinality bounds as constants ──────────────────────────────


def test_label_cap_constants_are_pinned() -> None:
    """The caps ARE the operator contract's cardinality guarantee: 100
    admitted values per capped dimension (the ~100-values-per-dimension
    ceiling the cloud vendors' guidance sets), one shared overflow value.
    Raising one of these silently would break every bounded scrape that
    sized its TSDB off this module's docs — the bump must land here."""
    assert otel_mod._MAX_QUEUE_LABEL_VALUES == 100  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._MAX_ACTOR_LABEL_VALUES == 100  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._MAX_BUCKET_LABEL_VALUES == 100  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._QUEUE_LABEL_OVERFLOW == "_other_"  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._ACTOR_LABEL_OVERFLOW == "_other_"  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._BUCKET_LABEL_OVERFLOW == "_other_"  # pyright: ignore[reportPrivateUsage]


def test_capped_dimensions_overflow_at_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every capped dimension funnels through one admission core: the first
    ``cap`` distinct values are admitted (never evicted), everything past
    the cap collapses onto the fixed overflow value — so the series count
    is hard-bounded at cap + 1 no matter how many distinct values the
    callers mint."""
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    for bounded, values in (
        (otel_mod._bounded_queue, otel_mod._queue_label_values),  # pyright: ignore[reportPrivateUsage]
        (otel_mod._bounded_cron_actor, otel_mod._cron_actor_label_values),  # pyright: ignore[reportPrivateUsage]
        (otel_mod._bounded_bucket, otel_mod._bucket_label_values),  # pyright: ignore[reportPrivateUsage]
    ):
        values.clear()
        admitted = [f"v-{i}" for i in range(_CAP)]
        overflow = [f"past-{i}" for i in range(_CAP + 50)]
        out = {bounded(v) for v in [*admitted, *overflow]}
        assert out == set(admitted) | {"_other_"}, bounded.__name__
        values.clear()


# ── 3. The per-queue leader gauges' boundedness ─────────────────────────


def test_per_queue_leader_gauges_emit_at_most_cap_plus_one_series(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The boundedness guarantee itself, per gauge: an unbounded feed of
    distinct queue names yields at most cap + 1 observations, and the
    overflow series carries the summed remainder so the reported total
    always equals the true total. A future gauge added to this family must
    route through the same partition (_observe_capped_per_queue) — a gauge
    that yields one series per input instead reds here."""
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    counts = {f"queue-{i}": 1 for i in range(_CAP + 50)}

    for update, observe in (
        (obs_mod.update_queue_depth_cache, otel_mod._observe_queue_depth),  # pyright: ignore[reportPrivateUsage]
        (obs_mod.update_queue_live_workers_cache, otel_mod._observe_queue_live_workers),  # pyright: ignore[reportPrivateUsage]
        (obs_mod.update_queue_utilization_cache, otel_mod._observe_queue_utilization),  # pyright: ignore[reportPrivateUsage]
    ):
        update(counts)
        observations = list(observe(CallbackOptions()))  # pyright: ignore[reportPrivateUsage]
        assert len(observations) == _CAP + 1, update.__name__
        reported = {
            str(obs.attributes["queue"]): obs.value for obs in observations if obs.attributes
        }
        assert reported["_other_"] == 50, update.__name__
        assert sum(reported.values()) == len(counts), update.__name__
    # Every cache the loop feeds was rebound above; leave them empty.
    obs_mod.update_queue_depth_cache({})
    obs_mod.update_queue_live_workers_cache({})
    obs_mod.update_queue_utilization_cache({})


# ── 4. The sampler's utilization math, and the fleet's pathological shapes ──


def test_utilization_multi_actor_routing_join_is_exact() -> None:
    """The denominator's capacity term is the SUM of max_concurrent over
    the actors ROUTED to the queue (actor_config.queue), multiplied by the
    live worker count — the same join fetch_queue_imbalance's cap CTE
    makes. Two actors (5 + 3) routed to one queue served by two workers:
    effective capacity 16, and a depth of 16 due rows is exactly one wave
    (utilization 1.0), not 16 waves."""
    utilization = _queue_utilization(depth={"q": 16}, live_workers={"q": 2}, capacity={"q": 8})
    assert utilization == {"q": 1.0}
    utilization = _queue_utilization(depth={"q": 8}, live_workers={"q": 2}, capacity={"q": 8})
    assert utilization == {"q": 0.5}


def test_utilization_zero_live_workers_with_positive_depth_omits_the_series() -> None:
    """The starvation-adjacent shape an operator must never see as a
    number: due work waiting, nobody to claim it. Division by zero would
    read as infinity or crash; a frozen 0.0 would read as idle. The
    sampler OMITS the series — depth > 0 with no utilization series is
    the alarm shape the docs' decision table keys on."""
    utilization = _queue_utilization(depth={"q": 7}, live_workers={}, capacity={"q": 8})
    assert utilization == {}


def test_utilization_zero_capacity_rows_omit_the_series() -> None:
    """The stranded classes: the capacity read returns no row for the
    queue (no routed actor at all), and an explicit zero-capacity row (a
    routed actor capped at 0). Both must omit the series, never emit
    0.0-with-depth or divide by zero."""
    assert _queue_utilization(depth={"q": 7}, live_workers={"q": 2}, capacity={}) == {}
    assert _queue_utilization(depth={"q": 7}, live_workers={"q": 2}, capacity={"q": 0}) == {}


def test_utilization_positive_capacity_with_no_due_depth_has_no_series() -> None:
    """An idle queue (capacity, workers, zero due rows) carries NO
    utilization series — the numerator iteration keys on the depth dict.
    That absence is unambiguous to an operator because the alarm rule is
    `depth > 0 AND no series`: with depth 0 there is nothing to starve."""
    utilization = _queue_utilization(depth={}, live_workers={"q": 2}, capacity={"q": 8})
    assert utilization == {}


def test_utilization_series_churns_when_a_worker_flaps() -> None:
    """A worker flapping live → dead → live across ticks must churn the
    series, not freeze a stale ratio: each tick REPLACES the cache, so a
    dead tick's scrape shows the series ABSENT (the collector marks it
    stale) and the next live tick republishes it. A gauge that retained
    the dead tick's ratio would keep reporting capacity the fleet lost."""
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME)
    gauge = meter.create_observable_gauge(
        "taskq.queue.utilization", callbacks=[otel_mod._observe_queue_utilization]
    )
    try:
        obs_mod.update_queue_utilization_cache(_queue_utilization({"q": 4}, {"q": 2}, {"q": 8}))
        assert _points(reader, "taskq.queue.utilization")  # live: the ratio exports
        obs_mod.update_queue_utilization_cache(_queue_utilization({"q": 4}, {}, {"q": 8}))
        assert _points(reader, "taskq.queue.utilization") == []  # dead: absent, not stale
        obs_mod.update_queue_utilization_cache(_queue_utilization({"q": 4}, {"q": 2}, {"q": 8}))
        returned = {
            str(dp.attributes["queue"]): float(dp.value)
            for dp in _points(reader, "taskq.queue.utilization")
            if dp.attributes
        }
        assert returned == {"q": 0.25}  # live again: the FRESH ratio (4 ÷ (8 x 2)), recomputed
    finally:
        obs_mod.update_queue_utilization_cache({})
        del gauge  # the provider-owned gauge drops with its reader


# ── 5. The sampler's SQL: the due-now numerator and the capacity join ────


def test_due_depth_template_is_the_due_now_population_not_the_depth_gauges() -> None:
    """The utilization numerator's template must be the dispatch index's
    own population — pending AND due — NOT the depth gauge's wider
    pending+scheduled population. A template that reads the IN-list would
    make every future-armed wave (scheduled jobs, cron armings) inflate
    the ratio: an operator would scale for work that cannot be claimed
    yet. Discriminating pins: the status filter is the single pending
    value, the scheduled_at bound is present, and the template is not the
    depth template."""
    sql = " ".join(_QUERY_QUEUE_DUE_DEPTH_SQL_TEMPLATE.split())
    assert "status = 'pending'" in sql, (
        "the utilization numerator must count the DUE-NOW population "
        "(status = 'pending'), not a wider status set"
    )
    assert "scheduled_at <= statement_timestamp()" in sql, (
        "the utilization numerator must be bounded to DUE rows; a "
        "future-armed wave must not read as starvation"
    )
    assert _QUERY_QUEUE_DUE_DEPTH_SQL_TEMPLATE != _QUERY_QUEUE_DEPTH_SQL_TEMPLATE


def test_capacity_template_is_the_twin_s_cap_cte() -> None:
    """The sampler's capacity term must stay the SQL twin's cap CTE
    definition: sum(max_concurrent) over actor_config ROUTED to the
    queue, NULLs excluded (an uncapped actor contributes no cap), grouped
    by the routed queue. A drift here makes the gauge and the
    fetch_queue_imbalance utilization column disagree for the same fleet
    — the one thing the 'same definition' promise forbids."""
    sql = " ".join(_QUERY_QUEUE_ACTOR_CAPACITY_SQL_TEMPLATE.split())
    assert "actor_config" in sql
    assert "sum(max_concurrent)" in sql
    assert "max_concurrent IS NOT NULL" in sql, (
        "the capacity sum must exclude NULL (uncapped) actors exactly "
        "like fetch_queue_imbalance's cap CTE"
    )
    assert "GROUP BY queue" in sql
    # The live-workers term stays the workers.queues unnest join the twin
    # uses — the denominator's other factor.
    live = " ".join(_QUERY_QUEUE_LIVE_WORKERS_SQL_TEMPLATE.split())
    assert "unnest" in live
    assert "workers" in live
    assert "last_seen_at" in live, "the live-workers term must keep its liveness bound"


@pytest.fixture(scope="module")
def plain_dsn() -> Iterator[str]:
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_PG_IMAGE, username="taskq", password="taskq", dbname="taskq"
    ).with_kwargs(labels=creator_labels()) as container:
        yield container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="module")
async def fleet_env(plain_dsn: str) -> AsyncIterator[tuple[Any, str]]:
    """A migrated schema seeded with every pathological capacity shape:

    * ``multi``   — TWO actors routed to it (max_concurrent 5 + 3), two
      live workers serving it: the routing-join exactness (capacity 8,
      effective 16).
    * ``ghost``   — due rows and a live worker, but NO actor_config row
      for the queue: the stranded class (capacity 0, series omitted).
    * ``uncapped``— one actor with max_concurrent NULL (uncapped) and a
      live worker: the capacity sum excludes it, so the series is
      omitted — the docs' caveat on what that absence can mean.
    * ``armed``   — due-now pending rows AND future-armed scheduled rows:
      the due-now/armed discrimination (the numerator must read the due
      set only).
    """
    conn = await asyncpg.connect(plain_dsn)
    schema = "obs_scaling_contract"
    now = datetime.now(UTC)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        async def job(queue: str, actor: str, status: str, due: bool) -> None:
            await conn.execute(
                f"""INSERT INTO {schema}.jobs (
                        id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at
                    ) VALUES ($1, $2, $3, '{{}}'::jsonb, 3, 'transient', $4::{schema}.job_status, $5)""",
                new_uuid(),
                actor,
                queue,
                status,
                now - timedelta(seconds=30) if due else now + timedelta(hours=1),
            )

        # multi: 20 due-now rows across two routed actors — 20 due rows
        # against effective capacity 16 (8 x 2 workers) is the > 1
        # starved reading the twin must agree with.
        for _ in range(20):
            await job("multi", "multi_a", "pending", due=True)
        await conn.execute(
            f"""INSERT INTO {schema}.actor_config (actor, max_concurrent, max_pending, queue)
                VALUES ('multi_a', 5, NULL, 'multi'), ('multi_b', 3, NULL, 'multi')"""
        )
        # ghost: 3 due rows, a worker serving the queue, no actor_config row.
        for _ in range(3):
            await job("ghost", "ghost_actor", "pending", due=True)
        # uncapped: 2 due rows, one uncapped routed actor.
        for _ in range(2):
            await job("uncapped", "uncapped_actor", "pending", due=True)
        await conn.execute(
            f"""INSERT INTO {schema}.actor_config (actor, max_concurrent, max_pending, queue)
                VALUES ('uncapped_actor', NULL, NULL, 'uncapped')"""
        )
        # armed: 4 due-now rows plus 6 future-armed rows on the same queue.
        for _ in range(4):
            await job("armed", "armed_actor", "pending", due=True)
        for _ in range(6):
            await job("armed", "armed_actor", "scheduled", due=False)
        await conn.execute(
            f"""INSERT INTO {schema}.actor_config (actor, max_concurrent, max_pending, queue)
                VALUES ('armed_actor', 2, NULL, 'armed')"""
        )
        # Two live workers: multi gets both, ghost and uncapped one each,
        # and armed one — so the armed queue's utilization computes on
        # both sides from its DUE rows alone (4, never the 6 armed ones).
        for queues in (["multi", "ghost"], ["multi", "uncapped", "armed"]):
            await conn.execute(
                f"""INSERT INTO {schema}.workers
                        (id, hostname, pid, queues, started_at, last_seen_at)
                    VALUES ($1, 'contract-host', 4242, $2, $3, $4)""",
                new_uuid(),
                queues,
                now - timedelta(hours=1),
                now - timedelta(seconds=5),
            )
        yield conn, schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


@pytest.mark.integration
async def test_sampler_reads_report_the_twin_s_ratio_for_the_same_fleet(
    fleet_env: tuple[Any, str],
) -> None:
    """The end-to-end pin: run the sampler's three templates on the
    seeded pathological fleet, compute the gauge cache the way
    ``_queue_depth_loop`` does, and require it to EQUAL
    ``fetch_queue_imbalance``'s utilization column per queue — same
    ratios, same omissions. A drift between the scrape and the SQL twin
    (a changed numerator population, a changed capacity join) reds here
    with the queue names in the diff."""
    conn, schema = fleet_env
    from taskq.insights import fetch_queue_imbalance

    due_rows = await conn.fetch(
        " ".join(_QUERY_QUEUE_DUE_DEPTH_SQL_TEMPLATE.split()).format(schema=schema)
    )
    live_rows = await conn.fetch(
        " ".join(_QUERY_QUEUE_LIVE_WORKERS_SQL_TEMPLATE.split()).format(schema=schema), 30
    )
    cap_rows = await conn.fetch(
        " ".join(_QUERY_QUEUE_ACTOR_CAPACITY_SQL_TEMPLATE.split()).format(schema=schema)
    )
    due_depth = {row["queue"]: int(row["count"]) for row in due_rows}
    live_workers = {row["queue"]: int(row["count"]) for row in live_rows}
    capacity = {row["queue"]: int(row["actor_capacity"]) for row in cap_rows}

    # The numerator DISCRIMINATES due-now from armed: the armed queue's 6
    # future-armed rows are invisible to it (total depth would read 10).
    assert due_depth["armed"] == 4, due_depth
    # The routing join is exact: two actors (5+3) routed to one queue sum to 8.
    assert capacity["multi"] == 8, capacity
    assert capacity.get("ghost") is None, "the stranded class must have no capacity row"
    assert capacity.get("uncapped") is None, "an uncapped actor contributes no capacity row"
    assert live_workers == {"multi": 2, "ghost": 1, "uncapped": 1, "armed": 1}, live_workers

    twin = {row["queue"]: row for row in await fetch_queue_imbalance(conn, schema=schema)}
    gauge = _queue_utilization(due_depth, live_workers, capacity)
    for queue in {*twin, *gauge}:
        expected = twin.get(queue, {}).get("utilization")
        reported = gauge.get(queue)
        assert reported == expected, (
            f"queue {queue!r}: the gauge reports {reported!r} but the SQL "
            f"twin reports {expected!r} — the scrape and the SQL contract "
            "disagree for the same fleet"
        )
    # The omissions line up too, and for the twin's OWN reasons: the
    # stranded class and the uncapped queue have utilization NULL on both
    # sides, and the starved multi queue reads > 1 on both.
    assert "ghost" not in gauge and twin["ghost"]["utilization"] is None
    assert "uncapped" not in gauge and twin["uncapped"]["utilization"] is None
    assert gauge["multi"] > 1 and twin["multi"]["utilization"] > 1
    assert gauge["armed"] == twin["armed"]["utilization"]
