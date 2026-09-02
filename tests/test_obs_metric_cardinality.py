"""Identity-like values must never be metric dimensions.

Azure Monitor caps custom metrics at 50,000 active time series per subscription
per region, allows 10 dimension keys per metric, and documents "fewer than 100
valid values" per dimension as the best practice (300 is a grey area; above
that, use custom logs instead). An active time series is any unique
combination of metric name, dimension key and dimension value seen in the past
12 hours.

``worker_id`` is a fresh UUID per worker *process*: on Kubernetes every deploy,
restart and autoscale event mints new values, so a ``worker_id`` dimension adds
time series without bound. The consequence is not a bad chart -- it is
throttled ingestion across every custom metric in the subscription, and Azure
does not backfill what was throttled, so it is not repairable after the fact.
The same argument applies to ``schedule_id``.

The assertion is therefore the failure mode itself: recording from several
distinct identities must produce ONE time series, not one per identity. Worker
and schedule attribution is not lost -- it lives on spans and log lines, where
cardinality is free (``taskq.worker_id`` on the cron-fire span; ``worker_id``
bound via contextvars onto every log line; ``schedule_id`` on the cron-fire and
auto-disable logs).
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from opentelemetry.metrics import CallbackOptions
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq._ids import new_uuid

#: ``(module attribute, metric name, instrument kind, record one sample for
#: this identity)`` for every instrument whose only dimension was an identity
#: value.
_CASES: list[tuple[str, str, str, Callable[[str], None]]] = [
    (
        "_lock_expires_in_seconds",
        "taskq.lock.expires_in_seconds",
        "histogram",
        lambda ident: obs_mod.record_lock_expires_in_seconds(ident, 30.0),
    ),
    (
        "_heartbeat_misses",
        "taskq.heartbeat.misses",
        "counter",
        obs_mod.record_heartbeat_miss,
    ),
    (
        "_leader_election_attempts",
        "taskq.leader.election_attempts",
        "counter",
        lambda ident: obs_mod.record_election_attempt(ident, won=True),
    ),
    (
        "_leader_election_failures",
        "taskq.leader.election_failures",
        "counter",
        lambda ident: obs_mod.record_election_attempt(ident, won=False),
    ),
    (
        "_cron_lock_contention",
        "taskq.cron.lock_contention",
        "counter",
        obs_mod.record_cron_lock_contention,
    ),
    (
        "_cron_consecutive_failures",
        "taskq.cron.consecutive_failures",
        "counter",
        lambda ident: obs_mod.record_cron_failure(ident, 1),
    ),
]


def _data_points(reader: InMemoryMetricReader, name: str) -> list[object]:
    data = reader.get_metrics_data()
    assert data is not None
    return [
        point
        for resource_metrics in data.resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
        if metric.name == name
        for point in metric.data.data_points  # type: ignore[attr-defined]  # Why: every instrument under test is a counter or histogram; both data types expose data_points.
    ]


@pytest.mark.parametrize(
    ("attr", "name", "kind", "record"), _CASES, ids=[case[1] for case in _CASES]
)
def test_distinct_identities_do_not_mint_new_time_series(
    attr: str,
    name: str,
    kind: str,
    record: Callable[[str], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter("taskq-cardinality-test")
    # Every instrument in the table, not just the one under test: some
    # recorders touch more than one (a lost election bumps attempts AND
    # failures), and the module-level singletons carry whatever an earlier
    # test left on them.
    for case_attr, case_name, case_kind, _ in _CASES:
        monkeypatch.setattr(
            otel_mod,
            case_attr,
            meter.create_histogram(case_name)
            if case_kind == "histogram"
            else meter.create_counter(case_name),
        )
    otel_mod.set_otel_enabled(True)

    identities = [str(new_uuid()) for _ in range(3)]
    for identity in identities:
        record(identity)

    points = _data_points(reader, name)
    assert len(points) == 1, (
        f"{name} minted {len(points)} time series for {len(identities)} identities; "
        "each restart/deploy would add another"
    )
    attributes = dict(getattr(points[0], "attributes", None) or {})
    assert not set(attributes.values()) & set(identities), (
        f"{name} still carries an identity value as a dimension: {attributes}"
    )


def test_heartbeat_consecutive_failures_gauge_has_one_series_per_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The observable gauge kept a per-``worker_id`` cache and yielded one
    Observation per key — the same explosion, one scrape at a time."""
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    identities = [str(new_uuid()) for _ in range(3)]
    for identity in identities:
        otel_mod.update_heartbeat_consecutive_failures(identity, 3)

    observations = list(otel_mod._observe_heartbeat_consecutive_failures(CallbackOptions()))  # pyright: ignore[reportPrivateUsage]  # Why: the callback is the only way to observe a synchronous gauge without a full SDK scrape.

    assert len(observations) == 1
    attributes = dict(observations[0].attributes or {})
    assert not set(attributes.values()) & set(identities)
    assert observations[0].value == 3
