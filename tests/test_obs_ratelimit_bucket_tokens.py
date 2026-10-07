"""Unit pins for ``taskq.ratelimit.bucket_tokens``: the scrape's live
admission state for rate-limit buckets.

The blind spot audited: a bucket's live token state reached only the
admin rate-limits PAGE (its own peek); the scrape showed denial and
refund-failure RATES and never the LEVEL. The cardinality doctrine is
the point of the pins: statically-registered buckets get named series;
a keyed-materialised bucket (``base_name:key``) creates NO per-key
series — its tokens are summed per kind onto ``_other_``.
"""

from collections.abc import Iterator
from datetime import timedelta

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.ratelimit.decision import RateLimitState
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.testing.otel import collect_metrics
from taskq.worker._leader_sweeps import (
    _bucket_tokens_by_kind,  # pyright: ignore[reportPrivateUsage]  # Why: the partition is the pin's subject; importing it is the test.
)


@pytest.fixture
def gauge_reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME)
    monkeypatch.setattr(
        otel_mod,
        "_ratelimit_bucket_tokens_gauge",
        meter.create_observable_gauge(
            "taskq.ratelimit.bucket_tokens",
            callbacks=[otel_mod._observe_ratelimit_bucket_tokens],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback is the point of the test.
        ),
    )
    yield reader
    obs_mod.update_ratelimit_bucket_tokens_cache({})


def _points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


def _registry_with_keyed(name: str, rl: RateLimitRegistry) -> None:
    """Stamp *name* as keyed-materialised the way the acquire path does."""
    rl._keyed_rate_limit_last_used[name] = float("inf")  # pyright: ignore[reportPrivateUsage]  # Why: the recency stamp is the registration the predicate reads; the test seeds it directly.


def _token_state(name: str, tokens: float, capacity: float = 10.0) -> RateLimitState:
    return RateLimitState(
        bucket_name=name,
        backend="redis",
        is_exhausted=tokens <= 0,
        tokens_remaining=tokens,
        capacity=capacity,
        refill_per_second=1.0,
    )


def _window_state(name: str, remaining: float) -> RateLimitState:
    return RateLimitState(
        bucket_name=name,
        backend="redis",
        is_exhausted=remaining <= 0,
        remaining=remaining,
        limit=100,
        window=timedelta(seconds=60),
        style="log",
    )


def _register_prim(rl: RateLimitRegistry, name: str, *, window: bool = False) -> None:
    """Register the primitive under *name* the way peek_all assumes.

    ``peek_all`` only returns states for REGISTERED rate limits (the
    production sampler's population), and the partition resolves each
    state's kind from the registry's own primitive — the test must
    register real ones for the lookup to answer.
    """
    if window:
        from taskq.ratelimit.sliding_window import SlidingWindow

        rl.register(SlidingWindow(name=name, limit=100, window=timedelta(seconds=60)))
    else:
        rl.register(TokenBucket(name=name, capacity=10, refill_per_second=1.0))


def test_static_buckets_get_named_series() -> None:
    """A statically-registered bucket keeps its own (bucket, kind) series."""
    rl = RateLimitRegistry()
    _register_prim(rl, "api")
    _register_prim(rl, "exports", window=True)
    states = {
        "api": _token_state("api", 4.5),
        "exports": _window_state("exports", 7.0),
    }
    series = _bucket_tokens_by_kind(rl, states)
    assert series == {
        ("api", "token_bucket"): 4.5,
        ("exports", "sliding_window_log"): 7.0,
    }


def test_keyed_buckets_aggregate_per_kind_and_never_become_labels() -> None:
    """The cardinality pin: N keyed materialisations contribute ONE
    ``_other_`` series per kind, carrying their SUM. No per-key series
    exists, no matter how many keys the store materialised."""
    rl = RateLimitRegistry()
    keyed_names = [f"tenant-bucket:tenant-{i}" for i in range(50)]
    for n in keyed_names:
        _register_prim(rl, n)
        _registry_with_keyed(n, rl)
    _register_prim(rl, "static")
    states: dict[str, RateLimitState] = {}
    for i, n in enumerate(keyed_names):
        states[n] = _token_state(n, float(i))
    states["static"] = _token_state("static", 9.0)

    series = _bucket_tokens_by_kind(rl, states)

    assert series == {
        ("static", "token_bucket"): 9.0,
        ("_other_", "token_bucket"): float(sum(range(50))),
    }


def test_mixed_keyed_kinds_aggregate_separately() -> None:
    """The keyed aggregate is per kind, so the scalar never mixes a token
    bucket's tokens with a sliding window's remaining admissions."""
    rl = RateLimitRegistry()
    for n in ("tb:1", "tb:2", "sw:1"):
        _register_prim(rl, n, window=n.startswith("sw:"))
        _registry_with_keyed(n, rl)
    states = {
        "tb:1": _token_state("tb:1", 1.0),
        "tb:2": _token_state("tb:2", 2.0),
        "sw:1": _window_state("sw:1", 3.0),
    }
    series = _bucket_tokens_by_kind(rl, states)
    assert series == {
        ("_other_", "token_bucket"): 3.0,
        ("_other_", "sliding_window_log"): 3.0,
    }


def test_gauge_serves_the_partition_as_series(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """The cache's shape IS the series set: the gauge callback yields one
    observation per entry and can never mint a series the sampler did
    not admit."""
    obs_mod.update_ratelimit_bucket_tokens_cache(
        {
            ("api", "token_bucket"): 4.5,
            ("_other_", "token_bucket"): 12.0,
        }
    )
    reported = {
        (
            str(dp.attributes["bucket"]),
            str(dp.attributes["kind"]),
        ): float(dp.value)
        for dp in _points(gauge_reader, "taskq.ratelimit.bucket_tokens")
        if dp.attributes
    }
    assert reported == {
        ("api", "token_bucket"): 4.5,
        ("_other_", "token_bucket"): 12.0,
    }


def test_gauge_clears_on_an_empty_sample(gauge_reader: InMemoryMetricReader) -> None:
    """A demoted leader clears the cache; the series must go stale rather
    than freeze at its last level."""
    obs_mod.update_ratelimit_bucket_tokens_cache({("api", "token_bucket"): 1.0})
    assert _points(gauge_reader, "taskq.ratelimit.bucket_tokens")
    obs_mod.update_ratelimit_bucket_tokens_cache({})
    assert _points(gauge_reader, "taskq.ratelimit.bucket_tokens") == []


def test_is_keyed_rate_limit_distinguishes_static_from_keyed() -> None:
    """The predicate the sampler partitions on: a static registration is
    never keyed, a stamped keyed materialisation is."""
    rl = RateLimitRegistry()
    _register_prim(rl, "api")
    assert rl.is_keyed_rate_limit("api") is False
    _register_prim(rl, "api:tenant-1")
    _registry_with_keyed("api:tenant-1", rl)
    assert rl.is_keyed_rate_limit("api:tenant-1") is True
    assert rl.is_keyed_rate_limit("never-registered") is False
