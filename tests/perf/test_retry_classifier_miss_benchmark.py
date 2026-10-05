"""Miss-path micro-benchmark for the built-in retry classifiers.

The built-in classifiers run for EVERY exception an actor raises (most
exceptions are not rate limits and not taxonomy shapes), so the
not-claimed path is the hot path: it must stay allocation-free
(pinned by ``test_miss_path_allocates_nothing`` in
tests/test_retry_after_header_sniffing.py) and sub-µs per call.

Wall-clock gate on a noise-robust statistic, same doctrine as
tests/perf/test_dispatch_benchmark.py: the BEST round's per-call mean
against a generous budget — min-of-N is the standard robust latency
statistic under load (per-call p50 needs per-call timer reads, whose
overhead at sub-µs scale would dominate the measurement; the mean over
10k calls amortizes it), and the budget only trips on a regression that
changes the cost CLASS (e.g. a header-parse or set-rebuild landing on
the miss path), never runner noise.
"""

import time

import pytest

from taskq.retry import (
    RetryClassifierHook,
    failure_taxonomy_classifier,
    rate_limit_aware_classifier,
)

ROUNDS = 7
WARMUP_PER_ROUND = 1_000
MEASURED_PER_ROUND = 10_000
# Sub-µs class: the measured miss path sits at a fraction of a microsecond
# (see the printed means); the budget leaves an order of magnitude of
# headroom so only a structural regression (an allocation, a header parse,
# an import landing on the miss path) fails the gate.
P50_BUDGET_US = 1.0


def _best_round_mean_us(classifier: RetryClassifierHook, exc: BaseException) -> float:
    round_means: list[float] = []
    for _ in range(ROUNDS):
        for _ in range(WARMUP_PER_ROUND):
            classifier(exc, 1)
        start = time.perf_counter()
        for _ in range(MEASURED_PER_ROUND):
            classifier(exc, 1)
        elapsed = time.perf_counter() - start
        round_means.append(elapsed / MEASURED_PER_ROUND * 1e6)
    return min(round_means)


@pytest.mark.parametrize(
    ("classifier_name", "classifier"),
    [
        ("rate_limit_aware_classifier", rate_limit_aware_classifier),
        ("failure_taxonomy_classifier", failure_taxonomy_classifier()),
    ],
    ids=["rate-limit-miss", "taxonomy-miss"],
)
def test_classifier_miss_path_is_sub_microsecond(
    classifier_name: str, classifier: RetryClassifierHook
) -> None:
    """Gate: best-round per-call mean stays sub-µs for both built-ins'
    miss paths (an unrelated RuntimeError claims nothing)."""
    exc = RuntimeError("unrelated failure")

    mean_us = _best_round_mean_us(classifier, exc)

    print(f"\n{classifier_name} miss-path best-round mean: {mean_us:.3f}µs/call")
    assert mean_us <= P50_BUDGET_US, (
        f"{classifier_name} miss path regressed to {mean_us:.3f}µs/call "
        f"(budget {P50_BUDGET_US}µs): something (an allocation, a header "
        "parse, a set rebuild) landed on the not-claimed path"
    )
