"""Miss-path micro-benchmark for the retry classifiers' composition.

The built-in classifiers run for EVERY exception an actor raises (most
exceptions are not rate limits and not taxonomy shapes), so the
not-claimed path is the hot path: it must stay allocation-free
(pinned by ``test_miss_path_allocates_nothing`` in
tests/test_retry_after_header_sniffing.py) and within its cost class.

HARDWARE-RELATIVE, not absolute. This gate once asserted an absolute
``1.0µs/call`` budget and red on CI (run 37263479699: 2.845µs taxonomy,
2.375µs rate-limit) while the same paths measure 0.17-0.23µs on the
builder's local box, a 12-17x pure-hardware spread at µs scale. An
absolute µs budget pins the calibration machine, not the code — the
"assumes my machine" class the landing doctrine's wall-clock law
forbids. The budgets here are RELATIVE instead: every test measures a
CALIBRATION baseline in the same process, on the same runner, in the
same run, and asserts the gated path's ratio to it. The ratio carries
across hardware (a slower runner scales numerator and denominator
together); the absolute µs figures are printed for the perf-evidence
record and never asserted.

Wall-clock statistics, same doctrine as tests/perf/test_dispatch_benchmark.py:
the BEST round's per-call mean (min-of-N is the standard robust latency
statistic under load), and the budget only trips on a regression that
changes the cost CLASS (e.g. a header-parse or set-rebuild landing on
the miss path), never on runner noise. Marked ``load_sensitive``: a
µs-scale wall-clock gate belongs in the serial lane, where co-tenancy
cannot inflate one side of a ratio more than the other.
"""

import time

import pytest

from taskq.retry import (
    RetryClassifierHook,
    compose_retry_classifiers,
    failure_taxonomy_classifier,
    rate_limit_aware_classifier,
)

ROUNDS = 7
WARMUP_PER_ROUND = 1_000
MEASURED_PER_ROUND = 10_000

#: How many classifiers the benchmark's composed fleet registers: the two
#: built-ins plus three no-opinion classifiers — the composition's
#: motivating shape, and the miss path's worst case (every classifier is
#: consulted, none claims).
FLEET_SIZE = 5

#: Measured bands (this file's own harness, best-round means):
#:
#:   landing box, clean runs (5 consecutive):    ratio band
#:     taxonomy / rate-limit                     1.23 - 2.05
#:     composed(5) / rate-limit                  3.31 - 3.54
#:   landing box, heavy co-tenant contention:    composed/rl up to 6.46
#:
#: The two built-ins do the same class of miss-path work (duck-typed
#: attribute reads, a name check, a membership test), so their legitimate
#: ratio is bounded by ~2x — the budget is the worst measured x ~1.5
#: headroom. The composed loop's structural factor is one pass over
#: FIVE sequential classifiers plus the composition's own overhead
#: (the isolation guards, the first-override-wins branch), so 5x is its
#: honest structural bound; the budget adds the composition-overhead
#: allowance on top of the clean measured max (3.54 x 1.7). A regression
#: that changes the miss path's cost CLASS (an attr chain added per
#: call, a try/except landing) is >= 2x on the composed loop and trips
#: both budgets; runner noise does not reach them. The contention figure
#: (6.46) sits ABOVE the composed budget on purpose: co-tenancy inflates
#: the longer loop more than the calibration call, so a µs-scale ratio
#: gate is only meaningful on a quiet runner — hence ``load_sensitive``
#: (the repo's serial lane) on every test in this file.
TAXONOMY_RATIO_BUDGET = 3.0
COMPOSED_RATIO_BUDGET = 6.0


def _noop_classifier(exc: BaseException, attempt: int) -> None:
    """A registered classifier with no opinion: the composition's
    per-classifier overhead, isolated from the built-ins' own work."""
    return None


#: The benchmark fleet: built-ins first (specificity order), then the
#: no-opinion classifiers that model the rest of a realistic registry.
_COMPOSED_FLEET = compose_retry_classifiers(
    rate_limit_aware_classifier,
    failure_taxonomy_classifier(),
    _noop_classifier,
    _noop_classifier,
    _noop_classifier,
)


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


def _calibration_baseline_us(exc: BaseException) -> tuple[float, float, float]:
    """Measure both built-ins' miss paths and return
    ``(baseline, rate_limit, taxonomy)`` where ``baseline`` is the
    HEALTHIER (cheaper) of the two: the mutual calibration anchor. If
    either built-in's miss path regresses, the other still anchors the
    band, so a regression can never hide inside its own denominator."""
    rate_limit_us = _best_round_mean_us(rate_limit_aware_classifier, exc)
    taxonomy_us = _best_round_mean_us(failure_taxonomy_classifier(), exc)
    return min(rate_limit_us, taxonomy_us), rate_limit_us, taxonomy_us


@pytest.mark.load_sensitive
def test_builtin_miss_paths_stay_in_proportion() -> None:
    """Gate: each built-in's miss path stays within
    ``TAXONOMY_RATIO_BUDGET`` of the same-run calibration baseline —
    the two built-ins cost the same CLASS of work, so one ballooning
    against the other is a regression (a header parse, an attr chain),
    whatever the runner's absolute speed. Absolute µs are recorded,
    never gated."""
    exc = RuntimeError("unrelated failure")

    baseline_us, rate_limit_us, taxonomy_us = _calibration_baseline_us(exc)
    worst_ratio = max(rate_limit_us, taxonomy_us) / baseline_us

    print(
        f"\n── built-in miss paths (hardware-relative, baseline={baseline_us:.3f}µs) ──\n"
        f"  rate_limit_aware_classifier: {rate_limit_us:.3f}µs/call "
        f"({rate_limit_us / baseline_us:.2f}x baseline, budget {TAXONOMY_RATIO_BUDGET}x)\n"
        f"  failure_taxonomy_classifier: {taxonomy_us:.3f}µs/call "
        f"({taxonomy_us / baseline_us:.2f}x baseline, budget {TAXONOMY_RATIO_BUDGET}x)"
    )
    assert worst_ratio <= TAXONOMY_RATIO_BUDGET, (
        f"a built-in miss path regressed relative to its peer: worst ratio "
        f"{worst_ratio:.2f}x > budget {TAXONOMY_RATIO_BUDGET}x "
        f"(rate-limit {rate_limit_us:.3f}µs, taxonomy {taxonomy_us:.3f}µs, "
        f"baseline {baseline_us:.3f}µs). Something (an allocation, a header "
        "parse, a set rebuild) landed on one built-in's not-claimed path — "
        "the ratio is hardware-relative, so this is a code regression, "
        "not a slow runner."
    )


@pytest.mark.load_sensitive
def test_composed_miss_path_scales_with_calibration_baseline() -> None:
    """Gate: the FIVE-classifier composition's miss path stays within
    ``COMPOSED_RATIO_BUDGET`` x the same-run calibration baseline — the
    structural factor (one pass over five sequential classifiers) plus
    the composition overhead, measured in the same process on the same
    runner. Absolute µs are recorded, never gated."""
    exc = RuntimeError("unrelated failure")

    baseline_us, rate_limit_us, taxonomy_us = _calibration_baseline_us(exc)
    composed_us = _best_round_mean_us(_COMPOSED_FLEET, exc)
    ratio = composed_us / baseline_us

    print(
        f"\n── composed miss path (hardware-relative, baseline={baseline_us:.3f}µs) ──\n"
        f"  composed({FLEET_SIZE}) miss: {composed_us:.3f}µs/call "
        f"({ratio:.2f}x baseline, budget {COMPOSED_RATIO_BUDGET}x)\n"
        f"  recorded, not gated: rate-limit {rate_limit_us:.3f}µs, "
        f"taxonomy {taxonomy_us:.3f}µs"
    )
    assert ratio <= COMPOSED_RATIO_BUDGET, (
        f"the composed miss path regressed relative to the calibration "
        f"baseline: {ratio:.2f}x > budget {COMPOSED_RATIO_BUDGET}x "
        f"(composed {composed_us:.3f}µs, baseline {baseline_us:.3f}µs). "
        "Something landed on the composition's loop (an allocation, an "
        "isolation-guard escalation, a logger lookup) — the ratio is "
        "hardware-relative, so this is a code regression, not a slow runner."
    )


@pytest.mark.load_sensitive
def test_relative_gate_has_teeth() -> None:
    """Drill: the relative gate must actually red on the defect it exists
    for. A classifier whose miss path developed a hot loop (the classic
    regression: retry logic creeping into the not-claimed path) is
    injected into the composed fleet and the gate's own statistic is
    computed — the drill asserts the statistic EXCEEDS the budget, i.e.
    the gate above reds on this hardware, without depending on absolute
    speed."""

    def hot_loop_classifier(exc: BaseException, attempt: int) -> None:
        # The defect under drill: ~100µs of busy work per miss — a
        # cost-CLASS change, orders of magnitude over the budget.
        for _ in range(2_000):
            pass
        return None

    drilled_fleet = compose_retry_classifiers(
        rate_limit_aware_classifier,
        failure_taxonomy_classifier(),
        hot_loop_classifier,
    )
    exc = RuntimeError("unrelated failure")

    baseline_us, _, _ = _calibration_baseline_us(exc)
    drilled_us = _best_round_mean_us(drilled_fleet, exc)
    drilled_ratio = drilled_us / baseline_us

    print(
        f"\n── drill: hot-loop classifier in the fleet ──\n"
        f"  drilled ratio {drilled_ratio:.2f}x vs budget {COMPOSED_RATIO_BUDGET}x "
        f"(drilled {drilled_us:.3f}µs, baseline {baseline_us:.3f}µs)"
    )
    assert drilled_ratio > COMPOSED_RATIO_BUDGET, (
        "the relative gate LOST ITS TEETH: a hot-loop miss path "
        f"({drilled_us:.3f}µs, {drilled_ratio:.2f}x baseline) did not "
        f"exceed the {COMPOSED_RATIO_BUDGET}x budget — the gate can no "
        "longer catch the regression it exists for"
    )
