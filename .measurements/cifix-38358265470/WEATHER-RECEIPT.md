# run 38358265470's first-attempt weather red (test 3.13) + the local repro
the leg red: 3 failed in tests/test_prometheus_metrics_review.py — all one root: taskq_leader_lock_contention_total never emitted by the follower fixture's probe window (the retry loop's holder re-observation lost the scrape race)
the same commit's test 3.12 (47m36s) + test 3.14 (38m36s): GREEN, same suite, same once-per-invocation probe shape
the local repro at the EXACT interpreter (3.13.15), the same head, solo x3: GREEN x3 (81.98s / 82.01s / 81.99s)
the verdict: the pre-existing flake class (the door-edges report's documented lottery — disjoint timing sets per run), not a regression: the pack's diff in this PR is the probe-sharing cure only; the failing trio's subject is untouched
the re-run: test 3.13 GREEN in 48m0s; the run completed success
