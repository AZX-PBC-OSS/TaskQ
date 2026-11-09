"""The gauge-fleet recurring-refresh example.

A contrived, self-contained demonstration of a recurring refresh flow built
entirely from the documented TaskQ mechanisms:

- a durable cursor advanced by each tick (your own table, one transaction
  with the work — ops.md §5, Pattern A's durability discipline),
- the increment fanned out when it is wide (per-item idempotency keys),
- the next tick scheduled on the durable timer (``scheduled_at`` promotion —
  ops.md §7, "scheduled re-enqueue"),
- an app-level running-run guard so a slow tick parks latecomers instead of
  stacking on the running one (ops.md §5, "choosing a dedup mechanism"),
- a threshold breach that pauses for approval and escalates fail-close when
  the window lapses (a typed error in ``non_retryable_exceptions``),
- per-phase progress pushes (guides/progress.md) and typed success receipts.

See ``README.md`` in this package for the tour and the drill harness:

    uv run python -m examples.gaugefleet.run_demo --drill all
"""
