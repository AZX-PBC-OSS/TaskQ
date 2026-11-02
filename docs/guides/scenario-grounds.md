# The Scenario Grounds — seven behaviors, run and verified

Seven runnable scripts live in [`examples/scenario_grounds/`](https://github.com/AZX-PBC-OSS/TaskQ/tree/main/examples/scenario_grounds), one production behavior each. Every script drives real workflows against a real Postgres (and a real Redis when a worker joins), prints the states it exercised and the rows it verified — one `EVIDENCE {…}` line per check — and ends green only when the rows told the truth it promised. The companion test (`tests/test_scenario_grounds.py`) runs each script and asserts the printed evidence's row-truth.

Each scenario builds its own schema from scratch (`grounds_s1` … `grounds_s7`), so any of them re-runs cleanly and none sees another's rows. To run one:

```bash
export TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq
export TASKQ_REDIS_URL=redis://localhost:6379/0

uv run python -m examples.scenario_grounds.s1_fanout_hold
```

The scenarios in one line each; the link on the scenario's name is its script:

1. [The fan-out with the approval hold](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/examples/scenario_grounds/s1_fanout_hold.py) — a batch fans out to two queues, the flow stops at a human gate, a typed resolve answers it, the flow publishes. Evidence: both lanes' rows claimed and finished, the hold's lifecycle (held → delivered), the terminal state.
2. [The three-tenant priority split](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/examples/scenario_grounds/s2_priority_split.py) — three tenants' runs (150 rows) on one actor at three priorities, the runs created round-robin across the tenants so the row ids alternate too; one worker claims one row per round. Because the ids interleave, a claim order that ignored priority would fragment across the tenants — the grouped drain the rows show can only come from the priority ordering. Evidence: the claim order read off the rows (30×50, then 10×50, then 0×50) and all 150 succeeded.
3. [The max_concurrent damper](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/examples/scenario_grounds/s3_max_concurrent.py) — ten flows race for one actor's two seats. Evidence: the rows' own start/finish instants folded into one running count — never more than two in flight, two genuinely reached, all ten succeeded.
4. [The reservation guard](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/examples/scenario_grounds/s4_reservation_guard.py) — a step and a competing plain actor share a two-seat reservation; the actor takes both seats first. Evidence: the step waits (parked, the denial's own stamp and bucket read off the row, no failure, no attempt spent) and goes again when the denial's retry hint says the seats may be free — a five-second lease makes the hint, and the wait, short.
5. [Retention, live](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/examples/scenario_grounds/s5_retention_live.py) — three terminal runs past a five-second horizon and one live run at its gate. Evidence: the history rows counted before the sweep starts and after it prunes, per table; the live run's rows untouched; the sweep's own prune record from the worker's event log.
6. [The retry ladder](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/examples/scenario_grounds/s6_retry_ladder.py) — a flaky body fails twice, then succeeds. Evidence: the attempts' ledger rows (failed, failed, succeeded), and the curve read off the re-pend's own provenance stamp — the re-pend statement stamps the row's metadata per attempt with the backoff it applied and its own due instant (one clock read), the scenario reads the stamp once after the run and reports the applied backoff per attempt, and the next attempt's claim honoring the stamped due instant.
7. [The degraded faces](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/examples/scenario_grounds/s7_degraded_faces.py) — three wrong deployments, three clean refusals: the uninstalled schema (the typed refusal + the page's notice, never a 500 — the page face mounts under the dev label, dev-only by design: production mounts the sidecar with its real auth dependency), the stale definition (the row parks loudly with its reason stamped; the run finishes when the definitions return), the misconfigured queue (the boot refuses, naming the queue and the remedy).

## What each scenario's rows prove

The scripts print two kinds of line: the story in plain English, and
one machine-readable `EVIDENCE {…}` line per check. The evidence lines
are the rows' own testimony — statuses, counts, orderings read back
from the database — and the companion test asserts them, so a scenario
that stops telling the truth fails its test rather than printing a
convincing story.

- **Statuses** — every run's terminal verdict comes off the root row;
  the hold's lifecycle comes off the signal row (`held`, then
  `delivered`); the parked row's reason comes off the row's own
  metadata stamp.
- **Counts** — the lanes' item rows, the 150 runs' census, the peak
  in-flight count, the history tables' before/after rows.
- **Orderings** — the claim order (each row's own claim instant), the
  attempts' ledger order, the ladder's curve read off the re-pend's
  own provenance stamp (the applied backoff per attempt, written by
  the re-pend statement itself).

## Where the behaviors are documented

Each scenario rides machinery with its own guide page:

- the hold and the resolve — [Workflows (DAG Engine)](workflows.md) (the human gate) and [Agent Fleets](agent-fleet.md);
- priority and the claim order — [Jobs & Clients](jobs-clients.md) (the enqueue's priority face) and [Workflows](workflows.md) (the priority precedence law — the per-call override, the step's declaration, the actor's, the column default);
- `max_concurrent` and reservations — [Rate Limiting](rate-limiting.md) (`ConcurrencyReservation`) and [Workers](workers.md) (choosing a concurrency control);
- the retention arms — [Workflows](workflows.md#the-workflow-tables-retention-arms-the-five-periods) (the five flow-history arms, wired into the leader's sweep loop — [Maintenance Sweeps](maintenance-sweeps.md) is the sweep family's general reasoning home); the prune record's progress count folds the stream channel's rows in — the arm's tick retires the doomed flows' `wf_node_progress` rows AND their `wf_node_stream` rows in one pass, so its count covers both tables;
- the ladder — [Retries](retries.md) and [Workflows](workflows.md) (the ladder's backoff curve);
- the degraded faces — [Troubleshooting](troubleshooting.md) and [Upgrading](upgrading.md) (the queue-default trap).

The full scenario story — how to run each script and what its evidence
means — is [the scenario grounds README](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/examples/scenario_grounds/README.md).
