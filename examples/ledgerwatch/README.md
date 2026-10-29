# LedgerWatch

A compliance-monitoring service built ON TaskQflow — the ADOPTION
SIMULATION's shipped product: a transaction batch flows **ingest →
enrich** (a fan-out over a flaky external API) → **branch by risk
class** → **HIGH-risk transactions hold for a human approval** (the
Expired arm escalates to a compliance queue) → **the rest auto-clear** →
a typed completion report; a scheduled **reconciliation pass** runs on a
durable timer.

The product is REAL TaskQflow machinery end to end: real Postgres rows,
real `taskq worker` subprocesses, the framework's retry ladders, holds,
sweeps and run-key arbiter. The MOCKS are only the EXTERNALS (the
enrichment API, the feed) — and the mocks carry the adverse world:

| dynamic | the profile (recorded in `config.py`) |
| --- | --- |
| FLAKINESS | ~15% of enrichment calls fail transiently (typed error); the retries land them |
| TIMEOUTS | ~5% of calls hang past the declared timeout; the timeout face fires, the ladder re-runs |
| DELAYS | 50–300 ms of realistic jitter per call |
| CRASHES | the drills SIGKILL real workers mid-fan-out and mid-approval-wait |
| DUPLICATES | the feed re-delivers batches; the pipeline is idempotent |

## Layout

| module | what it is |
| --- | --- |
| `config.py` | the adverse-world profile + the operational settings (env-tunable, recorded) |
| `models.py` | the typed payload contract (the discriminator law: REQUIRED `kind: Literal[...]` tags) |
| `enrichment.py` | the MOCK external API + the durable per-call journal (the drills' receipt) |
| `feed.py` | the at-least-once feed (deterministic batches) |
| `app.py` | the three graphs: `ledger_watch`, `txn_review` (the per-txn HITL hold), `reconciliation` (the durable timer) |
| `store.py` | the app's own idempotent tables: the journal, the compliance queue, the reports, the recon passes |
| `pooling.py` | the lazy pool (the deps binding is import-time; the pool is loop-time) |
| `client.py` | the trigger face: idempotent submits + the terminal waits |
| `display.py` | the operator's faces: the display API, the admin app, the progress listener |
| `worker.py` | the REAL worker entry: `python -m examples.ledgerwatch.worker` |
| `tests/` | the drill suite |

## Run the drills

A Postgres is required (the drill estate):

```bash
LEDGERWATCH_TEST_DSN=postgresql://postgres:taskq@localhost:5802/taskq \
  pytest examples/ledgerwatch/tests/ -q
```

The drills: the happy path (the typed receipt, exact), the
1000-transaction flake drill (all reach typed terminal; the journal and
the engine ledger agree; the concurrency overlap measured from the rows),
the SIGKILL crash drills (the restart resumes; the hold survives the
death), the duplicate-feed drill (zero double-effects), the timer drill
(fires no-earlier-than-scheduled; survives a worker death), the faces
(the pushed progress stream, the admin run explorer), and a 5-iteration
chaos suite. Every invariant runs UNCONDITIONALLY — a red is a finding,
reported, never a knob. The receipts land in
`.measurements/ledgerwatch/` (the local captures' home, gitignored by
the repo's convention); the friction list is
`examples/ledgerwatch/FINDINGS.md` — committed, with file:line.
