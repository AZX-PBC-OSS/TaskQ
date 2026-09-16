# Bulk-cancel drain: before/after measurement

The defect: `_cancel_where`'s drive statements re-selected "the next
`batch_size` matching rows `ORDER BY id LIMIT n`" from the bottom of the
key space on every batch, and the UPDATE re-scanned `jobs` through a
FROM-join the planner could hash-join against a seq scan — so batch N
re-walked the (N-1)·batch_size rows earlier batches had already handled,
on both arms (terminal pending/scheduled and cooperative running).

The fix, in this tree:

1. Both drive statements page with a keyset cursor (`AND id > $cursor
   ORDER BY id LIMIT $n`); the cursor advances on the window's hi key
   (`last_id` = last element of the window's ordered id array — stock
   PostgreSQL has no `max(uuid)` aggregate), including rows the UPDATE's
   EPQ re-check skipped, so the drain cannot spin on a claimed row.
2. The UPDATE restricts on `j.id = ANY (<this batch's ids>)` — a
   restriction clause on `jobs`, planned as one pkey probe per id — and
   the pre-update status/worker columns are recovered by joining the
   batch-sized `matching` snapshot back on the affected ids, never
   touching `jobs` twice.
3. Each batch transaction pins `plan_cache_mode = force_custom_plan`
   (same SET LOCAL capture/restore discipline as the batch
   `statement_timeout`): the drain issues one statement text per arm on a
   pooled connection, so past asyncpg's prepare threshold the plancache
   flips to a generic plan that binds the cursor as an unknown — and the
   re-walk returns mid-drain (measured below).
4. Migration `01.00.12_06_pre_jobs_cancel_drain_tag_indexes.sql`: two
   partial GIN tag indexes whose membership tracks each arm's live
   window.

## Method

Rows **discarded** = `Rows Removed by Filter × Actual Loops` summed over
the plan tree of `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` — the pins'
own instrumentation; rows **visited** = discarded + actual rows
returned; **buffers** = shared hit+read blocks summed over the tree.
Suite runs: `uv run --no-sync pytest tests/test_cancel_where_bounded.py
tests/test_rt_cancel_drain_keyset_cost.py` against the suite's
container pair. Plan-shape probes: same instrumentation driven by hand
against a throwaway `postgres:18-alpine` container on corpora shaped
like the pins (200-row/10-batch drains, 20-among-2000 cross shapes,
40k-row terminal history), each corpus `ANALYZE`d; the post-threshold
leg uses `PREPARE` + 5 `EXECUTE`s then `EXPLAIN EXECUTE` (the
plancache's generic-consideration threshold — the same protocol
`tests/test_index_audit.py` uses).

## The pinned shapes (suite, base commit → this tree)

| Pin | Base (red) | This tree (green) |
|---|---|---|
| `test_drain_batch_cost_does_not_grow_as_the_backlog_is_cancelled` | rows discarded per batch `[1, 41, 81, 121, 161, 201, 241, 281, 321, 361]` (worst 361 > bound 40) | passes; every batch ≤ 40 (probe-measured actual: 0 on all 10 batches) |
| `test_drain_batch_cost_is_independent_of_non_matching_backlog` | small=2, large=**2043** | passes; ≤ 40 |
| `test_running_arm_drain_batch_cost_does_not_grow_as_requests_accumulate` (new pin) | `[1, 41, 81, 121, 161, 201, 241, 281, 321, 361]` | passes; ≤ 40 (probe: 0) |
| `test_running_arm_drain_batch_cost_is_independent_of_non_matching_backlog` (new pin) | small=2, large=**41** | passes; ≤ 40 |

The running-arm cross pin's base margin is thin (41 vs the 40 bound) and
that is reported plainly: the pre-existing unpartial `jobs_tags_gin_idx`
already scopes the tag, so at this seed the base implementation discards
only the already-handled matches, not the foreign running backlog. The
pin still catches a plan that walks foreign running rows (which measures
in the thousands), and it closes the suite's previous blindness to the
running arm.

## Plan-shape probes (200-row drains, batch 20, custom plans)

| Shape | Discards per batch | Total visits | Total buffers |
|---|---|---|---|
| pending arm, base | `[0, 40, 80, …, 320, 360]` | 4530 | 7829 |
| pending arm, fixed | `[0] × 10` | 2320 | 7945 |
| running arm, base | `[0, 40, 80, …, 320, 360]` | 4530 | 7009 |
| running arm, fixed | `[0] × 10` | 2320 | 7929 |
| cross shape (20 matching among 2000 foreign), base | `[2000]` on the draining batch | 4203 | 816 |
| cross shape, fixed | `[0]` | 266 | 814 |

Buffers are flat by design at these sizes — the whole table is cached,
so page touches do not discriminate; rows discarded is the oracle that
sees the re-walk, which is why the pins assert it rather than buffers.

## The generic-plan flip (why the per-batch plan pin exists)

Same fixed statement, 200-row drain, server-prepared past the 5-execution
generic-consideration threshold:

| Plan mode | Post-threshold discards per batch |
|---|---|
| `auto` (default) | `[100, 120, 140, 160, 180]` — the re-walk returns |
| `force_custom_plan` (as the batch transaction sets) | `[0, 0, 0, 0, 0]` |

Without the pin, a drain longer than a handful of batches on a warm
pooled connection silently re-enters the quadratic regime — on exactly
the deep backlogs the fix exists for. The pin costs one extra GUC
capture/set and restore per batch (two statements), and a per-batch
replan of a two-table statement — sub-millisecond against a batched
UPDATE plus its event writes.

## Index decisions (all measured)

- **No plain `(id)` partial index over the active rows.** With one
  present, the drain's queue- and actor-filtered windows (`queue = $1
  AND status IN (…) AND id > $cursor ORDER BY id LIMIT $n`) switch from
  the keyed `jobs_queue_active_idx` / `jobs_actor_active_id_idx` seeks
  to the id-only index and resolve the keyed column as a post-scan
  filter — strictly worse for those drains, and it trips the
  `tests/test_index_audit.py` queue-seek guard. Verified on an
  audit-shaped corpus (5k queue backlog + 7k other active + 40k
  terminal): both windows pick `jobs_active_id_idx` the moment it
  exists, neither does when it does not.
- **The partial tag GINs ship** (`status IN ('pending','scheduled')` and
  `status='running' AND cancel_phase=0`, predicates verbatim). At suite
  scale they are neutral — the keyed partials and the unpartial
  `jobs_tags_gin_idx` already serve the measured shapes (verified:
  identical plans and discards with and without them at these sizes).
  Their load-bearing shape is the same-tag deep-history resume: the
  unpartial index's posting list keeps every row a drain has ever
  cancelled under that tag, so a re-run's batches re-fetch them until
  the per-batch statement timeout trips. These two indexes' membership
  tracks the live window — a row leaves the index in the same
  transaction that moves it out of the window.
- **The running arm gets no `(id)` partial either**, symmetric with the
  pending arm: nothing keyed on running+phase-0 exists to compete with,
  and no pin or measured shape required it. Its unfiltered and
  queue/actor-filtered forms are bounded by the keyset cursor to one
  visit per row per call.

## Accepted residuals (stated, not hidden)

- An **unfiltered** drain (`JobFilter()` with no conditions) pays one
  linear pass of the id space's terminal-history prefix per call (batch
  1 walks it once; the cursor then passes it for good). Measured: 40,000
  discarded on batch 1 of a 40k-terminal + 200-live corpus, zero on all
  later batches. Bounded, once per call, statement-timeout-guarded — and
  strictly better than the base shape, which paid it per batch. The
  id-only partial that would close it cannot ship (previous bullet but
  one).
- A row whose match status flaps *behind* the cursor mid-drain (claimed
  and released by a dispatcher inside one pass) is left for a re-run —
  the same non-atomic contract the drain already documented for
  concurrent enqueues ("a concurrent enqueue can slip a new matching row
  in between batches"). Job ids are UUIDv7, so new inserts sort above
  the cursor and are caught within the same call.

## Suite results (this tree, final state)

```
uv run --no-sync pytest tests/test_cancel_where_bounded.py tests/test_rt_cancel_drain_keyset_cost.py tests/test_sweepaudit_bounded_writes.py tests/test_migrations_unit.py -q
→ 117 passed

uv run --no-sync pytest tests/test_index_audit.py -q
→ 28 passed, 4 pre-existing failures owned by other units (verified red
  with this unit's migration absent: backlog-depth gauge grouping,
  move-queue composite-index pin, two event-retention outbox arms)
```
