# T25 — THE CROSS-RUN STEP CACHE: the Nix-style recursive address, the SUCCESS-ONLY store, the receipt-carrying hits

Lane: `feat/taskqflow-cross-run-cache` off `37ab05b8` (the
`feat/taskqflow` consolidator's measured-tax head). Worktree
`/tmp/opencode/wt-cache`; own PG on `:5786`
(`postgresql://postgres:taskq@localhost:5786/taskq`, `POSTGRES_PASSWORD=taskq`,
`POSTGRES_DB=taskq`, `max_connections=1000`, `fsync=off`, the `taskq` role),
destroyed at close. Push THIS BRANCH ONLY.

Design-first: THIS ticket is the read's output; every claim below was
verified against the tree at the base head before a line of code.

## THE TWO-CLAIMS LAW (the design's spine)

The estate already owns TWO dedup claims, and they do NOT overlap:

* **The run-key arbiter dedups CONCURRENT** — two callers racing one
  `run_key` get ONE run (`tests/test_wf_attack_runkey.py`; the typed
  `RunClaim`). It answers "who is THE run for this key", never "has this
  work ever been done".
* **The enqueue `idempotency_key` + the step-ledger memo dedup TEMPORAL,
  within a scope** — a resubmit after terminal never doubles the work,
  but the scope is the (flow, step key, map index) tuple: a NEW run with
  the same input re-executes everything.

The gap the cache closes: **the TEMPORAL, cross-RUN dedup** — the
idempotent re-run over the expensive body (the OCR's pixels, the LLM's
tokens) where the re-run's cost ×2 is the consumers' ordinary day. The
cache NEVER dedups concurrent execution (that is the arbiter's claim —
two concurrent misses both run their bodies; the law is explicit); it
dedups TEMPORAL: a LATER run over the same body + the same input reads
the EARLIER run's result and never executes the body at all.

## THE DESIGN

### (a) The content address — the Nix-style recursive hash

A cached step's address is the recursive hash over the TWO things the
result is a function of:

```python
address = content_hash({
    "schema": "wf-step-cache/v1",
    "code_version": <the body's §22.1 canonical hash — compute_code_version()>,
    "input": [<the RESOLVED args, jsonb-safe, in signature order>],
})
```

* The body half is **§22.1's stamper's canon** (`_version.py`:
  module + qualname + the body's own source, tors-canonical) — a body's
  code change IS a new address: the stale-code face is dead by
  construction, the same discipline the `jobs.code_version` record
  already ships.
* The input half is the **decoded input payload's canonical jsonb**:
  the runner's resolved args (`_resolve_args` — the parents' decoded
  results + the payload's data args, in signature order), encoded
  jsonb-safe (`encode_data_arg` — models dump through their own codec)
  and hashed by the tors canonical hash (dict-ordering-insensitive,
  pin-held).
* **Position-independent, ON PURPOSE.** The address carries NO run id,
  NO flow id, NO step key: the same body wired at any position in any
  run over the same input is the same computation — the cross-run
  face's whole point. (The step key IS in the address indirectly: it is
  determined by the body's module + qualname, which the code_version
  carries.) Two steps sharing ONE body function share the cache when
  their inputs agree — correct, and the position-independent point.
* The address is computed at the **claim face, on the rows**: the input
  is read from the rows (the parents' result columns + the row's
  payload), never from a closure. The DI deps tail is NOT part of the
  address (deps are process-level services, not data).
* Unaddressable bodies (the §22.1 unreadable-source face: a builtin, a
  partial, an exec'd re-slice) cache NOTHING — the body runs every
  time, the loss logged (`step-cache-unaddressable`). A cache whose
  key cannot be reproduced is the stale-truth hazard itself.

### (b) The cache table — the additive migration

One new table, its own lock class (CREATE TABLE only), the estate's
laws obeyed (FK-less, `{schema}`-token, no DB-side uuid — the address
is the PK, the design's explicit ruling, so the uuid7-app-side
invariant's APPEND-face does not apply: a hash-keyed table has no
creation order to preserve and no B-tree right edge to keep warm):

```sql
CREATE TABLE "{schema}".wf_step_cache (
    content_address text PRIMARY KEY,
    result          jsonb NOT NULL,
    run_id          uuid NOT NULL,
    expires_at      timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);
```

* `result` — the producing run's FULL result envelope (the
  `{"value": …}` wrapper, byte-equal to what the body-run's finalize
  wrote): a hit's row result is indistinguishable from a body-run's at
  the byte level, and the consumer's downstream decode sees the same
  typed shape (R3's face preserved through the ordinary parents'
  decode). The pointer-law bound rides the envelope (a body result is
  a jsonb VALUE, never an external pointer).
* `run_id` — **the producing run's id: THE RECEIPT.** Every cached
  result names the run that paid for it.
* `expires_at` — the in-DB TTL (the freshness leg).

Plus one index on `expires_at` (the sweep's scan), its own file per the
single-lock-class discipline.

### (c) THE TWO-CLAIMS WIRING — the claim path's lookup, the terminal store

In `_execute_claimed` (the shared tail — the in-process driver AND the
worker-hosted fleet door both land here, so ONE wiring serves both):

1. **BEFORE the body runs** (after the args resolve — the address needs
   the input; after the dispatch-time skip check — a skipped node never
   reads the cache): the LOOKUP by the address.
2. **A HIT** (the row exists AND `expires_at > clock_timestamp()` — the
   DB clock is the comparison, the estate's DB-clock doctrine): the
   step's result IS the cached envelope — `_finalize_success` delivers
   it through the ordinary fenced finalize (the downstream children
   decode it exactly as a body-run's), **the body NEVER runs**, the
   node's row carries THE RECEIPT in its `metadata`:
   `{"wf_cache_hit": {"address": …, "run_id": <the producing run>,
   "stored_at": …}}` — merged best-effort after the finalize applied
   (the projection's own asymmetry: a lost receipt is a logged
   freshness loss, never a node failure; the LEDGER owns the state).
3. **A MISS**: the body runs. **ON TERMINAL-SUCCEEDED ONLY** — the
   finalize applied — the result is written to the cache:
   `INSERT … ON CONFLICT (content_address) DO NOTHING` (the CAS).
   **THE SUCCESS-ONLY LAW**: the failure path writes NOTHING — a
   failed run's key never squats the address (the
   failed-run-squats-the-key bug closed by construction: a cached
   failure would be a lie the next run would read as truth). A fenced
   finalize (a zombie's write) stores nothing either — only a terminal
   that actually landed.
4. **The CAS's meaning**: two concurrent misses = ONE winning cache row
   (the second insert conflicts, does nothing); every SUBSEQUENT
   lookup reads the winner's payload + the winner's receipt. The cache
   dedups TEMPORAL; the concurrent-execution dedup stays the arbiter's
   — the two-claims law, kept honest.

The v1 surface is the PLAIN step (the wiring `step(...)` verb, kind
`"step"`): maps, routes, loops, chains, and the bodyless packers are
out (their forks/derived keys have their own semantics; the runner's
gate is defense in depth behind the wiring face).

### (d) The TTL — the freshness leg + the retention arm

* The store writes `expires_at = now + ttl`. The TTL is the
  **per-step opt-in's** `cache_ttl=` (seconds); the default is 24h
  (`STEP_CACHE_TTL_DEFAULT_S`) — the design's default, a named
  constant on the cache module.
* The lookup's freshness leg is `expires_at > clock_timestamp()` — an
  expired row IS a miss (the re-run re-fills it, overwriting via the
  CAS on its own success). Expired-but-present rows are never lies —
  only dead weight.
* The dead weight's cleanup is **the sweep's retention arm** (the
  retention policy's family: bounded batch, the period gate, the
  `timedelta(0)` disable sentinel — the delivered-outbox arm's own
  shape): `prune_expired_step_cache` deletes `expires_at <=
  clock_timestamp()` rows, one bounded batch per pass, registered on
  the leader's sweep table behind the new
  `workflow_step_cache_sweep_period` setting (default 1h, zero means
  off), with the pre-migration `UndefinedTableError` tolerance.

### (e) THE AUTHORING FACE — opt-in per step, default OFF

```python
step(body, params, cache=True)  # the 24h default TTL
step(body, params, cache=True, cache_ttl=600)  # the explicit freshness
```

* **Default OFF** — the cache is a decision, never a surprise: a step
  that did not ASK to be cached is never cached (the cross-run result
  reuse must be a property the author can reason about, not an
  optimization the engine takes).
* `cache_ttl` without `cache=True` is REFUSED at the wiring site (a
  silent no-op parameter is the surprise family); a non-positive TTL
  is refused (an always-expired cache is a lie); `cache=True` on a
  gather (the multi-parent fan-in) is refused in v1 (the surface is
  the plain step).
* **THE DETERMINISM HAZARD — the docs' honest paragraph**: the cache
  assumes the body is DETERMINISTIC for the same input. A
  non-deterministic body (`cache=True` on a clock-reader, a
  random-walker, a NOW()-sampler) + a cache hit = THE STALE TRUTH BY
  CHOICE: the author opted in, the engine delivered exactly what the
  address promised. The hazard is named in the guide and in the
  `step()` docstring — the cache is the determinism DECLARATION's
  enforcement point, not its substitute.

## THE PINS (red-first — `tests/test_wf_step_cache_pins.py`)

| Law | Pin | The red observed (at the base head) | The flip (the SAFE behavior) |
|---|---|---|---|
| the hit | `test_hit_the_body_runs_once_across_runs` | `TypeError` (no `cache=`) | the SECOND run's body never executes (the counter == 1); its node row carries the receipt (the address + the producing run's id); its consumer sees the SAME typed result |
| success-only | `test_success_only_a_failed_body_never_squats_the_key` | red (same absence) | the failing body's cache row count is 0 after BOTH runs; the SECOND run re-EXECUTES the body (never a cached failure) |
| the TTL | `test_expired_ttl_is_a_miss` | red | a backdated `expires_at` (the DB clock) makes the second run a MISS: the body re-executes and re-fills |
| the CAS | `test_concurrent_miss_one_winner` | red | two concurrent misses: ONE cache row, the winner's `run_id`, the loser's insert ignored; the NEXT lookup reads the winner's payload |
| the typed hit | `test_hit_delivers_the_declared_type` | red | the consumer's body (a declared model param) receives the model INSTANCE on the hit run — R3's decode face through the cache |
| the address | `test_code_change_is_a_new_address` | red | a body whose source differs (same input) is a DIFFERENT address: the second run executes (the stale-code face is dead) |
| the wiring | `test_ttl_without_cache_refuses_at_the_wiring` / `test_non_positive_ttl_refuses` / `test_cache_on_a_gather_refuses` | red | the named `WorkflowBuildError`s |
| the retention | `test_the_sweep_prunes_expired_rows_only` + the wiring/sentinel pin | red | expired rows delete, fresh rows survive; the sweep table registers the arm; `timedelta(0)` disables; the default keeps it alive |

## THE PROOF (the battery at the stamped sha)

ruff check + ruff format --check + pyright FULL 0/0/0 + the type gate +
the pins green + the wf battery ×2 (leg 1 with the scoped coverage gate)
+ the fast tier ×1 at `-n 8` (full capture) + `mkdocs build --strict`
(the guide's cache section: the two-claims law's user face + the
determinism hazard + the TTL — the verified snippets). Red-first: the
pins ran against the base head BEFORE the build; the receipts live in
`.measurements/` (head-stamped).

## THE DOCS

`docs/guides/workflows.md`: THE CROSS-RUN STEP CACHE section — the
opt-in face, the two-claims law's user face (the arbiter dedups
CONCURRENT; the cache dedups TEMPORAL), the determinism hazard, the
TTL, the receipt. A runnable fence (the docs-example tier executes it).

## THE REPORT

The landed surface's shape (the signatures), the address's canon, the
pins (the names + the red receipts), the battery numbers, the branch's
head. The work is evidence, never verdict.
