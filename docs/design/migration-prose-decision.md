# Migration prose: violations found, remediation options, decision

Status: DECISION REQUESTED (owner choice pending; nothing in
`src/taskq/migrations/` has been edited on this branch).

This document responds to the prose audit finding that 24 files under
`src/taskq/migrations/` carry prose that the house rules ban everywhere
else (em-dashes, filler vocabulary, ticket references, a competitor
attribution). The migration files were deliberately excluded from the
earlier prose sweep because they are a frozen, checksummed, fail-closed
record: the naive edit breaks every applied database's drift
verification. This document enumerates what actually violates, proves
the consequences by execution, and recommends one option.

## The enforcement machinery (what an edit actually touches)

Read from `src/taskq/migrate.py` and proven by execution in a scratch
schema (`mig_lab_hygiene`, disposable Postgres at `127.0.0.1:45432`;
the lab script is not committed, its full transcript is reproduced in
this document's claims):

- Each migration's checksum is `sha256(render(sql_template, schema))`
  (`Migration.checksum`, migrate.py:168). `render` substitutes only the
  `{schema}` token (migrate.py:323); `--` comments are NOT stripped.
  Any prose edit, even one hyphen, changes the checksum.
- The ledger row is written at apply time; every later run recomputes
  and compares (`_detect_checksum_drifts`, migrate.py:617).
- On mismatch the runner raises `ChecksumDriftError` BEFORE applying
  anything (migrate.py:813). Fail-closed by design.

### Executed proof A: a prose edit breaks apply (fail-closed)

Lab phases, all executed:

1. Clean apply: 32 migrations into scratch schema. Ledger row for
   `01.00.10_02:pre` stored checksum `3086db62c8521fc4...`.
2. Idempotent re-run: 0 applied, no drift.
3. Tamper ONE comment block (4 comment lines: dropped the `#139`
   ticket ref, reworded the header; zero SQL changed) in the APPLIED
   file `01.00.10_02_pre_keyed_row_fleet_reclaim.sql`:

   ```
   REFUSED, as designed. ChecksumDriftError:
     1 applied migration(s) no longer match the bundled files (checksum
     drift): 01.00.10_02:pre stored 60ea10282a7f vs file 0c20f806dac1.
   ```

   Exit before any statement runs. Every deployment behaves the same
   way: the tamper is detected on the NEXT `migrate` invocation, at
   apply time, forever.

4. Override (`apply_pending(allow_checksum_drift=True)`): the run
   proceeds, 0 pending, and the ledger KEEPS the stored checksum
   (`3086db62c8521fc4...` - verified equal after the override).
5. Restore the file (`git checkout --`): drift detection is clean
   again, 0 applied.

### Executed proof B: the override does not silence the warning

After the override, the ledger still holds the old checksum, so drift
is re-detected and re-warned on EVERY subsequent run. Executed: three
consecutive `allow_checksum_drift=True` runs against the tampered file
logged `migration-checksum-drift` three times. The only way to stop the
warning is to hand-rewrite each database's `schema_migrations.checksum`
row, which defeats the ledger's provenance purpose.

### Executed proof C: the CLI has no override path (gap)

`ChecksumDriftError`'s message and `apply_pending`'s docstring point CLI
operators at "the CLI's `--allow-checksum-drift`". The flag does not
exist: `taskq migrate up` (src/taskq/cli.py, `migrate_up`) exposes only
`--phase`, `--target`, `--max-steps`, `--ddl-lock-timeout`, and
`--pg-credential-provider`, and `_up` calls `apply_pending` without
`allow_checksum_drift`. Executed proof: applied a clean baseline, then
ran the tampered-file scenario through the real CLI:

```
$ taskq migrate up
{"event":"migration-checksum-drift","key":"01.00.10_02:pre",
 "stored_checksum":"70fa2971...","current_checksum":"2ca173dc..."}
migration failed: 1 applied migration(s) no longer match the bundled
files (checksum drift): ...
Action: fix the error and re-run `taskq migrate up`, already-applied
migrations are skipped.
exit-code: 1
```

A CLI operator's only remedies are: restore the file, or write a
bespoke Python snippet that calls `apply_pending(allow_checksum_drift=
True)`. After restoring the file, the re-run printed `no pending
migrations` (also executed).

### The byte-freeze on top

`tests/test_migrations_released_frozen.py` plus
`tests/data/released_migrations.sha256` pin the bytes of every file a
release has shipped (14 files pinned; the test passes on main). Its
docstring states the policy: "nothing is ever edited". Editing any
pinned file fails CI by design, independent of the ledger.

## (a) The exact violations (executed greps, main @ 61bd2465)

24 of 32 migration files carry at least one violation. (The three
newest families, 01.00.15/16/17, are clean and are the model to copy.)

Em-dashes: 68 lines across 18 files. Worst offenders:

| File | em-dash lines |
|---|---|
| 01.00.10_02_pre_keyed_row_fleet_reclaim.sql | 9 |
| 01.00.12_06_pre_jobs_cancel_drain_tag_indexes.sql | 8 |
| 01.00.06_01_pre_cancel_and_cascade_indexes.sql | 8 |
| 01.00.13_03_pre_jobs_batch_open_members_index.sql | 5 |
| 01.00.09_01_pre_round_robin_probe_index.sql | 5 |
| (12 more files) | 1-4 each |

Banned vocabulary (8 sites):

- filler `simply`: 01.00.08_01_pre_denial_counters.sql:43
  ("they simply keep widening"), 01.00.18_02_pre_claim_epoch.sql:56
  ("epoch 0 is simply the one")
- `essentially`: 01.00.02_01_pre_job_events_outbox.sql:8,
  01.00.07_01_pre_event_retention_index.sql:33 ("written on essentially
  every lifecycle transition")
- `load-bearing`: 01.00.12_06_pre_jobs_cancel_drain_tag_indexes.sql:37
  ("the phase term is load-bearing")
- borderline (contrastive "not just", arguably legitimate): three sites
  in 01.00.15_01, 01.00.13_01, 01.00.03_01

Ticket references (9 sites in 8 files): `#243` (01.00.12_09),
`#250` x3 (01.00.12_08, 01.00.12_07, 01.00.12_05), `#139` (01.00.10_02),
`#130` x2 (01.00.09_01 pre and post), `#29` x2 (01.00.04_01).

Competitor attribution (1 site): 01.00.00_01_pre_initial.sql:188
"Vendor parallel: River (state, finalized_at) WHERE finalized_at IS NOT
NULL." - the same pattern the prose sweep removed from
`_cancel_bulk.py` and `_sweeps.py`.

Of the 24 violating files, 11 are byte-pinned in the release manifest
(01.00.00 through 01.00.10_02, i.e. every file a release has shipped
except 01.00.01, 01.00.03_01_post, and 01.00.05, which are clean); 13
are not yet pinned (01.00.12_02 through 01.00.14_01, and
01.00.18_02).

## (b) Options and their real consequences

### Option (i): leave the applied files as-is; the ban applies forward

The 24 files keep their prose. New migration files follow the bans from
their first line (the house rule is now stated in
`docs/architecture.md`, section "Migration files are append-only", on
this branch).

Consequences, all verified:

- Zero operator impact: every applied database's ledger keeps matching
  its files; `taskq migrate up` stays green (executed: phase 2 of the
  lab, 0 applies, no drift).
- The byte-freeze manifest stays intact; CI stays green.
- The violations persist permanently in files nobody may edit. Future
  audits and greps will keep finding them; the audit scope must state
  that migrations are excluded, or it will keep re-flagging them.
- The in-file comments remain the primary documentation, which matters:
  the em-dash-heavy comment blocks (per-index lock analysis, plan
  behavior) are exactly what a maintainer reads during an incident, in
  place, next to the SQL they explain.

### Option (ii): prose-fix the files and ship a checksum-regeneration release

Edit the prose in all 24 files (em-dashes to commas/parentheses, ticket
refs unlinked or dropped, the River line rewritten), update the release
manifest for the 11 pinned files, and ship.

Consequences, quantified from the executed proofs:

- Every deployment that has applied any of the 24 files (which is every
  deployment that migrated at all, since 01.00.00_01 violates) fails
  its next `taskq migrate up` with exit 1 (proof C). The failure is
  fail-closed: nothing is applied, so the deploy step blocks until an
  operator intervenes.
- The documented CLI override does not exist (proof C), so each
  operator must either restore the old bytes out of the new package
  (impossible; the files ship changed), hand-rewrite the ledger row
  (`UPDATE <schema>.schema_migrations SET checksum = '<recomputed>'
  WHERE version = '01.00.10_02:pre'` - per database, per file, 24
  files), or run a bespoke `apply_pending(allow_checksum_drift=True)`
  snippet against production.
- Even after overriding once, `migration-checksum-drift` logs on every
  subsequent migrate, forever, unless the ledger row is rewritten
  (proof B). The fleet's deploy logs acquire a permanent warning that
  operators learn to ignore - the exact failure mode the frozen-bytes
  test's docstring warns about.
- The un-freeze also has to be memorialized in
  `released_migrations.sha256` (recompute 14 digests), and the frozen
  test's stated policy ("nothing is ever edited") is broken once,
  which weakens it for every future "just a comment" edit.

The honest summary: option (ii) buys prose consistency at the cost of a
breaking release for every existing database, a permanent warning
amplitude in every deployment's logs, and a hand-edit-or-bespoke-script
burden per deployment. There is no tooling in the repo to regenerate
ledger checksums; one would have to be written and shipped first.

### Option (iii): copy the violated prose intent into docs/, leave files frozen

Move the *content* of the violating comment blocks (the lock analyses,
the plan rationale, the ticket-context) into `docs/architecture.md` or
a new `docs/design/migrations-*.md`, and leave every migration file
byte-frozen.

Consequences:

- No drift, no release breakage, no manifest churn (executed
  reasoning: no file changes at all).
- The intent survives somewhere, but orphaned from its SQL: the
  migration files cannot carry a pointer to the docs page (adding one
  comment line to a frozen file is the same checksum break the option
  is trying to avoid). A reader of `01.00.13_03` during an incident has
  no way to discover the docs copy except by searching.
- Duplication is unmanaged: the docs copy and the frozen comments now
  state the same facts; only one can be updated, and nothing in CI
  detects when they diverge.

## (c) Recommendation: option (i)

Reasoning:

1. The checksum ledger is a load-carrying invariant for every applied
   database, and
   the executed proofs show its fail-closed behavior is exactly what
   makes the naive edit dangerous. Option (ii) manufactures a breaking
   event for every deployment to fix comments nobody executes. Option
   (iii) duplicates prose without a discoverability path and with
   unmanaged divergence.
2. The repo already codified this philosophy: the frozen-bytes manifest
   and its test exist precisely because "a tamper warning that always
   fires is one operators learn to ignore" (proof B shows the warning
   firing three for three). Editing shipped files contradicts the
   repo's own strongest invariant.
3. The violations are in comments, not user-facing surfaces. The cost
   of leaving them is aesthetic inconsistency inside frozen history;
   the cost of fixing them is fleet-wide breakage plus permanent log
   noise. The trade is not close.
4. House precedent already runs this way: when the prose audit found
   `01.00.14_01`'s lock-mode claim wrong, the correction went into
   `docs/architecture.md` ("the header stands as applied and this
   paragraph carries the correction"), not into the frozen file.
   Option (i) generalizes that pattern from factual corrections to
   prose hygiene.
5. The 13 unpinned files (01.00.12_02 through 01.00.14_01, 01.00.18_02)
   are technically still editable with no *released* deployment able to
   have applied them (the manifest pins through 01.00.10_02). If the
   owner wants a partial cleanup, that window exists, with one caveat:
   anyone running from an unreleased checkout and migrating since
   2026-09-15 would still see drift. The default recommendation is to
   leave even those alone; the window closes when the next release
   pins them.

Regardless of the option chosen, two repairs are worth making and are
made or flagged on this branch:

- `docs/architecture.md` claimed drift "is warned on, not rejected";
  the executed behavior is fail-closed rejection (proof A/C). Corrected
  on this branch.
- The `--allow-checksum-drift` gap (proof C) is an owner follow-up:
  either implement the flag the error message promises, or stop
  advertising it. Not implemented here (behavior change, owner's
  call).
