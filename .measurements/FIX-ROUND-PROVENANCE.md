# THE FIX ROUND'S PROVENANCE CORRECTIONS (the reviewer's A-D; the receipts law)

*Appended by the fix round on `fix/audit-seam-recursion-verify`. This file is
the CORRECTIONS' home: nothing in `.measurements/` is deleted or rewritten —
the corrections are appended BESIDE the records they correct (append-only,
history never deleted).*

---

## B — the provenance misattribution: which captures were scratch-line numbers

**The defect.** The consolidation's identity renumber round
(`3be7578f` — "consolidate: THE REBASE'S IDENTITY COLLISION CURED") renamed the
workflow chain's files to `01.00.23_04/05/06` (+ the split `01.00.23_07`) and
committed the name-keyed pins reading THOSE names
(`tests/test_wf_schema_migration.py`'s `_round_by_name("01.00.23_04_…")` family
— 8 name references). A LATER round renumbered the files again to
`01.00.24_01/02/03` **without retargeting the pins' names**. On every COMMITTED
tree from `3be7578f` through `2ac9c84b`, the name-keyed pins were therefore
**PROVABLY RED**: a `01.00.24` glob (`WORKFLOW_ROUND = "01.00.24"`) can never
contain a `01.00.23_04` name — `_round_by_name` cannot find the file, the pins
error deterministically. Verified from the committed trees themselves
(`git show <rev>:tests/test_wf_schema_migration.py` — 8 refs to `23_0[4567]` —
against `git ls-tree <rev> src/taskq/migrations/` — the files named
`24_01/02/03`, no `23_04/05/06`).

**The misattributed captures.** The rounds interleaved with that window claimed
GREEN while the committed trees provably red the name-keyed family:

| Record | Claimed | Status |
|---|---|---|
| the loaded-regime bar's rounds **L2/L3/L4** (12,996 passed ×3 — the LOADED-REGIME BAR'S RECORD) | green fast-tier under load | **SCRATCH-LINE NUMBERS**: the committed tree in that window reds the name-keyed pins deterministically; the greens measured the UNCOMMITTED SCRATCH LINE (the working tree where pin and file names temporarily agreed), never the committed heads |
| the rounds citing the name-keyed family's green inside `3be7578f..2ac9c84b` | various | same defect — any capture in the window claiming the schema-migration family green is a scratch-line number |

**What STANDS.**

- `p4-cov-run5.txt` ("190 passed") — written ONCE at `bdb21804`, which PRECEDES
  the identity collision: at `bdb21804` the pins and the files agree
  (0 refs to `23_0[4567]`, no files so named) — the capture is honest FOR ITS
  OWN TREE (`bdb21804`). It was never overwritten in the window. It may not be
  cited as evidence about any LATER tree.
- The HEAD's captures — the pins now read the TRUE identities
  (`01.00.24_01/02/03`, the reviewer-approved numbering; the renumber resolved
  to the SPLIT ONLY) and the files match. **Re-verified on the cured head
  ×2** (the touched pins: the schema-migration family + the pre-workflow
  tolerance family + the parent-id pins — 22 passed each round; captures:
  `.measurements/runs/fixround-touched-pins-x2-*.txt`). The head's numbers
  stand.
- The migration chain applies CLEAN on a fresh schema (57 migrations, the swept
  files' identities present, no phantom name required):
  `.measurements/runs/fixround-migration-chain-*.txt`.

## A — the phantom identities (cured this round)

The three single-lock-class files' OWN HEADERS (`01.00.24_01/02/03`) cited the
round's split as `01.00.23_01/02/03` — filenames that exist NOWHERE in the
chain (six stale lines: three in `24_01`'s split list, one in `24_02`'s
cross-reference, one in `24_02`'s claim-arbiter note, one in `24_03`'s
cross-reference). A deployer following the cross-references landed on nothing.
SWEPT to the true `24_01/02/03` identities this round; the chain applies clean
(the capture above).

## C — the trailing law (cured this round)

`test_copy_from_columns_carries_parent_id`'s docstring claimed the trailing
position "died with the consolidation" — FALSE since `93b255ff` restored it
(and that restoration had not been carried to this branch: the budget trio's
append had re-broken the tail — `COPY_FROM_COLUMNS[-1] == "budget_remaining_ms"`
on this head before the cure). The resolution carried: **the trio moves BEFORE
`parent_id`** (the archive-mirror parity intact — the list is the single source
the archive CSVs and the enqueue COPY both derive from; the record builder
writes `parent_id` last — the ARITY pin's coherence holds). The docstring now
names the trailing law, and the one-line assertion the omission-set comments
already cite (`COPY_FROM_COLUMNS[-1] == "parent_id"`) HOLDS (verified: the
touched pins' captures ×2).

## D — the mark law's residue (cured this round)

`test_wf_perf_bands.py`'s module docstring declares "All load_sensitive (the
serial perf lane)" while `test_fan_out_tx_band_1000_children` and
`test_join_fire_latency_band` (the two wall-clock bands) were UNMARKED. Both
carry `@pytest.mark.load_sensitive` now — the law's own state: nothing
unmarked and unconverted.
