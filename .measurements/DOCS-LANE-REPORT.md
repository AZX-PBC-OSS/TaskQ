# THE DOCS+EXAMPLES LANE — THE REPORT

*Branch `docs/taskqflow-consumer-guides` (base f7155214, the feat/taskqflow
tip; the PG17 line's 874e9e6f is NOT an ancestor, so the base stands).
The lane's PG (:5776) is destroyed at close. Worktree
/tmp/opencode/wt-docs. This report is the lane's own record; the parent
session's verdict is its own.*

## THE FOUR WORKS

1. **THE FOLDED PIN** (landed first): `HoldContext.reason is None after
   delivery` — `tests/test_wf_hitl_pins.py::test_hold_context_reason_is_none_after_delivery`.
   Green; mutation-probed (the held-gate dropped → the decision's
   homonymous reason leaks → RED). Receipts:
   `.measurements/docs-pin-reason-after-delivery.json` +
   `-mutred.txt`.
2. **THE INVENTORY**: `docs/taskqflow-coverage.md` — 24 shipped
   capabilities × [example | guide | pins | use-case]; the gaps NAMED
   (saga typed surface, named query handlers, subflows, event-time
   windows, multi-key channels, token-into-hold streaming) each with
   its disposition home; the stuck-running class recorded as
   structurally impossible, not a gap.
3. **THE GUIDES**: `docs/guides/agent-fleet.md`,
   `docs/guides/event-pipeline.md`, `docs/guides/task-stacks.md` —
   the worked examples walked line-by-line, each guide opening on the
   five-minute path; every fence verified (see the receipts).
4. **THE CATALOG**: `docs/guides/patterns.md` — the parity table (the
   pattern / as the systems name it / the face here / the link); the
   headline: ONE hold row, FIVE mechanisms (signal-and-wait,
   suspend-resume, deferrable-open-slot, poll-until-true,
   checkpoint-interrupt).

## THE NAMING LAWS' FINAL SHAPE (both amendments honored)

- The consumers'/fleet members' names: NOWHERE in docs/ or examples/
  (scrubbed: `cennan`/`TAStack` → the host's own CLI and worker
  entrypoints; the fleet-vote anecdote → the problem stated plainly).
- The orchestrators' names: WELCOME in the feature-parity frame
  (Temporal/Argo/Airflow/LangGraph/Prefect/Dagster/Kafka/Flink named
  where a FEATURE is the point). The migration page:
  `docs/guides/migrating-graph-checkpoints.md` (the short feature map +
  the verified port), the stub kept at the old path.
- The scrub list (file → names removed → reframe): `jobs-clients.md`
  (prefect's Late state → the stale-state lesson, generic);
  `architecture.md` (cennan/TAStack → the host's own entrypoints;
  dramatiq #791 → the queue-library conflation);
  `design/workflows-spec.md` (the five products → the generic
  mechanism nouns, the "no competitor" claim → the shape claim);
  `REVIEW-taskqflow.md` (the two name lists → the generic references +
  the rename note — the record never erased, the correction appended).

## THE VERIFICATION RECEIPTS (all at the final head, the lane's PG)

| Gate | Result |
|---|---|
| mkdocs strict | exit 0, 0 warnings — `runs/mkdocs-strict-docslane-20261010T113000.txt` |
| ruff check + format --check | pass, 1684 files |
| pyright FULL (src/taskq tests) | 0 errors, 0 warnings |
| the type gate | 38 markers red on BOTH pinned checkers, no strays |
| the fast tier x1 at -n 8 | **9615 passed, 3 skipped** — `runs/fasttier-final-20261010T110500-docslane.txt` |
| the docs-examples tier x2 | 93 fences ×2 green |
| the examples' test families x2 | 67 ×2 green (demo_legs, hitl_web_demo, doc_ingest, smoke, wiring, attack_demo) |
| the rv2 pin pack | 17/17 ×2 — `runs/rv2-pins-final-20261010T112000-docslane.txt` |
| the evidence-heads verifier | holds — `runs/evidence-heads-verify-20261010T111500-docslane.txt` |
| the phantom-API guard (no-exec APIs) | 6/6 |

## THE ESTATE'S HONEST STATE (recorded, not gamed)

- `wf-scoped-coverage`: **89.94 vs floor 90 — RED, identically at the
  lane's base f7155214** (327 tests there vs 328 here — the +1 is the
  folded pin). The prior live claim's 91.5 was recorded on dbd5d031's
  tree. The fresh red capture is the live claim; the cure is not this
  lane's to fake.
- The sibling boxes' future-dated claims (20261011 stamps) are marked
  SUPERSEDED-BY (the 874e9e6f precedent); the marks' target:
  `runs/docs-lane-rv2-13-cure-captures-20261010T103000.txt`.
- The docs lane's first fast-tier attempt (the rv2-13 circularity red)
  is recorded: `.measurements/docs-lane-fast-tier-n8-20261010.txt`.
