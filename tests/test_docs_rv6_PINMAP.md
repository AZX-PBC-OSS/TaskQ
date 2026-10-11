# PINMAP — the rv6-docs pin pack (the docs-currency convictions)

File: `tests/test_docs_rv6_pins.py` (drop-in at the branch's `tests/`;
sha256 `a6f2001ef8fe22557fde1438c04fbe9e31a7041a86fc249e118e7ab4424ca846`;
ruff-clean and pyright-clean against the repo configs — see RECEIPTS.md's
Environment leg for the venv/PYTHONPATH seam).
Tree pinned at `6e2297fe87ae2b1ad273c537f30fe7d82aa7019a`
(fix/docs-currency).
Fixtures: NONE beyond the filesystem (the pack is static — the estate's
own text plus a mechanical walk of THIS tree's `taskq.workflows.__all__`).
No markers: the pins are sync and container-free.

Convention for the "red verbatim" column: the pin's OWN failure
signature's first lines in the unmarked run (`red-run-raw.txt`) — the
assertion line and the first `E`-line carrying the conviction.

| Finding | Pin name | Red verbatim (first 2 lines) | Status |
|---|---|---|---|
| F-DOCS-1 (the wiring-verbs table omits `route`) | `test_docs1_every_wiring_verb_in_all_appears_in_the_table` | `assert not missing, (`<br>`E   AssertionError: F-DOCS-1: public wiring verbs missing from the api-reference table: ['chain_source', 'route'] — …` | xfail-strict |
| F-DOCS-2 (FanInBoundExceededError nowhere in docs/) | `test_docs2_the_fan_in_bounds_error_class_is_named_with_its_remedy` | `assert "FanInBoundExceededError" in section_text, (`<br>`E   AssertionError: F-DOCS-2: the fan-in bound's error CLASS is named nowhere in the guide (nor anywhere in docs/) — …` | xfail-strict |
| F-DOCS-3 (the single-value Promise[A\|B] disposition unstated) | `test_docs3_the_single_value_route_disposition_is_named` | `assert disposition is not None, (`<br>`E   AssertionError: F-DOCS-3: the single-value Promise[A\|B] disposition is unstated — route() is taught only over list sources …` | xfail-strict |
| F-DOCS-4 (the holdmap runbook's "five `wf_*` tables" lists six + jobs) | `test_docs4_a_wf_table_count_claim_matches_its_list` | (no red at this tree — the cited text is absent; the guard passes vacuously with the §7 witness holding) | GREEN guard (the anti-rot posture — see the unreproducible note) |

## The SAFE behavior each pin asserts (what flips it XPASS-strict)

* **rv6-docs1** — every public name in `taskq.workflows.__all__`
  resolving to a FUNCTION homed in the wiring layer
  (`taskq.workflows.api._graph` / `taskq.workflows.api._loop`) appears as
  a row in the api-reference's "The wiring verbs" table. The witnesses:
  the section exists; the known verbs `{step, gather, build}` are
  present (a deleted/renamed table reds loud, never vacuous). The cure
  adds the missing rows (`route` at minimum — see flag 1).
* **rv6-docs2** — the guide's fan-in bound section NAMES
  `FanInBoundExceededError` beside its remedy. The witnesses: the
  section exists and still teaches `child_driven` (a section rename or a
  remedy deletion reds loud). The class name need only appear inside the
  section's own text — any sentence shape.
* **rv6-docs3** — the guide names the single-value disposition
  explicitly: a passage ties the single-value / non-list case to its
  disposition (the chain step's in-body `Route`, or `route()`'s own
  build refusal of the shape) — in either sentence order. The witness:
  `route(source, arms)` is documented (a verb-section deletion reds
  loud). The pin takes no position on WHICH disposition the docs teach,
  only that the reader of a `Promise[A|B]` is told where it goes.
* **rv6-docs4** — every "N `wf_*` tables" count claim in `docs/`
  (number word or digit) is followed, within its own block, by a table
  listing exactly N `wf_*` rows. Zero claims is a lawful green (the law
  convicts claims, not absence); the guide's §7 existence is witnessed.

## Decision flags for the maintainer (the directions I chose where the
## finding left the rule open)

1. **F-DOCS-1's second conviction** — the mechanical walk convicts
   `chain_source` (homed in `_graph.py`, wires a graph node under the
   builder — a wiring verb by the pin's definition) beside the finding's
   named `route`. If the maintainers rule the T20 chain source belongs
   to the streaming section's OWN docs rather than the wiring-verbs
   table, the honest shapes are: add its row anyway (the table is the
   verbs' census), or narrow the pin's verb set with the exemption
   STATED in the table's intro. The pin as written holds the finding's
   letter ("every public verb in __all__ that is a wiring verb appears
   in the table").
2. **F-DOCS-1's runtime faces excluded by construction** — `run`,
   `cancel_workflow_run`, `retry_workflow_node` (`_run` / `_runner_exit`
   modules) and the emit helpers `chain_start` / `chain_fork`
   (`chain.py`, called from source bodies and the engine, never wiring a
   graph node) are NOT wiring verbs under the pin's module-seat
   definition. If a cure lands a verb in a NEW module, extend
   `_WIRING_MODULES` with it — flagged.
3. **F-DOCS-2's seat** — pinned at the guide's fan-in bound section (the
   remedy's home). The api-reference's `exceptions.md` (or a workflows
   error table) naming the class ALSO satisfies the finding's substance
   but NOT this pin (the pin wants the class where the operator reads
   the bound); if the maintainers prefer the reference-only seat, move
   the pin's read — flagged.
4. **F-DOCS-3's tolerance** — the regex accepts any order and any of the
   natural phrasings ("single-value", "non-list", "not a list", "list
   source" × "Route(", "chain", "refus", "map face"). A cure that
   teaches the disposition in the API-REFERENCE rather than the guide
   does NOT flip this pin (the finding's seat is the guide's
   route/conditional teaching) — flagged.
5. **F-DOCS-4's scope** — the law reads the count-claim's own block
   (claim → next heading) and counts `wf_*` table rows there; a block
   with NO table rows is not convicted (the claim might enumerate in
   prose — a subtler rot this cheap pin deliberately ignores, the
   finding's instance being the table shape). The `jobs` row is
   correctly NOT counted as a `wf_*` table (the convicted paragraph's
   own ambiguity — "five `wf_*` tables" listing six `wf_*` + `jobs`).

## Unreproducible findings (the severity note)

**F-DOCS-4 is UNREPRODUCIBLE at the prescribed tree.** The cited text
("served by five `wf_*` tables", `docs/guides/workflows.md:1830` area)
does not exist at `6e2297fe87ae2b1ad273c537f30fe7d82aa7019a` — the file
is 1613 lines and its §7 carries no table-count sentence. The paragraph
lives on the UNMERGED `feat/taskqflow-holdmap-cures` lane (introduced by
`1528719d2`'s docs leg — verified: `git show
1528719d2:docs/guides/workflows.md` line 1832 carries the sentence and
the six-`wf_*`-row table beneath it). This CHANGES the finding's posture
at this head, not its truth: the mismatch is real on the lane that owns
it, and the pin stands here as the GREEN guard that reds the day the
lane's paragraph merges with the mismatch intact (the docs-numbers
pattern, pre-seeded). If the review intends a LIVE conviction for
F-DOCS-4, the pin file must be dropped into the holdmap-cures lane's own
tree (the pin's law is tree-independent; it reds there today).

The other three findings are live at this head and were convicted by
inspection (the greps are the repro — RECEIPTS.md carries them); the
unmarked run reds all three exactly as designed.
