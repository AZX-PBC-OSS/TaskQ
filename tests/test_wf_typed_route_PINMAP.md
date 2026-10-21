# PINMAP — the T27 typed-route pin pack

File: `tests/test_wf_typed_route_pins.py` (this pack). Tree pinned at the
lane's source-final head `R` (see the report; the two-step shape: the
claims stamped at R, the measurement commit M carries them fresh via the
off-by-one rule). House fixtures: `wf_conn` / `wf_schema` /
`module_pg_pool` / `wf_sql` (`tests/_wf_fixtures.py`); the PG lane via
`TASKQ_TEST_PG_DSN`. Integration pins carry `@pytest.mark.integration`;
the compile-door pins carry no marker (pure compile). The red receipt:
`.measurements/t27-typed-route-reds.txt` (11 red at the base head
`d0491207` — the surface absent, every lazy import raising
`ImportError`).

| Rule | Pin(s) | Status at the base head d0491207 | The red observed (unmarked) | The SAFE behavior asserted (what flips it) |
|---|---|---|---|---|
| R1 door 1 (the wiring verb) | `test_r1_a_non_total_route_refuses_at_the_wiring` | red (xfail-strict framing: the pack ran plain — the reds captured, the build lands the flip) | `ImportError: cannot import name 'RouteArm'` | `route(...)` over `list[ImageItem \| AudioItem]` dropping `AudioItem` refuses at `app.get` with a `WorkflowBuildError` NAMING `AudioItem` — the element it drops would route NOTHING |
| R1 door 1 | `test_r1_an_unknown_arm_key_refuses_at_the_wiring` | red (same absence) | the same `ImportError` | a `Foreign` member keyed in the arms refuses NAMING `Foreign` |
| R1 door 2 (the validator) | `test_r1_e15_route_totality_reds_the_validator` | red | the same `ImportError` | `E15-route-totality` re-proves the totality from the COMPILED graph (the probe seam `app._compile` + the injection — E3's precedent), BOTH directions: the missing member (leg 1) and the unknown member (leg 2), each named |
| R1 door 3 (the runtime) | `test_r1_the_video_element_dies_loud_at_runtime` | red (integration) | the same `ImportError` | THE LIVE CONVICTION INVERTED: the source body that lied (a `VideoItem` element inside the declared `ImageItem \| AudioItem` union) → the source row terminal-FAILED `error_class='RouterNotTotal'` NAMING `VideoItem`, ZERO child rows routed (the skip-silent shape is dead), the run FAILED — never succeeded-having-routed-nothing |
| R2 (the join-back) | `test_r2_the_routed_results_join_addressable` | red (integration) | the same `ImportError` | the derived `media.join` row FIRES with the arms' returns packed (the typed sum: 2 image results + 2 audio results, self-discriminating), and `runner.result()` carries it |
| R3 (the decode) | `test_r3_the_arm_bodies_receive_their_declared_models` | red (integration) | the same `ImportError` | both arms' bodies receive their DECLARED models (the attribute access + the arms' own returns on the rows) — the chain's dict face (the convicted gap) never exists at the graph level |
| R3 (the arm contract, compile) | `test_r3_a_duck_typed_arm_param_refuses_at_build` | red | the same `ImportError` | E15 refuses the `dict`-param arm, the rule `E15-route-totality` + the arm's name in the message |
| R3 (the arm contract, compile) | `test_r3_an_unrelated_arm_param_refuses_at_build` | red | the same `ImportError` | E15 refuses the arm declaring the unrelated model, both type names in the message |
| R4 (the placement) | `test_r4_the_child_rows_stamp_their_arms_placement` | red (integration) | the same `ImportError` | the ROWS: `media.item:<ImageItem-tag>` on the gpu queue (elements 0, 2), `media.item:<AudioItem-tag>` on io (1, 3) — the fork's per-child actor/queue stamp |
| the e2e | `test_e2e_the_reviewers_scenario` | red (integration) | the same `ImportError` | the whole scenario: the images/audio map → the typed route → the two placements → the typed-sum join → the downstream summary `{"images": 2, "audios": 2}` ON result(); the rows are the receipt |
| the map face | `test_map_source_dict_form_is_the_same_machinery` | red | the same `ImportError` | `map_source(src, {A: fn_a, B: fn_b})` lowers onto the SAME attachment (`map_arms` set, `map_item` None, the `media.join` node present) — one machinery, two spellings |
| THE WORKED EXAMPLE (the maintainer's amendment 1) | `test_mime_route_e2e_the_mixed_corpus` | red (integration) | the same `ImportError` | `examples/doc_mime_route.py` LIVED: 2 text docs + 2 image docs + ONE unsupported mime → the text children on cpu, the OCR children on gpu, the dead-letter arm holds `doc-5` (the envelope recorded, the flow lives) → the report `{indexed: 12, dead: ["doc-5 (application/x-unknown: unsupported mime)"]}` ON result(). THE DEAD-LETTER'S DECODE FACE IS R3 AT THE FAN-IN: the chunk body's `isinstance` narrow proves the sum arrived as MODELS (dicts would index 0 — a silent sum) |
| THE BARRIER (the maintainer's amendment 2) | `test_barrier_chunk_fires_once_after_the_last_element` | red (integration) | the same `ImportError` | the staggered arms (the OCR's 150 ms stall): EXACTLY ONE chunk ledger claim, its `created_at` ≥ the LAST child's `updated_at` (the event order FROM THE LEDGER), the `wf_join_fire` row count = 1 (the exactly-once fire), the FULL batch on the chunk's row |
| THE BARRIER (fail_closed) | `test_barrier_fail_closed_the_chunk_never_fires` | red (integration) | the same `ImportError` | the failing OCR arm (max_attempts=1, the route's default fail_closed): the join blocked with `blocking_reason='failed_parent'` + the failed parent's id ON THE ROW, the chunk NEVER fires (no finished_at), the run FAILED |
| THE BARRIER (maybe) | `test_barrier_maybe_the_chunk_fires_on_the_survivors` | red (integration) | the same `ImportError` | `on_failure="maybe"`: the failing children absorbed (the join's `metadata.failures` carries BOTH with `policy='maybe'`), the chunk fires ONCE on the SURVIVORS (doc-1/doc-3 in, doc-2/doc-4 OUT of the sum), the flow lives (succeeded) — the partial-success law's fan-in face. **THE CURE THIS PIN FOUND**: the absorbed fire's join packed the failed children's slots as junk `{}` (undecodable — the chunk died `ValidationError`); the cure: the parent-results read excludes terminal-FAILED parents on ABSORBING edges (the absence is on the record — the envelope) |

## The type gate (the corpus: `tests/typeprobe/wf_graph_route_negative_types.py`)

| Probe | The face | The checkers |
|---|---|---|
| `probe_string_key_on_route_refused` | a STRING key on a route dict — the key is neither a union member nor any type | reds BOTH: `reportArgumentType` (pyright) / `invalid-argument-type` (ty) |
| `probe_the_green_face` | the type-keyed arms + the placement | clean |
| `probe_the_route_promise_type` | the heterogeneous sum: R solves to `ImageResult \| AudioResult`, the build fn's declared return pins it | clean |
| `probe_the_homogeneous_barrier_type` | THE BARRIER'S EDITOR FACE: the homogeneous arms solve R = `ExtractedText` exactly, pinned on the build fn's declared return (a drifted arm reds the RETURN — the list's invariance; pyright's face — ty's solving is looser here, recorded honestly; the RUNTIME decode is the enforcement on every checker) | clean (the drift reds — probe-verified: 2 pyright errors on the drifted arm) |

The gate: `uv run --no-sync python tests/typeprobe/_gate.py` — **39
markers** (38 at the base + the route's string-key marker), every marker
reding on pyright 1.1.414 AND ty 0.0.85 with its declared rule-ids, no
error outside the markers.

## The E15 reference row

The API reference (`docs/api-reference/workflows.md`) carries the
`E15-route-totality` row — `test_wf_attack_loop_docs.py::
test_docs_the_api_reference_covers_every_shipped_validate_rule` parses
`_validate.py`'s literals and refuses a shipped rule the reference
omits (the pin that caught E15's first landing).

## The red-first receipt

`.measurements/t27-typed-route-reds.txt`: `head=<the red-run head>`
(the pins commit on the ticketed base), `11 failed in 1.00s` — every
pin red on the absent surface BEFORE the build commit (`4d7d11f1`).
The greens at the lane head are the built code's evidence.
