# PINMAP — the rv5-cache pin pack (the T25 cross-run step-cache hostile review)

File: `tests/test_wf_step_cache_rv5_pins.py` (drop-in at the branch's
`tests/`; sha256
`1ddb30890f80d5980f85ab4d3c609e72c5df44c516973a39a665eec89373f033`;
ruff-clean and pyright-clean against the repo configs — see
RECEIPTS.md's Environment leg for the venv/PYTHONPATH seam).
Tree pinned at `0839019c1` (feat/taskqflow-cross-run-cache), based on
`37ab05b8`.
House fixtures used: `wf_conn` / `wf_schema` / `wf_pool` /
`module_pg_schema` (tests/_wf_fixtures.py via tests/conftest.py);
`TASKQ_TEST_PG_DSN` for the PG lane. The four PG pins are marked
`integration`; the three build/docs pins carry no marker. The async PG
pins pick up the estate's always-on G7 rows↔status teardown
automatically (file prefix `test_wf_`): the wedged state F-K1's red
leaves behind (a `running` root over a succeeded source + a pending
consumer) reconstructs `running` — agreement, no false teeth; both runs
carried zero teardown errors.

Convention for the "red verbatim" column: the pin's OWN failure
signature's first lines in the unmarked run (`red-run-raw.txt`) — the
assertion/raising line and the first `E`-line carrying the conviction.

| Finding | Pin name | Red verbatim (first 2 lines) | Status |
|---|---|---|---|
| F-K1 (MERGE-BLOCKER; cached route source's hit wedges the run forever) | `test_k1_a_cached_route_source_is_refused_or_the_hit_reforks` | `assert outcome == "terminal", (`<br>`E   AssertionError: F-K1: the cached route source's HIT wedged the run (drive returned 'max_ticks') — the hit's finalize never rebuilt the route fork: the children never spawn, the consumer's dep never releases, the run is a 'running' corpse over a succeeded source` | xfail-strict (integration) |
| F-K2 (the pins' own hole: the address's input leg) | `test_k2_two_inputs_to_one_cached_body_miss_each_other` | — (GREEN at head; the mutation drill reds it — `repro/k2-mutation-drill.txt`: `E   AssertionError: F-K2: one body over two inputs hashed to ONE address — the input leg is absent from the address`) | **GREEN guard** (integration) |
| F-K3 (cached body reading ctx.input → cross-tenant leak as terminal success) | `test_k3_a_cached_body_reading_ctx_input_is_a_build_diagnostic` | `assert any("cache" in d.message and "input" in d.message for d in diagnostics), (`<br>`E   AssertionError: F-K3: no build-time diagnostic named the cache+ctx.input hazard — the cross-tenant leak ships silently. validate() said: []` | xfail-strict |
| F-K4 (the closure hole's law is nowhere written) | `test_k4_the_docs_carry_the_closure_holes_law` | `assert "helper" in lowered, (`<br>`E   AssertionError: F-K4: the guide's cache section never names the closure hole — the law 'the address covers the body's own source; a changed HELPER does NOT move the address' is nowhere written` | xfail-strict |
| F-K5 (cache+Exit silently never stores) | `test_k5_cache_on_an_exit_returning_body_refuses` | `with pytest.raises((WorkflowBuildError, WorkflowValidationError)) as exc_info:`<br>`E   Failed: DID NOT RAISE any of (WorkflowBuildError, WorkflowValidationError)` | xfail-strict |
| F-K6 (no eviction door) | `test_k6_the_cache_purge_verb_evicts` | `assert result.exit_code == 0, (`<br>`E   AssertionError: F-K6: \`taskq cache purge <address>\` failed (exit=2) — the eviction door does not exist …` | xfail-strict (integration) |
| F-K7 (hit-finalize infra fault laddered as a body failure; the miss path's escapes as reclaim) | `test_k7_the_hit_finalize_infra_fault_is_the_reclaim_class` | `assert node["status"] != "failed", (`<br>`E   AssertionError: F-K7: the hit-finalize's infra fault terminal-FAILED the node (status='failed') — … one fault, two semantics` | xfail-strict (integration) |

## The SAFE behavior each pin asserts (what flips it XPASS-strict)

* **rv5-k1** — the route-source + cache combination is impossible: EITHER
  the wiring/validate door refuses it with the why named (the message
  carries "cache" and "route"/"fork"), OR the hit path rebuilds the
  route fork from the cached envelope — the second run's source row
  carries the receipt, the source body never re-runs, the arm children
  spawn and run, the `docs.join` row exists and succeeds, the consumer
  runs, the run terminalizes SUCCEEDED. Both cures satisfy the pin.
* **rv5-k2** — one cached body under two different wired inputs: the
  second input MISSES the first's entry (the body re-executes), and the
  two calls hold TWO distinct addresses (computed through the runner's
  own `_body_code_version` seam, the versioning pins' idiom). Already
  green; the mutation drill (the input leg dropped from
  `cache_address`'s doc) reds its FIRST assertion.
* **rv5-k3** — a `cache=True` body whose source reads `ctx.input`
  produces a build-time diagnostic (a `validate_compiled` entry whose
  message names the cache + the run-input) or a refusal naming the same.
* **rv5-k4** — the guide's cache section (and/or the api-reference)
  states the closure law: the address covers the body's OWN source; a
  changed HELPER (a module-level callable the body calls) does NOT move
  the address. The pin anchors the cache section, requires the surviving
  own-source claim, the word "helper", and a nearby negation binding the
  helper to the address.
* **rv5-k5** — a body whose declared return is `Exit[...]` under
  `cache=True` refuses at `app.get` (WorkflowBuildError or
  WorkflowValidationError) with the combination named.
* **rv5-k6** — `taskq cache purge <address>` exits 0, the named row is
  gone, and the next run MISSES (the body re-executes). The pin invokes
  the CLI through typer's CliRunner on the module's migrated schema (the
  house's `cli_settings` env idiom + `to_thread` — the CLI's verbs
  `asyncio.run`).
* **rv5-k7** — the infra-class fault (ConnectionError) at the HIT's
  finalize is the reclaim class: the node never terminal-fails, zero
  `failed` ledger rows land, and the cleared-fault re-drive completes
  the run SUCCEEDED via the hit (the body still never ran in the hit
  run — the receipt on the row proves the hit path). The pin tolerates
  every honest cure shape (the escape-to-machinery face, the in-runner
  reclaim classification, the re-pend-and-heal face) because it asserts
  the ROW STATE + the reclaim's proof, never the mechanism.

## Decision flags for the maintainer (the directions I chose where the
## finding left the rule open)

1. **F-K1's either-cure** — the pin is written to pass under EITHER cure
   (the refusal OR the re-fork), per the finding's own "either cure
   satisfies". Under the refusal cure the pin short-circuits at
   `app.get`; under the re-fork cure it runs the two-run drill. A cure
   that refuses at `route()` attach time (the source already cached) or
   at `step()` (a future route source unknowable) both land inside
   `app.get` — the pin's `try` covers both.
2. **F-K3's rule seat** — pinned as the build-time diagnostic (the
   finding's first direction, the house's W-rule doctrine). If the
   maintainers rule docs-only, rewrite this pin to pin the law's
   verbatim presence in the guide's cache section (the F-K4 pin's own
   pattern) — the live conviction (the cross-tenant leak) stands either
   way; only the cure's surface changes.
3. **F-K4's stronger leg NOT pinned** — the finding's optional validator
   note (a cache=True body calling module-level callables warns) is not
   pinned; the docs pin is the honest minimum. If the lanes cure at root
   (transitive hashing — a helper's change moves the address), this pin
   must flip to the NEW law's text (the docs would then state the
   closure IS covered) — the pin's docstring says so.
4. **F-K5's direction** — pinned as the wiring refusal (the finding's
   likely cure). The Exit-annotation face is compile-visible
   (`-> Exit[Report]` resolves through the hints walk), so the refusal
   is implementable at the door. If the maintainers instead make the
   store happen (the exit envelope cached + the hit replaying the
   skip-record through `_finalize_exit`), rewrite the pin: run 1 stores,
   run 2 hits, the downstream rows carry the SAME skipped-with-record
   shape. NOTE for that alternative: today the hit path finalizes
   through `_finalize_success` — a cached Exit envelope replayed as a
   plain success would resurrect the downstream the Exit skipped (the
   F-K1 class wearing the Exit hat); the refusal is the clean law.
5. **F-K6's face** — the pin invokes the CLI verb (`taskq cache purge
   <address>`), the finding's natural face. An admin-endpoint cure (the
   same semantics on the web surface) satisfies the LAW but not this
   pin's invocation — rewrite the leg to call the endpoint (the
   web_admin fixtures' idiom). The pattern form (`purge <pattern>`) is
   not pinned; the exact-address form is the minimum.
6. **F-K7's classification seat** — the pin asserts semantics, not
   mechanism. The miss path's behavior at this head (the fault escapes
   `drive()` raw; the fleet's machinery boundary classifies it infra →
   reclaim) is the reference; the pin requires the hit path to meet it.
   If the cure instead makes the hit path's finalize infrastructure
   fully inside the ladder's deterministic/infra classification (an
   in-runner reclaim arm), the pin's row-state assertions still hold.
7. **F-K2's mutation drill** — the drill mutated
   `cache_address`'s doc-shape leg; a mutation that drops the input at
   the CALLER (`api/_runner.py`'s `_cache_address(code_version,
   [encode_data_arg(a) for a in args])` → `[]`) reds the same guard
   through the CALLS==2 assertion instead (the second input hits). Both
   legs are the guard's teeth by construction.

## Unreproducible findings

NONE — all seven findings' live legs were convicted at `0839019c1`
before pinning (the pre-pin repro shapes + outputs are in `repro/` and
RECEIPTS.md). One artifact of MY OWN was corrected before pinning (the
convictions stand): the first heredoc-based probes ran bodies whose
source is unreadable (`<stdin>`), so the §22.1 stamper recorded
`code-version-unstamped` and the cache silently never engaged (the
`step-cache-unaddressable` warning — itself the honest face of an
unverifiable body); the file-based probes (real module-level sources)
convicted. The pin file's bodies are all module-level for exactly this
reason.
