# T27 — THE TYPED ROUTE AT THE GRAPH LEVEL: `route(promise, arms)` — the union's members as wiring keys, total or refused

Lane: `feat/taskqflow-typed-route` off `feat/taskqflow-routing-proof` @ `d0491207`
(the type-tagged `Route` foundation — the chain-surface cure + the 38-marker
type gate — is THERE). Worktree `/tmp/opencode/wt-route2`; own PG on `:5774`
(`postgresql://postgres:taskq@localhost:5774/taskq`, `POSTGRES_PASSWORD=taskq`,
`POSTGRES_DB=taskq`, `max_connections=1000`, `fsync=off`, the `taskq` role),
destroyed at close. Push THIS BRANCH ONLY (`feat/taskqflow` is the
consolidator's; the PG17 lane's branch is its own).

Design-first: THIS ticket is the 30-minute read's output; every claim below
was verified against the tree at the base head before a line of code.

## THE CONVICTION (the reviewer's, lived on the routing-proof corpus)

1. **The chain's route works but the chain is graph-INVISIBLE.** A chain's
   step rows exist only from the emit onward — no `NodeDecl`, no fan-in, no
   join-back (T20's law: "the run completes from the rows — never a join").
   The chain items ride untyped (`item: dict`), one placement per chain
   (`Chain.actor`/`Chain.queue` stamp EVERY row), and the dispatch vocabulary
   is the enum middleman. A per-element fan-out that CONVERGES (the map's
   join) is unwritable as a chain; a chain spelled as a DAG arm drags the
   graph into the per-record stream (the guide's own decision paragraph).
2. **The graph DSL's `skip=`-predicates are STRINGLY.** The predicate receives
   the flow's state — decoded JSON dicts — and evaluates a `bool`. The live
   conviction: a `"video"` tag skipped BOTH arms and the run terminalized
   SUCCEEDED having routed NOTHING — the exact silent drop
   `RouterNotTotal` exists to prevent, alive at the graph level. The typed
   fence must replace the predicate's teaching; the predicate's face is
   deprecated honestly (never silently removed — the battery's corpus rides
   it).

## THE DESIGN (the reviewer's prescription, verified against the tree)

### (a) The wiring verb — `route(promise, arms)`

A NEW graph verb beside `map_source` (`api/_graph.py`), exported through
`api` and the root:

```python
route[S, R](
    source: Promise[S],
    arms: dict[type, RouteArm | Callable[..., Awaitable[R]]],
    *,
    queue: str = "default",
    on_failure: EdgeFailurePolicy = "fail_closed",
    max_attempts: int = 3,
    aggregate: Callable[[list[Any]], object] | None = None,
) -> Promise[list[R]]
```

`RouteArm` (frozen/slots, beside `GateDecl`): `body: BodyFn`,
`actor: str | None = None`, `queue: str | None = None` — the per-arm
placement override (R4). A bare `Callable` arm is the `RouteArm(body=fn)`
shorthand. The union MEMBERS key the dict — the type-tagged `Route`'s
machinery (`chain._type_tag`, the `module.qualname` tag) extended to the
graph level.

`map_source(src, {A: fn_a, B: fn_b})` — the SAME machinery: the per-element
case spelled at the map face (the reviewer's (b)). `map_source` gains the
dict form by overload; both spellings lower through ONE
`_attach_route(source, arms, ...)` helper. A source carries the attachment
ONCE (`map_item` and `map_arms` are mutually exclusive — a node finalizes
once, one fork; the double-attach is the build refusal, the existing
double-map error's wording extended).

The lowering stamps the source's `NodeDecl`: `map_arms: dict[str, RouteArm]`
(keyed by type-tag) + the map-fork carrier fields it already owns
(`map_queue` = `queue=`, `map_max_attempts`, `map_on_failure`,
`map_aggregate`) — the route IS a map attachment (per-element fork + join);
`map_item` stays `None` and `map_arms is not None` IS the route marker. The
derived join node `<source>.join` (kind `map_join`, bodyless — the default
identity packer) is created exactly as the map's is: **R2's join-back is BY
CONSTRUCTION** — every routed child is a fork child with an edge to the
join, the join packs the arms' returns (the typed sum), and the flat
`Promise[list[R]]` carries it (R inferred from the arms' bodies' returns;
heterogeneous arms solve R to the union).

### (b) The route child rows ARE graph nodes

At the source's success-finalize (`_runner.py`), the route fork replaces the
map fork when `map_arms` is set: per element, the element's RUNTIME type tag
picks its arm, and the child row is

```python
ChildSpec(
    step_key=f"{source}.item:{tag}",      # the arm's OWN derived key
    actor=arm.actor or node.actor,        # R4: per-arm placement
    queue=arm.queue or node.map_queue,    # R4: processA on gpu, processB on io
    payload={"wf_item": <the element, jsonb>},
    map_index=i,                          # the element's index (the arbiter's discriminator)
)
```

The per-arm step key (`<source>.item:{tag}`) is the design's load-bearing
choice: the row's `step_key` NAMES the arm (the ledger receipt is direct),
the body resolves from the registry under it (D1 — `_register_bodies`
registers each arm body under its child key), and the idempotency key +
the claim arbiter discriminate normally (`map_index` rides every child).
The `.item:` segment keeps the map's derived namespace (the dot-law's
spirit: the fork owns the derived keys); the progress-schema inheritance
and the read-side item filter learn the route child's shape.

The runtime no-arm element (a body that returned a type outside the declared
union) raises `RouterNotTotal` — THE SAME class the chain face uses —
BEFORE any row is written: the source terminal-FAILS with
`error_class='RouterNotTotal'`, the run FAILS, nothing routed. The
skip-silent shape is DEAD at the graph level (R1's runtime door).

### (c) R3 — the arms' bodies receive the DECODED TYPED MODELS

The arm body's declared param type IS the decode's target: the child's
payload element is the jsonb dict; the runner's existing item-codec path
(`ITEM_KEY in payload` → `_coerce` → `coerce_arg`) validates it into the
arm body's annotation — the chain's dict face (the convicted gap) never
exists here. A mis-declared arm (the body claiming the WRONG arm's model)
dies LOUDLY in the coercion (`ValidationError`, the row fails, named) —
the chain lane's misroute drill, now at the graph level.

The arm-param contract is also a COMPILE conviction (E15's walk): an arm
body whose first param beyond `ctx` is un-annotated / `Any` / a plain dict
(the duck-shaped hole, E5's own conviction shape at the consumer face) is
refused — the route is the typed boundary, and a duck arm is the hole the
route exists to close. A param declaring an UNRELATED model is refused too
(the wiring promises data the arm cannot accept).

### (d) R4 — the per-arm placement

`RouteArm(queue="gpu")` / `RouteArm(queue="io")` stamps the fork's per-child
`actor`/`queue` (the `ChildSpec` carries both per child already — the
certified fork inserts them per row). The ROWS are the receipt: each route
child row's `queue` column reads its OWN arm's stamp.

### (e) R1 — THE TOTALITY FENCE AT THE GRAPH LEVEL (E15)

The validator's next free id is **E15** (E1–E14 are taken — read from
`_validate.py`). `_rule_route_totality` walks every `map_arms` node and
re-proves, checker-independently, from the compiled graph:

* the source body's declared return resolves to `list[<union of models>]`
  (an unresolvable hint SKIPS — the zero-false-positive doctrine; a
  non-list return is the refusal: the route is a map face);
* the arms' keys are EXACTLY the union's member types — **missing member =
  the build refusal naming it; unknown member = refused** (the message
  carries the house vocabulary: "the element it drops would route
  NOTHING — the silent drop the route exists to refuse");
* every arm body satisfies the typed-param contract (c).

The wiring verbs refuse the mechanically-impossible at the wiring site
(the map_source precedent): an empty arms dict, a non-type key, a
non-model union member, the double attachment. E15 re-proves the fence
from the compiled graph itself — the compiled graph is public, mutable
data (E3's own precedent), and the rule owns the shape injected into it.

The `skip=` predicates' stringly face: DEPRECATED in the docs — the guide's
routing section teaches the typed route; `SkipPredicate`'s docstring names
the conviction (the `"video"` tag that skipped both arms and
succeeded-having-routed-nothing). No code removal — the battery's corpus
rides the predicate; the deprecation is honest.

## THE PINS (xfail-strict, red-first — `tests/test_wf_typed_route_pins.py`)

| Rule | Pin | The red observed (at the base head) | The flip (the SAFE behavior) |
|---|---|---|---|
| R1 wiring | `test_r1_a_non_total_route_refuses_at_the_wiring` | `route(...)` absent → `ImportError`/`AttributeError` (red) | the verb refuses naming the dropped member (`AudioItem`) |
| R1 wiring | `test_r1_an_unknown_arm_key_refuses_at_the_wiring` | red (same absence) | refused naming the foreign member |
| R1 validator | `test_r1_e15_route_totality_reds_the_validator` | red | `validate_compiled` raises rule `E15-route-totality` on the injected non-total graph |
| R1 runtime | `test_r1_the_video_element_dies_loud_at_runtime` (integration) | red | the "video" element's run FAILS: source row `error_class='RouterNotTotal'`, no child routed, never succeeded-having-routed-nothing |
| R2 | `test_r2_the_routed_results_join_addressable` (integration) | red | the `<src>.join` row fires; its result carries the arms' returns (the typed sum); `result()` reads it |
| R3 | `test_r3_the_arm_bodies_receive_their_declared_models` (integration) | red | attribute access on the declared models works in BOTH arms; the arms' rows carry the arms' own returns |
| R3 | `test_r3_the_mis_declared_arm_dies_loudly` (integration) | red | the wrong-model arm's row fails with a named coercion error |
| R3 | `test_r3_a_duck_typed_arm_param_refuses_at_build` | red | E15 refuses the `dict`-param arm, naming the arm's key |
| R4 | `test_r4_the_child_rows_stamp_their_arms_placement` (integration) | red | the image children's rows queue `gpu`, the audio children's rows queue `io` |
| the e2e | `test_e2e_the_reviewers_scenario` ×2 (integration) | red | the images/audio map → the typed route → the two placements → the typed-sum join; the rows are the receipt, ×2 |

Plus the honest STATIC face in the typeprobe corpus
(`wf_graph_route_negative_types.py`, wired into `_CORPUS`): a STRING key on
a route dict reds both checkers (`reportArgumentType` / `invalid-argument-type`)
— the Route literal's own discipline, now at the graph level; the green
type-keyed face stays clean. The gate's marker count moves (38 → the new
count) — recorded in the receipts.

## THE PROOF (the battery at the stamped sha)

The e2e (the reviewer's exact scenario) ×2 + the wf battery ×2 (leg 1 with
`--cov=src/taskq/workflows --cov-branch` + the scoped coverage gate
`scripts/check_wf_coverage.py` at the 90.0 floor; leg 2 plain) + the type
gate (`tests/typeprobe/_gate.py`) + ruff check / ruff format --check /
pyright FULL 0/0/0 + the fast tier ×1 at `-n 8` under the CI's lane filter
(`-m "not slow and not load_sensitive"`, full capture) + `mkdocs build
--strict`. Red-first: the pins ran against the base head BEFORE the build;
the receipts live in `.measurements/` (head-stamped).

## THE DOCS

`docs/guides/workflows.md`: §1 gains THE TYPED ROUTE (the `route()` verb,
the `RouteArm` placement, the `map_source` dict form) in the flow API's
concept list; the dispatch-time-predicates paragraph names the skip=
conviction honestly (deprecated — the typed route is the taught face); the
§10 router section gains the graph-level route beside the chain's (the
"Chain or DAG?" decision guide extends: converge-and-dispatch is the
route's shape).

## THE REPORT

The ticket (this file), the landed surface's signatures, the pins (names +
red receipts), the battery numbers, the branch's head, and the VERIFIED
SNIPPET for the PR body. The work is evidence, never verdict.
