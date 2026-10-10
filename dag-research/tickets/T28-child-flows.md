# T28 — THE CHILD FLOWS: `ctx.spawn_flow` + the join hold — the sub-investigations lane

Lane: `feat/taskqflow-child-flows` off `feat/taskqflow` @ `16d24edd`
(the T25-merge tip; verified via `git -C /tmp/opencode/wt-land log --oneline -1`).
Design-first: THIS ticket is the 30-minute read's output; every claim below
was verified against the tree at the base head before a line of code.

The build order (#3): "a step that spawns a sub-flow and joins its result
(the agent spawning sub-investigations); the certified fork machinery is
the spine." THE ADOPTION LAW: "we are not boiling the ocean — we must have
what we need to adopt; key features missing/broken/untested = the effort
for nothing."

## THE DESIGN QUESTIONS, ANSWERED (the reads + the design law decide each)

### 1. THE SPAWN FACE — a body calls `ctx.spawn_flow(child_workflow_name, input=…) -> ChildHandle`

The child is a REAL flow: its OWN root row (`step_key='__flow__'`), its OWN
node rows, its OWN run-key claim — the run explorer sees it, the admin
operates it, the CLI lists it. NOT a hidden sub-graph, NOT a fork: the
fork's children are the SAME flow's nodes; a child flow is ANOTHER flow.

The spawn is the child's `create_flow` INSIDE the parent's step — the
create-seam's ONE-transaction law respected BY CONSTRUCTION: the child's
root + static nodes + edges + ROOT_START land in the child create's own
transaction (`FlowRunner.create_flow`'s existing shape, verified
`api/_runner.py:228-260`); the parent's step never wraps it in a second
transaction of the parent's own.

The API (the mixin's home: `workflows/_childflow.py`, composed into
`StepContext` beside `CtxWaitOps` — the §7b concerns-separate law):

```python
handle = await ctx.spawn_flow(
    "sub_investigation",  # the child's REGISTERED name
    input=SubProbe(topic="x"),  # the child's input (the root's payload)
    result=SubReport,  # type[M]: the child's typed result contract
)
```

The child's compiled graph resolves from the worker-execution registry
(`taskq.workflows._worker_execution.get_compiled_workflow` — the SAME
registry `app.get(name)` populates and the fleet door reads; verified
`_worker_execution.py:200-214`). An unregistered name is the TYPED
refusal (`ChildWorkflowUnknownError`), never a KeyError dragon.

The spawn's RUN KEY (the two-claims law at the flow level — G2's arbiter
is the rememberer): the child's run key is
`childflow:{parent_flow_id}:{node_key}:{slot}` under the child's OWN
scope `workflow-run:<child name>`. The SLOT is the spawn's per-site
identity, stable across retries:

* a LOOP-kind node: slot = the row's `iteration` (the ADVANCE write — the
  same counter the wait site's E13 law reads; one spawn per iteration —
  the shape law documented at the site);
* a plain node: slot = the per-attempt spawn cursor
  (`metadata.spawn_cursor_{attempt}` — the hold-cursor's exact pattern:
  a fresh attempt restarts at 0, so the retry re-derives the SAME slots
  and the arbiter ADOPTS the same children — never duplicates);
* an explicit `key=` argument overrides the slot (the author's identity).

The arbiter's verdicts flow through: `created` → the handle of a fresh
child; `existing-running` / `existing-terminal` → ADOPTION (the same
child, the same id). A terminal child adopted as-is is the DETERMINISTIC
REPLAY: the retried body re-decides off the same terminal child. A body
that wants a FRESH child per retry spells it: `key=f"probe-{ctx.attempt}"`.

### 2. THE JOIN FACE — the step that spawned WAITS on the child's terminal; the WAIT = the HOLD

```python
outcome = await handle.wait(timeout_s=600.0)
match outcome:
    case ChildOk(result=report):
        ...  # the child's TYPED result
    case ChildFailed():
        ...  # the child failed — the body decides
    case ChildCancelled():
        ...  # the child was cancelled
    case Expired():
        ...  # the JOIN's own patience expired
```

The wait is the HITL machinery's OWN shape (the certified hold): the join
site registers a hold on the PARENT's node (`register_hold` — signal name
`__child__:{child_flow_id}`, `payload_schema` = the declared result
model — the contract is mandatory at hold time, attack-4 F-P4's law;
`context.kind='child_join'` names the arm's query), marks the ledger
`awaited` (resume-not-retry — `mark_awaited`), and raises
`NodeHeldError` — the runner's EXISTING catch leaves the held
representation (pending + the deadline + the signal row as truth; NO
terminal, NO ledger failure) and the worker RELEASES THE SLOT (the slot
law — the child runs while the parent holds nothing but its row).

THE RESUME CONTRACT is the wait machinery's own: the body re-executes
FROM THE TOP — the spawn site re-runs and ADOPTS the same child (Q1's
key law); the join site then re-reads.

The wake — the child's terminal event is the hold's RESOLVER:

* **THE WAIT SITE READS THE CHILD ROW FIRST** (the row is the truth): a
  terminal child needs no hold at all — the site decodes the verdict and
  returns the typed member directly. This is what makes the resume face
  (and the crash-mid-wait face) pure row-replay: no queue, no cursor.
* **THE WAKE ARM** (`sweep_child_joins`, a NEW sweep arm in
  `workflows/_sweep.py`'s family, wired into `FlowRunner.tick` AFTER the
  root-maintain leg): finds held `__child__:%` holds, reads the child
  root's status; a terminal child → the hold is CAS'd `delivered` (the
  payload is THE POINTER — the child flow id — never the sole copy), the
  parent node's held marker clears and `scheduled_at=now()` (the
  `_DELIVER_RESUME_SQL` shape), and the arm broadcasts the T26
  transport's knock on `CHILD_TERMINAL_CHANNEL` (`pg_notify`, the
  pointer-only payload, the 8000-byte cap — `_broadcast` reused verbatim).
* **THE REAPER HALF** (the orphaned waiter): the child DIED — terminal
  `failed`/`cancelled`/`crashed`/`abandoned`, or the root row MISSING —
  the SAME arm delivers the same wake; the resumed body's join site reads
  the verdict and returns the fail-close member (`ChildFailed` /
  `ChildCancelled`). The union is CLOSED: `ChildOutcome[M] | Expired` —
  `ChildOk[M] | ChildFailed | ChildCancelled | Expired` — and the CHECKER
  forces the arms (a body matching only `ChildOk` falls through and reds
  against its declared return; the `Decision | Expired` pattern's
  sibling, exactly T26's amendment's law).
* **THE JOIN'S OWN TIMEOUT**: `timeout_s=` arms the hold's deadline (the
  DB clock, C8's precedence law — the WAIT's value wins, on the rows).
  The expiry sweep abandons the hold; the resumed site returns `Expired`
  ONCE per attempt (the face-marker pattern, `child_join_face_{attempt}`
  — the hold→expire→re-hold→∞ dragon's kill site, T26's amendment
  verbatim). `timeout_s=None` waits as long as the child lives — SAFE by
  the cascade (Q4): a cancelled parent cancels its children, so no join
  outlives its flow.

### 3. THE IDENTITY — the receipt in BOTH directions

* The parent's node row carries the child's flow id: `metadata.child_flows`
  jsonb array (an idempotent ensure-append — adoption re-runs must not
  duplicate the receipt). This is THE RECEIPT: the run explorer reads a
  node's spawned children off the row.
* The child's root row carries the parent's: `metadata.parent_flow_id`,
  `parent.parent_node_id`, `metadata.parent_step_key` — the provenance
  face (the estate's law): the child is answerable to its spawner from
  its OWN row, in any process, forever.
* Mechanism: `create_flow` gains an OPTIONAL `root_metadata_extra`
  passthrough (additive; `None` = the shipped shape byte-identical) —
  the stamps ride the create's ONE transaction (a post-create UPDATE
  would re-open the orphan window the create-seam closed).

### 4. THE LIFECYCLE — the cascade, the decision, the retry

* **THE PARENT CANCELLED → THE CHILD CANCELLED** (the cascade — the
  cancel's reverse-edge walk's extension): `cancel_workflow_run` collects
  the receipts (`metadata.child_flows` over the flow's rows) and cancels
  each child flow RECURSIVELY through itself (each child's cancel atomic
  in its own tx — the parent's tx commits first; the cascade is
  eventually-consistent per child, idempotent by the existing
  terminal-refusal). Grandchildren cascade through the same walk.
* **THE CHILD FAILED → THE PARENT'S BODY DECIDES**: `ChildFailed` is a
  MEMBER, never a raised error and never a silent swallow — the body's
  own match arm decides (fail the step, substitute, or finish with what
  it has). The ladder is never engaged by a child's failure unless the
  body raises.
* **THE RETRY FACE**: Q1's key law — the retry ADOPTS, never duplicates
  (the arbiter conflict returns the existing run; a terminal child is
  adopted as-is and the join reads its verdict deterministically).

### 5. THE FAN-OUT — N children from one body (the sub-investigations)

The body loops, spawning K children (per-iteration/per-cursor slots —
distinct keys), collects the handles, and joins each: the fork's
ALL-members semantics at the flow level, spelled by the body:

```python
handles = [await ctx.spawn_flow("sub_investigation", input=p, result=SubReport) for p in probes]
results = [await h.wait() for h in handles]  # the fan-in: the typed results list
```

No new join primitive: the handles' waits are independent holds (distinct
signal names — one child each), each woken by the same arm; the body's
own aggregation is the fan-in. The slots make the whole fan-out
retry-stable (the retry re-derives the same K keys and adopts).

## THE BUILD (the landed faces)

* `workflows/_childflow.py` (NEW): `ChildOk`/`ChildFailed`/`ChildCancelled`
  (+ the `ChildOutcome[M]` alias), `ChildHandle[M]` (`.flow_id`, `.wait`),
  `CtxSpawnOps.spawn_flow`, the join site, `sweep_child_joins`,
  `CHILD_TERMINAL_CHANNEL`, the receipt/provenance statements.
* `workflows/api/_ctx.py`: `StepContext(CtxSpawnOps, CtxWaitOps)`.
* `workflows/api/_runner.py`: `create_flow(root_metadata_extra=…)`; `tick`
  drives the descendant flows (the child is a real flow — the dev-loop
  driver walks the receipts; the fleet door needs nothing: the child's
  rows are queue-dispatched like any flow's).
* `workflows/ledger.py`: `insert_flow_run(extra_metadata=…)` passthrough.
* `workflows/api/_runner_exit.py`: the cancel cascade.
* `workflows/__init__.py`: the exported faces (the protocol-pin tax paid).

## THE PINS (red-first — `tests/test_wf_childflow_pins.py`)

1. **THE SPAWN→JOIN HAPPY PATH**: the parent's body gets the child's
   typed result; the rows: the parent's `child_flows` receipt AND the
   child root's `parent_flow_id`/`parent_node_id` — BOTH directions.
2. **THE CHILD-FAIL FACE**: the child fails → the parent's body sees
   `ChildFailed` (never silent, never raised) — the body's arm decides.
3. **THE CANCEL CASCADE**: the parent cancelled → the child's rows
   terminal (the root + the nodes), recursively.
4. **THE RETRY'S IDEMPOTENCY**: the body re-executed against the same
   parent node (the retry/resume) ADOPTS the same child flow id — the
   child-row count stays 1.
5. **THE FAN-OUT**: K children → the fan-in → the typed results list,
   every slot's receipt on the parent row.
6. **THE SLOT LAW**: during the child's run the parent's node is the held
   representation (pending + the hold marker, `awaited` on the ledger —
   never a failed attempt), and the child's nodes EXECUTE while the
   parent holds (the dev driver drives the child).
7. **THE CRASH-MID-WAIT**: the parent's attempt dies mid-hold; the
   successor's reclaim re-runs the body; the child's terminal (delivered
   by the wake arm) joins — the typed result arrives once, the wake arm
   is the belt for the lost attempt.

## THE PROOF

ruff + format + pyright FULL 0/0/0 + the type gate + the pins +
the wf battery ×2 + the fast tier ×1 at `-n 8` (full capture) +
mkdocs strict (the guide's child-flows section — the sub-investigations
snippet VERIFIED by the docs-examples harness, exec fence).

## AMENDMENT (the build's honest deviation, recorded per the house law)

The ticket's slot law said a LOOP-kind node's spawn slot reads the
row's `iteration`. The reads decided BETTER: the loop driver runs each
iteration on ITS OWN job row (`<loop>.iter<i>` — the ledger's
per-iteration identity), so ONE rule covers every shape — the
per-attempt spawn cursor on the EXECUTING node's row
(``metadata.spawn_cursor_{attempt}``). Distinct iteration rows carry
distinct cursors; a retry of an iteration re-derives the same slots;
the arbiter adopts. No loop special case shipped — the rule is
SIMPLER than the ticket's, and the ticket's law (retry-stable slots,
adoption never duplication) holds exactly.

Second deviation, face-plumbing: the wake arm lives in
``api/_hitl.py`` beside ``sweep_expired_signals`` (the statement-reuse
law — the arm reuses ``_DELIVER_RESUME_SQL``/``_broadcast`` from the
same module; a cross-module private import is the
``reportPrivateUsage`` tax), and the join's wait-site host is a PUBLIC
``JoinSite`` record (the handle reads its own fields — the protected
access never crosses the class boundary). The cancel cascade landed on
BOTH cancel faces (``cancel_workflow_run`` and the runner's
``cancel_workflow`` — one law, two surfaces, the same walk).
