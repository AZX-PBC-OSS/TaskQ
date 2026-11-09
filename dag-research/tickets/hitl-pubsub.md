# the hitl broadcast: PG LISTEN/NOTIFY of the approvals + the worked deep-research example

Design-first: this ticket is the 30-minute read's output; every claim below
was verified against the tree at the base head before a line of code.

## amendment (the maintainer's dx audit — the two faces that failed)

The first design taught the expiry as an exception and the listener as
`.events()` — the honest dx audit fails both faces: an exception cannot
be forced (a body that never catches it compiles clean and hangs the
operator), and ceremony hides the one-liner. The amended design:

1. **the expiry is A value, not an exception.** `ctx.wait_signal`'s
   timeout face returns a typed union — the declared decision models
   joined with the house ``Expired`` member — so the checker forces the
   fail-close arm: a body that matches only the decision arm falls
   through and reds (implicit-return-None against the body's declared
   `Done | Refine` return); a body that bare-unwraps
   (`outcome.verdict`) reds (`Expired` carries no verdict). The
   `SignalTimeoutError` exception stays in the vocabulary for the
   non-handled path — the escalation ladder's own use: a body that
   wants the failure raises it itself off the ``Expired`` member (the
   pin-e's body is exactly that shape). The pin comes first: the
   type-probe marker (the bare unwrap) ran against the pre-union tree
   and was silent — the checker could not force anything; the capture
   is the receipt, the union's landing is the flip.
2. **the listener's face**: directly async-iterable
   (`async for event in listener` — no `.events()` ceremony; the
   keepalive form stays on `frames()` for the SSE face), the events a
   closed union (`match event:` works exhaustively), the filter a
   parameter (`HitlListener(pool, schema).holds(run=flow_id)` — typed;
   the unfiltered stream still flows for the raw tail), the
   context-manager form for the scoped use.
3. **the dx test the design must pass**: the body author and the
   backend author each write their full flow with zero doc lookups —
   the example's snippets are the API tour. verbatim from the built
   tree:

   the body author's flow (verbatim, `examples/deep_research.py`
   `research_iteration`):

   ```python
   outcome = await ctx.wait_signal(
       (ContinueApproval,),
       timeout_s=APPROVAL_TIMEOUT_S,  # the real default: 120.0
       reason="the research loop wants to continue past the free passes",
       tool="continue_approval",
       args={"topic": carry.topic, "passes_done": len(carry.notes)},
   )
   match outcome:
       case ContinueApproval() as approval:
           if not approval.approved:
               return Done(carry.finish_with_what_you_have(note=approval.note))
           carry = carry.model_copy(update={"approved": True})
       case Expired():
           # the fail-close: nobody watching — the loop finishes
           # with what it has (the typed expiry is a result, never
           # a hang and never a crash).
           return Done(carry.finish_with_what_you_have())
   ```

   the backend author's flow (verbatim, the pin test's
   `test_deep_research_approve_path` — the built surface):

   ```python
   listener = HitlListener(wf_pool, wf_schema)
   async with listener:
       async for event in listener.holds(run=flow_id):
           match event:
               case HoldCreated():
                   created = event
                   break
               case HoldResolved() | HoldExpired():
                   continue
   ```

## The read (what the tree actually says)

1. **The hold-create leg is missing, not weak.** `register_hold`
   (`src/taskq/workflows/api/_hitl.py`) opens one tx for the
   `wf_signals` INSERT + the held representation and notifies NOTHING —
   the module docstring's "the hold's appearance is notifiable" is true
   only of the resolve (`HOLD_CHANNEL = "taskq_wf_holds"`, fired in
   `HitlClient.resolve` and `deliver_payload` with the pointer payload
   `{hold_id, run_id, event}`). A hold created today is discovered by
   polling (`client.hitl.list`) or not at all.
2. **The resolve knock's shape is pinned** (pin 12,
   `tests/test_wf_hitl_pins.py::test_pubsub_knock_is_a_pointer_and_the_consumer_converges`
   asserts `set(knock) == {"hold_id", "run_id", "event"}`). The new
   broadcast legs must not mutate that payload — the existing knob keeps
   its shape; the broadcast rides new channels.
3. **The channel decision: global, the schema rides the payload.** The
   schema-per-module fixture pattern
   (`tests/conftest.py`: `module_pg_schema` — one database per module,
   one schema per module within it) means many schemas share one
   database and `pg_notify` is per-database: a per-schema channel name
   would buy nothing (the listener still has to connect to the right
   database) and cost a computed channel name. decision: three global
   channels; every payload carries `"schema"`; the listener filters by
   payload — never by channel arithmetic.
   - `taskq_wf_hold_created` — create leg (renamed from the first cut's `taskq_wf_hold`: one letter from the legacy `taskq_wf_holds` knob — an ops typo's recipe; additive, unreleased):
     `{schema, flow_id, run_id, hold_id, signal, node_key, created_at}`
     (`flow_id == run_id` in this engine — the run's root row is the
     flow — both keys ship so the design's consumers and the codebase's
     vocabulary each read their own name).
   - `taskq_wf_hold_resolved` — resolve leg:
     `{schema, hold_id, flow_id, verdict_kind}` where `verdict_kind` is
     the payload model the typed door validated against (the fitted
     model's name, class-loaded or cold-process schema key) — the
     verdict's declared kind, never the payload itself (the
     pointer-only law holds).
   - `taskq_wf_hold_expired` — expiry leg (added by this read: the
     design's typed events name `HoldExpired`, and no leg = no event —
     the expiry sweep `sweep_expired_signals` is the only live timer on
     a held row): `{schema, hold_id, flow_id, signal, node_key}`.
4. **Transactional placement.** (a) the create NOTIFY goes inside
   `register_hold`'s existing tx (the INSERT + the held representation
   + the knock — a rollback of that tx is silent); (b) the resolve
   NOTIFY goes inside `HitlClient.resolve`'s CAS-winning tx (the audit
   row + the two knocks + the resume — the loser writes nothing);
   `deliver_payload`'s winner gets the same broadcast leg (the admin's
   deliver door rides it — a hold answered through either face is the
   same event to the listener). The expiry sweep notifies after its
   CAS UPDATE (autocommit statement = already committed — never a
   notify for a row that did not land).
5. **The listener surface.** `HitlListener` in
   `src/taskq/workflows/api/_hitl_listen.py` (house-named internal
   module, the class re-exported public from `taskq.workflows` beside
   `HitlClient`): async context manager over one dedicated connection
   from the pool; `async for event in listener.events():` yields typed
   pydantic events `HoldCreated` / `HoldResolved` / `HoldExpired` (a
   discriminated union on the `event` literal). the backfill: LISTEN
   first, THEN the snapshot `SELECT … WHERE status = 'held'` — any hold
   visible to the snapshot either notified before we listened (no
   notify will come) or after (its notify is deduped against the
   snapshot by `hold_id`) — the missed-event window is zero by
   construction (the #316 mid-stream re-check pattern generalized: the
   re-check at open, not just mid-stream). Reconnect (connection loss)
   re-backfills: a re-delivered `HoldCreated` is idempotent by
   `hold_id` at the consumer (the row is the truth; the event is a
   pointer). The reconnect/backoff/shield-release discipline is
   `_listen.py`'s, re-derived for a multi-channel dedicated connection
   (the shared `listen_with_reconnect` is single-channel and yields
   raw strings).
6. **The SSE face.** The admin's `sse.py` owns the topic vocabulary
   (`_valid_topics = {queues, jobs, workers, history}`) + the #316
   session re-check + the topic semaphore. A `holds` topic joins the
   vocabulary, streaming the listener's typed events as
   `event: hold` frames — same re-check cadence (before the first
   frame + before every event + at every keepalive), same semaphore
   budget, same bounded shielded close. The resolve endpoint is
   untouched (it rides `HitlClient.resolve` today — `_wf_actions.py` —
   and its knock now lands on both the legacy knob and the broadcast).
7. **The worked example.** `examples/deep_research.py` (sibling of
   `examples/workflows.py`, the house's demo-module pattern): the
   deep-research loop — `research_iteration(ctx, carry: ResearchState)
   -> Done[ResearchState] | Refine[ResearchState]`; iterations 1-3
   research the fake corpus and refine; past three the body waits on
   `ContinueApproval` with the real 120.0 s default
   (`APPROVAL_TIMEOUT_S`, module-level — the tests scale it by
   monkeypatch, condition-not-clock). the expiry is A value (the
   amendment — rewritten from the pre-amendment "the exception is
   caught" shape, which the machinery never produced anymore): the
   wait's outcome is the closed union `ContinueApproval | Expired`,
   the body matches `case Expired():` and returns the fail-close
   `Done(carry.finish_with_what_you_have())` — the user not watching
   is a result, typed, named, and checker-forced (a body that ignores
   the arm reds; a body that wants the failure raises
   `SignalTimeoutError` itself off the member — the escalation
   ladder's own use). The body's research append is deterministic (the
   re-execution doctrine: the resume re-runs the body from the top,
   the corpus digest replays cheap).

## The pins

- **P0 (the checker forces the fail-close arm):** the type-probe
  marker — a body that bare-unwraps the wait's outcome (`.verdict`
  without the match) — red against the pre-union tree (the exception
  face cannot force anything: the capture shows the checker silent on
  the exact shape that hangs an operator) and reds on the union face
  (`Expired` carries no verdict). The match-ignores arm reds through
  the body's own declared return (the fall-through None).
- **P1 (the transactional create):** a rolled-back hold-create emits no
  notification. The red: the notify sent outside the tx (autocommit)
  fires while the row rolls back — captured as the mutation proof; the
  green: the nested-savepoint rollback (outer tx, `register_hold`
  inside, raise out) leaves no `wf_signals` row and no `taskq_wf_hold_created`
  notify.
- **P2 (the zero-window backfill):** a hold created before the
  listener's start is still delivered — the listener's first event is
  the backfilled `HoldCreated`; and a hold whose notify and snapshot
  row both exist (the LISTEN→snapshot race) is deduped to one event
  (unit pin on the dedup set: the backfilled id's late notify yields
  nothing).
- **P3 (the resolve wake):** `HitlClient.resolve` → the listener's
  `HoldResolved` (with the fitted `verdict_kind`) within an
  `asyncio.wait_for` bound.
- **P4 (the expiry fail-close):** the example's loop, timeout scaled
  to ~1 s: the hold expires → the slot law holds while waiting (the
  node row is `pending` with the hold marker, `locked_by_worker` NULL
  — the worker released the slot, the pinned slot law) → the sweep
  abandons → the body's typed expiry → `Done(finished_with_what_you_have)`
  → the flow terminalizes `succeeded` with the finish-with-what-you-have
  result on the rows.
- **P5 (the escalation interplay):** a loop with
  `on_exhausted="escalate"` whose hold expires: the expiry leg's
  `HoldExpired` still delivers for the loop-kind hold (budget-paused
  rows are not blind to the broadcast), the body failure exhausts the
  loop with the escalation row, the flow terminalizes, and the
  escalation-kind exemption's leg still delivers — the
  `loop.escalation` consumer job dispatches despite the terminal flow
  (the dispatch fence's exemption, exercised end-to-end).

## The proof (the battery at the stamped sha)

ruff + ruff format + pyright full 0/0/0 + the type gate
(`tests/typeprobe/_gate.py`) + the wf battery x2 + the example's
end-to-end tests x2 (approve path + expiry fail-close) + the fast tier
at `-n 8` + `mkdocs --strict` + `scripts/verify_evidence_heads.py`.

## The docs

- `docs/guides/workflows.md` §4: the broadcast + the backfill + the
  timeout fail-close, with the snippet from the real example file.
- `examples/README.md`: one line for the deep-research example.
- The pr body gains one sentence + the verified snippet (delivered in
  the report; the body edit follows).
