# THE STRANGER TEST — the disposition (T15, the phase-5 tail)

The stranger: ONE fresh subagent (`opencode run --standalone`,
`fireworks-ai/glm-5p3-flash`, its OWN detached worktree
(`/tmp/opencode/stranger-wt` at the consolidated head `17914536`) + its
own PG), whose ONLY input was the docs (`docs/`) + the demo stack
(`examples/`) — forbidden from `src/`, `tests/`, the measurements. Its
own words are `stranger-report.md` (verbatim); the run transcript is
`stranger-transcript.txt`.

## Verdict

The demo runs end to end (attempt 2; attempt 1 died mid-run — see its
stumble 4); the workflow API IS explainable from the docs, but only by
assembling three pages that partly disagree.

## The disposition, per stumble (the stranger's numbers)

**CURED (the doc side):**

1. The demo undiscoverable from the front doors → the flagship-demo
   pointer added to `README.md` (§Quick start's own heading), and
   `docs/index.md` (Next Steps' first row).
3. `"collect"` vs `"maybe"` → the workflows guide's duality now names
   BOTH accepted spellings and states the truth (the engine treats
   both as the same absorption; pick the name that reads honestly at
   the call site). Verified against the engine's own vocabulary
   (the absorbing set `('collect', 'maybe')`).
4. The mid-run-death recovery → the demo README carries the recovery
   line (`docker compose -p taskq-fleet-demo down -v`).
5. Act 4's headline event flaky (`cron-fire-budget-deferred`: 11 on
   run 1, 0 on run 2) → REPORTED below (a demo-stack weather note is
   owed but the event's presence depends on the host's tick budget —
   the README's "watch for" overpromises).
6. The Act-4 digest count/listing mismatch → FIXED in
   `examples/fleet_demo/run_demo.py` (the listing now names the
   elision).
7. The guide inside-out + the jumbled numbering → the guide's head
   carries a START-HERE pointer (the API reference's thirty-second
   tour → the worked example → the internals) + THE READER'S KEY (the
   T-numbers decoded, the §N convention, the .measurements citations
   named as in-repo files). The full §-renumbering is NOT done (the
   section anchors are load-bearing across the docs; the renumber is
   its own rev) — REPORTED below.
10. The placeholder link (`https://github.com/#/examples`) → the real
    relative path; the "demo's README" → the actual file.
11. The `taskq flows` verbs missing from the API reference → the CLI
    reference now names every subcommand group + the flows verbs, and
    the guide's own diagnosis runbook (§7) now names `taskq flows
    resolve` (the verb the HITL story needs) beside the holds read.
12. The two "run a flow" surfaces → the truth stated: **there is no
    `workflows.run` one-call spelling** — the guide cited a phantom
    API; `FlowRunner.create_flow(run_key=…)` is the only create
    surface, and the guide now says so in as many words.
14. The README quick-start vs the demo → the README says the demo
    manages its own stack and the quick-start steps are NOT
    prerequisites.
15. `migrations applied: 0` reads like a failure → the demo README
    names it ("already applied", not a failure).
18. The payload-door ordering sentence → rewritten in plain words
    (the wiring's shape decides; the declaration order decides
    nothing).

**REPORTED (the API side — untouched, the rev's own list):**

- `HitlClient` has NO public re-export: `taskq.workflows.__all__`
  exports `FlowRunner` but not `HitlClient`, so the official example
  imports the private `taskq.workflows.api._hitl` path (the stranger's
  stumble 9). The cure is a one-line re-export + an `__all__` entry —
  an API-surface change, owned by the API rev, not a doc rev.
- `workflows.run(flow, input, key=…)` — the one-call run surface the
  guide cited DOES NOT EXIST (stumble 12's root). The doc now states
  the truth; whether the one-call surface SHOULD land is the API
  rev's call.
- `GateDecl`'s full contract + `ctx.wait_signal(..., tool=…, args=…)`
  are documented-but-never-demonstrated (stumble 17) — the demo-stack
  gap: the flagship demo runs NO workflows at all (stumble 2), so
  every workflow surface lacks a live, runnable demonstration. The
  demo act is its own rev.
- The §-renumbering of the workflows guide (stumble 7's second half)
  — the anchors are cross-referenced from other pages; the renumber
  is a mechanical rev with a link-check gate.
- "Fleet" means three things (the worker fleet / the fleet demo / the
  demo's `fleet` queue — stumble 13) — a naming rev, cosmetic.
- The guide's load-bearing citations to `.measurements/*` (stumble 8)
  are in-repo and verifiable for anyone with the source; a
  docs-only reader without the repo cannot open them — the citations'
  claims are restated inline where the argument needs them.

## The method's own note

The stranger's environment gave it `uv`/`docker`/a clean checkout; its
Run 1 died mid-act with NO diagnostic (its own stumble 4) — the death
was external (the sandbox), and the demo's re-runnability carried it.
The stumbles' VALUE is the newcomer's path, which is why the
discoverability cures (1, 14) land first among them.
