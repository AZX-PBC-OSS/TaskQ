# REVIEW-taskqflow — THE CONSOLIDATED CAMPAIGN RECORD

*The workflows campaign's durable record: the arc from the design-first law to the consolidation, every conviction's cure, the doctrine the campaign codified, where the evidence lives, and what remains open. Compiled 2026-10-09 from the campaign's own captured corpus (read-only reads of the repo trees' `.measurements/`, the pin files, the ticket DAG, and the branch histories). This record NEVER erases a verdict — the relayer's verdict was TRUE at `af1b8779`; the cures landed on the lanes after it, and the arc shows verdict → cure, honestly.*

---

## 1. THE ARC

```
the design-first law (2026-10-06)
  24 tickets decomposed (00-INDEX) · the kill list (K1-K5) · the abstraction contract
  · the PoC gates: P1 FINAL, P2 admin view, P3 dragon matrix — NOTHING built
  before its proof was FINAL
        │
        ▼
the build phases (serial, rebase-first, one PR group at a time)
  PR-2 schema (T03) → PR-3 engine + ledger (T04/T05) → PR-4 propagation/scale/
  rollup/retention (T06/T07/T08/T18) → PR-5 API/loop/HITL (T17/T09/T19/T10)
  → PR-6 streaming + progress designs (T20/T21) → PR-7 operator wave
  (T11/T12/T13/T16 + the designs) → the waves' own fixes (the arity, the
  flake-kill, the MVCC, the CI runtime)
        │
        ▼
the attack fronts (every phase red-teamed; builder ≠ red-teamer ≠ lander)
  attack (engine windows) → attack2 (phase-2 surfaces) → attack3 (T10/T19
  adversarial) → attack4 (the CLI + admin + nothing-stuck + the demo) →
  the certifiers (PHASE1-CERTIFICATION, cert2's estate round, the findings
  validator's FV round, the alignment auditor, the reviewer's A-D)
        │
        ▼
the convictions → the cures (each red-first, each pinned)
        │
        ▼
the consolidations (the rebase-carry waves; the final consolidation of the
  LAST lanes is OPEN — §4)
```

### THE CONVICTION → CURE TABLE

Verdicts: **CURED+PINNED** (the red is a permanent pin; the cure verified by the named certifier) · **CURED** (landed; the pin rides the lane) · **LANDED-ON-LANE** (the cure is on a lane/branch awaiting the final consolidation) · **OPEN** (§4).

| # | Conviction (what the red team / certifier convicted) | The cure (the lane's SHA + the certifier) | Status |
|---|---|---|---|
| 1 | **The crashed wedge** — the maintenance leg's failed-arm gate on live-only work: pending/scheduled siblings stranded forever behind one crashed arm (the 149-stranded-chains wedge) | `9a0711a5` (t20 lane: the failed arm gates on live UNRESOLVED work) — the certifier: the t20 fence probe (`e673ca11`) + the premature-terminal pin | CURED+PINNED |
| 2 | **The premature terminal** — the root finalizing mid-stream while page 0's chains hold live work | the t20 fence pins (30-sweep premature-terminal fence, `e673ca11`); the mutation captured in `t20-pin-reds.json` | CURED+PINNED |
| 3 | **The create seam** — a root row with zero node rows (the orphan shape a future statement-order regression could commit) surviving as a healthy-looking run | the create-seam round: the seam's two defenses — the frozen canonical identity + the re-entrancy guard (`9449a92d`); the reds machine-captured (`createseam-pin-reds.json`, `createseam-pin-reds-pre.txt`) — the certifier: the seam-verify lane (the branch `fix/audit-seam-recursion-verify`) | CURED+PINNED |
| 4 | **The torn cancel** — the cancel cascade's signal leg on the wrong connection: the "one-tx cancel" was a lie | `32297d6f` (p4 lane: the signal leg runs on the CALLER'S connection) — the certifier: the p4 fixer's ×3 green captures + the cancel pins (`attack/taskqflow-cancel-pins`, `d1075dd7`) | CURED+PINNED |
| 5 | **The untyped cold door** — a schema-less hold delivered to ANY payload from a fresh process (the audit hole) | `3d688fce` (p4: the hold's payload contract MANDATORY — the schema-less hold refuses loudly, the mint refuses to create one) + the upgrade-world backfill `01.00.30/01.00.32-adjacent` (the NULL → the explicit no-contract marker) — the certifier: the p4 fixer + the findings validator (F-P4-UNTYPED-COLD-DOOR) | CURED+PINNED |
| 6 | **The why-stuck lie** — the stuck-run diagnosis deriving the remedy from anything but the row's REASON | `d9692a81` (p4: the join marker stamped at create, the failed-parent cascade resolves, the attempt counters on the status read) — the certifier: the p4 fixer's battery | CURED+PINNED |
| 7 | **The loop's CONSUME-BUDGET dragon** — the arm reading (consuming) the budget instead of pausing: a held loop killed mid-hold, the operator's approval refused | the shipped arm's `AND NOT budget_paused`; the dragon kept red forever (`test_consume_budget_dragon_red_forever`) — re-enacted in-process after the fabricated-reds regeneration; the certifier: the T19 round (`09841cbc`) + the loop-cures round (`10d5b483`'s seven honest reds) | CURED+PINNED |
| 8 | **The loop carry/fences conflation** — one `carry=` param, two meanings (the initial value vs the carrier-type) resolved by distant isinstance branches; a dict carry silently unchecked | the typed split (`initial=` / `carry_type=`) — landed on the wedge-support port's line (`fe85ee7b` carries the rename + the typed-ctx adoption); the glossary rename (`ctx.step` → `ctx.substep`) carried across the merged corpus's call sites — the certifier: the loop-cures round's gates (pyright FULL 0, the type gate 13 markers ×2) | LANDED-ON-LANE |
| 9 | **The escalation dead letter** — `on_exhausted="escalate"` enqueuing to a ghost actor (no body resolved at claim: the escalation never lands) | attack-3 H1's cure: the escalation rides the SAME outbox the fired joins use, addressed to the REGISTERED escalation step (`loop.escalation`, D1) — `aa6cecf7` (the t20/phase-3 line), carried via the consolidations — the certifier: the attack-3 corpus (`PHASE3-ATTACK-REPORT.md`) | CURED+PINNED |
| 10 | **The signal-face dragons** (B1/B2): the abandoned hold auto-minting a new epoch (hold→expire→re-hold→∞); the union payload mis-narrowed by declaration order | attack-3 B1/B2's cures: `SignalTimeoutError` at the wait site (the deliberate re-wait is a NEW body decision); the deliver validates BY SHAPE (exactly one declared model must fit; the explicit `discriminator=` for the ambiguous union) — the certifier: the attack-3 red team + the FV round's hitl pins | CURED+PINNED |
| 11 | **The CLI four** — the stale `--app` traceback (F-P4-CLI-KEYERROR-TRACEBACK), the output contract's overlap-shape breach, the demo's cold-stack dead ends, the input pin (accepted-and-ignored input) | `92385152` + `5e393955` (the named refusal, exit 1, never a traceback — the committed pin), `3bee4833` (the demo's cold-stack resolve end-to-end), the input-pin cures (`189db78c`, `dce4ab0d`) — the certifier: the attack4-cli-races rounds ×3 | CURED+PINNED |
| 12 | **The admin five** — the red-team's workflow-explorer convictions: the map's children addressed by step-key-arbitrary reads (a child's attempt ledger unreachable), the SSE replay's contract drift, the resolve/deliver doors' undeclared-gate blindness, the page's zero-warning budget breaches, the run-page's latency band | `9ae44f4f` (the ADMIN-SURFACE WAVE — the red-team's six convictions cured, each pinned) + `cfd9a591` (the node panel's map-index addressing: `?map_index=N`, the named 404/400, the children census) — the certifier: the attack4-admin-chaos rounds ×4 + the web_admin sweep (904 green) | CURED+PINNED |
| 13 | **The progress gates (DH1–DH8)** — the every-emission-a-row log (6.5M-row incident's shape), the unpruned ring leak, the liar's display (failed with pct=99 inside), the stale-payload dragon, the closed-vocabulary sprouting, the silent-gap face, the crash window's loss amplification, the cardinality blow-up (per-child gauge) | the T21 build: the two-channel persistence (STATE upsert + the STREAM ring, ONE seq space — DH3), the coalescing emitter, the named-partial replay, the drop-accounting ON THE RECORD (appended == retained + dropped), the terminal-mark statement's ZERO-FINALIZE-CHANGES probe — `eddeb651`, `6295adb2`, `c10ac2ff` (the T21 line) — the certifier: the PoC gate PROVEN (DH1–DH8 closed, 3 clean rounds; `t21-20261008T033005Z`) + the storage pins (11 + 8) | CURED+PINNED |
| 14 | **The T20 taxonomy** — the streaming source's fork-atomicity (a kill lands a partial page or advances the cursor alone: the resume re-emits or skips), the router's non-totality (the foreign outcome silently dropped: the run "succeeds" minus a record), the chain's lineage split across two systems | the T20 build: the EMIT TX (the per-page statement group), the ROUTER (the typed-outcome surface, total or refused — `RouterNotTotal`), the chain's one trace per record — `528de99f`→`f0ebddc5` (the t20 line; in HEAD: `b5726bf1`, `a410361b`) — the certifier: the t20 pin-reds corpus (the mutation drills' machine capture) + the taxonomy's design round (`8fc300a8`) | CURED+PINNED |
| 15 | **The emit backpressure (DH9)** — the unbounded materialization (a fast source outpacing its workers: the run materializes without bound) | the EMIT BACKPRESSURE: the max-in-flight bound's DECLARED HOME (the per-workflow `max_in_flight=`), the admission fence (the in-tx advisory-lock gate + the bounded out-of-tx poll), the wide-page refusal, the ladder-owned stall — `ef5fcbcc` (t20), carried through the wedge port (`fe85ee7b`) — the certifier: the backpressure pins (the captured red: the unbounded variant blows the count 8 > 6) | CURED+PINNED (DH9 closed on the lane) |
| 16 | **The evidence estate's four classes** — (1) a coverage number cited without the tree it was measured on; (2) the behavioral claim held by the mutated SQL's SHAPE (the pin reading the mutant's text, never the outcome); (3) the fabricated reds (a hand-written payload recorded where no drill ran); (4) the zombie corpus (a wired-in-name-only probe estate) + the dirty-rule's staging-dependent parse | the evidence-integrity round's cures 1b–5: the HEAD-STAMP LAW (`f9ade8a5` — a number is evidence only OF the tree it was measured on), pin 23 behavioral (`05dee9a8`), THE PROVENANCE LAW + the artifacts' head-stamp (`73a8a36f`), wire-or-delete (`46fbc7a0`), the dirty rule's three fixes (`9238b181`, `5dc0762b`, `4bf53c86`) — the certifier: the findings validator (FV) + the reviewer's A-D round (the provenance corrections appended, `FIX-ROUND-PROVENANCE.md`) | CURED+PINNED |
| 17 | **The fabricated reds (the FV round's 11)** — literal-payload `.red()` calls: prose wearing an evidence file's clothes | the regeneration: 10 sites' payloads re-derived from the drills' own runs; `pin1-consume-budget-dragon` re-enacted IN-PROCESS (the mutant's SQL patched, the paused row fired); zero entries deleted — every red reproduced (the orphaned line `59ce82de`; the LAW carried by `test_fv_redlog_guard` on the lanes) — the certifier: the redlog guard itself | CURED+PINNED (the guard rides the lanes) |
| 18 | **The redis/parity question** — is the dispatch path's parity (chain vs plain) real, and does the redis client's single-point construction hold the resilience defaults? | the dispatch parity band measured (9.75/9.64ms — the same class, both faces; the integration battery round 2) + the redis resilience defaults landed from main's wave (`0c6d9088`, #647: single-point client construction with resilience defaults) — the certifier: the integration battery's band capture (`fd0a06b8`) | CURED (the bands pinned; the parity measured, not asserted) |
| 19 | **The scale/soak/security probe's seven** — the seven honest reds the loop-cures round captured (the pre-cure machine capture: the loop + the phase-3 pins' adversarial shapes), plus the estate's own scale/soak/security history (the CVE-zero hardening `686dc6cf`, the soak's teardown reaper `bd1c2439`, the lock-order inventory + the lost-job soak `8656b063`) | the loop-cures round: seven honest reds (pre-cure, machine-captured `loop-cures/loop-cures-reds-pre-cure.txt`) → the cured head's greens (32 passed; the wider families 88; pyright FULL 0; the type gate 13×2) — `10d5b483` — the certifier: the loop-cures' own gates + the solo captures (dev 27, typed-outcomes 14, the perf/chaos families 41) | CURED+PINNED |
| 20 | **The rotating-load flake class** — the batteries' victims red under xdist load, every one green solo (the number moved with the scheduling) | the class-map's structural cures (the COPY arity, the leaked-projection registry, the boot-race condition-not-clock, the ambient-PATH) + the honest multipliers (the load-degradation factor 1.25x; the loaded bar's condition-bounds) — the flake-kill round (`eb4336c8`, `3e499285`) — the certifier: the flake-kill report's class map C1–C8 (root cause → structural fix → the pin) | CURED+PINNED |
| 21 | **The registry-collision class** — two modules claiming one workflow name with different definitions: whichever shared an xdist worker reded second (the "rotating victims, green solo" mechanism) | the bench's registry-key accommodation (`doc_ingest` → `doc_ingest_bench`, the code verbatim) + the hot-reload guard (the content-fingerprint absorption, the shadow conviction standing) — the orphaned line `7119d7b2` — the certifier: the combined runs 9/9 ×3 | LANDED-ON-LANE |
| 22 | **The identity residues** — the migrations' headers citing phantom filenames; the provenance misattribution; the trailing law unenforced; the mark law self-contradicted | the fix round A-D: the six stale header lines swept, the provenance corrections appended, the trailing law carried (the trio before `parent_id`), both wall-clock bands marked — `815901e0` (branch `fix/audit-seam-recursion-verify`) — the certifier: the reviewer's own A-D report + the re-verification ×2 | CURED+PINNED |

**The honesty note.** The relayer's verdict — the convictions' list as the state of the campaign — was **TRUE AT `af1b8779`** (the grand-consolidator's round). The cures for several of those convictions landed on the LANES **after** that verdict (the seam's defenses, the admin wave, the loop-cures, the wedge port, the reviewer's A-D). The arc above shows the verdict → the cure for each; no verdict is rewritten, no capture deleted. The cures' SHAs live on the lanes and the fix branches — the FINAL consolidation of those lanes onto the PR head is the open item (§4).

---

## 2. THE DOCTRINE (what the campaign codified)

The campaign's output is not only code — the rounds codified laws that now govern every future round. The §7b sections, as the campaign wrote them:

1. **THE RECEIPTS LAW** — a redlog entry is machine-generated or it doesn't exist. A `.red()` payload built only from syntactic constants is prose wearing an evidence file's clothes; every red's payload must DERIVE at least one value from the drill's own run (a live read, a computed flag). The reds ledgers are append-only and run-scoped: a partial run adds its own record; history is never truncated, never rewritten.
2. **THE HEAD-STAMP LAW** — a measurement is evidence only OF THE TREE IT WAS MEASURED ON. The artifacts carry the head's stamp; a capture may not be cited for a tree it did not measure; the staleness anchor is the last SOURCE commit (the sinks-only appends never stale the number).
3. **THE PROVENANCE LAW** — replaces the shape rule: a record's chain (which tree, which line, whose drill) is part of the evidence. The corrections are APPENDED beside the records they correct — never a rewrite, never a deletion.
4. **THE SEPARATION OF RESPONSIBILITIES** — builder ≠ red-teamer ≠ lander ≠ certifier. If a gate reds on ANOTHER lane's semantics, the round STOPS and reports; a lane never cures a foreign conviction by editing a foreign tree (rebase-carry, never edit).
5. **THE NO-DEFERRALS LAW** — every finding lands with its pin or its recorded disposition-home. No "known issue" lists without a named owner and a gate; nothing left unmarked and unconverted (the mark law: a load-sensitive residual is MARKED and runs in the exclusive lane, or it is a defect).
6. **THE DIAGNOSTICS-FIRST LAW** — the refusal names the defect: `error_class` carries the dragon's name (`RouterNotTotal`, `SignalPayloadAmbiguousError`, `SignalTimeoutError`), the CLI's failures are named refusals with exit 1, never tracebacks; the remedy derives from the REASON the row carries.
7. **THE DESIGN LAW'S BALANCE** — the design-first gate (nothing builds before its PoC proof is FINAL) balanced by the maintainer's overrule power (the loop primitive: *"you do not cut must haves"* — the skeptic's prune flipped by the maintainer's decision + the spike evidence). The kill list is the can't-lie preamble: what is NOT built and why, with the evidence inline.
8. **THE RESOURCE LAW (§7b/§22.6)** — a sweep never touches a reclaim-owned row; the clock comparison is PG's own (the DB-clock doctrine); the batteries budget the 2× statement-timeout bound; the containers are named with owners and expiry (the inventory's survivors stand named; the lane's own PG expires with its report).
9. **THE LOADED BAR** — the fast tier's default survives the noisy neighbor: the condition-bounds carry their honest multipliers (the measured load-degradation factor), the loaded rounds run under the DELIBERATE background load (the neighbor's measured profile + pgbench on the co-tenant), and the bar's record distinguishes the saturated-box verdicts from the exclusive-lane verdicts (NOT CLAIMED from a saturated box).
10. **THE APPEND-ONLY CONVERSION** — the band artifacts are run-scoped files (the newest CITED), never write_text-in-place: the in-place rewrite is the torn-write race (eleven torn rows found in the ledgers' union — the race's own receipts) and a partial run's numbers falsify the recorded band.

---

## 3. THE EVIDENCE INDEX (where everything lives)

**The repo's `.measurements/` (the tracked corpus — the sinks are append-only, run-scoped):**

| Corpus | Path | What it holds |
|---|---|---|
| The pin packs (the reds' machine capture) | `.measurements/pin-reds.json`, `ledger-pin-reds.json`, `t06-propagation-reds.json`, `t10-pin-reds.json`, `t19-pin-reds.json`, `t20-pin-reds.json`, `createseam-pin-reds*.json` | every mutation drill's reds, one JSONL record per run (run id + ts + entries); the chronological union, zero rows dropped (the union audited: 0 missing across every ledger × every lane source) |
| The phase reports | `PHASE1-CERTIFICATION.md`, `PHASE2-R2-FIX-REPORT.md`, `PHASE3-REPORT.md` (+ `PHASE3-ATTACK-REPORT.md`, `PHASE3-FIX-REPORT.md` in `attack3/`), `T20-BUILD-REPORT.md`, `T21-REPORT.md`, `t17-dispositions.md` | the build rounds' own records: the commit maps, the red-first ledgers, the pin inventories, the unspecifications |
| The certification rounds | `.measurements/PHASE1-CERTIFICATION.md`, `p4-certified-sanity.txt`, the cert2 round's estate cure (the F-CERT2-1 reds in the p5 artifacts) | the independent certifiers' verdicts |
| The pattern maps | `patterns/ECOSYSTEM-MAP.md` + `patterns/probes/` (+ `probe-run-ecosystem.*`) | the ecosystem's migration map (the orchestrator-correspondence probes; the map's per-product spellings live in the internal corpus, the docs' user-facing catalog is the generic-mechanism distillation — the OSS-naming law) |
| The alignment audit | `ALIGNMENT-AUDIT.md` | the independent auditor's matrix: every proof corpus vs the built tree (ALIGNED / MISSING / SUPERSEDED / BACKLOG), at head `55faa1f2` |
| The execution verdict + cure | `EXECUTION-VERDICT.md` (the investigator's "TRUE IN TESTS ONLY") + `EXECUTION-CURE.md` (the worker-hosted execution door: the claimed row EXECUTES on the claiming worker; the probes' reds re-captured pre-cure) | the queue-is-the-runtime proof chain |
| The attack fronts | `attack/` (the engine windows' reds B1-B3, F4-F9), `attack2/` (the phase-2 report + the baselines), `attack3/` (the adversarial T10/T19 corpus + the type gates), `attack4/` + `attack4-fix/` (the CLI/admin/nothing-stuck rounds + the fixer's batteries) | the red-team waves' captures |
| The loop cures | `loop-cures/` (the seven honest reds pre-cure; the cured head's greens + gates) | the scale/soak/security probe round's receipts |
| The carry rounds | `carry/`, `carry2/` (CARRY-FACEC-REPORT.md + the pins' first runs/greens + the base-install cancel end-to-end) | the audit seam's carry evidence |
| The fix rounds | `FIX-ROUND-PROVENANCE.md` (+ `runs/fixround-*`), `p4-attack-held-drive-null-deadline.md` | the reviewer's A-D corrections + the re-verifications |
| The gate captures | `runs/` (the timestamped gate files: the batteries, the coverage gates, mkdocs, pyright, the bands, the e2e composed runs) | the batteries' own artifacts (run-scoped, the newest CITED) |

**The design estate (`/tmp/opencode/dag-research/` — this directory):**

| Corpus | Path |
|---|---|
| The ticket DAG + the kill list + the PR grouping | `tickets/00-INDEX.md` (+ `tickets/NN-*.md` — the 24 dispatch units) |
| The build protocol (the laws' original spelling) | `BUILD-PROTOCOL.md` |
| The design reviews | `PROPOSAL.md`, `SKEPTIC.md` (+ `skeptic_probe.py`), `TORS-REV-0.16.md`, `GAPS.md`/`GAPS-ESTATE.md`, `DEBUGGABILITY.md`, `KILL-LIST.md` |
| The case study (internal, never ships) | `CASE-STUDY-sai.md` |
| The type-probe estate | `evidence/` (the flagship's pyright/ty captures, the skeptic probes ×4 checkers), `tickets/typeprobe/` (the negative corpora + `_gate.py`), `tickets/guides/` (the migration guides incl. `migrating-graph-checkpoints.md` — the path renamed from `migrating-from-langgraph.md` by the docs lane's OSS-naming sweep, the stub kept at the old path — + `verify_guide.py`) |
| The campaign record | `REVIEW-taskqflow.md` (THIS file — linked from `tickets/00-INDEX.md`) |

**The Mermaid golden + the fence:** `tests/goldens/doc_ingest.mermaid` (byte-stable), `docs/examples/doc-ingest.md` (the fence — the example IS the under-test artifact), `docs/guides/workflows.md` (the built surface's guide).

---

## 4. THE OPEN ITEMS (honest)

1. **The final consolidation of the LAST lanes onto the PR head.** The cures' SHAs (§1's table) live on the lanes and the fix branches; several landed AFTER the last full consolidation. Awaiting the carry: the hardening branches' measurement rounds (`76436c03` — the coverage gate's fresh re-record, 92.33% vs floor 90 on `03e23043`'s tree; `28d4de68` — the battery's run-scoped redlog appends), the pin branches (`attack/taskqflow-pins` @ `0f081c77` — the loop + docs-honesty attack pins: the carry that lies, the unfenced advance, the phantom figure; `attack/taskqflow-cancel-pins` @ `d1075dd7`), and the seam-verify branch's tip (`9449a92d`'s line — the create seam's two defenses). The single-writer carry discipline applies (rebase-carry, never edit the lanes' trees; the .measurements ledgers = the chronological UNION, zero rows dropped; the migrations by landing order — RECOUNT on any collision).
2. **The final recertifier over THAT head.** After the last lanes land: one certifier runs the FULL battery on the consolidated head itself (the stash-drill discipline: the working tree must be the probe's only difference) — ruff + format repo-wide, pyright FULL 0, the entire wf pin suite + all attack rounds, the estate slice ×3, the type-probe gate (all markers, both pinned checkers), the bands, mkdocs strict, the e2e composed run, the scoped coverage gate at the floor, the ledger union's zero-rows audit — and stamps the head.
3. **The maintainer's approval.** The PR grouping's serial lane (PR-2 → PR-7's exit gate: the fresh-eyes cold-read — the demo end-to-end from the compose stack + the semantics explained back against a rubric) needs the maintainer's landing calls: the PR-8 conditional (T25's content-addressed rerun rides the certified core + the maintainer's build call), and the LATER-recorded items (§12's rerun machinery, §18.2's survivors) stay recorded-not-built without a ruling.
4. **The recorded-not-built (by law, not by omission):** the kill list's K1–K5 stand (the split/Router machinery, the full DAG replay runtime, the content-addressed rerun, the SPA admin + the tolerance machinery, the refusals list) — each with its falsifying probe or the maintainer's ruling inline; the ticket files 22/23/24/25 (the operator wave's designs + the conditional) remain design-state until the certified core carries them.

---

*The record's counts: 22 conviction families in the table — 19 CURED+PINNED, 2 LANDED-ON-LANE (awaiting the final consolidation), 0 OPEN in the table (the open items are the consolidation + the recertification + the approval, not uncured convictions). 10 doctrine laws. 1 rule throughout: a verdict is never erased — the arc shows what was true, what cured it, and who verified.*
