# The invariant corpus: how invariants are stated, pinned, and attacked

This repository's quality doctrine: **an invariant that is not written down and
pinned does not exist.** The 2026-09-22 campaign regressions (#457-#461) shipped
because the system-level invariants were never hypothesised: CI was green, the
full suite passed, and the behaviors that broke were encoded nowhere. This
document defines the process that closes that gap.

## The doctrine

1. **Hypothesise first.** Before a fix or feature touches a shared seam, state
   the invariant that must hold there - in this corpus, in one sentence, with
   the failure it prevents.
2. **Pin it.** The invariant gets a deterministic test (virtual clock, forced
   interleave, real-PG backdating - see `tests/test_rt_conservation_chaos.py`
   and `tests/test_attack_livelock_pins.py` for the established patterns).
3. **Attack it.** Red-team the pin: mutation-test the src it guards (the pin
   must red), and compose it with OTHER failures (single-failure tests miss the
   interleavings where real incidents live).
4. **Merge only against the corpus.** A PR that touches a seam must say which
   corpus entries cover it.

## The corpus

The authoritative seam -> callers -> interleavings -> invariant -> pin matrix
lives in `docs/design/seam-audit-matrix.md`. Every seam listed there has:
its callers enumerated, the interleavings that can violate the invariant,
the pin name that proves it, and its status (EXISTS / MISSING / ADDED).

## Per-PR requirements (enforced by review, checked by the template)

- **Blast-radius audit**: every caller of every changed seam is listed; each
  caller either has a pin covering the new semantics or gets one in the PR.
- **No weakened assertions. Ever.** A pin that fails under a new change is
  reconciled (rewritten to the NEW contract explicitly, old contract's fate
  recorded in the PR body) - never deleted, never loosened silently.
- **Mutation sharpness** for any pin guarding a production-loss class (dropped
  jobs, double-runs, stuck states, silent data loss, security redaction).
- **Composed failures** for any fix whose failure mode is an interleaving: the
  repro must construct the interleaving, not just the single failure.

## The gates

- **The system-e2e tier** (`tests/system_e2e/`): multi-process, stateful,
  production-shaped lifecycles (rolling deploys, leader loss, resource outages,
  cancel storms) with system invariants asserted continuously. Runs in CI.
- **The full suite** on every PR - but understood correctly: it proves the
  encoded behaviors. The corpus + audit are what force the encoding to be
  complete.
- **Combined-state red-team**: after every batch of src-affecting merges, the
  merged state is attacked as a single system before the next src PR merges.
  The 2026-09-22 regressions were found by reviewers, not by CI - the red-team
  pass exists so the next one is found by a pin in CI instead.
