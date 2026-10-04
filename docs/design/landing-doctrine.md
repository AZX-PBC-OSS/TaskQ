# The landing doctrine: the process contract for every change

How a change earns its place on `main`. Three parts: the machine (the
process a PR passes through), the hard laws (what the codebase itself
requires), and the incident pattern (the habits that keep state, tooling
and environment honest). Each rule carries its one-line rationale; none
of them is optional or subject to mood.

Related docs: the debugging playbook
([debugging-playbook.md](debugging-playbook.md)) — the method when a
lane goes red, the hardening ledger
([hardening-ledger.md](hardening-ledger.md)) — the defect classes the
method closed, and the [testing guide](../guides/testing.md) — lane and
marker doctrine. Migration mechanics live in
[CONTRIBUTING.md](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/CONTRIBUTING.md#authoring-database-migrations).

---

## 1. The machine

The process contract. It applies to every change, including docs, CI
workflow edits, and the maintainer's own PRs.

- **Red-first.** No fix lands without a failing proof that the fix turns
  green. *A fix without a red is a hypothesis; the red is what makes it
  a finding.*

- **No self-review, no self-landing.** The builder never red-teams its
  own PR and never lands it: builder, then a different red-team agent,
  then a third landing agent. *An author reviewing their own diff
  reviews their intent, not the artifact.*

- **One lane, one agent per PR.** Exactly one landing agent at a time,
  and one agent per PR, ever. *Duplicate agents race competing unions
  onto the same branch; the lane is serial by construction.*

- **The weather rule.** A 1-in-N red under load is a FINDING, not noise.
  A rerun is a concession with conjunctive preconditions — all must
  hold: the failing test is byte-identical to `main`, it passes locally
  twice, and `main`'s own head is green. One named rerun per red; a
  second reproduced red = STOP, no merge. A red your own change may
  have caused — a cure that resizes a lane or cap, now failing a
  sibling — is not weather: attribute it with a base control (the
  merge-base, run twice) before reaching for a rerun. *Rerunning past
  the first red launders a real defect, and the preconditions are what
  separate CI-local weather from the PR's own content.*

- **Bisect, don't rerun, for load-amplified mechanisms.** When load
  amplifies the failure, a green rerun proves luck, not weather:
  neutralize the suspect hunk on a scratch head and let the bisect
  convict. Bisect states are never pushed — the working branch carries
  verified trees only. *Precondition checklists can pass while the
  conclusion is false; the bisect is the only decisive evidence.*

- **The revert drill.** Every new pin must red when the defect it pins
  is reverted (the drill re-introduces the defect and expects red).
  *A pin that cannot red is decoration; the drill is the difference
  between coverage and teeth.*

## 2. The hard laws

The repo's own rules. CI enforces what it can; the rest is enforced in
review, and the review checks for them by name.

- **Shipped migrations are immutable.** Any edit to a bundled `.sql`
  file that has shipped, or to its checksum manifest, is an instant
  BLOCK. Fixes ship as NEW migrations; the CI upgrade gate blocks
  released-file edits. *Deployed databases pin released migrations by
  checksum; editing one rewrites history operators have already
  applied.* See [upgrading](../guides/upgrading.md) and
  [CONTRIBUTING.md](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/CONTRIBUTING.md#never-edit-a-released-migration).

- **Tests assert behavior, not implementation.** A pin states the
  observable contract and is proven by mutation, not by mirroring the
  source's shape, and the evidence it accepts must not be forgeable: a
  receipt counts only when the path the pin convicts is the only thing
  that can stamp it. *An implementation-shaped pin greens the refactor
  that breaks the behavior it was meant to protect; a forgeable
  receipt greens the defect that stamps its own proof.*

- **No hope-sleeps, no tolerance bumps.** A timing-sensitive assertion
  is fixed by re-anchoring on state, or by a wall-clock bound carrying
  in-code arithmetic derived from measured bands. Widening a sleep,
  a margin, or a timeout is forbidden as a fix. *A widened tolerance
  doesn't remove the race — it hides the defect the bound existed to
  expose.*

- **Cleanup by explicit name only.** Branches, containers, scratch
  schemas and databases are deleted by explicit name; never via
  wildcards or unfiltered `xargs`. *A wildcard delete is how a whole
  batch of sibling branches dies in one command.*

- **Conventional-commit titles.** Every PR title follows the
  conventional-commit grammar — `feat:`, `fix:`, `docs:`,
  `refactor:`, `test:`, `chore:` and the rest of the conventional
  types — and CI gates the exact allowed set. *The PR title is the
  squash subject and the changelog entry; release tooling reads the
  title, not the diff.*

## 3. The incident pattern

Three habits that recur whenever process or tooling misleads. They
apply to humans and agents alike.

- **Verify state before you act.** Read `reviewDecision` and
  `mergeStateStatus` before every merge attempt; check a PR's current
  state before dispatching work on it; verify a claimed commit by its
  diff, not by its log line; take branch names from the PR, not from a
  keyword guess. *The gate is the backstop, not the process — assuming
  state is how unapproved work merges and duplicate agents collide.*

- **The venv-copy hazard.** A copied venv is not an environment:
  console-script shebangs point at the absolute interpreter path of the
  machine it was copied from. Rebuild from scratch (`make install`),
  and probe the module path (`python -c "import taskq; print(taskq.__file__)"`)
  before trusting what you are testing against. *A stale environment
  fails in shapes that impersonate regressions — and acquit real ones.*

- **Attribute the environment before concluding.** A local failure may
  be a foreign service on your port — another project's container, a
  leftover stack holding your names. Identify the owner of the port,
  container and schema before blaming the diff or calling it weather;
  and hands off foreign services. *Misattributed environments cut both
  ways: foreign causes get charged to the code, and real defects get
  dismissed as noise.*
