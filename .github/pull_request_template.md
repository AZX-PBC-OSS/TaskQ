<!--
The quality doctrine lives in docs/design/invariant-corpus.md:
hypothesise the invariant, pin it, attack it, merge against the corpus.
-->

## What changed

<!-- One paragraph. The why belongs here, not in the commit title. -->

## Blast-radius audit (required for src changes)

<!-- Every caller of every changed seam, and whether a pin covers the new
semantics there. "N/A (test/docs only)" is a valid answer for non-src PRs. -->

- Seam(s) touched:
- Callers audited:
- Pins covering them:

## Proof

<!-- Red/green for fixes; mutation-sharp for loss-class pins; the suites that
ran. Claims limited to what actually ran. -->

- [ ] Red/green (or N/A)
- [ ] Mutation-sharp where the pin guards a loss class
- [ ] No assertions weakened - any reconciled pin is named with its old contract
- [ ] ruff / format / pyright clean
