#!/usr/bin/env bash
# Pre-merge verifier: run before merging ANY PR. Exit non-zero on any violation.
# Usage: scripts/verify_merge_ready.sh <PR_NUMBER>
# Checks (all must pass):
#   1. Every required check is SUCCESS on the PR's CURRENT head SHA
#      (a green run from an older head is a stale verdict and fails here).
#   2. reviewDecision is APPROVED.
#   3. GitHub reports the branch mergeable (no unresolved conflicts).
#   4. The branch contains origin/main (it was rebased onto the current main).
#   5. No unpushed work: the local branch tip (if present) matches origin.
set -euo pipefail

PR="${1:?usage: verify_merge_ready.sh <PR_NUMBER>}"
REPO="AZX-PBC-OSS/TaskQ"

head_sha=$(gh pr view "$PR" --repo "$REPO" --json headRefOid -q .headRefOid)
short=${head_sha:0:7}
decision=$(gh pr view "$PR" --repo "$REPO" --json reviewDecision -q '.reviewDecision // "NONE"')
mergeable=$(gh pr view "$PR" --repo "$REPO" --json mergeable -q .mergeable)
state=$(gh pr view "$PR" --repo "$REPO" --json state -q .state)

fail() { echo "FAIL: $*" >&2; exit 1; }

[ "$state" = "OPEN" ] || fail "PR $PR is $state, not OPEN"
[ "$decision" = "APPROVED" ] || fail "reviewDecision is $decision"

# 1. Check runs must exist for THIS head and all must be success/skipped.
#    Completeness (every REQUIRED check done) is GitHub's own mergeStateStatus
#    gate: BLOCKED/DIRTY/UNSTABLE mean required checks are pending or red;
#    CLEAN means the board is done and mergeable. Line-level greps catch any
#    non-required red or in-flight run regardless.
gh api "repos/$REPO/commits/$head_sha/check-runs?per_page=100" -q \
  '.check_runs[] | .conclusion // .status' > /tmp/vmr_runs.$$
total=$(grep -c . /tmp/vmr_runs.$$ || true)
[ "$total" -gt 0 ] || fail "no check runs recorded for head $short"
grep -qE '^(FAILURE|TIMED_OUT|CANCELLED|STARTED|IN_PROGRESS|QUEUED|PENDING)$' /tmp/vmr_runs.$$ &&
  fail "head $short has non-green/pending check runs:
$(grep -nE '^(FAILURE|TIMED_OUT|CANCELLED|STARTED|IN_PROGRESS|QUEUED|PENDING)$' /tmp/vmr_runs.$$ | head)"
mss=$(gh pr view "$PR" --repo "$REPO" --json mergeStateStatus -q .mergeStateStatus)
[ "$mss" = "CLEAN" ] || fail "mergeStateStatus=$mss (a required check is pending, failed, or the base moved - wait or rebase)"
echo "ok: $total check runs green on head $short (mergeStateStatus CLEAN)"

# 2. Mergeability.
[ "$mergeable" = "MERGEABLE" ] || fail "mergeable=$mergeable"

# 3. Branch contains origin/main (rebased onto current main).
git fetch origin main -q
git merge-base --is-ancestor origin/main "$head_sha" ||
  fail "branch does not contain origin/main - rebase onto main first"

# 4. Local branch (if any) matches origin - no unpushed work.
branch=$(gh pr view "$PR" --repo "$REPO" --json headRefName -q .headRefName)
if git show-ref --verify --quiet "refs/heads/$branch"; then
  local_tip=$(git rev-parse "refs/heads/$branch")
  [ "$local_tip" = "$head_sha" ] ||
    fail "local $branch ($local_tip) != origin ($head_sha) - push or discard local work"
fi

rm -f /tmp/vmr_runs.$$
echo "READY: PR $PR ($short) is approved, conflict-free, rebased on main, fully green."
