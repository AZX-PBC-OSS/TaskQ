# THE FULL-CORPUS PROOF AT THE CURED HEAD 52fd44e1 — run 2026-10-10T02:18:07Z, CI-exact invocation (pytest -n 2 -m 'not slow and not load_sensitive' --cov=taskq --cov-fail-under=90)
# VERDICT: 7 failed / 13,152 passed / 49m15s; coverage 95.17% (floor 90) — THE FLOOR HELD
# THE 7 REDS ARE ENVIRONMENTAL, RECEIPTED:
#  (a) the 6 auth/credential reds (InvalidPasswordError) reproduce IDENTICALLY at the untouched base d24f17b9 (the base-check receipt, this round) and are ABSENT from CI's own coverage-job failure list at d24f17b9 (they PASS under CI's service provisioning — the fixture's static taskq:taskq contract vs the hand-rolled container's postgres:postgres)
#  (b) the IPv6 red is transient — passes on re-run against the same container
#  (c) NOT ONE red touches the cures' files; the CI-fix lane's six CI convictions (the noise floor, both demo legs, the projection test, both worker-main races) are all covered by 52fd44e1
#  (d) NAMED FOLLOW-UP (post-merge lane): the auth tests' _StaticPgProvider hardcodes taskq:taskq — the fixture should derive the static password from the live DSN (the local-repro tax is real; CI is unaffected)
# the F in the mid-run dots was the auth cluster's first member; the run's terminal state is above
