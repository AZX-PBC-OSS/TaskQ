"""LedgerWatch — a compliance-monitoring service built on TaskQflow.

The product story (contrived on purpose — the generic shapes any textbook
pipeline shows):

1. INGEST — a transaction batch arrives from a feed (at-least-once: the
   feed re-delivers batches).
2. ENRICH — every transaction is risk-enriched by an EXTERNAL enrichment
   API (mocked here; the mock carries the adverse world: transient
   flakes, hangs past the declared timeout, realistic latency jitter).
3. BRANCH — the enriched transaction is routed by its risk class.
4. APPROVE / CLEAR — HIGH-risk transactions wait on a human approval
   (the HITL hold with a timeout); the Expired arm escalates to the
   compliance queue. Everything else auto-clears.
5. REPORT — a typed completion report, derived from the run's own
   collect, stored durably.
6. RECONCILE — a scheduled reconciliation pass runs on a durable timer
   (a DB-clock hold whose expiry IS the tick) and reports the estate's
   health.

Everything here adopts TaskQflow from its public docs, examples, and
docstrings: the workflow engine rows are the truth, the workers are real
``taskq worker`` processes, and the tests drive all of it against a real
Postgres.
"""
