"""The workflow progress statement constants (T21) — the two-channel
persistence's named surface.

THE BUNDLE DISCIPLINE (GAPS-ESTATE F10, ``_sql.py``'s docstring applies
verbatim): workflow SQL lives as named statement constants — never inline
f-strings at call sites. The schema identifier is the ONLY interpolated
value (validated at :meth:`WorkflowSql.build`); every caller-controlled
value uses asyncpg ``$N`` positional parameter binding (the JSONB-landmine
rule).

THE TEN STATEMENTS (the PoC's PROVEN enumeration — T21's engine needs,
ported verbatim from the proof's shape):

1.  ``PROGRESS_STATE_UPSERT_SQL`` — the STATE channel: latest-wins +
    the occurrence counter. ONE row per (node, channel) FOREVER.
2.  ``PROGRESS_STREAM_APPEND_TRIM_SQL`` — the STREAM channel: append +
    the drop-oldest trim in ONE statement (the drop count RETURNED).
3.  ``PROGRESS_RING_PRUNE_SQL`` — the retention sweep arm's backstop
    (the rank-based per-node trim, set-based over the over-bound owners).
4.  ``PROGRESS_REPLAY_NODE_SQL`` — the SSE face's node-scoped replay.
5.  ``PROGRESS_REPLAY_RUN_SQL`` — the SSE face's run-scoped replay.
6.  ``PROGRESS_RING_OLDEST_NODE_SQL`` — the named-partial check (node).
7.  ``PROGRESS_RING_OLDEST_RUN_SQL`` — the named-partial check (run).
8.  ``PROGRESS_STATE_READ_NODE_SQL`` — the state re-sync / backfill (node).
9.  ``PROGRESS_STATE_READ_RUN_SQL`` — the state re-sync / backfill (run).
10. ``PROGRESS_CHILD_RESULTS_SQL`` — the ``aggregate=`` fn's input (the
    children's RESULT rows — a READ; the fn runs at read time; no join).

The eleventh read — the map's progress line — is NOT new: it is the
CERTIFIED ``WORKFLOW_MAP_PROGRESS_SQL`` (T08) LEFT JOINed to the state
channel (the consolidation is the point; the grouped read and the
progress line are one read, never a second instrument).
"""

from __future__ import annotations

__all__ = [
    "PROGRESS_CHILD_RESULTS_SQL",
    "PROGRESS_REPLAY_NODE_SQL",
    "PROGRESS_REPLAY_RUN_SQL",
    "PROGRESS_RING_OLDEST_NODE_SQL",
    "PROGRESS_RING_OLDEST_RUN_SQL",
    "PROGRESS_RING_PRUNE_SQL",
    "PROGRESS_STATE_READ_NODE_SQL",
    "PROGRESS_STATE_READ_RUN_SQL",
    "PROGRESS_STATE_UPSERT_SQL",
    "PROGRESS_STREAM_APPEND_TRIM_SQL",
]

# 1. THE STATE CHANNEL UPSERT — latest-wins + the occurrence counter + the
# stream's dropped counter. ONE row per (node, channel) FOREVER, whatever
# the emission rate (DH1's fence: the every-emission-a-row shape is
# structurally impossible — there is no insert path).
PROGRESS_STATE_UPSERT_SQL = """\
INSERT INTO {schema}.wf_node_progress AS p
    (node_id, channel, pct, message, data, occurrences, dropped, last_seq, updated_at)
VALUES ($1, $2, $3, $4, $5::jsonb, $6::bigint, $7::bigint, $8::bigint, clock_timestamp())
ON CONFLICT (node_id, channel) DO UPDATE SET
    pct         = EXCLUDED.pct,
    message     = EXCLUDED.message,
    data        = EXCLUDED.data,
    occurrences = p.occurrences + EXCLUDED.occurrences,
    dropped     = p.dropped + EXCLUDED.dropped,
    last_seq    = EXCLUDED.last_seq,
    updated_at  = clock_timestamp()
RETURNING p.occurrences, p.dropped
"""

# 2. THE STREAM CHANNEL — append + the drop-oldest trim in ONE statement.
# The ring keeps the newest $6 rows for the node; every trimmed row is
# COUNTED (the dropped counter lands on the record in the same flush —
# the honest emitted-vs-delivered pair, DH2's fence). The trim is
# rank-based per node (the global seq-window trim was rejected: it
# under-retains under interleave — the PoC's red-team note).
PROGRESS_STREAM_APPEND_TRIM_SQL = """\
WITH ins AS (
    INSERT INTO {schema}.wf_node_stream (node_id, flow_id, class, kind, payload)
    VALUES ($1, $2, $3, $4, $5::jsonb)
    RETURNING seq
), trim AS (
    DELETE FROM {schema}.wf_node_stream v
    WHERE v.node_id = $1
      AND v.seq IN (
          SELECT seq FROM {schema}.wf_node_stream
          WHERE node_id = $1
          ORDER BY seq DESC
          OFFSET GREATEST($6::int - 1, 0)
      )
    RETURNING 1
)
SELECT (SELECT seq FROM ins) AS seq, (SELECT count(*) FROM trim)::int AS dropped
"""

# 3. THE RETENTION SWEEP ARM (the ring's backstop — DH1's "prunes on
# schedule"): every over-bound node's ring trimmed to the bound in one
# set-based pass, rank-based per node. Bounded twice: the owners set is
# LIMITed ($2) and each owner's trim is the append-trim's own rank shape —
# a leak (the red world's unpruned rings) drains over passes, never
# unbounded in one.
PROGRESS_RING_PRUNE_SQL = """\
WITH owners AS (
    SELECT node_id
    FROM {schema}.wf_node_stream
    GROUP BY node_id
    HAVING count(*) > $1::int
    LIMIT $2::int
), ranked AS (
    SELECT seq, row_number() OVER (PARTITION BY node_id ORDER BY seq DESC) AS rn
    FROM {schema}.wf_node_stream
    WHERE node_id IN (SELECT node_id FROM owners)
), victims AS (
    SELECT seq FROM ranked WHERE rn > $1::int
), gone AS (
    DELETE FROM {schema}.wf_node_stream v USING victims
    WHERE v.seq = victims.seq
    RETURNING 1
)
SELECT count(*)::int AS pruned FROM gone
"""

# 4-5. THE SSE FACE's REPLAY READS (decision f) — the seq-cursor replay,
# node- or run-scoped. Exactly one of the scope filters binds; the cursor
# is the ONE seq space's position.
PROGRESS_REPLAY_NODE_SQL = """\
SELECT s.seq, s.node_id, s.flow_id, s.class, s.kind, s.payload, s.emitted_at
FROM {schema}.wf_node_stream s
WHERE s.seq > $1::bigint AND s.node_id = $2::uuid
ORDER BY s.seq
LIMIT $3::int
"""

PROGRESS_REPLAY_RUN_SQL = """\
SELECT s.seq, s.node_id, s.flow_id, s.class, s.kind, s.payload, s.emitted_at
FROM {schema}.wf_node_stream s
WHERE s.seq > $1::bigint AND s.flow_id = $2::uuid
ORDER BY s.seq
LIMIT $3::int
"""

# 6-7. THE RING-OLDEST CHECKS — the named-partial mode's predicate input:
# the oldest seq the node's / the run's rings still retain. A cursor below
# it is a cursor the ring pruned past (DH6 — the honest degraded mode).
PROGRESS_RING_OLDEST_NODE_SQL = """\
SELECT min(seq)::bigint FROM {schema}.wf_node_stream WHERE node_id = $1::uuid
"""

PROGRESS_RING_OLDEST_RUN_SQL = """\
SELECT min(seq)::bigint FROM {schema}.wf_node_stream WHERE flow_id = $1::uuid
"""

# 8-9. THE STATE CHANNEL READS (the re-sync payload + the display's
# backfill). Latest-wins needs no history: these rows alone reconstruct
# the display's progress half at every connect (DH6's cure).
PROGRESS_STATE_READ_NODE_SQL = """\
SELECT p.node_id, p.channel, p.pct, p.message, p.data, p.occurrences,
       p.dropped, p.last_seq, p.updated_at
FROM {schema}.wf_node_progress p
WHERE p.node_id = $1::uuid
ORDER BY p.channel
"""

# The run-scoped read joins the jobs table on the flow link (the same
# flow-nodes shape the rollup reads — never a seq scan; the partial index
# 01.00.25_02 serves the bound).
PROGRESS_STATE_READ_RUN_SQL = """\
SELECT p.node_id, p.channel, p.pct, p.message, p.data, p.occurrences,
       p.dropped, p.last_seq, p.updated_at
FROM {schema}.wf_node_progress p
JOIN {schema}.jobs j ON j.id = p.node_id
WHERE (j.metadata->>'flow_id')::uuid = $1::uuid
  AND j.metadata ? 'flow_id'
"""

# 10. THE USER'S ``aggregate=`` FN INPUT (decision c): the children's
# RESULT rows — a READ; the fn runs over these AT READ TIME. No join node
# exists for this (DH8's fence: the join is for DATAFLOW, progress
# aggregation is OBSERVABILITY).
PROGRESS_CHILD_RESULTS_SQL = """\
SELECT id, status::text AS status, result
FROM {schema}.jobs
WHERE parent_id = $1::uuid AND status = 'succeeded' AND result IS NOT NULL
"""
