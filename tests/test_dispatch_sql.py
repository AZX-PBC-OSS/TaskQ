"""Unit tests for dispatch SQL constants and the dispatch_batch helper.

Tests assert SQL shape without a live PG connection.
"""

import re
from datetime import timedelta
from uuid import UUID

import pytest

from taskq.backend._dispatch_sql import (
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
    dispatch_batch,
)


def _cte_body(sql: str, cte_name: str) -> str:
    """Extract the body of a named CTE expression between the opening '(' and matching ')'."""
    marker = f"{cte_name} AS (\n"
    start = sql.index(marker) + len(marker)
    depth = 1
    i = start
    while i < len(sql) and depth > 0:
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
        i += 1
    return sql[start : i - 1]


# ── SQL shape assertions ──


class TestDispatchStrictFifoSql:
    def test_format_returns_non_empty(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        assert len(rendered) > 0

    def test_cte_declaration_order(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="schema_ns")
        # WITH RECURSIVE is the shared template's keyword: the
        # round-robin variant defines the recursive rr_keys arm under
        # it, and a RECURSIVE list with no recursive CTE (this variant)
        # is a no-op permission, so one template serves both.
        assert "WITH RECURSIVE params AS" in rendered
        expected_ctes = [
            "running_per_actor AS",
            "running_identities AS",
            "per_actor_capacity AS",
            "candidates AS",
            "identity_dedup AS",
            "ranked AS",
            "top_ids AS",
            "locked AS",
            "eligible_candidates AS",
            "eligible AS",
        ]
        for arm in expected_ctes:
            assert arm in rendered, f"CTE arm {arm!r} missing from rendered SQL"
        assert "UPDATE" in rendered, "rendered SQL must contain an UPDATE clause"
        assert rendered.index("UPDATE") > rendered.index("WITH"), "UPDATE must follow CTEs"
        # The strict-FIFO variant defines NO recursive arm: rr_keys is
        # the round-robin-only cohort enumeration (the template's prose
        # may mention the name; the arm must not exist — identity_dedup's
        # UNION ALL is a plain set operation, not a recursion).
        assert "rr_keys AS (" not in rendered
        assert "WITH RECURSIVE" in rendered

    def test_locked_contains_for_update_of_j_skip_locked(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        locked_body = _cte_body(rendered, "locked")
        assert "FOR UPDATE OF j2 SKIP LOCKED" in locked_body

    def test_limit_bound_moved_from_locked_to_top_ids(self) -> None:
        """The round's id set is finalized in top_ids BEFORE the heap is
        touched again; locked takes its row locks over that bounded set.

        locked carrying its own LIMIT was the shipped shape's defect
        carrier: the LIMIT rendered as a (SELECT ... FROM params)
        subquery the planner cannot fold, so the ranked→jobs re-join was
        estimated at the whole pending index range and planned as a hash
        join over a Seq Scan of the entire backlog (issue #130's
        O(depth) row work). The bound now lives in top_ids as a direct
        $2 parameter (folds to its value in custom-plan estimates), and
        locked drives jobs by primary key through a correlated LATERAL —
        correlation denies the hash-join path, so the lock step is
        limit_n pkey probes at every depth, shallow ones included.
        """
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        locked_body = _cte_body(rendered, "locked")
        assert "FOR UPDATE" in locked_body
        assert "LIMIT" not in locked_body, (
            "locked must not carry its own LIMIT — the bound lives in "
            "top_ids; a LIMIT here re-opens the whole-backlog re-join shape"
        )
        assert "CROSS JOIN LATERAL" in locked_body, (
            "locked must drive jobs by PK through a correlated LATERAL, "
            "not a plain join the planner can hash over the backlog"
        )
        assert "j2.id = t.id" in locked_body
        top_ids_body = _cte_body(rendered, "top_ids")
        assert "LIMIT $2::int" in top_ids_body, (
            "top_ids' bound must be the direct $2 parameter — subquery "
            "LIMITs never fold into row estimates and re-open the "
            "estimate cascade (see docs/design/sql-hotpath-followups.md)"
        )

    def test_no_subquery_limit_bounds_anywhere(self) -> None:
        """The CTE family must never return to (SELECT ... FROM params)
        LIMIT bounds: the planner cannot fold a subquery bound into a row
        estimate in ANY plan, so the candidate chain is estimated at the
        whole index range and the terminal joins get planned as hash
        joins over a Seq Scan of the entire pending backlog — the
        measured 1.04ms→55.8ms (1k→200k) depth scaling of issue #130.
        Direct $n parameters fold in custom-plan row estimates, unlike
        subquery bounds which never fold.
        """
        for variant, sql in (
            ("strict_fifo", DISPATCH_STRICT_FIFO_SQL),
            ("round_robin", DISPATCH_ROUND_ROBIN_SQL),
        ):
            rendered = sql.format(schema="taskq")
            assert "LIMIT (SELECT" not in rendered, (
                f"{variant}: a LIMIT bound rendered as a subquery — the "
                "planner cannot fold it; use the direct $n parameter"
            )

    def test_candidates_contains_schedule_to_close_predicate(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        candidates_body = _cte_body(rendered, "candidates")
        assert "schedule_to_close" in candidates_body
        assert "schedule_to_close IS NULL" in candidates_body
        # Why statement_timestamp (STABLE), not clock_timestamp (VOLATILE):
        # a volatile bound is never an Index Cond, so the candidates lateral
        # post-scan-filters the pending backlog instead of terminating at
        # the range boundary on jobs_actor_dispatch_idx — pinned by plan in
        # tests/test_sweepaudit_dispatch_bound.py.
        assert "schedule_to_close > statement_timestamp()" in candidates_body

    def test_locked_has_no_window_function(self) -> None:
        """locked CTE must NOT contain a window function — PG forbids
        FOR UPDATE + window functions in the same SELECT."""
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        locked_body = _cte_body(rendered, "locked")
        assert "OVER (" not in locked_body

    def test_eligible_candidates_contains_boolean_gate(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        body = _cte_body(rendered, "eligible_candidates")
        assert "max_concurrent IS NULL" in body
        assert "COALESCE(r.in_flight, 0)" in body
        assert "< ac.max_concurrent" in body

    def test_eligible_candidates_contains_actor_rank_window(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        body = _cte_body(rendered, "eligible_candidates")
        assert "ROW_NUMBER() OVER (" in body
        assert "PARTITION BY l.actor" in body
        assert "ORDER BY l.priority DESC, l.scheduled_at" in body
        assert "AS actor_rank" in body

    def test_eligible_contains_actor_rank_cap(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        body = _cte_body(rendered, "eligible")
        assert "ec.actor_rank <= ec.max_concurrent - ec.in_flight" in body

    def test_eligible_contains_limit_n(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        body = _cte_body(rendered, "eligible")
        # Direct $2 parameter (folds in custom-plan estimates), never the
        # subquery form — see test_no_subquery_limit_bounds_anywhere.
        assert "LIMIT $2::int" in body

    def test_final_update_contains_j_status_pending(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        assert "j.status = 'pending'" in rendered

    def test_final_update_contains_attempt_plus_1(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        assert "attempt = j.attempt + 1" in rendered

    def test_final_update_contains_returning(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        assert "RETURNING j.*" in rendered

    def test_contains_parameter_casts(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        params_body = _cte_body(rendered, "params")
        assert "$1::text[]" in params_body
        assert "$2::int" in params_body
        assert "$3::uuid" in params_body
        assert "$4::interval" in params_body
        assert "$5::int" in params_body

    def test_for_update_confined_to_locked_cte(self) -> None:
        """FOR UPDATE OF ... SKIP LOCKED must be confined to locked CTE to keep
        window functions out of the locking arm (PG forbids the combination)."""
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        locked_body = _cte_body(rendered, "locked")
        assert "FOR UPDATE OF j2 SKIP LOCKED" in locked_body
        # Window functions must NOT be in locked
        assert "OVER (" not in locked_body
        # But they MUST exist in downstream CTEs (the split is mandatory)
        candidates_body = _cte_body(rendered, "candidates")
        eligible_candidates_body = _cte_body(rendered, "eligible_candidates")
        assert "OVER (" in candidates_body or "OVER (" in eligible_candidates_body

    def test_eligible_candidates_contains_boolean_gate_and_actor_rank_columns(self) -> None:
        """Both boolean_gate and actor_rank columns are load-bearing for
        concurrency enforcement."""
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        body = _cte_body(rendered, "eligible_candidates")
        assert "AS boolean_gate" in body
        assert "AS actor_rank" in body

    def test_oversample_parameterized(self) -> None:
        """oversample is a parameterized multiplier used in the candidates
        LATERAL LIMIT — as the direct $5 parameter.

        The direct form is load-bearing, not stylistic: a parameter folds
        to its value in custom-plan row estimates, where the subquery
        form never folds and the garbage estimate cascades through the
        CTE chain into the terminal whole-backlog hash joins.
        """
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        params_body = _cte_body(rendered, "params")
        assert "oversample" in params_body
        candidates_body = _cte_body(rendered, "candidates")
        assert "LIMIT pac.residual * $5::int" in candidates_body

    def test_final_update_race_guard(self) -> None:
        """The terminal UPDATE re-finds its rows through a one-shot id
        array, with the pending re-check as the race guard.

        The ANY(ARRAY(SELECT ...)) shape is the depth bound for the
        terminal write: the array materializes once as an InitPlan and
        ``id = ANY(<array>)`` is served either as a Bitmap Index Scan on
        jobs_pkey (deep backlogs) or as a scan-level filter (shallow
        ones) — both carry at most limit_n rows per node. A FROM-clause
        join against eligible would leave the join strategy to the
        planner, which at shallow depths honestly prefers a whole-backlog
        seq scan + hash over limit_n random pkey probes — re-introducing
        depth-proportional row work exactly where it hides.

        The race guard itself is unchanged: a candidate that left the
        pending set between the lock step and this write must never be
        re-dispatched blind.
        """
        for variant, sql in (
            ("strict_fifo", DISPATCH_STRICT_FIFO_SQL),
            ("round_robin", DISPATCH_ROUND_ROBIN_SQL),
        ):
            rendered = sql.format(schema="taskq")
            assert "j.id = ANY(ARRAY(SELECT id FROM eligible))" in rendered, (
                f"{variant}: the UPDATE must bound its row-finding through "
                "the one-shot eligible id array"
            )
            assert "AND j.status = 'pending'" in rendered, (
                f"{variant}: the terminal pending re-check (race guard) must stay"
            )

    def test_per_actor_capacity_prefilters_idle_actors(self) -> None:
        """per_actor_capacity must carry an idle-actor prefilter that drops
        actors with no pending rows on the round's queues BEFORE the
        candidates CROSS JOIN fans the per-(actor, queue) lateral seek out
        over every registered actor.

        The prefilter must be a correlated per-queue LATERAL probe, not
        the EXISTS this CTE historically used. An EXISTS is a semi-join,
        and the planner executes it as a hash semi-join over a Seq Scan
        of the entire pending backlog whenever actor_config's row
        estimate makes one pass over jobs look cheaper than per-actor
        probes — and actor_config genuinely carries that estimate in
        production (one row per actor, far below autovacuum's insert
        threshold, so usually never analyzed; the planner defaults to
        ~440 rows even for a one-actor fleet). The LATERAL shape removes
        the option: the correlation on ac.actor denies the hashable
        inner path, and the per-queue equality from unnest(p.queues)
        plus the ORDER BY pins the probe to an index-ordered first-entry
        read (LIMIT 1) on jobs_actor_dispatch_idx.

        Override-safety is the load-bearing half of the shape: the
        probe must cover exactly the queues in the round's params —
        NOT the actor's actor_config home queue — so an
        ``enqueue(queue=...)`` override that lands a pending job on any
        subscribed queue keeps that actor probed. An actor filtered here
        contributes zero candidate rows either way (the lateral's
        ``j2.queue = sq.queue_name`` equality already annihilated every
        one of its pairs), so selection, fairness, and the locked/eligible
        stages are unchanged.
        """
        for variant, sql in (
            ("strict_fifo", DISPATCH_STRICT_FIFO_SQL),
            ("round_robin", DISPATCH_ROUND_ROBIN_SQL),
        ):
            rendered = sql.format(schema="taskq")
            body = _cte_body(rendered, "per_actor_capacity")
            assert "CROSS JOIN LATERAL" in body, (
                f"{variant}: per_actor_capacity lost the correlated LATERAL "
                "probe — an EXISTS form is a semi-join the planner can "
                "execute as a hash over a whole-backlog Seq Scan"
            )
            assert "unnest(p.queues)" in body, (
                f"{variant}: the probe must fan out over the round's queues from params"
            )
            assert "j.queue = pq.q" in body, (
                f"{variant}: one plain-equality probe per round queue — a "
                "queue = ANY(...) array predicate cannot serve the "
                "ORDER BY that pins the index-ordered read"
            )
            assert "ORDER BY j.priority DESC, j.scheduled_at, j.id" in body, (
                f"{variant}: the ORDER BY + LIMIT 1 pair is what makes the "
                "probe an index-ordered first-entry read"
            )
            assert "LIMIT 1" in body
            assert "j.status = 'pending'" in body, (
                f"{variant}: the prefilter must count only pending rows"
            )
            assert "ac.queue" not in body, (
                f"{variant}: the prefilter must not consult the actor's "
                "home queue — an enqueue(queue=...) override onto a "
                "subscribed queue must keep the actor probed"
            )

    def test_contains_vendor_derived_structural_patterns(self) -> None:
        """Verify essential structural patterns are present:
        - FOR UPDATE OF ... SKIP LOCKED for atomic row locking
        - DISTINCT ON for per-identity dedup and serialization
        - boolean_gate concurrency cap via LEFT JOIN + COUNT
        - LATERAL per-actor subquery for bounded subset exploration
        - the id set finalized before the heap re-join, bounded by a
          parameterized LIMIT for depth-safe execution
        """
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        assert "FOR UPDATE OF j2 SKIP LOCKED" in rendered
        assert "DISTINCT ON" in rendered
        assert "boolean_gate" in rendered
        assert "CROSS JOIN LATERAL" in rendered
        assert "top_ids AS (" in rendered
        assert "ranked AS MATERIALIZED (" in rendered


# ── DISPATCH_ROUND_ROBIN_SQL ──


class TestDispatchRoundRobinSql:
    def test_exists_and_is_string(self) -> None:
        assert isinstance(DISPATCH_ROUND_ROBIN_SQL, str)

    def test_is_valid_sql_not_todo_stub(self) -> None:
        assert "TODO" not in DISPATCH_ROUND_ROBIN_SQL

    def test_contains_fairness_rank(self) -> None:
        assert "fairness_rank" in DISPATCH_ROUND_ROBIN_SQL

    def test_contains_fairness_key_coalesce(self) -> None:
        assert "COALESCE(" in DISPATCH_ROUND_ROBIN_SQL
        assert "fairness_key" in DISPATCH_ROUND_ROBIN_SQL
        assert "__null__" in DISPATCH_ROUND_ROBIN_SQL

    def test_eligible_orders_by_fairness_rank(self) -> None:
        # In round-robin mode, fairness_rank must appear in ORDER BY of eligible
        # to interleave fairness_key cohorts before priority tiebreaking.
        eligible_body = _cte_body(DISPATCH_ROUND_ROBIN_SQL, "eligible")
        assert "fairness_rank" in eligible_body
        assert "ORDER BY" in eligible_body

    def test_no_initiative_or_ticket_id_embedded(self) -> None:
        assert re.search(r"\bI-M[0-9]+-[0-9]+\b", DISPATCH_ROUND_ROBIN_SQL) is None
        assert re.search(r"\bT-[0-9]{4}\b", DISPATCH_ROUND_ROBIN_SQL) is None

    def test_rr_keys_is_recursive_loose_index_scan(self) -> None:
        """The cohort enumeration must be the recursive row-compare walk,
        never a DISTINCT over the pending rows.

        Postgres 18 has no native skip scan (no enable_indexskipscan GUC),
        so `SELECT DISTINCT fairness_key` over a pair's pending rows is a
        full scan of them — the depth-proportional read the depth pin
        (tests/test_dispatch_backlog_depth_bound.py) forbids. The
        recursion enumerates one cohort key per bounded index seek, so
        its work is proportional to the cohort COUNT, never to any
        cohort's depth. The strict `>` row comparison guarantees
        progress (each step strictly advances the (actor, queue, key)
        triple), so the recursion terminates on a finite key space
        without a NULL-sentinel guard.
        """
        rendered = DISPATCH_ROUND_ROBIN_SQL.format(schema="taskq")
        assert "WITH RECURSIVE params AS" in rendered
        rr_body = _cte_body(rendered, "rr_keys")
        assert "UNION ALL" in rr_body
        assert "> (cur.actor, cur.queue, cur.fkey)" in rr_body, (
            "each enumeration step must seek the next strictly-greater "
            "(actor, queue, cohort) triple via a row-compare Index Cond"
        )
        assert "COALESCE(j4.fairness_key, '__null__')" in rr_body, (
            "the walk must enumerate COALESCE-normalized keys so the NULL "
            "cohort is one probe like any other"
        )

    def test_rr_candidates_are_bounded_per_cohort_probes(self) -> None:
        """The round-robin lateral must probe each cohort with
        ORDER BY + LIMIT and run the fairness window over that bounded
        union — never a window over every due row of the pair.

        A window function cannot short-circuit: the shipped shape
        computed ROW_NUMBER over EVERY due pending row and only then
        filtered fairness_rank <= residual * oversample, so the WindowAgg
        (and the scan feeding it) paid full backlog depth every round
        (issue #130). Per-cohort top-k probes yield the identical
        surviving rows with identical ranks while the window's input is
        at most cohorts * residual * oversample rows per pair.
        """
        rendered = DISPATCH_ROUND_ROBIN_SQL.format(schema="taskq")
        candidates_body = _cte_body(rendered, "candidates")
        assert "FROM rr_keys k" in candidates_body
        assert "COALESCE(j2.fairness_key, '__null__') = k.fkey" in candidates_body, (
            "the per-cohort probe must equality-match the COALESCE-normalized "
            "key on jobs_round_robin_probe_idx (a bare IS NULL probe cannot "
            "share the keyed cohorts' path; IS NOT DISTINCT FROM never "
            "indexes)"
        )
        assert "LIMIT pac.residual * $5::int" in candidates_body
        # The fairness window sits INSIDE the per-pair lateral, over the
        # bounded probe union — one partition per cohort of THIS
        # (actor, queue) pair, never a cross-pair merge of equal keys.
        assert "PARTITION BY COALESCE(c.fairness_key, '__null__')" in candidates_body
        assert "ROW_NUMBER() OVER (" in candidates_body


# ── dispatch_batch helper (unit test with fake connection) ──


class TestDispatchBatchUnit:
    @pytest.mark.asyncio
    async def test_calls_fetch_with_expected_arguments(self) -> None:
        captured_sql: str | None = None
        captured_args: tuple[object, ...] | None = None

        class _FakeConn:
            async def fetch(self, sql: str, *args: object) -> list[object]:
                nonlocal captured_sql, captured_args
                captured_sql = sql
                captured_args = args
                return [{"id": 1}, {"id": 2}]

        conn = _FakeConn()  # type: ignore[assignment] # Why: duck-typed fake; satisfies the fetch protocol but not asyncpg.Connection's full type
        worker_id = UUID("00000000-0000-0000-0000-000000000001")
        lock_lease = timedelta(seconds=30)
        rendered_sql = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")

        result = await dispatch_batch(
            conn,  # type: ignore[arg-type] # Why: duck-typed fake as above
            sql=rendered_sql,
            queues=["default", "critical"],
            limit_n=10,
            worker_id=worker_id,
            lock_lease=lock_lease,
        )

        assert captured_sql == rendered_sql
        assert captured_args is not None
        assert captured_args[0] == ["default", "critical"]
        assert captured_args[1] == 10
        assert captured_args[2] == worker_id
        assert captured_args[3] == lock_lease
        assert captured_args[4] == 2  # default oversample
        assert result == [{"id": 1}, {"id": 2}]

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_rows(self) -> None:
        class _FakeConn:
            async def fetch(self, sql: str, *args: object) -> list[object]:
                return []

        conn = _FakeConn()  # type: ignore[assignment] # Why: duck-typed fake; satisfies the fetch protocol but not asyncpg.Connection's full type
        worker_id = UUID("00000000-0000-0000-0000-000000000001")
        lock_lease = timedelta(seconds=30)
        rendered_sql = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")

        result = await dispatch_batch(
            conn,  # type: ignore[arg-type] # Why: duck-typed fake as above
            sql=rendered_sql,
            queues=["default"],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=lock_lease,
        )

        assert result == []
