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
        expected_ctes = [
            "WITH params",
            "running_per_actor AS",
            "running_identities AS",
            "per_actor_capacity AS",
            "candidates AS",
            "identity_dedup AS",
            "ranked AS",
            "locked AS",
            "eligible_candidates AS",
            "eligible AS",
        ]
        for arm in expected_ctes:
            assert arm in rendered, f"CTE arm {arm!r} missing from rendered SQL"
        assert "UPDATE" in rendered, "rendered SQL must contain an UPDATE clause"
        assert rendered.index("UPDATE") > rendered.index("WITH"), "UPDATE must follow CTEs"

    def test_locked_contains_for_update_of_j_skip_locked(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        locked_body = _cte_body(rendered, "locked")
        assert "FOR UPDATE OF j SKIP LOCKED" in locked_body

    def test_locked_contains_limit_with_for_update(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        locked_body = _cte_body(rendered, "locked")
        assert "LIMIT" in locked_body
        assert "FOR UPDATE" in locked_body

    def test_candidates_contains_schedule_to_close_predicate(self) -> None:
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        candidates_body = _cte_body(rendered, "candidates")
        assert "schedule_to_close" in candidates_body
        assert "schedule_to_close IS NULL" in candidates_body
        assert "schedule_to_close > clock_timestamp()" in candidates_body

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
        assert "LIMIT (SELECT limit_n FROM params)" in body

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
        """FOR UPDATE OF j SKIP LOCKED must be confined to locked CTE to keep
        window functions out of the locking arm (PG forbids the combination)."""
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        locked_body = _cte_body(rendered, "locked")
        assert "FOR UPDATE OF j SKIP LOCKED" in locked_body
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
        """oversample is a parameterized multiplier used in candidates LATERAL LIMIT."""
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        params_body = _cte_body(rendered, "params")
        assert "oversample" in params_body
        candidates_body = _cte_body(rendered, "candidates")
        assert "(SELECT oversample FROM params)" in candidates_body

    def test_final_update_race_guard(self) -> None:
        """The final UPDATE WHERE j.status = 'pending' is a race guard:
        a candidate transitioned by another producer between locked row-lock
        acquisition and the UPDATE would be re-dispatched without this predicate."""
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        assert "WHERE j.id = eligible.id" in rendered
        assert "AND j.status = 'pending'" in rendered

    def test_contains_vendor_derived_structural_patterns(self) -> None:
        """Verify structural patterns derived from vendor precedents are present:
        - FOR UPDATE OF ... SKIP LOCKED (river-style atomicity)
        - DISTINCT ON for per-identity dedup (procrastinate-style serialization)
        - boolean_gate concurrency cap (pgqueuer-style LEFT JOIN + COUNT)
        - LATERAL per-actor subquery (oban-style subset CTE fence)
        """
        rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
        assert "FOR UPDATE OF j SKIP LOCKED" in rendered
        assert "DISTINCT ON" in rendered
        assert "boolean_gate" in rendered
        assert "CROSS JOIN LATERAL" in rendered


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
