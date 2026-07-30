"""Unit tests for the shared filter→SQL WHERE condition builder.

Verifies that ``build_filter_conditions`` translates only the predicate
fields of ``JobFilter`` (queue, status, actor, identity_key, batch_id,
tags, active) into SQL fragments and parameters, and that cursor /
order_by are ignored (they are handled by the caller).
"""

import re
from uuid import uuid4

from taskq.backend._filter_sql import build_filter_conditions
from taskq.backend._protocol import IdentityKey, JobFilter
from taskq.backend.statemachine import ACTIVE_STATUSES, TERMINAL_STATUSES


class TestBuildFilterConditions:
    def test_empty_filter_produces_no_conditions(self) -> None:
        result = build_filter_conditions(JobFilter())
        assert result.conditions == ()
        assert result.params == ()

    def test_queue_filter(self) -> None:
        result = build_filter_conditions(JobFilter(queue="default"))
        assert len(result.conditions) == 1
        assert "queue" in result.conditions[0]
        assert result.params == ("default",)

    def test_tags_filter(self) -> None:
        result = build_filter_conditions(JobFilter(tags=("alpha", "beta")))
        assert len(result.conditions) == 1
        assert "tags" in result.conditions[0]
        assert result.params == (["alpha", "beta"],)

    def test_batch_id_filter(self) -> None:
        bid = uuid4()
        result = build_filter_conditions(JobFilter(batch_id=bid))
        assert len(result.conditions) == 1
        assert "metadata" in result.conditions[0]

    def test_active_true_filter(self) -> None:
        result = build_filter_conditions(JobFilter(active=True))
        assert len(result.conditions) == 1
        assert "status" in result.conditions[0]
        assert set(result.params[0]) == set(ACTIVE_STATUSES)  # type: ignore[arg-type]

    def test_active_false_filter(self) -> None:
        result = build_filter_conditions(JobFilter(active=False))
        assert len(result.conditions) == 1
        assert "status" in result.conditions[0]
        assert set(result.params[0]) == set(TERMINAL_STATUSES)  # type: ignore[arg-type]

    def test_status_sequence_filter(self) -> None:
        result = build_filter_conditions(JobFilter(status=["pending", "running"]))
        assert len(result.conditions) == 1
        assert "ANY" in result.conditions[0]
        assert result.params == (["pending", "running"],)

    def test_identity_key_filter(self) -> None:
        key = IdentityKey("tenant-acme")
        result = build_filter_conditions(JobFilter(identity_key=key))
        assert len(result.conditions) == 1
        assert "identity_key" in result.conditions[0]
        assert result.params == (key,)

    def test_combined_filters(self) -> None:
        result = build_filter_conditions(
            JobFilter(queue="e2e", actor="my_actor", tags=("run-123",)),
        )
        assert len(result.conditions) == 3

    def test_cursor_and_order_by_ignored(self) -> None:
        result = build_filter_conditions(
            JobFilter(cursor="some-cursor", order_by=None),
        )
        assert result.conditions == ()

    def test_parameter_numbering_is_sequential(self) -> None:
        """$N placeholders must be sequentially numbered and align
        positionally with the params tuple."""
        result = build_filter_conditions(JobFilter(queue="q1", actor="a1", tags=("t1",)))
        numbers = [int(re.search(r"\$(\d+)", c).group(1)) for c in result.conditions]
        assert numbers == list(range(1, len(numbers) + 1)), numbers
        assert result.params == ("q1", "a1", ["t1"])


class TestSQLInjectionSafety:
    """Verify user-supplied values are always parameterized, never
    interpolated into the SQL condition string.

    Every condition must use ``$N`` positional binding — no raw user
    value may appear in ``conditions``. Column names are hardcoded
    literals, not derived from input.
    """

    def test_queue_with_sql_metacharacters_is_parameterized(self) -> None:
        payload = "'; DROP TABLE jobs; --"
        result = build_filter_conditions(JobFilter(queue=payload))
        assert result.params == (payload,)
        for cond in result.conditions:
            assert payload not in cond
            assert "$" in cond

    def test_actor_with_sql_metacharacters_is_parameterized(self) -> None:
        payload = "admin' OR '1'='1"
        result = build_filter_conditions(JobFilter(actor=payload))
        assert result.params == (payload,)
        for cond in result.conditions:
            assert payload not in cond

    def test_tags_with_sql_metacharacters_are_parameterized(self) -> None:
        payload = ("'; DELETE FROM jobs WHERE '1'='1",)
        result = build_filter_conditions(JobFilter(tags=payload))
        assert result.params == (list(payload),)
        for cond in result.conditions:
            assert payload[0] not in cond

    def test_identity_key_with_sql_metacharacters_is_parameterized(self) -> None:
        payload = IdentityKey("x'; DROP TABLE jobs; --")
        result = build_filter_conditions(JobFilter(identity_key=payload))
        assert result.params == (payload,)
        for cond in result.conditions:
            assert str(payload) not in cond

    def test_conditions_only_contain_placeholders_and_column_names(self) -> None:
        """Every condition string must be composed solely of known column
        names, operators, and $N placeholders — never raw user input."""
        result = build_filter_conditions(
            JobFilter(
                queue="myqueue",
                actor="myactor",
                tags=("tag1", "tag2"),
                batch_id=uuid4(),
            )
        )
        allowed_substrings = {
            "queue",
            "status",
            "actor",
            "identity_key",
            "metadata",
            "tags",
            " = $",
            " = ANY($",
            " @> $",
            " && $",
            "::jsonb",
            "::text[]",
        }
        for cond in result.conditions:
            stripped = cond
            for s in allowed_substrings:
                stripped = stripped.replace(s, "")
            stripped = stripped.replace("$", "").replace("0", "").replace("1", "")
            stripped = stripped.replace("2", "").replace("3", "").replace("4", "")
            stripped = stripped.replace("5", "").replace("6", "").replace("7", "")
            stripped = stripped.replace("8", "").replace("9", "")
            assert stripped == "", (
                f"Unexpected content in condition: {cond!r} (residue: {stripped!r})"
            )
