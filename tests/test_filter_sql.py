"""Unit tests for the shared filter→SQL WHERE condition builder.

Verifies that ``build_filter_conditions`` translates only the predicate
fields of ``JobFilter`` (queue, status, actor, identity_key, batch_id,
tags, active) into SQL fragments and parameters, and that cursor /
order_by are ignored (they are handled by the caller).
"""

from uuid import uuid4

from taskq.backend._filter_sql import build_filter_conditions
from taskq.backend._protocol import JobFilter


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

    def test_combined_filters(self) -> None:
        result = build_filter_conditions(
            JobFilter(queue="e2e", actor="my_actor", tags=("run-123",)),
        )
        assert len(result.conditions) == 3

    def test_cursor_and_order_by_ignored(self) -> None:
        """cancel_where doesn't use cursor/order_by — the builder should
        not include them in conditions."""
        result = build_filter_conditions(
            JobFilter(cursor="some-cursor", order_by=None),
        )
        assert result.conditions == ()
