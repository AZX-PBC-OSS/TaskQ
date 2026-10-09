"""The T08 totality PROPERTY (GAPS-ESTATE F6a): the derivation table's
totality is a tested property — a hypothesis-driven generator over RANDOM
multisets of node views (including the two derived pending-row shapes —
join-wait and held — and the ABSORBED-failure class, B2's clause) asserts
the derivation yields EXACTLY ONE workflow status for every input. A new
node representation added without a derivation-table row reds the
property: the totality is the test.

Hypothesis is pinned in pyproject (the property test pattern of
test_backend_property.py); the profile keeps the CI budget sane.
"""

from __future__ import annotations

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from taskq.workflows._status import NodeView, derive_workflow_status

#: The node-view generator: every jobs status + the derived shapes.
_STATUSES = (
    "pending",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
    # The collector's view vocabulary (never a jobs row status — the
    # derivation's complete row reads it).
    "skipped",
)
_BLOCKING_REASONS = (None, "join", "orphan_parent", "failed_parent", "body_unavailable")


@st.composite
def _node_views(draw: st.DrawFn) -> NodeView:
    status = draw(st.sampled_from(_STATUSES))
    return NodeView(
        status=status,
        # The join-wait shape: pending + the counter carrying.
        deps_pending=draw(st.integers(0, 5) if status == "pending" else st.just(0)),
        blocking_reason=draw(
            st.sampled_from(_BLOCKING_REASONS) if status == "pending" else st.just(None)
        ),
        held=draw(st.booleans()) if status == "pending" else False,
        # B2's ABSORBED-FAILURE CLASS in the generator: the absorbed
        # clause's inputs are exercised, not just the plain failed row.
        # The terminal-crash fold's absorption consistency: a
        # crashed/abandoned row with an absorbing edge reads through it
        # the same way (the fold makes them the failed class).
        absorbed=draw(st.booleans()) if status in ("failed", "crashed", "abandoned") else False,
        cancel_in_flight=draw(st.booleans()),
    )


class TestDerivationTotality:
    """THE TOTALITY IS A PROPERTY: exactly one workflow status per input,
    zero inputs uncovered."""

    @settings(max_examples=500, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(st.lists(_node_views(), max_size=12))
    def test_exactly_one_status_for_every_multiset(self, nodes: list[NodeView]) -> None:
        result = derive_workflow_status(tuple(nodes))
        assert result in ("running", "failed", "blocked", "complete", "cancelled", "pending"), (
            f"the derivation yielded {result!r} — outside the workflow "
            f"status vocabulary (input: {nodes})"
        )

    @settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(st.lists(_node_views(), min_size=1, max_size=8))
    def test_the_derivation_is_deterministic(self, nodes: list[NodeView]) -> None:
        """The derivation is a PURE function of the multiset: the same
        input, the same status — order-independent (the table's rows read
        predicates over the WHOLE multiset, never a first-match)."""
        shuffled = list(reversed(nodes))
        assert derive_workflow_status(tuple(nodes)) == derive_workflow_status(tuple(shuffled)), (
            "the derivation read input ORDER — the table's rows are "
            "predicates over the multiset, the precedence is the ROW "
            "order, never the input order"
        )

    @settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])
    @given(st.lists(_node_views(), min_size=1, max_size=8))
    def test_the_absorbed_class_never_derives_failed_from_its_parent(
        self, nodes: list[NodeView]
    ) -> None:
        """B2's clause as a property: a multiset whose ONLY failures are
        ABSORBED (collect/maybe) derives through the parent's outcome —
        never 'failed' (the flip: un-absorb → failed)."""
        unabsorbed_failure = any(
            n.status in ("failed", "crashed", "abandoned") and not n.absorbed for n in nodes
        )
        derived = derive_workflow_status(tuple(nodes))
        if not unabsorbed_failure:
            assert derived != "failed", f"an all-absorbed multiset derived failed: {nodes}"
        else:
            # THE PRECEDENCE: failed only ever loses to row 1 — genuine
            # liveness (a RUNNING row — the reclaim arms' only input) or
            # a cancel in flight. The terminal-crash class is NOT
            # liveness (the crashed-terminal wedge's cure): a crashed
            # row's failed-class verdict never loses to itself.
            if derived != "failed":
                assert any(n.status == "running" for n in nodes) or any(
                    n.cancel_in_flight for n in nodes
                ), (
                    f"a non-absorbed failure with nothing running derived "
                    f"{derived!r} — the failed row lost its precedence: {nodes}"
                )
