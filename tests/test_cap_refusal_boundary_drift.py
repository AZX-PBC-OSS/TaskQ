"""Boundary-value differential probes for the PG vs in-memory
``_batch_cap_refusals`` implementations (see ``test_batch_cap_refusals_parity.py``
for the design-level pin and its rationale).

The existing parity suite drives five representative scenarios (simple
over-cap, exact-fill boundary at cap=2, idempotency dedupe, an override
cap, and an in-batch repeat). It does not drive several boundary values
that are exactly the kind of edge a one-sided future edit could get
wrong on only one backend: cap == 0, a batch landing exactly one item
past the cap, an override cap of 0 (as opposed to a carried cap of 0),
and an actor with no stored ``actor_config`` row and no prior jobs at
all (a genuinely fresh actor). These tests drive both implementations
through those values and assert identical output, extending the
parity pin's coverage rather than replacing it.
"""

from __future__ import annotations

from .test_batch_cap_refusals_parity import (
    _args,
    _capped_ref,
    _memory_refusals,
    _pg_refusals,
    _refusal_facts,
)


class TestCapRefusalBoundaryDrift:
    async def test_cap_zero_refuses_single_item_on_both(self) -> None:
        """cap == 0: even a single-item batch with no prior pending jobs
        must be refused on both backends (0 + 1 > 0)."""
        args_list = [_args(_capped_ref, 0, max_pending=0)]

        pg = await _pg_refusals(
            args_list,
            existing_counts={},
            stored_pairs=set(),
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={},
            stored_pairs=set(),
        )

        assert _refusal_facts(pg) == _refusal_facts(mem)
        assert _refusal_facts(pg) == [(_capped_ref.name, 0, 0)]

    async def test_override_cap_zero_matches(self) -> None:
        """An operator override of exactly 0 (as opposed to the carried
        cap being 0) must refuse identically on both backends."""
        args_list = [_args(_capped_ref, 0, max_pending=5)]

        pg = await _pg_refusals(
            args_list,
            existing_counts={},
            stored_pairs=set(),
            override_caps={_capped_ref.name: 0},
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={},
            stored_pairs=set(),
            override_caps={_capped_ref.name: 0},
        )

        assert _refusal_facts(pg) == _refusal_facts(mem)
        assert _refusal_facts(pg) == [(_capped_ref.name, 0, 0)]

    async def test_batch_one_past_cap_matches(self) -> None:
        """A batch exactly one item larger than the cap (cap=2, batch=3,
        no existing jobs) refuses on both, with identical current_count."""
        args_list = [
            _args(_capped_ref, 0),
            _args(_capped_ref, 1),
            _args(_capped_ref, 2),
        ]

        pg = await _pg_refusals(
            args_list,
            existing_counts={},
            stored_pairs=set(),
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={},
            stored_pairs=set(),
        )

        assert _refusal_facts(pg) == _refusal_facts(mem)
        assert _refusal_facts(pg) == [(_capped_ref.name, 0, 2)]

    async def test_fresh_actor_no_stored_row_no_prior_jobs_admits_on_both(
        self,
    ) -> None:
        """An actor with no actor_config row and no prior jobs at all —
        the carried literal is the only cap in play, existing count is
        the GROUP BY-absent (PG) / empty-scan (in-memory) zero case."""
        args_list = [_args(_capped_ref, 0)]

        pg = await _pg_refusals(
            args_list,
            existing_counts={},
            stored_pairs=set(),
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={},
            stored_pairs=set(),
        )

        assert pg == [] == mem

    async def test_dedupe_discount_lands_exactly_on_cap_boundary(self) -> None:
        """Batch count 3 against cap 2, with exactly 1 discounted via
        in-batch idempotency repeat: admitted == cap exactly, so both
        backends must ADMIT (not refuse) at the boundary."""
        args_list = [
            _args(_capped_ref, 0, idempotency_key="dup", idempotency_scope=""),
            _args(_capped_ref, 1, idempotency_key="dup", idempotency_scope=""),
            _args(_capped_ref, 2),
        ]

        pg = await _pg_refusals(
            args_list,
            existing_counts={},
            stored_pairs=set(),
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={},
            stored_pairs=set(),
        )

        # batch_count=3, deduped=1 (the repeat) -> admitted=2, have=0,
        # 0+2 > cap(2) is False -> both admit.
        assert pg == [] == mem

    async def test_mixed_capped_and_uncapped_actor_dedupe_isolated(self) -> None:
        """A batch mixing a capped actor and an uncapped actor, both using
        idempotency keys: the uncapped actor's items must not affect the
        capped actor's dedupe accounting on either backend, and the
        uncapped actor never appears in groups (invisible to backpressure)."""
        from .test_batch_cap_refusals_parity import _healthy_ref

        args_list = [
            _args(_capped_ref, 0, idempotency_key="c1", idempotency_scope=""),
            _args(_healthy_ref, 0, idempotency_key="h1", idempotency_scope=""),
            _args(_capped_ref, 1),
        ]

        pg = await _pg_refusals(
            args_list,
            existing_counts={_capped_ref.name: 1},
            stored_pairs={("", "c1")},
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={},
            stored_pairs={("", "c1")},
        )

        assert _refusal_facts(pg) == _refusal_facts(mem)
        # capped: batch_count=2, deduped=1 (c1 already stored) -> admitted=1
        # existing=1 (the stored c1 row itself) -> 1+1 > cap(2) is False
        assert pg == [] == mem
