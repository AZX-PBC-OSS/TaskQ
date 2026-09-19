"""Green pin: the PG and in-memory ``_batch_cap_refusals`` kernels
must stay algorithmically identical.

The design observation ("cap-refusal logic is two hand-maintained
implementations (PG and in-memory)") is that beyond the shared
``batch_cap_groups``
helper (``taskq.backend._protocol``), the REST of the cap-refusal
arithmetic - idempotency-pair dedup counting, effective-cap resolution
(stored override vs. carried literal), the
``admitted = batch_count - deduped`` / ``have + admitted > cap``
comparison, and refusal construction - is separately hand-written in
``taskq.backend._enqueue._batch_cap_refusals`` (PG) and
``taskq.testing._enqueue._batch_cap_refusals`` (in-memory). Nothing in
the type system forces the two to agree; only incidental per-backend
tests (each written against ONE implementation) catch a divergence, and
only if their scenarios happen to overlap.

This is a design/DRY finding, not an active bug: both implementations
are currently correct and in sync (verified by direct side-by-side
inspection and by the assertions below). There is nothing here to
reproduce as a RED test - the code does not currently misbehave. This
is therefore a GREEN PIN: it drives BOTH implementations with the exact
same input scenarios and asserts their outputs (refusal actor, current
count, and cap) are identical, item for item. Today it passes because
the two files happen to agree. A future edit to just one file - the
exact failure mode the finding describes, e.g. changing the idempotency-dedupe
key shape, the ``>`` vs ``>=`` cap comparison, or which of "stored
override" / "carried literal" wins - will make this test fail even
though no per-backend test alone would necessarily catch it, since each
of those is written against only one implementation.

Once the shared-kernel refactor lands (a shared pure kernel called from both
backends), this test still passes trivially - at that point it is
pinning an identity that is structurally guaranteed rather than
independently maintained, which is fine.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

from taskq import actor
from taskq.backend._enqueue import _batch_cap_refusals as _pg_batch_cap_refusals
from taskq.backend._protocol import EnqueueArgs
from taskq.backend._sql_templates import render as render_sql
from taskq.client._args import build_enqueue_args
from taskq.testing._enqueue import _batch_cap_refusals as _memory_batch_cap_refusals
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

from .test_enqueue_coverage import _FakeEnqueueConn, _Record

_SCHEMA_LABEL = "taskq"
_SQL = render_sql(_SCHEMA_LABEL)
_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int = 0
    seed: Any = None


@actor(name="cap_parity_healthy")
async def _healthy_ref(_payload: _Payload) -> None:
    pass


@actor(name="cap_parity_capped", max_pending=2)
async def _capped_ref(_payload: _Payload) -> None:
    pass


def _args(
    ref: Any,
    value: int = 0,
    *,
    max_pending: int | None = None,
    idempotency_key: str | None = None,
    idempotency_scope: str = "",
) -> EnqueueArgs:
    return build_enqueue_args(
        ref,
        _Payload(value=value),
        max_pending=max_pending if max_pending is not None else ref.max_pending,
        idempotency_key=idempotency_key,
        idempotency_scope=idempotency_scope,
    )


async def _pg_refusals(
    args_list: list[EnqueueArgs],
    *,
    existing_counts: dict[str, int],
    stored_pairs: set[tuple[str, str]],
    override_caps: dict[str, int] | None = None,
) -> list[Any]:
    """Drive the PG ``_batch_cap_refusals`` with a fake connection whose
    fetch results model the same world (existing pending counts, stored
    idempotency pairs, actor_config overrides) as the in-memory backend
    built by ``_memory_refusals``.

    ``existing_counts`` is the actor's TOTAL pending+scheduled count, as
    ``count_pending_jobs`` would report it - a stored idempotency-pair
    row is itself a pending job, so it is already included here and must
    NOT be added again on top of ``stored_pairs``. ``_memory_refusals``
    seeds the same total by creating one job per ``stored_pairs`` entry
    plus ``existing_counts`` additional (non-idempotency) jobs - so a
    caller reusing one ``existing_counts`` value for both helpers must
    pass the NET count (excluding what ``stored_pairs`` already seeds)
    to ``_memory_refusals``, or 0 when ``stored_pairs`` covers the whole
    count, exactly as the PG total already does implicitly.
    """
    override_caps = override_caps or {}
    count_records = [
        _Record({"actor": actor_name, "cnt": cnt}) for actor_name, cnt in existing_counts.items()
    ]
    override_records = [
        _Record({"actor": actor_name, "max_pending": cap})
        for actor_name, cap in override_caps.items()
    ]
    stored_records = [
        _Record(
            {
                "idempotency_scope": scope,
                "idempotency_key": key,
                # _batch_cap_refusals only reads scope/key off these rows.
            }
        )
        for scope, key in stored_pairs
    ]
    conn = _FakeEnqueueConn(
        fetch_map={
            "GROUP BY actor": count_records,
            "actor_config": override_records,
            "JOIN unnest": stored_records,
        }
    )
    return await _pg_batch_cap_refusals(conn, _SQL, args_list)  # type: ignore[arg-type]


async def _memory_refusals(
    args_list: list[EnqueueArgs],
    *,
    existing_counts: dict[str, int],
    stored_pairs: set[tuple[str, str]],
    override_caps: dict[str, int] | None = None,
) -> list[Any]:
    """Drive the in-memory ``_batch_cap_refusals`` with a backend seeded
    to the same world as ``_pg_refusals`` models."""
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    for actor_name, cap in (override_caps or {}).items():
        backend.register_actor_config(actor=actor_name, max_pending=cap)
    for actor_name, cnt in existing_counts.items():
        for i in range(cnt):
            await backend.enqueue(_seed_args(actor_name, i))
    for scope, key in stored_pairs:
        await backend.enqueue(
            _seed_args_with_idempotency(scope, key),
        )
    return await _memory_batch_cap_refusals(backend, args_list)


def _seed_args(actor_name: str, i: int) -> EnqueueArgs:
    # Why max_pending is forced to None via replace() (not the
    # build_enqueue_args kwarg - that falls back to ref.max_pending when
    # given None, it cannot express "uncapped" for a capped actor ref):
    # seeding existing rows must not itself trip the single-enqueue cap
    # check (InMemoryBackend.enqueue enforces max_pending on every call)
    # - the scenario's cap only matters for the batch call under test,
    # not for planting prior state.
    ref = _capped_ref if actor_name == _capped_ref.name else _healthy_ref
    args = build_enqueue_args(ref, _Payload(seed=i))
    return replace(args, max_pending=None)


def _seed_args_with_idempotency(scope: str, key: str) -> EnqueueArgs:
    args = build_enqueue_args(
        _capped_ref,
        _Payload(seed="dedupe-seed"),
        idempotency_key=key,
        idempotency_scope=scope,
    )
    return replace(args, max_pending=None)


def _refusal_facts(refusals: list[Any]) -> list[tuple[str, int, int]]:
    return [(r.actor, r.current_count, r.max_pending) for r in refusals]


class TestCapRefusalParity:
    """Each scenario below is run through BOTH ``_batch_cap_refusals``
    implementations with matching seeded state, and the resulting
    refusals must match exactly. This is the parity nothing
    in the type system enforces."""

    async def test_simple_over_cap_matches(self) -> None:
        args_list = [_args(_capped_ref, 0), _args(_capped_ref, 1)]

        pg = await _pg_refusals(
            args_list,
            existing_counts={_capped_ref.name: 2},
            stored_pairs=set(),
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={_capped_ref.name: 2},
            stored_pairs=set(),
        )

        assert _refusal_facts(pg) == _refusal_facts(mem)
        assert _refusal_facts(pg) == [(_capped_ref.name, 2, 2)]

    async def test_exact_fill_boundary_admits_on_both(self) -> None:
        """M1 ``>`` semantics: existing 1 + batch 1 == cap 2 admits on
        both implementations."""
        args_list = [_args(_capped_ref, 0)]

        pg = await _pg_refusals(
            args_list,
            existing_counts={_capped_ref.name: 1},
            stored_pairs=set(),
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={_capped_ref.name: 1},
            stored_pairs=set(),
        )

        assert pg == [] == mem

    async def test_idempotency_dedupe_discount_matches(self) -> None:
        """A batch of pure retries against stored idempotency pairs
        dedupes to zero net admission on both implementations, even
        though the raw batch count alone would exceed the cap."""
        args_list = [
            _args(_capped_ref, 0, idempotency_key="k1", idempotency_scope=""),
            _args(_capped_ref, 1, idempotency_key="k2", idempotency_scope=""),
        ]

        # existing_counts=2 IS the two stored-pair rows (each stored pair
        # is itself a stored, pending job): the in-memory seeding helper
        # creates one job per stored pair, so existing_counts must not
        # double-count them - mirrors how count_pending_jobs on the PG
        # side already reflects those same rows without a separate add.
        pg = await _pg_refusals(
            args_list,
            existing_counts={_capped_ref.name: 2},
            stored_pairs={("", "k1"), ("", "k2")},
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={},
            stored_pairs={("", "k1"), ("", "k2")},
        )

        assert pg == [] == mem

    async def test_effective_cap_override_matches(self) -> None:
        """A stored operator override (tighter than the carried literal)
        wins on both implementations, producing the same refusal."""
        args_list = [_args(_capped_ref, 0, max_pending=100)]

        pg = await _pg_refusals(
            args_list,
            existing_counts={_capped_ref.name: 1},
            stored_pairs=set(),
            override_caps={_capped_ref.name: 1},
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={_capped_ref.name: 1},
            stored_pairs=set(),
            override_caps={_capped_ref.name: 1},
        )

        assert _refusal_facts(pg) == _refusal_facts(mem)
        assert _refusal_facts(pg) == [(_capped_ref.name, 1, 1)]

    async def test_partial_dedupe_with_in_batch_repeat_matches(self) -> None:
        """A batch that repeats one idempotency pair internally (not
        just against stored rows) discounts every repeat past the
        first, on both implementations identically."""
        args_list = [
            _args(_capped_ref, 0, idempotency_key="repeat", idempotency_scope=""),
            _args(_capped_ref, 1, idempotency_key="repeat", idempotency_scope=""),
            _args(_capped_ref, 2, idempotency_key="repeat", idempotency_scope=""),
        ]

        pg = await _pg_refusals(
            args_list,
            existing_counts={_capped_ref.name: 0},
            stored_pairs=set(),
        )
        mem = await _memory_refusals(
            args_list,
            existing_counts={_capped_ref.name: 0},
            stored_pairs=set(),
        )

        # batch_count=3, deduped=2 (2nd and 3rd repeat) -> admitted=1,
        # have=0 -> 0+1 > cap(2) is False -> both admit.
        assert pg == [] == mem
