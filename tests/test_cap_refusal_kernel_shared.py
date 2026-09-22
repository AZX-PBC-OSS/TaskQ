"""The cap-refusal kernel must be SHARED, not merely identical.

``test_batch_cap_refusals_parity.py`` and
``test_cap_refusal_boundary_drift.py`` prove the two
``_batch_cap_refusals`` implementations agree on their outputs; parity of
outputs is necessary but does not prove the DRY property the confirmed
defect asks for: that the effective-cap resolution, the
idempotency-pair discount, and the refusal comparison live in ONE pure
kernel (``taskq.backend._protocol.batch_cap_refusal_kernel``) so a
future change to the cap logic is made once and both backends move
together. Two hand-maintained copies can pass every output pin while the
next edit silently updates only one of them.

This module pins the structural property itself, two ways:

1. NEGATIVE CONTROL: monkeypatch a deliberately divergent kernel into
   place and show BOTH implementations' refusals move together, in
   lockstep, away from their baseline. A backend still carrying inline
   cap arithmetic would be invisible to the patched kernel and would
   stay at baseline while the other moved - exactly the drift the defect
   describes. A light static assertion pins that both backend modules'
   ``_batch_cap_refusals`` actually invoke the kernel by name.

2. DIFFERENTIAL SWEEP: a randomized-but-seeded scenario grid (caps
   0/1/n, override present/absent, duplicate idempotency keys present/
   absent, in-batch repeats, stored-pair collisions, mixed capped/
   uncapped actors) drives both implementations through the same world
   and asserts identical refusal facts for every scenario. The seed is
   recorded below; rerunning the test replays the identical grid.
"""

from __future__ import annotations

import inspect
import random
from dataclasses import replace
from typing import Any

import pytest

from taskq import actor
from taskq.backend._enqueue import _batch_cap_refusals as _pg_batch_cap_refusals
from taskq.backend._protocol import (
    Container,
    EnqueueArgs,
    Mapping,
    batch_cap_groups,
    batch_cap_refusal_kernel,
)
from taskq.backend._sql_templates import render as render_sql
from taskq.client._args import build_enqueue_args
from taskq.exceptions import MaxPendingExceededError
from taskq.testing._enqueue import _batch_cap_refusals as _memory_batch_cap_refusals
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

from .test_batch_cap_refusals_parity import (
    _START,
    _capped_ref,
    _healthy_ref,
    _memory_refusals,
    _Payload,
    _pg_refusals,
    _refusal_facts,
)
from .test_enqueue_coverage import _FakeEnqueueConn, _Record


async def _memory_world(
    args_list: list[EnqueueArgs],
    *,
    existing_counts: dict[str, int],
    stored_pairs: set[tuple[str, str]],
    override_caps: dict[str, int | None] | None = None,
) -> list[Any]:
    """``_memory_refusals`` with arbitrary actor names, including unicode
    ones: the parity module's seeding helper hardwires the ref choice to
    its own two actors (any other name silently lands on the uncapped
    ref), which would make a unicode actor's seeded counts vanish - the
    mirror world here seeds each NAMED actor's rows under that name."""
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    for actor_name, cap in (override_caps or {}).items():
        backend.register_actor_config(actor=actor_name, max_pending=cap)
    for actor_name, cnt in existing_counts.items():
        for i in range(cnt):
            ref = _capped_ref if actor_name == _capped_ref.name else _healthy_ref
            base = build_enqueue_args(ref, _Payload(seed=i))
            await backend.enqueue(replace(base, max_pending=None, actor=actor_name))
    for scope, key in stored_pairs:
        base = build_enqueue_args(
            _capped_ref,
            _Payload(seed="dedupe-seed"),
            idempotency_key=key,
            idempotency_scope=scope,
        )
        await backend.enqueue(replace(base, max_pending=None))
    return await _memory_batch_cap_refusals(backend, args_list)


# The sweep's seed is part of the pin: the grid below is randomized to
# cover combinations no hand-written scenario list would think to pair,
# but it is fully determined by this seed, so a failure names a
# reproducible world, not a lottery.
_SWEEP_SEED = 165
_SWEEP_SCENARIOS = 64

_CAPPED = _capped_ref.name


_NFC_NAME = "cap_parity_caf\u00e9"
"""NFC: the e-acute as ONE codepoint (U+00E9)."""

_NFD_NAME = "cap_parity_cafe\u0301"
"""NFD: plain e + COMBINING ACUTE (U+0301), visually identical to NFC,
a different codepoint sequence, a different actor."""


@actor(name=_NFC_NAME, max_pending=2)
async def _capped_unicode_ref(_payload: _Payload) -> None:
    pass


@actor(name=_NFD_NAME, max_pending=2)
async def _capped_unicode_nfd_ref(_payload: _Payload) -> None:
    pass


# Actor names are free-form text (no _IDENT_RE gate on them), so NFC/NFD
# twins are reachable in production data. Python dict keys and PG text
# equality under a deterministic collation are both codepoint-exact, so
# the two names are distinct actors on BOTH sides; the sweep pins that
# neither side's grouping folds them together.
_UNICODE_CAPPED = _capped_unicode_ref.name
_UNICODE_NFD_CAPPED = _capped_unicode_nfd_ref.name
assert _UNICODE_NFD_CAPPED != _UNICODE_CAPPED


def _args_for(
    ref: Any,
    value: int,
    *,
    cap: int | None = 2,
    key: str | None = None,
) -> EnqueueArgs:
    """Build one batch item. ``cap=None`` means genuinely uncapped: the
    builder falls back to ``ref.max_pending`` when given None, so the
    uncapped shape needs ``replace`` afterwards (same trick as the
    parity pin's seed helpers)."""
    args = build_enqueue_args(
        ref,
        _Payload(value=value),
        max_pending=cap if cap is not None else ref.max_pending,
        idempotency_key=key,
        idempotency_scope="",
    )
    if cap is None:
        args = replace(args, max_pending=None)
    return args


class TestKernelIsShared:
    """The DRY guarantee: one kernel, both backends wired through it."""

    def test_both_backends_call_the_kernel_by_name(self) -> None:
        """Static assertion: each backend's ``_batch_cap_refusals``
        invokes ``batch_cap_refusal_kernel`` - the grouping helper alone
        is not enough, the resolution/discount/comparison must come from
        the shared kernel too."""
        import taskq.backend._enqueue as pg_module
        import taskq.testing._enqueue as memory_module

        for module in (pg_module, memory_module):
            source = inspect.getsource(module._batch_cap_refusals)
            assert "batch_cap_refusal_kernel(" in source, (
                f"{module.__name__}._batch_cap_refusals no longer routes "
                "through the shared cap-refusal kernel: the two "
                "implementations are drifting back into hand-maintained "
                "copies (the defect this pin exists to prevent)."
            )

    async def test_injected_kernel_divergence_moves_both_backends(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """NEGATIVE CONTROL: replace the kernel with a deliberately
        divergent one (inflate every refusal's cap to 0, refuse every
        capped actor when the real kernel would admit) and require BOTH
        implementations' refusals to move identically. An implementation
        still doing its own arithmetic ignores the patch and stays at
        baseline: the asymmetry this test turns red."""

        def _divergent_kernel(
            args_list: list[EnqueueArgs],
            *,
            stored_overrides: Mapping[str, int | None],
            stored_pairs: Container[tuple[str, str]],
            existing_counts: Mapping[str, int],
        ) -> list[MaxPendingExceededError]:
            real = batch_cap_refusal_kernel(
                args_list,
                stored_overrides=stored_overrides,
                stored_pairs=stored_pairs,
                existing_counts=existing_counts,
            )
            if real:
                # Divergence A: refuse at the tightened boundary (cap 0),
                # keeping the observed count.
                return [
                    MaxPendingExceededError(
                        actor=r.actor, current_count=r.current_count, max_pending=0
                    )
                    for r in real
                ]
            # Divergence B: refuse every capped actor outright.
            return [
                MaxPendingExceededError(actor=a, current_count=0, max_pending=0)
                for a in batch_cap_groups(args_list)
            ]

        # One world where the true kernel ADMITS (1 item, cap 2, empty
        # store) and one where it REFUSES (3 items, cap 2). Both are
        # driven through both backends twice: real kernel first
        # (baseline), divergent kernel second (injected).
        admit_args = [_args_for(_capped_ref, 0)]
        refuse_args = [_args_for(_capped_ref, i) for i in range(3)]
        no_existing: dict[str, int] = {}
        no_pairs: set[tuple[str, str]] = set()
        worlds: list[tuple[list[EnqueueArgs], dict[str, int], set[tuple[str, str]]]] = [
            (admit_args, no_existing, no_pairs),
            (refuse_args, no_existing, no_pairs),
        ]

        for args_list, existing, pairs in worlds:
            pg = await _pg_refusals(
                args_list,
                existing_counts=existing,
                stored_pairs=pairs,
            )
            mem = await _memory_refusals(
                args_list,
                existing_counts=existing,
                stored_pairs=pairs,
            )
            assert _refusal_facts(pg) == _refusal_facts(mem)

        monkeypatch.setattr("taskq.backend._enqueue.batch_cap_refusal_kernel", _divergent_kernel)
        monkeypatch.setattr("taskq.testing._enqueue.batch_cap_refusal_kernel", _divergent_kernel)

        for args_list, existing, pairs in worlds:
            pg = await _pg_refusals(
                args_list,
                existing_counts=existing,
                stored_pairs=pairs,
            )
            mem = await _memory_refusals(
                args_list,
                existing_counts=existing,
                stored_pairs=pairs,
            )
            assert pg != [] and mem != [], (
                "the divergent kernel did not move this backend's "
                "refusals: it is not resolving caps through the kernel"
            )
            # One change, both behave: the injected divergence shows up
            # identically on both sides (cap forced to 0, every capped
            # actor refused).
            assert _refusal_facts(pg) == _refusal_facts(mem)
            assert all(r.max_pending == 0 for r in pg)


class TestDifferentialSweep:
    """Seeded randomized grid: both backends, same world, same refusals."""

    async def test_seeded_grid_produces_identical_refusal_sets(self) -> None:
        # A fixed seed keeps the differential sweep deterministic; nothing
        # cryptographic. The seed is the pin: rerunning replays the grid.
        rng = random.Random(_SWEEP_SEED)  # noqa: S311
        mismatched: list[str] = []
        seen_axes: dict[str, bool] = {
            "carried_cap_zero": False,
            "carried_cap_one": False,
            "carried_cap_n": False,
            "uncapped_item": False,
            "override_present": False,
            "override_absent": False,
            "override_cleared_null": False,
            "duplicate_key_in_batch": False,
            "key_collides_with_stored_pair": False,
            "uncapped_actor_item_mixed_in": False,
        }
        for scenario in range(_SWEEP_SCENARIOS):
            # Stored operator override for the capped actor: absent (no
            # actor_config row), CLEARED (a row whose max_pending is NULL -
            # the operator ran `actor-config clear` - which must resolve to
            # the carried literal exactly like absent, never to 0), or a
            # tighter/equal/looser stored value (2 is the carried literal).
            override_roll: str | int = rng.choice(["absent", "cleared", 0, 1, 2, 3, 10])
            override: int | None
            if override_roll == "absent":
                override_caps: dict[str, int | None] | None = None
                override = None
            elif override_roll == "cleared":
                override_caps = {_CAPPED: None}
                override = None
                seen_axes["override_cleared_null"] = True
            else:
                stored_value = int(override_roll)
                override_caps = {_CAPPED: stored_value}
                override = stored_value
            if override is not None:
                seen_axes["override_present"] = True
            else:
                seen_axes["override_absent"] = True
            # Stored idempotency pairs: some may collide with batch keys,
            # some are unrelated. Sorted where iterated so the grid is
            # deterministic regardless of set hashing.
            stored_pairs = {("", f"stored-{rng.randint(0, 4)}") for _ in range(rng.randint(0, 3))}
            stored_keys = sorted(key for _, key in stored_pairs)

            # Live pending+scheduled rows BEYOND the stored-pair rows
            # (each stored pair seeds one pending job for the capped
            # actor, which the PG total must include and the in-memory
            # net count must not - see _pg_refusals' docstring).
            extra_existing = rng.randint(0, 2)
            pg_existing: dict[str, int] = {_CAPPED: extra_existing + len(stored_pairs)}

            # The batch: 1..5 items for the capped actor plus, on some
            # scenarios, an uncapped-actor item (invisible to
            # backpressure, must not perturb the capped actor's
            # accounting). Per item: carried cap 0/1/2/uncapped, and a
            # key that is new, repeated in-batch, or stored.
            key_pool = ["fresh-a", "fresh-b", "repeat-me", *stored_keys]
            caps = [rng.choice([0, 1, 2, None]) for _ in range(rng.randint(1, 5))]
            keys = [rng.choice([None, *key_pool]) for _ in caps]
            args_list = [
                _args_for(_capped_ref, i, cap=cap, key=key)
                for i, (cap, key) in enumerate(zip(caps, keys, strict=True))
            ]
            mix_uncapped_actor = rng.random() < 0.5
            if mix_uncapped_actor:
                args_list.append(_args_for(_healthy_ref, 0, cap=None, key="uncapped-key"))

            # Coverage bookkeeping: the grid must actually span the axes
            # the defect names, or the agreement proves nothing.
            if 0 in caps:
                seen_axes["carried_cap_zero"] = True
            if 1 in caps:
                seen_axes["carried_cap_one"] = True
            if 2 in caps:
                seen_axes["carried_cap_n"] = True
            if None in caps:
                seen_axes["uncapped_item"] = True
            seen_axes["override_present" if override is not None else "override_absent"] = True
            keyed = [k for k in keys if k is not None]
            if len(keyed) != len(set(keyed)):
                seen_axes["duplicate_key_in_batch"] = True
            if any(k in stored_keys for k in keyed):
                seen_axes["key_collides_with_stored_pair"] = True
            if mix_uncapped_actor:
                seen_axes["uncapped_actor_item_mixed_in"] = True

            pg = await _pg_refusals(
                args_list,
                existing_counts=pg_existing,
                stored_pairs=stored_pairs,
                override_caps=override_caps,
            )
            mem = await _memory_refusals(
                args_list,
                existing_counts={_CAPPED: extra_existing},
                stored_pairs=stored_pairs,
                override_caps=override_caps,
            )
            if _refusal_facts(pg) != _refusal_facts(mem):
                mismatched.append(
                    f"scenario {scenario}: pg={_refusal_facts(pg)!r} "
                    f"mem={_refusal_facts(mem)!r} "
                    f"override={override} stored_pairs={sorted(stored_pairs)} "
                    f"extra_existing={extra_existing} "
                    f"caps={caps} keys={keys}"
                )
        assert not mismatched, (
            f"{len(mismatched)}/{_SWEEP_SCENARIOS} seeded scenarios diverged "
            f"(seed={_SWEEP_SEED}):" + "\n".join(mismatched)
        )
        assert all(seen_axes.values()), (
            f"seed={_SWEEP_SEED} grid missed axes: "
            f"{[axis for axis, hit in seen_axes.items() if not hit]}"
        )


class TestAxesTheGridMissed:
    """The seeded grid's actor world is one ASCII capped actor plus one
    uncapped neighbour; these scenarios cover what that world cannot
    express: a CLEARED override row, a cross-actor duplicate pair with
    different caps, and unicode actor names (including NFC/NFD twins)."""

    @staticmethod
    async def _both_worlds(
        args_list: list[EnqueueArgs],
        *,
        existing_counts: dict[str, int],
        stored_pairs: set[tuple[str, str]],
        override_caps: dict[str, int | None] | None = None,
    ) -> tuple[list[Any], list[Any]]:
        """Run one world through both backends; returns (pg, mem) facts."""
        pg = await _pg_refusals(
            args_list,
            existing_counts=existing_counts,
            stored_pairs=stored_pairs,
            override_caps=override_caps,
        )
        mem = await _memory_world(
            args_list,
            existing_counts=existing_counts,
            stored_pairs=stored_pairs,
            override_caps=override_caps,
        )
        return _refusal_facts(pg), _refusal_facts(mem)

    async def test_cleared_override_row_resolves_to_the_carried_literal(
        self,
    ) -> None:
        """A stored actor_config row with max_pending NULL (the operator
        cleared the override) must resolve to the carried literal exactly
        like an absent row - on both backends.

        The failure this pins: an I/O mapping that coerces the cleared
        column instead of passing it through (``rec['max_pending'] or 0``
        or ``cfg.max_pending or 0``) turns 'no override' into a ZERO cap
        and refuses every batch for that actor. The seeded sweep cannot
        see this axis: its 'absent' roll builds no row at all."""
        args_list = [_args_for(_capped_ref, 0), _args_for(_capped_ref, 1)]

        cleared_pg, cleared_mem = await self._both_worlds(
            args_list,
            existing_counts={_CAPPED: 2},
            stored_pairs=set(),
            override_caps={_CAPPED: None},  # a row exists, its cap is NULL
        )
        absent_pg, absent_mem = await self._both_worlds(
            args_list,
            existing_counts={_CAPPED: 2},
            stored_pairs=set(),
            override_caps=None,  # no row at all
        )

        assert cleared_pg == absent_pg, (
            "a cleared (NULL) override row and an absent row resolved to "
            f"different caps: cleared={cleared_pg}, absent={absent_pg}"
        )
        assert cleared_pg == cleared_mem
        assert absent_pg == absent_mem
        # Carried literal 2, existing 2, batch 2 -> 4 > 2: refused at 2.
        assert cleared_pg == [(_CAPPED, 2, 2)], (
            "a cleared override must leave the carried literal standing "
            "(a 0-cap coercion would refuse with max_pending=0)"
        )

    async def test_cleared_override_for_a_registry_uncapped_actor_stays_uncapped(
        self,
    ) -> None:
        """A cleared row for an actor whose carried literal is None must
        not invent a cap (0 or otherwise) on either backend."""
        args_list = [_args_for(_healthy_ref, 0, cap=None)]
        pg, mem = await self._both_worlds(
            args_list,
            existing_counts={_healthy_ref.name: 50},
            stored_pairs=set(),
            override_caps={_healthy_ref.name: None},
        )
        assert pg == [] == mem, (
            f"a cleared override on an uncapped actor manufactured a cap: pg={pg}, mem={mem}"
        )

    async def test_cross_actor_duplicate_pair_discounts_the_second_carrier(
        self,
    ) -> None:
        """A (scope, key) pair carried by items of two DIFFERENT capped
        actors with DIFFERENT carried caps: the pair dedupes globally
        (the INSERT's arbiter is (idempotency_scope, idempotency_key),
        not per-actor), so the second carrier's item writes no row and
        consumes none of ITS actor's capacity. Both backends must make
        that identical call. (At write time such a batch aborts with the
        typed actor-mismatch refusal - the preflight's job is to answer
        identically BEFORE that, whatever the caller does with it.)"""
        first = _args_for(_capped_ref, 0, cap=2, key="shared-k")
        # The same pair, a different actor, a different carried cap.
        second = _args_for(_capped_unicode_ref, 0, cap=3, key="shared-k")
        args_list = [first, second]

        pg, mem = await self._both_worlds(
            args_list,
            existing_counts={_CAPPED: 0, _UNICODE_CAPPED: 0},
            stored_pairs=set(),
        )
        assert pg == mem
        # batch 1 discounted to 0 net for the second carrier: its actor
        # must NOT be refused for the pair it dedupes onto.
        assert all(actor_ != _UNICODE_CAPPED for actor_, _, _ in pg), (
            f"the second carrier of a duplicate pair was refused for "
            f"capacity it does not consume: {pg}"
        )

    async def test_unicode_actor_names_group_distinctly_on_both_backends(
        self,
    ) -> None:
        """NFC and NFD spellings of the SAME visible name are two actors
        everywhere: no normalization, casefold, or strip may enter either
        backend's grouping, and one actor's over-cap refusal must not
        drag its lookalike in."""
        nfc_item = _args_for(_capped_unicode_ref, 0, cap=2, key=None)
        nfd_item = _args_for(_capped_unicode_nfd_ref, 0, cap=2, key=None)
        args_list = [nfc_item, nfd_item]
        # Both lookalikes are over-cap on their own counts...
        existing = {_UNICODE_CAPPED: 2, _UNICODE_NFD_CAPPED: 2}

        pg, mem = await self._both_worlds(args_list, existing_counts=existing, stored_pairs=set())
        assert pg == mem
        assert {a for a, _, _ in pg} == {_UNICODE_CAPPED, _UNICODE_NFD_CAPPED}, (
            f"lookalike unicode actor names collapsed in the grouping: {pg}"
        )

        # ...and a cap breach on ONE of them leaves the other alone.
        pg_one, mem_one = await self._both_worlds(
            args_list,
            existing_counts={_UNICODE_CAPPED: 5, _UNICODE_NFD_CAPPED: 0},
            stored_pairs=set(),
        )
        assert pg_one == mem_one
        assert [a for a, _, _ in pg_one] == [_UNICODE_CAPPED], (
            f"one lookalike's refusal leaked into its twin: {pg_one}"
        )


class TestPgPreflightStatementShape:
    """The kernel extraction must not have changed the PG preflight's
    statement shape: the same fetches in the same order as the pre-extraction
    code (actor-config snapshot, then the grouped count, then the
    stored-pair probe when the batch carries keyed items), and no
    advisory-lock statement - the bulk tier's count-then-insert race is a
    DOCUMENTED residual (the single path takes the lock; bulk paths
    deliberately do not, for throughput). A silent new fetch, a reordered
    fetch, or a quietly added lock is exactly the atomicity drift this
    pin exists to catch."""

    _SQL = render_sql("taskq")

    @staticmethod
    def _recorded_conn(override_rows: list[_Record]) -> tuple[_FakeEnqueueConn, list[str]]:
        conn = _FakeEnqueueConn(
            fetch_map={
                "GROUP BY actor": [],
                "actor_config": override_rows,
                "JOIN unnest": [],
            }
        )
        fetch_sqls: list[str] = []
        inner_fetch = conn.fetch

        async def _recording_fetch(sql: str, *args: object) -> list[_Record]:
            fetch_sqls.append(sql)
            return await inner_fetch(sql, *args)

        conn.fetch = _recording_fetch  # type: ignore[method-assign]
        return conn, fetch_sqls

    async def test_three_fetches_in_documented_order_with_keyed_items(self) -> None:
        conn, fetch_sqls = self._recorded_conn([])
        args_list = [_args_for(_capped_ref, 0, key="k1")]

        refusals = await _pg_batch_cap_refusals(conn, self._SQL, args_list)

        assert refusals == []
        assert len(fetch_sqls) == 3, (
            f"the PG cap preflight ran {len(fetch_sqls)} fetches (pre-extraction "
            "shape: 3): a fetch was added or lost by the kernel extraction"
        )
        assert "actor_config" in fetch_sqls[0], "the override snapshot must come first"
        assert "GROUP BY actor" in fetch_sqls[1], "the pending count comes second"
        assert "JOIN unnest" in fetch_sqls[2], "the stored-pair probe comes last (keyed items)"
        assert not any("advisory" in sql.lower() for sql in fetch_sqls), (
            "the bulk preflight acquired an advisory lock: the documented "
            "no-lock residual changed shape"
        )
        assert not any("advisory" in sql.lower() for sql in conn.execute_calls), (
            "the preflight executed a lock statement outside the fetches"
        )

    async def test_two_fetches_without_keyed_items(self) -> None:
        conn, fetch_sqls = self._recorded_conn([])
        args_list = [_args_for(_capped_ref, 0)]  # no idempotency key

        await _pg_batch_cap_refusals(conn, self._SQL, args_list)

        assert len(fetch_sqls) == 2
        assert not any("JOIN unnest" in sql for sql in fetch_sqls), (
            "the stored-pair probe ran for a batch with no keyed items"
        )

    async def test_kernel_inputs_are_the_fetched_rows_not_re_fetched(self) -> None:
        """The three fetched record sets are the kernel's ONLY store view:
        the preflight must not issue extra round trips for data the kernel
        consumes (the override rows here are non-NULL and flow straight
        through, an int() coercion or a re-read would show up as shape
        drift above, the pass-through itself is pinned by the sweep's
        cleared-NULL axis)."""
        conn, fetch_sqls = self._recorded_conn([_Record({"actor": _CAPPED, "max_pending": 1})])
        args_list = [_args_for(_capped_ref, 0, key="k1"), _args_for(_capped_ref, 1)]

        refusals = await _pg_batch_cap_refusals(conn, self._SQL, args_list)

        assert len(fetch_sqls) == 3
        assert _refusal_facts(refusals) == [(_CAPPED, 0, 1)], (
            "the stored override (1) did not govern the refusal: the "
            "fetched rows are not the kernel's cap source"
        )
