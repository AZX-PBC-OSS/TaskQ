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

from taskq.backend._protocol import (
    Container,
    EnqueueArgs,
    Mapping,
    batch_cap_groups,
    batch_cap_refusal_kernel,
)
from taskq.client._args import build_enqueue_args
from taskq.exceptions import MaxPendingExceededError

from .test_batch_cap_refusals_parity import (
    _capped_ref,
    _healthy_ref,
    _memory_refusals,
    _Payload,
    _pg_refusals,
    _refusal_facts,
)

# The sweep's seed is part of the pin: the grid below is randomized to
# cover combinations no hand-written scenario list would think to pair,
# but it is fully determined by this seed, so a failure names a
# reproducible world, not a lottery.
_SWEEP_SEED = 165
_SWEEP_SCENARIOS = 64

_CAPPED = _capped_ref.name


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
            "duplicate_key_in_batch": False,
            "key_collides_with_stored_pair": False,
            "uncapped_actor_item_mixed_in": False,
        }
        for scenario in range(_SWEEP_SCENARIOS):
            # Stored operator override for the capped actor: absent, or
            # tighter/equal/looser than the carried literal (2).
            override = rng.choice([None, 0, 1, 2, 3, 10])
            override_caps = {_CAPPED: override} if override is not None else None
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
