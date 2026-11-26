"""The leader's sweep table is gated on bare ``hasattr`` probes of the live
backend — so a marker the real backend stops carrying silently deletes a whole
sweep arm, with no log line, no metric, and no failing test.

``_sweep_loop`` skips a spec when ``any(not hasattr(ctx.backend, attr) for attr
in spec.gated_on)``. That gate is what keeps the maintenance sweeps off the
in-memory backend, and it is also the ONLY thing that registers the eighteen
workflow arms — including ``wf_hold_stamp_reconcile``, which since issue #748
is the sole repair for a job row that carries a dead ``metadata.hold`` stamp
(the fleet's dispatch fence never claims a stamped row).

A rename or removal of ``PostgresBackend.workflow_sweeps_capable`` therefore
drops all eighteen arms in silence. Every existing pin of this table declares
the marker FOR ITSELF (a double assigning ``workflow_sweeps_capable = True``),
so nothing in the suite ever reads the real class's marker. These pins read it.
"""

from __future__ import annotations

import inspect
import re

import pytest

from taskq.backend.postgres import PostgresBackend
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker import _leader_sweeps as leader_sweeps

_GATED_ON = re.compile(r"gated_on=\(([^)]*)\)")
_NAME = re.compile(r'"([A-Za-z_][A-Za-z0-9_]*)"')


def _gated_names() -> list[str]:
    """Every backend attribute name the tick's sweep table probes, sorted."""
    source = inspect.getsource(leader_sweeps._sweep_loop)
    groups = _GATED_ON.findall(source)
    assert groups, "the sweep table's gated_on= rows vanished from _sweep_loop"
    return sorted({name for group in groups for name in _NAME.findall(group)})


def test_the_tick_sweep_table_still_declares_capability_gates() -> None:
    """The floor the rest of this module rests on: the table is found, and the
    workflow capability marker is one of the names it gates on."""
    names = _gated_names()
    assert "workflow_sweeps_capable" in names, names


def test_the_real_backend_carries_every_marker_the_tick_sweep_table_probes() -> None:
    """A name the sweep table probes that ``PostgresBackend`` does not carry is
    an arm that never runs, on every deployment, forever — the ``hasattr`` gate
    ``continue``s before the arm logs anything."""
    missing = [name for name in _gated_names() if not hasattr(PostgresBackend, name)]
    assert missing == [], (
        f"the leader's sweep table gates on {missing}, which PostgresBackend no longer "
        "carries: every arm gated on them is skipped silently by the hasattr probe in "
        "_sweep_loop (no log, no metric, no failure)"
    )


def test_the_workflow_capability_marker_is_a_class_level_fact() -> None:
    """The marker must be readable off the CLASS: the gate probes the backend the
    leader was handed, and a per-instance attribute assigned somewhere in a boot
    path is not what ``hasattr(ctx.backend, ...)`` reads on a fresh backend."""
    assert hasattr(PostgresBackend, "workflow_sweeps_capable")
    assert PostgresBackend.workflow_sweeps_capable is True


def test_the_in_memory_backend_stays_out_of_the_gated_arms() -> None:
    """The gate's other half: the in-memory twin declares none of the probes, so
    the maintenance arms stay off it. Pinning both halves is what makes a rename
    on either side visible instead of silently moving the arm to the wrong
    backend."""
    carried = [name for name in _gated_names() if hasattr(InMemoryBackend, name)]
    assert carried == [], (
        f"InMemoryBackend now carries {carried}: the hasattr gate would run those "
        "maintenance arms against the in-memory twin"
    )


@pytest.mark.parametrize("marker", ["workflow_sweeps_capable"])
def test_the_workflow_marker_guards_a_real_arm_not_an_empty_set(marker: str) -> None:
    """A marker that gates nothing is a dead probe; the workflow marker must
    still be attached to real spec rows (the reconcile among them)."""
    source = inspect.getsource(leader_sweeps._sweep_loop)
    gated_rows = [group for group in _GATED_ON.findall(source) if f'"{marker}"' in group]
    assert len(gated_rows) >= 1, f"no spec row gates on {marker!r}"
