"""Field-parity guard: the testing JobContext mirror carries every
actor-facing field of the production JobContext.

The class this file guards: an actor-facing field lands on the production
:class:`taskq.context.JobContext` and its test mirrors drift - proven
already: ``snooze_count`` reached all three production
construction sites but not the in-memory runner's ``_StubContext`` or this
mirror, so an actor written to the documented contract failed only under
the test backend. ``taskq/testing/job_context.py`` states the invariant in
its own docstring ("Field shape mirrors ``taskq.context.JobContext`` … and
adds ``deps``"); this test makes it executable, so the next field fails
here on arrival rather than being found by whoever's actor breaks.

Precedent: ``tests/test_in_memory_seam_registry.py`` - behavioural tests
pin each known member, a guard over the walked surface catches the next
one. The behavioural half for the deferral-cycle contract already exists
(``tests/test_stub_context_snooze_count.py`` for the runner,
``tests/test_actor_runner.py::test_actor_runner_snooze_count_parameter_reaches_the_context``
for the fixture); this file guards the *field set*.

The invariant is scoped to *public* fields: the production class's
underscore-prefixed fields are worker wiring the mirror deliberately
replaces (``_abort_requested`` surfaces as the public ``abort_requested``),
and the mirror's documented extras (``deps``, ``abort_requested``) are its
own. ``_StubContext`` is not walked here - it is a declared minimal subset
("the fields they read"), and its contract members are behaviour-pinned.
"""

from dataclasses import fields

from taskq.context import JobContext as ProductionJobContext
from taskq.testing.job_context import JobContext as TestingJobContext

#: The mirror's documented additions beyond the production field shape.
#: ``deps`` and ``abort_requested`` are the mirror's original extras;
#: ``progress_reports`` is the harness half of the documented progress
#: contract (the mirrored method surface): the fixture path has no
#: Redis/Postgres wiring, so reports are recorded on the context for
#: the test to inspect rather than published. ``cancel_origin`` is the
#: documented cancellation contract (``ctx.cancel_origin`` tells a
#: deploy apart from an operator cancel), mirrored for the harness.
_MIRROR_EXTRAS = {"deps", "abort_requested", "progress_reports", "cancel_origin"}


def _public_field_names(cls: type) -> set[str]:
    return {f.name for f in fields(cls) if not f.name.startswith("_")}


def test_testing_job_context_carries_every_actor_facing_field() -> None:
    """Every public field of the production JobContext exists on the
    testing mirror - a field an actor can read under the PG worker must be
    readable under the test harness, or the harness certifies actors that
    break in production (and vice versa)."""
    production = _public_field_names(ProductionJobContext)
    mirror = _public_field_names(TestingJobContext)

    missing = production - mirror
    assert not missing, (
        "testing JobContext is missing actor-facing field(s) "
        f"{sorted(missing)} that production JobContext carries - the "
        "mirror-drift class: add the field to the mirror (and to "
        "_StubContext / the actor_runner fixture if actors read it), or "
        "narrow the mirror docstring's parity claim if the omission is "
        "deliberate"
    )


def test_testing_job_context_extras_are_the_documented_ones() -> None:
    """The mirror's additions beyond production are exactly the documented
    ``deps`` (fixture-injected collaborators) and ``abort_requested``
    (the public spelling of production's private ``_abort_requested``) -
    a new extra is a deliberate act, not drift."""
    extras = _public_field_names(TestingJobContext) - _public_field_names(ProductionJobContext)
    assert extras == _MIRROR_EXTRAS, (
        f"testing JobContext grew undocumented extra field(s) {sorted(extras - _MIRROR_EXTRAS)}; "
        "if deliberate, document the addition in the mirror's docstring and register it here"
    )
