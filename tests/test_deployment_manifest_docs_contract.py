"""The shipped Kubernetes manifest must not break old pods mid-rollout.

A Deployment's init container runs on every new pod DURING a rolling
update, while old pods still serve. A bare ``taskq migrate up`` applies
pre AND post phases together, and post-phase migrations remove structures
the old release still needs — reproduced live: after
``01.00.03_01:post`` drops the old single-column idempotency index, every
enqueue from a not-yet-upgraded pod fails with
``InvalidColumnReferenceError`` (SQLSTATE 42P10). TaskQ's phase-split
tooling exists for exactly this discipline; the example adopters copy
must use it: ``--phase pre`` in the manifest, and the ``--phase post``
close-out documented as a human-gated step after the rollout confirms.

These tests pin that framing in docs/guides/deployment.md so the manifest
cannot drift back to the all-phases one-liner.
"""

from pathlib import Path

_DOCS = Path(__file__).resolve().parent.parent / "docs"


def _read(*parts: str) -> str:
    return (_DOCS.joinpath(*parts)).read_text()


def _k8s_section(text: str) -> str:
    """The ``## Kubernetes Deployment`` section of deployment.md."""
    section = text.split("## Kubernetes Deployment", 1)
    assert len(section) == 2, "deployment.md must keep its Kubernetes Deployment section"
    return section[1].split("\n## ", 1)[0]


def test_manifest_init_container_applies_only_the_pre_phase() -> None:
    """The initContainer's migrate command must carry ``--phase pre`` — and
    no bare all-phases ``migrate up`` may survive in the manifest."""
    k8s = _k8s_section(_read("guides", "deployment.md"))
    assert 'command: ["taskq", "migrate", "up", "--phase", "pre"]' in k8s, (
        "an init container fires per pod, mid-rollout; only the pre phase is "
        "safe against the old pods still serving — the manifest must not "
        "apply post"
    )
    assert 'command: ["taskq", "migrate", "up"]' not in k8s, (
        "a bare `migrate up` applies pre AND post together; in a rolling "
        "update the post phase breaks old pods' enqueue path"
    )


def test_post_phase_is_documented_as_a_human_gated_step() -> None:
    """The `--phase post` close-out must be documented as deliberate and
    manual — after the rollout confirms — never as an automated per-pod
    step."""
    text = _read("guides", "deployment.md")
    assert "--phase post" in text, (
        "the pre/post split leaves post pending by design; the guide must "
        "show operators how the split closes out"
    )
    k8s = _k8s_section(text)
    post_step = k8s.split("--phase post", 1)[0]
    assert "human" in post_step or "by hand" in post_step or "manual" in post_step, (
        "the post-phase step must be gated on a human confirming the "
        "rollout — an automated step fires unsequenced, mid-rollout, which "
        "is the hazard the phase split exists to remove"
    )


def test_deployment_guide_states_workers_refuse_boot_on_pending_pre_phase() -> None:
    """A worker against an unmigrated schema refuses to boot (it does not
    'fail later, at its first query') — the guide must match the boot
    guard, as upgrading.md already does."""
    text = _read("guides", "deployment.md")
    assert "fails later, at its first query" not in text, (
        "stale: the boot path refuses to start when a pre-phase migration "
        "is pending; there is no schema check deferred to first query"
    )
    assert "refuses to boot" in text, (
        "the deployment guide must state the worker's boot refusal for a "
        "schema with a pending pre-phase migration"
    )
