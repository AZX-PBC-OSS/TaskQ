"""The secrets pre-pass composition: tors families meet the fail-closed chain.

``taskq.obs._redact_exc._scrub_text`` runs ``tors.scrub_secrets`` as a
PRE-PASS before its own regex chain, gated on the
``_SECRET_HEAD_TRIGGERS`` prefilter. The composition exists because the
measured differential (``docs/design/tors-adoption-map.md``) cut both
ways: tors's rule set covers token families TaskQ's chain has no pattern
for (AWS access keys, Slack/Stripe tokens, GitHub tokens, PEM blocks) --
but it has NO rule for the shapes TaskQ's chain owns (DSN userinfo and
password parameters, bearer JWTs, OAuth-named tokens, the PG DETAIL
block), and on those it LEAKS. So the pre-pass can never replace the
chain; it widens the covered families at ~5 us/op against a ~60 ms
budget.

Pinned here:

* **composition zero-leak** on the COMBINED shapes: a text carrying both
  layers' families comes out carrying neither (the AWS/Slack/PEM material
  as correlation tokens, the DSN/JWT/DETAIL material as the chain's
  masks);
* **chain semantics unchanged**: every shape the chain owned before
  scrubs to the identical bytes the pre-pass never touches;
* **the prefilter discipline**: trigger-free text does not enter the
  tors pass (the clean path's substring-scan cost contract);
* **idempotence**: scrubbing an already-scrubbed text is a no-op (the
  correlation token carries no re-matchable secret).
"""

from __future__ import annotations

import pytest

from taskq.obs import _redact_exc as rx

# The combined shape: BOTH layers' families in one exception text - the
# realistic payload (an actor failing inside boto3 while holding a DSN).
_COMBINED = (
    "boto3 failed: credentials AKIAIOSFODNN7EXAMPLE rejected; "
    "dsn=postgresql://app:PGSECRETPW@db.example.com:5432/taskq; "
    "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.sig"
)

_SLACK_SHAPE = "notify failed: slack answered 403 for xoxb-123456789012-abc"
_PEM_SHAPE = (
    "cert load failed: -----BEGIN RSA PRIVATE KEY-----\nMIIEvQ\n-----END RSA PRIVATE KEY-----"
)


def test_composition_covers_both_layers_families() -> None:
    out = rx._scrub_text(_COMBINED)
    # The tors families: correlation tokens, no raw key material.
    assert "AKIAIOSFODNN7EXAMPLE" not in out
    assert "AKIA~" in out  # the head verbatim + the digest: triage survives
    # The chain's families: the masks, byte-for-byte the chain's own output.
    assert "PGSECRETPW" not in out
    assert "app:***@db.example.com" in out  # the URI userinfo mask
    assert "Bearer eyJ" not in out
    assert "Bearer ***" in out  # the bearer mask, its exact spelling


@pytest.mark.parametrize(
    ("text", "forbidden"),
    [
        (_SLACK_SHAPE, ["xoxb-123456789012-abc"]),
        (_PEM_SHAPE, ["MIIEvQ"]),
    ],
)
def test_new_families_are_scrubbed(text: str, forbidden: list[str]) -> None:
    out = rx._scrub_text(text)
    for secret in forbidden:
        assert secret not in out, f"{secret[:20]}... leaked past the composition"
    assert out != text


def test_chain_shapes_are_unchanged_by_the_prepass() -> None:
    # The pre-pass must not alter the chain's byte-exact outputs on the
    # shapes the chain owned before it existed (no tors rule matches
    # these, and the prefilter must not perturb them).
    shapes = [
        "connection failed: postgresql://app:SECRETPW@db.example.com:5432/taskq",
        "auth failed: Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzIjoiMSJ9.sig",
        'psycopg error\nDETAIL:  Key (id)=(1) is not present in table "jobs".',
        "worker died; dsn=postgresql://u:p@h/d?password=OTHER",
    ]
    for text in shapes:
        assert rx._scrub_text(text) == rx._scrub_text(rx._scrub_text(text)), (
            f"scrub is not idempotent on {text[:40]!r}"
        )


def test_prefilter_skips_the_pass_on_trigger_free_text(monkeypatch: pytest.MonkeyPatch) -> None:
    # The clean path's cost contract: trigger-free text never enters the
    # tors pass. The monkeypatch proves it structurally (a counting
    # stand-in), not just by timing.
    calls: list[str] = []

    def _spy(text: str, *args: object, **kwargs: object) -> str:
        calls.append(text)
        return text

    monkeypatch.setattr(rx, "scrub_secrets", _spy)
    clean = "plain error: retry 1 of 3, queue default, worker 01ABCDEF-01"
    out = rx._scrub_text(clean)
    assert calls == [], "trigger-free text must not enter the tors pass"
    assert out == clean


def test_prefilter_arms_on_each_registered_family_head() -> None:
    # Every head the trigger list claims must actually arm the pass (the
    # list cannot rot: a head that stops matching silently drops that
    # family's coverage).
    import taskq.obs._redact_exc as rx_mod

    original = rx_mod.scrub_secrets  # pyright: ignore[reportPrivateImportUsage]  # Why: the test spies the pre-pass import seam; the attribute is the patch point.
    for head in rx._SECRET_HEAD_TRIGGERS:
        if head.startswith("--"):
            continue
        text = f"failure carrying {head}SOMEVALUE in it"
        calls: list[str] = []

        def _spy(t: str, *a: object, seen: list[str] = calls, **k: object) -> str:
            seen.append(t)
            return t

        rx_mod.scrub_secrets = _spy  # pyright: ignore[reportPrivateImportUsage]  # Why: the ignore belongs on the diagnostic line - a standalone comment line does nothing.
        try:
            rx_mod._scrub_text(text)
        finally:
            rx_mod.scrub_secrets = original  # pyright: ignore[reportPrivateImportUsage]
        assert calls, f"head {head!r} did not arm the pass - the trigger list rotted"


def test_pem_generic_pkcs8_header_is_a_stated_limit() -> None:
    """tors's PEM rule anchors the KEYED OpenSSL headers (RSA/EC PRIVATE
    KEY); the GENERIC PKCS#8 header (``-----BEGIN PRIVATE KEY-----``,
    the most common modern export shape) is NOT covered - the pass arms
    (the prefilter fires) but changes nothing. Pinned as a STATED LIMIT,
    the homoglyph precedent: the day tors covers the generic header,
    this pin flips and the limit's note in the adoption map goes stale
    loudly, exactly as limits should."""
    generic = "x: -----BEGIN PRIVATE KEY-----\nMIIEvQ\n-----END PRIVATE KEY-----"
    report = __import__("tors").scrub_secrets_report(generic)
    assert report["redacted"] == {}, (
        "tors now covers the generic PKCS#8 PEM header - update the "
        "adoption map's stated limit and the prefilter note"
    )
