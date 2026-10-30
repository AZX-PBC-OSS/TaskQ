"""THE ROUND-3 RED-TEAM CURES' PINS — the §H ROUND-3 subsection's
residues on the admin surface, each cured and pinned:

- THE WALK BUDGET'S UNIT: ``_bounded_io_projection``'s size budget
  counts the SERVED size (keys + structure + values — the
  json-serialized length's honest proxy), not the string VALUES alone
  (a key-heavy doc's keys rode free: the served text ran ~46k for a
  claimed 1,023, and the panel's own truncation arm fired on top of
  the walk's); the note's number IS that accounting.
- THE MARKER'S COLLISION: the dict walk's budget-out marker landed on
  a FIXED key — a doc whose own entry's key equals the marker string
  was OVERWRITTEN; the marker disambiguates with an ordinal suffix
  until unique (the masked-key collision law's own shape).
- THE LOGOUT CSRF'S REMEDY: the logout 403's detail names the same
  reload remedy the action doors' CSRF refusals carry (the 403 loop's
  other carrier: submit → 403 → submit → 403, forever).
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")

from taskq._json import dumps_str as _json_dumps_str

# ── the walk budget's served-size accounting (R3, the panel) ─────────────


def test_the_walk_budget_counts_the_served_size_key_heavy_doc() -> None:
    """THE KEY-HEAVY DOC: 1,500 keys with trivial values — the served
    size is carried by the KEYS and the JSON structure, so a values-only
    budget never fired and the whole doc served past the panel's own
    cap (the note's number a lie by an order of magnitude). The budget
    counts the SERVED size: the served text lands under the
    accounting's own number (an honest proxy over-approximates what it
    bounds), under the panel's display cap (the truncation arm stays a
    backstop, not the first bound), and the note's number IS the
    accounting."""
    from taskq.web.admin import _wf_actions

    doc = {f"key-{i:06d}": "v" for i in range(1_500)}
    projected, note, stats = _wf_actions._bounded_io_projection(doc)
    assert note is not None, "the key-heavy doc's trim is NAMED, never silent"
    served = _json_dumps_str(_wf_actions._redact_io_doc(projected))
    assert len(served) <= stats["chars"], (
        f"THE UNIT LIE: the served text ran {len(served)} characters for a "
        f"claimed {stats['chars']} — the budget must count the served size "
        "(keys, structure, and values), not the string values alone"
    )
    assert len(served) < 10_000, (
        f"the panel's own truncation arm fired on top of the walk's "
        f"({len(served)} served) — the budget no longer bounds the key-heavy "
        "shape first"
    )
    assert str(stats["chars"]) in note, (
        f"the note's number is the accounting's own: {note!r} vs {stats}"
    )
    assert "keys, structure" in note, f"the note states the served truth: {note!r}"


def test_the_prune_marker_never_overwrites_a_legit_entry() -> None:
    """THE MARKER'S COLLISION: a doc whose OWN entry's key equals the
    marker string keeps that entry — the dict walk's budget-out marker
    disambiguates with an ordinal suffix until unique (the masked-key
    collision law's own shape), never overwrites. Pre-fix red: the
    budget-out assignment replaced the legit entry with the marker's
    own text."""
    from taskq.web.admin import _wf_actions

    doc = {
        _wf_actions._WALK_PRUNED_MARKER: "legit-entry",
        **{f"k{i}": "x" for i in range(2_000)},
    }
    projected, note, _stats = _wf_actions._bounded_io_projection(doc)
    assert isinstance(projected, dict)
    assert note is not None, "the 2,000-entry doc's trim is named"
    values = list(projected.values())
    assert "legit-entry" in values, (
        f"THE OVERWRITE: the doc's own marker-keyed entry was replaced — "
        f"{projected[_wf_actions._WALK_PRUNED_MARKER]!r}"
    )
    assert projected[_wf_actions._WALK_PRUNED_MARKER] == "legit-entry"
    # The walk's marker still rides, under its disambiguated key.
    suffixed = [k for k in projected if str(k).startswith(_wf_actions._WALK_PRUNED_MARKER)]
    assert len(suffixed) == 2, suffixed
    assert _wf_actions._WALK_PRUNED_MARKER + "[2]" in suffixed, suffixed


# ── the logout CSRF's remedy (R3, the session door) ──────────────────────


def test_the_logout_csrf_403_names_the_reload_remedy() -> None:
    """THE 403 LOOP'S OTHER CARRIER: the logout door's token refusal
    names the SAME remedy the action doors' CSRF refusals carry — a
    stale page's only fix is a fresh one, so the 403 self-describes it
    instead of reading as a bug (submit → 403 → submit → 403, forever).
    Pre-fix red: the detail named the mismatch and no remedy."""
    pytest.importorskip("authlib")
    from fastapi import FastAPI
    from fastapi.responses import Response
    from fastapi.testclient import TestClient

    from taskq.web.admin.auth import OIDCAuthConfig, create_oidc_auth
    from taskq.web.admin.auth._session import (  # pyright: ignore[reportPrivateUsage]  # Why: the session machinery's test seam is not re-exported.
        IdentityClaims,
        SessionManager,
    )

    config = OIDCAuthConfig(
        issuer="https://issuer.example/v2.0",
        client_id="cid",
        client_secret="sec",
        redirect_uri="https://app.example/admin/callback",
        session_secret="s" * 32,
    )
    bundle = create_oidc_auth(config, base_path="/admin")
    app = FastAPI()
    app.include_router(bundle.router, prefix="/admin")

    manager = SessionManager(secret="s" * 32)
    response = Response()
    manager.set_session_cookie(
        response, IdentityClaims(subject="u1", email=None, groups=frozenset(), raw={})
    )
    set_cookie = next(
        line
        for line in response.raw_headers
        if line[0].lower() == b"set-cookie" and line[1].startswith(b"taskq_session=")
    )
    session_cookie = set_cookie[1].decode().split(";", 1)[0].split("=", 1)[1]

    client = TestClient(app)
    client.cookies.set("taskq_session", session_cookie)
    posted = client.post("/admin/logout", data={"csrf_token": "0" * 64}, follow_redirects=False)
    assert posted.status_code == 403, posted.status_code
    detail: Any = posted.json()["detail"]
    assert "reload the page and submit again" in detail, (
        f"THE LOOP: the 403 names no remedy — {detail!r}"
    )
