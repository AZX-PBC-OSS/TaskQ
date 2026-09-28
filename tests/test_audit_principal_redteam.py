"""Red-team pins for the #463 audit binding choke point, round 2 (attack
review of the fix): credential shapes the first pass's mask chain missed.

The first fix proved ``Authorization: Bearer <JWT>`` cannot reach the
ledger. This module attacks the shapes its chain still bound verbatim --
each one was EXECUTED against a real ``admin_audit`` row before being
pinned here (the row really did carry the secret on the unguarded chain):

* a raw Authorization header VALUE (``request.headers["authorization"]``,
  the most natural "return the credential" mistake): a bare scheme word
  with NO ``Authorization:`` key beside it -- ``Bearer <opaque>``. The
  header-keyed bearer mask requires the ``authorization`` frame and the
  JWT mask requires dot structure, so an opaque token between them
  shipped whole.
* the non-bearer scheme words a rendered header carries: ``Authorization:
  Token <opaque>`` (the GitHub scheme still in the wild) and
  ``Authorization: Basic <base64 user:password>`` -- both credential
  frames, both outside the ``bearer``-only scheme word.

Every shape here carries credential FRAMING (a scheme word or header
name), so masking stays unambiguous; the honestly-framed attribution
cases at the bottom pin the over-redaction boundary the mask must keep
(a subject that merely CONTAINS the English word "bearer"/"basic" with
prose after it binds verbatim).

The typed path (:class:`IdentityClaims` ``.subject``) is pinned as the
documented contract: it binds verbatim (identity is not over-redacted)
-- the docs' masking promise covers the UNTYPED string shapes.
"""

from typing import Any

import asyncpg
import pytest

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")

from taskq._ids import new_uuid
from taskq._json import loads as _json_loads
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin._audit import record_admin_action
from taskq.web.admin.auth import IdentityClaims

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

# A realistic OPAQUE access token: no dot structure, so the JWT mask can
# never see it -- the framing is the only detection there is.
_OPAQUE = "v9xK2mQ7wR4tY6uI1oP3aS5dF8gH0jL2zC4vB6nM"
_B64_CREDS = "ZGVwbG95LWJvdDpWc21aN2Yza1Exd1A5eFIy"  # deploy-bot:<secret>
_JWT = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6ImFkbWluIn0."
    "dBjftJeZ4CVPmB92K27uhbUJU1p1r_wW1gFWFOEjXk"
)

# (label, principal, secret material that must not appear in the row)
_BYPASS_SHAPES: list[tuple[str, Any, tuple[str, ...]]] = [
    # The raw header VALUE (no header name around it), opaque token:
    # shipped verbatim through the unguarded chain.
    ("raw header value, bearer scheme, opaque token", f"Bearer {_OPAQUE}", (_OPAQUE,)),
    ("raw header value, bearer scheme, JWT", f"Bearer {_JWT}", (_JWT,)),
    ("raw header value, basic scheme", f"Basic {_B64_CREDS}", (_B64_CREDS,)),
    ("raw header value, token scheme", f"Token {_OPAQUE}", (_OPAQUE,)),
    ("rendered header, token scheme", f"Authorization: Token {_OPAQUE}", (_OPAQUE,)),
    ("rendered header, basic scheme", f"Authorization: Basic {_B64_CREDS}", (_B64_CREDS,)),
    ("quoted header, token scheme", f'"Authorization": "Token {_OPAQUE}"', (_OPAQUE,)),
    # A dict/str()-rendered principal carries the same frames.
    ("dict principal, token scheme", {"authorization": f"Token {_OPAQUE}"}, (_OPAQUE,)),
]

# Honest attribution the mask must NOT touch: the whole point of
# redact-in-place is that these bind verbatim.
_HONEST_SUBJECTS: list[str] = [
    "ops-admin@example.com",
    "deployment-operator",
    "svc-taskq-scheduler",
    "github.com/alice",
    "basic authentication reviewer",  # prose "basic" + 14-char word: under the token floor
    "bearer bond custodian",  # prose "bearer" + short word: not token-shaped
    "token revocation service",  # prose "token": the bare word is not masked
]


def _marker(subject: str, secrets: tuple[str, ...]) -> str | None:
    for s in secrets:
        if s in subject:
            return s
    return None


async def _record_and_fetch(
    conn: asyncpg.Connection,
    schema: str,
    *,
    principal: Any,
    target_id: str,
) -> str:
    """The full binding path the routes run: record_admin_action -> bind,
    then read the row back from the real table."""
    await record_admin_action(
        conn,
        schema=schema,
        principal=principal,
        action="job.cancel",
        target_type="job",
        target_id=target_id,
        reason="red-team probe",
    )
    subject: str | None = await conn.fetchval(
        f'SELECT principal_subject FROM "{schema}".admin_audit WHERE target_id = $1',  # Why: schema is a test-fixture identifier, validated by the fixtures upstream.
        target_id,
    )
    assert subject is not None, "the audit row did not land"
    return subject


# ── Bypass shapes: framed credentials must not reach the row ─────────────


@pytest.mark.parametrize(("label", "principal", "secrets"), _BYPASS_SHAPES)
async def test_framed_credential_shapes_never_reach_the_audit_row(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    label: str,
    principal: Any,
    secrets: tuple[str, ...],
) -> None:
    schema = module_pg_schema.schema_name
    subject = await _record_and_fetch(
        clean_pg_conn, schema, principal=principal, target_id=str(new_uuid())
    )
    leaked = _marker(subject, secrets)
    assert leaked is None, f"{label}: raw secret material {leaked!r} reached the audit row"
    assert "***" in subject, (
        f"{label}: the frame was dropped silently instead of redacted: {subject!r}"
    )


async def test_raw_bearer_header_value_folds_redacted_into_the_cancel_event(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: Any,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The fold path (:func:`fold_principal_into_cancel_event`) runs the
    same :func:`principal_subject` choke point: the raw header value
    (``Bearer <opaque>``) must fold in redacted, not verbatim."""
    from taskq.web.admin._audit import fold_principal_into_cancel_event

    schema = module_pg_schema.schema_name
    jid = new_uuid()
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".jobs '  # Why: schema is a test-fixture identifier, validated by the fixtures upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind) "
        f"VALUES ($1, 'test_actor', 'default', '{{}}'::jsonb, "
        f"'cancelled'::\"{schema}\".job_status, 3, 'transient')",
        jid,
    )
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".job_events (job_id, kind, detail) '
        f"VALUES ($1, 'cancel_request', '{{}}'::jsonb)",
        jid,
    )

    await fold_principal_into_cancel_event(
        module_pg_pool, schema=schema, job_id=jid, principal=f"Bearer {_OPAQUE}"
    )

    detail: str | None = await clean_pg_conn.fetchval(
        f'SELECT detail FROM "{schema}".job_events '
        "WHERE job_id = $1 AND kind = 'cancel_request' "
        "ORDER BY id DESC LIMIT 1",
        jid,
    )
    assert detail is not None, "the cancel_request event did not land"
    detail_obj = _json_loads(detail)
    folded = str(detail_obj)
    assert _OPAQUE not in folded, "the raw token folded into the event detail"
    assert detail_obj["principal_subject"].startswith("Bearer "), (
        f"the non-secret scheme frame must survive the fold: {detail_obj!r}"
    )


# ── Honest attribution: the mask must not over-redact ────────────────────


@pytest.mark.parametrize("subject_in", _HONEST_SUBJECTS)
async def test_honest_attribution_binds_verbatim(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    subject_in: str,
) -> None:
    schema = module_pg_schema.schema_name
    subject = await _record_and_fetch(
        clean_pg_conn, schema, principal=subject_in, target_id=str(new_uuid())
    )
    assert subject == subject_in, (
        f"the mask over-redacted honest attribution: {subject_in!r} -> {subject!r}"
    )


# ── The typed path is the documented contract ────────────────────────────


async def test_typed_subject_binds_verbatim_by_contract(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A typed claims principal binds its ``subject`` field verbatim: the
    subject field IS the identity, and over-redacting identity is its own
    falsification. Pinned as a DECISION (see the _bound_untyped_subject
    docstring): a change here must be a deliberate contract change, not
    drift."""
    claims = IdentityClaims(subject="ops-admin@example.com", email=None, groups=frozenset(), raw={})
    schema = module_pg_schema.schema_name
    subject = await _record_and_fetch(
        clean_pg_conn, schema, principal=claims, target_id=str(new_uuid())
    )
    assert subject == "ops-admin@example.com"
