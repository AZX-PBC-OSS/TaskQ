"""Cross-PR pin: #429's audit failure log path passes through #413's scrub.

``record_admin_action_safe`` and ``fold_principal_into_cancel_event``
convert their failure to ``error=str(exc)`` and log it. The #413 scrub
pass (``_scrub_exception_fields``) masks exactly that field name
(``error`` is in ``EXCEPTION_MESSAGE_FIELDS``), so a driver error whose
message quotes credential-shaped material -- the same class of message
the bearer/JWT/AWS canaries in _redact_exc stand in for -- must reach a
vendor root handler masked, and stay diagnostic. This pin holds the
composition shut: a future refactor that renames the field or logs the
exception outside the chain fails here instead of reopening the surface
on a NEW log path (the exact hole #413 closed was per-path; new paths
appear under attack, not review).
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")
import structlog

from taskq.web.admin._audit import ANONYMOUS_SUBJECT, record_admin_action_safe

pytestmark = [pytest.mark.fastapi]

# The canaries the _redact_exc suite uses the same shapes for: a bearer
# header value, a JWT, and an AWS presigned-URL signature (the #413 AWS
# pass masks the SIGNATURE parameter -- the credential; an AccessKeyId is
# an identifier, deliberately out of scope, so the AWS canary carries a
# signature value, per the contract _AWS_SIG_RE documents).
_BEARER = "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdef1234567890"
_JWT = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJvcGVuc3ViamVjdCI6MTIzfQ.SflKxwRJSMeKKF2QT4fwpMeJ"
_AWS = (
    "https://bucket.s3.amazonaws.com/obj?X-Amz-Date=20260922T000000Z"
    "&X-Amz-Signature=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)


class _ExplodingAcquirePool:
    """Pool double whose checkout raises with credential-shaped text.

    The realistic sources are driver errors quoting request context (a
    connection URI with embedded credentials, a server message echoing a
    header); the crafted message stands in for the whole class, the same
    stand-in discipline the _redact_exc canary tests use."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def acquire(self, *, timeout: float | None = None) -> Any:
        return self

    async def __aenter__(self) -> Any:
        raise self._exc

    async def __aexit__(self, *args: object) -> None:
        return None


class _ForeignHandler(logging.Handler):
    """Snapshot the record as a vendor root handler sees it."""

    def __init__(self) -> None:
        super().__init__()
        self.rendered: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.rendered.append(logging.Formatter().format(record))


@pytest.fixture()
def _reset_structlog_and_logging() -> Generator[None, None, None]:
    """Same reset discipline as tests/test_obs_logging.py's autouse fixture."""
    structlog.reset_defaults()
    yield
    structlog.reset_defaults()
    for handler in list(logging.root.handlers):
        if isinstance(handler, logging.StreamHandler) and isinstance(
            handler.formatter, structlog.stdlib.ProcessorFormatter
        ):
            logging.root.removeHandler(handler)
    logging.root.setLevel(logging.WARNING)


@pytest.mark.parametrize(
    "material",
    [
        f"connection failed for postgresql://ops:secret@db:5432/taskq ({_BEARER})",
        f"token rejected: {_JWT}",
        f"presign failed: {_AWS}",
    ],
    ids=["bearer", "jwt", "aws"],
)
def test_audit_failure_log_carries_scrubbed_error_text(
    material: str,
    _reset_structlog_and_logging: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The audit record's failure log goes out through the #413 scrub
    pass: no credential-shaped material in the record a vendor handler
    renders, on TaskQ's own stream either; the error CLASS stays."""
    from taskq.obs import setup_logging

    setup_logging(level="INFO", log_format="json")
    foreign = _ForeignHandler()
    logging.root.handlers.insert(0, foreign)
    pool = _ExplodingAcquirePool(RuntimeError(material))
    try:
        await_call = record_admin_action_safe(
            pool,  # type: ignore[arg-type]
            schema="taskq",
            principal=ANONYMOUS_SUBJECT,
            action="job.retry",
            target_type="job",
            target_id="00000000-0000-0000-0000-00000000000a",
        )
        import asyncio

        asyncio.run(await_call)
    finally:
        logging.root.removeHandler(foreign)

    # The failure degraded loudly (one warning reached the handler), and
    # nothing credential-shaped survived the chain.
    assert len(foreign.rendered) == 1
    rendered = foreign.rendered[0]
    for raw in (material, _BEARER, _JWT, "X-Amz-Signature=0123456789abcdef"):
        assert raw not in rendered
    own_output = capsys.readouterr().err
    assert "Bearer eyJhbGciOiJIUzI1NiJ9" not in own_output
    assert "X-Amz-Signature=0123456789abcdef" not in own_output
    assert "admin-audit-record-failed" in own_output
