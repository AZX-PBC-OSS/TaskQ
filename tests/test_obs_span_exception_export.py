"""Exported spans must never carry unscrubbed exception text.

The sibling module ``test_obs_exception_redaction.py`` proves the *scrubbers*
work and guards the source with an AST check. Neither can see this bug: the
leak is emitted by the OpenTelemetry SDK, not by any ``record_exception`` call
in TaskQ's source. ``Tracer.start_as_current_span`` defaults
``record_exception=True`` and ``set_status_on_exception=True``, so every span
that re-raises after scrubbing got a *second*, raw ``exception`` event and had
its scrubbed status description overwritten with ``str(exc)``.

So this module asserts on what a real SDK exporter actually shipped, for the
three call-site shapes that exist in production:

* a call site that scrubs and re-raises (``dispatch_batch``),
* a call site that marks the span errored without recording an event
  (``enqueue_span``),
* a bare ``safe_start_span`` with no local exception handling at all (the
  ``attempt.N`` span in ``worker/_consumer.py``, which wraps user job code).

The canary is planted in a Postgres ``DETAIL`` line — a real
``UniqueViolationError`` whose ``str()`` quotes the offending
``idempotency_key`` value, which in TaskQ is always caller-supplied and
routinely a tenant or subject identifier.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.trace import StatusCode

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import dispatch_batch
from taskq.client._args import enqueue_span
from taskq.obs import safe_start_span
from taskq.testing.otel import setup_tracer

#: Shaped like the tenant identifiers TaskQ's idempotency/identity/fairness
#: keys actually carry.
CANARY = "tenant-4417-SSN-078051120"

#: The static part of the same message. A fix that simply drops all exception
#: text would pass a "canary absent" assertion while destroying the signal, so
#: every case asserts this survives.
DIAGNOSTIC = "duplicate key value violates unique constraint"


def _unique_violation() -> asyncpg.exceptions.UniqueViolationError:
    exc = asyncpg.exceptions.UniqueViolationError(
        f'{DIAGNOSTIC} "jobs_idempotency_key_key"',
    )
    exc.detail = f"Key (idempotency_key)=({CANARY}) already exists."
    return exc


class _FetchRaises:
    """Stands in for the asyncpg connection ``dispatch_batch`` runs the CTE on."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    async def fetch(self, *args: object, **kwargs: object) -> Sequence[Any]:
        raise self._exc


async def _drive_bare_span(exc: BaseException) -> None:
    """The ``attempt.N`` span shape: no local try/except around the body."""
    with safe_start_span("attempt.1"):
        raise exc


async def _drive_enqueue_span(exc: BaseException) -> None:
    """The enqueue PRODUCER span: sets ERROR status, records no event."""
    with enqueue_span("send_email", "default", identity_key="k"):
        raise exc


async def _drive_dispatch_span(exc: BaseException) -> None:
    """The dispatch span: scrubs, records a scrubbed event, re-raises."""
    await dispatch_batch(
        _FetchRaises(exc),  # type: ignore[arg-type]  # Why: ConnLike is structural; this stand-in supplies the only member dispatch_batch calls.
        sql="SELECT 1",
        queues=["default"],
        limit_n=1,
        worker_id=new_uuid(),
        lock_lease=timedelta(seconds=30),
    )


@pytest.mark.parametrize(
    ("driver", "span_name"),
    [
        (_drive_bare_span, "attempt.1"),
        (_drive_enqueue_span, "enqueue send_email"),
        (_drive_dispatch_span, "dispatch"),
    ],
    ids=["consumer-attempt-span", "enqueue-span", "dispatch-span"],
)
async def test_exported_span_never_carries_postgres_detail(
    driver: Callable[[BaseException], Awaitable[None]],
    span_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _provider, exporter = setup_tracer(monkeypatch)
    exc = _unique_violation()
    # Precondition: the raw text really does leak, or this test proves nothing.
    assert CANARY in str(exc)

    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await driver(exc)

    spans = exporter.spans_named(span_name)
    assert len(spans) == 1
    span: ReadableSpan = spans[0]

    # Whole exported span — status description, every event, every attribute.
    exported = span.to_json()
    assert CANARY not in exported, f"row value reached the exporter:\n{exported}"

    # Suppressing the SDK's automatic behaviour must not leave the span with
    # no error signal, and must not cost the diagnostic template either.
    assert span.status.status_code is StatusCode.ERROR
    assert DIAGNOSTIC in exported
