"""Exception text must be scrubbed before the LogRecord reaches ANY handler.

``setup_logging`` installs its handler on the **root** logger, and
``worker_main`` calls it unconditionally. Azure Monitor's
``configure_azure_monitor()`` attaches its own ``LoggingHandler`` to that same
root logger, reads ``record.exc_info`` directly, and ships ``str(exc)`` plus
the full traceback to the App Insights ``exceptions`` table. So redaction that
lives only in TaskQ's own ``ProcessorFormatter`` protects only TaskQ's own
stream — every other root handler gets the record raw.

The tests below therefore assert on a **foreign** handler's view of the record,
the way a vendor SDK sees it, not on TaskQ's rendered line. Both channels are
checked in one pass: the foreign handler must see nothing, and TaskQ's own JSON
line must still carry the scrubbed, diagnostic exception fields.

Note the leak is not simply "``exc_info`` was left in the event dict":
``logging.Logger.exception()`` hard-codes ``exc_info=True`` on the record
regardless of what the event dict contains, so no structlog processor alone can
close it.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import asyncpg
import pytest
import structlog

from taskq.obs import get_logger, setup_logging

CANARY = "tenant-4417-SSN-078051120"
DIAGNOSTIC = "duplicate key value violates unique constraint"


def _unique_violation() -> asyncpg.exceptions.UniqueViolationError:
    exc = asyncpg.exceptions.UniqueViolationError(f'{DIAGNOSTIC} "jobs_idempotency_key_key"')
    exc.detail = f"Key (idempotency_key)=({CANARY}) already exists."
    return exc


class _ForeignHandler(logging.Handler):
    """Stands in for the ``LoggingHandler`` Azure Monitor attaches to root.

    Snapshots what it sees *inside* ``emit``. ``ProcessorFormatter`` mutates
    ``record.msg`` in place, so a handler that only stashed the record would
    be asserting on whatever TaskQ's own handler happened to leave behind —
    a pass that depends on root-handler ordering rather than on redaction.
    """

    def __init__(self) -> None:
        super().__init__()
        self.seen: list[tuple[object, str, str]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.seen.append((record.exc_info, record.getMessage(), logging.Formatter().format(record)))


def _emit_via_exception(log: structlog.stdlib.BoundLogger) -> None:
    log.exception("job-failed")


def _emit_via_exc_info_true(log: structlog.stdlib.BoundLogger) -> None:
    log.error("job-failed", exc_info=True)


def _emit_via_exc_info_object(log: structlog.stdlib.BoundLogger) -> None:
    log.error("job-failed", exc_info=_unique_violation())


@pytest.mark.parametrize(
    "emit",
    [_emit_via_exception, _emit_via_exc_info_true, _emit_via_exc_info_object],
    ids=["logger.exception", "exc_info=True", "exc_info=exc"],
)
def test_exception_text_is_scrubbed_before_any_handler_sees_it(
    emit: Callable[[structlog.stdlib.BoundLogger], None],
    capsys: pytest.CaptureFixture[str],
) -> None:
    setup_logging(level="INFO", log_format="json")
    foreign = _ForeignHandler()
    # First in line: the record as emitted, before TaskQ's own handler has
    # touched it. Azure's handler may be registered either side of ours.
    logging.root.handlers.insert(0, foreign)
    try:
        log = get_logger("taskq.test")
        try:
            raise _unique_violation()
        except asyncpg.exceptions.UniqueViolationError:
            emit(log)
    finally:
        logging.root.removeHandler(foreign)

    assert len(foreign.seen) == 1
    exc_info, message, rendered = foreign.seen[0]

    # What a vendor handler actually renders: the message plus, when
    # ``exc_info`` survives, the formatted traceback.
    assert CANARY not in rendered, f"row value reached a foreign handler:\n{rendered}"
    # ``record.exc_info`` is read directly by Azure Monitor's handler, so it is
    # not enough that the formatter happened not to render it.
    assert exc_info is None
    assert CANARY not in message

    # The redaction must not have cost the diagnostic on TaskQ's own channel.
    own_output = capsys.readouterr().err
    assert CANARY not in own_output
    assert DIAGNOSTIC in own_output
    assert "UniqueViolationError" in own_output
