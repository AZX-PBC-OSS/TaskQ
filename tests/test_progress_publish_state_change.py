"""Unit pins for ``_publish_state_change_event``'s two guard paths.

The consumer calls this on every state transition (six call sites in
``worker/_consumer.py``), on the hot path of a running worker. Two
behaviors keep that call site safe that no other test reaches:

* without a Redis client (the default deployment — progress streaming is
  an optional extra), the publish is a no-op, never an AttributeError;
* a malformed buffer cannot break the transition that published it: the
  event failure is logged and counted and the function returns, so a
  poisoned progress buffer degrades the SSE feed, never the job.

These pin the exact function the consumer imports
(``taskq.progress._publish._publish_state_change_event``); the publish
surface's integration behavior lives in tests/web_progress/.
"""

from dataclasses import dataclass
from uuid import UUID

import structlog
import structlog.testing

from taskq.progress._publish import _publish_state_change_event

_JOB_ID = UUID("018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f")


@dataclass(frozen=True)
class _Settings:
    schema_name: str = "taskq"


async def test_state_change_publish_without_redis_is_a_noop() -> None:
    """No Redis client (the default deployment) must return before any
    event is built: the progress surface is an optional extra, and a
    core-mode worker transitions states constantly."""
    with structlog.testing.capture_logs() as logs:
        await _publish_state_change_event(
            None,
            _Settings(),  # type: ignore[arg-type]  # Why: the function reads settings.schema_name only after the redis guard
            _JOB_ID,
            "email_actor",
            None,
            status="running",
            terminal=False,
        )

    assert logs == [], (
        "a no-op publish must be silent: the core deployment has no redis "
        "and no progress consumers, and a warning per state transition "
        "would be log spam on every job"
    )


async def test_a_malformed_buffer_degrades_the_feed_not_the_transition() -> None:
    """A pending state whose field the event model rejects (here: a
    non-integer ``step``) must not raise out of the publish: the failure
    is logged with its type and counted, and the state transition that
    carried it proceeds."""

    with structlog.testing.capture_logs() as logs:
        await _publish_state_change_event(
            object(),  # any non-None client passes the guard; the event fails first
            _Settings(),  # type: ignore[arg-type]  # Why: the function returns before settings.schema_name is read
            _JOB_ID,
            "email_actor",
            None,
            status="running",
            terminal=False,
            _override_pending_state={"step": "not-a-number"},
            _override_seq=1,
        )

    failures = [e for e in logs if e["event"] == "progress-publish-failure"]
    assert len(failures) == 1, (
        f"the event failure must surface exactly one progress-publish-failure "
        f"warning, got {[e['event'] for e in logs]}"
    )
    assert failures[0]["error_type"] == "ValidationError"
    assert failures[0]["job_id"] == str(_JOB_ID)
