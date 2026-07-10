"""Test-only helpers for TaskQ: FakeClock, InMemoryBackend, and stub utilities.

Every symbol here lives in ``taskq.testing`` — never in ``taskq.backend`` — so
production code does not pull in test-only helpers.

``run_until_drained``, ``tick_cancel_polling``, and ``register_cancel_event``
are methods on the re-exported :class:`InMemoryBackend` class.  The runner
logic lives in :mod:`taskq.testing._runner`.

Pytest fixtures are NOT re-exported here — they live in
:mod:`taskq.testing.fixtures` and are imported by
:mod:`tests.conftest` directly.  This avoids importing ``pytest`` /
``asyncpg`` at the ``taskq.testing`` top level.

``JobContext`` is re-exported for convenience; the eventual public
``JobContext`` will replace this test-scoped version.

OTel test utilities (``ListSpanExporter``, ``setup_tracer``, ``setup_meter``)
are NOT re-exported here — import them from ``taskq.testing.otel`` directly.
They require the ``[otel]`` extra (``opentelemetry-sdk``).
"""

from taskq.testing.actor import (
    EmptyPayload,
    FakeBackend,
    StubActorConfig,
    as_backend,
    default_actor_config,
)
from taskq.testing.assertions import (
    assert_attempt,
    assert_has_event,
    assert_has_otel_event,
    assert_has_span,
    assert_job_status,
    assert_job_terminal,
    assert_transition_sequence,
    parse_detail,
    pg_now,
    wait_for,
    wait_for_job_status,
    wait_for_leader,
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.job_context import JobContext
from taskq.testing.jobs import error_info, make_enqueue_args, make_job_row
from taskq.testing.pg import (
    DEFAULT_ACTORS,
    JobTriple,
    create_pending_job,
    create_running_job,
    create_worker,
    create_workered_running_job,
    get_job_triple,
    reset_schema,
    seed_actors,
    setup_running_job,
    truncate_schema,
)
from taskq.testing.settings import make_integration_settings, make_integration_settings_dict
from taskq.testing.spy import WarningSpy

__all__ = [
    "DEFAULT_ACTORS",
    "EmptyPayload",
    "FakeBackend",
    "FakeClock",
    "InMemoryBackend",
    "JobContext",
    "JobTriple",
    "StubActorConfig",
    "WarningSpy",
    "as_backend",
    "assert_attempt",
    "assert_has_event",
    "assert_has_otel_event",
    "assert_has_span",
    "assert_job_status",
    "assert_job_terminal",
    "assert_transition_sequence",
    "create_pending_job",
    "create_running_job",
    "create_worker",
    "create_workered_running_job",
    "default_actor_config",
    "error_info",
    "get_job_triple",
    "make_enqueue_args",
    "make_integration_settings",
    "make_integration_settings_dict",
    "make_job_row",
    "parse_detail",
    "pg_now",
    "reset_schema",
    "seed_actors",
    "setup_running_job",
    "truncate_schema",
    "wait_for",
    "wait_for_job_status",
    "wait_for_leader",
]
