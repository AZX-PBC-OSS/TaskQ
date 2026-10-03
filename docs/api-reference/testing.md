# Testing

`taskq.testing` is the test-support namespace: deterministic fakes, pytest
fixtures, behavioral assertions, settings factories, and chaos tools. Every
symbol lives under `taskq.testing`, never in `taskq.backend`, so production
code never pulls in test doubles.

```python
from taskq.testing import FakeClock, InMemoryBackend
```

The [Testing guide](../guides/testing.md) walks these tools in context; this
page is the namespace index: every public name, the module that defines it,
and the rendered reference for each module.

## `FakeClock`: one class, four import paths

`FakeClock`'s canonical home is `taskq.clock` — a production module that
imports only `datetime`. That placement (issue #625) is what lets
`import taskq` re-export `FakeClock` without pulling in the test-double
backend under `taskq.testing`. `taskq.testing.clock` is a backcompat shim
re-exporting the same class object, and the `taskq` package and the
`taskq.testing` namespace re-export it in turn. All four paths resolve to
one class object (`is`-identical, not copies):

```python
from taskq import FakeClock
from taskq.clock import FakeClock as canonical
from taskq.testing import FakeClock as namespaced
from taskq.testing.clock import FakeClock as shimmed

assert FakeClock is canonical is namespaced is shimmed
```

Prefer `from taskq.clock import FakeClock` (or the shorter
`from taskq import FakeClock`) in new code; `taskq.testing.clock` exists for
backcompat and keeps working.

## Namespace surface

`taskq.testing.__all__` is the package contract: 38 names, each re-exported
from the module that defines it:

| Home module | Names |
|---|---|
| `taskq.testing.in_memory` | `InMemoryBackend` |
| `taskq.testing.clock` | `FakeClock` (shim; canonical home `taskq.clock`) |
| `taskq.testing.actor` | `EmptyPayload`, `FakeBackend`, `StubActorConfig`, `as_backend`, `default_actor_config` |
| `taskq.testing.assertions` | `assert_attempt`, `assert_has_event`, `assert_has_otel_event`, `assert_has_span`, `assert_job_status`, `assert_job_terminal`, `assert_transition_sequence`, `parse_detail`, `pg_now`, `wait_for`, `wait_for_job_status`, `wait_for_leader` |
| `taskq.testing.health` | `unique_health_sock_path` |
| `taskq.testing.job_context` | `JobContext` |
| `taskq.testing.jobs` | `error_info`, `make_enqueue_args`, `make_job_row` |
| `taskq.testing.pg` | `DEFAULT_ACTORS`, `JobTriple`, `create_pending_job`, `create_running_job`, `create_worker`, `create_workered_running_job`, `get_job_triple`, `reset_schema`, `seed_actors`, `setup_running_job`, `truncate_schema` |
| `taskq.testing.settings` | `make_integration_settings`, `make_integration_settings_dict` |
| `taskq.testing.spy` | `WarningSpy` |

All 38 render under the package directive below. Three more surfaces are
deliberately **not** in the package `__all__` and have their own sections:
the pytest fixtures (`taskq.testing.fixtures` — keeps `pytest`/`asyncpg` off
the package import path), the OTel test utilities (`taskq.testing.otel` —
requires the `[otel]` extra), and the asyncpg chaos tools
(`taskq.testing.asyncpg_chaos`).

## Package surface (fakes, assertions, settings factories)

::: taskq.testing

## FakeClock home: taskq.clock

::: taskq.clock

## Backcompat shim: taskq.testing.clock

::: taskq.testing.clock

## Pytest fixtures

The fixtures are not re-exported from `taskq.testing.__init__` (importing
`pytest`/`asyncpg` at the package top level is deliberately avoided), so they
render from their defining module. The module defines 17 fixtures (sync via
`pytest.fixture`, async via `pytest_asyncio.fixture`) plus the
`run_isolation_token` helper and `RUN_TOKEN_ENV_VAR` used to isolate
per-run/per-worker database and schema names:

::: taskq.testing.fixtures

## Health-socket helpers

::: taskq.testing.health

## asyncpg chaos tools

`ChaosConnection` wraps a real asyncpg connection and raises
`ChaosException` (or a `fail_with` exception type) just before the configured
call number — counting `execute` / `fetchrow` / `fetch` / `fetchval` in
execution order — while `transaction()` still delegates to the real
connection so asyncpg rolls the transaction back. `ChaosPool` wraps a pool
whose `acquire()` hands out chaos connections. Import from
`taskq.testing.asyncpg_chaos` directly:

::: taskq.testing.asyncpg_chaos

## OTel test utilities

These require the `[otel]` extra (`opentelemetry-sdk`) and are not
re-exported from the package — import them from `taskq.testing.otel`
directly. The module's public surface: `ListSpanExporter` (records finished
spans; query with its `span_named` / `spans_named` / `events_on` methods),
`setup_tracer`, `setup_meter`, `collect_metrics`, `counter_value`,
`counter_data_points`, `histogram_points`, `reset_otel_gauge_caches`,
`restore_otel_enabled`, `restore_logging_configured`, and `attribute_str`.
The module also exports three `autouse` pytest fixtures —
`_otel_enabled_guard`, `_otel_gauge_cache_guard`, and
`_logging_configured_guard` — that activate automatically for any test
importing from it:

::: taskq.testing.otel
    options:
      members:
        - ListSpanExporter
        - attribute_str
        - collect_metrics
        - counter_data_points
        - counter_value
        - histogram_points
        - reset_otel_gauge_caches
        - restore_logging_configured
        - restore_otel_enabled
        - setup_meter
        - setup_tracer
