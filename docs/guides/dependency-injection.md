# Dependency Injection

TaskQ has a built-in async DI engine. Actors declare dependencies as type-annotated
parameters; the engine resolves them at dispatch time from a `ProviderRegistry`. The DI
graph is validated at worker startup — cycles, missing providers, and scope violations are
caught before any job runs.

---

## Scopes

Import from `taskq.di`:

```python
from taskq.di import Scope
```

| Scope | Value | Lifetime | Typical use |
|---|---|---|---|
| `Scope.PROCESS` | `0` | Worker process start to exit | Config, shared read-only singletons |
| `Scope.THREAD` | `1` | Thread spawn to thread close | Reserved — see below |
| `Scope.LOOP` | `2` | Event loop start to loop close | asyncpg pools, HTTP clients, Redis clients |
| `Scope.TRANSIENT` | `3` | Per actor invocation | Per-request context, one-shot helpers |

**`Scope.THREAD`** is reserved for a planned multi-thread worker deployment mode. It sits
between `PROCESS` and `LOOP` in the scope hierarchy. It is not used in the current
single-event-loop worker. Do not register providers at `THREAD` scope in application code.

**Scope rule:** a provider may only depend on providers of **the same or a wider (lower-value)
scope**, never on a narrower (higher-value) one. The dependency direction table:

| Provider scope | May depend on |
|---|---|
| `PROCESS` | `PROCESS` only |
| `THREAD` | `PROCESS`, `THREAD` |
| `LOOP` | `PROCESS`, `THREAD`, `LOOP` |
| `TRANSIENT` | any scope |

Violations raise `ScopeViolation` at `registry.validate()` time, not at runtime. Circular
dependencies raise `DependencyCycle` at `validate()` time.

---

## Registering providers

Create a `ProviderRegistry`, register your providers, then pass it to the
worker. The worker validates it internally as part of its bootstrap sequence
(see below).

```python
from taskq.di import ProviderRegistry, Scope

registry = ProviderRegistry()

# Register a pre-built value (no teardown needed)
registry.register_value(MyConfig, Scope.PROCESS, MyConfig(debug=False))

# Register an async generator factory (yield = teardown boundary)
async def make_db_pool():
    pool = await asyncpg.create_pool(dsn)
    try:
        yield pool
    finally:
        await pool.close()

registry.register_factory(asyncpg.Pool, Scope.LOOP, make_db_pool)

# Register a class with automatic lifecycle detection
registry.register_class(MyService, Scope.LOOP)

# Pass to the worker — do NOT call validate() yourself.
# The worker calls validate() after auto-registering WorkerSettings, Clock,
# and the asyncpg pool, so pre-validating would fail on those providers.
```

Pass the registry to `worker_main(di_registry=registry)`. The worker
auto-registers `WorkerSettings` and `Clock` at `Scope.PROCESS` and the
`asyncpg.Pool` at `Scope.LOOP` if they are not already present, then calls
`registry.validate(actors=..., rate_limit_registry=...)` to seal the registry.

!!! warning "Do not pre-validate"
    Calling `registry.validate(actors=[...])` before passing the registry to
    the worker raises `MissingProvider` for worker-injected providers
    (`WorkerSettings`, `Clock`, `asyncpg.Pool`) if any actor declares them as
    dependencies. The worker registers these automatically and then validates
    — let it own the validate call. After `validate()` the registry is sealed;
    further registrations raise `RuntimeError`.

### `register_value`

```python
registry.register_value(T, Scope.PROCESS, instance)
```

Registers a pre-built singleton. No teardown is run at scope close. Use for config objects,
read-only shared state, or any value that does not require cleanup.

### `register_factory`

```python
registry.register_factory(T, Scope.LOOP, factory)
```

Registers an async or sync factory. The factory is called once per scope lifetime. Factory
shape is detected automatically:

| Factory shape | Detection | Teardown |
|---|---|---|
| `async def f() -> T` | plain coroutine | none — value dropped at scope close |
| `async def f() -> AsyncIterator[T]` | async generator | resumes after `yield` at scope close |
| `def f() -> Iterator[T]` | sync generator | resumes after `yield` (wrapped in thread) |

The async-generator pattern is the most common — put teardown code after the `yield`:

```python
async def make_http_client():
    async with httpx.AsyncClient() as client:
        yield client
# client.aclose() is called when the LOOP scope closes
```

A plain `async def` factory returns its value and never runs teardown. Use it for types with
no cleanup requirements.

### `register_class`

```python
registry.register_class(MyService, Scope.LOOP)
```

Registers a class; the DI engine instantiates it and detects lifecycle methods
automatically. Priority order:

| Priority | Shape | Detection | Teardown |
|---|---|---|---|
| 1 | `AsyncContextManager` | has `__aenter__` and `__aexit__` | `await obj.__aexit__(None, None, None)` |
| 2 | `AsyncCloseable` | has `aclose` | `await obj.aclose()` |
| 3 | `SyncCloseable` | has `close` | `obj.close()` (wrapped in thread) |
| 4 | `Plain` | none of the above | no teardown |

```python
# Shape 1 — AsyncContextManager
class RedisClient:
    async def __aenter__(self) -> "RedisClient":
        await self._connect()
        return self
    async def __aexit__(self, *exc) -> None:
        await self._disconnect()

registry.register_class(RedisClient, Scope.LOOP)

# Shape 2 — AsyncCloseable (e.g. asyncpg Pool has aclose)
registry.register_class(MyAsyncResource, Scope.LOOP)

# Shape 4 — Plain (no teardown)
class ReadOnlyConfig:
    def __init__(self) -> None:
        self.value = os.environ["MY_VAR"]

registry.register_class(ReadOnlyConfig, Scope.PROCESS)
```

Teardown runs in **LIFO order** within each scope. Providers registered later are torn down
first.

---

## Declaring dependencies in an actor

Actors declare DI dependencies as keyword-only parameters. The worker resolves them from a
`ProviderRegistry` at dispatch time.

```python
from typing import Annotated
from taskq import actor
from taskq.context import JobContext
from taskq.di import Scope

@actor(name="send_email", queue="email")
async def send_email(
    payload: SendEmailPayload,
    ctx: JobContext[SendEmailPayload],
    *,
    mailer: MailerClient,                          # scope from registry default
    db: Annotated[asyncpg.Pool, Scope.LOOP],       # explicit scope assertion
) -> SendEmailResult:
    record = await db.fetch_one("SELECT * FROM users WHERE id = $1", payload.recipient_id)
    await mailer.send(to=record.email, subject=payload.subject)
    return SendEmailResult(message_id="ok")
```

`payload` and `ctx` are always supplied by the worker and must not be registered as
providers. All other annotated keyword parameters are resolved from the registry. Missing
providers raise `MissingProvider` at `validate()` time, not at runtime.

`Annotated[T, Scope.X]` asserts the expected scope. When the declared scope matches the
registered default it is redundant (and emits a `LifecycleDetectionWarning`). Use it only
when you intentionally want a narrower scope than the registered default.

---

## Thread safety for sync actors

DI resolution happens in the event loop **before** the sync function is dispatched to the
thread. Resolved kwargs are passed through:

| Scope | Safe in sync actor? | Notes |
|-------|---------------------|-------|
| `PROCESS` | Depends on object | Shared across event loop and thread |
| `LOOP` | **NOT safe** | `asyncpg.Connection`, `redis.asyncio.Redis` are not thread-safe |
| `TRANSIENT` | Safe if object is thread-safe | Fresh per invocation |

The worker logs a warning at startup validation when a sync actor declares a LOOP-scoped
dependency parameter. For thread-safe database access, register a sync driver connection
at `Scope.THREAD` or use `Scope.TRANSIENT`.

---

## Validation

`registry.validate(actors=[...])` runs five checks before sealing:

1. **Missing providers** — every DI parameter type on every actor must have a registered
   provider. Raises `MissingProvider`.
2. **Scope violations** — a provider may not depend on a narrower scope. Raises
   `ScopeViolation`.
3. **Dependency cycles** — the provider graph must be acyclic. Raises `DependencyCycle`.
4. **Factory shape** — factories must be callable and have a detectable shape.
5. **Seal** — after validation, further registrations raise `RuntimeError`.

All errors are caught at startup, before any job is dispatched.

### Who calls `validate()`?

When you pass `di_registry=` to `worker_main`, the worker calls `validate()`
for you as part of its bootstrap — after auto-registering `WorkerSettings`,
`Clock`, and `asyncpg.Pool`. **Do not call `validate()` yourself** in this
path; pre-validating would fail because the worker-injected providers are not
yet registered.

You only call `validate()` directly when you are **not** passing the registry
to a worker — e.g. in a standalone test that builds a `ProviderRegistry` and
resolves providers without a worker. `validate()` is idempotent: a second
call (including the worker's) is a no-op.

---

## See also

- [Actors](actors.md) — `@actor` decorator, `JobContext`, handler signatures
- [Workers](workers.md) — `open_worker_deps`, scope bootstrapping sequence
- [Rate Limiting](rate-limiting.md) — rate-limit registry wired via DI
- [API Reference — DI](../api-reference/di.md) — `ProviderRegistry`, `Scope` API docs
