# Cron Scheduling

TaskQ provides built-in cron scheduling for periodic job execution. Declare schedules with
the `cron(...)` function, and the worker's maintenance leader fires them at their declared
cadence. Schedules are persisted in the `cron_schedules` table and auto-discovered at worker
startup.

---

## How cron works

1. You declare a schedule with `cron(expression, actor_name, ...)` at module import time.
2. The `cron()` function validates the expression and auto-registers the spec via
   `register_cron()`.
3. At worker startup, the bootstrap iterates registered specs and calls `create_schedule()`
   for each one (create-only, skip-on-conflict).
4. The elected maintenance leader runs a `_cron_loop` that checks `cron_schedules.next_fire_at`
   and enqueues a job for each due schedule.
5. After firing, the leader computes the next fire time and updates `next_fire_at`.

The cron loop runs inside the maintenance leader's `TaskGroup` alongside the scheduled-wake
loop, sweep loops, and prune/archive loops.

---

## The `cron()` function

```python
from taskq import cron

# Fire every day at 03:00 UTC
cron("0 3 * * *", "daily_report")

# Fire every 15 minutes with a static payload
cron("*/15 * * * *", "health_check", static_payload={"endpoint": "/api/health"})

# Fire every Monday at 09:00 America/New_York
cron("0 9 * * 1", "weekly_summary", timezone="America/New_York")

# Fire every 30 seconds — the optional 6th field is seconds, appended after
# the standard 5-field expression, so */30 must be placed last for sub-minute
# intervals.
cron("* * * * * */30", "ticker")
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `expression` | `str` | required | Standard 5-field cron expression (`minute hour day month day_of_week`), validated via `croniter.is_valid()`. An optional 6th field is also supported, appended **after** the standard 5 as a seconds field (e.g. `"* * * * * */30"` fires every 30 seconds) — this is `croniter`'s non-standard extension, not a leading seconds field. For sub-minute intervals, place the step in the 6th field; `*/30` in the first (minute) field fires every second during minutes 0 and 30, not every 30 seconds. Calendar rules are croniter's: day-of-month and day-of-week combine with OR (a `0 0 1 * 1` schedule fires on the 1st of the month *and* on Mondays); `L` in the day-of-month field means the last day of the month; a day-of-month with no such day in a month (e.g. the 31st in April, the 29th of February in a non-leap year) is skipped, never clamped. |
| `actor` | `str` | required | Name of the actor to enqueue. Must match a registered `ActorRef.name`. |
| `payload_factory` | `str \| None` | `None` | Dotted path to a callable that returns the payload `dict` or `BaseModel`. Async factories are awaited with a 5s timeout. |
| `static_payload` | `dict[str, object] \| None` | `None` | Fixed payload dict included with every fire. Mutually exclusive with `payload_factory`. |
| `name` | `str` | `""` | Schedule discriminator. When multiple schedules target the same actor (per-property scheduling), each must have a distinct `name`. Combined with `actor` to form the unique constraint `(actor, name)`. Defaults to `""` (empty string) which is treated as the single (legacy) schedule for that actor. See [Per-property schedules](#per-property-schedules). |
| `identity_key` | `str \| None` | `None` | Opaque identity key passed through to `enqueue()` on every fire. Enables cron↔on-demand dedup: a cron fire and an ad-hoc `enqueue()` with the same `identity_key` are deduplicated by `unique_for` on the actor. See [Per-property schedules](#per-property-schedules). |
| `timezone` | `str` | `"UTC"` | IANA timezone name (e.g. `"America/New_York"`). Controls when the cron expression fires. |
| `dst_strategy` | `"skip" \| "firstof" \| "allof"` | `"skip"` | How DST gaps and overlaps are handled — see [DST strategies](#dst-strategies). |
| `enabled` | `bool` | `True` | Whether the schedule is active at registration time. |

`cron()` raises `ValueError` on invalid cron expressions or when both `payload_factory` and
`static_payload` are provided.

---

## Payload resolution

Each fire resolves the payload through one of two mechanisms:

### Static payload

Pass `static_payload={"key": "value"}` to include a fixed dict with every fire:

```python
cron(
    "0 * * * *",
    "hourly_sync",
    static_payload={"source": "internal", "batch_size": 100},
)
```

### Payload factory

Pass `payload_factory="module.path.to_callable"` for dynamic payloads. The factory is
resolved via `importlib.import_module` + `getattr` and cached. Async factories are awaited
with a 5-second timeout:

```python
# myapp/payloads.py
from pydantic import BaseModel


class SyncPayload(BaseModel):
    cutoff: str


def make_sync_payload() -> dict:
    from datetime import datetime, UTC

    return {"cutoff": datetime.now(UTC).isoformat()}


# In your schedule declaration:
cron("0 * * * *", "hourly_sync", payload_factory="myapp.payloads.make_sync_payload")
```

The factory may return a `dict` (used as-is) or a `BaseModel` (converted via `.model_dump()`).
Any other return type raises `TypeError`.

---

## DST strategies

When a cron expression fires at a time that falls in a DST gap (spring-forward) or overlap
(fall-back), the `dst_strategy` controls behaviour:

| Strategy | Gaps (spring-forward) | Overlaps (fall-back) |
|---|---|---|
| `"skip"` (default) | Advance to the next valid cron match after the gap | Use the first (earlier) occurrence |
| `"firstof"` | Same as `skip` | Explicitly select the earlier wall-clock time |
| `"allof"` | Same as `skip` | Fire at **both** occurrences (enqueue two jobs) |

For UTC schedules, DST handling is irrelevant and `"skip"` is always used.

### When a fire lands inside a repeated hour

A schedule's `next_fire_at` can end up ON an occurrence of a repeated hour — most commonly
after a leader outage spanning the fall-back, or a manual edit. The strategies answer this
consistently with their meaning above:

- `"skip"` and `"firstof"` treat the repeated hour as **one slot at the earlier occurrence**.
  Firing that occurrence consumes the slot, so the next fire is the following match — nothing
  is owed.
- `"allof"` still owes the **later pass**. The tick that fires the last earlier-pass match
  advances `next_fire_at` to the next occurrence the schedule still owes — for a range with
  one match that is the fired slot's own later occurrence; for a range with several it is
  the later pass's first match. Any occurrence that already has a queued job (a tick firing
  an earlier-pass match pre-schedules the next match's later occurrence) is skipped past,
  however many are queued in a row, so every occurrence of the repeated range is delivered
  exactly once — by its pre-scheduled job or by the schedule's own later tick, never both.

Singleton-flagged actors are the sequential case: nothing is ever pre-scheduled for them, so
the schedule's own ticks deliver the later pass one occurrence at a time, each exactly once.

Before this behaviour was fixed, an `"allof"` schedule whose `next_fire_at` landed on the
earlier occurrence jumped straight to the following year: the naive wall-clock walk cannot
distinguish the two occurrences, so the later one was silently lost — reachable after any
leader outage spanning a fall-back.

```python
# Fire at 02:30 every day in a timezone with DST transitions.
# "allof" means the job fires twice during fall-back overlap.
cron("30 2 * * *", "dst_aware_job", timezone="Europe/Amsterdam", dst_strategy="allof")
```

---

## Per-property schedules

By default, each actor has at most one cron schedule — the `cron_schedules`
table enforces a unique constraint on `actor`. The `name` and `identity_key`
parameters extend this to support **multiple schedules per actor**, each
targeting a different logical entity (a "property").

### `name` — multiple schedules per actor

The unique constraint is `(actor, name)`, not just `actor`. When `name` is
`""` (the default) the schedule is the single legacy schedule for that
actor. Pass a distinct `name` to create additional schedules for the same
actor:

```python
from taskq import cron

# Daily report for each tenant — one schedule per tenant, same actor.
cron("0 3 * * *", "daily_report", name="tenant:acme")
cron("0 4 * * *", "daily_report", name="tenant:globex")
cron("0 5 * * *", "daily_report", name="tenant:initech")
```

Each schedule fires the `daily_report` actor independently with its own
`next_fire_at`, `consecutive_failures`, and `enabled` state. Disabling one
schedule (via `handle.disable()`) does not affect the others.

### `identity_key` — cron↔on-demand dedup

The `identity_key` parameter is passed through to `enqueue()` on every cron
fire. When the actor has `unique_for` configured, this enables deduplication
between cron-fired jobs and ad-hoc on-demand enqueues for the same logical
entity:

```python
from datetime import timedelta
from taskq import actor


@actor(unique_for=timedelta(hours=6))
async def sync_tenant(payload: TenantPayload) -> None: ...


# Cron schedule that fires hourly for tenant "acme".
cron(
    "0 * * * *",
    "sync_tenant",
    name="tenant:acme",
    identity_key="tenant:acme",
    static_payload={"tenant_id": "acme"},
)
```

If an operator triggers an on-demand sync via:

```python
await client.enqueue(
    sync_tenant,
    TenantPayload(tenant_id="acme"),
    identity_key="tenant:acme",
)
```

…the `unique_for` window deduplicates: if the cron already fired within the
last 6 hours, the on-demand enqueue returns the existing job handle with
`was_existing=True` rather than creating a duplicate. See
[`unique_for` deduplication](actors.md#unique_for-deduplication) and
[Jobs & Clients — enqueue evaluation order](jobs-clients.md#enqueue-evaluation-order).

### Full per-property example

```python
from datetime import timedelta
from pydantic import BaseModel
from taskq import actor, cron


class SyncPayload(BaseModel):
    tenant_id: str


@actor(queue="sync", unique_for=timedelta(hours=6))
async def sync_tenant(payload: SyncPayload) -> None:
    # ... sync logic per tenant ...
    ...


# Register one cron schedule per tenant. Each carries a distinct name
# (so the (actor, name) constraint is satisfied) and an identity_key
# (so cron fires dedup against on-demand enqueues).
for tenant_id in ("acme", "globex", "initech"):
    cron(
        "0 * * * *",
        "sync_tenant",
        name=f"tenant:{tenant_id}",
        identity_key=f"tenant:{tenant_id}",
        static_payload={"tenant_id": tenant_id},
    )
```

---

## Schedule management

### `CronScheduleSpec`

The `cron()` function returns a `CronScheduleSpec` — an immutable dataclass that describes
the schedule. It is registered in the module-level registry at call time.

```python
from taskq import CronScheduleSpec

spec = CronScheduleSpec(
    actor="daily_report",
    cron_expr="0 3 * * *",
    timezone="UTC",
    enabled=True,
)
```

`CronScheduleSpec` fields mirror the `cron()` parameters above, including
`name` and `identity_key` for per-property scheduling.

### `ScheduleHandle`

When a schedule is created in the database, a `ScheduleHandle` is returned by
`JobsClient` methods. The handle provides async methods for runtime management:

```python
schedules = await client.list_schedules()
# Find the schedule by actor name or inspect schedule_id
handle = await client.create_schedule("daily_report", "0 3 * * *")

await handle.disable()  # set enabled=False
await handle.enable()  # set enabled=True (resets consecutive_failures and last_fire_error)
await handle.delete()  # remove the schedule row
```

### Manual registration

You can register schedules programmatically without the decorator:

```python
from taskq import register_cron, CronScheduleSpec

register_cron(
    CronScheduleSpec(
        actor="cleanup_job",
        cron_expr="0 4 * * *",
        timezone="UTC",
    )
)
```

`register_cron()` validates the cron expression at call time. The registry is a plain list —
deduplication is the caller's responsibility. The database `(actor, name)` unique constraint
prevents duplicate schedules from persisting at startup. When `name` is `""` (the default),
at most one schedule per actor is allowed — the legacy single-schedule behaviour.

### Auto-discovery at startup

At worker startup, the bootstrap iterates `get_registered_crons()` and calls
`create_schedule()` for each spec. This is **create-only, skip-on-conflict**: existing
`cron_schedules` rows are never modified by the registration pass. If a `cron()`
call's parameters change after the schedule was first registered, the operator must
manually update or delete and recreate the schedule.

---

## Failure handling

When a schedule's payload factory raises an exception (import error, `TypeError`, timeout),
the cron loop:

1. Increments `consecutive_failures` on the schedule row.
2. Records `last_fire_error` with the exception class and message.
3. Computes `next_fire_at` as usual and continues.

After a configurable number of consecutive failures, the schedule is auto-disabled. The
`taskq.cron.consecutive_failures` up-down counter tracks failures per schedule, and the
`taskq.cron.disabled_schedules` observable gauge tracks the count of disabled schedules.

Calling `handle.enable()` resets `consecutive_failures` to 0 and clears `last_fire_error`.

---

## Singleton and `max_pending` interaction

Cron fires honor the actor's `singleton` and `max_pending` flags exactly like client
enqueues: every fire carries the same `metadata["singleton"]` stamp (the partial
unique index that enforces one active singleton job per actor keys on exactly that
flag) and the same `max_pending` cap. A schedule whose actor is blocked is
**suppressed**, not failed:

- A singleton actor with an active job suppresses the slot. Watch for the
  `singleton-collision` log event with `detection_path="cron_tick_preflight"` — the
  same event shape the enqueue path emits, with `schedule_id` and `worker_id` added
  for cron attribution.
- An actor whose `pending + scheduled` count is at its `max_pending` cap suppresses
  the slot. Watch for the `max-pending-exceeded` log event and the
  `taskq.backpressure.errors` counter with `kind="max_pending"` — the same counter
  the enqueue path records.

A suppressed slot advances `next_fire_at` and nothing else: no `last_fired_at` stamp
(nothing fired), no `last_fire_error` write, and no `consecutive_failures` change in
either direction. Suppression says nothing about the schedule's health — the actor is
merely busy — so it must neither punish nor amnesty. The classification is
load-bearing: a collision routed into the failure path instead would strike the
schedule, and three consecutive collisions (the default
`TASKQ_CRON_AUTO_DISABLE_THRESHOLD`) would permanently auto-disable a healthy, busy
actor's own schedule. The failure path stays reserved for genuine defects — payload
factory errors, missing `actor_config` rows, and the like.

Suppression is re-evaluated on every tick and is never sticky: once the blocker goes
terminal or pending capacity frees up, the next due tick fires normally, and the fire
itself carries the singleton flag (so it becomes the next blocker — the guarantee is
held by the schedule's own fires, not by the client alone).

---

## Admin UI

The admin UI provides a schedules page at `/admin/schedules` that lists all cron schedules
ordered by `next_fire_at`, showing the actor, expression, timezone, enabled status, and next
fire time. If the cron migration has not been applied, the page shows a notice directing the
operator to run `taskq migrate up`.

---

## See also

- [Actors](actors.md) — `@actor` decorator reference
- [Workers](workers.md) — maintenance leader, cron loop, sweep loops
- [Configuration](configuration.md) — `TASKQ_CRON_CATCH_UP_WINDOW`, `TASKQ_CRON_AUTO_DISABLE_THRESHOLD`, and other settings
- [Admin UI](admin-ui.md) — schedules page
- [API Reference — CLI](../api-reference/cli.md) — `taskq` command reference
