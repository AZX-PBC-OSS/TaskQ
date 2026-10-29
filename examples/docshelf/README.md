# DocShelf — the adoption-sim product on TaskQ's workflow engine

A document-intelligence pipeline: documents arrive from an upload feed,
each document chunks (the map face), each chunk's extraction calls a mock
extraction API (flaky, slow, sometimes timing out), documents with many
chunks spawn **child flows** for the expensive sub-analyses (spawn +
join), the per-document results aggregate (the fan-in), documents branch
by class (the typed route: contracts → the summary arm, invoices → the
ledger arm, unknown → the quarantine arm), and the shelf publishes a
digest (the completion report). A nightly re-index runs on the durable
timer (cron) with a durable cursor (only NEW documents re-processed).

Everything external is **mocked at the boundary** (the extraction API,
the feed, the sub-analysis compute); the TaskQ machinery is **real** —
real Postgres, real rows, real worker processes.

## Layout

| file | what it is |
| --- | --- |
| `config.py` | the adverse dynamics, severity-tuned and recorded (flake 20%, timeouts 7%, delays 30-200 ms, sub-analyses 1-3 s) |
| `models.py` | the typed payload contract (every union member carries a `Literal` discriminator) |
| `store.py` | the app's own durable state (the document register, the API-call ledger, the parent↔child links, the re-index cursor) |
| `mockapi.py` | the mock extraction API + the deterministic "adversity rolls" |
| `deps.py` | the deps seam's ONE binding (the lazy pool holder) |
| `app.py` | the four workflows + the nightly cron + the feed's delivery |
| `worker_entry.py` | the `--actors` registry the worker CLI loads |
| `admin.py` | the admin sidecar builder (the operator's view) |
| `tests/` | the drills: happy path, flake, route, child, crash, duplicates, timer+cursor, cache, the operator faces, the chaos suite |

## Run it

```bash
# the two-minute Postgres (any reachable PG works)
docker run -d --name docshelf-pg -e POSTGRES_PASSWORD=taskq -e POSTGRES_DB=taskq -p 5804:5432 postgres:17

export TASKQ_PG_DSN=postgresql://postgres:taskq@localhost:5804/taskq
export TASKQ_SCHEMA_NAME=docshelf

# a worker (the import IS the workflow registration)
uv run taskq worker --actors examples.docshelf.worker_entry:ACTORS \
  --queues docshelf-feed,docshelf-cpu,docshelf-extract,docshelf-child,docshelf-summary,docshelf-ledger,docshelf-quarantine,docshelf-digest,docshelf-cron

# the admin sidecar (dev posture: the fail-closed auth off for the local tour)
TASKQ_ADMIN_UI_REQUIRE_AUTH=false TASKQ_PROGRESS_REQUIRE_AUTH=false \
  uv run uvicorn examples.docshelf.admin:create_app --factory --port 8001
```

## The drills

```bash
export DOCSHELF_PG_DSN=postgresql://postgres:taskq@localhost:5804/taskq
uv run pytest examples/docshelf/tests -x -q
```

Each drill's receipt (counts, retries, resumes, slot measurements, the
chaos variance) is captured under `.measurements/docshelf/`.
