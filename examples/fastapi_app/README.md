# FastAPI Example App

A minimal FastAPI application demonstrating enqueue, real-time job streaming
via SSE, and cancellation using only the public `taskq` surface.

## Running

```bash
uv run uvicorn examples.fastapi_app.main:app --reload
```

This app only enqueues jobs — it does not run a worker. The `process_item`
actor it enqueues is registered in `examples/worker.py`'s `ACTORS` dict, so
also start a worker in a separate terminal or the demo will hang with jobs
stuck `pending`:

```bash
uv run python examples/worker.py
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `TASKQ_PG_DSN` | Yes | Postgres DSN (e.g. `postgresql://user:pw@host/db`) |
| `TASKQ_REDIS_URL` | No | Redis URL; enables Redis transport for `tq.stream()` |
| `TASKQ_SCHEMA_NAME` | No | TaskQ schema name (default: `taskq`) |

## API

### Enqueue a job

```bash
curl -X POST http://localhost:8000/jobs
# → {"job_id": "a1b2c3d4-..."}
```

### Stream job events (SSE)

```bash
curl http://localhost:8000/jobs/{job_id}/stream
# → data: {"job_id":"...","status":"running","progress_state":{...},"progress_seq":1,"terminal":false}
# → data: {"job_id":"...","status":"succeeded","progress_state":{...},"progress_seq":3,"terminal":true}
```

**Note:** The SSE endpoint has no authentication — this is out of scope for
the demo. Production deployments must add auth.

### Cancel a job

```bash
curl -X DELETE http://localhost:8000/jobs/{job_id}
# → {"cancelled": true}
```

## Docker Compose

```bash
docker compose --profile example up
```

This starts 7 services: Postgres, Redis, two `examples/worker.py` replicas
(`worker-1`, `worker-2`), the `examples/app.py` and `examples/admin_app.py`
demo apps, and the FastAPI app on port 8002. Unlike the standalone `uvicorn`
command above, this always includes a worker, so enqueued jobs complete.
