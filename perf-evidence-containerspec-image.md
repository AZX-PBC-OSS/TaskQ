# Perf evidence: containerspec against TaskQ's image-build surfaces

**Verdict: ADOPT for the examples Compose E2E path (adopted); SKIP for the
production `Dockerfile` and the test-time containers.** Evaluated
`containerspec==0.1.7` (PyPI, the repo's existing `e2e` dependency group —
no vendoring) as a fluent, content-hashed `ImageSpec` builder.

## What was evaluated

| Surface | Spec-expressed? | Parity | Verdict |
|---|---|---|---|
| `examples/Dockerfile` (the compose E2E's build) | yes | **byte-equal** (below) | **ADOPT** — wired |
| root `Dockerfile` (production image) | yes (round-trip) | not asserted | **SKIP** — `HEALTHCHECK` unexpressible; release-tag-only build has no repeated-run cost to amortize |
| test-time containers (pgbouncer stack, timescale image) | n/a | n/a | **SKIP** — stock registry images + inline env/commands via testcontainers; there is no image build to content-hash |

## Parity (spec-built vs `docker build -f examples/Dockerfile .`)

Both images compared as full rootfs trees (`docker export`, sha256 per path,
size, mode) after back-to-back same-content-era builds.

- `/app` sources, the entire `/app/.venv` (all extras + dev group — both
  images install it: `uv sync` installs the `dev` group by default), and the
  uv-managed CPython 3.13.15: **byte-equal**.
- Image config (`Env`, `WorkingDir`, `Cmd`, `Entrypoint`): **identical**.
- Workload check inside both images: `import examples.app, examples.worker,
  examples.admin_app, examples.otel_setup, fastapi, uvicorn, asyncpg,
  taskq` — **green in both**.
- Full compose workload: 3× `up -d` on the spec image (content-hashed tag
  pinned via `TASKQ_EXAMPLE_IMAGE`) — app ready, HTTP enqueue through the
  trigger app, jobs driven to `succeeded` in the stack's DB, clean `down -v`.
  3/3 green.
- Documented non-deterministic build-timestamp artifacts (equal treatment on
  both sides; the only bytes that differ):
  - `root/.cache/uv/**` (~28k paths): uv's build-time HTTP/wheel cache.
  - uv-managed-python stdlib `*.pyc`: install-time compilation headers.
  - `taskq_py-*.dist-info/uv_cache.json` (+ its `RECORD` hash entry): uv
    editable-install build timestamps.
  - `usr/local/lib/python3.12/site-packages/uv/__pycache__/*.pyc`: pip
    compile-time mtime headers.
  - `/app/**/__pycache__/*.pyc`: copied from the build context per
    Docker's root-anchored dockerignore semantics (nested `__pycache__` is
    NOT excluded by a bare `__pycache__` pattern — verified against this
    daemon's build).
- `to_dockerfile()` round-trip vs the original: identical modulo a pip cache
  mount superset (`--mount=type=cache,target=/root/.cache/pip`) and the COPY
  source rewritten to the staged context path. Fidelity: high.

Root-Dockerfile round-trip gaps (why the SKIP): containerspec has no
`HEALTHCHECK` layer (the original's `taskq health ready` probe would be
silently lost — the orchestrator recipes rely on it), `.user()` renders a
`useradd -m -d /home/taskq` instead of the original's
`--system --no-create-home --shell /usr/sbin/nologin`, and the uv `RUN`s
lose their `--mount=type=cache` accelerators.

## Measured build times (this machine, docker 29.8.0 / buildx 0.37.1)

Cold = fresh isolated `docker-container` builder (empty cache, real base
pulls); warm = repeated run in the same content era. The compose stack
builds 5 services from one Dockerfile.

| Path | True cold | Repeated run (warm) |
|---|---|---|
| stock `docker compose -f examples/docker-compose.yml build` | 35.2 s / 37.8 s (two runs) | 2.4 s (default builder) – 5.5 s (container builder) |
| containerspec spec build | 43.1 s / 46.5 s (two runs) | **1.2 s** (tag-exists skip; includes the ~1.1 s context-digest re-verification) |
| warm stock build after a source edit | — | 21.7 s (`COPY . /app` busts → full `uv sync` re-run) |

Reading the numbers:

- **Repeated-run cost (the suite runs the compose stack per local run, and
  `tests/test_compose_stack_e2e.py` per CI run): ~2.4–5.5 s → ~1.2 s.** A
  2–4× cut of the build portion, and the spec path's cost is flat.
- **After a BuildKit cache eviction** (`docker builder prune` — this
  machine's daemon held 45 GB reclaimable; CI runners have no cache at all):
  the stock warm path collapses to its cold cost (~36 s) while the spec path
  stays at ~1.2 s, because the cache key is the image tag itself, not
  BuildKit's cache state. This is the material win: ~30×.
- The cold path is ~8–11 s slower via the spec (staging + digest walk +
  `--pull`); paid once per content era, it does not amortize into the
  repeated-run cost the adoption targets.
- Content churn busts both paths identically (the spec hash is content-
  derived, like BuildKit's layer keys): a source edit costs ~21 s either way.

## What landed

- `benchmarks/example_image_spec.py` — the spec expression + dockerignore-
  faithful, content-addressed context staging (stable staging path: the
  content hash covers the copy layer's source PATH STRING, so a per-run temp
  path would re-mint the hash and pay a full rebuild every run — the same
  lesson `tests/e2e/conftest.py`'s wheel cache learned). `--print-tag` is
  daemonless.
- `examples/docker-compose.yml` — the 5 build services gained
  `image: ${TASKQ_EXAMPLE_IMAGE:-taskq-example:dev}`; unset, behavior is
  byte-for-byte today's (compose builds `examples/Dockerfile`), so
  `tests/test_compose_stack_e2e.py`'s `up -d --build` semantics are
  untouched.
- `docs/guides/deployment.md` (Container section) + `examples/README.md` —
  the adoption and the opt-in recipe.
- `src/` untouched.
