# Production image for the TaskQ worker.
#
# Multi-stage: a uv builder resolves the locked dependency set (uv.lock,
# --frozen, dev group excluded) and a slim runtime ships only the resulting
# virtualenv. Reference documentation lives in docs/guides/deployment.md
# (Container); CI builds this image on every release tag, build only, no push.
#
# The image carries the taskq CLI and library only. Applications that use
# TaskQ extend it (PATH already points at /app/.venv/bin):
#
#     FROM taskq-worker
#     COPY myapp/ myapp/
#     CMD ["taskq", "worker", "--actors", "myapp.actors:registry"]

# syntax=docker/dockerfile:1

# ── builder ─────────────────────────────────────────────────────────────
# uv version-pinned to match pyproject's required-version (>=0.12.7,<0.13);
# its python3.13-trixie-slim image matches .python-version, so the sync
# below never downloads a second interpreter.
FROM ghcr.io/astral-sh/uv:0.12.17-python3.13-trixie-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# Dependency set first, so source edits do not bust the dependency layer.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --extra otel --extra redis

# The project itself, installed non-editable so the runtime stage needs no
# source tree.
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --extra otel --extra redis

# ── runtime ─────────────────────────────────────────────────────────────
# Same trixie distro family as the builder, so the venv's compiled
# extensions and bytecode land on a matching interpreter and libc.
FROM python:3.13-slim-trixie AS runtime

# Non-root, no home writes, no shell needs beyond the image defaults.
RUN groupadd --system taskq \
    && useradd --system --gid taskq --home-dir /app --no-create-home --shell /usr/sbin/nologin taskq

WORKDIR /app

COPY --from=builder --chown=root:root /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:${PATH}" \
    # Opt-in the TCP health listener on the port the deployment recipes use,
    # for orchestrators that cannot exec into the container. The HEALTHCHECK
    # below uses the in-container Unix socket either way.
    TASKQ_HEALTH_PORT=8600

USER taskq

EXPOSE 8600

# The worker serves /live and /ready on its Unix health socket
# (TASKQ_HEALTH_SOCKET_PATH, default /tmp/taskq_health.sock). `taskq health
# ready` is the same exec probe the Kubernetes and Compose recipes use; the
# readiness ping covers the dispatcher pool's Postgres reachability.
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=5 \
    CMD ["taskq", "health", "ready"]

# Full command form (no ENTRYPOINT), so orchestrators can override `command:`
# wholesale exactly as the deployment guide's recipes do.
CMD ["taskq", "worker", "--actors", "myapp.actors:registry"]
