# Example image for the TaskQ quickstart / compose stack (examples/docker-compose.yml).
#
# Base is the 3.14/trixie canary — one interpreter ahead of the production
# Dockerfile's 3.13/trixie pair, so the example stack exercises the next
# Python before production adopts it; uv pinned to the repo's validated
# 0.12.x line (pyproject required-version is >=0.12.7,<0.13), so the sync
# below cannot run on an unvetted uv release.
FROM python:3.14-slim-trixie

# UV_PYTHON_PREFERENCE=only-system: build the venv on THIS image's
# interpreter instead of letting uv download a managed CPython (whose
# bundled pip vendors known-vulnerable copies — see the pip removal below).
# UV_PYTHON=3.14 outranks the repo's .python-version dev pin (3.13) that
# `COPY . /app` ships into the build context: without it uv demands the
# 3.13 interpreter, finds none in this 3.14 image, and the sync dies (the
# same request governs the compose files' `uv run --no-sync` at container
# start, so the env var has to live here, in the image).
ENV UV_PYTHON_PREFERENCE=only-system \
    UV_PYTHON=3.14

RUN pip install --no-cache-dir uv==0.12.17

WORKDIR /app

COPY . /app

# --locked: refuse to install anything if uv.lock and pyproject.toml disagree
# (same discipline CI enforces on the repo itself). --no-dev: the example
# services need only the fastapi+redis extras; the default dev group would
# ship the docs/typecheck toolchain into the image. The uv cache is removed:
# its unpacked wheels are trivy-visible package metadata that never runs.
RUN uv sync --locked --no-dev --extra fastapi --extra redis \
    && rm -rf /root/.cache/uv

# pip is removed with the user setup: nothing in the example services
# installs packages at runtime, while the base image's pip vendors
# known-vulnerable copies that no pip release fixes yet
# (pip._vendor.msgpack 1.1.2 -> GHSA-6v7p-g79w-8964; the pkg_resources
# setuptools-70.3.0 code -> CVE-2025-47273). Non-root: the example services
# only read /app and talk to Postgres/Redis over the compose network.
RUN pip uninstall --no-cache-dir -y pip \
    && groupadd --system taskq \
    && useradd --system --gid taskq --home-dir /app --no-create-home --shell /usr/sbin/nologin taskq \
    && chown -R taskq:taskq /app

USER taskq
