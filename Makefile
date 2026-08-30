.PHONY: help install env test test-fast test-e2e test-cov lint format type-check clean build css

help:
	@echo "Available commands:"
	@echo "  make install      - Install package, all extras, dev and e2e dependencies"
	@echo "  make test         - Run all tests (parallel)"
	@echo "  make test-fast    - Run non-integration tests (parallel)"
	@echo "  make test-e2e      - Run e2e tests (containerized workers; serial)"
	@echo "  make lint         - Run ruff linter"
	@echo "  make format       - Format code with ruff"
	@echo "  make type-check   - Run pyright type checker"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make build        - Clean and build package"
	@echo "  make css          - Rebuild admin UI CSS from Tailwind source"

# ---------------------------------------------------------------------------
# Environment discipline. Mirrors the policy comment at the top of
# .github/workflows/ci.yaml, and tests/test_ci_workflow.py enforces both.
#
# Exactly one place in this file builds the environment (the `env` target).
# Everything that runs out of it goes through $(UVRUN), which is
# `uv run --no-sync`.
#
# Why that matters: a bare `uv run` re-resolves the interpreter and re-syncs on
# every invocation. Its implicit sync is inexact, so it will not uninstall an
# extra that is already there -- but when it decides the venv it found is on
# the wrong interpreter, it REMOVES that venv and rebuilds it from the DEFAULT
# dependency set. Every extra and every non-default group is gone, and the
# command that triggered it was something as innocent as `make lint`. This
# repo's .python-version is 3.13, so any venv built on another interpreter is
# one bare `uv run` away from being discarded.
#
# $(SYNC_ARGS) is the single definition of what a complete TaskQ development
# environment contains. `--group e2e` is included because `type-check` and
# `test-e2e` both need containerspec; keeping it out of the shared definition
# is what would make those two targets the only ones that quietly re-sync.
UVRUN := uv run --no-sync
SYNC_ARGS := --locked --all-extras --group dev --group e2e

# `--locked`, never `--frozen`: both refuse to re-resolve, so neither can
# install a version uv.lock never described. They differ on what happens when
# pyproject.toml and uv.lock disagree -- `--frozen` ignores it and installs a
# set silently missing a newly added dependency, `--locked` fails and tells you
# to run `uv lock`. Failing is the useful behaviour.
install:
	uv sync $(SYNC_ARGS)

# The prerequisite every target below takes, so `make test` works on a fresh
# clone without any target ever pruning the environment as a side effect.
#
# The check is `uv sync --check` (a ~40ms no-op when the environment already
# matches) rather than a file-mtime rule against pyproject.toml/uv.lock. An
# mtime rule is fooled by the exact accident this whole change exists to
# prevent: a manual `uv sync` that prunes the extras also refreshes the venv's
# mtime, so make would consider the environment fresh and every --no-sync
# target would then run against the pruned set in silence. `--check` compares
# the environment against what SYNC_ARGS asks for, so it notices.
env:
	@uv sync --check $(SYNC_ARGS) >/dev/null 2>&1 || uv sync $(SYNC_ARGS)

test: env
	$(UVRUN) pytest -n 4

test-cov: env
	$(UVRUN) pytest -n 4 --cov=taskq --cov-report=term-missing --cov-report=html --cov-fail-under=90

test-fast: env
	$(UVRUN) pytest -n 4 -m "not integration"

# `--group e2e` belongs in SYNC_ARGS, not on this line: passing it to `uv run`
# would ask uv to sync a second time, which is what --no-sync exists to stop.
test-e2e: env
	$(UVRUN) pytest --e2e -m e2e tests/e2e

lint: env
	$(UVRUN) ruff check .
	$(UVRUN) ruff format --check .

format: env
	$(UVRUN) ruff format .
	$(UVRUN) ruff check --fix .

type-check: env
	$(UVRUN) pyright src/taskq tests

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	rm -rf htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# `uv build` resolves its own isolated PEP 517 build environment and never
# touches .venv, so it takes no `env` prerequisite and no --no-sync.
build: clean
	uv build

css:
	npm install
	npm run build
