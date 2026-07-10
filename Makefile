.PHONY: help install test test-fast test-cov lint format type-check clean build css

help:
	@echo "Available commands:"
	@echo "  make install      - Install package, all extras, and dev dependencies"
	@echo "  make test         - Run all tests (parallel)"
	@echo "  make test-fast    - Run non-integration tests (parallel)"
	@echo "  make lint         - Run ruff linter"
	@echo "  make format       - Format code with ruff"
	@echo "  make type-check   - Run pyright type checker"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make build        - Clean and build package"
	@echo "  make css          - Rebuild admin UI CSS from Tailwind source"

install:
	uv sync --all-extras --group dev

test:
	uv run pytest -n 4

test-cov:
	uv run pytest -n 4 --cov=taskq --cov-report=term-missing --cov-report=html

test-fast:
	uv run pytest -n 4 -m "not integration"

lint:
	uv run ruff check .

format:
	uv run ruff format .
	uv run ruff check --fix .

type-check:
	uv run pyright src/taskq

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

build: clean
	uv build

css:
	npm install
	npm run build
