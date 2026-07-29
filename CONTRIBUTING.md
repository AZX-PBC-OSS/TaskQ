# Contributing to TaskQ

Thank you for your interest in contributing to TaskQ! We appreciate your time and effort in making this library better for everyone.

## Development Setup

### Prerequisites

- Python 3.12 or higher
- [uv](https://docs.astral.sh/uv/) - Fast Python package installer and resolver
- [Docker](https://docs.docker.com/get-docker/) - Required for integration tests (uses testcontainers)

### Getting Started

1. **Clone the repository:**
   ```bash
   git clone https://github.com/AZX-PBC-OSS/TaskQ.git
   cd TaskQ
   ```

2. **Install dependencies:**

   This project uses `uv` for dependency management. Install development dependencies (including all optional extras) with:
   ```bash
   uv sync --all-extras --group dev
   ```

   This will create a virtual environment and install all necessary dependencies including pytest, pyright, ruff, testcontainers, and hypothesis. The optional extras (`redis`, `fastapi`, `prometheus`, `dev`) are required for the full test suite.

## Running Tests

**IMPORTANT: Always use `uv run` to execute pytest and other development commands.**

Using `uv run` ensures that:
- Commands run in the correct virtual environment
- All dependencies are properly resolved
- You're using the exact versions specified in the project
- No conflicts with globally installed packages

### Test Commands

```bash
# Run all tests (integration tests require Docker)
uv run pytest

# Run unit tests only (skip integration tests that need Docker)
uv run pytest -m "not integration"

# Run tests in parallel
uv run pytest -n auto

# Run specific test file
uv run pytest tests/test_actor.py

# Run specific test class
uv run pytest tests/test_actor.py::TestActorRef

# Run specific test function
uv run pytest tests/test_batch.py::test_wait_for_batch

# Run with verbose output
uv run pytest -v

# Run with coverage report
uv run pytest --cov=taskq --cov-report=html

# Run tests matching a pattern
uv run pytest -k "test_enqueue"

# Run tests with output from print statements
uv run pytest -s
```

### Integration Tests

Integration tests are marked with `integration` and require Docker because they use [testcontainers](https://testcontainers.com/) to spin up real PostgreSQL and Redis instances. These tests verify behavior against actual database backends.

```bash
# Run only integration tests
uv run pytest -m "integration"

# Skip integration tests (no Docker required)
uv run pytest -m "not integration"
```

If you don't have Docker available, use `uv run pytest -m "not integration"` to run the unit test suite.

### Property-Based Tests

TaskQ uses [Hypothesis](https://hypothesis.readthedocs.io/) for property-based testing. These tests generate randomized inputs to find edge cases that hand-written tests might miss. Property-based tests live alongside the standard test suite and run as part of `uv run pytest`.

When adding new functionality, consider writing property-based tests for invariants that should hold across a range of inputs (e.g., serialization round-trips, rate limiter correctness under concurrent access).

### Understanding Test Output

The project is configured with the following pytest settings (in `pyproject.toml`):
- Coverage is automatically collected for the `taskq` package
- HTML coverage reports are generated in `htmlcov/`
- Coverage does not fail local test runs; the 90% threshold is enforced as a separate CI job
- Coverage reports exclude test files and implementation details

## Code Quality

### Type Checking

The project uses `pyright` for type checking:

```bash
# Run type checking
uv run pyright src/taskq tests

# Type check specific file
uv run pyright src/taskq/actor.py
```

### Linting and Formatting

The project uses `ruff` for both linting and formatting:

```bash
# Check for linting issues
uv run ruff check .

# Auto-fix linting issues
uv run ruff check --fix .

# Format code
uv run ruff format .

# Check formatting without making changes
uv run ruff format --check .
```

### Running All Quality Checks

Before submitting a PR, run all quality checks:

```bash
# Run tests with coverage
uv run pytest

# Run type checking
uv run pyright src/taskq tests

# Run linting
uv run ruff check .

# Check formatting
uv run ruff format --check .
```

## Making Changes

### Workflow

1. **Create a feature branch:**
   ```bash
   git checkout -b feat/your-feature-name
   ```

   Or for bug fixes:
   ```bash
   git checkout -b fix/bug-description
   ```

2. **Make your changes:**
   - Write clear, readable code
   - Follow existing code patterns and conventions
   - Add type hints to all functions and methods
   - Keep functions focused and modular

3. **Write tests:**
   - Add tests for all new functionality
   - Update existing tests if modifying behavior
   - Ensure tests are clear and well-documented
   - Test edge cases and error conditions
   - Run tests with `uv run pytest`

4. **Update documentation:**
   - Update README.md if adding new features
   - Add docstrings to new functions and classes
   - Update type hints and examples

5. **Verify your changes:**
   ```bash
   # Run all tests
   uv run pytest

   # Check types
   uv run pyright src/taskq tests

   # Check linting
   uv run ruff check .

   # Check formatting
   uv run ruff format --check .
   ```

## Authoring Database Migrations

TaskQ's schema evolves through SQL migration files bundled with the package and applied by the runner in `src/taskq/migrate.py`. This section is for TaskQ developers adding or changing those files. End users never author migrations — they only apply them — so operator-facing guidance (upgrading, failure recovery) lives in [docs/guides/upgrading.md](docs/guides/upgrading.md) and is out of scope here.

### File Naming and Discovery

Migration files live in `src/taskq/migrations/` and follow one naming convention:

```
{ver}_{nn}_{pre|post}_{description}.sql     # e.g. 01.00.03_01_pre_idempotency_scope.sql
```

- `{ver}` is three two-digit groups joined by dots (`01.00.03`), `{nn}` is a two-digit sequence, and `{description}` is lowercase `[a-z0-9_]+`. A filename that does not match this pattern fails discovery with a `ValueError`.
- `discover()` finds every `*.sql` file in the package automatically — there is no registry or index to update — and returns them sorted by version, with `pre` before `post` at the same version. The zero-padding is what makes the lexicographic sort chronological, so keep the two-digit groups.
- Migration versions are numbered independently of the package version: releases v0.1.0–v0.2.2 all shipped the same two migrations (`01.00.00_01` and `01.00.01_01`). Pick the next free `{ver}_{nn}`; do not try to mirror the package's semver.

### The Default Transaction Wrapper

By default each migration file runs inside its own transaction, so a failure rolls the whole file back. Keep migrations transactional unless Postgres makes that impossible.

### Non-Transactional Migrations (`-- taskq:no-transaction`)

Postgres forbids some statements inside a transaction block — `CREATE INDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY`, several `ALTER TYPE`/`VACUUM` forms. A migration that needs one of these opts out of the wrapper with a header directive:

```sql
-- taskq:no-transaction
```

The usual motivation is lock duration: a plain `CREATE INDEX` holds a lock that blocks readers and writers for the whole build, which is not acceptable on hot tables such as `jobs` or `job_events`. The `CONCURRENTLY` forms avoid that lock but must run outside a transaction.

Directive parsing rules (implemented by `_uses_transaction` in `src/taskq/migrate.py`):

- **Leading comment block only.** Only blank lines and `--` line comments before the first SQL token are scanned; a directive later in the file is ignored, so a stray mention cannot silently flip a migration's semantics.
- **Prefix match.** A trailing note after the token is fine and encouraged: `-- taskq:no-transaction — CIC cannot run inside a transaction`.
- **Case-insensitive**, like SQL itself.
- **Token-bounded.** A `\b` word boundary stops prefix drift, so `-- taskq:no-transactional` does NOT opt out.
- **Typos surface.** A `taskq:` header line that doesn't match the directive (a typo, or a lookalike such as `no-transactional`) logs a `migration-directive-unrecognized` warning naming the file, instead of silently running the migration transactional — the worst outcome for a lock-duration-motivated migration.
- Files are decoded as `utf-8-sig`, so a BOM from a Windows editor cannot silently disable the directive.

A non-transactional file is executed statement by statement (a multi-statement string would run as one implicit transaction and defeat the opt-out). Two rules keep that safe:

1. **The migration must be idempotent and re-runnable.** Nothing rolls back: a mid-file failure leaves earlier statements in place, the ledger records the migration only after every statement succeeds, and the migration is re-executed on the next `migrate up`. Write every statement to tolerate being re-run (`IF NOT EXISTS`, guarded inserts, ...).
2. **Drop interrupted-build debris first.** An interrupted `CREATE INDEX CONCURRENTLY` leaves an INVALID index behind, and `IF NOT EXISTS` alone would then silently skip rebuilding it. Pair the statements:

   ```sql
   -- taskq:no-transaction
   -- NOT redundant with IF NOT EXISTS below: an interrupted CREATE INDEX
   -- CONCURRENTLY leaves an INVALID index that IF NOT EXISTS alone would
   -- silently skip rebuilding, so drop the debris first.
   DROP INDEX CONCURRENTLY IF EXISTS "{schema}".jobs_foo_idx;
   CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_foo_idx ON "{schema}".jobs (foo);
   ```

Transaction-control statements are rejected in non-transactional files: `BEGIN`, `COMMIT`, `ROLLBACK` (plus their Postgres synonyms `END`, `ABORT`, `START`), `SAVEPOINT`, `RELEASE`, and `SET LOCAL` / `SET TRANSACTION`. The transaction-control group would silently re-open a transaction — defeating `CONCURRENTLY` and, on failure, poisoning the caller's connection. `SET LOCAL`/`SET TRANSACTION` are rejected for the opposite reason: outside a transaction they are silent no-ops, so you would believe e.g. `statement_timeout` was disabled for a long build when it was not. The guard rejects the whole file before any statement executes. Plain `SET` / `SET SESSION` and `CHECKPOINT` are deliberately allowed: they are session-scoped (or transaction-agnostic) and behave identically either way.

### The `{schema}` Placeholder

Always qualify objects with the literal `"{schema}"` token — the runner substitutes the configured schema name at apply time (after validating it as an identifier), which is what lets multiple TaskQ instances share one Postgres cluster. Never hardcode a schema name. Substitution uses `str.format`, so a literal curly brace in the SQL is written doubled (`{{` / `}}`).

### Pre and Post Phases for Rolling Deploys

Destructive changes are split across a deploy boundary:

- **`pre`** adds structures that BOTH old and new code tolerate (e.g. a new index alongside the old one). Safe to apply before or during the code rollout.
- **`post`** removes the old structures after every worker in the fleet has been upgraded.

The runner enforces the ordering: a post-phase migration is refused until its same-version pre-phase counterpart is applied (or applies earlier in the same run). See `01.00.03_01_pre_idempotency_scope.sql` for the canonical example, including the "PHASE OBLIGATIONS" header that spells out the deployment sequence.

### Never Edit a Released Migration

The ledger (`{schema}.schema_migrations`) records each applied migration under its `{ver}_{nn}:{phase}` key together with a SHA-256 checksum of the rendered SQL. Editing a file after it shipped makes the checksum drift, and the runner logs a `migration-checksum-drift` warning on every subsequent apply. Treat released migrations as frozen — releases v0.1.0 through v0.2.2 shipped only `01.00.00_01` and `01.00.01_01`, and those files have not changed since. Fix forward with a new migration instead.

### File Header Convention

Every migration file opens with a `--` header comment block, modeled on the existing files:

1. One short paragraph stating what the migration does and why.
2. The forward-only reminder: "Forward-only; there is no down migration. To revert, restore from backup."
3. The substitution note: 'The literal "{schema}" token is substituted at apply time by the migration runner.'

Migrations with operational impact add named ops-note sections under banner comments — see the `-- ── Maintenance-window caveat ... ──` block in `01.00.02_01_pre_job_events_outbox.sql`. State the lock impact plainly: which lock the statement takes, on which table, how long it can be held (build time is proportional to row count), what it blocks (readers/writers, hot paths), and what operators of large deployments should do instead (e.g. build the index manually with `CONCURRENTLY` during a maintenance window). Phase-coupled migrations additionally document the deployment sequence and the failure modes of applying out of order (see "PHASE OBLIGATIONS" in `01.00.03_01_pre_idempotency_scope.sql`).

### What CI Proves for You

- **tests/test_migrations.py** applies the full bundled set to a real PostgreSQL and pins that a second `apply_pending` is a no-op.
- **tests/test_migrations_populated.py** applies every bundled migration one step at a time onto seeded data. New migrations join this harness automatically — unknown keys get the generic per-step invariants — and its seeder intersects live columns from `information_schema`, so a future NOT NULL-without-default column fails there as the alarm. Review `_MIGRATION_SPECIFIC_CHECKS` when adding a migration whose populated-DB effect deserves a sharper assertion.
- **`test_bundled_migrations_are_all_transactional`** (tests/test_migrations_unit.py) pins that no bundled migration carries the no-transaction directive yet. It becomes an allowlist once PRs #25/#27 land the first bundled no-transaction migration.
- **Directive-parsing, transaction-control-guard, and statement-splitter unit tests** (tests/test_migrations_unit.py) pin every rule quoted above.
- **`test_discover_directive_parsing_applies_end_to_end`** (tests/test_migrate_no_transaction.py) exercises the real `discover()` parse against a real database, directive-with-trailing-note included.
- **`test_migrate_up_cli_reports_failed_no_transaction_migration`** (tests/test_migrate_no_transaction.py) proves the `migrate up` failure report end to end: what failed, what state it left the schema in, and the one action to take.
- **`test_interrupted_concurrent_build_remedy_drop_and_rebuild`** (tests/test_migrate_no_transaction.py) stages a real interrupted `CREATE INDEX CONCURRENTLY` and proves the drop-and-rebuild remedy replaces the INVALID index with a valid one.

### Local Loop

```bash
uv run pytest tests/test_migrations.py tests/test_migrations_populated.py tests/test_migrations_unit.py tests/test_migrate_no_transaction.py -q
```

The integration files need Docker, like the rest of the integration suite.

### Non-Goals

- **No downgrade machinery.** There is no `down` operation and none is planned; reverting means restoring from a database backup.
- **No auto-retry in the runner.** Re-running `migrate up` (or restarting the worker) IS the heal — migrations are idempotent by contract, so a failed apply is fixed forward, not retried by the framework.
- **No `migrate status` health probing.** `migrate status` lists applied and pending migrations; it deliberately does not validate or repair schema state.
- **End users never author migrations.** This section covers the bundled files only; there is no user-defined migration hook.

## Pull Request Process

### Before Submitting

1. **Ensure all tests pass:**
   ```bash
   uv run pytest
   ```

2. **Ensure type checking passes:**
   ```bash
   uv run pyright src/taskq tests
   ```

3. **Ensure code is properly formatted:**
   ```bash
   uv run ruff format .
   uv run ruff check --fix .
   ```

4. **Verify coverage hasn't decreased:**
   ```bash
   uv run pytest --cov=taskq --cov-report=term-missing
   ```

   Coverage should remain at or above 90%. CI enforces this via a dedicated `coverage` job; local `uv run pytest` does not fail on coverage.

### Submitting Your PR

1. **Push your branch:**
   ```bash
   git push origin feat/your-feature-name
   ```

2. **Create a pull request on GitHub**

3. **In your PR description:**
   - Clearly describe what changes you made
   - Explain why the changes are needed
   - Reference any related issues
   - Include examples of new functionality (if applicable)
   - List any breaking changes

### PR Description Template

```markdown
## Summary
Brief description of what this PR does

## Changes
- Change 1
- Change 2
- Change 3

## Testing
Describe how you tested these changes

## Related Issues
Fixes #123
```

## Code Review

### What to Expect

- All changes must pass automated tests and type checking
- Code reviewers will check for:
  - Implementation correctness
  - Code clarity and maintainability
  - Adequate test coverage
  - Documentation completeness
  - Adherence to project conventions

- You may be asked to make revisions
- Reviews are constructive - they help improve code quality

### Addressing Feedback

When reviewers request changes:

1. Make the requested changes in your branch
2. Run tests again: `uv run pytest`
3. Push the updates: `git push origin feat/your-feature-name`
4. Respond to reviewer comments

## Development Tips

### Virtual Environment

The `uv run` command automatically manages the virtual environment. You don't need to manually activate it.

If you prefer to activate the environment manually:
```bash
# uv creates a .venv directory
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate  # Windows
```

However, we recommend using `uv run` for consistency.

### Debugging Tests

To debug a specific test with pdb:

```bash
# Add breakpoint() in your test or code
# Then run with -s to see output
uv run pytest -s tests/test_actor.py::test_specific_function
```

### Coverage Reports

After running tests with coverage, view the HTML report:

```bash
uv run pytest --cov=taskq --cov-report=html
# Open htmlcov/index.html in your browser
```

## Code Style Guidelines

### General Principles

- Write clear, self-documenting code
- Use meaningful variable and function names
- Keep functions short and focused (ideally under 50 lines)
- Avoid deep nesting (max 3-4 levels)
- Comments should explain "why", not "what"

### Type Hints

Always use type hints:

```python
# Good
def process_value(value: str, default: int = 0) -> int:
    return int(value) if value else default

# Bad
def process_value(value, default=0):
    return int(value) if value else default
```

### Docstrings

Use clear docstrings for public APIs:

```python
def enqueue(self, actor: ActorRef[...], *args: P.args, **kwargs: P.kwargs) -> JobHandle:
    """Enqueue a job for the given actor.

    Args:
        actor: The actor reference to invoke.
        *args: Positional arguments to pass to the actor.
        **kwargs: Keyword arguments to pass to the actor.

    Returns:
        A handle that can be awaited to retrieve the job result.

    Raises:
        QueueFullError: If the queue's max pending limit has been reached.
    """
```

### Error Messages

Write helpful error messages:

```python
# Good
raise ValueError(
    f"Invalid queue '{queue}'. "
    f"Configured queues: {', '.join(sorted(self._queues))}"
)

# Bad
raise ValueError("Invalid queue")
```

## Getting Help

- **Questions?** Open a [GitHub Discussion](https://github.com/AZX-PBC-OSS/TaskQ/discussions)
- **Bug Reports?** Open an [Issue](https://github.com/AZX-PBC-OSS/TaskQ/issues)
- **Feature Requests?** Open an [Issue](https://github.com/AZX-PBC-OSS/TaskQ/issues) with the `enhancement` label

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Thank You!

Your contributions help make TaskQ better for everyone. We appreciate your time and effort!
