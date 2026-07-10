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
uv run pyright src/taskq

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
uv run pyright src/taskq

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
   uv run pyright src/taskq

   # Check linting
   uv run ruff check .

   # Check formatting
   uv run ruff format --check .
   ```

## Pull Request Process

### Before Submitting

1. **Ensure all tests pass:**
   ```bash
   uv run pytest
   ```

2. **Ensure type checking passes:**
   ```bash
   uv run pyright src/taskq
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
