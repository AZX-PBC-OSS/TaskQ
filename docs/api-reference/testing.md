# Testing

`InMemoryBackend`, `FakeClock`, pytest fixtures, and test assertions.

## Package surface (fakes, assertions, settings factories)

::: taskq.testing

## Pytest fixtures

The fixtures are not re-exported from `taskq.testing.__init__` (importing
`pytest`/`asyncpg` at the package top level is deliberately avoided), so they
render from their defining module:

::: taskq.testing.fixtures

## Health-socket helpers

::: taskq.testing.health
