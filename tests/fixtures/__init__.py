"""Test fixtures for integration tests.

Provides ``always_failing_factory`` — a payload factory that always raises,
used by cron auto-disable integration tests.
"""


def always_failing_factory() -> dict[str, object]:
    msg = "always_failing_factory: deliberate failure for cron auto-disable test"
    raise RuntimeError(msg)
