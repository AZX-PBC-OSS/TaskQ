"""The system-e2e tier: stateful, multi-process simulations of the system running in production shape.

See ``docs/design/system-e2e-tier.md`` for what the tier covers and how to
run it. The tier's own conftest (``tests/system_e2e/conftest.py``) carries
the shared fixtures; ``tests/system_e2e/actors.py`` is the single actor
module both the test process and the worker subprocesses import.
"""
