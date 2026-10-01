"""Backcompat shim: re-export ``FakeClock`` from its canonical home.

``FakeClock`` lives in :mod:`taskq.clock` (production, dependency-free —
imports only ``datetime``) so ``import taskq`` never pulls in
``taskq.testing`` and its test-double backend.  Import either way:

    from taskq.clock import FakeClock            # canonical
    from taskq.testing.clock import FakeClock    # backcompat shim
"""

from taskq.clock import FakeClock

__all__ = ["FakeClock"]
