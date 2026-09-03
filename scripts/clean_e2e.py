#!/usr/bin/env python3
"""Remove every e2e stray this repo's test tier can leave on a machine.

Usage (from the repo root, via the Makefile):

    make clean-e2e

What it removes:

* stale test containers and pid-suffixed e2e networks (the standard sweep
  from ``taskq.testing._shared_containers`` — the same one every e2e run
  starts with)
* worker images the e2e tier built: pid-owned ``taskq-e2e-worker-r<pid>``
  repositories whose owner pid is dead, plus legacy
  ``taskq-e2e-worker:sha-*`` images from before pid ownership (no owner is
  recorded in those names, so the automatic session sweep leaves them;
  removal here is non-force — any image a container still references is
  refused and reported, never ripped out)
* cached wheel files under ``dist-e2e-wheels/`` (rebuilt on demand by the
  next run)
* dead-owner wheel scratch dirs (``dist-e2e-<pid>/``)

Resources whose owner pid is alive — a concurrently running e2e session —
are skipped and reported, never removed.

The keep/remove rules live in ``tests/e2e/_image_hygiene.py`` (pyright- and
ruff-checked there with unit tests in ``tests/e2e/test_image_hygiene.py``);
``scripts/`` is outside every quality gate, so this stays a thin wrapper.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
# The hygiene module lives in the e2e test tier; bootstrap the repo root so
# ``tests.e2e._image_hygiene`` imports as a package path. This must run
# before the import below, which is why that import is not at the top.
sys.path.insert(0, str(_REPO_ROOT))

from tests.e2e._image_hygiene import clean_e2e_strays  # noqa: E402


def main() -> int:
    clean_e2e_strays()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
