"""BISECT 647-DIAG: TEMPORARY diagnostic — DELETE BEFORE MERGE.

Runs its own module-scoped worker probe and ALWAYS fails with the full
evidence dump (all served series names + the probe subprocess's
stdout/stderr tails + timing markers). The CI red truncates the ground
truth to 'assert []'; this surfaces it either way.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests._prom_review import _PROBE_DIAG, parse_exposition, run_worker_probe


@pytest.fixture(scope="module")
def _diag_scrapes(pg_dsn: str, module_pg_schema: Any, tmp_path_factory: Any) -> dict[str, str]:
    return run_worker_probe(
        pg_dsn,
        module_pg_schema.schema_name,
        tmp_path_factory.mktemp("prom_bisect647_diag"),
    )


def test_bisect647_diag_always_dump(_diag_scrapes: dict[str, str]) -> None:
    live = parse_exposition(_diag_scrapes["LIVE"])
    names = ", ".join(sorted(live.names()))
    diag = "\n\n".join(f"== probe {k} ==\n{v}" for k, v in _PROBE_DIAG.items()) or (
        "<no probe diag captured>"
    )
    pytest.fail(
        "BISECT 647-DIAG (always-dump) — served series:\n" + names + "\n\n" + diag,
        pytrace=False,
    )
