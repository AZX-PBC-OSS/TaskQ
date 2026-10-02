"""BISECT 647-DIAG: TEMPORARY diagnostic — DELETE BEFORE MERGE.

Runs its own module-scoped worker probe. When the escalation series is
absent this test FAILS with the full evidence dump (all served series
names + the probe subprocess's stdout/stderr tails), which is the ground
truth the CI red hides behind its truncated ``assert []``.
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


def test_bisect647_diag_abandoned_series_or_full_dump(_diag_scrapes: dict[str, str]) -> None:
    live = parse_exposition(_diag_scrapes["LIVE"])
    names = live.names()
    if "taskq_jobs_abandoned_total" in names:
        return
    diag = "\n\n".join(f"== probe {k} ==\n{v}" for k, v in _PROBE_DIAG.items()) or (
        "<no probe diag captured>"
    )
    pytest.fail(
        "ESCALATION SERIES ABSENT — served series:\n"
        + ", ".join(sorted(names))
        + "\n\n"
        + diag,
        pytrace=False,
    )
