"""The license-DOWNGRADE pins at the unit tier: a schema converted under
the full TSL license and met again by this module after the server's
license was downgraded to apache (``ALTER SYSTEM SET
timescaledb.license = 'apache'`` - the GUC cannot change in-session).

Two states are measured on real 2.30.1 (the lifecycle module carries the
container proofs):

* the stale policies cannot be removed under apache - EVERY removal API
  (``remove_retention_policy``, ``remove_columnstore_policy``,
  ``remove_compression_policy``) refuses with ``FeatureNotSupportedError``
  - so :func:`taskq.timescale._remove_registered_policies` SKIPS the
  removal calls in the apache mode and lets the loud remaining-check
  speak. The refusal must be HONEST about that: the policies did not
  "survive the removal APIs" (no removal API was attempted), the license
  did the surviving, and the only remedy names the license restore;
* the disable refuses loudly rather than swapping tables under a live
  policy - the refusal message is the operator's whole surface in that
  moment, so its text is pinned here.

Unit tier: the database reads are faked at the ``asyncpg`` boundary, the
house pattern of ``tests/test_cli_doctor.py``.
"""

from typing import Any

import pytest

from taskq.timescale import StorageMode, _remove_registered_policies


class _DowngradedConn:
    """A conn speaking for a TSL-built server downgraded to the apache
    license, still carrying ITS OWN conversion's policy jobs.

    ``fetch`` answers the policy-jobs reads (the pre-read for the report
    and the loud remaining-check run the same query shape); every
    ``execute`` is RECORDED and succeeds - under the apache license the
    removals are skipped outright, so the recording is the census: a
    removal statement reaching this stub in the apache mode is a bug the
    real server would refuse.
    """

    def __init__(self, remaining: list[dict[str, str]]) -> None:
        self.remaining = remaining
        self.executed: list[str] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, str]]:
        if "timescaledb_information.jobs" in query:
            return list(self.remaining)
        raise AssertionError(f"unexpected fetch: {query}")

    async def fetchval(self, query: str, *args: Any) -> Any:
        if "_timescaledb_catalog.hypertable" in query:
            # The _is_hypertable probe: under the apache downgrade the tables
            # are still hypertables (the disable refused before any swap).
            return True
        raise AssertionError(f"unexpected fetchval: {query}")

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append(query)
        return "OK"

    async def close(self) -> None: ...


_REMAINING = [
    {"hypertable_name": "jobs_archive", "proc_name": "policy_retention", "horizon": "2 days"},
    {
        "hypertable_name": "job_attempts_archive",
        "proc_name": "policy_retention",
        "horizon": "2 days",
    },
    {"hypertable_name": "job_events", "proc_name": "policy_retention", "horizon": "1 day"},
    {
        "hypertable_name": "jobs_archive",
        "proc_name": "policy_compression",
        "horizon": "1 day",
    },
]


async def test_downgrade_refusal_names_the_license_and_the_remedy() -> None:
    """The apache-mode refusal for surviving policies must say the license
    is the reason the policies are still there (the removal APIs were
    SKIPPED, never attempted - the recorded execute log is the proof) and
    must name the only honest remedy: the license restore."""
    conn = _DowngradedConn(_REMAINING)

    with pytest.raises(RuntimeError) as exc:
        await _remove_registered_policies(conn, "dgs", mode=StorageMode.TIMESCALE_APACHE)

    message = str(exc.value)
    # The honest mechanism: the license, not a claimed removal attempt.
    assert "'apache'" in message, message
    assert "survived the removal APIs" not in message, message
    assert "never attempted" in message, message
    # The honest remedy: under apache NOTHING can remove the policies -
    # every removal API refuses - so the way out is the license restore.
    assert "ALTER SYSTEM SET timescaledb.license = 'timescale'" in message, message
    # And the census: zero removal statements were attempted under apache.
    assert conn.executed == [], conn.executed


async def test_tsl_mode_surviving_policy_refusal_keeps_its_own_truth() -> None:
    """In the full TSL mode the removals ARE attempted (the skip is the
    apache mode's alone): a policy surviving THEM is a different failure
    with the message's original truth - the removal APIs ran and the
    policies outlived them. The mode branch must not blur the two."""
    conn = _DowngradedConn(_REMAINING)

    with pytest.raises(RuntimeError) as exc:
        await _remove_registered_policies(conn, "dgs", mode=StorageMode.TIMESCALE_TSL)

    message = str(exc.value)
    assert "survived the removal APIs" in message, message
    # ... and here the removal calls really did go out (every removal API
    # the TSL mode arms, per table and per trash name).
    assert any("remove_retention_policy" in q for q in conn.executed), conn.executed
