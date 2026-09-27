"""The storage-mode DETECTION pins, one per mode and per detection arm.

``taskq doctor``'s first finding family and the docs' support matrix both
render from :func:`taskq.timescale.detect_storage_mode`'s verdict, so the
verdict is pinned arm by arm, at the ``asyncpg`` boundary, before any
container is needed (the REAL-server pins - the 2.30.1 image, the same
image configured down to the apache license, and plain Postgres - live in
``test_timescale_three_mode_lifecycle.py``, which runs this same function
against all three containers).

The arms, in the order the function consults them:

* no ``timescaledb`` extension installed → ``vanilla`` - even when the
  server OFFERS the extension (``pg_available_extensions`` has the row):
  availability is not the mode, installation is;
* installed + ``timescaledb.license = 'timescale'`` → ``timescale-tsl``
  (the 2.30.1 image's measured value; the GUC is the extension's own
  statement of which feature set is in force);
* installed + ``'tsl'`` (the value older builds spell) → ``timescale-tsl``;
* installed + ``'apache'`` → ``timescale-apache`` - the arm that makes
  both Apache-edition builds AND TSL-capable builds configured down to
  Apache features (``ALTER SYSTEM SET timescaledb.license = 'apache'``)
  detect identically, because their capability consequences ARE identical
  (measured on 2.30.1: under the apache license the server refuses the
  retention policies AND the columnstore alike);
* installed + no license GUC at all (a build old enough to predate it) →
  the fallback: the compression machinery's presence decides, because at
  those ages it is the only capability the classification exists to
  distinguish.

Unit tier: the database reads are faked at the ``asyncpg`` boundary, the
house pattern of ``tests/test_cli_doctor.py``.
"""

from typing import Any

import pytest

from taskq.timescale import (
    TimescaleCapability,
    detect_storage_mode,
    probe_timescale_capability,
)


class _ProbeConn:
    """A conn that answers exactly the probe's queries, nothing else.

    ``license`` is what ``current_setting('timescaledb.license', true)``
    returns; ``compress_chunk`` is what ``to_regproc('compress_chunk')``
    resolves to. Any other query is a test bug - fail it loudly.
    """

    def __init__(
        self,
        *,
        available: bool,
        installed: bool,
        license: str
        | None,  # Why: the GUC's own name, shadowing the built-in deliberately at this fake's boundary.
        compress_chunk: str | None = None,
    ) -> None:
        self.available = available
        self.installed = installed
        self.license = license
        self.compress_chunk = compress_chunk
        self.queries: list[str] = []

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.queries.append(query)
        if "pg_available_extensions" in query:
            return self.available
        if "pg_extension" in query:
            return self.installed
        if "shared_preload_libraries" in query:
            return "timescaledb"
        if "timescaledb.license" in query:
            return self.license
        if "to_regproc" in query:
            return self.compress_chunk
        raise AssertionError(f"unexpected probe query: {query}")


@pytest.mark.parametrize(
    ("license", "expected"),
    [
        pytest.param("timescale", "timescale-tsl", id="timescale-license"),
        pytest.param("tsl", "timescale-tsl", id="legacy-tsl-license"),
        pytest.param("apache", "timescale-apache", id="apache-license"),
    ],
)
async def test_license_guc_decides_the_mode_when_installed(license: str, expected: str) -> None:
    """The extension installed, the license GUC present: the GUC's value
    alone classifies the mode - all three license spellings pinned."""
    conn = _ProbeConn(available=True, installed=True, license=license)
    assert await detect_storage_mode(conn) == expected


async def test_extension_absent_is_vanilla_even_when_offered() -> None:
    """A server that OFFERS the extension but has not created it is a
    vanilla server to every runtime path: no hypertables exist, the
    deploy step with the flag on is what creates it. Detection reads
    installation, never availability - and the license GUC's absence
    (a vanilla server defines none) reads as ``None``, never an error."""
    conn = _ProbeConn(available=True, installed=False, license=None)
    assert await detect_storage_mode(conn) == "vanilla"
    # The verdict is vanilla on the license's VALUE, not its absence: a
    # server reporting any license with no extension installed is still
    # vanilla (the arm ordering, pinned by the probe's verdict below).
    offered_only = _ProbeConn(available=True, installed=False, license="apache")
    assert await detect_storage_mode(offered_only) == "vanilla"


async def test_extension_installed_without_the_license_guc_falls_back_to_compression() -> None:
    """The GUC-absent corner (a build old enough to predate it): the
    compression machinery's presence decides - ``compress_chunk`` defined
    reads as TSL-capable, absent reads as the Apache feature set."""
    tsl = _ProbeConn(available=True, installed=True, license=None, compress_chunk="compress_chunk")
    assert await detect_storage_mode(tsl) == "timescale-tsl"
    apache = _ProbeConn(available=True, installed=True, license=None, compress_chunk=None)
    assert await detect_storage_mode(apache) == "timescale-apache"


async def test_probe_carries_the_license_on_the_capability() -> None:
    """The capability probe reports the license GUC verbatim (missing_ok:
    ``None``, never an error) so callers that already probed - the deploy
    step's enable path - classify without a second round trip."""
    conn = _ProbeConn(available=True, installed=True, license="apache")
    capability = await probe_timescale_capability(conn)
    assert isinstance(capability, TimescaleCapability)
    assert capability.installed is True
    assert capability.license == "apache"
