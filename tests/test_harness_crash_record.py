"""The readiness gate's crash record: one reader, the full output.

A worker that exits before its health socket answers must surface with
its crash output, not a bare timeout — and the output must come from the
ONE reader that owns the worker's stderr. When the harness spawned the
worker without a ``log_sink``, a daemon tee thread owns the stderr pipe
and writes the durable log next to the health socket; ``communicate()``
on that handle would be a second reader on the same pipe — stealing
bytes from the tee (a corrupted durable log) while usually receiving
nothing itself (an empty crash record). These controls pin the contract
on all three stderr wirings the gate can be handed:

- tee-owned (``spawn_worker`` without ``log_sink``): the crash record is
  read from the tee's log after its EOF, and the log is complete;
- sink-owned (``log_sink``): both streams ride the caller's file and no
  pipe exists — the record is read from the file (a ``communicate()``
  here returns no stream at all; the pre-fix gate crashed on
  ``None.decode()`` instead of reporting the crash);
- raw pipes (a hand-built ``WorkerProc`` with neither record): the bare
  ``Popen`` caller's own shape — the gate reads the pipes once.

The probe child is the broken entry ``_crash_entry``: a deterministic
banner on stderr, then a nonzero exit — no DSN, no broker, no fixtures.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from taskq.testing.health import unique_health_sock_path
from tests.system_e2e._crash_entry import BANNER, BANNER_LINES, CRASH_RC
from tests.system_e2e._harness import (
    WorkerProc,
    reap,
    spawn_worker,
    wait_for_socket,
    wait_worker_ready,
)

pytestmark = pytest.mark.integration

_BAD_DSN = "postgresql://crashprobe:crashprobe@127.0.0.1:1/crashprobe"
_PROBE_SCHEMA = "crashprobe"
_ENTRY = "tests.system_e2e._crash_entry"


def _assert_full_record(message: str) -> None:
    """The record the gate owes on every wiring: the exit code and the
    full crash output — which for the probe child is its whole banner."""
    assert f"rc={CRASH_RC}" in message, message
    for line in BANNER_LINES:
        assert line in message, message


def test_tee_owned_stderr_reports_the_teed_log_and_leaves_it_complete() -> None:
    """The harness spawn's crash record: read from the tee's log (never
    from the tee's pipe), naming the log, with the log complete on disk."""
    worker = spawn_worker(_BAD_DSN, _PROBE_SCHEMA, tag="crashrec-tee", entry=_ENTRY)
    log_path = Path(worker.sock_path + ".worker.log")
    try:
        with pytest.raises(RuntimeError) as raised:
            wait_worker_ready(worker)
    finally:
        reap(worker)
    message = str(raised.value)
    _assert_full_record(message)
    assert str(log_path) in message, message  # the record names the log it read
    assert log_path.read_text() == BANNER  # the teed log is complete: every byte


def test_sink_owned_stderr_reports_the_sink_file(tmp_path: Path) -> None:
    """The log_sink spawn's crash record: both streams ride the file, no
    pipe exists, and the report reads the file it names."""
    sink = tmp_path / "worker.log"
    worker = spawn_worker(
        _BAD_DSN, _PROBE_SCHEMA, tag="crashrec-sink", entry=_ENTRY, log_sink=str(sink)
    )
    try:
        with pytest.raises(RuntimeError) as raised:
            wait_worker_ready(worker)
    finally:
        reap(worker)
    message = str(raised.value)
    _assert_full_record(message)
    assert str(sink) in message, message
    assert sink.read_text() == BANNER


def test_raw_pipe_handle_reads_its_pipes_once() -> None:
    """A handle with no tee and no sink — the bare-Popen caller's shape —
    still gets the crash record, from one communicate() on its own pipes."""
    sock_path = unique_health_sock_path("crashrec-raw")
    proc = subprocess.Popen(  # noqa: S603  # Why: fixed argv, no shell, this interpreter, project-owned module; no untrusted input.
        [sys.executable, "-m", _ENTRY],
        cwd=os.environ.get("TASKQ_REPO_ROOT", os.getcwd()),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    worker = WorkerProc(proc=proc, sock_path=sock_path)
    with pytest.raises(RuntimeError) as raised:
        wait_for_socket(sock_path, worker)
    _assert_full_record(str(raised.value))
