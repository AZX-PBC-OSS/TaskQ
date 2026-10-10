"""The claims registry (the measurement estate's ONE index) — the
run-scoped captures' manifest, ``.measurements/runs/CLAIMS.json``.

THE MANIFEST LAW: every capture the head-stamp verifier judges is
REGISTERED HERE — one JSON array of ``{stem, file, head_sha,
captured_at}`` entries in append order — and the MACHINERY (the writers
below, ``scripts/check_wf_coverage.py``, and the verifier
``scripts/verify_evidence_heads.py``) reads THE MANIFEST, never the
filenames. The convicted shape this replaces: the verifier grouped the
estate by PARSING each filename's run-scoped tail (the ``_stem`` /
``_run_order`` heuristics) — a name-format law the machines enforced on
the humans' own files. A name is for HUMANS; the registry is for the
machines: a capture not in the manifest does not exist to the verdict,
and a filename convention can never again BE the claim (the
sha-in-the-name one-off stems the old parser split wrong are exactly the
rot this kills).

The writers APPEND one entry per capture (atomically: an exclusive
``flock`` over a read-modify-write ending in ``os.replace`` — concurrent
``pytest -n`` workers each land their own entry, none torn, none lost).
The files stay the captures; the manifest is the index; the filename
conventions remain for the humans browsing the estate."""

from __future__ import annotations

import os
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Final, cast

import orjson

from taskq._json import loads

REPO = Path(__file__).resolve().parent.parent.parent.parent
RUNS = REPO / ".measurements" / "runs"
MANIFEST = RUNS / "CLAIMS.json"

#: The manifest's schema version (bump on a shape change; the verifier
#: refuses a manifest it cannot read rather than guessing one).
VERSION: Final[int] = 1


@contextmanager
def _locked() -> Generator[None]:
    """The exclusive registry lock (the concurrent writers' mutex)."""
    import fcntl

    RUNS.mkdir(parents=True, exist_ok=True)
    lock_path = RUNS / "CLAIMS.lock"
    with lock_path.open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _read() -> dict[str, object]:
    if not MANIFEST.is_file():
        return {"version": VERSION, "claims": []}
    try:
        data: object = loads(MANIFEST.read_bytes())
    except (ValueError, OSError):
        return {"version": VERSION, "claims": []}
    if not isinstance(data, dict):
        return {"version": VERSION, "claims": []}
    return cast("dict[str, object]", data)


def _write(data: dict[str, object]) -> None:
    tmp = MANIFEST.with_suffix(".json.tmp")
    # OPT_INDENT_2: the registry is machine-read but human-audited — the
    # diff of an append is one block, reviewable in place.
    tmp.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2) + b"\n")
    os.replace(tmp, MANIFEST)


def record_claim(stem: str, file: str, head_sha: str, captured_at: str | None = None) -> None:
    """Append ONE claim entry (atomic under the registry lock): the
    capture's evidence-kind *stem*, its filename under ``runs/``, the
    head the capture was measured on, and the wall-clock instant."""
    entry = {
        "stem": stem,
        "file": file,
        "head_sha": head_sha,
        "captured_at": captured_at or time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    with _locked():
        data = _read()
        claims = data.get("claims")
        if not isinstance(claims, list):  # pragma: no cover - a corrupt registry is rebuilt below
            claims = []
        claims.append(entry)
        _write({"version": VERSION, "claims": claims})


def load_claims(runs_dir: Path | None = None) -> list[dict[str, str]]:
    """The registry's entries, in append order (the verifier's input).
    A missing or unreadable manifest is the EMPTY registry — the caller
    (the verifier) treats empty as the estate having no claims."""
    manifest = (runs_dir / "CLAIMS.json") if runs_dir is not None else MANIFEST
    try:
        data: object = loads(manifest.read_bytes())
    except (ValueError, OSError):
        return []
    if not isinstance(data, dict):
        return []
    record: dict[str, object] = cast("dict[str, object]", data)
    claims: object = record.get("claims")
    if not isinstance(claims, list):
        return []
    return [
        cast("dict[str, str]", c)  # the registry's entries are flat str→str records
        for c in cast("list[object]", claims)
        if isinstance(c, dict)
    ]
