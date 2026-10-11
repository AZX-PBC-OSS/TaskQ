"""THE SUPERSESSION CHAIN'S PINS — the estate-cures round, cure 1.

The evidence-heads verifier's law read "the link must resolve": a live
claim marked ``superseded_by`` was judged by the HEAD link's existence
ALONE. The structural hole: a supersession chain's TARGET was never
freshness- or content-checked — the walk never happened — so a stem
could GREEN while its effective (terminal) claim was stale or a ghost,
the rot hidden one hop behind a resolving link (the round-four sin's
shape, at the registry's own door).

THE COUNTEREXAMPLE (built below, one pin per shape): stem A's live
claim is marked ``superseded_by`` a capture whose file EXISTS — the old
law's whole check — while the chain's END is a history entry recorded
on a stale head, or a ghost, or the chain never terminates at all. The
pre-fix verifier returned rc=0 on exactly this estate (the green
receipt is the cure's own capture); the fixed verifier walks the chain
to its TERMINAL entry and applies the head-sha rule THERE.

Each pin runs the verifier script as a subprocess against a synthetic
runs-dir (the rv2-13 precedent: the script is the subject, its rc is
the verdict) and asserts the conviction — and the CONTROL pin proves
the fix does not over-convict: an honest chain (resolving links, a
fresh on-disk terminal) stays GREEN.
"""

from __future__ import annotations

import json
import subprocess  # Why: the verifier's verdict is its exit code — the script IS the subject (the rv2-13 pin's own shape).
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
_VERIFIER = _REPO / "scripts" / "verify_evidence_heads.py"

#: A head sha that is STALE by construction: the repo's root commit — an
#: ancestor with the whole tree's source changes since, the off-by-one
#: rule can never call it fresh.
_STALE_SHA = subprocess.run(  # Why: fixed argv, no shell; git resolves on the dev/CI PATH exactly as the repo's own scripts invoke it.
    ["git", "rev-list", "--max-parents=0", "HEAD"],  # noqa: S607  # Why: as above.
    cwd=_REPO,
    capture_output=True,
    text=True,
    check=True,
).stdout.strip()


def _head() -> str:
    return subprocess.run(  # Why: as above — the fixed-argv git call.
        ["git", "rev-parse", "HEAD"],  # noqa: S607  # Why: as above.
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _estate(tmp_path: Path, claims: list[dict[str, str]], ghosts: tuple[str, ...] = ()) -> Path:
    """Write the synthetic registry: *claims* is the manifest's array;
    *ghosts* names capture files deliberately LEFT OFF DISK."""
    runs = tmp_path / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    files = {c["file"] for c in claims} - set(ghosts)
    for name in files:
        (runs / name).write_text(f"# capture {name}\n")
    (runs / "CLAIMS.json").write_text(json.dumps({"version": 1, "claims": claims}))
    return runs


def _verify(runs: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603  # Why: the interpreter is sys.executable, the script is the repo's own verifier; fixed argv, no shell.
        [sys.executable, str(_VERIFIER), "--runs-dir", str(runs)],
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=False,
    )


def _e(stem: str, file: str, head: str, ts: str, **kw: str) -> dict[str, str]:
    entry = {"stem": stem, "file": file, "head_sha": head, "captured_at": ts}
    entry.update(kw)
    return entry


HEAD = _head()


def test_the_stale_terminal_one_hop_reds(tmp_path: Path) -> None:
    """THE COUNTEREXAMPLE, one hop: A's live claim is superseded_by a
    capture whose file EXISTS (the old law's whole check), and the chain
    lands on a history entry recorded on a STALE head. The pre-fix
    verifier greened exactly this estate (rc=0 — the cure's green
    receipt); the fixed law convicts at the chain's END."""
    runs = _estate(
        tmp_path,
        [
            _e("drill", "drill-a.txt", HEAD, "t0", superseded_by="runs/proof.txt"),
            _e("proof", "proof.txt", _STALE_SHA, "t1"),
            _e(
                "proof", "proof-fresh.txt", HEAD, "t2"
            ),  # the stem's LIVE entry is fresh — the old law saw only this
        ],
    )
    proc = _verify(runs)
    assert proc.returncode == 1, "the stale terminal must red the verifier"
    assert "TERMINAL claim proof.txt is stale" in proc.stdout
    assert "drill" in proc.stdout  # the stem whose EFFECTIVE claim is red


def test_the_stale_terminal_two_hops_reds(tmp_path: Path) -> None:
    """THE WALK IS TO THE END: A → mid (fresh-looking, superseded) →
    terminal (stale). The conviction must name the chain's LAST entry —
    a one-hop check would have stopped at the resolving first link."""
    runs = _estate(
        tmp_path,
        [
            _e("deep-drill", "deep-a.txt", HEAD, "t0", superseded_by="runs/mid.txt"),
            _e("mid", "mid.txt", HEAD, "t1", superseded_by="runs/end.txt"),
            _e("mid", "mid-fresh.txt", HEAD, "t2"),
            _e("end", "end.txt", _STALE_SHA, "t3"),
            _e("end", "end-fresh.txt", HEAD, "t4"),
        ],
    )
    proc = _verify(runs)
    assert proc.returncode == 1, "the stale terminal TWO HOPS DEEP must red"
    assert "TERMINAL claim end.txt is stale" in proc.stdout


def test_the_ghost_terminal_reds(tmp_path: Path) -> None:
    """CONTENT AT THE END: the chain lands on a registry entry whose
    capture is NOT ON DISK. Every link resolves; the effective claim is
    a ghost. (The ghost file is built by the estate builder and then
    deliberately left off disk.)"""
    claims = [
        _e("ghost-drill", "ghost-a.txt", HEAD, "t0", superseded_by="runs/mid.txt"),
        _e("mid", "mid.txt", HEAD, "t1", superseded_by="runs/ghost.txt"),
        _e("mid", "mid-fresh.txt", HEAD, "t2"),
        _e("ghost", "ghost.txt", HEAD, "t3"),
        _e("ghost", "ghost-fresh.txt", HEAD, "t4"),
    ]
    runs = _estate(tmp_path, claims, ghosts=("ghost.txt",))
    proc = _verify(runs)
    assert proc.returncode == 1, "the chain's ghost terminal must red"
    assert "dead mid-walk" in proc.stdout


def test_the_cycling_chain_reds(tmp_path: Path) -> None:
    """NO TERMINAL, NO CLAIM: two live claims supersede each other — the
    chain re-enters itself and never terminates. A chain with no end has
    no effective claim; the pre-fix law saw two resolving links (green)."""
    runs = _estate(
        tmp_path,
        [
            _e("cyclist-a", "a.txt", HEAD, "t0", superseded_by="runs/b.txt"),
            _e("cyclist-b", "b.txt", HEAD, "t1", superseded_by="runs/a.txt"),
        ],
    )
    proc = _verify(runs)
    assert proc.returncode == 1, "the cycling chain must red"
    assert "CYCLES" in proc.stdout


def test_the_dangling_chain_reds(tmp_path: Path) -> None:
    """THE DANGLING CHAIN: the link's target file exists on disk but NO
    registry entry claims it — the chain exits the index and lands on a
    capture that does not exist to the verdict (the manifest is the
    estate's only index)."""
    runs = _estate(
        tmp_path,
        [
            _e("dangler", "d.txt", HEAD, "t0", superseded_by="runs/orphan.txt"),
        ],
    )
    (runs / "orphan.txt").write_text("# an unindexed capture\n")  # on disk, claimed by NO entry
    proc = _verify(runs)
    assert proc.returncode == 1, "the dangling chain must red"
    assert "NO registry entry claims it" in proc.stdout


def test_the_honest_chain_stays_green(tmp_path: Path) -> None:
    """THE CONTROL: a chain whose links resolve and whose TERMINAL is a
    fresh, on-disk capture stays GREEN — the fix convicts the broken
    chains, never the honest one (the over-conviction would break the
    live estate at every re-point)."""
    runs = _estate(
        tmp_path,
        [
            _e("honest", "h1.txt", HEAD, "t0", superseded_by="runs/h2.txt"),
            _e("honest-proof", "h2.txt", HEAD, "t1"),
        ],
    )
    proc = _verify(runs)
    assert proc.returncode == 0, "the honest chain must stay green:\n" + proc.stdout + proc.stderr


def test_the_stale_terminal_is_never_saved_by_its_stems_fresh_tail(tmp_path: Path) -> None:
    """THE HOLE'S EDGE: the stale terminal's OWN stem carries a fresh
    live claim — the old law saw the fresh tail and greened the estate
    while the CHAIN (pointing at the history entry behind it) proved
    nothing. The chain's walk indexes EVERY entry, history included:
    the conviction stands."""
    runs = _estate(
        tmp_path,
        [
            _e("edge", "edge-a.txt", HEAD, "t0", superseded_by="runs/old.txt"),
            _e("old", "old.txt", _STALE_SHA, "t1"),
            _e("old", "new.txt", HEAD, "t2"),
        ],
    )
    proc = _verify(runs)
    assert proc.returncode == 1, "the chain into history must red despite the fresh tail"
    assert "TERMINAL claim old.txt is stale" in proc.stdout


@pytest.mark.parametrize("ghost", [True, False])
def test_the_cited_import_terminal_is_history_by_declaration(tmp_path: Path, ghost: bool) -> None:
    """The CITED-IMPORT escape applies WHERE THE CHAIN LANDS: a terminal
    declared ``kind: CITED-IMPORT…`` is history by its own declaration —
    never re-recordable — so the chain greens through it (stale is
    honest for an imported provenance record), while a ghost terminal is
    still a ghost (a declaration cannot conjure the file)."""
    claims = [
        _e("import-drill", "i-a.txt", HEAD, "t0", superseded_by="runs/imported.txt"),
        _e(
            "imported",
            "imported.txt",
            _STALE_SHA,
            "t1",
            kind="CITED-IMPORT (the P1 spike's session evidence gone; the import IS the provenance)",
        ),
    ]
    runs = _estate(tmp_path, claims, ghosts=("imported.txt",) if ghost else ())
    proc = _verify(runs)
    if ghost:
        assert proc.returncode == 1, "the CITED-IMPORT declaration cannot conjure a ghost"
        assert "GHOST" in proc.stdout
    else:
        assert proc.returncode == 0, (
            "the CITED-IMPORT terminal is history by its own declaration:\n"
            + proc.stdout
            + proc.stderr
        )
