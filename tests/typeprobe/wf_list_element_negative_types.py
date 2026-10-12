"""NEGATIVE TYPE PROBES — the wiring's ELEMENT-TYPE seam (pyright
1.1.414 + ty 0.0.85).

Run:  uv run --no-sync python tests/typeprobe/_gate.py
      (the CI `type-probes` job's single step; the corpus is checked under
      THIS directory's own pyrightconfig.json — NOT the root pyproject)

THE E5 DRILL (the engine-faces lane's conviction): the drill wired a
``list[Word]`` producer into a ``list[str]`` consumer — BUILD-TIME
VALIDATION PASSED and the run died at RUNTIME with a pydantic
ValidationError on every attempt. The typed wiring's law (the checkers +
``validate()`` catch the mismatch BEFORE the run) had an ELEMENT-type
blind spot at the DECODED-payload seam: the LIST level was checked
(``gather``'s element preservation — the handle-flow probe in
``wf_generic_step_negative_types``), the ELEMENT level was not.

THE TWO FACES OF THE ELEMENT LAW (the honest boundary, spelled so it
cannot be misread):

* THE CHECKERS' FACE — the HANDLE flow: a consumer that declares the
  handle type ``Promise[list[str]]`` rejects ``Promise[list[Word]]`` —
  the covariant promise + the invariant list make the ELEMENT mismatch
  the checker's error, on both pinned checkers (the MUST_ERROR probe
  below, drill-typed: a MODEL's list into a SCALAR's list — the exact
  wound the drill's pydantic ValidationError showed at runtime).
* THE VALIDATOR'S FACE — the DECODED-payload flow: the bodies take
  decoded data, not handles, so the wiring call itself is erased
  (``*args: object``) and the checkers CANNOT see the edge's payload
  there. The wire-type walk was PROBED and refused honestly: a precise
  ``step`` overload for the single-promise list seam cannot convict
  while a permissive fallback overload exists — the fallback always
  matched last (the seam the fallback erases is the two-faces boundary
  of ``wf_generic_step_negative_types``'s wiring-site probe, still
  enforced). ``validate()``'s ``E5-incompatible-consumer`` element arm
  owns the decoded seam at build (the checker-independent half — the
  runtime pins: ``tests/test_wf_validate_pins.py``, the engine-faces
  pack: ``tests/test_wf_enginefaces_pins.py``).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from taskq.workflows import Promise, WorkflowApp, build, step


class Ingest(BaseModel):
    doc_id: str


class Word(BaseModel):
    text: str


async def _words_body(ctx: Any, params: Ingest) -> list[Word]:
    return [Word(text=params.doc_id)]


async def _str_list_consumer(ctx: Any, words: list[str]) -> dict[str, int]:
    return {"n": len(words)}


async def _words_list_consumer(ctx: Any, words: list[Word]) -> dict[str, int]:
    return {"n": len(words)}


def _str_join_consumer(p: Promise[list[str]]) -> object:
    """A typed consumer declaring the handle ``Promise[list[str]]`` — the
    shape the checkers CAN see (the element contract on the handle)."""
    return p


def probe_list_element_handle_mismatch() -> None:
    """THE DRILL'S ELEMENT MISMATCH, on the handle: ``Promise[list[Word]]``
    is not ``Promise[list[str]]`` — the covariant promise + the invariant
    list make the MODEL's-list-into-a-SCALAR's-list wiring the checker's
    error on both pinned checkers. This is the checker half of the law:
    the element type is part of the wiring's contract."""
    app = WorkflowApp()

    @app.workflow("probe_list_elem")
    def probe_list_elem() -> Promise[object]:
        words = step(_words_body, Ingest(doc_id="d"), key="words")
        return _str_join_consumer(
            words
        )  # MUST_ERROR(reportArgumentType, invalid-argument-type): Promise[list[Word]] is not Promise[list[str]] — the element contract

    _ = app


def probe_matching_list_element_is_the_green_face() -> None:
    """The element law's GREEN face — the element types AGREE — must stay
    clean under both checkers (the zero-false-positive budget: the law
    convicts the mismatch, never the wiring)."""
    app = WorkflowApp()

    @app.workflow("probe_list_elem_ok")
    def probe_list_elem_ok() -> Promise[object]:
        words = step(_words_body, Ingest(doc_id="d"), key="words")
        return build(step(_words_list_consumer, words, key="sink_words"))

    _ = app


def probe_wiring_site_decoded_payload_is_the_validators_face() -> None:
    """THE HONEST GAP — deliberately NOT an asserted probe: the drill's
    wiring site itself (the consumer body takes the DECODED
    ``list[str]``; the promise carries ``list[Word]``). The checkers do
    NOT flag this call — the verbs' args are erased (``*args: object``),
    and the wire-type walk cannot convict while the permissive fallback
    erases the seam (probed; see the module docstring's two-faces
    section). ``validate()``'s ``E5-incompatible-consumer`` element arm
    owns it at build — this line must stay CLEAN under both checkers (a
    red here is the gate's unmarked-error failure, the two-faces
    boundary enforced)."""
    app = WorkflowApp()

    @app.workflow("probe_wiring_gap")
    def probe_wiring_gap() -> Promise[object]:
        words = step(_words_body, Ingest(doc_id="d"), key="words")
        return build(
            step(_str_list_consumer, words, key="sink_words")
        )  # NOT a checker error — validate()'s E5 element face (the two-faces section)

    _ = app
