"""NEGATIVE TYPE PROBES — the generic wiring mechanism (pyright 1.1.414 +
ty 0.0.85).

Run:  uv run --no-sync python tests/typeprobe/_gate.py
      (the CI `type-probes` job's single step; the corpus is checked under
      THIS directory's own pyrightconfig.json — NOT the root pyproject)

The type-mechanism round's corpus: the SHIPPED signatures carry the
probe-proven mechanism — ``step[R](body: Callable[..., Awaitable[R]]) ->
Promise[R]`` (R INFERRED from the body's declared return),
``gather[R](promises: list[Promise[R]]) -> Promise[list[R]]`` (the
element type PRESERVED — no erase to ``Promise[Any]``), and
``build[R](result: Promise[R], *residuals: Promise[Never])`` (the
residual slot refuses every real promise — the covariant handle-flow
law). Each ``MUST_ERROR(rules)`` marker names the EXACT rule-ids the
pinned checkers must emit (the gate asserts the ids, not merely "an
error" — a stray import error must not satisfy a probe).

THE TWO FACES — what the CHECKER owns vs what ``validate()`` owns (the
honest boundary, stated so it cannot be misread):

* THE CHECKER'S FACE — the HANDLE flow: the promise as a value threaded
  through the wiring. A typed consumer that declares the handle type
  (``Promise[Config]``) rejects a mismatched handle (``Promise[Report]``)
  — the contravariant-consumer rejection, live because :class:`Promise`
  is covariant. ``build``'s residual slot (``Promise[Never]``) rejects
  every real data handle — an unconsumed promise passed to the terminal
  is the checker's error. ``gather`` preserves the element type, so a
  join's mismatch is caught the same way. ``R``'s inference is checked
  by the GREEN corpus (the estate's wiring compiles clean under the
  full-tree run).
* THE VALIDATOR'S FACE — the DECODED-payload flow: the bodies take
  decoded models, not handles, so the wiring call itself is erased
  (``*args: object``) and the checker CANNOT see the edge's payload.
  The producer-Report→consumer-Config wiring site is NOT a checker
  error — ``validate()``'s ``E5-incompatible-consumer`` owns it at
  build (the probe below documents the shape; its clean line is
  deliberate). The runtime pin: ``tests/test_wf_validate_pins.py``.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from taskq.workflows import Promise, WorkflowApp, build, gather, step


class Config(BaseModel):
    key: str


class Report(BaseModel):
    ref: str


async def _produces_report(ctx: Any, params: Config) -> Report:
    return Report(ref=params.key)


async def _wants_config(ctx: Any, params: Config) -> Config:
    return params


def _handle_consumer(p: Promise[Config]) -> object:
    """A typed consumer that takes the HANDLE (the promise in a
    contravariant position) — the shape the checker CAN see."""
    return p


def _list_consumer(p: Promise[list[Config]]) -> object:
    """The JOIN's typed consumer: the handle to the collected list."""
    return p


def probe_residual_handle_is_the_error() -> None:
    """CANONICAL WRONG GRAPH (b): an unconsumed data handle passed to the
    terminal as a residual. ``build``'s residual slot is
    ``Promise[Never]`` — a produces-nothing body's slot (``-> NoReturn``)
    — and covariant ``Promise[Report]`` is not assignable to it. This is
    the checker's half of validate()'s E2-produced-never-consumed."""
    app = WorkflowApp()

    @app.workflow("probe_residual")
    def probe_residual() -> object:
        terminal = step(_produces_report, Config(key="k"), key="t")
        stray = step(_produces_report, Config(key="k"), key="s")
        return build(
            terminal, stray
        )  # MUST_ERROR(reportArgumentType, invalid-argument-type): a real promise in the Promise[Never] residual slot

    _ = app


def probe_consumer_handle_mismatch() -> None:
    """CANONICAL WRONG GRAPH (a) — the HANDLE half: a consumer declaring
    the handle type ``Promise[Config]`` handed ``Promise[Report]``. The
    contravariant-consumer rejection: covariant ``Promise`` makes the
    mismatch the checker's error (the producer's type threads to the
    consumer through the handle)."""
    app = WorkflowApp()

    @app.workflow("probe_consumer_handle")
    def probe_consumer_handle() -> object:
        produced = step(_produces_report, Config(key="k"), key="p")
        return _handle_consumer(
            produced
        )  # MUST_ERROR(reportArgumentType, invalid-argument-type): Promise[Report] is not Promise[Config]

    _ = app


def probe_gather_element_type_mismatch() -> None:
    """gather PRESERVES the element type (no erase to ``Promise[Any]``):
    a join over Report promises is ``Promise[list[Report]]``, so a
    consumer wanting ``Promise[list[Config]]`` reds — the join's element
    type is part of the handle-flow contract."""
    app = WorkflowApp()

    @app.workflow("probe_gather_elem")
    def probe_gather_elem() -> object:
        produced = step(_produces_report, Config(key="k"), key="p")
        joined = gather([produced])
        return _list_consumer(
            joined
        )  # MUST_ERROR(reportArgumentType, invalid-argument-type): Promise[list[Report]] is not Promise[list[Config]]

    _ = app


def probe_wiring_site_decoded_payload_is_the_validators_face() -> None:
    """THE HONEST GAP — deliberately NOT an asserted probe: the wiring site
    where the consumer body takes the DECODED payload (``Config``) and
    the promise carries ``Report``. The checker does NOT flag this call
    (the verbs' args are erased — bodies take decoded data, not handles,
    so the wiring-site payload compat is statically unobservable THERE);
    ``validate()``'s ``E5-incompatible-consumer`` owns it at build (the
    runtime pin: ``tests/test_wf_validate_pins.py``). This line must stay
    CLEAN under both checkers — a red here is the gate's unmarked-error
    failure, the two-faces boundary enforced."""
    app = WorkflowApp()

    @app.workflow("probe_wiring_gap")
    def probe_wiring_gap() -> object:
        produced = step(_produces_report, Config(key="k"), key="p")
        return build(
            step(_wants_config, produced, key="c")
        )  # NOT a checker error — validate()'s E5 face (the two-faces section)

    _ = app
