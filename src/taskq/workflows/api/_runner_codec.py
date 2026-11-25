"""The flow RUNNER's row-codec seam (the split of ``api/_runner``): the
jsonb envelope's boundary — every value that crosses a row (a wiring
arg, a body return, the loop's carry, the flow's input) walks these
typed helpers, decoded once, encoded jsonb-safe.

The envelope's law: a body's bare dict return never collides with the
engine's envelope keys (the ``{"value": …}`` wrapper); the Any-contract
walk is the house style ``_types.py``'s decoders use — every branch
asserts the runtime shape it consumes, and each USE of an Unknown member
carries the targeted ignore with the Why.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final, NoReturn, cast

from pydantic import BaseModel, TypeAdapter

from taskq._json import loads as _json_loads
from taskq.backend._protocol import ConnLike
from taskq.workflows.api._runner_errors import (
    WorkflowResultDetailUnavailableError,
    WorkflowRunError,
)
from taskq.workflows.api._sql_runner import PARENT_RESULTS_BY_KEY_SQL_TEMPLATE, render_sql

__all__ = ["FlowEntryShim", "LoopCarryContractError"]


@dataclass(slots=True)
class FlowEntryShim:
    """The FlowEntry shape the run-key claim needs, built from the
    compiled workflow (the typed door's runtime form). Mutable: the
    protocol's members are writable (the estate's carrier convention)."""

    name: str
    payload: dict[str, object] | str | None
    actor: str = "wf"
    queue: str = "default"
    max_attempts: int = 3
    retry_kind: str = "transient"
    trace_id: str | None = None


def next_data(iterator: Any) -> object:
    """One data-arg position's value (the payload's wf_args array is the
    wiring's recorded data, in order)."""
    return cast(object, next(iterator))


def encode_data_arg(value: object) -> object:
    """The jsonb-safe form of one wiring value (a pydantic model dumps
    through its own codec — the typed boundary; lists and dicts walk).
    Everything else is already a JSON value by the wiring's contract."""
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        items = cast(list[object], value)  # the walk's declared members
        return [encode_data_arg(v) for v in items]
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)  # the walk's declared members
        walked_map: dict[str, object] = {}
        for k, v in mapping.items():
            walked_map[str(k)] = encode_data_arg(v)
        return walked_map
    return value


def jsonable(value: object) -> object:
    """The carry/feedback's jsonb-safe form (the walk's boundary)."""
    return encode_data_arg(value)


def rehydrate_carry(declared: object, raw: object, *, committed: bool) -> object:
    """The value the body
    receives is an instance of the DECLARED initial's type once a carry
    is committed — after every resume and on the memo-replay path alike
    — and ``None`` before that (the pre-first-carry seed; the type-only
    declaration's documented initial — the only ``None`` the contract
    passes).

    The jsonb round-trip is typeless (a model dumps to a dict; the row
    reads a plain dict back), so the driver re-hydrates through the
    declared type's own validator at the one point a carry reaches a
    body. The declared initial rides the compiled definition's spec, so
    the validator is resolvable in any process that runs the body (the
    registry is the only body source, D1) — no wire-format tag is needed
    and the canonical dump stays a plain jsonb value. Rehydration is
    idempotent: an already-typed carry (the in-process ``Refine``
    feedback) passes through untouched.

    ``committed`` is the carry's PROVENANCE, supplied by the driver per
    face — the seed's gate, not a value property: the row read derives
    it from the iteration counter the atomic advance owns (the advance
    writes ``carry`` and ``iteration`` in one statement, and only the
    driver's own init/advance under the claim fence writes that pair,
    so iteration 0 ⟺ nothing has been committed TO THE ROW — a recorded
    iteration terminal the advance never reached (the crash window) is
    still a commit, which is why the memo replay below supplies ``True``
    regardless); the memo replay (the recorded feedback) and
    the in-memory ``Refine`` (the feedback the advance is about to
    write) are commits by construction. A body owns neither write and
    cannot mark its own feedback as a seed.

    * declared ``None`` — the untyped contract: the raw value passes
      (the pre-typed-loop face is unchanged).
    * the PRE-FIRST-CARRY seed (a declared type, raw ``None``,
      ``committed=False``) — nothing has been committed to the row yet
      (and no memo replay hit: a recorded terminal the advance never
      reached is a commit and overrides this face), so ``None`` passes
      through as ``None``: the type-only declaration's documented
      initial (``carry_type=`` with no model ``initial=``, the driver's
      iteration-0 seed) and a resume before any advance committed one
      to the row both land here.
    * a COMMITTED ``None`` (a declared model or dict, raw ``None``,
      ``committed=True``) — the LOUD refusal: a committed carry of no
      shape is the same silent type change this seam refuses (a body's
      ``Refine(None)`` arrives as ``NoneType`` at the next boundary).
      A JSON-native declared type keeps the untyped face — the jsonb
      boundary honors the declaration, so the ``None`` passes.
    * a pydantic model declared — ``model_validate`` (the type's own
      codec, the same validator the wiring args walk).
    * a dict SUBCLASS declared (``collections.Counter`` — the counting
      carry) — reconstructed through the class's dict constructor.
    * a JSON-native declared type (dict/list/str/int/float/bool) — the
      jsonb round-trip preserves the shape; the value passes.
    * anything else — the LOUD refusal: the jsonb boundary cannot honor
      the declaration (a tuple-declared carry would arrive a list), and
      a silent type change is the exact lie this seam exists to refuse.
    """
    if declared is None:
        return raw
    if raw is None and not committed:
        # THE PRE-FIRST-CARRY SEED (the ONLY ``None`` the contract
        # passes): the driver seeds ``carry = None`` at iteration 0 and
        # a resume before the first ``Refine`` reads the init's ``None``
        # back — no carry exists yet, so there is nothing to validate.
        # The flag is the driver's provenance record (see the docstring):
        # a COMMITTED ``None`` — a ``Refine(None)`` — skips this branch
        # and walks the law below, refusing loudly against a declared
        # model/dict exactly as any other wrong-typed carry. Passing the
        # seed through here (not at the call site) keeps the contract in
        # its ONE owner: the validator the body's carry reaches, whose
        # single caller this seam documents.
        return None
    # the declared face may arrive as the class itself (the loop's
    # ``carry_type=`` declaration — the spec's own field since the split) or as
    # an instance (the initial carry's value — the derivation's other face).
    # The class is the contract's home; the instance contributes its type.
    declared_type: type[object] = declared if isinstance(declared, type) else type(declared)
    if isinstance(raw, declared_type):
        return raw
    if issubclass(declared_type, BaseModel):
        if not isinstance(raw, dict):
            raise LoopCarryContractError(
                f"the loop's declared carry ({declared_type.__name__}) arrived "
                f"as {type(raw).__name__!r} — the jsonb value cannot be "
                "re-hydrated through the declared type's validator"
            )
        return declared_type.model_validate(raw)
    if issubclass(declared_type, dict):
        if not isinstance(raw, dict):
            raise LoopCarryContractError(
                f"the loop's declared carry ({declared_type.__name__}) arrived "
                f"as {type(raw).__name__!r} — the jsonb value cannot be "
                "re-hydrated through the declared type's validator"
            )
        # The dict-subclass constructor (collections.Counter — the
        # counting carry): the class builds from its mapping. Pyright's
        # type[dict] face has no per-subclass ctor signature; the
        # targeted ignore is the declared laundering (the constructor's
        # own contract — Counter({"acc": 3}) is the shape).
        dict_type = cast("type[dict[str, object]]", declared_type)
        return dict_type(cast("dict[str, object]", raw))  # pyright: ignore[reportCallIssue]  # Why: the type[dict] face's overload set is the base ctor's; the subclass builds from its mapping — the raw is a dict (the guard above).
    if declared_type in (dict, list, str, int, float, bool):
        return raw
    raise LoopCarryContractError(
        f"the loop's declared carry type {declared_type.__name__!r} cannot "
        "round-trip the jsonb boundary — declare a pydantic model, a dict "
        "subclass, or a JSON-native type (the carry contract: every committed "
        "carry is revalidated at each boundary — the advance, a resume, the "
        "memo replay — and the body receives the declared type; a committed "
        "None against a declared model or dict is refused loudly — None "
        "passes only as the pre-first-carry seed)"
    )


class LoopCarryContractError(WorkflowRunError):
    """The carry's declared type cannot round-trip the jsonb boundary —
    the declaration is refused LOUDLY (a silent type change is the lie
    the typed carry exists to refuse)."""


def encode_result(value: object) -> dict[str, object] | None:
    """The result envelope: the body's return rides the row's jsonb
    ``result`` (wrapped so a bare dict body return never collides with
    the engine's envelope keys). The value walks the typed boundary (a
    body may return a list of models — the map's shape)."""
    if value is None:
        return None
    return {"value": encode_data_arg(value)}


#: The runner's envelope key set (the bounded-collect reconciliation): the
#: non-truncated envelope is one key (``{"value": …}``); the
#: ``_bounded_collect``'s truncated envelope adds exactly the two honest
#: markers (the ``__truncated__`` count + the ``detail_home`` pointer).
#: A body's bare dict return never collides with either shape —
#: ``encode_result`` wraps it, so the decode sees the body's dict inside
#: the envelope's ``value``, never at the envelope's own key set.
_RESULT_ENVELOPE_KEYS: frozenset[str] = frozenset({"value", "__truncated__", "detail_home"})


def decode_result(raw: object) -> object:
    """jsonb decoded once: asyncpg returns ``str`` on un-coded
    connections — parse through the estate's seam, never per call site.
    The runner's own envelope unwraps BY ITS KNOWN KEY SET — ``value``
    alone (the pre-fix envelope) or ``value`` + the truncation markers
    (``__truncated__``, ``detail_home``; the bounded collect's shape) —
    not by ``len(decoded) == 1``: the truncated envelope is THREE keys,
    and the consumer's typed decode must see the declared ``list[...]``
    either way (the pre-fix tree unwrapped the one-key form only, and
    every consumer wired past a truncating collect received the envelope
    dict where its param declared a list — the merge regression this
    key-set door closes). The truncation face stays honest on the row:
    the markers ride the RAW jsonb, and the finalize's metadata receipt
    (``wf_collect_truncated``) carries the count for the reads that
    decide. A body's dict return never collides with the envelope keys
    (the wrap is unconditional)."""
    if raw is None:
        return None
    # The Any-contract walk (the _json seam's parse contract — the same
    # house style _types.py's decoders use): every branch asserts the
    # runtime shape it consumes, and each use of an Unknown member carries
    # the targeted ignore with the Why.
    decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    if "value" in decoded:  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk.
        keys = set(decoded)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]  # Why: the same walk — set() over an Any dict is partially unknown by construction.
        if keys <= _RESULT_ENVELOPE_KEYS:
            # either face. unwrap to the declared value — the consumer's typed
            # decode never sees the wrapper.
            return cast(object, decoded["value"])  # pyright: ignore[reportUnknownArgumentType]  # Why: the walk's boundary — the cast is the declared laundering (the envelope's key set is the runner's own).
    return cast(object, decoded)  # pyright: ignore[reportUnknownArgumentType]  # Why: the same boundary.


#: The durable NODE-KIND stamp the bounded collect's readers need: a
#: bodyless ``gather`` node's row carries ``{"wf_node_kind": "gather"}``
#: from birth (``FlowRunner._insert_static_nodes``), so a reader that has
#: no ``CompiledWorkflow`` in hand — ``ChildHandle._ok_result``, and any
#: recursive re-read at depth — still knows the join's own packer rule
#: (a gather flattens one level, a ``map_join`` never does). It rides
#: ``jobs.metadata``, NOT the ``jobs.result`` envelope: the envelope's key
#: set is ``decode_result``'s wire contract (a fourth key breaks
#: rolling-upgrade reads), while unknown metadata keys are ignored by
#: every older reader.
WF_NODE_KIND_KEY: Final[str] = "wf_node_kind"


def node_kind_from_metadata(raw: object) -> str | None:
    """The durable node-kind stamp on one row's ``metadata`` (``None``
    when the row carries no stamp — a pre-stamp in-flight row, or a node
    kind the write path never stamps). asyncpg hands jsonb back as a
    ``str`` on un-coded connections and as a decoded value on coded ones;
    both faces walk the same typed seam."""
    if raw is None:
        return None
    decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
    if not isinstance(decoded, dict):  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk.
        return None
    stamped = decoded.get(WF_NODE_KIND_KEY)  # pyright: ignore[reportUnknownMemberType,reportAttributeTypeIssue,reportUnknownVariableType]  # Why: the same walk.
    return stamped if isinstance(stamped, str) else None


async def decode_result_with_detail(
    conn: ConnLike,
    schema: str,
    *,
    node_id: object,
    raw: object,
    metadata: object,
    step_key: str,
    result_expired: bool = False,
    resolve_node_kind: Callable[[str], str | None] | None = None,
    _ancestry: frozenset[object] = frozenset(),
) -> object:
    """THE ONE DETAIL RE-READ (the read half of ``_bounded_collect``):
    the row's declared value — the FULL collect — decoded once, from the
    ``detail_home`` face the truncation marker names (the parent rows,
    through the same ordered parent read the join packed: edge order, map
    children distinct, absorbed parents excluded).

    Every workflow join-row reader goes through THIS function — the
    runner's ``_parent_results`` / ``result()`` / ``_flow_state()``, and
    the runner-less ``ChildHandle._ok_result`` — so the unwrap, the
    recursion, the gather flattening, and the fail-closed completeness
    check have ONE owner and cannot drift apart (they already had: three
    readers each decoded the bare envelope and each returned the 64 KiB
    prefix as if it were the collect).

    * a non-truncated row (``result_truncation`` is ``None``) decodes
      directly — the common case pays no query, and ``decode_result``
      alone is the whole contract there.
    * a truncated row re-reads its parents and decodes each of them
      through the SAME function: a truncated ``gather`` wrapping a
      truncated ``map_join`` (or a nested ``gather``) rebuilds at every
      depth, because each parent row may itself be a bounded summary.
    * the one-level gather flatten mirrors the write path
      (``_execute_claimed_tail``'s ``body is None`` collect: flatten when
      every parent value is a list) so the re-read hands back exactly the
      shape the join packed — a ``map_join`` over list-returning items is
      ``list[list[T]]`` and is NEVER flattened. The node kind comes from
      the durable ``metadata`` stamp first, then the caller's compiled
      graph (``resolve_node_kind``) for rows created before the stamp
      existed — a runner-less reader has no graph, and the stamp is what
      makes it correct.
    * every failure where the whole value cannot be rebuilt — parent
      rows gone, an expired NULL parent result, a count short
      of ``len(prefix) + __truncated__``, a marker that is not a positive
      count, a prefix that is not a list, or an edge cycle — is
      :class:`~taskq.workflows.api._runner_errors.WorkflowResultDetailUnavailableError`,
      never ``[]`` and never the prefix.
    """

    def refuse(reason: str, expected: int | None = None, rebuilt: int | None = None) -> NoReturn:
        raise WorkflowResultDetailUnavailableError(
            node_id=node_id, step_key=step_key, reason=reason, expected=expected, rebuilt=rebuilt
        )

    if result_expired:
        refuse(
            "the parent row's result TTL has expired and its result is NULL; "
            "the surviving row and edge cannot supply a durable collect item"
        )
    decoded = decode_result(raw)
    truncated = result_truncation(raw)
    if truncated is None:
        return decoded

    if node_id in _ancestry:
        refuse("the truncated collect's detail rows form a cycle (a node is its own ancestor)")
    if not isinstance(decoded, list):
        refuse(
            "the bounded collect's truncated envelope carries a non-list prefix "
            f"({type(decoded).__name__}) — there is no collect to rebuild"
        )
    # ``_bounded_collect`` only ever records the items it DROPPED, so a marker of
    # zero or less on a truncated shape is corruption, not a fully-spelled
    # collect: rebuilding from a lying marker is exactly the silent short read
    # this seam exists to refuse.
    if truncated <= 0:
        refuse(
            f"the bounded collect's truncation marker ({truncated}) is not a positive "
            "count — the row's summary cannot be reconciled with its detail"
        )
    prefix = cast("list[object]", decoded)
    rows = await conn.fetch(render_sql(PARENT_RESULTS_BY_KEY_SQL_TEMPLATE, schema), node_id)
    values: list[object] = []
    for row in rows:
        values.append(
            await decode_result_with_detail(
                conn,
                schema,
                node_id=row["id"],  # pyright: ignore[reportUnknownArgumentType]  # Why: the Record walk's boundary — the statement names the column.
                raw=row["result"],  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
                metadata=row["metadata"],  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
                step_key=str(row["step_key"]),  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
                result_expired=bool(row["result_expired"]),  # pyright: ignore[reportUnknownArgumentType]  # Why: same walk.
                resolve_node_kind=resolve_node_kind,
                _ancestry=_ancestry | {node_id},
            )
        )
    kind = node_kind_from_metadata(metadata) or (
        resolve_node_kind(step_key) if resolve_node_kind is not None else None
    )
    if kind == "gather" and values and all(isinstance(v, list) for v in values):
        # the gather's flatten, mirrored (the identity packer's own rule —
        # the join row's pre-truncation value was the flat list; the re-read
        # must hand the consumer the same shape).
        values = [item for v in values for item in cast("list[object]", v)]
    expected_total = len(prefix) + truncated
    if len(values) != expected_total:
        refuse(
            "the parent detail rows are missing or incomplete (the full collect's "
            "only home is gone: purged rows, a missing wf_edge row, or a partially "
            "deleted fork)",
            expected=expected_total,
            rebuilt=len(values),
        )
    return values


def result_truncation(raw: object) -> int | None:
    """The RAW row's truncation face (the marker's read): the count
    when the envelope is the bounded collect's TRUNCATED shape, ``None``
    otherwise (a None raw, a plain envelope, a body's own dict). The
    decode's unwrap drops the markers from the consumer's value — THIS
    seam is how the reads recover the count (the runner's detail
    re-read keys on it: ``detail_home`` says the full collect lives on
    the children's own result rows)."""
    if raw is None:
        return None
    decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
    if not isinstance(decoded, dict):  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk.
        return None
    count = decoded.get("__truncated__")  # pyright: ignore[reportUnknownMemberType,reportAttributeTypeIssue,reportUnknownVariableType]  # Why: the same walk.
    return count if isinstance(count, int) and not isinstance(count, bool) else None


# ── the typed boundary is complete (the typed boundary): a param's
# annotation — bare model, list, union, generic — re-validates the jsonb
# round-trip's value. The collect's dicts reached the body raw and the
# union's `match` fell to the never-arm (the doc-ingest fence's route
# failure); the walk below closes it. The adapters cache per annotation —
# the module-level bodies' annotations are immortal.

_TYPE_ADAPTERS: dict[object, TypeAdapter[object]] = {}  # pyright: ignore[reportUnknownVariableType]  # Why: TypeAdapter's generic for arbitrary annotations is partially unknown by construction; the cache is the boundary.


def type_adapter_for(param: object) -> Any:
    """The annotation's TypeAdapter, cached. The any return is the
    boundary's declared contract (TypeAdapter's generic for arbitrary
    annotations is partially unknown by construction — the cache IS the
    seam)."""
    adapter = _TYPE_ADAPTERS.get(param)
    if adapter is None:
        built: Any = TypeAdapter(param)  # pyright: ignore[reportUnknownVariableType]  # Why: TypeAdapter's generic for arbitrary annotations is partially unknown by construction; the cache is the seam.
        _TYPE_ADAPTERS[param] = built
        adapter = built
    return adapter


def coerce_arg(
    raw: Any,
    *,
    param: Any,
) -> object:
    # The any params are the walk's declared contract: the caller's
    # body_hints walk is the Any-contract's boundary (the annotations are
    # objects); the return is the typed door's product.
    """The payload codec walk: the body's declared param annotation
    re-validates the jsonb round-trip's value (the decode once, typed
    end to end). A BARE model is the fast path; a LIST/union/generic
    annotation walks the TypeAdapter. the POSITION IS the CALLER'S
    GUARD (the accept-and-ignore fix): the caller owns the
    position arithmetic (``position >= len(params)`` is its early
    return) — this walk takes the RESOLVED annotation, nothing else;
    the ``position``/``params`` kwargs it once accepted were consumed
    by nothing."""
    if param is object or param is Any:
        return raw
    if isinstance(param, type) and issubclass(param, BaseModel):
        if isinstance(raw, dict):
            return param.model_validate(raw)
        return raw  # the non-dict JSON values need no codec
    adapter = type_adapter_for(cast("object", param))
    raw_arg: Any = raw
    validated: object = adapter.validate_python(raw_arg)  # pyright: ignore[reportUnknownMemberType]  # Why: the cached boundary (the annotation walk); the Any-typed walk is the codec's declared contract.
    return validated
