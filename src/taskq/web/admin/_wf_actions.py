"""The workflow-run explorer's MACHINE routes: the node panel API,
the SSE node_state stream (the revisioned-snapshot replay), and the
typed actions (deliver / resolve / abandon / cancel — each guarded, each
audited).

The pages live in ``workflows``; the split is the read/write concern
boundary (the same shape the jobs page + backend routes keep).

The stream (the replay contract, pinned): every frame is a revisioned
full snapshot; the rows are the durable ring — a reconnect replays by
construction (the next snapshot carries the whole state), and the
seq-cursor drops stale frames client-side. A killed connection cannot
strand the page on a stale render, and a memory ring would be a second
copy of a ledger that already exists. The session law (the
same re-check ``/sse/{topic}`` carries): the stream re-invokes the
host's ``session_verifier`` before the first frame and once per poll
iteration — a session revoked mid-stream ends the stream at the next
tick, fail-closed on any verifier error or a check that outlives
``SESSION_RECHECK_TIMEOUT_SECS``. The subscribe law: an unknown run id is a 404 at subscribe — the
estate's own SSE convention (``/sse/{topic}`` refuses an unknown topic
at subscribe; the empty-snapshot-forever stream was an existence oracle
behind the auth gate and a wasted poll task per probe). A run that
exists but whose node rows are not inserted yet still streams (the
generator's not-started state).

The typed door: deliver + resolve validate through the bound
``TypedGate``s of the WorkflowApp the host mounted (``create_router(
workflow_app=...)``). Without one the endpoints answer ``501`` with the
named reason — no untyped deliver surface ships. The audited refusal:
the gate's 422 is an operator action — the refusal writes
its own ``admin_audit`` row (``refused`` + the reason) before the
HTTPException propagates, so an attacker's shape-probing lands in the
trail the page claims covers every Resolve/deliver; the hold survives.

The audit: every resolve/deliver/cancel writes its ``admin_audit``
row (principal + reason + run/node) — "who approved this" is a row, not
a log line. The resolve/deliver rides HitlClient's own same-tx row; the
cancel rides the engine cascade's row; the audit entries carry the
operator principal the auth dependency captured.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import datetime
from typing import Annotated, Any, Final, cast

import asyncpg
import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import RedirectResponse, StreamingResponse
from tors import truncate_to_bounds

from taskq._json import dumps as _json_dumps
from taskq._json import dumps_jsonb_str as _dumps_jsonb_str
from taskq._json import dumps_str as _json_dumps_str
from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.settings import TaskQSettings
from taskq.web._pool import BoundedPool
from taskq.web._sse_limit import acquire_sse_slot, release_after, session_gate
from taskq.web.admin._factory import (
    get_admin_pool,
    get_base_path,
    get_principal,
    get_schema,
    get_session_verifier,
    get_settings,
    get_workflow_app,
    require_actions_enabled,
    validate_csrf,
)
from taskq.web.admin._wf_rows import (
    NOT_INSTALLED,
    RunView,
    fetch_run_view,
    run_state_json,
)

logger = structlog.get_logger("taskq.web.admin.wf_actions")

#: The refusal banner keys the redirect-carrying action routes hand back
#: (the closed vocabulary the detail page's ``REFUSAL_BANNERS`` maps to a
#: copy — the refused-op contract: the operator lands on the defined
#: refused state, never a 400 wall). Defined here (the routes' home) so
#: a route and its banner copy cannot drift apart silently.
CANCEL_NOT_APPLIED_BANNER: Final[str] = "cancel-not-applied"
RERUN_NOT_APPLIED_BANNER: Final[str] = "rerun-not-applied"
RERUN_REFUSED_BANNER: Final[str] = "rerun-refused"
#: The deadlock's named banner: the retry faces catch the
#: same named pair the cancel/resolve CLI verbs carry and answer the
#: audited refusal + this banner (the race's loser and the remedy
#: named).
RERUN_RACED_BANNER: Final[str] = "rerun-raced"
#: The deadlock's refusal text (the named pair's shared copy): the audit
#: row records these words and the CLI's ``flows retry`` echoes the same
#: refusal.
DEADLOCK_REFUSAL: Final[str] = (
    "the run's writers raced and PG resolved the deadlock in another "
    "writer's favour — the retry is being applied OR the run cancelled "
    "by the winner; re-check the run's settled state"
)
#: The writer-race's honest-loser pair (the admin JSON routes' deadlock
#: arm): a ``DeadlockDetectedError``/``SerializationError`` out of the
#: client's own transaction is PG killing one writer of a raced pair —
#: the route answers the refused verdict (the CLI verbs' named refusal is
#: the wording model), never a 500.
RACED_WRITER_ERRORS: Final[tuple[type[Exception], ...]] = (
    asyncpg.DeadlockDetectedError,
    asyncpg.SerializationError,
)
#: The partial rerun's key: the run-rerun composition tracks the
#: applied/refused counts, and a refusal landing mid-composition carries
#: them — ``rerun-partial:<rows>:<keys>:<total>`` (the rows
#: count leads) — the detail page renders
#: the counts (the half-rerun's honest banner; a bare ``rerun-refused``
#: would imply nothing moved).
RERUN_PARTIAL_BANNER: Final[str] = "rerun-partial"


def rerun_partial_banner(rows: int, keys: int, total: int) -> str:
    """The partial rerun's redirect param value (the counts ride the
    key — the page's ``_refusal_banner`` parses the shape back). The
    ROWS count leads (a map key's grant re-arms
    several rows — the key count alone under-counted the moved rows)."""
    return f"{RERUN_PARTIAL_BANNER}:{rows}:{keys}:{total}"


#: The SSE frame cadence (seconds) — the poll is the transport's pace;
#: the snapshot-per-frame shape is the replay contract's body.
_STREAM_POLL_S = 1.0

_STREAM_KEEPALIVE_S = 30.0

#: The SSE revision's source (the two ledgers' max clocks — the row
#: ids are uuid7, and max(uuid) is not a PG function; the clocks are
#: monotonic-enough revision facts for the emit decision, and the
#: frame's own seq carries the ordering): a composite string, one query.
_RUN_REVISION_SQL = (
    'SELECT COALESCE((SELECT max(l.updated_at)::text FROM "{schema}".wf_step_ledger l '
    "  WHERE l.flow_id = $1), 'none') || ':' || "
    'COALESCE((SELECT max(s.created_at)::text FROM "{schema}".wf_signals s '
    "  WHERE s.workflow_id = $1), 'none') || ':' || "
    'COALESCE((SELECT max(n.finished_at)::text FROM "{schema}".jobs n '
    "  WHERE (n.metadata->>'flow_id')::uuid = $1 AND n.metadata ? 'flow_id'), 'none') "
    "AS revision"
)

#: The subscribe probe: one indexed root-row read decides
#: the stream's 404-before-first-frame (the existence oracle's refusal).
_RUN_EXISTS_SQL = "SELECT 1 FROM \"{schema}\".jobs WHERE id = $1 AND step_key = '__flow__'"

#: The node detail panel's read (the recorded failure-down: status header, attempts,
#: the trace id, captured error, one upstream hop — the causal chain
#: via parent_id). Map children share one step key (``<src>.item``), so
#: the panel addresses a child by (step, map_index): the bare read
#: lands on the first child in map order (deterministic), and the
#: ``map_index=`` query param selects the row itself — the run page's
#: observation surface (``watch this item's attempt go 1 → 2``) is
#: (step, map_index)-addressable, not step-key-arbitrary.
_NODE_PANEL_SQL = (
    "SELECT id, step_key, status, attempt, max_attempts, retry_kind, map_index, "
    "trace_id, error_class, error_message, error_traceback, "
    "metadata->>'error' AS captured_error, parent_id, "
    "metadata->'wf_cache_hit' AS cache_hit, "
    "payload, result, "
    "created_at, started_at, finished_at "
    'FROM "{schema}".jobs WHERE step_key = $1 '
    "AND (metadata->>'flow_id')::uuid = $2 AND step_key <> '__flow__' "
    "ORDER BY map_index LIMIT 1"
)

_NODE_PANEL_CHILD_SQL = (
    "SELECT id, step_key, status, attempt, max_attempts, retry_kind, map_index, "
    "trace_id, error_class, error_message, error_traceback, "
    "metadata->>'error' AS captured_error, parent_id, "
    "metadata->'wf_cache_hit' AS cache_hit, "
    "payload, result, "
    "created_at, started_at, finished_at "
    'FROM "{schema}".jobs WHERE step_key = $1 '
    "AND (metadata->>'flow_id')::uuid = $2 AND map_index = $3 "
    "AND step_key <> '__flow__'"
)

#: The map's children census (the panel's addressing surface): one row
#: per child, in map order — the operator clicks through to a child.
#: (The statement lives below, beside the render cap it serves.)

_NODE_PARENT_SQL = (
    'SELECT id, step_key, status, error_class FROM "{schema}".jobs '
    'WHERE id = (SELECT parent_id FROM "{schema}".jobs WHERE id = $1)'
)

_NODE_LEDGER_SQL = (
    "SELECT step_key, attempt, status, error_class, error_message, created_at "
    'FROM "{schema}".wf_step_ledger WHERE job_id = $1 ORDER BY id'
)

#: The node I/O panel's capture read: the latest failed attempt's
#: failure IO-capture with a capture on it (a later attempt's
#: success must not hide the failed attempt's capture: the diagnostic's
#: whole point is the failure's input + error), falling back to the
#: latest terminal ledger row when no failed row carries one. The panel
#: labels which attempt's capture it shows.
_NODE_IO_CAPTURE_FAILED_SQL = (
    'SELECT attempt, status, capture FROM "{schema}".wf_step_ledger '
    "WHERE job_id = $1 AND status = 'failed' AND capture IS NOT NULL "
    "ORDER BY attempt DESC LIMIT 1"
)

_NODE_IO_CAPTURE_SQL = (
    'SELECT attempt, status, capture FROM "{schema}".wf_step_ledger '
    "WHERE job_id = $1 AND status IN ('succeeded','failed') "
    "ORDER BY attempt DESC LIMIT 1"
)

#: The panel's children render cap (bounded render): a 10k
#: fan-out's census ships the first cap rows + the total — the "N more"
#: arm names the rest; the rows stay one indexed query away (the child
#: read is (step, map_index)-addressable).
_PANEL_CHILDREN_CAP: Final[int] = 200

#: The children census read: the cap bounds the rows; ``count(*) over ()``
#: carries the total in the same statement (one read, no second count).
_NODE_CHILDREN_SQL = (
    "SELECT map_index, status, attempt, max_attempts, error_class, "
    "count(*) OVER () AS total "
    'FROM "{schema}".jobs WHERE step_key = $1 '
    "AND (metadata->>'flow_id')::uuid = $2 AND step_key <> '__flow__' "
    "ORDER BY map_index LIMIT $3"
)

#: The root-row status read (the pruned-history arm's discriminator):
#: a failed node with an empty step history on a terminal run is
#: retention's prune, never a policy suppression.
_ROOT_STATUS_SQL = "SELECT status FROM \"{schema}\".jobs WHERE id = $1 AND step_key = '__flow__'"


def _cache_hit_of(raw: object) -> dict[str, object] | None:
    """The wf_cache_hit jsonb's decode (str or dict — asyncpg's bare ->
    lands a string; a typed path a dict). A corrupted row reads None:
    the receipt is provenance, never load-bearing. The casts are the
    jsonb boundary's declared contract (RFC 8259: the wire's object keys
    are strings) — the decode's shape, never an inferred Unknown."""
    if isinstance(raw, dict):
        return cast("dict[str, object]", raw)
    if isinstance(raw, str):
        try:
            decoded: object = _json_loads(raw)
        except ValueError:
            return None
        return cast("dict[str, object]", decoded) if isinstance(decoded, dict) else None
    return None


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


#: The panel's display cap (chars): error_message / error_traceback /
#: metadata->>'error' render bounded — the full detail stays in the rows
#: (the recorded failure-down fetch reads the same rows; the cap is the display's:
#: engine-written rows carry the write-side caps,
#: but a row written by any other path — or a hostile DB write — served
#: unbounded through this endpoint, a 15MB response from a foreign row).
_PANEL_FIELD_CAP_CHARS: Final[int] = 10_000
_PANEL_SUFFIX_RESERVE: Final[int] = 80


def _bound_for_panel(text: str | None) -> str | None:
    """The read-side bound (the display-capped delivery): an over-cap
    field truncates WITH the dropped-character count named — the same
    honesty the capture's ``__truncated__`` marker and the CLI's event
    lines keep. ``truncate_to_bounds`` is grapheme-safe (tors, the house
    call); the row itself is never touched."""
    if text is None or len(text) <= _PANEL_FIELD_CAP_CHARS:
        return text
    bound = _PANEL_FIELD_CAP_CHARS - _PANEL_SUFFIX_RESERVE
    remaining = len(text) - bound
    suffix = f"… [truncated: +{remaining} characters stay in the row]"
    return truncate_to_bounds(text, bound) + suffix


def _panel_truncated(texts: list[str | None]) -> bool:
    """Whether any of the panel's bounded fields actually truncated (the
    'truncated' marker's boolean face — the JS can render the honesty
    without parsing the suffix)."""
    return any(t is not None and len(t) > _PANEL_FIELD_CAP_CHARS for t in texts)


#: The walk budget (the bounded-display law):
#: the result face's redaction walk runs on a pre-trimmed projection —
#: the doc's depth, node count, and served size each budgeted
#: before the walk, never a multi-MB walk per drill-down. The size
#: budget counts the served size — keys, structure, and values (a
#: values-only count once let a key-heavy doc's keys and
#: JSON syntax ride free — the served text ran ~46k for a claimed
#: 1,023, and the panel's own truncation arm fired on top of the
#: walk's). Every node carries a flat syntax frame (the quotes, colon,
#: comma, and braces its JSON render needs) beside its content's
#: length — the json-serialized length's honest proxy. Escape
#: inflation can still render longer (a kept head's escaped bytes);
#: the panel's own cap remains the backstop for that, never the first
#: bound a budgeted doc meets. The budget sits under the display cap
#: with room for the masks (the served text stays post-redaction —
#: the honesty law).
_WALK_DEPTH_BUDGET: Final[int] = 32
_WALK_NODE_BUDGET: Final[int] = 2_048
_WALK_CHAR_BUDGET: Final[int] = 8_000
_WALK_PRUNED_MARKER: Final[str] = "…walk budget exceeded — the rest is not shown…"
#: The string render's own quote pair — a kept string's served length
#: is its characters plus the two quotes it renders with.
_WALK_STRING_QUOTES: Final[int] = 2
#: The JSON punctuation a node's own render can carry (quotes, colon,
#: comma, braces) — the flat per-node charge the served-size accounting
#: adds beside each node's content length.
_WALK_NODE_SYNTAX_CHARS: Final[int] = 4
#: The worst cut's own served render (the marker riding a dict entry's
#: key and value, quoted and coloned, plus the nodes' syntax frames) —
#: the char budget reserves it so a cut always lands the census at or
#: under the budget: the census is the served size's honest count,
#: never an overshoot itself.
_WALK_CUT_RESERVE: Final[int] = 2 * len(_WALK_PRUNED_MARKER) + 4 * _WALK_NODE_SYNTAX_CHARS


def _bounded_io_projection(doc: object) -> tuple[object, str | None, dict[str, int]]:
    """The display walk's pre-trim: project the
    doc into the walk budget before the redaction walk — depth, node
    count, and served size each capped, every cut site carrying
    the named marker (never a silent discard). Returns (the projected
    doc, the named size arm — None when the whole doc fit — and the
    walk's own census: the nodes and served characters actually
    processed).

    The order is the honesty's own: the trim runs before the redaction
    (a multi-MB doc never pays the multi-MB walk), and the redaction
    still runs on everything served (the projected doc's strings go
    through the same chain — the served text stays post-redaction). A
    secret cut by the trim is absent entirely (never served raw); a
    secret inside the kept band is masked by the chain.

    The size budget's unit is the served text (the values-only count
    under-counted): keys, structure, and values all count —
    the census is the json-serialized length's honest proxy, and the
    note's number is the same accounting (a key-heavy doc's keys can no
    longer ride free past the budget)."""
    state = {"nodes": 0, "chars": 0, "pruned": 0, "done": 0}

    def _budget_out() -> bool:
        # The first cut ends the walk: once any site is trimmed, every
        # further node is the marker — a long list of cheap strings past
        # the budget must not earn one marker per item (the markers
        # would out-grow the budget they bound; the red-measure: 300
        # marker copies ballooned the "bounded" text to 17k). The char
        # check runs against the budget less the cut's own render
        # reserve — a cut must land the census at/under the budget.
        return (
            bool(state["done"])
            or state["nodes"] > _WALK_NODE_BUDGET
            or state["chars"] > _WALK_CHAR_BUDGET - _WALK_CUT_RESERVE
        )

    def _mark() -> str:
        state["pruned"] += 1
        state["done"] = 1
        # The marker's own content counts (it is served text).
        state["chars"] += len(_WALK_PRUNED_MARKER)
        return _WALK_PRUNED_MARKER

    def _marker_key(out: dict[object, object]) -> str:
        # The collision-safe marker: a doc whose own
        # entry's key equals the marker string must not be overwritten
        # by the walk's prune marker — the ordinal suffix disambiguates
        # until unique (the masked-key collision law's own shape), the
        # legit entry survives beside the marker.
        marker_key = _WALK_PRUNED_MARKER
        ordinal = 2
        while marker_key in out:
            marker_key = f"{_WALK_PRUNED_MARKER}[{ordinal}]"
            ordinal += 1
        return marker_key

    def _walk(value: object, depth: int) -> object:
        state["nodes"] += 1
        # The syntax frame: every node's render carries punctuation
        # (quotes, colon, comma, braces) — the flat charge that makes
        # the count a proxy for the served size (keys and
        # structure charge too).
        state["chars"] += _WALK_NODE_SYNTAX_CHARS
        if depth > _WALK_DEPTH_BUDGET or _budget_out():
            return _mark()
        if isinstance(value, str):
            # The string's served render: the pair of quotes rides the
            # count — the proxy counts the text the panel carries, not
            # the bare value.
            rendered = len(value) + _WALK_STRING_QUOTES
            remaining = _WALK_CHAR_BUDGET - _WALK_CUT_RESERVE - state["chars"]
            if rendered > remaining:
                # The cut is named at its own site (the same marker
                # _bound_for_panel's suffix carries): the kept head rides
                # with the marker — a partial value never reads as whole,
                # and a token cut mid-body carries no secret (the body is
                # the cut part; the head still meets the mask below). The
                # head is sized so the census lands at/under the budget.
                head = value[: max(remaining - _WALK_STRING_QUOTES, 0)]
                state["chars"] += len(head) + _WALK_STRING_QUOTES
                _mark()
                return head + _WALK_PRUNED_MARKER
            state["chars"] += rendered
            return value
        if isinstance(value, dict):
            out: dict[object, object] = {}
            # The cast is the jsonb boundary's declared contract (RFC
            # 8259: the wire's object keys are strings) — the same face
            # _redact_io_doc's walk declares.
            for k, v in cast("dict[str, object]", value).items():
                state["nodes"] += 1
                # The key's served render: the key node's syntax frame
                # plus the key's own characters (keys charge too).
                state["chars"] += _WALK_NODE_SYNTAX_CHARS + len(k)
                if _budget_out():
                    # The cut replaces this entry: the marker rides as
                    # the key (collision-safe, above) and the value —
                    # both contents charge (the frames already have).
                    state["chars"] += len(_WALK_PRUNED_MARKER)
                    out[_marker_key(out)] = _mark()
                    return out
                out[k] = _walk(v, depth + 1)
            return out
        if isinstance(value, list):
            out_list: list[object] = []
            for v in cast("list[object]", value):
                if _budget_out():
                    # The cut replaces this element: the marker's node
                    # (frame + content) stands where the element would.
                    state["nodes"] += 1
                    state["chars"] += _WALK_NODE_SYNTAX_CHARS
                    out_list.append(_mark())
                    return out_list
                out_list.append(_walk(v, depth + 1))
            return out_list
        # The scalar leaf: the jsonb boundary's own scalar kinds — the
        # exact render length (cheap; no recursion, no big materialize).
        if value is None or isinstance(value, bool | int | float):
            state["chars"] += len(_json_dumps_str(value))
        return value

    projected = _walk(doc, 0)
    if state["pruned"] == 0:
        return projected, None, {"nodes": state["nodes"], "chars": state["chars"]}
    note = (
        f"the result exceeded the panel's walk budget — showing the first "
        f"{state['chars']} characters of the SERVED doc (keys, structure, "
        f"and values counted; {state['nodes']} nodes walked, "
        f"{state['pruned']} cut sites)"
    )
    return projected, note, {"nodes": state["nodes"], "chars": state["chars"]}


def _jsonb_doc(raw: object) -> object:
    """The jsonb column's decode at the panel boundary (asyncpg hands a
    STR for an untyped jsonb expression; a typed path a dict). A
    corrupted row reads None — the I/O view is a display, never
    load-bearing."""
    if isinstance(raw, str):
        try:
            return _json_loads(raw)
        except ValueError:
            return None
    return raw


#: The node payload's keys that are the wired input (the runner's own
#: envelope vocabulary — api/_sql_runner's INPUT_KEY/WF_ARGS_KEY/ITEM_KEY).
#: Every other payload key is engine bookkeeping and stays off the display.
_WF_PAYLOAD_INPUT_KEYS: Final[frozenset[str]] = frozenset({"wf_input", "wf_args", "wf_item"})


def _redact_io_doc(doc: object) -> object:
    """The I/O doc's display redaction (the same symmetry the capture
    chain runs): the full
    chain the input face runs — ``mask_credentials`` (the vetted chain,
    the tors pre-pass included) → the hold-context's token-head pass —
    over every string the doc carries: values and key names both (a
    ``{"sk-…": …}`` key masks like the value it held). Deep nesting and
    list elements walk. Over-redaction is the safe failure: the row
    keeps the truth, the panel shows the masked form.

    The masked-key collision law: two keys
    that mask to the same ``***`` must not overwrite each other (a
    dict keyed on the masked name keeps only the last) — colliding
    masked keys are disambiguated ``***[2]``, ``***[3]``, … until
    unique: every masked entry survives, no raw key byte rides, and
    the ordinal keeps the count on the record.

    The private passes are imported for the same reason the capture
    pipeline composes them (a copy could drift from the masks the
    composition pin holds): this is the chain, not a re-derivation."""
    from taskq.obs._redact_exc import mask_credentials  # pyright: ignore[reportPrivateUsage]
    from taskq.workflows._capture import _mask_hold_token  # pyright: ignore[reportPrivateUsage]

    def _mask_str(text: str) -> str:
        return _mask_hold_token(mask_credentials(text))

    def _walk(value: object) -> object:
        if isinstance(value, str):
            return _mask_str(value)
        if isinstance(value, dict):
            # The cast is the jsonb boundary's declared contract (RFC
            # 8259: the wire's object keys are strings) — the decode's
            # shape, never an inferred Unknown.
            walked: dict[object, object] = {}
            for k, v in cast("dict[str, object]", value).items():
                masked_key = _mask_str(k)
                # The collision (5b): the second key masking to the same
                # form gets the ordinal suffix — no entry is silently
                # dropped, no raw key is exposed by the disambiguation.
                unique = masked_key
                ordinal = 2
                while unique in walked:
                    unique = f"{masked_key}[{ordinal}]"
                    ordinal += 1
                walked[unique] = _walk(v)
            return walked
        if isinstance(value, list):
            return [_walk(v) for v in cast("list[object]", value)]
        return value

    return _walk(doc)


def _node_io_view(
    node: asyncpg.Record,
    capture_row: asyncpg.Record | None,
    *,
    capture_pruned: bool = False,
) -> dict[str, object]:
    """The node I/O view (the panel's input/output faces — the engine's
    own display disciplines, never a raw dump of what a policy
    suppressed):

    * ``input`` — the node's wired args (the entry input ``wf_input``,
      the wiring's recorded data args ``wf_args``, the map child's item
      ``wf_item``), read off the row's payload and redacted through the
      engine's own pipeline at display (``_redact_io_doc``: the vetted
      chain → the token-head pass, values and key names both — exactly
      the hold-context discipline; the rows-alone law keeps the
      workflow's own ``redact=`` hook unresolvable here, and
      over-redaction is the safe failure);
    * ``result`` — the terminal node's result envelope, decoded and
      unwrapped the read-side display's own way (``{"value": x}`` alone
      → ``x``, the shape read_map_aggregate decodes), through the same
      full chain the input face runs (the token-head
      pass included — the chain-only face once rendered bare ``sk-…``
      heads raw), bounded, with the ``truncated`` flag computed
      post-redaction (the flag describes the text served);
    *   ``capture`` — the failure IO-capture as the engine wrote it,
        then through the same display-time chain the other faces run
        (the write's own mask — a pre-chain-
        release write or a hostile row — is not trusted at display; the
        panel adds no unmasked byte and only bounds the display),
        labeled with which attempt's capture it shows
        (``capture_attempt`` / ``capture_status`` — the latest
        failed attempt's capture preferred, so a later success's NULL
        capture cannot hide it);
    * ``capture_suppressed`` — a FAILED node whose capture policy
      captured nothing: the defined state, named as such — the panel
      never substitutes raw rows for a suppressed capture;
    * ``capture_pruned`` — a failed node whose step history the
      retention arms deleted (the ledger read finds no rows for an
      attempted node on a terminal run): the honest arm — the
      capture was not policy-suppressed, the flow history is gone."""
    io: dict[str, object] = {
        "input": None,
        "result": None,
        "capture": None,
        "capture_suppressed": node["status"] == "failed",
        "capture_pruned": False,
    }

    payload = _jsonb_doc(node["payload"])
    if isinstance(payload, dict):
        wired = {
            k: v
            for k, v in cast("dict[str, object]", payload).items()
            if k in _WF_PAYLOAD_INPUT_KEYS
        }
        if wired:
            io["input"] = _redact_io_doc(wired)

    result = _jsonb_doc(node["result"])
    if result is not None and node["status"] == "succeeded":
        # The result face is the success path's (the output's "decoded
        # result" arm): a failed node's output is the capture below —
        # the engine writes no result on the failure path, and an empty
        # envelope rendered as one would be a lie of shape.
        # The envelope unwrap (the read-side display's own shape): a lone
        # ``{"value": …}`` is the runner's wrapper, not the body's return.
        unwrapped: object = result
        if isinstance(result, dict) and set(cast("dict[str, object]", result)) == {"value"}:
            unwrapped = cast("dict[str, object]", result)["value"]
        # The bounded walk: the display walks a
        # pre-trimmed projection (depth/width/chars budgeted before the
        # walk — a multi-MB row is a bounded walk, never a multi-MB
        # dump), then the full chain (the token-head pass included, key
        # names masked) runs on what is served; the text and its
        # truncated flag both describe the post-redaction bytes. The trim is
        # never silent: the named size arm rides the
        # face when the projection cut.
        projected, walk_note, _walk_stats = _bounded_io_projection(unwrapped)
        text = _json_dumps_str(_redact_io_doc(projected))
        face: dict[str, object] = {
            "text": _bound_for_panel(text),
            "truncated": len(text) > _PANEL_FIELD_CAP_CHARS,
        }
        if walk_note is not None:
            # The named size arm: the walk
            # budget's cut is on the record — the served size's honest
            # count ("the first N characters of the served doc") —
            # never a silent discard.
            face["walk_note"] = walk_note
        io["result"] = face

    if capture_row is not None:
        capture = _jsonb_doc(capture_row["capture"])
        if isinstance(capture, dict):
            # The capture face's display-time mask (the symmetry's own
            # logic): the input/result
            # faces re-run the chain at display (the hostile-row
            # defense); the capture face trusts the write no longer —
            # the same chain runs here (a pre-chain-release write's raw
            # canary renders masked, never raw). The cast is the jsonb
            # boundary's declared contract (the decode's shape, never an
            # inferred Unknown).
            cap_map = cast(
                "dict[str, object]",
                _redact_io_doc(cast("dict[str, object]", capture)),
            )
            view: dict[str, object] = {}
            for key in ("input", "error"):
                value = cap_map.get(key)
                if isinstance(value, str):
                    view[key] = _bound_for_panel(value)
            marker = cap_map.get("__truncated__")
            if marker is not None:
                view["__truncated__"] = marker
            if view:
                io["capture"] = view
                io["capture_suppressed"] = False
                # The capture's label: which attempt's capture the
                # panel shows — the failed attempt the read preferred.
                io["capture_attempt"] = capture_row["attempt"]
                io["capture_status"] = capture_row["status"]
    if capture_pruned:
        # The pruned arm wins: the history is gone — naming a
        # policy suppression here would be the retention lie.
        io["capture_pruned"] = True
        io["capture_suppressed"] = False
    return io


async def _audit_rerun_refusal(
    pool: BoundedPool,
    *,
    schema: str,
    principal: Any,
    action: str,
    target_type: str,
    target_id: str,
    run_id: str,
    reason: str | None,
    refusal: str,
    applied: int,
    total: int,
    rows: int = 0,
) -> None:
    """The audited rerun refusal (the audited-refusal law at the rerun
    door): a refused rerun — grantless, cancelled, mid-composition, or
    the raced loser — writes its own ``admin_audit`` row (``refused`` +
    the counts) before the redirect lands. The refusal moves no rows of
    its own, so the row stands alone (the same-tx discipline's
    degenerate case), written with the safe recorder: a bookkeeping row
    the trail missed is the exact invisible-probe failure mode, but the
    redirect's refusal state must not fake-fail on a bookkeeping blip.
    ``applied``/``total`` are the composition's key counts and ``rows``
    the rows the granted keys re-opened (the map-key retry's honest
    census — the half-rerun's state, on the record). The detail carries
    the ``run_id`` the run page's trail keys on — the refusal renders
    on the run's trail, not just the target lookup."""
    from taskq.audit import record_admin_action_safe

    await record_admin_action_safe(
        pool,
        schema=schema,
        principal=principal,
        action=action,
        target_type=target_type,
        target_id=target_id,
        reason=reason,
        detail={
            "refused": refusal,
            "applied": applied,
            "total": total,
            "rows": rows,
            "run_id": run_id,
        },
    )


async def _audit_gate_refusal(
    pool: BoundedPool,
    *,
    schema: str,
    principal: Any,
    action: str,
    target_type: str = "hold",
    target_id: str,
    run_id: str,
    node_key: str,
    reason: Any,
    refusal: str,
) -> None:
    """The audited refusal: the typed door's 422 is an
    operator action — it writes its ``admin_audit`` row (``refused`` +
    the gate's reason) before the HTTPException propagates. The refusal
    moves no rows, so the row stands alone (nothing to share a tx with —
    the same-tx discipline's degenerate case); it is written with the
    unsafe recorder so a failure surfaces (a refusal the trail missed is
    the exact invisible-probe failure mode). The detail carries the run_id
    the trail's page query keys on — the refusal renders on the run's
    trail, not just the hold's target lookup. ``target_type``/``target_id``
    name what the refusal was about: a hold (the default) by its canonical
    id, a run-scoped refusal (the deliver door's wire-shape pre-check,
    before any hold is looked up) by the run's canonical spelling."""
    from taskq.web.admin._audit import record_admin_action

    async with pool.acquire() as conn:
        await record_admin_action(
            conn,
            schema=schema,
            principal=principal,
            action=action,
            target_type=target_type,
            target_id=target_id,
            reason=reason if isinstance(reason, str) else None,
            detail={
                "refused": str(refusal),
                "node": node_key,
                "stage": "admin-payload-gate",
                "run_id": run_id,
            },
        )


async def _validated_through_gates_audited(
    pool: BoundedPool,
    *,
    schema: str,
    principal: Any,
    wf_app: Any,
    hold: Any,
    decision: dict[str, object],
    action: str,
    target_id: str,
    reason: Any,
) -> dict[str, object]:
    """The typed door + the audit-on-refusal: validate through the
    bound gates; a 422 refusal lands its audit row first, then
    propagates. The success path is exactly ``_validate_through_gates``."""
    try:
        return _validate_through_gates(wf_app, hold, decision)
    except HTTPException as exc:
        await _audit_gate_refusal(
            pool,
            schema=schema,
            principal=principal,
            action=action,
            target_id=target_id,
            run_id=hold.run_id,
            node_key=hold.node_key,
            reason=reason,
            refusal=str(exc.detail),
        )
        raise


def _validate_through_gates(
    wf_app: Any, hold: Any, decision: dict[str, object]
) -> dict[str, object]:
    """The typed door (the hitl-proof's send boundary): the payload
    re-validates against the bound gate's declared models before any row
    moves. A wrong payload raises the named 422 the operator sees;
    nothing is delivered. The validation core is the shared
    ``validate_gate_payload`` (the CLI's twin body) with THIS surface's
    candidate policy — ``fallback_all=True``: the admin loops every
    mounted workflow (it cannot know the run's) and falls back to ALL
    bound models when none names the signal."""
    from pydantic import BaseModel

    from taskq.workflows._cli import gate_models_for, validate_gate_payload

    models: list[type[BaseModel]] = []
    for workflow in getattr(wf_app, "_workflows", {}):
        try:
            models.extend(gate_models_for(wf_app, workflow, hold.node_key))
        except KeyError:
            continue
    if not models:
        raise HTTPException(
            status_code=422,
            detail=f"node {hold.node_key!r} declares no bound gate in the mounted "
            "definitions — the delivery refuses (no untyped door ships)",
        )
    dumped, refusal = validate_gate_payload(models, hold.signal_name, decision, fallback_all=True)
    if dumped is None:
        assert refusal is not None  # the shared core always states its reason
        raise HTTPException(status_code=422, detail=refusal)
    return dumped


async def _form_body(request: Request) -> dict[str, Any]:
    """The action body's read — the guarded-POST discipline: the fields
    arrive form-encoded (the admin's own convention; the CSRF token is a
    form field, so the body must be one). ``decision``/``payload`` are
    JSON text fields (the payload's shape is the typed door's business,
    parsed here — a malformed one is the named 400, never a traceback)."""
    form = await request.form()
    body: dict[str, Any] = {k: v for k, v in form.items() if isinstance(v, str)}
    for json_field in ("decision", "payload"):
        raw = body.get(json_field)
        if isinstance(raw, str):
            try:
                body[json_field] = _json_loads(raw)
            except ValueError:
                raise HTTPException(
                    status_code=400, detail=f"{json_field} is not valid JSON"
                ) from None
    return body


async def _stream_generator(
    pool: BoundedPool,
    schema: str,
    run_id: uuid.UUID,
    cursor: int,
    session_verifier: Callable[[], Awaitable[bool]] | None = None,
) -> AsyncGenerator[str, None]:
    """The feed: snapshot-on-revision, full replacement each frame (the
    replay contract's body — see the module docstring).

    The session law (the shared ``session_gate``, ``taskq.web._sse_limit``):
    ``session_verifier`` re-runs before the first frame and
    once per poll iteration — a session revoked mid-stream ends the
    stream at the next tick, before any further frame. Fail-closed: a
    verifier error is revocation, and a check that outlives
    ``SESSION_RECHECK_TIMEOUT_SECS`` (a wedged IdP introspection) is
    unknown state, not a pass.

    The generator trap (P2's pin 6): state crosses as parameters — a
    reassignment of a variable captured from the endpoint's scope is the
    UnboundLocalError dragon; nothing here shadows it."""
    seq = cursor
    last_revision: str | None = None
    since_keepalive = 0.0
    _session_still_valid = session_gate(
        session_verifier,
        logger,
        timeout_warning="wf-run-sse-session-recheck-timeout",
        timeout_fields={"run_id": str(run_id)},
    )

    while True:
        # The session re-check gates every iteration (before the poll and
        # every frame it would yield — the revoked session's stream
        # terminates at the next tick, never keeps receiving frames).
        if not await _session_still_valid():
            logger.warning(
                "wf-run-sse-session-revoked",
                run_id=str(run_id),
                stream_phase="streaming",
            )
            return
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(_RUN_REVISION_SQL.format(schema=schema), run_id)
                revision = str(row["revision"]) if row is not None else "0"
                view: RunView | None = None
                if revision != last_revision:
                    view = await fetch_run_view(conn, schema, run_id)
        except asyncpg.exceptions.UndefinedTableError:
            yield _frame(seq, {"status": "uninstalled", "detail": NOT_INSTALLED})
            return
        except asyncpg.exceptions.PostgresConnectionError:
            # The poll's degrade: the keepalive continues (the stream
            # survives a PG blip; the next good poll catches up — the
            # snapshot is the whole state, nothing was lost).
            logger.warning("wf-stream-poll-degraded", run_id=str(run_id))
            yield ": keepalive\n\n"
            await asyncio.sleep(_STREAM_POLL_S)
            continue
        if view is not None:
            seq += 1
            last_revision = revision
            yield _frame(seq, run_state_json(view, seq=seq))
            since_keepalive = 0.0
        elif revision != last_revision:
            # The not-started run (the states matrix's defined state): the
            # revision moved but the run has no rows yet — the subscribe
            # 404s an unknown run id (the existence oracle's refusal), so
            # this branch is the mid-stream race only: the root row the
            # subscribe probe found is pruned between subscribe and this
            # poll. Still a defined state, never a silent hang.
            seq += 1
            last_revision = revision
            yield _frame(
                seq,
                {
                    "seq": seq,
                    "run_id": str(run_id),
                    "status": "unknown",
                    "root_status": "unknown",
                    "nodes": [],
                    "holds": [],
                },
            )
        else:
            since_keepalive += _STREAM_POLL_S
            if since_keepalive >= _STREAM_KEEPALIVE_S:
                yield ": keepalive\n\n"
                since_keepalive = 0.0
        await asyncio.sleep(_STREAM_POLL_S)


def _frame(seq: int, payload: dict[str, Any]) -> str:
    return f"id: {seq}\nevent: run_state\ndata: {_json_dumps(payload).decode('utf-8')}\n\n"


def _run_redirect(
    base_path: str, run_id: uuid.UUID, *, banner: str | None = None
) -> RedirectResponse:
    """The action routes' redirect back to the run page (the base_path
    law: the URL rides the mounted prefix — a relative redirect would
    land the operator on a raw 404). A refusal banner key rides
    the ``?error=`` param the detail page declares."""
    url = f"{base_path}/workflows/{run_id}" + (f"?error={banner}" if banner else "")
    return RedirectResponse(url=url, status_code=303)


async def _form_reason(request: Request) -> str | None:
    """The action form's optional ``reason`` field (the audit row's
    why — absent/blank reads None). The form is already parsed by the
    CSRF dependency; Starlette caches the read."""
    raw = (await request.form()).get("reason")
    return raw if isinstance(raw, str) and raw else None


def register_actions(router: APIRouter) -> None:
    """Attach the machine routes to *router*."""

    @router.get("/api/runs/{run_id}/nodes/{node_key}")
    async def node_panel(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        node_key: str,
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        map_index: int | None = None,
    ) -> dict[str, Any]:
        """The node detail panel's data (the click → panel surface).

        The map-index addressing: a map's children share one step key —
        the panel addresses a child by (step, ``map_index``); the bare
        read lands on the first child in map order and carries the
        ``children`` census, so the operator can click through to the
        child whose story they are watching (the demo README's
        observation: ``watch doc-doomed's attempt go 1 → 2``)."""
        if map_index is not None and (map_index < 0 or map_index > 32767):
            # The column is a smallint: a value beyond its range can
            # never name a real map sibling — the typed 400, not the
            # int2 codec's overflow 500.
            raise HTTPException(status_code=400, detail="map_index must be 0..32767")
        if "\x00" in node_key:
            # A NUL byte can never name a declared node (the keys are
            # identifiers); the read binds it raw — refuse before the
            # bind, never a 500 from the encoding.
            raise HTTPException(status_code=404, detail="no such node")
        async with pool.acquire() as conn:
            if map_index is None:
                node = await conn.fetchrow(_NODE_PANEL_SQL.format(schema=schema), node_key, run_id)
            else:
                node = await conn.fetchrow(
                    _NODE_PANEL_CHILD_SQL.format(schema=schema), node_key, run_id, map_index
                )
            if node is None:
                raise HTTPException(status_code=404, detail="Node not found")
            parent = await conn.fetchrow(_NODE_PARENT_SQL.format(schema=schema), node["id"])
            ledger = await conn.fetch(_NODE_LEDGER_SQL.format(schema=schema), node["id"])
            # The node I/O's capture read: the latest failed
            # attempt's capture preferred — a later attempt's success
            # must not hide the failed attempt's capture (the
            # diagnostic's whole point); the fallback keeps the latest
            # terminal row's capture when no failed row carries one.
            capture_row = await conn.fetchrow(
                _NODE_IO_CAPTURE_FAILED_SQL.format(schema=schema), node["id"]
            )
            if capture_row is None:
                capture_row = await conn.fetchrow(
                    _NODE_IO_CAPTURE_SQL.format(schema=schema), node["id"]
                )
            # The pruned-history discriminator: a failed node with
            # an attempt on the clock, an empty step history, and a
            # terminal root is the retention arms' prune — the honest
            # arm the panel names (never the suppression lie). One
            # bounded root read decides.
            capture_pruned = False
            if (
                capture_row is None
                and node["status"] == "failed"
                and int(node["attempt"]) > 0
                and not ledger
            ):
                root_status = await conn.fetchval(_ROOT_STATUS_SQL.format(schema=schema), run_id)
                capture_pruned = root_status in TERMINAL_STATUSES
            # The children's render cap (bounded read): the first
            # cap rows + the total (count(*) over ()) — a 10k fan-out
            # ships 200 rows, never 10k; the "N more" arm names the rest.
            siblings = await conn.fetch(
                _NODE_CHILDREN_SQL.format(schema=schema), node_key, run_id, _PANEL_CHILDREN_CAP
            )
        children_total = int(siblings[0]["total"]) if siblings else 0
        children = [
            {
                "map_index": r["map_index"],
                "status": r["status"],
                "attempt": r["attempt"],
                "max_attempts": r["max_attempts"],
                # The class field rides the same bound: a hostile row's 200KB
                # error_class once served whole
                # through the panel while the sibling fields were bound.
                "error_class": _bound_for_panel(r["error_class"]),
            }
            for r in siblings
        ]
        return {
            "key": node["step_key"],
            "map_index": node["map_index"],
            "status": node["status"],
            "attempt": node["attempt"],
            "max_attempts": node["max_attempts"],
            "retry_kind": node["retry_kind"],
            "trace_id": str(node["trace_id"]) if node["trace_id"] else None,
            "error_class": _bound_for_panel(node["error_class"]),
            # The read-side bound: the display-capped
            # delivery — a foreign/hostile row's 5MB error fields render
            # bounded with the dropped-count named; the full detail
            # stays in the rows (the CLI/SQL surface reads them
            # uncapped).
            "error_message": _bound_for_panel(node["error_message"]),
            "error_traceback": _bound_for_panel(node["error_traceback"]),
            "captured_error": _bound_for_panel(node["captured_error"]),
            "error_truncated": _panel_truncated(
                [
                    node["error_class"],
                    node["error_message"],
                    node["error_traceback"],
                    node["captured_error"],
                ]
            ),
            "parent": dict(parent) | {"id": str(parent["id"])} if parent else None,
            # The cache-hit receipt (the provenance face): the cross-run cache's hit stamp (address
            # + producing run + stored instant), None on an executed
            # step. A step that never executed must not read like one
            # that did. The jsonb arrives as a JSON string through the
            # bare -> operator — decoded here so the panel's JSON is the
            # object the JS renders.
            "cache_hit": _cache_hit_of(node["cache_hit"]),
            "timeline": [
                dict(r)
                | {
                    "created_at": _iso(r["created_at"]),
                    # The panel's display cap covers the ledger's own
                    # error field (the attempt rows' error_message
                    # rides the same read-side bound as the job
                    # columns' — a 500k-char ledger message renders
                    # capped with the dropped count named, never raw).
                    "error_message": _bound_for_panel(r["error_message"]),
                }
                for r in ledger
            ],
            "children": children if len(children) > 1 else [],
            # The children's total (the "N more" arm): the
            # count beyond the render cap — the JS names the rest.
            "children_total": children_total,
            # The node I/O (the panel's input/output faces): the wired
            # args, the decoded result, the failure capture — each
            # through the engine's own display discipline (see
            # _node_io_view; never a raw dump of a suppressed capture,
            # and the pruned-history arm names retention's prune).
            "io": _node_io_view(node, capture_row, capture_pruned=capture_pruned),
            "created_at": _iso(node["created_at"]),
            "started_at": _iso(node["started_at"]),
            "finished_at": _iso(node["finished_at"]),
        }

    @router.get("/api/runs/{run_id}/stream")
    async def run_stream(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        settings: TaskQSettings = Depends(get_settings),
        session_verifier: Callable[[Request], Awaitable[bool]] | None = Depends(
            get_session_verifier
        ),
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        """The run's SSE state feed (the revisioned-snapshot replay —
        the module docstring states the contract).

        The subscribe law: an unknown run id is a 404 here —
        the estate's own SSE convention (``/sse/{topic}`` refuses an
        unknown topic at subscribe; this endpoint refuses an unknown run
        the same way). The empty-snapshot-forever stream was an
        existence oracle behind the auth gate and a poll task per probe,
        held until the client let go; the 404 is the defined refusal. A
        run that exists but has no node rows yet still streams (the
        generator's not-started state)."""
        # The existence probe (bounded, one indexed read, before the slot
        # acquire — a refused stream must not consume the SSE budget the
        # cap guards).
        async with pool.acquire() as conn:
            try:
                exists = await conn.fetchval(_RUN_EXISTS_SQL.format(schema=schema), run_id)
            except asyncpg.exceptions.UndefinedTableError:
                exists = True  # the generator's uninstalled degrade answers the stream
        if not exists:
            raise HTTPException(status_code=404, detail="Workflow run not found")
        cursor = 0
        if last_event_id is not None:
            try:
                cursor = max(0, int(last_event_id))
            except ValueError:
                cursor = 0
        # The cap: each stream pins a poll task + a socket for as long as
        # the client holds it — the uncapped scan's guard (the SSE-cap
        # law) refuses an endpoint without this, and a burst of open
        # streams would exhaust exactly what the cap bounds.
        sse_slot = await acquire_sse_slot(
            "wf-run-stream", settings.admin_max_sse_connections, surface="admin"
        )
        # The session law's binding (the sse.py re-check pattern): the
        # verifier reads the same session cookie the request arrived
        # with; a session revoked mid-stream fails the re-check at the
        # next tick even though those bytes are unchanged.
        _session_verifier: Callable[[], Awaitable[bool]] | None = (
            (lambda: session_verifier(request)) if session_verifier is not None else None
        )
        return StreamingResponse(
            release_after(
                sse_slot,
                _stream_generator(pool, schema, run_id, cursor, _session_verifier),
                "wf-run-stream",
                surface="admin",
            ),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @router.post("/api/runs/{run_id}/resolve")
    async def run_resolve(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        _actions: None = Depends(require_actions_enabled),
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        principal: Any = Depends(get_principal),
        wf_app: Any = Depends(get_workflow_app),
    ) -> dict[str, Any]:
        """Reply to a hold by id (the typed door; the audit row)."""
        if wf_app is None:
            raise HTTPException(
                status_code=501,
                detail="no workflow definitions mounted (mount the router with "
                "workflow_app=...) — the resolve refuses to deliver untyped",
            )
        body = await _form_body(request)
        hold_id = body.get("hold_id")
        decision = body.get("decision")
        reason = body.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise HTTPException(status_code=400, detail="reason must be a string")
        if not isinstance(hold_id, str) or not isinstance(decision, dict):
            raise HTTPException(
                status_code=400,
                detail="the resolve needs {'hold_id': str, 'decision': object, 'reason'?: str}",
            )
        decision_doc = cast(dict[str, object], decision)
        from taskq.workflows.api._hitl import (
            HitlClient,
            _as_hold_id,  # pyright: ignore[reportPrivateUsage]  # Why: the id's canonical form is the hold module's own normalizer — the refusal's audit pointer carries the form the operator's queries match.
        )

        # The id's shape is the door's own check: a malformed hold id is
        # an audited 400 (the caller's typo), never a 500 from the
        # canonicalizer's parse escaping the action. The pointer's
        # sanitation is the audit leaf's own law (the NUL rendered as
        # its visible escape, bounded to the index's shape) — the door
        # binds what the caller sent.
        try:
            canonical_hold_id = str(_as_hold_id(hold_id))
        except ValueError:
            from taskq.web.admin._audit import record_admin_action

            async with pool.acquire() as conn:
                await record_admin_action(
                    conn,
                    schema=schema,
                    principal=principal,
                    action="hitl.resolve",
                    target_type="hold",
                    target_id=hold_id,
                    reason=reason if isinstance(reason, str) else None,
                    detail={
                        "refused": "the hold id is not a valid id",
                        "node": "unknown",
                        "stage": "admin-payload-gate",
                        "run_id": str(run_id),
                    },
                )
            raise HTTPException(status_code=400, detail="the hold id is not a valid id") from None

        client = HitlClient(pool.pool, schema=schema)
        hold = await client.get(canonical_hold_id)
        if hold is None or hold.run_id != str(run_id):
            raise HTTPException(status_code=404, detail="Hold not found on this run")
        # The audited refusal: the gate's 422 writes its
        # admin_audit row (refused + the reason) before propagating —
        # the shape-probing operator lands in the trail.
        validated = await _validated_through_gates_audited(
            pool,
            schema=schema,
            principal=principal,
            wf_app=wf_app,
            hold=hold,
            decision=decision_doc,
            action="hitl.resolve",
            target_id=str(_as_hold_id(hold_id)),
            reason=reason,
        )
        # The payload's wire shape: a control character (a NUL above
        # all) cannot ride the jsonb bind — the door refuses it as the
        # typed gate's own refusal (audited, the hold survives), never
        # a 500 from the serializer.
        try:
            _dumps_jsonb_str(validated)
        except ValueError as exc:
            await _audit_gate_refusal(
                pool,
                schema=schema,
                principal=principal,
                action="hitl.resolve",
                target_id=canonical_hold_id,
                run_id=str(run_id),
                node_key=hold.node_key,
                reason=reason,
                refusal=str(exc),
            )
            raise HTTPException(status_code=422, detail=str(exc)) from None
        try:
            result = await client.resolve(
                canonical_hold_id,
                validated,
                reason=reason,
                principal=principal,
            )
        except RACED_WRITER_ERRORS:
            # the race's honest loser (the CLI resolve verb's own named
            # refusal — the wording model): PG killed this writer; the
            # route answers the refused verdict, never a 500.
            return {
                "status": "refused",
                "reason": f"the hold {canonical_hold_id}'s writers raced and PG resolved "
                "the deadlock in another writer's favour — the hold is being "
                "resolved OR the run cancelled by the winner; re-check the "
                "run's settled state",
            }
        return {"status": result.status, "reason": result.reason}

    @router.post("/api/runs/{run_id}/abandon")
    async def run_abandon(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        _actions: None = Depends(require_actions_enabled),
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        principal: Any = Depends(get_principal),
    ) -> dict[str, Any]:
        """Abandon a hold by id (the operator door; the audit row): the
        wait fails closed on its next drive — the body sees the
        ``Expired`` member carrying ``abandoned_by="operator"``.

        The resolve route's guards without the payload gate: an abandon
        is a stop-wait, never a verdict — there is no decision to
        validate, so no typed-gate pass and no mounted-definitions
        requirement. Timer holds are refused (the clock is their only
        resolver — the client's own refusal, audited there); the audit
        row rides the client's own winning tx under this route's
        principal — the route never double-audits."""
        body = await _form_body(request)
        hold_id = body.get("hold_id")
        reason = body.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise HTTPException(status_code=400, detail="reason must be a string")
        if not isinstance(hold_id, str):
            raise HTTPException(
                status_code=400,
                detail="the abandon needs {'hold_id': str, 'reason'?: str}",
            )
        from taskq.workflows.api._hitl import (
            HitlClient,
            _as_hold_id,  # pyright: ignore[reportPrivateUsage]  # Why: the id's canonical form is the hold module's own normalizer — the refusal's audit pointer carries the form the operator's queries match.
        )

        # The id's shape is the door's own check (the resolve route's
        # arm): a malformed hold id is an audited 400 (the caller's
        # typo), never a 500 from the canonicalizer's parse escaping the
        # action.
        try:
            canonical_hold_id = str(_as_hold_id(hold_id))
        except ValueError:
            from taskq.web.admin._audit import record_admin_action

            async with pool.acquire() as conn:
                await record_admin_action(
                    conn,
                    schema=schema,
                    principal=principal,
                    action="hitl.abandon",
                    target_type="hold",
                    target_id=hold_id,
                    reason=reason,
                    detail={
                        "refused": "the hold id is not a valid id",
                        "node": "unknown",
                        # the abandon has no payload gate — this audit row
                        # names the gate that refused: the id's shape.
                        "stage": "admin-hold-id-gate",
                        "run_id": str(run_id),
                    },
                )
            raise HTTPException(status_code=400, detail="the hold id is not a valid id") from None

        client = HitlClient(pool.pool, schema=schema)
        hold = await client.get(canonical_hold_id)
        if hold is None or hold.run_id != str(run_id):
            raise HTTPException(status_code=404, detail="Hold not found on this run")
        # the abandon (the audit row rides the client's own tx — the
        # principal is THIS route's, never a second row from here). The
        # race's honest loser: the same named pair the resolve route
        # wraps — the refused verdict, never a 500.
        try:
            result = await client.abandon(canonical_hold_id, reason=reason, principal=principal)
        except RACED_WRITER_ERRORS:
            return {
                "status": "refused",
                "reason": f"the hold {canonical_hold_id}'s writers raced and PG resolved "
                "the deadlock in another writer's favour — the hold is being "
                "abandoned OR resolved/cancelled by the winner; re-check the "
                "run's settled state",
            }
        return {"status": result.status, "reason": result.reason}

    @router.post("/api/runs/{run_id}/deliver")
    async def run_deliver(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        _actions: None = Depends(require_actions_enabled),
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        principal: Any = Depends(get_principal),
        wf_app: Any = Depends(get_workflow_app),
    ) -> dict[str, Any]:
        """Deliver to a run's held node by (run, node) — the wire form's
        address; the same typed door the resolve rides."""
        if wf_app is None:
            raise HTTPException(
                status_code=501,
                detail="no workflow definitions mounted (mount the router with "
                "workflow_app=...) — the deliver refuses to deliver untyped",
            )
        body = await _form_body(request)
        node_key = body.get("node")
        payload = body.get("payload")
        reason = body.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise HTTPException(status_code=400, detail="reason must be a string")
        if not isinstance(node_key, str) or not isinstance(payload, dict):
            raise HTTPException(
                status_code=400,
                detail="the deliver needs {'node': str, 'payload': object, 'reason'?: str}",
            )
        payload_doc = cast(dict[str, object], payload)
        from taskq.workflows.api._hitl import HitlClient

        # The payload's wire shape: a control character (a NUL above
        # all) cannot ride the jsonb bind — the door refuses it as the
        # typed gate's own refusal (audited, the hold survives), never
        # a 500 from the serializer.
        try:
            _dumps_jsonb_str(payload_doc)
        except ValueError as exc:
            await _audit_gate_refusal(
                pool,
                schema=schema,
                principal=principal,
                action="hitl.deliver",
                # The refusal is about the run: no hold was looked up
                # (the wire shape preceded the lookup) — the row targets
                # the run by its canonical spelling, the form every
                # run-targeted row speaks.
                target_type="workflow_run",
                target_id=str(run_id),
                run_id=str(run_id),
                node_key=node_key,
                reason=reason,
                refusal=str(exc),
            )
            raise HTTPException(status_code=422, detail=str(exc)) from None

        client = HitlClient(pool.pool, schema=schema)
        held = [h for h in await client.list(str(run_id)) if h.node_key == node_key]
        if not held:
            raise HTTPException(
                status_code=404, detail=f"node {node_key!r} holds nothing on this run"
            )
        if len(held) > 1:
            raise HTTPException(
                status_code=409,
                detail="ambiguous: the node holds "
                f"{len(held)} signals — address one BY ID (the resolve's door)",
            )
        # The audited refusal: the deliver door's 422 audits
        # exactly like the resolve's (the page's claim names deliver).
        validated = await _validated_through_gates_audited(
            pool,
            schema=schema,
            principal=principal,
            wf_app=wf_app,
            hold=held[0],
            decision=payload_doc,
            action="hitl.resolve",
            target_id=held[0].hold_id,
            reason=reason,
        )
        result = await client.resolve(
            held[0].hold_id,
            validated,
            reason=reason,
            principal=principal,
        )
        return {"status": result.status, "reason": result.reason}

    @router.post("/api/runs/{run_id}/cancel")
    async def run_cancel(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        _actions: None = Depends(require_actions_enabled),
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        principal: Any = Depends(get_principal),
        base_path: str = Depends(get_base_path),
    ) -> Any:
        """Stop the run (the engine's cancel cascade; the audit row)."""
        from taskq.workflows import cancel_workflow_run

        form = await request.form()
        raw_reason = form.get("reason")
        reason = raw_reason if isinstance(raw_reason, str) and raw_reason else None
        stopped = await cancel_workflow_run(
            pool.pool,
            schema=schema,
            flow_id=JobId(run_id),
            reason=reason,
            principal=principal,
        )
        if stopped == 0:
            # Idempotent: an already-terminal run cancels nothing — the
            # operator hears the refusal where they land (the jobs page's
            # refused-op contract: the redirect carries the banner key).
            return _run_redirect(base_path, run_id, banner=CANCEL_NOT_APPLIED_BANNER)
        return _run_redirect(base_path, run_id)

    @router.post("/api/runs/{run_id}/nodes/{node_key}/retry")
    async def node_retry(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        node_key: str,
        request: Request,
        _actions: None = Depends(require_actions_enabled),
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        principal: Any = Depends(get_principal),
        base_path: str = Depends(get_base_path),
    ) -> RedirectResponse:
        """Re-arm one terminal-failed node (the node rerun) — the
        engine's own manual-resume verb (``retry_workflow_node``):
        the failed→pending CAS + the blocked closure's re-open + the
        terminal-failed flow's re-open; the attempt ordinal continues
        (the next claim buys exactly one more attempt). No engine
        semantics are invented here — the route is the verb's audited
        HTTP face. The root key is not a node: the CAS's own
        guard refuses the run's root key — ``__flow__`` lands the
        not-applied banner like any other grantless key (the run's own
        re-run is the run-rerun's concern).

        The audit: the verb writes its own same-transaction
        ``admin_audit`` row (``workflow.retry_node``, principal + reason
        + run/node) — the same discipline the resolve/cancel routes
        carry. The refusals (the audited-refusal law): a live/unknown
        node (the CAS granted nothing) and a cancelled run (the verb's
        second-decision refusal) write their own ``admin_audit`` row
        (``refused`` + the reason) before the redirect, and land the
        named banner — the refused-op contract, never a 400 wall."""
        from taskq.workflows import WorkflowRunError, retry_workflow_node

        # The node key's shape is the door's own check: a NUL byte can
        # never ride the CAS's text bind (the encoding refusal) — an
        # audited refusal, never a 500. (Length is not the door's
        # business: a long declared key binds fine; the pointer's bound
        # is the audit leaf's own law.)
        if "\x00" in node_key:
            await _audit_rerun_refusal(
                pool,
                schema=schema,
                principal=principal,
                action="workflow.retry_node",
                target_type="workflow_node",
                target_id=f"{run_id}:{node_key}",
                run_id=str(run_id),
                reason=None,
                refusal="the node key is not a valid key",
                applied=0,
                total=1,
            )
            return _run_redirect(base_path, run_id, banner=RERUN_REFUSED_BANNER)

        async with pool.acquire() as conn:
            try:
                exists = await conn.fetchval(_RUN_EXISTS_SQL.format(schema=schema), run_id)
            except asyncpg.exceptions.UndefinedTableError:
                exists = True  # the verb's own failure is the defined answer
        if not exists:
            raise HTTPException(status_code=404, detail="Workflow run not found")
        reason = await _form_reason(request)
        try:
            reopened = await retry_workflow_node(
                pool.pool,
                schema=schema,
                flow_id=JobId(run_id),
                node_key=node_key,
                reason=reason,
                principal=principal,
            )
        except WorkflowRunError as exc:
            await _audit_rerun_refusal(
                pool,
                schema=schema,
                principal=principal,
                action="workflow.retry_node",
                target_type="workflow_node",
                target_id=f"{run_id}:{node_key}",
                run_id=str(run_id),
                reason=reason,
                refusal=str(exc),
                applied=0,
                total=1,
            )
            return _run_redirect(base_path, run_id, banner=RERUN_REFUSED_BANNER)
        except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError):
            # The race's honest loser (the retry faces' named pair): a laggard writer's deadlock lands the
            # audited refusal row + the raced banner, never a 500 wall —
            # the same pair the cancel/resolve CLI verbs name.
            await _audit_rerun_refusal(
                pool,
                schema=schema,
                principal=principal,
                action="workflow.retry_node",
                target_type="workflow_node",
                target_id=f"{run_id}:{node_key}",
                run_id=str(run_id),
                reason=reason,
                refusal=DEADLOCK_REFUSAL,
                applied=0,
                total=1,
            )
            return _run_redirect(base_path, run_id, banner=RERUN_RACED_BANNER)
        if reopened == 0:
            await _audit_rerun_refusal(
                pool,
                schema=schema,
                principal=principal,
                action="workflow.retry_node",
                target_type="workflow_node",
                target_id=f"{run_id}:{node_key}",
                run_id=str(run_id),
                reason=reason,
                refusal="not-applied: the node is live, unknown, not terminal-failed, "
                "or the run's own root key",
                applied=0,
                total=1,
            )
            return _run_redirect(base_path, run_id, banner=RERUN_NOT_APPLIED_BANNER)
        return _run_redirect(base_path, run_id)

    @router.post("/api/runs/{run_id}/rerun")
    async def run_rerun(  # pyright: ignore[reportUnusedFunction]
        run_id: uuid.UUID,
        request: Request,
        _actions: None = Depends(require_actions_enabled),
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        principal: Any = Depends(get_principal),
        base_path: str = Depends(get_base_path),
    ) -> RedirectResponse:
        """Re-enqueue a failed run's work (the run rerun).

        The scope, stated honestly: the engine has no run-level
        re-enqueue verb — the run-key arbiter is terminal-forever (a
        fresh input, or a cancelled run, is a new run — the engine's own
        run-key-arbiter contract, and the CLI's remedy line says the same). What the
        engine does offer is the per-node manual resume
        (``retry_workflow_node``: the node's CAS re-arm + the blocked
        children's reset + the flow re-open + the exit-skipped branches'
        re-arm — each call audited in its own transaction). This route
        composes that verb over the run's terminal-failed step keys (the
        rows' census — a map's failed children share one step key, and
        the verb's CAS re-arms every failed row of the key at once). No
        engine semantics are invented: nothing here writes a row the
        verb does not.

        The partial state, named: the composition tracks the
        applied/refused counts — a refusal landing mid-composition (the
        run cancelled between calls, a verb raise) redirects with the
        partial banner ("N of M re-opened before the refusal") and the
        refusal's own audit row carries the counts, agreeing with the
        per-key rows the granted calls wrote.

        The closure walk's blindness, stated: the verb's blocked-
        closure read walks ``wf_edge`` — a run whose edge ledger the
        retention arms pruned re-opens no blocked rows (the stamps stand
        as the rows remain); the pruned-history run's rerun is
        best-effort by the rows that survive, and the page's capture
        block names the prune rather than a policy suppression.

        The refusals: nothing resumable → the not-applied banner; a
        cancelled run → the verb's second-decision refusal banner (the
        first call raises before any row moves). Both write their own
        audit row (the audited-refusal law)."""
        from taskq.workflows import WorkflowRunError, retry_workflow_node

        async with pool.acquire() as conn:
            try:
                view = await fetch_run_view(conn, schema, run_id)
            except asyncpg.exceptions.UndefinedTableError:
                view = None
        if view is None:
            raise HTTPException(status_code=404, detail="Workflow run not found")
        failed_keys = sorted({n.key for n in view.nodes if n.status == "failed"})
        reason = await _form_reason(request)
        # The two counts (the map-key retry's
        # honest census): the verb returns the rows it re-armed (a map's
        # failed children share one step key — one key's grant can move
        # several rows), so the composition tracks keys applied and rows
        # re-opened; the audit row carries both (the applied-count is no
        # longer a key count wearing a row count's clothes).
        keys_applied = 0
        rows_reopened = 0
        raced = False
        refusal: str | None = None
        try:
            for key in failed_keys:
                rows = await retry_workflow_node(
                    pool.pool,
                    schema=schema,
                    flow_id=JobId(run_id),
                    node_key=key,
                    reason=reason,
                    principal=principal,
                )
                if rows:
                    keys_applied += 1
                    rows_reopened += rows
        except WorkflowRunError as exc:
            refusal = str(exc)
        except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError):
            # The race's honest loser (the composition's own catch — the
            # same named pair the per-node door catches): the deadlock is
            # a refusal with the counts on the record (the half-rerun's
            # law holds for the raced loser too), never a 500 wall.
            refusal = DEADLOCK_REFUSAL
            raced = True
        if refusal is not None or keys_applied == 0:
            # The audited refusal (the audited-refusal law at the rerun door): the
            # refusal's own row — the counts on it, agreeing with the
            # per-key rows the granted calls wrote (the half-rerun is on
            # the record); the rows count rides beside the keys
            # (the map-key retry's honest census).
            await _audit_rerun_refusal(
                pool,
                schema=schema,
                principal=principal,
                action="workflow.rerun",
                target_type="workflow_run",
                target_id=str(run_id),
                run_id=str(run_id),
                reason=reason,
                refusal=refusal
                or "not-applied: no run row was resumable (no failed node, no "
                "skip-envelope row grants)",
                applied=keys_applied,
                total=len(failed_keys),
                rows=rows_reopened,
            )
        if refusal is not None:
            if keys_applied:
                # The half-rerun's honest banner: the run was
                # partly re-opened before the refusal — the operator
                # hears the counts, never a clean redirect (the audit
                # row names the refusal's kind — the race included).
                return _run_redirect(
                    base_path,
                    run_id,
                    banner=rerun_partial_banner(rows_reopened, keys_applied, len(failed_keys)),
                )
            if raced:
                # The raced loser's own banner: "the run is cancelled"
                # would lie for a deadlock — the race's loser answers the
                # raced banner (the remedy: reload, re-check, re-run).
                return _run_redirect(base_path, run_id, banner=RERUN_RACED_BANNER)
            return _run_redirect(base_path, run_id, banner=RERUN_REFUSED_BANNER)
        if keys_applied == 0:
            return _run_redirect(base_path, run_id, banner=RERUN_NOT_APPLIED_BANNER)
        return _run_redirect(base_path, run_id)
