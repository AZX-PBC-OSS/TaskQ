"""The workflow-run explorer's MACHINE routes (T11): the node panel API,
the SSE node_state stream (the revisioned-snapshot replay), and the
typed actions (deliver / resolve / cancel — each guarded, each audited).

The pages live in ``workflows``; the split is the read/write concern
boundary (the same shape the jobs page + backend routes keep).

THE STREAM (the replay contract, PINNED): every frame is a revisioned
FULL SNAPSHOT; the rows are the durable ring — a reconnect replays BY
CONSTRUCTION (the next snapshot carries the whole state), and the
seq-cursor drops stale frames client-side. A killed connection cannot
strand the page on a stale render, and a memory ring would be a second
copy of a ledger that already exists. THE SESSION LAW (Q4's cure — the
same re-check ``/sse/{topic}`` got for #316): the stream re-invokes the
host's ``session_verifier`` before the first frame and once per poll
iteration — a session revoked MID-STREAM ends the stream at the next
tick, fail-closed on any verifier error or a check that outlives
``SESSION_RECHECK_TIMEOUT_SECS``. THE SUBSCRIBE LAW (the existence
oracle's cure, Q7): an unknown run id is a 404 AT SUBSCRIBE — the
estate's own SSE convention (``/sse/{topic}`` refuses an unknown topic
at subscribe; the empty-snapshot-forever stream was an existence oracle
behind the auth gate and a wasted poll task per probe). A run that
EXISTS but whose node rows are not inserted yet still streams (the
generator's not-started state).

THE TYPED DOOR: deliver + resolve validate through the bound
``TypedGate``s of the WorkflowApp the host mounted (``create_router(
workflow_app=...)``). Without one the endpoints answer ``501`` with the
named reason — NO untyped deliver surface ships. THE AUDITED REFUSAL
(Q3's cure): the gate's 422 IS an operator action — the refusal writes
its own ``admin_audit`` row (``refused`` + the reason) before the
HTTPException propagates, so an attacker's shape-probing lands IN the
trail the page claims covers every Resolve/deliver; the hold survives.

THE AUDIT (G4): every resolve/deliver/cancel writes its ``admin_audit``
row (principal + reason + run/node) — "who approved this" is a ROW, not
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
from taskq._json import dumps_str as _json_dumps_str
from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.settings import TaskQSettings
from taskq.web._pool import BoundedPool
from taskq.web._sse_limit import SESSION_RECHECK_TIMEOUT_SECS, acquire_sse_slot, release_after
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

#: The refusal banner KEYS the redirect-carrying action routes hand back
#: (the closed vocabulary the detail page's ``REFUSAL_BANNERS`` maps to a
#: copy — the refused-op contract: the operator lands on the DEFINED
#: refused state, never a 400 wall). Defined HERE (the routes' home) so
#: a route and its banner copy cannot drift apart silently.
CANCEL_NOT_APPLIED_BANNER: Final[str] = "cancel-not-applied"
RERUN_NOT_APPLIED_BANNER: Final[str] = "rerun-not-applied"
RERUN_REFUSED_BANNER: Final[str] = "rerun-refused"
#: THE DEADLOCK'S NAMED BANNER (the round-2 conviction): a laggard
#: writer's deadlock used to land a 500 wall — the retry faces catch the
#: SAME named pair the cancel/resolve CLI verbs carry and answer the
#: audited refusal + THIS banner (the race's honest loser, the remedy
#: named; the audited-refusal law).
RERUN_RACED_BANNER: Final[str] = "rerun-raced"
#: THE DEADLOCK'S REFUSAL TEXT (the named pair's shared copy): the audit
#: row records these words and the CLI's ``flows retry`` echoes the same
#: law (the race's honest loser — the winner's own voice is honest).
DEADLOCK_REFUSAL: Final[str] = (
    "the run's writers raced and PG resolved the deadlock in another "
    "writer's favour — the retry is being applied OR the run cancelled "
    "by the winner; re-check the run's settled state"
)
#: THE PARTIAL RERUN'S KEY (H-C5): the run-rerun composition tracks the
#: applied/refused counts, and a refusal landing MID-composition carries
#: them — ``rerun-partial:<rows>:<keys>:<total>`` (the round-2
#: conviction: the ROWS count leads) — the detail page renders
#: the counts (the half-rerun's honest banner; a bare ``rerun-refused``
#: would imply nothing moved).
RERUN_PARTIAL_BANNER: Final[str] = "rerun-partial"


def rerun_partial_banner(rows: int, keys: int, total: int) -> str:
    """The partial rerun's redirect param value (the counts ride the
    key — the page's ``_refusal_banner`` parses the shape back). The
    ROWS count leads (the round-2 conviction: a map key's grant re-arms
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

#: The subscribe probe (Q7's cure): one indexed root-row read decides
#: the stream's 404-before-first-frame (the existence oracle's refusal).
_RUN_EXISTS_SQL = "SELECT 1 FROM \"{schema}\".jobs WHERE id = $1 AND step_key = '__flow__'"

#: The node detail panel's read (the drill-down: status header, attempts,
#: the trace id, captured error, one upstream hop — §10.3's causal chain
#: via parent_id). MAP CHILDREN share one step key (``<src>.item``), so
#: the panel addresses a child by (step, map_index): the bare read
#: lands on the FIRST child in map order (deterministic), and the
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
#: per child, in map order — the operator clicks THROUGH to a child.
#: (The statement lives below, beside the render cap it serves.)

_NODE_PARENT_SQL = (
    'SELECT id, step_key, status, error_class FROM "{schema}".jobs '
    'WHERE id = (SELECT parent_id FROM "{schema}".jobs WHERE id = $1)'
)

_NODE_LEDGER_SQL = (
    "SELECT step_key, attempt, status, error_class, error_message, created_at "
    'FROM "{schema}".wf_step_ledger WHERE job_id = $1 ORDER BY id'
)

#: The node I/O panel's capture read: the LATEST FAILED attempt's
#: failure IO-capture WITH a capture on it (H-C6 — a later attempt's
#: SUCCESS must not hide the failed attempt's capture: the diagnostic's
#: whole point is the failure's input + error), falling back to the
#: latest terminal ledger row when no failed row carries one. The panel
#: labels WHICH attempt's capture it shows.
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

#: The panel's children RENDER cap (H-C9's bounded render): a 10k
#: fan-out's census ships the FIRST cap rows + the total — the "N more"
#: arm names the rest; the rows stay one indexed query away (the child
#: read is (step, map_index)-addressable).
_PANEL_CHILDREN_CAP: Final[int] = 200

#: The children census read: the cap bounds the rows; ``count(*) OVER ()``
#: carries the TOTAL in the same statement (one read, no second count).
_NODE_CHILDREN_SQL = (
    "SELECT map_index, status, attempt, max_attempts, error_class, "
    "count(*) OVER () AS total "
    'FROM "{schema}".jobs WHERE step_key = $1 '
    "AND (metadata->>'flow_id')::uuid = $2 AND step_key <> '__flow__' "
    "ORDER BY map_index LIMIT $3"
)

#: The ROOT-ROW STATUS read (the pruned-history arm's discriminator,
#: H-C7): a FAILED node with an EMPTY step history on a TERMINAL run is
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


#: The panel's display cap (CHARS): error_message / error_traceback /
#: metadata->>'error' render bounded — the full detail stays in the ROWS
#: (the drill-down fetch reads the same rows; the cap is the DISPLAY's,
#: Q5's read-side cure: engine-written rows carry the write-side caps,
#: but a row written by ANY other path — or a hostile DB write — served
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


#: THE WALK BUDGET (the bounded-display law, the round-2 conviction):
#: the result face's redaction walk runs on a PRE-TRIMMED projection —
#: the doc's depth, node count, and SERVED SIZE each budgeted
#: BEFORE the walk, never a multi-MB walk per drill-down. The size
#: budget counts the SERVED size — keys, structure, and values (the
#: round-3 cure: a values-only count let a key-heavy doc's keys and
#: JSON syntax ride FREE — the served text ran ~46k for a claimed
#: 1,023, and the panel's own truncation arm fired on top of the
#: walk's). Every node carries a flat syntax frame (the quotes, colon,
#: comma, and braces its JSON render needs) beside its content's
#: length — the json-serialized length's honest proxy. Escape
#: inflation can still render longer (a kept head's escaped bytes);
#: the panel's own cap remains the BACKSTOP for that, never the first
#: bound a budgeted doc meets. The budget sits under the display cap
#: with room for the masks (the served text stays post-redaction —
#: the honesty law).
_WALK_DEPTH_BUDGET: Final[int] = 32
_WALK_NODE_BUDGET: Final[int] = 2_048
_WALK_CHAR_BUDGET: Final[int] = 8_000
_WALK_PRUNED_MARKER: Final[str] = "…walk budget exceeded — the rest is not shown…"
#: The string render's own quote pair — a kept string's served length
#: is its characters PLUS the two quotes it renders with.
_WALK_STRING_QUOTES: Final[int] = 2
#: The JSON punctuation a node's own render can carry (quotes, colon,
#: comma, braces) — the flat per-node charge the served-size accounting
#: adds beside each node's content length.
_WALK_NODE_SYNTAX_CHARS: Final[int] = 4
#: The worst cut's own served render (the marker riding a dict entry's
#: key AND value, quoted and coloned, plus the nodes' syntax frames) —
#: the char budget RESERVES it so a cut always lands the census at or
#: under the budget: the census is the served size's honest count,
#: never an overshoot itself.
_WALK_CUT_RESERVE: Final[int] = 2 * len(_WALK_PRUNED_MARKER) + 4 * _WALK_NODE_SYNTAX_CHARS


def _bounded_io_projection(doc: object) -> tuple[object, str | None, dict[str, int]]:
    """THE DISPLAY WALK'S PRE-TRIM (the round-2 conviction): project the
    doc into the walk budget BEFORE the redaction walk — depth, node
    count, and SERVED SIZE each capped, every cut site carrying
    the named marker (never a silent discard). Returns (the projected
    doc, the named size arm — None when the whole doc fit — and the
    walk's own census: the nodes and served characters actually
    processed).

    THE ORDER IS THE HONESTY'S OWN: the trim runs BEFORE the redaction
    (a multi-MB doc never pays the multi-MB walk), and the REDACTION
    still runs on everything served (the projected doc's strings go
    through the same chain — the served text stays post-redaction). A
    secret cut by the trim is absent entirely (never served raw); a
    secret inside the kept band is masked by the chain.

    THE SIZE BUDGET'S UNIT IS THE SERVED TEXT (the round-3 cure — the
    values-only count lied): keys, structure, and values all count —
    the census is the json-serialized length's honest proxy, and the
    note's number is the SAME accounting (a key-heavy doc's keys can no
    longer ride free past the budget)."""
    state = {"nodes": 0, "chars": 0, "pruned": 0, "done": 0}

    def _budget_out() -> bool:
        # THE FIRST CUT ENDS THE WALK: once any site is trimmed, every
        # further node is the marker — a long list of cheap strings past
        # the budget must not earn one marker PER ITEM (the markers
        # would out-grow the budget they bound; the red-measure: 300
        # marker copies ballooned the "bounded" text to 17k). The char
        # check runs against the budget LESS the cut's own render
        # reserve — a cut must land the census at/under the budget.
        return (
            bool(state["done"])
            or state["nodes"] > _WALK_NODE_BUDGET
            or state["chars"] > _WALK_CHAR_BUDGET - _WALK_CUT_RESERVE
        )

    def _mark() -> str:
        state["pruned"] += 1
        state["done"] = 1
        # THE MARKER'S OWN CONTENT counts (it is served text).
        state["chars"] += len(_WALK_PRUNED_MARKER)
        return _WALK_PRUNED_MARKER

    def _marker_key(out: dict[object, object]) -> str:
        # THE COLLISION-SAFE MARKER (the round-3 cure): a doc whose OWN
        # entry's key equals the marker string must not be OVERWRITTEN
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
        # THE SYNTAX FRAME: every node's render carries punctuation
        # (quotes, colon, comma, braces) — the flat charge that makes
        # the census the served size's honest proxy (the round-3 cure:
        # the keys and the structure used to ride free).
        state["chars"] += _WALK_NODE_SYNTAX_CHARS
        if depth > _WALK_DEPTH_BUDGET or _budget_out():
            return _mark()
        if isinstance(value, str):
            # THE STRING'S SERVED RENDER: the pair of quotes rides the
            # count — the proxy counts the text the panel carries, not
            # the bare value.
            rendered = len(value) + _WALK_STRING_QUOTES
            remaining = _WALK_CHAR_BUDGET - _WALK_CUT_RESERVE - state["chars"]
            if rendered > remaining:
                # THE CUT IS NAMED AT ITS OWN SITE (the same honesty
                # _bound_for_panel's suffix carries): the kept head rides
                # WITH the marker — a partial value never reads as whole,
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
            # THE CAST IS THE JSONB BOUNDARY's declared contract (RFC
            # 8259: the wire's object keys are strings) — the same face
            # _redact_io_doc's walk declares.
            for k, v in cast("dict[str, object]", value).items():
                state["nodes"] += 1
                # THE KEY'S SERVED RENDER: the key node's syntax frame
                # PLUS the key's own characters (the round-3 conviction:
                # the keys were free).
                state["chars"] += _WALK_NODE_SYNTAX_CHARS + len(k)
                if _budget_out():
                    # THE CUT REPLACES THIS ENTRY: the marker rides as
                    # the key (collision-safe, above) AND the value —
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
                    # THE CUT REPLACES THIS ELEMENT: the marker's node
                    # (frame + content) stands where the element would.
                    state["nodes"] += 1
                    state["chars"] += _WALK_NODE_SYNTAX_CHARS
                    out_list.append(_mark())
                    return out_list
                out_list.append(_walk(v, depth + 1))
            return out_list
        # THE SCALAR LEAF: the jsonb boundary's own scalar kinds — the
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


#: The node payload's keys that ARE the wired input (the runner's own
#: envelope vocabulary — api/_sql_runner's INPUT_KEY/WF_ARGS_KEY/ITEM_KEY).
#: Every other payload key is engine bookkeeping and stays off the display.
_WF_PAYLOAD_INPUT_KEYS: Final[frozenset[str]] = frozenset({"wf_input", "wf_args", "wf_item"})


def _redact_io_doc(doc: object) -> object:
    """THE I/O DOC'S DISPLAY REDACTION (H-C2's symmetry cure): the FULL
    chain the input face runs — ``mask_credentials`` (the vetted chain,
    the tors pre-pass included) → the hold-context's token-head pass —
    over EVERY string the doc carries: VALUES and KEY NAMES both (a
    ``{"sk-…": …}`` key masks like the value it held). Deep nesting and
    list elements walk. Over-redaction is the safe failure: the row
    keeps the truth, the panel shows the masked form.

    THE MASKED-KEY COLLISION LAW (the round-2 conviction): two keys
    that mask to the SAME ``***`` must not overwrite each other (a
    dict keyed on the masked name keeps only the last) — colliding
    masked keys are disambiguated ``***[2]``, ``***[3]``, … until
    unique: every masked entry SURVIVES, no raw key byte rides, and
    the ordinal keeps the count on the record.

    The private passes are imported for the SAME reason the capture
    pipeline composes them (a copy could drift from the masks the
    composition pin holds): this is the CHAIN, not a re-derivation."""
    from taskq.obs._redact_exc import mask_credentials  # pyright: ignore[reportPrivateUsage]
    from taskq.workflows._capture import _mask_hold_token  # pyright: ignore[reportPrivateUsage]

    def _mask_str(text: str) -> str:
        return _mask_hold_token(mask_credentials(text))

    def _walk(value: object) -> object:
        if isinstance(value, str):
            return _mask_str(value)
        if isinstance(value, dict):
            # THE CAST IS THE JSONB BOUNDARY's declared contract (RFC
            # 8259: the wire's object keys are strings) — the decode's
            # shape, never an inferred Unknown.
            walked: dict[object, object] = {}
            for k, v in cast("dict[str, object]", value).items():
                masked_key = _mask_str(k)
                # THE COLLISION (5b): the second key masking to the same
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
    """THE NODE I/O VIEW (the panel's input/output faces — the engine's
    own display disciplines, never a raw dump of what a policy
    suppressed):

    * ``input`` — the node's WIRED args (the entry input ``wf_input``,
      the wiring's recorded data args ``wf_args``, the map child's item
      ``wf_item``), read off the row's payload and REDACTED THROUGH THE
      ENGINE'S OWN PIPELINE at display (``_redact_io_doc``: the vetted
      chain → the token-head pass, VALUES and KEY NAMES both — exactly
      the hold-context discipline; the rows-alone law keeps the
      workflow's own ``redact=`` hook unresolvable here, and
      over-redaction is the safe failure);
    * ``result`` — the terminal node's result envelope, decoded and
      UNWRAPPED the read-side display's own way (``{"value": x}`` alone
      → ``x``, the shape read_map_aggregate decodes), through the SAME
      full chain the input face runs (H-C2's symmetry: the token-head
      pass included — the red's chain-only face rendered bare ``sk-…``
      heads RAW), bounded, with the ``truncated`` flag computed
      POST-redaction (the flag describes the text SERVED — H-C9);
    *   ``capture`` — the failure IO-capture as the engine wrote it,
        THEN through the SAME display-time chain the other faces run
        (the round-2 conviction: the write's own mask — a pre-chain-
        release write or a hostile row — is not trusted at display; the
        panel adds no unmasked byte and only bounds the display),
        labeled with WHICH attempt's capture it shows
        (``capture_attempt`` / ``capture_status`` — H-C6: the LATEST
        FAILED attempt's capture preferred, so a later success's NULL
        capture cannot hide it);
    * ``capture_suppressed`` — a FAILED node whose capture policy
      captured nothing: the DEFINED state, named as such — the panel
      never substitutes raw rows for a suppressed capture;
    * ``capture_pruned`` — a FAILED node whose step history the
      RETENTION arms deleted (the ledger read finds no rows for an
      attempted node on a terminal run): the honest arm (H-C7) — the
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
        # THE RESULT FACE IS THE SUCCESS PATH'S (the output's "decoded
        # result" arm): a failed node's output is the CAPTURE below —
        # the engine writes no result on the failure path, and an empty
        # envelope rendered as one would be a lie of shape.
        # THE ENVELOPE UNWRAP (the read-side display's own shape): a lone
        # ``{"value": …}`` is the runner's wrapper, not the body's return.
        unwrapped: object = result
        if isinstance(result, dict) and set(cast("dict[str, object]", result)) == {"value"}:
            unwrapped = cast("dict[str, object]", result)["value"]
        # THE BOUNDED WALK (the round-2 conviction): the display walks a
        # PRE-TRIMMED projection (depth/width/chars budgeted BEFORE the
        # walk — a multi-MB row is a bounded walk, never a multi-MB
        # dump), then the FULL chain (the token-head pass included, key
        # names masked) runs on what is served; the text and its
        # truncated flag both describe the POST-redaction bytes (H-C2 +
        # H-C9). The trim is never silent: the named size arm rides the
        # face when the projection cut.
        projected, walk_note, _walk_stats = _bounded_io_projection(unwrapped)
        text = _json_dumps_str(_redact_io_doc(projected))
        face: dict[str, object] = {
            "text": _bound_for_panel(text),
            "truncated": len(text) > _PANEL_FIELD_CAP_CHARS,
        }
        if walk_note is not None:
            # THE NAMED SIZE ARM (the round-2 conviction): the walk
            # budget's cut is on the record — the served size's honest
            # count ("the first N characters of the SERVED doc") —
            # never a silent discard.
            face["walk_note"] = walk_note
        io["result"] = face

    if capture_row is not None:
        capture = _jsonb_doc(capture_row["capture"])
        if isinstance(capture, dict):
            # THE CAPTURE FACE'S DISPLAY-TIME MASK (the round-2
            # conviction — the symmetry's own logic): the input/result
            # faces re-run the chain at display (the hostile-row
            # defense); the capture face trusts the write NO LONGER —
            # the SAME chain runs here (a pre-chain-release write's raw
            # canary renders masked, never raw). The cast is the JSONB
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
                # THE CAPTURE'S LABEL (H-C6): WHICH attempt's capture the
                # panel shows — the failed attempt the read preferred.
                io["capture_attempt"] = capture_row["attempt"]
                io["capture_status"] = capture_row["status"]
    if capture_pruned:
        # THE PRUNED ARM WINS (H-C7): the history is GONE — naming a
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
    reason: str | None,
    refusal: str,
    applied: int,
    total: int,
    rows: int = 0,
) -> None:
    """THE AUDITED RERUN REFUSAL (Q3's law at the rerun door, H-C5 +
    H-C9): a refused rerun — grantless, cancelled, MID-composition, or
    the raced loser — writes its own ``admin_audit`` row (``refused`` +
    the counts) before the redirect lands. The refusal moves no rows of
    its own, so the row stands alone (the same-tx discipline's
    degenerate case), written with the SAFE recorder: a bookkeeping row
    the trail missed is the exact invisible-probe conviction, but the
    redirect's refusal state must not fake-fail on a bookkeeping blip.
    ``applied``/``total`` are the composition's KEY counts and ``rows``
    the ROWS the granted keys re-opened (the map-key retry's honest
    census — the half-rerun's state, on the record)."""
    from taskq.audit import record_admin_action_safe

    await record_admin_action_safe(
        pool,
        schema=schema,
        principal=principal,
        action=action,
        target_type=target_type,
        target_id=target_id,
        reason=reason,
        detail={"refused": refusal, "applied": applied, "total": total, "rows": rows},
    )


async def _audit_gate_refusal(
    pool: BoundedPool,
    *,
    schema: str,
    principal: Any,
    action: str,
    target_id: str,
    run_id: str,
    node_key: str,
    reason: Any,
    refusal: str,
) -> None:
    """THE AUDITED REFUSAL (Q3's cure): the typed door's 422 is an
    operator ACTION — it writes its ``admin_audit`` row (``refused`` +
    the gate's reason) before the HTTPException propagates. The refusal
    moves no rows, so the row stands alone (nothing to share a tx with —
    the same-tx discipline's degenerate case); it is written with the
    UNsafe recorder so a failure SURFACES (a refusal the trail missed is
    the exact invisible-probe conviction). The detail carries the run_id
    the trail's page query keys on — the refusal renders on THE RUN's
    trail, not just the hold's target lookup."""
    from taskq.web.admin._audit import record_admin_action

    async with pool.acquire() as conn:
        await record_admin_action(
            conn,
            schema=schema,
            principal=principal,
            action=action,
            target_type="hold",
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
    """The typed door + the audit-on-refusal (Q3): validate through the
    bound gates; a 422 refusal lands its audit row FIRST, then
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
    """THE TYPED DOOR (the hitl-proof's send boundary): the payload
    re-validates against the bound gate's declared models BEFORE any row
    moves. A wrong payload raises the named 422 the operator SEES;
    nothing is delivered."""
    from pydantic import BaseModel, ValidationError

    from taskq.workflows._cli import gate_models_for

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
    members = hold.signal_name.split("|")
    candidates = [m for m in models if m.__name__ in members] or models
    errors: list[str] = []
    for model in candidates:
        try:
            validated = model.model_validate(decision)
        except ValidationError as exc:
            errors.append(
                f"{model.__name__}: "
                + "; ".join(
                    f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}" for e in exc.errors()
                )
            )
            continue
        dumped: dict[str, object] = validated.model_dump(mode="json")
        return dumped
    raise HTTPException(
        status_code=422, detail="pydantic refused the payload: " + " | ".join(errors)
    )


async def _form_body(request: Request) -> dict[str, Any]:
    """The action body's read — the guarded-POST discipline: the fields
    arrive FORM-ENCODED (the admin's own convention; the CSRF token is a
    form field, so the body must be one). ``decision``/``payload`` are
    JSON TEXT fields (the payload's shape is the typed door's business,
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

    THE SESSION LAW (Q4's cure, the #316 pattern ported from
    ``sse.py``): ``session_verifier`` re-runs before the first frame and
    once per poll iteration — a session revoked mid-stream ends the
    stream at the next tick, before ANY further frame. Fail-closed: a
    verifier error is revocation, and a check that outlives
    ``SESSION_RECHECK_TIMEOUT_SECS`` (a wedged IdP introspection) is
    unknown state, not a pass.

    THE GENERATOR TRAP (P2's pin 6): state crosses as PARAMETERS — a
    reassignment of a variable captured from the endpoint's scope is the
    UnboundLocalError dragon; nothing here shadows it."""
    seq = cursor
    last_revision: str | None = None
    since_keepalive = 0.0
    _recheck_count = {"n": 0}

    async def _session_still_valid() -> bool:
        if session_verifier is None:
            return True
        first_check = _recheck_count["n"] == 0
        try:
            _recheck_count["n"] += 1
            return bool(
                await asyncio.wait_for(
                    session_verifier(),
                    timeout=SESSION_RECHECK_TIMEOUT_SECS,
                )
            )
        except TimeoutError:
            logger.warning(
                "wf-run-sse-session-recheck-timeout",
                run_id=str(run_id),
                timeout_secs=SESSION_RECHECK_TIMEOUT_SECS,
                stream_phase="initial" if first_check else "streaming",
            )
            return False
        except Exception:
            # Fail closed: an unknown session state must not keep an
            # admin stream open (the caller names the revocation).
            return False

    while True:
        # THE SESSION RE-CHECK GATES EVERY ITERATION (before the poll and
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
            # THE NOT-STARTED RUN (the states matrix's defined state): the
            # revision moved but the run has no rows yet — the subscribe
            # 404s an UNKNOWN run id (the existence oracle's cure), so
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
    land the operator on the convicted 404). A refusal banner key rides
    the ``?error=`` param the detail page DECLARES."""
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

        THE MAP-INDEX ADDRESSING: a map's children share one step key —
        the panel addresses a child by (step, ``map_index``); the bare
        read lands on the FIRST child in map order and carries the
        ``children`` census, so the operator can click through to the
        child whose story they are watching (the demo README's
        observation: ``watch doc-doomed's attempt go 1 → 2``)."""
        if map_index is not None and map_index < 0:
            raise HTTPException(status_code=400, detail="map_index must be >= 0")
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
            # THE NODE I/O's capture read (H-C6): the LATEST FAILED
            # attempt's capture preferred — a later attempt's SUCCESS
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
            # THE PRUNED-HISTORY DISCRIMINATOR (H-C7): a FAILED node with
            # an ATTEMPT on the clock, an EMPTY step history, and a
            # TERMINAL root is the retention arms' prune — the honest
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
            # THE CHILDREN'S RENDER CAP (H-C9's bounded read): the first
            # cap rows + the total (count(*) OVER ()) — a 10k fan-out
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
                # THE CLASS FIELD RIDES THE SAME BOUND (the rv2 finding's
                # cure): a hostile row's 200KB error_class served whole
                # through the census while the sibling fields were bound.
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
            # THE READ-SIDE BOUND (Q5's cure): the display-capped
            # delivery — a foreign/hostile row's 5MB error fields render
            # bounded with the dropped-count named; the full detail
            # stays in the ROWS (the CLI/SQL surface reads them
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
            # THE CACHE-HIT RECEIPT (the audit drill's cure — the
            # provenance face): the cross-run cache's hit stamp (address
            # + producing run + stored instant), None on an executed
            # step. A step that never executed must not read like one
            # that did. The jsonb arrives as a JSON string through the
            # bare -> operator — decoded HERE so the panel's JSON is the
            # object the JS renders.
            "cache_hit": _cache_hit_of(node["cache_hit"]),
            "timeline": [dict(r) | {"created_at": _iso(r["created_at"])} for r in ledger],
            "children": children if len(children) > 1 else [],
            # THE CHILDREN'S TOTAL (H-C9's "N more" arm): the census's
            # count beyond the render cap — the JS names the rest.
            "children_total": children_total,
            # THE NODE I/O (the panel's input/output faces): the wired
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

        THE SUBSCRIBE LAW (Q7's cure): an unknown run id is a 404 HERE —
        the estate's own SSE convention (``/sse/{topic}`` refuses an
        unknown topic at subscribe; this endpoint refuses an unknown RUN
        the same way). The empty-snapshot-forever stream was an
        existence oracle behind the auth gate AND a poll task per probe,
        held until the client let go; the 404 is the defined refusal. A
        run that exists but has no node rows yet still streams (the
        generator's not-started state)."""
        # THE EXISTENCE PROBE (bounded, one indexed read, BEFORE the slot
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
        # THE CAP: each stream pins a poll task + a socket for as long as
        # the client holds it — the uncapped scan's guard (the SSE-cap
        # law) refuses an endpoint without this, and a burst of open
        # streams would exhaust exactly what the cap bounds.
        sse_slot = await acquire_sse_slot(
            "wf-run-stream", settings.admin_max_sse_connections, surface="admin"
        )
        # THE SESSION LAW's binding (the #316 pattern, sse.py): the
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
        """Reply to a hold BY ID (the typed door; the audit row)."""
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
        from taskq.workflows.api._hitl import HitlClient

        client = HitlClient(pool.pool, schema=schema)
        hold = await client.get(hold_id)
        if hold is None or hold.run_id != str(run_id):
            raise HTTPException(status_code=404, detail="Hold not found on this run")
        # THE AUDITED REFUSAL (Q3's cure): the gate's 422 writes its
        # admin_audit row (refused + the reason) before propagating —
        # the shape-probing operator lands IN the trail.
        validated = await _validated_through_gates_audited(
            pool,
            schema=schema,
            principal=principal,
            wf_app=wf_app,
            hold=hold,
            decision=decision_doc,
            action="hitl.resolve",
            target_id=hold_id,
            reason=reason,
        )
        result = await client.resolve(
            hold_id,
            validated,
            reason=reason,
            principal=principal,
        )
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
        """Deliver to a run's held node BY (run, node) — the wire form's
        address; the SAME typed door the resolve rides."""
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
        # THE AUDITED REFUSAL (Q3's cure): the deliver door's 422 audits
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
        """Re-arm ONE terminal-failed node (THE NODE RERUN) — the
        ENGINE'S OWN manual-resume verb (``retry_workflow_node``, T12):
        the failed→pending CAS + the blocked closure's re-open + the
        terminal-FAILED flow's re-open; the attempt ordinal CONTINUES
        (the next claim buys exactly one more attempt). No engine
        semantics are invented here — the route is the verb's audited
        HTTP face. THE ROOT KEY IS NOT A NODE (H-C1): the CAS's own
        guard refuses the run's root key — ``__flow__`` lands the
        not-applied banner like any other grantless key (the run's own
        re-run is the RUN-rerun's concern).

        THE AUDIT (G4): the verb writes its own same-transaction
        ``admin_audit`` row (``workflow.retry_node``, principal + reason
        + run/node) — the same discipline the resolve/cancel routes
        carry. THE REFUSALS (Q3's audited-refusal law): a live/unknown
        node (the CAS granted nothing) and a CANCELLED run (the verb's
        second-decision refusal) write THEIR OWN ``admin_audit`` row
        (``refused`` + the reason) before the redirect, and land the
        named banner — the refused-op contract, never a 400 wall."""
        from taskq.workflows import WorkflowRunError, retry_workflow_node

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
                reason=reason,
                refusal=str(exc),
                applied=0,
                total=1,
            )
            return _run_redirect(base_path, run_id, banner=RERUN_REFUSED_BANNER)
        except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError):
            # THE RACE'S HONEST LOSER (the retry faces' named pair — the
            # round-2 conviction): a laggard writer's deadlock lands the
            # AUDITED refusal row + the raced banner, never a 500 wall —
            # the same pair the cancel/resolve CLI verbs name.
            await _audit_rerun_refusal(
                pool,
                schema=schema,
                principal=principal,
                action="workflow.retry_node",
                target_type="workflow_node",
                target_id=f"{run_id}:{node_key}",
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
        """Re-enqueue a FAILED run's work (THE RUN RERUN).

        THE SCOPE, STATED HONESTLY: the engine has NO run-level
        re-enqueue verb — the run-key arbiter is terminal-forever (a
        fresh input, or a CANCELLED run, is a NEW run — the engine's own
        G2 contract, and the CLI's remedy line says the same). What the
        engine DOES offer is the per-node manual resume
        (``retry_workflow_node``: the node's CAS re-arm + the blocked
        children's reset + the flow re-open + the exit-skipped branches'
        re-arm — each call audited in its own transaction). This route
        composes THAT verb over the run's terminal-FAILED step keys (the
        rows' census — a map's failed children share one step key, and
        the verb's CAS re-arms every failed row of the key at once). No
        engine semantics are invented: nothing here writes a row the
        verb does not.

        THE PARTIAL STATE, NAMED (H-C5): the composition tracks the
        applied/refused counts — a refusal landing MID-composition (the
        run cancelled between calls, a verb raise) redirects with the
        PARTIAL banner ("N of M re-opened before the refusal") and the
        refusal's own audit row carries the counts, agreeing with the
        per-key rows the granted calls wrote.

        THE CLOSURE WALK'S BLINDNESS, STATED (H-C7): the verb's blocked-
        closure read walks ``wf_edge`` — a run whose edge ledger the
        retention arms PRUNED re-opens no blocked rows (the stamps stand
        as the rows remain); the pruned-history run's rerun is
        best-effort by the rows that survive, and the page's capture
        block names the prune rather than a policy suppression.

        THE REFUSALS: nothing resumable → the not-applied banner; a
        CANCELLED run → the verb's second-decision refusal banner (the
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
        # THE TWO COUNTS (the round-2 conviction — the map-key retry's
        # honest census): the verb returns the ROWS it re-armed (a map's
        # failed children share one step key — ONE key's grant can move
        # several rows), so the composition tracks KEYS applied AND ROWS
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
            # THE RACE'S HONEST LOSER (the composition's own catch — the
            # same named pair the per-node door catches): the deadlock is
            # a REFUSAL with the counts on the record (the half-rerun's
            # law holds for the raced loser too), never a 500 wall.
            refusal = DEADLOCK_REFUSAL
            raced = True
        if refusal is not None or keys_applied == 0:
            # THE AUDITED REFUSAL (Q3's law at the rerun door): the
            # refusal's own row — the counts ON IT, agreeing with the
            # per-key rows the granted calls wrote (the half-rerun is on
            # the record, H-C5); the ROWS count rides beside the keys
            # (the map-key retry's honest census).
            await _audit_rerun_refusal(
                pool,
                schema=schema,
                principal=principal,
                action="workflow.rerun",
                target_type="workflow_run",
                target_id=str(run_id),
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
                # THE HALF-RERUN'S HONEST BANNER (H-C5): the run was
                # PARTLY re-opened before the refusal — the operator
                # hears the counts, never a clean redirect (the audit
                # row names the refusal's kind — the race included).
                return _run_redirect(
                    base_path,
                    run_id,
                    banner=rerun_partial_banner(rows_reopened, keys_applied, len(failed_keys)),
                )
            if raced:
                # THE RACED LOSER'S OWN BANNER: "the run is cancelled"
                # would LIE for a deadlock — the race's loser answers the
                # raced banner (the remedy: reload, re-check, re-run).
                return _run_redirect(base_path, run_id, banner=RERUN_RACED_BANNER)
            return _run_redirect(base_path, run_id, banner=RERUN_REFUSED_BANNER)
        if keys_applied == 0:
            return _run_redirect(base_path, run_id, banner=RERUN_NOT_APPLIED_BANNER)
        return _run_redirect(base_path, run_id)
