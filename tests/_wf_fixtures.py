"""Fixtures + seed helpers for the workflow pin suites, registered from the
root conftest.

These fixtures live here instead of ``tests/workflows/conftest.py`` for the
same reason ``tests/web_admin/_fixtures.py`` does: pytest 9.1.1 drops a
nested conftest's fixtures for a file revisited non-adjacently in the
argument list (pytest-dev/pytest#14971). The workflow pins span four files
(the engine's finalize/sweep/fork families, the schema's structural +
band families, the ledger family), so the seed helpers and the red-log
sink live here ONCE — a fixture duplicated across two files is a seam owed
now, and this is the seam's home.

Every helper drives the REAL engine surfaces (``taskq.workflows``); the
seed shapes are the flow-row/join-row/running-node representations the
engine's own statements read back.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import AsyncIterator, Callable, Iterator
from hashlib import sha256
from pathlib import Path
from typing import Any, Final, cast
from uuid import (
    uuid4,  # noqa: TID251  # Why: the redlog RUN ID is deliberately NOT a persisted id — no B-tree, no ordering; randomness is the point (attribution token).
)

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing._claims import load_claims, record_claim
from taskq.workflows import Promise
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.engine import render_workflow_sql

MEASUREMENTS = Path(__file__).parent.parent / ".measurements"


def runtime_refusal_builder(fn: Callable[[], object]) -> Callable[[], Promise[object]]:
    """THE NEGATIVE PROBE'S NAMED SEAM (the typed door's runtime face):
    the ``@app.workflow`` decorator takes ``Callable[[], Promise[R]]`` —
    a build function whose runtime return is NOT a promise (a bare dict,
    a bare ``None``) is refused STATICALLY at the decoration site (the
    probe corpus pins that refusal). The RUNTIME refusals those lying
    declarations used to reach — the build door's "the return IS the
    terminal promise" ``TypeError``, the E2 produced-never-consumed
    verdict — are still runtime behavior worth pinning: the probes
    construct the static-impossible shape through THIS seam, one cast,
    named and commented, so the checker sees the lie typed and the
    runtime door keeps its pin."""
    return cast("Callable[[], Promise[object]]", fn)


def source_changes_since(head_sha_value: str) -> bool:
    """THE OFF-BY-ONE RULE (the head-stamp verifier's own, mirrored for
    the in-suite guards): whether ANY commit since *head_sha_value*
    touched anything OUTSIDE the measurements estate. A claim recorded
    on H is fresh at H2 when H2 is H plus measurement-only changes —
    the source tree the claim verifies is content-identical. An
    unresolvable claimed head (not an ancestor) = stale by definition."""
    import subprocess

    proc = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            f"{head_sha_value}..HEAD",
            "--",
            ".",
            ":(exclude).measurements",
        ],
        cwd=Path(__file__).parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return True
    return bool(proc.stdout.strip())


def head_sha() -> str:
    """THE HEAD-STAMP LAW (the evidence-integrity round, cure 5): every
    evidence artifact RECORDS ITS HEAD — the sha the verification
    (``scripts/verify_evidence_heads.py``) re-runs the artifact against.
    An unstamped capture is a rumor; a stale one is history, never the
    live claim."""
    import subprocess

    out = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).parent.parent,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return out.strip()


#: The provenance law's version marker — a sink record stamped with this
#: law carries FULLY-CITED entries (run + capture path + sha256); the
#: guard verifies every record stamped with it and grandfathered NOTHING
#: forward: a pre-law record is history, never the live claim.
REDLOG_LAW = "provenance-1"


def write_band_artifact(name: str, payload: dict[str, object]) -> Path:
    """THE APPEND-ONLY CONVERSION (the receipts law): a band artifact is
    a RUN-SCOPED file, never a write_text-in-place name. The convicted
    defect: every run rewrote the SAME file whole — two concurrent runs'
    writes tore each other (the 11 torn rows the ledgers' union found),
    and a partial run's numbers falsified the recorded band (the newest
    subset is not the newest measurement). Each run writes
    ``runs/{stem}-{timestamp}-{token}.json`` — AND APPENDS THE CLAIM to
    the registry (``CLAIMS.json``; :func:`record_claim`) — the newest
    CITED (:func:`latest_band_artifact`) is the record the report reads.
    The filename convention stays for the humans; the claim lives in the
    manifest (the machinery never parses the name)."""
    runs = MEASUREMENTS / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    stem = name.removesuffix(".json")
    path = runs / f"{stem}-{time.strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:4]}.json"
    record = dict(payload)
    # THE HEAD-STAMP LAW: the artifact carries the head it was measured
    # on — the verification (scripts/verify_evidence_heads.py) re-runs it
    # on its claimed head.
    record.setdefault("head_sha", head_sha())
    path.write_text(json.dumps(record, indent=2, default=str))
    # THE CLAIMS REGISTRY: the manifest is the estate's index — the
    # verifier reads it, never the filename's run-scoped tail.
    record_claim(stem=stem, file=path.name, head_sha=str(record["head_sha"]))
    return path


def latest_band_artifact(stem: str) -> Path | None:
    """The newest CITED: the newest run-scoped capture for *stem* (the
    registry's append-order tail for the stem — the record a report
    cites). Read off the CLAIMS manifest, never off the filenames."""
    claims = [c for c in load_claims() if c.get("stem") == stem]
    for claim in reversed(claims):
        candidate = MEASUREMENTS / "runs" / claim.get("file", "")
        if candidate.is_file():
            return candidate
    return None


# ── THE LOADED BAR (the load-flake band's discipline — finding 3's cure) ─

#: The bar: ambient load (1-min run-queue average) per core at or below
#: which the box counts QUIET and a wall-clock band may assert honestly.
#: Above it, an absolute wall-clock bound measures the NEIGHBORS, not the
#: code under test — the pin that greened solo and flaked loaded (the
#: census's own record: "green solo x2" while the box sat at 61/72).
LOADED_BAR_PER_CORE: Final[float] = 0.5

#: The loaded round's honesty scale: a deadline polled under load stretches
#: by at most this factor (the box at 2x saturation gets 2x the bound —
#: condition-not-clock: the CONDITION scales the clock, never a fixed
#: number pretending the machine is quiet).
LOADED_SCALE_MAX: Final[float] = 4.0


def load_bar() -> float:
    """The box's ambient load, normalized per core (the loaded bar's
    reading). Condition-not-clock: an absolute wall-clock band asserts
    only when THIS reading is quiet."""
    import os

    return os.getloadavg()[0] / (os.cpu_count() or 1)


def loaded_scale() -> float:
    """The polling scale the current load earns: 1.0 on a quiet box,
    growing linearly to :data:`LOADED_SCALE_MAX` at 4x saturation. A
    poll's deadline multiplies by this — the poll waits on STATE
    (condition-not-clock), and the wait's BOUND acknowledges the box it
    runs on (state-not-sleep: the poll reacts to the observed state each
    tick, never a fixed sleep pretending to know when the state lands)."""
    bar = load_bar()
    if bar <= LOADED_BAR_PER_CORE:
        return 1.0
    return min(1.0 + (bar - LOADED_BAR_PER_CORE), LOADED_SCALE_MAX)


def band_assertion_allowed() -> bool:
    """Whether an ABSOLUTE wall-clock band may assert on this box, now:
    the loaded-bar gate. A loaded box waives the absolute bound honestly
    (the artifact records the waiver and the reading) — the RELATIVE
    drills (red vs. green) assert unconditionally, load-invariantly."""
    return load_bar() <= LOADED_BAR_PER_CORE


#: The terminal-status SQL set — the statement-side literal the engine's
#: guards spell (the twin of statemachine.TERMINAL_STATUSES); the pins'
#: convicted variants re-spell it.
TERMINAL_SQL = "('succeeded','failed','cancelled','crashed','abandoned')"


class RedLog:
    """The red-output sink: a file that gets READ (BUILD-PROTOCOL §2).
    Every pin's convicted variant appends its observed dragon here; the
    fixture flushes on teardown.

    THE PRESERVATION LAW (attack-3's hygiene finding, the fixer's own
    lane): the evidence sinks are APPEND-ONLY and RUN-SCOPED — flush()
    appends ONE JSONL record (``{"run": …, "entries": [...]}``) and
    never rewrites the file. The convicted defect: ``write_text``
    rewrote the WHOLE sink per run, so a PARTIAL run (one pin file
    re-run in isolation) replaced the corpus with ONLY that subset's
    entries — a full run's evidence silently deleted, the recorded band
    numbers falsified by whichever subset ran last. Append-only: a
    partial run adds its own run-scoped record; history is never
    truncated. The run id (pid + a token + the timestamp) makes each
    record attributable.

    THE PROVENANCE LAW (the evidence-integrity round, cure 3): every
    entry cites a RECEIPT CHAIN — (a) its drill's run-id, (b) the
    drill's captured output FILE PATH, (c) a sha256 of that file. The
    capture is written at RECORD time (the drill's own observation,
    machine-written, in the run's own capture directory); the guard
    (:mod:`tests.test_fv_redlog_guard`) re-verifies the chain at guard
    time — a cited capture that is missing or mismatching FAILS the
    guard. The law REPLACES the syntactic shape rule (the AST
    literal-only scan): that guard was defeatable twice over (a
    keyword-form payload skipped inspection; a name-laundered
    module-level constant walked past it) — the receipt chain inspects
    the SINK, never the call's shape, and convictions are by provenance.

    The captures' home (``.measurements/redlog-captures/<run>/``) is
    deliberately UNIGNORED-adjacent residue: ``.measurements/`` is
    gitignored, the sinks live there as run records, and the guard
    verifies captures of records still alive on this tree.
    """

    def __init__(self, filename: str) -> None:
        self._filename = filename
        self._run_id = f"{time.strftime('%Y%m%dT%H%M%S')}-{os.getpid()}-{uuid4().hex[:8]}"
        self.entries: list[dict[str, str]] = []

    def red(self, pin: str, mutation: str, observed: Any) -> None:
        """Record one convicted red WITH its receipt: the entry carries
        the run-id, the drill's captured output file path, and the
        capture's sha256 — the chain the guard verifies."""
        capture_dir = MEASUREMENTS / "redlog-captures" / self._run_id
        capture_dir.mkdir(parents=True, exist_ok=True)
        sanitized = "".join(c if c.isalnum() or c in "-_" else "_" for c in pin) or "pin"
        capture = capture_dir / f"{sanitized}.json"
        payload = {
            "pin": pin,
            "mutation": mutation,
            "observed": observed,
            "run": self._run_id,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        capture.write_text(json.dumps(payload, indent=2, default=str, sort_keys=True))
        digest = sha256(capture.read_bytes()).hexdigest()
        self.entries.append(
            {
                "pin": pin,
                "mutation": mutation,
                "red": json.dumps(observed, default=str),
                "run": self._run_id,
                "capture": str(capture.relative_to(MEASUREMENTS)),
                "sha256": digest,
            }
        )

    def flush(self) -> None:
        MEASUREMENTS.mkdir(exist_ok=True)
        record = {
            "run": self._run_id,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "law": REDLOG_LAW,
            # THE HEAD-STAMP LAW: the sink record carries the head the
            # drills ran on (the receipt chain's fourth link — the run
            # is attributable to a TREE, not just a process).
            "head_sha": head_sha(),
            "entries": self.entries,
        }
        with (MEASUREMENTS / self._filename).open("a") as sink:
            sink.write(json.dumps(record) + "\n")


@pytest.fixture
def engine_redlog() -> Iterator[RedLog]:
    """The red sink for the engine pins (one file, read by the gate)."""
    log = RedLog("pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def ledger_redlog() -> Iterator[RedLog]:
    """The red sink for the ledger pins."""
    log = RedLog("ledger-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def hitl_redlog() -> Iterator[RedLog]:
    """The red sink for the T10 HITL pins (the convicted variants)."""
    log = RedLog("t10-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def loop_redlog() -> Iterator[RedLog]:
    """The red sink for the T19 loop pins (the mutation drills' reds)."""
    log = RedLog("t19-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def propagation_redlog() -> Iterator[RedLog]:
    """The red sink for the T06 propagation pins (the phase-2 family)."""
    log = RedLog("t06-propagation-reds.json")
    yield log
    log.flush()


@pytest.fixture
def t20_redlog() -> Iterator[RedLog]:
    """The red sink for the T20 streaming pins (the convicted variants)."""
    log = RedLog("t20-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def progress_redlog() -> Iterator[RedLog]:
    """The red sink for the T21 progress pins (the convicted variants)."""
    log = RedLog("t21-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def createseam_redlog() -> Iterator[RedLog]:
    """The red sink for the create-seam pins (the create's atomicity, the
    run-key claim's honesty, the root-marker fence, the reap belt, the
    packaged run)."""
    log = RedLog("createseam-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def wedge_redlog() -> Iterator[RedLog]:
    """The red sink for the crashed-terminal wedge pins (the pre-cure
    tree's corpse-greens: the reconstruction's running, the maintenance
    pass's non-heal, the G7 blind spot)."""
    log = RedLog("wedge-pin-reds.json")
    yield log
    log.flush()


#: The G7 always-on assertion's mapping: the §17.5 derivation's workflow
#: status → the flow ROOT row's job_status (the root's legal vocabulary).
#: The mapping is the TERMINAL states' expectation; the LAW is stated in
#: :func:`g7_check` (the root row is a cache, and each terminal root has
#: exactly one writer whose semantics decide what it may claim).
G7_DERIVED_TO_ROOT: dict[str, str] = {
    "complete": "succeeded",
    "failed": "failed",
    "cancelled": "cancelled",
    "blocked": "running",
    "running": "running",
    "pending": "pending",
}


@pytest.fixture
async def wf_g7_status_truth(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> AsyncIterator[None]:
    """G7 (T08): the ALWAYS-ON metamorphic assertion — at test end, the
    REPORTED workflow status (the flow root row's own status, the
    linearization point the engine's maintenance leg writes) equals the
    status RECONSTRUCTED FROM ROWS ALONE (the §17.5 derivation over the
    node rows + the ledger — D4's two-source rule). Registered for every
    workflow integration test via the collection hook (tests/conftest.py
    adds it to the wf-family files); catches status-drift continuously,
    not just in the dedicated pin's scenarios. A deliberately-lying
    fixture status (a root hand-written to a state the rows cannot
    derive) reds the suite (the drill pin proves the teeth)."""
    yield
    await g7_check(wf_conn, wf_schema, wf_sql)


async def g7_check(wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql) -> None:
    """The G7 assertion's body (one home — the fixture and the teeth-drill
    pin both run THIS, never a re-spelled copy).

    THE LAW (the cache's semantics, stated from each terminal root's
    writer — the phase-2 attack's H4 round made this check ALWAYS-ON for
    real, so the law must red lies and never red the flips' windows):

    * the root row is a CACHE of the derivation;
    * a root claiming 'succeeded' is written by ONE writer only — the
      maintenance leg's complete branch (all rows terminal, none failed,
      none cancelled) — so the rows MUST derive 'complete'. A
      prematurely-complete root over rows that derive anything else is
      THE status-cache lie this check exists to catch (the teeth pin's
      own lie is this shape);
    * a root claiming 'failed' may never contradict a COMPLETED run (the
      cascade and the maintenance leg both write it from a non-absorbed
      failure on the rows; the rows cannot un-fail);
    * a root claiming 'cancelled' is the cancel arm's linearization
      point — the rows may lag it in every direction (a straggler child
      terminal-failing after the flip derives 'failed'; the flip
      stands) — nothing to assert against;
    * a LIVE root ('running'/'pending') may lag the rows' terminal
      verdict by one sweep pass — the maintenance leg's lag window (the
      H1 cure heals it in one pass; the H1 pin carries that teeth).

    THE WEDGE LEG (the crashed-terminal wedge's red-drill — the
    attacker's proof was G7 staying green ON THE CORPSE): a live root
    whose rows reconstruct a TERMINAL verdict contradicts the rows.
    The lag window is ONE pass, so the check RUNS the maintenance leg
    itself (the same statement the sweep drives) and re-reads: the
    reported state must now equal the rows' terminal verdict. A root
    that still disagrees is THE WEDGE — a run the reported state claims
    is live while the rows say it terminalized and nothing will ever
    change either side (the corpse that reports 'running' forever). The
    drill pin (tests/test_wf_t20_crashed_terminal_wedge.py) reds this
    leg against a no-op maintenance mutant — the assertion can fail on
    a wedge, it is not a decoration."""
    from taskq.workflows._status import reconstruct_workflow_status

    flows = await wf_conn.fetch(
        f"SELECT id, status FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__'"
    )
    for flow in flows:
        reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(flow["id"]))
        status = flow["status"]
        if status == "succeeded":
            assert reconstructed == "complete", (
                f"the reported status drifted from the rows: flow {flow['id']} "
                f"reports 'succeeded' but the rows reconstruct "
                f"{reconstructed!r} — the status cache has arrived (G7): a "
                "prematurely-complete root is written by nothing but a "
                "cache"
            )
        elif status == "failed":
            assert reconstructed != "complete", (
                f"the reported status drifted from the rows: flow {flow['id']} "
                f"reports 'failed' but the rows reconstruct 'complete' — "
                "the wrong verdict on a completed run (G7)"
            )
        elif status in ("running", "pending") and reconstructed in (
            "failed",
            "cancelled",
            "complete",
        ):
            # THE WEDGE LEG: the rows have spoken a TERMINAL verdict; the
            # lag window is one sweep pass — run it, re-read, and the
            # root must agree. (The maintain statement's own derivation
            # re-derives the root from the node rows — a disagreement
            # AFTER the pass is the two derivations' drift or a wedging
            # sweep.) ONE HONEST LAG SURVIVES the leg: the §17.5
            # cancelled/failed PRECEDENCE derives a terminal verdict OVER
            # live rows (a fired-but-unclaimed join row is claimable
            # work the derivation's cancelled row outranks) — the root
            # legitimately waits for it. THE WEDGE is the ALL-TERMINAL
            # corpse: no live-class row exists anywhere and the root
            # still reports live — nothing will ever change either side.
            await wf_conn.execute(wf_sql.workflow_root_maintain, 200)
            healed = await wf_conn.fetchval(
                f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow["id"]
            )
            expected = G7_DERIVED_TO_ROOT[reconstructed]
            if healed == expected:
                continue
            remaining = await wf_conn.fetchval(
                f"""SELECT count(*) FROM "{wf_schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1
                      AND metadata ? 'flow_id'
                      AND step_key <> '__flow__'
                      AND status IN ('pending', 'scheduled', 'running')""",
                flow["id"],
            )
            if remaining:
                # THE HONEST PRECEDENCE LAG: the terminal verdict derives
                # OVER live rows (the fired-but-unclaimed join is
                # claimable work) — the root waits for them, the law's
                # lag window covers it.
                continue
            assert healed == expected, (
                f"THE WEDGE (G7's terminal-contradiction leg): flow "
                f"{flow['id']} reports {healed!r} while the rows "
                f"reconstruct {reconstructed!r} with NO live-class row "
                f"left on the run — a TERMINAL verdict the one-pass heal "
                f"(the maintenance leg) did not land. The run's reported "
                "state contradicts its own rows AND the heal: a wedged "
                "run — the operator must notice forever (the "
                "crashed-terminal wedge's signature: the corpse reports "
                "'running' over a deterministic death)"
            )


@pytest.fixture
async def wf_conn(clean_pg_conn: asyncpg.Connection) -> asyncpg.Connection:
    """The per-test clean connection on the module's migrated schema."""
    return clean_pg_conn


@pytest.fixture
def wf_schema(module_pg_schema: Any) -> str:
    return module_pg_schema.schema_name


@pytest.fixture
async def wf_pool(module_pg_schema: Any) -> AsyncIterator[asyncpg.Pool]:
    """The RUNNER's pool (T09): the flow runner acquires from a pool (its
    claims + finalizes); the module's DSN builds it — one pool per test,
    closed at teardown. The loop pins (T19) share it (a fixture
    duplicated across two files is a seam owed NOW — this is the
    seam's home, beside the other wf fixtures)."""
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    yield pool
    await pool.close()


@pytest.fixture
def wf_sql(module_pg_schema: Any) -> WorkflowSql:
    """The workflow statement bundle rendered for the module's schema."""
    return render_workflow_sql(module_pg_schema.schema_name)


class FlowStandIn:
    """The registered-definition stand-in (T09's API formalizes it; the
    run-key claim needs the placement fields only)."""

    def __init__(self, name: str = "ledger-flow") -> None:
        self.name = name
        self.actor = "wf"
        self.queue = "default"
        self.max_attempts = 3
        self.retry_kind = "transient"
        self.payload: dict[str, object] = {"flow": name}
        self.trace_id: str | None = None


async def seed_flow(
    conn: asyncpg.Connection, schema: str, *, status: str = "running", workflow: str | None = None
) -> JobId:
    """The flow-run row: a jobs row, step_key = the entry marker, the run
    scope. Its status IS the run's status (the linearization point).
    ``workflow`` stamps the root's metadata with the workflow's registered
    name — the sweep's fire arm resolves a healed join's reducer body from
    that REGISTERED DEFINITION (the durable, cross-process leg; the
    process-local memo is a cache). The stamp is what ``insert_flow_run``
    writes for a named flow; the seed passes it through so the pins drive
    the shipped shape."""
    flow_id = new_uuid()
    metadata: dict[str, object] = {"flow_id": str(flow_id)}
    if workflow:
        metadata["workflow"] = workflow
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', $2, "
        "'__flow__', $4::jsonb, 'workflow-run', $3)",
        flow_id,
        status,
        f"flow:{flow_id}",
        json.dumps(metadata),
    )
    return JobId(flow_id)


async def seed_join(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    step_key: str = "join",
    deps: int = 1,
    scheduled_in: float | None = None,
    consumers: list[dict[str, object]] | None = None,
) -> JobId:
    """A join-wait row: pending + deps_pending = deps + blocking_reason
    join. ``scheduled_in`` seconds in the future makes it a HELD row (P3
    rule 1's representation: the signal deadline is the only live timer)."""
    join_id = new_uuid()
    # VALUES expressions cannot reference the row's own columns -- the due
    # case is now() minus an hour, not the column minus an hour.
    scheduled_sql = (
        "now() - interval '1 hour'"
        if scheduled_in is None
        else f"now() + interval '{scheduled_in} seconds'"
    )
    meta: dict[str, object] = {"flow_id": str(flow_id), "blocking_reason": "join"}
    if consumers:
        meta["consumers"] = consumers
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, deps_pending, metadata, scheduled_at) "
        f"VALUES ($1, 'wf', 'default', '{{}}', 3, 'transient', 'pending', $2, $3, "
        f"$4::jsonb, {scheduled_sql})",
        join_id,
        step_key,
        deps,
        json.dumps(meta),
    )
    return JobId(join_id)


async def seed_edge(
    conn: asyncpg.Connection, schema: str, child_id: JobId, parent_id: JobId, flow_id: JobId
) -> None:
    """One edge-ledger row (the join counter's ONLY truth source)."""
    await conn.execute(
        f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
        child_id,
        parent_id,
        flow_id,
    )


async def seed_running_node(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, *, step_key: str = "a"
) -> JobId:
    """A running node: the finalize fence's admitted shape (its claim view:
    a fresh worker, attempt 1, epoch 0)."""
    node_id = new_uuid()
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, locked_by_worker, lock_expires_at, "
        "claim_epoch, step_key, metadata) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 1, $2, "
        "now() + interval '90 seconds', 0, $3, $4)",
        node_id,
        worker_id,
        step_key,
        json.dumps({"flow_id": str(flow_id)}),
    )
    return JobId(node_id)


async def claim_view(
    conn: asyncpg.Connection, schema: str, node_id: JobId
) -> tuple[JobId, int, int]:
    """The node's CURRENT claim view (worker, attempt, epoch) — what its
    own finalize presents to the fence."""
    rec = await conn.fetchrow(
        f'SELECT locked_by_worker, attempt, claim_epoch FROM "{schema}".jobs WHERE id = $1',
        node_id,
    )
    assert rec is not None and rec["locked_by_worker"] is not None
    return JobId(rec["locked_by_worker"]), rec["attempt"], rec["claim_epoch"]


async def node_state(conn: asyncpg.Connection, schema: str, node_id: JobId) -> dict[str, Any]:
    """The node row's engine-relevant columns (metadata jsonb arrives as
    str on un-coded connections — decoded here)."""
    rec = await conn.fetchrow(
        f'SELECT status, deps_pending, metadata, result FROM "{schema}".jobs WHERE id = $1',
        node_id,
    )
    assert rec is not None
    return {
        "status": rec["status"],
        "deps_pending": rec["deps_pending"],
        "metadata": rec["metadata"]
        if isinstance(rec["metadata"], dict)
        else json.loads(rec["metadata"] or "{}"),
        "result": rec["result"],
    }


async def fire_count(conn: asyncpg.Connection, schema: str, join_id: JobId) -> int:
    """How many fire rows the join has (exactly one = exactly-once)."""
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_join_fire WHERE join_job_id = $1',
            join_id,
        )
    )
