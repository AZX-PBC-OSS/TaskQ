"""FULL-STACK compose E2E: the deployment-level proof for examples/docker-compose.yml.

The per-fixture testcontainers suites prove TaskQ's internals against
throwaway services; nothing stands up the EXAMPLE STACK itself.  This module
does exactly that: ``docker compose -f examples/docker-compose.yml up -d
--build`` (its postgres + redis + two workers + trigger app + admin sidecar),
readiness by the stack's own observables, then the whole deployment loop:

1. enqueue through the trigger app's HTTP seam (``POST /enqueue/counter``),
2. enqueue through the app's own client shape - a ``TaskQ`` client built
   against the compose postgres (published port) exactly the way
   ``examples/app.py`` builds one - via the ``summer`` actor,
3. assert the COMPOSED workers drive every job to ``succeeded`` in the
   database the stack owns (read through the postgres container),
4. assert the admin sidecar's job list shows the completed jobs and its
   count endpoint agrees,
5. tear down with ``down -v`` and prove the label sweep finds no leftovers.

The compose file is the source of truth: if it is out of date with the code,
these tests fail and the fix belongs in the compose file (a broken example
stack is a broken window in the operator's face).

Run with::

    uv run pytest tests/test_compose_stack_e2e.py -v

Requires Docker.  Module-scoped lifecycle: one stack for the whole module,
built on first use, torn down (and proven torn down) at module exit.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import TypeAdapter

from taskq import JobId, TaskQ
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker

pytestmark = [
    pytest.mark.integration,
    # The first test's setup bears the cold image build (uv sync inside the
    # Dockerfile); the default 300s per-test timeout is not enough for it.
    pytest.mark.timeout(900),
]

_REPO_ROOT = Path(__file__).resolve().parents[1]
_COMPOSE_FILE = _REPO_ROOT / "examples" / "docker-compose.yml"

# The compose project label docker stamps on every container/network/volume it
# creates - the deployment-level analogue of creator_labels() for testcontainers.
_PROJECT_LABEL = "com.docker.compose.project"

# A counter job sleeps 1s per step (examples/actors/basic.py); n=2 keeps the
# batch short while still exercising progress + the real dispatch loop.
_COUNTER_N = 2
_COUNTER_JOBS = 6
_SUMMER_VALUES = ("11,22", "1000,33")
_SUMMER_EXPECTED = [33, 1033]


@pytest.fixture(scope="module")
def compose_schema() -> str:
    """The deployed stack's OWN database/schema name - fixed by the deployment.

    examples/docker-compose.yml sets ``POSTGRES_DB: taskq`` and no
    ``TASKQ_SCHEMA_NAME``, so every service resolves the library default: the
    database AND the TaskQ schema are both the deployment's fixed ``taskq`` -
    not a per-test marker. The value lives in this fixture rather than a
    module-level constant (the suite-hygiene pin bans shared schema
    constants) while staying the one honest source for the name the stack
    under test is pinned to.
    """
    return "taskq"


# ── Subprocess / docker helpers ─────────────────────────────────────────────


def _free_port() -> int:
    """Grab a free TCP port from the kernel (released before compose binds it)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run(cmd: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)  # noqa: S603


def _tail(text: str, lines: int = 40) -> str:
    return "\n".join(text.splitlines()[-lines:])


def _compose_base_cmd(project: str, override: Path) -> list[str]:
    return [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(_COMPOSE_FILE),
        "-f",
        str(override),
    ]


def _leftovers_for(project: str) -> dict[str, list[str]]:
    """Everything docker still holds for this compose project, by its label."""
    found: dict[str, list[str]] = {}
    for kind, cmd in (
        ("containers", ["docker", "ps", "-a", "-q"]),
        ("volumes", ["docker", "volume", "ls", "-q"]),
        ("networks", ["docker", "network", "ls", "-q"]),
    ):
        result = _run([*cmd, "--filter", f"label={_PROJECT_LABEL}={project}"], timeout=30)
        found[kind] = [line for line in result.stdout.split() if line]
    return found


def _poll_until(desc: str, probe: Callable[[], object], *, timeout: float) -> Any:
    """Poll until probe() returns a truthy value, failing with the description."""
    deadline = time.monotonic() + timeout
    last: object = None
    while time.monotonic() < deadline:
        last = probe()
        if last:
            return last
        time.sleep(1.0)
    pytest.fail(f"timed out after {timeout}s waiting for {desc}; last probe: {last!r}")


def _http_get_ok(url: str) -> bool:
    try:
        with httpx.Client(timeout=5.0, follow_redirects=True) as client:
            return client.get(url).status_code == 200
    except httpx.HTTPError:
        return False


# ── The stack under test ────────────────────────────────────────────────────


@dataclass
class ComposeStack:
    """A live examples-compose deployment and the seams to drive it."""

    project: str
    compose_cmd: list[str]
    app_port: int
    admin_port: int
    pg_port: int
    schema: str

    @property
    def app_url(self) -> str:
        return f"http://127.0.0.1:{self.app_port}"

    @property
    def admin_url(self) -> str:
        return f"http://127.0.0.1:{self.admin_port}/admin"

    @property
    def pg_dsn(self) -> str:
        return f"postgresql://taskq:taskq@127.0.0.1:{self.pg_port}/{self.schema}"

    def compose(self, *args: str, timeout: float = 600.0) -> subprocess.CompletedProcess[str]:
        result = _run([*self.compose_cmd, *args], timeout=timeout)
        if result.returncode != 0:
            pytest.fail(
                f"compose {' '.join(args)} failed (rc={result.returncode}):\n"
                f"{_tail(result.stdout)}\n{_tail(result.stderr)}"
            )
        return result

    def psql(self, sql: str) -> str:
        """Run SQL through the stack's own postgres container and return the rows."""
        result = self.compose(
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "taskq",
            "-d",
            self.schema,
            "-At",
            "-c",
            sql,
            timeout=60.0,
        )
        return result.stdout.strip()


def _override_file(path: Path, app_port: int, admin_port: int, pg_port: int) -> None:
    """Write the test-only compose override.

    ``!override`` replaces the base file's port lists (otherwise compose merges
    both mappings and 8000/8001 stay published - this remap keeps the module
    collision-free under xdist/parallel runs); the creator labels MERGE into
    every service so the shared-daemon sweep rules recognize the stack as ours.
    The postgres port is published ONLY for the test process's ``TaskQ`` client
    (the app.py client shape against the compose postgres); the base file
    publishes none.
    """
    creator_pair = "\n".join(f'    "{k}": "{v}"' for k, v in creator_labels().items())
    path.write_text(
        "x-creator-labels: &creator_labels\n"
        "  labels:\n"
        f"{creator_pair}\n"
        "\n"
        "services:\n"
        "  redis: *creator_labels\n"
        "  worker-1: *creator_labels\n"
        "  worker-2: *creator_labels\n"
        "  app:\n"
        "    <<: *creator_labels\n"
        "    ports: !override\n"
        f'      - "{app_port}:8000"\n'
        "  admin:\n"
        "    <<: *creator_labels\n"
        "    ports: !override\n"
        f'      - "{admin_port}:8001"\n'
        "  postgres:\n"
        "    <<: *creator_labels\n"
        "    ports: !override\n"
        f'      - "{pg_port}:5432"\n'
    )


# ── Module-scoped lifecycle ─────────────────────────────────────────────────


@pytest.fixture(scope="module")
def compose_stack(
    tmp_path_factory: pytest.TempPathFactory, compose_schema: str
) -> Iterator[ComposeStack]:
    """Stand up the examples compose stack, wait for its own observables, tear it down.

    The stack boots through the compose CLI against a UNIQUE project name (so
    parallel runs never collide).  Teardown is ``down -v --remove-orphans``
    plus a label-sweep assertion - the deployment-level counterpart of the
    testcontainers cleanup discipline.
    """
    skip_test_without_docker()

    project = f"taskq-compose-e2e-{os.getpid()}"
    override_path = tmp_path_factory.mktemp("compose-e2e") / "compose.e2e-override.yml"
    app_port, admin_port, pg_port = _free_port(), _free_port(), _free_port()
    _override_file(override_path, app_port, admin_port, pg_port)

    stack = ComposeStack(
        project=project,
        compose_cmd=_compose_base_cmd(project, override_path),
        app_port=app_port,
        admin_port=admin_port,
        pg_port=pg_port,
        schema=compose_schema,
    )

    try:
        result = _run([*stack.compose_cmd, "up", "-d", "--build"], timeout=900.0)
        if result.returncode != 0:
            logs = _run([*stack.compose_cmd, "logs", "--tail", "50"], timeout=60.0)
            pytest.fail(
                "examples/docker-compose.yml FAILED to come up - the example stack "
                "is broken for the operator:\n"
                f"{_tail(result.stdout)}\n{_tail(result.stderr)}\n"
                f"stack logs:\n{_tail(logs.stdout)}"
            )

        # Readiness by the stack's own observables:
        #   1. the trigger app serves (lifespan done = migrations applied),
        _poll_until(
            "the trigger app to serve GET /",
            lambda: _http_get_ok(stack.app_url),
            timeout=180.0,
        )
        #   2. the admin sidecar serves,
        _poll_until(
            "the admin sidecar to serve GET /admin/",
            lambda: _http_get_ok(stack.admin_url),
            timeout=120.0,
        )
        #   3. BOTH composed workers heartbeat into the stack's own DB.
        _poll_until(
            "both composed workers to register in taskq.workers",
            lambda: (
                stack.psql(
                    "SELECT count(*) FROM taskq.workers "
                    "WHERE last_seen_at > now() - interval '30 seconds'"
                )
                == "2"
            ),
            timeout=180.0,
        )

        yield stack
    finally:
        _run([*stack.compose_cmd, "down", "-v", "--remove-orphans"], timeout=300.0)
        remaining = _leftovers_for(project)
        assert not any(remaining.values()), (
            "compose teardown left project leftovers - the label sweep must come "
            f"back empty: {remaining}"
        )


@dataclass
class CompletedJobs:
    """The module's workload: jobs enqueued through both real seams, then terminal."""

    counter_ids: list[str]
    summer_ids: list[str]


def _uuid_array(job_ids: list[str]) -> str:
    return ",".join(f"'{job_id}'" for job_id in job_ids)


@pytest.fixture(scope="module")
async def completed_jobs(compose_stack: ComposeStack) -> CompletedJobs:
    """Enqueue through the deployment's real seams and wait for ALL jobs to go terminal.

    Two seams:
    - the trigger app's HTTP API (``POST /enqueue/{actor}`` over the published
      port) - the browser/operator-facing path,
    - a ``TaskQ`` client in THIS process against the compose postgres, built
      exactly the way ``examples/app.py`` builds its client (dsn + schema).
    """
    counter_ids: list[str] = []

    async with httpx.AsyncClient(timeout=30.0) as http:
        for _ in range(_COUNTER_JOBS):
            response = await http.post(
                f"{compose_stack.app_url}/enqueue/counter", data={"n": str(_COUNTER_N)}
            )
            # F3's one envelope: a plain enqueue answers 201 {"job_id","url"}.
            assert response.status_code == 201, response.text
            counter_ids.append(response.json()["job_id"])

    from examples.actors.advanced import SumPayload, SumResult, summer

    summer_ids: list[str] = []
    async with TaskQ(dsn=compose_stack.pg_dsn, schema=compose_stack.schema) as tq:
        for values in _SUMMER_VALUES:
            handle = await tq.enqueue(summer, SumPayload(values=values))
            summer_ids.append(str(handle.job_id))

    all_ids = counter_ids + summer_ids

    def _terminal_rows() -> list[str]:
        return compose_stack.psql(
            "SELECT id::text || '|' || status "
            f"FROM taskq.jobs WHERE id = ANY(ARRAY[{_uuid_array(all_ids)}]::uuid[])"
        ).splitlines()

    def _all_succeeded() -> object:
        rows = _terminal_rows()
        return rows if len(rows) == len(all_ids) and all("succeeded" in r for r in rows) else None

    # THE deployment-level wait: the COMPOSED workers (not this process) must
    # drive every job to a terminal state in the stack's own database.
    rows = _poll_until(
        f"all {len(all_ids)} enqueued jobs to reach a terminal state in the stack's DB",
        _all_succeeded,
        timeout=180.0,
    )
    assert isinstance(rows, list)

    for row in rows:
        job_id, status = row.split("|")[:2]
        assert status == "succeeded", job_id

    async with TaskQ(dsn=compose_stack.pg_dsn, schema=compose_stack.schema) as tq:
        summer_adapter: TypeAdapter[SumResult] = TypeAdapter(SumResult)
        for job_id, expected in zip(summer_ids, _SUMMER_EXPECTED, strict=True):
            # The result_adapter is the app.py shape: TypeAdapter(SumResult).
            handle = await tq.get(JobId(job_id), result_adapter=summer_adapter)
            assert handle is not None, job_id
            result = await handle.wait(timeout=30.0)
            assert result is not None and result.total == expected, job_id

    return CompletedJobs(counter_ids=counter_ids, summer_ids=summer_ids)


# ── Tests ───────────────────────────────────────────────────────────────────


def test_stack_boots_and_serves_the_operator_endpoints(compose_stack: ComposeStack) -> None:
    """The stack is up: app and admin serve, both workers are live in the DB."""
    stack = compose_stack

    with httpx.Client(timeout=30.0, follow_redirects=True) as http:
        index = http.get(stack.app_url)
        assert index.status_code == 200
        assert "counter" in index.text, "the trigger app's actor cards must render"

        admin = http.get(stack.admin_url)
        assert admin.status_code == 200
        assert "TaskQ Admin" in admin.text

    assert (
        stack.psql(
            "SELECT count(*) FROM taskq.workers WHERE last_seen_at > now() - interval '30 seconds'"
        )
        == "2"
    ), "both composed workers must be heartbeating"


def test_http_enqueued_jobs_all_reach_succeeded_through_the_composed_workers(
    compose_stack: ComposeStack,
    completed_jobs: CompletedJobs,
) -> None:
    """The whole loop: N HTTP enqueues -> N succeeded rows in the stack's DB."""
    stack = compose_stack
    ids = _uuid_array(completed_jobs.counter_ids)

    rows = stack.psql(
        "SELECT id::text || '|' || status || '|' || attempt::text || '|' || "
        "COALESCE(finished_at::text, '') "
        f"FROM taskq.jobs WHERE id = ANY(ARRAY[{ids}]::uuid[])"
    ).splitlines()
    assert len(rows) == _COUNTER_JOBS
    for row in rows:
        job_id, status, attempt, finished_at = row.split("|")
        assert status == "succeeded", job_id
        assert attempt == "1", job_id
        assert finished_at, job_id


def test_client_enqueued_typed_results_round_trip_through_the_composed_workers(
    completed_jobs: CompletedJobs,
) -> None:
    """A test-process TaskQ client gets its typed results back from composed workers."""
    assert len(completed_jobs.summer_ids) == len(_SUMMER_VALUES)


def test_admin_sidecar_shows_the_completed_jobs_and_counts_agree(
    compose_stack: ComposeStack,
    completed_jobs: CompletedJobs,
) -> None:
    """The admin sidecar's job list shows every completed job; its counts agree."""
    stack = compose_stack

    with httpx.Client(timeout=30.0, follow_redirects=True) as http:
        for actor_name, ids in (
            ("counter", completed_jobs.counter_ids),
            ("summer", completed_jobs.summer_ids),
        ):
            listing = http.get(
                f"{stack.admin_url}/jobs", params={"status": "succeeded", "actor": actor_name}
            )
            assert listing.status_code == 200
            for job_id in ids:
                assert job_id in listing.text, (
                    f"{job_id} must appear in the admin job list for actor={actor_name}"
                )

            counts = http.get(
                f"{stack.admin_url}/jobs/count",
                params={"status": "succeeded", "actor": actor_name},
            )
            assert counts.status_code == 200
            assert counts.json() == {"count": len(ids)}, actor_name


def test_compose_project_holds_exactly_the_expected_objects_midrun(
    compose_stack: ComposeStack,
) -> None:
    """Mid-run, docker holds ONLY this project's labeled objects (cleanup determinism)."""
    leftovers = _leftovers_for(compose_stack.project)
    assert set(leftovers) == {"containers", "volumes", "networks"}
    # 6 services + the compose default network + the pgdata volume; anything
    # else under this unique project label would mean the stack leaked.
    assert len(leftovers["containers"]) == 6
    assert len(leftovers["networks"]) == 1
    assert len(leftovers["volumes"]) == 1
