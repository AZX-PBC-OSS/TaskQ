"""Composition pin: the deps teardown runs after every consumer has finished.

``_main`` opens ``open_worker_deps`` around the ``asyncio.TaskGroup`` that
spawns the producer and every consumer, so the group's join completes
before the role pools close. ``shield_until_complete`` at the consumer
sites is only safe because of that nesting: a release that absorbs the
consumer's cancellation finishes its write while the pool is still open.
Invert the nesting and the same release races a pool closing underneath
it, which is the stranded-``running``-row shape the release arms exist to
prevent.

The other pins drive ``consume_one_job`` / ``_release_unstarted_claim``
against a hand-assembled deps and a double whose "pool closed" flag the
test sets itself. That proves the write is held through cancellation; it
cannot prove the teardown is ordered after the holder. This pin runs the
real ``_main`` bootstrap body and asserts the ordering at the one place it
is observable: the moment the deps context exits.

The sibling loops are the shapes ``test_worker_main``'s harness already
stubs (no database is involved); the deps teardown itself is the harness's
``open_worker_deps`` stand-in, wrapped here to record what it saw.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from taskq.settings import WorkerSettings
from taskq.worker._bootstrap import _main
from tests.conftest import unique_health_sock_path
from tests.test_worker_main import _use_test_harness


@pytest.fixture
def settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "30.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "10.0",
            "TASKQ_TERMINATION_GRACE_PERIOD": "60.0",
            "TASKQ_LOCK_LEASE": "45.0",
            "TASKQ_HEARTBEAT_INTERVAL": "5.0",
            "TASKQ_MAX_CONCURRENCY": "2",
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("composition_order"),
        }
    )


async def test_deps_teardown_runs_after_every_consumer_has_finished(
    settings: WorkerSettings,
) -> None:
    """``open_worker_deps`` must not exit while a consumer is still running.

    The consumers here finish a beat after the shutdown event fires, the
    shape every real consumer loop has (a terminal write, a deregister, the
    registry bookkeeping). With the deps context enclosing the group, the
    teardown cannot see them running: the join is what ends the block. With
    the nesting the other way round the teardown runs first and this fails,
    which is the regression the ordering exists for.
    """
    with _use_test_harness(settings, set_shutdown=False) as harness:
        import taskq.worker._bootstrap as bootstrap
        import taskq.worker.run as run_mod

        consumer_tasks: list[asyncio.Task[Any]] = []
        all_spawned = asyncio.Event()
        still_running_at_teardown: list[str] = []

        # The harness patched ``open_worker_deps`` with a mock; wrap the exit
        # it installed so the teardown can be observed without replacing it.
        # ``open_worker_deps`` is imported into ``_bootstrap`` from
        # ``taskq.worker.deps`` rather than re-exported, so reading it here is
        # the patched module attribute and not a public import; the ``Any``
        # cast is the harness's shape.
        mock_open: Any = bootstrap.open_worker_deps  # pyright: ignore[reportPrivateImportUsage]  # Why: the harness replaces this module attribute; it is not part of _bootstrap's public surface.
        inner_exit = mock_open.return_value.__aexit__

        async def _recording_exit(*args: object) -> Any:
            still_running_at_teardown.extend(
                str(task) for task in consumer_tasks if not task.done()
            )
            return await inner_exit(*args)

        mock_open.return_value.__aexit__ = _recording_exit

        # The harness's consumer fake parks on the shutdown event; this one
        # also takes a beat to unwind afterwards, so "the consumer is still
        # running" is the state at the moment of the teardown rather than a
        # scheduling accident.
        mock_consumer: Any = run_mod.consumer_loop_stub

        async def _unwinding_consumer(*args: object, **kwargs: object) -> None:
            task = asyncio.current_task()
            assert task is not None
            consumer_tasks.append(task)
            if len(consumer_tasks) == settings.max_concurrency:
                all_spawned.set()
            assert harness.shutdown_event is not None
            await harness.shutdown_event.wait()
            await asyncio.sleep(0.05)

        mock_consumer.side_effect = _unwinding_consumer

        main_task = asyncio.create_task(_main(settings))
        await asyncio.wait_for(all_spawned.wait(), timeout=30.0)
        assert harness.shutdown_event is not None
        harness.shutdown_event.set()
        result = await asyncio.wait_for(main_task, timeout=30.0)

    assert result == 0
    assert still_running_at_teardown == [], (
        "open_worker_deps closed while these consumer tasks were still running: "
        f"{still_running_at_teardown}. The deps teardown must be ordered after the "
        "TaskGroup join - a release that absorbs the consumer's cancellation relies "
        "on the pool still being open when its write lands."
    )
