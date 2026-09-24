"""Growth pins for the worker's process-lifetime maps.

The unbounded-growth audit (weeks-long worker, millions of jobs) enumerated
every long-lived dict/set in :mod:`taskq` and pinned each one's bound. A map
whose length grows monotonically with N iterations of its driving shape is
the slow-leak signature; these pins fix the bound so a refactor cannot
reintroduce the growth silently.

The maps with a real eviction mechanism (the ratelimit registry's keyed
idle sweep, the admin run-now cooldown map) are pinned at their own test
sites; this module pins the maps that are bounded BY CONSTRUCTION, where
the guarantee is the key's identity scope, not a sweep.
"""

import asyncio
import importlib

import pytest

# The package's ``__init__`` exports the ``cron`` decorator, which shadows
# the submodule attribute on ``taskq``; importlib reaches the module
# itself.

cron_mod = importlib.import_module("taskq.cron")

pytest.importorskip("fastapi")


# ── taskq.web.admin.sse._TOPIC_SEMAPHORES ────────────────────────────────


def test_topic_semaphores_bounded_by_topic_vocabulary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """N mount/lookups over the closed topic set -> map length == 4.

    The map is keyed ``(topic, limit)``; *topic* is validated against the
    endpoint's closed four-value vocabulary before the lookup, and *limit*
    comes from the process's own settings, so the map cannot exceed
    4 x (number of distinct limits the process ever ran with). A topic
    string from outside the vocabulary raises HTTPException(400) in the
    endpoint before reaching this lookup, never a new key.
    """
    import taskq.web.admin.sse as sse_mod

    monkeypatch.setattr(
        sse_mod, "_TOPIC_SEMAPHORES", {}
    )  # Why: module-global map, a sibling test's keys must not bleed in.
    topics = ["queues", "jobs", "workers", "history"]
    for _ in range(10_000):
        for topic in topics:
            sse_mod._get_semaphore(topic, 10)
    assert len(sse_mod._TOPIC_SEMAPHORES) == len(topics)


def test_topic_semaphores_keyed_by_limit_does_not_leak_shared_topics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two limits on one topic get two semaphores, and no more appear.

    The limit is part of the key (a later mount must enforce the limit it
    was given), so the map's worst case is the cross product of the topic
    vocabulary and the distinct limits seen, both closed sets.
    """
    import taskq.web.admin.sse as sse_mod

    monkeypatch.setattr(
        sse_mod, "_TOPIC_SEMAPHORES", {}
    )  # Why: module-global map, a sibling test's keys must not bleed in.
    for _ in range(1_000):
        for topic in ["queues", "jobs", "workers", "history"]:
            sse_mod._get_semaphore(topic, 10)
            sse_mod._get_semaphore(topic, 20)
    assert len(sse_mod._TOPIC_SEMAPHORES) == 8


# ── taskq.web._sse_limit._SEMAPHORES ─────────────────────────────────────


def test_sse_limit_semaphores_bounded_by_endpoint_family() -> None:
    """N acquire/release rounds for the one family key -> map length == 1.

    Every caller passes a fixed endpoint-family string (``progress-stream``
    is the only production call site) and a settings-derived limit, so the
    map is keyed by the (family, limit) cross product of closed sets.
    """
    from taskq.web._sse_limit import _SEMAPHORES, acquire_sse_slot, release_after

    async def _noop_stream():
        yield ""

    async def _rounds(n: int) -> None:
        # One loop for every round: an asyncio.Semaphore binds to the
        # first loop that awaits it, which is exactly the process-lifetime
        # sharing the module map exists to provide.
        for _ in range(n):
            sem = await acquire_sse_slot("progress-stream", 4)
            async for _ in release_after(sem, _noop_stream()):
                pass

    asyncio.run(_rounds(1_000))
    assert len(_SEMAPHORES) == 1


# ── taskq.cron._factory_cache ────────────────────────────────────────────


def test_factory_cache_bounded_by_distinct_dotted_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """N schedule resolves over K distinct dotted paths -> map length == K.

    Cron schedule rows churn (created and deleted through the client, CLI
    and admin UI), but a row's ``payload_factory`` is a dotted path into
    the user's own code, and importable module attributes form a finite
    set: resolving a million rows that reference the same five factories
    must not grow the cache past those five. Distinct paths require
    distinct code objects, so the map is bounded by the codebase, not by
    the schedule-churn rate.
    """
    monkeypatch.setattr(cron_mod, "_factory_cache", {})
    paths = [
        "taskq._json.loads",
        "taskq.cron.compute_next_fire_after",
        "taskq.cron.resolve_payload",
        "taskq.cron.cron",
        "taskq.retry.Retry",
    ]
    for _ in range(100_000):
        for dotted_path in paths:
            cron_mod._resolve_factory(dotted_path)
    assert len(cron_mod._factory_cache) == len(paths)


def test_factory_cache_failed_resolutions_are_not_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """N failed resolves of a missing path -> map length unchanged.

    The negative space matters as much as the positive one: a schedule
    row pointing at a not-yet-deployed factory fails every fire until its
    code ships, and caching the failure (or caching per failure
    count) would turn one broken row into unbounded cache growth.
    """
    monkeypatch.setattr(cron_mod, "_factory_cache", {})
    cron_mod._resolve_factory("taskq._json.loads")
    size_before = len(cron_mod._factory_cache)
    with pytest.raises((ImportError, AttributeError)):
        for _ in range(10_000):
            cron_mod._resolve_factory("taskq._json.does_not_exist")
    assert len(cron_mod._factory_cache) == size_before


def test_factory_cache_repeated_schedule_churn_over_same_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """N create/delete cycles of schedules over K paths -> map length == K.

    The schedule-churn shape end to end: each cycle resolves the path a
    fresh row would carry. Identical rows resolve identical dotted paths,
    so churn in the ``cron_schedules`` table alone cannot grow the cache.
    """
    monkeypatch.setattr(cron_mod, "_factory_cache", {})
    paths = ["taskq._json.loads", "taskq.cron.resolve_payload"]
    for cycle in range(50_000):
        cron_mod._resolve_factory(paths[cycle % len(paths)])
    assert len(cron_mod._factory_cache) == len(paths)
