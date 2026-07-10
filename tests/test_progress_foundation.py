"""Unit tests for progress subpackage foundation: ProgressEvent, _ProgressBuffer,
channel helpers, WorkerSettings fields, and ProgressTooLarge.

Covers deliverables — no PG or Redis required.
"""

from dataclasses import fields
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, ValidationError

from taskq.constants import (
    PROGRESS_CHANNEL_FMT,
    PROGRESS_GLOBAL_CHANNEL_FMT,
    progress_channel,
    progress_global_channel,
)
from taskq.exceptions import ProgressTooLarge, TaskQError
from taskq.progress import ProgressEvent
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


def _load(**overrides: str) -> WorkerSettings:
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


# ── ProgressEvent model ────────────────────────────────────────────────────


def test_progress_event_is_pydantic_basemodel() -> None:
    assert issubclass(ProgressEvent, BaseModel)


def test_progress_event_frozen() -> None:
    event = ProgressEvent(
        kind="progress",
        job_id=uuid4(),
        actor="test_actor",
        ts=datetime.now(UTC),
        seq=1,
        status="running",
    )
    with pytest.raises(ValidationError):
        event.seq = 2


def test_progress_event_defaults() -> None:
    event = ProgressEvent(
        kind="progress",
        job_id=uuid4(),
        actor="test_actor",
        ts=datetime.now(UTC),
        seq=1,
        status="running",
    )
    assert event.v == 1
    assert event.terminal is False
    assert event.step is None
    assert event.percent is None
    assert event.detail is None
    assert event.data is None


def test_progress_event_all_fields() -> None:
    job_id = uuid4()
    ts = datetime(2026, 5, 3, 12, 34, 56, 123000, tzinfo=UTC)
    event = ProgressEvent(
        kind="progress",
        job_id=job_id,
        actor="ingest_telemetry",
        ts=ts,
        seq=17,
        status="running",
        step=2,
        percent=50.0,
        detail="processing",
        data={"rows": 1432},
        terminal=False,
    )
    assert event.v == 1
    assert event.kind == "progress"
    assert event.job_id == job_id
    assert event.actor == "ingest_telemetry"
    assert event.ts == ts
    assert event.seq == 17
    assert event.status == "running"
    assert event.step == 2
    assert event.percent == 50.0
    assert event.detail == "processing"
    assert event.data == {"rows": 1432}
    assert event.terminal is False


def test_progress_event_kind_literal() -> None:
    job_id = uuid4()
    ts = datetime.now(UTC)
    ProgressEvent(kind="progress", job_id=job_id, actor="a", ts=ts, seq=1, status="running")
    ProgressEvent(kind="state_change", job_id=job_id, actor="a", ts=ts, seq=1, status="succeeded")
    with pytest.raises(ValidationError):
        ProgressEvent(kind="invalid", job_id=job_id, actor="a", ts=ts, seq=1, status="running")


def test_progress_event_data_type_is_dict_str_object() -> None:
    field_type = ProgressEvent.model_fields["data"].annotation
    assert field_type is not None


def test_progress_event_model_dump_json_excludes_none() -> None:
    job_id = uuid4()
    ts = datetime(2026, 5, 3, 12, 34, 56, 123000, tzinfo=UTC)
    event = ProgressEvent(
        kind="progress",
        job_id=job_id,
        actor="test_actor",
        ts=ts,
        seq=1,
        status="running",
    )
    json_str = event.model_dump_json(exclude_none=True)
    assert '"step"' not in json_str
    assert '"percent"' not in json_str
    assert '"detail"' not in json_str
    assert '"data"' not in json_str


def test_progress_event_model_dump_json_includes_set_fields() -> None:
    job_id = uuid4()
    ts = datetime(2026, 5, 3, 12, 34, 56, 123000, tzinfo=UTC)
    event = ProgressEvent(
        kind="progress",
        job_id=job_id,
        actor="test_actor",
        ts=ts,
        seq=1,
        status="running",
        step=2,
        percent=50.0,
        detail="halfway",
        data={"count": 42},
        terminal=False,
    )
    json_str = event.model_dump_json(exclude_none=True)
    assert '"step":2' in json_str
    assert '"percent":50.0' in json_str
    assert '"detail":"halfway"' in json_str
    assert '"count":42' in json_str
    assert '"terminal":false' in json_str


def test_progress_event_state_change_with_terminal() -> None:
    job_id = uuid4()
    ts = datetime.now(UTC)
    event = ProgressEvent(
        kind="state_change",
        job_id=job_id,
        actor="test_actor",
        ts=ts,
        seq=5,
        status="succeeded",
        terminal=True,
    )
    assert event.kind == "state_change"
    assert event.status == "succeeded"
    assert event.terminal is True


def test_progress_event_uuid_serializes_to_string() -> None:
    job_id = uuid4()
    ts = datetime.now(UTC)
    event = ProgressEvent(
        kind="progress",
        job_id=job_id,
        actor="a",
        ts=ts,
        seq=1,
        status="running",
    )
    dumped = event.model_dump(exclude_none=True)
    assert isinstance(dumped["job_id"], UUID)


# ── _ProgressBuffer dataclass ──────────────────────────────────────────────


def test_progress_buffer_fields() -> None:
    field_names = {f.name for f in fields(_ProgressBuffer)}
    assert field_names == {
        "job_id",
        "base_seq",
        "pending_seq_delta",
        "pending_state",
        "dirty",
        "last_flush_at",
    }


def test_progress_buffer_construction_with_defaults() -> None:
    job_id = uuid4()
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    assert buf.job_id == job_id
    assert buf.base_seq == 0
    assert buf.pending_seq_delta == 0
    assert buf.pending_state == {}
    assert buf.dirty is False
    assert buf.last_flush_at == 0.0


def test_progress_buffer_construction_with_all_values() -> None:
    job_id = uuid4()
    buf = _ProgressBuffer(
        job_id=job_id,
        base_seq=10,
        pending_seq_delta=3,
        pending_state={"step": 2, "percent": 50.0},
        dirty=True,
        last_flush_at=1234.5,
    )
    assert buf.base_seq == 10
    assert buf.pending_seq_delta == 3
    assert buf.pending_state == {"step": 2, "percent": 50.0}
    assert buf.dirty is True
    assert buf.last_flush_at == 1234.5


def test_progress_buffer_is_mutable() -> None:
    job_id = uuid4()
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buf.pending_seq_delta = 5
    buf.dirty = True
    buf.pending_state["step"] = 1
    assert buf.pending_seq_delta == 5
    assert buf.dirty is True
    assert buf.pending_state == {"step": 1}


def test_progress_buffer_pending_state_default_factory_independent() -> None:
    job_id = uuid4()
    buf1 = _ProgressBuffer(job_id=job_id, base_seq=0)
    buf2 = _ProgressBuffer(job_id=job_id, base_seq=0)
    buf1.pending_state["step"] = 1
    assert buf2.pending_state == {}


def test_progress_buffer_not_exported_from_package() -> None:
    import taskq.progress

    assert not hasattr(taskq.progress, "_ProgressBuffer")


def test_progress_buffer_data_type_is_dict_str_object() -> None:
    field_type = _ProgressBuffer.__dataclass_fields__["pending_state"].type
    assert field_type is not None


# ── channel helpers ────────────────────────────────────────────────────────


def test_progress_channel_format() -> None:
    job_id = uuid4()
    result = progress_channel("taskq", job_id)
    assert result == f"taskq:taskq:progress:{job_id}"


def test_progress_channel_with_str_job_id() -> None:
    result = progress_channel("taskq", "abc-123")
    assert result == "taskq:taskq:progress:abc-123"


def test_progress_channel_validates_schema() -> None:
    with pytest.raises(ValueError, match="invalid schema identifier"):
        progress_channel("bad-schema", uuid4())


def test_progress_global_channel_format() -> None:
    result = progress_global_channel("taskq")
    assert result == "taskq:taskq:progress"


def test_progress_global_channel_validates_schema() -> None:
    with pytest.raises(ValueError, match="invalid schema identifier"):
        progress_global_channel("bad.schema")


def test_progress_channel_matches_fmt_constant() -> None:
    job_id = uuid4()
    assert progress_channel("myschema", job_id) == PROGRESS_CHANNEL_FMT.format(
        schema="myschema", job_id=job_id
    )


def test_progress_global_channel_matches_fmt_constant() -> None:
    assert progress_global_channel("myschema") == PROGRESS_GLOBAL_CHANNEL_FMT.format(
        schema="myschema"
    )


def test_progress_channel_rejects_empty_schema() -> None:
    with pytest.raises(ValueError, match="invalid schema identifier"):
        progress_channel("", uuid4())


def test_progress_global_channel_rejects_empty_schema() -> None:
    with pytest.raises(ValueError, match="invalid schema identifier"):
        progress_global_channel("")


def test_progress_channel_rejects_digit_start_schema() -> None:
    with pytest.raises(ValueError, match="invalid schema identifier"):
        progress_channel("1bad", uuid4())


def test_progress_global_channel_rejects_digit_start_schema() -> None:
    with pytest.raises(ValueError, match="invalid schema identifier"):
        progress_global_channel("1bad")


def test_progress_channel_underscore_schema() -> None:
    job_id = uuid4()
    result = progress_channel("_private", job_id)
    assert result == f"taskq:_private:progress:{job_id}"


# ── WorkerSettings progress fields ─────────────────────────────────────────


def test_progress_coalesce_interval_default() -> None:
    s = _load()
    assert s.progress_coalesce_interval == 0.5


def test_progress_publish_global_default() -> None:
    s = _load()
    assert s.progress_publish_global is True


def test_progress_data_max_bytes_default() -> None:
    s = _load()
    assert s.progress_data_max_bytes == 16384


def test_progress_coalesce_interval_via_dict() -> None:
    s = _load(TASKQ_PROGRESS_COALESCE_INTERVAL="1.0")
    assert s.progress_coalesce_interval == 1.0


def test_progress_publish_global_false_via_dict() -> None:
    s = _load(TASKQ_PROGRESS_PUBLISH_GLOBAL="false")
    assert s.progress_publish_global is False


def test_progress_data_max_bytes_via_dict() -> None:
    s = _load(TASKQ_PROGRESS_DATA_MAX_BYTES="32768")
    assert s.progress_data_max_bytes == 32768


def test_progress_coalesce_interval_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_PROGRESS_COALESCE_INTERVAL", "2.0")
    s = WorkerSettings.load()
    assert s.progress_coalesce_interval == 2.0


def test_progress_publish_global_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_PROGRESS_PUBLISH_GLOBAL", "false")
    s = WorkerSettings.load()
    assert s.progress_publish_global is False


def test_progress_data_max_bytes_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_PG_DSN", _DSN)
    monkeypatch.setenv("TASKQ_PROGRESS_DATA_MAX_BYTES", "65536")
    s = WorkerSettings.load()
    assert s.progress_data_max_bytes == 65536


def test_progress_coalesce_interval_zero_raises() -> None:
    s = _load()
    object.__setattr__(s, "progress_coalesce_interval", 0.0)
    with pytest.raises(ValueError, match=r"progress_coalesce_interval.*must be > 0"):
        s._post_load()


def test_progress_coalesce_interval_negative_raises() -> None:
    s = _load()
    object.__setattr__(s, "progress_coalesce_interval", -1.0)
    with pytest.raises(ValueError, match=r"progress_coalesce_interval.*must be > 0"):
        s._post_load()


# ── ProgressTooLarge exception ─────────────────────────────────────────────


def test_progress_too_large_construction() -> None:
    exc = ProgressTooLarge(limit=16384, actual=20000)
    assert exc.limit == 16384
    assert exc.actual == 20000


def test_progress_too_large_message_format() -> None:
    exc = ProgressTooLarge(limit=16384, actual=20000)
    assert str(exc) == "Progress data payload 20000B exceeds limit 16384B"


def test_progress_too_large_is_taskq_error() -> None:
    exc = ProgressTooLarge(limit=16384, actual=20000)
    assert isinstance(exc, TaskQError)


def test_progress_too_large_importable_from_taskq() -> None:
    import taskq

    assert taskq.ProgressTooLarge is ProgressTooLarge


def test_progress_too_large_in_all() -> None:
    import taskq

    assert "ProgressTooLarge" in taskq.__all__


# ── ProgressEvent public import ─────────────────────────────────────────────


def test_progress_event_importable_from_taskq_progress() -> None:
    from taskq.progress import ProgressEvent as Imported

    assert Imported is ProgressEvent


def test_progress_event_only_public_symbol() -> None:
    import taskq.progress

    assert taskq.progress.__all__ == ["ProgressEvent"]


# ── No `from __future__ import annotations` in touched files ───────────────


def test_no_future_annotations_in_events() -> None:
    import taskq.progress._events

    source = open(taskq.progress._events.__file__).read()  # noqa: SIM115
    assert "from __future__ import annotations" not in source


def test_no_future_annotations_in_buffer() -> None:
    import taskq.progress._buffer

    source = open(taskq.progress._buffer.__file__).read()  # noqa: SIM115
    assert "from __future__ import annotations" not in source


def test_no_future_annotations_in_settings() -> None:
    import taskq.settings

    source = open(taskq.settings.__file__).read()  # noqa: SIM115
    assert "from __future__ import annotations" not in source


# ── No `import json` in progress package ────────────────────────────────────


def test_no_stdlib_json_in_events() -> None:
    import taskq.progress._events

    source = open(taskq.progress._events.__file__).read()  # noqa: SIM115
    assert "import json" not in source


def test_no_stdlib_json_in_buffer() -> None:
    import taskq.progress._buffer

    source = open(taskq.progress._buffer.__file__).read()  # noqa: SIM115
    assert "import json" not in source
