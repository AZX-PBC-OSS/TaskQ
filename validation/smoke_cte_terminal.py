"""Ad-hoc smoke test: merged mark_* CTE statements against real PG."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg

from taskq._ids import new_job_id, new_uuid
from taskq.backend._sql_templates import render

DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
SCHEMA = "tq_smoke_cte"


async def main() -> int:
    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{SCHEMA}"')
    from taskq.migrate import apply_pending

    await apply_pending(conn, schema=SCHEMA)
    await conn.execute(
        f'INSERT INTO "{SCHEMA}".actor_config (actor, queue) VALUES ($1, $2)',
        "smoke_actor",
        "default",
    )
    sql = render(SCHEMA)
    t = sql  # shorthand

    async def seed_running(worker: UUID | None = None, **kw: object) -> tuple[UUID, UUID]:
        wid = worker or new_uuid()
        jid = new_job_id()
        await conn.execute(
            f"INSERT INTO \"{SCHEMA}\".workers (id, hostname, pid, queues) VALUES ($1, 'h', 0, ARRAY['default']) ON CONFLICT (id) DO NOTHING",
            wid,
        )
        await conn.execute(
            f"""INSERT INTO "{SCHEMA}".jobs (id, actor, queue, payload, max_attempts, retry_kind,
                status, scheduled_at, started_at, locked_by_worker, lock_expires_at, schedule_to_close, attempt)
                VALUES ($1, 'smoke_actor', 'default', '{{}}'::jsonb, $2, 'transient', 'running',
                clock_timestamp(), clock_timestamp(), $3, clock_timestamp() + interval '30 seconds', $4, $5)""",
            jid,
            kw.get("max_attempts", 3),
            wid,
            kw.get("schedule_to_close"),
            kw.get("attempt", 1),
        )
        return wid, jid

    # 1. mark_succeeded
    wid, jid = await seed_running()
    rec = await conn.fetchrow(
        t.mark_succeeded, jid, wid, json.dumps({"ok": True}), 10, 0, None, None
    )
    assert rec is not None and rec["status"] == "succeeded", rec
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    ev = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_events WHERE job_id = $1', jid)
    assert len(att) == 1 and att[0]["outcome"] == "succeeded" and att[0]["worker_id"] == wid, att
    assert att[0]["duration_ms"] is not None and att[0]["duration_ms"] >= 0, att[0]["duration_ms"]
    d = ev[0]["detail"] if isinstance(ev[0]["detail"], dict) else json.loads(ev[0]["detail"])
    assert d == {"from_state": "running", "to_state": "succeeded", "worker_id": str(wid)}, d
    print("mark_succeeded OK; duration_ms =", att[0]["duration_ms"])

    # 2. mark_succeeded wrong worker -> False, nothing written
    wid, jid = await seed_running()
    wrong = new_uuid()
    rec = await conn.fetchrow(t.mark_succeeded, jid, wrong, None, None, 0, None, None)
    assert rec is None
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    ev = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_events WHERE job_id = $1', jid)
    assert len(att) == 0 and len(ev) == 0
    print("mark_succeeded fence OK (0 rows -> no attempts/events)")

    # 3. mark_succeeded with deleted worker row -> attempt.worker_id NULL
    wid, jid = await seed_running()
    await conn.execute(f'DELETE FROM "{SCHEMA}".workers WHERE id = $1', wid)
    rec = await conn.fetchrow(t.mark_succeeded, jid, wid, None, None, 0, None, None)
    assert rec is not None
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    assert len(att) == 1 and att[0]["worker_id"] is None, att[0]
    print("mark_succeeded holder-CTE NULL OK")

    # 4. mark_failed terminal
    wid, jid = await seed_running(max_attempts=1)
    rec = await conn.fetchrow(t.mark_failed, jid, wid, "ValueError", "boom", "tb", 0, None)
    assert rec is not None and rec["status"] == "failed"
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    ev = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_events WHERE job_id = $1', jid)
    assert att[0]["outcome"] == "failed" and att[0]["error_class"] == "ValueError"
    d = ev[0]["detail"] if isinstance(ev[0]["detail"], dict) else json.loads(ev[0]["detail"])
    assert d == {
        "from_state": "running",
        "to_state": "failed",
        "error_class": "ValueError",
        "worker_id": str(wid),
    }, d
    print("mark_failed OK")

    # 5. mark_retry retried arm (attempt row with now_ts duration, event scheduled)
    wid, jid = await seed_running(max_attempts=3)
    rec = await conn.fetchrow(
        t.mark_retry, jid, wid, timedelta(seconds=10), "ValueError", "boom", "tb", 0, None
    )
    assert rec is not None and rec["outcome_branch"] == "retried" and rec["status"] == "scheduled"
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    ev = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_events WHERE job_id = $1', jid)
    assert att[0]["outcome"] == "failed" and att[0]["duration_ms"] is not None
    d = ev[0]["detail"] if isinstance(ev[0]["detail"], dict) else json.loads(ev[0]["detail"])
    assert d["to_state"] == "scheduled" and d["error_class"] == "ValueError", d
    print("mark_retry retried OK")

    # 6. mark_retry deadline_failed arm
    wid, jid = await seed_running(
        max_attempts=3, schedule_to_close=datetime.now(UTC) - timedelta(seconds=5)
    )
    rec = await conn.fetchrow(
        t.mark_retry, jid, wid, timedelta(seconds=10), "ValueError", "boom", "tb", 0, None
    )
    assert (
        rec is not None and rec["outcome_branch"] == "deadline_failed" and rec["status"] == "failed"
    )
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    ev = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_events WHERE job_id = $1', jid)
    assert att[0]["outcome"] == "failed" and att[0]["error_class"] == "DeadlineExceeded", att[0]
    d = ev[0]["detail"] if isinstance(ev[0]["detail"], dict) else json.loads(ev[0]["detail"])
    assert d["to_state"] == "failed" and d["error_class"] == "DeadlineExceeded", d
    print("mark_retry deadline OK")

    # 7. mark_cancelled
    wid, jid = await seed_running()
    rec = await conn.fetchrow(t.mark_cancelled, jid, wid, 0, None)
    assert rec is not None and rec["status"] == "cancelled"
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    ev = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_events WHERE job_id = $1', jid)
    assert att[0]["outcome"] == "cancelled"
    d = ev[0]["detail"] if isinstance(ev[0]["detail"], dict) else json.loads(ev[0]["detail"])
    assert d == {"from_state": "running", "to_state": "cancelled", "worker_id": str(wid)}, d
    print("mark_cancelled OK")

    # 8. mark_abandoned (cancel_phase=2; holder from row's locked_by_worker)
    wid, jid = await seed_running()
    await conn.execute(f'UPDATE "{SCHEMA}".jobs SET cancel_phase = 2 WHERE id = $1', jid)
    rec = await conn.fetchrow(t.mark_abandoned, jid, 0, None)
    assert rec is not None and rec["status"] == "abandoned"
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    ev = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_events WHERE job_id = $1', jid)
    assert att[0]["outcome"] == "cancelled" and att[0]["worker_id"] == wid, att[0]
    d = ev[0]["detail"] if isinstance(ev[0]["detail"], dict) else json.loads(ev[0]["detail"])
    assert d == {"from_state": "running", "to_state": "abandoned", "worker_id": str(wid)}, d
    print("mark_abandoned OK")

    # 9. mark_snoozed (outcome param) + metadata update + reservation_denied outcome
    wid, jid = await seed_running()
    rec = await conn.fetchrow(
        t.mark_snoozed,
        jid,
        wid,
        timedelta(seconds=5),
        json.dumps({"snooze_count": 1}),
        0,
        None,
        "snoozed",
    )
    assert rec is not None and rec["outcome_branch"] == "snoozed" and rec["status"] == "scheduled"
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    ev = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_events WHERE job_id = $1', jid)
    assert att[0]["outcome"] == "snoozed" and att[0]["duration_ms"] is not None
    d = ev[0]["detail"] if isinstance(ev[0]["detail"], dict) else json.loads(ev[0]["detail"])
    assert d == {"from_state": "running", "to_state": "scheduled", "worker_id": str(wid)}, d
    row = await conn.fetchrow(f'SELECT metadata FROM "{SCHEMA}".jobs WHERE id = $1', jid)
    assert row is not None
    md = row["metadata"] if isinstance(row["metadata"], dict) else json.loads(row["metadata"])
    assert md.get("snooze_count") == 1, md
    print("mark_snoozed OK")

    # 10. mark_snoozed deadline arm
    wid, jid = await seed_running(schedule_to_close=datetime.now(UTC) - timedelta(seconds=5))
    rec = await conn.fetchrow(
        t.mark_snoozed, jid, wid, timedelta(seconds=5), None, 0, None, "snoozed"
    )
    assert rec is not None and rec["outcome_branch"] == "failed" and rec["status"] == "failed"
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    assert att[0]["outcome"] == "failed" and att[0]["error_class"] == "DeadlineExceeded", att[0]
    print("mark_snoozed deadline OK")

    # 11. mark_retry_after consume_budget=True (max_attempts arm)
    wid, jid = await seed_running(attempt=3, max_attempts=3)
    rec = await conn.fetchrow(
        t.mark_retry_after_consume_true, jid, wid, timedelta(seconds=5), 0, None
    )
    assert (
        rec is not None
        and rec["outcome_branch"] == "max_attempts_failed"
        and rec["status"] == "failed"
    )
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    ev = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_events WHERE job_id = $1', jid)
    assert att[0]["outcome"] == "failed" and att[0]["error_class"] == "MaxAttemptsExceeded", att[0]
    d = ev[0]["detail"] if isinstance(ev[0]["detail"], dict) else json.loads(ev[0]["detail"])
    assert d["error_class"] == "MaxAttemptsExceeded", d
    print("mark_retry_after consume_true max_attempts OK")

    # 12. mark_retry_after consume_budget=True snoozed arm
    wid, jid = await seed_running(attempt=1, max_attempts=3)
    rec = await conn.fetchrow(
        t.mark_retry_after_consume_true, jid, wid, timedelta(seconds=5), 0, None
    )
    assert rec is not None and rec["outcome_branch"] == "snoozed" and rec["status"] == "scheduled"
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    ev = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_events WHERE job_id = $1', jid)
    assert att[0]["outcome"] == "snoozed" and att[0]["error_class"] == "RetryAfter", att[0]
    d = ev[0]["detail"] if isinstance(ev[0]["detail"], dict) else json.loads(ev[0]["detail"])
    assert d["to_state"] == "scheduled" and "error_class" not in d, d
    print("mark_retry_after consume_true snoozed OK")

    # 13. mark_retry_after consume_budget=False (max_attempts +1 preserved)
    wid, jid = await seed_running(attempt=1, max_attempts=3)
    rec = await conn.fetchrow(
        t.mark_retry_after_consume_false, jid, wid, timedelta(seconds=5), 0, None
    )
    assert rec is not None and rec["outcome_branch"] == "snoozed"
    att = await conn.fetch(f'SELECT * FROM "{SCHEMA}".job_attempts WHERE job_id = $1', jid)
    assert att[0]["outcome"] == "snoozed" and att[0]["attempt"] == 1, att[0]
    print("mark_retry_after consume_false OK")

    # 14. progress_state merge (|| concat) still lands via merged template
    wid, jid = await seed_running()
    await conn.execute(
        f'UPDATE "{SCHEMA}".jobs SET progress_state = \'{{"x": 1}}\'::jsonb WHERE id = $1', jid
    )
    rec = await conn.fetchrow(t.mark_succeeded, jid, wid, None, None, 7, json.dumps({"y": 2}), None)
    assert rec is not None
    ps = (
        rec["progress_state"]
        if isinstance(rec["progress_state"], dict)
        else json.loads(rec["progress_state"])
    )
    assert ps == {"x": 1, "y": 2} and rec["progress_seq"] == 7, ps
    print("progress_state merge OK")

    # 15. result_expires_at fallback ($7) still applies when no override
    await conn.execute(
        f'UPDATE "{SCHEMA}".actor_config SET result_ttl = NULL WHERE actor = $1', "smoke_actor"
    )
    wid, jid = await seed_running()
    rec = await conn.fetchrow(
        t.mark_succeeded, jid, wid, None, None, 0, None, timedelta(seconds=60)
    )
    assert rec is not None
    assert rec["result_expires_at"] is not None
    skew = rec["result_expires_at"] - rec["finished_at"]
    assert timedelta(seconds=59) < skew < timedelta(seconds=61), skew
    print("result_expires_at fallback OK")

    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.close()
    print("\nALL SMOKE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
