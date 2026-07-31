"""Worker container entrypoint — imported ONLY inside the container.

The image sets PYTHONPATH=/app, so the e2e package is imported here via
absolute ``e2e.`` imports (mirrors ``examples/worker.py``). Migrations are
NOT run here: the e2e conftest migrates the module schema before the
container starts (``TASKQ_MIGRATE_ON_START=false``).
"""

import os
import sys
from typing import Any

from e2e.actors import (
    batch_abort_worker,
    batch_finalizer,
    capped_worker,
    concurrent_tracked_worker,
    cron_heartbeat,
    deliver_tenant_webhook,
    deliver_webhook,
    enrich_order,
    generate_report,
    import_contacts_chunk,
    import_contacts_csv,
    long_running_job,
    loop_blocker_job,
    quick_result,
    rebuild_search_index,
    send_welcome_email,
    short_lived_job,
    slow_deliver_webhook,
    sync_user_profile,
)
from e2e.di import build_registry
from taskq import ActorRef
from taskq.cron import CronScheduleSpec
from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main

ACTORS: dict[str, ActorRef[Any, Any]] = {
    "send_welcome_email": send_welcome_email,
    "sync_user_profile": sync_user_profile,
    "generate_report": generate_report,
    "import_contacts_csv": import_contacts_csv,
    "import_contacts_chunk": import_contacts_chunk,
    "deliver_webhook": deliver_webhook,
    "deliver_tenant_webhook": deliver_tenant_webhook,
    "rebuild_search_index": rebuild_search_index,
    "enrich_order": enrich_order,
    "capped_worker": capped_worker,
    "quick_result": quick_result,
    "slow_deliver_webhook": slow_deliver_webhook,
    "long_running_job": long_running_job,
    "loop_blocker_job": loop_blocker_job,
    "cron_heartbeat": cron_heartbeat,
    "short_lived_job": short_lived_job,
    "concurrent_tracked_worker": concurrent_tracked_worker,
    "batch_abort_worker": batch_abort_worker,
    "batch_finalizer": batch_finalizer,
}


def _e2e_cron_registry() -> list[CronScheduleSpec] | None:
    """Cron schedules for the cron e2e module only.

    Gated on TASKQ_E2E_CRON so the schedule fires solely inside the
    dedicated cron-test container — a once-a-minute job in every e2e
    worker would break the other modules' idle gates.
    """
    if os.environ.get("TASKQ_E2E_CRON") != "1":
        return None
    return [
        CronScheduleSpec(
            actor="cron_heartbeat",
            cron_expr="* * * * *",
            static_payload={"run_id": "cron-static", "beat": 0},
        )
    ]


if __name__ == "__main__":
    settings = WorkerSettings.load()
    until_idle = os.environ.get("TASKQ_UNTIL_IDLE") == "true"
    sys.exit(
        worker_main(
            settings,
            actor_registry=ACTORS,
            di_registry=build_registry(),
            cron_registry=_e2e_cron_registry(),
            until_idle=until_idle,
        )
    )
