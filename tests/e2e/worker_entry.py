"""Worker container entrypoint — imported ONLY inside the container.

The image sets PYTHONPATH=/app, so the e2e package is imported here via
absolute ``e2e.`` imports (mirrors ``examples/worker.py``). Migrations are
NOT run here: the e2e conftest migrates the module schema before the
container starts (``TASKQ_MIGRATE_ON_START=false``).
"""

import sys
from typing import Any

from e2e.actors import (
    deliver_webhook,
    enrich_order,
    generate_report,
    import_contacts_chunk,
    import_contacts_csv,
    rebuild_search_index,
    send_welcome_email,
    sync_user_profile,
)
from e2e.di import build_registry
from taskq import ActorRef
from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main

ACTORS: dict[str, ActorRef[Any, Any]] = {
    "send_welcome_email": send_welcome_email,
    "sync_user_profile": sync_user_profile,
    "generate_report": generate_report,
    "import_contacts_csv": import_contacts_csv,
    "import_contacts_chunk": import_contacts_chunk,
    "deliver_webhook": deliver_webhook,
    "rebuild_search_index": rebuild_search_index,
    "enrich_order": enrich_order,
}

if __name__ == "__main__":
    settings = WorkerSettings.load()
    sys.exit(worker_main(settings, actor_registry=ACTORS, di_registry=build_registry()))
