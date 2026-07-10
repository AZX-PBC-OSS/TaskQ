"""FastAPI router factory for the built-in admin UI.

Importing this module requires the ``taskq[fastapi]`` optional extra.
"""

from taskq.web.admin._factory import (
    AdminBundle,
    create_router,
    get_base_path,
    get_pg_pool,
    get_redis_client,
    get_schema,
    get_settings,
    get_templates,
    setup_admin_state,
)

__all__ = [
    "AdminBundle",
    "create_router",
    "get_base_path",
    "get_pg_pool",
    "get_redis_client",
    "get_schema",
    "get_settings",
    "get_templates",
    "setup_admin_state",
]
