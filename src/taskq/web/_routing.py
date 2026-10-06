"""Shared ASGI routing helpers for the web surface.

Importing this module requires the ``taskq[fastapi]`` optional extra.
"""

from typing import Any

from fastapi.routing import APIRoute


class HeadForGetRoute(APIRoute):
    """APIRoute that answers HEAD for GET routes (F6).

    Starlette's own ``Route`` adds HEAD to every GET route; FastAPI's
    ``APIRoute`` does not, so every monitor's ``HEAD`` check against a
    TaskQ page or endpoint answered 405 - a route that serves cannot be
    distinguished from one that is down. Apply via
    ``APIRouter(route_class=HeadForGetRoute)`` (or
    ``app.router.route_class = HeadForGetRoute`` before registering
    routes): each GET registration gains HEAD, handled by the same
    handler (a HEAD response's body is the server's to strip).
    """

    def __init__(self, path: str, endpoint: Any, **kwargs: Any) -> None:
        methods = kwargs.get("methods")
        if methods is not None:
            upper = {str(m).upper() for m in methods}
            if "GET" in upper and "HEAD" not in upper:
                kwargs = {**kwargs, "methods": [*(str(m) for m in methods), "HEAD"]}
        super().__init__(path, endpoint, **kwargs)


__all__ = ["HeadForGetRoute"]
