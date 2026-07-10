"""Static file serving route for the admin UI."""

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse


def register(router: APIRouter, static_dir: Path) -> None:
    """Attach the ``GET /static/{path:path}`` route with path-traversal prevention."""
    static_dir_resolved = static_dir.resolve()

    @router.get("/static/{path:path}")
    async def serve_static(path: str) -> FileResponse:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        resolved = (static_dir / path).resolve()
        # Why: is_relative_to rejects sibling directories whose names share a
        # string prefix with static_dir (e.g. /srv/static vs /srv/static-evil),
        # which a naive str.startswith check would incorrectly allow.
        if not resolved.is_relative_to(static_dir_resolved):
            raise HTTPException(status_code=404)
        if not resolved.is_file():
            raise HTTPException(status_code=404)
        resp = FileResponse(resolved)
        resp.headers["Cache-Control"] = "public, max-age=86400"
        return resp
