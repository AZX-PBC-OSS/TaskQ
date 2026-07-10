"""FastAPI routes — enqueue, stream, and cancel jobs using the public TaskQ surface."""

from collections.abc import AsyncGenerator
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from examples.fastapi_app.actors import ItemPayload, process_item
from taskq import JobId, TaskQ

router = APIRouter()


def get_tq(request: Request) -> TaskQ:
    return request.app.state.tq


@router.post("/jobs")
async def enqueue_job(tq: TaskQ = Depends(get_tq)) -> dict[str, str]:
    handle = await tq.enqueue(process_item, ItemPayload())
    return {"job_id": str(handle.job_id)}


@router.get("/jobs/{job_id}/stream")
async def stream_job(job_id: UUID, tq: TaskQ = Depends(get_tq)) -> StreamingResponse:
    async def generator() -> AsyncGenerator[str, None]:
        async for event in tq.stream(JobId(job_id)):
            yield f"data: {event.model_dump_json()}\n\n"

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.delete("/jobs/{job_id}")
async def cancel_job(job_id: UUID, tq: TaskQ = Depends(get_tq)) -> dict[str, bool]:
    await tq.cancel(JobId(job_id))
    return {"cancelled": True}
