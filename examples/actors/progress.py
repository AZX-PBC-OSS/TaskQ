"""Progress-reporting actor — demonstrates ctx.progress() and JobHandle.progress_stream().

The ``file_processor`` actor simulates multi-step file processing (parse, validate,
transform, write).  It calls ``ctx.progress()`` at each step so clients can display
a live progress bar.  The ``progress_stream`` on :class:`~taskq.JobHandle` yields
:class:`~taskq.progress.ProgressEvent` objects in real time when Redis is configured.
"""

import asyncio

from pydantic import BaseModel

from taskq import JobContext, actor


class FileProcessorPayload(BaseModel):
    filename: str = "data.csv"
    rows: int = 1000


@actor(name="file_processor", queue="examples")
async def file_processor(
    payload: FileProcessorPayload, ctx: JobContext[FileProcessorPayload]
) -> None:
    """Processes a file in four steps, reporting progress at each stage."""
    steps = ["parsing", "validating", "transforming", "writing"]
    total = len(steps)

    for i, label in enumerate(steps):
        ctx.check_cancelled()
        step_num = i + 1
        percent = round(i / total * 100, 1)
        await ctx.progress(
            step=step_num,
            percent=percent,
            detail=f"{label} {payload.filename}",
            data={"rows_processed": payload.rows * i // total},
        )
        await asyncio.sleep(1)

    await ctx.progress(
        step=total,
        percent=100.0,
        detail="done",
        data={"rows_processed": payload.rows},
    )
