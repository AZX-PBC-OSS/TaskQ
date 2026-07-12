"""Real-world scenario actors — realistic patterns you'd use in production.

These actors demonstrate TaskQ patterns that map to real application needs:

- ``send_digest_email``: Email digest pipeline — fan-out per user with retry,
  dedup via ``identity_key``, and typed results. Uses DI for the email client.
- ``process_csv_upload``: ETL pipeline — parse → validate → transform → load
  with progress reporting at each stage, cooperative cancellation, and
  sub-job fan-out for batch processing.
- ``generate_thumbnail``: CPU-bound sync actor — image processing via Pillow,
  demonstrates sync actors with ``ctx.should_abort()`` and ``result_ttl``.
"""

import asyncio
import time
from datetime import timedelta

from pydantic import BaseModel, Field

from examples.actors.di import SmtpClient
from taskq import JobContext, RetryPolicy, actor

# ── Email digest pipeline ──────────────────────────────────────────────────


class DigestEmailPayload(BaseModel):
    user_id: str = Field(description="User identifier")
    email: str = Field(description="Recipient email address")
    period: str = Field(default="weekly", description="Digest period: daily, weekly, monthly")


class DigestEmailResult(BaseModel):
    message_id: str
    recipients: int
    articles_included: int


@actor(
    name="send_digest_email",
    queue="examples",
    retry=RetryPolicy(max_attempts=3, base=timedelta(seconds=5)),
    unique_for=timedelta(minutes=30),
    result_ttl=timedelta(hours=24),
)
async def send_digest_email(
    payload: DigestEmailPayload,
    ctx: JobContext[DigestEmailPayload],
    *,
    smtp: SmtpClient,
) -> DigestEmailResult:
    """Send a digest email to a user — retries on transient SMTP failures.

    Deduplicated per ``user_id`` within a 30-minute window so a double-click
    on "send digest" doesn't spam the user. Returns a typed result with the
    message ID for audit trail.
    """
    ctx.log.info("digest-start", user_id=payload.user_id, period=payload.period)

    articles = await _fetch_user_articles(payload.user_id, payload.period)
    if not articles:
        ctx.log.info("digest-skipped", user_id=payload.user_id, reason="no_articles")
        return DigestEmailResult(message_id="", recipients=0, articles_included=0)

    body = _render_digest(payload.user_id, articles)
    subject = f"Your {payload.period} digest"

    ctx.check_cancelled()
    message_id = await smtp.send(payload.email, subject, body)

    ctx.log.info("digest-sent", user_id=payload.user_id, articles=len(articles))
    return DigestEmailResult(
        message_id=message_id,
        recipients=1,
        articles_included=len(articles),
    )


async def _fetch_user_articles(user_id: str, period: str) -> list[dict[str, str]]:
    """Simulate fetching articles for a user's digest period."""
    await asyncio.sleep(0.2)
    return [
        {"title": "TaskQ 1.0 released", "url": "https://example.com/1"},
        {"title": "Async patterns in Python", "url": "https://example.com/2"},
        {"title": "Postgres SKIP LOCKED explained", "url": "https://example.com/3"},
    ]


def _render_digest(user_id: str, articles: list[dict[str, str]]) -> str:
    """Render the digest email body."""
    lines = [f"Hello {user_id},", "", f"Here are your {len(articles)} articles:", ""]
    for i, article in enumerate(articles, 1):
        lines.append(f"{i}. {article['title']} — {article['url']}")
    lines.append("")
    lines.append("— TaskQ Digest")
    return "\n".join(lines)


# ── CSV upload ETL pipeline ────────────────────────────────────────────────


class CsvUploadPayload(BaseModel):
    filename: str = Field(description="Name of the uploaded CSV file")
    row_count: int = Field(default=1000, ge=1, le=100_000, description="Number of rows to process")
    chunk_size: int = Field(default=500, ge=100, le=5000, description="Rows per chunk")


@actor(
    name="process_csv_upload",
    queue="examples",
    retry=RetryPolicy(max_attempts=2, base=timedelta(seconds=10)),
)
async def process_csv_upload(
    payload: CsvUploadPayload,
    ctx: JobContext[CsvUploadPayload],
) -> None:
    """ETL pipeline: parse → validate → transform → load with progress and fan-out.

    Reports progress at each stage via ``ctx.progress()``. For large files,
    fans out chunk-processing as sub-jobs via ``ctx.jobs.enqueue_batch()``
    so multiple workers can process chunks in parallel.
    """
    stages = ["parsing", "validating", "chunking", "dispatching"]
    total_stages = len(stages)

    for i, stage in enumerate(stages):
        ctx.check_cancelled()
        await ctx.progress(
            step=i + 1,
            percent=round(i / total_stages * 100, 1),
            detail=f"{stage} {payload.filename}",
        )
        await asyncio.sleep(0.5)

    chunks = [
        {
            "chunk_id": j,
            "start_row": j * payload.chunk_size,
            "end_row": min((j + 1) * payload.chunk_size, payload.row_count),
        }
        for j in range((payload.row_count + payload.chunk_size - 1) // payload.chunk_size)
    ]

    from taskq.batch import EnqueueItem

    items = [
        EnqueueItem(
            actor_ref=process_csv_chunk,
            payload=CsvChunkPayload(
                filename=payload.filename,
                chunk_id=c["chunk_id"],
                start_row=c["start_row"],
                end_row=c["end_row"],
            ),
            metadata={"source": "csv_upload", "filename": payload.filename},
        )
        for c in chunks
    ]
    await ctx.jobs.enqueue_batch(items)

    await ctx.progress(
        step=total_stages,
        percent=100.0,
        detail=f"dispatched {len(chunks)} chunks",
        data={"total_chunks": len(chunks), "total_rows": payload.row_count},
    )
    ctx.log.info("csv-upload-dispatched", filename=payload.filename, chunks=len(chunks))


class CsvChunkPayload(BaseModel):
    filename: str
    chunk_id: int
    start_row: int
    end_row: int


@actor(
    name="process_csv_chunk",
    queue="examples",
    retry=RetryPolicy(max_attempts=3, base=timedelta(seconds=2)),
)
async def process_csv_chunk(
    payload: CsvChunkPayload,
    ctx: JobContext[CsvChunkPayload],
) -> None:
    """Process a single chunk of CSV rows — a sub-job dispatched by the ETL pipeline."""
    row_count = payload.end_row - payload.start_row
    ctx.log.info(
        "chunk-start",
        filename=payload.filename,
        chunk_id=payload.chunk_id,
        rows=row_count,
    )

    batch = max(1, row_count // 10)
    for i in range(10):
        ctx.check_cancelled()
        await ctx.progress(
            step=i + 1,
            percent=round((i + 1) / 10 * 100, 1),
            detail=f"chunk {payload.chunk_id}: rows {payload.start_row + i * batch}-{payload.start_row + (i + 1) * batch}",
        )
        await asyncio.sleep(0.1)

    ctx.log.info("chunk-done", chunk_id=payload.chunk_id, rows=row_count)


# ── Thumbnail generation (sync actor) ──────────────────────────────────────


class ThumbnailPayload(BaseModel):
    image_url: str = Field(description="Source image URL or path")
    width: int = Field(default=200, ge=1, le=2000)
    height: int = Field(default=200, ge=1, le=2000)
    format: str = Field(default="webp", description="Output format: webp, jpeg, png")


class ThumbnailResult(BaseModel):
    output_path: str
    width: int
    height: int
    format: str
    source_bytes: int


@actor(
    name="generate_thumbnail",
    queue="examples",
    result_ttl=timedelta(hours=6),
    retry=RetryPolicy(max_attempts=2),
)
def generate_thumbnail(
    payload: ThumbnailPayload,
    ctx: JobContext[ThumbnailPayload],
) -> ThumbnailResult:
    """Generate a thumbnail from an image — CPU-bound sync actor.

    In production this would use Pillow or wand to resize the image.
    Here we simulate CPU-bound work with periodic cancellation checks.
    The worker runs this in a thread via ``asyncio.to_thread`` so the
    event loop stays responsive for other actors.
    """
    ctx.log.info("thumbnail-start", url=payload.image_url, size=f"{payload.width}x{payload.height}")

    for i in range(5):
        if ctx.should_abort():
            ctx.log.info("thumbnail-cancelled", step=i)
            raise RuntimeError("cancelled by user request")
        time.sleep(0.3)

    output_path = f"/tmp/thumbs/{payload.image_url.split('/')[-1].rsplit('.', 1)[0]}_{payload.width}x{payload.height}.{payload.format}"  # noqa: S108  # Why: example code simulating a file path.

    ctx.log.info("thumbnail-done", output_path=output_path)
    return ThumbnailResult(
        output_path=output_path,
        width=payload.width,
        height=payload.height,
        format=payload.format,
        source_bytes=1_048_576,
    )
