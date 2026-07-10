"""Sync actor — demonstrates a plain ``def`` actor dispatched via asyncio.to_thread.

Sync actors run on a thread, freeing the event loop for other work.
They must cooperate with cancellation by polling :meth:`JobContext.should_abort`.
"""

import time
from datetime import timedelta

from pydantic import BaseModel, Field

from taskq import JobContext, actor


class WordCountPayload(BaseModel):
    text: str = Field(
        default="TaskQ makes background jobs simple and reliable",
        description="Text to count words in",
    )


class WordCountResult(BaseModel):
    word_count: int
    char_count: int
    processed_at: str | None = None


@actor(name="count_words", queue="examples", result_ttl=timedelta(minutes=5))
def count_words(payload: WordCountPayload, ctx: JobContext[WordCountPayload]) -> WordCountResult:
    """Counts words and characters — runs synchronously via asyncio.to_thread.

    Sleeps briefly to simulate work and polls ctx.should_abort() for
    cooperative cancellation support.
    """
    words = payload.text.split()
    for _i, _word in enumerate(words):
        if ctx.should_abort():
            raise RuntimeError("cancelled")
        time.sleep(0.2)

    return WordCountResult(
        word_count=len(words),
        char_count=len(payload.text),
        processed_at=None,
    )
