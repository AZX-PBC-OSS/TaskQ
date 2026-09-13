"""Native Python actor for the e2e prototype — registry entry kind 1.

A real taskq ``@actor``-decorated handler with its :class:`ActorRef`,
showing that native and foreign (Node-runtime-hosted) entries coexist in
the mini-worker's dispatch table. The mini-worker calls the ref directly
(construction of the :class:`JobContext` mirrors the real dispatcher);
all durability still flows through the same terminal writes as the
foreign path.
"""

from pydantic import BaseModel, Field
from taskq.actor import actor
from taskq.context import JobContext
from taskq.retry import RetryPolicy


class NativePayload(BaseModel):
    text: str = Field(min_length=1, max_length=500)


class NativeResult(BaseModel):
    word_count: int
    upper: str


@actor(
    name="native_wordcount",
    queue="native",
    retry=RetryPolicy(kind="transient", max_attempts=3),
)
async def native_wordcount(payload: NativePayload, ctx: JobContext[NativePayload]) -> NativeResult:
    """Count words — deliberately trivial; the point is registry coexistence."""
    words = payload.text.split()
    await ctx.progress(step=1, percent=50.0)
    return NativeResult(word_count=len(words), upper=payload.text.upper())
