"""Cron-scheduled ticker actor — periodic scheduling.

The ``ticker`` actor fires automatically every 30 seconds via the cron
loop.  No manual enqueue is needed; the schedule is registered at
worker startup by the ``cron()`` call below.
"""

from datetime import UTC, datetime

from pydantic import BaseModel

from taskq import JobContext, actor, cron


class TickerPayload(BaseModel):
    pass


@actor(name="ticker", queue="examples")
async def ticker(payload: TickerPayload, ctx: JobContext[TickerPayload]) -> None:
    """Fires every 30 seconds to demonstrate cron scheduling."""
    ctx.log.info("ticker fired", fired_at=datetime.now(UTC).isoformat())


cron("* * * * * */30", "ticker", name="ticker")
