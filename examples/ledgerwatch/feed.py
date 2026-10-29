"""The mock transaction feed — the at-least-once world's producer side.

A batch is generated from a spec (the test/drill's construction), and
the feed's delivery is AT-LEAST-ONCE: ``deliver()`` may hand the SAME
batch to the pipeline twice (the duplicate-feed drill's world). The
pipeline's answer to re-delivery is the run-key arbiter + idempotent
writes — never a batch id blacklist.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from examples.ledgerwatch.models import Batch, TransactionRecord


@dataclass(frozen=True)
class BatchSpec:
    """How to synthesize one batch (the drills' recorded construction)."""

    batch_id: str
    count: int
    #: Every Nth transaction is HIGH risk (amount at/above the
    #: threshold) — 0 means none.
    high_every: int = 0
    seed: int = 0


_MERCHANTS = ("acme-grocery", "northwind-fuel", "parallel-coffee", "vector-books")
_CHANNELS = ("web", "pos", "ach")


def build_batch(spec: BatchSpec) -> Batch:
    """One batch from one spec — DETERMINISTIC (same spec, same batch):
    the re-delivery carries the SAME transactions, byte for byte."""
    rng = random.Random(f"feed:{spec.batch_id}:{spec.seed}")  # noqa: S311  # Why: the feed is deterministic per spec (the re-delivery carries the SAME batch).
    txns = [
        TransactionRecord(
            txn_id=f"{spec.batch_id}-T{i:05d}",
            amount_cents=(spec.high_every and i % spec.high_every == 0 and 75_500)
            or rng.randint(100, 49_000),
            merchant=rng.choice(_MERCHANTS),
            channel=rng.choice(_CHANNELS),
        )
        for i in range(spec.count)
    ]
    return Batch(batch_id=spec.batch_id, transactions=txns)


@dataclass
class MockFeed:
    """The feed: delivers batches, and (the at-least-once world) may
    deliver any batch AGAIN — tracked here so the drills can assert the
    pipeline's answer, and the delivery ledger shows the re-delivery."""

    deliveries: list[str] = field(default_factory=list)

    async def deliver(self, batch: Batch) -> str:
        self.deliveries.append(batch.batch_id)
        return batch.batch_id

    async def redeliver(self, batch: Batch) -> str:
        """The re-delivery: the SAME batch, byte for byte, again."""
        return await self.deliver(batch)
