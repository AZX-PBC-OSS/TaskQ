"""The MOCK external boundary — flaky, slow, sometimes hanging.

Everything here is DocShelf's own mock of the OUTSIDE world; the
TaskQ machinery it rides is real. The adverse profile is deterministic
(sha256 rolls per chunk key + attempt ordinal), so drills are
reproducible and the chaos suite's variance is honest, not random.
"""

from __future__ import annotations

import asyncio
import hashlib
import math

from examples.docshelf.config import AdverseDynamics
from examples.docshelf.models import ChunkRef, ExtractedChunk


class ExtractionTransientError(Exception):
    """The mock API's transient failure face (the ladder lands it)."""


class ExtractionExhaustedError(Exception):
    """The app's inner retry budget exhausted — the chunk dies LOUDLY."""


def roll(*parts: object) -> float:
    """A deterministic uniform [0, 1) from the parts — reproducible adversity."""
    digest = hashlib.sha256(":".join(str(p) for p in parts).encode()).hexdigest()
    return int(digest[:8], 16) / float(0xFFFFFFFF)


class MockExtractionAPI:
    """The mock extraction API: 30-200 ms of jitter, ~20% transient
    failures, ~7% hangs past the declared timeout — the recorded profile."""

    def __init__(self, cfg: AdverseDynamics) -> None:
        self._cfg = cfg

    def delay_s(self, chunk_key: str) -> float:
        span = self._cfg.extract_delay_max_s - self._cfg.extract_delay_min_s
        return self._cfg.extract_delay_min_s + roll("delay", chunk_key) * span

    def is_flaky(self, chunk_key: str, attempt: int) -> bool:
        return roll("flake", chunk_key, attempt) < self._cfg.extract_flake_rate

    def is_hang(self, chunk_key: str, attempt: int) -> bool:
        v = roll("hang", chunk_key, attempt)
        return (
            self._cfg.extract_flake_rate
            <= v
            < self._cfg.extract_flake_rate + self._cfg.extract_timeout_rate
        )

    async def call(self, chunk: ChunkRef, attempt: int) -> ExtractedChunk:
        """ONE mock API call: the delay, the flake, the hang — each
        attempt's fate is deterministic in (chunk_key, attempt)."""
        await asyncio.sleep(self.delay_s(chunk.chunk_key))
        if self.is_flaky(chunk.chunk_key, attempt):
            raise ExtractionTransientError(
                f"the mock extraction API flaked on {chunk.chunk_key} (attempt {attempt})"
            )
        if self.is_hang(chunk.chunk_key, attempt):
            # Hang past the declared timeout: the caller's timeout face
            # fires (the body enforces it); this sleep never completes.
            await asyncio.sleep(self._cfg.extract_timeout_s * 10)
        tokens = max(1, len(chunk.text.split()))
        n_entities = int(roll("entities", chunk.chunk_key) * 5)
        alphabet = "abcdefghij"
        entities = [
            f"ENT-{alphabet[int(roll('e', chunk.chunk_key, i) * 10)]}{i}" for i in range(n_entities)
        ]
        return ExtractedChunk(
            doc_id=chunk.doc_id,
            idx=chunk.idx,
            chunk_key=chunk.chunk_key,
            tokens=tokens,
            entities=entities,
        )


def chunk_count(doc_id: str, content: str, large_floor: int = 8, large_ceiling: int = 12) -> int:
    """Deterministic chunking: 2-6 chunks normally; ~15% of documents
    are LARGE (8-12 chunks) — the child-flow face's trigger."""
    r = roll("chunks", doc_id)
    if r >= 0.85:
        return large_floor + int(roll("chunks-big", doc_id) * (large_ceiling - large_floor + 1))
    words = max(1, len(content.split()))
    base = 2 + int(r * 5)
    return max(2, min(base, 6, max(2, math.ceil(words / 3))))


def sub_analysis_duration_s(doc_id: str, cfg: AdverseDynamics) -> float:
    """The sub-analysis's 1-3 s run time, deterministic per doc."""
    span = cfg.sub_analysis_max_s - cfg.sub_analysis_min_s
    return cfg.sub_analysis_min_s + roll("sub-duration", doc_id) * span


class SubAnalysisBoomError(Exception):
    """The poison documents' terminal failure (the child drill's arm)."""
