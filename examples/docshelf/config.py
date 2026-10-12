"""DocShelf's configuration — INCLUDING the injected adverse dynamics.

THE ADVERSE DYNAMICS ARE THE APP'S OWN CONFIG, SEVERITY-TUNED AND
RECORDED HERE. Every external system is mocked at the boundary (the
mock extraction API, the upload feed, the sub-analysis compute); the
TaskQ machinery (Postgres, rows, workers, the retry ladders, the
holds, the timers) is real. The severities below are the adopted
profile the drills run under — every drill's receipt names them.

Each knob reads an env override so a drill can tighten or loosen the
world without touching the code; the defaults are the recorded
adoption profile.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    return raw if raw else default


@dataclass(frozen=True, slots=True)
class AdverseDynamics:
    """The injected world, recorded. The drills' receipts cite these."""

    #: FLAKINESS — the mock extraction API fails transiently on ~20% of
    #: calls; the retry ladder lands it. No document dies of flakiness.
    extract_flake_rate: float = 0.20

    #: TIMEOUTS — ~7% of calls hang past the declared timeout; the
    #: timeout face fires and the ladder runs.
    extract_timeout_rate: float = 0.07

    #: The DECLARED timeout the extraction body enforces on the mock call.
    extract_timeout_s: float = 1.0

    #: DELAYS — extraction takes 30-200 ms of jitter. The chunk fan-out's
    #: concurrency is real (the drills measure the overlap).
    extract_delay_min_s: float = 0.03
    extract_delay_max_s: float = 0.200

    #: SLOW STEPS — the sub-analysis child flows run 1-3 s. The parent's
    #: slot is released while the child runs (the slot law, observed).
    sub_analysis_min_s: float = 1.0
    sub_analysis_max_s: float = 3.0

    #: The extraction ladder's depth (stamped on the map's child rows).
    #: The ladder HONORS this stamp (friction F1 is CURED — the holdmap
    #: lane's ladder-inheritance cure: the map item ladders to the
    #: wired max_attempts, pinned green). The inner budget below stays
    #: as the cheap in-attempt absorber — a flake costs a re-call, not
    #: a body re-run.
    extract_max_attempts: int = 8

    #: The app's INNER retry budget (the flaky-call absorber): 10 inner
    #: calls at the recorded profile (flake 0.20 + timeout 0.07 per
    #: call) die with probability ~0.27^10 ≈ 2e-6 per chunk — no
    #: document dies of flakiness at this depth (a 12-chunk document
    #: dies at ~2.5e-5, a 500-doc batch at ~1.2e-2 expected deaths —
    #: and the drills' receipts would name it).
    extract_inner_retries: int = 10


@dataclass(frozen=True, slots=True)
class DocShelfConfig:
    """The whole app's config: the product's knobs + the adverse world."""

    #: Documents with at least this many chunks spawn a CHILD FLOW for
    #: the expensive sub-analysis; smaller documents analyze inline.
    many_chunks_threshold: int = 4

    #: The parent's hold timeout while a child flow runs. Long by design
    #: (a child runs 1-3 s; the hold must outlive the worst fleet stall),
    #: declared on the gate so the compile surfaces see it.
    sub_wait_timeout_s: float = 120.0

    #: The nightly re-index's schedule (6-field cron OK; the 6th field
    #: is seconds). Tests override to a seconds cadence.
    reindex_cron: str = "0 3 * * *"

    #: The feed's PAGE SIZE (one flow run per page): the measured safe
    #: scale for one flow's route fan (the digest's early-fire face past
    #: ~150 elements — friction F4).
    delivery_page_docs: int = 100

    #: The happy-path batch size (the 50-document shelf).
    happy_batch_docs: int = 50

    #: The flake drill's batch size (the 500-document batch).
    flake_batch_docs: int = 500

    #: The child drill arms a document's sub-analysis to fail terminally
    #: (its own ladder exhausted); the failure must land in the parent's
    #: typed failure arm — never silent.
    poison_prefix: str = "poison-"

    dynamics: AdverseDynamics = AdverseDynamics()

    @classmethod
    def load(cls) -> DocShelfConfig:
        dyn = AdverseDynamics(
            extract_flake_rate=_env_float("DOCSHELF_FLAKE_RATE", 0.20),
            extract_timeout_rate=_env_float("DOCSHELF_TIMEOUT_RATE", 0.07),
            extract_timeout_s=_env_float("DOCSHELF_EXTRACT_TIMEOUT_S", 1.0),
            extract_delay_min_s=_env_float("DOCSHELF_DELAY_MIN_S", 0.03),
            extract_delay_max_s=_env_float("DOCSHELF_DELAY_MAX_S", 0.200),
            sub_analysis_min_s=_env_float("DOCSHELF_SUB_MIN_S", 1.0),
            sub_analysis_max_s=_env_float("DOCSHELF_SUB_MAX_S", 3.0),
            extract_max_attempts=_env_int("DOCSHELF_EXTRACT_ATTEMPTS", 8),
            extract_inner_retries=_env_int("DOCSHELF_INNER_RETRIES", 10),
        )
        return cls(
            many_chunks_threshold=_env_int("DOCSHELF_MANY_CHUNKS", 4),
            sub_wait_timeout_s=_env_float("DOCSHELF_SUB_WAIT_S", 120.0),
            reindex_cron=_env_str("DOCSHELF_REINDEX_CRON", "0 3 * * *"),
            happy_batch_docs=_env_int("DOCSHELF_HAPPY_DOCS", 50),
            flake_batch_docs=_env_int("DOCSHELF_FLAKE_DOCS", 500),
            dynamics=dyn,
        )


#: The queues DocShelf's workers subscribe to (one actor, one queue —
#: the placement law).
DOCSHELF_QUEUES: tuple[str, ...] = (
    "docshelf-feed",
    "docshelf-cpu",
    "docshelf-extract",
    "docshelf-child",
    "docshelf-summary",
    "docshelf-ledger",
    "docshelf-quarantine",
    "docshelf-digest",
    "docshelf-cron",
)
