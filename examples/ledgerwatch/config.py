"""LedgerWatch's configuration — INCLUDING the adverse-world profile.

The adverse dynamics are part of the product's own config, recorded here
(severity-tuned), because an adopting team's mock boundary is where the
chaos lives: the mock enrichment API fails transiently, hangs past its
declared timeout, and takes realistic time. The pipeline must survive all
of it — the tests prove it, never the config.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DynamicsProfile:
    """The injected world at the mock enrichment boundary.

    Every field is severity-tuned and RECORDED (this class is the
    record): a test run's behavior is only interpretable against the
    profile that produced it.
    """

    #: Fraction of enrichment calls that fail transiently with a typed
    #: error. The retry ladder must land them.
    flake_rate: float = 0.0
    #: Fraction of enrichment calls that HANG past the client's declared
    #: timeout. The timeout face fires; the ladder re-runs the attempt.
    timeout_rate: float = 0.0
    #: Wall-clock jitter the mock API charges per call (seconds).
    latency_min_s: float = 0.0
    latency_max_s: float = 0.0
    #: THE DECLARED TIMEOUT on the external call (the client's own
    #: deadline, enforced with ``asyncio.timeout``).
    enrich_timeout_s: float = 1.0
    #: How far past the declared timeout the hanging calls run before
    #: the server-side would finish (they are cancelled at the deadline).
    hang_overshoot_s: float = 2.0

    def labeled(self) -> dict[str, float]:
        """The profile as a plain dict (the receipts carry it)."""
        return {
            "flake_rate": self.flake_rate,
            "timeout_rate": self.timeout_rate,
            "latency_min_s": self.latency_min_s,
            "latency_max_s": self.latency_max_s,
            "enrich_timeout_s": self.enrich_timeout_s,
            "hang_overshoot_s": self.hang_overshoot_s,
        }


#: The CLEAN profile: the world behaving. The happy-path drill's exact
#: receipt comes from this profile.
CLEAN_PROFILE = DynamicsProfile(latency_min_s=0.005, latency_max_s=0.02)

#: The ADVERSE profile: 15% transient flake + 5% hangs past the declared
#: timeout + 50-300 ms of realistic jitter. The drills' world.
ADVERSE_PROFILE = DynamicsProfile(
    flake_rate=0.15,
    timeout_rate=0.05,
    latency_min_s=0.05,
    latency_max_s=0.30,
    enrich_timeout_s=1.0,
    hang_overshoot_s=2.0,
)

#: The risk-class threshold (the generic industry rule: transactions at
#: or above the threshold amount need human approval).
HIGH_RISK_THRESHOLD_CENTS = 50_000


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    return float(raw) if raw else default


@dataclass(frozen=True, slots=True)
class LedgerWatchSettings:
    """The service's operational settings (env-tunable, recorded)."""

    #: The HITL approval window (seconds). A HIGH-risk transaction no
    #: human approves within the window escalates to the compliance
    #: queue. The expiry is AT-LEAST (DB-clock deadline + the sweep's
    #: cadence) — the fail-close is the guarantee, never the precision.
    approval_window_s: float = 300.0
    #: The reconciliation timer's scheduled delay (seconds).
    recon_delay_s: float = 60.0
    #: The enrichment ladder's budget (per map child). NOTE (the
    #: adoption's finding F3): the framework's map-item ladder caps at
    #: 3 attempts regardless of this knob (the row carries the wired
    #: value; the ladder does not honor it — evidence in the repo's
    #: finding ledger), so the DEEP retry budget for transient
    #: enrichment errors is the CLIENT's own policy below.
    enrich_max_attempts: int = 6
    #: The client's OWN retry policy for transient enrichment errors
    #: (in-body: the external-call retries are the product's concern;
    #: each retry is journalled as its own call).
    enrich_client_retries: int = 8

    def labeled(self) -> dict[str, float | int]:
        return {
            "approval_window_s": self.approval_window_s,
            "recon_delay_s": self.recon_delay_s,
            "enrich_max_attempts": self.enrich_max_attempts,
            "enrich_client_retries": self.enrich_client_retries,
        }


def load_settings() -> LedgerWatchSettings:
    """The env-tuned settings (LW_* env vars, all optional)."""
    return LedgerWatchSettings(
        approval_window_s=_env_float("LW_APPROVAL_WINDOW_S", 300.0),
        recon_delay_s=_env_float("LW_RECON_DELAY_S", 60.0),
        enrich_max_attempts=int(_env_float("LW_ENRICH_MAX_ATTEMPTS", 6)),
        enrich_client_retries=int(_env_float("LW_ENRICH_CLIENT_RETRIES", 8)),
    )


def load_profile() -> DynamicsProfile:
    """The env-selected dynamics profile (LW_PROFILE=clean|adverse).

    Default: clean. The drills run with LW_PROFILE=adverse.
    """
    match os.environ.get("LW_PROFILE", "clean"):
        case "adverse":
            return ADVERSE_PROFILE
        case _:
            return CLEAN_PROFILE


#: The queues the service's nodes ride (one actor, one queue — the
#: estate's law).
QUEUE_CPU = "lw-cpu"
QUEUE_ENRICH = "lw-enrich"
QUEUE_RECON = "lw-recon"

#: Every queue a LedgerWatch worker must subscribe to.
ALL_QUEUES = (QUEUE_CPU, QUEUE_ENRICH, QUEUE_RECON)
