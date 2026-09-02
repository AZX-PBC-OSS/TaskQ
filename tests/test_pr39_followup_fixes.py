"""Regression tests for PR #39 follow-up fixes.

Each test validates a specific issue from XBeg9's final approved review that
was merged without being addressed (rcbevans: "To make progress I'll merge
as is and address remaining comments suggestions in a follow up").

The tests are behavioral: they drive the real code with fake deps and
assert the contract that XBeg9 identified as broken or missing.

Coverage map (where each fix's pin lives after the merge dedupe):

- Fix 1 (cron transient-retry), Fix 2 (probe cleanup before guard raises),
  Fix 3 (probe continue), Fix 5 (open_dedicated_conn connection timeout),
  Fix 6 (cron timeout sleeps before re-issuing) and Fix 8
  (QueryCanceledError classification) are pinned by the behavioral variants
  in tests/test_watchdog_safety.py.
- Fix 7 (settings description wording) was retired as a prose pin: the
  behavior it gestured at — which loops the dispatcher_command_timeout
  invariant actually checks — is pinned behaviorally by
  tests/test_settings.py (test_producer_loop_not_checked_by_validator and
  the reject/allow tests above it).
- Fix 4 (validator must not check the producer loop) lives here.
"""

import pytest

# ═══════════════════════════════════════════════════════════════════════════
# Fix 4: Settings validator must not claim the producer is bounded
# ═══════════════════════════════════════════════════════════════════════════
#
# XBeg9 (settings.py:1024): "Not asking you to fix the producer here — it's
# never been bounded... What's new is this validator saying it's fine. The
# model is timeout + period, which holds for scheduled_wake and cron because
# you wrapped them. The producer isn't wrapped... Either wrap the producer
# like the leader loops, or drop it from the validator and say per-statement
# only."
#
# Fix: remove the producer from the invariant check; update the description.


async def test_settings_validator_does_not_check_producer_loop() -> None:
    """The settings validator must not certify the producer loop as bounded
    when it is not wrapped in asyncio.timeout. The validator's model
    (timeout + period) does not hold for the producer's multi-statement
    dispatch_batch. Removing the producer from the check stops the invariant
    from making a false guarantee."""

    from taskq.settings import WorkerSettings

    # A config where the producer would fail the invariant if checked:
    # notify_enabled=False, poll_interval=1.0 (producer_period=1.0),
    # dispatcher_command_timeout=9.0, watchdog_stale_floor=10.0.
    # Leader loops: budget=10, 9+1=10 >= 10 → FAILS.
    # Producer: budget=10, 9+1=10 >= 10 → would also FAIL.
    # But we want to test the producer specifically. Use a config where
    # the leader passes but the producer would fail if checked:
    # timeout=8.0, notify_enabled=True, notify_poll_interval=1.0,
    # watchdog_stale_floor=9.0.
    # Leader: budget=max(5, 9)=9, 8+1=9 >= 9 → FAILS.
    # Hmm, both share the floor. The only way to test is to verify the
    # error message does NOT mention "producer" when only the leader fails.

    # Use a config that fails the leader check:
    settings_dict = {
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_DISPATCHER_COMMAND_TIMEOUT": "9.5",
        "TASKQ_WATCHDOG_STALE_FLOOR": "10.0",
        "TASKQ_NOTIFY_ENABLED": "false",
        "TASKQ_POLL_INTERVAL": "1.0",
    }

    with pytest.raises(Exception) as exc_info:
        WorkerSettings.load_from_dict(settings_dict)

    error_msg = str(exc_info.value)
    assert "producer" not in error_msg.lower(), (
        f"the validator must not check the producer loop (it is not wrapped; "
        f"the timeout + period model does not hold for multi-statement "
        f"dispatch_batch). Error mentions producer: {error_msg}"
    )
