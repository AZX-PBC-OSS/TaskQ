"""The deterministic feed generator.

The feed is generated, not read from a real system, so the drills are
reproducible: the same row count + seed always produces the same file,
the same content hash, and the same invalid rows. Invalid rows are
placed by a fixed stride, so the expected quarantine count is exact by
construction, not by assertion.
"""

import hashlib
import json
import random
from datetime import UTC, date, timedelta
from pathlib import Path

from examples.ledger_import.models import CURRENCIES, MEMO_MAX_CHARS

# Every NTH row is invalid; the classes cycle in a fixed order so each
# intake rule gets its share of the quarantine.
INVALID_STRIDE = 13
INVALID_CLASSES = (
    "negative_amount",
    "unsupported_currency",
    "bad_account_format",
    "memo_too_long",
    "future_posting",
)
# The feed's invalid classes are minted to trip exactly one intake rule
# each; the validator's reason strings are the quarantine's taxonomy.
CLASS_TO_REASON = {
    "negative_amount": "amount_must_be_positive",
    "unsupported_currency": "currency_not_supported",
    "bad_account_format": "account_code_format",
    "memo_too_long": "memo_too_long",
    "future_posting": "posted_at_not_in_future",
}

ACCOUNTS = [f"AC{i:06d}" for i in range(1, 21)]


def expected_invalid_count(rows: int) -> int:
    """How many invalid rows a feed of ``rows`` carries (the stride's count)."""
    return len(range(INVALID_STRIDE, rows, INVALID_STRIDE))


def expected_invalid_reason_counts(rows: int) -> dict[str, int]:
    """The per-reason quarantine counts for a feed of ``rows`` (exact)."""
    counts: dict[str, int] = {CLASS_TO_REASON[c]: 0 for c in INVALID_CLASSES}
    n = 0  # 0-based invalid-row index
    for i in range(rows):
        if i > 0 and i % INVALID_STRIDE == 0:
            counts[CLASS_TO_REASON[INVALID_CLASSES[n % len(INVALID_CLASSES)]]] += 1
            n += 1
    return counts


def _row_at(i: int, rng: random.Random, today: date) -> dict[str, object]:
    """Row ``i``'s raw shape — invalid iff ``i`` is the stride's row."""
    invalid_class = None
    if i > 0 and i % INVALID_STRIDE == 0:
        invalid_class = INVALID_CLASSES[(i // INVALID_STRIDE - 1) % len(INVALID_CLASSES)]

    account = rng.choice(ACCOUNTS)
    amount = round(rng.uniform(1, 5_000), 2)  # valid rows: always positive
    currency = rng.choice(CURRENCIES)
    memo_words = rng.sample(
        ("invoice", "refund", "subscription", "adjustment", "transfer", "fee"), k=3
    )
    memo = " ".join(memo_words) + f" #{i:06d}"
    posted = today - timedelta(days=rng.randint(0, 365))

    row: dict[str, object] = {
        "entry_id": f"e_{i:06d}",
        "account": account,
        "amount": amount,
        "currency": currency,
        "memo": memo,
        "posted_at": posted.isoformat(),
    }

    if invalid_class == "negative_amount":
        row["amount"] = -abs(amount) - 1.0
    elif invalid_class == "unsupported_currency":
        row["currency"] = "XXX"
    elif invalid_class == "bad_account_format":
        row["account"] = f"acct-{i}"
    elif invalid_class == "memo_too_long":
        row["memo"] = "m" * (MEMO_MAX_CHARS + 1)
    elif invalid_class == "future_posting":
        row["posted_at"] = (today + timedelta(days=10)).isoformat()
    return row


def generate_feed(path: str, rows: int, seed: int = 7) -> str:
    """Write the feed as JSONL; return its sha256 (the content key).

    Deterministic for a given (rows, seed, day): the rng is seeded and
    the posting dates are anchored to today, so the future-posting rows
    are genuinely in the future when the import validates them.
    """
    rng = random.Random(seed)  # noqa: S311 — deterministic feed, not crypto
    today = date.today()
    digest = hashlib.sha256()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8") as fh:
        for i in range(rows):
            line = json.dumps(_row_at(i, rng, today), sort_keys=True)
            digest.update(line.encode())
            digest.update(b"\n")
            fh.write(line + "\n")
    return digest.hexdigest()


def read_feed(path: str) -> list[dict[str, object]]:
    """Read every record back (the ingest's raw shape)."""
    with Path(path).open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def feed_sha256(path: str) -> str:
    """The feed file's content hash (the run input's content key)."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def utc_now_iso() -> str:
    from datetime import datetime

    return datetime.now(UTC).isoformat()
