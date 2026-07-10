"""UUID and base62 generation helpers for TaskQ.

Uses UUIDv7 (time-ordered) so job and worker IDs are monotonically
increasing, which improves B-tree locality for PostgreSQL index inserts
and makes IDs naturally sortable by creation time.

Also provides :func:`new_base62` for short identifiers where a full UUID
is overkill.  Three precision modes are available:

- ``"random"`` — pure random, no timestamp.  Good for test bucket names
  and other labels where sortability doesn't matter.
- ``"second"`` — timestamp with second precision + random suffix.
  IDs generated in different seconds sort correctly; within the same
  second, order is arbitrary but IDs are still unique.
- ``"millisecond"`` — timestamp with millisecond precision + random suffix.
  Finer-grained sortability at the cost of more timestamp characters.
"""

from __future__ import annotations

import os
import time as _time
from enum import Enum
from uuid import UUID

import uuid_utils

from taskq.backend._protocol import JobId

__all__ = ["new_base62", "new_job_id", "new_uuid"]

_BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_BASE62_LEN = len(_BASE62)

_TS_CHARS_SECOND = 6
_TS_CHARS_MILLISECOND = 8
_MIN_LEN_SECOND = _TS_CHARS_SECOND + 1
_MIN_LEN_MILLISECOND = _TS_CHARS_MILLISECOND + 1
_MIN_LEN_RANDOM = 1


class _Precision(Enum):
    RANDOM = "random"
    SECOND = "second"
    MILLISECOND = "millisecond"


def _encode_int(value: int, width: int) -> str:
    out: list[str] = []
    for _ in range(width):
        out.append(_BASE62[value % _BASE62_LEN])
        value //= _BASE62_LEN
    if value:
        raise ValueError(f"value {value} does not fit in {width} base62 characters")
    return "".join(reversed(out))


def _random_base62(count: int) -> str:
    n = _BASE62_LEN**count
    rand = int.from_bytes(os.urandom((n.bit_length() + 7) // 8), "big") % n
    return _encode_int(rand, count)


def new_base62(
    length: int = 8,
    *,
    precision: str = "random",
    _now: float | None = None,
) -> str:
    """Return a base62 identifier of *length* characters.

    Three *precision* modes control the timestamp prefix:

    ``"random"``
        Pure random — no timestamp.  Minimum length 1.
        Good for test bucket names and labels.

    ``"second"``
        Leading chars encode the current Unix timestamp at second
        precision; remaining chars are random.  IDs from different
        seconds sort correctly (same B-tree locality benefit as UUIDv7).
        Minimum length 7 (6 timestamp + 1 random).

    ``"millisecond"``
        Same as ``"second"`` but with millisecond timestamp precision.
        Minimum length 9 (8 timestamp + 1 random).

    :param length: total character count (must be >= minimum for *precision*).
    :param precision: ``"random"``, ``"second"``, or ``"millisecond"``.
    :param _now: override the current time as a Unix timestamp (seconds).
        For testing only; production callers should omit this parameter.
    :raises ValueError: if *length* is below the minimum for *precision*.
    """
    prec = _Precision(precision)

    if prec is _Precision.RANDOM:
        if length < _MIN_LEN_RANDOM:
            raise ValueError(
                f"length must be >= {_MIN_LEN_RANDOM} for random precision, got {length}"
            )
        return _random_base62(length)

    now = _now if _now is not None else _time.time()

    if prec is _Precision.SECOND:
        ts_chars = _TS_CHARS_SECOND
        ts_val = int(now)
        min_len = _MIN_LEN_SECOND
    else:
        ts_chars = _TS_CHARS_MILLISECOND
        ts_val = int(now * 1000)
        min_len = _MIN_LEN_MILLISECOND

    if length < min_len:
        raise ValueError(f"length must be >= {min_len} for {precision} precision, got {length}")

    rand_chars = length - ts_chars
    return _encode_int(ts_val, ts_chars) + _random_base62(rand_chars)


def new_uuid() -> UUID:
    """Return a new UUIDv7 as a stdlib :class:`~uuid.UUID`."""
    return UUID(bytes=uuid_utils.uuid7().bytes)


def new_job_id() -> JobId:
    """Return a new UUIDv7 wrapped as a :data:`JobId`."""
    return JobId(new_uuid())
