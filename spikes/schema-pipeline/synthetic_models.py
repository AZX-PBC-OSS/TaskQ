"""Synthetic models covering schema features ABSENT from examples/actors/.

The real actor corpus exercises: defaults, optional, required, lists,
constrained ints (ge/le), field descriptions, UUID format, empty models.
It does NOT contain: enums, nested models, unions (other than Optional),
date/datetime, constrained strings. Those matrix rows are driven by the
synthetic models below so every cell is executed.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field


class Priority(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Address(BaseModel):
    street: str
    city: str
    zip_code: str = Field(pattern=r"^\d{5}$")


class EnumPayload(BaseModel):
    priority: Priority = Priority.MEDIUM


class ConstrainedStringPayload(BaseModel):
    slug: str = Field(min_length=3, max_length=20)
    label: str = Field(default="task", pattern=r"^[a-z]+$")


class NestedPayload(BaseModel):
    address: Address
    tags: list[str] = Field(default_factory=list)


class UnionPayload(BaseModel):
    value: int | str
    maybe: float | None = None


class DatetimePayload(BaseModel):
    started_at: datetime = Field(default_factory=lambda: datetime(2026, 1, 1, tzinfo=UTC))
    due_date: date = date(2026, 12, 31)


MODELS = [EnumPayload, ConstrainedStringPayload, NestedPayload, UnionPayload, DatetimePayload]


def main() -> None:
    out = Path(__file__).resolve().parent / "schemas"
    out.mkdir(parents=True, exist_ok=True)
    for model in MODELS:
        (out / f"synthetic.{model.__name__}.json").write_text(
            json.dumps(model.model_json_schema(), indent=2) + "\n"
        )
    print(f"collected {len(MODELS)} synthetic schemas")


if __name__ == "__main__":
    sys.exit(main())
