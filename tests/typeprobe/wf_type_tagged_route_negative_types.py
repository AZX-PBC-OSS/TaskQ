"""NEGATIVE TYPE PROBES — the type-tagged route (the routing proof's
cure): the Route literal's TWO faces are checker-gated.

The type-tagged face's keys are the payload union's member TYPES — a
raw STRING key (the enum face's habit, or the pre-cure author's guess)
is a checker error at the literal, and the declaration-time totality
check would refuse it too; the probe holds the STATIC face. The green
face: a route keyed by the union's member types passes UNMARKED (the
narrowed-arms story — the type IS the tag).

Probes:

* ``probe_string_key_refused`` — a string key on a Route whose
  vocabulary is types: the key is neither an enum member nor a class —
  ``reportArgumentType`` (pyright) / ``invalid-argument-type`` (ty).
* ``probe_string_key_on_enum_refused`` — the same string key on the
  ENUM face's route: the pre-existing discipline, now statically
  guarded at the annotation too.
"""

from __future__ import annotations

import enum

from pydantic import BaseModel

from taskq.workflows.chain import DONE, Chain, Route, Step


class Summary(BaseModel):
    doc_id: str
    text: str


class Unreadable(BaseModel):
    doc_id: str
    reason: str


class Outcome(enum.Enum):
    OK = "ok"


async def body(ctx: object, item: dict[str, object]) -> Summary | Unreadable:
    doc_id = str(item["doc_id"])
    return Summary(doc_id=doc_id, text="t")


async def reduce_body(ctx: object, item: Summary) -> Summary:
    return item


def probe_string_key_refused() -> None:
    """A STRING key on a type-tagged route — the key is neither an enum
    member nor a payload type: the checker refuses the literal."""
    bad_route = Route(
        {
            Summary: "reduce",
            Unreadable: DONE,
            "Summary": "reduce",  # MUST_ERROR(reportArgumentType, invalid-argument-type): a str key
        }
    )
    Chain(
        name="string-key-type-route",
        start="screen",
        steps={
            "screen": Step(body=body, outcomes=Summary | Unreadable, route=bad_route),
            "reduce": Step(body=reduce_body, outcomes=Summary, route=None),
        },
    )


def probe_string_key_on_enum_refused() -> None:
    """A STRING key on the enum face's route — the same static refusal
    (the enum face's unknown-key drill, guarded at the annotation)."""
    bad_enum_route = Route({"ok": DONE})  # MUST_ERROR(reportArgumentType, invalid-argument-type)
    Chain(
        name="string-key-enum-route",
        start="screen",
        steps={
            "screen": Step(body=body, outcomes=Outcome, route=bad_enum_route),
        },
    )


def probe_type_keyed_route_clean() -> None:
    """THE GREEN FACE: the route keyed by the union's member types —
    the narrowed-arms declaration passes UNMARKED."""
    Chain(
        name="type-route-clean",
        start="screen",
        steps={
            "screen": Step(
                body=body,
                outcomes=Summary | Unreadable,
                route=Route({Summary: "reduce", Unreadable: DONE}),
            ),
            "reduce": Step(body=reduce_body, outcomes=Summary, route=None),
        },
    )
