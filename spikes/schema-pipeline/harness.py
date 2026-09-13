"""Shared fixture corpus + acceptance harness for the schema-pipeline spike.

For each canonical JSON Schema we build:
  1. one VALID instance, generated from the schema itself (satisfies types,
     constraints, enums, required) — proven valid against the schema with
     the `jsonschema` Draft 2020-12 validator;
  2. deterministic INVALID mutants (type flip, constraint violation, missing
     required, bad enum, bad format) — each proven *invalid* against the
     canonical schema before being added to the corpus.

Acceptance checking is done with pydantic's TypeAdapter so the original and
regenerated models are exercised identically.
"""

from __future__ import annotations

import json
import random
import string
from typing import Any

from jsonschema import Draft202012Validator

RNG = random.Random(0xC0FFEE)


# ---------------------------------------------------------------- instance gen
PATTERN_SAMPLES = [
    (r"^\d{5}$", "12345"),
    (r"^\d+$", "123"),
    (r"^[a-z]+$", "abcde"),
    (r"^[a-zA-Z]+$", "abcde"),
    (r"^[a-z0-9_]+$", "abc_123"),
]


def _gen_string(prop: dict[str, Any], rng: random.Random) -> str:
    min_len = prop.get("minLength", 0)
    max_len = prop.get("maxLength", max(min_len, 8))
    n = max(min_len, min(8, max_len))
    s = "".join(rng.choice(string.ascii_letters) for _ in range(n))
    fmt = prop.get("format")
    if fmt == "uuid":
        return "0198f36a-1c7d-7000-8000-3b3a9c1e2d4f"  # uuid7-shaped, valid uuid
    if fmt == "date-time":
        return "2026-09-12T10:00:00Z"
    if fmt == "date":
        return "2026-09-12"
    if fmt == "time":
        return "10:00:00Z"
    if fmt == "email":
        return "user@example.com"
    if fmt == "uri":
        return "https://example.com/x"
    pattern = prop.get("pattern")
    if pattern:
        for rx, sample in PATTERN_SAMPLES:
            if rx == pattern:
                return sample
        raise ValueError(f"cannot sample pattern {pattern!r}")
    while len(s) < min_len:
        s += "x"
    return s[:max_len] if max_len >= min_len else s


def _gen_number(prop: dict[str, Any], rng: random.Random, integer: bool) -> float:
    lo = prop.get("minimum")
    hi = prop.get("maximum")
    xlo = prop.get("exclusiveMinimum")
    xhi = prop.get("exclusiveMaximum")
    lo = lo if lo is not None else (xlo + 1 if xlo is not None else None)
    hi = hi if hi is not None else (xhi - 1 if xhi is not None else None)
    if lo is None and hi is None:
        return 7 if integer else 7.5
    if lo is None:
        lo = hi - 10
    if hi is None:
        hi = lo + 10
    v = rng.uniform(lo, hi)
    return int(v) if integer else v


def _generate(prop: dict[str, Any], root: dict[str, Any], rng: random.Random) -> Any:
    if "$ref" in prop:
        ref = prop["$ref"]
        target = root
        for part in ref.lstrip("#/").split("/"):
            target = target[part]
        return _generate(target, root, rng)

    for combo_key in ("anyOf", "oneOf"):
        if combo_key in prop:
            # prefer a branch that is not "null" so the instance stays meaningful
            branches = [b for b in prop[combo_key] if b.get("type") != "null"]
            branch = (branches or prop[combo_key])[0]
            merged = {k: v for k, v in prop.items() if k != combo_key}
            merged.update(branch)
            return _generate(merged, root, rng)

    if "enum" in prop:
        return prop["enum"][0]
    if "const" in prop:
        return prop["const"]

    types = prop.get("type")
    if isinstance(types, list):
        types = [t for t in types if t != "null"] or types
        t = types[0]
    else:
        t = types

    if t == "object" or "properties" in prop:
        obj = {}
        for name, sub in (prop.get("properties") or {}).items():
            obj[name] = _generate(sub, root, rng)
        for req in prop.get("required", []):
            obj.setdefault(req, _generate(prop.get("properties", {}).get(req, {"type": "string"}), root, rng))
        return obj
    if t == "array":
        item = _generate(prop.get("items", {"type": "string"}), root, rng)
        return [item] * min(prop.get("minItems", 1), 2)
    if t == "integer":
        return _gen_number(prop, rng, integer=True)
    if t == "number":
        return _gen_number(prop, rng, integer=False)
    if t == "boolean":
        return True
    if t == "null":
        return None
    return _gen_string(prop, rng)  # string or type-less default


def valid_instance(schema: dict[str, Any], rng: random.Random = RNG) -> Any:
    """Generate an instance that validates against the canonical schema."""
    return _generate(schema, schema, rng)


# ------------------------------------------------------------------- mutants
def _iter_props(schema: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    return list((schema.get("properties") or {}).items())


def _deref(schema: dict[str, Any], prop: dict[str, Any]) -> dict[str, Any]:
    if "$ref" in prop:
        target = schema
        for part in prop["$ref"].lstrip("#/").split("/"):
            target = target[part]
        return target
    return prop


def _resolve_type(prop: dict[str, Any]) -> str | None:
    t = prop.get("type")
    if isinstance(t, list):
        non_null = [x for x in t if x != "null"]
        return non_null[0] if non_null else "null"
    if t:
        return t
    for combo in ("anyOf", "oneOf"):
        for branch in prop.get(combo, []):
            tt = _resolve_type(branch)
            if tt and tt != "null":
                return tt
    return None


def _is_nullable(prop: dict[str, Any]) -> bool:
    t = prop.get("type")
    if isinstance(t, list) and "null" in t:
        return True
    for combo in ("anyOf", "oneOf"):
        if any(b.get("type") == "null" for b in prop.get(combo, [])):
            return True
    return False


def make_mutants(schema: dict[str, Any], valid: Any) -> dict[str, Any]:
    """Build mutants, keep only those the canonical schema PROVABLY rejects."""
    validator = Draft202012Validator(schema)
    mutants: dict[str, Any] = {}

    def add(name: str, candidate: Any) -> None:
        if validator.is_valid(candidate):
            return  # not a real violation — discard
        mutants[name] = candidate

    props = _iter_props(schema)
    required = schema.get("required", [])

    # 0. explicit null for every non-nullable property — exposes nullable-widening
    for pname, prop in props:
        if not _is_nullable(_deref(schema, prop)):
            m = json.loads(json.dumps(valid))
            m[pname] = None
            add("null_violation_" + pname, m)

    # 1. type flip: find a non-null prop and give it a wrong-typed value
    for pname, prop in props:
        t = _resolve_type(_deref(schema, prop))
        if t in ("integer", "number"):
            m = json.loads(json.dumps(valid))
            m[pname] = "not-a-number"
            add("type_flip_" + pname, m)
            break
    for pname, prop in props:
        t = _resolve_type(_deref(schema, prop))
        if t in ("string",):
            m = json.loads(json.dumps(valid))
            m[pname] = {"__invalid__": "object-where-string"}
            add("type_flip_" + pname, m)
            break

    # 2. constraint violation: exceed maximum / minimum
    for pname, prop in props:
        d = _deref(schema, prop)
        if "maximum" in d:
            m = json.loads(json.dumps(valid))
            m[pname] = d["maximum"] + 1
            add("max_violation_" + pname, m)
            break
    for pname, prop in props:
        d = _deref(schema, prop)
        if "minimum" in d:
            m = json.loads(json.dumps(valid))
            m[pname] = d["minimum"] - 1
            add("min_violation_" + pname, m)
            break

    # 3. missing required
    for pname in required:
        m = json.loads(json.dumps(valid))
        m.pop(pname, None)
        add("missing_required_" + pname, m)
        break

    # 4. enum violation
    for pname, prop in props:
        d = _deref(schema, prop)
        if "enum" in d:
            m = json.loads(json.dumps(valid))
            m[pname] = "__not_in_enum__"
            add("enum_violation_" + pname, m)
            break

    # 5. format violation (uuid)
    for pname, prop in props:
        d = _deref(schema, prop)
        if d.get("format") == "uuid":
            m = json.loads(json.dumps(valid))
            m[pname] = "definitely-not-a-uuid"
            add("format_violation_" + pname, m)
            break

    # 6. garbage nested object: object-typed prop fed an unrelated shape
    for pname, prop in props:
        d = _deref(schema, prop)
        if _resolve_type(d) == "object" and ("properties" in d or "$ref" in d):
            m = json.loads(json.dumps(valid))
            m[pname] = {"__garbage__": ["not", "the", "right", "shape"]}
            add("garbage_nested_" + pname, m)
            break

    return mutants


def build_corpus(schema: dict[str, Any]) -> dict[str, Any]:
    rng = random.Random(0xC0FFEE)
    valid = valid_instance(schema, rng)
    # the valid fixture is only usable if the canonical schema itself agrees
    assert Draft202012Validator(schema).is_valid(valid), (
        f"generated valid fixture does not satisfy schema: {valid!r}"
    )
    return {"valid": valid, "invalid": make_mutants(schema, valid)}


# --------------------------------------------------------------- acceptance
def accepts(model: type, fixture: Any) -> bool:
    """True if TypeAdapter(model) accepts the fixture (JSON round-tripped)."""
    from pydantic import TypeAdapter

    ta = TypeAdapter(model)
    payload = json.loads(json.dumps(fixture))  # normalize through JSON
    try:
        ta.validate_python(payload)
        return True
    except Exception:
        return False


# ----------------------------------------------------- schema diff (shared)
def _inline_nullable(prop: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Collapse anyOf/oneOf [T, null] into (T, nullable=True)."""
    for combo in ("anyOf", "oneOf"):
        if combo in prop:
            branches = prop[combo]
            if any(b.get("type") == "null" for b in branches):
                non_null = [b for b in branches if b.get("type") != "null"]
                effective = {k: v for k, v in prop.items() if k != combo}
                if non_null:
                    effective.update(non_null[0])
                return effective, True
    return prop, False


CONFLICT_KEYS = ("enum", "minimum", "maximum", "pattern", "format", "minLength", "maxLength")


def prop_drifts(orig: dict[str, Any], regen: dict[str, Any]) -> list[str]:
    """Compare per-property semantics of two model_json_schema() outputs."""
    drifts: list[str] = []
    op, rp = orig.get("properties", {}), regen.get("properties", {})

    for name in sorted(set(op) | set(rp)):
        o, r = op.get(name), rp.get(name)
        if o is None:
            drifts.append(f"property '{name}': ADDED by codegen")
            continue
        if r is None:
            drifts.append(f"property '{name}': DROPPED by codegen")
            continue
        o_eff, o_null = _inline_nullable(o)
        r_eff, r_null = _inline_nullable(r)
        if o_null != r_null:
            drifts.append(f"property '{name}': NULLABILITY drift (nullable {o_null} -> {r_null})")
        if o_eff.get("default", "__absent__") != r_eff.get("default", "__absent__"):
            drifts.append(
                f"property '{name}': default {o_eff.get('default', '<absent>')!r} -> "
                f"{r_eff.get('default', '<absent>')!r}"
            )
        o_desc, r_desc = o_eff.get("description"), r_eff.get("description")
        if o_desc != r_desc:
            drifts.append(f"property '{name}': description {o_desc!r} -> {r_desc!r}")
        for key in CONFLICT_KEYS:
            if o_eff.get(key) != r_eff.get(key):
                drifts.append(f"property '{name}': {key} {o_eff.get(key)!r} -> {r_eff.get(key)!r}")

    oreq, rreq = set(orig.get("required", [])), set(regen.get("required", []))
    for name in sorted(oreq - rreq):
        drifts.append(f"required -> OPTIONAL drift: '{name}' no longer required")
    for name in sorted(rreq - oreq):
        drifts.append(f"optional -> REQUIRED drift: '{name}' now required")
    return drifts
