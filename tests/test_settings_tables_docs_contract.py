"""The settings tables are a CONTRACT, not prose: docs cannot drift from code.

The docs review's L-tier finding: the pages that tabulate the settings
surface (``configuration.md``'s ``Env Var`` reference, ``workers.md``'s
``WorkerSettings`` table, the per-guide ``Variable`` tables in
``admin-ui.md`` / ``cli.md`` / ``deployment.md`` / ``observability.md`` /
``progress.md``, ``timescaledb.md``'s flag table, and ``sso.md``'s
``OIDCSettings`` / ``SAMLSettings`` field tables) were silent-drift
surfaces -- nothing failed CI when the code's settings changed and the
tables went stale. This module pins them in both directions:

1. **documented-but-doesn't-exist** -- every ``TASKQ_*`` name a docs
   settings table tabulates must be a real field of the canonical
   settings classes (``TaskQSettings`` / ``WorkerSettings`` /
   ``OIDCSettings`` / ``SAMLSettings`` in ``taskq.settings``, the single
   source of truth every consumer loads through).
2. **every documented default matches the code default** -- the
   ``Default`` cell of each tabulated row must agree with the field's
   ``FieldInfo`` default, through the rendering resolver documented
   below.
3. **exists-but-undocumented** -- every public settings field must
   appear (by its env-var name) in at least one docs settings table,
   so a new knob cannot ship without a table row.

The resolver's honest limits (what is pinnable and how -- deliberately
no vacuous skips):

- **Scalar defaults** (bool/int/float) compare numerically, so the
  docs' ``1750.0`` and the code's ``1750`` agree, and ``True``/``true``
  spellings are the same default.
- **timedelta defaults** compare as durations: the docs render them as
  ``7d`` / ``1h`` / ``24h`` (``configuration.md``) and ISO ``PT24H``
  (``workers.md``) -- same quantity, different spellings.
- **``None`` defaults** accept the docs' prose for "unset": ``None``,
  ``unset``, ``_(none)_``, ``(unbounded)``, ``derived ...``, ``falls
  back to ...``. These defaults are genuinely dynamic (the value is
  computed from other settings at load), so the pin asserts the *unset*
  state, which is the static, pinnable part.
- **Empty-string defaults** (``TASKQ_HEALTH_TOKEN`` and the SSO
  secret/credential fields) are "not supplied" defaults; the docs
  render them ``"" (empty)`` / ``_(none)_`` / ``n/a``, all accepted.
- **Non-empty string defaults** compare as exact backticked spans
  (``taskq``, ``json``, ``INFO``, ``03:00``, ``none`` ...).
- **The DSN default** (``TASKQ_PG_DSN``) is compared credential-masked:
  dotenvmodel's ``PostgresDsn`` masks the password in its ``str`` while
  the docs print the real default DSN; masking both sides the same way
  compares host/port/database while staying honest about the secret.
- **``TASKQ_QUEUES``** is the one factory default; its docs cell is the
  list literal ``["default"]``, compared quote-style-normalized.

The drills at the bottom prove the pin is red-first in-suite: each
feeds a mutated copy (a staled default, a new undocumented field, a
phantom documented name) through the same reconciliation the real
assertions use and asserts the specific failure is reported. The same
mutations were also applied to the real files and observed to go red
before this PR shipped.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from taskq.settings import OIDCSettings, SAMLSettings, TaskQSettings, WorkerSettings

_REPO = Path(__file__).resolve().parent.parent
_DOCS = _REPO / "docs"

# ── docs-side parsing ──────────────────────────────────────────────────
# A "settings table" is any markdown table whose header names a
# settings-variable column. The spellings in use across docs/ (swept):
# configuration.md's "Env Var", workers.md's and sso.md's "Env var",
# the per-guide "Variable" tables, progress.md's "Environment
# variable", timescaledb.md's "Setting", ops.md's "Knob".

_VAR_COLUMN_NAMES = frozenset(
    {"Env Var", "Env var", "Environment variable", "Variable", "Setting", "Knob"}
)
_DEFAULT_COLUMN = "Default"
_ROW_RE = re.compile(r"^\|")
_SEPARATOR_RE = re.compile(r"^\|[-: |]+\|$")
_TASKQ_NAME_RE = re.compile(r"TASKQ_[A-Z0-9_]+")


@dataclass(frozen=True)
class _DocsRow:
    """One tabulated settings row: the name(s) it documents and its Default cell."""

    names: tuple[str, ...]
    default_cell: str | None
    where: str


def _split_cells(line: str) -> list[str]:
    """Split a table line on ``|`` -- NOT on the escaped ``\\|`` the docs
    use inside type cells (``str \\| None``), which would otherwise shift
    every later column (this bug silently misread Default cells)."""
    return [c.replace("\\|", "|").strip() for c in re.split(r"(?<!\\)\|", line)]


def _markdown_tables(text: str) -> list[tuple[int, list[str], list[list[str]]]]:
    """Every markdown table as (1-based start line, header, rows), in order."""
    lines = text.splitlines()
    tables: list[tuple[int, list[str], list[list[str]]]] = []
    i = 0
    while i < len(lines):
        if _ROW_RE.match(lines[i]) and i + 1 < len(lines) and _SEPARATOR_RE.match(lines[i + 1]):
            header = _split_cells(lines[i].strip("|"))
            rows: list[list[str]] = []
            j = i + 2
            while j < len(lines) and _ROW_RE.match(lines[j]):
                rows.append(_split_cells(lines[j].strip("|")))
                j += 1
            tables.append((i + 1, header, rows))
            i = j
        else:
            i += 1
    return tables


def _docs_settings_rows() -> list[_DocsRow]:
    """Every ``TASKQ_*`` row across every docs settings table."""
    rows: list[_DocsRow] = []
    for md_path in sorted(_DOCS.rglob("*.md")):
        rel = md_path.relative_to(_REPO)
        for table_line, header, table_rows in _markdown_tables(md_path.read_text()):
            var_col = next((k for k, h in enumerate(header) if h in _VAR_COLUMN_NAMES), None)
            if var_col is None:
                continue
            default_col = header.index(_DEFAULT_COLUMN) if _DEFAULT_COLUMN in header else None
            for row_no, row in enumerate(table_rows, 1):
                if len(row) <= var_col:
                    continue
                names = tuple(
                    n for n in _TASKQ_NAME_RE.findall(row[var_col]) if not n.endswith("_")
                )
                if not names:
                    continue
                default_cell = (
                    row[default_col] if default_col is not None and default_col < len(row) else None
                )
                rows.append(
                    _DocsRow(
                        names=names,
                        default_cell=default_cell,
                        where=f"{rel}:{table_line} row {row_no}",
                    )
                )
    return rows


# ── code-side inventory ────────────────────────────────────────────────


@dataclass(frozen=True)
class _CodeSetting:
    env_name: str
    cls_name: str
    field_name: str
    default: object


def _code_inventory() -> dict[str, _CodeSetting]:
    """The env-var -> default map of every public settings field.

    Source of truth: the four settings classes' ``_fields`` (dotenvmodel
    stores field metadata there; the metaclass resolves annotation
    strings before this runs). ``default_factory`` is resolved once --
    the only factory default is ``queues``.
    """
    inventory: dict[str, _CodeSetting] = {}
    for cls in (TaskQSettings, WorkerSettings, OIDCSettings, SAMLSettings):
        # pyright treats a "# pyright:" comment as a directive, so this note
        # avoids that prefix: the _fields store read below is dotenvmodel's
        # field-metadata surface (there is no public accessor); reading it
        # here is the whole point of the code side of this pin.
        fields = cls._fields  # pyright: ignore[reportPrivateUsage]  # Why: dotenvmodel's field-metadata store; no public accessor exists.
        for field_name, (_field_type, info) in fields.items():
            env_name = info.alias if info.alias else f"{cls.env_prefix}{field_name.upper()}"
            default = info.default_factory() if info.default_factory is not None else info.default
            inventory[env_name] = _CodeSetting(
                env_name=env_name,
                cls_name=cls.__name__,
                field_name=field_name,
                default=default,
            )
    return inventory


# ── the default resolver ───────────────────────────────────────────────

_SPAN_RE = re.compile(r"`([^`]+)`")
_SHORT_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(d|h|m|s)$", re.IGNORECASE)
_ISO_DURATION_RE = re.compile(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?)?$", re.IGNORECASE)
_NONE_MARKERS = ("none", "unset", "unbounded", "(none)", "derived", "falls back", "n/a")
_EMPTY_STR_MARKERS = ("n/a", "(none)", "(empty)")


def _mask_dsn(url: str) -> str:
    """Mask userinfo credentials so a DSN compares on host/port/database."""
    return re.sub(r"(://)[^@/]*@", r"\1***@", url)


def _coerce_duration(text: str) -> timedelta | None:
    """``7d`` / ``1h`` / ``24h`` / ``PT24H`` -> the duration, else ``None``.

    The docs render timedeltas two ways: ``configuration.md`` uses the
    short human form, ``workers.md`` the ISO 8601 form.
    """
    short = _SHORT_DURATION_RE.match(text.strip())
    if short is not None:
        amount = float(short.group(1))
        unit = short.group(2).lower()
        if unit == "d":
            return timedelta(days=amount)
        if unit == "h":
            return timedelta(hours=amount)
        if unit == "m":
            return timedelta(minutes=amount)
        return timedelta(seconds=amount)
    iso = _ISO_DURATION_RE.match(text.strip())
    if iso is not None and (iso.group(1) or iso.group(2) or iso.group(3)):
        return timedelta(
            days=int(iso.group(1) or 0),
            hours=int(iso.group(2) or 0),
            minutes=int(iso.group(3) or 0),
        )
    return None


def _looks_numeric(span: str) -> bool:
    try:
        float(span.strip())
    except ValueError:
        return False
    return True


def _default_matches(default: object, cell: str) -> bool:
    """Does the docs ``Default`` cell render *default*?

    The one implementation of the resolver semantics documented in the
    module docstring.
    """
    spans = _SPAN_RE.findall(cell)
    plain = cell.replace("`", "").strip().lower()

    if default is None:
        return any(s.strip().lower() == "none" for s in spans) or any(
            marker in plain for marker in _NONE_MARKERS
        )

    if isinstance(default, str):
        if "://" in default:  # a DSN: compare credential-masked
            return any("://" in s and _mask_dsn(s) == _mask_dsn(default) for s in spans)
        if default == "":  # the "not supplied" default: n/a / "" / (none)
            return any(s.strip() in ('""', "''") for s in spans) or any(
                marker in plain for marker in _EMPTY_STR_MARKERS
            )
        return default in spans  # exact backticked literal

    if isinstance(default, bool):
        return any(s.strip().lower() == str(default).lower() for s in spans)

    if isinstance(default, timedelta):
        return any(_coerce_duration(s) == default for s in spans)

    if isinstance(default, (int, float)):  # bool was handled above
        return any(
            s.strip().lower() not in ("true", "false", "none")
            and _looks_numeric(s)
            and float(s.strip()) == float(default)
            for s in spans
        )

    if isinstance(default, list):  # the queues factory default: ["default"]
        rendered = str(default)
        return any(s.replace('"', "'") == rendered for s in spans)

    return False  # an unknown default shape is never silently pinnable


# ── reconciliation (shared by the real pins and the drills) ────────────


def _documented_unknown_settings(
    docs_rows: list[_DocsRow], inventory: dict[str, _CodeSetting]
) -> list[str]:
    return [
        f"{name} tabulated at {row.where} matches no settings field"
        for row in docs_rows
        for name in row.names
        if name not in inventory
    ]


def _documented_default_mismatches(
    docs_rows: list[_DocsRow], inventory: dict[str, _CodeSetting]
) -> list[str]:
    mismatches: list[str] = []
    for row in docs_rows:
        if row.default_cell is None:
            continue
        for name in row.names:
            setting = inventory.get(name)
            if setting is None:
                continue  # reported by _documented_unknown_settings
            if not _default_matches(setting.default, row.default_cell):
                mismatches.append(
                    f"{name} at {row.where}: docs Default {row.default_cell!r} "
                    f"!= code default {setting.default!r} ({setting.cls_name}.{setting.field_name})"
                )
    return mismatches


def _undocumented_code_settings(
    docs_rows: list[_DocsRow], inventory: dict[str, _CodeSetting]
) -> list[str]:
    documented = {name for row in docs_rows for name in row.names}
    return sorted(
        f"{setting.env_name} ({setting.cls_name}.{setting.field_name})"
        for setting in inventory.values()
        if setting.env_name not in documented
    )


# ── the pins ───────────────────────────────────────────────────────────


def test_the_scanned_surface_is_not_vacuous() -> None:
    """The parser must have found a real inventory on both sides, or every
    other assertion here proves nothing (the test_suite_hygiene pattern:
    state the floor a discovery walk must clear)."""
    docs_rows = _docs_settings_rows()
    inventory = _code_inventory()
    documented = {name for row in docs_rows for name in row.names}
    assert len(inventory) >= 150, f"code inventory degraded to {len(inventory)} fields"
    assert len(documented) >= 150, f"docs tables degraded to {len(documented)} names"
    # The canonical reference pages must keep contributing rows.
    configuration_rows = [r for r in docs_rows if "configuration.md" in r.where]
    sso_rows = [r for r in docs_rows if r.where.startswith("docs/guides/sso.md")]
    workers_rows = [r for r in docs_rows if "workers.md" in r.where]
    assert len(configuration_rows) >= 100, (
        f"configuration.md contributed only {len(configuration_rows)} rows"
    )
    assert len(sso_rows) >= 15, f"sso.md contributed only {len(sso_rows)} rows"
    assert len(workers_rows) >= 40, f"workers.md contributed only {len(workers_rows)} rows"


def test_every_documented_setting_exists_in_code() -> None:
    """No docs settings table may tabulate a ``TASKQ_*`` name the settings
    classes do not define -- a renamed/removed field must leave the docs
    in the same commit, or this goes red."""
    offenders = _documented_unknown_settings(_docs_settings_rows(), _code_inventory())
    assert not offenders, (
        "Documented-but-nonexistent settings (docs tables reference names "
        "the settings classes do not define):\n  " + "\n  ".join(offenders)
    )


def test_every_documented_default_matches_the_code_default() -> None:
    """Every tabulated Default cell must agree with the field's code
    default (rendering-resolver semantics in the module docstring)."""
    offenders = _documented_default_mismatches(_docs_settings_rows(), _code_inventory())
    assert not offenders, (
        "Documented defaults that do not match the code defaults:\n  " + "\n  ".join(offenders)
    )


def test_every_code_setting_is_documented_in_a_docs_table() -> None:
    """Every public settings field must appear in at least one docs
    settings table -- a new knob cannot ship silently undocumented."""
    offenders = _undocumented_code_settings(_docs_settings_rows(), _code_inventory())
    assert not offenders, "Code settings missing from every docs settings table:\n  " + "\n  ".join(
        offenders
    )


# ── red-first drills ───────────────────────────────────────────────────
# The pin's failure modes, proven in-suite: each drill feeds a mutated
# copy through the SAME reconciliation the real assertions run and
# asserts the specific offender is named.


def test_drill_a_staled_documented_default_turns_the_pin_red() -> None:
    """Mutating the code's default for a documented field must make the
    default pin name that field."""
    docs_rows = _docs_settings_rows()
    inventory = _code_inventory()
    stale = dict(inventory)
    stale["TASKQ_MAX_CONCURRENCY"] = _CodeSetting(
        env_name="TASKQ_MAX_CONCURRENCY",
        cls_name="WorkerSettings",
        field_name="max_concurrency",
        default=9,
    )
    offenders = _documented_default_mismatches(docs_rows, stale)
    assert offenders and all("TASKQ_MAX_CONCURRENCY" in o for o in offenders), offenders


def test_drill_an_undocumented_code_setting_turns_the_pin_red() -> None:
    """A new settings field with no docs row must be named by the
    reverse-coverage pin."""
    docs_rows = _docs_settings_rows()
    inventory = _code_inventory()
    grown = dict(inventory)
    grown["TASKQ_BRAND_NEW_KNOB"] = _CodeSetting(
        env_name="TASKQ_BRAND_NEW_KNOB",
        cls_name="WorkerSettings",
        field_name="brand_new_knob",
        default=False,
    )
    offenders = _undocumented_code_settings(docs_rows, grown)
    assert "TASKQ_BRAND_NEW_KNOB (WorkerSettings.brand_new_knob)" in offenders, offenders


def test_drill_a_phantom_documented_setting_turns_the_pin_red() -> None:
    """A docs row for a setting the code never had must be named by the
    forward pin -- the typo'd-env-var class ``taskq doctor`` reports at
    runtime, caught here at review time."""
    docs_rows = _docs_settings_rows()
    inventory = _code_inventory()
    phantom_rows = [
        *docs_rows,
        _DocsRow(
            names=(
                "TASKQ_MAX_CONCURENCY",
            ),  # the missing-second-c typo configuration.md warns about
            default_cell="`4`",
            where="drill",
        ),
    ]
    offenders = _documented_unknown_settings(phantom_rows, inventory)
    assert offenders == ["TASKQ_MAX_CONCURENCY tabulated at drill matches no settings field"], (
        offenders
    )
