# CLI

The `taskq` console entry point (Typer).

The directive below renders `taskq.cli` as a Python module: the `typer.Typer`
sub-apps and the command callbacks behind each `taskq` subcommand. That is the
implementation, not the command surface - flags, defaults, exit codes and
example output are documented in the
[CLI reference](../guides/cli.md), which is the operator-facing document for
every subcommand.

The subcommand groups: `taskq worker`, `taskq migrate`, `taskq doctor`,
`taskq insights`, `taskq ui`, `taskq actor-config`, and — for the workflow
engine's runs — `taskq flows` (`list` / `status` / `holds` / `signal` /
`resolve` / `cancel` / `retry`; the workflows' operator surface, documented
with examples in the [CLI reference's `taskq flows` section](../guides/cli.md#taskq-flows)
and the [workflow guide's diagnosis runbook](../guides/workflows.md)).

## Module reference

::: taskq.cli
