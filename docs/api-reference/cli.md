# CLI

The `taskq` console entry point (Typer).

The directive below renders `taskq.cli` as a Python module: the `typer.Typer`
sub-apps and the command callbacks behind each `taskq` subcommand. That is the
implementation, not the command surface - flags, defaults, exit codes and
example output are documented in the
[CLI reference](../guides/cli.md), which is the operator-facing document for
every subcommand.

## Module reference

::: taskq.cli
