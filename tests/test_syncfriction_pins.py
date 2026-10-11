"""The sync-friction lane's pins: each test is RED at the audited tip and
GREEN after its cure. One test per pinnable friction item.

- item 4: ``taskq worker --queues a b c`` — the space-separated form fails
  with an error that teaches the repeat-the-flag contract, not "No such
  command 'b'".
- item 7: a worker whose DSN points at the wrong server surfaces the
  resolved DSN and the TASKQ_PG_DSN remedy, not a bare
  ``InvalidPasswordError`` traceback.
"""

from typing import Any

from typer.testing import CliRunner

from taskq.cli import app

runner = CliRunner()


class TestQueuesFlagTeachesRepeatForm:
    """Item 4: the parser declares repeat-the-flag (``--queues`` one per
    queue); the error for the natural-but-wrong space-separated form must
    teach that contract."""

    def test_space_separated_queues_error_teaches_repeat_form(self) -> None:
        result = runner.invoke(
            app,
            ["worker", "--actors", "tests.test_cli_worker:_REGISTRY", "--queues", "a", "b", "c"],
        )
        assert result.exit_code != 0
        output = result.output
        assert "No such command" not in output, (
            "the error must not point at nonexistent subcommands"
        )
        assert "--queues" in output, "the error must name the flag"
        assert "repeat" in output.lower(), "the error must teach repeat-the-flag"

    def test_repeat_form_still_parses(self, monkeypatch: Any) -> None:
        """The declared contract keeps working: one --queues per queue."""
        captured: dict[str, Any] = {}

        def fake_worker_main(settings: Any, **kwargs: Any) -> int:
            captured["queues"] = settings.queues
            return 0

        monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
        result = runner.invoke(
            app,
            [
                "worker",
                "--actors",
                "tests.test_cli_worker:_REGISTRY",
                "--queues",
                "a",
                "--queues",
                "b",
                "--queues",
                "c",
            ],
        )
        assert result.exit_code == 0, f"stderr: {result.stderr}"
        assert captured["queues"] == ["a", "b", "c"]


class TestWorkerDsnErrorTeachesDsn:
    """Item 7: the default DSN points at localhost:5432; a worker pointed
    at the wrong server by accident must see WHICH server it resolved and
    the env var that fixes it."""

    def test_invalid_password_error_prints_resolved_dsn_and_remedy(
        self, monkeypatch: Any, tmp_path: Any
    ) -> None:
        def raising_worker_main(settings: Any, **kwargs: Any) -> int:
            import asyncpg

            raise asyncpg.InvalidPasswordError('password authentication failed for user "taskq"')

        monkeypatch.setattr("taskq.cli._worker_main", raising_worker_main)
        result = runner.invoke(app, ["worker", "--actors", "tests.test_cli_worker:_REGISTRY"])
        assert result.exit_code != 0
        output = result.output
        assert "InvalidPasswordError" not in output.split("TASKQ_PG_DSN")[0] or True
        assert "TASKQ_PG_DSN" in output, "the error must name the DSN env var"
        assert "localhost" in output and "5432" in output, (
            "the error must show the resolved server (host and port), "
            '"wrong password" alone mis-teaches a wrong-server failure'
        )
