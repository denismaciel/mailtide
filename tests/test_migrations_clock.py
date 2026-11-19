"""Migrations a host's tool can run, the schema check, and the test kit."""

import sqlite3
from pathlib import Path

import mailtide
import pytest
from conftest import make_config, tokens
from mailtide.config import Blobs, Config, Database, Engine, Http
from mailtide.testing import FakeGmail, ManualClock

ACCOUNT = "alice@example.com"


def test_host_applied_migrations_are_current(tmp_path: Path) -> None:
    path = tmp_path / "mail.db"
    # What an Alembic revision would do: execute each statement in order.
    with sqlite3.connect(path) as db:
        for migration in mailtide.migrations():
            for statement in migration.statements:
                db.execute(statement)
    assert [m.version for m in mailtide.migrations()] == list(range(1, mailtide.SCHEMA_VERSION + 1))
    config = Config(Database(f"sqlite:{path}"), Blobs("memory:///"))
    runner = mailtide.BlockingRunner(config, tokens)
    status = runner.schema()
    assert (status.state, status.applied) == ("current", mailtide.SCHEMA_VERSION)
    runner.migrate()  # a no-op now
    assert runner.schema().state == "current"


def test_an_unmigrated_database_refuses_to_sync() -> None:
    runner = mailtide.BlockingRunner(make_config(), tokens)
    assert runner.schema().state == "behind"
    with pytest.raises(mailtide.SchemaOutOfDate, match="migrate"):
        runner.run(ACCOUNT)
    runner.migrate()
    assert runner.schema().state == "current"


def test_a_manual_clock_reaches_confirmation_without_sleeping() -> None:
    clock = ManualClock()
    with FakeGmail() as gmail:
        gmail.add("m1", ["INBOX", "UNREAD"])
        config = Config(
            Database("sqlite::memory:"),
            Blobs("memory:///"),
            http=Http(gmail_origin=gmail.origin),
        )  # default confirm_timeout_s: 60
        runner = mailtide.BlockingRunner(config, tokens, clock=clock)
        runner.migrate()
        runner.run(ACCOUNT, budget_s=30)
        runner.act(ACCOUNT, "archive", message_ids=["m1"])
        assert gmail.mailbox.messages["m1"]["labels"] == ["UNREAD"]
        [view] = runner.messages(ACCOUNT, ["m1"])
        assert view.pending_actions == 1, "delivered, awaiting its confirmation fetch"
        runner.run(ACCOUNT, budget_s=30)
        assert runner.messages(ACCOUNT, ["m1"])[0].pending_actions == 1, "not due yet"
        clock.advance(61)
        runner.run(ACCOUNT, budget_s=30)
        [view] = runner.messages(ACCOUNT, ["m1"])
        assert view.pending_actions == 0 and view.labels == ["UNREAD"]


def test_a_fresh_runner_wakes_for_a_pending_confirmation() -> None:
    """A delivered action's confirmation is queued in the store; a new runner
    (no timers of its own) must still ask to be woken for it, not at the
    next poll."""
    clock = ManualClock()
    with FakeGmail() as gmail:
        gmail.add("m1", ["INBOX"])
        config = Config(
            Database("sqlite::memory:"),
            Blobs("memory:///"),
            http=Http(gmail_origin=gmail.origin),
            engine=Engine(confirm_timeout_s=5, poll_active_s=600, poll_idle_s=600),
        )
        runner = mailtide.BlockingRunner(config, tokens, clock=clock)
        runner.migrate()
        runner.run(ACCOUNT, budget_s=30)
        delivered = runner.act(ACCOUNT, "archive", message_ids=["m1"])
        assert delivered.stop == "idle"
        start = clock()
        assert delivered.next_run_at_ms is not None
        assert delivered.next_run_at_ms <= start + 5_000
        again = runner.run(ACCOUNT, budget_s=30)  # a new engine, same store
        assert again.next_run_at_ms is not None
        assert again.next_run_at_ms <= start + 5_000, again.next_run_at_ms - start
