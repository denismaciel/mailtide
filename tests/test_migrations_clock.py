"""Migrations a host's tool can run, the schema check, and the test kit."""

import sqlite3
from collections.abc import Callable
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
    assert mailtide.SCHEMA_VERSION == 8  # 8: message facts (version only)
    assert [m.version for m in mailtide.migrations()] == list(range(1, mailtide.SCHEMA_VERSION + 1))
    config = Config(Database(f"sqlite:{path}"), Blobs("memory:///"))
    runner = mailtide.BlockingRunner(config, tokens)
    status = runner.schema()
    assert (status.state, status.applied) == ("current", mailtide.SCHEMA_VERSION)
    runner.migrate()  # a no-op now
    runner.track("alice@example.com")
    assert runner.schema().state == "current"


def test_an_unmigrated_database_refuses_to_sync() -> None:
    runner = mailtide.BlockingRunner(make_config(), tokens)
    assert runner.schema().state == "behind"
    with pytest.raises(mailtide.SchemaOutOfDate, match="migrate"):
        runner.run(ACCOUNT)
    runner.migrate()
    runner.track("alice@example.com")
    assert runner.schema().state == "current"


def every_database_call(
    runner: mailtide.BlockingRunner, state: str
) -> dict[str, Callable[[], object]]:
    """Every runner method that reads or writes the database."""
    return {
        "track": lambda: runner.track(ACCOUNT),
        "run": lambda: runner.run(ACCOUNT),
        "act": lambda: runner.act(ACCOUNT, "archive", message_ids=["m1"]),
        "admit": lambda: runner.admit(ACCOUNT, "archive", message_ids=["m1"]),
        "prioritise": lambda: runner.prioritise(ACCOUNT, ["m1"]),
        "drift": lambda: runner.drift(ACCOUNT, ["m1"]),
        "search": lambda: runner.search(ACCOUNT, "x"),
        "search_threads": lambda: runner.search_threads(ACCOUNT, "x"),
        "notify": lambda: runner.notify(ACCOUNT, 7),
        "status": lambda: runner.status(ACCOUNT),
        "accounts": runner.accounts,
        "resume": lambda: runner.resume(ACCOUNT),
        "forget": lambda: runner.forget(ACCOUNT),
        "labels": lambda: runner.labels(ACCOUNT),
        "messages": lambda: runner.messages(ACCOUNT, ["m1"]),
        "threads": lambda: runner.threads(ACCOUNT, ["t-m1"]),
        "changes": lambda: runner.changes(ACCOUNT),
        "compact": lambda: runner.compact(ACCOUNT, state),
        "ack": lambda: runner.ack(ACCOUNT, "indexer", state),
        "consumers": lambda: runner.consumers(ACCOUNT),
        "drop_consumer": lambda: runner.drop_consumer(ACCOUNT, "indexer"),
        "message_ids": lambda: runner.message_ids(ACCOUNT),
        "refresh_facts": lambda: runner.refresh_facts(ACCOUNT),
    }


def a_state_string() -> str:
    runner = mailtide.BlockingRunner(make_config(), tokens)
    runner.migrate()
    runner.track(ACCOUNT)
    page = runner.changes(ACCOUNT)
    assert isinstance(page, mailtide.ChangePage)
    return page.next


def test_reads_refuse_an_unmigrated_database_as_writes_do() -> None:
    runner = mailtide.BlockingRunner(make_config(), tokens)
    for name, call in every_database_call(runner, a_state_string()).items():
        with pytest.raises(mailtide.SchemaOutOfDate, match="migrate"):
            call()
            pytest.fail(name)
    assert runner.schema().state == "behind"


def test_reads_refuse_a_database_a_newer_build_migrated(tmp_path: Path) -> None:
    """A build that is too old refuses instead of misreading rows it does not
    know; only the schema status, health and blob reads still answer."""
    path = tmp_path / "mail.db"
    config = Config(Database(f"sqlite:{path}?mode=rwc"), Blobs("memory:///"))
    newer = mailtide.BlockingRunner(config, tokens)
    newer.migrate()
    newer.track(ACCOUNT)
    with sqlite3.connect(path) as db:
        db.execute(
            "INSERT INTO mailtide_schema (version) VALUES (?)", (mailtide.SCHEMA_VERSION + 1,)
        )
    older = mailtide.BlockingRunner(config, tokens)
    for name, call in every_database_call(older, a_state_string()).items():
        with pytest.raises(mailtide.SchemaOutOfDate, match="upgrade"):
            call()
            pytest.fail(name)
    assert older.schema().state == "ahead"
    assert older.health().database == "ok"
    with pytest.raises(mailtide.BlobNotFound):
        older.read_blob(ACCOUNT, "m1")


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
        runner.track("alice@example.com")
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
        runner.track("alice@example.com")
        runner.run(ACCOUNT, budget_s=30)
        delivered = runner.act(ACCOUNT, "archive", message_ids=["m1"])
        assert delivered.stop == "idle"
        start = clock()
        assert delivered.next_run_at_ms is not None
        assert delivered.next_run_at_ms <= start + 5_000
        again = runner.run(ACCOUNT, budget_s=30)  # a new engine, same store
        assert again.next_run_at_ms is not None
        assert again.next_run_at_ms <= start + 5_000, again.next_run_at_ms - start
