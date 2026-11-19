"""Read-only (shadow) mode, drift checks and host-verified pushes."""

import asyncio
import dataclasses
from pathlib import Path

import mailtide
import pytest
from conftest import make_config, tokens
from mailtide.config import Database
from mailtide.testing import FakeGmail

ACCOUNT = "alice@example.com"


def runner_for(gmail: FakeGmail, **changes: object) -> mailtide.BlockingRunner:
    config = dataclasses.replace(make_config(gmail.origin), **changes)
    runner = mailtide.BlockingRunner(config, tokens)
    runner.migrate()
    return runner


def test_read_only_mirrors_and_refuses_actions() -> None:
    with FakeGmail() as gmail:
        gmail.add("m1", ["INBOX", "UNREAD"])
        runner = runner_for(gmail, access="read_only")
        assert runner.run(ACCOUNT, budget_s=30).imported == 1
        with pytest.raises(mailtide.ReadOnlyMode):
            runner.act(ACCOUNT, "archive", message_ids=["m1"])
        with pytest.raises(mailtide.ReadOnlyMode):
            runner.admit(ACCOUNT, "archive", message_ids=["m1"])
        assert gmail.mailbox.messages["m1"]["labels"] == ["INBOX", "UNREAD"]


def test_an_invalid_access_names_the_field() -> None:
    with pytest.raises(ValueError, match="access"):
        mailtide.BlockingRunner(dataclasses.replace(make_config(), access="sometimes"), tokens)


def test_drift_finds_only_unexplained_differences() -> None:
    with FakeGmail() as gmail:
        for message in ["same", "changed", "gone"]:
            gmail.add(message, ["INBOX"])
        runner = runner_for(gmail)
        runner.run(ACCOUNT, budget_s=30)
        with gmail.mailbox.lock:
            # Changed behind the mirror's back without a history record, as a
            # lost history entry would; and deleted outright.
            gmail.mailbox.messages["changed"]["labels"] = ["STARRED"]
            del gmail.mailbox.messages["gone"]
        results = {r.message_id: r for r in runner.drift(ACCOUNT, ["same", "changed", "gone"])}
        assert results["same"].verdict == "matches"
        assert results["gone"].verdict == "missing_in_gmail"
        changed = results["changed"]
        assert changed.verdict == "differs"
        assert (changed.mirror_labels, changed.gmail_labels) == (["INBOX"], ["STARRED"])


def test_the_async_runner_samples_mirrored_messages() -> None:
    with FakeGmail() as gmail:
        for message in ["a", "b", "c"]:
            gmail.add(message, ["INBOX"])

        async def main() -> list[mailtide.DriftResult]:
            runner = mailtide.Runner(make_config(gmail.origin), tokens)
            await runner.migrate()
            await runner.run(ACCOUNT, budget_s=30)
            return await runner.drift(ACCOUNT, sample=2)

        sampled = asyncio.run(main())
        assert len(sampled) == 2
        assert {r.verdict for r in sampled} == {"matches"}


def test_notify_syncs_only_when_behind() -> None:
    with FakeGmail() as gmail:
        gmail.add("m1", ["INBOX"])
        runner = runner_for(gmail)
        assert runner.notify(ACCOUNT, 1).outcome == "unknown_account"
        runner.run(ACCOUNT, budget_s=30)
        cursor = runner.status(ACCOUNT).cursor
        assert cursor is not None
        assert runner.notify(ACCOUNT, cursor).outcome == "current"
        gmail.add("m2", ["INBOX"])
        notified = runner.notify(ACCOUNT, cursor + 1)
        assert notified.outcome == "ran"
        assert notified.run is not None and notified.run.imported == 1


def test_actions_admitted_before_read_only_are_blocked_not_sent(tmp_path: Path) -> None:
    with FakeGmail() as gmail:
        gmail.add("m1", ["INBOX", "UNREAD"])
        database = Database(f"sqlite:{tmp_path / 'mail.db'}?mode=rwc")
        writer = runner_for(gmail, database=database)
        writer.run(ACCOUNT, budget_s=30)
        assert writer.admit(ACCOUNT, "archive", message_ids=["m1"]) == 1
        shadow = runner_for(gmail, database=database, access="read_only")
        shadow.run(ACCOUNT, budget_s=30)
        [view] = shadow.messages(ACCOUNT, ["m1"])
        assert (view.pending_actions, view.blocked_actions) == (0, 1)
        assert view.labels == ["INBOX", "UNREAD"], "a blocked action stops overlaying"
        assert gmail.mailbox.messages["m1"]["labels"] == ["INBOX", "UNREAD"]
