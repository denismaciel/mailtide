"""Permanent deletion: purge and empty, against the fake Gmail."""

import asyncio
import dataclasses

import mailtide
import pytest
from conftest import make_config, tokens
from mailtide.testing import FakeGmail

ACCOUNT = "alice@example.com"


def runner_for(gmail: FakeGmail, **changes: object) -> mailtide.BlockingRunner:
    config = dataclasses.replace(make_config(gmail.origin), **changes)
    runner = mailtide.BlockingRunner(config, tokens)
    runner.migrate()
    runner.track(ACCOUNT)
    return runner


def deleted_in_feed(runner: mailtide.BlockingRunner) -> list[str]:
    page = runner.changes(ACCOUNT, None, 500)
    assert isinstance(page, mailtide.ChangePage)
    return [c.id for c in page.changes if isinstance(c, mailtide.MessageDeleted)]


def test_purge_needs_the_config_switch() -> None:
    with FakeGmail() as gmail:
        gmail.add("t", ["TRASH"])
        runner = runner_for(gmail)
        assert make_config().purge == "off"
        runner.run(ACCOUNT, budget_s=30)
        with pytest.raises(mailtide.PurgeDisabled):
            runner.purge(ACCOUNT, message_ids=["t"])
        with pytest.raises(mailtide.PurgeDisabled):
            runner.empty(ACCOUNT, "trash")
        assert gmail.mailbox.deletes == []
        assert "t" in gmail.mailbox.messages

        runner = runner_for(gmail, purge="trash_and_spam")
        runner.run(ACCOUNT, budget_s=30)
        with pytest.raises(mailtide.PurgeDisabled, match="anywhere"):
            runner.purge(ACCOUNT, message_ids=["t"], anywhere=True)
        assert runner.purge(ACCOUNT, message_ids=["t"]).purged == ["t"]
        assert "t" not in gmail.mailbox.messages
        with pytest.raises(ValueError, match="purge"):
            runner_for(gmail, purge="always")


def test_purge_reports_purged_and_refused_ids() -> None:
    with FakeGmail() as gmail:
        gmail.add("t", ["TRASH"])
        gmail.add("s", ["SPAM"])
        gmail.add("i", ["INBOX"])
        gmail.add("r", ["TRASH"])
        runner = runner_for(gmail, purge="trash_and_spam")
        runner.run(ACCOUNT, budget_s=30)
        # Restored on another client after the last sync: refused.
        with gmail.mailbox.lock:
            gmail.mailbox.restore("r")
        outcome = runner.purge(ACCOUNT, message_ids=["t", "s", "i", "r", "nope"])
        assert outcome.purged == ["t", "s"]
        assert outcome.refused == {
            "i": "not_in_trash_or_spam",
            "r": "not_in_trash_or_spam",
            "nope": "not_in_mirror",
        }
        assert outcome.sent == [] and outcome.unsent == []
        assert gmail.mailbox.deletes == [["t"], ["s"]]
        assert deleted_in_feed(runner) == ["t", "s"]
        assert [v.id for v in runner.messages(ACCOUNT, ["t", "s", "i"])] == ["i"]
        with pytest.raises(mailtide.TooManyMessages):
            runner.purge(ACCOUNT, message_ids=[f"m{n}" for n in range(1001)])
        with pytest.raises(ValueError, match="exactly one"):
            runner.purge(ACCOUNT)


def test_empty_trash_and_spam() -> None:
    with FakeGmail() as gmail:
        for n in range(6):
            gmail.add(f"t{n}", ["TRASH"])
        gmail.add("s", ["SPAM"])
        gmail.add("i", ["INBOX"])
        runner = runner_for(gmail, purge="trash_and_spam")
        runner.run(ACCOUNT, budget_s=30)
        emptied = runner.empty(ACCOUNT, "trash")
        assert (emptied.purged, emptied.sent, emptied.skipped, emptied.remaining) == (6, 0, 0, 0)
        assert gmail.mailbox.deletes == [[f"t{n}" for n in range(6)]], "one batchDelete"
        assert runner.empty(ACCOUNT, "spam").purged == 1
        assert list(gmail.mailbox.messages) == ["i"]
        with pytest.raises(ValueError, match="bin"):
            runner.empty(ACCOUNT, "inbox")


def test_a_modify_only_token_raises_purge_scope_required() -> None:
    with FakeGmail() as gmail:
        gmail.add("t", ["TRASH"])
        runner = runner_for(gmail, purge="trash_and_spam")
        runner.run(ACCOUNT, budget_s=30)
        gmail.mailbox.full_scope = False
        with pytest.raises(mailtide.PurgeScopeRequired, match="https://mail.google.com/"):
            runner.purge(ACCOUNT, message_ids=["t"])
        assert gmail.mailbox.messages["t"]["labels"] == ["TRASH"], "not deleted, not moved"
        gmail.add("n", ["INBOX"])
        outcome = runner.run(ACCOUNT, budget_s=30)
        assert outcome.stop == "idle", "the account keeps syncing"
        assert outcome.imported == 1


def test_blocking_runner_has_purge_and_empty() -> None:
    with FakeGmail() as gmail:
        gmail.add("a", ["TRASH"])
        gmail.add("b", ["TRASH"], thread="t-a")
        gmail.add("c", ["SPAM"])
        config = dataclasses.replace(make_config(gmail.origin), purge="trash_and_spam")

        async def main() -> tuple[mailtide.PurgeOutcome, mailtide.EmptyOutcome]:
            runner = mailtide.Runner(config, tokens)
            await runner.migrate()
            await runner.track(ACCOUNT)
            await runner.run(ACCOUNT, budget_s=30)
            purged = await runner.purge(ACCOUNT, thread_id="t-a")
            emptied = await runner.empty(ACCOUNT, "spam", budget_s=30)
            return purged, emptied

        purged, emptied = asyncio.run(main())
        assert sorted(purged.purged) == ["a", "b"]
        assert emptied.purged == 1
        assert gmail.mailbox.messages == {}
        # The blocking runner has the same methods, with the same signatures.
        for name in ("purge", "empty"):
            assert hasattr(mailtide.BlockingRunner, name)
            assert hasattr(mailtide.Runner, name)
