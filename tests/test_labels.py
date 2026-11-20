"""Label definitions: what the label ids on messages are called."""

import asyncio
from typing import Literal

import mailtide
import pytest
from conftest import make_config, tokens
from fake_gmail import Mailbox
from mailtide.config import Blobs, Config, Database, Engine, Http
from mailtide.testing import FakeGmail, ManualClock

ACCOUNT = "alice@example.com"


def described(labels: list[mailtide.Label]) -> list[tuple[str, str, str]]:
    return [(label.id, label.name, label.kind) for label in labels]


def test_labels_name_the_ids_on_messages_and_follow_renames(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.label("SENT", "SENT", kind="system")
    mailbox.label("Label_12", "Receipts")
    mailbox.add("m1", ["INBOX", "Label_12"])

    async def main() -> None:
        r = mailtide.Runner(make_config(origin), tokens)
        await r.migrate()
        assert await r.labels(ACCOUNT) == []  # untracked, as messages() answers
        await r.track(ACCOUNT)
        assert await r.labels(ACCOUNT) == []  # nothing synced yet
        await r.run(ACCOUNT, budget_s=30)
        labels = await r.labels(ACCOUNT)
        assert described(labels) == [
            ("INBOX", "INBOX", "system"),
            ("Label_12", "Receipts", "user"),
            ("SENT", "SENT", "system"),
        ]
        names = {label.id: label.name for label in labels}
        [view] = await r.messages(ACCOUNT, ["m1"])
        assert [names[label] for label in view.labels] == ["INBOX", "Receipts"]
        page = await r.changes(ACCOUNT)
        assert isinstance(page, mailtide.ChangePage)

        # A rename leaves no history record; the next label refresh (here the
        # reconciliation after Gmail dropped the history) carries it.
        mailbox.label("Label_12", "Invoices")
        mailbox.expire_history()
        await r.run(ACCOUNT, budget_s=30)
        later = await r.changes(ACCOUNT, since=page.next)
        assert isinstance(later, mailtide.ChangePage)
        assert any(isinstance(c, mailtide.LabelDefinitionsChanged) for c in later.changes)
        assert ("Label_12", "Invoices", "user") in described(await r.labels(ACCOUNT))

    asyncio.run(main())


def definition_changes(page: mailtide.ChangePage | mailtide.StateTooOld) -> int:
    assert isinstance(page, mailtide.ChangePage)
    return sum(isinstance(c, mailtide.LabelDefinitionsChanged) for c in page.changes)


@pytest.mark.parametrize("access", ["read_write", "read_only"])
def test_a_live_account_refreshes_renamed_labels_after_the_interval(
    access: Literal["read_write", "read_only"],
) -> None:
    """No reconciliation needed: once the definitions are older than
    ``label_refresh_s``, the next run lists them again; an unchanged list
    adds nothing to the feed. Read-only runners refresh too (it is a read)."""
    clock = ManualClock()
    with FakeGmail() as gmail:
        gmail.mailbox.label("Label_12", "Receipts")
        gmail.add("m1", ["INBOX", "Label_12"])
        config = Config(
            Database("sqlite::memory:"),
            Blobs("memory:///"),
            access=access,
            http=Http(gmail_origin=gmail.origin),
            engine=Engine(label_refresh_s=600),
        )
        runner = mailtide.BlockingRunner(config, tokens, clock=clock)
        runner.migrate()
        runner.track(ACCOUNT)
        runner.run(ACCOUNT, budget_s=30)
        assert ("Label_12", "Receipts", "user") in described(runner.labels(ACCOUNT))
        page = runner.changes(ACCOUNT)
        assert isinstance(page, mailtide.ChangePage)

        gmail.mailbox.label("Label_12", "Invoices")
        clock.advance(300)
        runner.run(ACCOUNT, budget_s=30)
        assert ("Label_12", "Receipts", "user") in described(runner.labels(ACCOUNT)), "not due yet"

        clock.advance(301)
        runner.run(ACCOUNT, budget_s=30)
        assert ("Label_12", "Invoices", "user") in described(runner.labels(ACCOUNT))
        later = runner.changes(ACCOUNT, since=page.next)
        assert definition_changes(later) == 1
        assert gmail.mailbox.history_floor == 0, "history never expired"
        status = runner.status(ACCOUNT)
        assert status is not None and status.phase == "live"

        clock.advance(601)
        runner.run(ACCOUNT, budget_s=30)
        assert isinstance(later, mailtide.ChangePage)
        assert definition_changes(runner.changes(ACCOUNT, since=later.next)) == 0


def test_blocking_labels(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.label("Label_1", "Travel")
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    runner.track(ACCOUNT)
    runner.run(ACCOUNT, budget_s=30)
    assert described(runner.labels(ACCOUNT)) == [
        ("INBOX", "INBOX", "system"),
        ("Label_1", "Travel", "user"),
    ]
    with pytest.raises(ValueError, match="invalid account"):
        runner.labels("not an account")
