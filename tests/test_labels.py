"""Label definitions: what the label ids on messages are called."""

import asyncio

import mailtide
import pytest
from conftest import make_config, tokens
from fake_gmail import Mailbox

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
