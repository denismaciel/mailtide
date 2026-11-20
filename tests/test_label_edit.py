"""Label editing: create, rename, restyle and delete labels in Gmail through
the runner; Gmail's answer is in the mirror at once."""

import asyncio

import mailtide
import pytest
from conftest import make_config, tokens
from fake_gmail import Mailbox
from mailtide.config import Blobs, Config, Database, Engine, Http
from mailtide.testing import FakeGmail, ManualClock

ACCOUNT = "alice@example.com"


def names(labels: list[mailtide.Label]) -> dict[str, str]:
    return {label.id: label.name for label in labels if label.kind == "user"}


def test_label_lifecycle(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"])

    async def main() -> None:
        r = mailtide.Runner(make_config(origin), tokens)
        await r.migrate()
        await r.track(ACCOUNT)
        await r.run(ACCOUNT, budget_s=30)
        page = await r.changes(ACCOUNT)
        assert isinstance(page, mailtide.ChangePage)

        green = mailtide.LabelColor("#FFFFFF", "#16a766")
        work = await r.create_label(ACCOUNT, "Work", color=green, list_visibility="show_if_unread")
        assert (work.name, work.kind, work.parent_id) == ("Work", "user", None)
        assert work.color == mailtide.LabelColor("#ffffff", "#16a766"), "sent lower-cased"
        assert work.list_visibility == "show_if_unread"
        child = await r.create_label(ACCOUNT, "Work/2026")
        assert child.parent_id == work.id
        assert names(await r.labels(ACCOUNT)) == {work.id: "Work", child.id: "Work/2026"}

        await r.act(ACCOUNT, "change_labels", message_ids=["m1"], add_labels=[child.id])
        assert mailbox.messages["m1"]["labels"] == ["INBOX", child.id]

        update = await r.update_label(ACCOUNT, work.id, name="Job")
        assert isinstance(update, mailtide.LabelUpdate)
        assert update.label.name == "Job"
        assert update.label.color == work.color, "a rename keeps the style"
        assert [label.name for label in update.sublabels] == ["Job/2026"]
        restyled = await r.update_label(ACCOUNT, work.id, color=None, message_visibility="hide")
        assert (restyled.label.name, restyled.label.color) == ("Job", None)
        assert restyled.label.message_visibility == "hide"

        with pytest.raises(mailtide.LabelHasSublabels) as refused:
            await r.delete_label(ACCOUNT, work.id)
        assert refused.value.ids == [child.id]
        deletion = await r.delete_label(ACCOUNT, work.id, sublabels="delete")
        assert isinstance(deletion, mailtide.LabelDeletion)
        assert (deletion.deleted, deletion.already_gone) == ([child.id, work.id], [])
        assert names(await r.labels(ACCOUNT)) == {}
        [view] = await r.messages(ACCOUNT, ["m1"])
        assert view.labels == ["INBOX"], "a deleted label leaves the message at once"
        again = await r.delete_label(ACCOUNT, work.id)
        assert (again.deleted, again.already_gone) == ([], [work.id])

        later = await r.changes(ACCOUNT, since=page.next)
        assert isinstance(later, mailtide.ChangePage)
        assert any(isinstance(c, mailtide.LabelDefinitionsChanged) for c in later.changes)

        with pytest.raises(mailtide.InvalidLabel, match="blank"):
            await r.create_label(ACCOUNT, "  ")
        with pytest.raises(mailtide.InvalidLabel, match="palette"):
            await r.create_label(ACCOUNT, "X", color=mailtide.LabelColor("#123456", "#ffffff"))
        with pytest.raises(mailtide.InvalidLabel, match="system label"):
            await r.update_label(ACCOUNT, "INBOX", name="Mine")
        with pytest.raises(mailtide.LabelNotFound):
            await r.update_label(ACCOUNT, "Label_99", list_visibility="hide")

    asyncio.run(main())


def test_a_taken_name_raises_label_name_taken(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    lid = mailbox.create_label("Receipts")
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    runner.track(ACCOUNT)
    runner.run(ACCOUNT, budget_s=30)
    with pytest.raises(mailtide.LabelNameTaken) as taken:
        runner.create_label(ACCOUNT, "receipts")
    existing = taken.value.existing
    assert existing is not None
    assert (existing.id, existing.name) == (lid, "Receipts")


def test_a_label_deleted_elsewhere_reaches_the_feed() -> None:
    clock = ManualClock()
    with FakeGmail() as gmail:
        lid = gmail.mailbox.create_label("Old")
        gmail.add("m1", ["INBOX", lid])
        gmail.add("m2", [lid])
        config = Config(
            Database("sqlite::memory:"),
            Blobs("memory:///"),
            http=Http(gmail_origin=gmail.origin),
            engine=Engine(label_refresh_s=600),
        )
        runner = mailtide.BlockingRunner(config, tokens, clock=clock)
        runner.migrate()
        runner.track(ACCOUNT)
        runner.run(ACCOUNT, budget_s=30)
        page = runner.changes(ACCOUNT)
        assert isinstance(page, mailtide.ChangePage)

        with gmail.mailbox.lock:
            gmail.mailbox.delete_label(lid, history=False)
        clock.advance(601)
        runner.run(ACCOUNT, budget_s=30)
        later = runner.changes(ACCOUNT, since=page.next)
        assert isinstance(later, mailtide.ChangePage)
        assert any(isinstance(c, mailtide.LabelDefinitionsChanged) for c in later.changes)
        removed = {c.id: c.removed for c in later.changes if isinstance(c, mailtide.LabelsChanged)}
        assert removed == {"m1": [lid], "m2": [lid]}
        assert [view.labels for view in runner.messages(ACCOUNT, ["m1", "m2"])] == [["INBOX"], []]


def test_blocking_runner_has_the_label_methods(gmail: tuple[Mailbox, str]) -> None:
    _, origin = gmail
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    runner.track(ACCOUNT)
    runner.run(ACCOUNT, budget_s=30)
    label = runner.create_label(ACCOUNT, "Inbox zero", message_visibility="hide")
    assert isinstance(label, mailtide.Label)
    update = runner.update_label(ACCOUNT, label.id, name="Done", sublabels="leave")
    assert update.label.name == "Done"
    deletion = runner.delete_label(ACCOUNT, label.id)
    assert deletion.deleted == [label.id]
    read_only = mailtide.BlockingRunner(
        Config(Database("sqlite::memory:"), Blobs("memory:///"), access="read_only"), tokens
    )
    read_only.migrate()
    read_only.track(ACCOUNT)
    with pytest.raises(mailtide.ReadOnlyMode):
        read_only.create_label(ACCOUNT, "New")
