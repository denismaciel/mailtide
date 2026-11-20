"""Drafts: the Runner draft API against the fake Gmail (SPEC §7a)."""

import asyncio
import dataclasses

import mailtide
import pytest
from conftest import make_config, tokens
from mailtide import Address, OutgoingMessage
from mailtide.testing import FakeGmail, ManualClock

ACCOUNT = "alice@example.com"


def draft_bytes(token: str, text: str = "Hello Ann\r\n") -> bytes:
    """A draft's bytes, carrying ``token`` as its ``X-Mailtide-Id``."""
    message = OutgoingMessage(
        Address(ACCOUNT),
        to=[Address("ann@example.org")],
        subject="Plan",
        tracking_id=token,
        text=text,
    )
    return mailtide.compose_message(message, purpose="draft").raw


def feed(runner: mailtide.BlockingRunner) -> list[mailtide.Change]:
    page = runner.changes(ACCOUNT, limit=5000)
    assert isinstance(page, mailtide.ChangePage) and not page.has_more
    return page.changes


def drafts_in(changes: list[mailtide.Change]) -> list[tuple]:
    """The draft entries of a feed, as ``(kind, draft id, message id, …)``."""
    entries: list[tuple] = []
    for change in changes:
        match change:
            case mailtide.DraftSaved():
                entries.append(("saved", change.draft_id, change.message_id, change.origin))
            case mailtide.DraftRemoved():
                entries.append(("removed", change.draft_id, change.last_message_id, change.reason))
            case mailtide.DraftSent():
                entries.append(("sent", change.draft_id, change.message_id, change.thread_id))
    return entries


def blocking(gmail: FakeGmail, clock: ManualClock | None = None) -> mailtide.BlockingRunner:
    """A synced runner on a manual clock: a run never waits for the draft
    listing it schedules."""
    runner = mailtide.BlockingRunner(
        make_config(gmail.origin), tokens, clock=clock or ManualClock()
    )
    runner.migrate()
    runner.track(ACCOUNT)
    assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
    return runner


def test_draft_lifecycle() -> None:
    token = mailtide.new_tracking_id()
    with FakeGmail() as gmail:

        async def main() -> None:
            r = mailtide.Runner(make_config(gmail.origin), tokens, clock=ManualClock())
            await r.migrate()
            await r.track(ACCOUNT)
            assert (await r.run(ACCOUNT, budget_s=30)).stop == "idle"

            saved = await r.create_draft(ACCOUNT, draft_bytes(token))
            assert isinstance(saved, mailtide.DraftRef)
            assert gmail.mailbox.drafts == {saved.draft_id: saved.message_id}
            [view] = await r.drafts(ACCOUNT)
            assert isinstance(view, mailtide.DraftView)
            with pytest.raises(AttributeError):
                object.__setattr__(view, "state", "stale")  # frozen
            assert (view.draft_id, view.message_id, view.thread_id) == (
                saved.draft_id,
                saved.message_id,
                saved.thread_id,
            )
            assert (view.tracking_id, view.state, view.retry_after_ms) == (token, "saved", None)

            updated = await r.update_draft(
                ACCOUNT,
                saved.draft_id,
                draft_bytes(token, "Hello again\r\n"),
                if_message_id=saved.message_id,
            )
            assert updated.draft_id == saved.draft_id
            assert updated.message_id != saved.message_id
            assert updated.thread_id == saved.thread_id, "an update keeps the thread"
            view = await r.draft(ACCOUNT, saved.draft_id)
            assert view is not None and view.message_id == updated.message_id

            content = await r.read_draft(ACCOUNT, saved.draft_id)
            assert isinstance(content, mailtide.DraftContent)
            assert (content.draft_id, content.message_id, content.thread_id) == (
                updated.draft_id,
                updated.message_id,
                updated.thread_id,
            )
            assert mailtide.tracking_id(content.raw) == token
            assert b"Hello again" in content.raw
            assert b"@mail.gmail.com>" in content.raw, "Gmail's own Message-ID"

            assert await r.delete_draft(ACCOUNT, saved.draft_id) == "deleted"
            assert gmail.mailbox.drafts == {}
            assert await r.delete_draft(ACCOUNT, saved.draft_id) == "already_gone"
            assert await r.drafts(ACCOUNT) == []
            assert await r.draft(ACCOUNT, saved.draft_id) is None
            with pytest.raises(mailtide.DraftNotFound):
                await r.read_draft(ACCOUNT, saved.draft_id)

        asyncio.run(main())


def test_send_draft_returns_the_sent_message() -> None:
    with FakeGmail() as gmail:
        gmail.add("m1", ["INBOX"], thread="t")
        runner = blocking(gmail)
        saved = runner.create_draft(ACCOUNT, draft_bytes(mailtide.new_tracking_id()), thread_id="t")
        assert saved.thread_id == "t"

        sent = runner.send_draft(ACCOUNT, saved.draft_id, if_message_id=saved.message_id)
        assert isinstance(sent, mailtide.SentDraft)
        assert sent.thread_id == "t"
        assert sent.message_id not in (saved.message_id, "m1")
        assert gmail.mailbox.messages[sent.message_id]["labels"] == ["SENT"]
        assert gmail.mailbox.drafts == {}
        assert runner.drafts(ACCOUNT) == []
        with pytest.raises(mailtide.DraftNotFound):
            runner.send_draft(ACCOUNT, saved.draft_id)

        assert drafts_in(feed(runner)) == [
            ("saved", saved.draft_id, saved.message_id, "local"),
            ("sent", saved.draft_id, sent.message_id, "t"),
        ]
        assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
        changes = feed(runner)
        kinds = [
            (c.kind, getattr(c, "id", None) or getattr(c, "message_id", None)) for c in changes
        ]
        draft_sent = kinds.index(("draft_sent", sent.message_id))
        assert ("message_imported", sent.message_id) in kinds[draft_sent:]
        [view] = runner.messages(ACCOUNT, [sent.message_id])
        assert view.thread_id == "t" and view.labels == ["SENT"]


def test_stale_update_raises_draft_conflict() -> None:
    token = mailtide.new_tracking_id()
    with FakeGmail() as gmail:
        runner = blocking(gmail)
        saved = runner.create_draft(ACCOUNT, draft_bytes(token))
        elsewhere = gmail.update_draft(
            saved.draft_id, draft_bytes(token, "Edited elsewhere\r\n"), thread=saved.thread_id
        )

        with pytest.raises(mailtide.DraftConflict) as raised:
            runner.update_draft(
                ACCOUNT,
                saved.draft_id,
                draft_bytes(token, "Mine\r\n"),
                if_message_id=saved.message_id,
            )
        assert isinstance(raised.value, mailtide.MailtideError)
        assert raised.value.current_message_id == elsewhere
        assert gmail.mailbox.drafts == {saved.draft_id: elsewhere}, "nothing was written"

        # A send checked against the same stale message is refused too, and
        # the index learns the current message from it.
        with pytest.raises(mailtide.DraftConflict) as raised:
            runner.send_draft(ACCOUNT, saved.draft_id, if_message_id=saved.message_id)
        assert raised.value.current_message_id == elsewhere
        assert gmail.mailbox.drafts == {saved.draft_id: elsewhere}, "nothing was sent"
        view = runner.draft(ACCOUNT, saved.draft_id)
        assert view is not None and (view.message_id, view.state) == (elsewhere, "saved")
        again = runner.update_draft(
            ACCOUNT, saved.draft_id, draft_bytes(token, "Mine\r\n"), if_message_id=elsewhere
        )
        assert gmail.mailbox.drafts == {saved.draft_id: again.message_id}


def test_draft_changed_elsewhere_reaches_the_feed() -> None:
    clock = ManualClock()
    token = mailtide.new_tracking_id()
    with FakeGmail() as gmail:
        runner = blocking(gmail, clock)
        saved = runner.create_draft(ACCOUNT, draft_bytes(token))
        elsewhere = gmail.update_draft(
            saved.draft_id, draft_bytes(token, "Edited elsewhere\r\n"), thread=saved.thread_id
        )

        assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
        clock.advance(11)  # past draft_refresh_delay_s: the drafts are listed
        assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
        assert drafts_in(feed(runner)) == [
            ("saved", saved.draft_id, saved.message_id, "local"),
            ("saved", saved.draft_id, elsewhere, "remote"),
        ]
        # Re-pointed at a message mailtide did not save: no token until Gmail's
        # copy is imported (after draft_settle_s), then the one it carries.
        view = runner.draft(ACCOUNT, saved.draft_id)
        assert view is not None
        assert (view.message_id, view.state, view.tracking_id) == (elsewhere, "saved", None)
        clock.advance(121)
        assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
        view = runner.draft(ACCOUNT, saved.draft_id)
        assert view is not None and view.tracking_id == token

        gmail.delete_draft(saved.draft_id)
        assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
        clock.advance(11)
        assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
        assert drafts_in(feed(runner))[-1] == ("removed", saved.draft_id, elsewhere, "gone")
        assert runner.drafts(ACCOUNT) == []


def test_blocking_runner_has_the_draft_methods() -> None:
    names = ("create_draft", "update_draft", "delete_draft", "send_draft")
    for name in (*names, "drafts", "draft", "read_draft"):
        assert callable(getattr(mailtide.Runner, name)), name
        assert callable(getattr(mailtide.BlockingRunner, name)), name

    token = mailtide.new_tracking_id()
    with FakeGmail() as gmail:
        runner = blocking(gmail)
        saved = runner.create_draft(ACCOUNT, draft_bytes(token), budget_s=10)
        updated = runner.update_draft(ACCOUNT, saved.draft_id, draft_bytes(token, "Two\r\n"))
        [view] = runner.drafts(ACCOUNT, after=None, limit=10)
        assert view.message_id == updated.message_id
        assert runner.drafts(ACCOUNT, after=saved.draft_id) == []
        assert runner.draft(ACCOUNT, "r-unknown") is None
        content = runner.read_draft(ACCOUNT, saved.draft_id, fresh=True)
        assert b"Two" in content.raw
        sent = runner.send_draft(ACCOUNT, saved.draft_id)
        assert gmail.mailbox.messages[sent.message_id]["labels"] == ["SENT"]

        other = runner.create_draft(ACCOUNT, draft_bytes(mailtide.new_tracking_id()))
        assert runner.delete_draft(ACCOUNT, other.draft_id) == "deleted"
        with pytest.raises(mailtide.DraftNotFound):
            runner.update_draft(ACCOUNT, other.draft_id, draft_bytes(token))


def test_draft_bytes_need_a_tracking_id() -> None:
    plain = b"From: alice@example.com\r\nSubject: Plan\r\n\r\nHello\r\n"
    token = mailtide.new_tracking_id()
    twice = draft_bytes(token).replace(
        b"\r\n\r\n", f"\r\nX-Mailtide-Id: {token}\r\n\r\n".encode(), 1
    )
    with FakeGmail() as gmail:
        config = dataclasses.replace(make_config(gmail.origin), max_message_bytes=4096)
        runner = mailtide.BlockingRunner(config, tokens, clock=ManualClock())
        runner.migrate()
        runner.track(ACCOUNT)
        assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
        for raw in (plain, twice, plain.replace(b"Subject", b"X-Mailtide-Id: short\r\nSubject")):
            with pytest.raises(ValueError, match="X-Mailtide-Id"):
                runner.create_draft(ACCOUNT, raw)
        with pytest.raises(mailtide.DraftTooLarge) as raised:
            runner.create_draft(ACCOUNT, draft_bytes(token, "x" * 5000))
        assert raised.value.limit == 4096
        assert gmail.mailbox.drafts == {}, "refused before any call"

        saved = runner.create_draft(ACCOUNT, draft_bytes(token))
        view = runner.draft(ACCOUNT, saved.draft_id)
        assert view is not None and view.tracking_id == token
        with pytest.raises(ValueError, match="X-Mailtide-Id"):
            runner.update_draft(ACCOUNT, saved.draft_id, draft_bytes(mailtide.new_tracking_id()))
