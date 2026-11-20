"""Raw outbox, draft routing and reply plans against the real bindings."""

import asyncio
import dataclasses

import mailtide
import pytest
from conftest import make_config, tokens
from mailtide.config import Blobs, Database
from mailtide.testing import FakeGmail, ManualClock

ACCOUNT = "alice@example.com"


def outgoing(token: str | None = None) -> bytes:
    return (
        f"From: {ACCOUNT}\r\nTo: Bob <bob@example.com>\r\nSubject: Reply\r\n"
        f"X-Mailtide-Id: {token or mailtide.new_tracking_id()}\r\n"
        "Message-ID: <client@example.com>\r\n\r\nPlain body\r\n"
    ).encode()


def blocking(gmail: FakeGmail, clock: ManualClock | None = None) -> mailtide.BlockingRunner:
    runner = mailtide.BlockingRunner(
        make_config(gmail.origin), tokens, clock=clock or ManualClock()
    )
    runner.migrate()
    runner.track(ACCOUNT)
    runner.run(ACCOUNT, budget_s=5)
    return runner


def test_send_reports_receipt_and_import_confirmation() -> None:
    with FakeGmail() as gmail:

        async def exercise() -> None:
            runner = mailtide.Runner(make_config(gmail.origin), tokens, clock=ManualClock())
            await runner.migrate()
            await runner.track(ACCOUNT)
            await runner.run(ACCOUNT, budget_s=5)
            sent = await runner.send(ACCOUNT, outgoing(), budget_s=5)
            assert isinstance(sent, mailtide.SendOutcome)
            assert sent.submission.state == "sent"
            assert sent.submission.message_id
            assert sent.submission.thread_id
            page = await runner.changes(ACCOUNT, limit=1000)
            assert isinstance(page, mailtide.ChangePage)
            entries = [
                entry for entry in page.changes if isinstance(entry, mailtide.SubmissionChanged)
            ]
            assert [entry.submission.state for entry in entries] == [
                "pending",
                "claimed",
                "sending",
                "confirming",
                "sent",
            ]
            assert entries[3].submission.message_id == sent.submission.message_id
            assert entries[4].submission.tracking_id == sent.submission.tracking_id
            assert gmail.mailbox.send_calls == 1

        asyncio.run(exercise())


def test_undo_send_cancels_before_delivery() -> None:
    with FakeGmail() as gmail:
        clock = ManualClock()
        runner = blocking(gmail, clock)
        raw = outgoing()
        row = runner.submit(ACCOUNT, raw, send_at_ms=clock() + 12_000)
        assert runner.cancel_submission(ACCOUNT, row.key).state == "canceled"
        clock.advance(60)
        runner.run(ACCOUNT, budget_s=5)
        assert gmail.mailbox.send_calls == 0
        assert runner.submission_content(ACCOUNT, row.key) == raw
        sent = runner.send(ACCOUNT, outgoing(), budget_s=5)
        assert isinstance(sent, mailtide.SendOutcome)
        with pytest.raises(mailtide.CannotUnsend) as error:
            runner.cancel_submission(ACCOUNT, sent.submission.key)
        assert error.value.state == "sent"


def test_scheduled_send_is_delivered_by_run(tmp_path) -> None:
    with FakeGmail() as gmail:
        clock = ManualClock()
        blob_dir = tmp_path / "blobs"
        blob_dir.mkdir()
        config = dataclasses.replace(
            make_config(gmail.origin),
            database=Database(f"sqlite:{tmp_path / 'outbox.db'}?mode=rwc"),
            blobs=Blobs(blob_dir.as_uri()),
        )
        runner = mailtide.BlockingRunner(config, tokens, clock=clock)
        runner.migrate()
        runner.track(ACCOUNT)
        runner.run(ACCOUNT, budget_s=5)
        due = clock() + 1_000
        row = runner.submit(ACCOUNT, outgoing(), send_at_ms=due)
        pause = runner.run(ACCOUNT, budget_s=5)
        assert pause.next_run_at_ms is not None and pause.next_run_at_ms <= due
        assert gmail.mailbox.send_calls == 0
        clock.advance(1)
        restarted = mailtide.BlockingRunner(config, tokens, clock=clock)
        restarted.run(ACCOUNT, budget_s=5)
        assert restarted.submission(ACCOUNT, row.key).state == "sent"
        assert gmail.mailbox.send_calls == 1


def test_lost_answer_is_confirmed_without_a_second_request() -> None:
    with FakeGmail() as gmail:
        runner = blocking(gmail)
        gmail.mailbox.drop_send_answer = True
        raw = outgoing()
        first = runner.send(ACCOUNT, raw, budget_s=5)
        second = runner.send(ACCOUNT, raw, budget_s=5)
        assert isinstance(first, mailtide.SendOutcome)
        assert isinstance(second, mailtide.SendOutcome)
        assert first.submission.state == second.submission.state == "sent"
        assert first.submission.message_id == second.submission.message_id
        assert gmail.mailbox.send_calls == 1


def test_raw_admission_respects_the_shared_size_limit() -> None:
    with FakeGmail() as gmail:
        raw = outgoing()
        config = dataclasses.replace(make_config(gmail.origin), max_message_bytes=len(raw))
        runner = mailtide.BlockingRunner(config, tokens, clock=ManualClock())
        runner.migrate()
        runner.track(ACCOUNT)
        with pytest.raises(mailtide.SendTooLarge) as error:
            runner.submit(ACCOUNT, raw + b"!")
        assert error.value.limit == len(raw)
        assert runner.submissions(ACCOUNT) == []
        sent = runner.send(ACCOUNT, raw, budget_s=5)
        assert isinstance(sent, mailtide.SendOutcome)
        assert sent.submission.state == "sent"
        assert gmail.mailbox.send_calls == 1


def test_send_draft_uses_existing_receipt_and_errors() -> None:
    with FakeGmail() as gmail:
        runner = blocking(gmail)
        draft = runner.create_draft(ACCOUNT, outgoing())
        with pytest.raises(mailtide.DraftConflict):
            runner.send(ACCOUNT, draft_id=draft.draft_id, if_message_id="old")
        sent = runner.send(ACCOUNT, draft_id=draft.draft_id, if_message_id=draft.message_id)
        assert isinstance(sent, mailtide.SentDraft)
        with pytest.raises(mailtide.DraftNotFound):
            runner.send(ACCOUNT, draft_id=draft.draft_id)
        assert runner.submissions(ACCOUNT) == []
        assert gmail.mailbox.send_calls == 0


def test_reply_plan_and_forward_plan() -> None:
    with FakeGmail() as gmail:
        raw = outgoing()
        gmail.add("original", ["INBOX"], raw=raw)
        runner = blocking(gmail)
        reply = mailtide.reply_plan(raw, [ACCOUNT])
        forward = mailtide.forward_plan(raw)
        assert reply.subject == "Re: Reply"
        assert reply.in_reply_to == "<client@example.com>"
        assert reply.to[0].email == "bob@example.com"
        assert mailtide.quote_plain(reply.quote_text) == "> Plain body\n"
        assert forward.subject == "Fwd: Reply"
        assert forward.original.from_[0].email == ACCOUNT
        assert (
            runner.reply_plan(ACCOUNT, "original", own_addresses=[ACCOUNT]).subject == reply.subject
        )
        assert runner.forward_plan(ACCOUNT, "original").references == forward.references
        with pytest.raises(AttributeError):
            reply.subject = "different"


def test_blocking_runner_has_the_send_methods() -> None:
    with FakeGmail() as gmail:
        runner = blocking(gmail)
        row = runner.submit(ACCOUNT, outgoing(), key="host-key")
        assert runner.submission(ACCOUNT, "host-key").tracking_id == row.tracking_id
        assert [value.key for value in runner.submissions(ACCOUNT, states=["pending"])] == [
            "host-key"
        ]
        assert runner.submissions(ACCOUNT, after="host-key") == []
        assert runner.submission_content(ACCOUNT, "host-key") is not None
        assert runner.cancel_submission(ACCOUNT, "host-key").state == "canceled"
