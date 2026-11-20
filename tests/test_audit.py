"""The periodic mailbox audit through the Python runner."""

import mailtide
from conftest import tokens
from mailtide.config import Blobs, Config, Database, Engine, Http
from mailtide.testing import FakeGmail, ManualClock

ACCOUNT = "alice@example.com"


def test_the_audit_repairs_messages_history_missed() -> None:
    clock = ManualClock()
    with FakeGmail() as gmail:
        gmail.add("kept", ["INBOX"])
        gmail.add("vanished", ["INBOX"])
        runner = mailtide.BlockingRunner(
            Config(
                Database("sqlite::memory:"),
                Blobs("memory:///"),
                http=Http(gmail_origin=gmail.origin),
                engine=Engine(audit_interval_s=30),
            ),
            tokens,
            clock=clock,
        )
        runner.migrate()
        runner.track(ACCOUNT)
        # One run lists, the next sweeps, and the last clears the temporary ids.
        for _ in range(3):
            assert runner.run(ACCOUNT).stop == "idle"
        before = runner.changes(ACCOUNT)
        assert isinstance(before, mailtide.ChangePage)

        gmail.add("missed", ["INBOX"])
        gmail.mailbox.history.pop()
        del gmail.mailbox.messages["vanished"]
        clock.advance(29)
        early = runner.run(ACCOUNT)
        assert early.status is not None
        assert early.status.audit_found_missing == 0
        assert runner.messages(ACCOUNT, ["missed"]) == []

        clock.advance(1)
        listing = runner.run(ACCOUNT)
        assert listing.status is not None
        assert listing.next_run_at_ms == clock()
        assert listing.status.audit_found_missing == 1
        assert listing.status.audit_found_unlisted == 0
        assert runner.messages(ACCOUNT, ["missed"])[0].content is not None

        sweep = runner.run(ACCOUNT)
        assert sweep.status is not None
        assert sweep.status.audit_found_missing == 0
        assert sweep.status.audit_found_unlisted == 1
        assert runner.messages(ACCOUNT, ["vanished"]) == []
        assert sweep.cursor_after == listing.cursor_after
