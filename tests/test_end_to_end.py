"""BlockingRunner (reqwest, SQLite, blob store) against a fake Gmail over HTTP."""

import json
import sys
import sysconfig
from concurrent.futures import ThreadPoolExecutor

import mailtide
import pytest
from fake_gmail import Mailbox

ACCOUNT = "alice@example.com"
PREFIX = "/gmail/v1/users/me/"


def test_bootstrap_import_and_follow(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX", "UNREAD"])
    mailbox.add("m2", [])
    runner = mailtide.BlockingRunner(
        "sqlite::memory:",
        "memory:///",
        lambda account: ("test-token", 2**62),
        gmail_origin=origin,
    )
    runner.migrate()

    outcome = runner.run(ACCOUNT, budget_s=30)
    assert outcome.stop == "idle", outcome.blocked
    assert outcome.status is not None and outcome.status.phase == "live"
    assert outcome.imported == 2
    raw = runner.read_blob(ACCOUNT, "m2")
    assert mailtide.parse_message(raw).text.strip() == "Body of m2"

    page = runner.changes(ACCOUNT)
    assert page.changes
    mailbox.add("m3", ["INBOX"])
    outcome = runner.run(ACCOUNT, budget_s=30)
    assert outcome.stop == "idle"
    assert outcome.cursor_after == 3
    assert outcome.imported == 1
    later = runner.changes(ACCOUNT, since=page.next)
    assert any("m3" in json.dumps(change) for change in later.changes)


def test_actions_overlay_deliver_and_settle(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX", "UNREAD"])
    runner = mailtide.BlockingRunner(
        "sqlite::memory:",
        "memory:///",
        lambda account: ("test-token", 2**62),
        gmail_origin=origin,
        confirm_timeout_s=0,
    )
    runner.migrate()
    assert runner.run(ACCOUNT, budget_s=30).stop == "idle"

    outcome = runner.act(ACCOUNT, "archive", message_ids=["m1"])
    assert outcome.stop == "idle", outcome.blocked
    assert mailbox.messages["m1"]["labels"] == ["UNREAD"]
    [view] = runner.messages(ACCOUNT, ["m1"])
    assert view.labels == ["UNREAD"]
    assert view.subject == "Message m1"

    runner.act(ACCOUNT, "mark_read", thread_id="t-m1")
    assert mailbox.messages["m1"]["labels"] == []
    runner.run(ACCOUNT, budget_s=30)
    [view] = runner.messages(ACCOUNT, ["m1"])
    assert view.labels == [] and view.observed_labels == []
    assert view.pending_actions == 0

    with pytest.raises(ValueError):
        runner.act(ACCOUNT, "explode", message_ids=["m1"])


def gil_disabled_build() -> bool:
    return bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def test_free_threaded_import_keeps_the_gil_disabled() -> None:
    if not gil_disabled_build():
        pytest.skip("GIL build")
    assert not sys._is_gil_enabled()


def test_concurrent_use_from_many_threads(gmail: tuple[Mailbox, str]) -> None:
    """Hammer one Runner from many threads (no GIL on free-threaded builds)."""
    mailbox, origin = gmail
    ids = [f"m{i}" for i in range(24)]
    for mid in ids:
        mailbox.add(mid, ["INBOX", "UNREAD"])
    runner = mailtide.BlockingRunner(
        "sqlite::memory:",
        "memory:///",
        lambda account: ("test-token", 2**62),
        gmail_origin=origin,
        confirm_timeout_s=0,
    )
    runner.migrate()

    # Racing runs of one account: the store fences them; the newest wins.
    with ThreadPoolExecutor(max_workers=8) as pool:
        stops = list(pool.map(lambda _: runner.run(ACCOUNT, budget_s=30).stop, range(8)))
    assert set(stops) <= {"idle", "superseded"}, stops
    assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
    assert len(runner.messages(ACCOUNT, ids)) == len(ids)

    archived = ids[:12]

    def act(mid: str) -> str:
        return runner.act(ACCOUNT, "archive", message_ids=[mid], budget_s=30).stop

    def read(mid: str) -> int:
        views = runner.messages(ACCOUNT, ids)
        runner.changes(ACCOUNT)
        parsed = mailtide.parse_message(runner.read_blob(ACCOUNT, mid))
        assert parsed.text is not None and mid in parsed.text
        return len(views)

    with ThreadPoolExecutor(max_workers=12) as pool:
        acts = [pool.submit(act, mid) for mid in archived]
        reads = [pool.submit(read, mid) for mid in ids[12:]]
        assert all(f.result() in {"idle", "superseded"} for f in acts)
        assert all(f.result() == len(ids) for f in reads)

    # Superseded runs left work for the next one: run until settled.
    for _ in range(5):
        assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
        views = {v.id: v for v in runner.messages(ACCOUNT, ids)}
        if all(v.pending_actions == 0 for v in views.values()):
            break
    for mid in archived:
        assert "INBOX" not in mailbox.messages[mid]["labels"], mid
        assert "INBOX" not in views[mid].labels, mid
    for mid in ids[12:]:
        assert views[mid].labels == ["INBOX", "UNREAD"], mid
    if gil_disabled_build():
        assert not sys._is_gil_enabled()
