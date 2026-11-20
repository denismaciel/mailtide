"""Thread views: Runner.threads against the fake Gmail, and the host's list."""

import asyncio

import mailtide
import pytest
from conftest import make_config, tokens
from fake_gmail import Mailbox

ACCOUNT = "alice@example.com"


def fields(view: mailtide.MessageView) -> tuple:
    content = view.content
    return (
        view.id,
        view.thread_id,
        view.labels,
        view.observed_labels,
        view.checkpoint,
        view.unverified,
        view.pending_actions,
        view.blocked_actions,
        None if content is None else (content.blob, content.internal_date_ms, content.subject),
    )


def conversation(mailbox: Mailbox) -> None:
    """Ann writes, I reply copying Bob, and a reply is in my drafts."""
    mailbox.add(
        "m1",
        ["INBOX"],
        thread="t",
        date_ms=1_000,
        sender="Ann <ann@example.com>",
        to=["me@example.com"],
    )
    mailbox.add(
        "m2",
        ["INBOX", "UNREAD"],
        thread="t",
        date_ms=2_000,
        sender="me@example.com",
        to=["ANN@example.com"],
        cc=["Bob <bob@example.com>"],
    )
    mailbox.add("d1", ["DRAFT"], thread="t", date_ms=3_000)


def test_thread_view_fields(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    conversation(mailbox)

    async def main() -> None:
        r = mailtide.Runner(make_config(origin), tokens)
        await r.migrate()
        await r.track(ACCOUNT)
        assert (await r.run(ACCOUNT, budget_s=30)).stop == "idle"
        [view] = await r.threads(ACCOUNT, ["t"])
        assert isinstance(view, mailtide.ThreadView)
        with pytest.raises(AttributeError):
            object.__setattr__(view, "unread", False)  # frozen
        assert view.id == "t"
        assert [m.id for m in view.messages] == ["m1", "m2", "d1"]
        assert view.labels == ["INBOX", "UNREAD"]
        assert view.unread
        assert view.latest is view.messages[1]
        assert view.latest.content is not None
        assert view.latest.content.subject == "Message m2"
        assert (view.message_count, view.draft_count, view.awaiting_import) == (2, 1, 0)
        assert view.participants == [
            "Ann <ann@example.com>",
            "me@example.com",
            "Bob <bob@example.com>",
        ]
        ids = [m.id for m in view.messages]
        assert [fields(m) for m in view.messages] == [
            fields(m) for m in await r.messages(ACCOUNT, ids)
        ]

    asyncio.run(main())


def rows_in_gmail(mailbox: Mailbox) -> dict[str, tuple]:
    """What the host's table should hold: the same rule over Gmail's messages."""
    threads: dict[str, list[tuple[int, str, list[str]]]] = {}
    for mid, m in mailbox.messages.items():
        threads.setdefault(m["thread"], []).append((m["date_ms"], mid, m["labels"]))
    rows = {}
    for tid, members in threads.items():
        mail = sorted(member for member in members if "DRAFT" not in member[2])
        labels = sorted({label for _, _, labels in mail for label in labels})
        latest = mail[-1][1] if mail else None
        rows[tid] = (latest, labels, "UNREAD" in labels, len(mail))
    return rows


def test_a_host_list_follows_the_feed(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX", "UNREAD"], thread="a", date_ms=1_000)
    mailbox.add("m2", ["INBOX"], thread="b", date_ms=2_000)

    async def main() -> None:
        r = mailtide.Runner(make_config(origin), tokens)
        await r.migrate()
        await r.track(ACCOUNT)
        # The host's read model: one row per thread, kept by the feed.
        table: dict[str, tuple] = {}
        since: str | None = None

        async def follow() -> None:
            nonlocal since
            while True:
                page = await r.changes(ACCOUNT, since=since)
                assert isinstance(page, mailtide.ChangePage)
                touched = list(
                    dict.fromkeys(
                        thread_id
                        for change in page.changes
                        if (thread_id := getattr(change, "thread_id", None))
                    )
                )
                views = {view.id: view for view in await r.threads(ACCOUNT, touched)}
                for tid in touched:
                    if tid in views:
                        view = views[tid]
                        latest = view.latest.id if view.latest else None
                        table[tid] = (latest, view.labels, view.unread, view.message_count)
                    else:
                        table.pop(tid, None)
                since = page.next
                if not page.has_more:
                    return

        assert (await r.run(ACCOUNT, budget_s=30)).stop == "idle"
        await follow()
        assert table == rows_in_gmail(mailbox)

        mailbox.add("m3", ["INBOX", "UNREAD"], thread="a", date_ms=3_000)  # a reply
        assert (await r.run(ACCOUNT, budget_s=30)).stop == "idle"
        await follow()
        assert table["a"] == ("m3", ["INBOX", "UNREAD"], True, 2)
        assert table == rows_in_gmail(mailbox)

        assert (await r.act(ACCOUNT, "archive", thread_id="b")).stop == "idle"
        await follow()
        assert table["b"] == ("m2", [], False, 1)
        assert table == rows_in_gmail(mailbox)

        mailbox.delete("m2")
        assert (await r.run(ACCOUNT, budget_s=30)).stop == "idle"
        await follow()
        assert "b" not in table
        assert table == rows_in_gmail(mailbox)

    asyncio.run(main())


def test_unknown_threads_and_untracked_accounts_read_as_empty(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"], thread="t")

    async def main() -> None:
        r = mailtide.Runner(make_config(origin), tokens)
        await r.migrate()
        assert await r.threads(ACCOUNT, ["t"]) == []  # untracked, as messages() answers
        await r.track(ACCOUNT)
        assert await r.threads(ACCOUNT, ["t"]) == []  # nothing synced yet
        assert (await r.run(ACCOUNT, budget_s=30)).stop == "idle"
        assert await r.threads(ACCOUNT, []) == []
        assert await r.threads(ACCOUNT, ["unknown", ""]) == []
        assert [view.id for view in await r.threads(ACCOUNT, ["unknown", "t"])] == ["t"]

    asyncio.run(main())


def test_blocking_runner_threads(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    conversation(mailbox)
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    runner.track(ACCOUNT)
    assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
    [view] = runner.threads(ACCOUNT, ["t"])
    assert isinstance(view, mailtide.ThreadView)
    assert [m.id for m in view.messages] == ["m1", "m2", "d1"]
    assert view.latest is view.messages[1]
    assert (view.unread, view.message_count, view.draft_count) == (True, 2, 1)
    assert [fields(m) for m in view.messages] == [
        fields(m) for m in runner.messages(ACCOUNT, ["m1", "m2", "d1"])
    ]
    assert runner.threads(ACCOUNT, ["unknown"]) == []
