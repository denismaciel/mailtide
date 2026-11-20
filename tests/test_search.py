"""Search: Gmail's own query through the quota gate, hits marked against the mirror."""

import asyncio
import dataclasses
import inspect

import mailtide
import pytest
from conftest import make_config, tokens
from fake_gmail import FakeGmail
from mailtide.config import Quota
from mailtide.testing import ManualClock

ACCOUNT = "alice@example.com"


def synced(gmail: FakeGmail, ids: list[str], clock: ManualClock | None = None):
    """A blocking runner that synced ``ids`` from ``gmail``."""
    for mid in ids:
        gmail.add(mid, ["INBOX"])
    runner = mailtide.BlockingRunner(make_config(gmail.origin), tokens, clock=clock)
    runner.migrate()
    runner.track(ACCOUNT)
    runner.run(ACCOUNT)
    return runner


def test_search_returns_hits_and_pages() -> None:
    with FakeGmail() as gmail:
        gmail.script_search("from:alice", ["m3", "m1", "x9"])

        async def scenario() -> tuple[mailtide.SearchPage, mailtide.SearchPage]:
            for mid in ("m1", "m3"):
                gmail.add(mid, ["INBOX"])
            runner = mailtide.Runner(make_config(gmail.origin), tokens)
            await runner.migrate()
            await runner.track(ACCOUNT)
            await runner.run(ACCOUNT)
            first = await runner.search(ACCOUNT, "from:alice", limit=2)
            assert first.next_page_token is not None
            last = await runner.search(
                ACCOUNT, "from:alice", limit=2, page_token=first.next_page_token
            )
            changes = await runner.changes(ACCOUNT, since=first.mirror_state)
            assert isinstance(changes, mailtide.ChangePage)
            assert changes.changes == [], "nothing changed since the page"
            return first, last

        first, last = asyncio.run(scenario())

    assert [(h.message_id, h.thread_id, h.state) for h in first.hits] == [
        ("m3", "t-m3", "imported"),
        ("m1", "t-m1", "imported"),
    ]
    assert (first.estimate, first.deleted, first.phase) == (3, 0, "live")
    assert first.cursor is not None
    assert [(h.message_id, h.state) for h in last.hits] == [("x9", "unknown")]
    assert last.next_page_token is None
    assert [s["maxResults"] for s in gmail.mailbox.searches] == [["2"], ["2"]]
    assert "includeSpamTrash" not in gmail.mailbox.searches[0]


def test_hits_hydrate_with_messages() -> None:
    with FakeGmail() as gmail:
        gmail.mailbox.withheld.add("m2")  # its body is not fetched yet
        runner = synced(gmail, ["m1", "m2"], clock=ManualClock())
        gmail.script_search("invoice", ["m1", "m2", "x9"])

        page = runner.search(ACCOUNT, "invoice")
        views = runner.messages(ACCOUNT, [hit.message_id for hit in page.hits])

    assert [h.state for h in page.hits] == ["imported", "not_imported", "unknown"]
    assert [v.id for v in views] == ["m1", "m2"], "the unknown hit has no view yet"
    assert views[0].content is not None
    assert views[1].content is None
    assert views[1].labels == ["INBOX"]


def test_page_token_for_another_query_raises() -> None:
    with FakeGmail() as gmail:
        runner = synced(gmail, [])
        gmail.script_search("from:alice", ["m1", "m2"])
        token = runner.search(ACCOUNT, "from:alice", limit=1).next_page_token
        assert token is not None
        before = gmail.mailbox.requests

        with pytest.raises(mailtide.InvalidPageToken):
            runner.search(ACCOUNT, "from:bob", limit=1, page_token=token)
        with pytest.raises(mailtide.InvalidPageToken):
            runner.search(ACCOUNT, "from:alice", limit=2, page_token=token)

        assert gmail.mailbox.requests == before, "Gmail was never asked"


def test_refused_search_raises_search_refused() -> None:
    with FakeGmail() as gmail:
        runner = synced(gmail, ["m1"])
        gmail.mailbox.search_refusal = (400, "invalidArgument")

        with pytest.raises(mailtide.SearchRefused):
            runner.search(ACCOUNT, "label:")

        gmail.mailbox.search_refusal = None
        gmail.add("m2", ["INBOX"])
        outcome = runner.run(ACCOUNT)
    assert outcome.stop == "idle"
    assert outcome.imported == 1, "the account still syncs"
    status = runner.status(ACCOUNT)
    assert status is not None and status.phase != "blocked"


def test_search_threads() -> None:
    with FakeGmail() as gmail:
        runner = synced(gmail, ["m1", "m2"])
        gmail.script_search("has:attachment", ["m2", "m1", "m2"])

        page = runner.search_threads(ACCOUNT, "has:attachment")

    assert [(h.thread_id, h.history_id) for h in page.hits] == [
        ("t-m2", gmail.mailbox.history_id),
        ("t-m1", gmail.mailbox.history_id),
    ]
    assert isinstance(page.hits[0], mailtide.ThreadHit)
    assert page.next_page_token is None
    assert page.phase == "live"


def test_blocking_runner_has_search() -> None:
    for name in ("search", "search_threads"):
        assert callable(getattr(mailtide.BlockingRunner, name))
        assert inspect.iscoroutinefunction(getattr(mailtide.Runner, name))
    with FakeGmail() as gmail:
        runner = synced(gmail, ["m1"])
        gmail.script_search("x", ["m1"])
        assert isinstance(runner.search(ACCOUNT, "x"), mailtide.SearchPage)
        assert isinstance(runner.search_threads(ACCOUNT, "x"), mailtide.ThreadSearchPage)
        with pytest.raises(ValueError, match="non-blank"):
            runner.search(ACCOUNT, "   ")
        misspelt: dict = {"lables": ["INBOX"]}
        with pytest.raises(TypeError, match="lables"):
            runner.search(ACCOUNT, "x", **misspelt)


def test_search_literal_quotes_user_text() -> None:
    assert mailtide.search_literal("alice@example.org") == "alice@example.org"
    assert mailtide.search_literal("rapport} OR {kangaroo") == '"rapport or kangaroo"'
    assert mailtide.search_literal("from:alice") == '"from alice"'
    assert mailtide.search_literal("Z | K AROUND 2") == '"z k around 2"'
    with pytest.raises(ValueError):
        mailtide.search_literal(' "()" ')
    assert mailtide.search_by_message_id("<id123@example.org>") == "rfc822msgid:id123@example.org"
    with pytest.raises(ValueError):
        mailtide.search_by_message_id("<two words@example.org>")


def test_unknown_label_is_checked_with_gmail() -> None:
    with FakeGmail() as gmail:
        runner = mailtide.BlockingRunner(make_config(gmail.origin), tokens)
        runner.migrate()
        runner.track(ACCOUNT)
        gmail.script_search("x", ["m1"])
        # No label listing yet: Gmail is asked, has INBOX, and the search runs.
        runner.search(ACCOUNT, "x", label_ids=("INBOX",))
        assert gmail.mailbox.searches[0]["labelIds"] == ["INBOX"]

        runner.run(ACCOUNT)
        with pytest.raises(mailtide.SearchRefused) as deleted:
            runner.search(ACCOUNT, "x", label_ids=["INBOX", "Label_9"])
        assert isinstance(deleted.value, mailtide.UnknownLabel)
        assert deleted.value.label_id == "Label_9"
        assert len(gmail.mailbox.searches) == 1, "no search for an unknown label"

        gmail.mailbox.label("Label_2", "Projects")  # created after the listing
        runner.search(ACCOUNT, "x", label_ids=["Label_2"])
        assert gmail.mailbox.searches[1]["labelIds"] == ["Label_2"]
        assert all(label.id != "Label_2" for label in runner.labels(ACCOUNT)), "nothing recorded"


def test_a_deferred_search_raises_quota_deferred() -> None:
    with FakeGmail() as gmail:
        # The smallest budget the config allows: 86 units a minute.
        quota = Quota(account_per_minute=86)
        config = dataclasses.replace(make_config(gmail.origin), quota=quota)
        runner = mailtide.BlockingRunner(config, tokens)
        runner.migrate()
        runner.track(ACCOUNT)
        sent = 0
        with pytest.raises(mailtide.QuotaDeferred) as deferred:
            for _ in range(10):  # 10 units each: the budget lasts 8
                runner.search_threads(ACCOUNT, "x", budget_s=0.1)
                sent += 1
        assert sent == 8
        assert deferred.value.until_ms > 0
        assert len(gmail.mailbox.searches) == sent, "the deferred one was never sent"
