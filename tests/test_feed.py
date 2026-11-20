"""The change feed: typed entries a consumer matches exhaustively."""

import re
from typing import assert_never

import mailtide
from conftest import make_config, tokens
from fake_gmail import Mailbox
from mailtide.testing import FakeGmail

ACCOUNT = "alice@example.com"


def describe(change: mailtide.Change) -> str:
    """What a consumer does with each kind (exhaustive: a new kind fails type checking)."""
    match change:
        case mailtide.MessageDiscovered():
            return f"discovered {change.id}"
        case mailtide.MessageImported():
            direction = "sent" if "SENT" in change.labels else "received"
            return f"imported {change.id} ({direction}, {change.size} bytes)"
        case mailtide.LabelsChanged():
            return f"labels {change.id} +{change.added} -{change.removed}"
        case mailtide.OverlayChanged():
            return f"overlay {change.id} {change.labels}"
        case mailtide.MessageDeleted():
            return f"deleted {change.id}"
        case mailtide.LabelDefinitionsChanged():
            return "label definitions"
        case _:
            assert_never(change)


def test_the_feed_tells_a_pipeline_what_happened(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("in1", ["INBOX", "UNREAD"])
    mailbox.add("out1", ["SENT"])
    runner = mailtide.BlockingRunner(make_config(origin, confirm_timeout_s=0), tokens)
    runner.migrate()
    runner.track("alice@example.com")
    runner.run(ACCOUNT, budget_s=30)

    page = runner.changes(ACCOUNT)
    assert isinstance(page, mailtide.ChangePage)
    described = [describe(c) for c in page.changes]
    assert "label definitions" in described
    assert {c.id for c in page.changes if isinstance(c, mailtide.MessageDiscovered)} == {
        "in1",
        "out1",
    }
    imported = {c.id: c for c in page.changes if isinstance(c, mailtide.MessageImported)}
    assert set(imported) == {"in1", "out1"}
    assert "SENT" in imported["out1"].labels and imported["out1"].blob.endswith("out1.eml")
    # Entries carry a stable identity for deduplication on replay.
    identities = [(c.state, c.index) for c in page.changes]
    assert len(identities) == len(set(identities))

    # A broker action shows as an overlay, then Gmail's change as labels.
    runner.admit(ACCOUNT, "mark_read", message_ids=["in1"])
    runner.run(ACCOUNT, budget_s=30)
    later_page = runner.changes(ACCOUNT, since=page.next)
    assert isinstance(later_page, mailtide.ChangePage)
    later = later_page.changes
    kinds = [c.kind for c in later if getattr(c, "id", None) == "in1"]
    assert kinds[0] == "overlay_changed"
    assert "labels_changed" in kinds
    removed = [c for c in later if isinstance(c, mailtide.LabelsChanged) and c.id == "in1"]
    assert removed[-1].removed == ["UNREAD"]


def test_imports_carry_their_envelope() -> None:
    with FakeGmail() as gmail:
        gmail.add("in1", ["INBOX"])
        gmail.add("in2", ["INBOX"])
        runner = mailtide.BlockingRunner(make_config(gmail.origin), tokens)
        runner.migrate()
        runner.track(ACCOUNT)
        runner.run(ACCOUNT, budget_s=30)

        page = runner.changes(ACCOUNT)
        assert isinstance(page, mailtide.ChangePage)
        imported = {c.id: c for c in page.changes if isinstance(c, mailtide.MessageImported)}
        assert set(imported) == {"in1", "in2"}
        # A message list renders from the feed alone: no second read.
        for view in runner.messages(ACCOUNT, ["in1", "in2"]):
            content, stored = imported[view.id].content, view.content
            assert content is not None and stored is not None
            assert content.subject == stored.subject == f"Message {view.id}"
            assert content.sender == stored.sender == f"sender-{view.id}@example.com"
            assert content.rfc822_message_id == stored.rfc822_message_id
            assert content.rfc822_message_id == f"<{view.id}@example.com>"
            assert content.blob == imported[view.id].blob


def test_compaction_and_rebuilding(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    for mid in ["a", "b", "c"]:
        mailbox.add(mid, ["INBOX"])
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    runner.track("alice@example.com")
    runner.run(ACCOUNT, budget_s=30)
    page = runner.changes(ACCOUNT)
    assert isinstance(page, mailtide.ChangePage)
    # The consumer processed everything: compact it away.
    assert runner.compact(ACCOUNT, page.next) > 0
    too_old = runner.changes(ACCOUNT)
    assert isinstance(too_old, mailtide.StateTooOld) and too_old.oldest == page.next
    # Rebuild: enumerate the mirror, then follow the feed from the floor.
    ids = runner.message_ids(ACCOUNT, limit=2)
    ids += runner.message_ids(ACCOUNT, after=ids[-1], limit=2)
    assert ids == ["a", "b", "c"]
    assert [v.id for v in runner.messages(ACCOUNT, ids)] == ids
    after = runner.changes(ACCOUNT, since=too_old.oldest)
    assert isinstance(after, mailtide.ChangePage) and after.changes == []


STATE = re.compile(r"mt2-[0-9a-f]+\.[0-9a-f]+(\.[0-9a-f]+)?")


def read_to_end(
    runner: mailtide.BlockingRunner, since: str | None, limit: int
) -> list[mailtide.ChangePage]:
    """Every page from `since` on, following `next` while `has_more`."""
    pages = []
    while True:
        page = runner.changes(ACCOUNT, since=since, limit=limit)
        assert isinstance(page, mailtide.ChangePage)
        pages.append(page)
        since = page.next
        if not page.has_more:
            return pages


def entries(pages: list[mailtide.ChangePage]) -> list[mailtide.Change]:
    return [change for page in pages for change in page.changes]


def bootstrapped(gmail: FakeGmail, messages: int) -> tuple[mailtide.BlockingRunner, int]:
    """A runner that synced `messages` messages, and the account's generation."""
    for n in range(messages):
        gmail.add(f"m{n:02}", ["INBOX"])
    runner = mailtide.BlockingRunner(make_config(gmail.origin), tokens)
    runner.migrate()
    generation = runner.track(ACCOUNT).generation
    assert generation is not None
    runner.run(ACCOUNT, budget_s=30)
    return runner, generation


def test_a_large_commit_pages_within_the_limit() -> None:
    with FakeGmail() as gmail:
        runner, _ = bootstrapped(gmail, 40)
        pages = read_to_end(runner, None, 7)
        assert all(len(page.changes) <= 7 for page in pages)
        discovered = [c.id for c in entries(pages) if isinstance(c, mailtide.MessageDiscovered)]
        assert sorted(discovered) == [f"m{n:02}" for n in range(40)]
        # Every `next` is a state string; some point inside the commit that
        # discovered the mailbox, each from a page that commit fills alone.
        assert all(STATE.fullmatch(page.next) for page in pages)
        inside = [page for page in pages if page.next.count(".") == 2]
        assert inside
        for page in inside:
            assert len(page.changes) == 7 and len({c.state for c in page.changes}) == 1
        # The identities are those of one unbounded read.
        whole = entries(read_to_end(runner, None, 5_000))
        assert [(c.state, c.index) for c in entries(pages)] == [(c.state, c.index) for c in whole]


def test_a_state_string_from_before_split_pages_still_resumes() -> None:
    with FakeGmail() as gmail:
        runner, generation = bootstrapped(gmail, 5)
        whole = entries(read_to_end(runner, None, 5_000))
        first_commit = whole[0].state
        size = sum(1 for c in whole if c.state == first_commit)
        # As an older build wrote it: generation and commit, two hex fields.
        old = f"mt2-{generation:x}.{int(first_commit.split('.')[1], 16):x}"
        # A new build's position at the end of that commit is the same string.
        page = runner.changes(ACCOUNT, limit=size)
        assert isinstance(page, mailtide.ChangePage)
        assert page.next == old
        resumed = runner.changes(ACCOUNT, since=old, limit=1)
        assert isinstance(resumed, mailtide.ChangePage)
        assert [(c.state, c.index) for c in resumed.changes] == [
            (whole[size].state, whole[size].index)
        ]


def test_compacting_through_a_mid_commit_position_keeps_the_rest() -> None:
    with FakeGmail() as gmail:
        runner, _ = bootstrapped(gmail, 10)
        whole = entries(read_to_end(runner, None, 5_000))
        # Read the first commit whole, then 3 entries of the next, larger one.
        first = sum(1 for c in whole if c.state == whole[0].state)
        page = runner.changes(ACCOUNT, limit=first)
        assert isinstance(page, mailtide.ChangePage) and page.next.count(".") == 1
        page = runner.changes(ACCOUNT, since=page.next, limit=3)
        assert isinstance(page, mailtide.ChangePage)
        assert page.next.count(".") == 2 and page.has_more
        # Only the whole commit before the reader's goes.
        assert runner.compact(ACCOUNT, page.next) == first
        rest = entries(read_to_end(runner, page.next, 500))
        read = first + 3
        assert [(c.state, c.index) for c in rest] == [(c.state, c.index) for c in whole[read:]]
        too_old = runner.changes(ACCOUNT)
        assert isinstance(too_old, mailtide.StateTooOld)
