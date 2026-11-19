"""The change feed: typed entries a consumer matches exhaustively."""

from typing import assert_never

import mailtide
from conftest import make_config, tokens
from fake_gmail import Mailbox

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
