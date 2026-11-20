"""Operations a host needs besides syncing: config, status, admission, resume, forget."""

import dataclasses

import mailtide
import pytest
from conftest import make_config, tokens
from fake_gmail import Mailbox
from mailtide.config import Blobs, Config, Database, Engine, Http, Quota, Topic
from mailtide.testing import ManualClock

ACCOUNT = "alice@example.com"


def counting(calls: list[str]):
    def source(account: str) -> tuple[str, int]:
        calls.append(account)
        return ("test-token", 2**62)

    return source


@pytest.mark.parametrize(
    ("change", "field"),
    [
        ({"database": Database("postgresql+asyncpg://h/db")}, "database.url"),
        ({"database": Database("sqlite::memory:", max_connections=0)}, "database.max_connections"),
        ({"engine": Engine(poll_active_s=1000.0)}, "engine.poll_active_s"),
        ({"http": Http(request_timeout_s=float("nan"))}, "http.request_timeout_s"),
        ({"http": Http(gmail_origin="http://evil.example")}, "http.gmail_origin"),
        ({"watch": Topic("")}, "watch.name"),
    ],
)
def test_invalid_config_names_the_field(change: dict, field: str) -> None:
    config = dataclasses.replace(make_config(), **change)
    with pytest.raises(ValueError, match=field.replace(".", r"\.")):
        mailtide.BlockingRunner(config, tokens)


def test_unreachable_postgres_is_lazy_then_typed() -> None:
    config = Config(
        database=Database("postgres://nobody@127.0.0.1:1/none", acquire_timeout_s=1.0),
        blobs=Blobs("memory:///"),
    )
    runner = mailtide.BlockingRunner(config, tokens)  # lazy: nothing connects yet
    assert runner.health().database == "unavailable"
    with pytest.raises(mailtide.StoreUnavailable):
        runner.status(ACCOUNT)


def test_status_admit_resume_forget(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX", "UNREAD"])
    calls: list[str] = []
    runner = mailtide.BlockingRunner(make_config(origin, confirm_timeout_s=0), counting(calls))
    runner.migrate()
    assert runner.health().database == "ok"
    assert runner.status(ACCOUNT) is None
    assert runner.accounts() == []
    runner.track(ACCOUNT)

    assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
    status = runner.status(ACCOUNT)
    assert status is not None
    assert (status.phase, status.messages, status.pending_imports) == ("live", 1, 0)
    assert runner.accounts() == [ACCOUNT]
    # Tokens are cached: one mint for the whole run.
    assert len(calls) == 1

    # Reading status never claims the account.
    epoch = status.epoch
    assert runner.status(ACCOUNT).epoch == epoch

    # Admit without delivering: visible at once, delivered by the next run.
    assert runner.admit(ACCOUNT, "archive", message_ids=["m1"]) == 1
    assert runner.admit(ACCOUNT, "archive", message_ids=["unknown"]) == 0
    [view] = runner.messages(ACCOUNT, ["m1"])
    assert view.labels == ["UNREAD"] and view.pending_actions == 1
    assert mailbox.messages["m1"]["labels"] == ["INBOX", "UNREAD"]
    assert runner.status(ACCOUNT).actions_pending == 1
    runner.run(ACCOUNT, budget_s=30)
    assert mailbox.messages["m1"]["labels"] == ["UNREAD"]

    assert runner.resume(ACCOUNT).outcome == "not_blocked"
    assert runner.resume("bob@example.com").outcome == "unknown"

    forgotten = runner.forget(ACCOUNT)
    assert (forgotten.outcome, forgotten.messages, forgotten.blobs) == ("forgotten", 1, 1)
    assert runner.status(ACCOUNT) is None
    assert runner.forget(ACCOUNT).outcome == "unknown"


def test_forget_leaves_other_accounts_content(gmail: tuple[Mailbox, str]) -> None:
    """Blobs are per account: two mailboxes holding the same message keep
    two copies, and forgetting one account never touches the other's."""
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"])
    # Its address extends the first one's: blobs are kept by path segment.
    other = "alice@example.com.au"
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    for account in (ACCOUNT, other):
        runner.track(account)
        assert runner.run(account, budget_s=30).stop == "idle"
    raw = runner.read_blob(other, "m1")
    assert runner.read_blob(ACCOUNT, "m1") == raw
    [before] = runner.messages(other, ["m1"])
    assert before.content is not None

    forgotten = runner.forget(ACCOUNT)
    assert (forgotten.outcome, forgotten.messages, forgotten.blobs) == ("forgotten", 1, 1)
    with pytest.raises(mailtide.BlobNotFound):
        runner.read_blob(ACCOUNT, "m1")
    assert runner.read_blob(other, "m1") == raw
    [after] = runner.messages(other, ["m1"])
    assert after.content is not None
    assert (after.labels, after.content.blob, after.content.size) == (
        before.labels,
        before.content.blob,
        before.content.size,
    )
    assert after.content.blob.startswith(f"{other}/")


def test_a_resumed_account_syncs_again(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"])
    revoked = {"on": True}

    def source(account: str) -> tuple[str, int]:
        if revoked["on"]:
            raise mailtide.DelegationRevoked(account)
        return ("test-token", 2**62)

    runner = mailtide.BlockingRunner(make_config(origin), source)
    runner.migrate()
    runner.track("alice@example.com")
    assert runner.run(ACCOUNT, budget_s=10).stop == "blocked"
    status = runner.status(ACCOUNT)
    assert status.phase == "blocked" and "delegation_revoked" in status.blocked
    assert runner.run(ACCOUNT, budget_s=10).stop == "blocked"

    revoked["on"] = False  # the operator fixed delegation
    resumed = runner.resume(ACCOUNT)
    assert (resumed.outcome, resumed.phase) == ("resumed", "bootstrapping")
    assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
    assert runner.status(ACCOUNT).messages == 1


def test_missing_gmail_scope_blocks_instead_of_looping(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.refuse = "insufficientPermissions"
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    runner.track("alice@example.com")
    outcome = runner.run(ACCOUNT, budget_s=30)
    assert outcome.stop == "blocked"
    assert outcome.blocked is not None and outcome.blocked.startswith("api_access_denied")
    assert mailbox.requests <= 2, f"no retry loop ({mailbox.requests} requests)"
    assert runner.status(ACCOUNT).phase == "blocked"


def test_an_invalid_label_change_is_rejected_when_built(gmail: tuple[Mailbox, str]) -> None:
    _, origin = gmail
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    runner.track("alice@example.com")
    with pytest.raises(ValueError, match="both added and removed"):
        runner.admit(
            "alice@example.com",
            "change_labels",
            message_ids=["m1"],
            add_labels=["Label_1"],
            remove_labels=["Label_1"],
        )
    with pytest.raises(ValueError, match="SENT"):
        runner.admit("alice@example.com", "change_labels", message_ids=["m1"], add_labels=["SENT"])


def test_a_failing_token_source_shows_in_the_outcome(gmail: tuple[Mailbox, str]) -> None:
    """Failures are retried after backoff, so the run can end idle; the
    outcome must still say requests are failing."""
    _, origin = gmail

    def broken(account: str) -> tuple[str, int]:
        raise RuntimeError("credentials unavailable")

    runner = mailtide.BlockingRunner(make_config(origin), broken)
    runner.migrate()
    runner.track("alice@example.com")
    out = runner.run("alice@example.com", budget_s=5)
    assert out.failed_requests >= 1
    assert out.last_failure == "transient"
    assert out.gmail_requests == 0


def test_only_tracked_accounts_sync(gmail: tuple[Mailbox, str]) -> None:
    _, origin = gmail
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    with pytest.raises(mailtide.AccountNotTracked):
        runner.run("carol@example.com", budget_s=5)
    assert runner.status("carol@example.com") is None
    first = runner.track("carol@example.com")
    assert first.outcome == "tracked" and first.generation
    assert runner.track("carol@example.com").outcome == "already_tracked"
    assert runner.run("carol@example.com", budget_s=30).stop == "idle"


def test_a_state_from_before_forget_is_too_old(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"])
    clock = ManualClock()
    runner = mailtide.BlockingRunner(make_config(origin), tokens, clock=clock)
    runner.migrate()
    runner.track("dave@example.com")
    runner.run("dave@example.com", budget_s=30)
    old = runner.changes("dave@example.com").next
    runner.forget("dave@example.com")
    clock.advance(1)
    runner.track("dave@example.com")
    runner.run("dave@example.com", budget_s=30)
    assert isinstance(runner.changes("dave@example.com", old), mailtide.StateTooOld)
    assert isinstance(runner.changes("dave@example.com"), mailtide.ChangePage)


def test_a_missing_blob_is_not_found(gmail: tuple[Mailbox, str]) -> None:
    _, origin = gmail
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    with pytest.raises(mailtide.BlobNotFound) as raised:
        runner.read_blob("alice@example.com", "nope")
    assert "alice" not in str(raised.value)


def backlog_runner(origin: str) -> mailtide.BlockingRunner:
    """A runner whose quota admits the bootstrap and two Backfill imports
    (70% of 250 units), and the clock never moves: the rest wait."""
    config = dataclasses.replace(make_config(origin), quota=Quota(account_per_minute=250))
    runner = mailtide.BlockingRunner(config, tokens, clock=ManualClock())
    runner.migrate()
    runner.track(ACCOUNT)
    return runner


def test_prioritise_then_the_feed_reports_the_import(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    for n in range(10):
        mailbox.add(f"m{n}", ["INBOX"])
    runner = backlog_runner(origin)
    runner.run(ACCOUNT, budget_s=30)
    waiting = [v.id for v in runner.messages(ACCOUNT, list(mailbox.messages)) if v.content is None]
    assert len(waiting) >= 5, "the quota holds the backlog back"
    opened = waiting[0]
    since = runner.changes(ACCOUNT).next

    result = runner.prioritise(ACCOUNT, [opened])
    assert isinstance(result, mailtide.PrioritiseResult)
    assert result.queued == [opened]
    assert result.imported == result.unknown == result.blocked == result.over_limit == []
    runner.run(ACCOUNT, budget_s=30)

    page = runner.changes(ACCOUNT, since)
    assert isinstance(page, mailtide.ChangePage)
    imported = [e for e in page.changes if isinstance(e, mailtide.MessageImported)]
    assert [e.id for e in imported] == [opened], "only the opened message jumped the queue"
    assert imported[0].content is not None
    [view] = runner.messages(ACCOUNT, [opened])
    assert view.content is not None


def test_prioritise_reports_imported_and_unknown_ids(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"])
    runner = mailtide.BlockingRunner(make_config(origin), tokens)
    runner.migrate()
    runner.track(ACCOUNT)
    assert runner.run(ACCOUNT, budget_s=30).stop == "idle"

    result = runner.prioritise(ACCOUNT, ["m1", "made-up", "m1"])
    assert (result.queued, result.imported, result.unknown) == ([], ["m1"], ["made-up"])
    assert result.blocked == result.over_limit == []
    with pytest.raises(ValueError, match="at most 100"):
        runner.prioritise(ACCOUNT, [f"x{n}" for n in range(101)])
