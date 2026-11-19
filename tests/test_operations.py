"""Operations a host needs besides syncing: config, status, admission, resume, forget."""

import dataclasses

import mailtide
import pytest
from conftest import make_config, tokens
from fake_gmail import Mailbox
from mailtide.config import Blobs, Config, Database, Engine, Http, Topic

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
    outcome = runner.run(ACCOUNT, budget_s=30)
    assert outcome.stop == "blocked"
    assert outcome.blocked is not None and outcome.blocked.startswith("api_access_denied")
    assert mailbox.requests <= 2, f"no retry loop ({mailbox.requests} requests)"
    assert runner.status(ACCOUNT).phase == "blocked"
