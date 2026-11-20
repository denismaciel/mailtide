import importlib.metadata

import mailtide
import pytest
from conftest import make_config

ACCOUNT = "alice@example.com"


def tokens(account: str) -> tuple[str, int]:
    return ("test-token", 2**62)


def test_version_matches_package() -> None:
    assert mailtide.version() == importlib.metadata.version("mailtide")


def test_parse_message() -> None:
    raw = (
        b"From: Ann <ann@example.com>\r\nSubject: Quote\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\nHello\r\n"
    )
    parsed = mailtide.parse_message(raw)
    assert parsed.headers[1] == ("Subject", "Quote")
    assert parsed.text is not None and parsed.text.strip() == "Hello"
    assert parsed.attachments == []


def test_verify_push_rejects_missing_token() -> None:
    with pytest.raises(mailtide.InvalidPush):
        mailtide.verify_push("", b"{}", b'{"keys": []}', "aud", "sa@example.com")


def test_empty_account_has_no_changes() -> None:
    runner = mailtide.BlockingRunner(make_config(), tokens)
    runner.migrate()
    runner.track("alice@example.com")
    page = runner.changes(ACCOUNT)
    assert page.changes == []
    assert not page.has_more


def test_revoked_delegation_blocks_the_account() -> None:
    def revoked(account: str) -> tuple[str, int]:
        raise mailtide.DelegationRevoked(f"{account} revoked")

    runner = mailtide.BlockingRunner(make_config(), revoked)
    runner.migrate()
    runner.track("alice@example.com")
    outcome = runner.run(ACCOUNT, budget_s=10)
    assert outcome.stop == "blocked"
    assert outcome.blocked is not None and "delegation_revoked" in outcome.blocked


def test_invalid_account_is_a_value_error() -> None:
    runner = mailtide.BlockingRunner(make_config(), tokens)
    with pytest.raises(ValueError):
        runner.run("not-an-address")


def test_quota_deferred_is_a_mailtide_error() -> None:
    """A direct call the quota gate would hold past its budget raises it."""
    assert issubclass(mailtide.QuotaDeferred, mailtide.MailtideError)
    assert "QuotaDeferred" in mailtide.__all__


def test_write_outcome_unknown_is_a_mailtide_error() -> None:
    """A direct write whose answer was lost raises it."""
    assert issubclass(mailtide.WriteOutcomeUnknown, mailtide.MailtideError)
    assert "WriteOutcomeUnknown" in mailtide.__all__
