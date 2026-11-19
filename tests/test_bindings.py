import mailtide
import pytest

ACCOUNT = "alice@example.com"


def tokens(account: str) -> tuple[str, int]:
    return ("test-token", 2**62)


def test_version_matches_package() -> None:
    assert mailtide.version() == "0.1.0"


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
    runner = mailtide.BlockingRunner("sqlite::memory:", "memory:///", tokens)
    runner.migrate()
    page = runner.changes(ACCOUNT)
    assert page.changes == []
    assert not page.has_more


def test_revoked_delegation_blocks_the_account() -> None:
    def revoked(account: str) -> tuple[str, int]:
        raise mailtide.DelegationRevoked(f"{account} revoked")

    runner = mailtide.BlockingRunner("sqlite::memory:", "memory:///", revoked)
    runner.migrate()
    outcome = runner.run(ACCOUNT, budget_s=10)
    assert outcome.stop == "blocked"
    assert outcome.blocked is not None and "delegation_revoked" in outcome.blocked


def test_invalid_account_is_a_value_error() -> None:
    runner = mailtide.BlockingRunner("sqlite::memory:", "memory:///", tokens)
    with pytest.raises(ValueError):
        runner.run("not-an-address")
