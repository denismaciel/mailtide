from collections.abc import Iterator

import pytest
from fake_gmail import Mailbox, serve
from mailtide.config import Blobs, Config, Database, Engine, Http


def tokens(account: str) -> tuple[str, int]:
    """A token source that never expires."""
    return ("test-token", 2**62)


def make_config(origin: str | None = None, confirm_timeout_s: float = 60.0) -> Config:
    """An in-memory runner config, pointed at a fake Gmail when given."""
    return Config(
        database=Database("sqlite::memory:"),
        blobs=Blobs("memory:///"),
        http=Http(gmail_origin=origin),
        engine=Engine(confirm_timeout_s=confirm_timeout_s),
    )


@pytest.fixture
def gmail() -> Iterator[tuple[Mailbox, str]]:
    yield from serve()
