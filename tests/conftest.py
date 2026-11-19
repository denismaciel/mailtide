from collections.abc import Iterator

import pytest
from fake_gmail import Mailbox, serve


@pytest.fixture
def gmail() -> Iterator[tuple[Mailbox, str]]:
    yield from serve()
