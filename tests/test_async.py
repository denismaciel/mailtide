"""The async Runner: awaitables on asyncio, cancellation, async token sources."""

import asyncio
import sys
import sysconfig
import time

import mailtide
import pytest
from conftest import make_config
from fake_gmail import Mailbox

ACCOUNT = "alice@example.com"


def tokens(account: str) -> tuple[str, int]:
    return ("test-token", 2**62)


def runner(origin: str, token_source=tokens) -> mailtide.Runner:
    return mailtide.Runner(make_config(origin, confirm_timeout_s=0), token_source)


def test_methods_are_awaitable(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX", "UNREAD"])
    mailbox.add("m2", [])

    async def main() -> None:
        r = runner(origin)
        await r.migrate()
        await r.track("alice@example.com")
        outcome = await r.run(ACCOUNT, budget_s=30)
        assert outcome.stop == "idle", outcome.blocked
        assert outcome.imported == 2
        raw = await r.read_blob(ACCOUNT, "m2")
        assert isinstance(raw, bytes)
        assert mailtide.parse_message(raw).text.strip() == "Body of m2"
        page = await r.changes(ACCOUNT)
        assert page.changes and not page.has_more

        acted = await r.act(ACCOUNT, "archive", message_ids=["m1"])
        assert acted.admitted == 1
        assert mailbox.messages["m1"]["labels"] == ["UNREAD"]
        await r.run(ACCOUNT, budget_s=30)
        [view] = await r.messages(ACCOUNT, ["m1"])
        assert view.labels == ["UNREAD"] and view.pending_actions == 0

    asyncio.run(main())


def test_cancelling_a_run_stops_it_and_releases_its_quota(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"])

    async def main() -> None:
        r = runner(origin)
        await r.migrate()
        await r.track("alice@example.com")
        mailbox.delay_s = 3.0  # Gmail hangs: the run is stuck in a request
        task = asyncio.create_task(r.run(ACCOUNT, budget_s=60))
        await asyncio.sleep(0.5)
        started = time.monotonic()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert time.monotonic() - started < 1.0, "cancellation must not wait for Gmail"

        # The abandoned request gave its quota lease back, and nothing is
        # half-done: a later run completes normally.
        mailbox.delay_s = 0.0
        outcome = await r.run(ACCOUNT, budget_s=30)
        assert outcome.stop == "idle", outcome.blocked
        assert len(await r.messages(ACCOUNT, ["m1"])) == 1

    asyncio.run(main())


def test_timeouts_compose_with_asyncio(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"])

    async def main() -> None:
        r = runner(origin)
        await r.migrate()
        await r.track("alice@example.com")
        mailbox.delay_s = 3.0
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.5):
                await r.run(ACCOUNT, budget_s=60)

    asyncio.run(main())


def test_async_token_sources_are_awaited_on_the_callers_loop(
    gmail: tuple[Mailbox, str],
) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"])
    seen_loops: list[asyncio.AbstractEventLoop] = []

    async def mint(account: str) -> tuple[str, int]:
        seen_loops.append(asyncio.get_running_loop())
        await asyncio.sleep(0)
        return ("test-token", 2**62)

    async def main() -> None:
        r = runner(origin, mint)
        await r.migrate()
        await r.track("alice@example.com")
        outcome = await r.run(ACCOUNT, budget_s=30)
        assert outcome.stop == "idle", outcome.blocked
        assert seen_loops and all(loop is asyncio.get_running_loop() for loop in seen_loops)

    asyncio.run(main())


def test_a_block_is_durable_and_stops_calling_the_token_source() -> None:
    calls: list[str] = []

    def revoked(account: str) -> tuple[str, int]:
        calls.append(account)
        raise mailtide.DelegationRevoked(f"{account} revoked")

    async def main() -> None:
        r = mailtide.Runner(make_config(), revoked)
        await r.migrate()
        await r.track("alice@example.com")
        first = await r.run(ACCOUNT, budget_s=10)
        assert first.stop == "blocked"
        assert first.commits >= 1, "the block itself must be committed"
        before = len(calls)
        second = await r.run(ACCOUNT, budget_s=10)
        assert second.stop == "blocked"
        assert len(calls) == before, "a blocked account must not mint tokens"

    asyncio.run(main())


def test_many_concurrent_awaits_on_one_loop(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    ids = [f"m{i}" for i in range(16)]
    for mid in ids:
        mailbox.add(mid, ["INBOX", "UNREAD"])

    async def main() -> None:
        r = runner(origin)
        await r.migrate()
        await r.track("alice@example.com")
        stops = await asyncio.gather(*(r.run(ACCOUNT, budget_s=30) for _ in range(6)))
        assert {o.stop for o in stops} <= {"idle", "superseded"}
        assert (await r.run(ACCOUNT, budget_s=30)).stop == "idle"
        results = await asyncio.gather(
            *(r.act(ACCOUNT, "mark_read", message_ids=[mid]) for mid in ids[:8]),
            *(r.messages(ACCOUNT, ids) for _ in range(8)),
            *(r.read_blob(ACCOUNT, mid) for mid in ids),
        )
        assert all(isinstance(blob, bytes) for blob in results[16:])
        for _ in range(5):
            assert (await r.run(ACCOUNT, budget_s=30)).stop == "idle"
            views = {v.id: v for v in await r.messages(ACCOUNT, ids)}
            if all(v.pending_actions == 0 for v in views.values()):
                break
        for mid in ids[:8]:
            assert views[mid].labels == ["INBOX"], mid
            assert mailbox.messages[mid]["labels"] == ["INBOX"], mid
        if sysconfig.get_config_var("Py_GIL_DISABLED"):
            assert not sys._is_gil_enabled()

    asyncio.run(main())
