"""Logging reaches the host's logger with structured fields and no addresses;
progress reaches the caller's event loop and context."""

import asyncio
import contextvars
import dataclasses
import logging
import threading
import time

import mailtide
from conftest import make_config, tokens
from fake_gmail import Mailbox
from mailtide.config import Log

ACCOUNT = "alice@example.com"


class Collect(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def wait_for(predicate, timeout_s: float = 3.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline and not predicate():
        time.sleep(0.02)


def test_events_reach_the_host_logger_without_addresses(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"])
    handler = Collect()
    logger = logging.getLogger("mailtide.test")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    config = dataclasses.replace(
        make_config(origin), log=Log(level="debug", logger="mailtide.test")
    )
    runner = mailtide.BlockingRunner(config, tokens)
    runner.migrate()
    runner.run(ACCOUNT, budget_s=30)
    wait_for(lambda: any(r.getMessage() == "run finished" for r in handler.records))

    finished = next(r for r in handler.records if r.getMessage() == "run finished")
    assert finished.levelno == logging.INFO
    fields = finished.mailtide
    assert fields["stop"] == "idle" and fields["commits"] >= 1 and fields["imported"] == 1
    assert isinstance(fields["duration_ms"], int)
    assert len(fields["account"]) == 16 and "@" not in fields["account"]
    requests = [r for r in handler.records if r.getMessage() == "gmail request"]
    assert {r.mailtide["request"] for r in requests} >= {"list_labels", "import_message"}
    for record in handler.records:
        assert "alice" not in record.getMessage() + repr(getattr(record, "mailtide", {}))


def test_off_logs_nothing(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX"])
    handler = Collect()
    logger = logging.getLogger("mailtide.quiet")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    config = dataclasses.replace(make_config(origin), log=Log(level="off", logger="mailtide.quiet"))
    runner = mailtide.BlockingRunner(config, tokens)
    runner.migrate()
    runner.run(ACCOUNT, budget_s=30)
    time.sleep(0.3)
    assert handler.records == []


def test_progress_reaches_the_callers_loop_and_context(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    for i in range(6):
        mailbox.add(f"m{i}", ["INBOX"])
    mailbox.delay_s = 0.25  # a slow Gmail: the run lasts a few seconds
    activity = contextvars.ContextVar("activity", default=None)
    seen: list[tuple[int, object, bool]] = []

    async def main() -> None:
        loop_thread = threading.get_ident()
        activity.set("heartbeat-me")

        def on_progress(progress: mailtide.Progress) -> None:
            seen.append(
                (progress.gmail_requests, activity.get(), threading.get_ident() == loop_thread)
            )

        runner = mailtide.Runner(make_config(origin), tokens)
        await runner.migrate()
        outcome = await runner.run(ACCOUNT, budget_s=60, on_progress=on_progress)
        assert outcome.stop == "idle"
        await asyncio.sleep(0)  # let the last scheduled callbacks run

    asyncio.run(main())
    assert seen, "progress was reported"
    assert all(context == "heartbeat-me" and on_loop for _, context, on_loop in seen)
    assert [n for n, _, _ in seen] == sorted(n for n, _, _ in seen)
