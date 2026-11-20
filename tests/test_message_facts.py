"""Message facts: envelope fields and the attachment summary at import."""

import asyncio
import sqlite3
from pathlib import Path

import mailtide
import pytest
from conftest import make_config, tokens
from fake_gmail import Mailbox
from mailtide.config import Blobs, Config, Database, Http

ACCOUNT = "alice@example.com"

REPORT = (
    b"From: =?utf-8?Q?Ren=C3=A9?= <rene@example.org>\r\n"
    b"To: a@example.org, =?utf-8?Q?Andr=C3=A9?= <andre@example.org>\r\n"
    b"Bcc: b@example.org\r\n"
    b'Reply-To: "Desk, Help" <help@example.org>\r\n'
    b"Subject: =?utf-8?Q?Caf=C3=A9_report?=\r\n"
    b"Date: Tue, 06 Oct 2026 09:00:00 +0200\r\n"
    b"List-Id: Reports <reports.example.org>\r\n"
    b"List-Unsubscribe: <mailto:leave@example.org>, <https://example.org/u>\r\n"
    b"List-Unsubscribe-Post: List-Unsubscribe=One-Click\r\n"
    b"Precedence: Bulk\r\n"
    b"X-Mailtide-Id:\r\n k3Jd9_x-Q2mZ8pLw\r\n"
    b'Content-Type: multipart/mixed; boundary="m"\r\n'
    b"\r\n"
    b"--m\r\n"
    b'Content-Type: multipart/related; boundary="r"\r\n'
    b"\r\n"
    b"--r\r\n"
    b"Content-Type: text/html\r\n"
    b"\r\n"
    b'<p>See the chart</p><img src="cid:chart@x">\r\n'
    b"--r\r\n"
    b"Content-Type: image/png\r\n"
    b"Content-ID: <chart@x>\r\n"
    b"Content-Transfer-Encoding: base64\r\n"
    b"\r\n"
    b"iVBORw0KGgo=\r\n"
    b"--r--\r\n"
    b"--m\r\n"
    b'Content-Type: application/pdf; name="report.pdf"\r\n'
    b'Content-Disposition: attachment; filename="report.pdf"\r\n'
    b"Content-Transfer-Encoding: base64\r\n"
    b"\r\n"
    b"JVBERi0xLjQK\r\n"
    b"--m--\r\n"
)


def synced(mailbox: Mailbox, origin: str, config: Config | None = None) -> mailtide.BlockingRunner:
    mailbox.add("m1", ["INBOX"], raw=REPORT)
    mailbox.add("m2", ["INBOX"])
    runner = mailtide.BlockingRunner(config or make_config(origin), tokens)
    runner.migrate()
    runner.track(ACCOUNT)
    assert runner.run(ACCOUNT, budget_s=30).stop == "idle"
    return runner


def test_content_carries_the_new_facts_and_attachments(gmail: tuple[Mailbox, str]) -> None:
    runner = synced(*gmail)
    [view] = runner.messages(ACCOUNT, ["m1"])
    content = view.content
    assert content is not None
    assert content.facts_version == 1
    assert content.sender == "=?utf-8?Q?Ren=C3=A9?= <rene@example.org>", "as written"
    assert (content.sender_name, content.sender_email) == ("René", "rene@example.org")
    assert content.bcc == ["b@example.org"]
    assert content.reply_to == ['"Desk, Help" <help@example.org>']
    assert content.sent_at_ms == 1_791_270_000_000
    assert content.sent_at_offset_min == 120
    assert content.list_id == "reports.example.org"
    assert content.list_unsubscribe == ["mailto:leave@example.org", "https://example.org/u"]
    assert content.one_click_unsubscribe
    assert (content.auto_submitted, content.precedence) == (None, "bulk")
    assert content.tracking_id == "k3Jd9_x-Q2mZ8pLw"
    assert (content.attachment_count, content.inline_count, content.protocol_count) == (1, 1, 0)
    assert content.has_attachment is True
    assert content.attachments is not None
    image, pdf = content.attachments
    assert isinstance(pdf, mailtide.AttachmentInfo)
    assert (image.content_type, image.inline, image.content_id) == ("image/png", True, "chart@x")
    assert (pdf.filename, pdf.content_type, pdf.size, pdf.inline) == (
        "report.pdf",
        "application/pdf",
        9,
        False,
    )
    assert pdf.disposition == "attachment"
    assert pdf.part.endswith(f".b.9.{content.size}")
    [plain] = runner.messages(ACCOUNT, ["m2"])
    assert plain.content is not None
    assert plain.content.attachments == [] and plain.content.has_attachment is False


def test_import_entry_carries_the_facts(gmail: tuple[Mailbox, str]) -> None:
    runner = synced(*gmail)
    page = runner.changes(ACCOUNT)
    assert isinstance(page, mailtide.ChangePage)
    [entry] = [c for c in page.changes if isinstance(c, mailtide.MessageImported) and c.id == "m1"]
    [view] = runner.messages(ACCOUNT, ["m1"])
    assert entry.content is not None and view.content is not None
    assert entry.content.tracking_id == "k3Jd9_x-Q2mZ8pLw"
    fields = (
        "facts_version",
        "sender_name",
        "bcc",
        "sent_at_ms",
        "attachment_count",
        "tracking_id",
    )
    for field in fields:
        assert getattr(entry.content, field) == getattr(view.content, field), field
    assert entry.content.attachments is not None and view.content.attachments is not None
    assert [a.part for a in entry.content.attachments] == [a.part for a in view.content.attachments]


def test_content_from_before_shows_version_zero_and_no_list(
    gmail: tuple[Mailbox, str], tmp_path: Path
) -> None:
    mailbox, origin = gmail
    path = tmp_path / "mail.db"
    config = Config(
        Database(f"sqlite:{path}?mode=rwc"), Blobs("memory:///"), http=Http(gmail_origin=origin)
    )
    runner = synced(mailbox, origin, config)
    del runner
    # The row as a build from before message facts wrote it.
    with sqlite3.connect(path) as db:
        db.execute("UPDATE mailtide_messages SET content = json_remove(content, '$.facts')")
    runner = mailtide.BlockingRunner(config, tokens)
    [view] = runner.messages(ACCOUNT, ["m1"])
    content = view.content
    assert content is not None
    assert content.facts_version == 0
    assert content.attachments is None
    assert content.attachment_count is None and content.inline_count is None
    assert content.has_attachment is None, "unknown, not none"
    assert content.protocol_count is None and content.tracking_id is None
    assert content.subject == "=?utf-8?Q?Caf=C3=A9_report?="


def test_parse_addresses_and_decode_header(gmail: tuple[Mailbox, str]) -> None:
    runner = synced(*gmail)
    [view] = runner.messages(ACCOUNT, ["m1"])
    content = view.content
    assert content is not None
    def pairs(value: str) -> list[tuple[str | None, str]]:
        return [(a.name, a.email) for a in mailtide.parse_addresses(value)]

    to = [pair for value in content.to for pair in pairs(value)]
    assert to == [(None, "a@example.org"), ("André", "andre@example.org")]
    assert pairs(content.reply_to[0]) == [("Desk, Help", "help@example.org")]
    assert pairs("Team: a@x.org, b@x.org;, nobody") == [(None, "a@x.org"), (None, "b@x.org")]
    assert content.subject is not None
    assert mailtide.decode_header(content.subject) == "Café report"
    assert mailtide.decode_header("plain") == "plain"


def pdf_part(content: mailtide.MessageContent | None) -> str:
    assert content is not None and content.attachments is not None
    [pdf] = [a for a in content.attachments if a.content_type == "application/pdf"]
    return pdf.part


def test_read_part_returns_one_attachment(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    runner = synced(mailbox, origin)
    [view] = runner.messages(ACCOUNT, ["m1"])
    part = pdf_part(view.content)
    assert runner.read_part(ACCOUNT, "m1", part) == b"%PDF-1.4\n"

    async def main() -> bytes:
        r = mailtide.Runner(make_config(origin), tokens)
        await r.migrate()
        await r.track(ACCOUNT)
        await r.run(ACCOUNT, budget_s=30)
        [view] = await r.messages(ACCOUNT, ["m1"])
        parts = await r.parts(ACCOUNT, "m1")
        assert [p.role for p in parts] == ["html", "inline", "attached"]
        assert parts[2].part == pdf_part(view.content)
        return await r.read_part(ACCOUNT, "m1", pdf_part(view.content), max_bytes=9)

    assert asyncio.run(main()) == b"%PDF-1.4\n"


def test_part_errors_are_typed(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    runner = synced(mailbox, origin)
    [view] = runner.messages(ACCOUNT, ["m1"])
    part = pdf_part(view.content)
    with pytest.raises(mailtide.PartTooLarge, match="open_part"):
        runner.read_part(ACCOUNT, "m1", part, max_bytes=8)
    for bad in ("not a reference", "1.2.b.3", part + "0"):
        with pytest.raises(mailtide.PartNotFound):
            runner.read_part(ACCOUNT, "m1", bad)
    offset, length, encoding, size, total = part.split(".")
    stale = ".".join([offset, length, encoding, size, str(int(total) + 1)])
    with pytest.raises(mailtide.PartNotFound, match="another length|bytes"):
        runner.read_part(ACCOUNT, "m1", stale)
    with pytest.raises(mailtide.BlobNotFound):
        runner.read_part(ACCOUNT, "m-unknown", part)
    with pytest.raises(mailtide.BlobNotFound):
        runner.parts(ACCOUNT, "m-unknown")
    assert issubclass(mailtide.PartNotFound, mailtide.MailtideError)


def test_open_part_streams_async_and_blocking(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    runner = synced(mailbox, origin)
    [view] = runner.messages(ACCOUNT, ["m1"])
    part = pdf_part(view.content)
    with runner.open_part(ACCOUNT, "m1", part, chunk_size=4) as stream:
        assert isinstance(stream, mailtide.PartStream)
        assert (stream.size, stream.part) == (9, part)
        chunks = list(stream)
    assert chunks == [b"%PDF", b"-1.4", b"\n"]
    assert stream.closed

    async def main() -> list[bytes]:
        r = mailtide.Runner(make_config(origin), tokens)
        await r.migrate()
        await r.track(ACCOUNT)
        await r.run(ACCOUNT, budget_s=30)
        async with await r.open_part(ACCOUNT, "m1", part, chunk_size=5) as stream:
            return [chunk async for chunk in stream]

    assert asyncio.run(main()) == [b"%PDF-", b"1.4\n"]


def test_cancelling_a_part_stream_releases_it(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    runner = synced(mailbox, origin)
    [view] = runner.messages(ACCOUNT, ["m1"])
    part = pdf_part(view.content)
    with runner.open_part(ACCOUNT, "m1", part, chunk_size=1) as stream:
        for _ in stream:
            break
    assert stream.closed, "leaving the loop and its with drops the read"
    with pytest.raises(StopIteration):
        next(stream)

    async def main() -> mailtide.PartStream:
        r = mailtide.Runner(make_config(origin), tokens)
        await r.migrate()
        await r.track(ACCOUNT)
        await r.run(ACCOUNT, budget_s=30)
        stream = await r.open_part(ACCOUNT, "m1", part, chunk_size=1)
        started = asyncio.Event()

        async def consume() -> None:
            async with stream:
                async for _ in stream:
                    started.set()
                    await asyncio.sleep(60)

        task = asyncio.create_task(consume())
        await started.wait()
        assert not stream.closed
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return stream

    assert asyncio.run(main()).closed, "a cancelled task drops the read"


def test_refresh_facts_fills_old_rows_and_emits_facts_refreshed(
    gmail: tuple[Mailbox, str], tmp_path: Path
) -> None:
    mailbox, origin = gmail
    path = tmp_path / "mail.db"
    config = Config(
        Database(f"sqlite:{path}?mode=rwc"), Blobs("memory:///"), http=Http(gmail_origin=origin)
    )
    runner = synced(mailbox, origin, config)
    since = runner.changes(ACCOUNT)
    assert isinstance(since, mailtide.ChangePage)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE mailtide_messages SET content = json_remove(content, '$.facts')")
    [view] = runner.messages(ACCOUNT, ["m1"])
    assert view.content is not None and view.content.facts_version == 0
    requests = mailbox.requests

    done = runner.refresh_facts(ACCOUNT, limit=1)
    assert isinstance(done, mailtide.FactsRefresh)
    assert (done.examined, done.refreshed, done.unreadable, done.next) == (1, 1, [], "m1")
    done = runner.refresh_facts(ACCOUNT, after=done.next)
    assert (done.examined, done.refreshed, done.next) == (1, 1, None)
    assert mailbox.requests == requests, "no Gmail call"

    page = runner.changes(ACCOUNT, since=since.next)
    assert isinstance(page, mailtide.ChangePage)
    refreshed = [c for c in page.changes if isinstance(c, mailtide.FactsRefreshed)]
    assert [(c.kind, c.id) for c in refreshed] == [
        ("facts_refreshed", "m1"),
        ("facts_refreshed", "m2"),
    ]
    content = refreshed[0].content
    assert content is not None
    assert content.facts_version == 1 and content.attachment_count == 1
    assert content.tracking_id == "k3Jd9_x-Q2mZ8pLw"
    again = runner.refresh_facts(ACCOUNT)
    assert (again.examined, again.refreshed) == (2, 0)
