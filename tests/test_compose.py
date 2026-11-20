import re

import mailtide
import pytest
from mailtide import Address, Attachment, OutgoingMessage

PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(256))


def message(**fields: object) -> OutgoingMessage:
    defaults: dict[str, object] = {
        "to": [Address("bob@example.org", "Bob")],
        "subject": "Fleet policy",
        "text": "Hello Bob\r\n",
    }
    defaults.update(fields)
    return OutgoingMessage(Address("ann@example.com", "Ann Lee"), **defaults)


def test_compose_then_parse_round_trips() -> None:
    composed = mailtide.compose_message(
        message(
            html='<p>Hello <img src="cid:logo"></p>',
            inline=[Attachment("image/png", PNG, filename="logo.png", content_id="logo")],
            attachments=[Attachment("application/pdf", b"%PDF-1.7", filename="quote.pdf")],
        )
    )
    parsed = mailtide.parse_message(composed.raw)
    assert parsed.text == "Hello Bob\r\n"
    assert parsed.html == '<p>Hello <img src="cid:logo"></p>'
    image, pdf = parsed.attachments
    assert (image.content_type, image.content_id, image.data) == ("image/png", "logo", PNG)
    assert (pdf.filename, pdf.content_type, pdf.data) == (
        "quote.pdf",
        "application/pdf",
        b"%PDF-1.7",
    )
    assert composed.recipients == ["bob@example.org"]


def test_compose_mints_a_message_id_on_the_senders_domain() -> None:
    first = mailtide.compose_message(message())
    second = mailtide.compose_message(message())
    assert first.message_id is not None and second.message_id is not None
    assert first.message_id != second.message_id
    assert re.fullmatch(r"<[0-9a-f]{32}@example\.com>", first.message_id)
    assert f"\r\nMessage-ID: {first.message_id}\r\n".encode() in first.raw
    assert first.tracking_id is not None and second.tracking_id is not None
    assert first.tracking_id != second.tracking_id
    assert re.fullmatch(r"[A-Za-z0-9_-]{22}", first.tracking_id)
    assert mailtide.tracking_id(first.raw) == first.tracking_id


def test_a_fixed_message_id_and_date_give_identical_bytes() -> None:
    fixed = message(
        message_id=mailtide.new_message_id("ann@example.com"),
        tracking_id=mailtide.new_tracking_id(),
        sent_at_ms=1_795_165_200_000,
    )
    saved = mailtide.compose_message(fixed, purpose="draft")
    again = mailtide.compose_message(fixed, purpose="draft")
    assert saved.raw == again.raw
    assert b"\r\nDate: Fri, 20 Nov 2026 09:00:00 +0000\r\n" in saved.raw
    assert saved.message_id == fixed.message_id
    assert saved.tracking_id == fixed.tracking_id


def test_invalid_input_raises_compose_error_naming_the_field() -> None:
    secret = "secret-7f3a"
    with pytest.raises(mailtide.ComposeError) as raised:
        mailtide.compose_message(message(cc=["ok@example.org", f"{secret} <x@example.org>"]))
    error = raised.value
    assert isinstance(error, ValueError)
    assert (error.field, error.index) == ("cc", 1)
    assert "display name" in error.reason
    assert secret not in str(error)

    with pytest.raises(mailtide.ComposeError) as raised:
        mailtide.compose_message(message(subject=f"{secret}\r\nBcc: eve@example.org"))
    assert (raised.value.field, raised.value.index) == ("subject", None)
    assert secret not in str(raised.value)

    with pytest.raises(mailtide.ComposeError) as raised:
        mailtide.compose_message(message(tracking_id="short"))
    assert raised.value.field == "tracking_id"

    with pytest.raises(mailtide.ComposeError) as raised:
        mailtide.new_message_id("not an address")
    assert raised.value.field == "sender"


def test_a_draft_needs_no_recipient() -> None:
    draft = mailtide.compose_message(message(to=[]), purpose="draft")
    assert draft.recipients == []
    assert b"\r\nTo:" not in draft.raw
    with pytest.raises(mailtide.ComposeError) as raised:
        mailtide.compose_message(message(to=[]))
    assert (raised.value.field, raised.value.reason) == ("recipients", "is missing")
    with pytest.raises(ValueError, match="purpose"):
        mailtide.compose_message(message(), purpose="later")


def test_parsed_attachments_can_be_composed_again() -> None:
    original = mailtide.compose_message(
        message(attachments=[Attachment("text/csv", b"a,b\r\n1,2\r\n", filename="rows.csv")])
    )
    parsed = mailtide.parse_message(original.raw)
    forwarded = mailtide.compose_message(message(attachments=parsed.attachments))
    (again,) = mailtide.parse_message(forwarded.raw).attachments
    assert (again.filename, again.content_type, again.data) == (
        "rows.csv",
        "text/csv",
        b"a,b\r\n1,2\r\n",
    )
    built = Attachment("application/octet-stream", b"\x00\xff")
    assert (built.filename, built.content_id, built.data) == (None, None, b"\x00\xff")


def test_parse_addresses_bridges_mirror_headers() -> None:
    # As MessageContent.sender, .to and .cc hold them: header values as written.
    sender = '"Doe, Jane" <jane@example.org>'
    to = ["=?UTF-8?B?w4lsb2RpZQ==?= <elodie@example.org>", "bob@example.org"]
    cc = ["Team: carol@example.org, Dan <dan@example.org>;"]
    assert mailtide.parse_addresses(sender) == [Address("jane@example.org", "Doe, Jane")]
    recipients = [a for value in to for a in mailtide.parse_addresses(value)]
    copies = [a for value in cc for a in mailtide.parse_addresses(value)]
    assert recipients == [Address("elodie@example.org", "Élodie"), Address("bob@example.org")]
    assert [a.email for a in copies] == ["carol@example.org", "dan@example.org"]
    reply = mailtide.compose_message(
        message(
            to=mailtide.parse_addresses(sender),
            cc=recipients + copies,
            in_reply_to="<orig@example.org>",
            references=["<root@example.org>", "<orig@example.org>"],
        )
    )
    assert reply.recipients == [
        "jane@example.org",
        "elodie@example.org",
        "bob@example.org",
        "carol@example.org",
        "dan@example.org",
    ]
    assert b"\r\nReferences: <root@example.org> <orig@example.org>\r\n" in reply.raw
