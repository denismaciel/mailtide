//! Tests ported by name from himalaya's `src/shared/message/part.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615): its part
//! walk and its has-attachment rows, as `features/message-facts.toml` maps
//! them. A row's "counts" is our `attached`; "does not count" is a part that
//! is inline or not listed at all. Its quote-text cases map to `send`.

use std::fmt::Write as _;

use mailtide::gmail::{AttachmentFact, MessageFacts};
use mailtide_mime::{PartDecoder, PartInfo, PartRole, facts, parts, reply_plan};

const PDF_ATTACHMENT: &[u8] = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/plain\r\n\r\nhello\r\n--b\r\nContent-Type: application/pdf; name=\"a.pdf\"\r\nContent-Disposition: attachment; filename=\"a.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0xLjQK\r\n--b--\r\n";
const PDF_ALONE: &[u8] = b"Content-Type: application/pdf; name=\"a.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0xLjQK\r\n";
const RFC822: &[u8] = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/plain\r\n\r\nhello\r\n--b\r\nContent-Type: message/rfc822\r\n\r\nFrom: c@example.org\r\nSubject: inner\r\n\r\ninner\r\n--b--\r\n";
const PDF_INLINE: &[u8] = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/plain\r\n\r\nhello\r\n--b\r\nContent-Type: application/pdf; name=\"a.pdf\"\r\nContent-Disposition: inline; filename=\"a.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0xLjQK\r\n--b--\r\n";
const PNG_INLINE: &[u8] = b"Content-Type: multipart/related; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/html\r\n\r\n<img src=\"cid:i\">\r\n--b\r\nContent-Type: image/png\r\nContent-ID: <i>\r\nContent-Disposition: inline\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0xLjQK\r\n--b--\r\n";
const CSV_ATTACHMENT: &[u8] = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/plain\r\n\r\nhello\r\n--b\r\nContent-Type: text/csv; name=\"a.csv\"\r\nContent-Disposition: attachment; filename=\"a.csv\"\r\n\r\na,b\r\n--b--\r\n";
const PLAIN: &[u8] = b"Content-Type: text/plain\r\n\r\nhello\r\n";
const ALTERNATIVE: &[u8] = b"Content-Type: multipart/alternative; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/plain\r\n\r\nhello\r\n--b\r\nContent-Type: text/html\r\n\r\n<p>hello</p>\r\n--b--\r\n";
const PNG_BARE: &[u8] = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/plain\r\n\r\nhello\r\n--b\r\nContent-Type: image/png; name=\"a.png\"\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0xLjQK\r\n--b--\r\n";
const SIGNED_BARE: &[u8] = b"Content-Type: multipart/signed; protocol=\"application/pgp-signature\"; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/plain\r\n\r\nhello\r\n--b\r\nContent-Type: application/pgp-signature; name=\"signature.asc\"\r\n\r\nsig\r\n--b--\r\n";
const SIGNED_DISPOSED: &[u8] = b"Content-Type: multipart/signed; protocol=\"application/pkcs7-signature\"; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/plain\r\n\r\nhello\r\n--b\r\nContent-Type: application/pkcs7-signature; name=\"smime.p7s\"\r\nContent-Disposition: attachment; filename=\"smime.p7s\"\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0xLjQK\r\n--b--\r\n";
const SIGNED_WITH_PDF: &[u8] = b"Content-Type: multipart/signed; protocol=\"application/pgp-signature\"; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: multipart/mixed; boundary=\"c\"\r\n\r\n--c\r\nContent-Type: text/plain\r\n\r\nhello\r\n--c\r\nContent-Type: application/pdf; name=\"a.pdf\"\r\nContent-Disposition: attachment; filename=\"a.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0xLjQK\r\n--c--\r\n--b\r\nContent-Type: application/pgp-signature; name=\"signature.asc\"\r\n\r\nsig\r\n--b--\r\n";
const ENCRYPTED: &[u8] = b"Content-Type: multipart/encrypted; protocol=\"application/pgp-encrypted\"; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: application/pgp-encrypted\r\n\r\nVersion: 1\r\n--b\r\nContent-Type: application/octet-stream\r\n\r\ncipher\r\n--b--\r\n";
const PNG_WITH_CONTENT_ID: &[u8] = b"Content-Type: multipart/related; boundary=\"b\"\r\n\r\n--b\r\nContent-Type: text/html\r\n\r\n<img src=\"cid:i\">\r\n--b\r\nContent-Type: image/png\r\nContent-ID: <i>\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0xLjQK\r\n--b--\r\n";

/// (attached, inline) and the listed parts' types.
fn row(raw: &[u8]) -> (u32, u32, Vec<String>) {
    let facts: MessageFacts = facts(raw);
    let types = facts
        .parts
        .iter()
        .map(|part: &AttachmentFact| part.content_type.clone())
        .collect();
    (facts.attached, facts.inline, types)
}

/// A part's bytes, read from its range in `raw`.
fn bytes(raw: &[u8], info: &PartInfo) -> Vec<u8> {
    let part = info.part.expect("a part with a range");
    let start = usize::try_from(part.offset).expect("offset");
    let end = usize::try_from(part.offset + part.length).expect("end");
    let mut decoder = PartDecoder::new(&part);
    let mut out = Vec::new();
    decoder.feed(raw.get(start..end).expect("range"), &mut out);
    decoder.finish(&mut out);
    out
}

/// A message whose top-level part sits `levels` containers deep.
fn nested(levels: usize) -> Vec<u8> {
    let mut raw =
        String::from("From: Ann <ann@example.org>\r\nDate: Tue, 06 Oct 2026 09:00:00 +0200\r\n");
    for level in 1..levels {
        let _ = write!(
            raw,
            "Content-Type: multipart/mixed; boundary=\"b{level}\"\r\n\r\n--b{level}\r\n"
        );
    }
    raw.push_str("Content-Type: text/plain\r\n\r\ndeep\r\n");
    for level in (1..levels).rev() {
        let _ = write!(raw, "--b{level}--\r\n");
    }
    raw.into_bytes()
}

/// A `multipart/mixed` message holding `count` text parts.
fn wide(count: usize) -> Vec<u8> {
    let mut raw = String::from("Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n");
    for n in 0..count {
        let _ = write!(raw, "--b\r\nContent-Type: text/plain\r\n\r\npart {n}\r\n");
    }
    raw.push_str("--b--\r\n");
    raw.into_bytes()
}

#[test]
fn a_message_past_the_parse_limits_still_yields_facts() {
    let deep = nested(100);
    let facts = facts(&deep);
    assert_eq!(facts.sender_email.as_deref(), Some("ann@example.org"));
    assert!(facts.sent_at.is_some());
    assert_eq!((facts.attached, facts.inline), (1, 0));
    let cut = &facts.parts[0];
    assert_eq!(cut.content_type, "multipart/mixed");
    assert!(cut.part.range().end <= cut.part.total);
    assert!(
        String::from_utf8_lossy(&deep[usize::try_from(cut.part.offset).expect("offset")..])
            .contains("deep"),
        "the cut part holds the content that was not parsed"
    );

    let (attached, inline, types) = row(&wide(5_000));
    assert_eq!((attached, inline), (1, 0), "only the cut part is listed");
    assert_eq!(types, ["text/plain"]);
}

#[test]
fn a_part_range_decodes_to_bytes_in_the_parts_own_charset() {
    let raw = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\
        \r\n\
        --b\r\n\
        Content-Type: text/plain; charset=ISO-8859-1\r\n\
        Content-Disposition: attachment; filename=\"notes.txt\"\r\n\
        Content-Transfer-Encoding: quoted-printable\r\n\
        \r\n\
        caf=E9=\r\n au lait\r\n\
        --b\r\n\
        Content-Type: text/plain; charset=ISO-8859-1\r\n\
        Content-Disposition: attachment; filename=\"b64.txt\"\r\n\
        Content-Transfer-Encoding: base64\r\n\
        \r\n\
        Y2Fm6Q==\r\n\
        --b\r\n\
        Content-Type: text/plain\r\n\
        \r\n\
        as is\r\n\
        --b--\r\n";
    let parts = parts(raw);
    let bytes: Vec<Vec<u8>> = parts.iter().map(|info| bytes(raw, info)).collect();

    assert_eq!(bytes[0], b"caf\xe9 au lait");
    assert_eq!(bytes[1], b"caf\xe9");
    assert_eq!(bytes[2], b"as is");
    assert_eq!(parts[0].charset.as_deref(), Some("ISO-8859-1"));
}

#[test]
fn the_range_of_an_attached_message_is_that_message() {
    use base64::Engine as _;
    let inner = b"Subject: inner\r\nContent-Type: text/plain\r\n\r\ninner body";
    let mut raw = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\
        \r\n\
        --b\r\n\
        Content-Type: message/rfc822\r\n\
        \r\n"
        .to_vec();
    raw.extend_from_slice(inner);
    raw.extend_from_slice(
        b"\r\n--b\r\nContent-Type: message/rfc822\r\nContent-Transfer-Encoding: base64\r\n\r\n",
    );
    raw.extend_from_slice(
        base64::engine::general_purpose::STANDARD
            .encode(inner)
            .as_bytes(),
    );
    raw.extend_from_slice(b"\r\n--b--\r\n");

    let messages: Vec<PartInfo> = parts(&raw)
        .into_iter()
        .filter(|info| info.content_type == "message/rfc822")
        .collect();
    assert_eq!(messages.len(), 2);
    assert_eq!(bytes(&raw, &messages[0]), inner);
    assert_eq!(bytes(&raw, &messages[1]), inner);
}

#[test]
fn a_part_disposed_as_attachment_is_attached() {
    assert_eq!(row(PDF_ATTACHMENT), (1, 0, vec!["application/pdf".into()]));
}

#[test]
fn a_message_that_is_one_binary_part_has_one_attached_part() {
    let (attached, inline, types) = row(PDF_ALONE);
    assert_eq!(
        (attached, inline, types),
        (1, 0, vec!["application/pdf".into()])
    );
    assert_eq!(facts(PDF_ALONE).parts[0].part.size, 9);
}

#[test]
fn an_attached_message_without_disposition_is_attached() {
    assert_eq!(row(RFC822), (1, 0, vec!["message/rfc822".into()]));
}

#[test]
fn a_binary_part_disposed_inline_is_listed_as_inline() {
    assert_eq!(row(PDF_INLINE), (0, 1, vec!["application/pdf".into()]));
}

#[test]
fn an_image_disposed_inline_and_referenced_is_inline() {
    assert_eq!(row(PNG_INLINE), (0, 1, vec!["image/png".into()]));
    assert!(facts(PNG_INLINE).parts[0].referenced);
}

#[test]
fn a_text_part_disposed_as_attachment_is_attached() {
    assert_eq!(row(CSV_ATTACHMENT), (1, 0, vec!["text/csv".into()]));
}

#[test]
fn a_text_body_is_not_listed() {
    assert_eq!(row(PLAIN), (0, 0, vec![]));
}

#[test]
fn the_bodies_of_an_alternative_are_not_listed() {
    assert_eq!(row(ALTERNATIVE), (0, 0, vec![]));
}

#[test]
fn an_undeclared_image_referenced_from_the_html_is_inline() {
    assert_eq!(row(PNG_WITH_CONTENT_ID), (0, 1, vec!["image/png".into()]));
}

#[test]
fn an_undeclared_unreferenced_image_is_attached() {
    assert_eq!(row(PNG_BARE), (1, 0, vec!["image/png".into()]));
}

#[test]
fn a_signature_part_is_not_listed() {
    assert_eq!(row(SIGNED_BARE), (0, 0, vec![]));
    let roles: Vec<PartRole> = parts(SIGNED_BARE).iter().map(|info| info.role).collect();
    assert_eq!(roles, [PartRole::Text, PartRole::Protocol]);
}

#[test]
fn a_signature_part_disposed_as_attachment_is_not_listed() {
    assert_eq!(row(SIGNED_DISPOSED), (0, 0, vec![]));
}

#[test]
fn an_attachment_inside_a_signed_body_is_attached() {
    assert_eq!(row(SIGNED_WITH_PDF), (1, 0, vec!["application/pdf".into()]));
}

#[test]
fn the_parts_of_an_encrypted_message_are_not_listed() {
    assert_eq!(row(ENCRYPTED), (0, 0, vec![]));
    assert!(
        parts(ENCRYPTED)
            .iter()
            .all(|info| info.role == PartRole::Protocol)
    );
}

#[test]
fn an_html_only_original_offers_plain_text_to_quote() {
    let raw = b"Content-Type: text/html; charset=utf-8\r\n\r\n\
        <html><head><style>p { color: red }</style></head>\
        <body><p>Caf\xc3\xa9 &amp; tea</p><p>at <b>noon</b><br>sharp</p></body></html>";
    let plan = reply_plan(raw, &[], false).unwrap();
    let text = plan.quote_text.expect("plain text");
    let lines: Vec<_> = text.lines().filter(|line| !line.is_empty()).collect();
    assert_eq!(lines, ["Café & tea", "at noon", "sharp"]);
}

#[test]
fn flowed_text_is_unwrapped_before_it_is_offered_for_quoting() {
    for (charset, transfer, body) in [
        ("us-ascii", "8bit", "Some text \r\ngoes on."),
        ("utf-8", "8bit", "Some text \r\ngoes on."),
        ("iso-8859-1", "quoted-printable", "Caf=E9=20\r\ngoes on."),
    ] {
        let raw = format!(
            "Content-Type: text/plain; charset={charset}; format=flowed\r\n\
             Content-Transfer-Encoding: {transfer}\r\n\r\n{body}"
        );
        let plan = reply_plan(raw.as_bytes(), &[], false).unwrap();
        let expected = if charset == "iso-8859-1" {
            "Café goes on."
        } else {
            "Some text goes on."
        };
        assert_eq!(plan.quote_text.as_deref(), Some(expected));
    }
}
