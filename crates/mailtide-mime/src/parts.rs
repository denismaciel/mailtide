//! Every leaf part of a raw message, in document order, with its role and
//! the byte range of its body: one walk shared by [`crate::facts`] and
//! [`parts`], so a part reference means the same thing in both.

use std::collections::BTreeSet;

use mail_parser::{
    Encoding, Message, MessagePart, MessageRef, PartFlags, PartKind, PartRole as Role, Source,
};
use mailtide::gmail::{PartRef, TransferEncoding, renders_inline};

/// What a leaf part is to its message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PartRole {
    /// A text body part.
    Text,
    /// An HTML body part.
    Html,
    /// Listed and attached: offered as a download.
    Attached,
    /// Listed and inline: shown inside the message.
    Inline,
    /// An RFC 1847 signature or encryption part.
    Protocol,
}

/// One leaf part of a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartInfo {
    /// Where its body is in the raw message; `None` for a part inside a
    /// transfer-encoded attached message, which has no range there.
    pub part: Option<PartRef>,
    /// Its role in the message it belongs to.
    pub role: PartRole,
    /// The declared media type, lowercased; `application/octet-stream`
    /// when absent or unreadable.
    pub content_type: String,
    /// The decoded filename.
    pub filename: Option<String>,
    /// Its size after transfer decoding.
    pub size: u64,
    /// The `Content-ID`, without angle brackets.
    pub content_id: Option<String>,
    /// The `charset` parameter, as written.
    pub charset: Option<String>,
    /// The reference of the attached message (`message/rfc822` part) this
    /// part is in; `None` for parts of the top-level message.
    pub container: Option<PartRef>,
    /// How many attached messages this part is inside (0 for the
    /// top-level message's own parts).
    pub depth: u32,
}

/// Every leaf part of `raw` in document order, nested messages included.
/// Never fails: a message that does not parse has no parts.
#[must_use]
pub fn parts(raw: &[u8]) -> Vec<PartInfo> {
    let Some(message) = crate::parser().parse(raw) else {
        return Vec::new();
    };
    let mut all: Vec<Leaf<'_>> = message
        .messages()
        .flat_map(|owner| leaves(&message, owner))
        .collect();
    // Part ids number every part of every message in document order.
    all.sort_by_key(|leaf| leaf.part.id());
    all.into_iter()
        .map(|leaf| {
            let part = leaf.part;
            let container = part.message().container();
            PartInfo {
                part: leaf.reference,
                role: leaf.role,
                content_type: leaf.content_type,
                filename: part.attachment_name().map(str::to_owned),
                size: leaf.size,
                content_id: part.content_id().map(str::to_owned),
                charset: part
                    .content_type()
                    .and_then(|ct| ct.attribute("charset"))
                    .map(str::to_owned),
                container: container.and_then(|container| reference(&message, container)),
                depth: depth(part.message()),
            }
        })
        .collect()
}

/// A leaf of one message, classified.
pub(crate) struct Leaf<'m> {
    pub(crate) part: MessagePart<'m>,
    pub(crate) role: PartRole,
    pub(crate) reference: Option<PartRef>,
    pub(crate) content_type: String,
    pub(crate) size: u64,
    pub(crate) disposition: Option<String>,
    pub(crate) referenced: bool,
    pub(crate) related: bool,
}

/// The leaves of `owner` (its own parts, not those of messages attached to
/// it), in document order.
pub(crate) fn leaves<'m>(message: &'m Message<'_>, owner: MessageRef<'m>) -> Vec<Leaf<'m>> {
    let shown = cid_references(owner);
    owner
        .parts()
        .filter(is_leaf)
        .map(|part| {
            let content_type = media_type(part);
            let disposition = part
                .content_disposition()
                .map(|disposition| disposition.ctype().to_ascii_lowercase());
            let referenced = part.content_id().is_some_and(|id| shown.contains(id));
            let related = is_related_component(part);
            let role = if is_protocol(part) {
                PartRole::Protocol
            } else if is_listed(part) {
                if renders_inline(&content_type, disposition.as_deref(), referenced, related) {
                    PartRole::Inline
                } else {
                    PartRole::Attached
                }
            } else if matches!(part.kind(), PartKind::Html) {
                PartRole::Html
            } else {
                PartRole::Text
            };
            Leaf {
                part,
                role,
                reference: reference(message, part),
                content_type,
                size: decoded_size(part),
                disposition,
                referenced,
                related,
            }
        })
        .collect()
}

/// A leaf: anything but a multipart, except a multipart a parse limit left
/// unopened, which holds content nobody else reports.
fn is_leaf(part: &MessagePart<'_>) -> bool {
    !part.is_multipart() || part.flags().contains(PartFlags::LIMIT_REACHED)
}

/// Whether the summary lists the part: anything but a text or HTML body
/// part (RFC 8621's body lists, as mail-parser computes them), and always a
/// part a parse limit cut.
fn is_listed(part: MessagePart<'_>) -> bool {
    if part.flags().contains(PartFlags::LIMIT_REACHED) {
        return true;
    }
    let text = matches!(part.kind(), PartKind::Text | PartKind::Html);
    let body = matches!(
        part.role(),
        Role::TextBody | Role::HtmlBody | Role::CopiedBody | Role::Dropped
    );
    !(text && body)
}

/// An RFC 1847 protocol part, or a part inside one: a child other than the
/// first of `multipart/signed`, or any child of `multipart/encrypted`.
fn is_protocol(part: MessagePart<'_>) -> bool {
    let mut current = part;
    while let Some(parent) = current.parent() {
        if parent.is_content_type("multipart", "encrypted")
            || (parent.is_content_type("multipart", "signed") && !is_first_child(parent, current))
        {
            return true;
        }
        current = parent;
    }
    false
}

/// A child, other than the first, of `multipart/related` (RFC 2387: a
/// component the root refers to).
fn is_related_component(part: MessagePart<'_>) -> bool {
    part.parent().is_some_and(|parent| {
        parent.is_content_type("multipart", "related") && !is_first_child(parent, part)
    })
}

fn is_first_child(parent: MessagePart<'_>, child: MessagePart<'_>) -> bool {
    parent
        .children()
        .next()
        .is_some_and(|first| first.id() == child.id())
}

/// The declared type, lowercased; `application/octet-stream` when absent or
/// unreadable.
fn media_type(part: MessagePart<'_>) -> String {
    match part.content_type() {
        Some(ct) => match ct.subtype() {
            Some(subtype) if !ct.ctype().is_empty() && !subtype.is_empty() => {
                format!("{}/{subtype}", ct.ctype()).to_ascii_lowercase()
            }
            Some(_) | None => OCTET_STREAM.to_owned(),
        },
        None => OCTET_STREAM.to_owned(),
    }
}

const OCTET_STREAM: &str = "application/octet-stream";

/// The Content-IDs the message's HTML body parts show through `cid:` URLs
/// (RFC 2392: the id without angle brackets, percent-encoded).
fn cid_references(owner: MessageRef<'_>) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for part in owner.html_body() {
        if !matches!(part.kind(), PartKind::Html) {
            continue;
        }
        let Some(html) = part.text() else { continue };
        let bytes = html.as_bytes();
        let mut at = 0;
        while let Some(found) = find_cid(bytes.get(at..).unwrap_or_default()) {
            let start = at + found + 4;
            let rest = bytes.get(start..).unwrap_or_default();
            let end = rest
                .iter()
                .position(|byte| {
                    byte.is_ascii_whitespace()
                        || matches!(byte, b'"' | b'\'' | b'<' | b'>' | b'(' | b')' | b'\\')
                })
                .unwrap_or(rest.len());
            let id = percent_decode(rest.get(..end).unwrap_or_default());
            if !id.is_empty() {
                ids.insert(id);
            }
            at = start + end;
        }
    }
    ids
}

/// Where `cid:` (any case) starts in `bytes`.
fn find_cid(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window.eq_ignore_ascii_case(b"cid:"))
}

fn percent_decode(bytes: &[u8]) -> String {
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        let escaped = (byte == b'%')
            .then(|| bytes.get(index + 1..index + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        if let Some(decoded) = escaped {
            out.push(decoded);
            index += 3;
        } else {
            out.push(byte);
            index += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The reference of a part whose body is in the raw message.
pub(crate) fn reference(message: &Message<'_>, part: MessagePart<'_>) -> Option<PartRef> {
    if part.message().source() != Source::Raw {
        return None;
    }
    let body = part.raw_body();
    let (encoding, irregular) = match part.encoding() {
        Encoding::None => (TransferEncoding::Identity, false),
        Encoding::QuotedPrintable => (
            TransferEncoding::QuotedPrintable,
            encodify::qp::BODY.strict().decoded_len(body).is_err(),
        ),
        Encoding::Base64 => (
            TransferEncoding::Base64,
            encodify::base64::MIME.decoded_len(body).is_err(),
        ),
    };
    Some(PartRef {
        offset: u64::from(part.offset_body()),
        length: u64::try_from(body.len()).ok()?,
        encoding,
        irregular,
        size: decoded_size(part),
        total: u64::try_from(message.raw().len()).ok()?,
    })
}

fn decoded_size(part: MessagePart<'_>) -> u64 {
    u64::try_from(part.decoded_len()).unwrap_or(u64::MAX)
}

/// How many attached messages `owner` is inside.
fn depth(owner: MessageRef<'_>) -> u32 {
    let mut depth = 0;
    let mut current = owner;
    while let Some(container) = current.container() {
        depth += 1;
        current = container.message();
    }
    depth
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PartDecoder;

    #[test]
    fn a_part_reference_round_trips_through_its_text_form() {
        let example = PartRef {
            offset: 1042,
            length: 6920,
            encoding: TransferEncoding::Base64,
            irregular: false,
            size: 5_110,
            total: 48_211,
        };
        assert_eq!(example.to_string(), "1042.6920.b.5110.48211");
        for (encoding, code) in [
            (TransferEncoding::Identity, 'n'),
            (TransferEncoding::QuotedPrintable, 'q'),
            (TransferEncoding::Base64, 'b'),
        ] {
            for irregular in [false, true] {
                let part = PartRef {
                    offset: 7,
                    length: 0,
                    encoding,
                    irregular,
                    size: 0,
                    total: 7,
                };
                let text = part.to_string();
                let expected = if irregular {
                    code.to_ascii_uppercase()
                } else {
                    code
                };
                assert_eq!(text, format!("7.0.{expected}.0.7"));
                assert_eq!(PartRef::parse(&text), Ok(part));
                assert_eq!(text.parse::<PartRef>(), Ok(part));
            }
        }
        let json = serde_json::to_string(&example).expect("serialises");
        assert_eq!(json, "\"1042.6920.b.5110.48211\"");
        assert_eq!(serde_json::from_str::<PartRef>(&json).ok(), Some(example));
        for bad in [
            "",
            "1042.6920.b.5110",
            "1042.6920.b.48211",
            "1042.6920.b.5110.48211.1",
            "1042.6920.x.5110.48211",
            "1042.6920.bb.5110.48211",
            "01042.6920.b.5110.48211",
            "1042.6920.b.05110.48211",
            "+1042.6920.b.5110.48211",
            "-1.6920.b.5110.48211",
            " 1042.6920.b.5110.48211",
            "1042.6920.b.5110.48211\n",
            "48000.6920.b.5110.48211",
            "1042.6920.b.6921.48211",
            "18446744073709551615.1.n.1.18446744073709551615",
            "a.b.c.d.e",
        ] {
            assert!(PartRef::parse(bad).is_err(), "{bad:?}");
            assert!(
                serde_json::from_str::<PartRef>(&format!("{bad:?}")).is_err(),
                "{bad:?}"
            );
        }
    }

    fn fixtures() -> Vec<std::path::PathBuf> {
        let root =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mail-parser");
        let mut dirs = vec![root];
        let mut files = Vec::new();
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir)
                .expect("fixture directory")
                .flatten()
            {
                let path = entry.path();
                if path.is_dir() {
                    dirs.push(path);
                } else if path.extension().is_some_and(|ext| ext == "eml") {
                    files.push(path);
                }
            }
        }
        files.sort();
        files
    }

    fn decode_in_pieces(part: &PartRef, body: &[u8], piece: usize) -> Vec<u8> {
        let mut decoder = PartDecoder::new(part);
        let mut out = Vec::new();
        for chunk in body.chunks(piece.max(1)) {
            decoder.feed(chunk, &mut out);
        }
        decoder.finish(&mut out);
        out
    }

    #[test]
    fn every_fixture_range_decodes_to_what_the_parser_decodes() {
        let files = fixtures();
        assert!(files.len() > 150, "the corpus is there");
        let mut ranges = 0;
        for file in files {
            let raw = std::fs::read(&file).expect("fixture");
            let Some(message) = crate::parser().parse(&raw) else {
                continue;
            };
            for part in message.parts() {
                let Some(reference) = reference(&message, part) else {
                    assert_ne!(part.message().source(), Source::Raw);
                    continue;
                };
                let range = usize::try_from(reference.offset).expect("offset")
                    ..usize::try_from(reference.offset + reference.length).expect("end");
                let body = raw.get(range).expect("range inside the blob");
                assert_eq!(
                    body,
                    part.raw_body(),
                    "{} part {}",
                    file.display(),
                    part.id()
                );
                let expected = part.decoded();
                for piece in [1, 3, 64, body.len()] {
                    assert_eq!(
                        decode_in_pieces(&reference, body, piece),
                        expected.as_ref(),
                        "{} part {} in pieces of {piece}",
                        file.display(),
                        part.id()
                    );
                }
                assert_eq!(expected.len(), part.decoded_len(), "{}", file.display());
                ranges += 1;
            }
            for info in parts(&raw) {
                if let Some(reference) = info.part {
                    let start = usize::try_from(reference.offset).expect("offset");
                    let end = usize::try_from(reference.offset + reference.length).expect("end");
                    let body = raw.get(start..end).expect("range inside the blob");
                    let decoded = decode_in_pieces(&reference, body, 5);
                    assert_eq!(
                        u64::try_from(decoded.len()).ok(),
                        Some(info.size),
                        "{}",
                        file.display()
                    );
                }
            }
        }
        assert!(ranges > 500, "{ranges} ranges checked");
    }

    #[test]
    fn roles_cover_text_html_attached_inline_and_protocol() {
        let raw = b"Content-Type: multipart/mixed; boundary=\"m\"\r\n\r\n\
            --m\r\nContent-Type: multipart/signed; protocol=\"application/pgp-signature\"; boundary=\"s\"\r\n\r\n\
            --s\r\nContent-Type: multipart/related; boundary=\"r\"\r\n\r\n\
            --r\r\nContent-Type: multipart/alternative; boundary=\"a\"\r\n\r\n\
            --a\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nhello\r\n\
            --a\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<p>hello</p><img src=\"cid:logo@x\">\r\n\
            --a--\r\n\
            --r\r\nContent-Type: image/png\r\nContent-ID: <logo@x>\r\nContent-Transfer-Encoding: base64\r\n\r\niVBORw0KGgo=\r\n\
            --r--\r\n\
            --s\r\nContent-Type: application/pgp-signature; name=\"signature.asc\"\r\n\r\nsig\r\n\
            --s--\r\n\
            --m\r\nContent-Type: application/pdf; name=\"a.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0=\r\n\
            --m\r\nContent-Type: message/rfc822\r\n\r\n\
            Subject: inner\r\nContent-Type: multipart/mixed; boundary=\"i\"\r\n\r\n\
            --i\r\nContent-Type: text/plain\r\n\r\ninner\r\n\
            --i\r\nContent-Type: image/gif; name=\"g.gif\"\r\nContent-Disposition: attachment\r\n\r\nGIF\r\n\
            --i--\r\n\
            --m--\r\n";
        let parts = parts(raw);
        let summary: Vec<(PartRole, &str, u32)> = parts
            .iter()
            .map(|part| (part.role, part.content_type.as_str(), part.depth))
            .collect();
        assert_eq!(
            summary,
            [
                (PartRole::Text, "text/plain", 0),
                (PartRole::Html, "text/html", 0),
                (PartRole::Inline, "image/png", 0),
                (PartRole::Protocol, "application/pgp-signature", 0),
                (PartRole::Attached, "application/pdf", 0),
                (PartRole::Attached, "message/rfc822", 0),
                (PartRole::Text, "text/plain", 1),
                (PartRole::Attached, "image/gif", 1),
            ]
        );
        assert!(parts.iter().all(|part| part.part.is_some()));
        let message = parts[5].part;
        assert_eq!(parts[6].container, message);
        assert_eq!(parts[7].container, message);
        assert!(parts[..6].iter().all(|part| part.container.is_none()));
        assert_eq!(parts[2].content_id.as_deref(), Some("logo@x"));
        assert_eq!(parts[0].charset.as_deref(), Some("utf-8"));
        assert_eq!(parts[4].filename.as_deref(), Some("a.pdf"));
        assert_eq!((parts[2].size, parts[4].size), (8, 5));
    }
}
