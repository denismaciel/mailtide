//! MIME for stored and outgoing messages (SPEC §5.5): [`parse`] reads a
//! raw message into headers, text, HTML and attachments; [`compose`] writes
//! one from structured input, deterministically. The driver stores
//! [`facts`] at import; [`parts`] lists every leaf part with its byte
//! range, and [`PartDecoder`] decodes one part in chunks. None of it does
//! any I/O, and the sync engine itself parses nothing.

mod address;
mod compose;
mod decode;
mod facts;
mod message_id;
mod parts;
mod respond;
mod tracking;

pub use address::{Address, parse_addresses};
pub use compose::{
    ComposeError, ComposeOptions, Composed, OutgoingMessage, Purpose, Reason, compose,
};
pub use decode::PartDecoder;
pub use facts::facts;
pub use message_id::MessageId;
pub use parts::{PartInfo, PartRole, parts};
pub use respond::{
    ForwardHeaders, ForwardPart, ForwardPlan, ReplyPlan, forward_plan, quote_plain, reply_plan,
};
pub use tracking::{
    TRACKING_HEADER, is_tracking_id, new_tracking_id, tracking_headers, tracking_id,
};

use mail_parser::{HeaderForm, MessageParser};
use mailtide::gmail::PartRef;

/// Nesting depth past which a container is kept as one leaf.
const MAX_DEPTH: usize = 32;
/// Parts past which the rest of a message is kept as one leaf.
const MAX_PARTS: usize = 2_000;

/// The parser every function here uses, so a part reference means the same
/// thing to all of them. Never fails on malformed input; past its limits a
/// container is kept as one part.
fn parser() -> MessageParser {
    MessageParser::new()
        .max_depth(MAX_DEPTH)
        .max_parts(MAX_PARTS)
}

/// A parsed message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedMessage {
    /// Header name/value pairs, in order.
    pub headers: Vec<(String, String)>,
    /// The plain-text body, if any.
    pub text: Option<String>,
    /// The HTML body, if any.
    pub html: Option<String>,
    /// Attachments.
    pub attachments: Vec<Attachment>,
    /// Where each attachment's body is in the raw message, in the same
    /// order, as [`parts`] and the attachment summary name it; `None`
    /// inside a transfer-encoded attached message. Apart from
    /// [`Attachment`], which [`compose`] takes as input too.
    pub attachment_parts: Vec<Option<PartRef>>,
}

/// An attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attachment {
    /// File name, if given.
    pub filename: Option<String>,
    /// Media type.
    pub content_type: String,
    /// Content-ID, for inline parts.
    pub content_id: Option<String>,
    /// Decoded bytes.
    pub bytes: Vec<u8>,
}

/// A message that couldn't be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MimeError(pub String);

/// Parse an RFC 822 message. Header values are unfolded but otherwise as
/// written (encoded words included); bodies are transfer-decoded and
/// converted to UTF-8.
pub fn parse(rfc822: &[u8]) -> Result<ParsedMessage, MimeError> {
    let message = parser()
        .parse(rfc822)
        .ok_or_else(|| MimeError("not an RFC 822 message".into()))?;
    let headers = message
        .headers()
        .iter()
        .map(|header| (header.raw_name().to_owned(), unfold(header.raw_value())))
        .collect();
    let attachments = message
        .attachments()
        .map(|part| Attachment {
            filename: part.attachment_name().map(str::to_owned),
            content_type: part.content_type().map_or_else(
                || "application/octet-stream".to_owned(),
                |ct| match ct.subtype() {
                    Some(sub) => format!("{}/{sub}", ct.ctype()),
                    None => ct.ctype().to_owned(),
                },
            ),
            content_id: part.content_id().map(str::to_owned),
            bytes: part.decoded().into_owned(),
        })
        .collect();
    let attachment_parts = message
        .attachments()
        .map(|part| parts::reference(&message, part))
        .collect();
    Ok(ParsedMessage {
        headers,
        text: message.body_text(0).map(std::borrow::Cow::into_owned),
        html: message.body_html(0).map(std::borrow::Cow::into_owned),
        attachments,
        attachment_parts,
    })
}

/// Decode the RFC 2047 encoded words in an unstructured header value (a
/// `Subject` as the header facts hold it), adjacent words joined.
#[must_use]
pub fn decode_header(value: &str) -> String {
    let parsed = HeaderForm::Text.parse(value.as_bytes());
    parsed
        .value()
        .as_text()
        .map_or_else(|| value.trim().to_owned(), str::to_owned)
}

/// A header value as written, with its folding undone.
fn unfold(raw: &[u8]) -> String {
    let text = String::from_utf8_lossy(raw);
    text.split(['\r', '\n'])
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    const MESSAGE: &[u8] = b"From: Ann <ann@example.com>\r\n\
Subject: Quote for\r\n  fleet policy\r\n\
Content-Type: multipart/mixed; boundary=\"b\"\r\n\
\r\n\
--b\r\n\
Content-Type: text/plain; charset=utf-8\r\n\
Content-Transfer-Encoding: quoted-printable\r\n\
\r\n\
Hello =E2=98=95\r\n\
--b\r\n\
Content-Type: application/pdf; name=\"quote.pdf\"\r\n\
Content-Disposition: attachment; filename=\"quote.pdf\"\r\n\
Content-Transfer-Encoding: base64\r\n\
\r\n\
JVBERi0=\r\n\
--b--\r\n";

    #[test]
    fn parses_headers_bodies_and_attachments() {
        let parsed = parse(MESSAGE).expect("parse");
        assert_eq!(
            parsed.headers[0],
            ("From".to_owned(), "Ann <ann@example.com>".to_owned())
        );
        assert_eq!(
            parsed.headers[1],
            ("Subject".to_owned(), "Quote for fleet policy".to_owned())
        );
        assert_eq!(
            parsed.text.as_deref().map(str::trim),
            Some("Hello \u{2615}")
        );
        assert_eq!(parsed.attachments.len(), 1);
        let pdf = &parsed.attachments[0];
        assert_eq!(pdf.filename.as_deref(), Some("quote.pdf"));
        assert_eq!(pdf.content_type, "application/pdf");
        assert_eq!(pdf.bytes, b"%PDF-");
    }
}
