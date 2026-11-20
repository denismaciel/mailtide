//! Composition: an RFC 5322 message from structured input (SPEC §5.5).

use std::borrow::Cow;
use std::fmt;

use encodify::base64::MIME;
use mail_builder::MessageBuilder;
use mail_builder::headers::address::Address as HeaderAddress;
use mail_builder::headers::content_type::ContentType;
use mail_builder::headers::date::Date;
use mail_builder::headers::message_id::MessageId as HeaderId;
use mail_builder::headers::text::Text;
use mail_builder::headers::{Header, HeaderType};
use mail_builder::mime::{BodyPart, MimePart};

use crate::tracking::{TRACKING_HEADER, is_tracking_id};
use crate::{Address, Attachment, MessageId};

/// Why an input was refused. Never carries the offending value, so a host
/// can log it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reason {
    /// A required value is absent or empty.
    Missing,
    /// A control character (CR, LF, NUL, ...): the header-injection rule.
    ControlCharacter,
    /// Non-ASCII where only ASCII is written.
    NotAscii,
    /// A display name and an address given together as the address.
    NameInAddress,
    /// White space inside an address or id.
    Whitespace,
    /// An address without `@`.
    NoAt,
    /// An address with nothing before the `@`.
    EmptyLocalPart,
    /// An address with nothing after the `@`.
    EmptyDomain,
    /// A quoted local part (`"a b"@x`), which compose does not write.
    QuotedLocalPart,
    /// A domain literal (`a@[192.0.2.1]`), which compose does not write.
    DomainLiteral,
    /// A local part that is not a dot-atom.
    InvalidLocalPart,
    /// A domain that is not a host name.
    InvalidDomain,
    /// Longer than its limit.
    TooLong,
    /// An id that is not one token.
    InvalidId,
    /// A message's own id that is not `left@right` in dot-atom form.
    NotDotAtom,
    /// A tracking id that is not 16 to 64 characters of `A-Za-z0-9_-`.
    InvalidTrackingId,
    /// Given twice where it must be unique.
    Duplicate,
    /// A header compose writes itself, or a `Content-*` header.
    ReservedHeader,
    /// A header name outside printable ASCII, or with a colon.
    InvalidHeaderName,
    /// A media type that is not `type/subtype`.
    InvalidContentType,
    /// A `multipart/*` media type for a part.
    Multipart,
    /// An inline part in a message without an HTML body.
    NoHtml,
    /// The message is larger than `max_bytes`.
    TooLarge {
        /// The size found: the parts' sum, or the written bytes.
        size: usize,
        /// The limit given.
        limit: usize,
    },
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Missing => "is missing",
            Self::ControlCharacter => "contains a control character",
            Self::NotAscii => "is not ASCII",
            Self::NameInAddress => "holds a display name; give the name separately",
            Self::Whitespace => "contains white space",
            Self::NoAt => "has no @",
            Self::EmptyLocalPart => "has no local part",
            Self::EmptyDomain => "has no domain",
            Self::QuotedLocalPart => "has a quoted local part",
            Self::DomainLiteral => "has a domain literal",
            Self::InvalidLocalPart => "has a local part that is not a dot-atom",
            Self::InvalidDomain => "has a domain that is not a host name",
            Self::TooLong => "is too long",
            Self::InvalidId => "is not one id",
            Self::NotDotAtom => "is not left@right in dot-atom form",
            Self::InvalidTrackingId => "is not 16 to 64 characters of A-Za-z0-9_-",
            Self::Duplicate => "is given twice",
            Self::ReservedHeader => "is a header compose writes itself",
            Self::InvalidHeaderName => "is not a header name",
            Self::InvalidContentType => "is not a type/subtype",
            Self::Multipart => "is a multipart type",
            Self::NoHtml => "needs an HTML body",
            Self::TooLarge { size, limit } => {
                return write!(f, "is {size} bytes, over the limit of {limit}");
            }
        };
        f.write_str(text)
    }
}

/// An input [`compose`] refused: which field, which entry of a list, and
/// why. Its `Display` holds no address, subject or body text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeError {
    /// The field of [`OutgoingMessage`], or `recipients` or `message`.
    pub field: &'static str,
    /// The entry of a list field.
    pub index: Option<usize>,
    /// Why.
    pub reason: Reason,
}

impl fmt::Display for ComposeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.index {
            Some(index) => write!(f, "{}[{index}] {}", self.field, self.reason),
            None => write!(f, "{} {}", self.field, self.reason),
        }
    }
}

impl std::error::Error for ComposeError {}

/// A message to write, shaped like a JMAP Email creation (RFC 8621 §4.6):
/// flat fields from which [`compose`] builds one fixed MIME tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutgoingMessage {
    /// The author; always required, also for a draft.
    pub from: Address,
    /// The sender, when another than the author.
    pub sender: Option<Address>,
    /// Where replies go.
    pub reply_to: Vec<Address>,
    /// Primary recipients.
    pub to: Vec<Address>,
    /// Copied recipients.
    pub cc: Vec<Address>,
    /// Blind-copied recipients, written as a header: Gmail's send has no
    /// envelope and reads them from it.
    pub bcc: Vec<Address>,
    /// The subject; none writes no `Subject`.
    pub subject: Option<String>,
    /// Unix seconds, written as `Date` in UTC. Gmail replaces it with the
    /// time of each draft save and of the send.
    pub sent_at: i64,
    /// The message's own id, written as given when present. Gmail replaces
    /// it on every send and draft save, so it does not identify the message
    /// there: `tracking_id` does.
    pub message_id: Option<MessageId>,
    /// The private identity written as `X-Mailtide-Id`, which Gmail keeps
    /// through saves and the send: 16 to 64 characters of `A-Za-z0-9_-`
    /// (see [`new_tracking_id`](crate::new_tracking_id)).
    pub tracking_id: Option<String>,
    /// The id of the message this replies to.
    pub in_reply_to: Option<MessageId>,
    /// The thread's ids, oldest first.
    pub references: Vec<MessageId>,
    /// Custom headers, written in order after the ones compose writes.
    pub headers: Vec<(String, String)>,
    /// The plain-text body.
    pub text: Option<String>,
    /// The HTML body.
    pub html: Option<String>,
    /// Parts the HTML refers to by `cid:`; each needs a `content_id`.
    pub inline: Vec<Attachment>,
    /// Attachments, in order. Their `content_id` is not written.
    pub attachments: Vec<Attachment>,
}

impl OutgoingMessage {
    /// A message from `from`, written at `sent_at`, with every other field
    /// empty.
    #[must_use]
    pub fn new(from: Address, sent_at: i64) -> Self {
        Self {
            from,
            sender: None,
            reply_to: Vec::new(),
            to: Vec::new(),
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: None,
            sent_at,
            message_id: None,
            tracking_id: None,
            in_reply_to: None,
            references: Vec::new(),
            headers: Vec::new(),
            text: None,
            html: None,
            inline: Vec::new(),
            attachments: Vec::new(),
        }
    }
}

/// What the message is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Purpose {
    /// A draft: recipients may be missing.
    Draft,
    /// A send: at least one of To, Cc and Bcc.
    #[default]
    Send,
}

/// How [`compose`] checks the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ComposeOptions {
    /// Draft or send.
    pub purpose: Purpose,
    /// The largest message accepted, in bytes as written.
    pub max_bytes: Option<usize>,
}

/// A written message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Composed {
    /// The RFC 5322 bytes, CRLF line ends.
    pub raw: Vec<u8>,
    /// The Message-ID written, in angle brackets. Gmail replaces it, so the
    /// mirror's `rfc822_message_id` differs when the message went through
    /// Gmail.
    pub message_id: Option<String>,
    /// The `X-Mailtide-Id` written: what identifies the message after Gmail
    /// saved or sent it.
    pub tracking_id: Option<String>,
    /// To, Cc and Bcc addresses in order, each once (ignoring ASCII case).
    pub recipients: Vec<String>,
}

/// Headers compose writes itself; a custom header may not be one of them.
const RESERVED: [&str; 13] = [
    "From",
    "Sender",
    "Reply-To",
    "To",
    "Cc",
    "Bcc",
    "Subject",
    "Date",
    "Message-ID",
    "In-Reply-To",
    "References",
    "MIME-Version",
    TRACKING_HEADER,
];
/// Longest custom header name, so `Name: ` leaves room on a 78-column line.
const MAX_HEADER_NAME: usize = 76;
/// Longest line of an attached message written unencoded (RFC 5322 2.1.1).
const MAX_LINE: usize = 998;

/// Write `message` as RFC 5322 bytes.
///
/// Pure and deterministic: no clock, host name or random source is read.
/// The date and the ids are inputs and each multipart boundary is derived
/// from them, so the same input always gives the same bytes. The input is
/// checked first and refused, never repaired; only line ends in the bodies
/// become CRLF, and a message of one part ends with one.
///
/// The tree, each level present only when it has more than one child:
///
/// ```text
/// multipart/mixed
/// ├─ multipart/alternative
/// │  ├─ text/plain
/// │  └─ multipart/related; type="text/html"
/// │     ├─ text/html
/// │     └─ inline parts
/// └─ attachments
/// ```
///
/// With neither text nor HTML an empty text part stands in.
pub fn compose(
    message: &OutgoingMessage,
    options: &ComposeOptions,
) -> Result<Composed, ComposeError> {
    let content_ids = validate(message, options.purpose)?;
    if let Some(limit) = options.max_bytes {
        let size = part_sizes(message);
        if size > limit {
            return Err(too_large(size, limit));
        }
    }

    let text = message.text.as_deref().map(crlf_text);
    let html = message.html.as_deref().map(crlf_text);
    let inline: Vec<Leaf<'_>> = message
        .inline
        .iter()
        .zip(&content_ids)
        .map(|(part, id)| Leaf::new(part, Disposition::Inline(id.as_str())))
        .collect();
    let attachments: Vec<Leaf<'_>> = message
        .attachments
        .iter()
        .map(|part| Leaf::new(part, Disposition::Attachment))
        .collect();

    let [mixed, alternative, related] = {
        let unencoded: Vec<&[u8]> = text
            .iter()
            .chain(&html)
            .map(|body| body.as_bytes())
            .chain(
                inline
                    .iter()
                    .chain(&attachments)
                    .filter_map(Leaf::unencoded),
            )
            .collect();
        let used = [
            !attachments.is_empty(),
            text.is_some() && html.is_some(),
            !inline.is_empty(),
        ];
        [MIXED, ALTERNATIVE, RELATED]
            .map(|level| used[usize::from(level)].then(|| boundary(message, level, &unencoded)))
    };

    let html = html.map(|html| {
        let html = MimePart::new("text/html", BodyPart::Text(html));
        match related {
            Some(boundary) => {
                let content_type = ContentType::new("multipart/related")
                    .attribute("type", "text/html")
                    .attribute("boundary", boundary);
                let mut parts = vec![html];
                parts.extend(inline.into_iter().map(Leaf::into_part));
                MimePart::new(content_type, parts)
            }
            None => html,
        }
    });
    let text = text.map(|text| MimePart::new("text/plain", BodyPart::Text(text)));
    let body = match (text, html, alternative) {
        (Some(text), Some(html), Some(boundary)) => {
            let content_type =
                ContentType::new("multipart/alternative").attribute("boundary", boundary);
            MimePart::new(content_type, vec![text, html])
        }
        (Some(part), _, _) | (None, Some(part), _) => part,
        (None, None, _) => MimePart::new("text/plain", BodyPart::Text(Cow::Borrowed(""))),
    };
    let root = match mixed {
        Some(boundary) => {
            let content_type = ContentType::new("multipart/mixed").attribute("boundary", boundary);
            let mut parts = vec![body];
            parts.extend(attachments.into_iter().map(Leaf::into_part));
            MimePart::new(content_type, parts)
        }
        None => body,
    };

    let mut raw = Vec::new();
    write_headers(message, &mut raw);
    MessageBuilder::new().body(root).serialize_body(&mut raw);
    // A message of one part ends with its body: end it with a line break.
    if !raw.ends_with(b"\r\n") {
        raw.extend_from_slice(b"\r\n");
    }
    if let Some(limit) = options.max_bytes
        && raw.len() > limit
    {
        return Err(too_large(raw.len(), limit));
    }
    Ok(Composed {
        raw,
        message_id: message.message_id.as_ref().map(MessageId::to_string),
        tracking_id: message.tracking_id.clone(),
        recipients: recipients(message),
    })
}

fn too_large(size: usize, limit: usize) -> ComposeError {
    ComposeError {
        field: "message",
        index: None,
        reason: Reason::TooLarge { size, limit },
    }
}

/// Check every field; the inline parts' content ids, bare, in order.
fn validate(message: &OutgoingMessage, purpose: Purpose) -> Result<Vec<MessageId>, ComposeError> {
    let at = |field: &'static str, index: Option<usize>| {
        move |reason| ComposeError {
            field,
            index,
            reason,
        }
    };
    message.from.check().map_err(at("from", None))?;
    if let Some(sender) = &message.sender {
        sender.check().map_err(at("sender", None))?;
    }
    for (field, list) in [
        ("reply_to", &message.reply_to),
        ("to", &message.to),
        ("cc", &message.cc),
        ("bcc", &message.bcc),
    ] {
        for (index, address) in list.iter().enumerate() {
            address.check().map_err(at(field, Some(index)))?;
        }
    }
    if purpose == Purpose::Send
        && message.to.is_empty()
        && message.cc.is_empty()
        && message.bcc.is_empty()
    {
        return Err(at("recipients", None)(Reason::Missing));
    }
    if let Some(subject) = &message.subject {
        no_control(subject).map_err(at("subject", None))?;
    }
    if let Some(id) = &message.message_id {
        id.check_own().map_err(at("message_id", None))?;
    }
    if let Some(token) = &message.tracking_id
        && !is_tracking_id(token)
    {
        return Err(at("tracking_id", None)(Reason::InvalidTrackingId));
    }
    for (index, (name, value)) in message.headers.iter().enumerate() {
        check_header_name(name)
            .and_then(|()| no_control(value))
            .map_err(at("headers", Some(index)))?;
    }
    if !message.inline.is_empty() && message.html.is_none() {
        return Err(at("inline", Some(0))(Reason::NoHtml));
    }
    let mut content_ids: Vec<MessageId> = Vec::with_capacity(message.inline.len());
    for (index, part) in message.inline.iter().enumerate() {
        let fail = at("inline", Some(index));
        check_part(part).map_err(fail)?;
        let id = MessageId::parse(part.content_id.as_deref().unwrap_or_default()).map_err(fail)?;
        if content_ids.contains(&id) {
            return Err(fail(Reason::Duplicate));
        }
        content_ids.push(id);
    }
    for (index, part) in message.attachments.iter().enumerate() {
        check_part(part).map_err(at("attachments", Some(index)))?;
    }
    Ok(content_ids)
}

fn no_control(value: &str) -> Result<(), Reason> {
    if value.chars().any(char::is_control) {
        Err(Reason::ControlCharacter)
    } else {
        Ok(())
    }
}

/// An RFC 5322 field name, short enough to fold after, and not one compose
/// writes or a `Content-*` header.
fn check_header_name(name: &str) -> Result<(), Reason> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| (b'!'..=b'~').contains(&byte) && byte != b':')
    {
        return Err(Reason::InvalidHeaderName);
    }
    if name.len() > MAX_HEADER_NAME {
        return Err(Reason::TooLong);
    }
    let reserved = RESERVED
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(name))
        || name
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("content-"));
    if reserved {
        return Err(Reason::ReservedHeader);
    }
    Ok(())
}

/// A leaf's media type is `type/subtype` (RFC 2045 tokens, no parameters)
/// and not multipart; its file name has no control character.
fn check_part(part: &Attachment) -> Result<(), Reason> {
    let is_token = |value: &str| {
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| (b'!'..=b'~').contains(&byte) && !b"()<>@,;:\\\"/[]?=".contains(&byte))
    };
    let (kind, subtype) = part
        .content_type
        .split_once('/')
        .ok_or(Reason::InvalidContentType)?;
    if !is_token(kind) || !is_token(subtype) {
        return Err(Reason::InvalidContentType);
    }
    if kind.eq_ignore_ascii_case("multipart") {
        return Err(Reason::Multipart);
    }
    if let Some(filename) = &part.filename {
        no_control(filename)?;
    }
    Ok(())
}

/// The bytes of every part, before encoding.
fn part_sizes(message: &OutgoingMessage) -> usize {
    let bodies = [&message.text, &message.html]
        .into_iter()
        .flatten()
        .map(String::len);
    let parts = message
        .inline
        .iter()
        .chain(&message.attachments)
        .map(|part| part.bytes.len());
    bodies.chain(parts).sum()
}

/// To, Cc and Bcc, each address once.
fn recipients(message: &OutgoingMessage) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for address in message.to.iter().chain(&message.cc).chain(&message.bcc) {
        if !out
            .iter()
            .any(|seen| seen.eq_ignore_ascii_case(&address.email))
        {
            out.push(address.email.clone());
        }
    }
    out
}

/// Write the message headers in their fixed order: From, Sender, Reply-To,
/// To, Cc, Bcc, Subject, Date, Message-ID, In-Reply-To, References,
/// X-Mailtide-Id, the custom ones, MIME-Version. Written here rather than by
/// `MessageBuilder::serialize`, which would make up a Message-ID (from the
/// clock) when none is given.
fn write_headers(message: &OutgoingMessage, out: &mut Vec<u8>) {
    let mut headers: Vec<(&str, HeaderType<'_>)> = vec![("From", mailbox(&message.from).into())];
    if let Some(sender) = &message.sender {
        headers.push(("Sender", mailbox(sender).into()));
    }
    for (name, list) in [
        ("Reply-To", &message.reply_to),
        ("To", &message.to),
        ("Cc", &message.cc),
        ("Bcc", &message.bcc),
    ] {
        if !list.is_empty() {
            let list = HeaderAddress::new_list(list.iter().map(mailbox).collect());
            headers.push((name, list.into()));
        }
    }
    if let Some(subject) = &message.subject {
        headers.push(("Subject", Text::new(subject.as_str()).into()));
    }
    headers.push(("Date", Date::new(message.sent_at).into()));
    if let Some(id) = &message.message_id {
        headers.push(("Message-ID", HeaderId::new(id.as_str()).into()));
    }
    if let Some(id) = &message.in_reply_to {
        headers.push(("In-Reply-To", HeaderId::new(id.as_str()).into()));
    }
    if !message.references.is_empty() {
        let ids = message.references.iter().map(MessageId::as_str);
        headers.push(("References", HeaderId::new_list(ids).into()));
    }
    if let Some(token) = &message.tracking_id {
        headers.push((TRACKING_HEADER, Text::new(token.as_str()).into()));
    }
    for (name, value) in &message.headers {
        headers.push((name.as_str(), Text::new(value.as_str()).into()));
    }
    for (name, value) in headers {
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b": ");
        value.write_header(out, name.len() + 2);
    }
    out.extend_from_slice(b"MIME-Version: 1.0\r\n");
}

fn mailbox(address: &Address) -> HeaderAddress<'_> {
    HeaderAddress::new_address(address.name.as_deref(), address.email.as_str())
}

/// Line ends as CRLF: a bare CR or LF becomes CRLF.
fn crlf_text(text: &str) -> Cow<'_, str> {
    if !has_bare_line_end(text.as_bytes()) {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len() + text.len() / 16);
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\r' => {
                chars.next_if_eq(&'\n');
                out.push_str("\r\n");
            }
            '\n' => out.push_str("\r\n"),
            other => out.push(other),
        }
    }
    Cow::Owned(out)
}

/// Line ends as CRLF, for bytes: a bare LF becomes CRLF.
fn crlf_bytes(bytes: &[u8]) -> Cow<'_, [u8]> {
    if !bytes
        .iter()
        .enumerate()
        .any(|(at, &byte)| byte == b'\n' && (at == 0 || bytes[at - 1] != b'\r'))
    {
        return Cow::Borrowed(bytes);
    }
    let mut out = Vec::with_capacity(bytes.len() + bytes.len() / 16);
    for (at, &byte) in bytes.iter().enumerate() {
        if byte == b'\n' && (at == 0 || bytes[at - 1] != b'\r') {
            out.push(b'\r');
        }
        out.push(byte);
    }
    Cow::Owned(out)
}

/// A CR not followed by LF, or an LF not preceded by CR.
fn has_bare_line_end(bytes: &[u8]) -> bool {
    bytes.iter().enumerate().any(|(at, &byte)| match byte {
        b'\r' => bytes.get(at + 1) != Some(&b'\n'),
        b'\n' => at == 0 || bytes[at - 1] != b'\r',
        _ => false,
    })
}

/// RFC 2045 7bit: no NUL, no 8-bit byte, CR and LF only as CRLF, lines of
/// at most 998 bytes.
fn is_seven_bit(bytes: &[u8]) -> bool {
    !has_bare_line_end(bytes)
        && !bytes.iter().any(|&byte| byte == 0 || byte > 0x7F)
        && bytes
            .split(|&byte| byte == b'\n')
            .all(|line| line.len() <= MAX_LINE + 1)
}

/// Text mail-builder writes back exactly whichever encoding it picks: no
/// bare CR or LF and no control character but tab, CR and LF (a 7bit choice
/// would write those as they are).
fn is_exact_text(bytes: &[u8]) -> bool {
    !has_bare_line_end(bytes)
        && !bytes
            .iter()
            .any(|&byte| byte < b' ' && !matches!(byte, b'\t' | b'\r' | b'\n'))
}

/// Where a leaf sits.
#[derive(Clone, Copy)]
enum Disposition<'a> {
    /// In `multipart/related`, with its content id.
    Inline(&'a str),
    /// In `multipart/mixed`.
    Attachment,
}

/// How a leaf's bytes are written.
enum Encoding<'a> {
    /// mail-builder chooses: base64 for anything but text, and for text a
    /// 7bit, quoted-printable or base64 that gives back the same bytes.
    Library,
    /// As they are, `7bit`: an attached message, line ends CRLF.
    Unencoded(Cow<'a, [u8]>),
    /// Base64 written here, for text mail-builder would not write exactly.
    Base64(String),
}

/// An inline part or attachment, ready to write.
struct Leaf<'a> {
    part: &'a Attachment,
    content_type: &'a str,
    disposition: Disposition<'a>,
    encoding: Encoding<'a>,
}

impl<'a> Leaf<'a> {
    fn new(part: &'a Attachment, disposition: Disposition<'a>) -> Self {
        let (content_type, encoding) = if part.content_type.eq_ignore_ascii_case("message/rfc822") {
            // RFC 2046 5.2.1: never base64. When it cannot be 7bit, it is
            // carried as a file instead, bytes exact.
            let bytes = crlf_bytes(&part.bytes);
            if is_seven_bit(&bytes) {
                (part.content_type.as_str(), Encoding::Unencoded(bytes))
            } else {
                ("application/octet-stream", Encoding::Library)
            }
        } else if part.content_type.starts_with("text/") && !is_exact_text(&part.bytes) {
            (
                part.content_type.as_str(),
                Encoding::Base64(MIME.encode(&part.bytes)),
            )
        } else {
            (part.content_type.as_str(), Encoding::Library)
        };
        Self {
            part,
            content_type,
            disposition,
            encoding,
        }
    }

    /// The bytes written as they are, where a boundary could occur.
    fn unencoded(&self) -> Option<&[u8]> {
        match &self.encoding {
            Encoding::Unencoded(bytes) => Some(bytes),
            Encoding::Library if self.content_type.starts_with("text/") => Some(&self.part.bytes),
            Encoding::Library | Encoding::Base64(_) => None,
        }
    }

    fn into_part(self) -> MimePart<'a> {
        let (contents, transfer) = match self.encoding {
            Encoding::Library => (BodyPart::Binary(Cow::Borrowed(&self.part.bytes[..])), None),
            Encoding::Unencoded(bytes) => (BodyPart::Binary(bytes), Some("7bit")),
            Encoding::Base64(text) => (
                BodyPart::Binary(Cow::Owned(text.into_bytes())),
                Some("base64"),
            ),
        };
        let (kind, content_id) = match self.disposition {
            Disposition::Inline(id) => ("inline", Some(id)),
            Disposition::Attachment => ("attachment", None),
        };
        let mut disposition = ContentType::new(kind);
        if let Some(filename) = &self.part.filename {
            disposition = disposition.attribute("filename", filename.as_str());
        }
        let mut part =
            MimePart::new(self.content_type, contents).header("Content-Disposition", disposition);
        if let Some(id) = content_id {
            part = part.cid(id);
        }
        if let Some(transfer) = transfer {
            part = part.transfer_encoding(transfer);
        }
        part
    }
}

/// The three multipart levels, each with boundaries of its own.
const MIXED: u8 = 0;
const ALTERNATIVE: u8 = 1;
const RELATED: u8 = 2;

/// The boundary of one multipart level: derived from the message's ids, its
/// date and author, and the level, skipping any candidate that occurs in a
/// body written unencoded. `=_` occurs in neither base64 nor
/// quoted-printable, so encoded bodies cannot hold it.
fn boundary(message: &OutgoingMessage, level: u8, unencoded: &[&[u8]]) -> String {
    let identity = format!(
        "{}\n{}\n{}\n{}",
        message.message_id.as_ref().map_or("", MessageId::as_str),
        message.tracking_id.as_deref().unwrap_or_default(),
        message.sent_at,
        message.from.email,
    );
    let seed = fnv1a(identity.as_bytes());
    let mut attempt = 0u32;
    loop {
        let candidate = format!("=_{seed:016x}.{level}.{attempt}");
        let needle = candidate.as_bytes();
        if !unencoded
            .iter()
            .any(|body| body.windows(needle.len()).any(|window| window == needle))
        {
            return candidate;
        }
        attempt += 1;
    }
}

/// 64-bit FNV-1a.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(name: Option<&str>, email: &str) -> Address {
        Address {
            name: name.map(str::to_owned),
            email: email.to_owned(),
        }
    }

    fn id(value: &str) -> MessageId {
        MessageId::parse(value).expect("id")
    }

    fn part(content_type: &str, filename: Option<&str>, bytes: &[u8]) -> Attachment {
        Attachment {
            filename: filename.map(str::to_owned),
            content_type: content_type.to_owned(),
            content_id: None,
            bytes: bytes.to_vec(),
        }
    }

    fn image(content_id: &str) -> Attachment {
        Attachment {
            content_id: Some(content_id.to_owned()),
            ..part(
                "image/png",
                Some("logo.png"),
                &[0x89, b'P', b'N', b'G', 0, 0xff],
            )
        }
    }

    /// From Ann to Bob, 2026-11-20 09:00:00 UTC.
    fn message() -> OutgoingMessage {
        let mut message =
            OutgoingMessage::new(address(Some("Ann Lee"), "ann@example.com"), 1_795_165_200);
        message.message_id = Some(id("<m1@example.com>"));
        message.to = vec![address(None, "bob@example.org")];
        message.subject = Some("Fleet policy".into());
        message.text = Some("Hello Bob\r\n".into());
        message
    }

    fn fullest() -> OutgoingMessage {
        let mut message = message();
        message.html = Some("<p>Hello <img src=\"cid:logo\"></p>\r\n".into());
        message.inline = vec![image("logo")];
        message.attachments = vec![part("application/pdf", Some("quote.pdf"), b"%PDF-1.7")];
        message
    }

    fn written(message: &OutgoingMessage) -> String {
        let composed = compose(message, &ComposeOptions::default()).expect("compose");
        String::from_utf8(composed.raw).expect("ASCII")
    }

    fn refused(message: &OutgoingMessage) -> ComposeError {
        compose(message, &ComposeOptions::default()).expect_err("refused")
    }

    fn header_block(raw: &str) -> &str {
        raw.split_once("\r\n\r\n")
            .map_or(raw, |(headers, _)| headers)
    }

    /// The media type of every part, in document order.
    fn content_types(raw: &str) -> Vec<&str> {
        raw.split("\r\n")
            .filter_map(|line| line.strip_prefix("Content-Type: "))
            .map(|value| value.split(';').next().unwrap_or(value))
            .collect()
    }

    fn boundaries(raw: &str) -> Vec<&str> {
        raw.split("boundary=\"")
            .skip(1)
            .filter_map(|rest| rest.split('"').next())
            .collect()
    }

    #[test]
    fn text_only_is_one_text_plain_part() {
        let raw = written(&message());
        assert_eq!(content_types(&raw), ["text/plain"]);
        assert!(
            header_block(&raw).ends_with(
                "MIME-Version: 1.0\r\nContent-Type: text/plain; charset=\"utf-8\"\r\n\
                 Content-Transfer-Encoding: 7bit"
            ),
            "{raw}"
        );
        assert!(!raw.contains("multipart"));
        assert!(raw.ends_with("\r\n\r\nHello Bob\r\n"), "{raw}");
    }

    #[test]
    fn text_and_html_are_one_alternative_with_text_first() {
        let mut message = message();
        message.html = Some("<p>Hello Bob</p>".into());
        let raw = written(&message);
        assert_eq!(
            content_types(&raw),
            ["multipart/alternative", "text/plain", "text/html"]
        );
        let parsed = crate::parse(raw.as_bytes()).expect("parse");
        assert_eq!(parsed.text.as_deref(), Some("Hello Bob\r\n"));
        assert_eq!(parsed.html.as_deref(), Some("<p>Hello Bob</p>"));
        assert!(parsed.attachments.is_empty());
    }

    #[test]
    fn inline_images_sit_in_a_related_part_under_the_html() {
        let mut message = fullest();
        message.attachments.clear();
        message.inline.push(image("<chart@example.com>"));
        let raw = written(&message);
        assert_eq!(
            content_types(&raw),
            [
                "multipart/alternative",
                "text/plain",
                "multipart/related",
                "text/html",
                "image/png",
                "image/png"
            ]
        );
        assert!(raw.contains("Content-Type: multipart/related; type=\"text/html\";"));
        assert!(raw.contains(
            "Content-Disposition: inline; filename=\"logo.png\"\r\nContent-ID: <logo>\r\n"
        ));
        assert!(raw.contains("Content-ID: <chart@example.com>\r\n"));
        // The related part closes before the alternative does.
        let [alternative, related] = boundaries(&raw)[..] else {
            panic!("{raw}");
        };
        let closes = |boundary: &str| raw.find(&format!("--{boundary}--")).expect("closed");
        assert!(closes(related) < closes(alternative));
        let parsed = crate::parse(raw.as_bytes()).expect("parse");
        assert_eq!(parsed.html, message.html);
        let ids: Vec<_> = parsed
            .attachments
            .iter()
            .map(|part| part.content_id.as_deref())
            .collect();
        assert_eq!(ids, [Some("logo"), Some("chart@example.com")]);
    }

    #[test]
    fn attachments_wrap_the_body_in_mixed() {
        let mut message = message();
        message.attachments = vec![
            part("application/pdf", Some("quote.pdf"), b"%PDF-1.7"),
            part("application/octet-stream", None, &[1, 2, 3]),
            part("text/csv", Some("rows.csv"), b"a,b\r\n1,2\r\n"),
        ];
        let raw = written(&message);
        assert_eq!(
            content_types(&raw),
            [
                "multipart/mixed",
                "text/plain",
                "application/pdf",
                "application/octet-stream",
                "text/csv"
            ]
        );
        assert!(raw.contains("Content-Disposition: attachment; filename=\"quote.pdf\"\r\n"));
        assert!(raw.contains(
            "Content-Type: application/octet-stream\r\nContent-Disposition: attachment\r\n"
        ));
        let parsed = crate::parse(raw.as_bytes()).expect("parse");
        assert_eq!(parsed.text.as_deref(), Some("Hello Bob\r\n"));
        let names: Vec<_> = parsed
            .attachments
            .iter()
            .map(|part| (part.filename.as_deref(), part.bytes.as_slice()))
            .collect();
        assert_eq!(
            names,
            [
                (Some("quote.pdf"), &b"%PDF-1.7"[..]),
                (None, &[1, 2, 3][..]),
                (Some("rows.csv"), &b"a,b\r\n1,2\r\n"[..]),
            ]
        );
    }

    #[test]
    fn a_message_with_no_body_gets_an_empty_text_part() {
        let mut message = message();
        message.text = None;
        let raw = written(&message);
        assert_eq!(content_types(&raw), ["text/plain"]);
        assert!(
            raw.ends_with("Content-Transfer-Encoding: 7bit\r\n\r\n"),
            "{raw}"
        );

        message.attachments = vec![part("application/pdf", Some("quote.pdf"), b"%PDF")];
        let raw = written(&message);
        assert_eq!(
            content_types(&raw),
            ["multipart/mixed", "text/plain", "application/pdf"]
        );
        let parsed = crate::parse(raw.as_bytes()).expect("parse");
        assert_eq!(parsed.attachments.len(), 1);
        assert!(parsed.text.unwrap_or_default().is_empty());
    }

    #[test]
    fn an_inline_part_without_html_is_refused() {
        let mut message = message();
        message.inline = vec![image("logo")];
        assert_eq!(
            refused(&message),
            ComposeError {
                field: "inline",
                index: Some(0),
                reason: Reason::NoHtml
            }
        );
    }

    #[test]
    fn an_inline_part_needs_a_unique_content_id() {
        let mut message = fullest();
        message.inline[0].content_id = None;
        assert_eq!(refused(&message).reason, Reason::Missing);

        let mut message = fullest();
        message.inline.push(image("<logo>"));
        let error = refused(&message);
        assert_eq!((error.index, error.reason), (Some(1), Reason::Duplicate));

        for malformed in ["", "a b", "a<b", "a\r\nb"] {
            let mut message = fullest();
            message.inline[0].content_id = Some(malformed.into());
            assert_eq!(refused(&message).field, "inline", "{malformed:?}");
        }

        let mut message = fullest();
        message.inline[0].content_id = Some("<logo@example.com>".into());
        let raw = written(&message);
        assert!(raw.contains("Content-ID: <logo@example.com>\r\n"), "{raw}");
    }

    #[test]
    fn a_multipart_attachment_type_is_refused() {
        for (content_type, reason) in [
            ("multipart/mixed", Reason::Multipart),
            ("Multipart/Related", Reason::Multipart),
            ("application", Reason::InvalidContentType),
            ("application/", Reason::InvalidContentType),
            ("/pdf", Reason::InvalidContentType),
            ("application/pdf; name=x.pdf", Reason::InvalidContentType),
            ("application/pdf/x", Reason::InvalidContentType),
            ("appli cation/pdf", Reason::InvalidContentType),
        ] {
            let mut message = message();
            message.attachments = vec![part(content_type, None, b"x")];
            assert_eq!(
                refused(&message),
                ComposeError {
                    field: "attachments",
                    index: Some(0),
                    reason
                },
                "{content_type}"
            );
        }
        let mut message = message();
        message.attachments = vec![part("application/vnd.ms-excel", None, b"x")];
        assert!(compose(&message, &ComposeOptions::default()).is_ok());
    }

    #[test]
    fn an_8_bit_attached_message_becomes_a_base64_file() {
        let eight_bit = "Subject: inner\r\n\r\n\u{e9}t\u{e9}\r\n"
            .as_bytes()
            .to_vec();
        let long_line = format!("Subject: inner\r\n\r\n{}\r\n", "x".repeat(999)).into_bytes();
        for inner in [eight_bit, long_line] {
            let mut message = message();
            message.attachments = vec![part("message/rfc822", Some("inner.eml"), &inner)];
            let raw = written(&message);
            assert!(raw.is_ascii());
            assert!(!raw.contains("message/rfc822"), "{raw}");
            assert!(raw.contains(
                "Content-Type: application/octet-stream\r\n\
                 Content-Disposition: attachment; filename=\"inner.eml\"\r\n\
                 Content-Transfer-Encoding: base64\r\n"
            ));
            let parsed = crate::parse(raw.as_bytes()).expect("parse");
            let [file] = &parsed.attachments[..] else {
                panic!("{raw}");
            };
            assert_eq!(file.filename.as_deref(), Some("inner.eml"));
            assert_eq!(file.bytes, inner);
        }
    }

    #[test]
    fn compose_then_parse_returns_the_input() {
        let every_byte: Vec<u8> = (0..=255).collect();
        let mut message = fullest();
        message.text = Some("Caf\u{e9} \u{2615}\r\nsecond line\r\n".into());
        message.html = Some("<p>Caf\u{e9} <img src=\"cid:logo\"></p>".into());
        message.inline = vec![Attachment {
            bytes: every_byte.clone(),
            ..image("logo")
        }];
        message.attachments = vec![
            part("application/octet-stream", Some("all.bin"), &every_byte),
            part("text/plain", Some("notes.txt"), "caf\u{e9}\r\n".as_bytes()),
            part("text/plain", Some("bare.txt"), b"one\rtwo\nthree\0"),
            part("application/pdf", Some("r\u{e9}sum\u{e9}.pdf"), b"%PDF-1.7"),
        ];
        let raw = written(&message);
        let parsed = crate::parse(raw.as_bytes()).expect("parse");
        assert_eq!(parsed.text, message.text);
        assert_eq!(parsed.html, message.html);
        let mut expected = message.inline.clone();
        expected.extend(message.attachments.iter().cloned());
        assert_eq!(parsed.attachments, expected);
    }

    #[test]
    fn headers_are_written_in_a_fixed_order() {
        let mut message = message();
        message.headers = vec![
            ("X-Priority".into(), "1".into()),
            ("List-Unsubscribe".into(), "<mailto:u@example.com>".into()),
        ];
        message.references = vec![id("<r0@example.org>"), id("<r1@example.org>")];
        message.in_reply_to = Some(id("<r1@example.org>"));
        message.bcc = vec![address(None, "eve@example.org")];
        message.cc = vec![address(None, "carol@example.org")];
        message.reply_to = vec![address(None, "team@example.com")];
        message.sender = Some(address(None, "assistant@example.com"));
        message.tracking_id = Some(TRACKING.into());
        let raw = written(&message);
        let names: Vec<_> = header_block(&raw)
            .split("\r\n")
            .filter(|line| !line.starts_with([' ', '\t']))
            .filter_map(|line| line.split_once(':').map(|(name, _)| name))
            .collect();
        assert_eq!(
            names,
            [
                "From",
                "Sender",
                "Reply-To",
                "To",
                "Cc",
                "Bcc",
                "Subject",
                "Date",
                "Message-ID",
                "In-Reply-To",
                "References",
                "X-Mailtide-Id",
                "X-Priority",
                "List-Unsubscribe",
                "MIME-Version",
                "Content-Type",
                "Content-Transfer-Encoding",
            ]
        );
        assert!(raw.starts_with(
            "From: \"Ann Lee\" <ann@example.com>\r\nSender: <assistant@example.com>\r\n"
        ));
        assert!(raw.contains("\r\nDate: Fri, 20 Nov 2026 09:00:00 +0000\r\n"));

        let mut bare = OutgoingMessage::new(address(None, "ann@example.com"), 0);
        bare.message_id = Some(id("m2@example.com"));
        bare.text = Some("x".into());
        let raw = compose(
            &bare,
            &ComposeOptions {
                purpose: Purpose::Draft,
                max_bytes: None,
            },
        )
        .expect("draft");
        let raw = String::from_utf8(raw.raw).expect("ASCII");
        assert!(
            raw.starts_with(
                "From: <ann@example.com>\r\nDate: Thu, 1 Jan 1970 00:00:00 +0000\r\n\
                 Message-ID: <m2@example.com>\r\nMIME-Version: 1.0\r\n"
            ),
            "{raw}"
        );
        assert!(!raw.contains("Subject"));
    }

    #[test]
    fn bcc_is_written_as_a_header() {
        let mut message = message();
        message.to.clear();
        message.bcc = vec![address(Some("Eve"), "eve@example.org")];
        for purpose in [Purpose::Send, Purpose::Draft] {
            let composed = compose(
                &message,
                &ComposeOptions {
                    purpose,
                    max_bytes: None,
                },
            )
            .expect("compose");
            let raw = String::from_utf8(composed.raw).expect("ASCII");
            assert!(
                raw.contains("\r\nBcc: \"Eve\" <eve@example.org>\r\n"),
                "{raw}"
            );
            assert_eq!(composed.recipients, ["eve@example.org"]);
        }
    }

    #[test]
    fn a_send_needs_a_recipient_and_a_draft_does_not() {
        let mut message = message();
        message.to.clear();
        let draft = ComposeOptions {
            purpose: Purpose::Draft,
            max_bytes: None,
        };
        assert_eq!(
            refused(&message),
            ComposeError {
                field: "recipients",
                index: None,
                reason: Reason::Missing
            }
        );
        let composed = compose(&message, &draft).expect("draft");
        assert!(composed.recipients.is_empty());
        assert!(!String::from_utf8_lossy(&composed.raw).contains("\r\nTo:"));

        message.from.email = String::new();
        let error = compose(&message, &draft).expect_err("no From");
        assert_eq!((error.field, error.reason), ("from", Reason::Missing));
    }

    #[test]
    fn a_reserved_header_name_is_refused() {
        for name in [
            "From",
            "to",
            "CC",
            "bcc",
            "Sender",
            "reply-to",
            "Subject",
            "DATE",
            "message-id",
            "In-Reply-To",
            "references",
            "Mime-Version",
            "Content-Type",
            "content-transfer-encoding",
            "CONTENT-X",
        ] {
            let mut message = message();
            message.headers = vec![("X-Ok".into(), "1".into()), (name.into(), "v".into())];
            assert_eq!(
                refused(&message),
                ComposeError {
                    field: "headers",
                    index: Some(1),
                    reason: Reason::ReservedHeader
                },
                "{name}"
            );
        }
        let mut message = message();
        message.headers = vec![("Contentious".into(), "1".into())];
        assert!(compose(&message, &ComposeOptions::default()).is_ok());
    }

    #[test]
    fn a_custom_header_name_must_be_printable_ascii_without_a_colon() {
        for (name, reason) in [
            ("", Reason::InvalidHeaderName),
            ("X Thing", Reason::InvalidHeaderName),
            ("X:Thing", Reason::InvalidHeaderName),
            ("X-Thing\r\nBcc", Reason::InvalidHeaderName),
            ("X-Th\u{ee}ng", Reason::InvalidHeaderName),
            ("X-Thing\t", Reason::InvalidHeaderName),
        ] {
            let mut message = message();
            message.headers = vec![(name.into(), "v".into())];
            assert_eq!(refused(&message).reason, reason, "{name:?}");
        }
        let long = format!("X-{}", "a".repeat(74));
        let mut message = message();
        message.headers = vec![(long.clone(), "v".into())];
        assert!(written(&message).contains(&format!("\r\n{long}: v\r\n")));
        message.headers = vec![(format!("{long}a"), "v".into())];
        assert_eq!(refused(&message).reason, Reason::TooLong);
    }

    #[test]
    fn a_line_break_in_a_custom_header_value_is_refused() {
        for value in [
            "a\r\nBcc: eve@example.org",
            "a\nb",
            "a\rb",
            "nul\0",
            "tab\there",
        ] {
            let mut message = message();
            message.headers = vec![("X-Note".into(), value.into())];
            assert_eq!(
                refused(&message),
                ComposeError {
                    field: "headers",
                    index: Some(0),
                    reason: Reason::ControlCharacter
                },
                "{value:?}"
            );
        }
        let mut message = message();
        message.headers = vec![("X-Note".into(), "caf\u{e9}".into())];
        let raw = written(&message);
        assert!(raw.contains("\r\nX-Note: =?utf-8?"), "{raw}");
        let parsed = crate::parse(raw.as_bytes()).expect("parse");
        let value = parsed
            .headers
            .iter()
            .find(|(name, _)| name == "X-Note")
            .map(|(_, value)| crate::parse_addresses(value).len());
        assert_eq!(value, Some(0));
    }

    #[test]
    fn threading_ids_from_the_mirror_are_written_in_order() {
        let mut message = message();
        // As `MessageContent` holds them: bracketed; and as a host may type them.
        message.in_reply_to = Some(id("<b@x.example>"));
        message.references = ["<z@x.example>", "a@x.example", "<b@x.example>"]
            .into_iter()
            .map(id)
            .collect();
        let raw = written(&message);
        assert!(raw.contains("\r\nIn-Reply-To: <b@x.example>\r\n"), "{raw}");
        assert!(
            raw.contains("\r\nReferences: <z@x.example> <a@x.example> <b@x.example>\r\n"),
            "{raw}"
        );
    }

    #[test]
    fn errors_name_the_field_and_position_never_the_value() {
        let secret = "secret-7f3a";
        let cases: Vec<(OutgoingMessage, &str, Option<usize>)> = vec![
            {
                let mut m = message();
                m.to.push(address(None, &format!("{secret} <x@y.example>")));
                (m, "to", Some(1))
            },
            {
                let mut m = message();
                m.cc = vec![address(Some(&format!("{secret}\r\n")), "c@example.org")];
                (m, "cc", Some(0))
            },
            {
                let mut m = message();
                m.subject = Some(format!("{secret}\r\nBcc: eve@example.org"));
                (m, "subject", None)
            },
            {
                let mut m = message();
                m.headers = vec![(format!("{secret}:"), secret.into())];
                (m, "headers", Some(0))
            },
            {
                let mut m = message();
                m.attachments = vec![part("application/pdf", Some(&format!("{secret}\n")), b"")];
                (m, "attachments", Some(0))
            },
            {
                let mut m = message();
                m.message_id = Some(id(secret));
                (m, "message_id", None)
            },
        ];
        for (message, field, index) in cases {
            let error = refused(&message);
            assert_eq!((error.field, error.index), (field, index));
            let shown = error.to_string();
            assert!(shown.starts_with(field), "{shown}");
            assert!(!shown.contains(secret), "{shown}");
            assert!(!format!("{error:?}").contains(secret));
        }
        assert_eq!(
            ComposeError {
                field: "to",
                index: Some(2),
                reason: Reason::NoAt
            }
            .to_string(),
            "to[2] has no @"
        );
    }

    #[test]
    fn composing_twice_gives_identical_bytes() {
        let message = fullest();
        let first = compose(&message, &ComposeOptions::default()).expect("compose");
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let message = message.clone();
                std::thread::spawn(move || compose(&message, &ComposeOptions::default()))
            })
            .collect();
        for thread in threads {
            assert_eq!(thread.join().expect("thread"), Ok(first.clone()));
        }
        assert_eq!(first.message_id.as_deref(), Some("<m1@example.com>"));
    }

    #[test]
    fn every_multipart_of_one_message_has_its_own_boundary() {
        let raw = written(&fullest());
        let found = boundaries(&raw);
        assert_eq!(found.len(), 3, "{raw}");
        assert!(found[0] != found[1] && found[1] != found[2] && found[0] != found[2]);
        let mut other = fullest();
        other.message_id = Some(id("<m2@example.com>"));
        let other = written(&other);
        assert!(boundaries(&other).iter().all(|b| !found.contains(b)));
        let parsed = crate::parse(raw.as_bytes()).expect("parse");
        assert_eq!(parsed.attachments.len(), 2);
    }

    #[test]
    fn a_boundary_found_in_a_body_is_replaced() {
        let first = boundaries(&written(&fullest()))
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let [mixed, alternative, related] = &first[..] else {
            panic!("{first:?}");
        };
        let mut message = fullest();
        message.text = Some(format!("--{alternative}--\r\n"));
        message.html = Some(format!("<p>--{related}</p>"));
        message.attachments.push(part(
            "message/rfc822",
            Some("inner.eml"),
            format!("Subject: x\r\n\r\n--{mixed}\r\n").as_bytes(),
        ));
        let raw = written(&message);
        let again = boundaries(&raw);
        assert!(again.iter().all(|b| !first.iter().any(|f| f == b)), "{raw}");
        let parsed = crate::parse(raw.as_bytes()).expect("parse");
        assert_eq!(parsed.text, message.text);
        assert_eq!(parsed.html, message.html);
        assert_eq!(parsed.attachments.len(), 3);
        assert_eq!(parsed.attachments[2].bytes, message.attachments[1].bytes);
    }

    #[test]
    fn a_message_over_the_limit_is_refused_before_encoding() {
        let mut message = message();
        message.text = Some("x".repeat(100));
        message.attachments = vec![part("application/octet-stream", None, &[7; 900])];
        let error = compose(
            &message,
            &ComposeOptions {
                purpose: Purpose::Send,
                max_bytes: Some(999),
            },
        )
        .expect_err("too large");
        // The sum of the parts, not an encoded size.
        assert_eq!(
            error,
            ComposeError {
                field: "message",
                index: None,
                reason: Reason::TooLarge {
                    size: 1000,
                    limit: 999
                }
            }
        );
    }

    #[test]
    fn the_limit_applies_to_the_encoded_size() {
        let mut message = message();
        message.text = None;
        message.attachments = vec![part("application/octet-stream", None, &[7; 3000])];
        let size = written(&message).len();
        assert!(size > 4000);
        let options = |max_bytes| ComposeOptions {
            purpose: Purpose::Send,
            max_bytes: Some(max_bytes),
        };
        assert_eq!(
            compose(&message, &options(size - 1)).map(|c| c.raw.len()),
            Err(ComposeError {
                field: "message",
                index: None,
                reason: Reason::TooLarge {
                    size,
                    limit: size - 1
                }
            })
        );
        assert_eq!(
            compose(&message, &options(size)).map(|c| c.raw.len()),
            Ok(size)
        );
    }

    const TRACKING: &str = "AAECAwQFBgcICQoLDA0ODw";

    #[test]
    fn a_tracking_id_is_written_once_and_returned() {
        let mut message = fullest();
        message.tracking_id = Some(TRACKING.into());
        message.headers = vec![("X-Note".into(), "kept".into())];
        let composed = compose(&message, &ComposeOptions::default()).expect("compose");
        let raw = String::from_utf8(composed.raw.clone()).expect("ASCII");
        assert_eq!(composed.tracking_id.as_deref(), Some(TRACKING));
        assert_eq!(raw.matches("X-Mailtide-Id").count(), 1, "{raw}");
        // After the ids compose writes, before the custom headers.
        assert!(
            header_block(&raw).contains(&format!(
                "\r\nMessage-ID: <m1@example.com>\r\nX-Mailtide-Id: {TRACKING}\r\nX-Note: kept\r\n"
            )),
            "{raw}"
        );
        assert_eq!(crate::tracking_id(&composed.raw).as_deref(), Some(TRACKING));

        message.tracking_id = None;
        let composed = compose(&message, &ComposeOptions::default()).expect("compose");
        assert_eq!(composed.tracking_id, None);
        assert!(!String::from_utf8_lossy(&composed.raw).contains("X-Mailtide-Id"));
        assert_eq!(crate::tracking_id(&composed.raw), None);
    }

    #[test]
    fn a_malformed_or_duplicated_tracking_id_is_refused() {
        for token in [
            "a".repeat(15),
            "a".repeat(65),
            format!("{TRACKING}="),
            format!("{TRACKING} x"),
            format!("{TRACKING}\r\nBcc: eve@example.org"),
            format!("{TRACKING}\u{e9}"),
            String::new(),
        ] {
            let mut message = message();
            message.tracking_id = Some(token.clone());
            assert_eq!(
                refused(&message),
                ComposeError {
                    field: "tracking_id",
                    index: None,
                    reason: Reason::InvalidTrackingId
                },
                "{token:?}"
            );
        }
        for name in ["X-Mailtide-Id", "x-mailtide-id", "X-MAILTIDE-ID"] {
            let mut message = message();
            message.tracking_id = Some(TRACKING.into());
            message.headers = vec![(name.into(), TRACKING.into())];
            assert_eq!(refused(&message).reason, Reason::ReservedHeader, "{name}");
            message.tracking_id = None;
            assert_eq!(refused(&message).reason, Reason::ReservedHeader, "{name}");
        }
        let mut message = message();
        message.tracking_id = Some("a".repeat(64));
        assert!(compose(&message, &ComposeOptions::default()).is_ok());
    }

    #[test]
    fn a_message_without_a_message_id_is_composed() {
        let mut message = fullest();
        message.message_id = None;
        message.tracking_id = Some(TRACKING.into());
        let composed = compose(&message, &ComposeOptions::default()).expect("compose");
        assert_eq!(composed.message_id, None);
        let raw = String::from_utf8(composed.raw.clone()).expect("ASCII");
        assert!(!raw.to_ascii_lowercase().contains("message-id"), "{raw}");
        assert!(header_block(&raw).contains("\r\nDate: "), "{raw}");
        assert_eq!(boundaries(&raw).len(), 3);
        // Still a function of its input.
        assert_eq!(compose(&message, &ComposeOptions::default()), Ok(composed));
        let parsed = crate::parse(raw.as_bytes()).expect("parse");
        assert_eq!(parsed.attachments.len(), 2);
    }

    /// xorshift64: a seeded generator, so every run checks the same inputs.
    struct Prng(u64);

    impl Prng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: usize) -> usize {
            usize::try_from(self.next() % bound as u64).unwrap_or_default()
        }

        fn text(&mut self) -> String {
            const PIECES: [&str; 10] = [
                "a", " ", "\r\n", "\n", "\r", "\u{e9}", "=", "--", "\t", "\0",
            ];
            let mut out = String::new();
            for _ in 0..self.below(40) {
                if self.below(10) == 0 {
                    out.push_str(&"long".repeat(self.below(400)));
                } else {
                    out.push_str(PIECES[self.below(PIECES.len())]);
                }
            }
            out
        }

        fn bytes(&mut self) -> Vec<u8> {
            if self.below(2) == 0 {
                return self.text().into_bytes();
            }
            (0..self.below(3000))
                .map(|_| u8::try_from(self.next() & 0xff).unwrap_or_default())
                .collect()
        }
    }

    #[test]
    fn seeded_messages_have_only_crlf_and_no_line_over_998_bytes() {
        const TYPES: [&str; 4] = [
            "text/plain",
            "application/octet-stream",
            "message/rfc822",
            "text/html",
        ];
        let mut prng = Prng(0x5eed_c0de_0000_0001);
        for case in 0..500 {
            let mut message = message();
            message.text = (prng.below(4) > 0).then(|| prng.text());
            message.html = (prng.below(4) > 0).then(|| prng.text());
            message.subject = Some("s\u{e9} ".repeat(prng.below(80)));
            if message.html.is_some() {
                message.inline = (0..prng.below(3))
                    .map(|n| Attachment {
                        content_id: Some(format!("c{n}")),
                        ..part(TYPES[prng.below(4)], None, &prng.bytes())
                    })
                    .collect();
            }
            message.attachments = (0..prng.below(4))
                .map(|_| {
                    let name = "n\u{e9}".repeat(prng.below(60));
                    part(TYPES[prng.below(4)], Some(&name), &prng.bytes())
                })
                .collect();
            let raw = compose(&message, &ComposeOptions::default())
                .expect("compose")
                .raw;
            for (at, &byte) in raw.iter().enumerate() {
                match byte {
                    b'\r' => assert_eq!(raw.get(at + 1), Some(&b'\n'), "case {case}: bare CR"),
                    b'\n' => assert_eq!(raw[at - 1], b'\r', "case {case}: bare LF"),
                    _ => {}
                }
            }
            assert!(raw.ends_with(b"\r\n"), "case {case}");
            let longest = raw.split(|&byte| byte == b'\n').map(<[u8]>::len).max();
            assert!(longest <= Some(MAX_LINE + 1), "case {case}: {longest:?}");
            let parsed = crate::parse(&raw).expect("parse");
            assert_eq!(
                parsed.attachments.len(),
                message.inline.len() + message.attachments.len(),
                "case {case}"
            );
        }
    }
}
