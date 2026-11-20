//! Gmail's data model as the engine sees it (SPEC §5).
//!
//! These are decoded facts, not wire JSON: `mailtide-gmail` turns Gmail's
//! responses into these types at the boundary.
//!
//! Zero counterpart: `lib/driver/types.ts`.

use crate::ids::{BlobRef, HistoryId, LabelId, MessageId, PageToken, ThreadId, Timestamp};
use crate::labels::LabelSet;

/// One record from `history.list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryRecord {
    /// A message appeared in the mailbox.
    MessageAdded {
        /// The record's own history id.
        id: HistoryId,
        /// The message, with its labels at that point.
        message: MessageRef,
    },
    /// A message was permanently deleted.
    MessageDeleted {
        /// The record's own history id.
        id: HistoryId,
        /// The deleted message.
        message_id: MessageId,
    },
    /// Labels were added to a message.
    LabelsAdded {
        /// The record's own history id.
        id: HistoryId,
        /// The message.
        message_id: MessageId,
        /// The labels added.
        labels: LabelSet,
    },
    /// Labels were removed from a message.
    LabelsRemoved {
        /// The record's own history id.
        id: HistoryId,
        /// The message.
        message_id: MessageId,
        /// The labels removed.
        labels: LabelSet,
    },
}

/// One page of `history.list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryPage {
    /// Records in history order.
    pub records: Vec<HistoryRecord>,
    /// The mailbox history id Gmail reported with this page.
    pub history_id: HistoryId,
    /// The next page, if any. Empty pages may still carry one.
    pub next_page_token: Option<PageToken>,
}

/// A message as a history record reports it: identity and labels, with no
/// checkpoint of its own (the record's id is the checkpoint).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRef {
    /// The message.
    pub id: MessageId,
    /// Its thread.
    pub thread_id: ThreadId,
    /// Its labels.
    pub labels: LabelSet,
}

/// A complete observation of a message's labels at a known checkpoint, as
/// `messages.get` reports it or a history record implies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageObservation {
    /// The message.
    pub id: MessageId,
    /// Its thread.
    pub thread_id: ThreadId,
    /// Its labels at `checkpoint`.
    pub labels: LabelSet,
    /// The last history id that modified the message (SPEC §6.5).
    pub checkpoint: HistoryId,
}

impl MessageRef {
    /// This message observed at `checkpoint`.
    #[must_use]
    pub fn observed_at(&self, checkpoint: HistoryId) -> MessageObservation {
        MessageObservation {
            id: self.id.clone(),
            thread_id: self.thread_id.clone(),
            labels: self.labels.clone(),
            checkpoint,
        }
    }
}

/// One page of `messages.list` (ids only; Gmail returns no labels here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage {
    /// Message and thread ids, newest first.
    pub messages: Vec<(MessageId, ThreadId)>,
    /// The next page, if any.
    pub next_page_token: Option<PageToken>,
}

/// The facts the engine needs from a message's headers (SPEC §4).
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct HeaderFacts {
    /// `Message-ID`.
    pub rfc822_message_id: Option<String>,
    /// `In-Reply-To`.
    pub in_reply_to: Option<String>,
    /// `References`, in order.
    pub references: Vec<String>,
    /// `From`.
    pub from: Option<String>,
    /// `To`.
    pub to: Vec<String>,
    /// `Cc`.
    pub cc: Vec<String>,
    /// `Subject`.
    pub subject: Option<String>,
    /// Gmail's snippet.
    pub snippet: Option<String>,
}

/// Split an address list as the header facts hold it: on commas outside
/// quotes and angle brackets, each entry trimmed and kept as written
/// (encoded words included).
#[must_use]
pub fn split_addresses(value: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut angle = 0u32;
    for c in value.chars() {
        match c {
            '"' => quoted = !quoted,
            '<' => angle += 1,
            '>' => angle = angle.saturating_sub(1),
            ',' if !quoted && angle == 0 => {
                if !current.trim().is_empty() {
                    out.push(current.trim().to_owned());
                }
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    if !current.trim().is_empty() {
        out.push(current.trim().to_owned());
    }
    out
}

/// The version of the extractor that wrote a message's [`MessageFacts`].
/// Raising it makes a facts refresh extract every older row again.
pub const FACTS_VERSION: u32 = 1;

/// At most this many parts are kept in [`MessageFacts::parts`].
pub const MAX_LISTED_PARTS: usize = 32;
/// Filenames are kept up to this many bytes, cut at a character boundary.
pub const MAX_FILENAME_BYTES: usize = 255;
/// At most this many `List-Unsubscribe` URIs are kept.
pub const MAX_UNSUBSCRIBE_URIS: usize = 4;
/// A `List-Unsubscribe` URI longer than this is left out, never cut.
pub const MAX_UNSUBSCRIBE_URI_BYTES: usize = 2_048;

/// What a message's raw content says beyond the [`HeaderFacts`]: more
/// envelope fields and a bounded attachment summary (SPEC §4), extracted
/// from the stored bytes by `mailtide-mime`.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct MessageFacts {
    /// The [`FACTS_VERSION`] of the extractor that wrote these.
    pub version: u32,
    /// `Bcc`, split like `to` and `cc` and kept as written.
    pub bcc: Vec<String>,
    /// `Reply-To`, split like `to` and `cc` and kept as written.
    pub reply_to: Vec<String>,
    /// The `Date` header; `None` when missing or unreadable.
    pub sent_at: Option<SentAt>,
    /// The decoded display name of the first `From` mailbox.
    pub sender_name: Option<String>,
    /// The address of the first `From` mailbox.
    pub sender_email: Option<String>,
    /// The `List-Id` identifier: inside the angle brackets, else the
    /// trimmed value.
    pub list_id: Option<String>,
    /// The `List-Unsubscribe` URIs in order, at most
    /// [`MAX_UNSUBSCRIBE_URIS`] of at most [`MAX_UNSUBSCRIBE_URI_BYTES`].
    pub list_unsubscribe: Vec<String>,
    /// RFC 8058 one-click unsubscribe: `List-Unsubscribe-Post:
    /// List-Unsubscribe=One-Click` beside an https URI.
    pub one_click_unsubscribe: bool,
    /// The `Auto-Submitted` keyword, lowercased, parameters dropped.
    pub auto_submitted: Option<String>,
    /// The `Precedence` keyword, lowercased, parameters dropped.
    pub precedence: Option<String>,
    /// The token of the first `X-Mailtide-Id` header: the identity of a
    /// message mailtide wrote, which Gmail keeps byte for byte (a sent copy
    /// or a saved draft carries the token it was written with). `None` when
    /// absent or not 16 to 64 characters of `A-Za-z0-9_-`.
    pub tracking_id: Option<String>,
    /// How many listed parts are attached, beyond the list too.
    pub attached: u32,
    /// How many listed parts are inline, beyond the list too.
    pub inline: u32,
    /// How many RFC 1847 signature and encryption parts the message has:
    /// counted, never listed.
    pub protocol: u32,
    /// The first [`MAX_LISTED_PARTS`] listed parts, in document order.
    pub parts: Vec<AttachmentFact>,
}

impl MessageFacts {
    /// The facts of a message without any optional header or listed part,
    /// at the current version.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: FACTS_VERSION,
            ..Self::default()
        }
    }

    /// Whether the message has an attached part (the paperclip).
    #[must_use]
    pub fn has_attachment(&self) -> bool {
        self.attached > 0
    }

    /// Whether Gmail's `has:attachment` search operator matches the message:
    /// it counts every part that is not a text or HTML body, inline images
    /// and signature parts included, so it is broader than
    /// [`Self::has_attachment`].
    #[must_use]
    pub fn gmail_has_attachment(&self) -> bool {
        self.attached > 0 || self.inline > 0 || self.protocol > 0
    }
}

/// When the sender says the message was written: the `Date` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SentAt {
    /// The instant.
    pub at: Timestamp,
    /// The sender's UTC offset in minutes (`-300` for `-0500`).
    pub offset_minutes: i32,
}

/// A part the attachment summary lists: a leaf of the top-level message
/// that is neither a text or HTML body part nor an RFC 1847 protocol part.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AttachmentFact {
    /// Where its body is in the stored blob.
    pub part: PartRef,
    /// The decoded filename, at most [`MAX_FILENAME_BYTES`].
    pub filename: Option<String>,
    /// The declared media type, lowercased; `application/octet-stream` when
    /// absent or unreadable. Never sniffed.
    pub content_type: String,
    /// The `Content-ID`, without angle brackets.
    pub content_id: Option<String>,
    /// The declared `Content-Disposition` type, lowercased.
    pub disposition: Option<String>,
    /// Whether an HTML body of the message shows it through `cid:`.
    pub referenced: bool,
    /// Whether it is a child, other than the first, of `multipart/related`.
    pub related: bool,
}

impl AttachmentFact {
    /// Whether the part is shown inside the message rather than attached
    /// ([`renders_inline`] over the stored ingredients).
    #[must_use]
    pub fn is_inline(&self) -> bool {
        renders_inline(
            &self.content_type,
            self.disposition.as_deref(),
            self.referenced,
            self.related,
        )
    }
}

/// The inline rule. A listed part is inline when an HTML body shows it
/// through `cid:` (whatever its disposition), when it is declared `inline`,
/// or when it is an undeclared component of `multipart/related` that is not
/// text; else it is attached. An attached message is always attached.
#[must_use]
pub fn renders_inline(
    content_type: &str,
    disposition: Option<&str>,
    referenced: bool,
    related: bool,
) -> bool {
    let (kind, subtype) = content_type.split_once('/').unwrap_or((content_type, ""));
    if kind == "message" && matches!(subtype, "rfc822" | "global") {
        return false;
    }
    referenced
        || disposition == Some("inline")
        || (disposition.is_none() && related && kind != "text")
}

/// A part's transfer encoding, as a [`PartRef`] names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransferEncoding {
    /// None (`7bit`, `8bit`, `binary`, absent or unknown): the bytes as written.
    Identity,
    /// `quoted-printable`.
    QuotedPrintable,
    /// `base64`.
    Base64,
}

/// Where one part's body is in a stored raw message: its byte range, its
/// transfer encoding, its size once decoded, and the length of the blob it
/// was computed against. It names bytes in an immutable blob, so it stays
/// valid across parser upgrades and refreshes, and reading the part needs
/// nothing but the blob; a read larger than allowed is refused from the
/// reference alone.
///
/// Its text form is `offset.length.encoding.size.total`, e.g.
/// `1042.6920.b.5110.48211`, with `n`, `q` or `b` for the encoding, upper
/// case when the body is malformed for it (such a part is decoded whole,
/// never in chunks). Hosts treat it as opaque.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct PartRef {
    /// Where the body starts in the blob.
    pub offset: u64,
    /// The encoded length of the body.
    pub length: u64,
    /// Its transfer encoding.
    pub encoding: TransferEncoding,
    /// Whether the body is malformed for its encoding.
    pub irregular: bool,
    /// Its size after transfer decoding: exactly what reading it returns.
    pub size: u64,
    /// The length of the blob the range was computed against.
    pub total: u64,
}

/// Text that is not a part reference: not in the canonical text form, or a
/// range outside the blob length it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidPartRef(pub String);

impl PartRef {
    /// The byte range of the body in the blob.
    #[must_use]
    pub fn range(&self) -> std::ops::Range<u64> {
        self.offset..self.offset + self.length
    }

    /// Parse the text form. Only the canonical spelling is accepted, so a
    /// reference has exactly one.
    pub fn parse(text: &str) -> Result<Self, InvalidPartRef> {
        let invalid = || InvalidPartRef(text.to_owned());
        let fields: Vec<&str> = text.split('.').collect();
        let [offset, length, code, size, total] = fields.as_slice() else {
            return Err(invalid());
        };
        let number = |field: &str| field.parse::<u64>().map_err(|_| invalid());
        let (encoding, irregular) = match *code {
            "n" => (TransferEncoding::Identity, false),
            "N" => (TransferEncoding::Identity, true),
            "q" => (TransferEncoding::QuotedPrintable, false),
            "Q" => (TransferEncoding::QuotedPrintable, true),
            "b" => (TransferEncoding::Base64, false),
            "B" => (TransferEncoding::Base64, true),
            _ => return Err(invalid()),
        };
        let part = Self {
            offset: number(offset)?,
            length: number(length)?,
            encoding,
            irregular,
            size: number(size)?,
            total: number(total)?,
        };
        let fits = part
            .offset
            .checked_add(part.length)
            .is_some_and(|end| end <= part.total);
        // Decoding never makes a body longer.
        if !fits || part.size > part.length || part.to_string() != text {
            return Err(invalid());
        }
        Ok(part)
    }
}

impl std::fmt::Display for PartRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = match (self.encoding, self.irregular) {
            (TransferEncoding::Identity, false) => 'n',
            (TransferEncoding::Identity, true) => 'N',
            (TransferEncoding::QuotedPrintable, false) => 'q',
            (TransferEncoding::QuotedPrintable, true) => 'Q',
            (TransferEncoding::Base64, false) => 'b',
            (TransferEncoding::Base64, true) => 'B',
        };
        write!(
            f,
            "{}.{}.{code}.{}.{}",
            self.offset, self.length, self.size, self.total
        )
    }
}

impl std::str::FromStr for PartRef {
    type Err = InvalidPartRef;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Self::parse(text)
    }
}

impl From<PartRef> for String {
    fn from(part: PartRef) -> Self {
        part.to_string()
    }
}

impl TryFrom<String> for PartRef {
    type Error = InvalidPartRef;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::parse(&text)
    }
}

impl std::fmt::Display for InvalidPartRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "not a part reference: {:?}", self.0)
    }
}

/// A message whose raw content the host fetched and stored durably.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedMessage {
    /// Identity, labels and checkpoint at fetch time.
    pub observation: MessageObservation,
    /// Where the host stored the raw content (written before this is reported).
    pub blob: BlobRef,
    /// Size of the raw content in bytes.
    pub size: u64,
    /// Gmail's `internalDate`.
    pub internal_date: Timestamp,
    /// Header facts.
    pub headers: HeaderFacts,
    /// What the driver extracted from the stored bytes; `None` when the
    /// extractor gave up on the message, which is imported all the same.
    pub facts: Option<Box<MessageFacts>>,
}

/// The kind of a Gmail label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelKind {
    /// Built into Gmail (`INBOX`, `UNREAD`, `CATEGORY_*`, …).
    System,
    /// Created by the user (`Label_…`).
    User,
}

/// A label definition from `labels.list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Label {
    /// The label id.
    pub id: LabelId,
    /// Its display name. Gmail nests labels by `/` in the name.
    pub name: String,
    /// System or user.
    pub kind: LabelKind,
    /// Whether it shows in the label list (`labelListVisibility`); `None`
    /// when Gmail omits it (most system labels) or before a listing filled
    /// it.
    pub list_visibility: Option<LabelListVisibility>,
    /// Whether messages show under it (`messageListVisibility`); `None` as
    /// above.
    pub message_visibility: Option<MessageListVisibility>,
    /// Its colour; `None` for no colour (and for system labels).
    pub color: Option<LabelColor>,
}

impl Label {
    /// A definition with no visibility or colour known, as Gmail lists most
    /// system labels.
    #[must_use]
    pub fn plain(id: LabelId, name: impl Into<String>, kind: LabelKind) -> Self {
        Self {
            id,
            name: name.into(),
            kind,
            list_visibility: None,
            message_visibility: None,
            color: None,
        }
    }
}

/// Whether a label shows in the label list (`labelListVisibility`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LabelListVisibility {
    /// `labelShow`.
    Show,
    /// `labelShowIfUnread`: only while it has unread messages.
    ShowIfUnread,
    /// `labelHide`.
    Hide,
}

impl LabelListVisibility {
    /// Gmail's spelling.
    #[must_use]
    pub fn gmail_name(self) -> &'static str {
        match self {
            Self::Show => "labelShow",
            Self::ShowIfUnread => "labelShowIfUnread",
            Self::Hide => "labelHide",
        }
    }

    /// The value Gmail spells `name`, if it is one.
    #[must_use]
    pub fn from_gmail_name(name: &str) -> Option<Self> {
        [Self::Show, Self::ShowIfUnread, Self::Hide]
            .into_iter()
            .find(|value| value.gmail_name() == name)
    }
}

/// Whether messages show under a label (`messageListVisibility`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MessageListVisibility {
    /// `show`.
    Show,
    /// `hide`.
    Hide,
}

impl MessageListVisibility {
    /// Gmail's spelling.
    #[must_use]
    pub fn gmail_name(self) -> &'static str {
        match self {
            Self::Show => "show",
            Self::Hide => "hide",
        }
    }

    /// The value Gmail spells `name`, if it is one.
    #[must_use]
    pub fn from_gmail_name(name: &str) -> Option<Self> {
        [Self::Show, Self::Hide]
            .into_iter()
            .find(|value| value.gmail_name() == name)
    }
}

/// A label's colour: Gmail's text and background pair (`#rrggbb`), as Gmail
/// reports it. A colour a caller sets is checked against Google's palette
/// before it is sent (`mailtide_gmail::labels::palette_color`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelColor {
    /// `textColor`.
    pub text: String,
    /// `backgroundColor`.
    pub background: String,
}

/// A user label to create (`labels.create`). Both visibilities are always
/// sent, so the label shows as asked whatever Gmail's defaults are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewLabel {
    /// Its name; `Parent/Child` nests it under `Parent`.
    pub name: String,
    /// Whether it shows in the label list.
    pub list_visibility: LabelListVisibility,
    /// Whether messages show under it.
    pub message_visibility: MessageListVisibility,
    /// Its colour, if any.
    pub color: Option<LabelColor>,
}

/// The fields of a label to change (`labels.patch`): only those given are
/// sent, so the others keep their values.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LabelPatch {
    /// A new name.
    pub name: Option<String>,
    /// A new list visibility.
    pub list_visibility: Option<LabelListVisibility>,
    /// A new message visibility.
    pub message_visibility: Option<MessageListVisibility>,
    /// What to do with the colour.
    pub color: ColorChange,
}

impl LabelPatch {
    /// Whether it changes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.name.is_none()
            && self.list_visibility.is_none()
            && self.message_visibility.is_none()
            && self.color == ColorChange::Keep
    }
}

/// What a patch does to a label's colour.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ColorChange {
    /// Leave it (nothing is sent).
    #[default]
    Keep,
    /// Set this pair.
    Set(LabelColor),
    /// Remove it (`color: null`, which keeps the other fields).
    Clear,
}
