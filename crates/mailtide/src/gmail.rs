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
    /// Its display name.
    pub name: String,
    /// System or user.
    pub kind: LabelKind,
}
