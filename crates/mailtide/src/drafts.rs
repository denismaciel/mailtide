//! Drafts: Gmail's drafts as the engine sees them (SPEC §4, §7a).
//!
//! A draft is a container with a stable id whose message is replaced on
//! every save. The mirror keeps a small index, draft id → current message,
//! that the answers of mailtide's own draft calls and the sync engine keep
//! true.

use crate::ids::{DraftId, MessageId, PageToken, ThreadId};
use crate::labels::LabelSet;

/// The RFC 5322 bytes of a message, as a host gives them or Gmail returns
/// them. Shown by size only: drafts are megabytes, and their text is
/// private.
#[derive(Clone, PartialEq, Eq)]
pub struct Rfc822(pub Vec<u8>);

impl std::fmt::Debug for Rfc822 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Rfc822({} bytes)", self.0.len())
    }
}

/// How much of a draft's message `drafts.get` returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DraftFormat {
    /// Ids and labels (20 units).
    Minimal,
    /// The raw message as well (60 units).
    Raw,
}

/// One entry of `drafts.list`: the draft and the message it holds now.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct ListedDraft {
    /// The draft.
    pub id: DraftId,
    /// Its current message.
    pub message: MessageId,
    /// That message's thread.
    pub thread_id: ThreadId,
}

/// One page of `drafts.list`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftPage {
    /// The drafts on this page.
    pub drafts: Vec<ListedDraft>,
    /// The next page, if any.
    pub next_page_token: Option<PageToken>,
}

/// A draft as `drafts.get` returned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedDraft {
    /// The draft and its current message.
    pub draft: ListedDraft,
    /// The message's labels (a draft's message carries `DRAFT`).
    pub labels: LabelSet,
    /// The message's bytes, when asked for in [`DraftFormat::Raw`].
    pub raw: Option<Rfc822>,
}

/// The answer to `drafts.create` or `drafts.update`: the draft id always,
/// the new message's id and thread when Gmail gave them (it always has, but
/// a write is never recorded without both: a missing one is read with
/// `drafts.get`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenDraft {
    /// The draft.
    pub id: DraftId,
    /// Its new message, if the answer named it.
    pub message: Option<MessageId>,
    /// That message's thread, if the answer named it.
    pub thread_id: Option<ThreadId>,
}

impl WrittenDraft {
    /// The draft and its message, if the answer named both.
    #[must_use]
    pub fn complete(&self) -> Option<ListedDraft> {
        Some(ListedDraft {
            id: self.id.clone(),
            message: self.message.clone()?,
            thread_id: self.thread_id.clone()?,
        })
    }
}
