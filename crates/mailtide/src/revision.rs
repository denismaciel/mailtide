//! State strings and the change log for consumers (SPEC §4).
//!
//! Every applied commit appends change-log entries with a monotonic sequence,
//! in the same transaction. Hosts ask "what changed since state S", in the
//! spirit of JMAP `/changes` (RFC 8620).
//!
//! Zero counterpart: `lib/mail-data-revision.ts`.

use crate::ids::{BlobRef, MessageId, ThreadId, Timestamp};
use crate::labels::LabelSet;

/// A position in an account's change log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateSeq(pub u64);

/// The opaque form of a [`StateSeq`] handed to consumers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateString(pub String);

/// A position in a specific incarnation of an account's change log: the
/// account's generation (new each time it is tracked) and the sequence in it.
/// A position from another generation, or beyond the log's head, is not a
/// place to resume from: the consumer must rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedPosition {
    /// The account's generation.
    pub generation: u64,
    /// The sequence within it.
    pub seq: StateSeq,
}

impl FeedPosition {
    /// Encode for consumers.
    #[must_use]
    pub fn encode(self) -> StateString {
        StateString(format!("{PREFIX}{:x}.{:x}", self.generation, self.seq.0))
    }

    /// Decode a consumer's state string.
    pub fn decode(state: &StateString) -> Result<Self, InvalidState> {
        let body = state.0.strip_prefix(PREFIX).ok_or(InvalidState)?;
        let (generation, seq) = body.split_once('.').ok_or(InvalidState)?;
        Ok(Self {
            generation: u64::from_str_radix(generation, 16).map_err(|_| InvalidState)?,
            seq: StateSeq(u64::from_str_radix(seq, 16).map_err(|_| InvalidState)?),
        })
    }

    /// Whether this is a place to resume `page`'s log from: same generation,
    /// not beyond its head.
    #[must_use]
    pub fn fits(self, generation: u64, head: StateSeq) -> bool {
        self.generation == generation && self.seq <= head
    }
}

const PREFIX: &str = "mt2-";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_strings_round_trip() {
        let at = FeedPosition {
            generation: 7,
            seq: StateSeq(48_879),
        };
        assert_eq!(at.encode(), StateString("mt2-7.beef".into()));
        assert_eq!(FeedPosition::decode(&at.encode()), Ok(at));
    }

    #[test]
    fn foreign_state_strings_are_rejected() {
        for bad in ["beef", "mt1-beef", "mt2-zz.1", "mt2-1"] {
            assert_eq!(
                FeedPosition::decode(&StateString(bad.into())),
                Err(InvalidState),
                "{bad}"
            );
        }
    }

    #[test]
    fn positions_fit_only_their_generation_and_log() {
        let at = FeedPosition {
            generation: 7,
            seq: StateSeq(10),
        };
        assert!(at.fits(7, StateSeq(10)));
        assert!(!at.fits(8, StateSeq(10)), "another incarnation");
        assert!(!at.fits(7, StateSeq(9)), "beyond the head");
    }
}

/// A state string that isn't one of ours.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidState;

/// What changed in the mirror, recorded in the same commit that changed it,
/// with what that commit knew. Consumers replay these in order; each entry is
/// identified by its state and its position within it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Change {
    /// A message row appeared (from history, a listing or a fetch), with the
    /// labels Gmail showed then.
    MessageDiscovered {
        /// The message.
        id: MessageId,
        /// Its thread.
        thread_id: ThreadId,
        /// Labels when discovered.
        labels: LabelSet,
    },
    /// The message's raw content was stored for the first time. Emitted once
    /// per message: drive "new mail" processing from this.
    MessageImported {
        /// The message.
        id: MessageId,
        /// Its thread.
        thread_id: ThreadId,
        /// Labels as of the import (e.g. `SENT` for outgoing mail).
        labels: LabelSet,
        /// Gmail's `internalDate`.
        internal_date: Timestamp,
        /// Where the raw content is.
        blob: BlobRef,
        /// Raw size in bytes.
        size: u64,
    },
    /// Gmail's labels for the message changed.
    LabelsChanged {
        /// The message.
        id: MessageId,
        /// Its thread.
        thread_id: ThreadId,
        /// Labels it gained.
        added: LabelSet,
        /// Labels it lost.
        removed: LabelSet,
    },
    /// Pending user actions changed how the message should look (an action
    /// was admitted, settled or refused).
    OverlayChanged {
        /// The message.
        id: MessageId,
        /// Its thread.
        thread_id: ThreadId,
        /// The labels users should see now.
        labels: LabelSet,
    },
    /// The message was deleted in Gmail.
    MessageDeleted {
        /// The message.
        id: MessageId,
        /// Its thread, if the mirror held the message.
        thread_id: Option<ThreadId>,
        /// Its last labels, if the mirror held the message.
        labels: Option<LabelSet>,
    },
    /// The account's label definitions were replaced.
    LabelDefinitionsChanged,
}

impl Change {
    /// The message it is about, if any.
    #[must_use]
    pub fn message(&self) -> Option<&MessageId> {
        match self {
            Self::MessageDiscovered { id, .. }
            | Self::MessageImported { id, .. }
            | Self::LabelsChanged { id, .. }
            | Self::OverlayChanged { id, .. }
            | Self::MessageDeleted { id, .. } => Some(id),
            Self::LabelDefinitionsChanged => None,
        }
    }
}

/// One change in the feed, with its stable identity: the state (commit) it
/// was recorded in and its position there. Replays repeat identities, so a
/// consumer that records the identities it processed can deduplicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeEntry {
    /// The commit that recorded it.
    pub state: StateSeq,
    /// Its position within that commit.
    pub index: u32,
    /// What changed.
    pub change: Change,
}

/// What reading the change feed produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeFeed {
    /// The changes after the requested state.
    Page(ChangePage),
    /// The requested state was compacted away: changes from `oldest` on are
    /// kept. Rebuild from the mirror (`message_ids` + `messages`), then
    /// follow the feed from the account's current state.
    TooOld {
        /// The account's generation.
        generation: u64,
        /// The oldest state the feed can still continue from.
        oldest: StateSeq,
    },
}

/// One page of changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangePage {
    /// The account's generation (see [`FeedPosition`]).
    pub generation: u64,
    /// The log's newest position.
    pub head: StateSeq,
    /// Changes after the requested state, whole commits only.
    pub changes: Vec<ChangeEntry>,
    /// The state to ask from next.
    pub next: StateSeq,
    /// Whether more changes are waiting.
    pub has_more: bool,
}
