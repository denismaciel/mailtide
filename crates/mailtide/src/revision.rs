//! State strings and the change log for consumers (SPEC §4).
//!
//! Every applied commit appends change-log entries with a monotonic sequence,
//! in the same transaction. Hosts ask "what changed since state S", in the
//! spirit of JMAP `/changes` (RFC 8620).
//!
//! Zero counterpart: `lib/mail-data-revision.ts`.

use crate::ids::{BlobRef, MessageId, ThreadId, Timestamp};
use crate::labels::LabelSet;
use crate::mirror::StoredContent;

/// A position in an account's change log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateSeq(pub u64);

/// The opaque form of a [`StateSeq`] handed to consumers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateString(pub String);

/// How far a consumer has read: everything up to and including `entry` of
/// commit `seq`, or the whole commit when `entry` is `None`.
///
/// Cursors order by commit, then entry, with the whole commit after every
/// entry of it: the order of the log's `(seq, idx)` key, with the whole
/// commit as an index above every entry's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedCursor {
    /// The commit.
    pub seq: StateSeq,
    /// The last entry of it read, or `None` when all of it was.
    pub entry: Option<u32>,
}

impl FeedCursor {
    /// The cursor after reading entry `index` of commit `seq`: the whole
    /// commit when that entry is its last.
    #[must_use]
    pub fn read_through(seq: StateSeq, index: u32, last_of_commit: bool) -> Self {
        Self {
            seq,
            entry: (!last_of_commit).then_some(index),
        }
    }

    /// The last commit wholly behind the cursor: what a consumer holding it
    /// can compact through (the commit itself, or the one before when the
    /// cursor is inside it).
    #[must_use]
    pub fn compactable(self) -> StateSeq {
        match self.entry {
            None => self.seq,
            Some(_) => StateSeq(self.seq.0.saturating_sub(1)),
        }
    }

    /// Whether compacting through `floor` removed entries this cursor has
    /// not read: it is behind the floor, or inside the floor commit
    /// (compaction removes whole commits).
    #[must_use]
    pub fn is_compacted(self, floor: StateSeq) -> bool {
        self < Self::from(floor)
    }

    /// The sort key: the entry, or the whole commit above every entry.
    fn key(self) -> (StateSeq, u64) {
        (
            self.seq,
            self.entry.map_or(u64::from(u32::MAX) + 1, u64::from),
        )
    }
}

impl From<StateSeq> for FeedCursor {
    /// The whole commit `seq` (and everything before it).
    fn from(seq: StateSeq) -> Self {
        Self { seq, entry: None }
    }
}

impl Ord for FeedCursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

impl PartialOrd for FeedCursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// The most entries one page of the feed holds; a larger `limit` is clamped
/// to it (the caller continues from `next`).
pub const MAX_FEED_PAGE: u32 = 5_000;

/// A position in a specific incarnation of an account's change log: the
/// account's generation (new each time it is tracked) and how far into it.
/// A position from another generation, or beyond the log's head, is not a
/// place to resume from: the consumer must rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedPosition {
    /// The account's generation.
    pub generation: u64,
    /// How far into its log.
    pub at: FeedCursor,
}

impl FeedPosition {
    /// Encode for consumers: `mt2-<generation>.<seq>` for a whole commit
    /// (the form every build reads), `mt2-<generation>.<seq>.<entry>` inside
    /// one. Fields are hex.
    #[must_use]
    pub fn encode(self) -> StateString {
        let FeedCursor { seq, entry } = self.at;
        StateString(match entry {
            None => format!("{PREFIX}{:x}.{:x}", self.generation, seq.0),
            Some(entry) => format!("{PREFIX}{:x}.{:x}.{entry:x}", self.generation, seq.0),
        })
    }

    /// Decode a consumer's state string (either form).
    pub fn decode(state: &StateString) -> Result<Self, InvalidState> {
        let body = state.0.strip_prefix(PREFIX).ok_or(InvalidState)?;
        let mut fields = body.split('.');
        let generation = hex(fields.next())?;
        let seq = StateSeq(hex(fields.next())?);
        let entry = match fields.next() {
            None => None,
            Some(entry) => Some(u32::try_from(hex(Some(entry))?).map_err(|_| InvalidState)?),
        };
        if fields.next().is_some() {
            return Err(InvalidState);
        }
        Ok(Self {
            generation,
            at: FeedCursor { seq, entry },
        })
    }

    /// Whether this is a place to resume `page`'s log from: same generation,
    /// not beyond its head.
    #[must_use]
    pub fn fits(self, generation: u64, head: StateSeq) -> bool {
        self.generation == generation && self.at.seq <= head
    }
}

/// One hex field of a state string: digits only (no sign, never empty).
fn hex(field: Option<&str>) -> Result<u64, InvalidState> {
    let field = field.ok_or(InvalidState)?;
    if field.is_empty() || !field.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(InvalidState);
    }
    u64::from_str_radix(field, 16).map_err(|_| InvalidState)
}

const PREFIX: &str = "mt2-";

#[cfg(test)]
mod tests {
    use super::*;

    fn at(generation: u64, seq: u64, entry: Option<u32>) -> FeedPosition {
        FeedPosition {
            generation,
            at: FeedCursor {
                seq: StateSeq(seq),
                entry,
            },
        }
    }

    #[test]
    fn state_strings_round_trip() {
        let at = at(7, 48_879, None);
        assert_eq!(at.encode(), StateString("mt2-7.beef".into()));
        assert_eq!(FeedPosition::decode(&at.encode()), Ok(at));
    }

    #[test]
    fn positions_inside_a_commit_round_trip() {
        let state = StateString("mt2-7.beef.2a".into());
        let decoded = FeedPosition::decode(&state);
        assert_eq!(decoded, Ok(at(7, 0xbeef, Some(0x2a))));
        assert_eq!(decoded.map(FeedPosition::encode), Ok(state));
    }

    #[test]
    fn two_field_state_strings_still_mean_the_whole_commit() {
        // As an older build wrote it, before positions could be inside a
        // commit.
        let old = StateString("mt2-7.beef".into());
        assert_eq!(FeedPosition::decode(&old), Ok(at(7, 0xbeef, None)));
        let boundary = FeedPosition {
            generation: 7,
            at: FeedCursor::from(StateSeq(0xbeef)),
        };
        assert_eq!(boundary.encode(), old, "byte for byte the old string");
        assert_eq!(
            FeedCursor::read_through(StateSeq(0xbeef), 3, true),
            boundary.at,
            "reading a commit's last entry reaches the commit boundary"
        );
    }

    #[test]
    fn foreign_state_strings_are_rejected() {
        for bad in [
            "beef", "mt1-beef", "mt2-zz.1", "mt2-1", "mt2-+1.2", "mt2-.1",
        ] {
            assert_eq!(
                FeedPosition::decode(&StateString(bad.into())),
                Err(InvalidState),
                "{bad}"
            );
        }
    }

    #[test]
    fn malformed_three_field_strings_are_rejected() {
        for bad in [
            "mt2-1.2.",
            "mt2-1.2.zz",
            "mt2-1.2.3.4",
            "mt2-1.2.+3",
            "mt2-1.2.100000000",
        ] {
            assert_eq!(
                FeedPosition::decode(&StateString(bad.into())),
                Err(InvalidState),
                "{bad}"
            );
        }
        assert_eq!(
            FeedPosition::decode(&StateString("mt2-1.2.ffffffff".into())),
            Ok(at(1, 2, Some(u32::MAX))),
            "the largest index still fits"
        );
    }

    #[test]
    fn cursors_order_by_commit_then_entry_with_the_whole_commit_last() {
        let s = StateSeq(5);
        let entry = |seq: StateSeq, i| FeedCursor {
            seq,
            entry: Some(i),
        };
        let ordered = [
            entry(s, 0),
            entry(s, 1),
            entry(s, u32::MAX),
            FeedCursor::from(s),
            entry(StateSeq(6), 0),
            FeedCursor::from(StateSeq(6)),
        ];
        for pair in ordered.windows(2) {
            assert!(pair[0] < pair[1], "{:?} < {:?}", pair[0], pair[1]);
        }
        assert!(
            entry(s, 2).is_compacted(s),
            "the rest of the floor commit is gone"
        );
        assert!(!FeedCursor::from(s).is_compacted(s));
        assert!(FeedCursor::from(StateSeq(4)).is_compacted(s));
        assert_eq!(entry(s, 2).compactable(), StateSeq(4));
        assert_eq!(FeedCursor::from(s).compactable(), s);
    }

    #[test]
    fn positions_fit_only_their_generation_and_log() {
        let at = at(7, 10, None);
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
    /// For a [`Change::MessageImported`] entry, the content the mirror
    /// stores for its message (where the raw content is and its header
    /// facts), so a consumer needs no second read. Read with the feed, not
    /// kept in the log: entries recorded before it existed carry it too.
    /// `None` once the message is deleted, and for every other kind.
    pub content: Option<StoredContent>,
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
    /// Changes after the requested position, oldest first: at most the
    /// page's limit, and a page may end inside a commit.
    pub changes: Vec<ChangeEntry>,
    /// The position to ask from next: the last entry returned (the whole
    /// commit when it was the commit's last), or the requested position
    /// when the page is empty.
    pub next: FeedCursor,
    /// Whether more changes are waiting.
    pub has_more: bool,
}
