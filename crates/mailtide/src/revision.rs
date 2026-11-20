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

/// How many entries a store reads for a page of `limit`: the limit (clamped
/// to `1..=MAX_FEED_PAGE`) and one more, which says where the page's last
/// commit ends.
#[must_use]
pub fn page_window(limit: u32) -> u32 {
    limit.clamp(1, MAX_FEED_PAGE) + 1
}

/// Where a page ends ([`cut_page`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageCut {
    /// How many entries of the window the page holds.
    pub take: usize,
    /// The position after them; `None` for an empty page (the caller's
    /// position stands).
    pub next: Option<FeedCursor>,
    /// Whether entries follow the page.
    pub has_more: bool,
}

/// Cut a page from `window`, the `(state, index)` of the first
/// [`page_window`]`(limit)` entries after the reader's position (fewer at the
/// end of the log): the page ends at the last commit boundary within `limit`
/// entries, so it holds whole commits (or the rest of the commit the reader
/// is inside). Only when no boundary fits, because the first commit (or its
/// rest) is larger than `limit`, is it split: the page is its first `limit`
/// entries and `next` points inside it.
///
/// An entry ends its commit when the window's next entry belongs to another
/// commit, or when the window ends before `limit + 1` entries: commits are
/// written whole, so none of its entries can appear later.
#[must_use]
pub fn cut_page(window: &[(StateSeq, u32)], limit: u32) -> PageCut {
    let limit = usize::try_from(limit.clamp(1, MAX_FEED_PAGE)).unwrap_or(usize::MAX);
    let ends_commit = |i: usize| window.get(i + 1).is_none_or(|next| next.0 != window[i].0);
    let within = window.len().min(limit);
    let (take, next) = match (0..within).rev().find(|&i| ends_commit(i)) {
        Some(last) => (last + 1, Some(FeedCursor::from(window[last].0))),
        None => match within.checked_sub(1) {
            None => (0, None),
            Some(last) => (
                within,
                Some(FeedCursor {
                    seq: window[last].0,
                    entry: Some(window[last].1),
                }),
            ),
        },
    };
    PageCut {
        take,
        next,
        has_more: window.len() > take,
    }
}

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
            cut_page(&[(StateSeq(0xbeef), 0), (StateSeq(0xbeef), 1)], 5).next,
            Some(boundary.at),
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

    /// A window of commits of these sizes (from entry 0 of the first).
    fn window(sizes: &[(u64, u32)]) -> Vec<(StateSeq, u32)> {
        sizes
            .iter()
            .flat_map(|&(seq, size)| (0..size).map(move |i| (StateSeq(seq), i)))
            .collect()
    }

    #[test]
    fn pages_end_at_the_last_commit_boundary_within_the_limit() {
        let cut = |window: &[(StateSeq, u32)], limit| {
            let cut = cut_page(window, limit);
            (cut.take, cut.next, cut.has_more)
        };
        let whole = |seq| Some(FeedCursor::from(StateSeq(seq)));
        let inside = |seq, entry| {
            Some(FeedCursor {
                seq: StateSeq(seq),
                entry: Some(entry),
            })
        };
        // Commits of 3 and 2 with limit 4 (window 5): the first commit only,
        // not 4 entries ending inside the second.
        assert_eq!(cut(&window(&[(1, 3), (2, 2)]), 4), (3, whole(1), true));
        // The limit on a boundary: both commits.
        assert_eq!(
            cut(&window(&[(1, 3), (2, 1), (3, 1)]), 4),
            (4, whole(2), true)
        );
        // A commit of exactly the limit is one page.
        assert_eq!(cut(&window(&[(1, 4), (2, 1)]), 4), (4, whole(1), true));
        assert_eq!(cut(&window(&[(1, 4)]), 4), (4, whole(1), false));
        // No boundary fits: the first commit is larger than the limit.
        assert_eq!(cut(&window(&[(1, 5)]), 4), (4, inside(1, 3), true));
        // The rest of a commit a reader is inside (entries 4 to 6), then a
        // whole commit: back to boundaries.
        let rest: Vec<_> = (4..7)
            .map(|i| (StateSeq(1), i))
            .chain(window(&[(2, 1), (3, 3)]))
            .collect();
        assert_eq!(cut(&rest, 4), (4, whole(2), true));
        // The end of the log: everything left, no more.
        assert_eq!(cut(&window(&[(1, 2), (2, 1)]), 4), (3, whole(2), false));
        assert_eq!(cut(&[], 4), (0, None, false));
        // Clamped: limit 0 reads one entry.
        assert_eq!(page_window(0), 2);
        assert_eq!(page_window(u32::MAX), MAX_FEED_PAGE + 1);
        assert_eq!(cut(&window(&[(1, 2)]), 0), (1, inside(1, 0), true));
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
    /// page's limit, whole commits unless one commit alone is larger (see
    /// [`cut_page`]).
    pub changes: Vec<ChangeEntry>,
    /// The position to ask from next: the last entry returned (the whole
    /// commit when it was the commit's last), or the requested position
    /// when the page is empty.
    pub next: FeedCursor,
    /// Whether more changes are waiting.
    pub has_more: bool,
}
