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

// ------------------------------------------------------------ consumer cursors

/// The most consumers one account can register.
pub const MAX_CONSUMERS: usize = 16;

/// How long a consumer that is behind may stay silent before it stops
/// holding the log, unless a compaction says otherwise (7 days).
pub const DEFAULT_MAX_IDLE: std::time::Duration = std::time::Duration::from_hours(7 * 24);

/// The name a consumer acknowledges under: 1 to 64 letters, digits, dots,
/// dashes and underscores.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConsumerName(String);

/// A consumer name that isn't 1 to 64 characters of `[A-Za-z0-9._-]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidConsumerName;

impl ConsumerName {
    /// Validate `name`.
    pub fn parse(name: &str) -> Result<Self, InvalidConsumerName> {
        let plain = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_');
        if (1..=64).contains(&name.len()) && name.bytes().all(plain) {
            Ok(Self(name.to_owned()))
        } else {
            Err(InvalidConsumerName)
        }
    }

    /// The name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// How far a named consumer has acknowledged the feed, and when it was last
/// heard from (its last ack, or the last compaction that found it at the
/// head).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerCursor {
    /// The consumer.
    pub name: ConsumerName,
    /// Its position: may be inside a commit (see [`FeedCursor`]).
    pub at: FeedCursor,
    /// When it was last seen.
    pub seen_at: Timestamp,
}

impl ConsumerCursor {
    /// Whether compaction removed entries it has not acknowledged (see
    /// [`FeedCursor::is_compacted`]): it must rebuild.
    #[must_use]
    pub fn is_lost(&self, floor: StateSeq) -> bool {
        self.at.is_compacted(floor)
    }

    /// Whether it holds the log under `retention`: not lost, and seen within
    /// `max_idle` of `now` (exactly `max_idle` ago still holds).
    #[must_use]
    pub fn is_live(&self, floor: StateSeq, retention: &Retention) -> bool {
        !self.is_lost(floor) && retention.now.since(self.seen_at) <= retention.max_idle
    }
}

/// An account's registered consumers, with what a listing needs to describe
/// them: the generation their positions belong to and the log's floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerList {
    /// The account's generation (0 for an untracked account).
    pub generation: u64,
    /// The log's floor: a cursor behind it is lost.
    pub floor: StateSeq,
    /// The cursors, in name order.
    pub cursors: Vec<ConsumerCursor>,
}

/// What a compaction may drop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// An explicit bound: never compact past it. `None`: as far as the
    /// live consumers allow.
    pub through: Option<StateSeq>,
    /// The time of the compaction.
    pub now: Timestamp,
    /// How long a consumer that is behind may stay silent and still hold
    /// the log.
    pub max_idle: std::time::Duration,
}

impl Retention {
    /// As far as the live consumers allow, with the default idle limit.
    #[must_use]
    pub fn new(now: Timestamp) -> Self {
        Self {
            through: None,
            now,
            max_idle: DEFAULT_MAX_IDLE,
        }
    }

    /// No further than `through` (today's explicit bound).
    #[must_use]
    pub fn through(self, through: StateSeq) -> Self {
        Self {
            through: Some(through),
            ..self
        }
    }
}

/// What acknowledging a position did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckOutcome {
    /// The cursor is set to the position.
    Acked(ConsumerCursor),
    /// The position is not one to hold the log from: behind the floor (or
    /// inside the floor commit), beyond the head, or of another generation.
    /// Nothing was stored; the consumer rebuilds, as after
    /// [`ChangeFeed::TooOld`].
    TooOld {
        /// The account's generation.
        generation: u64,
        /// The oldest state the feed can still continue from.
        oldest: StateSeq,
    },
    /// The name is new and the account already has [`MAX_CONSUMERS`].
    TooManyConsumers,
}

/// The last commit a compaction may drop, or `None` to drop nothing: the
/// rule every store applies (SPEC §4).
///
/// A live cursor (see [`ConsumerCursor::is_live`]) holds everything after
/// the last commit wholly behind it ([`FeedCursor::compactable`]). An
/// explicit bound is capped by the slowest live cursor; with no bound the
/// target is that cursor's hold, the head when every registered cursor is
/// idle or lost, and nothing when no consumer is registered (the host keeps
/// positions itself).
#[must_use]
pub fn compaction_target<'a>(
    cursors: impl IntoIterator<Item = &'a ConsumerCursor>,
    floor: StateSeq,
    head: StateSeq,
    retention: &Retention,
) -> Option<StateSeq> {
    let mut registered = false;
    let mut hold: Option<StateSeq> = None;
    for cursor in cursors {
        registered = true;
        if cursor.is_live(floor, retention) {
            let mine = cursor.at.compactable();
            hold = Some(hold.map_or(mine, |held| held.min(mine)));
        }
    }
    match (retention.through, hold) {
        (Some(through), Some(hold)) => Some(through.min(hold)),
        (Some(bound), None) | (None, Some(bound)) => Some(bound),
        (None, None) => registered.then_some(head),
    }
}

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

    const HOUR: std::time::Duration = std::time::Duration::from_hours(1);

    fn cursor(name: &str, at: FeedCursor, seen_at: i64) -> ConsumerCursor {
        ConsumerCursor {
            name: ConsumerName::parse(name).expect("a valid name"),
            at,
            seen_at: Timestamp(seen_at),
        }
    }

    fn whole(seq: u64) -> FeedCursor {
        FeedCursor::from(StateSeq(seq))
    }

    /// Compaction at time 0 with a one-hour idle limit.
    fn retention(through: Option<u64>) -> Retention {
        Retention {
            through: through.map(StateSeq),
            now: Timestamp(0),
            max_idle: HOUR,
        }
    }

    #[test]
    fn consumer_names_are_short_and_plain() {
        for good in ["a", "indexer", "Search.v2", "notify-1_x", &"n".repeat(64)] {
            assert_eq!(
                ConsumerName::parse(good).map(|n| n.as_str().to_owned()),
                Ok(good.to_owned())
            );
        }
        for bad in ["", &"n".repeat(65), "a b", "a/b", "é", "a:b", "a\n"] {
            assert_eq!(
                ConsumerName::parse(bad),
                Err(InvalidConsumerName),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn compaction_stops_at_the_slowest_live_consumer() {
        let cursors = [cursor("slow", whole(10), 0), cursor("fast", whole(40), 0)];
        assert_eq!(
            compaction_target(&cursors, StateSeq(0), StateSeq(50), &retention(None)),
            Some(StateSeq(10))
        );
    }

    #[test]
    fn an_explicit_bound_is_capped_by_live_consumers() {
        let cursors = [cursor("slow", whole(10), 0)];
        let target = |through| {
            compaction_target(
                &cursors,
                StateSeq(0),
                StateSeq(50),
                &retention(Some(through)),
            )
        };
        assert_eq!(target(30), Some(StateSeq(10)));
        assert_eq!(target(5), Some(StateSeq(5)));
        assert_eq!(
            compaction_target(&[], StateSeq(0), StateSeq(50), &retention(Some(30))),
            Some(StateSeq(30)),
            "no cursors: the bound itself, as before"
        );
    }

    #[test]
    fn a_cursor_inside_a_commit_holds_that_commit() {
        let inside = FeedCursor {
            seq: StateSeq(20),
            entry: Some(3),
        };
        assert_eq!(
            compaction_target(
                &[cursor("reader", inside, 0)],
                StateSeq(0),
                StateSeq(50),
                &retention(None)
            ),
            Some(StateSeq(19))
        );
    }

    #[test]
    fn an_idle_consumer_stops_holding_the_log() {
        let limit = i64::try_from(HOUR.as_millis()).expect("an hour fits");
        let at = |seen_at| {
            compaction_target(
                &[cursor("idle", whole(10), seen_at)],
                StateSeq(0),
                StateSeq(50),
                &retention(None),
            )
        };
        assert_eq!(at(-limit - 1), Some(StateSeq(50)), "idle: ignored");
        assert_eq!(at(-limit), Some(StateSeq(10)), "exactly at the limit");
    }

    #[test]
    fn with_only_idle_consumers_everything_goes() {
        let idle = [
            cursor("a", whole(10), -10_000_000),
            cursor("b", whole(20), -10_000_000),
        ];
        assert_eq!(
            compaction_target(&idle, StateSeq(0), StateSeq(50), &retention(None)),
            Some(StateSeq(50)),
            "everyone registered is dead"
        );
        assert_eq!(
            compaction_target(&[], StateSeq(0), StateSeq(50), &retention(None)),
            None,
            "no cursor and no bound: the host is not using cursors"
        );
    }

    #[test]
    fn a_lost_cursor_never_holds_the_log() {
        let floor = StateSeq(30);
        let lost = cursor("lost", whole(10), 0);
        assert!(lost.is_lost(floor));
        let inside_floor = cursor(
            "inside",
            FeedCursor {
                seq: floor,
                entry: Some(0),
            },
            0,
        );
        assert!(inside_floor.is_lost(floor), "the floor commit is gone");
        assert_eq!(
            compaction_target(
                &[lost.clone(), inside_floor],
                floor,
                StateSeq(50),
                &retention(None)
            ),
            Some(StateSeq(50)),
            "lost cursors do not pin the floor"
        );
        assert_eq!(
            compaction_target(
                &[lost, cursor("live", whole(40), 0)],
                floor,
                StateSeq(50),
                &retention(None)
            ),
            Some(StateSeq(40))
        );
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
    /// The facts extracted from the message's raw content were brought up
    /// to date (a facts refresh, SPEC §10.3): read them again from the
    /// entry's content.
    FactsRefreshed {
        /// The message.
        id: MessageId,
        /// Its thread.
        thread_id: ThreadId,
    },
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
            | Self::MessageDeleted { id, .. }
            | Self::FactsRefreshed { id, .. } => Some(id),
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
    /// For a [`Change::MessageImported`] or [`Change::FactsRefreshed`]
    /// entry, the content the mirror stores for its message now (where the
    /// raw content is, its header facts and its message facts), so a
    /// consumer needs no second read. Read with the feed, not kept in the
    /// log: entries recorded before it existed carry it too, and an import
    /// entry shows facts refreshed since. `None` once the message is
    /// deleted, and for every other kind.
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
