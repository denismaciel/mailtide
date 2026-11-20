//! Drafts: Gmail's drafts as the engine sees them (SPEC §4, §7a).
//!
//! A draft is a container with a stable id whose message is replaced on
//! every save. The mirror keeps a small index, draft id → current message,
//! that the answers of mailtide's own draft calls and the sync engine keep
//! true.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::ids::{DraftId, HistoryId, MessageId, PageToken, ThreadId, Timestamp};
use crate::labels::{self, LabelSet};
use crate::revision::Change;

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

// ------------------------------------------------------------ the index

/// A send of a draft mailtide started and has not settled (SPEC §7a).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SendAttempt {
    /// When the send began.
    pub started: Timestamp,
    /// Before this, the request may still be in flight: another send of
    /// the draft is refused (`SendInProgress`), and a listing does not
    /// prove it unsent.
    pub retry_at: Timestamp,
    /// The account's cursor when the send began: an unconfirmed send ends
    /// only once history has moved past it.
    pub cursor: Option<HistoryId>,
}

/// Where a draft row stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DraftState {
    /// The row names the draft's current message.
    Saved,
    /// Its message was deleted in Gmail (replaced, deleted or sent
    /// elsewhere); the next refresh says which.
    Stale,
    /// A send is in flight, or its answer was lost.
    Sending(SendAttempt),
    /// A send whose answer was lost, and the draft is gone from Gmail's
    /// list: sent, or deleted elsewhere. A `SENT` import carrying its
    /// tracking id confirms it; otherwise it ends after the grace period.
    SendUnconfirmed(SendAttempt),
}

/// [`DraftState`] without its bookkeeping, as hosts see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DraftStatus {
    /// See [`DraftState::Saved`].
    Saved,
    /// See [`DraftState::Stale`].
    Stale,
    /// See [`DraftState::Sending`].
    Sending {
        /// When the send began (names the attempt).
        started: Timestamp,
        /// Until when its request may be in flight: another send is
        /// refused before then.
        retry_at: Timestamp,
    },
    /// See [`DraftState::SendUnconfirmed`].
    SendUnconfirmed,
}

impl DraftState {
    /// As hosts see it.
    #[must_use]
    pub fn status(&self) -> DraftStatus {
        match self {
            Self::Saved => DraftStatus::Saved,
            Self::Stale => DraftStatus::Stale,
            Self::Sending(attempt) => DraftStatus::Sending {
                started: attempt.started,
                retry_at: attempt.retry_at,
            },
            Self::SendUnconfirmed(_) => DraftStatus::SendUnconfirmed,
        }
    }

    /// The send attempt, while one is unsettled.
    #[must_use]
    pub fn attempt(&self) -> Option<&SendAttempt> {
        match self {
            Self::Sending(attempt) | Self::SendUnconfirmed(attempt) => Some(attempt),
            Self::Saved | Self::Stale => None,
        }
    }
}

/// One row of the draft index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftRecord {
    /// The draft.
    pub id: DraftId,
    /// Its current message, as far as the mirror knows.
    pub message: MessageId,
    /// That message's thread.
    pub thread_id: ThreadId,
    /// The `X-Mailtide-Id` of the bytes mailtide last saved in it; `None`
    /// for a draft only seen in Gmail's list.
    pub tracking_id: Option<String>,
    /// Where it stands.
    pub state: DraftState,
    /// When the row last changed.
    pub since: Timestamp,
}

/// A draft as hosts read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftView {
    /// The draft.
    pub draft_id: DraftId,
    /// Its current message.
    pub message_id: MessageId,
    /// That message's thread.
    pub thread_id: ThreadId,
    /// The `X-Mailtide-Id` of the bytes mailtide saved in it.
    pub tracking_id: Option<String>,
    /// The Message-ID Gmail's copy carries, once the current message is
    /// imported. Gmail assigns a new one on every save and on send.
    pub rfc822_message_id: Option<String>,
    /// Where it stands.
    pub state: DraftStatus,
}

impl DraftRecord {
    /// The host's view, given the Message-ID of the imported message.
    #[must_use]
    pub fn view(&self, rfc822_message_id: Option<String>) -> DraftView {
        DraftView {
            draft_id: self.id.clone(),
            message_id: self.message.clone(),
            thread_id: self.thread_id.clone(),
            tracking_id: self.tracking_id.clone(),
            rfc822_message_id,
            state: self.state.status(),
        }
    }
}

/// The index: one row per draft id.
pub type DraftIndex = BTreeMap<DraftId, DraftRecord>;

/// Who saved a draft, in a [`Change::DraftSaved`] entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DraftOrigin {
    /// mailtide's own call.
    Local,
    /// Another client, seen in Gmail's list.
    Remote,
}

/// Why a draft left the index, in a [`Change::DraftRemoved`] entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RemovedReason {
    /// Deleted through mailtide.
    Deleted,
    /// Gone from Gmail: deleted or sent elsewhere.
    Gone,
    /// mailtide sent it, the answer was lost, and no sent message was
    /// matched within the grace period. It was sent at most once.
    SendUnconfirmed,
}

/// What mailtide's own draft call learned, recorded without an engine
/// (`Store::record_draft`, unfenced like `admit`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DraftWrite {
    /// A create or update answered: the draft holds this message now.
    Saved {
        /// The draft and its new message.
        draft: ListedDraft,
        /// The `X-Mailtide-Id` of the bytes saved.
        tracking_id: String,
        /// A create (the row is new); an update never re-creates a row
        /// that left the index meanwhile (its send settled, or it is gone).
        created: bool,
    },
    /// The draft is gone: deleted here, or Gmail answered 404. A row
    /// whose send is unsettled is left alone: the send causes that 404,
    /// and the send's own record settles it.
    Removed {
        /// The draft.
        draft: DraftId,
        /// Why.
        reason: RemovedReason,
    },
    /// An update's answer was lost: the row may name a replaced message.
    Stale {
        /// The draft.
        draft: DraftId,
        /// When to list the drafts to find out.
        refresh_at: Timestamp,
    },
    /// A create's answer was lost: list the drafts at `at`, so a draft
    /// Gmail did create enters the index.
    Refresh {
        /// When.
        at: Timestamp,
    },
    /// Mark the draft as sending: a compare-and-set from `saved` (or from
    /// a `sending` row whose request has certainly timed out), so one
    /// caller in any process wins.
    BeginSend {
        /// The draft.
        draft: DraftId,
        /// Until when this send's request may be in flight: after the
        /// latest time it may be issued, the request timeout and a margin
        /// for late application.
        retry_at: Timestamp,
    },
    /// How the send begun at `started` ended. Applies only while that
    /// attempt holds the row (a retry may have taken it over), except
    /// `Sent`, which is a fact whoever holds it.
    EndSend {
        /// The draft.
        draft: DraftId,
        /// The attempt's start.
        started: Timestamp,
        /// How it ended.
        end: SendEnd,
    },
}

/// How a send ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendEnd {
    /// Gmail sent it: this is the new message.
    Sent {
        /// The sent message.
        message: MessageId,
        /// Its thread.
        thread_id: ThreadId,
    },
    /// Nothing was sent (refused, rate limited, deferred): back to saved.
    Unsent,
    /// A retry found the draft gone: it may have been sent.
    Unconfirmed,
    /// A retry found the draft holding another message: back to saved, at
    /// that message.
    Changed {
        /// The message it holds now.
        message: MessageId,
        /// Its thread.
        thread_id: ThreadId,
    },
    /// The first attempt found no such draft (404).
    Gone,
}

/// What [`DraftWrite`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DraftRecorded {
    /// Another submission owns the tracking token.
    TokenConflict,
    /// Recorded (possibly as a no-op).
    Done,
    /// `BeginSend`: the row is sending now; `retry` when it took over an
    /// attempt whose request had timed out, `stale` when the row was stale:
    /// either way the draft is read before sending.
    Started {
        /// The row as marked.
        draft: DraftRecord,
        /// Whether an earlier attempt's answer was lost.
        retry: bool,
        /// Whether the row was stale (Gmail may hold another message).
        stale: bool,
    },
    /// `BeginSend`: no row for the draft.
    NotIndexed,
    /// `BeginSend`: another send holds it until `retry_at`.
    InProgress {
        /// When a retry may read the draft.
        retry_at: Timestamp,
    },
    /// `BeginSend`: a send's answer was lost and the draft is gone.
    Unconfirmed,
    /// `EndSend`: another attempt holds the row; nothing changed.
    Superseded,
}

/// What a draft rule did beside the rows: feed entries, whether to list the
/// drafts again, and whether the index changed (a listing claimed before
/// then must not apply).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DraftEffects {
    /// Feed entries, in order.
    pub changes: Vec<Change>,
    /// Enqueue the account's `RefreshDrafts` job.
    pub refresh: bool,
    /// Bump the draft generation.
    pub changed: bool,
    /// List again at this time (a send's request may be in flight until
    /// then, or its grace period ends then).
    pub refresh_at: Option<Timestamp>,
}

impl DraftEffects {
    fn changed() -> Self {
        Self {
            changed: true,
            ..Self::default()
        }
    }
}

/// A draft-index change inside a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DraftOp {
    /// A complete `drafts.list` listing, from a `RefreshDrafts` claim.
    Reconcile(DraftReconcile),
}

/// [`DraftOp::Reconcile`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftReconcile {
    /// Every draft listed, all pages.
    pub listed: Vec<ListedDraft>,
    /// The draft generation read at the claim: a local write since then
    /// fences the listing out.
    pub generation: u64,
    /// When the job was claimed (the listing is no older).
    pub claimed_at: Timestamp,
    /// How long an unconfirmed send waits for its sent message.
    pub grace: Duration,
    /// After [`STALE_LISTINGS`] stale listings in a row, the next waits
    /// this long (the periodic interval) instead of the short delay.
    pub every: Duration,
}

/// Stale listings in a row after which the next waits the periodic
/// interval: a listing that keeps losing to local writes, or a Gmail that
/// keeps listing what history deleted, must not relist every few seconds.
pub const STALE_LISTINGS: u32 = 3;

/// What a listing did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciled {
    /// Applied.
    Applied(DraftEffects),
    /// Not applied: the index changed since the claim, or the listing
    /// is not coherent (an id or message twice). List again.
    Stale,
}

fn removed(record: &DraftRecord, reason: RemovedReason) -> Change {
    Change::DraftRemoved {
        draft_id: record.id.clone(),
        last_message_id: record.message.clone(),
        reason,
    }
}

fn saved(draft: &ListedDraft, origin: DraftOrigin) -> Change {
    Change::DraftSaved {
        draft_id: draft.id.clone(),
        message_id: draft.message.clone(),
        thread_id: draft.thread_id.clone(),
        origin,
    }
}

/// Drop rows other than `keep` that name `message` (one message belongs to
/// at most one draft; such a row is wrong, and the next listing would drop
/// it anyway).
fn evict(index: &mut DraftIndex, keep: &DraftId, message: &MessageId, effects: &mut DraftEffects) {
    let others: Vec<DraftId> = index
        .values()
        .filter(|r| &r.id != keep && &r.message == message)
        .map(|r| r.id.clone())
        .collect();
    for id in others {
        if let Some(record) = index.remove(&id) {
            effects.changes.push(removed(&record, RemovedReason::Gone));
        }
    }
}

/// Apply mailtide's own draft call to the index (pure). `cursor` is the
/// account's, recorded on a send attempt.
pub fn record(
    index: &mut DraftIndex,
    write: &DraftWrite,
    now: Timestamp,
    cursor: Option<HistoryId>,
) -> (DraftRecorded, DraftEffects) {
    let mut effects = DraftEffects::changed();
    let outcome = match write {
        DraftWrite::Saved {
            draft,
            created: false,
            ..
        } if !index.contains_key(&draft.id) => {
            effects = DraftEffects::default();
            DraftRecorded::Done
        }
        DraftWrite::Saved {
            draft, tracking_id, ..
        } => {
            evict(index, &draft.id, &draft.message, &mut effects);
            let previous = index.get(&draft.id);
            if previous.is_none_or(|p| p.message != draft.message) {
                effects.changes.push(saved(draft, DraftOrigin::Local));
            }
            // A row being sent keeps its fence: the send settles it.
            let state = previous.map_or(DraftState::Saved, |p| match p.state {
                DraftState::Sending(_) | DraftState::SendUnconfirmed(_) => p.state,
                DraftState::Saved | DraftState::Stale => DraftState::Saved,
            });
            index.insert(
                draft.id.clone(),
                DraftRecord {
                    id: draft.id.clone(),
                    message: draft.message.clone(),
                    thread_id: draft.thread_id.clone(),
                    tracking_id: Some(tracking_id.clone()),
                    state,
                    since: now,
                },
            );
            DraftRecorded::Done
        }
        DraftWrite::Removed { draft, reason } => {
            if index
                .get(draft)
                .is_some_and(|r| r.state.attempt().is_none())
                && let Some(record) = index.remove(draft)
            {
                effects.changes.push(removed(&record, *reason));
            }
            DraftRecorded::Done
        }
        DraftWrite::Stale { draft, .. } => {
            effects.refresh = true;
            if let Some(record) = index.get_mut(draft)
                && record.state == DraftState::Saved
            {
                record.state = DraftState::Stale;
                record.since = now;
            }
            DraftRecorded::Done
        }
        DraftWrite::Refresh { .. } => {
            effects.refresh = true;
            DraftRecorded::Done
        }
        DraftWrite::BeginSend { draft, retry_at } => {
            begin_send(index, draft, *retry_at, now, cursor)
        }
        DraftWrite::EndSend {
            draft,
            started,
            end,
        } => end_send(index, draft, *started, end, now, &mut effects),
    };
    if !matches!(outcome, DraftRecorded::Done | DraftRecorded::Started { .. }) {
        effects = DraftEffects::default();
    }
    (outcome, effects)
}

fn begin_send(
    index: &mut DraftIndex,
    draft: &DraftId,
    retry_at: Timestamp,
    now: Timestamp,
    cursor: Option<HistoryId>,
) -> DraftRecorded {
    let Some(record) = index.get_mut(draft) else {
        return DraftRecorded::NotIndexed;
    };
    let retry = match record.state {
        DraftState::Sending(attempt) if now < attempt.retry_at => {
            return DraftRecorded::InProgress {
                retry_at: attempt.retry_at,
            };
        }
        DraftState::SendUnconfirmed(_) => return DraftRecorded::Unconfirmed,
        DraftState::Sending(_) => true,
        DraftState::Saved | DraftState::Stale => false,
    };
    let stale = record.state == DraftState::Stale;
    record.state = DraftState::Sending(SendAttempt {
        started: now,
        retry_at: retry_at.max(now),
        cursor,
    });
    record.since = now;
    DraftRecorded::Started {
        draft: record.clone(),
        retry,
        stale,
    }
}

fn end_send(
    index: &mut DraftIndex,
    draft: &DraftId,
    started: Timestamp,
    end: &SendEnd,
    now: Timestamp,
    effects: &mut DraftEffects,
) -> DraftRecorded {
    if let SendEnd::Sent { message, thread_id } = end {
        let Some(record) = index.remove(draft) else {
            return DraftRecorded::Superseded;
        };
        effects.changes.push(Change::DraftSent {
            draft_id: record.id,
            message_id: message.clone(),
            thread_id: thread_id.clone(),
        });
        return DraftRecorded::Done;
    }
    let Some(record) = index.get_mut(draft) else {
        return DraftRecorded::Superseded;
    };
    let attempt = match record.state {
        DraftState::Sending(attempt) if attempt.started == started => attempt,
        DraftState::Saved
        | DraftState::Stale
        | DraftState::Sending(_)
        | DraftState::SendUnconfirmed(_) => return DraftRecorded::Superseded,
    };
    record.since = now;
    match end {
        SendEnd::Unsent | SendEnd::Sent { .. } => record.state = DraftState::Saved,
        SendEnd::Unconfirmed => record.state = DraftState::SendUnconfirmed(attempt),
        SendEnd::Gone => {
            if let Some(record) = index.remove(draft) {
                effects.changes.push(removed(&record, RemovedReason::Gone));
            }
        }
        SendEnd::Changed { message, thread_id } => {
            record.message = message.clone();
            record.thread_id = thread_id.clone();
            record.state = DraftState::Saved;
            let listed = ListedDraft {
                id: draft.clone(),
                message: message.clone(),
                thread_id: thread_id.clone(),
            };
            effects.changes.push(saved(&listed, DraftOrigin::Remote));
            evict(index, draft, message, effects);
        }
    }
    DraftRecorded::Done
}

/// Apply a complete listing (pure). `tombstoned` says whether the mirror
/// knows a message is deleted; `cursor` is the account's.
pub fn reconcile(
    index: &mut DraftIndex,
    op: &DraftReconcile,
    generation: u64,
    tombstoned: impl Fn(&MessageId) -> bool,
    cursor: Option<HistoryId>,
) -> Reconciled {
    let ids: BTreeSet<&DraftId> = op.listed.iter().map(|d| &d.id).collect();
    let messages: BTreeSet<&MessageId> = op.listed.iter().map(|d| &d.message).collect();
    if op.generation != generation
        || ids.len() != op.listed.len()
        || messages.len() != op.listed.len()
    {
        return Reconciled::Stale;
    }
    // A draft listed with a message the mirror knows is deleted is listed
    // late (the listing is not a snapshot): it says nothing about that
    // draft, which keeps its row (it is in `ids`, so never missing); the
    // rest of the listing applies.
    let listed: Vec<&ListedDraft> = op
        .listed
        .iter()
        .filter(|d| !tombstoned(&d.message))
        .collect();
    let at = op.claimed_at;
    let mut effects = DraftEffects::default();
    // Rows the listing lacks first, so a re-pointed row never meets one
    // that still names its message.
    let missing: Vec<DraftId> = index
        .keys()
        .filter(|id| !ids.contains(id))
        .cloned()
        .collect();
    for id in missing {
        let Some(record) = index.get_mut(&id) else {
            continue;
        };
        match record.state {
            DraftState::Saved | DraftState::Stale => {
                if let Some(record) = index.remove(&id) {
                    effects.changes.push(removed(&record, RemovedReason::Gone));
                }
            }
            DraftState::Sending(attempt) if at >= attempt.retry_at => {
                record.state = DraftState::SendUnconfirmed(attempt);
                record.since = at;
            }
            DraftState::Sending(_) => {}
            DraftState::SendUnconfirmed(attempt) => {
                let moved_past = cursor.is_some_and(|c| attempt.cursor.is_none_or(|a| c > a));
                if moved_past
                    && at >= attempt.started.plus(op.grace)
                    && let Some(record) = index.remove(&id)
                {
                    effects
                        .changes
                        .push(removed(&record, RemovedReason::SendUnconfirmed));
                }
            }
        }
    }
    for listed in listed {
        match index.get_mut(&listed.id) {
            None => {
                effects.changes.push(saved(listed, DraftOrigin::Remote));
                index.insert(
                    listed.id.clone(),
                    DraftRecord {
                        id: listed.id.clone(),
                        message: listed.message.clone(),
                        thread_id: listed.thread_id.clone(),
                        tracking_id: None,
                        state: DraftState::Saved,
                        since: at,
                    },
                );
            }
            Some(record) => {
                let settles = match record.state {
                    DraftState::Saved | DraftState::Stale => true,
                    // Still listed once its request had certainly ended:
                    // the send never happened.
                    DraftState::Sending(attempt) | DraftState::SendUnconfirmed(attempt) => {
                        at >= attempt.retry_at
                    }
                };
                if !settles {
                    continue;
                }
                let moved =
                    record.message != listed.message || record.thread_id != listed.thread_id;
                if moved {
                    effects.changes.push(saved(listed, DraftOrigin::Remote));
                }
                if moved || record.state != DraftState::Saved {
                    // Edited elsewhere: its token is the import's to tell.
                    if record.message != listed.message {
                        record.tracking_id = None;
                    }
                    record.message = listed.message.clone();
                    record.thread_id = listed.thread_id.clone();
                    record.state = DraftState::Saved;
                    record.since = at;
                }
            }
        }
    }
    effects.changed = !effects.changes.is_empty();
    effects.refresh_at = next_listing(index, at, op.grace);
    Reconciled::Applied(effects)
}

/// When to list again after a listing at `at`: once an unsettled send's
/// request has certainly ended, or its grace period has (nothing else
/// would before the periodic listing).
fn next_listing(index: &DraftIndex, at: Timestamp, grace: Duration) -> Option<Timestamp> {
    index
        .values()
        .filter_map(|r| match r.state {
            DraftState::Sending(attempt) => Some(attempt.retry_at),
            DraftState::SendUnconfirmed(attempt) => Some(attempt.started.plus(grace)),
            DraftState::Saved | DraftState::Stale => None,
        })
        .filter(|due| *due > at)
        .min()
}

/// A message the mirror deleted: a row naming it is out of date (pure).
pub fn on_tombstone(index: &mut DraftIndex, message: &MessageId) -> DraftEffects {
    let mut effects = DraftEffects::default();
    for record in index.values_mut().filter(|r| &r.message == message) {
        effects.refresh = true;
        effects.changed = true;
        if record.state == DraftState::Saved {
            record.state = DraftState::Stale;
        }
    }
    effects
}

/// A message row the mirror created: a draft no row explains was saved
/// elsewhere (pure). Our own saves are recorded before history shows them.
#[must_use]
pub fn on_new_message(index: &DraftIndex, message: &MessageId, labels: &LabelSet) -> DraftEffects {
    if labels::is_draft(labels) && !index.values().any(|r| &r.message == message) {
        DraftEffects {
            changes: Vec::new(),
            refresh: true,
            changed: true,
            refresh_at: None,
        }
    } else {
        DraftEffects::default()
    }
}

/// A message imported for the first time (pure), with the tracking id its
/// facts carry (`None`: no facts; `Some(None)`: no header).
///
/// - A draft's current message tells the row's token: set or cleared from
///   the facts, so a draft adopted by a listing, or edited elsewhere,
///   carries what Gmail holds.
/// - A `SENT` message carrying the token of exactly one draft whose send is
///   unsettled confirms that send, in whatever thread it is. Two rows with
///   one token settle nothing (mailtide refuses to save such a pair, so
///   only another client could make one).
pub fn on_import(
    index: &mut DraftIndex,
    message: &MessageId,
    thread_id: &ThreadId,
    labels: &LabelSet,
    tracking_id: Option<Option<&str>>,
) -> DraftEffects {
    let mut effects = DraftEffects::default();
    let Some(tracking_id) = tracking_id else {
        return effects;
    };
    if let Some(record) = index.values_mut().find(|r| &r.message == message) {
        if record.tracking_id.as_deref() != tracking_id {
            record.tracking_id = tracking_id.map(str::to_owned);
            effects.changed = true;
        }
        return effects;
    }
    let Some(tracking_id) = tracking_id.filter(|_| labels::has(labels, labels::system::SENT))
    else {
        return effects;
    };
    let matches: Vec<&DraftId> = index
        .values()
        .filter(|r| r.tracking_id.as_deref() == Some(tracking_id))
        .map(|r| &r.id)
        .collect();
    let [id] = matches.as_slice() else {
        return effects;
    };
    let id = (*id).clone();
    if index.get(&id).is_some_and(|r| r.state.attempt().is_some())
        && let Some(record) = index.remove(&id)
    {
        effects.changed = true;
        effects.changes.push(Change::DraftSent {
            draft_id: record.id,
            message_id: message.clone(),
            thread_id: thread_id.clone(),
        });
    }
    effects
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::{Commit, CommitOutcome, DeletionEvidence, JobPlan, MessageOp};
    use crate::gmail::{HeaderFacts, MessageFacts, MessageObservation};
    use crate::ids::{AccountId, BlobRef, CommitId, ENGINE_VERSION};
    use crate::jobs::JobKind;
    use crate::labels::set;
    use crate::mirror::AccountMirror;
    use crate::quota::Priority;

    const TOKEN: &str = "tracking-0123456789";
    const RETRY: Duration = Duration::from_mins(1);
    const GRACE: Duration = Duration::from_hours(1);

    fn mirror() -> AccountMirror {
        let mut mirror = AccountMirror::new(AccountId("alice@example.com".into()), 1);
        mirror.claim(ENGINE_VERSION).expect("claim");
        mirror
    }

    fn draft(id: &str, message: &str, thread: &str) -> ListedDraft {
        ListedDraft {
            id: DraftId(id.into()),
            message: MessageId(message.into()),
            thread_id: ThreadId(thread.into()),
        }
    }

    fn commit(mirror: &AccountMirror, at: i64) -> Commit {
        let now = Timestamp(at);
        Commit {
            id: CommitId {
                epoch: mirror.record.epoch,
                seq: 0,
            },
            account: mirror.account.clone(),
            epoch: mirror.record.epoch,
            account_generation: mirror.record.generation,
            engine_version: ENGINE_VERSION,
            cursor: None,
            claim: None,
            sync: None,
            messages: Vec::new(),
            labels: Vec::new(),
            account_ops: Vec::new(),
            job_plan: JobPlan {
                priority: Priority::Live,
                import_due_at: now,
                draft_import_due_at: now,
                verify_due_at: now,
                cleanup_due_at: now,
                deliver_due_at: now,
                confirm_due_at: now,
                draft_refresh_due_at: now.plus(Duration::from_secs(10)),
            },
            jobs: Vec::new(),
            intents: Vec::new(),
            drafts: Vec::new(),
        }
    }

    fn apply(mirror: &mut AccountMirror, commit: &Commit) {
        assert!(
            matches!(mirror.apply(commit), CommitOutcome::Applied { .. }),
            "{commit:?}"
        );
    }

    fn observe(id: &str, thread: &str, labels: &[&str], checkpoint: u64) -> MessageOp {
        MessageOp::Observe(MessageObservation {
            id: MessageId(id.into()),
            thread_id: ThreadId(thread.into()),
            labels: set(labels.iter().copied()),
            checkpoint: HistoryId(checkpoint),
        })
    }

    fn tombstone(id: &str, at: u64) -> MessageOp {
        MessageOp::Tombstone {
            id: MessageId(id.into()),
            evidence: DeletionEvidence::History { at: HistoryId(at) },
        }
    }

    fn sent_import(id: &str, thread: &str, labels: &[&str], token: Option<&str>) -> MessageOp {
        MessageOp::MarkImported {
            observation: MessageObservation {
                id: MessageId(id.into()),
                thread_id: ThreadId(thread.into()),
                labels: set(labels.iter().copied()),
                checkpoint: HistoryId(50),
            },
            blob: BlobRef(format!("{id}.eml")),
            size: 10,
            internal_date: Timestamp(0),
            headers: Box::new(HeaderFacts::default()),
            facts: Some(Box::new(MessageFacts {
                tracking_id: token.map(str::to_owned),
                ..MessageFacts::empty()
            })),
        }
    }

    fn save(mirror: &mut AccountMirror, listed: ListedDraft, at: i64) {
        let write = DraftWrite::Saved {
            draft: listed,
            tracking_id: TOKEN.into(),
            created: true,
        };
        assert_eq!(
            mirror.record_draft(&write, Timestamp(at)),
            DraftRecorded::Done
        );
    }

    fn begin_send(mirror: &mut AccountMirror, id: &str, at: i64) -> DraftRecorded {
        let write = DraftWrite::BeginSend {
            draft: DraftId(id.into()),
            retry_at: Timestamp(at).plus(RETRY),
        };
        mirror.record_draft(&write, Timestamp(at))
    }

    fn listing(mirror: &AccountMirror, listed: Vec<ListedDraft>, claimed_at: i64) -> DraftOp {
        DraftOp::Reconcile(DraftReconcile {
            listed,
            generation: mirror.record.drafts_generation,
            claimed_at: Timestamp(claimed_at),
            grace: GRACE,
            every: Duration::from_mins(15),
        })
    }

    fn reconcile(mirror: &mut AccountMirror, listed: Vec<ListedDraft>, at: i64) {
        let mut c = commit(mirror, at);
        c.drafts = vec![listing(mirror, listed, at)];
        apply(mirror, &c);
    }

    fn draft_changes(mirror: &AccountMirror) -> Vec<Change> {
        mirror
            .log
            .iter()
            .map(|(_, change)| change.clone())
            .filter(|change| {
                matches!(
                    change,
                    Change::DraftSaved { .. }
                        | Change::DraftRemoved { .. }
                        | Change::DraftSent { .. }
                )
            })
            .collect()
    }

    fn refresh_queued(mirror: &AccountMirror) -> bool {
        mirror.jobs.contains_key(&JobKind::RefreshDrafts)
    }

    fn state(mirror: &AccountMirror, id: &str) -> Option<DraftState> {
        mirror.drafts.get(&DraftId(id.into())).map(|r| r.state)
    }

    #[test]
    fn a_local_save_points_the_draft_at_its_new_message() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        save(&mut mirror, draft("r1", "m2", "t1"), 2);
        let row = &mirror.drafts[&DraftId("r1".into())];
        assert_eq!(row.message, MessageId("m2".into()));
        assert_eq!(row.tracking_id.as_deref(), Some(TOKEN));
        assert_eq!(row.state, DraftState::Saved);
        assert_eq!(
            mirror.record.drafts_generation, 2,
            "each write fences older listings"
        );
        assert_eq!(
            draft_changes(&mirror),
            vec![
                saved(&draft("r1", "m1", "t1"), DraftOrigin::Local),
                saved(&draft("r1", "m2", "t1"), DraftOrigin::Local),
            ]
        );
        save(&mut mirror, draft("r1", "m2", "t1"), 3);
        assert_eq!(
            draft_changes(&mirror).len(),
            2,
            "the same answer twice logs once"
        );
        assert!(!refresh_queued(&mirror));
    }

    #[test]
    fn a_draft_message_no_row_points_at_schedules_a_refresh() {
        let mut mirror = mirror();
        let mut c = commit(&mirror, 1_000);
        c.messages = vec![observe("m9", "t9", &["DRAFT"], 10)];
        apply(&mut mirror, &c);
        assert!(refresh_queued(&mirror));
        assert_eq!(
            mirror.jobs[&JobKind::RefreshDrafts].state,
            crate::jobs::JobState::Ready {
                due_at: Timestamp(11_000)
            },
            "after draft_refresh_delay, so a burst of saves shares one listing"
        );
        assert_eq!(
            mirror.record.drafts_generation, 1,
            "a listing in flight is out of date"
        );
        let mut other = self::mirror();
        let mut c = commit(&other, 1_000);
        c.messages = vec![observe("m1", "t1", &["INBOX"], 10)];
        apply(&mut other, &c);
        assert!(!refresh_queued(&other), "not a draft");
    }

    #[test]
    fn a_tombstone_under_a_draft_marks_it_stale_and_schedules_a_refresh() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        let mut c = commit(&mirror, 2);
        c.messages = vec![tombstone("m1", 20)];
        apply(&mut mirror, &c);
        assert_eq!(
            state(&mirror, "r1"),
            Some(DraftState::Stale),
            "kept until the list says"
        );
        assert!(refresh_queued(&mirror));
    }

    #[test]
    fn our_own_write_seen_again_in_history_schedules_nothing() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        let mut c = commit(&mirror, 2);
        c.messages = vec![observe("m1", "t1", &["DRAFT"], 10)];
        apply(&mut mirror, &c);
        save(&mut mirror, draft("r1", "m2", "t1"), 3);
        let mut c = commit(&mirror, 4);
        c.messages = vec![tombstone("m1", 11), observe("m2", "t1", &["DRAFT"], 12)];
        apply(&mut mirror, &c);
        assert!(!refresh_queued(&mirror), "{:?}", mirror.jobs);
        assert_eq!(state(&mirror, "r1"), Some(DraftState::Saved));
    }

    #[test]
    fn a_listing_naming_a_tombstoned_message_is_not_applied() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m0", "t1"), 0);
        let mut c = commit(&mirror, 1);
        c.messages = vec![tombstone("m1", 5)];
        apply(&mut mirror, &c);
        // r1 is listed late, at a message the mirror knows is deleted: that
        // entry says nothing (the row keeps its message); r2 applies.
        let mut c = commit(&mirror, 2);
        c.drafts = vec![listing(
            &mirror,
            vec![draft("r1", "m1", "t1"), draft("r2", "m2", "t2")],
            2,
        )];
        apply(&mut mirror, &c);
        assert_eq!(
            mirror.drafts.get(&DraftId("r1".into())).map(|r| &r.message),
            Some(&MessageId("m0".into()))
        );
        assert!(mirror.drafts.contains_key(&DraftId("r2".into())));
        // A listing of another generation is fenced out, and three in a row
        // back off to the periodic interval.
        for (n, at) in [(1, 3), (2, 4), (3, 5)] {
            mirror.jobs.clear();
            let mut c = commit(&mirror, at);
            c.drafts = vec![DraftOp::Reconcile(DraftReconcile {
                listed: Vec::new(),
                generation: mirror.record.drafts_generation + 1,
                claimed_at: Timestamp(at),
                grace: GRACE,
                every: Duration::from_mins(15),
            })];
            if n > 1 {
                mirror.jobs.insert(
                    JobKind::RefreshDrafts,
                    crate::jobs::Job {
                        kind: JobKind::RefreshDrafts,
                        priority: Priority::Live,
                        failed_attempts: n - 1,
                        state: crate::jobs::JobState::Running(crate::jobs::AttemptId(n.into())),
                    },
                );
                c.claim = Some(crate::jobs::JobClaim {
                    kind: JobKind::RefreshDrafts,
                    attempt: crate::jobs::AttemptId(n.into()),
                });
                c.jobs = vec![crate::jobs::JobOp::Finish {
                    claim: crate::jobs::JobClaim {
                        kind: JobKind::RefreshDrafts,
                        attempt: crate::jobs::AttemptId(n.into()),
                    },
                    outcome: crate::jobs::JobOutcome::Done,
                }];
            }
            apply(&mut mirror, &c);
            assert_eq!(mirror.drafts.len(), 2, "fenced out: nothing removed");
            let job = &mirror.jobs[&JobKind::RefreshDrafts];
            assert_eq!(job.failed_attempts, n);
            let due = if n >= STALE_LISTINGS {
                Timestamp(at).plus(Duration::from_mins(15))
            } else {
                Timestamp(at).plus(Duration::from_secs(10))
            };
            assert_eq!(job.state, crate::jobs::JobState::Ready { due_at: due });
        }
    }

    #[test]
    fn a_listed_draft_whose_message_the_mirror_never_saw_is_verified() {
        let mut mirror = mirror();
        let mut c = commit(&mirror, 1);
        c.messages = vec![observe("m1", "t1", &["DRAFT"], 5)];
        apply(&mut mirror, &c);
        mirror.jobs.clear();
        reconcile(
            &mut mirror,
            vec![draft("r1", "m1", "t1"), draft("r2", "m2", "t2")],
            2,
        );
        assert!(
            mirror
                .jobs
                .contains_key(&JobKind::Verify(MessageId("m2".into())))
        );
        assert!(
            !mirror
                .jobs
                .contains_key(&JobKind::Verify(MessageId("m1".into()))),
            "a known message needs nothing"
        );
    }

    #[test]
    fn a_draft_missing_from_a_listing_is_removed() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        reconcile(&mut mirror, Vec::new(), 2);
        assert!(mirror.drafts.is_empty());
        assert_eq!(
            draft_changes(&mirror).last(),
            Some(&Change::DraftRemoved {
                draft_id: DraftId("r1".into()),
                last_message_id: MessageId("m1".into()),
                reason: RemovedReason::Gone,
            })
        );
    }

    #[test]
    fn a_sending_draft_missing_from_a_listing_waits_for_confirmation() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        assert!(matches!(
            begin_send(&mut mirror, "r1", 1_000),
            DraftRecorded::Started { retry: false, .. }
        ));
        reconcile(&mut mirror, Vec::new(), 2_000);
        assert!(
            matches!(state(&mirror, "r1"), Some(DraftState::Sending(_))),
            "the request may still be in flight"
        );
        reconcile(&mut mirror, Vec::new(), 61_000);
        assert!(matches!(
            state(&mirror, "r1"),
            Some(DraftState::SendUnconfirmed(_))
        ));
        assert!(
            !draft_changes(&mirror)
                .iter()
                .any(|c| matches!(c, Change::DraftRemoved { .. })),
            "not removed: a SENT import or the grace period settles it"
        );
    }

    #[test]
    fn a_sent_import_carrying_the_drafts_tracking_id_settles_the_send() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        begin_send(&mut mirror, "r1", 1_000);
        let mut c = commit(&mirror, 2_000);
        c.messages = vec![sent_import("s1", "t1", &["SENT"], Some(TOKEN))];
        apply(&mut mirror, &c);
        assert!(mirror.drafts.is_empty());
        assert_eq!(
            draft_changes(&mirror).last(),
            Some(&Change::DraftSent {
                draft_id: DraftId("r1".into()),
                message_id: MessageId("s1".into()),
                thread_id: ThreadId("t1".into()),
            })
        );
    }

    #[test]
    fn a_lost_send_is_matched_by_tracking_id_in_any_thread() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        begin_send(&mut mirror, "r1", 1_000);
        let mut c = commit(&mirror, 2_000);
        c.messages = vec![
            sent_import("x1", "t1", &["INBOX"], Some(TOKEN)),
            sent_import("x2", "t1", &["SENT"], Some("another-token-0123456")),
            sent_import("x3", "t1", &["SENT"], None),
        ];
        // An import whose facts the extractor could not give (or a row
        // from before facts existed) carries no token: it settles nothing,
        // and the send ends unconfirmed after the grace period.
        let mut without_facts = sent_import("x4", "t1", &["SENT"], Some(TOKEN));
        if let MessageOp::MarkImported { facts, .. } = &mut without_facts {
            *facts = None;
        }
        c.messages.push(without_facts);
        apply(&mut mirror, &c);
        assert!(
            matches!(state(&mirror, "r1"), Some(DraftState::Sending(_))),
            "only a SENT message whose facts carry the draft's token settles it"
        );
        let mut c = commit(&mirror, 3_000);
        c.messages = vec![sent_import(
            "s1",
            "t-elsewhere",
            &["SENT", "INBOX"],
            Some(TOKEN),
        )];
        apply(&mut mirror, &c);
        assert_eq!(
            draft_changes(&mirror).last(),
            Some(&Change::DraftSent {
                draft_id: DraftId("r1".into()),
                message_id: MessageId("s1".into()),
                thread_id: ThreadId("t-elsewhere".into()),
            })
        );
    }

    #[test]
    fn an_unconfirmed_send_ends_as_removed_after_the_grace_period() {
        let mut mirror = mirror();
        let mut c = commit(&mirror, 0);
        c.cursor = Some(crate::commit::CursorMove {
            expected: None,
            to: HistoryId(100),
        });
        c.sync = Some(crate::state::SyncState::Live);
        apply(&mut mirror, &c);
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        begin_send(&mut mirror, "r1", 1_000);
        reconcile(&mut mirror, Vec::new(), 61_000);
        let after_grace = 1_000 + 3_600_000;
        reconcile(&mut mirror, Vec::new(), after_grace);
        assert!(
            matches!(state(&mirror, "r1"), Some(DraftState::SendUnconfirmed(_))),
            "the cursor has not moved past the attempt: its SENT message may be coming"
        );
        let mut c = commit(&mirror, after_grace);
        c.cursor = Some(crate::commit::CursorMove {
            expected: Some(HistoryId(100)),
            to: HistoryId(120),
        });
        apply(&mut mirror, &c);
        reconcile(&mut mirror, Vec::new(), after_grace - 1);
        assert!(
            state(&mirror, "r1").is_some(),
            "not before the grace period"
        );
        reconcile(&mut mirror, Vec::new(), after_grace);
        assert_eq!(state(&mirror, "r1"), None);
        assert_eq!(
            draft_changes(&mirror).last(),
            Some(&Change::DraftRemoved {
                draft_id: DraftId("r1".into()),
                last_message_id: MessageId("m1".into()),
                reason: RemovedReason::SendUnconfirmed,
            })
        );
    }

    #[test]
    fn a_still_listed_sending_draft_returns_to_saved() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        begin_send(&mut mirror, "r1", 1_000);
        reconcile(&mut mirror, vec![draft("r1", "m1", "t1")], 30_000);
        assert!(
            matches!(state(&mirror, "r1"), Some(DraftState::Sending(_))),
            "claimed while the request may be in flight: proves nothing"
        );
        reconcile(&mut mirror, vec![draft("r1", "m1", "t1")], 61_000);
        assert_eq!(state(&mirror, "r1"), Some(DraftState::Saved));
        assert!(matches!(
            begin_send(&mut mirror, "r1", 62_000),
            DraftRecorded::Started { retry: false, .. }
        ));
    }

    #[test]
    fn a_refresh_that_changes_nothing_appends_no_feed_entry() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        let entries = mirror.log.len();
        let generation = mirror.record.drafts_generation;
        let mut c = commit(&mirror, 2);
        c.drafts = vec![listing(&mirror, vec![draft("r1", "m1", "t1")], 2)];
        c.account_ops = vec![crate::commit::AccountOp::SetDraftsRefreshedAt(Timestamp(2))];
        apply(&mut mirror, &c);
        assert_eq!(mirror.log.len(), entries);
        assert_eq!(mirror.record.drafts_generation, generation);
        assert_eq!(mirror.record.drafts_refreshed_at, Some(Timestamp(2)));
        reconcile(
            &mut mirror,
            vec![draft("r1", "m2", "t1"), draft("r2", "m3", "t3")],
            3,
        );
        assert_eq!(
            draft_changes(&mirror)[1..],
            [
                saved(&draft("r1", "m2", "t1"), DraftOrigin::Remote),
                saved(&draft("r2", "m3", "t3"), DraftOrigin::Remote),
            ]
        );
    }

    #[test]
    fn a_sent_message_the_mirror_has_not_seen_is_verified() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        let DraftRecorded::Started { draft: row, .. } = begin_send(&mut mirror, "r1", 1_000) else {
            panic!("started");
        };
        let started = row.state.attempt().map(|a| a.started).expect("an attempt");
        let sent = DraftWrite::EndSend {
            draft: DraftId("r1".into()),
            started,
            end: SendEnd::Sent {
                message: MessageId("s1".into()),
                thread_id: ThreadId("t1".into()),
            },
        };
        assert_eq!(
            mirror.record_draft(&sent, Timestamp(2_000)),
            DraftRecorded::Done
        );
        assert!(
            mirror
                .jobs
                .contains_key(&JobKind::Verify(MessageId("s1".into()))),
            "fetched at once, whatever history and listings miss"
        );
    }

    #[test]
    fn a_send_is_begun_once_and_retried_only_after_its_request_timed_out() {
        let mut mirror = mirror();
        assert_eq!(begin_send(&mut mirror, "r1", 0), DraftRecorded::NotIndexed);
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        let DraftRecorded::Started { draft: row, .. } = begin_send(&mut mirror, "r1", 1_000) else {
            panic!("started");
        };
        let started = row.state.attempt().map(|a| a.started).expect("an attempt");
        assert_eq!(
            begin_send(&mut mirror, "r1", 2_000),
            DraftRecorded::InProgress {
                retry_at: Timestamp(61_000)
            }
        );
        assert!(matches!(
            begin_send(&mut mirror, "r1", 61_000),
            DraftRecorded::Started { retry: true, .. }
        ));
        let stale_end = DraftWrite::EndSend {
            draft: DraftId("r1".into()),
            started,
            end: SendEnd::Unsent,
        };
        assert_eq!(
            mirror.record_draft(&stale_end, Timestamp(62_000)),
            DraftRecorded::Superseded,
            "the retry holds the row now"
        );
    }

    #[test]
    fn a_removal_leaves_a_row_whose_send_is_unsettled() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        begin_send(&mut mirror, "r1", 1_000);
        // An update or delete answered 404 while the send consumed the
        // draft: the send's own record settles the row, not the 404.
        for reason in [RemovedReason::Gone, RemovedReason::Deleted] {
            let removal = DraftWrite::Removed {
                draft: DraftId("r1".into()),
                reason,
            };
            assert_eq!(
                mirror.record_draft(&removal, Timestamp(2_000)),
                DraftRecorded::Done
            );
            assert!(matches!(state(&mirror, "r1"), Some(DraftState::Sending(_))));
        }
        assert_eq!(draft_changes(&mirror).len(), 1, "only the save");
    }

    #[test]
    fn an_update_answer_after_the_send_settled_does_not_recreate_the_row() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        let DraftRecorded::Started { draft: row, .. } = begin_send(&mut mirror, "r1", 1_000) else {
            panic!("started");
        };
        let started = row.state.attempt().map(|a| a.started).expect("an attempt");
        let sent = DraftWrite::EndSend {
            draft: DraftId("r1".into()),
            started,
            end: SendEnd::Sent {
                message: MessageId("s1".into()),
                thread_id: ThreadId("t1".into()),
            },
        };
        mirror.record_draft(&sent, Timestamp(2_000));
        let late = DraftWrite::Saved {
            draft: draft("r1", "m2", "t1"),
            tracking_id: TOKEN.into(),
            created: false,
        };
        assert_eq!(
            mirror.record_draft(&late, Timestamp(3_000)),
            DraftRecorded::Done
        );
        assert!(mirror.drafts.is_empty(), "not resurrected");
        assert!(matches!(
            draft_changes(&mirror).last(),
            Some(Change::DraftSent { .. })
        ));
    }

    #[test]
    fn the_rows_token_follows_its_current_messages_import() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        // Edited elsewhere: the listing re-points the row and clears the
        // token it can no longer vouch for.
        reconcile(&mut mirror, vec![draft("r1", "m2", "t1")], 2);
        let token = |m: &AccountMirror| {
            m.drafts
                .get(&DraftId("r1".into()))
                .and_then(|r| r.tracking_id.clone())
        };
        assert_eq!(token(&mirror), None);
        // The import of its current message tells what Gmail holds.
        let mut c = commit(&mirror, 3);
        c.messages = vec![sent_import(
            "m2",
            "t1",
            &["DRAFT"],
            Some("other-token-0123456"),
        )];
        apply(&mut mirror, &c);
        assert_eq!(token(&mirror).as_deref(), Some("other-token-0123456"));
        reconcile(&mut mirror, vec![draft("r1", "m3", "t1")], 4);
        let mut c = commit(&mirror, 5);
        c.messages = vec![sent_import("m3", "t1", &["DRAFT"], None)];
        apply(&mut mirror, &c);
        assert_eq!(token(&mirror), None, "no header: cleared");
    }

    #[test]
    fn a_row_repointed_at_an_imported_message_takes_its_token() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        // Edited elsewhere; the new message is imported before the listing.
        let mut c = commit(&mirror, 2);
        c.messages = vec![sent_import("m2", "t1", &["DRAFT"], Some(TOKEN))];
        apply(&mut mirror, &c);
        reconcile(&mut mirror, vec![draft("r1", "m2", "t1")], 3);
        assert_eq!(
            mirror
                .drafts
                .get(&DraftId("r1".into()))
                .and_then(|r| r.tracking_id.as_deref()),
            Some(TOKEN)
        );
    }

    #[test]
    fn two_rows_with_one_token_settle_nothing() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        // Another client copied the header into a second draft.
        reconcile(
            &mut mirror,
            vec![draft("r1", "m1", "t1"), draft("r2", "m2", "t2")],
            2,
        );
        let mut c = commit(&mirror, 3);
        c.messages = vec![sent_import("m2", "t2", &["DRAFT"], Some(TOKEN))];
        apply(&mut mirror, &c);
        begin_send(&mut mirror, "r1", 1_000);
        let mut c = commit(&mirror, 2_000);
        c.messages = vec![sent_import("s1", "t1", &["SENT"], Some(TOKEN))];
        apply(&mut mirror, &c);
        assert!(
            matches!(state(&mirror, "r1"), Some(DraftState::Sending(_))),
            "ambiguous: left to the listing and the grace period"
        );
        assert!(
            !draft_changes(&mirror)
                .iter()
                .any(|c| matches!(c, Change::DraftSent { .. }))
        );
    }

    #[test]
    fn a_reimported_older_sent_message_settles_nothing() {
        let mut mirror = mirror();
        // An earlier send of a draft with this token, long settled.
        let mut c = commit(&mirror, 1);
        c.messages = vec![sent_import("s0", "t0", &["SENT"], Some(TOKEN))];
        apply(&mut mirror, &c);
        save(&mut mirror, draft("r1", "m1", "t1"), 2);
        begin_send(&mut mirror, "r1", 1_000);
        // The old message is imported again (a verification, a refetch).
        let mut c = commit(&mirror, 2_000);
        c.messages = vec![sent_import("s0", "t0", &["SENT"], Some(TOKEN))];
        apply(&mut mirror, &c);
        assert!(matches!(state(&mirror, "r1"), Some(DraftState::Sending(_))));
    }

    #[test]
    fn a_listing_lists_again_once_an_unsettled_send_has_certainly_ended() {
        let mut mirror = mirror();
        save(&mut mirror, draft("r1", "m1", "t1"), 1);
        begin_send(&mut mirror, "r1", 1_000);
        mirror.jobs.clear();
        reconcile(&mut mirror, vec![draft("r1", "m1", "t1")], 2_000);
        assert!(matches!(state(&mirror, "r1"), Some(DraftState::Sending(_))));
        assert_eq!(
            mirror.jobs[&JobKind::RefreshDrafts].state,
            crate::jobs::JobState::Ready {
                due_at: Timestamp(1_000).plus(RETRY)
            }
        );
        // A listing asked for earlier keeps its time; a later one is
        // brought forward.
        let refresh = |at: i64| DraftWrite::Refresh { at: Timestamp(at) };
        mirror.record_draft(&refresh(90_000), Timestamp(2_001));
        mirror.record_draft(&refresh(5_000), Timestamp(2_002));
        assert_eq!(
            mirror.jobs[&JobKind::RefreshDrafts].state,
            crate::jobs::JobState::Ready {
                due_at: Timestamp(5_000)
            }
        );
    }
}
