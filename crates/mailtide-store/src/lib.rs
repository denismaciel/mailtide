//! Storage for mailtide mirrors.
//!
//! The [`mailtide`] core emits atomic commits of conditional operations; a
//! store applies each in one transaction with epoch, version and cursor
//! fencing, appends the change log in the same transaction, and hands new
//! engines their snapshot. Ships memory, `SQLite` and Postgres stores, their
//! migrations, and a conformance suite every store must pass.
//!
//! SPEC §4, §10.3. Zero counterpart: the Durable Object `SQLite` stores
//! (`lib/mail-*-store.ts`).

use std::future::Future;

use mailtide::commit::{Commit, CommitOutcome};
use mailtide::gmail::Label;
use mailtide::ids::{AccountId, EngineVersion, Epoch, MessageId, ThreadId, Timestamp};
use mailtide::jobs::{AttemptId, ClaimOutcome, Prioritised};
use mailtide::mirror::{Located, MessageView};
use mailtide::outbox::ActionRequest;
use mailtide::revision::{
    AckOutcome, ChangeFeed, ConsumerList, ConsumerName, FeedCursor, FeedPosition, Retention,
};
use mailtide::state::StoreSnapshot;
use mailtide::status::{AccountStatus, ForgetOutcome, ResumeOutcome, TrackOutcome};
use mailtide::thread::ThreadView;

pub mod config;
pub mod conformance;
pub mod memory;
pub mod postgres;
pub mod sql;
pub mod sqlite;

/// A store failure, as distinct from a commit's guard rejecting it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// The store couldn't be reached or failed; nothing was applied.
    Unavailable(String),
    /// The outcome is unknown (e.g. the connection dropped during commit):
    /// the commit may or may not have applied. Rebuild the engine.
    OutcomeUnknown(String),
    /// A guard rejected the operation (e.g. claiming with an older version).
    Rejected(mailtide::commit::Rejection),
    /// Stored data failed validation when read back.
    Corrupt(String),
    /// The store can't be opened with this configuration.
    Misconfigured(String),
    /// The account isn't tracked: mirroring starts only with `track`.
    NotTracked,
    /// The database's schema doesn't match this build: `applied` below
    /// `expected` needs migrations; above it, a newer mailtide migrated the
    /// database and this build must neither read nor write it. Nothing was
    /// read or written.
    SchemaOutOfDate {
        /// The newest migration applied (0: none).
        applied: u32,
        /// The newest this build knows.
        expected: u32,
    },
}

/// The store contract.
pub trait Store: Send + Sync {
    /// Start mirroring `account` (a no-op if it already is), as incarnation
    /// `generation` of its feed: use a value never used for this account
    /// before, e.g. the current time in milliseconds.
    fn track(
        &self,
        account: &AccountId,
        generation: u64,
    ) -> impl Future<Output = Result<TrackOutcome, StoreError>> + Send;

    /// Claim the account for a new engine: bump its epoch, record `version`,
    /// and return the snapshot to build the engine from. Fails if a newer
    /// engine version already wrote to the account.
    fn claim(
        &self,
        account: &AccountId,
        version: EngineVersion,
    ) -> impl Future<Output = Result<StoreSnapshot, StoreError>> + Send;

    /// Apply a commit atomically. Rejections are outcomes, not errors.
    fn apply(
        &self,
        commit: &Commit,
    ) -> impl Future<Output = Result<CommitOutcome, StoreError>> + Send;

    /// Claim up to `limit` jobs of at least `floor` priority due at `now`
    /// for `attempt`, highest priority first (see
    /// [`mailtide::jobs::pick_next_from`]).
    fn claim_jobs(
        &self,
        account: &AccountId,
        epoch: Epoch,
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
        floor: mailtide::quota::Priority,
    ) -> impl Future<Output = Result<ClaimOutcome, StoreError>> + Send;

    /// The change feed after `since`: at most `limit` entries (clamped to
    /// `1..=MAX_FEED_PAGE`), in `(state, index)` order, ending at the last
    /// commit boundary within `limit` and inside a commit only when that
    /// commit alone is larger (see [`mailtide::revision::cut_page`]); or
    /// `TooOld` if entries after `since` were compacted away (it is behind
    /// the floor, or inside the floor commit).
    fn changes(
        &self,
        account: &AccountId,
        since: FeedCursor,
        limit: u32,
    ) -> impl Future<Output = Result<ChangeFeed, StoreError>> + Send;

    /// Drop whole commits of the feed as `retention` allows, under the
    /// account row lock: no further than its bound, nor past the slowest
    /// live consumer (see [`mailtide::revision::compaction_target`]);
    /// cursors at the head are seen at `retention.now` first. Later reads
    /// from before the new floor get `TooOld`. Returns how many entries were
    /// removed. Unfenced: no epoch is involved.
    fn compact(
        &self,
        account: &AccountId,
        retention: &Retention,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

    /// Consumer `consumer` has processed the feed up to `at`: set its cursor
    /// there at `now` (see [`mailtide::mirror::AccountMirror::ack`]), under
    /// the account row lock. Unfenced, and never touches the mirror or the
    /// feed. `NotTracked` for an unknown account.
    fn ack(
        &self,
        account: &AccountId,
        consumer: &ConsumerName,
        at: FeedPosition,
        now: Timestamp,
    ) -> impl Future<Output = Result<AckOutcome, StoreError>> + Send;

    /// The account's registered consumers, in name order, with its
    /// generation and floor (empty, generation 0, for an unknown account).
    fn consumers(
        &self,
        account: &AccountId,
    ) -> impl Future<Output = Result<ConsumerList, StoreError>> + Send;

    /// Forget consumer `consumer`: it no longer holds the log. Whether it
    /// was registered.
    fn drop_consumer(
        &self,
        account: &AccountId,
        consumer: &ConsumerName,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Up to `limit` message ids after `after`, in id order.
    fn message_ids(
        &self,
        account: &AccountId,
        after: Option<&MessageId>,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<MessageId>, StoreError>> + Send;

    /// Admit a user action without an engine (no epoch fence), atomically;
    /// the current engine delivers it. Returns how many messages got new
    /// intents (0 for unknown or draft targets, or nothing new).
    fn admit(
        &self,
        account: &AccountId,
        request: &ActionRequest,
        now: Timestamp,
    ) -> impl Future<Output = Result<u32, StoreError>> + Send;

    /// A user opened these messages: raise their queued body imports to the
    /// Foreground class (see [`mailtide::mirror::AccountMirror::prioritise`]),
    /// atomically and without an engine (no epoch fence, like `admit`): the
    /// change only ever raises a class, so it is safe beside any engine.
    /// Creates no row, job or feed entry. An untracked account answers every
    /// id as unknown.
    fn prioritise(
        &self,
        account: &AccountId,
        ids: &[MessageId],
        now: Timestamp,
    ) -> impl Future<Output = Result<Prioritised, StoreError>> + Send;

    /// What the store holds about an account, read without claiming it (so
    /// it never preempts a running sync). `None` for an unknown account.
    fn status(
        &self,
        account: &AccountId,
    ) -> impl Future<Output = Result<Option<AccountStatus>, StoreError>> + Send;

    /// Every account the store holds.
    fn accounts(&self) -> impl Future<Output = Result<Vec<AccountId>, StoreError>> + Send;

    /// Resume a blocked account after an operator fixed the cause (see
    /// [`mailtide::mirror::AccountMirror::resume`]).
    fn resume(
        &self,
        account: &AccountId,
        now: Timestamp,
    ) -> impl Future<Output = Result<ResumeOutcome, StoreError>> + Send;

    /// Delete everything the store holds for an account (shadow resets,
    /// offboarding). An engine still running for it is fenced out: its
    /// commits are rejected. Blob content is the caller's to delete.
    fn forget(
        &self,
        account: &AccountId,
    ) -> impl Future<Output = Result<ForgetOutcome, StoreError>> + Send;

    /// How hosts see these messages; unknown ids are left out.
    fn messages(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> impl Future<Output = Result<Vec<MessageView>, StoreError>> + Send;

    /// How hosts see these threads, in the order asked: every member with its
    /// view, folded by [`mailtide::thread::view`]. A thread the mirror has no
    /// message in is left out, as is every id of an untracked account; no ids
    /// read nothing. Each answer comes from one consistent read of its rows.
    fn threads(
        &self,
        account: &AccountId,
        ids: &[ThreadId],
    ) -> impl Future<Output = Result<Vec<ThreadView>, StoreError>> + Send;

    /// What the mirror holds for each of `ids` (at most 500), in the order
    /// given: a row with or without its content, a tombstone, or nothing
    /// (see [`mailtide::mirror::AccountMirror::locate`]). A read that claims
    /// nothing, by primary key; an untracked account holds nothing.
    fn locate(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> impl Future<Output = Result<Vec<(MessageId, Located)>, StoreError>> + Send;

    /// The account's label definitions as the mirror last synced them, in id
    /// order: empty before the first label sync, and for an unknown account.
    /// A `LabelDefinitionsChanged` feed entry means they changed: read again.
    fn labels(
        &self,
        account: &AccountId,
    ) -> impl Future<Output = Result<Vec<Label>, StoreError>> + Send;
}
