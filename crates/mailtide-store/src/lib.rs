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
use mailtide::ids::{AccountId, EngineVersion, Epoch, MessageId, Timestamp};
use mailtide::jobs::{AttemptId, ClaimOutcome};
use mailtide::mirror::MessageView;
use mailtide::outbox::ActionRequest;
use mailtide::revision::{ChangeFeed, StateSeq};
use mailtide::state::StoreSnapshot;
use mailtide::status::{AccountStatus, ForgetOutcome, ResumeOutcome, TrackOutcome};

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

    /// The change feed after `since`, or `TooOld` if it was compacted away.
    fn changes(
        &self,
        account: &AccountId,
        since: StateSeq,
        limit: u32,
    ) -> impl Future<Output = Result<ChangeFeed, StoreError>> + Send;

    /// Drop feed entries at or before `through` once every consumer has
    /// processed them; later reads from before it get `TooOld`. Returns how
    /// many were removed.
    fn compact(
        &self,
        account: &AccountId,
        through: StateSeq,
    ) -> impl Future<Output = Result<u64, StoreError>> + Send;

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

    /// The account's label definitions as the mirror last synced them, in id
    /// order: empty before the first label sync, and for an unknown account.
    /// A `LabelDefinitionsChanged` feed entry means they changed: read again.
    fn labels(
        &self,
        account: &AccountId,
    ) -> impl Future<Output = Result<Vec<Label>, StoreError>> + Send;
}
