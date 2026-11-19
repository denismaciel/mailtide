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
use mailtide::ids::{AccountId, EngineVersion, Epoch, MessageId, Timestamp};
use mailtide::jobs::{AttemptId, ClaimedJob};
use mailtide::mirror::MessageView;
use mailtide::outbox::ActionRequest;
use mailtide::revision::{ChangePage, StateSeq};
use mailtide::state::StoreSnapshot;

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
}

/// The store contract.
pub trait Store: Send + Sync {
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

    /// Claim up to `limit` jobs due at `now` for `attempt`, highest priority
    /// first (see [`mailtide::jobs::pick_next`]).
    fn claim_jobs(
        &self,
        account: &AccountId,
        epoch: Epoch,
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
    ) -> impl Future<Output = Result<Vec<ClaimedJob>, StoreError>> + Send;

    /// Changes after `since`, for consumers.
    fn changes(
        &self,
        account: &AccountId,
        since: StateSeq,
        limit: u32,
    ) -> impl Future<Output = Result<ChangePage, StoreError>> + Send;

    /// Admit a user action without an engine (no epoch fence), atomically;
    /// the current engine delivers it. Returns how many messages got new
    /// intents (0 for unknown or draft targets, or nothing new).
    fn admit(
        &self,
        account: &AccountId,
        request: &ActionRequest,
        now: Timestamp,
    ) -> impl Future<Output = Result<u32, StoreError>> + Send;

    /// How hosts see these messages; unknown ids are left out.
    fn messages(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> impl Future<Output = Result<Vec<MessageView>, StoreError>> + Send;
}
