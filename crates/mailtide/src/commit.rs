//! Atomic commits and the store contract (SPEC §10.3).
//!
//! A commit is a list of *conditional* operations. The store evaluates each
//! condition against its own rows inside one transaction, so the engine needs
//! no copy of the mirror in memory. Guards that span effects live on the
//! commit: the writer's epoch and engine version, the cursor it expects, and
//! the job claim it finishes. If any guard fails, nothing applies
//! (invariants I1, I2, I8). Operations that create obligations enqueue their
//! jobs in the same transaction (review #1), so every message's next step is
//! durable (I6).

use crate::gmail::{HeaderFacts, Label, MessageFacts, MessageObservation};
use crate::ids::{
    AccountId, BlobRef, CommitId, EngineVersion, Epoch, HistoryId, MessageId, Timestamp,
};
use crate::jobs::{JobClaim, JobOp};
use crate::labels::LabelSet;
use crate::outbox::IntentOp;
use crate::quota::Priority;
use crate::reconcile::ListScope;
use crate::retry::Cooldown;
use crate::revision::StateSeq;
use crate::state::SyncState;

/// One atomic unit of change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    /// Identifies the commit in `CommitApplied` / `CommitRejected`.
    pub id: CommitId,
    /// The account.
    pub account: AccountId,
    /// The writer's epoch; stale epochs are rejected.
    pub epoch: Epoch,
    /// The writer's engine version; older versions are rejected.
    pub engine_version: EngineVersion,
    /// Move the cursor, conditionally on its current value.
    pub cursor: Option<CursorMove>,
    /// The job claim this commit finishes; the whole commit applies only
    /// while the claim holds.
    pub claim: Option<JobClaim>,
    /// The new lifecycle state, if it changes.
    pub sync: Option<SyncState>,
    /// Message operations, each conditional.
    pub messages: Vec<MessageOp>,
    /// Label definition changes.
    pub labels: Vec<LabelOp>,
    /// Other account-level changes.
    pub account_ops: Vec<AccountOp>,
    /// Timing for jobs the store enqueues on its own.
    pub job_plan: JobPlan,
    /// Job changes.
    pub jobs: Vec<JobOp>,
    /// Intent changes (phase 6).
    pub intents: Vec<IntentOp>,
}

/// A conditional cursor advance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorMove {
    /// The cursor the engine believes is stored.
    pub expected: Option<HistoryId>,
    /// The new cursor. Never lower than `expected`.
    pub to: HistoryId,
}

/// The timing policy for jobs the store enqueues as part of an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobPlan {
    /// Priority of enqueued jobs.
    pub priority: Priority,
    /// When an import of a non-draft message becomes due.
    pub import_due_at: Timestamp,
    /// When an import of a draft becomes due (drafts churn on every save).
    pub draft_import_due_at: Timestamp,
    /// When verifying an unknown or unlisted message becomes due.
    pub verify_due_at: Timestamp,
    /// When deleting a tombstoned message's raw content becomes due.
    pub cleanup_due_at: Timestamp,
    /// When delivering admitted intents becomes due.
    pub deliver_due_at: Timestamp,
    /// When to verify a delivered change with a `get` if history hasn't
    /// confirmed it.
    pub confirm_due_at: Timestamp,
}

/// An account-level change other than the lifecycle and the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountOp {
    /// Record the watch's expiration.
    SetWatchExpiration(Timestamp),
    /// Record a cooldown.
    SetCooldown(Cooldown),
    /// Clear any cooldown.
    ClearCooldown,
    /// The mailbox saw activity at this time (polling speeds up). Never
    /// moves backwards.
    SetLastActivity(Timestamp),
    /// The label definitions were listed at this time (SPEC §6.6). Never
    /// moves backwards.
    SetLabelsRefreshedAt(Timestamp),
    /// Block the account until an operator acts. The store wraps the state
    /// it holds at apply time (not the engine's view, which may be behind a
    /// commit still in flight), so the stored state stays valid.
    Block(crate::retry::BlockingFailure),
}

/// Why the store may treat a message as deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionEvidence {
    /// A history record deleted it.
    History {
        /// The record's id.
        at: HistoryId,
    },
    /// A `get` returned 404; the deletion happened no later than the
    /// committed cursor at that time.
    VerifiedMissing {
        /// The committed cursor when the 404 arrived.
        no_later_than: HistoryId,
    },
}

impl DeletionEvidence {
    /// The checkpoint the tombstone records.
    #[must_use]
    pub fn checkpoint(self) -> HistoryId {
        match self {
            Self::History { at } => at,
            Self::VerifiedMissing { no_later_than } => no_later_than,
        }
    }
}

/// A message operation. Every variant is applied only if its condition holds;
/// skipped operations are counted, not errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageOp {
    /// A complete observation of the message's labels at a checkpoint. If the
    /// message is unknown and not tombstoned, create it as pending and enqueue
    /// its import; otherwise replace its labels if the checkpoint is not older.
    Observe(MessageObservation),
    /// Add or remove labels if the checkpoint is not older than the stored
    /// one. For a message the store doesn't know (and hasn't tombstoned),
    /// enqueue a verification instead.
    PatchLabels {
        /// The message.
        id: MessageId,
        /// Labels added.
        add: LabelSet,
        /// Labels removed.
        remove: LabelSet,
        /// The history record's id.
        checkpoint: HistoryId,
    },
    /// The raw content is stored: record it with the observation made while
    /// fetching it (labels and checkpoint as in [`MessageOp::Observe`]).
    MarkImported {
        /// What the fetch observed.
        observation: MessageObservation,
        /// Where the raw content is.
        blob: BlobRef,
        /// Raw size in bytes.
        size: u64,
        /// Gmail's `internalDate`.
        internal_date: Timestamp,
        /// Header facts.
        headers: Box<HeaderFacts>,
        /// The facts extracted from the stored bytes, if any.
        facts: Option<Box<MessageFacts>>,
    },
    /// Delete the message and keep a tombstone; cancels its other jobs and
    /// intents and enqueues cleanup of any raw content. Applies even to
    /// unknown ids, so a racing discovery can't resurrect it.
    Tombstone {
        /// The message.
        id: MessageId,
        /// Why it's deleted.
        evidence: DeletionEvidence,
    },
    /// Reconciliation listed the message in `scope` at `baseline`: create it
    /// as pending (with its import) if unknown and not tombstoned, and
    /// remember the scope until [`MessageOp::FinishReconcile`].
    Listed {
        /// The message.
        id: MessageId,
        /// Its thread.
        thread_id: crate::ids::ThreadId,
        /// Where it was listed.
        scope: ListScope,
        /// The reconciliation baseline.
        baseline: HistoryId,
    },
    /// Reconciliation finished at `baseline`: every message whose checkpoint
    /// is older than the baseline gets exactly the labels it was listed under;
    /// messages listed nowhere get a verification. Clears what was remembered.
    FinishReconcile {
        /// The reconciliation baseline.
        baseline: HistoryId,
    },
}

/// A label definition change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelOp {
    /// Replace all label definitions with Gmail's current list. A list equal
    /// to the stored one (in any order) changes nothing and logs nothing.
    ReplaceAll(Vec<Label>),
}

/// The result of applying a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Applied; the account's change log advanced to `state`.
    Applied {
        /// The new change-log position.
        state: StateSeq,
        /// Conditional operations that were skipped.
        skipped: u32,
    },
    /// Not applied; nothing changed.
    Rejected(Rejection),
}

/// Why a store rejected a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    /// Another engine instance took over the account.
    StaleEpoch,
    /// A newer engine version wrote to the account.
    StaleVersion,
    /// The stored cursor isn't the expected one.
    CursorMismatch,
    /// The job claim no longer holds (another attempt took the job).
    StaleClaim,
    /// Applying it would leave the lifecycle and cursor contradicting each
    /// other (a bug in the writer); nothing was applied.
    InvalidTransition,
}
