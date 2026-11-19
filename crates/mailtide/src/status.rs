//! Per-account sync status (SPEC §13).

use crate::ids::{AccountId, HistoryId, Timestamp};
use crate::retry::{BlockingFailure, Cooldown};
use crate::state::SyncPhase;

/// What a host shows about an account's sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    /// The account.
    pub account: AccountId,
    /// Lifecycle phase.
    pub phase: SyncPhase,
    /// Why the account is blocked, if it is.
    pub blocked: Option<BlockingFailure>,
    /// The committed cursor.
    pub cursor: Option<HistoryId>,
    /// When the cursor last advanced.
    pub cursor_advanced_at: Option<Timestamp>,
    /// Active cooldown.
    pub cooldown: Option<Cooldown>,
    /// Import progress while bootstrapping or reconciling.
    pub import: Option<ImportProgress>,
}

/// Import progress. Never reports done while work remains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImportProgress {
    /// Messages discovered so far.
    pub discovered: u64,
    /// Messages with content stored.
    pub imported: u64,
    /// Whether discovery has finished (the total is known).
    pub discovery_complete: bool,
}

/// What the store holds about an account, read without claiming it (so a
/// dashboard never preempts a running sync).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountStatus {
    /// The account.
    pub account: AccountId,
    /// The stored lifecycle (with the block reason, if blocked).
    pub sync: crate::state::SyncState,
    /// The engine that last claimed the account.
    pub epoch: crate::ids::Epoch,
    /// The committed cursor.
    pub cursor: Option<HistoryId>,
    /// When the watch expires, if one was set.
    pub watch_expiration: Option<Timestamp>,
    /// When the mailbox last saw activity.
    pub last_activity: Option<Timestamp>,
    /// Active cooldown.
    pub cooldown: Option<Cooldown>,
    /// The change-feed position (pass to `changes` to read from here).
    pub state: crate::revision::StateSeq,
    /// Message rows.
    pub messages: u64,
    /// Messages whose content isn't stored yet.
    pub pending_imports: u64,
    /// Messages whose labels await verification.
    pub unverified: u64,
    /// Durable jobs by state.
    pub jobs: JobCounts,
    /// User actions by state.
    pub actions: ActionCounts,
}

/// Durable jobs by state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct JobCounts {
    /// Waiting to run (due or not).
    pub ready: u64,
    /// Claimed by an engine.
    pub running: u64,
    /// Parked until an operator acts.
    pub blocked: u64,
}

/// User actions (intents) by state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ActionCounts {
    /// Admitted, not yet sent.
    pub pending: u64,
    /// Sent, awaiting confirmation (including unresolved sends).
    pub delivered: u64,
    /// Refused by Gmail for good.
    pub blocked: u64,
}

impl JobCounts {
    /// Count one job.
    pub fn add(&mut self, state: &crate::jobs::JobState) {
        match state {
            crate::jobs::JobState::Ready { .. } => self.ready += 1,
            crate::jobs::JobState::Running(_) => self.running += 1,
            crate::jobs::JobState::Blocked(_) => self.blocked += 1,
        }
    }
}

impl ActionCounts {
    /// Count one intent.
    pub fn add(&mut self, state: &crate::outbox::IntentState) {
        match state {
            crate::outbox::IntentState::Pending => self.pending += 1,
            crate::outbox::IntentState::Delivered { .. }
            | crate::outbox::IntentState::Unresolved => {
                self.delivered += 1;
            }
            crate::outbox::IntentState::Blocked(_) => self.blocked += 1,
        }
    }
}

/// What resuming a blocked account did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// It was blocked; it resumes in this phase, and jobs blocked by the
    /// account problem are ready again.
    Resumed {
        /// The phase it resumes in.
        phase: SyncPhase,
        /// Jobs made ready again.
        jobs: u64,
    },
    /// It wasn't blocked; nothing changed.
    NotBlocked,
    /// The store has never seen the account.
    Unknown,
}

/// What forgetting an account removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgetOutcome {
    /// Everything the store held for it is gone.
    Forgotten {
        /// Message rows deleted.
        messages: u64,
    },
    /// The store has never seen the account.
    Unknown,
}
