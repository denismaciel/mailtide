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
