//! What the engine is built from: a bounded, validated snapshot of the
//! account's durable state (SPEC §10, review C1, review #2).
//!
//! The engine holds only account-level state and in-flight commands, never
//! the mirror itself; per-message checks happen in the store through
//! conditional operations. A new engine built from any store state, fed the
//! remaining events, reaches the same store state as one that never crashed
//! (invariant I11).

use crate::ids::{AccountId, EngineVersion, Epoch, HistoryId, Timestamp};
use crate::labels::LabelSet;
use crate::reconcile::ReconcileProgress;
use crate::retry::{BlockingFailure, Cooldown};

/// Where an account is in its lifecycle, as one value (no contradicting
/// fields).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SyncState {
    /// No cursor yet: the initial import is running.
    Bootstrapping(ReconcileProgress),
    /// Following history from the cursor.
    Live,
    /// History expired: rebuilding from a snapshot.
    Reconciling(ReconcileProgress),
    /// Stopped until an operator acts; resumes into `resume`.
    Blocked {
        /// Why.
        reason: BlockingFailure,
        /// The state to resume.
        resume: Box<SyncState>,
    },
}

/// The coarse phase, for status and display.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPhase {
    /// The initial import is running.
    Bootstrapping,
    /// Following history.
    Live,
    /// Rebuilding after history expired.
    Reconciling,
    /// Stopped until an operator acts.
    Blocked,
}

impl SyncState {
    /// The coarse phase.
    #[must_use]
    pub fn phase(&self) -> SyncPhase {
        match self {
            Self::Bootstrapping(_) => SyncPhase::Bootstrapping,
            Self::Live => SyncPhase::Live,
            Self::Reconciling(_) => SyncPhase::Reconciling,
            Self::Blocked { .. } => SyncPhase::Blocked,
        }
    }

    /// The reconciliation in progress, if any.
    #[must_use]
    pub fn reconcile(&self) -> Option<&ReconcileProgress> {
        match self {
            Self::Bootstrapping(progress) | Self::Reconciling(progress) => Some(progress),
            Self::Live | Self::Blocked { .. } => None,
        }
    }
}

/// The durable account state a store hands to a new engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreSnapshot {
    /// The account.
    pub account: AccountId,
    /// The epoch this engine instance claimed (higher than any before).
    pub epoch: Epoch,
    /// The newest engine version that has written to the account.
    pub engine_version: EngineVersion,
    /// The committed cursor; `None` before the first reconciliation finishes.
    pub cursor: Option<HistoryId>,
    /// The lifecycle.
    pub sync: SyncState,
    /// When the watch expires, if one is active.
    pub watch_expiration: Option<Timestamp>,
    /// Last user activity, for active/idle polling.
    pub last_activity: Option<Timestamp>,
    /// Active cooldown, if Gmail asked us to slow down.
    pub cooldown: Option<Cooldown>,
    /// Seed for deterministic jitter, stored per account.
    pub rng_seed: u64,
    /// When the label definitions were last listed; `None` before the first
    /// listing (SPEC §6.6).
    pub labels_refreshed_at: Option<Timestamp>,
    /// When the last mailbox completeness audit finished.
    pub audited_at: Option<Timestamp>,
    /// The next page of an unfinished audit.
    pub audit_progress: Option<crate::audit::AuditProgress>,
    /// Persisted audit retry time and page budget.
    pub audit_schedule: crate::audit::AuditSchedule,
    /// The account incarnation, changed when the account is tracked again.
    pub account_generation: u64,
    /// The generation of the stored label definitions.
    pub label_generation: u64,
    /// The ids of the stored label definitions, to notice ids they lack.
    pub label_ids: LabelSet,
    /// When the drafts were last listed; `None` before the first listing
    /// (SPEC §7a).
    pub drafts_refreshed_at: Option<Timestamp>,
}

/// A snapshot whose fields contradict each other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidSnapshot {
    /// Live without a cursor.
    LiveWithoutCursor,
    /// Bootstrapping although a cursor exists.
    BootstrappingWithCursor,
    /// Blocked inside blocked.
    NestedBlock,
}

/// A snapshot that passed [`StoreSnapshot::validate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidSnapshot(StoreSnapshot);

impl ValidSnapshot {
    /// The snapshot.
    #[must_use]
    pub fn into_inner(self) -> StoreSnapshot {
        self.0
    }
}

impl StoreSnapshot {
    /// Check that the fields agree with each other.
    pub fn validate(self) -> Result<ValidSnapshot, InvalidSnapshot> {
        check(&self.sync, self.cursor)?;
        Ok(ValidSnapshot(self))
    }
}

fn check(sync: &SyncState, cursor: Option<HistoryId>) -> Result<(), InvalidSnapshot> {
    match (sync, cursor) {
        (SyncState::Live, None) => Err(InvalidSnapshot::LiveWithoutCursor),
        (SyncState::Bootstrapping(_), Some(_)) => Err(InvalidSnapshot::BootstrappingWithCursor),
        (SyncState::Blocked { resume, .. }, cursor) => match resume.as_ref() {
            SyncState::Blocked { .. } => Err(InvalidSnapshot::NestedBlock),
            other => check(other, cursor),
        },
        (SyncState::Live, Some(_))
        | (SyncState::Bootstrapping(_), None)
        | (SyncState::Reconciling(_), _) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::ENGINE_VERSION;
    use crate::reconcile::ReconcileReason;

    fn snapshot(cursor: Option<u64>, sync: SyncState) -> StoreSnapshot {
        StoreSnapshot {
            account: AccountId("a@example.com".into()),
            epoch: Epoch(1),
            engine_version: ENGINE_VERSION,
            cursor: cursor.map(HistoryId),
            sync,
            watch_expiration: None,
            last_activity: None,
            cooldown: None,
            rng_seed: 0,
            labels_refreshed_at: None,
            audited_at: None,
            audit_progress: None,
            audit_schedule: crate::audit::AuditSchedule::default(),
            account_generation: 1,
            label_generation: 0,
            label_ids: LabelSet::new(),
            drafts_refreshed_at: None,
        }
    }

    #[test]
    fn contradicting_snapshots_are_rejected() {
        assert_eq!(
            snapshot(None, SyncState::Live).validate(),
            Err(InvalidSnapshot::LiveWithoutCursor)
        );
        let bootstrapping =
            SyncState::Bootstrapping(ReconcileProgress::begin(ReconcileReason::Bootstrap));
        assert_eq!(
            snapshot(Some(1), bootstrapping.clone()).validate(),
            Err(InvalidSnapshot::BootstrappingWithCursor)
        );
        assert!(snapshot(None, bootstrapping).validate().is_ok());
        assert!(snapshot(Some(1), SyncState::Live).validate().is_ok());
    }
}
