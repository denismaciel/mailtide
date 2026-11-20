//! The periodic mailbox completeness audit (SPEC §6.3).

use crate::commit::Rejection;
use crate::ids::{AccountId, Epoch, MessageId, PageToken, Timestamp};
use crate::jobs::{JobKind, NewJob};
use crate::quota::Priority;

/// The Gmail page size, and the maximum number of ids compared and jobs
/// queued by one audit step.
pub const AUDIT_PAGE_SIZE: usize = 500;

/// How to continue an unfinished audit. The store keeps the ids seen so
/// far separately from reconciliation's seen set.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AuditProgress {
    /// Fetch the next Gmail page with this token.
    Listing(PageToken),
    /// Read mirror rows in primary key order and verify omissions.
    Sweep(Option<MessageId>),
    /// Remove the audit's temporary ids in primary key order.
    Cleanup {
        /// Last id cleared. Advances even if more ids remain.
        after: Option<MessageId>,
        /// The audit compared the full listing. A rejected page token only
        /// clears the temporary ids and restarts the audit.
        complete: bool,
    },
}

/// An atomic, epoch-fenced audit step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRequest {
    /// The audited account.
    pub account: AccountId,
    /// The account claim that performed this step.
    pub epoch: Epoch,
    /// The audit state when the step started.
    pub expected: Option<AuditProgress>,
    /// The work to apply.
    pub step: AuditStep,
    /// When the step ran.
    pub now: Timestamp,
}

/// What to apply to the current audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditStep {
    /// A Gmail page. The next token and its observations commit together.
    Page {
        /// The ids Gmail listed.
        ids: Vec<MessageId>,
        /// The next page, or the end of the Gmail listing.
        next: Option<PageToken>,
    },
    /// Process one page of the mirror sweep or temporary-id cleanup.
    Continue,
    /// Drop an unusable Gmail continuation without sweeping for omissions.
    Restart,
}

/// Counts reported for a committed audit step.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuditReport {
    /// Gmail listed these ids, which had neither a row nor a tombstone.
    pub found_missing: u32,
    /// The mirror had these rows, which Gmail did not list.
    pub found_unlisted: u32,
}

/// The outcome of one fenced audit step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOutcome {
    /// Committed without moving the history cursor or adding feed entries.
    Applied {
        /// When the audit completed, if this was its final step.
        completed_at: Option<Timestamp>,
        /// What this step found.
        report: AuditReport,
    },
    /// Superseded, or no longer applicable to the account.
    Rejected(Rejection),
}

impl AuditRequest {
    /// Check the epoch, lifecycle, continuation and page bound before writing.
    pub fn check(
        &self,
        epoch: Epoch,
        live: bool,
        progress: Option<&AuditProgress>,
    ) -> Result<(), Rejection> {
        if epoch != self.epoch {
            return Err(Rejection::StaleEpoch);
        }
        if !live || self.expected.as_ref() != progress {
            return Err(Rejection::InvalidTransition);
        }
        let valid = match (&self.step, progress) {
            (AuditStep::Page { ids, next }, None) => {
                ids.len() <= AUDIT_PAGE_SIZE && (next.is_none() || !ids.is_empty())
            }
            (AuditStep::Page { ids, next }, Some(AuditProgress::Listing(token))) => {
                ids.len() <= AUDIT_PAGE_SIZE
                    && (next.is_none() || !ids.is_empty() && next.as_ref() != Some(token))
            }
            (
                AuditStep::Continue,
                Some(AuditProgress::Sweep(_) | AuditProgress::Cleanup { .. }),
            )
            | (AuditStep::Restart, Some(AuditProgress::Listing(_))) => true,
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(Rejection::InvalidTransition)
        }
    }
}

/// The audit only queues ordinary, deduplicated Backfill verification jobs.
#[must_use]
pub fn verify(id: MessageId, now: Timestamp) -> NewJob {
    NewJob {
        kind: JobKind::Verify(id),
        priority: Priority::Backfill,
        due_at: now,
    }
}

/// The end of Gmail's listing starts the mirror sweep.
#[must_use]
pub fn after_listing(next: Option<PageToken>) -> AuditProgress {
    next.map_or(AuditProgress::Sweep(None), AuditProgress::Listing)
}

/// Start clearing the audit's temporary ids.
#[must_use]
pub fn cleanup(complete: bool) -> AuditProgress {
    AuditProgress::Cleanup {
        after: None,
        complete,
    }
}

/// The next mirror page, or the beginning of cleanup.
#[must_use]
pub fn after_sweep(ids: &[MessageId]) -> AuditProgress {
    if ids.len() == AUDIT_PAGE_SIZE {
        AuditProgress::Sweep(ids.last().cloned())
    } else {
        cleanup(true)
    }
}

/// The next cleanup page, or the time recorded when it completes.
#[must_use]
pub fn after_cleanup(
    ids: &[MessageId],
    complete: bool,
    now: Timestamp,
) -> (Option<AuditProgress>, Option<Timestamp>) {
    if ids.len() == AUDIT_PAGE_SIZE {
        (
            Some(AuditProgress::Cleanup {
                after: ids.last().cloned(),
                complete,
            }),
            None,
        )
    } else {
        (None, complete.then_some(now))
    }
}
