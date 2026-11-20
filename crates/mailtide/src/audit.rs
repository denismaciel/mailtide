//! The periodic mailbox completeness audit (SPEC §6.3).

use std::time::Duration;

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
        after: Option<String>,
        /// The audit compared the full listing. An abandoned audit
        /// clears only its temporary state.
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
    /// Account incarnation at claim time.
    pub account_generation: u64,
    /// The audit state when the step started.
    pub expected: Option<AuditProgress>,
    /// The work to apply.
    pub step: AuditStep,
    /// When the step ran.
    pub now: Timestamp,
    /// Maximum delay before starting a replacement audit.
    pub interval: Duration,
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
    /// Retain the current continuation and wait before asking again.
    Defer(Timestamp),
    /// A transient failure retried this step at the supplied time.
    Retry(Timestamp),
    /// Abandon the audit and clean its temporary state.
    Abandon(AuditAbandonment),
}

/// Counts reported for a committed audit step.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuditReport {
    /// Gmail listed these ids, which had neither a row nor a tombstone.
    pub found_missing: u32,
    /// The mirror had these rows, which Gmail did not list.
    pub found_unlisted: u32,
    /// Why the audit was abandoned in this run.
    pub abandoned: Option<AuditAbandonment>,
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
        /// Earliest next request when the audit is backing off.
        retry_at: Option<Timestamp>,
    },
    /// Superseded, or no longer applicable to the account.
    Rejected(Rejection),
}

impl AuditRequest {
    /// Check the epoch, lifecycle, continuation and page bound before writing.
    pub fn check(
        &self,
        epoch: Epoch,
        account_generation: u64,
        live: bool,
        progress: Option<&AuditProgress>,
    ) -> Result<(), Rejection> {
        if epoch != self.epoch || account_generation != self.account_generation {
            return Err(Rejection::StaleEpoch);
        }
        if !live || self.expected.as_ref() != progress {
            return Err(Rejection::InvalidTransition);
        }
        let valid = match (&self.step, progress) {
            (AuditStep::Page { ids, next }, None | Some(AuditProgress::Listing(_))) => {
                valid_page(ids, next.as_ref())
            }
            (
                AuditStep::Continue,
                Some(AuditProgress::Sweep(_) | AuditProgress::Cleanup { .. }),
            )
            | (
                AuditStep::Defer(_) | AuditStep::Retry(_) | AuditStep::Abandon(_),
                None | Some(AuditProgress::Listing(_)),
            ) => true,
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(Rejection::InvalidTransition)
        }
    }
}

/// Why a completeness scan stopped early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditAbandonment {
    /// The scan exhausted its page allowance.
    PageBudget,
    /// The page repeated a token already seen in this audit.
    RepeatedToken,
    /// A page added no new ids to the scan.
    NoProgress,
    /// The response could not be used as a Gmail page.
    MalformedPage,
    /// Gmail refused a listing request.
    Rejected,
    /// Transient failures exhausted the attempt limit for this step.
    RetryLimit,
}

impl AuditAbandonment {
    /// Stable, payload-free reason for hosts.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PageBudget => "page_budget",
            Self::RepeatedToken => "repeated_token",
            Self::NoProgress => "no_progress",
            Self::MalformedPage => "malformed_page",
            Self::Rejected => "rejected",
            Self::RetryLimit => "retry_limit",
        }
    }
}

/// Durable work and retry limits for the current audit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuditSchedule {
    /// Earliest Gmail retry or replacement audit.
    pub retry_at: Option<Timestamp>,
    abandonments: u32,
    #[serde(default)]
    failures: u32,
    /// Remaining pages of the current listing or sweep.
    pub pages_left: Option<u64>,
}

/// Consecutive transient failures allowed on a listing step.
pub const AUDIT_FAILURE_LIMIT: u32 = 5;

impl AuditSchedule {
    /// Consecutive failures of the current step.
    #[must_use]
    pub fn failures(&self) -> u32 {
        self.failures
    }

    /// Count a failed attempt and remember the latest permitted retry.
    pub fn failed(&mut self, until: Timestamp) -> Option<AuditAbandonment> {
        self.defer(until);
        self.failures = self.failures.saturating_add(1);
        (self.failures >= AUDIT_FAILURE_LIMIT).then_some(AuditAbandonment::RetryLimit)
    }

    /// Add a retry floor without losing a later one.
    pub fn defer(&mut self, until: Timestamp) {
        self.retry_at = Some(self.retry_at.map_or(until, |at| at.max(until)));
    }

    /// Record an abandonment with exponential backoff capped at the interval.
    pub fn abandon(&mut self, request: &AuditRequest) -> AuditProgress {
        self.abandonments = self.abandonments.saturating_add(1);
        self.wait_after_abandonment(request);
        self.pages_left = None;
        self.failures = 0;
        cleanup(false)
    }

    /// Leave the retry floor in the future even if cleanup ran slowly.
    pub fn wait_after_abandonment(&mut self, request: &AuditRequest) {
        let delay = Duration::from_mins(1)
            .saturating_mul(2_u32.pow(self.abandonments.saturating_sub(1).min(31)))
            .min(request.interval);
        self.defer(request.now.plus(delay));
    }

    /// Count one page against a fixed budget sampled when the phase starts.
    pub fn page_completed(&mut self, rows: u64, more: bool) -> Result<(), AuditAbandonment> {
        let budget = self.pages_left.unwrap_or_else(|| {
            rows.div_ceil(u64::try_from(AUDIT_PAGE_SIZE).unwrap_or(u64::MAX))
                .saturating_mul(2)
                .saturating_add(8)
        });
        self.retry_at = None;
        self.failures = 0;
        self.pages_left = more.then(|| budget.saturating_sub(1));
        if more && budget <= 1 {
            Err(AuditAbandonment::PageBudget)
        } else {
            Ok(())
        }
    }
}

/// Check the wire page bound and reject unusable ids and tokens.
#[must_use]
pub fn valid_page(ids: &[MessageId], next: Option<&PageToken>) -> bool {
    ids.len() <= AUDIT_PAGE_SIZE
        && ids.iter().all(|id| !id.0.is_empty())
        && next.is_none_or(|token| !token.0.is_empty())
}

/// Key for a message id in the temporary audit table.
#[must_use]
pub fn message_key(id: &MessageId) -> String {
    format!("m:{}", id.0)
}

/// Key for a continuation in the temporary audit table.
#[must_use]
pub fn token_key(token: &PageToken) -> String {
    format!("t:{}", token.0)
}

/// Prefer the observable paging fault when a page also exhausts its budget.
#[must_use]
pub fn listing_failure(
    repeated: bool,
    stalled: bool,
    bounded: Result<(), AuditAbandonment>,
) -> Option<AuditAbandonment> {
    if repeated {
        Some(AuditAbandonment::RepeatedToken)
    } else if stalled {
        Some(AuditAbandonment::NoProgress)
    } else {
        bounded.err()
    }
}

/// Cleanup stays runnable even when a replacement listing must wait.
#[must_use]
pub fn retry_at(progress: Option<&AuditProgress>, schedule: AuditSchedule) -> Option<Timestamp> {
    if matches!(progress, Some(AuditProgress::Cleanup { .. })) {
        None
    } else {
        schedule.retry_at
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
    ids: &[String],
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abandonment_backoff_doubles_up_to_the_interval() {
        let mut schedule = AuditSchedule::default();
        let interval = Duration::from_mins(3);
        for (attempt, delay) in [60, 120, 180, 180].into_iter().enumerate() {
            let now = Timestamp(i64::try_from(attempt).unwrap() * 1_000_000);
            let request = AuditRequest {
                account: AccountId("a@example.com".into()),
                epoch: Epoch(1),
                account_generation: 1,
                expected: None,
                step: AuditStep::Abandon(AuditAbandonment::Rejected),
                now,
                interval,
            };
            assert_eq!(schedule.abandon(&request), cleanup(false));
            assert_eq!(
                schedule.retry_at,
                Some(now.plus(Duration::from_secs(delay)))
            );
        }
    }
}
