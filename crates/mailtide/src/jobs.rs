//! Durable work items: body import, verification, blob cleanup (SPEC §6.4).
//!
//! Deduplicated per kind and message; priorities `foreground > live >
//! backfill`. A job is ready, running under one claim, or blocked. Failed
//! attempts are counted separately from claims (review #8). Commits that
//! finish a job carry its claim, and apply only while the claim still holds.
//! Every message seen is in exactly one recorded state (I6).
//!
//! Zero counterpart: `mailbox_import_jobs` and `lib/gmail-priority.ts`.

use crate::ids::{MessageId, Timestamp};
use crate::quota::Priority;
use crate::retry::{BlockingFailure, RetryableFailure};

/// What a job does.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JobKind {
    /// Fetch raw content and record it.
    Import(MessageId),
    /// Fetch labels for a message the store can't vouch for (`get`; 404 → tombstone).
    Verify(MessageId),
    /// Delete the raw content of a tombstoned message after the grace period.
    BlobCleanup(MessageId),
    /// Send a message's pending label intents to Gmail (phase 6).
    Deliver(MessageId),
}

impl JobKind {
    /// The message the job is about.
    #[must_use]
    pub fn message(&self) -> &MessageId {
        match self {
            Self::Import(id) | Self::Verify(id) | Self::BlobCleanup(id) | Self::Deliver(id) => id,
        }
    }
}

/// Identifies one claim of a job. Monotonic: an engine's epoch in the high
/// bits, its claim counter in the low bits, so a later claim (by this engine
/// or a newer one) has a larger id.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct AttemptId(pub u64);

/// A job's state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobState {
    /// Waiting to run once due.
    Ready {
        /// Not before.
        due_at: Timestamp,
    },
    /// Claimed by an attempt.
    Running(AttemptId),
    /// Parked until an operator retries.
    Blocked(BlockingFailure),
}

/// A durable job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    /// What it does; also its dedupe key.
    pub kind: JobKind,
    /// Admission class.
    pub priority: Priority,
    /// Failed attempts so far (quota deferrals don't count).
    pub failed_attempts: u32,
    /// Where it is.
    pub state: JobState,
}

/// What a claim for due jobs produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// These jobs are now held by the claim's attempt (possibly none).
    Claimed(Vec<ClaimedJob>),
    /// The store refused: another engine holds the account.
    Rejected(crate::commit::Rejection),
}

/// A job claimed for this engine: the store hands these out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedJob {
    /// What it does.
    pub kind: JobKind,
    /// Admission class.
    pub priority: Priority,
    /// Failed attempts so far.
    pub failed_attempts: u32,
    /// The claim; results apply only while it holds.
    pub attempt: AttemptId,
    /// For [`JobKind::Deliver`]: the label changes to send, as of the claim
    /// (`None` when nothing is pending any more).
    pub modify: Option<crate::outbox::ModifyGroup>,
}

impl ClaimedJob {
    /// The claim reference a finishing commit carries.
    #[must_use]
    pub fn claim(&self) -> JobClaim {
        JobClaim {
            kind: self.kind.clone(),
            attempt: self.attempt,
        }
    }
}

/// A reference to one claim of one job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobClaim {
    /// The job.
    pub kind: JobKind,
    /// The attempt holding it.
    pub attempt: AttemptId,
}

/// A job to add.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewJob {
    /// What it does.
    pub kind: JobKind,
    /// Admission class.
    pub priority: Priority,
    /// Not before.
    pub due_at: Timestamp,
}

/// How a claimed job ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOutcome {
    /// Done: remove the job.
    Done,
    /// Failed; try again at `due_at` (counts a failed attempt).
    Retry {
        /// The failure.
        failure: RetryableFailure,
        /// When to try again.
        due_at: Timestamp,
    },
    /// Not attempted (quota); try again at `due_at` (no failed attempt).
    Defer {
        /// When to try again.
        due_at: Timestamp,
    },
    /// Park until an operator retries.
    Block(BlockingFailure),
}

/// A job change inside a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOp {
    /// Add the job unless one of the same kind exists; raise the existing
    /// job's priority if the new one is higher.
    Enqueue(NewJob),
    /// Finish a claimed job, if the claim still holds.
    Finish {
        /// The claim.
        claim: JobClaim,
        /// How it ended.
        outcome: JobOutcome,
    },
}

/// The next job to run among `jobs`: ready and due, highest priority first,
/// then earliest due (pure; stores use it to hand out claims).
#[must_use]
pub fn pick_next(jobs: &[Job], now: Timestamp) -> Option<&Job> {
    jobs.iter()
        .filter_map(|job| match job.state {
            JobState::Ready { due_at } if due_at <= now => Some((job, due_at)),
            JobState::Ready { .. } | JobState::Running(_) | JobState::Blocked(_) => None,
        })
        .min_by(|(a, a_due), (b, b_due)| {
            b.priority
                .cmp(&a.priority)
                .then(a_due.cmp(b_due))
                .then(a.kind.cmp(&b.kind))
        })
        .map(|(job, _)| job)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::Blocking;

    fn job(id: &str, priority: Priority, state: JobState) -> Job {
        Job {
            kind: JobKind::Import(MessageId(id.into())),
            priority,
            failed_attempts: 0,
            state,
        }
    }

    fn ready(due_at: i64) -> JobState {
        JobState::Ready {
            due_at: Timestamp(due_at),
        }
    }

    #[test]
    fn picks_highest_priority_then_earliest_due() {
        let jobs = [
            job("old-backfill", Priority::Backfill, ready(1)),
            job("late-live", Priority::Live, ready(50)),
            job("early-live", Priority::Live, ready(10)),
        ];
        assert_eq!(
            pick_next(&jobs, Timestamp(100)).map(|j| &j.kind),
            Some(&jobs[2].kind)
        );
    }

    #[test]
    fn skips_running_blocked_and_future_jobs() {
        let blocked = JobState::Blocked(BlockingFailure {
            class: Blocking::Permanent,
            detail: String::new(),
        });
        let jobs = [
            job(
                "running",
                Priority::Foreground,
                JobState::Running(AttemptId(1)),
            ),
            job("blocked", Priority::Foreground, blocked),
            job("future", Priority::Foreground, ready(500)),
            job("ready", Priority::Backfill, ready(1)),
        ];
        assert_eq!(
            pick_next(&jobs, Timestamp(100)).map(|j| &j.kind),
            Some(&jobs[3].kind)
        );
        assert_eq!(pick_next(&jobs[..3], Timestamp(100)), None);
    }
}
