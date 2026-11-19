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
use crate::retry::BlockingFailure;

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
    Claimed {
        /// The claimed jobs.
        jobs: Vec<ClaimedJob>,
        /// When the earliest job left ready becomes due (see [`next_due`]).
        next_due: Option<Timestamp>,
    },
    /// The store refused: another engine holds the account.
    Rejected(crate::commit::Rejection),
}

/// What a claimed job does, with what it needs as of the claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimedWork {
    /// Fetch raw content and record it.
    Import(MessageId),
    /// Fetch labels to confirm or tombstone the message.
    Verify(MessageId),
    /// Delete a tombstoned message's raw content.
    BlobCleanup(MessageId),
    /// Send the message's pending label changes.
    Deliver(MessageId, Delivery),
}

/// A delivery job's plan, read when it was claimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    /// Send these label changes.
    Send(crate::outbox::ModifyGroup),
    /// Nothing is pending any more (settled or superseded): finish the job.
    Settled,
}

impl ClaimedWork {
    /// The work for `kind`; `plan` reads a delivery's pending changes.
    pub fn plan(
        kind: JobKind,
        plan: impl FnOnce(&MessageId) -> Option<crate::outbox::ModifyGroup>,
    ) -> Self {
        match kind {
            JobKind::Import(id) => Self::Import(id),
            JobKind::Verify(id) => Self::Verify(id),
            JobKind::BlobCleanup(id) => Self::BlobCleanup(id),
            JobKind::Deliver(id) => {
                let delivery = plan(&id).map_or(Delivery::Settled, Delivery::Send);
                Self::Deliver(id, delivery)
            }
        }
    }

    /// The job this is.
    #[must_use]
    pub fn kind(&self) -> JobKind {
        match self {
            Self::Import(id) => JobKind::Import(id.clone()),
            Self::Verify(id) => JobKind::Verify(id.clone()),
            Self::BlobCleanup(id) => JobKind::BlobCleanup(id.clone()),
            Self::Deliver(id, _) => JobKind::Deliver(id.clone()),
        }
    }
}

/// A job claimed for this engine: the store hands these out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedJob {
    /// What it does.
    pub work: ClaimedWork,
    /// Admission class.
    pub priority: Priority,
    /// Failed attempts so far.
    pub failed_attempts: u32,
    /// The claim; results apply only while it holds.
    pub attempt: AttemptId,
}

impl ClaimedJob {
    /// The job this claims.
    #[must_use]
    pub fn kind(&self) -> JobKind {
        self.work.kind()
    }

    /// The label changes a delivery sends (none for other jobs or a settled
    /// delivery).
    #[must_use]
    pub fn group(&self) -> Option<&crate::outbox::ModifyGroup> {
        match &self.work {
            ClaimedWork::Deliver(_, Delivery::Send(group)) => Some(group),
            ClaimedWork::Deliver(_, Delivery::Settled)
            | ClaimedWork::Import(_)
            | ClaimedWork::Verify(_)
            | ClaimedWork::BlobCleanup(_) => None,
        }
    }

    /// The intent versions a delivery sends.
    #[must_use]
    pub fn delivery_versions(&self) -> Vec<crate::outbox::Delivery> {
        self.group().map(|g| g.versions.clone()).unwrap_or_default()
    }

    /// The claim reference a finishing commit carries.
    #[must_use]
    pub fn claim(&self) -> JobClaim {
        JobClaim {
            kind: self.kind(),
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
    pick_next_from(jobs, now, Priority::Backfill)
}

/// [`pick_next`] among jobs of at least `floor` priority (the engine raises
/// the floor while the quota gate holds back a lower class).
#[must_use]
pub fn pick_next_from(jobs: &[Job], now: Timestamp, floor: Priority) -> Option<&Job> {
    jobs.iter()
        .filter(|job| job.priority >= floor)
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

/// When the earliest ready job among `jobs` is due: the engine wakes then
/// even if it has no timer of its own for it (after a restart, or for work
/// queued with a delay, like a delivery's confirmation). Running and blocked
/// jobs don't count.
#[must_use]
pub fn next_due(jobs: &[Job]) -> Option<Timestamp> {
    jobs.iter()
        .filter_map(|job| match job.state {
            JobState::Ready { due_at } => Some(due_at),
            JobState::Running(_) | JobState::Blocked(_) => None,
        })
        .min()
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
