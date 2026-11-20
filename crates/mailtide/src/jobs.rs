//! Durable work items: body import, verification, blob cleanup (SPEC §6.4).
//!
//! Deduplicated per kind and message (an account-level kind, such as the
//! draft index refresh, has no message and one row per account); priorities
//! `foreground > live > backfill`. A job is ready, running under one claim, or blocked. Failed
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
    /// List the account's drafts and reconcile the draft index with them
    /// (account-level: one per account).
    RefreshDrafts,
    /// Process the account's earliest due raw submission.
    SendSubmissions,
}

impl JobKind {
    /// The message the job is about; `None` for an account-level job.
    #[must_use]
    pub fn message(&self) -> Option<&MessageId> {
        match self {
            Self::Import(id) | Self::Verify(id) | Self::BlobCleanup(id) | Self::Deliver(id) => {
                Some(id)
            }
            Self::RefreshDrafts | Self::SendSubmissions => None,
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
    /// Deferred by the quota gate at the job's current priority.
    Deferred {
        /// Not before, unless promoted to a higher class.
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
    /// List the drafts; the listing applies only if the draft index is
    /// still at `generation` (no local write or draft trigger since).
    RefreshDrafts {
        /// The account's draft generation at the claim.
        generation: u64,
        /// When the job was claimed.
        claimed_at: Timestamp,
    },
    /// A claimed account queue slot for raw submissions.
    SendSubmissions,
}

/// What a claim reads beside the job itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimContext {
    /// The account's draft generation.
    pub drafts_generation: u64,
    /// The claim's time.
    pub now: Timestamp,
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
        context: ClaimContext,
        plan: impl FnOnce(&MessageId) -> Option<crate::outbox::ModifyGroup>,
    ) -> Self {
        match kind {
            JobKind::RefreshDrafts => Self::RefreshDrafts {
                generation: context.drafts_generation,
                claimed_at: context.now,
            },
            JobKind::SendSubmissions => Self::SendSubmissions,
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
            Self::RefreshDrafts { .. } => JobKind::RefreshDrafts,
            Self::SendSubmissions => JobKind::SendSubmissions,
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
            | ClaimedWork::BlobCleanup(_)
            | ClaimedWork::RefreshDrafts { .. }
            | ClaimedWork::SendSubmissions => None,
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
        /// When admission was refused.
        at: Timestamp,
        /// The class the gate considered.
        priority: Priority,
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
            JobState::Ready { due_at } | JobState::Deferred { due_at } if due_at <= now => {
                Some((job, due_at))
            }
            JobState::Ready { .. }
            | JobState::Deferred { .. }
            | JobState::Running(_)
            | JobState::Blocked(_) => None,
        })
        .min_by(|(a, a_due), (b, b_due)| {
            b.priority
                .cmp(&a.priority)
                .then(a_due.cmp(b_due))
                .then(a.kind.cmp(&b.kind))
        })
        .map(|(job, _)| job)
}

/// Most message ids one `prioritise` call takes (a long thread fits; more
/// go in another call).
pub const PRIORITISE_MAX_IDS: usize = 100;

/// Most import jobs an account holds in the Foreground class at once (ready
/// or running). Backfill may use 70% of the 4,800 units a minute; the 1,440
/// it can't touch fetch 24 raw messages (60 units each), so the raised set
/// is what one minute of reserved budget serves.
pub const FOREGROUND_IMPORT_CAP: usize = 24;

/// What a `prioritise` call did with each distinct id, in request order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Prioritised {
    /// Its import is queued in the Foreground class (raised now, or already).
    pub queued: Vec<MessageId>,
    /// Its content is already stored: read it now.
    pub imported: Vec<MessageId>,
    /// The mirror has no row for it (never discovered, or deleted).
    pub unknown: Vec<MessageId>,
    /// Its import is parked after a permanent failure (`resume` is the
    /// operator's tool).
    pub blocked: Vec<MessageId>,
    /// It could be raised, but the account already has
    /// [`FOREGROUND_IMPORT_CAP`] Foreground imports; it stays queued in its
    /// own class. Ask again as imports land.
    pub over_limit: Vec<MessageId>,
}

impl Prioritised {
    /// Every distinct id of `ids` unknown (what an untracked account answers).
    #[must_use]
    pub fn unknown(ids: &[MessageId]) -> Self {
        let mut seen = std::collections::BTreeSet::new();
        Self {
            unknown: ids.iter().filter(|id| seen.insert(*id)).cloned().collect(),
            ..Self::default()
        }
    }
}

/// Raise `job` to the Foreground class because a user is waiting for it
/// (pure). A ready job that is delayed without ever failing (a quota
/// deferral, a draft's settle delay) becomes due `now`; one backing off
/// after a failure keeps its `due_at`. A running job keeps its claim and
/// gets the class for its next attempt. A Foreground or blocked job is
/// returned unchanged. Attempts and identity never change.
#[must_use]
pub fn prioritise(job: &Job, now: Timestamp) -> Job {
    if job.priority == Priority::Foreground || matches!(job.state, JobState::Blocked(_)) {
        return job.clone();
    }
    let state = match job.state {
        JobState::Deferred { due_at } => JobState::Ready {
            due_at: due_at.min(now),
        },
        JobState::Ready { due_at } if due_at > now && job.failed_attempts == 0 => {
            JobState::Ready { due_at: now }
        }
        ref state => state.clone(),
    };
    Job {
        priority: Priority::Foreground,
        state,
        ..job.clone()
    }
}

/// When the earliest ready job among `jobs` is due: the engine wakes then
/// even if it has no timer of its own for it (after a restart, or for work
/// queued with a delay, like a delivery's confirmation). Running and blocked
/// jobs don't count.
#[must_use]
pub fn next_due(jobs: &[Job]) -> Option<Timestamp> {
    jobs.iter()
        .filter_map(|job| match job.state {
            JobState::Ready { due_at } | JobState::Deferred { due_at } => Some(due_at),
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

    #[test]
    fn prioritising_raises_the_class_and_keeps_attempts() {
        for priority in [Priority::Backfill, Priority::Live] {
            let mut queued = job("m", priority, ready(10));
            queued.failed_attempts = 2;
            let raised = prioritise(&queued, Timestamp(100));
            assert_eq!(raised.priority, Priority::Foreground);
            assert_eq!(raised.kind, queued.kind);
            assert_eq!(raised.failed_attempts, 2);
            assert_eq!(raised.state, ready(10), "already due: untouched");
        }
    }

    #[test]
    fn prioritising_makes_a_delayed_job_due_unless_it_is_backing_off() {
        let deferred = job("deferred", Priority::Backfill, ready(500));
        assert_eq!(prioritise(&deferred, Timestamp(100)).state, ready(100));
        let mut failed = job("failed", Priority::Backfill, ready(500));
        failed.failed_attempts = 1;
        let raised = prioritise(&failed, Timestamp(100));
        assert_eq!(raised.state, ready(500), "the backoff stands");
        assert_eq!(raised.priority, Priority::Foreground);
    }

    #[test]
    fn prioritising_a_running_or_foreground_job_changes_only_what_is_lower() {
        let running = job("running", Priority::Live, JobState::Running(AttemptId(7)));
        let raised = prioritise(&running, Timestamp(100));
        assert_eq!(raised.state, JobState::Running(AttemptId(7)));
        assert_eq!(raised.priority, Priority::Foreground);
        let foreground = job("fg", Priority::Foreground, ready(500));
        assert_eq!(prioritise(&foreground, Timestamp(100)), foreground);
        let blocked = job(
            "blocked",
            Priority::Backfill,
            JobState::Blocked(BlockingFailure {
                class: Blocking::Permanent,
                detail: String::new(),
            }),
        );
        assert_eq!(prioritise(&blocked, Timestamp(100)), blocked);
    }
}
