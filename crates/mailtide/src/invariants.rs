//! The invariants of SPEC §9 as executable checks with structured evidence
//! (review #19), shared by unit tests, the simulator and store conformance.
//!
//! Checks run over [`AccountMirror`] snapshots taken before and after a
//! commit. Convergence (I9) is checked by the simulator against its own
//! ground truth.

use crate::commit::{Commit, CommitOutcome};
use crate::ids::{HistoryId, MessageId};
use crate::jobs::{JobKind, JobOp, JobState};
use crate::mirror::{AccountMirror, Content};
use crate::quota::Priority;

/// An invariant that doesn't hold, with its evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// I1: the cursor moved in a commit that didn't move it, or was rejected.
    CursorMovedWithoutCommit {
        /// Before.
        before: Option<HistoryId>,
        /// After.
        after: Option<HistoryId>,
    },
    /// I2: the cursor went backwards.
    CursorRegressed {
        /// Before.
        before: HistoryId,
        /// After.
        after: HistoryId,
    },
    /// I3: a message's checkpoint went backwards.
    CheckpointRegressed {
        /// The message.
        message: MessageId,
        /// Before.
        before: HistoryId,
        /// After.
        after: HistoryId,
    },
    /// I5: a tombstoned message exists again.
    Resurrected {
        /// The message.
        message: MessageId,
    },
    /// I6: a message without content has no import job.
    MissingImportJob {
        /// The message.
        message: MessageId,
    },
    /// Outside a claim's finish, a job's class went down (a user's raise,
    /// SPEC §6.4, was lost).
    PriorityLowered {
        /// The job.
        job: JobKind,
        /// Before.
        before: Priority,
        /// After.
        after: Priority,
    },
    /// I9: the mirror differs from Gmail after faults healed.
    NotConverged {
        /// A message whose state differs.
        message: MessageId,
        /// What differs.
        difference: String,
    },
    /// A rejected commit changed something.
    RejectedCommitChangedState,
    /// The stored lifecycle and cursor no longer form a valid snapshot.
    InvalidState(crate::state::InvalidSnapshot),
}

/// Check what a single commit may break.
#[must_use]
pub fn check_commit(
    before: &AccountMirror,
    after: &AccountMirror,
    commit: &Commit,
    outcome: &CommitOutcome,
) -> Vec<Violation> {
    let mut violations = Vec::new();
    match outcome {
        CommitOutcome::Rejected(_) => {
            if before != after {
                violations.push(Violation::RejectedCommitChangedState);
            }
            return violations;
        }
        CommitOutcome::Applied { .. } => {}
    }
    if let Err(invalid) = after.snapshot().validate() {
        violations.push(Violation::InvalidState(invalid));
    }
    if commit.cursor.is_none() && before.record.cursor != after.record.cursor {
        violations.push(Violation::CursorMovedWithoutCommit {
            before: before.record.cursor,
            after: after.record.cursor,
        });
    }
    if let (Some(b), Some(a)) = (before.record.cursor, after.record.cursor)
        && a < b
    {
        violations.push(Violation::CursorRegressed {
            before: b,
            after: a,
        });
    }
    let finished: Vec<&JobKind> = commit
        .claim
        .iter()
        .map(|claim| &claim.kind)
        .chain(commit.jobs.iter().filter_map(|op| match op {
            JobOp::Finish { claim, .. } => Some(&claim.kind),
            JobOp::Enqueue(_) => None,
        }))
        .collect();
    violations.extend(lowered(before, after, &finished));
    for (id, record) in &after.messages {
        if let (Some(old), Some(new)) = (
            before.messages.get(id).and_then(|r| r.checkpoint),
            record.checkpoint,
        ) && new < old
        {
            violations.push(Violation::CheckpointRegressed {
                message: id.clone(),
                before: old,
                after: new,
            });
        }
    }
    violations.extend(check_state(after));
    violations
}

/// Check an unfenced raise of opened messages' imports
/// ([`AccountMirror::prioritise`]): no class goes down, and every state
/// invariant still holds.
#[must_use]
pub fn check_prioritise(before: &AccountMirror, after: &AccountMirror) -> Vec<Violation> {
    let mut violations = lowered(before, after, &[]);
    violations.extend(check_state(after));
    violations
}

/// Jobs present before and after whose class went down, except `finished`
/// (a finish may remove a job that a later op re-creates afresh).
fn lowered(before: &AccountMirror, after: &AccountMirror, finished: &[&JobKind]) -> Vec<Violation> {
    after
        .jobs
        .iter()
        .filter(|(kind, _)| !finished.contains(kind))
        .filter_map(|(kind, job)| {
            let old = before.jobs.get(kind)?;
            (job.priority < old.priority).then(|| Violation::PriorityLowered {
                job: kind.clone(),
                before: old.priority,
                after: job.priority,
            })
        })
        .collect()
}

/// Check what must hold in every state.
#[must_use]
pub fn check_state(mirror: &AccountMirror) -> Vec<Violation> {
    let mut violations = Vec::new();
    for id in mirror.messages.keys() {
        if mirror.tombstones.contains_key(id) {
            violations.push(Violation::Resurrected {
                message: id.clone(),
            });
        }
    }
    for (id, record) in &mirror.messages {
        if record.content == Content::Pending {
            let has_job = mirror
                .jobs
                .get(&JobKind::Import(id.clone()))
                .is_some_and(|job| {
                    matches!(
                        job.state,
                        JobState::Ready { .. } | JobState::Running(_) | JobState::Blocked(_)
                    )
                });
            if !has_job {
                violations.push(Violation::MissingImportJob {
                    message: id.clone(),
                });
            }
        }
    }
    violations
}
