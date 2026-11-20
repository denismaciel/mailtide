//! The invariants of SPEC §9 as executable checks with structured evidence
//! (review #19), shared by unit tests, the simulator and store conformance.
//!
//! Checks run over [`AccountMirror`] snapshots taken before and after a
//! commit. Convergence (I9) is checked by the simulator against its own
//! ground truth.

use crate::commit::{Commit, CommitOutcome};
use crate::ids::{HistoryId, LabelId, MessageId};
use crate::jobs::{JobKind, JobOp, JobState};
use crate::mirror::{AccountMirror, Content};
use crate::quota::Priority;
use crate::revision::{FeedCursor, Retention, StateSeq};

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
    /// I5, as a purge needs it: a message is tombstoned while Gmail still
    /// has it. Only Gmail's evidence (a `messagesDeleted` record, a 404)
    /// may make a tombstone, so this never holds.
    TombstoneOfLiveMessage {
        /// The message.
        message: MessageId,
    },
    /// A purge deleted a message that was not in trash or spam (or the bin
    /// being emptied) in Gmail at the moment of the delete
    /// (`docs/features/purge.md`).
    PurgedOutsideTrashOrSpam {
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
    /// The change feed read in pages of random limits differs from the log
    /// it pages through, or a page held more than its limit.
    FeedPagesDiverged {
        /// How many entries the reader had read.
        read: u64,
        /// What differs.
        difference: String,
    },
    /// A compaction cut off a consumer that was live: it removed entries
    /// the consumer had not acknowledged.
    ConsumerCutOff {
        /// The consumer.
        consumer: String,
        /// Its position.
        at: FeedCursor,
        /// The floor after the compaction.
        floor: StateSeq,
    },
    /// After a compaction, the log still holds a commit every live
    /// consumer (and the explicit bound, if any) had passed.
    AcknowledgedEntriesKept {
        /// The last commit every live consumer had passed.
        acknowledged: StateSeq,
        /// A commit at or before it still in the log.
        kept: StateSeq,
    },
    /// A label listing took a label off a row whose message still has it
    /// in Gmail (the simulator checks it: an empty or truncated listing is
    /// no proof of a deletion).
    LiveLabelStripped {
        /// The message.
        message: MessageId,
        /// The label.
        label: LabelId,
    },
    /// Two label definitions share an id.
    DuplicateLabel {
        /// The id.
        label: LabelId,
    },
    /// A message row carries a label remembered as deleted (SPEC §6.6).
    DeletedLabelOnMessage {
        /// The message.
        message: MessageId,
        /// The label.
        label: LabelId,
    },
    /// An intent names a label remembered as deleted.
    DeletedLabelInIntent {
        /// The intent's message.
        message: MessageId,
        /// The label.
        label: LabelId,
    },
    /// A rejected commit changed something.
    RejectedCommitChangedState,
    /// A facts refresh changed more than a message's facts and raw size, or
    /// added or removed a message (SPEC §10.3).
    RefreshChangedMore {
        /// The message, if one row changed beyond its facts.
        message: Option<MessageId>,
    },
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

/// Check a compaction under `retention` ([`AccountMirror::compact`]): no
/// consumer live before it is cut off, and no commit every live consumer
/// (and the bound, if given) had passed is left in the log. Liveness is
/// judged after the compaction saw the cursors at the head, against the
/// floor before it.
#[must_use]
pub fn check_compaction(
    before: &AccountMirror,
    after: &AccountMirror,
    retention: &Retention,
) -> Vec<Violation> {
    let mut violations = Vec::new();
    let mut acknowledged = retention.through;
    let mut any_live = false;
    for cursor in after.consumers.values() {
        if !cursor.is_live(before.record.log_floor, retention) {
            continue;
        }
        any_live = true;
        if cursor.is_lost(after.record.log_floor) {
            violations.push(Violation::ConsumerCutOff {
                consumer: cursor.name.as_str().to_owned(),
                at: cursor.at,
                floor: after.record.log_floor,
            });
        }
        let passed = cursor.at.compactable();
        acknowledged = Some(acknowledged.map_or(passed, |a| a.min(passed)));
    }
    if let (true, Some(acknowledged)) = (any_live, acknowledged)
        && let Some((kept, _)) = after.log.iter().find(|(seq, _)| *seq <= acknowledged)
    {
        violations.push(Violation::AcknowledgedEntriesKept {
            acknowledged,
            kept: *kept,
        });
    }
    violations
}

/// Check what a facts refresh may change: only the facts and raw size of
/// rows it applied to, and the change log. Every row keeps its labels,
/// checkpoint, verification floor, thread, blob and header facts; no row
/// appears or goes; tombstones, jobs, intents and the account row stay.
#[must_use]
pub fn check_refresh(before: &AccountMirror, after: &AccountMirror) -> Vec<Violation> {
    let mut violations = Vec::new();
    if before.messages.len() != after.messages.len()
        || before.tombstones != after.tombstones
        || before.jobs != after.jobs
        || before.intents != after.intents
        || before.record != after.record
    {
        violations.push(Violation::RefreshChangedMore { message: None });
    }
    for (id, new) in &after.messages {
        let Some(old) = before.messages.get(id) else {
            violations.push(Violation::RefreshChangedMore {
                message: Some(id.clone()),
            });
            continue;
        };
        let same_content = match (&old.content, &new.content) {
            (Content::Pending, Content::Pending) => true,
            (Content::Imported(old), Content::Imported(new)) => {
                old.blob == new.blob
                    && old.internal_date == new.internal_date
                    && old.headers == new.headers
            }
            (Content::Pending | Content::Imported(_), _) => false,
        };
        let same_row = old.thread_id == new.thread_id
            && old.labels == new.labels
            && old.checkpoint == new.checkpoint
            && old.unverified == new.unverified;
        if !(same_content && same_row) {
            violations.push(Violation::RefreshChangedMore {
                message: Some(id.clone()),
            });
        }
    }
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

/// Check that every tombstone is for a message Gmail no longer has
/// (`gmail_has` answers for Gmail's ground truth, which only a simulator
/// knows).
#[must_use]
pub fn check_tombstones(
    mirror: &AccountMirror,
    gmail_has: impl Fn(&MessageId) -> bool,
) -> Vec<Violation> {
    mirror
        .tombstones
        .keys()
        .filter(|id| gmail_has(id))
        .map(|id| Violation::TombstoneOfLiveMessage {
            message: id.clone(),
        })
        .collect()
}

/// Check what must hold in every state.
#[must_use]
pub fn check_state(mirror: &AccountMirror) -> Vec<Violation> {
    let mut violations = check_labels(mirror);
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

/// The label invariants (SPEC §6.6): definition ids are unique, and no row
/// and no intent names a label remembered as deleted.
fn check_labels(mirror: &AccountMirror) -> Vec<Violation> {
    let mut violations = Vec::new();
    let mut defined = std::collections::BTreeSet::new();
    for label in &mirror.labels {
        if !defined.insert(&label.id) {
            violations.push(Violation::DuplicateLabel {
                label: label.id.clone(),
            });
        }
    }
    let deleted = &mirror.record.deleted_labels;
    for (id, record) in &mirror.messages {
        for label in record.labels.iter().filter(|l| deleted.contains_key(*l)) {
            violations.push(Violation::DeletedLabelOnMessage {
                message: id.clone(),
                label: label.clone(),
            });
        }
    }
    for (message, label) in mirror.intents.keys() {
        if deleted.contains_key(label) {
            violations.push(Violation::DeletedLabelInIntent {
                message: message.clone(),
                label: label.clone(),
            });
        }
    }
    violations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gmail::{HeaderFacts, Label, LabelKind};
    use crate::ids::{AccountId, BlobRef, ThreadId, Timestamp};
    use crate::labels::set;
    use crate::mirror::{MessageRecord, StoredContent};
    use crate::outbox::{Intent, IntentState, IntentVersion, LabelChange};

    #[test]
    fn no_row_or_intent_names_a_deleted_label() {
        let mut mirror = AccountMirror::new(AccountId("a@example.com".into()), 1);
        let gone = LabelId("Label_1".into());
        let message = MessageId("m".into());
        mirror.messages.insert(
            message.clone(),
            MessageRecord {
                id: message.clone(),
                thread_id: ThreadId("t".into()),
                labels: set(["INBOX", "Label_1"]),
                checkpoint: Some(HistoryId(1)),
                unverified: None,
                content: Content::Imported(Box::new(StoredContent {
                    blob: BlobRef("k".into()),
                    size: 1,
                    internal_date: Timestamp(0),
                    headers: HeaderFacts::default(),
                    facts: None,
                })),
            },
        );
        mirror.intents.insert(
            (message.clone(), gone.clone()),
            Intent {
                message: message.clone(),
                label: gone.clone(),
                change: LabelChange::Remove,
                version: IntentVersion(1),
                base_checkpoint: None,
                state: IntentState::Pending,
            },
        );
        let inbox = Label::plain(LabelId("INBOX".into()), "INBOX", LabelKind::System);
        mirror.labels = vec![inbox.clone(), inbox];
        assert_eq!(
            check_state(&mirror),
            vec![Violation::DuplicateLabel {
                label: LabelId("INBOX".into())
            }],
            "a label in use is not deleted"
        );
        mirror.labels.pop();
        mirror
            .record
            .deleted_labels
            .insert(gone.clone(), Timestamp(0));
        assert_eq!(
            check_state(&mirror),
            vec![
                Violation::DeletedLabelOnMessage {
                    message: message.clone(),
                    label: gone.clone(),
                },
                Violation::DeletedLabelInIntent {
                    message: message.clone(),
                    label: gone.clone(),
                },
            ]
        );
        mirror.intents.clear();
        if let Some(record) = mirror.messages.get_mut(&message) {
            record.labels.remove(&gone);
        }
        assert!(check_state(&mirror).is_empty());
    }
}
