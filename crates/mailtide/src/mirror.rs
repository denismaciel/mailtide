//! How a store applies commits: the pure transition rules every store shares
//! (SPEC §10.3, review #1).
//!
//! Each message operation is decided by a pure function over the stored row
//! and tombstone; [`AccountMirror`] applies whole commits in memory with the
//! same rules. The memory store and the simulator use `AccountMirror`
//! directly; SQL stores load the rows a commit touches, call the same
//! decision functions inside their transaction, and write the results.

use std::collections::{BTreeMap, BTreeSet};

use crate::commit::{
    AccountOp, Commit, CommitOutcome, DeletionEvidence, JobPlan, LabelOp, MessageOp, Rejection,
};
use crate::gmail::{HeaderFacts, Label, LabelKind, MessageFacts, MessageObservation};
use crate::ids::{
    AccountId, BlobRef, EngineVersion, Epoch, HistoryId, LabelId, MessageId, ThreadId, Timestamp,
    stable_hash,
};
use crate::jobs::{
    AttemptId, ClaimedJob, ClaimedWork, FOREGROUND_IMPORT_CAP, Job, JobClaim, JobKind, JobOp,
    JobOutcome, JobState, NewJob, Prioritised,
};
use crate::labels::{self, LabelSet};
use crate::outbox::{self, ActionRequest, Delivery, Intent, IntentOp, IntentState, IntentVersion};
use crate::quota::Priority;
use crate::reconcile::{ListScope, ReconcileProgress, ReconcileReason};
use crate::retry::Cooldown;
use crate::revision::{
    AckOutcome, Change, ChangePage, ConsumerCursor, ConsumerList, ConsumerName, FeedCursor,
    FeedPosition, MAX_CONSUMERS, Retention, StateSeq, compaction_target, cut_page, page_window,
};
use crate::state::{StoreSnapshot, SyncState};

/// A message as a store holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRecord {
    /// The message.
    pub id: MessageId,
    /// Its thread.
    pub thread_id: ThreadId,
    /// Observed labels (Gmail's state at `checkpoint`).
    pub labels: LabelSet,
    /// Where the labels were last observed in full; `None` until then.
    pub checkpoint: Option<HistoryId>,
    /// `Some(floor)` while the labels can't be trusted: history was lost
    /// (reconciliation) or the row came from a listing, and deltas since then
    /// were applied on an unverified base. Only a full observation at or after
    /// `floor` settles them; until then a verification fetch stays queued.
    pub unverified: Option<HistoryId>,
    /// Whether the raw content is stored.
    pub content: Content,
}

/// A message's raw content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Content {
    /// Not yet imported; an import job is recorded.
    Pending,
    /// Stored.
    Imported(Box<StoredContent>),
}

/// Where and what the raw content is.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredContent {
    /// Where the raw content is.
    pub blob: BlobRef,
    /// Raw size in bytes.
    pub size: u64,
    /// Gmail's `internalDate`.
    pub internal_date: Timestamp,
    /// Header facts.
    pub headers: HeaderFacts,
    /// The facts extracted from the raw content (SPEC §4); `None` in rows
    /// written before they existed, or when the extractor gave up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub facts: Option<MessageFacts>,
}

/// A message as hosts read it: labels with pending actions applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageView {
    /// The message.
    pub id: MessageId,
    /// Its thread.
    pub thread_id: ThreadId,
    /// What users should see: observed labels with pending actions applied.
    pub labels: LabelSet,
    /// What Gmail last showed.
    pub observed_labels: LabelSet,
    /// Bin labels that an unsettled local action removes, including a
    /// removal of a label absent from the observed row.
    pub bin_removals: LabelSet,
    /// Where the labels were last observed in full.
    pub checkpoint: Option<HistoryId>,
    /// Whether the labels await verification.
    pub unverified: bool,
    /// The raw content, once imported.
    pub content: Option<StoredContent>,
    /// Actions not yet confirmed by Gmail.
    pub pending_actions: u32,
    /// Actions Gmail refused for good.
    pub blocked_actions: u32,
}

impl MessageRecord {
    /// The view of this row given its intents.
    #[must_use]
    pub fn view(&self, intents: &[Intent]) -> MessageView {
        let count = |blocked: bool| {
            u32::try_from(
                intents
                    .iter()
                    .filter(|intent| matches!(intent.state, IntentState::Blocked(_)) == blocked)
                    .count(),
            )
            .unwrap_or(u32::MAX)
        };
        MessageView {
            id: self.id.clone(),
            thread_id: self.thread_id.clone(),
            labels: labels::effective(&self.labels, intents),
            observed_labels: self.labels.clone(),
            bin_removals: intents
                .iter()
                .filter(|intent| {
                    intent.overlays()
                        && intent.change == outbox::LabelChange::Remove
                        && matches!(
                            intent.label.0.as_str(),
                            labels::system::TRASH | labels::system::SPAM
                        )
                })
                .map(|intent| intent.label.clone())
                .collect(),
            checkpoint: self.checkpoint,
            unverified: self.unverified.is_some(),
            content: match &self.content {
                Content::Pending => None,
                Content::Imported(stored) => Some((**stored).clone()),
            },
            pending_actions: count(false),
            blocked_actions: count(true),
        }
    }
}

/// What the mirror holds for a message id (see [`AccountMirror::locate`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Located {
    /// A row; `imported` once its content is stored.
    Row {
        /// Whether the content is stored.
        imported: bool,
    },
    /// The message was deleted (I5: for good).
    Tombstone,
    /// Neither: never discovered here.
    Absent,
}

/// The account row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRecord {
    /// The current writer's epoch.
    pub epoch: Epoch,
    /// The newest engine version that wrote.
    pub engine_version: EngineVersion,
    /// The committed cursor.
    pub cursor: Option<HistoryId>,
    /// The lifecycle.
    pub sync: SyncState,
    /// When the watch expires.
    pub watch_expiration: Option<Timestamp>,
    /// Last user activity.
    pub last_activity: Option<Timestamp>,
    /// Active cooldown.
    pub cooldown: Option<Cooldown>,
    /// Seed for deterministic jitter.
    pub rng_seed: u64,
    /// When the label definitions were last listed.
    pub labels_refreshed_at: Option<Timestamp>,
    /// Change-feed entries at or before this state were compacted away.
    pub log_floor: StateSeq,
    /// Which incarnation of the account this is: set when it is tracked, so
    /// feed positions from before a forget are recognisably stale.
    pub generation: u64,
    /// Bumped by every local label write ([`AccountMirror::record_label`]):
    /// a listing sent under an older value is stale and not applied.
    pub label_generation: u64,
    /// Labels known to be deleted, with when: no observation may put them
    /// back on a row. Kept for [`DELETED_LABEL_MEMORY`]; a listing that
    /// names one again restores it.
    pub deleted_labels: BTreeMap<LabelId, Timestamp>,
}

/// How long a deleted label's id is remembered: longer than Gmail keeps
/// history, so no replayed record can carry it after that (SPEC §6.6).
pub const DELETED_LABEL_MEMORY: std::time::Duration = std::time::Duration::from_hours(30 * 24);

impl AccountRecord {
    /// A freshly tracked account: no cursor, bootstrapping.
    #[must_use]
    pub fn new(account: &AccountId, generation: u64) -> Self {
        Self {
            epoch: Epoch(0),
            engine_version: EngineVersion(0),
            cursor: None,
            sync: SyncState::Bootstrapping(ReconcileProgress::begin(ReconcileReason::Bootstrap)),
            watch_expiration: None,
            last_activity: None,
            cooldown: None,
            rng_seed: stable_hash(account.as_str().as_bytes()),
            labels_refreshed_at: None,
            log_floor: StateSeq(0),
            generation,
            label_generation: 0,
            deleted_labels: BTreeMap::new(),
        }
    }
}

// ------------------------------------------------------------ guards

/// Check a commit's guards against the account row and the job it finishes.
pub fn check_guards(
    account: &AccountRecord,
    commit: &Commit,
    claimed: Option<&Job>,
) -> Result<(), Rejection> {
    if commit.epoch != account.epoch {
        return Err(Rejection::StaleEpoch);
    }
    if commit.engine_version < account.engine_version {
        return Err(Rejection::StaleVersion);
    }
    if let Some(cursor) = &commit.cursor
        && (cursor.expected != account.cursor || Some(cursor.to) < cursor.expected)
    {
        return Err(Rejection::CursorMismatch);
    }
    if let Some(claim) = &commit.claim
        && !claim_holds(claimed, claim)
    {
        return Err(Rejection::StaleClaim);
    }
    Ok(())
}

fn claim_holds(job: Option<&Job>, claim: &JobClaim) -> bool {
    job.is_some_and(|job| job.kind == claim.kind && job.state == JobState::Running(claim.attempt))
}

// ------------------------------------------------------------ message decisions

/// What one message operation does to the stored row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageEffect {
    /// Nothing: the condition didn't hold.
    Skip,
    /// Write this row.
    Put(MessageRecord),
    /// Delete the row and keep a tombstone at this checkpoint.
    Delete {
        /// The message.
        id: MessageId,
        /// The tombstone's checkpoint.
        checkpoint: HistoryId,
    },
    /// Leave the row; only the jobs and seen-set effects apply.
    Keep,
}

/// The full effect of one message operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    /// The row change.
    pub row: MessageEffect,
    /// Jobs to enqueue.
    pub enqueue: Vec<NewJob>,
    /// Jobs to cancel.
    pub cancel: Vec<JobKind>,
}

impl Decision {
    fn skip() -> Self {
        Self {
            row: MessageEffect::Skip,
            enqueue: Vec::new(),
            cancel: Vec::new(),
        }
    }

    fn row(row: MessageEffect) -> Self {
        Self {
            row,
            enqueue: Vec::new(),
            cancel: Vec::new(),
        }
    }

    fn with_job(mut self, job: NewJob) -> Self {
        self.enqueue.push(job);
        self
    }
}

fn import_job(id: &MessageId, labels: &LabelSet, plan: &JobPlan) -> NewJob {
    NewJob {
        kind: JobKind::Import(id.clone()),
        priority: plan.priority,
        due_at: if labels::is_draft(labels) {
            plan.draft_import_due_at
        } else {
            plan.import_due_at
        },
    }
}

/// Whether a full observation at `incoming` may replace a row's labels.
fn settles_labels(record: &MessageRecord, incoming: HistoryId) -> bool {
    labels::accepts(record.checkpoint, incoming)
        && record.unverified.is_none_or(|floor| incoming >= floor)
}

/// [`MessageOp::Observe`].
#[must_use]
pub fn observe(
    current: Option<&MessageRecord>,
    tombstone: Option<HistoryId>,
    observation: &MessageObservation,
    plan: &JobPlan,
) -> Decision {
    if tombstone.is_some() {
        return Decision::skip();
    }
    match current {
        None => Decision::row(MessageEffect::Put(MessageRecord {
            id: observation.id.clone(),
            thread_id: observation.thread_id.clone(),
            labels: observation.labels.clone(),
            checkpoint: Some(observation.checkpoint),
            unverified: None,
            content: Content::Pending,
        }))
        .with_job(import_job(&observation.id, &observation.labels, plan)),
        Some(record) if settles_labels(record, observation.checkpoint) => {
            Decision::row(MessageEffect::Put(MessageRecord {
                thread_id: observation.thread_id.clone(),
                labels: observation.labels.clone(),
                checkpoint: Some(observation.checkpoint),
                unverified: None,
                ..record.clone()
            }))
        }
        Some(_) => Decision::skip(),
    }
}

/// [`MessageOp::PatchLabels`]. On a trusted row a newer delta applies and
/// advances the checkpoint. On an unverified row it applies for display only:
/// the base may be wrong, so the checkpoint stays and the row waits for a
/// full observation at or after this delta.
#[must_use]
pub fn patch_labels(
    current: Option<&MessageRecord>,
    tombstone: Option<HistoryId>,
    id: &MessageId,
    add: &LabelSet,
    remove: &LabelSet,
    checkpoint: HistoryId,
    plan: &JobPlan,
) -> Decision {
    if tombstone.is_some() {
        return Decision::skip();
    }
    let verify = NewJob {
        kind: JobKind::Verify(id.clone()),
        priority: plan.priority,
        due_at: plan.verify_due_at,
    };
    let Some(record) = current else {
        return Decision::row(MessageEffect::Keep).with_job(verify);
    };
    if !labels::accepts(record.checkpoint, checkpoint) {
        return Decision::skip();
    }
    let labels = record
        .labels
        .union(add)
        .filter(|label| !remove.contains(*label))
        .cloned()
        .collect();
    match record.unverified {
        None => Decision::row(MessageEffect::Put(MessageRecord {
            labels,
            checkpoint: Some(checkpoint),
            ..record.clone()
        })),
        Some(floor) => Decision::row(MessageEffect::Put(MessageRecord {
            labels,
            unverified: Some(floor.max(checkpoint)),
            ..record.clone()
        }))
        .with_job(verify),
    }
}

/// [`MessageOp::MarkImported`]. A message deleted while its content was being
/// fetched gets a cleanup job for the content just written (I12). The content
/// always lands; the labels only when the observation settles them.
#[must_use]
pub fn mark_imported(
    current: Option<&MessageRecord>,
    tombstone: Option<HistoryId>,
    observation: &MessageObservation,
    content: StoredContent,
    plan: &JobPlan,
) -> Decision {
    if tombstone.is_some() {
        return Decision::row(MessageEffect::Keep).with_job(NewJob {
            kind: JobKind::BlobCleanup(observation.id.clone()),
            priority: plan.priority,
            due_at: plan.cleanup_due_at,
        });
    }
    let base = current.cloned().unwrap_or_else(|| MessageRecord {
        id: observation.id.clone(),
        thread_id: observation.thread_id.clone(),
        labels: observation.labels.clone(),
        checkpoint: None,
        unverified: None,
        content: Content::Pending,
    });
    let content = Content::Imported(Box::new(content));
    if settles_labels(&base, observation.checkpoint) {
        Decision::row(MessageEffect::Put(MessageRecord {
            thread_id: observation.thread_id.clone(),
            labels: observation.labels.clone(),
            checkpoint: Some(observation.checkpoint),
            unverified: None,
            content,
            ..base
        }))
    } else {
        Decision::row(MessageEffect::Put(MessageRecord { content, ..base }))
    }
}

/// [`MessageOp::Tombstone`]. Content that is stored, or may be being written
/// by a running import, gets a cleanup job. A message already tombstoned
/// (no row, a tombstone) is skipped: deletion is final, and the sync that
/// later reads the deletion a purge recorded logs no second
/// `MessageDeleted`.
#[must_use]
pub fn tombstone(
    current: Option<&MessageRecord>,
    existing: Option<HistoryId>,
    id: &MessageId,
    evidence: DeletionEvidence,
    import_running: bool,
    plan: &JobPlan,
) -> Decision {
    if current.is_none() && existing.is_some() {
        return Decision::skip();
    }
    let checkpoint = existing.map_or(evidence.checkpoint(), |t| t.max(evidence.checkpoint()));
    let has_content = matches!(current.map(|r| &r.content), Some(Content::Imported(_)));
    let mut decision = Decision::row(MessageEffect::Delete {
        id: id.clone(),
        checkpoint,
    });
    decision.cancel = vec![
        JobKind::Import(id.clone()),
        JobKind::Verify(id.clone()),
        JobKind::Deliver(id.clone()),
    ];
    // Content may exist even when the row says otherwise: an import can
    // write its blob and crash before committing. Deleting a missing blob is
    // a no-op, so every deleted message that had a row gets a delayed
    // cleanup; then no crash window can leak content.
    let _ = (has_content, import_running);
    if current.is_some() {
        decision.enqueue.push(NewJob {
            kind: JobKind::BlobCleanup(id.clone()),
            priority: plan.priority,
            due_at: plan.cleanup_due_at,
        });
    }
    decision
}

/// [`MessageOp::Listed`]: the row effect; the caller also records the scope.
#[must_use]
pub fn listed(
    current: Option<&MessageRecord>,
    tombstone: Option<HistoryId>,
    id: &MessageId,
    thread_id: &ThreadId,
    plan: &JobPlan,
) -> Decision {
    match (current, tombstone) {
        (_, Some(_)) => Decision::skip(),
        (Some(_), None) => Decision::row(MessageEffect::Keep),
        (None, None) => Decision::row(MessageEffect::Put(MessageRecord {
            id: id.clone(),
            thread_id: thread_id.clone(),
            labels: LabelSet::new(),
            checkpoint: None,
            unverified: Some(HistoryId(0)),
            content: Content::Pending,
        }))
        .with_job(import_job(id, &LabelSet::new(), plan)),
    }
}

/// [`MessageOp::FinishReconcile`] for one row.
///
/// Listings can't prove an older message's labels: Gmail's pagination is not
/// a consistent snapshot, so a concurrent change can make a listing skip a
/// message, and the changes that happened while history was unavailable are
/// otherwise lost. So every message older than the baseline gets an exact
/// verification fetch, and a message the listing created gets provisional
/// labels with no checkpoint (its import's exact fetch wins). A first
/// bootstrap pays nothing extra: every message is new and imported anyway.
#[must_use]
pub fn finish_reconcile(
    record: &MessageRecord,
    scopes: Option<&BTreeSet<ListScope>>,
    baseline: HistoryId,
    plan: &JobPlan,
) -> Decision {
    let verify = NewJob {
        kind: JobKind::Verify(record.id.clone()),
        priority: plan.priority,
        due_at: plan.verify_due_at,
    };
    match (record.checkpoint, scopes) {
        (Some(checkpoint), _) if checkpoint >= baseline => Decision::row(MessageEffect::Keep),
        (None, Some(scopes)) => Decision::row(MessageEffect::Put(MessageRecord {
            labels: scopes
                .iter()
                .filter_map(|scope| match scope {
                    ListScope::All => None,
                    ListScope::Label(label) => Some(label.clone()),
                })
                .collect(),
            ..record.clone()
        })),
        // History was lost: the labels are unverified until a full
        // observation at or after the last one we had.
        (checkpoint, _) => Decision::row(MessageEffect::Put(MessageRecord {
            unverified: Some(
                record
                    .unverified
                    .unwrap_or(HistoryId(0))
                    .max(checkpoint.unwrap_or(HistoryId(0))),
            ),
            ..record.clone()
        }))
        .with_job(verify),
    }
}

// ------------------------------------------------------------ job decisions

/// Enqueue a job: insert if new, otherwise only raise its priority.
#[must_use]
pub fn enqueue(existing: Option<&Job>, new: &NewJob) -> Job {
    match existing {
        None => Job {
            kind: new.kind.clone(),
            priority: new.priority,
            failed_attempts: 0,
            state: JobState::Ready { due_at: new.due_at },
        },
        Some(job) => Job {
            priority: job.priority.max(new.priority),
            ..job.clone()
        },
    }
}

/// Finish a claimed job. `None` removes it.
#[must_use]
pub fn finish(job: &Job, outcome: &JobOutcome) -> Option<Job> {
    match outcome {
        JobOutcome::Done => None,
        JobOutcome::Retry { due_at } => Some(Job {
            failed_attempts: job.failed_attempts.saturating_add(1),
            state: JobState::Ready { due_at: *due_at },
            ..job.clone()
        }),
        JobOutcome::Defer { due_at } => Some(Job {
            state: JobState::Ready { due_at: *due_at },
            ..job.clone()
        }),
        JobOutcome::Block(failure) => Some(Job {
            state: JobState::Blocked(failure.clone()),
            ..job.clone()
        }),
    }
}

/// What writing `record` over `previous` changes, for the feed (pure).
#[must_use]
pub fn row_changes(previous: Option<&MessageRecord>, record: &MessageRecord) -> Vec<Change> {
    let mut changes = Vec::new();
    match previous {
        None => changes.push(Change::MessageDiscovered {
            id: record.id.clone(),
            thread_id: record.thread_id.clone(),
            labels: record.labels.clone(),
        }),
        Some(previous) if previous.labels != record.labels => {
            changes.push(Change::LabelsChanged {
                id: record.id.clone(),
                thread_id: record.thread_id.clone(),
                added: record
                    .labels
                    .difference(&previous.labels)
                    .cloned()
                    .collect(),
                removed: previous
                    .labels
                    .difference(&record.labels)
                    .cloned()
                    .collect(),
            });
        }
        Some(_) => {}
    }
    let newly_imported = matches!(record.content, Content::Imported(_))
        && !matches!(previous.map(|p| &p.content), Some(Content::Imported(_)));
    if let (true, Content::Imported(stored)) = (newly_imported, &record.content) {
        changes.push(Change::MessageImported {
            id: record.id.clone(),
            thread_id: record.thread_id.clone(),
            labels: record.labels.clone(),
            internal_date: stored.internal_date,
            blob: stored.blob.clone(),
            size: stored.size,
        });
    }
    changes
}

// ------------------------------------------------------------ facts refresh

/// Facts extracted again from a message's stored raw content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactsUpdate {
    /// The message.
    pub id: MessageId,
    /// The blob the facts were extracted from.
    pub blob: BlobRef,
    /// The raw size the row held when the refresher read it.
    pub read_size: u64,
    /// The length of the blob the facts were extracted from.
    pub size: u64,
    /// The facts.
    pub facts: MessageFacts,
}

/// Whether a facts refresh applies to `record` (pure): only while the row is
/// still the one the refresher read (imported, same blob, same size) and the
/// update improves it (its facts are missing or older, or the blob's length
/// differs from the size the row holds). A message deleted, re-imported or
/// refreshed meanwhile is left alone.
#[must_use]
pub fn refresh_applies(record: &MessageRecord, update: &FactsUpdate) -> bool {
    let Content::Imported(stored) = &record.content else {
        return false;
    };
    let older = stored
        .facts
        .as_ref()
        .is_none_or(|facts| facts.version < update.facts.version);
    stored.blob == update.blob
        && stored.size == update.read_size
        && (older || stored.size != update.size)
}

// ------------------------------------------------------------ intent decisions

/// The intents admitting `action` on a message adds, given its labels and
/// current intents. An identical change already pending or delivered is not
/// admitted again (it would only be resent); a blocked one is retried with a
/// new version. Empty when admission fails (drafts) or nothing is new.
#[must_use]
pub fn admit(record: &MessageRecord, current: &[Intent], action: &outbox::Action) -> Vec<Intent> {
    let effective = labels::effective(&record.labels, current);
    let Ok(changes) = outbox::expand(&record.id, action, &effective) else {
        return Vec::new();
    };
    changes
        .into_iter()
        .filter(|(label, change)| {
            !current.iter().any(|intent| {
                &intent.label == label
                    && intent.change == *change
                    && !matches!(intent.state, IntentState::Blocked(_))
            })
        })
        .map(|(label, change)| {
            let previous = current
                .iter()
                .filter(|intent| intent.label == label)
                .map(|intent| intent.version)
                .max()
                .unwrap_or(IntentVersion(0));
            Intent {
                message: record.id.clone(),
                label,
                change,
                version: IntentVersion(previous.0 + 1),
                base_checkpoint: record.checkpoint,
                state: IntentState::Pending,
            }
        })
        .collect()
}

/// Whether a confirmation fetch settles an intent (I7). Pending intents are
/// always delivered. A delivered one settles only on a `get` claimed after
/// its delivery (`confirmation` is that fetch's claim): whatever it shows is
/// Gmail's state, so the overlay goes. History can't settle it: a record
/// fetched after delivery may still predate it.
#[must_use]
pub fn settles(intent: &Intent, confirmation: AttemptId) -> bool {
    match intent.state {
        IntentState::Pending | IntentState::Blocked(_) => false,
        IntentState::Delivered { attempt, .. } => confirmation > attempt,
    }
}

/// `op` with the labels remembered as deleted taken out of what it would
/// store: a fetch, an import or a history record from before a deletion
/// must not put the id back on a row.
fn without_deleted(op: &MessageOp, deleted: &BTreeMap<LabelId, Timestamp>) -> MessageOp {
    let keep = |labels: &LabelSet| -> LabelSet {
        labels
            .iter()
            .filter(|label| !deleted.contains_key(*label))
            .cloned()
            .collect()
    };
    match op {
        MessageOp::Observe(observation) => MessageOp::Observe(MessageObservation {
            labels: keep(&observation.labels),
            ..observation.clone()
        }),
        MessageOp::PatchLabels {
            id,
            add,
            remove,
            checkpoint,
        } => MessageOp::PatchLabels {
            id: id.clone(),
            add: keep(add),
            remove: remove.clone(),
            checkpoint: *checkpoint,
        },
        MessageOp::MarkImported {
            observation,
            blob,
            size,
            internal_date,
            headers,
            facts,
        } => MessageOp::MarkImported {
            observation: MessageObservation {
                labels: keep(&observation.labels),
                ..observation.clone()
            },
            blob: blob.clone(),
            size: *size,
            internal_date: *internal_date,
            headers: headers.clone(),
            facts: facts.clone(),
        },
        MessageOp::Tombstone { .. }
        | MessageOp::Listed { .. }
        | MessageOp::FinishReconcile { .. } => op.clone(),
    }
}

/// Whether `label` is one of Gmail's own: listed as a system label, or one
/// of the fixed system ids. Never forgotten, whatever a listing says.
fn is_system_label(label: &Label) -> bool {
    label.kind == LabelKind::System || labels::system::is_system(&label.id.0)
}

/// The labels `op` removes from the definitions `stored` while the label
/// generation is `generation`: those a current, complete listing lacks, or
/// the one a `Remove` names; never one of Gmail's own. Every store applies
/// the removal with [`AccountMirror`]; a SQL store loads the rows of these.
#[must_use]
pub fn removed_labels(stored: &[Label], generation: u64, op: &LabelOp) -> Vec<LabelId> {
    match op {
        LabelOp::ReplaceAll {
            labels,
            generation: listed_under,
            ..
        } if *listed_under == generation && complete_listing(stored, labels) => stored
            .iter()
            .filter(|label| {
                !is_system_label(label) && labels.iter().all(|listed| listed.id != label.id)
            })
            .map(|label| label.id.clone())
            .collect(),
        LabelOp::Remove { id, .. }
            if !labels::system::is_system(&id.0)
                && !stored
                    .iter()
                    .any(|label| &label.id == id && label.kind == LabelKind::System) =>
        {
            vec![id.clone()]
        }
        LabelOp::ReplaceAll { .. } | LabelOp::Remove { .. } | LabelOp::Upsert(_) => Vec::new(),
    }
}

/// Whether `listed` can be a whole `labels.list` answer given the stored
/// definitions: it names `INBOX` and every system label stored (a mailbox
/// always lists them). An empty or truncated answer is not proof that the
/// labels it lacks were deleted; it changes nothing.
#[must_use]
pub fn complete_listing(stored: &[Label], listed: &[Label]) -> bool {
    let named = |id: &LabelId| listed.iter().any(|label| &label.id == id);
    named(&LabelId(labels::system::INBOX.to_owned()))
        && stored
            .iter()
            .filter(|label| is_system_label(label))
            .all(|label| named(&label.id))
}

/// Put label definitions in the order stores return them: by id, byte-wise
/// (never a database collation, so every store agrees).
pub fn sort_labels(labels: &mut [Label]) {
    labels.sort_by(|a, b| a.id.cmp(&b.id));
}

/// Whether two label lists define the same labels, in whatever order: a
/// refresh that lists what is stored changes nothing (SPEC §6.6).
#[must_use]
pub fn same_labels(stored: &[Label], listed: &[Label]) -> bool {
    let mut stored = stored.to_vec();
    let mut listed = listed.to_vec();
    sort_labels(&mut stored);
    sort_labels(&mut listed);
    stored == listed
}

// ------------------------------------------------------------ the in-memory mirror

/// One account's complete mirror, applying commits with the shared rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountMirror {
    /// The account.
    pub account: AccountId,
    /// The account row.
    pub record: AccountRecord,
    /// Messages by id.
    pub messages: BTreeMap<MessageId, MessageRecord>,
    /// Tombstones by id.
    pub tombstones: BTreeMap<MessageId, HistoryId>,
    /// Jobs by kind.
    pub jobs: BTreeMap<JobKind, Job>,
    /// Reconciliation: scopes each message was listed in.
    pub seen: BTreeMap<MessageId, BTreeSet<ListScope>>,
    /// Label definitions.
    pub labels: Vec<Label>,
    /// Pending user actions, one per (message, label).
    pub intents: BTreeMap<(MessageId, crate::ids::LabelId), Intent>,
    /// The change log.
    pub log: Vec<(StateSeq, Change)>,
    /// The latest change-log position.
    pub seq: StateSeq,
    /// The registered feed consumers, by name. Commits never touch them.
    pub consumers: BTreeMap<ConsumerName, ConsumerCursor>,
}

impl AccountMirror {
    /// An empty mirror for a freshly tracked `account`.
    #[must_use]
    pub fn new(account: AccountId, generation: u64) -> Self {
        Self {
            record: AccountRecord::new(&account, generation),
            account,
            messages: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            jobs: BTreeMap::new(),
            seen: BTreeMap::new(),
            labels: Vec::new(),
            intents: BTreeMap::new(),
            log: Vec::new(),
            seq: StateSeq(0),
            consumers: BTreeMap::new(),
        }
    }

    /// A message's intents.
    #[must_use]
    pub fn intents_of(&self, id: &MessageId) -> Vec<Intent> {
        self.intents
            .values()
            .filter(|intent| &intent.message == id)
            .cloned()
            .collect()
    }

    /// How hosts see a message.
    #[must_use]
    pub fn view(&self, id: &MessageId) -> Option<MessageView> {
        Some(self.messages.get(id)?.view(&self.intents_of(id)))
    }

    /// The rows of thread `id`, drafts included, in id order. The empty id
    /// is never a thread: rows a record without a thread id created are not
    /// grouped together (SPEC §4).
    pub fn thread_members<'a>(
        &'a self,
        id: &'a ThreadId,
    ) -> impl Iterator<Item = &'a MessageRecord> + 'a {
        self.messages
            .values()
            .filter(move |record| !id.0.is_empty() && &record.thread_id == id)
    }

    /// How hosts see a thread; `None` if the mirror has no message in it.
    #[must_use]
    pub fn thread(&self, id: &ThreadId) -> Option<crate::thread::ThreadView> {
        let members = self
            .thread_members(id)
            .map(|record| record.view(&self.intents_of(&record.id)))
            .collect();
        crate::thread::view(id, members)
    }

    /// What the mirror holds for each of `ids`, in the order given: a row
    /// (with or without its content), a tombstone, or nothing. A read: it
    /// changes nothing.
    #[must_use]
    pub fn locate(&self, ids: &[MessageId]) -> Vec<(MessageId, Located)> {
        ids.iter()
            .map(|id| {
                let located = match (self.messages.get(id), self.tombstones.contains_key(id)) {
                    (_, true) => Located::Tombstone,
                    (Some(record), false) => Located::Row {
                        imported: matches!(record.content, Content::Imported(_)),
                    },
                    (None, false) => Located::Absent,
                };
                (id.clone(), located)
            })
            .collect()
    }

    /// The label definitions as last synced, in id order.
    #[must_use]
    pub fn label_definitions(&self) -> Vec<Label> {
        let mut labels = self.labels.clone();
        sort_labels(&mut labels);
        labels
    }

    /// Hand the account to a new engine: bump the epoch, record the version,
    /// release claims held by earlier engines.
    pub fn claim(&mut self, version: EngineVersion) -> Result<StoreSnapshot, Rejection> {
        if version < self.record.engine_version {
            return Err(Rejection::StaleVersion);
        }
        self.record.epoch = Epoch(self.record.epoch.0 + 1);
        self.record.engine_version = version;
        for job in self.jobs.values_mut() {
            if let JobState::Running(_) = job.state {
                job.state = JobState::Ready {
                    due_at: Timestamp(i64::MIN),
                };
            }
        }
        Ok(self.snapshot())
    }

    /// The snapshot a new engine is built from.
    #[must_use]
    pub fn snapshot(&self) -> StoreSnapshot {
        StoreSnapshot {
            account: self.account.clone(),
            epoch: self.record.epoch,
            engine_version: self.record.engine_version,
            cursor: self.record.cursor,
            sync: self.record.sync.clone(),
            watch_expiration: self.record.watch_expiration,
            last_activity: self.record.last_activity,
            cooldown: self.record.cooldown.clone(),
            rng_seed: self.record.rng_seed,
            labels_refreshed_at: self.record.labels_refreshed_at,
            label_ids: self.labels.iter().map(|label| label.id.clone()).collect(),
        }
    }

    /// Apply a commit atomically: all of it, or (on a rejected guard) none.
    pub fn apply(&mut self, commit: &Commit) -> CommitOutcome {
        let claimed = commit
            .claim
            .as_ref()
            .and_then(|claim| self.jobs.get(&claim.kind));
        if let Err(rejection) = check_guards(&self.record, commit, claimed) {
            return CommitOutcome::Rejected(rejection);
        }
        let mut next = self.clone();
        let skipped = next.apply_unguarded(commit);
        // Never persist a lifecycle and cursor that contradict each other
        // (e.g. Live without a cursor): the next engine couldn't start.
        if next.snapshot().validate().is_err() {
            return CommitOutcome::Rejected(Rejection::InvalidTransition);
        }
        *self = next;
        CommitOutcome::Applied {
            state: self.seq,
            skipped,
        }
    }

    fn apply_unguarded(&mut self, commit: &Commit) -> u32 {
        self.seq = StateSeq(self.seq.0 + 1);
        let seq = self.seq;
        if let Some(sync) = &commit.sync {
            self.record.sync = sync.clone();
        }
        if let Some(cursor) = &commit.cursor {
            self.record.cursor = Some(cursor.to);
        }
        for op in &commit.labels {
            self.apply_label(op, seq);
        }
        for op in &commit.account_ops {
            match op {
                AccountOp::SetWatchExpiration(at) => self.record.watch_expiration = Some(*at),
                AccountOp::SetCooldown(cooldown) => self.record.cooldown = Some(cooldown.clone()),
                AccountOp::ClearCooldown => self.record.cooldown = None,
                AccountOp::SetLastActivity(at) => {
                    self.record.last_activity =
                        Some(self.record.last_activity.map_or(*at, |t| t.max(*at)));
                }
                AccountOp::SetLabelsRefreshedAt(at) => {
                    self.record.labels_refreshed_at =
                        Some(self.record.labels_refreshed_at.map_or(*at, |t| t.max(*at)));
                }
                AccountOp::Block(reason) => {
                    let resume = match std::mem::replace(&mut self.record.sync, SyncState::Live) {
                        SyncState::Blocked { resume, .. } => resume,
                        current => Box::new(current),
                    };
                    self.record.sync = SyncState::Blocked {
                        reason: reason.clone(),
                        resume,
                    };
                }
            }
        }
        let mut skipped = 0;
        for op in &commit.messages {
            if !self.apply_message(op, &commit.job_plan, seq) {
                skipped += 1;
            }
            self.settle(op, commit.claim.as_ref(), seq);
        }
        for op in &commit.intents {
            if !self.apply_intent(op, &commit.job_plan, seq) {
                skipped += 1;
            }
        }
        for op in &commit.jobs {
            match op {
                JobOp::Enqueue(new) => self.enqueue(new),
                JobOp::Finish { claim, outcome } => {
                    if claim_holds(self.jobs.get(&claim.kind), claim) {
                        match self
                            .jobs
                            .get(&claim.kind)
                            .and_then(|job| finish(job, outcome))
                        {
                            Some(job) => {
                                self.jobs.insert(claim.kind.clone(), job);
                            }
                            None => {
                                self.jobs.remove(&claim.kind);
                            }
                        }
                    } else {
                        skipped += 1;
                    }
                }
            }
        }
        self.keep_verifying(commit);
        // Changes admitted while a delivery was in flight go out next.
        for op in &commit.jobs {
            if let JobOp::Finish { claim, .. } = op
                && let JobKind::Deliver(id) = &claim.kind
            {
                self.enqueue_delivery(id, &commit.job_plan);
            }
        }
        skipped
    }

    /// Apply one label definition change (see [`LabelOp`]).
    fn apply_label(&mut self, op: &LabelOp, seq: StateSeq) {
        match op {
            LabelOp::ReplaceAll {
                labels,
                generation,
                at,
            } => {
                // A local write since the listing was sent: it is stale. A
                // listing without Gmail's system labels is no listing at
                // all (a mailbox always lists them): it proves nothing.
                if *generation != self.record.label_generation
                    || !complete_listing(&self.labels, labels)
                {
                    return;
                }
                let cutoff = at.minus(DELETED_LABEL_MEMORY);
                let deleted = &mut self.record.deleted_labels;
                deleted.retain(|_, deleted_at| *deleted_at > cutoff);
                for label in labels {
                    deleted.remove(&label.id);
                }
                let gone = removed_labels(&self.labels, self.record.label_generation, op);
                if !same_labels(&self.labels, labels) {
                    self.labels.clone_from(labels);
                    self.log.push((seq, Change::LabelDefinitionsChanged));
                }
                for id in gone {
                    self.forget_label(&id, *at, seq);
                }
            }
            // An answer that lands after the label's deletion was recorded
            // (a patch racing a delete) must not bring it back: only a
            // listing restores a deleted id.
            LabelOp::Upsert(label) if self.record.deleted_labels.contains_key(&label.id) => {}
            LabelOp::Upsert(label) => {
                match self.labels.iter_mut().find(|stored| stored.id == label.id) {
                    Some(stored) if stored == label => {}
                    Some(stored) => {
                        stored.clone_from(label);
                        self.log.push((seq, Change::LabelDefinitionsChanged));
                    }
                    None => {
                        self.labels.push(label.clone());
                        self.log.push((seq, Change::LabelDefinitionsChanged));
                    }
                }
            }
            // Gmail's own labels are never deleted.
            LabelOp::Remove { .. }
                if removed_labels(&self.labels, self.record.label_generation, op).is_empty() => {}
            LabelOp::Remove { id, at } => {
                let before = self.labels.len();
                self.labels.retain(|label| &label.id != id);
                if self.labels.len() != before {
                    self.log.push((seq, Change::LabelDefinitionsChanged));
                }
                self.forget_label(id, *at, seq);
            }
        }
    }

    /// A label is gone from Gmail: it leaves every row, trusted or
    /// unverified, without moving a checkpoint (Gmail's own `labelsRemoved`
    /// records then change nothing), its intents are dropped as a deleted
    /// message's are, and its id is remembered as deleted from `at`.
    fn forget_label(&mut self, id: &LabelId, at: Timestamp, seq: StateSeq) {
        self.record.deleted_labels.insert(id.clone(), at);
        let carrying: Vec<MessageId> = self
            .messages
            .values()
            .filter(|record| record.labels.contains(id))
            .map(|record| record.id.clone())
            .collect();
        for message in carrying {
            if let Some(previous) = self.messages.get(&message) {
                let mut record = previous.clone();
                record.labels.remove(id);
                for change in row_changes(Some(previous), &record) {
                    self.log.push((seq, change));
                }
                self.messages.insert(message, record);
            }
        }
        let affected: BTreeSet<MessageId> = self
            .intents
            .keys()
            .filter(|(_, label)| label == id)
            .map(|(message, _)| message.clone())
            .collect();
        self.intents.retain(|(_, label), _| label != id);
        for message in affected {
            self.log_overlay(&message, seq);
        }
    }

    /// Record the answer of a local label write (a create, a patch or a
    /// delete the driver made): outside any engine, like
    /// [`Self::admit_action`], since a label write must survive a sync
    /// superseding another. Bumps the label generation, so a listing sent
    /// before it is not applied.
    pub fn record_label(&mut self, op: &LabelOp) {
        self.seq = StateSeq(self.seq.0 + 1);
        let seq = self.seq;
        self.apply_label(op, seq);
        self.record.label_generation += 1;
    }

    /// An unverified row keeps a verification queued until one settles it
    /// (a fetch from before the latest delta doesn't).
    fn keep_verifying(&mut self, commit: &Commit) {
        let touched: BTreeSet<MessageId> = commit
            .messages
            .iter()
            .filter_map(|op| match op {
                MessageOp::Observe(o) | MessageOp::MarkImported { observation: o, .. } => {
                    Some(o.id.clone())
                }
                MessageOp::PatchLabels { id, .. } => Some(id.clone()),
                MessageOp::Tombstone { .. }
                | MessageOp::Listed { .. }
                | MessageOp::FinishReconcile { .. } => None,
            })
            .chain(
                commit
                    .claim
                    .iter()
                    .map(|claim| claim.kind.message().clone()),
            )
            .collect();
        let touched: BTreeSet<MessageId> = touched
            .into_iter()
            .chain(commit.intents.iter().flat_map(|op| {
                match op {
                    IntentOp::Delivered { versions, .. } | IntentOp::Block { versions, .. } => {
                        versions
                            .iter()
                            .map(|d| d.message.clone())
                            .collect::<Vec<_>>()
                    }
                    IntentOp::Request(_) => Vec::new(),
                }
            }))
            .collect();
        for id in touched {
            let unverified = self
                .messages
                .get(&id)
                .is_some_and(|r| r.unverified.is_some());
            // A delivered action waits for a fetch claimed after it.
            let awaiting = self.messages.contains_key(&id)
                && self.intents.values().any(|intent| {
                    intent.message == id && matches!(intent.state, IntentState::Delivered { .. })
                });
            let verify_queued = self.jobs.contains_key(&JobKind::Verify(id.clone()));
            let import_queued = self.jobs.contains_key(&JobKind::Import(id.clone()));
            if awaiting && !verify_queued {
                self.enqueue(&NewJob {
                    kind: JobKind::Verify(id),
                    priority: Priority::Live,
                    due_at: commit.job_plan.confirm_due_at,
                });
            } else if unverified && !verify_queued && !import_queued {
                self.enqueue(&NewJob {
                    kind: JobKind::Verify(id),
                    priority: commit.job_plan.priority,
                    due_at: commit.job_plan.verify_due_at,
                });
            }
        }
    }

    /// Enqueue delivery for a message with pending intents (no-op otherwise).
    fn enqueue_delivery(&mut self, id: &MessageId, plan: &JobPlan) {
        let pending = self
            .intents
            .values()
            .any(|intent| &intent.message == id && intent.state == IntentState::Pending);
        if pending && self.messages.contains_key(id) {
            self.enqueue(&NewJob {
                kind: JobKind::Deliver(id.clone()),
                priority: Priority::Foreground,
                due_at: plan.deliver_due_at,
            });
        }
    }

    /// Drop the intents of deleted messages; clear delivered intents a
    /// confirmation fetch settles.
    fn settle(&mut self, op: &MessageOp, claim: Option<&JobClaim>, seq: StateSeq) {
        let id = match (op, claim) {
            (MessageOp::Tombstone { id, .. }, _) => {
                // The deletion itself is the change consumers see.
                self.intents.retain(|(message, _), _| message != id);
                return;
            }
            (MessageOp::Observe(observation), Some(claim))
                if claim.kind == JobKind::Verify(observation.id.clone()) =>
            {
                (&observation.id, claim.attempt)
            }
            // Only a verification fetch can confirm a delivery: an
            // observation from history or an import may predate it.
            (
                MessageOp::Observe(_)
                | MessageOp::PatchLabels { .. }
                | MessageOp::MarkImported { .. }
                | MessageOp::Listed { .. }
                | MessageOp::FinishReconcile { .. },
                _,
            ) => return,
        };
        let (id, attempt) = id;
        let before = self.intents.len();
        self.intents
            .retain(|(message, _), intent| message != id || !settles(intent, attempt));
        if self.intents.len() != before {
            self.log_overlay(id, seq);
        }
    }

    /// Record how a message should look now that its intents changed.
    fn log_overlay(&mut self, id: &MessageId, seq: StateSeq) {
        if let Some(record) = self.messages.get(id) {
            let labels = labels::effective(&record.labels, &self.intents_of(id));
            let thread_id = record.thread_id.clone();
            self.log.push((
                seq,
                Change::OverlayChanged {
                    id: id.clone(),
                    thread_id,
                    labels,
                },
            ));
        }
    }

    /// Apply one intent operation; `false` if it changed nothing.
    fn apply_intent(&mut self, op: &IntentOp, plan: &JobPlan, seq: StateSeq) -> bool {
        match op {
            IntentOp::Request(request) => {
                let (targets, action): (Vec<MessageId>, _) = match request {
                    ActionRequest::Messages { ids, action } => (ids.clone(), action),
                    ActionRequest::Thread { id, action } => (
                        self.thread_members(id)
                            .filter(|record| !labels::is_draft(&record.labels))
                            .map(|record| record.id.clone())
                            .collect(),
                        action,
                    ),
                };
                let mut admitted = false;
                for target in targets {
                    let Some(record) = self.messages.get(&target) else {
                        continue;
                    };
                    // A change to a label remembered as deleted is not
                    // admitted: Gmail would refuse to add it, and removing
                    // it changes nothing.
                    let new: Vec<Intent> = admit(record, &self.intents_of(&target), action)
                        .into_iter()
                        .filter(|intent| !self.record.deleted_labels.contains_key(&intent.label))
                        .collect();
                    if new.is_empty() {
                        continue;
                    }
                    for intent in new {
                        self.intents
                            .insert((intent.message.clone(), intent.label.clone()), intent);
                    }
                    self.log_overlay(&target, seq);
                    self.enqueue_delivery(&target, plan);
                    admitted = true;
                }
                admitted
            }
            IntentOp::Delivered { versions, at, by } => {
                let mut any = false;
                for delivery in versions {
                    if let Some(intent) = self.matching(delivery) {
                        intent.state = IntentState::Delivered {
                            at: *at,
                            attempt: *by,
                        };
                        any = true;
                    }
                }
                let messages: BTreeSet<MessageId> =
                    versions.iter().map(|d| d.message.clone()).collect();
                for message in messages {
                    if self.messages.contains_key(&message) {
                        self.enqueue(&NewJob {
                            kind: JobKind::Verify(message),
                            priority: Priority::Live,
                            due_at: plan.confirm_due_at,
                        });
                    }
                }
                any
            }
            IntentOp::Block { versions, failure } => {
                let mut any = false;
                for delivery in versions {
                    if let Some(intent) = self.matching(delivery) {
                        intent.state = IntentState::Blocked(failure.clone());
                        any = true;
                        let message = delivery.message.clone();
                        self.log_overlay(&message, seq);
                    }
                }
                any
            }
        }
    }

    /// The pending intent a delivery carried, if it is still that version.
    fn matching(&mut self, delivery: &Delivery) -> Option<&mut Intent> {
        self.intents
            .get_mut(&(delivery.message.clone(), delivery.label.clone()))
            .filter(|intent| {
                intent.version == delivery.version && intent.state == IntentState::Pending
            })
    }

    fn enqueue(&mut self, new: &NewJob) {
        let job = enqueue(self.jobs.get(&new.kind), new);
        self.jobs.insert(new.kind.clone(), job);
    }

    /// Apply one message operation; `false` if its condition didn't hold.
    fn apply_message(&mut self, op: &MessageOp, plan: &JobPlan, seq: StateSeq) -> bool {
        let filtered;
        let op = if self.record.deleted_labels.is_empty() {
            op
        } else {
            filtered = without_deleted(op, &self.record.deleted_labels);
            &filtered
        };
        let decision = match op {
            MessageOp::Observe(observation) => observe(
                self.messages.get(&observation.id),
                self.tombstones.get(&observation.id).copied(),
                observation,
                plan,
            ),
            MessageOp::PatchLabels {
                id,
                add,
                remove,
                checkpoint,
            } => patch_labels(
                self.messages.get(id),
                self.tombstones.get(id).copied(),
                id,
                add,
                remove,
                *checkpoint,
                plan,
            ),
            MessageOp::MarkImported {
                observation,
                blob,
                size,
                internal_date,
                headers,
                facts,
            } => mark_imported(
                self.messages.get(&observation.id),
                self.tombstones.get(&observation.id).copied(),
                observation,
                StoredContent {
                    blob: blob.clone(),
                    size: *size,
                    internal_date: *internal_date,
                    headers: (**headers).clone(),
                    facts: facts.as_deref().cloned(),
                },
                plan,
            ),
            MessageOp::Tombstone { id, evidence } => {
                let import_running = matches!(
                    self.jobs
                        .get(&JobKind::Import(id.clone()))
                        .map(|job| &job.state),
                    Some(JobState::Running(_))
                );
                self.seen.remove(id);
                tombstone(
                    self.messages.get(id),
                    self.tombstones.get(id).copied(),
                    id,
                    *evidence,
                    import_running,
                    plan,
                )
            }
            MessageOp::Listed {
                id,
                thread_id,
                scope,
                baseline: _,
            } => {
                let decision = listed(
                    self.messages.get(id),
                    self.tombstones.get(id).copied(),
                    id,
                    thread_id,
                    plan,
                );
                if decision.row != MessageEffect::Skip {
                    self.seen
                        .entry(id.clone())
                        .or_default()
                        .insert(scope.clone());
                }
                decision
            }
            MessageOp::FinishReconcile { baseline } => {
                self.finish_reconciliation(*baseline, plan, seq);
                return true;
            }
        };
        self.realize(decision, seq)
    }

    /// [`MessageOp::FinishReconcile`]: every row, with the scopes it was
    /// listed in.
    fn finish_reconciliation(&mut self, baseline: HistoryId, plan: &JobPlan, seq: StateSeq) {
        let mut seen = std::mem::take(&mut self.seen);
        // A label deleted since it was enumerated lists nothing.
        for scopes in seen.values_mut() {
            scopes.retain(|scope| match scope {
                ListScope::All => true,
                ListScope::Label(label) => !self.record.deleted_labels.contains_key(label),
            });
        }
        let decisions: Vec<Decision> = self
            .messages
            .values()
            .map(|record| finish_reconcile(record, seen.get(&record.id), baseline, plan))
            .collect();
        for decision in decisions {
            self.realize(decision, seq);
        }
    }

    /// Carry out a decision; `false` if it was a skip.
    fn realize(&mut self, decision: Decision, seq: StateSeq) -> bool {
        let applied = match decision.row {
            MessageEffect::Skip => false,
            MessageEffect::Keep => true,
            MessageEffect::Put(record) => {
                let previous = self.messages.get(&record.id);
                for change in row_changes(previous, &record) {
                    self.log.push((seq, change));
                }
                self.messages.insert(record.id.clone(), record);
                true
            }
            MessageEffect::Delete { id, checkpoint } => {
                let previous = self.messages.remove(&id);
                self.tombstones.insert(id.clone(), checkpoint);
                self.log.push((
                    seq,
                    Change::MessageDeleted {
                        id,
                        thread_id: previous.as_ref().map(|r| r.thread_id.clone()),
                        labels: previous.map(|r| r.labels),
                    },
                ));
                true
            }
        };
        for kind in &decision.cancel {
            self.jobs.remove(kind);
        }
        for job in &decision.enqueue {
            self.enqueue(job);
        }
        applied
    }

    /// What the store holds about the account (pure; no claim).
    #[must_use]
    pub fn status(&self) -> crate::status::AccountStatus {
        let mut jobs = crate::status::JobCounts::default();
        for job in self.jobs.values() {
            jobs.add(&job.state);
        }
        let mut actions = crate::status::ActionCounts::default();
        for intent in self.intents.values() {
            actions.add(&intent.state);
        }
        let count = |pred: &dyn Fn(&MessageRecord) -> bool| {
            self.messages.values().filter(|r| pred(r)).count() as u64
        };
        crate::status::AccountStatus {
            account: self.account.clone(),
            sync: self.record.sync.clone(),
            epoch: self.record.epoch,
            cursor: self.record.cursor,
            watch_expiration: self.record.watch_expiration,
            last_activity: self.record.last_activity,
            cooldown: self.record.cooldown.clone(),
            state: self.seq,
            generation: self.record.generation,
            messages: self.messages.len() as u64,
            pending_imports: count(&|r| matches!(r.content, Content::Pending)),
            unverified: count(&|r| r.unverified.is_some()),
            jobs,
            actions,
        }
    }

    /// Resume a blocked account after an operator fixed the cause: restore
    /// the phase it was blocked in and make jobs blocked by the account
    /// problem ready again (jobs Gmail refused for good stay blocked). The
    /// next engine to claim the account carries on.
    pub fn resume(&mut self, now: Timestamp) -> crate::status::ResumeOutcome {
        let resume = match &self.record.sync {
            SyncState::Blocked { resume, .. } => (**resume).clone(),
            SyncState::Bootstrapping(_) | SyncState::Live | SyncState::Reconciling(_) => {
                return crate::status::ResumeOutcome::NotBlocked;
            }
        };
        let phase = resume.phase();
        self.record.sync = resume;
        let mut jobs = 0;
        for job in self.jobs.values_mut() {
            if let JobState::Blocked(crate::retry::BlockingFailure {
                class: crate::retry::Blocking::Account(_),
                ..
            }) = job.state
            {
                job.state = JobState::Ready { due_at: now };
                jobs += 1;
            }
        }
        crate::status::ResumeOutcome::Resumed { phase, jobs }
    }

    /// Admit a user action directly, outside any engine: a user's intent is
    /// not fenced by whichever engine currently holds the account, so a sync
    /// that supersedes another can't lose it. Delivery is a durable job the
    /// current engine runs. Returns how many messages got new intents.
    pub fn admit_action(&mut self, request: &ActionRequest, now: Timestamp) -> u32 {
        let plan = JobPlan {
            priority: Priority::Foreground,
            import_due_at: now,
            draft_import_due_at: now,
            verify_due_at: now,
            cleanup_due_at: now,
            deliver_due_at: now,
            confirm_due_at: now,
        };
        self.seq = StateSeq(self.seq.0 + 1);
        let seq = self.seq;
        self.apply_intent(&IntentOp::Request(request.clone()), &plan, seq);
        let admitted = self.log.iter().filter(|(s, _)| *s == seq).count();
        if admitted > 0 {
            self.record.last_activity = Some(self.record.last_activity.map_or(now, |t| t.max(now)));
        }
        u32::try_from(admitted).unwrap_or(u32::MAX)
    }

    /// Record deletions a purge proved with `messagesDeleted` history
    /// records, outside any engine: like
    /// [`Self::admit_action`] it is not fenced by an epoch. Each goes
    /// through the same tombstone rule as a sync's: the row goes, the
    /// tombstone stays, `MessageDeleted` is logged with the last thread and
    /// labels, the message's jobs and intents are cancelled and its content
    /// gets a cleanup after [`crate::engine::BLOB_GRACE`]. Applying a
    /// deletion ahead of the cursor is sound because deletion is final
    /// (I5): every later record about the id is skipped, and a repeated
    /// tombstone logs nothing. Returns how many messages it deleted.
    pub fn record_deleted(
        &mut self,
        deletions: &[(MessageId, DeletionEvidence)],
        now: Timestamp,
    ) -> u32 {
        let plan = JobPlan {
            priority: Priority::Live,
            import_due_at: now,
            draft_import_due_at: now,
            verify_due_at: now,
            cleanup_due_at: now.plus(crate::engine::BLOB_GRACE),
            deliver_due_at: now,
            confirm_due_at: now,
        };
        let seq = StateSeq(self.seq.0 + 1);
        let mut deleted = 0;
        for (id, evidence) in deletions {
            let op = MessageOp::Tombstone {
                id: id.clone(),
                evidence: *evidence,
            };
            if self.apply_message(&op, &plan, seq) {
                deleted += 1;
                self.settle(&op, None, seq);
            }
        }
        if deleted > 0 {
            self.seq = seq;
        }
        deleted
    }

    /// Up to `limit` ids of messages whose observed labels hold `label`,
    /// after `after`, in id order (what `empty` pages through).
    #[must_use]
    pub fn message_ids_labelled(
        &self,
        label: &crate::ids::LabelId,
        after: Option<&MessageId>,
        limit: u32,
    ) -> Vec<MessageId> {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        self.messages
            .values()
            .filter(|record| after.is_none_or(|after| record.id > *after))
            .filter(|record| record.labels.contains(label))
            .take(limit)
            .map(|record| record.id.clone())
            .collect()
    }

    /// Import jobs in the Foreground class, ready or running (what
    /// [`FOREGROUND_IMPORT_CAP`] bounds).
    #[must_use]
    pub fn foreground_imports(&self) -> usize {
        self.jobs
            .values()
            .filter(|job| {
                matches!(job.kind, JobKind::Import(_))
                    && job.priority == Priority::Foreground
                    && matches!(job.state, JobState::Ready { .. } | JobState::Running(_))
            })
            .count()
    }

    /// A user opened these messages: raise their queued imports to the
    /// Foreground class, outside any engine (like [`Self::admit_action`]).
    /// Safe without the epoch fence because it only ever raises a class and
    /// never creates or finishes a job; nothing else changes, so the feed
    /// and the account's state stay where they were. Each distinct id is
    /// reported in exactly one group of [`Prioritised`].
    pub fn prioritise(&mut self, ids: &[MessageId], now: Timestamp) -> Prioritised {
        let foreground = self.foreground_imports();
        self.prioritise_counted(ids, now, foreground)
    }

    /// [`Self::prioritise`] for a mirror holding only the rows and jobs of
    /// `ids` (a SQL store's partial load), with the account's Foreground
    /// imports counted by the store.
    pub fn prioritise_counted(
        &mut self,
        ids: &[MessageId],
        now: Timestamp,
        mut foreground: usize,
    ) -> Prioritised {
        let mut report = Prioritised::default();
        let mut seen = BTreeSet::new();
        for id in ids {
            if !seen.insert(id) {
                continue;
            }
            let kind = JobKind::Import(id.clone());
            let group = match (
                self.messages.get(id).map(|r| &r.content),
                self.jobs.get(&kind),
            ) {
                (Some(Content::Imported(_)), _) => &mut report.imported,
                // No row (never discovered, or tombstoned); a pending row
                // always has its import (I6), so none is created here.
                (None, _) | (Some(Content::Pending), None) => &mut report.unknown,
                (Some(Content::Pending), Some(job)) => match job.state {
                    JobState::Blocked(_) => &mut report.blocked,
                    JobState::Ready { .. } | JobState::Running(_)
                        if job.priority == Priority::Foreground =>
                    {
                        &mut report.queued
                    }
                    JobState::Ready { .. } | JobState::Running(_)
                        if foreground >= FOREGROUND_IMPORT_CAP =>
                    {
                        &mut report.over_limit
                    }
                    JobState::Ready { .. } | JobState::Running(_) => {
                        let raised = crate::jobs::prioritise(job, now);
                        self.jobs.insert(kind, raised);
                        foreground += 1;
                        &mut report.queued
                    }
                },
            };
            group.push(id.clone());
        }
        report
    }

    /// Bring stored facts up to date, outside any engine: like an admitted
    /// action, a refresh is not fenced by the engine holding the account and
    /// does not supersede it. Each update applies only by [`refresh_applies`];
    /// every applied one logs a `FactsRefreshed` entry, all under one new
    /// state (none when nothing applied). Returns how many applied.
    pub fn refresh_facts(&mut self, updates: &[FactsUpdate]) -> u32 {
        let seq = StateSeq(self.seq.0 + 1);
        let mut applied = 0;
        for update in updates {
            let Some(record) = self.messages.get_mut(&update.id) else {
                continue;
            };
            if !refresh_applies(record, update) {
                continue;
            }
            if let Content::Imported(stored) = &mut record.content {
                stored.facts = Some(update.facts.clone());
                stored.size = update.size;
            }
            self.log.push((
                seq,
                Change::FactsRefreshed {
                    id: record.id.clone(),
                    thread_id: record.thread_id.clone(),
                },
            ));
            applied += 1;
        }
        if applied > 0 {
            self.seq = seq;
        }
        applied
    }

    /// When the earliest ready job is due (see [`crate::jobs::next_due`]).
    #[must_use]
    pub fn next_due(&self) -> Option<Timestamp> {
        let jobs: Vec<Job> = self.jobs.values().cloned().collect();
        crate::jobs::next_due(&jobs)
    }

    /// Claim up to `limit` due jobs for `attempt`, for the engine of `epoch`.
    /// A claim from an older engine is refused, so its jobs can't be held by
    /// an engine that no longer exists.
    pub fn claim_jobs(
        &mut self,
        epoch: Epoch,
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
    ) -> Result<Vec<ClaimedJob>, Rejection> {
        self.claim_jobs_from(epoch, limit, now, attempt, crate::quota::Priority::Backfill)
    }

    /// [`Self::claim_jobs`] for jobs of at least `floor` priority.
    pub fn claim_jobs_from(
        &mut self,
        epoch: Epoch,
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
        floor: crate::quota::Priority,
    ) -> Result<Vec<ClaimedJob>, Rejection> {
        if epoch != self.record.epoch {
            return Err(Rejection::StaleEpoch);
        }
        let mut claimed = Vec::new();
        for _ in 0..limit {
            let jobs: Vec<Job> = self.jobs.values().cloned().collect();
            let Some(job) = crate::jobs::pick_next_from(&jobs, now, floor) else {
                break;
            };
            let kind = job.kind.clone();
            let work = ClaimedWork::plan(kind.clone(), |id| {
                outbox::plan_delivery(&self.intents_of(id))
                    .into_iter()
                    .next()
            });
            claimed.push(ClaimedJob {
                work,
                priority: job.priority,
                failed_attempts: job.failed_attempts,
                attempt,
            });
            if let Some(job) = self.jobs.get_mut(&kind) {
                job.state = JobState::Running(attempt);
            }
        }
        Ok(claimed)
    }

    /// Drop change-feed entries as `retention` allows (see
    /// [`Self::retain`]). Returns how many were removed.
    pub fn compact(&mut self, retention: &Retention) -> u64 {
        let before = self.log.len();
        if let Some(through) = self.retain(retention) {
            self.log.retain(|(seq, _)| *seq > through);
        }
        (before - self.log.len()) as u64
    }

    /// Decide a compaction without touching the log (stores that keep the
    /// log elsewhere delete it themselves): every cursor at the head is seen
    /// now, then the floor rises to the target of [`compaction_target`],
    /// clamped to the head. Returns that target; `None` drops nothing.
    /// Cursors are never removed: one left behind the floor is lost.
    pub fn retain(&mut self, retention: &Retention) -> Option<StateSeq> {
        let head = FeedCursor::from(self.seq);
        for cursor in self.consumers.values_mut() {
            if cursor.at >= head {
                cursor.seen_at = cursor.seen_at.max(retention.now);
            }
        }
        let through = compaction_target(
            self.consumers.values(),
            self.record.log_floor,
            self.seq,
            retention,
        )?
        .min(self.seq);
        self.record.log_floor = self.record.log_floor.max(through);
        Some(through)
    }

    /// Consumer `name` has processed the feed up to `at`, at `now`: set its
    /// cursor there (also backwards: the consumer's own position is the
    /// truth), registering it if the name is new. A position this log can't
    /// be held from (behind the floor or inside the floor commit, beyond
    /// the head, of another generation) is `TooOld` and stores nothing.
    pub fn ack(&mut self, name: &ConsumerName, at: FeedPosition, now: Timestamp) -> AckOutcome {
        let floor = self.record.log_floor;
        if !at.fits(self.record.generation, self.seq) || at.at.is_compacted(floor) {
            return AckOutcome::TooOld {
                generation: self.record.generation,
                oldest: floor,
            };
        }
        if !self.consumers.contains_key(name) && self.consumers.len() >= MAX_CONSUMERS {
            return AckOutcome::TooManyConsumers;
        }
        let cursor = ConsumerCursor {
            name: name.clone(),
            at: at.at,
            seen_at: now,
        };
        self.consumers.insert(name.clone(), cursor.clone());
        AckOutcome::Acked(cursor)
    }

    /// Forget consumer `name`: it no longer holds the log. Whether it was
    /// registered.
    pub fn drop_consumer(&mut self, name: &ConsumerName) -> bool {
        self.consumers.remove(name).is_some()
    }

    /// The registered consumers, in name order.
    #[must_use]
    pub fn consumer_list(&self) -> ConsumerList {
        ConsumerList {
            generation: self.record.generation,
            floor: self.record.log_floor,
            cursors: self.consumers.values().cloned().collect(),
        }
    }

    /// Up to `limit` message ids after `after`, in id order (to rebuild a
    /// consumer's copy, e.g. after `ChangeFeed::TooOld`).
    #[must_use]
    pub fn message_ids(&self, after: Option<&MessageId>, limit: u32) -> Vec<MessageId> {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        self.messages
            .keys()
            .filter(|id| after.is_none_or(|after| *id > after))
            .take(limit)
            .cloned()
            .collect()
    }

    /// The feed after `since`, or `TooOld` if entries after it were
    /// compacted away (it is behind the floor, or inside the floor commit).
    #[must_use]
    pub fn feed(&self, since: FeedCursor, limit: u32) -> crate::revision::ChangeFeed {
        if since.is_compacted(self.record.log_floor) {
            return crate::revision::ChangeFeed::TooOld {
                generation: self.record.generation,
                oldest: self.record.log_floor,
            };
        }
        crate::revision::ChangeFeed::Page(self.changes(since, limit))
    }

    /// The content stored for an import or refresh entry's message, if it
    /// is still here.
    fn imported_content(&self, change: &Change) -> Option<StoredContent> {
        let (Change::MessageImported { id, .. } | Change::FactsRefreshed { id, .. }) = change
        else {
            return None;
        };
        match &self.messages.get(id)?.content {
            Content::Imported(stored) => Some((**stored).clone()),
            Content::Pending => None,
        }
    }

    /// Changes after `since` (no compaction check; see [`Self::feed`]): at
    /// most `limit` (clamped to `1..=MAX_FEED_PAGE`), ending at the last
    /// commit boundary within it, inside a commit only when that commit is
    /// larger than `limit` (see [`crate::revision::cut_page`]). An entry's
    /// index counts from its commit's first entry, wherever the page starts.
    #[must_use]
    pub fn changes(&self, since: FeedCursor, limit: u32) -> ChangePage {
        let window = usize::try_from(page_window(limit)).unwrap_or(usize::MAX);
        // The log is in commit order: start at the first entry of `since`'s
        // commit and number entries from there.
        let first = self.log.partition_point(|(seq, _)| *seq < since.seq);
        let entries: Vec<_> = self.log[first..]
            .iter()
            .scan(
                None,
                |position: &mut Option<(StateSeq, u32)>, (seq, change)| {
                    let index = match *position {
                        Some((current, index)) if current == *seq => index + 1,
                        Some(_) | None => 0,
                    };
                    *position = Some((*seq, index));
                    Some((*seq, index, change))
                },
            )
            .filter(|(seq, index, _)| {
                FeedCursor {
                    seq: *seq,
                    entry: Some(*index),
                } > since
            })
            .take(window)
            .collect();
        let identities: Vec<_> = entries
            .iter()
            .map(|(seq, index, _)| (*seq, *index))
            .collect();
        let cut = cut_page(&identities, limit);
        let changes = entries
            .into_iter()
            .take(cut.take)
            .map(|(state, index, change)| crate::revision::ChangeEntry {
                state,
                index,
                change: change.clone(),
                content: self.imported_content(change),
            })
            .collect();
        ChangePage {
            generation: self.record.generation,
            head: self.seq,
            changes,
            next: cut.next.unwrap_or(since),
            has_more: cut.has_more,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CursorMove;
    use crate::ids::{CommitId, ENGINE_VERSION, LabelId};
    use crate::labels::set;
    use crate::quota::Priority;
    use crate::revision::MAX_FEED_PAGE;

    fn account() -> AccountId {
        AccountId("alice@example.com".into())
    }

    fn plan() -> JobPlan {
        JobPlan {
            priority: Priority::Live,
            import_due_at: Timestamp(0),
            draft_import_due_at: Timestamp(120),
            verify_due_at: Timestamp(0),
            cleanup_due_at: Timestamp(1_000),
            deliver_due_at: Timestamp(0),
            confirm_due_at: Timestamp(0),
        }
    }

    fn commit(mirror: &AccountMirror) -> Commit {
        Commit {
            id: CommitId {
                epoch: mirror.record.epoch,
                seq: 0,
            },
            account: account(),
            epoch: mirror.record.epoch,
            engine_version: ENGINE_VERSION,
            cursor: None,
            claim: None,
            sync: None,
            messages: Vec::new(),
            labels: Vec::new(),
            account_ops: Vec::new(),
            job_plan: plan(),
            jobs: Vec::new(),
            intents: Vec::new(),
        }
    }

    fn observation(id: &str, labels: &[&str], checkpoint: u64) -> MessageObservation {
        MessageObservation {
            id: MessageId(id.into()),
            thread_id: ThreadId("t".into()),
            labels: set(labels.iter().copied()),
            checkpoint: HistoryId(checkpoint),
        }
    }

    fn claimed() -> AccountMirror {
        let mut mirror = AccountMirror::new(account(), 1);
        mirror.claim(ENGINE_VERSION).expect("claim");
        mirror
    }

    #[test]
    fn observing_a_new_message_records_it_and_its_import() {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![MessageOp::Observe(observation("a", &["INBOX"], 10))];
        assert!(matches!(
            mirror.apply(&c),
            CommitOutcome::Applied { skipped: 0, .. }
        ));
        assert_eq!(
            mirror.messages[&MessageId("a".into())].content,
            Content::Pending
        );
        assert!(
            mirror
                .jobs
                .contains_key(&JobKind::Import(MessageId("a".into())))
        );
    }

    #[test]
    fn older_observations_never_overwrite_newer_state() {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![
            MessageOp::Observe(observation("a", &["INBOX"], 20)),
            MessageOp::Observe(observation("a", &["SPAM"], 10)),
        ];
        assert!(matches!(
            mirror.apply(&c),
            CommitOutcome::Applied { skipped: 1, .. }
        ));
        assert_eq!(
            mirror.messages[&MessageId("a".into())].labels,
            set(["INBOX"])
        );
    }

    #[test]
    fn tombstones_reject_resurrection() {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![
            MessageOp::Tombstone {
                id: MessageId("a".into()),
                evidence: DeletionEvidence::History { at: HistoryId(30) },
            },
            MessageOp::Observe(observation("a", &["INBOX"], 40)),
        ];
        let _ = mirror.apply(&c);
        assert!(!mirror.messages.contains_key(&MessageId("a".into())));
        assert_eq!(mirror.tombstones[&MessageId("a".into())], HistoryId(30));
    }

    #[test]
    fn label_patches_for_unknown_messages_enqueue_verification() {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![MessageOp::PatchLabels {
            id: MessageId("x".into()),
            add: set(["STARRED"]),
            remove: LabelSet::new(),
            checkpoint: HistoryId(5),
        }];
        let _ = mirror.apply(&c);
        assert!(
            mirror
                .jobs
                .contains_key(&JobKind::Verify(MessageId("x".into())))
        );
    }

    #[test]
    fn guards_reject_the_whole_commit() {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![MessageOp::Observe(observation("a", &["INBOX"], 10))];
        c.cursor = Some(CursorMove {
            expected: Some(HistoryId(1)),
            to: HistoryId(2),
        });
        assert_eq!(
            mirror.apply(&c),
            CommitOutcome::Rejected(Rejection::CursorMismatch)
        );
        assert!(mirror.messages.is_empty());
        let mut stale = commit(&mirror);
        stale.epoch = Epoch(0);
        assert_eq!(
            mirror.apply(&stale),
            CommitOutcome::Rejected(Rejection::StaleEpoch)
        );
    }

    #[test]
    fn a_new_claim_supersedes_old_writers_and_releases_their_jobs() {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![MessageOp::Observe(observation("a", &["INBOX"], 10))];
        let _ = mirror.apply(&c);
        let epoch = mirror.record.epoch;
        let jobs = mirror
            .claim_jobs(epoch, 4, Timestamp(0), AttemptId(1))
            .expect("current epoch");
        assert_eq!(jobs.len(), 1);
        let old = commit(&mirror);
        mirror.claim(ENGINE_VERSION).expect("second claim");
        assert_eq!(
            mirror.apply(&old),
            CommitOutcome::Rejected(Rejection::StaleEpoch)
        );
        let epoch = mirror.record.epoch;
        assert_eq!(
            mirror
                .claim_jobs(epoch, 4, Timestamp(0), AttemptId(2))
                .map(|j| j.len()),
            Ok(1)
        );
        assert_eq!(
            mirror.claim_jobs(Epoch(epoch.0 - 1), 4, Timestamp(0), AttemptId(3)),
            Err(Rejection::StaleEpoch)
        );
    }

    #[test]
    fn reconciliation_verifies_every_older_message() {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![
            MessageOp::Observe(observation("same", &["INBOX"], 5)),
            MessageOp::Observe(observation("differs", &["INBOX"], 5)),
            MessageOp::Observe(observation("unlisted", &["INBOX"], 5)),
        ];
        let _ = mirror.apply(&c);
        let inbox = ListScope::Label(LabelId("INBOX".into()));
        let starred = ListScope::Label(LabelId("STARRED".into()));
        let listed = |id: &str, scope: ListScope| MessageOp::Listed {
            id: MessageId(id.into()),
            thread_id: ThreadId("t".into()),
            scope,
            baseline: HistoryId(50),
        };
        let mut listing = commit(&mirror);
        listing.messages = vec![
            listed("same", ListScope::All),
            listed("same", inbox.clone()),
            listed("differs", ListScope::All),
            listed("differs", starred),
            listed("new", ListScope::All),
            listed("new", inbox),
            MessageOp::FinishReconcile {
                baseline: HistoryId(50),
            },
        ];
        let _ = mirror.apply(&listing);
        let verify = |id: &str| {
            mirror
                .jobs
                .contains_key(&JobKind::Verify(MessageId(id.into())))
        };
        assert!(
            verify("same"),
            "even an agreeing listing can't prove older labels"
        );
        assert!(
            verify("differs"),
            "a listing that disagrees is checked, not trusted"
        );
        assert_eq!(
            mirror.messages[&MessageId("differs".into())].labels,
            set(["INBOX"])
        );
        assert!(verify("unlisted"));
        let new = &mirror.messages[&MessageId("new".into())];
        assert_eq!(new.labels, set(["INBOX"]));
        assert_eq!(
            new.checkpoint, None,
            "provisional until its import observes it"
        );
        assert!(mirror.seen.is_empty());
    }

    #[test]
    fn deleting_a_message_whose_import_may_have_crashed_schedules_cleanup() {
        // The import wrote the blob, then the process died before its
        // commit: the row still says Pending and no import is running.
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![MessageOp::Observe(observation("a", &["INBOX"], 1))];
        mirror.apply(&c);
        let mut deleted = commit(&mirror);
        deleted.messages = vec![MessageOp::Tombstone {
            id: MessageId("a".into()),
            evidence: DeletionEvidence::History { at: HistoryId(2) },
        }];
        mirror.apply(&deleted);
        assert!(
            mirror
                .jobs
                .contains_key(&JobKind::BlobCleanup(MessageId("a".into())))
        );
    }

    /// A trashed message with a pending action, as a purge finds it.
    fn trashed_with_an_action() -> AccountMirror {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![MessageOp::Observe(MessageObservation {
            thread_id: ThreadId("t-a".into()),
            ..observation("a", &["TRASH", "Label_1"], 10)
        })];
        mirror.apply(&c);
        mirror.admit_action(
            &ActionRequest::Messages {
                ids: vec![MessageId("a".into())],
                action: crate::outbox::Action::SetStar(crate::outbox::StarState::Starred),
            },
            Timestamp(0),
        );
        mirror
    }

    #[test]
    fn a_recorded_deletion_is_a_tombstone_like_any_other() {
        let mut mirror = trashed_with_an_action();
        let a = MessageId("a".into());
        assert!(mirror.jobs.contains_key(&JobKind::Import(a.clone())));
        assert!(mirror.jobs.contains_key(&JobKind::Deliver(a.clone())));
        assert!(!mirror.intents_of(&a).is_empty());
        // Recorded outside any engine: a newer epoch doesn't fence it.
        mirror.claim(ENGINE_VERSION).expect("a newer engine");
        let before = mirror.seq;
        let deleted = mirror.record_deleted(
            &[(a.clone(), DeletionEvidence::History { at: HistoryId(12) })],
            Timestamp(5_000),
        );
        assert_eq!(deleted, 1);
        assert!(!mirror.messages.contains_key(&a), "the row goes");
        assert_eq!(mirror.tombstones.get(&a), Some(&HistoryId(12)));
        assert!(mirror.intents_of(&a).is_empty(), "its intents go too");
        assert!(!mirror.jobs.contains_key(&JobKind::Import(a.clone())));
        assert!(!mirror.jobs.contains_key(&JobKind::Deliver(a.clone())));
        assert_eq!(
            mirror.jobs[&JobKind::BlobCleanup(a.clone())].state,
            JobState::Ready {
                due_at: Timestamp(5_000).plus(crate::engine::BLOB_GRACE)
            },
            "its content is cleaned up after the usual grace period"
        );
        assert_eq!(mirror.seq, StateSeq(before.0 + 1));
        let logged: Vec<&Change> = mirror
            .log
            .iter()
            .filter(|(seq, _)| *seq == mirror.seq)
            .map(|(_, change)| change)
            .collect();
        assert_eq!(
            logged,
            [&Change::MessageDeleted {
                id: a.clone(),
                thread_id: Some(ThreadId("t-a".into())),
                labels: Some(set(["Label_1", "TRASH"])),
            }]
        );
        assert!(crate::invariants::check_state(&mirror).is_empty());
        // An observation from before the deletion can't bring it back.
        let mut late = commit(&mirror);
        late.messages = vec![MessageOp::Observe(observation("a", &["TRASH"], 11))];
        mirror.apply(&late);
        assert!(!mirror.messages.contains_key(&a));
    }

    #[test]
    fn history_replaying_a_recorded_deletion_logs_nothing() {
        let mut mirror = trashed_with_an_action();
        let a = MessageId("a".into());
        mirror.record_deleted(
            &[(a.clone(), DeletionEvidence::History { at: HistoryId(12) })],
            Timestamp(0),
        );
        let entries = mirror.log.len();
        let seq = mirror.seq;
        // The sync reads the same messagesDeleted record later.
        let mut sync = commit(&mirror);
        sync.messages = vec![MessageOp::Tombstone {
            id: a.clone(),
            evidence: DeletionEvidence::History { at: HistoryId(12) },
        }];
        assert!(matches!(
            mirror.apply(&sync),
            CommitOutcome::Applied { skipped: 1, .. }
        ));
        assert_eq!(mirror.log.len(), entries, "no second MessageDeleted");
        // Recording it again changes nothing either, not even the state.
        let seq_after_sync = mirror.seq;
        assert_eq!(seq_after_sync.0, seq.0 + 1, "only the sync's own commit");
        assert_eq!(
            mirror.record_deleted(
                &[(a, DeletionEvidence::History { at: HistoryId(12) })],
                Timestamp(0)
            ),
            0
        );
        assert_eq!(mirror.seq, seq_after_sync);
        assert_eq!(mirror.log.len(), entries);
    }

    #[test]
    fn content_written_for_a_deleted_message_is_cleaned_up() {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![MessageOp::Tombstone {
            id: MessageId("a".into()),
            evidence: DeletionEvidence::History { at: HistoryId(3) },
        }];
        let _ = mirror.apply(&c);
        let mut imported = commit(&mirror);
        imported.messages = vec![MessageOp::MarkImported {
            observation: observation("a", &["INBOX"], 2),
            blob: BlobRef("k".into()),
            size: 1,
            internal_date: Timestamp(0),
            headers: Box::default(),
            facts: None,
        }];
        let _ = mirror.apply(&imported);
        assert!(
            mirror
                .jobs
                .contains_key(&JobKind::BlobCleanup(MessageId("a".into())))
        );
    }

    #[test]
    fn changes_page_through_the_log() {
        let mut mirror = claimed();
        for id in ["a", "b", "c"] {
            let mut c = commit(&mirror);
            c.messages = vec![MessageOp::Observe(observation(id, &["INBOX"], 1))];
            let _ = mirror.apply(&c);
        }
        let first = mirror.changes(StateSeq(0).into(), 2);
        assert_eq!(first.changes.len(), 2);
        assert!(first.has_more);
        let rest = mirror.changes(first.next, 10);
        let ids: Vec<_> = rest
            .changes
            .iter()
            .filter_map(|e| e.change.message())
            .collect();
        assert_eq!(ids, vec![&MessageId("c".into())]);
        assert!(!rest.has_more);
    }

    /// A mirror with `ids` discovered and their imports queued at `priority`.
    fn pending(ids: &[&str], priority: Priority) -> AccountMirror {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.job_plan.priority = priority;
        c.messages = ids
            .iter()
            .map(|id| MessageOp::Observe(observation(id, &["INBOX"], 1)))
            .collect();
        let _ = mirror.apply(&c);
        mirror
    }

    fn ids(ids: &[&str]) -> Vec<MessageId> {
        ids.iter().map(|id| MessageId((*id).into())).collect()
    }

    fn import_of<'a>(mirror: &'a mut AccountMirror, id: &str) -> &'a mut Job {
        mirror
            .jobs
            .get_mut(&JobKind::Import(MessageId(id.into())))
            .expect("an import job")
    }

    #[test]
    fn each_id_is_reported_in_exactly_one_group() {
        let mut mirror = pending(
            &["raise", "foreground", "imported", "gone", "blocked", "over"],
            Priority::Backfill,
        );
        import_of(&mut mirror, "foreground").priority = Priority::Foreground;
        import_of(&mut mirror, "blocked").state =
            JobState::Blocked(crate::retry::BlockingFailure {
                class: crate::retry::Blocking::Permanent,
                detail: String::new(),
            });
        mirror
            .jobs
            .remove(&JobKind::Import(MessageId("imported".into())));
        mirror
            .messages
            .get_mut(&MessageId("imported".into()))
            .expect("row")
            .content = Content::Imported(Box::new(StoredContent {
            blob: BlobRef("k".into()),
            size: 1,
            internal_date: Timestamp(0),
            headers: HeaderFacts::default(),
            facts: None,
        }));
        let mut c = commit(&mirror);
        c.messages = vec![MessageOp::Tombstone {
            id: MessageId("gone".into()),
            evidence: DeletionEvidence::History { at: HistoryId(2) },
        }];
        let _ = mirror.apply(&c);
        let jobs = mirror.jobs.len();

        // One Foreground slot is left: the first raise takes it.
        let report = mirror.prioritise_counted(
            &ids(&[
                "raise",
                "foreground",
                "imported",
                "gone",
                "never",
                "blocked",
                "over",
                "raise",
            ]),
            Timestamp(50),
            FOREGROUND_IMPORT_CAP - 1,
        );
        assert_eq!(
            report,
            Prioritised {
                queued: ids(&["raise", "foreground"]),
                imported: ids(&["imported"]),
                unknown: ids(&["gone", "never"]),
                blocked: ids(&["blocked"]),
                over_limit: ids(&["over"]),
            }
        );
        assert_eq!(mirror.jobs.len(), jobs, "no job created or removed");
        assert_eq!(
            import_of(&mut mirror, "raise").priority,
            Priority::Foreground
        );
        assert_eq!(import_of(&mut mirror, "over").priority, Priority::Backfill);
        assert!(matches!(
            import_of(&mut mirror, "blocked").state,
            JobState::Blocked(_)
        ));
    }

    #[test]
    fn the_foreground_import_cap_counts_jobs_already_raised() {
        let earlier: Vec<String> = (1..FOREGROUND_IMPORT_CAP)
            .map(|n| format!("earlier-{n:02}"))
            .collect();
        let mut all: Vec<&str> = earlier.iter().map(String::as_str).collect();
        all.extend(["a", "b"]);
        let mut mirror = pending(&all, Priority::Backfill);
        let raised = mirror.prioritise(&ids(&all[..FOREGROUND_IMPORT_CAP - 1]), Timestamp(0));
        assert_eq!(raised.queued.len(), FOREGROUND_IMPORT_CAP - 1);
        assert_eq!(mirror.foreground_imports(), FOREGROUND_IMPORT_CAP - 1);

        let report = mirror.prioritise(&ids(&["a", "b"]), Timestamp(0));
        assert_eq!(report.queued, ids(&["a"]));
        assert_eq!(report.over_limit, ids(&["b"]));
        // Asking again changes nothing.
        assert_eq!(mirror.prioritise(&ids(&["a", "b"]), Timestamp(0)), report);

        // One Foreground import finishes: its slot is free again.
        let epoch = mirror.record.epoch;
        let [job] = mirror
            .claim_jobs(epoch, 1, Timestamp(0), AttemptId(1))
            .expect("current epoch")
            .try_into()
            .expect("one job");
        assert_eq!(job.priority, Priority::Foreground);
        let mut done = commit(&mirror);
        done.claim = Some(job.claim());
        done.jobs = vec![JobOp::Finish {
            claim: job.claim(),
            outcome: JobOutcome::Done,
        }];
        let _ = mirror.apply(&done);
        let report = mirror.prioritise(&ids(&["b"]), Timestamp(0));
        assert_eq!(report.queued, ids(&["b"]));
        assert_eq!(mirror.foreground_imports(), FOREGROUND_IMPORT_CAP);
    }

    #[test]
    fn prioritising_writes_no_feed_entry_and_keeps_the_state() {
        let mut mirror = pending(&["a", "b"], Priority::Backfill);
        let before = mirror.clone();
        let report = mirror.prioritise(&ids(&["a"]), Timestamp(0));
        assert_eq!(report.queued, ids(&["a"]));
        assert_eq!(mirror.log, before.log);
        assert_eq!(mirror.seq, before.seq);
        assert_eq!(mirror.record, before.record);
        assert_eq!(mirror.messages, before.messages);
        assert_eq!(mirror.intents, before.intents);
        assert_eq!(
            mirror.changes(StateSeq(0).into(), 100),
            before.changes(StateSeq(0).into(), 100)
        );
        assert_ne!(mirror.jobs, before.jobs, "only the job changed");
    }

    #[test]
    fn a_prioritised_import_is_claimed_before_live_and_backfill_work() {
        let mut mirror = pending(&["backfill", "opened"], Priority::Backfill);
        let mut live = commit(&mirror);
        live.messages = vec![MessageOp::Observe(observation("live", &["INBOX"], 2))];
        let _ = mirror.apply(&live);
        let _ = mirror.prioritise(&ids(&["opened"]), Timestamp(0));
        let epoch = mirror.record.epoch;

        let mut any = mirror.clone();
        let claimed = any
            .claim_jobs(epoch, 1, Timestamp(0), AttemptId(1))
            .expect("current epoch");
        assert_eq!(
            claimed
                .iter()
                .map(|j| (j.kind(), j.priority))
                .collect::<Vec<_>>(),
            vec![(
                JobKind::Import(MessageId("opened".into())),
                Priority::Foreground
            )]
        );
        let claimed = mirror
            .claim_jobs_from(epoch, 4, Timestamp(0), AttemptId(1), Priority::Foreground)
            .expect("current epoch");
        assert_eq!(
            claimed.iter().map(ClaimedJob::kind).collect::<Vec<_>>(),
            vec![JobKind::Import(MessageId("opened".into()))]
        );
    }

    /// A mirror whose log holds one commit per size, each discovering that
    /// many new messages (one entry each), with the commits' states.
    fn logged(sizes: &[usize]) -> (AccountMirror, Vec<StateSeq>) {
        let mut mirror = claimed();
        let states = sizes
            .iter()
            .map(|&size| append(&mut mirror, size))
            .collect();
        (mirror, states)
    }

    /// Apply a commit discovering `size` new messages; its state.
    fn append(mirror: &mut AccountMirror, size: usize) -> StateSeq {
        let mut c = commit(mirror);
        let base = mirror.messages.len();
        c.messages = (base..base + size)
            .map(|n| MessageOp::Observe(observation(&format!("m{n:05}"), &["INBOX"], 1)))
            .collect();
        let _ = mirror.apply(&c);
        mirror.seq
    }

    /// Read from `since` to the end with `limit`: the entries, and each
    /// page's size.
    fn read_all(
        mirror: &AccountMirror,
        since: FeedCursor,
        limit: u32,
    ) -> (Vec<crate::revision::ChangeEntry>, Vec<ChangePage>) {
        let (mut entries, mut pages, mut since) = (Vec::new(), Vec::new(), since);
        loop {
            let page = mirror.changes(since, limit);
            pages.push(page.clone());
            entries.extend(page.changes);
            since = page.next;
            if !page.has_more {
                return (entries, pages);
            }
        }
    }

    fn identities(entries: &[crate::revision::ChangeEntry]) -> Vec<(StateSeq, u32)> {
        entries.iter().map(|e| (e.state, e.index)).collect()
    }

    fn start() -> FeedCursor {
        StateSeq(0).into()
    }

    #[test]
    fn a_page_never_exceeds_its_limit() {
        let (mirror, _) = logged(&[7]);
        let mut since = start();
        let mut pages = Vec::new();
        loop {
            let page = mirror.changes(since, 3);
            pages.push((page.changes.len(), page.has_more));
            since = page.next;
            if !page.has_more {
                break;
            }
        }
        assert_eq!(pages, vec![(3, true), (3, true), (1, false)]);
    }

    #[test]
    fn pages_of_every_limit_concatenate_to_the_log() {
        // Every log of up to 4 commits of 0 to 4 entries.
        let mut logs: Vec<Vec<usize>> = vec![Vec::new()];
        let mut shorter = logs.clone();
        for _ in 0..4 {
            shorter = shorter
                .iter()
                .flat_map(|log| {
                    (0..=4).map(move |size| {
                        let mut log = log.clone();
                        log.push(size);
                        log
                    })
                })
                .collect();
            logs.extend(shorter.iter().cloned());
        }
        for sizes in logs {
            let (mirror, _) = logged(&sizes);
            let whole = mirror.changes(start(), MAX_FEED_PAGE);
            assert!(!whole.has_more);
            let logged: Vec<_> = mirror.log.iter().map(|(seq, c)| (*seq, c)).collect();
            let read: Vec<_> = whole.changes.iter().map(|e| (e.state, &e.change)).collect();
            assert_eq!(read, logged, "{sizes:?}: one read is the log");
            for limit in 1..=6 {
                let (entries, pages) = read_all(&mirror, start(), limit);
                assert_eq!(entries, whole.changes, "{sizes:?} with limit {limit}");
                for page in &pages {
                    let n = page.changes.len();
                    assert!(n <= limit as usize, "{sizes:?} with limit {limit}");
                    // Inside a commit only as a full page of one commit
                    // larger than the limit.
                    if page.next.entry.is_some() {
                        let size = sizes[usize::try_from(page.next.seq.0).unwrap_or(0) - 1];
                        assert!(
                            n == limit as usize
                                && size > n
                                && page.changes.iter().all(|e| e.state == page.next.seq),
                            "{sizes:?} with limit {limit}: {page:?}"
                        );
                    }
                }
                // The promise: no commit above the limit, no position
                // inside one.
                if sizes.iter().all(|&size| size <= limit as usize) {
                    assert!(
                        pages.iter().all(|page| page.next.entry.is_none()),
                        "{sizes:?} with limit {limit}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_commit_of_exactly_the_limit_is_one_page() {
        let (mirror, states) = logged(&[3, 3]);
        let first = mirror.changes(start(), 3);
        assert_eq!(first.changes.len(), 3);
        assert_eq!(first.next, FeedCursor::from(states[0]));
        assert!(first.has_more);
        let second = mirror.changes(first.next, 3);
        assert_eq!(second.next, FeedCursor::from(states[1]));
        assert!(!second.has_more);
    }

    #[test]
    fn a_split_commit_resumes_then_pages_return_to_boundaries() {
        // Commits of 1, 7, 2, 2 with limit 4: the first page ends before
        // the large commit, which is split 4 + 3; its rest ends a page that
        // stops at the boundary within the limit, then whole commits.
        let (mirror, states) = logged(&[1, 7, 2, 2]);
        let (_, pages) = read_all(&mirror, start(), 4);
        let shape: Vec<_> = pages.iter().map(|p| (p.changes.len(), p.next)).collect();
        assert_eq!(
            shape,
            vec![
                (1, FeedCursor::from(states[0])),
                (
                    4,
                    FeedCursor {
                        seq: states[1],
                        entry: Some(3),
                    }
                ),
                (3, FeedCursor::from(states[1])),
                (4, FeedCursor::from(states[3])),
            ]
        );
        assert_eq!(
            pages.iter().map(|p| p.has_more).collect::<Vec<_>>(),
            vec![true, true, true, false]
        );
    }

    #[test]
    fn a_page_that_ends_a_commit_returns_the_commit_position() {
        let (mirror, states) = logged(&[2, 3]);
        let first = mirror.changes(start(), 2);
        assert_eq!(first.next, FeedCursor::from(states[0]), "the whole commit");
        let at = |cursor| crate::revision::FeedPosition {
            generation: 1,
            at: cursor,
        };
        assert_eq!(
            at(first.next).encode().0,
            format!("mt2-1.{:x}", states[0].0),
            "two fields: an older build reads it, compaction can go through it"
        );
        let second = mirror.changes(first.next, 2);
        assert_eq!(
            second.next,
            FeedCursor {
                seq: states[1],
                entry: Some(1),
            },
            "inside the second commit"
        );
        assert_eq!(
            at(second.next).encode().0,
            format!("mt2-1.{:x}.1", states[1].0)
        );
        let last = mirror.changes(second.next, 2);
        assert_eq!(last.changes.len(), 1);
        assert_eq!(last.next, FeedCursor::from(states[1]));
        assert!(!last.has_more);
    }

    #[test]
    fn entry_identities_do_not_depend_on_where_pages_split() {
        let (mirror, _) = logged(&[3, 5, 1]);
        let whole = identities(&mirror.changes(start(), MAX_FEED_PAGE).changes);
        assert_eq!(whole.len(), 9);
        for limit in [1, 3] {
            assert_eq!(
                identities(&read_all(&mirror, start(), limit).0),
                whole,
                "limit {limit}"
            );
        }
    }

    #[test]
    fn commits_appended_between_pages_are_not_skipped() {
        let (mut mirror, states) = logged(&[4]);
        let first = mirror.changes(start(), 2);
        assert!(first.has_more);
        let second_state = append(&mut mirror, 2);
        let third_state = append(&mut mirror, 1);
        let (rest, _) = read_all(&mirror, first.next, 2);
        assert_eq!(
            identities(&rest),
            vec![
                (states[0], 2),
                (states[0], 3),
                (second_state, 0),
                (second_state, 1),
                (third_state, 0),
            ]
        );
    }

    #[test]
    fn a_position_inside_a_compacted_commit_is_too_old() {
        let (mut mirror, states) = logged(&[4, 2]);
        let inside = FeedCursor {
            seq: states[0],
            entry: Some(2),
        };
        assert!(matches!(
            mirror.feed(inside, 10),
            crate::revision::ChangeFeed::Page(_)
        ));
        mirror.compact(&Retention::new(Timestamp(0)).through(states[0]));
        assert_eq!(
            mirror.feed(inside, 10),
            crate::revision::ChangeFeed::TooOld {
                generation: 1,
                oldest: states[0],
            },
            "the rest of the commit is gone"
        );
        let crate::revision::ChangeFeed::Page(page) = mirror.feed(states[0].into(), 10) else {
            panic!("the whole compacted commit is a place to continue from");
        };
        assert_eq!(
            identities(&page.changes),
            vec![(states[1], 0), (states[1], 1)]
        );
    }

    #[test]
    fn a_limit_of_zero_or_above_the_maximum_is_clamped() {
        let (mirror, _) = logged(&[MAX_FEED_PAGE as usize + 1]);
        let one = mirror.changes(start(), 0);
        assert_eq!(one.changes.len(), 1, "a limit of zero reads one entry");
        assert!(one.has_more);
        let most = mirror.changes(start(), u32::MAX);
        assert_eq!(most.changes.len(), MAX_FEED_PAGE as usize);
        assert!(most.has_more, "the caller continues from `next`");
        let rest = mirror.changes(most.next, u32::MAX);
        assert_eq!(rest.changes.len(), 1);
        assert!(!rest.has_more);
    }

    fn in_thread(thread: &str, id: &str, labels: &[&str], checkpoint: u64) -> MessageObservation {
        MessageObservation {
            thread_id: ThreadId(thread.into()),
            ..observation(id, labels, checkpoint)
        }
    }

    fn import_in(thread: &str, id: &str, labels: &[&str], checkpoint: u64, date: i64) -> MessageOp {
        MessageOp::MarkImported {
            observation: in_thread(thread, id, labels, checkpoint),
            blob: BlobRef(format!("{id}.eml")),
            size: 10,
            internal_date: Timestamp(date),
            headers: Box::new(HeaderFacts {
                from: Some(format!("{id}@example.com")),
                ..HeaderFacts::default()
            }),
            facts: None,
        }
    }

    fn applied(mirror: &mut AccountMirror, ops: Vec<MessageOp>) {
        let mut c = commit(mirror);
        c.messages = ops;
        assert!(matches!(mirror.apply(&c), CommitOutcome::Applied { .. }));
    }

    fn members(view: &crate::thread::ThreadView) -> Vec<&str> {
        view.messages.iter().map(|m| m.id.0.as_str()).collect()
    }

    #[test]
    fn a_thread_action_reaches_exactly_the_views_non_draft_messages() {
        let mut mirror = claimed();
        applied(
            &mut mirror,
            vec![
                import_in("t1", "a", &["INBOX", "UNREAD"], 10, 1),
                MessageOp::Observe(in_thread("t1", "b", &["INBOX", "UNREAD"], 11)),
                import_in("t1", "d", &["DRAFT", "UNREAD"], 12, 3),
                import_in("t2", "c", &["INBOX", "UNREAD"], 13, 4),
            ],
        );
        let t1 = ThreadId("t1".into());
        let before = mirror.thread(&t1).expect("t1");
        let admitted = mirror.admit_action(
            &ActionRequest::Thread {
                id: t1.clone(),
                action: outbox::Action::SetRead(outbox::ReadState::Read),
            },
            Timestamp(0),
        );
        assert_eq!(admitted, before.message_count);
        let targets: BTreeSet<&str> = mirror
            .intents
            .keys()
            .map(|(message, _)| message.0.as_str())
            .collect();
        let non_draft: BTreeSet<&str> = before
            .messages
            .iter()
            .filter(|m| !labels::is_draft(&m.labels))
            .map(|m| m.id.0.as_str())
            .collect();
        assert_eq!(targets, non_draft);
        assert_eq!(targets, BTreeSet::from(["a", "b"]));
        let after = mirror.thread(&t1).expect("t1");
        assert!(!after.unread);
        assert!(mirror.thread(&ThreadId("t2".into())).expect("t2").unread);
    }

    #[test]
    fn a_reply_found_by_history_joins_its_thread_before_it_is_imported() {
        let mut mirror = claimed();
        applied(&mut mirror, vec![import_in("t", "a", &["INBOX"], 10, 100)]);
        let t = ThreadId("t".into());
        let first = mirror.thread(&t).expect("t");
        assert!(!first.unread);
        assert_eq!(first.latest, Some(MessageId("a".into())));
        // History discovers the reply: its labels count at once.
        applied(
            &mut mirror,
            vec![MessageOp::Observe(in_thread(
                "t",
                "b",
                &["INBOX", "UNREAD"],
                20,
            ))],
        );
        let discovered = mirror.thread(&t).expect("t");
        assert_eq!(members(&discovered), ["a", "b"]);
        assert!(discovered.unread);
        assert_eq!(discovered.latest, Some(MessageId("a".into())));
        assert_eq!(discovered.awaiting_import, 1);
        // Its import makes it latest.
        applied(
            &mut mirror,
            vec![import_in("t", "b", &["INBOX", "UNREAD"], 21, 200)],
        );
        let imported = mirror.thread(&t).expect("t");
        assert_eq!(imported.latest, Some(MessageId("b".into())));
        assert_eq!(imported.awaiting_import, 0);
        assert_eq!(imported.participants, ["a@example.com", "b@example.com"]);
    }

    #[test]
    fn a_full_observation_in_another_thread_moves_the_message() {
        let mut mirror = claimed();
        applied(
            &mut mirror,
            vec![
                import_in("t1", "a", &["INBOX"], 10, 1),
                import_in("t1", "b", &["INBOX", "UNREAD"], 11, 2),
            ],
        );
        // A verification fetch sees b in another thread.
        applied(
            &mut mirror,
            vec![MessageOp::Observe(in_thread(
                "t2",
                "b",
                &["INBOX", "UNREAD"],
                30,
            ))],
        );
        let t1 = mirror.thread(&ThreadId("t1".into())).expect("t1");
        assert_eq!(members(&t1), ["a"]);
        assert!(!t1.unread);
        let t2 = mirror.thread(&ThreadId("t2".into())).expect("t2");
        assert_eq!(members(&t2), ["b"]);
        assert!(t2.unread);
    }

    #[test]
    fn an_empty_thread_id_is_never_a_thread() {
        let mut mirror = claimed();
        applied(
            &mut mirror,
            vec![
                MessageOp::Observe(in_thread("", "a", &["INBOX"], 10)),
                MessageOp::Observe(in_thread("", "b", &["INBOX"], 11)),
            ],
        );
        let empty = ThreadId(String::new());
        assert_eq!(mirror.thread_members(&empty).count(), 0);
        assert_eq!(mirror.thread(&empty), None);
        let admitted = mirror.admit_action(
            &ActionRequest::Thread {
                id: empty.clone(),
                action: outbox::Action::Archive,
            },
            Timestamp(0),
        );
        assert_eq!(admitted, 0);
        assert!(mirror.intents.is_empty());
        // The import carries the real thread id and the message joins it.
        applied(&mut mirror, vec![import_in("t", "a", &["INBOX"], 12, 1)]);
        let t = mirror.thread(&ThreadId("t".into())).expect("t");
        assert_eq!(members(&t), ["a"]);
        assert_eq!(mirror.thread(&empty), None);
    }

    fn consumer(name: &str) -> ConsumerName {
        ConsumerName::parse(name).expect("a valid name")
    }

    /// A position in `mirror`'s generation.
    fn position(mirror: &AccountMirror, at: impl Into<FeedCursor>) -> FeedPosition {
        FeedPosition {
            generation: mirror.record.generation,
            at: at.into(),
        }
    }

    #[test]
    fn ack_sets_the_position_and_the_time() {
        let (mut mirror, states) = logged(&[1, 1, 1]);
        let reader = consumer("reader");
        let first = mirror.ack(&reader, position(&mirror, states[1]), Timestamp(10));
        let expected = |at: StateSeq, seen_at| ConsumerCursor {
            name: reader.clone(),
            at: at.into(),
            seen_at: Timestamp(seen_at),
        };
        assert_eq!(first, AckOutcome::Acked(expected(states[1], 10)));
        assert_eq!(
            mirror.consumer_list().cursors,
            vec![expected(states[1], 10)]
        );
        // Moving on, then back to an older checkpoint the log still holds.
        mirror.ack(&reader, position(&mirror, states[2]), Timestamp(20));
        let back = mirror.ack(&reader, position(&mirror, states[0]), Timestamp(30));
        assert_eq!(back, AckOutcome::Acked(expected(states[0], 30)));
        assert_eq!(
            mirror.consumer_list().cursors,
            vec![expected(states[0], 30)]
        );
    }

    #[test]
    fn an_ack_behind_the_floor_is_too_old_and_stores_nothing() {
        let (mut mirror, states) = logged(&[3, 1, 1]);
        mirror.compact(&Retention::new(Timestamp(0)).through(states[1]));
        let too_old = AckOutcome::TooOld {
            generation: 1,
            oldest: states[1],
        };
        let reader = consumer("reader");
        let inside_floor = FeedCursor {
            seq: states[1],
            entry: Some(0),
        };
        for at in [states[0].into(), inside_floor] {
            assert_eq!(
                mirror.ack(&reader, position(&mirror, at), Timestamp(1)),
                too_old
            );
            assert!(mirror.consumers.is_empty(), "nothing registered");
        }
        mirror.ack(&reader, position(&mirror, states[2]), Timestamp(1));
        let registered = mirror.consumers.clone();
        assert_eq!(
            mirror.ack(&reader, position(&mirror, states[0]), Timestamp(2)),
            too_old
        );
        let beyond = position(&mirror, StateSeq(states[2].0 + 1));
        assert_eq!(mirror.ack(&reader, beyond, Timestamp(2)), too_old);
        let other_generation = FeedPosition {
            generation: 2,
            at: states[2].into(),
        };
        assert_eq!(mirror.ack(&reader, other_generation, Timestamp(2)), too_old);
        assert_eq!(mirror.consumers, registered, "the cursor is unchanged");
    }

    #[test]
    fn a_consumer_at_the_head_is_seen_by_compaction() {
        let day = std::time::Duration::from_hours(24);
        let (mut mirror, states) = logged(&[1, 1]);
        let reader = consumer("reader");
        mirror.ack(&reader, position(&mirror, states[1]), Timestamp(0));
        // A quiet mailbox: daily compactions for a month, no acks.
        let mut now = Timestamp(0);
        for _ in 0..30 {
            now = now.plus(day);
            mirror.compact(&Retention::new(now));
            assert_eq!(mirror.consumers[&reader].seen_at, now, "seen at the head");
        }
        // Mail arrives; the reader falls behind and its idle clock starts.
        let head = append(&mut mirror, 1);
        let fell_behind = now;
        now = now.plus(day);
        mirror.compact(&Retention::new(now));
        assert_eq!(mirror.consumers[&reader].seen_at, fell_behind);
        assert_eq!(
            mirror.record.log_floor, states[1],
            "it still holds its entries"
        );
        assert_eq!(mirror.log.len(), 1);
        // Silent for longer than the idle limit: it stops holding the log.
        now = fell_behind
            .plus(crate::revision::DEFAULT_MAX_IDLE)
            .plus(day);
        assert_eq!(mirror.compact(&Retention::new(now)), 1);
        assert_eq!(mirror.record.log_floor, head);
        assert!(mirror.consumers[&reader].is_lost(mirror.record.log_floor));
    }

    #[test]
    fn the_number_of_consumers_per_account_is_bounded() {
        let (mut mirror, states) = logged(&[1]);
        let at = position(&mirror, states[0]);
        for n in 0..MAX_CONSUMERS {
            let name = consumer(&format!("c{n}"));
            assert!(matches!(
                mirror.ack(&name, at, Timestamp(0)),
                AckOutcome::Acked(_)
            ));
        }
        let extra = consumer("extra");
        assert_eq!(
            mirror.ack(&extra, at, Timestamp(0)),
            AckOutcome::TooManyConsumers
        );
        assert!(!mirror.consumers.contains_key(&extra));
        assert!(matches!(
            mirror.ack(&consumer("c3"), at, Timestamp(1)),
            AckOutcome::Acked(_)
        ));
        assert!(mirror.drop_consumer(&consumer("c3")));
        assert!(matches!(
            mirror.ack(&extra, at, Timestamp(1)),
            AckOutcome::Acked(_)
        ));
    }

    fn import_op(id: &str, checkpoint: u64, facts: Option<MessageFacts>) -> MessageOp {
        MessageOp::MarkImported {
            observation: observation(id, &["INBOX"], checkpoint),
            blob: BlobRef(format!("{id}.eml")),
            size: 120,
            internal_date: Timestamp(0),
            headers: Box::default(),
            facts: facts.map(Box::new),
        }
    }

    fn stored(mirror: &AccountMirror, id: &str) -> Option<StoredContent> {
        mirror.view(&MessageId(id.into()))?.content
    }

    #[test]
    fn content_stored_before_facts_existed_still_reads() {
        let before = r#"{"blob":"a.eml","size":120,"internal_date":0,"headers":{"rfc822_message_id":null,"in_reply_to":null,"references":[],"from":"a@x.org","to":[],"cc":[],"subject":"hi","snippet":null}}"#;
        let content: StoredContent = serde_json::from_str(before).expect("old content reads");
        assert_eq!(content.facts, None);
        assert_eq!(content.headers.subject.as_deref(), Some("hi"));
        assert_eq!(
            serde_json::to_string(&content).expect("serialises"),
            before,
            "written back as the previous build wrote it"
        );
        let newer = r#"{"blob":"a.eml","size":120,"internal_date":0,"headers":{"rfc822_message_id":null,"in_reply_to":null,"references":[],"from":null,"to":[],"cc":[],"subject":null,"snippet":null},"facts":{"version":9,"sender_email":"a@x.org","unknown":true},"later":1}"#;
        let content: StoredContent = serde_json::from_str(newer).expect("unknown keys are skipped");
        let facts = content.facts.expect("facts");
        assert_eq!(facts.version, 9);
        assert_eq!(facts.sender_email.as_deref(), Some("a@x.org"));
        assert!(facts.parts.is_empty());
    }

    #[test]
    fn an_import_stores_the_facts_it_was_given() {
        let mut mirror = claimed();
        let facts = MessageFacts {
            sender_email: Some("a@x.org".into()),
            attached: 1,
            ..MessageFacts::empty()
        };
        let mut c = commit(&mirror);
        c.messages = vec![import_op("a", 3, Some(facts.clone()))];
        let _ = mirror.apply(&c);
        assert_eq!(
            stored(&mirror, "a").and_then(|content| content.facts),
            Some(facts.clone())
        );
        let page = mirror.changes(StateSeq(0).into(), 10);
        let imported = page
            .changes
            .iter()
            .find(|entry| matches!(entry.change, Change::MessageImported { .. }))
            .expect("import entry");
        assert_eq!(
            imported
                .content
                .as_ref()
                .and_then(|content| content.facts.clone()),
            Some(facts)
        );
        let mut c = commit(&mirror);
        c.messages = vec![import_op("b", 3, None)];
        let _ = mirror.apply(&c);
        assert_eq!(
            stored(&mirror, "b").map(|content| content.facts),
            Some(None)
        );
    }

    fn refreshed(id: &str, read_size: u64, size: u64, version: u32) -> FactsUpdate {
        FactsUpdate {
            id: MessageId(id.into()),
            blob: BlobRef(format!("{id}.eml")),
            read_size,
            size,
            facts: MessageFacts {
                version,
                sender_email: Some(format!("{id}@x.org")),
                ..MessageFacts::default()
            },
        }
    }

    fn imported_with(id: &str, version: Option<u32>) -> AccountMirror {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![import_op(
            id,
            3,
            version.map(|version| MessageFacts {
                version,
                ..MessageFacts::default()
            }),
        )];
        let _ = mirror.apply(&c);
        mirror
    }

    #[test]
    fn a_refresh_applies_only_to_the_same_blob_at_an_older_version() {
        for (version, update, applies) in [
            (None, refreshed("a", 120, 120, 1), true),
            (Some(0), refreshed("a", 120, 120, 1), true),
            (Some(1), refreshed("a", 120, 120, 1), false),
            (Some(2), refreshed("a", 120, 120, 1), false),
            (Some(1), refreshed("a", 120, 130, 1), true),
            (None, refreshed("a", 110, 120, 1), false),
            (
                None,
                FactsUpdate {
                    blob: BlobRef("other.eml".into()),
                    ..refreshed("a", 120, 120, 1)
                },
                false,
            ),
        ] {
            let mut mirror = imported_with("a", version);
            let record = mirror.messages[&MessageId("a".into())].clone();
            assert_eq!(
                refresh_applies(&record, &update),
                applies,
                "{version:?} {update:?}"
            );
            assert_eq!(
                mirror.refresh_facts(std::slice::from_ref(&update)),
                u32::from(applies)
            );
            let content = stored(&mirror, "a").expect("imported");
            if applies {
                assert_eq!(content.facts, Some(update.facts));
                assert_eq!(content.size, update.size, "the size is the blob's length");
            } else {
                assert_eq!(content.facts.map(|facts| facts.version), version);
                assert_eq!(content.size, 120);
            }
        }
    }

    #[test]
    fn a_refresh_of_a_deleted_or_pending_message_applies_nothing() {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![
            MessageOp::Observe(observation("pending", &["INBOX"], 2)),
            import_op("gone", 2, None),
        ];
        let _ = mirror.apply(&c);
        let mut c = commit(&mirror);
        c.messages = vec![MessageOp::Tombstone {
            id: MessageId("gone".into()),
            evidence: DeletionEvidence::History { at: HistoryId(4) },
        }];
        let _ = mirror.apply(&c);
        let before = mirror.clone();
        let updates = [
            refreshed("pending", 120, 120, 1),
            refreshed("gone", 120, 120, 1),
            refreshed("never", 120, 120, 1),
        ];
        assert_eq!(mirror.refresh_facts(&updates), 0);
        assert_eq!(mirror, before, "no row, no entry, no new state");
    }

    #[test]
    fn a_refresh_touches_only_the_facts() {
        let mut mirror = imported_with("a", None);
        let _ = mirror.admit_action(
            &ActionRequest::Messages {
                ids: vec![MessageId("a".into())],
                action: outbox::Action::Archive,
            },
            Timestamp(5),
        );
        let before = mirror.clone();
        assert_eq!(mirror.refresh_facts(&[refreshed("a", 120, 120, 1)]), 1);
        let (old, new) = (
            &before.messages[&MessageId("a".into())],
            &mirror.messages[&MessageId("a".into())],
        );
        assert_eq!(
            (&new.labels, new.checkpoint, new.unverified, &new.thread_id),
            (&old.labels, old.checkpoint, old.unverified, &old.thread_id)
        );
        let (Content::Imported(old), Content::Imported(new)) = (&old.content, &new.content) else {
            panic!("imported");
        };
        assert_eq!(
            (&new.blob, new.size, new.internal_date, &new.headers),
            (&old.blob, old.size, old.internal_date, &old.headers)
        );
        assert_eq!(mirror.intents, before.intents);
        assert_eq!(mirror.jobs, before.jobs);
        assert_eq!(mirror.record, before.record);
    }

    #[test]
    fn a_refresh_records_one_entry_per_message_changed_in_one_state() {
        let mut mirror = claimed();
        let mut c = commit(&mirror);
        c.messages = vec![
            import_op("a", 2, None),
            import_op("b", 2, Some(MessageFacts::empty())),
            import_op("c", 2, None),
        ];
        let _ = mirror.apply(&c);
        let head = mirror.seq;
        let applied = mirror.refresh_facts(&[
            refreshed("a", 120, 120, 1),
            refreshed("b", 120, 120, 1),
            refreshed("c", 120, 120, 1),
        ]);
        assert_eq!(applied, 2, "b is current");
        assert_eq!(mirror.seq, StateSeq(head.0 + 1));
        let page = mirror.changes(head.into(), 100);
        let entries: Vec<(&Change, StateSeq)> = page
            .changes
            .iter()
            .map(|entry| (&entry.change, entry.state))
            .collect();
        let thread = ThreadId("t".into());
        assert_eq!(
            entries,
            [
                (
                    &Change::FactsRefreshed {
                        id: MessageId("a".into()),
                        thread_id: thread.clone()
                    },
                    mirror.seq
                ),
                (
                    &Change::FactsRefreshed {
                        id: MessageId("c".into()),
                        thread_id: thread
                    },
                    mirror.seq
                ),
            ]
        );
        assert!(page.changes.iter().all(|entry| {
            entry
                .content
                .as_ref()
                .and_then(|content| content.facts.as_ref())
                .is_some_and(|facts| facts.version == 1)
        }));
    }

    // ------------------------------------------------------------ label definitions

    fn defined(id: &str, name: &str) -> Label {
        let kind = if id.starts_with("Label_") {
            crate::gmail::LabelKind::User
        } else {
            crate::gmail::LabelKind::System
        };
        Label::plain(LabelId(id.into()), name, kind)
    }

    fn listed(mirror: &mut AccountMirror, labels: &[&str], at: i64) {
        let mut c = commit(mirror);
        c.labels = vec![LabelOp::ReplaceAll {
            labels: labels.iter().map(|id| defined(id, id)).collect(),
            generation: mirror.record.label_generation,
            at: Timestamp(at),
        }];
        assert!(matches!(mirror.apply(&c), CommitOutcome::Applied { .. }));
    }

    fn entries(mirror: &AccountMirror) -> Vec<Change> {
        mirror
            .log
            .iter()
            .filter(|(seq, _)| *seq == mirror.seq)
            .map(|(_, change)| change.clone())
            .collect()
    }

    fn labels_of(mirror: &AccountMirror, id: &str) -> LabelSet {
        mirror.messages[&MessageId(id.into())].labels.clone()
    }

    const DAY: i64 = 24 * 60 * 60 * 1_000;

    #[test]
    fn a_removed_label_leaves_every_message_and_logs_each_change() {
        let mut mirror = claimed();
        listed(&mut mirror, &["INBOX", "Label_1"], 0);
        applied(
            &mut mirror,
            vec![
                MessageOp::Observe(observation("a", &["INBOX", "Label_1"], 10)),
                MessageOp::Observe(observation("b", &["Label_1"], 10)),
                MessageOp::Observe(observation("c", &["INBOX"], 10)),
            ],
        );
        mirror
            .messages
            .get_mut(&MessageId("b".into()))
            .expect("b")
            .unverified = Some(HistoryId(12));
        mirror.record_label(&LabelOp::Remove {
            id: LabelId("Label_1".into()),
            at: Timestamp(100),
        });
        assert_eq!(labels_of(&mirror, "a"), set(["INBOX"]));
        assert_eq!(labels_of(&mirror, "b"), set([]));
        assert_eq!(labels_of(&mirror, "c"), set(["INBOX"]));
        let b = &mirror.messages[&MessageId("b".into())];
        assert_eq!(
            (b.checkpoint, b.unverified),
            (Some(HistoryId(10)), Some(HistoryId(12))),
            "no checkpoint moves: an unverified row stays unverified"
        );
        let removed = |id: &str| Change::LabelsChanged {
            id: MessageId(id.into()),
            thread_id: ThreadId("t".into()),
            added: LabelSet::new(),
            removed: set(["Label_1"]),
        };
        assert_eq!(
            entries(&mirror),
            vec![Change::LabelDefinitionsChanged, removed("a"), removed("b")]
        );
        assert_eq!(mirror.label_definitions(), vec![defined("INBOX", "INBOX")]);
        assert_eq!(
            mirror.record.deleted_labels,
            BTreeMap::from([(LabelId("Label_1".into()), Timestamp(100))])
        );
        // A history record of the same deletion changes nothing more.
        let mut c = commit(&mirror);
        c.messages = vec![MessageOp::PatchLabels {
            id: MessageId("a".into()),
            add: LabelSet::new(),
            remove: set(["Label_1"]),
            checkpoint: HistoryId(11),
        }];
        mirror.apply(&c);
        assert_eq!(labels_of(&mirror, "a"), set(["INBOX"]));
        assert!(entries(&mirror).is_empty());
    }

    #[test]
    fn a_removed_label_drops_its_intents() {
        let mut mirror = claimed();
        listed(&mut mirror, &["INBOX", "STARRED", "Label_1"], 0);
        applied(
            &mut mirror,
            vec![
                MessageOp::Observe(observation("a", &["INBOX"], 1)),
                MessageOp::Observe(observation("b", &["INBOX"], 1)),
                MessageOp::Observe(observation("c", &["INBOX", "Label_1"], 1)),
            ],
        );
        let change = |ids: &[&str], add: &[&str], remove: &[&str]| ActionRequest::Messages {
            ids: ids.iter().map(|id| MessageId((*id).into())).collect(),
            action: outbox::Action::ChangeLabels(
                outbox::LabelDelta::new(
                    add.iter().map(|l| LabelId((*l).into())).collect(),
                    remove.iter().map(|l| LabelId((*l).into())).collect(),
                )
                .expect("a valid change"),
            ),
        };
        mirror.admit_action(&change(&["a"], &["Label_1", "STARRED"], &[]), Timestamp(0));
        mirror.admit_action(&change(&["b"], &["Label_1"], &[]), Timestamp(0));
        mirror.admit_action(&change(&["c"], &[], &["Label_1"]), Timestamp(0));
        // One delivered and one blocked: every state goes with the label.
        let key = |id: &str| (MessageId(id.into()), LabelId("Label_1".into()));
        mirror.intents.get_mut(&key("b")).expect("b").state = IntentState::Delivered {
            at: Timestamp(0),
            attempt: AttemptId(1),
        };
        mirror.intents.get_mut(&key("c")).expect("c").state =
            IntentState::Blocked(crate::retry::BlockingFailure {
                class: crate::retry::Blocking::Permanent,
                detail: String::new(),
            });
        listed(&mut mirror, &["INBOX", "STARRED"], 5);
        assert_eq!(
            mirror
                .intents
                .keys()
                .map(|(m, l)| (m.0.as_str(), l.0.as_str()))
                .collect::<Vec<_>>(),
            [("a", "STARRED")]
        );
        let overlays: Vec<MessageId> = entries(&mirror)
            .into_iter()
            .filter_map(|change| match change {
                Change::OverlayChanged { id, .. } => Some(id),
                _ => None,
            })
            .collect();
        assert_eq!(overlays, ids(&["a", "b", "c"]));
        // b's delivery finds nothing to send; a's sends the star alone.
        let claimed_jobs = mirror
            .claim_jobs(mirror.record.epoch, 10, Timestamp(0), AttemptId(9))
            .expect("claim");
        let work: Vec<&ClaimedWork> = claimed_jobs
            .iter()
            .map(|job| &job.work)
            .filter(|work| matches!(work, ClaimedWork::Deliver(..)))
            .collect();
        assert!(matches!(
            work[0],
            ClaimedWork::Deliver(_, crate::jobs::Delivery::Send(group))
                if group.add == vec![LabelId("STARRED".into())]
        ));
        assert!(matches!(
            work[1],
            ClaimedWork::Deliver(_, crate::jobs::Delivery::Settled)
        ));
    }

    #[test]
    fn an_observation_naming_a_deleted_label_is_stored_without_it() {
        let mut mirror = claimed();
        listed(&mut mirror, &["INBOX", "Label_1"], 0);
        applied(
            &mut mirror,
            vec![MessageOp::Observe(observation("a", &["INBOX"], 10))],
        );
        mirror.record_label(&LabelOp::Remove {
            id: LabelId("Label_1".into()),
            at: Timestamp(100),
        });
        // A fetch from before the deletion, and a replayed labelsAdded.
        applied(
            &mut mirror,
            vec![
                MessageOp::Observe(observation("a", &["INBOX", "Label_1"], 11)),
                MessageOp::PatchLabels {
                    id: MessageId("a".into()),
                    add: set(["Label_1", "STARRED"]),
                    remove: LabelSet::new(),
                    checkpoint: HistoryId(12),
                },
                MessageOp::MarkImported {
                    observation: observation("b", &["Label_1"], 12),
                    blob: BlobRef("b".into()),
                    size: 1,
                    internal_date: Timestamp(0),
                    headers: Box::default(),
                    facts: None,
                },
            ],
        );
        assert_eq!(labels_of(&mirror, "a"), set(["INBOX", "STARRED"]));
        assert_eq!(labels_of(&mirror, "b"), set([]));
        assert!(crate::invariants::check_state(&mirror).is_empty());
    }

    #[test]
    fn a_listing_that_names_a_deleted_label_restores_it() {
        let mut mirror = claimed();
        listed(&mut mirror, &["INBOX", "Label_1"], 0);
        mirror.record_label(&LabelOp::Remove {
            id: LabelId("Label_1".into()),
            at: Timestamp(100),
        });
        listed(&mut mirror, &["INBOX", "Label_1"], 200);
        assert!(mirror.record.deleted_labels.is_empty());
        assert_eq!(
            mirror.label_definitions(),
            vec![defined("INBOX", "INBOX"), defined("Label_1", "Label_1")]
        );
        applied(
            &mut mirror,
            vec![MessageOp::Observe(observation("a", &["Label_1"], 10))],
        );
        assert_eq!(labels_of(&mirror, "a"), set(["Label_1"]));
    }

    #[test]
    fn the_deleted_memory_is_pruned_after_thirty_days() {
        let mut mirror = claimed();
        listed(&mut mirror, &["INBOX", "Label_1"], 0);
        mirror.record_label(&LabelOp::Remove {
            id: LabelId("Label_1".into()),
            at: Timestamp(DAY),
        });
        listed(&mut mirror, &["INBOX"], 30 * DAY);
        assert!(
            mirror
                .record
                .deleted_labels
                .contains_key(&LabelId("Label_1".into())),
            "29 days on"
        );
        listed(&mut mirror, &["INBOX"], 31 * DAY + 1);
        assert!(mirror.record.deleted_labels.is_empty(), "past 30 days");
    }

    #[test]
    fn a_listing_sent_before_a_local_label_write_is_not_applied() {
        let mut mirror = claimed();
        listed(&mut mirror, &["INBOX", "Label_3"], 0);
        let sent_under = mirror.record.label_generation;
        mirror.record_label(&LabelOp::Upsert(defined("Label_2", "New")));
        mirror.record_label(&LabelOp::Remove {
            id: LabelId("Label_3".into()),
            at: Timestamp(10),
        });
        let mut stale = commit(&mirror);
        stale.labels = vec![LabelOp::ReplaceAll {
            labels: vec![defined("INBOX", "INBOX"), defined("Label_3", "Label_3")],
            generation: sent_under,
            at: Timestamp(20),
        }];
        assert!(matches!(
            mirror.apply(&stale),
            CommitOutcome::Applied { .. }
        ));
        assert!(entries(&mirror).is_empty());
        assert_eq!(
            mirror.label_definitions(),
            vec![defined("INBOX", "INBOX"), defined("Label_2", "New")],
            "the label just created stays, the one just deleted does not return"
        );
        assert!(
            mirror
                .record
                .deleted_labels
                .contains_key(&LabelId("Label_3".into()))
        );
        listed(&mut mirror, &["INBOX", "Label_2"], 30);
        assert_eq!(
            mirror.label_definitions(),
            vec![defined("INBOX", "INBOX"), defined("Label_2", "Label_2")],
            "a listing sent after the writes applies"
        );
    }

    #[test]
    fn a_rename_changes_no_message_row() {
        let mut mirror = claimed();
        listed(&mut mirror, &["INBOX", "Label_1"], 0);
        applied(
            &mut mirror,
            vec![MessageOp::Observe(observation("a", &["Label_1"], 10))],
        );
        let before = mirror.messages.clone();
        mirror.record_label(&LabelOp::Upsert(defined("Label_1", "Renamed")));
        assert_eq!(entries(&mirror), vec![Change::LabelDefinitionsChanged]);
        assert_eq!(mirror.messages, before);
        assert_eq!(
            mirror.label_definitions(),
            vec![defined("INBOX", "INBOX"), defined("Label_1", "Renamed")]
        );
    }

    #[test]
    fn an_upsert_equal_to_the_stored_label_logs_nothing() {
        let mut mirror = claimed();
        listed(&mut mirror, &["INBOX", "Label_1"], 0);
        let generation = mirror.record.label_generation;
        mirror.record_label(&LabelOp::Upsert(defined("Label_1", "Label_1")));
        assert!(entries(&mirror).is_empty());
        assert_eq!(mirror.record.label_generation, generation + 1);
    }

    #[test]
    fn a_listing_without_the_system_labels_proves_nothing() {
        let mut mirror = claimed();
        listed(
            &mut mirror,
            &["INBOX", "SPAM", "UNREAD", "Label_1", "Label_2"],
            0,
        );
        applied(
            &mut mirror,
            vec![MessageOp::Observe(observation(
                "a",
                &["INBOX", "UNREAD", "Label_1"],
                10,
            ))],
        );
        let before = (mirror.label_definitions(), mirror.messages.clone());
        // Gmail answered with no labels, or a listing cut short: neither is
        // proof that what it lacks was deleted.
        for listing in [
            &[][..],
            &["INBOX", "Label_2"][..],
            &["SPAM", "UNREAD", "Label_2"][..],
        ] {
            listed(&mut mirror, listing, 5);
            assert!(entries(&mirror).is_empty(), "{listing:?}");
            assert_eq!(
                (mirror.label_definitions(), mirror.messages.clone()),
                before,
                "{listing:?}"
            );
            assert!(mirror.record.deleted_labels.is_empty(), "{listing:?}");
        }
        assert!(complete_listing(&[], &[defined("INBOX", "INBOX")]));
        assert!(!complete_listing(&[], &[defined("Label_1", "Label_1")]));
    }

    #[test]
    fn a_system_label_is_never_forgotten() {
        let mut mirror = claimed();
        // YELLOW_STAR is not a fixed id: its kind says it is Gmail's.
        let mut c = commit(&mirror);
        c.labels = vec![LabelOp::ReplaceAll {
            labels: vec![
                defined("INBOX", "INBOX"),
                defined("YELLOW_STAR", "YELLOW_STAR"),
                defined("Label_1", "Label_1"),
            ],
            generation: 0,
            at: Timestamp(0),
        }];
        mirror.apply(&c);
        applied(
            &mut mirror,
            vec![MessageOp::Observe(observation(
                "a",
                &["INBOX", "IMPORTANT", "YELLOW_STAR"],
                10,
            ))],
        );
        for id in ["IMPORTANT", "YELLOW_STAR"] {
            mirror.record_label(&LabelOp::Remove {
                id: LabelId(id.into()),
                at: Timestamp(1),
            });
        }
        // A complete listing lacking it: still Gmail's own.
        listed(&mut mirror, &["INBOX", "Label_1"], 2);
        assert_eq!(
            labels_of(&mirror, "a"),
            set(["IMPORTANT", "INBOX", "YELLOW_STAR"])
        );
        assert!(mirror.record.deleted_labels.is_empty());
    }

    #[test]
    fn a_late_upsert_does_not_bring_a_deleted_label_back() {
        let mut mirror = claimed();
        listed(&mut mirror, &["INBOX", "Label_1"], 0);
        // A patch answered 200, a concurrent delete recorded first, then
        // the patch's answer lands.
        mirror.record_label(&LabelOp::Remove {
            id: LabelId("Label_1".into()),
            at: Timestamp(10),
        });
        mirror.record_label(&LabelOp::Upsert(defined("Label_1", "Renamed")));
        assert!(entries(&mirror).is_empty());
        assert_eq!(mirror.label_definitions(), vec![defined("INBOX", "INBOX")]);
        assert!(
            mirror
                .record
                .deleted_labels
                .contains_key(&LabelId("Label_1".into()))
        );
        // Only a listing restores it.
        listed(&mut mirror, &["INBOX", "Label_1"], 20);
        assert!(mirror.record.deleted_labels.is_empty());
    }

    #[test]
    fn a_finishing_reconciliation_lists_no_deleted_label() {
        let mut mirror = claimed();
        listed(&mut mirror, &["INBOX", "Label_1"], 0);
        // A row the listing created: its labels come from the scopes it was
        // listed in, one of them a label deleted before the finish.
        let listing = |scope: ListScope| MessageOp::Listed {
            id: MessageId("n".into()),
            thread_id: ThreadId("t".into()),
            scope,
            baseline: HistoryId(50),
        };
        applied(
            &mut mirror,
            vec![
                listing(ListScope::All),
                listing(ListScope::Label(LabelId("INBOX".into()))),
                listing(ListScope::Label(LabelId("Label_1".into()))),
            ],
        );
        mirror.record_label(&LabelOp::Remove {
            id: LabelId("Label_1".into()),
            at: Timestamp(10),
        });
        applied(
            &mut mirror,
            vec![MessageOp::FinishReconcile {
                baseline: HistoryId(50),
            }],
        );
        assert_eq!(labels_of(&mirror, "n"), set(["INBOX"]));
        assert!(crate::invariants::check_state(&mirror).is_empty());
    }
}
