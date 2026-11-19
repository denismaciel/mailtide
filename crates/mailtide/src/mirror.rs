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
use crate::gmail::{HeaderFacts, Label, MessageObservation};
use crate::ids::{
    AccountId, BlobRef, EngineVersion, Epoch, HistoryId, MessageId, ThreadId, Timestamp,
    stable_hash,
};
use crate::jobs::{
    AttemptId, ClaimedJob, ClaimedWork, Job, JobClaim, JobKind, JobOp, JobOutcome, JobState, NewJob,
};
use crate::labels::{self, LabelSet};
use crate::outbox::{self, ActionRequest, Delivery, Intent, IntentOp, IntentState, IntentVersion};
use crate::quota::Priority;
use crate::reconcile::{ListScope, ReconcileProgress, ReconcileReason};
use crate::retry::Cooldown;
use crate::revision::{Change, ChangePage, StateSeq};
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
    /// Change-feed entries at or before this state were compacted away.
    pub log_floor: StateSeq,
}

impl AccountRecord {
    /// A fresh account: no cursor, bootstrapping.
    #[must_use]
    pub fn new(account: &AccountId) -> Self {
        Self {
            epoch: Epoch(0),
            engine_version: EngineVersion(0),
            cursor: None,
            sync: SyncState::Bootstrapping(ReconcileProgress::begin(ReconcileReason::Bootstrap)),
            watch_expiration: None,
            last_activity: None,
            cooldown: None,
            rng_seed: stable_hash(account.as_str().as_bytes()),
            log_floor: StateSeq(0),
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
/// by a running import, gets a cleanup job.
#[must_use]
pub fn tombstone(
    current: Option<&MessageRecord>,
    existing: Option<HistoryId>,
    id: &MessageId,
    evidence: DeletionEvidence,
    import_running: bool,
    plan: &JobPlan,
) -> Decision {
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
}

impl AccountMirror {
    /// An empty mirror for `account`.
    #[must_use]
    pub fn new(account: AccountId) -> Self {
        Self {
            record: AccountRecord::new(&account),
            account,
            messages: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            jobs: BTreeMap::new(),
            seen: BTreeMap::new(),
            labels: Vec::new(),
            intents: BTreeMap::new(),
            log: Vec::new(),
            seq: StateSeq(0),
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
            match op {
                LabelOp::ReplaceAll(labels) => {
                    self.labels.clone_from(labels);
                    self.log.push((seq, Change::LabelDefinitionsChanged));
                }
            }
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
                        self.messages
                            .values()
                            .filter(|record| {
                                &record.thread_id == id && !labels::is_draft(&record.labels)
                            })
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
                    let new = admit(record, &self.intents_of(&target), action);
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
            } => mark_imported(
                self.messages.get(&observation.id),
                self.tombstones.get(&observation.id).copied(),
                observation,
                StoredContent {
                    blob: blob.clone(),
                    size: *size,
                    internal_date: *internal_date,
                    headers: (**headers).clone(),
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
                let seen = std::mem::take(&mut self.seen);
                let decisions: Vec<Decision> = self
                    .messages
                    .values()
                    .map(|record| finish_reconcile(record, seen.get(&record.id), *baseline, plan))
                    .collect();
                for decision in decisions {
                    self.realize(decision, seq);
                }
                return true;
            }
        };
        self.realize(decision, seq)
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
        if epoch != self.record.epoch {
            return Err(Rejection::StaleEpoch);
        }
        let mut claimed = Vec::new();
        for _ in 0..limit {
            let jobs: Vec<Job> = self.jobs.values().cloned().collect();
            let Some(job) = crate::jobs::pick_next(&jobs, now) else {
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

    /// Changes after `since`: at most `limit`, except that a page always
    /// ends on a whole commit (a state string names a commit, so a page cut
    /// inside one would lose its remaining changes).
    #[must_use]
    /// Drop change-feed entries at or before `through` (clamped to the
    /// current state) once every consumer has processed them. Returns how
    /// many were removed.
    pub fn compact(&mut self, through: StateSeq) -> u64 {
        let through = through.min(self.seq);
        let before = self.log.len();
        self.log.retain(|(seq, _)| *seq > through);
        self.record.log_floor = self.record.log_floor.max(through);
        (before - self.log.len()) as u64
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

    /// The feed after `since`, or `TooOld` if that was compacted away.
    #[must_use]
    pub fn feed(&self, since: StateSeq, limit: u32) -> crate::revision::ChangeFeed {
        if since < self.record.log_floor {
            return crate::revision::ChangeFeed::TooOld {
                oldest: self.record.log_floor,
            };
        }
        crate::revision::ChangeFeed::Page(self.changes(since, limit))
    }

    /// Changes after `since` (no compaction check; see [`Self::feed`]).
    #[must_use]
    pub fn changes(&self, since: StateSeq, limit: u32) -> ChangePage {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX).max(1);
        let after: Vec<&(StateSeq, Change)> =
            self.log.iter().filter(|(seq, _)| *seq > since).collect();
        let mut take = after.len().min(limit);
        if let Some((last, _)) = take.checked_sub(1).and_then(|i| after.get(i)) {
            while after.get(take).is_some_and(|(seq, _)| seq == last) {
                take += 1;
            }
        }
        let page = &after[..take];
        // Pages hold whole commits, so an entry's index is its count within
        // its state.
        let mut changes = Vec::with_capacity(page.len());
        let mut position: Option<(StateSeq, u32)> = None;
        for (seq, change) in page {
            let index = match position {
                Some((current, index)) if current == *seq => index + 1,
                Some(_) | None => 0,
            };
            position = Some((*seq, index));
            changes.push(crate::revision::ChangeEntry {
                state: *seq,
                index,
                change: change.clone(),
            });
        }
        ChangePage {
            changes,
            next: page.last().map_or(since, |(seq, _)| *seq),
            has_more: after.len() > take,
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
        let mut mirror = AccountMirror::new(account());
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
        let first = mirror.changes(StateSeq(0), 2);
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
}
