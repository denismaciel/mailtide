//! A deterministic Gmail simulator with fault injection (SPEC §12).
//!
//! Drives the real [`mailtide::Engine`] against an in-memory Gmail and an
//! in-memory store ([`AccountMirror`], the same rules every store uses).
//! Everything is decided by one seeded generator, so a run replays exactly
//! from its seed. During the fault phase the simulator interleaves mailbox
//! activity, out-of-order results, transient failures, rate limits, history
//! expiry, dropped and duplicated pushes, lost commit acknowledgements and
//! crashes (the engine is rebuilt from the store; results meant for the dead
//! engine still arrive). Label definitions are created, renamed and deleted
//! while the account is live. Invariants are checked after every commit. Two
//! named feed readers take one page of a random limit between steps and
//! acknowledge it, while the log is compacted at random steps: the steady
//! one must never be cut off and must end with the whole change log; the
//! other stalls, and once the simulated clock passes the idle limit it is
//! cut off. Then all faults heal and the mirror must converge to Gmail, its
//! label definitions within the refresh interval.
//!
//! Zero counterpart, by role: `lib/imap/testing/memory-server.ts`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mailtide::EngineConfig;
use mailtide::command::{Command, CommandKind, GmailRequest};
use mailtide::commit::{Commit, CommitOutcome, DeletionEvidence, LabelOp};
use mailtide::engine::Engine;
use mailtide::event::{Event, EventKind};
use mailtide::gmail::{
    FACTS_VERSION, HeaderFacts, HistoryPage, HistoryRecord, ImportedMessage, Label, LabelKind,
    ListPage, MessageFacts, MessageObservation, MessageRef,
};
use mailtide::ids::{
    AccountId, BlobRef, CommandId, ENGINE_VERSION, HistoryId, LabelId, MessageId, PageToken,
    ThreadId, Timestamp, splitmix64, stable_hash,
};
use mailtide::invariants::{self, Violation};
use mailtide::jobs::{JobKind, JobState};
use mailtide::labels::{self, LabelSet};
use mailtide::mirror::{AccountMirror, Content, FactsUpdate};
use mailtide::outbox::{
    Action, ActionRequest, IntentState, LabelDelta, ModifyGroup, ReadState, RestoreDestination,
    StarState,
};
use mailtide::purge::{self, Bin, PurgeScan, Scope, Verdict};
use mailtide::reconcile::ListScope;
use mailtide::retry::{Blocking, BlockingFailure, Failure, Retryable, RetryableFailure};
use mailtide::revision::{
    AckOutcome, Change, ChangeFeed, ConsumerName, FeedCursor, FeedPosition, Retention, StateSeq,
};
use mailtide::schedule::TimerKind;

// ------------------------------------------------------------ randomness

/// A probability: finite and within `[0, 1]` (review #20).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Probability(f64);

/// A value that isn't a probability.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InvalidProbability(pub f64);

impl Probability {
    /// Never.
    pub const NEVER: Self = Self(0.0);

    /// A probability, if `value` is one.
    ///
    /// # Errors
    ///
    /// When `value` is not finite or lies outside `[0, 1]`.
    pub fn new(value: f64) -> Result<Self, InvalidProbability> {
        if value.is_finite() && (0.0..=1.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidProbability(value))
        }
    }

    /// The value.
    #[must_use]
    pub fn get(self) -> f64 {
        self.0
    }
}

/// The simulator's only source of randomness.
#[derive(Debug, Clone)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        splitmix64(self.0)
    }

    fn below(&mut self, n: usize) -> usize {
        match u64::try_from(n) {
            Ok(0) | Err(_) => 0,
            Ok(n) => usize::try_from(self.next() % n).unwrap_or(0),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn chance(&mut self, p: Probability) -> bool {
        p.0 > 0.0 && self.unit() < p.0
    }

    fn probability(&mut self, max: f64) -> Probability {
        Probability::new(self.unit() * max).unwrap_or(Probability::NEVER)
    }
}

// ------------------------------------------------------------ configuration

/// Fault probabilities for one run, per step.
#[derive(Debug, Clone, PartialEq)]
pub struct Faults {
    /// Mailbox activity happens this step.
    pub user_activity: Probability,
    /// All history so far expires (the next history read returns 404).
    pub history_expired: Probability,
    /// A push is dropped.
    pub push_dropped: Probability,
    /// A push is delivered twice.
    pub push_duplicated: Probability,
    /// A Gmail request fails transiently.
    pub transient: Probability,
    /// A Gmail request is rate limited.
    pub rate_limited: Probability,
    /// A commit applies but its acknowledgement is lost (the host rebuilds).
    pub lost_commit_ack: Probability,
    /// The host crashes and rebuilds the engine from the store.
    pub crash: Probability,
    /// The host runs a facts refresh this step (outside the engine).
    pub facts_refresh: Probability,
    /// Gmail answers a label listing without labels, or cut short.
    pub bad_listing: Probability,
}

impl Faults {
    /// No faults and no activity: used once faults heal.
    #[must_use]
    pub fn none() -> Self {
        Self {
            user_activity: Probability::NEVER,
            history_expired: Probability::NEVER,
            push_dropped: Probability::NEVER,
            push_duplicated: Probability::NEVER,
            transient: Probability::NEVER,
            rate_limited: Probability::NEVER,
            lost_commit_ack: Probability::NEVER,
            crash: Probability::NEVER,
            facts_refresh: Probability::NEVER,
            bad_listing: Probability::NEVER,
        }
    }

    /// Fault rates drawn per seed ("swarm testing"), so different seeds
    /// stress different combinations.
    fn swarm(rng: &mut Rng) -> Self {
        Self {
            user_activity: rng.probability(0.4),
            history_expired: rng.probability(0.004),
            push_dropped: rng.probability(0.5),
            push_duplicated: rng.probability(0.3),
            transient: rng.probability(0.1),
            rate_limited: rng.probability(0.03),
            lost_commit_ack: rng.probability(0.02),
            crash: rng.probability(0.01),
            facts_refresh: rng.probability(0.05),
            bad_listing: rng.probability(0.2),
        }
    }
}

/// One concrete mailbox change. Targets are picked by index into the
/// current messages, so a script keeps its meaning when replayed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserOp {
    /// New mail arrives, in a new thread or an existing one.
    Receive {
        /// Reply in the n-th existing thread, if any.
        thread: Option<usize>,
    },
    /// A draft is saved (a new message id; the previous draft is deleted).
    SaveDraft,
    /// A label is toggled on the n-th non-draft message.
    Relabel {
        /// Which message.
        pick: usize,
        /// Which label.
        label: LabelId,
    },
    /// The n-th message is deleted forever.
    Delete {
        /// Which message.
        pick: usize,
    },
    /// A user label is created (Gmail records no history for it).
    CreateLabel,
    /// The n-th user label is renamed (no history either).
    RenameLabel {
        /// Which label.
        pick: usize,
    },
    /// The n-th user label is deleted: Gmail removes it from its messages,
    /// with one `labelsRemoved` record each.
    DeleteLabel {
        /// Which label.
        pick: usize,
    },
    /// The n-th user label is deleted and its messages lose it without a
    /// history record (the other answer to `label-edit-delete-history`).
    /// Only for a label the mirror's definitions hold: Gmail records every
    /// deletion, and what a listing proves is about labels it defined.
    DeleteLabelQuietly {
        /// Which label.
        pick: usize,
    },
    /// The host creates a user label through the driver: Gmail creates it,
    /// and the driver records the answer, unless it crashes in between.
    HostCreateLabel,
    /// The host renames the n-th user label through the driver.
    HostRenameLabel {
        /// Which label.
        pick: usize,
    },
    /// The host deletes the n-th user label through the driver.
    HostDeleteLabel {
        /// Which label.
        pick: usize,
    },
    /// 31 days pass: past the deleted-label memory, and past Gmail's
    /// history (a cursor from before reads 404).
    MonthPasses,
    /// The user acts in the host's UI on the n-th mirrored message (or
    /// its whole thread); the engine delivers it to Gmail.
    HostAction {
        /// Which message, among those the mirror shows.
        pick: usize,
        /// The action.
        action: Action,
        /// Act on the message's whole thread.
        whole_thread: bool,
    },
    /// The user opens the n-th mirrored message whose body isn't there yet
    /// (or, if every body is, any mirrored message): the host calls
    /// `prioritise`, a store write outside the engine.
    HostOpen {
        /// Which message, among those the mirror shows.
        pick: usize,
    },
    /// The user deletes the n-th mirrored message (or its whole thread)
    /// for good: the host's purge call, outside the engine, with faults
    /// (another client's change since the last sync, a quota deferral, a
    /// 5xx, a 404 for a live message, an unread confirmation, a crash
    /// before the record) drawn as it runs.
    HostPurge {
        /// Which message, among those the mirror shows.
        pick: usize,
        /// Purge the message's whole thread.
        whole_thread: bool,
    },
    /// The user empties the trash (or spam) for good: the host's `empty`.
    HostEmpty {
        /// Spam rather than trash.
        spam: bool,
    },
}

/// Whether the mirror converged once faults healed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Convergence {
    /// Mirror = Gmail after this many steps of healing.
    Reached {
        /// Steps taken after healing.
        after_steps: u64,
    },
    /// Still different when the healing budget ran out.
    NotReached {
        /// What differs.
        difference: String,
        /// The last commands and events, oldest first.
        trace: Vec<String>,
    },
}

/// An invariant that broke, and when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedViolation {
    /// The step.
    pub step: u64,
    /// What broke.
    pub violation: Violation,
}

/// Counts of what a run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    /// Commits applied.
    pub commits: u64,
    /// Commits rejected.
    pub rejections: u64,
    /// Engine rebuilds (crashes and lost acknowledgements).
    pub rebuilds: u64,
    /// Times history expired.
    pub expiries: u64,
    /// Messages in Gmail at the end.
    pub messages: u64,
    /// Threads in Gmail at the end.
    pub threads: u64,
    /// User operations applied.
    pub user_ops: u64,
    /// Label changes the engine delivered to Gmail (user actions in the host).
    pub deliveries: u64,
    /// Label definitions created, renamed or deleted in Gmail.
    pub label_edits: u64,
    /// Of those, the ones the host made through the driver and recorded.
    pub label_writes_recorded: u64,
    /// Of those, the ones whose record a crash lost.
    pub label_writes_lost: u64,
    /// Label deletions that left no history record.
    pub quiet_label_deletes: u64,
    /// Deliveries Gmail refused because an add named a label it lacks.
    pub refused_deliveries: u64,
    /// Label listings Gmail answered without labels or cut short.
    pub bad_listings: u64,
    /// Listings not applied because a local label write made them stale.
    pub fenced_listings: u64,
    /// Deleted-label memory entries that expired.
    pub forgotten_deletions: u64,
    /// `labels.list` calls Gmail answered.
    pub label_listings: u64,
    /// Messages users opened in the host.
    pub opens: u64,
    /// Imports those opens raised to the Foreground class.
    pub raised: u64,
    /// Feed pages the reader took that ended inside a commit (one larger
    /// than the page's limit).
    pub feed_pages_split: u64,
    /// Compactions of the change log.
    pub compactions: u64,
    /// Log entries they removed.
    pub compacted: u64,
    /// Readers cut off for staying silent past the idle limit.
    pub consumers_cut_off: u64,
    /// Message facts a refresh brought up to date.
    pub facts_refreshed: u64,
    /// Purges and emptyings started on a live account.
    pub purges: u64,
    /// Messages they deleted in Gmail.
    pub purge_deletes: u64,
    /// Messages they recorded as deleted from Gmail's evidence.
    pub purged: u64,
    /// Purge batches dispatched.
    pub purge_batches: u64,
    /// Remaining targets that left their bin during a preceding single.
    pub purge_restores_between_requests: u64,
    /// Purge token failures before any send.
    pub purge_token_failures: u64,
    /// Purge releases that failed after a send.
    pub purge_release_failures: u64,
    /// Partially applied batch refusals.
    pub purge_partial_refusals: u64,
    /// Purge confirmations with withheld deletion records.
    pub purge_late_confirmations: u64,
}

/// The outcome of one run.
#[derive(Debug, Clone, PartialEq)]
pub struct RunResult {
    /// The seed, for replay.
    pub seed: u64,
    /// The fault rates the seed chose.
    pub faults: Faults,
    /// Steps taken in the fault phase.
    pub steps: u64,
    /// Invariants that broke.
    pub violations: Vec<ObservedViolation>,
    /// Whether the mirror converged after healing.
    pub convergence: Convergence,
    /// What happened.
    pub stats: Stats,
}

impl RunResult {
    /// No violations, and converged.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.violations.is_empty() && matches!(self.convergence, Convergence::Reached { .. })
    }
}

/// Run one seeded simulation of `steps` fault-phase steps.
#[must_use]
pub fn run(seed: u64, steps: u64) -> RunResult {
    let mut rng = Rng(seed);
    let faults = Faults::swarm(&mut rng);
    Sim::new(rng, faults, None).run(seed, steps)
}

/// Run like [`run`], also returning every trace line that contains `grep`
/// (untruncated), to follow one message or command through a failing seed.
#[must_use]
pub fn debug_run(seed: u64, steps: u64, grep: &str) -> (RunResult, Vec<String>) {
    let mut rng = Rng(seed);
    let faults = Faults::swarm(&mut rng);
    let mut sim = Sim::new(rng, faults, None);
    sim.grep = Some(grep.to_owned());
    sim.run_inner(seed, steps)
}

/// Replay a fixed script of mailbox changes (for shrinking a failure).
#[must_use]
pub fn replay(seed: u64, script: &[UserOp]) -> RunResult {
    let mut rng = Rng(seed);
    let faults = Faults::swarm(&mut rng);
    let steps = u64::try_from(script.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(4)
        .max(50);
    Sim::new(rng, faults, Some(script.to_vec())).run(seed, steps)
}

// ------------------------------------------------------------ fake Gmail

const LABELS: [&str; 6] = ["INBOX", "UNREAD", "STARRED", "IMPORTANT", "SPAM", "TRASH"];
const HISTORY_PAGE: usize = 3;
const LIST_PAGE: usize = 5;

#[derive(Debug, Clone)]
struct GmailMessage {
    thread: ThreadId,
    labels: LabelSet,
    modified: HistoryId,
    /// `internalDate`: two messages in a row share one, so ties occur.
    date: Timestamp,
}

/// What `messages.modify` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Modified {
    Done,
    /// A message doesn't exist (404).
    NotFound,
    /// An add names a label the mailbox lacks (400).
    UnknownLabel,
}

#[derive(Debug, Clone)]
struct Gmail {
    messages: BTreeMap<MessageId, GmailMessage>,
    history: Vec<HistoryRecord>,
    last_history: u64,
    expired_before: u64,
    next_message: u64,
    next_thread: u64,
    draft: Option<MessageId>,
    /// User label definitions: id to name.
    user_labels: BTreeMap<LabelId, String>,
    next_label: u64,
}

impl Default for Gmail {
    fn default() -> Self {
        Self {
            messages: BTreeMap::new(),
            history: Vec::new(),
            last_history: 0,
            expired_before: 0,
            next_message: 0,
            next_thread: 0,
            draft: None,
            user_labels: BTreeMap::from([(LabelId("Label_1".into()), "Label_1".into())]),
            next_label: 1,
        }
    }
}

impl Gmail {
    fn current(&self) -> HistoryId {
        HistoryId(self.last_history)
    }

    fn next_id(&mut self) -> HistoryId {
        self.last_history += 1;
        HistoryId(self.last_history)
    }

    fn add(&mut self, thread: ThreadId, labels: LabelSet) -> MessageId {
        self.next_message += 1;
        let id = MessageId(format!("m{:06}", self.next_message));
        let at = self.next_id();
        self.history.push(HistoryRecord::MessageAdded {
            id: at,
            message: MessageRef {
                id: id.clone(),
                thread_id: thread.clone(),
                labels: labels.clone(),
            },
        });
        self.messages.insert(
            id.clone(),
            GmailMessage {
                thread,
                labels,
                modified: at,
                date: Timestamp(i64::try_from(self.next_message / 2).unwrap_or(i64::MAX)),
            },
        );
        id
    }

    /// `messages.modify`. Gmail refuses the whole request when an add names
    /// a label it does not have (400 `labelId not found`); removing one is
    /// accepted and changes nothing (`label-edit-modify-deleted-label`).
    fn modify(&mut self, group: &ModifyGroup) -> Modified {
        if group
            .messages
            .iter()
            .any(|id| !self.messages.contains_key(id))
        {
            return Modified::NotFound;
        }
        if group.add.iter().any(|label| !self.defines(label)) {
            return Modified::UnknownLabel;
        }
        for id in &group.messages {
            let current = self.messages[id].labels.clone();
            let added: LabelSet = group
                .add
                .iter()
                .filter(|l| !current.contains(*l))
                .cloned()
                .collect();
            let removed: LabelSet = group
                .remove
                .iter()
                .filter(|l| current.contains(*l))
                .cloned()
                .collect();
            if !added.is_empty() {
                let at = self.next_id();
                self.history.push(HistoryRecord::LabelsAdded {
                    id: at,
                    message_id: id.clone(),
                    labels: added.clone(),
                });
            }
            if !removed.is_empty() {
                let at = self.next_id();
                self.history.push(HistoryRecord::LabelsRemoved {
                    id: at,
                    message_id: id.clone(),
                    labels: removed.clone(),
                });
            }
            if let Some(message) = self.messages.get_mut(id) {
                message.labels.extend(added);
                message.labels.retain(|l| !removed.contains(l));
                message.modified = HistoryId(self.last_history);
            }
        }
        Modified::Done
    }

    /// Whether the mailbox has the label `id`.
    fn defines(&self, id: &LabelId) -> bool {
        LABELS.contains(&id.0.as_str()) || id.0 == "DRAFT" || self.user_labels.contains_key(id)
    }

    fn delete(&mut self, id: &MessageId) {
        if self.messages.remove(id).is_some() {
            let at = self.next_id();
            self.history.push(HistoryRecord::MessageDeleted {
                id: at,
                message_id: id.clone(),
            });
        }
    }

    fn new_thread(&mut self) -> ThreadId {
        self.next_thread += 1;
        ThreadId(format!("t{:06}", self.next_thread))
    }

    fn apply(&mut self, op: &UserOp) {
        match op {
            UserOp::Receive { thread } => {
                let threads: Vec<ThreadId> = self
                    .messages
                    .values()
                    .map(|m| m.thread.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let thread = match thread {
                    Some(n) if !threads.is_empty() => threads[n % threads.len()].clone(),
                    Some(_) | None => self.new_thread(),
                };
                self.add(thread, labels::set(["INBOX", "UNREAD"]));
            }
            UserOp::SaveDraft => {
                if let Some(old) = self.draft.take() {
                    self.delete(&old);
                }
                let thread = self.new_thread();
                self.draft = Some(self.add(thread, labels::set(["DRAFT"])));
            }
            UserOp::Relabel { pick, label } => self.relabel(*pick, label),
            // Goes through the engine, not straight to Gmail.
            UserOp::HostAction { .. }
            | UserOp::HostOpen { .. }
            | UserOp::HostPurge { .. }
            | UserOp::HostEmpty { .. }
            | UserOp::MonthPasses => {}
            UserOp::Delete { pick } => {
                let ids: Vec<MessageId> = self.messages.keys().cloned().collect();
                if ids.is_empty() {
                    return;
                }
                let id = ids[pick % ids.len()].clone();
                if self.draft.as_ref() == Some(&id) {
                    self.draft = None;
                }
                self.delete(&id);
            }
            UserOp::CreateLabel => {
                self.next_label += 1;
                let id = LabelId(format!("Label_{}", self.next_label));
                self.user_labels
                    .insert(id, format!("label {}", self.next_label));
            }
            UserOp::RenameLabel { pick } => {
                if let Some(name) = self
                    .user_label(*pick)
                    .and_then(|id| self.user_labels.get_mut(&id))
                {
                    name.push('+');
                }
            }
            UserOp::DeleteLabel { pick } | UserOp::HostDeleteLabel { pick } => {
                if let Some(id) = self.user_label(*pick) {
                    self.delete_label(&id, true);
                }
            }
            UserOp::DeleteLabelQuietly { pick } => {
                if let Some(id) = self.user_label(*pick) {
                    self.delete_label(&id, false);
                }
            }
            UserOp::HostCreateLabel => {
                self.next_label += 1;
                let id = LabelId(format!("Label_{}", self.next_label));
                self.user_labels
                    .insert(id, format!("host label {}", self.next_label));
            }
            UserOp::HostRenameLabel { pick } => {
                if let Some(name) = self
                    .user_label(*pick)
                    .and_then(|id| self.user_labels.get_mut(&id))
                {
                    name.push('*');
                }
            }
        }
    }

    /// The definition of user label `id`, as `labels.create` or
    /// `labels.patch` answers it.
    fn user_label_definition(&self, id: &LabelId) -> Option<Label> {
        self.user_labels
            .get(id)
            .map(|name| Label::plain(id.clone(), name.clone(), LabelKind::User))
    }

    fn user_label(&self, pick: usize) -> Option<LabelId> {
        let ids: Vec<&LabelId> = self.user_labels.keys().collect();
        (!ids.is_empty()).then(|| ids[pick % ids.len()].clone())
    }

    /// Delete a user label: it leaves every message carrying it, and those
    /// removals are recorded as history when `recorded`.
    fn delete_label(&mut self, label: &LabelId, recorded: bool) {
        self.user_labels.remove(label);
        if !recorded {
            for message in self.messages.values_mut() {
                message.labels.remove(label);
            }
            return;
        }
        let carrying: Vec<MessageId> = self
            .messages
            .iter()
            .filter(|(_, m)| m.labels.contains(label))
            .map(|(id, _)| id.clone())
            .collect();
        for id in carrying {
            let at = self.next_id();
            if let Some(message) = self.messages.get_mut(&id) {
                message.labels.remove(label);
                message.modified = at;
            }
            self.history.push(HistoryRecord::LabelsRemoved {
                id: at,
                message_id: id,
                labels: LabelSet::from([label.clone()]),
            });
        }
    }

    /// Labels a message may carry now: the system ones and the defined user
    /// ones.
    fn assignable(&self) -> Vec<LabelId> {
        LABELS
            .iter()
            .map(|id| LabelId((*id).to_owned()))
            .chain(self.user_labels.keys().cloned())
            .collect()
    }

    fn relabel(&mut self, pick: usize, label: &LabelId) {
        let candidates: Vec<MessageId> = self
            .messages
            .iter()
            .filter(|(_, m)| !labels::is_draft(&m.labels))
            .map(|(id, _)| id.clone())
            .collect();
        if candidates.is_empty() {
            return;
        }
        let id = candidates[pick % candidates.len()].clone();
        let undefined = label.0.starts_with("Label_") && !self.user_labels.contains_key(label);
        if undefined && !self.messages[&id].labels.contains(label) {
            // Gmail refuses to add a label that doesn't exist.
            return;
        }
        let at = self.next_id();
        let Some(message) = self.messages.get_mut(&id) else {
            return;
        };
        let change = LabelSet::from([label.clone()]);
        if message.labels.contains(label) {
            message.labels.remove(label);
            self.history.push(HistoryRecord::LabelsRemoved {
                id: at,
                message_id: id,
                labels: change,
            });
        } else {
            message.labels.insert(label.clone());
            self.history.push(HistoryRecord::LabelsAdded {
                id: at,
                message_id: id,
                labels: change,
            });
        }
        message.modified = at;
    }

    fn history_page(&self, start: HistoryId, token: Option<&PageToken>) -> Option<HistoryPage> {
        if start.0 < self.expired_before {
            return None;
        }
        let records: Vec<&HistoryRecord> = self
            .history
            .iter()
            .filter(|record| mailtide::history::record_id(record) > start)
            .collect();
        let offset = token.and_then(|t| t.0.parse::<usize>().ok()).unwrap_or(0);
        let page: Vec<HistoryRecord> = records
            .iter()
            .skip(offset)
            .take(HISTORY_PAGE)
            .map(|record| (*record).clone())
            .collect();
        let more = offset + HISTORY_PAGE < records.len();
        Some(HistoryPage {
            records: page,
            history_id: self.current(),
            next_page_token: more.then(|| PageToken((offset + HISTORY_PAGE).to_string())),
        })
    }

    fn list(&self, scope: &ListScope, token: Option<&PageToken>) -> ListPage {
        let ids: Vec<(MessageId, ThreadId)> = self
            .messages
            .iter()
            .rev()
            .filter(|(_, m)| match scope {
                ListScope::All => true,
                ListScope::Label(label) => m.labels.contains(label),
            })
            .map(|(id, m)| (id.clone(), m.thread.clone()))
            .collect();
        let offset = token.and_then(|t| t.0.parse::<usize>().ok()).unwrap_or(0);
        let more = offset + LIST_PAGE < ids.len();
        ListPage {
            messages: ids.into_iter().skip(offset).take(LIST_PAGE).collect(),
            next_page_token: more.then(|| PageToken((offset + LIST_PAGE).to_string())),
        }
    }

    fn get(&self, id: &MessageId) -> Option<MessageObservation> {
        self.messages.get(id).map(|m| MessageObservation {
            id: id.clone(),
            thread_id: m.thread.clone(),
            labels: m.labels.clone(),
            checkpoint: m.modified,
        })
    }

    /// Messages by thread.
    fn threads(&self) -> BTreeMap<&ThreadId, Vec<(&MessageId, &GmailMessage)>> {
        let mut threads: BTreeMap<&ThreadId, Vec<(&MessageId, &GmailMessage)>> = BTreeMap::new();
        for (id, message) in &self.messages {
            threads
                .entry(&message.thread)
                .or_default()
                .push((id, message));
        }
        threads
    }

    /// `labels.list`, in id order.
    fn labels(&self) -> Vec<Label> {
        let mut labels: Vec<Label> = LABELS
            .iter()
            .chain(["DRAFT"].iter())
            .map(|id| Label::plain(LabelId((*id).to_owned()), *id, LabelKind::System))
            .chain(
                self.user_labels
                    .iter()
                    .map(|(id, name)| Label::plain(id.clone(), name.clone(), LabelKind::User)),
            )
            .collect();
        mailtide::mirror::sort_labels(&mut labels);
        labels
    }
}

// ------------------------------------------------------------ the feed readers

/// How long a reader may stay silent behind the head and still hold the log
/// (the simulated clock moves by timers: minutes to hours per run).
const MAX_IDLE: Duration = Duration::from_mins(30);

/// The chance that the log is compacted after a step.
const COMPACTION: f64 = 0.1;

/// A named consumer of the change feed: between steps it takes one page of
/// a random limit from where it stopped and acknowledges it, as a host's
/// pipeline would. The steady reader never stops, so compaction must never
/// cut it off, and at the end its entries must be the log's (the compacted
/// part included), entry for entry. The stalled reader stops after a random
/// step; at the end it is either cut off (its read is too old) or reads on
/// to the same log. Each draws from its own generator, so runs take the same
/// steps as without them.
struct FeedReader {
    name: ConsumerName,
    rng: Rng,
    at: FeedCursor,
    read: Vec<(StateSeq, u32, Change)>,
    /// The step after which it neither reads nor acknowledges.
    stalls_after: Option<u64>,
}

impl FeedReader {
    fn steady(rng: &Rng) -> Self {
        Self::new(
            "steady",
            Rng(splitmix64(rng.0 ^ 0x0fee_d0fe_ed00_f00d)),
            None,
        )
    }

    fn stalled(rng: &Rng) -> Self {
        let mut own = Rng(splitmix64(rng.0 ^ 0x5741_11ed_0000_0001));
        let stalls_after = u64::try_from(own.below(200)).unwrap_or(0) + 1;
        Self::new("stalled", own, Some(stalls_after))
    }

    fn new(name: &str, rng: Rng, stalls_after: Option<u64>) -> Self {
        Self {
            name: ConsumerName::parse(name).expect("a valid consumer name"),
            rng,
            at: StateSeq(0).into(),
            read: Vec::new(),
            stalls_after,
        }
    }

    /// Whether it has stopped by `step`.
    fn is_stalled(&self, step: u64) -> bool {
        self.stalls_after.is_some_and(|last| step > last)
    }

    /// Take one page and acknowledge it at `now`: whether more are waiting.
    /// A read that is too old is a cut-off; a page above its limit, or one
    /// that ends inside a commit it does not fill alone, is a divergence.
    fn take_page(
        &mut self,
        mirror: &mut AccountMirror,
        now: Timestamp,
        stats: &mut Stats,
    ) -> Result<bool, Violation> {
        let limit = u32::try_from(self.rng.below(8) + 1).unwrap_or(1);
        let page = match mirror.feed(self.at, limit) {
            ChangeFeed::Page(page) => page,
            ChangeFeed::TooOld { oldest, .. } => {
                return Err(Violation::ConsumerCutOff {
                    consumer: self.name.as_str().to_owned(),
                    at: self.at,
                    floor: oldest,
                });
            }
        };
        if page.changes.len() > usize::try_from(limit).unwrap_or(usize::MAX) {
            return Err(self.diverged(format!(
                "a page of {} entries above its limit {limit}",
                page.changes.len()
            )));
        }
        // A page ends inside a commit only when that commit alone fills it.
        if page.next.entry.is_some() {
            if page.changes.len() != usize::try_from(limit).unwrap_or(usize::MAX)
                || page
                    .changes
                    .iter()
                    .any(|entry| entry.state != page.next.seq)
            {
                return Err(self.diverged(format!(
                    "a page of {} entries with limit {limit} ends inside commit {:?} \
                     without being filled by it",
                    page.changes.len(),
                    page.next.seq
                )));
            }
            stats.feed_pages_split += 1;
        }
        self.read.extend(
            page.changes
                .into_iter()
                .map(|entry| (entry.state, entry.index, entry.change)),
        );
        self.at = page.next;
        let position = FeedPosition {
            generation: page.generation,
            at: page.next,
        };
        match mirror.ack(&self.name, position, now) {
            AckOutcome::Acked(_) => Ok(page.has_more),
            refused => Err(self.diverged(format!(
                "the ack of {:?} after reading it was refused: {refused:?}",
                page.next
            ))),
        }
    }

    /// At the end of a run: a reader the compactions cut off must find its
    /// read too old; any other reads to the end, and its entries must be
    /// `compacted` then the log, numbered within their commit.
    fn finish(
        &mut self,
        mirror: &mut AccountMirror,
        compacted: &[(StateSeq, Change)],
        now: Timestamp,
        stats: &mut Stats,
    ) -> Option<Violation> {
        let lost = mirror
            .consumers
            .get(&self.name)
            .is_some_and(|cursor| cursor.is_lost(mirror.record.log_floor));
        if lost {
            if !matches!(mirror.feed(self.at, 1), ChangeFeed::TooOld { .. }) {
                return Some(self.diverged("a lost reader read on".to_owned()));
            }
            stats.consumers_cut_off += 1;
            return None;
        }
        loop {
            match self.take_page(mirror, now, stats) {
                Ok(true) => {}
                Ok(false) => break,
                Err(violation) => return Some(violation),
            }
        }
        let mut logged = Vec::with_capacity(compacted.len() + mirror.log.len());
        let mut position: Option<(StateSeq, u32)> = None;
        for (seq, change) in compacted.iter().chain(&mirror.log) {
            let index = match position {
                Some((current, index)) if current == *seq => index + 1,
                Some(_) | None => 0,
            };
            position = Some((*seq, index));
            logged.push((*seq, index, change));
        }
        let read: Vec<_> = self
            .read
            .iter()
            .map(|(seq, index, change)| (*seq, *index, change))
            .collect();
        if read == logged {
            return None;
        }
        let at = read
            .iter()
            .zip(&logged)
            .position(|(a, b)| a != b)
            .unwrap_or(read.len().min(logged.len()));
        Some(self.diverged(format!(
            "{} read {} entries, the log holds {}; first difference at {at}: {:?} != {:?}",
            self.name.as_str(),
            read.len(),
            logged.len(),
            read.get(at),
            logged.get(at)
        )))
    }

    fn diverged(&self, difference: String) -> Violation {
        Violation::FeedPagesDiverged {
            read: u64::try_from(self.read.len()).unwrap_or(u64::MAX),
            difference,
        }
    }
}

// ------------------------------------------------------------ the simulated host

#[derive(Clone, Copy, PartialEq, Eq)]
enum PurgeFault {
    None,
    Deferred,
    Token,
    Release,
    Missing,
    Partial,
    Late,
    Server(bool),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PurgePoint {
    AfterCheck,
    DuringSingle,
}

struct Sim {
    rng: Rng,
    faults: Faults,
    script: Option<Vec<UserOp>>,
    account: AccountId,
    gmail: Gmail,
    mirror: AccountMirror,
    engine: Engine,
    now: Timestamp,
    pending: Vec<Command>,
    deliveries: Vec<Event>,
    timers: BTreeMap<TimerKind, Timestamp>,
    blobs: BTreeSet<MessageId>,
    violations: Vec<ObservedViolation>,
    stats: Stats,
    reader: FeedReader,
    stalled: FeedReader,
    /// Decides when the log is compacted, from its own generator.
    compactor: Rng,
    purge_rng: Rng,
    /// The entries compaction removed, in log order.
    compacted: Vec<(StateSeq, Change)>,
    step: u64,
    trace: std::collections::VecDeque<String>,
    /// Debugging: every line containing this text is kept in `matched`.
    grep: Option<String>,
    matched: Vec<String>,
}

const TRACE: usize = 40;

impl Sim {
    fn note(&mut self, line: &str) {
        if let Some(grep) = &self.grep
            && line.contains(grep.as_str())
        {
            self.matched.push(format!("step {}: {line}", self.step));
        }
        if self.trace.len() == TRACE {
            self.trace.pop_front();
        }
        self.trace.push_back(line.chars().take(240).collect());
    }

    fn new(rng: Rng, faults: Faults, script: Option<Vec<UserOp>>) -> Self {
        let account = AccountId("sim@example.com".into());
        let mut mirror = AccountMirror::new(account.clone(), 1);
        let engine = build_engine(&mut mirror);
        let mut gmail = Gmail::default();
        for n in 0..5 {
            gmail.apply(&UserOp::Receive {
                thread: (n % 2 == 0).then_some(0),
            });
        }
        let reader = FeedReader::steady(&rng);
        let stalled = FeedReader::stalled(&rng);
        let compactor = Rng(splitmix64(rng.0 ^ 0xc0_4ac7_0000_0002));
        let purge_rng = Rng(splitmix64(rng.0 ^ 0x0050_7572_6765));
        Self {
            rng,
            faults,
            script,
            account,
            gmail,
            mirror,
            engine,
            now: Timestamp(1_000_000),
            pending: Vec::new(),
            deliveries: Vec::new(),
            timers: BTreeMap::new(),
            blobs: BTreeSet::new(),
            violations: Vec::new(),
            stats: Stats::default(),
            reader,
            stalled,
            compactor,
            purge_rng,
            compacted: Vec::new(),
            step: 0,
            trace: std::collections::VecDeque::new(),
            grep: None,
            matched: Vec::new(),
        }
    }

    fn run(self, seed: u64, steps: u64) -> RunResult {
        self.run_inner(seed, steps).0
    }

    fn run_inner(mut self, seed: u64, steps: u64) -> (RunResult, Vec<String>) {
        let start = self.engine.start(self.now);
        self.pending.extend(start);
        for _ in 0..steps {
            self.step += 1;
            self.fault_step();
        }
        let fault_steps = self.step;
        let faults = std::mem::replace(&mut self.faults, Faults::none());
        let convergence = self.heal();
        self.housekeeping();
        for reader in [&mut self.reader, &mut self.stalled] {
            if let Some(violation) =
                reader.finish(&mut self.mirror, &self.compacted, self.now, &mut self.stats)
            {
                self.violations.push(ObservedViolation {
                    step: self.step,
                    violation,
                });
            }
        }
        self.stats.messages = u64::try_from(self.gmail.messages.len()).unwrap_or(u64::MAX);
        self.stats.threads = u64::try_from(self.gmail.threads().len()).unwrap_or(u64::MAX);
        let result = RunResult {
            seed,
            faults,
            steps: fault_steps,
            violations: self.violations,
            convergence,
            stats: self.stats,
        };
        (result, self.matched)
    }

    fn fault_step(&mut self) {
        if self.rng.chance(self.faults.user_activity) {
            self.user_op();
        }
        if self.rng.chance(self.faults.history_expired) {
            // Everything before the current position expires; Gmail never
            // rejects the history id it reports as current.
            self.gmail.expired_before = self.gmail.last_history;
            self.stats.expiries += 1;
        }
        if self.rng.chance(self.faults.crash) {
            self.rebuild();
        }
        if self.rng.chance(self.faults.facts_refresh) {
            self.refresh_facts();
        }
        self.advance();
        self.read_feed();
        self.compact();
    }

    /// The feed readers that have not stalled take their page for this step.
    fn read_feed(&mut self) {
        for reader in [&mut self.reader, &mut self.stalled] {
            if reader.is_stalled(self.step) {
                continue;
            }
            if let Err(violation) = reader.take_page(&mut self.mirror, self.now, &mut self.stats) {
                self.violations.push(ObservedViolation {
                    step: self.step,
                    violation,
                });
            }
        }
    }

    /// Now and then, compact the log as the host would: usually as far as
    /// the readers allow, sometimes with an explicit bound too.
    fn compact(&mut self) {
        if !self
            .compactor
            .chance(Probability::new(COMPACTION).unwrap_or(Probability::NEVER))
        {
            return;
        }
        let mut retention = Retention::new(self.now);
        retention.max_idle = MAX_IDLE;
        if self.compactor.below(3) == 0 {
            let head = usize::try_from(self.mirror.seq.0).unwrap_or(usize::MAX);
            let through = u64::try_from(self.compactor.below(head.saturating_add(1))).unwrap_or(0);
            retention = retention.through(StateSeq(through));
        }
        self.compact_with(retention);
    }

    /// The host's housekeeping after a quiet spell longer than the idle
    /// limit (a busy run's clock barely moves): the readers that still read
    /// take a page, then the log is compacted as far as they allow.
    fn housekeeping(&mut self) {
        self.now = self.now.plus(MAX_IDLE).plus(Duration::from_mins(1));
        self.read_feed();
        let mut retention = Retention::new(self.now);
        retention.max_idle = MAX_IDLE;
        self.compact_with(retention);
    }

    fn compact_with(&mut self, retention: Retention) {
        let before = self.mirror.clone();
        let removed = self.mirror.compact(&retention);
        let removed_entries = usize::try_from(removed).unwrap_or(usize::MAX);
        self.compacted
            .extend(before.log.iter().take(removed_entries).cloned());
        self.stats.compactions += 1;
        self.stats.compacted += removed;
        self.note(&format!("compact {retention:?}: {removed} removed"));
        for violation in invariants::check_compaction(&before, &self.mirror, &retention) {
            self.violations.push(ObservedViolation {
                step: self.step,
                violation,
            });
        }
    }

    /// A host's facts refresh, as `Store::refresh_facts` runs it: unfenced,
    /// between the engine's commits. It brings rows imported without current
    /// facts up to date, and also sends updates that must not apply (for a
    /// deleted message, another blob, a stale size).
    fn refresh_facts(&mut self) {
        let mut updates: Vec<FactsUpdate> = self
            .mirror
            .messages
            .values()
            .filter_map(|record| match &record.content {
                Content::Imported(stored)
                    if stored
                        .facts
                        .as_ref()
                        .is_none_or(|facts| facts.version < FACTS_VERSION) =>
                {
                    Some(FactsUpdate {
                        id: record.id.clone(),
                        blob: stored.blob.clone(),
                        read_size: stored.size,
                        size: stored.size,
                        facts: MessageFacts::empty(),
                    })
                }
                Content::Imported(_) | Content::Pending => None,
            })
            .collect();
        let stale = self.mirror.tombstones.keys().take(2).map(|id| FactsUpdate {
            id: id.clone(),
            blob: BlobRef(format!("{}/{}", self.account.as_str(), id.0)),
            read_size: 1_000,
            size: 1_000,
            facts: MessageFacts::empty(),
        });
        let elsewhere = self.mirror.messages.keys().take(1).map(|id| FactsUpdate {
            id: id.clone(),
            blob: BlobRef("elsewhere".into()),
            read_size: 1_000,
            size: 1_000,
            facts: MessageFacts::empty(),
        });
        updates.extend(stale.chain(elsewhere).collect::<Vec<_>>());
        let before = self.mirror.clone();
        let applied = self.mirror.refresh_facts(&updates);
        self.note(&format!("facts refresh: {applied} of {}", updates.len()));
        self.stats.facts_refreshed += u64::from(applied);
        for violation in invariants::check_refresh(&before, &self.mirror) {
            self.violations.push(ObservedViolation {
                step: self.step,
                violation,
            });
        }
    }

    fn user_op(&mut self) {
        let op = match &mut self.script {
            Some(script) => {
                if script.is_empty() {
                    return;
                }
                script.remove(0)
            }
            None => self.random_op(),
        };
        self.stats.user_ops += 1;
        if let UserOp::HostAction {
            pick,
            action,
            whole_thread,
        } = op
        {
            self.host_action(pick, action, whole_thread);
            return;
        }
        if let UserOp::HostOpen { pick } = op {
            self.host_open(pick);
            return;
        }
        if let UserOp::HostPurge { pick, whole_thread } = op {
            self.host_purge(pick, whole_thread);
            return;
        }
        if let UserOp::HostEmpty { spam } = op {
            self.host_empty(spam);
            return;
        }
        if op == UserOp::MonthPasses {
            self.now = self.now.plus(Duration::from_hours(31 * 24));
            // Gmail keeps history about a week: nothing from before is
            // available any more.
            self.gmail.expired_before = self.gmail.last_history;
            self.stats.expiries += 1;
            return;
        }
        let op = match op {
            // Only a label the mirror defined is deleted without a record.
            UserOp::DeleteLabelQuietly { pick } => match self.gmail.user_label(pick) {
                Some(id) if self.mirror.labels.iter().any(|label| label.id == id) => {
                    self.stats.quiet_label_deletes += 1;
                    op
                }
                Some(_) | None => UserOp::DeleteLabel { pick },
            },
            other => other,
        };
        if matches!(
            op,
            UserOp::CreateLabel
                | UserOp::RenameLabel { .. }
                | UserOp::DeleteLabel { .. }
                | UserOp::DeleteLabelQuietly { .. }
                | UserOp::HostCreateLabel
                | UserOp::HostRenameLabel { .. }
                | UserOp::HostDeleteLabel { .. }
        ) {
            self.stats.label_edits += 1;
        }
        // The label a host write names, before Gmail applies it.
        let target = match &op {
            UserOp::HostCreateLabel => {
                Some(LabelId(format!("Label_{}", self.gmail.next_label + 1)))
            }
            UserOp::HostRenameLabel { pick } | UserOp::HostDeleteLabel { pick } => {
                self.gmail.user_label(*pick)
            }
            _ => None,
        };
        self.gmail.apply(&op);
        // What the driver records of Gmail's answer.
        let record = match (&op, target) {
            (UserOp::HostCreateLabel | UserOp::HostRenameLabel { .. }, Some(id)) => {
                self.gmail.user_label_definition(&id).map(LabelOp::Upsert)
            }
            (UserOp::HostDeleteLabel { .. }, Some(id)) => {
                Some(LabelOp::Remove { id, at: self.now })
            }
            _ => None,
        };
        if let Some(record) = record {
            self.record_label(&record);
        }
        if !self.rng.chance(self.faults.push_dropped) {
            let push = Event {
                now: self.now,
                kind: EventKind::PushReceived {
                    history_id: self.gmail.current(),
                },
            };
            if self.rng.chance(self.faults.push_duplicated) {
                self.deliveries.push(push.clone());
            }
            self.deliveries.push(push);
        }
    }

    /// The driver records Gmail's answer to a label write, outside the
    /// engine, unless the host crashes between the answer and the record
    /// (the next listing then brings the change).
    fn record_label(&mut self, op: &LabelOp) {
        if self.rng.chance(self.faults.crash) {
            self.stats.label_writes_lost += 1;
            return;
        }
        self.stats.label_writes_recorded += 1;
        self.note(&format!("record {op:?}"));
        self.mirror.record_label(op);
        for violation in invariants::check_state(&self.mirror) {
            self.violations.push(ObservedViolation {
                step: self.step,
                violation,
            });
        }
    }

    /// After a commit carrying a label listing: no row lost a label its
    /// message still has in Gmail (an empty or truncated listing proves no
    /// deletion); count fenced listings and expired memory.
    fn check_listing(&mut self, before: &AccountMirror, commit: &Commit, outcome: &CommitOutcome) {
        let mut any = false;
        for op in &commit.labels {
            if let LabelOp::ReplaceAll { generation, .. } = op {
                any = true;
                if *generation != before.record.label_generation {
                    self.stats.fenced_listings += 1;
                }
            }
        }
        if !any || !matches!(outcome, CommitOutcome::Applied { .. }) {
            return;
        }
        let expired = before
            .record
            .deleted_labels
            .keys()
            .filter(|id| {
                !self.mirror.record.deleted_labels.contains_key(*id)
                    && !self.mirror.labels.iter().any(|label| &label.id == *id)
            })
            .count();
        self.stats.forgotten_deletions += u64::try_from(expired).unwrap_or(u64::MAX);
        let mut stripped = Vec::new();
        for (id, row) in &before.messages {
            let (Some(after), Some(gmail)) =
                (self.mirror.messages.get(id), self.gmail.messages.get(id))
            else {
                continue;
            };
            for label in row.labels.difference(&after.labels) {
                if gmail.labels.contains(label) {
                    stripped.push(Violation::LiveLabelStripped {
                        message: id.clone(),
                        label: label.clone(),
                    });
                }
            }
        }
        for violation in stripped {
            self.violations.push(ObservedViolation {
                step: self.step,
                violation,
            });
        }
    }

    fn host_action(&mut self, pick: usize, action: Action, whole_thread: bool) {
        let visible: Vec<(&MessageId, &ThreadId)> = self
            .mirror
            .messages
            .values()
            .map(|record| (&record.id, &record.thread_id))
            .collect();
        if visible.is_empty() {
            return;
        }
        let (id, thread) = visible[pick % visible.len()];
        let request = if whole_thread {
            ActionRequest::Thread {
                id: thread.clone(),
                action,
            }
        } else {
            ActionRequest::Messages {
                ids: vec![id.clone()],
                action,
            }
        };
        self.deliver(EventKind::ActionRequested { action: request });
    }

    /// Every tombstone is for a message Gmail no longer has.
    fn check_tombstones(&mut self) {
        let gmail = &self.gmail.messages;
        for violation in invariants::check_tombstones(&self.mirror, |id| gmail.contains_key(id)) {
            self.violations.push(ObservedViolation {
                step: self.step,
                violation,
            });
        }
    }

    /// The host purges the n-th mirrored message (or its thread).
    fn host_purge(&mut self, pick: usize, whole_thread: bool) {
        let visible: Vec<(MessageId, ThreadId)> = self
            .mirror
            .messages
            .values()
            .map(|record| (record.id.clone(), record.thread_id.clone()))
            .collect();
        if visible.is_empty() {
            return;
        }
        let (id, thread) = visible[pick % visible.len()].clone();
        let targets = if whole_thread {
            match self.mirror.thread(&thread) {
                Some(view) => purge::thread_targets(&view.messages),
                None => return,
            }
        } else {
            vec![id]
        };
        if let Some(mut scan) = self.purge_check() {
            self.purge_round(&targets, Scope::TrashOrSpam, &mut scan);
        }
    }

    /// The host empties the trash or spam, as far as the mirror holds it:
    /// a few ids at a time, each chunk judged on the rolling history read,
    /// with another client acting between chunks.
    fn host_empty(&mut self, spam: bool) {
        const CHUNK: u32 = 8;
        let bin = if spam { Bin::Spam } else { Bin::Trash };
        let Some(mut scan) = self.purge_check() else {
            return;
        };
        let mut after: Option<MessageId> = None;
        loop {
            let page = self
                .mirror
                .message_ids_labelled(&bin.label(), after.as_ref(), CHUNK);
            let Some(last) = page.last() else {
                return;
            };
            if after.is_some() {
                self.elsewhere();
            }
            after = Some(last.clone());
            if !self.purge_round(&page, Scope::Bin(bin), &mut scan) {
                return;
            }
        }
    }

    /// Another client may change Gmail (the mirror has not seen it).
    fn elsewhere(&mut self) {
        if self.rng.chance(self.faults.user_activity) {
            let op = self.random_op();
            if !matches!(
                op,
                UserOp::HostAction { .. }
                    | UserOp::HostOpen { .. }
                    | UserOp::HostPurge { .. }
                    | UserOp::HostEmpty { .. }
            ) {
                self.gmail.apply(&op);
            }
        }
    }

    /// Set up a private history scan for a live account.
    fn purge_check(&mut self) -> Option<PurgeScan> {
        self.mirror.record.cursor?;
        if self.mirror.record.sync != mailtide::state::SyncState::Live {
            return None;
        }
        self.elsewhere();
        self.stats.purges += 1;
        Some(PurgeScan::new())
    }

    fn purge_round(&mut self, targets: &[MessageId], scope: Scope, scan: &mut PurgeScan) -> bool {
        self.purge_round_with(targets, scope, scan, None, |sim, point| {
            if point == PurgePoint::DuringSingle {
                sim.elsewhere();
            }
        })
    }

    /// A fresh pre-check and a confirmation per wire request. The hook
    /// lets tests put a mailbox change on either side of the last check.
    fn purge_round_with(
        &mut self,
        targets: &[MessageId],
        scope: Scope,
        scan: &mut PurgeScan,
        fixed_fault: Option<PurgeFault>,
        mut activity: impl FnMut(&mut Self, PurgePoint),
    ) -> bool {
        let mut pending = targets.to_vec();
        while !pending.is_empty() {
            let Some(head) = scan.head().or(self.mirror.record.cursor) else {
                return false;
            };
            if !self.read_history(head, scan, &[]) {
                return false;
            }
            let mut gone = Vec::new();
            pending.retain(
                |id| match scan.verdict(id, self.mirror.view(id).as_ref(), scope) {
                    Verdict::Eligible => true,
                    Verdict::Gone { .. } => {
                        gone.push(id.clone());
                        false
                    }
                    Verdict::Refused(_) => false,
                },
            );
            self.record_purge_evidence(&gone, scan);
            let Some(request) = purge::delete_requests(&pending).into_iter().next() else {
                break;
            };
            let ids = purge::request_ids(&request);
            pending.drain(..ids.len());
            let single = matches!(request, GmailRequest::DeleteMessage { .. });
            let fault = fixed_fault.unwrap_or_else(|| self.purge_fault(single));
            if fault == PurgeFault::Deferred {
                return false;
            }
            if fault == PurgeFault::Token {
                self.stats.purge_token_failures += 1;
                return false;
            }
            if !single {
                self.stats.purge_batches += 1;
            }
            activity(self, PurgePoint::AfterCheck);
            let applied = match fault {
                PurgeFault::Missing | PurgeFault::Server(false) => &[][..],
                PurgeFault::Partial => {
                    self.stats.purge_partial_refusals += 1;
                    &ids[..ids.len() / 2]
                }
                _ => ids.as_slice(),
            };
            for id in applied {
                self.delete_checked(id, scope);
            }
            if single && !pending.is_empty() {
                activity(self, PurgePoint::DuringSingle);
                let restored = pending
                    .iter()
                    .filter(|id| {
                        self.gmail
                            .messages
                            .get(*id)
                            .is_some_and(|message| !labels_in_scope(&message.labels, scope))
                    })
                    .count();
                self.stats.purge_restores_between_requests += u64::try_from(restored).unwrap_or(0);
            }
            if !self.purge_rng.chance(self.faults.push_dropped) {
                self.deliveries.push(Event {
                    now: self.now,
                    kind: EventKind::PushReceived {
                        history_id: self.gmail.current(),
                    },
                });
            }
            if fault == PurgeFault::Release {
                self.stats.purge_release_failures += 1;
            }
            if self.purge_rng.chance(self.faults.crash)
                || self.purge_rng.chance(self.faults.transient)
            {
                return false;
            }
            let hidden = if fault == PurgeFault::Late {
                self.stats.purge_late_confirmations += 1;
                applied
            } else {
                &[]
            };
            if !self.read_history(scan.head().unwrap_or(head), scan, hidden) {
                return false;
            }
            self.record_purge_evidence(&ids, scan);
            if matches!(fault, PurgeFault::Release | PurgeFault::Server(_)) {
                return false;
            }
        }
        true
    }

    fn purge_fault(&mut self, single: bool) -> PurgeFault {
        if self.purge_rng.chance(self.faults.rate_limited) {
            return PurgeFault::Deferred;
        }
        if !self.purge_rng.chance(self.faults.transient) {
            return PurgeFault::None;
        }
        match self.purge_rng.below(7) {
            0 => PurgeFault::Token,
            1 => PurgeFault::Release,
            2 => PurgeFault::Missing,
            3 if !single => PurgeFault::Partial,
            3 | 4 => PurgeFault::Late,
            5 => PurgeFault::Server(false),
            _ => PurgeFault::Server(true),
        }
    }

    fn record_purge_evidence(&mut self, checked: &[MessageId], scan: &PurgeScan) {
        let evidence: Vec<(MessageId, DeletionEvidence)> = checked
            .iter()
            .filter_map(|id| {
                scan.deleted(id)
                    .map(|at| (id.clone(), DeletionEvidence::History { at }))
            })
            .collect();
        if evidence.is_empty() {
            return;
        }
        let before = self.mirror.record.cursor;
        let recorded = self.mirror.record_deleted(&evidence, self.now);
        self.stats.purged += u64::from(recorded);
        self.note(&format!("purge recorded {evidence:?}"));
        for violation in invariants::check_state(&self.mirror) {
            self.violations.push(ObservedViolation {
                step: self.step,
                violation,
            });
        }
        if before != self.mirror.record.cursor {
            self.violations.push(ObservedViolation {
                step: self.step,
                violation: Violation::CursorMovedWithoutCommit {
                    before,
                    after: self.mirror.record.cursor,
                },
            });
        }
        self.check_tombstones();
    }

    /// Gmail deletes `id`, which must be in the bin at that moment.
    fn delete_checked(&mut self, id: &MessageId, scope: Scope) {
        let Some(message) = self.gmail.messages.get(id) else {
            return;
        };
        if !labels_in_scope(&message.labels, scope) {
            self.violations.push(ObservedViolation {
                step: self.step,
                violation: Violation::PurgedOutsideTrashOrSpam {
                    message: id.clone(),
                },
            });
        }
        self.gmail.delete(id);
        self.stats.purge_deletes += 1;
    }

    /// Read every history page from `start` into `scan`, at most
    /// [`purge::PURGE_HISTORY_PAGES`]; `false` if history expired or is
    /// too long (the purge is refused as stale).
    fn read_history(&self, start: HistoryId, scan: &mut PurgeScan, hidden: &[MessageId]) -> bool {
        let mut token: Option<PageToken> = None;
        for _ in 0..purge::PURGE_HISTORY_PAGES {
            let Some(mut page) = self.gmail.history_page(start, token.as_ref()) else {
                return false;
            };
            page.records.retain(|record| {
                !matches!(
                    record,
                    HistoryRecord::MessageDeleted { message_id, .. } if hidden.contains(message_id)
                )
            });
            scan.read(&page);
            match page.next_page_token {
                None => return true,
                next => token = next,
            }
        }
        false
    }

    fn host_open(&mut self, pick: usize) {
        let pending: Vec<&MessageId> = self
            .mirror
            .messages
            .values()
            .filter(|record| record.content == Content::Pending)
            .map(|record| &record.id)
            .collect();
        let visible: Vec<&MessageId> = if pending.is_empty() {
            self.mirror.messages.keys().collect()
        } else {
            pending
        };
        if visible.is_empty() {
            return;
        }
        let id = visible[pick % visible.len()].clone();
        self.stats.opens += 1;
        let before = self.mirror.clone();
        let report = self.mirror.prioritise(&[id], self.now);
        let raised = self
            .mirror
            .foreground_imports()
            .saturating_sub(before.foreground_imports());
        self.stats.raised += u64::try_from(raised).unwrap_or(u64::MAX);
        self.note(&format!("open {report:?}"));
        for violation in invariants::check_prioritise(&before, &self.mirror) {
            self.violations.push(ObservedViolation {
                step: self.step,
                violation,
            });
        }
    }

    fn random_action(&mut self) -> Action {
        match self.rng.below(11) {
            0 => Action::SetRead(ReadState::Read),
            1 => Action::SetRead(ReadState::Unread),
            2 => Action::SetStar(StarState::Starred),
            3 => Action::SetStar(StarState::Unstarred),
            4 => Action::Archive,
            5 => Action::MoveToInbox,
            6 => Action::MoveToTrash,
            7 => Action::RestoreFromTrash(RestoreDestination::Inbox),
            8 => Action::MarkSpam,
            9 => Action::MarkNotSpam(RestoreDestination::Archive),
            _ => Action::ChangeLabels(
                LabelDelta::new(
                    vec![LabelId("Label_1".into())],
                    vec![LabelId("Label_2".into())],
                )
                .expect("a valid change"),
            ),
        }
    }

    fn random_op(&mut self) -> UserOp {
        let pick = self.rng.below(1_000);
        if self.rng.below(4) == 0 {
            match self.rng.below(12) {
                0..=3 => return UserOp::HostOpen { pick },
                4 => {
                    return UserOp::HostPurge {
                        pick,
                        whole_thread: self.rng.below(3) == 0,
                    };
                }
                5 => {
                    return UserOp::HostEmpty {
                        spam: self.rng.below(2) == 0,
                    };
                }
                _ => {}
            }
            let action = self.random_action();
            return UserOp::HostAction {
                pick,
                action,
                whole_thread: self.rng.below(3) == 0,
            };
        }
        match self.rng.below(11) {
            0..=3 => UserOp::Receive {
                thread: pick.is_multiple_of(2).then_some(pick),
            },
            4 => UserOp::SaveDraft,
            5..=7 => {
                let labels = self.gmail.assignable();
                UserOp::Relabel {
                    pick,
                    label: labels[self.rng.below(labels.len())].clone(),
                }
            }
            8 => UserOp::Delete { pick },
            _ => match self.rng.below(8) {
                0 => UserOp::CreateLabel,
                1 => UserOp::RenameLabel { pick },
                2 => UserOp::DeleteLabel { pick },
                3 => UserOp::DeleteLabelQuietly { pick },
                4 => UserOp::HostCreateLabel,
                5 => UserOp::HostRenameLabel { pick },
                6 => UserOp::HostDeleteLabel { pick },
                _ => UserOp::MonthPasses,
            },
        }
    }

    /// Do one thing: deliver a result, execute a command, or let time pass.
    fn advance(&mut self) {
        let total = self.pending.len() + self.deliveries.len();
        if total == 0 {
            self.fire_next_timer();
            return;
        }
        let pick = self.rng.below(total);
        if pick < self.deliveries.len() {
            let event = self.deliveries.remove(pick);
            self.note(&format!("event {:?}", event.kind));
            let commands = self.engine.handle(event);
            self.pending.extend(commands);
            if self.engine.stopped().is_some() {
                self.rebuild();
            }
        } else {
            let command = self.pending.remove(pick - self.deliveries.len());
            self.note(&format!("command {:?}", command.kind));
            self.execute(command);
        }
    }

    fn fire_next_timer(&mut self) {
        let Some((&timer, &at)) = self.timers.iter().min_by_key(|(_, at)| **at) else {
            // Nothing scheduled: poll, as a host's safety net would.
            self.now = self.now.plus(Duration::from_mins(1));
            self.deliveries.push(Event {
                now: self.now,
                kind: EventKind::TimerFired {
                    timer: TimerKind::Poll,
                },
            });
            return;
        };
        self.timers.remove(&timer);
        self.now = self.now.max(at);
        self.deliveries.push(Event {
            now: self.now,
            kind: EventKind::TimerFired { timer },
        });
    }

    fn rebuild(&mut self) {
        self.stats.rebuilds += 1;
        self.timers.clear();
        self.engine = build_engine(&mut self.mirror);
        let start = self.engine.start(self.now);
        self.pending.extend(start);
    }

    fn deliver(&mut self, kind: EventKind) {
        self.deliveries.push(Event {
            now: self.now,
            kind,
        });
    }

    fn execute(&mut self, command: Command) {
        let id = command.id;
        match command.kind {
            CommandKind::Gmail(call) => {
                let failure = if self.rng.chance(self.faults.transient) {
                    Some((Retryable::Transient, None))
                } else if self.rng.chance(self.faults.rate_limited) {
                    Some((Retryable::AccountRateLimited, Some(Duration::from_secs(5))))
                } else {
                    None
                };
                let kind = match failure {
                    Some((class, retry_after)) => EventKind::RequestFailed {
                        command: id,
                        failure: Failure::Retryable(RetryableFailure {
                            class,
                            retry_after,
                            detail: "simulated".into(),
                        }),
                    },
                    None => self.gmail_result(id, &call.request),
                };
                self.deliver(kind);
            }
            CommandKind::Commit(commit) => {
                let before = self.mirror.clone();
                let outcome = self.mirror.apply(&commit);
                self.check_listing(&before, &commit, &outcome);
                for violation in invariants::check_commit(&before, &self.mirror, &commit, &outcome)
                {
                    self.violations.push(ObservedViolation {
                        step: self.step,
                        violation,
                    });
                }
                self.check_tombstones();
                match &outcome {
                    CommitOutcome::Applied { .. } => self.stats.commits += 1,
                    CommitOutcome::Rejected(_) => self.stats.rejections += 1,
                }
                if self.rng.chance(self.faults.lost_commit_ack) {
                    // The host can't tell whether the commit applied: rebuild.
                    self.rebuild();
                    return;
                }
                let kind = match outcome {
                    CommitOutcome::Applied { .. } => EventKind::CommitApplied { commit: commit.id },
                    CommitOutcome::Rejected(reason) => EventKind::CommitRejected {
                        commit: commit.id,
                        reason,
                    },
                };
                self.deliver(kind);
            }
            CommandKind::Schedule { timer, at } => {
                self.timers.insert(timer, at);
            }
            CommandKind::ClaimJobs {
                epoch,
                limit,
                now,
                attempt,
                floor,
                ..
            } => {
                // A claim from an older engine is refused, and the engine is
                // told (it stops as superseded), exactly as the runner does.
                let kind = match self
                    .mirror
                    .claim_jobs_from(epoch, limit, now, attempt, floor)
                {
                    Ok(jobs) => EventKind::JobsClaimed {
                        command: id,
                        jobs,
                        next_due: self.mirror.next_due(),
                    },
                    Err(reason) => EventKind::JobsClaimRejected {
                        command: id,
                        reason,
                    },
                };
                self.deliver(kind);
            }
            CommandKind::DeleteBlob { message, .. } => {
                self.blobs.remove(&message);
                self.deliver(EventKind::BlobDeleted { command: id });
            }
            CommandKind::ReportStatus(_) => {}
        }
    }

    /// `labels.list`, now and then answered without labels or cut short.
    fn list_labels(&mut self, command: CommandId) -> EventKind {
        self.stats.label_listings += 1;
        let mut labels = self.gmail.labels();
        if self.rng.chance(self.faults.bad_listing) {
            self.stats.bad_listings += 1;
            if self.rng.chance(Probability(0.5)) {
                // No labels: the driver refuses to decode it and asks again
                // later.
                return EventKind::RequestFailed {
                    command,
                    failure: Failure::Retryable(RetryableFailure {
                        class: Retryable::Transient,
                        retry_after: None,
                        detail: "label listing without labels".into(),
                    }),
                };
            }
            // Cut short: what it lacks was not deleted.
            labels.truncate(self.rng.below(labels.len()));
        }
        EventKind::LabelsListed {
            command,
            labels,
            generation: self.mirror.record.label_generation,
        }
    }

    /// A delete request: Gmail deletes the ids that exist and answers 2xx
    /// naming none (`purge-batch-delete-partial`, assumed); a single delete
    /// of a missing id is a 404 (`purge-delete-missing`, assumed). The
    /// engine never sends one.
    fn deleted(&mut self, command: CommandId, request: &GmailRequest) -> EventKind {
        if let GmailRequest::DeleteMessage { id } = request
            && !self.gmail.messages.contains_key(id)
        {
            return EventKind::MessageNotFound {
                command,
                message_id: id.clone(),
            };
        }
        let ids = mailtide::purge::request_ids(request);
        for id in &ids {
            self.gmail.delete(id);
        }
        EventKind::MessagesDeleted { command, ids }
    }

    fn gmail_result(&mut self, command: CommandId, request: &GmailRequest) -> EventKind {
        match request {
            GmailRequest::GetProfile => EventKind::ProfileFetched {
                command,
                history_id: self.gmail.current(),
            },
            GmailRequest::ListHistory { start, page_token } => {
                match self.gmail.history_page(*start, page_token.as_ref()) {
                    Some(page) => EventKind::HistoryFetched { command, page },
                    None => EventKind::HistoryExpired { command },
                }
            }
            GmailRequest::ListMessages { scope, page_token } => EventKind::MessagesListed {
                command,
                page: self.gmail.list(scope, page_token.as_ref()),
            },
            GmailRequest::GetMessage { id } => match self.gmail.get(id) {
                Some(message) => EventKind::MessageFetched { command, message },
                None => EventKind::MessageNotFound {
                    command,
                    message_id: id.clone(),
                },
            },
            GmailRequest::ImportMessage { id } => match self.gmail.get(id) {
                Some(observation) => {
                    // The host stores the content before reporting it.
                    self.blobs.insert(id.clone());
                    EventKind::MessageImported {
                        command,
                        message: Box::new(ImportedMessage {
                            blob: BlobRef(format!("{}/{}", self.account.as_str(), id.0)),
                            size: 1_000,
                            internal_date: self.gmail.messages.get(id).map_or(self.now, |m| m.date),
                            headers: HeaderFacts::default(),
                            // Some imports come from a build without facts,
                            // for the refresh to bring up to date.
                            facts: stable_hash(id.0.as_bytes())
                                .is_multiple_of(2)
                                .then(|| Box::new(MessageFacts::empty())),
                            observation,
                        }),
                    }
                }
                None => EventKind::MessageNotFound {
                    command,
                    message_id: id.clone(),
                },
            },
            GmailRequest::ListLabels => self.list_labels(command),
            GmailRequest::Watch => EventKind::WatchRenewed {
                command,
                expiration: self.now.plus(Duration::from_hours(168)),
            },
            GmailRequest::Modify(group) => {
                match self.gmail.modify(group) {
                    Modified::Done => {}
                    Modified::NotFound => {
                        return EventKind::MessageNotFound {
                            command,
                            message_id: group.messages[0].clone(),
                        };
                    }
                    Modified::UnknownLabel => {
                        self.stats.refused_deliveries += 1;
                        return EventKind::RequestFailed {
                            command,
                            failure: Failure::Blocking(BlockingFailure {
                                class: Blocking::Permanent,
                                detail: "400 invalidArgument labelId not found".into(),
                            }),
                        };
                    }
                }
                self.stats.deliveries += 1;
                // Gmail notifies about our own changes too.
                if !self.rng.chance(self.faults.push_dropped) {
                    self.deliver(EventKind::PushReceived {
                        history_id: self.gmail.current(),
                    });
                }
                EventKind::LabelsModified { command }
            }
            // Searches, their label checks and label writes are the
            // driver's direct calls; the engine never issues one, so the
            // simulated Gmail refuses them (a bug would show as a blocked
            // request).
            GmailRequest::Search(_)
            | GmailRequest::GetLabel { .. }
            | GmailRequest::CreateLabel(_)
            | GmailRequest::PatchLabel { .. }
            | GmailRequest::DeleteLabel { .. } => EventKind::RequestFailed {
                command,
                failure: Failure::Blocking(BlockingFailure {
                    class: Blocking::Permanent,
                    detail: "the engine issues no direct call".into(),
                }),
            },
            GmailRequest::DeleteMessage { .. } | GmailRequest::BatchDeleteMessages { .. } => {
                self.deleted(command, request)
            }
            GmailRequest::ListDrafts { .. } => EventKind::DraftsListed {
                command,
                page: mailtide::drafts::DraftPage {
                    drafts: Vec::new(),
                    next_page_token: None,
                },
            },
            // The driver's direct calls: no engine command makes them.
            GmailRequest::GetDraft { .. }
            | GmailRequest::CreateDraft { .. }
            | GmailRequest::UpdateDraft { .. }
            | GmailRequest::DeleteDraft { .. }
            | GmailRequest::SendDraft { .. } => EventKind::RequestFailed {
                command,
                failure: mailtide::retry::Failure::Blocking(mailtide::retry::BlockingFailure {
                    class: mailtide::retry::Blocking::Permanent,
                    detail: "not an engine request".into(),
                }),
            },
        }
    }

    /// Heal all faults and run until the mirror matches Gmail. Label
    /// definitions must match within the refresh interval (plus the longest
    /// backoff a failure before healing can leave).
    fn heal(&mut self) -> Convergence {
        const BUDGET: u64 = 50_000;
        let labels_by = self
            .now
            .plus(EngineConfig::default().label_refresh)
            .plus(Duration::from_mins(2));
        let mut difference = String::from("never idle");
        for healing_step in 0..BUDGET {
            self.step += 1;
            if self.now > labels_by
                && let Some(stale) = self.label_difference()
            {
                return Convergence::NotReached {
                    difference: format!("{stale}, past the refresh interval"),
                    trace: self.trace.iter().cloned().collect(),
                };
            }
            if self.pending.is_empty() && self.deliveries.is_empty() {
                match self.difference() {
                    None => {
                        return Convergence::Reached {
                            after_steps: healing_step,
                        };
                    }
                    Some(found) => difference = found,
                }
            }
            self.advance();
            self.read_feed();
            self.compact();
        }
        Convergence::NotReached {
            difference,
            trace: self.trace.iter().cloned().collect(),
        }
    }

    /// How the mirror's label definitions differ from Gmail's, if they do.
    fn label_difference(&self) -> Option<String> {
        let gmail = self.gmail.labels();
        let mirror = self.mirror.label_definitions();
        (gmail != mirror).then(|| format!("label definitions {mirror:?} != Gmail {gmail:?}"))
    }

    /// How a thread's view differs from the same rule restated over Gmail's
    /// messages (SPEC §4), once every message is imported and every action
    /// settled: members by date then id, the union of the non-draft members'
    /// labels, unread, the newest non-draft member, the counts.
    fn thread_difference(&self) -> Option<String> {
        for (thread, mut members) in self.gmail.threads() {
            members.sort_by_key(|(id, message)| (message.date, *id));
            let non_draft: Vec<&(&MessageId, &GmailMessage)> = members
                .iter()
                .filter(|(_, message)| !labels::is_draft(&message.labels))
                .collect();
            let union: LabelSet = non_draft
                .iter()
                .flat_map(|(_, message)| message.labels.iter().cloned())
                .collect();
            let count = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
            let expected = (
                members
                    .iter()
                    .map(|(id, _)| (*id).clone())
                    .collect::<Vec<_>>(),
                labels::has(&union, labels::system::UNREAD),
                union,
                non_draft.last().map(|(id, _)| (*id).clone()),
                count(non_draft.len()),
                count(members.len() - non_draft.len()),
                0,
            );
            let Some(view) = self.mirror.thread(thread) else {
                return Some(format!("thread {} has no view", thread.0));
            };
            let found = (
                view.messages
                    .iter()
                    .map(|m| m.id.clone())
                    .collect::<Vec<_>>(),
                view.unread,
                view.labels,
                view.latest,
                view.message_count,
                view.draft_count,
                view.awaiting_import,
            );
            if found != expected {
                return Some(format!(
                    "thread {} view {found:?} != Gmail {expected:?}",
                    thread.0
                ));
            }
        }
        None
    }

    /// How the mirror differs from Gmail, if it does.
    fn difference(&self) -> Option<String> {
        if let Some(labels) = self.label_difference() {
            return Some(labels);
        }
        let gmail: BTreeSet<&MessageId> = self.gmail.messages.keys().collect();
        let mirror: BTreeSet<&MessageId> = self.mirror.messages.keys().collect();
        if let Some(missing) = gmail.difference(&mirror).next() {
            return Some(format!("{} is in Gmail but not mirrored", missing.0));
        }
        if let Some(extra) = mirror.difference(&gmail).next() {
            return Some(format!("{} is mirrored but not in Gmail", extra.0));
        }
        // Raw content of deleted messages must be scheduled for cleanup,
        // including content written by an import that crashed before its
        // commit.
        if let Some(orphan) = self.blobs.iter().find(|id| {
            !self.gmail.messages.contains_key(*id)
                && !self
                    .mirror
                    .jobs
                    .contains_key(&JobKind::BlobCleanup((*id).clone()))
        }) {
            return Some(format!(
                "{} has content but no cleanup after deletion",
                orphan.0
            ));
        }
        for (id, message) in &self.gmail.messages {
            let record = &self.mirror.messages[id];
            if record.labels != message.labels {
                return Some(format!(
                    "{} labels {:?} != Gmail {:?}",
                    id.0, record.labels, message.labels
                ));
            }
            if record.thread_id != message.thread {
                return Some(format!(
                    "{} thread {} != Gmail {}",
                    id.0, record.thread_id.0, message.thread.0
                ));
            }
            if !matches!(record.content, Content::Imported(_)) || !self.blobs.contains(id) {
                return Some(format!("{} content not imported", id.0));
            }
        }
        if let Some(thread) = self.thread_difference() {
            return Some(thread);
        }
        if let Some(intent) = self
            .mirror
            .intents
            .values()
            .find(|intent| !matches!(intent.state, IntentState::Blocked(_)))
        {
            return Some(format!("intent {intent:?} unsettled"));
        }
        if let Some((kind, _)) = self.mirror.jobs.iter().find(|(kind, job)| {
            !matches!(kind, JobKind::BlobCleanup(_)) && !matches!(job.state, JobState::Blocked(_))
        }) {
            return Some(format!("job {kind:?} still pending"));
        }
        None
    }
}

fn labels_in_scope(labels: &LabelSet, scope: Scope) -> bool {
    match scope {
        Scope::Bin(bin) => labels.contains(&bin.label()),
        Scope::TrashOrSpam => {
            labels::has(labels, labels::system::TRASH) || labels::has(labels, labels::system::SPAM)
        }
        Scope::Anywhere => true,
    }
}

fn build_engine(mirror: &mut AccountMirror) -> Engine {
    let snapshot = mirror
        .claim(ENGINE_VERSION)
        .expect("the simulator's engine version is current");
    let snapshot = snapshot
        .validate()
        .expect("the store only holds valid snapshots");
    Engine::new(snapshot, EngineConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mailtide::engine::JOB_CONCURRENCY;
    use mailtide::quota::Priority;

    #[test]
    fn probabilities_must_be_valid() {
        assert!(Probability::new(0.5).is_ok());
        assert!(Probability::new(-0.1).is_err());
        assert!(Probability::new(1.5).is_err());
        assert!(Probability::new(f64::NAN).is_err());
    }

    #[test]
    fn runs_are_reproducible_from_their_seed() {
        assert_eq!(run(7, 300), run(7, 300));
    }

    #[test]
    fn a_quiet_mailbox_bootstraps_and_converges() {
        let result = replay(1, &[]);
        assert!(result.passed(), "{result:#?}");
    }

    #[test]
    #[ignore = "debugging aid: prints the first failing seed's trace"]
    fn print_first_failure() {
        if let Some(result) = (0..200).map(|seed| run(seed, 400)).find(|r| !r.passed()) {
            println!("seed {} stats {:?}", result.seed, result.stats);
            if let Convergence::NotReached { difference, trace } = &result.convergence {
                println!("difference: {difference}");
                for line in trace {
                    println!("  {line}");
                }
            }
            for violation in &result.violations {
                println!("violation {violation:?}");
            }
        }
    }

    #[test]
    fn opening_messages_never_breaks_the_invariants() {
        // Other seeds than the general campaign: opens happen in every
        // phase, beside crashes, expiries and rate limits.
        let results: Vec<RunResult> = (200..400).map(|seed| run(seed, 400)).collect();
        let failures: Vec<&RunResult> = results.iter().filter(|r| !r.passed()).collect();
        assert!(
            failures.is_empty(),
            "{} failing seeds, first: {:#?}",
            failures.len(),
            failures.first()
        );
        let opens: u64 = results.iter().map(|r| r.stats.opens).sum();
        let raised: u64 = results.iter().map(|r| r.stats.raised).sum();
        assert!(
            opens > 0 && raised > 0,
            "{opens} opens raised {raised} imports"
        );
    }

    #[test]
    fn purges_never_tombstone_a_message_gmail_still_has() {
        // Every delete must find its message in the bin, every tombstone
        // must have lost its Gmail message, and the mirror must converge.
        let results: Vec<RunResult> = (400..600).map(|seed| run(seed, 400)).collect();
        let failures: Vec<&RunResult> = results.iter().filter(|r| !r.passed()).collect();
        assert!(
            failures.is_empty(),
            "{} failing seeds, first: {:#?}",
            failures.len(),
            failures.first()
        );
        let calls: u64 = results.iter().map(|r| r.stats.purges).sum();
        let deletes: u64 = results.iter().map(|r| r.stats.purge_deletes).sum();
        let recorded: u64 = results.iter().map(|r| r.stats.purged).sum();
        assert!(
            calls > 0 && deletes > 0 && recorded > 0,
            "{calls} purges deleted {deletes} and recorded {recorded}"
        );
    }

    fn purge_fixture(n: usize) -> (Sim, Vec<MessageId>) {
        let mut sim = Sim::new(Rng(11), Faults::none(), Some(Vec::new()));
        while sim.gmail.messages.len() < n {
            sim.gmail.apply(&UserOp::Receive { thread: None });
        }
        let ids: Vec<_> = sim.gmail.messages.keys().take(n).cloned().collect();
        assert!(matches!(
            sim.gmail.modify(&ModifyGroup {
                messages: ids.clone(),
                add: vec![Bin::Trash.label()],
                remove: vec![LabelId(labels::system::INBOX.into())],
                versions: Vec::new(),
            }),
            Modified::Done
        ));
        let start = sim.engine.start(sim.now);
        sim.pending.extend(start);
        assert!(matches!(sim.heal(), Convergence::Reached { .. }));
        (sim, ids)
    }

    fn restore(sim: &mut Sim, id: &MessageId) {
        assert!(matches!(
            sim.gmail.modify(&ModifyGroup {
                messages: vec![id.clone()],
                add: vec![LabelId(labels::system::INBOX.into())],
                remove: vec![Bin::Trash.label()],
                versions: Vec::new(),
            }),
            Modified::Done
        ));
    }

    #[test]
    fn a_restore_during_one_single_delete_protects_the_next_target() {
        let (mut sim, ids) = purge_fixture(2);
        let mut scan = sim.purge_check().expect("live");
        assert!(
            sim.purge_round_with(&ids, Scope::TrashOrSpam, &mut scan, None, |sim, point| {
                if point == PurgePoint::DuringSingle {
                    restore(sim, &ids[1]);
                }
            },)
        );
        assert_eq!(sim.stats.purge_restores_between_requests, 1);
        assert_eq!(sim.stats.purge_deletes, 1);
        assert!(sim.gmail.messages.contains_key(&ids[1]));
        assert!(sim.violations.is_empty());
    }

    #[test]
    fn a_restore_after_the_last_observation_can_escape_the_check() {
        let (mut sim, ids) = purge_fixture(1);
        let mut scan = sim.purge_check().expect("live");
        assert!(
            sim.purge_round_with(&ids, Scope::TrashOrSpam, &mut scan, None, |sim, point| {
                if point == PurgePoint::AfterCheck {
                    restore(sim, &ids[0]);
                }
            },)
        );
        assert_eq!(sim.stats.purge_deletes, 1);
        assert!(matches!(
            sim.violations.as_slice(),
            [ObservedViolation { violation: Violation::PurgedOutsideTrashOrSpam { message }, .. }]
            if message == &ids[0]
        ));
    }

    #[test]
    fn empty_in_the_model_exercises_batches_and_the_final_small_page() {
        let (mut sim, _) = purge_fixture(9);
        sim.host_empty(false);
        assert_eq!(sim.stats.purge_batches, 1);
        assert_eq!(sim.stats.purge_deletes, 9);
        assert_eq!(sim.stats.purged, 9);
        assert!(sim.violations.is_empty());
    }

    #[test]
    fn purge_faults_keep_evidence_and_application_distinct() {
        for fault in [
            PurgeFault::Token,
            PurgeFault::Release,
            PurgeFault::Partial,
            PurgeFault::Late,
        ] {
            let (mut sim, ids) = purge_fixture(if fault == PurgeFault::Release { 2 } else { 5 });
            let mut scan = sim.purge_check().expect("live");
            let completed =
                sim.purge_round_with(&ids, Scope::TrashOrSpam, &mut scan, Some(fault), |_, _| {});
            let counters = (
                sim.stats.purge_token_failures,
                sim.stats.purge_release_failures,
                sim.stats.purge_partial_refusals,
                sim.stats.purge_late_confirmations,
                sim.stats.purge_deletes,
                sim.stats.purged,
                completed,
            );
            let expected = match fault {
                PurgeFault::Token => (1, 0, 0, 0, 0, 0, false),
                PurgeFault::Release => (0, 1, 0, 0, 1, 1, false),
                PurgeFault::Partial => (0, 0, 1, 0, 2, 2, true),
                PurgeFault::Late => (0, 0, 0, 1, 5, 0, true),
                _ => unreachable!("case specifies a fault"),
            };
            assert_eq!(counters, expected);
            assert!(sim.violations.is_empty());
            assert!(matches!(sim.heal(), Convergence::Reached { .. }));
        }
    }

    #[test]
    fn an_opened_message_is_imported_within_a_bounded_number_of_imports() {
        const MESSAGES: usize = 200;
        let mut sim = Sim::new(Rng(11), Faults::none(), Some(Vec::new()));
        while sim.gmail.messages.len() < MESSAGES {
            sim.gmail.apply(&UserOp::Receive { thread: None });
        }
        let start = sim.engine.start(sim.now);
        sim.pending.extend(start);
        let advance = |sim: &mut Sim| {
            sim.step += 1;
            assert!(sim.step < 100_000, "the simulation stalled");
            sim.advance();
        };
        while sim.mirror.messages.len() < MESSAGES {
            advance(&mut sim);
        }
        let imported = |sim: &Sim| {
            sim.mirror
                .messages
                .values()
                .filter(|r| matches!(r.content, Content::Imported(_)))
                .count()
        };
        // The simulator's clock stands still while listing, so queued
        // imports tie on due time and run in id order: open the one the
        // backlog would import last.
        let backlog: Vec<(Timestamp, &MessageId)> = sim
            .mirror
            .jobs
            .values()
            .filter_map(|job| match (&job.kind, &job.state, job.priority) {
                (JobKind::Import(id), JobState::Ready { due_at }, Priority::Backfill) => {
                    Some((*due_at, id))
                }
                _ => None,
            })
            .collect();
        assert!(backlog.len() > 50, "{} queued at Backfill", backlog.len());
        let opened = backlog
            .iter()
            .max()
            .map(|(_, id)| (*id).clone())
            .expect("a backlog");
        let report = sim
            .mirror
            .prioritise(std::slice::from_ref(&opened), sim.now);
        assert_eq!(report.queued, vec![opened.clone()]);
        let before = imported(&sim);
        while sim.mirror.messages[&opened].content == Content::Pending {
            advance(&mut sim);
        }
        let others = imported(&sim) - before - 1;
        assert!(
            others <= JOB_CONCURRENCY as usize,
            "{others} other imports completed first"
        );
    }

    #[test]
    fn a_feed_reader_with_random_limits_sees_the_whole_log() {
        let results: Vec<RunResult> = (0..100).map(|seed| run(seed, 400)).collect();
        let diverged: Vec<_> = results
            .iter()
            .flat_map(|result| &result.violations)
            .filter(|v| matches!(v.violation, Violation::FeedPagesDiverged { .. }))
            .collect();
        assert!(diverged.is_empty(), "{diverged:#?}");
        let split = results
            .iter()
            .filter(|r| r.stats.feed_pages_split > 0)
            .count();
        assert!(
            split > results.len() / 2,
            "most runs' readers page inside commits ({split} did)"
        );
        // A scripted run with crashes, rebuilds and admissions too.
        let script: Vec<UserOp> = (0..40)
            .map(|n| match n % 4 {
                0 | 1 => UserOp::Receive { thread: None },
                2 => UserOp::HostAction {
                    pick: n,
                    action: Action::Archive,
                    whole_thread: false,
                },
                _ => UserOp::Delete { pick: n },
            })
            .collect();
        let result = replay(3, &script);
        assert!(result.passed(), "{result:#?}");
    }

    /// Violations of the consumer rules: a live reader cut off, or a
    /// compaction that kept what every live reader had passed.
    fn consumer_violations(results: &[RunResult]) -> Vec<&ObservedViolation> {
        results
            .iter()
            .flat_map(|result| &result.violations)
            .filter(|v| {
                matches!(
                    v.violation,
                    Violation::ConsumerCutOff { .. }
                        | Violation::AcknowledgedEntriesKept { .. }
                        | Violation::FeedPagesDiverged { .. }
                )
            })
            .collect()
    }

    #[test]
    fn a_steady_consumer_is_never_cut_off() {
        let results: Vec<RunResult> = (400..500).map(|seed| run(seed, 400)).collect();
        let violations = consumer_violations(&results);
        assert!(violations.is_empty(), "{violations:#?}");
        let compacting = results.iter().filter(|r| r.stats.compacted > 0).count();
        assert!(
            compacting == results.len(),
            "every run compacts around its readers ({compacting} did)"
        );
    }

    #[test]
    fn a_stalled_consumer_is_cut_off_and_the_log_shrinks() {
        let results: Vec<RunResult> = (500..600).map(|seed| run(seed, 400)).collect();
        let violations = consumer_violations(&results);
        assert!(violations.is_empty(), "{violations:#?}");
        // The stalled reader stops by step 200 and falls behind; the
        // housekeeping after a quiet spell past the idle limit cuts it off,
        // and its read is then too old (checked as each run finishes).
        let cut_off = results
            .iter()
            .filter(|r| r.stats.consumers_cut_off == 1)
            .count();
        assert!(
            cut_off > results.len() * 9 / 10,
            "most runs cut the stalled reader off ({cut_off} did)"
        );
        assert!(results.iter().all(|r| r.stats.consumers_cut_off <= 1));
        assert!(
            results
                .iter()
                .filter(|r| r.stats.consumers_cut_off == 1)
                .all(|r| r.stats.compacted > 0),
            "what it held was removed"
        );
    }

    #[test]
    fn converged_mirrors_group_messages_as_gmail_does() {
        // A scripted mailbox: replies in one thread, a draft, a deleted
        // member and actions on whole threads.
        let script = [
            UserOp::Receive { thread: Some(0) },
            UserOp::Receive { thread: Some(0) },
            UserOp::SaveDraft,
            UserOp::Receive { thread: Some(1) },
            UserOp::Delete { pick: 2 },
            UserOp::HostAction {
                pick: 0,
                action: Action::SetRead(ReadState::Read),
                whole_thread: true,
            },
            UserOp::HostAction {
                pick: 3,
                action: Action::MarkSpam,
                whole_thread: true,
            },
            UserOp::Receive { thread: Some(0) },
        ];
        for seed in 0..50 {
            let result = replay(seed, &script);
            assert!(result.passed(), "{result:#?}");
        }
        // Random mailboxes: convergence compares every thread id and every
        // thread's view with Gmail's grouping.
        let results: Vec<RunResult> = (0..100).map(|seed| run(seed, 300)).collect();
        let failures: Vec<&RunResult> = results.iter().filter(|r| !r.passed()).collect();
        assert!(failures.is_empty(), "{:#?}", failures.first());
        let messages: u64 = results.iter().map(|r| r.stats.messages).sum();
        let threads: u64 = results.iter().map(|r| r.stats.threads).sum();
        assert!(
            threads > 0 && messages > threads,
            "threads hold several messages"
        );
        assert!(
            results.iter().any(|r| r.stats.deliveries > 0),
            "actions reached Gmail"
        );
    }

    #[test]
    fn facts_refreshes_interleaved_with_sync_keep_the_invariants() {
        let mut refreshed = 0;
        for seed in 0..100 {
            let mut rng = Rng(seed);
            let faults = Faults {
                facts_refresh: Probability::new(0.3).expect("valid"),
                ..Faults::swarm(&mut rng)
            };
            let result = Sim::new(rng, faults, None).run(seed, 400);
            assert!(result.passed(), "seed {seed}: {result:#?}");
            refreshed += result.stats.facts_refreshed;
        }
        assert!(refreshed > 100, "refreshes applied: {refreshed}");
    }

    #[test]
    fn labels_edited_here_and_elsewhere_converge_under_faults() {
        // Other seeds than the general campaign: labels created, renamed and
        // deleted by the host through the driver (some records lost to a
        // crash) and by another client (some deletions without history),
        // beside actions adding labels Gmail no longer has.
        let results: Vec<RunResult> = (600..800)
            .map(|seed| {
                // Some crashes in every run: a crash between Gmail's answer
                // and the record is one of the paths.
                let mut rng = Rng(seed);
                let faults = Faults {
                    crash: Probability::new(0.02).expect("valid"),
                    ..Faults::swarm(&mut rng)
                };
                Sim::new(rng, faults, None).run(seed, 400)
            })
            .collect();
        let failures: Vec<&RunResult> = results.iter().filter(|r| !r.passed()).collect();
        assert!(
            failures.is_empty(),
            "{} failing seeds, first: {:#?}",
            failures.len(),
            failures.first()
        );
        let total = |stat: fn(&Stats) -> u64| results.iter().map(|r| stat(&r.stats)).sum::<u64>();
        assert!(
            total(|s| s.label_writes_recorded) > 0,
            "host writes recorded"
        );
        assert!(total(|s| s.label_writes_lost) > 0, "host writes lost");
        assert!(
            total(|s| s.quiet_label_deletes) > 0,
            "deletions without history"
        );
        assert!(
            total(|s| s.refused_deliveries) > 0,
            "adds of deleted labels refused"
        );
        assert!(
            total(|s| s.bad_listings) > 0,
            "empty and truncated listings"
        );
        assert!(total(|s| s.fenced_listings) > 0, "stale listings fenced");
        assert!(total(|s| s.forgotten_deletions) > 0, "the memory expired");
        // A script: an action adds a label another client deletes before it
        // is delivered, the host deletes and recreates labels.
        let script = [
            UserOp::Receive { thread: None },
            UserOp::HostAction {
                pick: 0,
                action: Action::ChangeLabels(
                    LabelDelta::new(vec![LabelId("Label_1".into())], Vec::new())
                        .expect("a valid change"),
                ),
                whole_thread: false,
            },
            UserOp::DeleteLabel { pick: 0 },
            UserOp::HostCreateLabel,
            UserOp::HostAction {
                pick: 1,
                action: Action::SetStar(StarState::Starred),
                whole_thread: false,
            },
            UserOp::HostDeleteLabel { pick: 0 },
            UserOp::HostCreateLabel,
            UserOp::DeleteLabelQuietly { pick: 0 },
        ];
        for seed in 0..50 {
            let result = replay(seed, &script);
            assert!(result.passed(), "{result:#?}");
        }
    }

    #[test]
    fn many_seeds_hold_the_invariants_and_converge() {
        let failures: Vec<RunResult> = (0..200)
            .map(|seed| run(seed, 400))
            .filter(|result| !result.passed())
            .collect();
        assert!(
            failures.is_empty(),
            "{} failing seeds, first: {:#?}",
            failures.len(),
            failures.first()
        );
    }
}
