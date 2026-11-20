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
//! while the account is live. Invariants are checked after every commit. A
//! feed reader takes one page of a random limit between steps and must end
//! with the whole change log. Then all faults heal and the mirror must
//! converge to Gmail, its label definitions within the refresh interval.
//!
//! Zero counterpart, by role: `lib/imap/testing/memory-server.ts`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mailtide::EngineConfig;
use mailtide::command::{Command, CommandKind, GmailRequest};
use mailtide::commit::CommitOutcome;
use mailtide::engine::Engine;
use mailtide::event::{Event, EventKind};
use mailtide::gmail::{
    HeaderFacts, HistoryPage, HistoryRecord, ImportedMessage, Label, LabelKind, ListPage,
    MessageObservation, MessageRef,
};
use mailtide::ids::{
    AccountId, BlobRef, CommandId, ENGINE_VERSION, HistoryId, LabelId, MessageId, PageToken,
    ThreadId, Timestamp, splitmix64,
};
use mailtide::invariants::{self, Violation};
use mailtide::jobs::{JobKind, JobState};
use mailtide::labels::{self, LabelSet};
use mailtide::mirror::{AccountMirror, Content};
use mailtide::outbox::{
    Action, ActionRequest, IntentState, LabelDelta, ModifyGroup, ReadState, RestoreDestination,
    StarState,
};
use mailtide::reconcile::ListScope;
use mailtide::retry::{Failure, Retryable, RetryableFailure};
use mailtide::revision::{Change, FeedCursor, StateSeq};
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
    /// The n-th user label is deleted: Gmail removes it from its messages.
    DeleteLabel {
        /// Which label.
        pick: usize,
    },
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
    /// User operations applied.
    pub user_ops: u64,
    /// Label changes the engine delivered to Gmail (user actions in the host).
    pub deliveries: u64,
    /// Label definitions created, renamed or deleted in Gmail.
    pub label_edits: u64,
    /// `labels.list` calls Gmail answered.
    pub label_listings: u64,
    /// Messages users opened in the host.
    pub opens: u64,
    /// Imports those opens raised to the Foreground class.
    pub raised: u64,
    /// Feed pages the reader took that ended inside a commit (one larger
    /// than the page's limit).
    pub feed_pages_split: u64,
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
            },
        );
        id
    }

    /// `messages.modify`: `false` if a message doesn't exist.
    fn modify(&mut self, group: &ModifyGroup) -> bool {
        if group
            .messages
            .iter()
            .any(|id| !self.messages.contains_key(id))
        {
            return false;
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
        true
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
            UserOp::HostAction { .. } | UserOp::HostOpen { .. } => {}
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
            UserOp::DeleteLabel { pick } => {
                if let Some(id) = self.user_label(*pick) {
                    self.delete_label(&id);
                }
            }
        }
    }

    fn user_label(&self, pick: usize) -> Option<LabelId> {
        let ids: Vec<&LabelId> = self.user_labels.keys().collect();
        (!ids.is_empty()).then(|| ids[pick % ids.len()].clone())
    }

    /// Delete a user label: it leaves every message carrying it, and those
    /// removals are recorded as history.
    fn delete_label(&mut self, label: &LabelId) {
        self.user_labels.remove(label);
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

    /// `labels.list`, in id order.
    fn labels(&self) -> Vec<Label> {
        let mut labels: Vec<Label> = LABELS
            .iter()
            .chain(["DRAFT"].iter())
            .map(|id| Label {
                id: LabelId((*id).to_owned()),
                name: (*id).to_owned(),
                kind: LabelKind::System,
            })
            .chain(self.user_labels.iter().map(|(id, name)| Label {
                id: id.clone(),
                name: name.clone(),
                kind: LabelKind::User,
            }))
            .collect();
        mailtide::mirror::sort_labels(&mut labels);
        labels
    }
}

// ------------------------------------------------------------ the feed reader

/// A consumer of the change feed: between steps it takes one page of a
/// random limit from where it stopped, as a host's pipeline would, and at
/// the end its entries must be the log's, entry for entry. It draws from its
/// own generator, so runs take the same steps as without it.
struct FeedReader {
    rng: Rng,
    at: FeedCursor,
    read: Vec<(StateSeq, u32, Change)>,
}

impl FeedReader {
    fn new(rng: &Rng) -> Self {
        Self {
            rng: Rng(splitmix64(rng.0 ^ 0x0fee_d0fe_ed00_f00d)),
            at: StateSeq(0).into(),
            read: Vec::new(),
        }
    }

    /// Take one page: whether more are waiting. A page above its limit, or
    /// one that ends inside a commit it does not fill alone, is a
    /// divergence.
    fn take_page(&mut self, mirror: &AccountMirror, stats: &mut Stats) -> Result<bool, Violation> {
        let limit = u32::try_from(self.rng.below(8) + 1).unwrap_or(1);
        let page = mirror.changes(self.at, limit);
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
        Ok(page.has_more)
    }

    /// Read to the end, then compare with the log, entries numbered within
    /// their commit.
    fn finish(&mut self, mirror: &AccountMirror, stats: &mut Stats) -> Option<Violation> {
        loop {
            match self.take_page(mirror, stats) {
                Ok(true) => {}
                Ok(false) => break,
                Err(violation) => return Some(violation),
            }
        }
        let mut logged = Vec::with_capacity(mirror.log.len());
        let mut position: Option<(StateSeq, u32)> = None;
        for (seq, change) in &mirror.log {
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
            "read {} entries, the log holds {}; first difference at {at}: {:?} != {:?}",
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
        let reader = FeedReader::new(&rng);
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
        if let Some(violation) = self.reader.finish(&self.mirror, &mut self.stats) {
            self.violations.push(ObservedViolation {
                step: self.step,
                violation,
            });
        }
        self.stats.messages = u64::try_from(self.gmail.messages.len()).unwrap_or(u64::MAX);
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
        self.advance();
        self.read_feed();
    }

    /// The feed reader takes its page for this step.
    fn read_feed(&mut self) {
        if let Err(violation) = self.reader.take_page(&self.mirror, &mut self.stats) {
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
        if matches!(
            op,
            UserOp::CreateLabel | UserOp::RenameLabel { .. } | UserOp::DeleteLabel { .. }
        ) {
            self.stats.label_edits += 1;
        }
        self.gmail.apply(&op);
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
            if self.rng.below(3) == 0 {
                return UserOp::HostOpen { pick };
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
            _ => match self.rng.below(3) {
                0 => UserOp::CreateLabel,
                1 => UserOp::RenameLabel { pick },
                _ => UserOp::DeleteLabel { pick },
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
                for violation in invariants::check_commit(&before, &self.mirror, &commit, &outcome)
                {
                    self.violations.push(ObservedViolation {
                        step: self.step,
                        violation,
                    });
                }
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
                            internal_date: self.now,
                            headers: HeaderFacts::default(),
                            observation,
                        }),
                    }
                }
                None => EventKind::MessageNotFound {
                    command,
                    message_id: id.clone(),
                },
            },
            GmailRequest::ListLabels => {
                self.stats.label_listings += 1;
                EventKind::LabelsListed {
                    command,
                    labels: self.gmail.labels(),
                }
            }
            GmailRequest::Watch => EventKind::WatchRenewed {
                command,
                expiration: self.now.plus(Duration::from_hours(168)),
            },
            GmailRequest::Modify(group) => {
                if !self.gmail.modify(group) {
                    return EventKind::MessageNotFound {
                        command,
                        message_id: group.messages[0].clone(),
                    };
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
            if !matches!(record.content, Content::Imported(_)) || !self.blobs.contains(id) {
                return Some(format!("{} content not imported", id.0));
            }
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
