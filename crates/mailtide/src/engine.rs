//! The per-account sync state machine (SPEC §3, §10).
//!
//! Zero counterpart: the `ZeroDriver` Durable Object in `routes/agent/index.ts`,
//! minus its I/O. One engine per account; the host serializes its events.
//!
//! The engine holds account-level state and what it is waiting for. Every
//! durable fact goes through a [`Commit`]; the engine adopts a commit's effect
//! (cursor, lifecycle) only once the store reports it applied. Results for
//! commands of another epoch, or for commands it isn't waiting on, are
//! ignored.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use crate::audit::{
    self, AuditAbandonment, AuditOutcome, AuditProgress, AuditReport, AuditRequest, AuditStep,
};
use crate::command::{Command, CommandKind, GmailCall, GmailRequest};
use crate::commit::{
    AccountOp, Commit, CursorMove, DeletionEvidence, JobPlan, LabelOp, MessageOp, Rejection,
};
use crate::drafts::{DraftOp, DraftPage, DraftReconcile, ListedDraft};
use crate::event::{Event, EventKind};
use crate::gmail::{
    HistoryPage, HistoryRecord, ImportedMessage, Label, ListPage, MessageObservation,
};
use crate::history::{self, CursorUpdate};
use crate::ids::{
    CommandId, CommitId, ENGINE_VERSION, HistoryId, LabelId, MessageId, PageToken, Timestamp,
};
use crate::jobs::{
    AttemptId, ClaimedJob, ClaimedWork, Delivery, JobKind, JobOp, JobOutcome, NewJob,
};
use crate::labels::LabelSet;
use crate::outbox::{ActionRequest, IntentOp};
use crate::quota::{self, Priority};
use crate::reconcile::{ListScope, ReconcileProgress, ReconcileReason, ReconcileStep};
use crate::retry::{Backoff, Blocking, BlockingFailure, Failure, Retryable, RetryableFailure};
use crate::schedule::{self, TimerKind};
use crate::state::{StoreSnapshot, SyncState, ValidSnapshot};
use crate::status::{ImportProgress, Status};

/// Tunables, set by the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    /// Poll interval while the account is active (SPEC §6.2).
    pub poll_active: Duration,
    /// Poll interval while the account is idle.
    pub poll_idle: Duration,
    /// How long without user activity counts as idle.
    pub idle_after: Duration,
    /// Whether this engine keeps a `users.watch` alive.
    pub watch: WatchPolicy,
    /// Renew the watch this long before it expires.
    pub watch_renew_margin: Duration,
    /// Delay before importing a draft's body, since drafts churn on save.
    pub draft_settle: Duration,
    /// After delivering an action, confirm it with a `get` this much later;
    /// until then it overlays (SPEC §7).
    pub confirm_timeout: Duration,
    /// List the label definitions again once they are this old (SPEC §6.6).
    pub label_refresh: Duration,
    /// How long after a draft change seen in history the drafts are listed
    /// (SPEC §7a); changes within it share one listing.
    pub draft_refresh_delay: Duration,
    /// List the drafts at least this often while live, the safety net
    /// under history and listing consistency.
    pub draft_refresh_every: Duration,
    /// How long a send whose answer was lost waits for its sent message
    /// before it ends as unconfirmed.
    pub draft_send_grace: Duration,
    /// Time between mailbox completeness audits (SPEC §6.3).
    pub audit_interval: Duration,
}

/// Whether the engine keeps a Gmail push watch alive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchPolicy {
    /// Call `users.watch` on start and renew it before it expires.
    Renew,
    /// Never call it: changes are found by polling (or the host's own
    /// pushes). No renewal timer exists.
    Off,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            poll_active: Duration::from_mins(1),
            poll_idle: Duration::from_mins(15),
            idle_after: Duration::from_hours(1),
            watch: WatchPolicy::Renew,
            watch_renew_margin: Duration::from_hours(1),
            draft_settle: Duration::from_mins(2),
            confirm_timeout: Duration::from_mins(1),
            label_refresh: Duration::from_mins(15),
            draft_refresh_delay: Duration::from_secs(10),
            draft_refresh_every: Duration::from_mins(15),
            draft_send_grace: Duration::from_hours(1),
            audit_interval: Duration::from_hours(24),
        }
    }
}

/// How many jobs run at once per account. Stays under Gmail's per-user
/// concurrency limit together with history and listing calls.
pub const JOB_CONCURRENCY: u32 = 4;

/// How long a tombstoned message's raw content is kept before cleanup.
pub const BLOB_GRACE: Duration = Duration::from_hours(24);

/// How long Gmail's bandwidth cap pauses an account.
pub const BANDWIDTH_COOLDOWN: Duration = Duration::from_hours(1);

/// What a command was for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Waiting {
    /// A history page of the current pass.
    History,
    /// `labels.list` for reconciliation.
    Labels,
    /// `labels.list` to refresh the definitions (SPEC §6.6).
    LabelRefresh,
    /// A point lookup to decide whether an omitted label was deleted.
    LabelProof(LabelId),
    /// An audit's Gmail page.
    AuditPage,
    /// The store applying an audit step.
    AuditStore,
    /// `getProfile` for the reconciliation baseline.
    Baseline,
    /// A `messages.list` page of the current reconciliation.
    Listing,
    /// `users.watch`.
    Watch,
    /// The job claim.
    Claim,
    /// A claimed job's work.
    Job(ClaimedJob),
}

/// What to adopt once a commit applies.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AfterCommit {
    /// A history page: adopt the cursor, then fetch the next page or finish.
    HistoryPage {
        cursor: Option<HistoryId>,
        next_page: Option<PageToken>,
    },
    /// A lifecycle change during reconciliation: continue it.
    Reconcile,
    /// Reconciliation finished: adopt the cursor and follow history.
    ReconcileDone(HistoryId),
    /// A job finished or was rescheduled: run more.
    Job,
    /// Bookkeeping (watch, cooldown, blocking): nothing to continue.
    Bookkeeping,
    /// A label listing: release the deliveries waiting for it.
    Listing,
    /// A user action was admitted: deliver it.
    Action,
}

/// An in-progress history pass.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoryPass {
    /// The cursor the pass started from (Gmail pages share it).
    start: HistoryId,
    /// The last page token, to detect empty-page loops.
    last_token: Option<PageToken>,
}

/// When to list the label definitions again (SPEC §6.6). Kept in memory but
/// seeded from the persisted `labels_refreshed_at` on start, so whether a
/// refresh is due never depends on how long this engine has run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct LabelRefresh {
    /// When the next refresh is due; `None` while one is in flight.
    due: Option<Timestamp>,
    /// No refresh before this (backoff after a failure); cleared by a listing.
    not_before: Option<Timestamp>,
    /// Refreshes that failed in a row.
    failures: u32,
    /// When the latest request was sent, to throttle rechecks after a refusal.
    last_sent: Option<Timestamp>,
    /// In-flight proof work for one held listing.
    proof: Option<LabelProof>,
    /// Ids the definitions lack, seen since the last listing was sent.
    unknown: LabelSet,
    /// Unknown ids the listing in flight was sent for.
    asking: LabelSet,
    /// Unknown ids a listing sent after they were seen did not return. They
    /// ask for no further refresh (the periodic one still runs), so an id
    /// Gmail never lists can't make the engine list labels over and over.
    unlisted: LabelSet,
}

#[derive(Debug, Default)]
struct Audit {
    due: bool,
    attempted: bool,
    next_run: Option<Timestamp>,
    report: AuditReport,
}

/// A core-complete listing and its bounded, serial point lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LabelProof {
    labels: Vec<Label>,
    generation: u64,
    reconciling: bool,
    retained: LabelSet,
    remaining: VecDeque<LabelId>,
    removed: LabelSet,
}

/// Most point lookups one listing can request.
pub const LABEL_PROOF_LIMIT: usize = 50;

/// Why the engine stopped accepting events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stopped {
    /// Another engine instance or version took over the account, or the
    /// store's cursor moved under this engine.
    Superseded(Rejection),
    /// A permanent failure blocked the account.
    Blocked(BlockingFailure),
}

/// The sync state machine for one Gmail account.
#[derive(Debug)]
pub struct Engine {
    state: StoreSnapshot,
    config: EngineConfig,
    backoff: Backoff,
    next_command: u64,
    next_commit: u64,
    next_attempt: u64,
    waiting: BTreeMap<CommandId, Waiting>,
    after: BTreeMap<CommitId, AfterCommit>,
    pass: Option<HistoryPass>,
    /// Labels Gmail listed, to enumerate once the baseline is captured.
    label_ids: Vec<LabelId>,
    /// The label-refresh schedule.
    labels: LabelRefresh,
    audit: Audit,
    /// Deliveries Gmail refused once while adding a label it may no longer
    /// have, held (their claim kept) until a label listing sent after the
    /// refusal has been applied; `true` once that listing is sent.
    parked: Vec<(ClaimedJob, bool)>,
    /// Intent versions whose refused delivery was given a listing. A new
    /// intent on the same message can wait for a listing of its own.
    relisted: BTreeMap<MessageId, Vec<crate::outbox::Delivery>>,
    label_listing_failures: u32,
    /// When the periodic draft listing is due; `None` until known.
    drafts_due: Option<Timestamp>,
    /// The pages of the draft listing in flight, by its claim.
    drafts_listing: Option<(AttemptId, Vec<ListedDraft>)>,
    /// The highest history id a push or page reported.
    notified: Option<HistoryId>,
    /// Another pass is needed once the current one ends.
    follow_up: bool,
    /// Jobs claimed and not yet finished.
    running_jobs: u32,
    /// Priority classes the quota gate is holding back, until when. Claims
    /// skip them meanwhile: otherwise every claim picks up held-back jobs
    /// only to defer (and commit) each again, once per freed quota slot.
    held_back: BTreeMap<Priority, Timestamp>,
    /// A claim is outstanding.
    claiming: bool,
    /// When the scheduled `Jobs` timer fires, if one is set (a host keeps
    /// one timer per kind, so a later schedule must not replace a sooner one).
    jobs_due: Option<Timestamp>,
    /// History or listing attempts that failed in a row.
    sync_failures: u32,
    /// History expiries in a row, without a successful page in between.
    expiries_in_row: u32,
    /// Don't continue reconciling before this time (repeated expiries).
    reconcile_not_before: Option<Timestamp>,
    cursor_advanced_at: Option<Timestamp>,
    discovered: u64,
    imported: u64,
    stopped: Option<Stopped>,
}

impl Engine {
    /// An engine resuming from a validated store snapshot.
    #[must_use]
    pub fn new(snapshot: ValidSnapshot, config: EngineConfig) -> Self {
        Self {
            state: snapshot.into_inner(),
            config,
            backoff: Backoff::DEFAULT,
            next_command: 0,
            next_commit: 0,
            next_attempt: 0,
            waiting: BTreeMap::new(),
            after: BTreeMap::new(),
            pass: None,
            label_ids: Vec::new(),
            labels: LabelRefresh::default(),
            audit: Audit::default(),
            parked: Vec::new(),
            relisted: BTreeMap::new(),
            label_listing_failures: 0,
            drafts_due: None,
            drafts_listing: None,
            notified: None,
            follow_up: false,
            running_jobs: 0,
            held_back: BTreeMap::new(),
            claiming: false,
            jobs_due: None,
            sync_failures: 0,
            expiries_in_row: 0,
            reconcile_not_before: None,
            cursor_advanced_at: None,
            discovered: 0,
            imported: 0,
            stopped: None,
        }
    }

    /// Why the engine stopped, if it did.
    #[must_use]
    pub fn stopped(&self) -> Option<&Stopped> {
        self.stopped.as_ref()
    }

    /// The epoch this engine claimed.
    #[must_use]
    pub fn epoch(&self) -> crate::ids::Epoch {
        self.state.epoch
    }

    /// When another claim should resume the audit, if this one used its step.
    #[must_use]
    pub fn audit_next_run(&self) -> Option<Timestamp> {
        self.audit.next_run
    }

    /// The first commands after start: schedule timers, resume any
    /// reconciliation, catch up on history.
    #[must_use]
    pub fn start(&mut self, now: Timestamp) -> Vec<Command> {
        if let SyncState::Blocked { reason, .. } = &self.state.sync {
            self.stopped = Some(Stopped::Blocked(reason.clone()));
            return vec![self.status_command()];
        }
        let poll = self.next_poll(now);
        let mut out = vec![self.schedule(TimerKind::Poll, poll)];
        match self.config.watch {
            WatchPolicy::Renew => {
                let renewal = schedule::next_watch_renewal(
                    self.state.watch_expiration,
                    now,
                    self.config.watch_renew_margin,
                );
                out.push(self.schedule(TimerKind::WatchRenewal, renewal));
            }
            WatchPolicy::Off => {}
        }
        out.push(self.schedule_jobs(now));
        out.extend(self.resume_sync(now));
        // On start: the definitions are due if they were listed `label_refresh`
        // ago or never (a reconciliation that lists them anyway counts).
        self.labels.due = Some(schedule::label_refresh_due(
            self.state.labels_refreshed_at,
            now,
            self.config.label_refresh,
        ));
        out.extend(self.pump_labels(now));
        self.drafts_due = Some(schedule::draft_refresh_due(
            self.state.drafts_refreshed_at,
            now,
            self.config.draft_refresh_every,
        ));
        out.extend(self.pump_drafts(now));
        out.extend(self.schedule_audit(now));
        out.push(self.status_command());
        out
    }

    /// Consume one event and return the commands it calls for.
    #[must_use]
    pub fn handle(&mut self, event: Event) -> Vec<Command> {
        if self.stopped.is_some() {
            return Vec::new();
        }
        let now = event.now;
        match event.kind {
            EventKind::PushReceived { history_id } => self.on_push(history_id, now),
            EventKind::TimerFired { timer } => self.on_timer(timer, now),
            EventKind::ProfileFetched {
                command,
                history_id,
            } => self.on_profile(command, history_id, now),
            EventKind::HistoryFetched { command, page } => self.on_history(command, &page, now),
            EventKind::HistoryExpired { command } => {
                if self.waiting.remove(&command) != Some(Waiting::History) {
                    return Vec::new();
                }
                self.pass = None;
                self.history_expired(now)
            }
            EventKind::MessagesListed { command, page } => self.on_listed(command, page, now),
            EventKind::AuditListed { command, ids, next } => {
                self.on_audit_page(command, ids, next, now)
            }
            EventKind::AuditPageMalformed { command } => {
                if self.waiting.remove(&command) == Some(Waiting::AuditPage) {
                    self.abandon_audit(AuditAbandonment::MalformedPage, now)
                } else {
                    Vec::new()
                }
            }
            EventKind::AuditAdvanced { command, outcome } => {
                self.on_audit_advanced(command, outcome, now)
            }
            EventKind::MessageFetched { command, message } => {
                self.on_fetched(command, message, now)
            }
            EventKind::MessageImported { command, message } => {
                self.on_imported(command, *message, now)
            }
            EventKind::MessageNotFound {
                command,
                message_id,
            } => self.on_not_found(command, message_id, now),
            EventKind::LabelsListed {
                command,
                labels,
                generation,
                stored_user_labels,
            } => match self.waiting.remove(&command) {
                Some(waiting @ (Waiting::Labels | Waiting::LabelRefresh)) => {
                    self.on_labels_listed(waiting, labels, generation, stored_user_labels, now)
                }
                Some(_) | None => Vec::new(),
            },
            EventKind::LabelFetched { command, .. } | EventKind::LabelExists { command } => {
                self.on_label_fetched(command, now)
            }
            EventKind::WatchRenewed {
                command,
                expiration,
            } => self.on_watch(command, expiration, now),
            EventKind::CommitApplied { commit } => self.on_applied(commit, now),
            EventKind::CommitRejected { commit, reason } => self.on_rejected(commit, reason, now),
            EventKind::JobsClaimed {
                command,
                jobs,
                next_due,
            } => self.on_claimed(command, jobs, next_due, now),
            EventKind::JobsClaimRejected { command, reason } => {
                if self.waiting.remove(&command) != Some(Waiting::Claim) {
                    return Vec::new();
                }
                // Another engine holds the account: stop.
                self.claiming = false;
                self.stopped = Some(Stopped::Superseded(reason));
                vec![self.status_command()]
            }
            EventKind::BlobDeleted { command } => self.on_blob_deleted(command, now),
            EventKind::RequestFailed { command, failure } => self.on_failed(command, &failure, now),
            EventKind::RequestDeferred { command, until } => self.on_deferred(command, until, now),
            EventKind::ActionRequested { action } => self.on_action(action, now),
            EventKind::LabelsModified { command } => self.on_modified(command, now),
            EventKind::DraftsListed { command, page } => self.on_drafts_listed(command, page, now),
            // Search, label writes, and draft methods are direct driver calls.
            EventKind::Searched { .. }
            | EventKind::LabelSaved { .. }
            | EventKind::LabelNameTaken { .. }
            | EventKind::LabelDeleted { .. }
            | EventKind::MessagesDeleted { .. }
            | EventKind::DraftFetched { .. }
            | EventKind::DraftSaved { .. }
            | EventKind::DraftDeleted { .. }
            | EventKind::DraftSent { .. }
            | EventKind::DraftNotFound { .. }
            | EventKind::MessageSent { .. } => Vec::new(),
        }
    }

    /// The account's current sync status.
    #[must_use]
    pub fn status(&self) -> Status {
        let phase = self.state.sync.phase();
        Status {
            account: self.state.account.clone(),
            phase,
            blocked: match &self.state.sync {
                SyncState::Blocked { reason, .. } => Some(reason.clone()),
                SyncState::Bootstrapping(_) | SyncState::Live | SyncState::Reconciling(_) => None,
            },
            cursor: self.state.cursor,
            cursor_advanced_at: self.cursor_advanced_at,
            cooldown: self.state.cooldown.clone(),
            label_listing_failures: self.label_listing_failures,
            audit: self.audit.report,
            import: self.state.sync.reconcile().map(|progress| ImportProgress {
                discovered: self.discovered,
                imported: self.imported,
                discovery_complete: matches!(progress.step, ReconcileStep::Verifying { .. }),
            }),
        }
    }

    // ------------------------------------------------------------ triggers

    fn on_push(&mut self, history_id: HistoryId, now: Timestamp) -> Vec<Command> {
        self.notified = Some(self.notified.map_or(history_id, |n| n.max(history_id)));
        match self.state.cursor {
            Some(cursor) if history_id > cursor => self.resume_sync(now),
            // Duplicate or stale push, or no cursor yet: nothing to do.
            Some(_) | None => Vec::new(),
        }
    }

    fn on_timer(&mut self, timer: TimerKind, now: Timestamp) -> Vec<Command> {
        match timer {
            TimerKind::Poll => {
                let poll = self.next_poll(now);
                let mut out = vec![self.schedule(TimerKind::Poll, poll)];
                if !self.cooling_down(now) {
                    out.extend(self.resume_sync(now));
                }
                out
            }
            TimerKind::WatchRenewal => match self.config.watch {
                WatchPolicy::Renew => {
                    vec![self.gmail(GmailRequest::Watch, Priority::Live, Waiting::Watch)]
                }
                WatchPolicy::Off => Vec::new(),
            },
            TimerKind::Jobs => {
                if self.jobs_due.is_some_and(|due| due <= now) {
                    self.jobs_due = None;
                }
                self.claim_jobs(now)
            }
            TimerKind::CooldownOver => {
                if self.cooling_down(now) || self.state.cooldown.is_none() {
                    return Vec::new();
                }
                self.state.cooldown = None;
                let mut out = vec![self.commit_bookkeeping(now, vec![AccountOp::ClearCooldown])];
                out.extend(self.resume_sync(now));
                out.extend(self.claim_jobs(now));
                out.extend(self.pump_labels(now));
                out.extend(self.pump_audit(now));
                out
            }
            TimerKind::Labels => self.pump_labels(now),
            TimerKind::Drafts => self.pump_drafts(now),
            TimerKind::Audit => {
                let due = self.audit_due(now);
                if due > now {
                    return self.schedule_audit(now).into_iter().collect();
                }
                self.audit.due = true;
                self.pump_audit(now)
            }
        }
    }

    /// Continue whatever sync work is due: reconciliation, or a history pass.
    fn resume_sync(&mut self, now: Timestamp) -> Vec<Command> {
        // Gmail asked us to slow down: no sync until the cooldown is over,
        // however many pushes arrive (the CooldownOver timer resumes).
        if let Some(cooldown) = self.state.cooldown.as_ref().filter(|c| c.until > now) {
            let until = cooldown.until;
            return vec![self.schedule(TimerKind::CooldownOver, until)];
        }
        if self.busy_syncing() {
            self.follow_up = true;
            return Vec::new();
        }
        match self.state.sync.clone() {
            SyncState::Bootstrapping(progress) | SyncState::Reconciling(progress) => {
                self.continue_reconcile(&progress, now)
            }
            SyncState::Live => self.request_pass(),
            SyncState::Blocked { .. } => Vec::new(),
        }
    }

    fn busy_syncing(&self) -> bool {
        self.pass.is_some()
            || self
                .labels
                .proof
                .as_ref()
                .is_some_and(|proof| proof.reconciling)
            || self.waiting.values().any(|w| match w {
                Waiting::History | Waiting::Labels | Waiting::Baseline | Waiting::Listing => true,
                // A label refresh never holds history back.
                Waiting::LabelRefresh
                | Waiting::LabelProof(_)
                | Waiting::AuditPage
                | Waiting::AuditStore
                | Waiting::Watch
                | Waiting::Claim
                | Waiting::Job(_) => false,
            })
            || self.after.values().any(|a| match a {
                AfterCommit::HistoryPage { .. }
                | AfterCommit::Reconcile
                | AfterCommit::ReconcileDone(_) => true,
                AfterCommit::Job
                | AfterCommit::Bookkeeping
                | AfterCommit::Listing
                | AfterCommit::Action => false,
            })
    }

    // ------------------------------------------------------------ history

    fn request_pass(&mut self) -> Vec<Command> {
        if self.busy_syncing() {
            self.follow_up = true;
            return Vec::new();
        }
        let Some(start) = self.state.cursor else {
            return Vec::new();
        };
        self.pass = Some(HistoryPass {
            start,
            last_token: None,
        });
        self.follow_up = false;
        vec![self.gmail(
            GmailRequest::ListHistory {
                start,
                page_token: None,
            },
            Priority::Live,
            Waiting::History,
        )]
    }

    fn on_history(
        &mut self,
        command: CommandId,
        page: &HistoryPage,
        now: Timestamp,
    ) -> Vec<Command> {
        if self.waiting.remove(&command) != Some(Waiting::History) {
            return Vec::new();
        }
        self.sync_failures = 0;
        self.expiries_in_row = 0;
        let Some(pass) = self.pass.clone() else {
            return Vec::new();
        };
        if page.records.is_empty()
            && history::is_repeated_token(pass.last_token.as_ref(), page.next_page_token.as_ref())
        {
            // An empty-page loop: give up on this pass and poll again later.
            self.pass = None;
            let poll = self.next_poll(now);
            return vec![self.schedule(TimerKind::Poll, poll)];
        }
        self.notified = Some(
            self.notified
                .map_or(page.history_id, |n| n.max(page.history_id)),
        );
        let refresh =
            self.note_label_ids(page.records.iter().filter_map(record_labels).flatten(), now);
        let fold = history::fold_page(page);
        self.discovered += u64::try_from(fold.added).unwrap_or(0);
        let cursor_move = match fold.cursor {
            CursorUpdate::AdvanceTo(to) if Some(to) > self.state.cursor => Some(CursorMove {
                expected: self.state.cursor,
                to,
            }),
            CursorUpdate::AdvanceTo(_) | CursorUpdate::Unchanged => None,
        };
        if let Some(pass) = self.pass.as_mut() {
            pass.last_token.clone_from(&fold.next_page);
        }
        if fold.messages.is_empty() && cursor_move.is_none() {
            let mut out = self.after_history_page(None, fold.next_page, now);
            out.extend(refresh);
            return out;
        }
        let after = AfterCommit::HistoryPage {
            cursor: cursor_move.map(|m| m.to),
            next_page: fold.next_page,
        };
        let mut commit = self.commit(now, Priority::Live);
        commit.cursor = cursor_move;
        let mut out = Vec::new();
        if !fold.messages.is_empty() {
            commit.account_ops = vec![AccountOp::SetLastActivity(now)];
            out.extend(self.note_activity(now));
        }
        commit.messages = fold.messages;
        out.insert(0, self.send_commit(commit, after));
        out.extend(refresh);
        out
    }

    /// The mailbox is active: poll at the active interval from now on.
    fn note_activity(&mut self, now: Timestamp) -> Vec<Command> {
        let was_idle = self
            .state
            .last_activity
            .is_none_or(|t| now.since(t) >= self.config.idle_after);
        self.state.last_activity = Some(now);
        if was_idle {
            let poll = self.next_poll(now);
            vec![self.schedule(TimerKind::Poll, poll)]
        } else {
            Vec::new()
        }
    }

    fn after_history_page(
        &mut self,
        cursor: Option<HistoryId>,
        next_page: Option<PageToken>,
        now: Timestamp,
    ) -> Vec<Command> {
        if let Some(cursor) = cursor {
            self.adopt_cursor(cursor, now);
        }
        let Some(pass) = self.pass.clone() else {
            return Vec::new();
        };
        if let Some(token) = next_page {
            return vec![self.gmail(
                GmailRequest::ListHistory {
                    start: pass.start,
                    page_token: Some(token),
                },
                Priority::Live,
                Waiting::History,
            )];
        }
        self.pass = None;
        let behind = matches!((self.notified, self.state.cursor), (Some(n), Some(c)) if n > c);
        let mut out = vec![self.schedule_jobs(now)];
        if self.follow_up || behind {
            self.follow_up = false;
            out.extend(self.request_pass());
        }
        out
    }

    fn adopt_cursor(&mut self, cursor: HistoryId, now: Timestamp) {
        if Some(cursor) > self.state.cursor {
            self.state.cursor = Some(cursor);
            self.cursor_advanced_at = Some(now);
        }
    }

    // ------------------------------------------------------------ reconciliation

    /// History is gone: reconcile, backing off if it keeps happening, so a
    /// baseline that expires at once can't make reconciliation spin.
    fn history_expired(&mut self, now: Timestamp) -> Vec<Command> {
        self.expiries_in_row += 1;
        if self.expiries_in_row > 1 {
            let failure = RetryableFailure {
                class: Retryable::Transient,
                retry_after: None,
                detail: "history expired again".into(),
            };
            let seed = self.jitter_seed();
            self.reconcile_not_before =
                Some(self.backoff.next(self.expiries_in_row, &failure, now, seed));
        }
        self.begin_reconcile(ReconcileReason::HistoryExpired, now)
    }

    fn begin_reconcile(&mut self, reason: ReconcileReason, now: Timestamp) -> Vec<Command> {
        self.cancel_label_proofs();
        let progress = ReconcileProgress::begin(reason);
        let sync = match (reason, self.state.cursor) {
            (ReconcileReason::Bootstrap, None) => SyncState::Bootstrapping(progress),
            (ReconcileReason::Bootstrap, Some(_))
            | (ReconcileReason::HistoryExpired | ReconcileReason::Requested, _) => {
                SyncState::Reconciling(progress)
            }
        };
        self.change_sync(sync, now)
    }

    /// Commit a lifecycle change; continue reconciling once it applies.
    fn change_sync(&mut self, sync: SyncState, now: Timestamp) -> Vec<Command> {
        self.state.sync = sync.clone();
        let mut commit = self.commit(now, Priority::Backfill);
        commit.sync = Some(sync);
        vec![self.send_commit(commit, AfterCommit::Reconcile)]
    }

    /// The same lifecycle with reconciliation moved to `progress`.
    fn with_progress(&self, progress: ReconcileProgress) -> SyncState {
        match &self.state.sync {
            SyncState::Bootstrapping(_) => SyncState::Bootstrapping(progress),
            SyncState::Reconciling(_) | SyncState::Live | SyncState::Blocked { .. } => {
                SyncState::Reconciling(progress)
            }
        }
    }

    fn continue_reconcile(&mut self, progress: &ReconcileProgress, now: Timestamp) -> Vec<Command> {
        if let Some(not_before) = self.reconcile_not_before {
            if now < not_before {
                return vec![self.schedule(TimerKind::Poll, not_before)];
            }
            self.reconcile_not_before = None;
        }
        match &progress.step {
            ReconcileStep::CaptureBaseline => {
                vec![self.list_labels(Priority::Backfill, Waiting::Labels, now)]
            }
            ReconcileStep::Enumerating { .. } => match progress.listing() {
                Some((scope, page_token)) => {
                    let request = GmailRequest::ListMessages {
                        scope: scope.clone(),
                        page_token: page_token.cloned(),
                    };
                    vec![self.gmail(request, Priority::Backfill, Waiting::Listing)]
                }
                None => self.finish_reconcile(progress, now),
            },
            ReconcileStep::Verifying { .. } => self.finish_reconcile(progress, now),
        }
    }

    fn on_labels_listed(
        &mut self,
        waiting: Waiting,
        labels: Vec<Label>,
        generation: u64,
        stored_user_labels: Vec<LabelId>,
        now: Timestamp,
    ) -> Vec<Command> {
        if !crate::mirror::complete_listing(&labels) {
            return self.invalid_label_listing(waiting, now);
        }
        let listed: LabelSet = labels.iter().map(|label| label.id.clone()).collect();
        let omitted: Vec<_> = stored_user_labels
            .into_iter()
            .filter(|id| !listed.contains(id))
            .collect();
        self.labels.proof = Some(LabelProof {
            labels,
            generation,
            reconciling: waiting == Waiting::Labels,
            remaining: omitted.iter().take(LABEL_PROOF_LIMIT).cloned().collect(),
            retained: omitted.into_iter().collect(),
            removed: LabelSet::new(),
        });
        self.continue_label_proofs(now)
    }

    fn continue_label_proofs(&mut self, now: Timestamp) -> Vec<Command> {
        let Some(mut proof) = self.labels.proof.take() else {
            return Vec::new();
        };
        let priority = if proof.reconciling {
            Priority::Backfill
        } else {
            Priority::Live
        };
        if let Some(id) = proof.remaining.pop_front() {
            self.labels.proof = Some(proof);
            return vec![self.gmail(
                GmailRequest::GetLabel { id: id.clone() },
                priority,
                Waiting::LabelProof(id),
            )];
        }

        if proof.reconciling {
            self.label_ids = proof.labels.iter().map(|label| label.id.clone()).collect();
        }
        let mut out = self.labels_listed(&proof.labels, proof.retained, now);
        let mut commit = self.commit(now, priority);
        commit.labels = vec![LabelOp::ReplaceAll {
            labels: proof.labels,
            generation: proof.generation,
            at: now,
            removed: proof.removed,
        }];
        commit.account_ops = vec![AccountOp::SetLabelsRefreshedAt(now)];
        out.insert(0, self.send_commit(commit, AfterCommit::Listing));
        if proof.reconciling {
            out.push(self.gmail(
                GmailRequest::GetProfile,
                Priority::Backfill,
                Waiting::Baseline,
            ));
        }
        out
    }

    fn on_label_fetched(&mut self, command: CommandId, now: Timestamp) -> Vec<Command> {
        if !matches!(self.waiting.get(&command), Some(Waiting::LabelProof(_))) {
            return Vec::new();
        }
        let waiting = if self
            .labels
            .proof
            .as_ref()
            .is_some_and(|proof| proof.reconciling)
        {
            Waiting::Labels
        } else {
            Waiting::LabelRefresh
        };
        // One live label disproves the whole listing, including earlier proofs.
        self.invalid_label_listing(waiting, now)
    }

    fn on_label_proof_failed(
        &mut self,
        id: LabelId,
        failure: &Failure,
        now: Timestamp,
    ) -> Vec<Command> {
        let mut out = match failure {
            Failure::NotFound => {
                if let Some(proof) = self.labels.proof.as_mut() {
                    proof.retained.remove(&id);
                    proof.removed.insert(id);
                }
                Vec::new()
            }
            Failure::Retryable(failure) => self.cool_down(failure, now),
            Failure::Blocking(_) => Vec::new(),
        };
        out.extend(self.continue_label_proofs(now));
        out
    }

    fn cancel_label_proofs(&mut self) {
        self.labels.proof = None;
        self.waiting
            .retain(|_, waiting| !matches!(waiting, Waiting::LabelProof(_)));
    }

    fn invalid_label_listing(&mut self, waiting: Waiting, now: Timestamp) -> Vec<Command> {
        self.label_listing_failures += 1;
        let failure = RetryableFailure {
            class: Retryable::Transient,
            retry_after: None,
            detail: "incomplete label listing".into(),
        };
        let mut out = self.retry(waiting, &failure, now);
        out.extend(self.listing_ended(now));
        out
    }

    /// Gmail listed the label definitions (for a refresh or a
    /// reconciliation): adopt them and plan the next refresh.
    fn labels_listed(
        &mut self,
        labels: &[Label],
        mut listed: LabelSet,
        now: Timestamp,
    ) -> Vec<Command> {
        listed.extend(labels.iter().map(|label| label.id.clone()));
        self.state.labels_refreshed_at =
            Some(self.state.labels_refreshed_at.map_or(now, |at| at.max(now)));
        let refresh = &mut self.labels;
        refresh.failures = 0;
        refresh.not_before = None;
        let asked = std::mem::take(&mut refresh.asking);
        refresh
            .unlisted
            .extend(asked.into_iter().filter(|id| !listed.contains(id)));
        refresh.unlisted.retain(|id| !listed.contains(id));
        refresh.unknown.retain(|id| !listed.contains(id));
        let interval = self.config.label_refresh;
        let mut due = schedule::label_refresh_due(Some(now), now, interval);
        // Ids seen while this listing was in flight may postdate it: ask
        // once more for those it lacks.
        if !refresh.unknown.is_empty() {
            due = due.min(schedule::unknown_label_refresh(Some(now), now, interval));
        }
        refresh.due = Some(due);
        self.state.label_ids = listed;
        vec![self.schedule(TimerKind::Labels, due)]
    }

    /// Label ids the stored definitions lack ask for a refresh soon: one
    /// per batch, at most once per [`schedule::UNKNOWN_LABEL_GAP`].
    fn note_label_ids<'a>(
        &mut self,
        ids: impl IntoIterator<Item = &'a LabelId>,
        now: Timestamp,
    ) -> Vec<Command> {
        let mut fresh = false;
        for id in ids {
            let refresh = &mut self.labels;
            if !self.state.label_ids.contains(id)
                && !refresh.asking.contains(id)
                && !refresh.unlisted.contains(id)
            {
                fresh |= refresh.unknown.insert(id.clone());
            }
        }
        if !fresh {
            return Vec::new();
        }
        let at = schedule::unknown_label_refresh(
            self.state.labels_refreshed_at,
            now,
            self.config.label_refresh,
        );
        let at = self
            .labels
            .not_before
            .map_or(at, |not_before| at.max(not_before));
        self.labels.due = Some(self.labels.due.map_or(at, |due| due.min(at)));
        self.pump_labels(now)
    }

    /// Labels a delivery was refused for, which may be deleted. A held job
    /// needs a prompt answer. Other rechecks share the unknown-label gap.
    fn recheck_labels(&mut self, ids: LabelSet, now: Timestamp) -> Vec<Command> {
        if ids.is_empty() {
            return Vec::new();
        }
        self.labels.unlisted.retain(|id| !ids.contains(id));
        self.labels.unknown.extend(ids);
        let parked = !self.parked.is_empty();
        let requested = if parked {
            now
        } else {
            schedule::label_refresh_due(self.labels.last_sent, now, schedule::UNKNOWN_LABEL_GAP)
        };
        let at = self
            .labels
            .not_before
            .map_or(requested, |not_before| requested.max(not_before));
        self.labels.due = Some(self.labels.due.map_or(at, |due| due.min(at)));
        self.pump_labels(now)
    }

    /// Send the refresh if it is due; otherwise wake when it is.
    fn pump_labels(&mut self, now: Timestamp) -> Vec<Command> {
        let listing = self
            .waiting
            .values()
            .any(|w| matches!(w, Waiting::Labels | Waiting::LabelRefresh));
        // A listing in flight plans the next one when it returns.
        if listing || self.labels.proof.is_some() || self.capturing_baseline() {
            return Vec::new();
        }
        let Some(due) = self.labels.due else {
            return Vec::new();
        };
        let due = self.labels.not_before.map_or(due, |at| due.max(at));
        if due > now {
            return vec![self.schedule(TimerKind::Labels, due)];
        }
        // Gmail asked us to slow down: `CooldownOver` comes back here.
        if self.cooling_down(now) {
            return Vec::new();
        }
        self.labels.due = None;
        vec![self.list_labels(Priority::Live, Waiting::LabelRefresh, now)]
    }

    fn capturing_baseline(&self) -> bool {
        self.state
            .sync
            .reconcile()
            .is_some_and(|progress| progress.step == ReconcileStep::CaptureBaseline)
    }

    /// `labels.list`, remembering which unknown ids it is sent for.
    fn list_labels(&mut self, priority: Priority, waiting: Waiting, now: Timestamp) -> Command {
        self.labels.last_sent = Some(now);
        let unknown = std::mem::take(&mut self.labels.unknown);
        self.labels.asking.extend(unknown);
        // Reconciliation's listing replaces a refresh in flight: two
        // listings answered out of order could apply the older last.
        if waiting == Waiting::Labels {
            self.cancel_label_proofs();
            self.waiting.retain(|_, w| *w != Waiting::LabelRefresh);
        }
        for (_, sent) in &mut self.parked {
            *sent = true;
        }
        self.gmail(GmailRequest::ListLabels, priority, waiting)
    }

    /// A label listing ended (applied, failed or refused): the deliveries
    /// held for it go again (finishing their job lets the store enqueue
    /// their pending changes, minus those on a label the listing removed).
    /// Those held since it was sent wait for the next listing, sent now.
    fn listing_ended(&mut self, now: Timestamp) -> Vec<Command> {
        if self.stopped.is_some() {
            return Vec::new();
        }
        let (released, held): (Vec<_>, Vec<_>) = std::mem::take(&mut self.parked)
            .into_iter()
            .partition(|(_, sent)| *sent);
        self.parked = held;
        let mut out: Vec<Command> = released
            .into_iter()
            .map(|(job, _)| self.finish_job(&job, JobOutcome::Done, Vec::new(), now))
            .collect();
        if !self.parked.is_empty() {
            self.labels.due = Some(now);
            out.extend(self.pump_labels(now));
        }
        out
    }

    /// A listing didn't happen: the ids it was sent for are unknown again.
    fn label_listing_failed(&mut self) {
        self.cancel_label_proofs();
        let asked = std::mem::take(&mut self.labels.asking);
        self.labels.unknown.extend(asked);
    }

    /// Retry a refresh no sooner than `at`, without blocking anything else.
    fn retry_labels_at(&mut self, at: Timestamp) -> Command {
        self.labels.not_before = Some(at);
        self.labels.due = Some(at);
        self.schedule(TimerKind::Labels, at)
    }

    /// Gmail refused a refresh for good: mail sync goes on, and the next
    /// periodic refresh tries again. The ids it was sent for ask no more.
    fn label_refresh_refused(&mut self, now: Timestamp) -> Vec<Command> {
        self.cancel_label_proofs();
        let asked = std::mem::take(&mut self.labels.asking);
        self.labels.unlisted.extend(asked);
        vec![self.retry_labels_at(now.plus(self.config.label_refresh))]
    }

    fn on_profile(
        &mut self,
        command: CommandId,
        history_id: HistoryId,
        now: Timestamp,
    ) -> Vec<Command> {
        if self.waiting.remove(&command) != Some(Waiting::Baseline) {
            return Vec::new();
        }
        let Some(progress) = self.state.sync.reconcile().cloned() else {
            return Vec::new();
        };
        let progress = progress.with_baseline(history_id, std::mem::take(&mut self.label_ids));
        let sync = self.with_progress(progress);
        self.change_sync(sync, now)
    }

    fn on_listed(&mut self, command: CommandId, page: ListPage, now: Timestamp) -> Vec<Command> {
        if self.waiting.remove(&command) != Some(Waiting::Listing) {
            return Vec::new();
        }
        self.sync_failures = 0;
        let Some(progress) = self.state.sync.reconcile().cloned() else {
            return Vec::new();
        };
        let (Some(baseline), Some((scope, _))) = (progress.baseline(), progress.listing()) else {
            return Vec::new();
        };
        let scope = scope.clone();
        if scope == ListScope::All {
            self.discovered += u64::try_from(page.messages.len()).unwrap_or(0);
        }
        let messages = page
            .messages
            .into_iter()
            .map(|(id, thread_id)| MessageOp::Listed {
                id,
                thread_id,
                scope: scope.clone(),
                baseline,
            })
            .collect();
        let sync = self.with_progress(progress.after_page(page.next_page_token));
        self.state.sync = sync.clone();
        let mut commit = self.commit(now, Priority::Backfill);
        commit.messages = messages;
        commit.sync = Some(sync);
        vec![self.send_commit(commit, AfterCommit::Reconcile)]
    }

    fn finish_reconcile(&mut self, progress: &ReconcileProgress, now: Timestamp) -> Vec<Command> {
        let Some(baseline) = progress.baseline() else {
            return vec![self.list_labels(Priority::Backfill, Waiting::Labels, now)];
        };
        let to = self
            .state
            .cursor
            .map_or(baseline, |cursor| cursor.max(baseline));
        let mut commit = self.commit(now, Priority::Backfill);
        commit.messages = vec![MessageOp::FinishReconcile { baseline }];
        commit.cursor = Some(CursorMove {
            expected: self.state.cursor,
            to,
        });
        commit.sync = Some(SyncState::Live);
        vec![self.send_commit(commit, AfterCommit::ReconcileDone(to))]
    }

    // ------------------------------------------------------------ mailbox audit

    fn audit_due(&self, now: Timestamp) -> Timestamp {
        let due = if self.state.audit_progress.is_some() {
            now
        } else {
            schedule::label_refresh_due(self.state.audited_at, now, self.config.audit_interval)
        };
        audit::retry_at(
            self.state.audit_progress.as_ref(),
            self.state.audit_schedule,
        )
        .map_or(due, |retry| due.max(retry))
    }

    fn schedule_audit(&mut self, now: Timestamp) -> Option<Command> {
        (self.state.sync == SyncState::Live && !self.audit.attempted)
            .then(|| self.schedule(TimerKind::Audit, self.audit_due(now)))
    }

    /// Wait until the history pass and job claim have drained. Each account
    /// claim can perform one audit step and leaves the next to a future run.
    fn pump_audit(&mut self, now: Timestamp) -> Vec<Command> {
        if !self.audit.due
            || self.audit.attempted
            || self.state.sync != SyncState::Live
            || self.busy_syncing()
            || self.claiming
            || self.running_jobs != 0
            || self.cooling_down(now)
        {
            return Vec::new();
        }
        self.audit.attempted = true;
        match &self.state.audit_progress {
            None | Some(AuditProgress::Listing(_)) => {
                let page_token = match &self.state.audit_progress {
                    Some(AuditProgress::Listing(token)) => Some(token.clone()),
                    _ => None,
                };
                vec![self.gmail(
                    GmailRequest::AuditMessages { page_token },
                    Priority::Backfill,
                    Waiting::AuditPage,
                )]
            }
            Some(AuditProgress::Sweep(_) | AuditProgress::Cleanup { .. }) => {
                vec![self.advance_audit(AuditStep::Continue, now)]
            }
        }
    }

    fn advance_audit(&mut self, step: AuditStep, now: Timestamp) -> Command {
        let id = self.command_id();
        self.waiting.insert(id, Waiting::AuditStore);
        Command {
            id,
            kind: CommandKind::AdvanceAudit(AuditRequest {
                account: self.state.account.clone(),
                epoch: self.state.epoch,
                account_generation: self.state.account_generation,
                expected: self.state.audit_progress.clone(),
                step,
                now,
                interval: self.config.audit_interval,
            }),
        }
    }

    fn on_audit_page(
        &mut self,
        command: CommandId,
        ids: Vec<MessageId>,
        next: Option<PageToken>,
        now: Timestamp,
    ) -> Vec<Command> {
        if self.waiting.remove(&command) != Some(Waiting::AuditPage)
            || self.state.sync != SyncState::Live
        {
            return Vec::new();
        }
        if !audit::valid_page(&ids, next.as_ref()) {
            return self.abandon_audit(AuditAbandonment::MalformedPage, now);
        }
        vec![self.advance_audit(AuditStep::Page { ids, next }, now)]
    }

    fn on_audit_advanced(
        &mut self,
        command: CommandId,
        outcome: AuditOutcome,
        now: Timestamp,
    ) -> Vec<Command> {
        if self.waiting.remove(&command) != Some(Waiting::AuditStore) {
            return Vec::new();
        }
        match outcome {
            AuditOutcome::Applied {
                completed_at,
                report,
                retry_at,
            } => {
                self.audit.report = report;
                self.audit.next_run = Some(
                    retry_at
                        .or_else(|| completed_at.map(|at| at.plus(self.config.audit_interval)))
                        .unwrap_or(now),
                );
                if report.found_missing > 0 || report.found_unlisted > 0 {
                    vec![self.schedule_jobs(now)]
                } else {
                    Vec::new()
                }
            }
            AuditOutcome::Rejected(Rejection::StaleEpoch | Rejection::StaleVersion) => {
                self.stopped = Some(Stopped::Superseded(Rejection::StaleEpoch));
                vec![self.status_command()]
            }
            AuditOutcome::Rejected(_) => Vec::new(),
        }
    }

    fn abandon_audit(&mut self, reason: AuditAbandonment, now: Timestamp) -> Vec<Command> {
        if self.state.sync == SyncState::Live {
            vec![self.advance_audit(AuditStep::Abandon(reason), now)]
        } else {
            Vec::new()
        }
    }

    fn defer_audit(&mut self, until: Timestamp, now: Timestamp) -> Vec<Command> {
        if self.state.sync == SyncState::Live {
            vec![self.advance_audit(AuditStep::Defer(until), now)]
        } else {
            Vec::new()
        }
    }

    fn audit_failed(&mut self, failure: &Failure, now: Timestamp) -> Vec<Command> {
        match failure {
            Failure::Retryable(retryable) => {
                let mut out = self.cool_down(retryable, now);
                let retry = self.backoff.next(
                    self.state.audit_schedule.failures(),
                    retryable,
                    now,
                    self.jitter_seed(),
                );
                let until = self
                    .state
                    .cooldown
                    .as_ref()
                    .map_or(retry, |cooldown| retry.max(cooldown.until));
                if self.state.sync == SyncState::Live {
                    out.push(self.advance_audit(AuditStep::Retry(until), now));
                }
                out
            }
            Failure::Blocking(_) | Failure::NotFound => {
                self.abandon_audit(AuditAbandonment::Rejected, now)
            }
        }
    }

    // ------------------------------------------------------------ jobs

    fn claim_jobs(&mut self, now: Timestamp) -> Vec<Command> {
        if self.claiming || self.running_jobs >= JOB_CONCURRENCY || self.cooling_down(now) {
            return Vec::new();
        }
        self.held_back.retain(|_, until| *until > now);
        // Wake when the first held-back class reopens: claims skip it until
        // then, so nothing else would bring its jobs back.
        let mut out: Vec<Command> = self
            .held_back
            .values()
            .min()
            .copied()
            .map(|reopen| self.schedule_jobs(reopen))
            .into_iter()
            .collect();
        let floor = [Priority::Backfill, Priority::Live, Priority::Foreground]
            .into_iter()
            .find(|priority| !self.held_back.contains_key(priority));
        let Some(floor) = floor else {
            return out;
        };
        self.claiming = true;
        let attempt = self.attempt();
        let id = self.command_id();
        self.waiting.insert(id, Waiting::Claim);
        out.push(Command {
            id,
            kind: CommandKind::ClaimJobs {
                account: self.state.account.clone(),
                epoch: self.state.epoch,
                account_generation: self.state.account_generation,
                limit: JOB_CONCURRENCY - self.running_jobs,
                now,
                attempt,
                floor,
            },
        });
        out
    }

    fn on_claimed(
        &mut self,
        command: CommandId,
        jobs: Vec<ClaimedJob>,
        next_due: Option<Timestamp>,
        now: Timestamp,
    ) -> Vec<Command> {
        if self.waiting.remove(&command) != Some(Waiting::Claim) {
            return Vec::new();
        }
        self.claiming = false;
        // A listing can drop every intent from a held delivery. Once the
        // store has no more work for us, any remaining retry versions are
        // obsolete even if no settled delivery was claimed to clear them.
        if jobs.is_empty() && next_due.is_none() && self.running_jobs == 0 {
            self.relisted.clear();
        }
        let mut out = Vec::new();
        for job in jobs {
            self.running_jobs += 1;
            let priority = job.priority;
            match &job.work {
                ClaimedWork::Import(id) => {
                    let request = GmailRequest::ImportMessage { id: id.clone() };
                    out.push(self.gmail(request, priority, Waiting::Job(job)));
                }
                ClaimedWork::Verify(id) => {
                    let request = GmailRequest::GetMessage { id: id.clone() };
                    out.push(self.gmail(request, priority, Waiting::Job(job)));
                }
                ClaimedWork::Deliver(_, Delivery::Send(group)) => {
                    let request = GmailRequest::Modify(group.clone());
                    out.push(self.gmail(request, priority, Waiting::Job(job)));
                }
                ClaimedWork::Deliver(message, Delivery::Settled) => {
                    self.relisted.remove(message);
                    out.push(self.finish_job(&job, JobOutcome::Done, Vec::new(), now));
                }
                ClaimedWork::RefreshDrafts { .. } => {
                    self.drafts_listing = Some((job.attempt, Vec::new()));
                    let request = GmailRequest::ListDrafts { page_token: None };
                    out.push(self.gmail(request, priority, Waiting::Job(job)));
                }
                ClaimedWork::BlobCleanup(message) => {
                    let message = message.clone();
                    let id = self.command_id();
                    self.waiting.insert(id, Waiting::Job(job));
                    out.push(Command {
                        id,
                        kind: CommandKind::DeleteBlob {
                            account: self.state.account.clone(),
                            message,
                        },
                    });
                }
            }
        }
        // Wake for queued future work this engine has no timer for (queued
        // with a delay, or before a restart). Work already due but left by
        // the claim limit is picked up as running jobs finish.
        if let Some(due) = next_due.filter(|due| *due > now) {
            out.push(self.schedule_jobs(due));
        }
        out.extend(self.pump_audit(now));
        if out.is_empty() {
            return vec![self.status_command()];
        }
        out
    }

    /// The job `command` was issued for, if `answers` says this result is
    /// one for it (right kind, right message). A result that doesn't fit
    /// leaves the job waiting for its real answer.
    fn take_job(
        &mut self,
        command: CommandId,
        answers: impl FnOnce(&JobKind) -> bool,
    ) -> Option<ClaimedJob> {
        match self.waiting.remove(&command) {
            Some(Waiting::Job(job)) if answers(&job.kind()) => Some(job),
            Some(other) => {
                self.waiting.insert(command, other);
                None
            }
            None => None,
        }
    }

    /// A commit that finishes `job` with `outcome`, guarded by its claim.
    fn finish_job(
        &mut self,
        job: &ClaimedJob,
        outcome: JobOutcome,
        messages: Vec<MessageOp>,
        now: Timestamp,
    ) -> Command {
        self.finish_job_with(job, outcome, messages, Vec::new(), now)
    }

    /// [`Self::finish_job`] with intent changes.
    fn finish_job_with(
        &mut self,
        job: &ClaimedJob,
        outcome: JobOutcome,
        messages: Vec<MessageOp>,
        intents: Vec<IntentOp>,
        now: Timestamp,
    ) -> Command {
        let mut commit = self.commit(now, job.priority);
        commit.claim = Some(job.claim());
        commit.messages = messages;
        commit.intents = intents;
        commit.jobs = vec![JobOp::Finish {
            claim: job.claim(),
            outcome,
        }];
        self.send_commit(commit, AfterCommit::Job)
    }

    // ------------------------------------------------------------ actions (SPEC §7)

    /// Admit a user action; the store expands it into intents and enqueues
    /// their delivery, which runs as a job.
    fn on_action(&mut self, action: ActionRequest, now: Timestamp) -> Vec<Command> {
        let mut commit = self.commit(now, Priority::Foreground);
        commit.intents = vec![IntentOp::Request(action)];
        commit.account_ops = vec![AccountOp::SetLastActivity(now)];
        let mut out = vec![self.send_commit(commit, AfterCommit::Action)];
        out.extend(self.note_activity(now));
        out
    }

    fn on_modified(&mut self, command: CommandId, now: Timestamp) -> Vec<Command> {
        let Some(job) = self.take_job(command, |kind| matches!(kind, JobKind::Deliver(_))) else {
            return Vec::new();
        };
        if let Some(message) = job.kind().message() {
            self.relisted.remove(message);
        }
        let versions = job.delivery_versions();
        let intents = vec![IntentOp::Delivered {
            versions,
            at: now,
            by: job.attempt,
        }];
        vec![self.finish_job_with(&job, JobOutcome::Done, Vec::new(), intents, now)]
    }

    // ------------------------------------------------------------ drafts (SPEC §7a)

    /// A `drafts.list` page of the refresh job: ask for the next, or commit
    /// the whole listing. The store applies it only if the draft index is
    /// still at the generation the claim read; otherwise it lists again.
    fn on_drafts_listed(
        &mut self,
        command: CommandId,
        page: DraftPage,
        now: Timestamp,
    ) -> Vec<Command> {
        let Some(job) = self.take_job(command, |kind| *kind == JobKind::RefreshDrafts) else {
            return Vec::new();
        };
        let ClaimedWork::RefreshDrafts {
            generation,
            claimed_at,
        } = job.work
        else {
            return Vec::new();
        };
        let mut listed = match self.drafts_listing.take() {
            Some((attempt, listed)) if attempt == job.attempt => listed,
            Some(_) | None => Vec::new(),
        };
        listed.extend(page.drafts);
        if let Some(token) = page.next_page_token {
            self.drafts_listing = Some((job.attempt, listed));
            let request = GmailRequest::ListDrafts {
                page_token: Some(token),
            };
            let priority = job.priority;
            return vec![self.gmail(request, priority, Waiting::Job(job))];
        }
        let mut commit = self.commit(now, job.priority);
        commit.claim = Some(job.claim());
        commit.jobs = vec![JobOp::Finish {
            claim: job.claim(),
            outcome: JobOutcome::Done,
        }];
        commit.drafts = vec![DraftOp::Reconcile(DraftReconcile {
            listed,
            generation,
            claimed_at,
            grace: self.config.draft_send_grace,
            every: self.config.draft_refresh_every,
        })];
        commit.account_ops = vec![AccountOp::SetDraftsRefreshedAt(now)];
        self.state.drafts_refreshed_at =
            Some(self.state.drafts_refreshed_at.map_or(now, |at| at.max(now)));
        self.drafts_due = Some(now.plus(self.config.draft_refresh_every));
        let at = now.plus(self.config.draft_refresh_every);
        vec![
            self.send_commit(commit, AfterCommit::Job),
            self.schedule(TimerKind::Drafts, at),
        ]
    }

    /// Gmail refused the listing for good (or 404): drop the job; the next
    /// periodic listing tries again.
    fn drafts_refused(&mut self, job: &ClaimedJob, now: Timestamp) -> Vec<Command> {
        self.drafts_listing = None;
        let at = now.plus(self.config.draft_refresh_every);
        self.drafts_due = Some(at);
        vec![
            self.finish_job(job, JobOutcome::Done, Vec::new(), now),
            self.schedule(TimerKind::Drafts, at),
        ]
    }

    /// Enqueue the periodic draft listing if it is due and the account is
    /// live; otherwise wake when it will be. Bootstrapping and reconciling
    /// end with a listing of their own; a blocked engine has stopped.
    fn pump_drafts(&mut self, now: Timestamp) -> Vec<Command> {
        let Some(due) = self.drafts_due else {
            return Vec::new();
        };
        if self.state.sync != SyncState::Live {
            return Vec::new();
        }
        if due > now {
            return vec![self.schedule(TimerKind::Drafts, due)];
        }
        self.drafts_due = None;
        let mut commit = self.commit(now, Priority::Live);
        commit.jobs = vec![JobOp::Enqueue(NewJob {
            kind: JobKind::RefreshDrafts,
            priority: Priority::Live,
            due_at: now,
        })];
        // The job runs with the others when the jobs timer fires.
        vec![
            self.send_commit(commit, AfterCommit::Bookkeeping),
            self.schedule_jobs(now),
        ]
    }

    fn on_imported(
        &mut self,
        command: CommandId,
        message: ImportedMessage,
        now: Timestamp,
    ) -> Vec<Command> {
        let expected = JobKind::Import(message.observation.id.clone());
        let Some(job) = self.take_job(command, |kind| *kind == expected) else {
            return Vec::new();
        };
        self.imported += 1;
        let refresh = self.note_label_ids(&message.observation.labels, now);
        let ops = vec![MessageOp::MarkImported {
            observation: message.observation,
            blob: message.blob,
            size: message.size,
            internal_date: message.internal_date,
            headers: Box::new(message.headers),
            facts: message.facts,
        }];
        let mut out = vec![self.finish_job(&job, JobOutcome::Done, ops, now)];
        out.extend(refresh);
        out
    }

    fn on_fetched(
        &mut self,
        command: CommandId,
        message: MessageObservation,
        now: Timestamp,
    ) -> Vec<Command> {
        let expected = JobKind::Verify(message.id.clone());
        let Some(job) = self.take_job(command, |kind| *kind == expected) else {
            return Vec::new();
        };
        let refresh = self.note_label_ids(&message.labels, now);
        let mut out = vec![self.finish_job(
            &job,
            JobOutcome::Done,
            vec![MessageOp::Observe(message)],
            now,
        )];
        out.extend(refresh);
        out
    }

    fn on_not_found(&mut self, command: CommandId, id: MessageId, now: Timestamp) -> Vec<Command> {
        let answers = |kind: &JobKind| match kind {
            JobKind::Import(m) | JobKind::Verify(m) | JobKind::Deliver(m) => *m == id,
            JobKind::BlobCleanup(_) | JobKind::RefreshDrafts => false,
        };
        let Some(job) = self.take_job(command, answers) else {
            return Vec::new();
        };
        self.relisted.remove(&id);
        let ops = vec![MessageOp::Tombstone {
            id,
            evidence: DeletionEvidence::VerifiedMissing {
                no_later_than: self.state.cursor.unwrap_or(HistoryId(0)),
            },
        }];
        vec![self.finish_job(&job, JobOutcome::Done, ops, now)]
    }

    fn on_blob_deleted(&mut self, command: CommandId, now: Timestamp) -> Vec<Command> {
        let Some(job) = self.take_job(command, |kind| matches!(kind, JobKind::BlobCleanup(_)))
        else {
            return Vec::new();
        };
        vec![self.finish_job(&job, JobOutcome::Done, Vec::new(), now)]
    }

    // ------------------------------------------------------------ outcomes

    fn on_applied(&mut self, commit: CommitId, now: Timestamp) -> Vec<Command> {
        let Some(after) = self.after.remove(&commit) else {
            return Vec::new();
        };
        match after {
            AfterCommit::HistoryPage { cursor, next_page } => {
                self.after_history_page(cursor, next_page, now)
            }
            AfterCommit::Reconcile => {
                let mut out = match self.state.sync.reconcile().cloned() {
                    Some(progress) => self.continue_reconcile(&progress, now),
                    None => Vec::new(),
                };
                out.push(self.schedule_jobs(now));
                if !self.parked.is_empty() {
                    out.extend(self.pump_labels(now));
                }
                out
            }
            AfterCommit::ReconcileDone(cursor) => {
                self.state.sync = SyncState::Live;
                self.adopt_cursor(cursor, now);
                let mut out = vec![self.status_command(), self.schedule_jobs(now)];
                out.extend(self.request_pass());
                // The reconciliation enqueued a listing of its own (the
                // store's rule); the periodic one follows from it.
                out.extend(self.pump_drafts(now));
                out.extend(self.schedule_audit(now));
                out
            }
            AfterCommit::Job => {
                self.running_jobs = self.running_jobs.saturating_sub(1);
                self.claim_jobs(now)
            }
            AfterCommit::Action => self.claim_jobs(now),
            AfterCommit::Bookkeeping => Vec::new(),
            AfterCommit::Listing => self.listing_ended(now),
        }
    }

    fn on_rejected(&mut self, commit: CommitId, reason: Rejection, now: Timestamp) -> Vec<Command> {
        // A rejection of a commit this engine didn't send (an earlier engine's,
        // delivered late) says nothing about this engine.
        let Some(after) = self.after.remove(&commit) else {
            return Vec::new();
        };
        let after = Some(after);
        match reason {
            // Another attempt took the job over: drop this result.
            Rejection::StaleClaim => match after {
                Some(AfterCommit::Job) => {
                    self.running_jobs = self.running_jobs.saturating_sub(1);
                    self.claim_jobs(now)
                }
                Some(_) | None => Vec::new(),
            },
            // This engine's view is stale (another engine, a newer version,
            // a moved cursor), or it built a commit the store refused as
            // contradictory: continuing would repeat it. Stop; the reason
            // reaches the host, which builds a new engine from the store.
            Rejection::StaleEpoch
            | Rejection::StaleVersion
            | Rejection::CursorMismatch
            | Rejection::InvalidTransition => {
                self.stopped = Some(Stopped::Superseded(reason));
                vec![self.status_command()]
            }
        }
    }

    fn on_watch(
        &mut self,
        command: CommandId,
        expiration: Timestamp,
        now: Timestamp,
    ) -> Vec<Command> {
        if self.waiting.remove(&command) != Some(Waiting::Watch) {
            return Vec::new();
        }
        self.state.watch_expiration = Some(expiration);
        let renewal =
            schedule::next_watch_renewal(Some(expiration), now, self.config.watch_renew_margin);
        vec![
            self.commit_bookkeeping(now, vec![AccountOp::SetWatchExpiration(expiration)]),
            self.schedule(TimerKind::WatchRenewal, renewal),
        ]
    }

    fn on_deferred(
        &mut self,
        command: CommandId,
        until: Timestamp,
        now: Timestamp,
    ) -> Vec<Command> {
        match self.waiting.remove(&command) {
            Some(Waiting::Job(job)) => {
                // This class and every lower one wait until the gate reopens.
                for priority in [Priority::Backfill, Priority::Live, Priority::Foreground] {
                    if priority <= job.priority {
                        let held = self.held_back.entry(priority).or_insert(until);
                        *held = (*held).max(until);
                    }
                }
                vec![
                    self.finish_job(
                        &job,
                        JobOutcome::Defer {
                            due_at: until,
                            at: now,
                            priority: job.priority,
                        },
                        Vec::new(),
                        now,
                    ),
                    self.schedule_jobs(until),
                ]
            }
            Some(Waiting::Claim) => {
                self.claiming = false;
                vec![self.schedule_jobs(until)]
            }
            Some(
                waiting @ (Waiting::History
                | Waiting::Labels
                | Waiting::Baseline
                | Waiting::Listing),
            ) => {
                self.pass = None;
                let mut out = vec![self.schedule(TimerKind::Poll, until)];
                if waiting == Waiting::Labels {
                    self.label_listing_failed();
                    self.reconcile_not_before = Some(until);
                    out.extend(self.listing_ended(now));
                }
                out
            }
            Some(Waiting::LabelRefresh) => {
                self.label_listing_failed();
                let mut out = vec![self.retry_labels_at(until)];
                out.extend(self.listing_ended(now));
                out
            }
            Some(Waiting::LabelProof(_)) => self.continue_label_proofs(now),
            Some(Waiting::AuditPage) => self.defer_audit(until, now),
            Some(Waiting::Watch) => vec![self.schedule(TimerKind::WatchRenewal, until)],
            Some(Waiting::AuditStore) | None => Vec::new(),
        }
    }

    fn on_failed(&mut self, command: CommandId, failure: &Failure, now: Timestamp) -> Vec<Command> {
        let Some(waiting) = self.waiting.remove(&command) else {
            return Vec::new();
        };
        if let Waiting::LabelProof(id) = waiting {
            return self.on_label_proof_failed(id, failure, now);
        }
        if waiting == Waiting::AuditPage {
            return self.audit_failed(failure, now);
        }
        let listing = matches!(waiting, Waiting::Labels | Waiting::LabelRefresh);
        let mut out = match failure {
            Failure::Retryable(retryable) => {
                let mut out = self.cool_down(retryable, now);
                out.extend(self.retry(waiting, retryable, now));
                out
            }
            Failure::Blocking(blocking) => self.on_refused(command, waiting, blocking, now),
            Failure::NotFound => self.on_missing(command, waiting, now),
        };
        // No listing came of it: deliveries waiting for one go again.
        if listing {
            out.extend(self.listing_ended(now));
        }
        out
    }

    /// Gmail refused a request for good.
    fn on_refused(
        &mut self,
        command: CommandId,
        waiting: Waiting,
        blocking: &BlockingFailure,
        now: Timestamp,
    ) -> Vec<Command> {
        match (&blocking.class, waiting) {
            // Drafts are not mail: a refused listing, even for a scope the
            // token lacks, never blocks the account; the next periodic
            // listing tries again (the account's own requests still tell).
            (_, Waiting::Job(job)) if job.kind() == JobKind::RefreshDrafts => {
                self.drafts_refused(&job, now)
            }
            // The account itself is unusable: everything stops.
            (Blocking::Account(_), _) => self.block(blocking.clone(), now),
            (Blocking::Permanent, Waiting::Job(job)) => match job.kind() {
                // This change can't be applied. Gmail refuses a whole modify
                // for one add naming a label it does not have (a deleted
                // one), without saying which. So the first time a group
                // adding labels that may be gone is refused, nothing is
                // blocked: the labels are listed at once, and the delivery
                // waits for that listing, which drops the intents on a
                // deleted label, then goes again. Refused again, the adds
                // that may be gone are blocked (all of it if nothing else
                // is left), and the rest, still pending, goes on its own.
                JobKind::Deliver(message) => {
                    let doubtful = job.group().map(doubtful_adds).unwrap_or_default();
                    let (adds, rest): (Vec<_>, Vec<_>) = job
                        .delivery_versions()
                        .into_iter()
                        .partition(|delivery| doubtful.contains(&delivery.label));
                    if !adds.is_empty()
                        && self.relisted.get(&message).is_none_or(|previous| {
                            adds.iter().all(|version| !previous.contains(version))
                        })
                    {
                        self.relisted.insert(message, adds);
                        self.parked.push((job, false));
                        return self.recheck_labels(doubtful, now);
                    }
                    let versions = if adds.is_empty() || rest.is_empty() {
                        self.relisted.remove(&message);
                        adds.into_iter().chain(rest).collect()
                    } else {
                        adds
                    };
                    let intents = vec![IntentOp::Block {
                        versions,
                        failure: blocking.clone(),
                    }];
                    let mut out = vec![self.finish_job_with(
                        &job,
                        JobOutcome::Done,
                        Vec::new(),
                        intents,
                        now,
                    )];
                    out.extend(self.recheck_labels(doubtful, now));
                    out
                }
                JobKind::Import(_) | JobKind::Verify(_) | JobKind::BlobCleanup(_) => {
                    vec![self.finish_job(
                        &job,
                        JobOutcome::Block(blocking.clone()),
                        Vec::new(),
                        now,
                    )]
                }
                // Drafts are not mail: never park or block over them; the
                // next periodic listing tries again.
                JobKind::RefreshDrafts => self.drafts_refused(&job, now),
            },
            // A sync request mailtide built itself, refused for good, will be
            // refused next time too: stop for an operator instead of looping.
            (
                Blocking::Permanent,
                Waiting::History | Waiting::Labels | Waiting::Baseline | Waiting::Listing,
            ) => {
                self.pass = None;
                self.block(blocking.clone(), now)
            }
            // Definitions are not mail: never block the account over them.
            (Blocking::Permanent, Waiting::LabelRefresh) => self.label_refresh_refused(now),
            (Blocking::Permanent, Waiting::Claim) => {
                // Claims go to the store, which doesn't answer with Gmail
                // failures; free the slot regardless.
                let _ = command;
                self.claiming = false;
                Vec::new()
            }
            // The watch can't be renewed: polling still finds changes; the
            // next engine tries again.
            (
                Blocking::Permanent,
                Waiting::Watch | Waiting::LabelProof(_) | Waiting::AuditPage | Waiting::AuditStore,
            ) => Vec::new(),
        }
    }

    /// Gmail answered 404.
    fn on_missing(&mut self, command: CommandId, waiting: Waiting, now: Timestamp) -> Vec<Command> {
        match waiting {
            Waiting::Job(job) => match job.kind() {
                JobKind::Import(id) | JobKind::Verify(id) | JobKind::Deliver(id) => {
                    let id = id.clone();
                    self.waiting.insert(command, Waiting::Job(job));
                    self.on_not_found(command, id, now)
                }
                // Already gone: nothing to clean up.
                JobKind::BlobCleanup(_) => {
                    vec![self.finish_job(&job, JobOutcome::Done, Vec::new(), now)]
                }
                JobKind::RefreshDrafts => self.drafts_refused(&job, now),
            },
            // The cursor is too old: reconcile.
            Waiting::History => {
                self.pass = None;
                self.history_expired(now)
            }
            // These endpoints don't 404 for a working account: treat it as a
            // refusal rather than loop.
            Waiting::Labels | Waiting::Baseline | Waiting::Listing => {
                self.pass = None;
                self.block(
                    BlockingFailure {
                        class: Blocking::Permanent,
                        detail: "unexpected 404 on a sync request".into(),
                    },
                    now,
                )
            }
            Waiting::LabelRefresh => self.label_refresh_refused(now),
            Waiting::Claim => {
                self.claiming = false;
                Vec::new()
            }
            Waiting::Watch | Waiting::LabelProof(_) | Waiting::AuditPage | Waiting::AuditStore => {
                Vec::new()
            }
        }
    }

    /// Reschedule the work behind a failed command.
    fn retry(
        &mut self,
        waiting: Waiting,
        failure: &RetryableFailure,
        now: Timestamp,
    ) -> Vec<Command> {
        let seed = self.jitter_seed();
        match waiting {
            Waiting::Job(job) => {
                let due_at = self.backoff.next(job.failed_attempts, failure, now, seed);
                let outcome = JobOutcome::Retry { due_at };
                vec![
                    self.finish_job(&job, outcome, Vec::new(), now),
                    self.schedule_jobs(due_at),
                ]
            }
            Waiting::Claim => {
                self.claiming = false;
                let at = self.backoff.next(0, failure, now, seed);
                vec![self.schedule_jobs(at)]
            }
            Waiting::History | Waiting::Labels | Waiting::Baseline | Waiting::Listing => {
                self.pass = None;
                self.sync_failures += 1;
                let at = self.backoff.next(self.sync_failures, failure, now, seed);
                if waiting == Waiting::Labels {
                    self.label_listing_failed();
                    self.reconcile_not_before = Some(at);
                }
                vec![self.schedule(TimerKind::Poll, at)]
            }
            Waiting::LabelRefresh => {
                self.label_listing_failed();
                self.labels.failures += 1;
                let at = self.backoff.next(self.labels.failures, failure, now, seed);
                vec![self.retry_labels_at(at)]
            }
            Waiting::Watch => {
                let at = self.backoff.next(0, failure, now, seed);
                vec![self.schedule(TimerKind::WatchRenewal, at)]
            }
            Waiting::LabelProof(_) | Waiting::AuditPage | Waiting::AuditStore => Vec::new(),
        }
    }

    /// A cooldown for rate limits that apply beyond one request.
    fn cool_down(&mut self, failure: &RetryableFailure, now: Timestamp) -> Vec<Command> {
        let seed = self.jitter_seed();
        let cooldown = self
            .backoff
            .cooldown(failure, &self.state.account, now, seed);
        match cooldown {
            Some(cooldown)
                if self
                    .state
                    .cooldown
                    .as_ref()
                    .is_none_or(|current| current.until < cooldown.until) =>
            {
                let until = cooldown.until;
                self.state.cooldown = Some(cooldown.clone());
                vec![
                    self.commit_bookkeeping(now, vec![AccountOp::SetCooldown(cooldown)]),
                    self.schedule(TimerKind::CooldownOver, until),
                ]
            }
            Some(_) | None => Vec::new(),
        }
    }

    /// Stop on an account problem; it resumes where it was once unblocked.
    fn block(&mut self, reason: BlockingFailure, now: Timestamp) -> Vec<Command> {
        let resume = match self.state.sync.clone() {
            SyncState::Blocked { resume, .. } => resume,
            current => Box::new(current),
        };
        self.state.sync = SyncState::Blocked {
            reason: reason.clone(),
            resume,
        };
        self.stopped = Some(Stopped::Blocked(reason.clone()));
        let mut commit = self.commit(now, Priority::Live);
        commit.account_ops = vec![AccountOp::Block(reason)];
        vec![
            self.send_commit(commit, AfterCommit::Bookkeeping),
            self.status_command(),
        ]
    }

    // ------------------------------------------------------------ building blocks

    fn cooling_down(&self, now: Timestamp) -> bool {
        self.state.cooldown.as_ref().is_some_and(|c| c.until > now)
    }

    fn next_poll(&self, now: Timestamp) -> Timestamp {
        schedule::next_poll(
            &self.state.account,
            now,
            self.state.last_activity,
            self.config.poll_active,
            self.config.poll_idle,
            self.config.idle_after,
        )
    }

    fn jitter_seed(&self) -> u64 {
        self.state.rng_seed ^ self.next_command
    }

    fn command_id(&mut self) -> CommandId {
        let id = CommandId {
            epoch: self.state.epoch,
            seq: self.next_command,
        };
        self.next_command += 1;
        id
    }

    fn attempt(&mut self) -> AttemptId {
        let id = AttemptId((self.state.epoch.0 << 32) | self.next_attempt);
        self.next_attempt += 1;
        id
    }

    fn gmail(&mut self, request: GmailRequest, priority: Priority, waiting: Waiting) -> Command {
        let id = self.command_id();
        self.waiting.insert(id, waiting);
        Command {
            id,
            kind: CommandKind::Gmail(GmailCall {
                account: self.state.account.clone(),
                cost: quota::cost(&request),
                request,
                priority,
            }),
        }
    }

    /// Schedule the `Jobs` timer at `at` unless it already fires sooner. A
    /// redundant schedule re-sends the current time, so it is harmless.
    fn schedule_jobs(&mut self, at: Timestamp) -> Command {
        let at = match self.jobs_due {
            Some(due) if due <= at => due,
            Some(_) | None => at,
        };
        self.jobs_due = Some(at);
        self.schedule(TimerKind::Jobs, at)
    }

    fn schedule(&mut self, timer: TimerKind, at: Timestamp) -> Command {
        Command {
            id: self.command_id(),
            kind: CommandKind::Schedule { timer, at },
        }
    }

    fn status_command(&mut self) -> Command {
        Command {
            id: self.command_id(),
            kind: CommandKind::ReportStatus(self.status()),
        }
    }

    fn commit(&mut self, now: Timestamp, priority: Priority) -> Commit {
        let id = CommitId {
            epoch: self.state.epoch,
            seq: self.next_commit,
        };
        self.next_commit += 1;
        Commit {
            id,
            account: self.state.account.clone(),
            epoch: self.state.epoch,
            account_generation: self.state.account_generation,
            engine_version: ENGINE_VERSION,
            cursor: None,
            claim: None,
            sync: None,
            messages: Vec::new(),
            labels: Vec::new(),
            account_ops: Vec::new(),
            job_plan: JobPlan {
                priority,
                import_due_at: now,
                draft_import_due_at: now.plus(self.config.draft_settle),
                verify_due_at: now,
                cleanup_due_at: now.plus(BLOB_GRACE),
                deliver_due_at: now,
                confirm_due_at: now.plus(self.config.confirm_timeout),
                draft_refresh_due_at: now.plus(self.config.draft_refresh_delay),
            },
            jobs: Vec::new(),
            intents: Vec::new(),
            drafts: Vec::new(),
        }
    }

    fn commit_bookkeeping(&mut self, now: Timestamp, ops: Vec<AccountOp>) -> Command {
        let mut commit = self.commit(now, Priority::Live);
        commit.account_ops = ops;
        self.send_commit(commit, AfterCommit::Bookkeeping)
    }

    fn send_commit(&mut self, commit: Commit, after: AfterCommit) -> Command {
        self.after.insert(commit.id, after);
        Command {
            id: self.command_id(),
            kind: CommandKind::Commit(Box::new(commit)),
        }
    }
}

/// The labels a modify group adds that Gmail may refuse it for as unknown:
/// every add but a fixed system label's.
fn doubtful_adds(group: &crate::outbox::ModifyGroup) -> LabelSet {
    group
        .add
        .iter()
        .filter(|label| !crate::labels::system::is_system(&label.0))
        .cloned()
        .collect()
}

/// The label ids a history record names, if any.
fn record_labels(record: &HistoryRecord) -> Option<&LabelSet> {
    match record {
        HistoryRecord::MessageAdded { message, .. } => Some(&message.labels),
        HistoryRecord::LabelsAdded { labels, .. } | HistoryRecord::LabelsRemoved { labels, .. } => {
            Some(labels)
        }
        HistoryRecord::MessageDeleted { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{AccountId, Epoch};
    use crate::state::SyncPhase;

    fn engine_with(cursor: Option<u64>, sync: SyncState) -> Engine {
        let snapshot = StoreSnapshot {
            account: AccountId("alice@example.com".into()),
            epoch: Epoch(1),
            engine_version: ENGINE_VERSION,
            cursor: cursor.map(HistoryId),
            sync,
            watch_expiration: None,
            last_activity: None,
            cooldown: None,
            rng_seed: 7,
            labels_refreshed_at: None,
            audited_at: None,
            audit_progress: None,
            audit_schedule: audit::AuditSchedule::default(),
            account_generation: 1,
            label_ids: LabelSet::new(),
            drafts_refreshed_at: None,
        };
        Engine::new(
            snapshot.validate().expect("valid snapshot"),
            EngineConfig::default(),
        )
    }

    /// The drafts listing enqueued by a starting or woken engine, if any.
    fn draft_enqueues(commands: &[Command]) -> usize {
        commands
            .iter()
            .filter(|c| match &c.kind {
                CommandKind::Commit(commit) => commit.jobs.iter().any(
                    |op| matches!(op, JobOp::Enqueue(job) if job.kind == JobKind::RefreshDrafts),
                ),
                _ => false,
            })
            .count()
    }

    fn drafts_timer(commands: &[Command]) -> Option<Timestamp> {
        commands.iter().find_map(|c| match c.kind {
            CommandKind::Schedule {
                timer: TimerKind::Drafts,
                at,
            } => Some(at),
            _ => None,
        })
    }

    #[test]
    fn the_draft_index_is_refreshed_at_most_every_fifteen_minutes_while_live() {
        let every = EngineConfig::default().draft_refresh_every;
        let now = Timestamp(10_000_000);
        // Never listed: at once.
        let mut engine = live(100);
        assert_eq!(draft_enqueues(&engine.start(now)), 1);
        // Listed a minute ago (persisted, so a restart neither repeats nor
        // skips it): a timer for 14 minutes on, nothing before.
        let listed = now.minus(Duration::from_mins(1));
        let mut engine = live(100);
        engine.state.drafts_refreshed_at = Some(listed);
        let started = engine.start(now);
        assert_eq!(draft_enqueues(&started), 0);
        assert_eq!(drafts_timer(&started), Some(listed.plus(every)));
        let early = engine.handle(Event {
            now: listed.plus(every).minus(Duration::from_secs(1)),
            kind: EventKind::TimerFired {
                timer: TimerKind::Drafts,
            },
        });
        assert_eq!(draft_enqueues(&early), 0);
        let due = listed.plus(every);
        let fired = engine.handle(Event {
            now: due,
            kind: EventKind::TimerFired {
                timer: TimerKind::Drafts,
            },
        });
        assert_eq!(draft_enqueues(&fired), 1);
        // The listing itself: 5 units a page, its time recorded, the next
        // one an interval later.
        let job = ClaimedJob {
            work: ClaimedWork::RefreshDrafts {
                generation: 3,
                claimed_at: due,
            },
            priority: Priority::Live,
            failed_attempts: 0,
            attempt: AttemptId(9),
        };
        let claim = engine.claim_jobs(due);
        let claim = claim
            .iter()
            .find(|c| matches!(c.kind, CommandKind::ClaimJobs { .. }))
            .expect("a claim")
            .id;
        let commands = engine.handle(Event {
            now: due,
            kind: EventKind::JobsClaimed {
                command: claim,
                jobs: vec![job],
                next_due: None,
            },
        });
        let list = commands
            .iter()
            .find_map(|c| match &c.kind {
                CommandKind::Gmail(call) => Some((c.id, call.clone())),
                _ => None,
            })
            .expect("drafts.list");
        assert_eq!(
            list.1.request,
            GmailRequest::ListDrafts { page_token: None }
        );
        assert_eq!(list.1.cost, quota::QuotaUnits(5));
        let page = DraftPage {
            drafts: Vec::new(),
            next_page_token: None,
        };
        let done = engine.handle(Event {
            now: due,
            kind: EventKind::DraftsListed {
                command: list.0,
                page,
            },
        });
        let commit = first_commit(done.clone()).expect("the listing's commit");
        assert!(
            commit
                .account_ops
                .contains(&AccountOp::SetDraftsRefreshedAt(due))
        );
        assert!(matches!(
            commit.drafts.as_slice(),
            [DraftOp::Reconcile(DraftReconcile { generation: 3, .. })]
        ));
        assert_eq!(drafts_timer(&done), Some(due.plus(every)));
        no_periodic_listing_unless_live(now);
    }

    /// A refused listing (403 for a scope the token lacks, 400, 404) drops
    /// the job and waits for the next periodic listing: drafts never block
    /// the account, whatever the refusal's class.
    #[test]
    fn a_refused_draft_listing_never_blocks_the_account() {
        let every = EngineConfig::default().draft_refresh_every;
        let refusals = [
            Failure::Blocking(BlockingFailure {
                class: Blocking::Account(crate::retry::AccountProblem::ApiAccessDenied),
                detail: "403".into(),
            }),
            Failure::Blocking(BlockingFailure {
                class: Blocking::Permanent,
                detail: "400".into(),
            }),
            Failure::NotFound,
        ];
        for failure in refusals {
            let now = Timestamp(10_000_000);
            let mut engine = live(100);
            assert_eq!(draft_enqueues(&engine.start(now)), 1);
            let claim = engine.claim_jobs(now);
            let claim = claim
                .iter()
                .find(|c| matches!(c.kind, CommandKind::ClaimJobs { .. }))
                .expect("a claim")
                .id;
            let commands = engine.handle(Event {
                now,
                kind: EventKind::JobsClaimed {
                    command: claim,
                    jobs: vec![ClaimedJob {
                        work: ClaimedWork::RefreshDrafts {
                            generation: 0,
                            claimed_at: now,
                        },
                        priority: Priority::Live,
                        failed_attempts: 0,
                        attempt: AttemptId(9),
                    }],
                    next_due: None,
                },
            });
            let list = commands
                .iter()
                .find(|c| matches!(c.kind, CommandKind::Gmail(_)))
                .expect("drafts.list")
                .id;
            let refused = engine.handle(Event {
                now,
                kind: EventKind::RequestFailed {
                    command: list,
                    failure: failure.clone(),
                },
            });
            assert_eq!(engine.state.sync, SyncState::Live, "{failure:?}");
            let commit = first_commit(refused.clone()).expect("the job's finish");
            assert!(
                commit.jobs.iter().any(|op| matches!(
                    op,
                    JobOp::Finish {
                        outcome: JobOutcome::Done,
                        ..
                    }
                )),
                "{failure:?}"
            );
            assert_eq!(drafts_timer(&refused), Some(now.plus(every)), "{failure:?}");
        }
    }

    /// Not while bootstrapping or reconciling: they end with a listing of
    /// their own. A blocked engine has stopped.
    fn no_periodic_listing_unless_live(now: Timestamp) {
        for sync in [
            SyncState::Bootstrapping(ReconcileProgress::begin(ReconcileReason::Bootstrap)),
            SyncState::Reconciling(ReconcileProgress::begin(ReconcileReason::HistoryExpired)),
        ] {
            let cursor = match sync {
                SyncState::Bootstrapping(_) => None,
                _ => Some(100),
            };
            let mut engine = engine_with(cursor, sync);
            let started = engine.start(now);
            assert_eq!(draft_enqueues(&started), 0);
            assert_eq!(drafts_timer(&started), None);
        }
        let blocked = SyncState::Blocked {
            reason: BlockingFailure {
                class: Blocking::Permanent,
                detail: String::new(),
            },
            resume: Box::new(SyncState::Live),
        };
        let mut engine = engine_with(Some(100), blocked);
        assert_eq!(draft_enqueues(&engine.start(now)), 0);
    }

    /// After the gate defers a backfill job, claims skip backfill until it
    /// reopens (instead of deferring and committing each queued job again),
    /// but still take live work.
    #[test]
    fn a_backfill_deferral_raises_the_claim_floor_until_the_gate_reopens() {
        let mut engine = live(100);
        let claim = engine.claim_jobs(Timestamp(0));
        let claim = claim.first().expect("a claim").id;
        let job = ClaimedJob {
            work: ClaimedWork::Import(MessageId("old".into())),
            priority: Priority::Backfill,
            failed_attempts: 0,
            attempt: AttemptId(1),
        };
        let commands = engine.handle(event(EventKind::JobsClaimed {
            command: claim,
            jobs: vec![job],
            next_due: None,
        }));
        let import = commands
            .iter()
            .find(|c| matches!(&c.kind, CommandKind::Gmail(_)))
            .expect("an import")
            .id;
        let _ = engine.handle(event(EventKind::RequestDeferred {
            command: import,
            until: Timestamp(5_000),
        }));
        let floor = |commands: &[Command]| {
            commands.iter().find_map(|c| match c.kind {
                CommandKind::ClaimJobs { floor, .. } => Some(floor),
                _ => None,
            })
        };
        engine.claiming = false;
        let held = engine.claim_jobs(Timestamp(1_000));
        assert_eq!(floor(&held), Some(Priority::Live));
        assert!(
            held.iter().any(|c| matches!(
                c.kind,
                CommandKind::Schedule {
                    timer: TimerKind::Jobs,
                    at: Timestamp(5_000)
                }
            )),
            "a wake for when backfill reopens"
        );
        engine.claiming = false;
        assert_eq!(
            floor(&engine.claim_jobs(Timestamp(5_000))),
            Some(Priority::Backfill)
        );
    }

    /// Answer the engine's job claim among `commands` from `mirror`, as a
    /// store would.
    fn claim_from(mirror: &mut crate::mirror::AccountMirror, commands: &[Command]) -> Event {
        let (id, epoch, limit, now, attempt, floor) = commands
            .iter()
            .find_map(|c| match c.kind {
                CommandKind::ClaimJobs {
                    epoch,
                    limit,
                    now,
                    attempt,
                    floor,
                    ..
                } => Some((c.id, epoch, limit, now, attempt, floor)),
                _ => None,
            })
            .expect("a claim");
        let jobs = mirror
            .claim_jobs_from(epoch, limit, now, attempt, floor)
            .expect("the engine's epoch");
        event(EventKind::JobsClaimed {
            command: id,
            jobs,
            next_due: mirror.next_due(),
        })
    }

    /// The engine and a store, driven as a host would: commits applied in
    /// order, claims answered from the store, Gmail requests kept for the
    /// test to answer.
    struct Driven {
        engine: Engine,
        mirror: crate::mirror::AccountMirror,
        requests: Vec<(CommandId, GmailRequest)>,
    }

    impl Driven {
        /// Message `m` (in the inbox), labels `defined`, `actions` admitted.
        fn new(defined: &[&str], actions: Vec<crate::outbox::Action>) -> Self {
            use crate::gmail::{Label, LabelKind};
            let mut mirror =
                crate::mirror::AccountMirror::new(AccountId("alice@example.com".into()), 1);
            mirror.claim(ENGINE_VERSION).expect("claim");
            for id in defined {
                let kind = if id.starts_with("Label_") {
                    LabelKind::User
                } else {
                    LabelKind::System
                };
                mirror.record_label(&LabelOp::Upsert(Label::plain(
                    LabelId((*id).into()),
                    *id,
                    kind,
                )));
            }
            let id = MessageId("m".into());
            mirror.messages.insert(
                id.clone(),
                crate::mirror::MessageRecord {
                    id: id.clone(),
                    thread_id: crate::ids::ThreadId("t".into()),
                    labels: crate::labels::set(["INBOX"]),
                    checkpoint: Some(HistoryId(50)),
                    unverified: None,
                    content: crate::mirror::Content::Pending,
                },
            );
            for action in actions {
                mirror.admit_action(
                    &crate::outbox::ActionRequest::Messages {
                        ids: vec![id.clone()],
                        action,
                    },
                    Timestamp(0),
                );
            }
            let mut driven = Self {
                engine: live(100),
                mirror,
                requests: Vec::new(),
            };
            let claim = driven.engine.claim_jobs(Timestamp(0));
            driven.settle(claim);
            driven
        }

        /// Carry out `commands` and whatever they lead to, but Gmail's.
        fn settle(&mut self, commands: Vec<Command>) {
            let mut queue: std::collections::VecDeque<Command> = commands.into();
            while let Some(command) = queue.pop_front() {
                let answer = match &command.kind {
                    CommandKind::Commit(commit) => Some(event(match self.mirror.apply(commit) {
                        crate::commit::CommitOutcome::Applied { .. } => {
                            EventKind::CommitApplied { commit: commit.id }
                        }
                        crate::commit::CommitOutcome::Rejected(reason) => {
                            EventKind::CommitRejected {
                                commit: commit.id,
                                reason,
                            }
                        }
                    })),
                    CommandKind::ClaimJobs { .. } => {
                        Some(claim_from(&mut self.mirror, std::slice::from_ref(&command)))
                    }
                    CommandKind::Gmail(call) => {
                        self.requests.push((command.id, call.request.clone()));
                        None
                    }
                    CommandKind::Schedule { .. }
                    | CommandKind::DeleteBlob { .. }
                    | CommandKind::ReportStatus(_)
                    | CommandKind::AdvanceAudit(_) => None,
                };
                if let Some(answer) = answer {
                    queue.extend(self.engine.handle(answer));
                }
            }
        }

        /// The modify asked for: its command, adds and removes.
        fn modify(&mut self) -> (CommandId, Vec<LabelId>, Vec<LabelId>) {
            let at = self
                .requests
                .iter()
                .position(|(_, r)| matches!(r, GmailRequest::Modify(_)))
                .expect("a modify");
            match self.requests.remove(at) {
                (id, GmailRequest::Modify(group)) => (id, group.add, group.remove),
                _ => unreachable!("a modify"),
            }
        }

        fn has(&self, request: &GmailRequest) -> bool {
            self.requests.iter().any(|(_, r)| r == request)
        }

        /// Gmail refuses the modify asked for for good (a 400).
        fn refuse(&mut self) -> (Vec<LabelId>, Vec<LabelId>) {
            let (command, add, remove) = self.modify();
            let answer = self.engine.handle(event(EventKind::RequestFailed {
                command,
                failure: Failure::Blocking(BlockingFailure {
                    class: Blocking::Permanent,
                    detail: "400 invalidArgument labelId not found".into(),
                }),
            }));
            self.settle(answer);
            (add, remove)
        }

        /// Gmail applies the modify asked for.
        fn deliver(&mut self) -> (Vec<LabelId>, Vec<LabelId>) {
            let (command, add, remove) = self.modify();
            let answer = self
                .engine
                .handle(event(EventKind::LabelsModified { command }));
            self.settle(answer);
            (add, remove)
        }

        /// Gmail answers the listing asked for with `listed`.
        fn list(&mut self, listed: &[&str]) {
            self.answer_labels(listed.iter().copied().chain(["UNREAD"]));
            while let Some(at) = self
                .requests
                .iter()
                .position(|(_, request)| matches!(request, GmailRequest::GetLabel { .. }))
            {
                let (command, _) = self.requests.remove(at);
                let answer = self.engine.handle(event(EventKind::RequestFailed {
                    command,
                    failure: Failure::NotFound,
                }));
                self.settle(answer);
            }
        }

        fn answer_labels<'a>(&mut self, listed: impl IntoIterator<Item = &'a str>) {
            use crate::gmail::{Label, LabelKind};
            let at = self
                .requests
                .iter()
                .position(|(_, r)| *r == GmailRequest::ListLabels)
                .expect("a listing");
            let (command, _) = self.requests.remove(at);
            let labels = listed
                .into_iter()
                .map(|id| {
                    let kind = if id.starts_with("Label_") {
                        LabelKind::User
                    } else {
                        LabelKind::System
                    };
                    Label::plain(LabelId(id.into()), id, kind)
                })
                .collect();
            let answer = self.engine.handle(event(EventKind::LabelsListed {
                command,
                labels,
                generation: self.mirror.record.label_generation,
                stored_user_labels: self.mirror.user_labels_oldest_first(),
            }));
            self.settle(answer);
        }

        /// A commit to apply to the store as another engine's.
        fn mirror_commit(&self) -> Commit {
            Commit {
                id: CommitId {
                    epoch: self.mirror.record.epoch,
                    seq: 9_999,
                },
                account: self.mirror.account.clone(),
                epoch: self.mirror.record.epoch,
                account_generation: self.mirror.record.generation,
                engine_version: ENGINE_VERSION,
                cursor: None,
                claim: None,
                sync: None,
                messages: Vec::new(),
                labels: Vec::new(),
                account_ops: Vec::new(),
                job_plan: JobPlan {
                    priority: Priority::Live,
                    import_due_at: Timestamp(0),
                    draft_import_due_at: Timestamp(0),
                    verify_due_at: Timestamp(0),
                    cleanup_due_at: Timestamp(0),
                    deliver_due_at: Timestamp(0),
                    confirm_due_at: Timestamp(0),
                    draft_refresh_due_at: Timestamp(0),
                },
                jobs: Vec::new(),
                intents: Vec::new(),
                drafts: Vec::new(),
            }
        }

        /// Each intent's label, and whether it is blocked.
        fn intents(&self) -> Vec<(&str, bool)> {
            self.mirror
                .intents
                .values()
                .map(|intent| {
                    (
                        intent.label.0.as_str(),
                        matches!(intent.state, crate::outbox::IntentState::Blocked(_)),
                    )
                })
                .collect()
        }
    }

    fn ids(ids: &[&str]) -> Vec<LabelId> {
        ids.iter().map(|id| LabelId((*id).into())).collect()
    }

    fn add_labels(labels: &[&str]) -> crate::outbox::Action {
        crate::outbox::Action::ChangeLabels(
            crate::outbox::LabelDelta::new(ids(labels), Vec::new()).expect("valid"),
        )
    }

    /// Gmail refuses a whole modify with 400 `labelId not found` when one
    /// add names a deleted label (`label-edit-modify-deleted-label`): the
    /// first refusal blocks nothing; the delivery waits for a label listing,
    /// which drops the intent on the deleted label, then goes again with
    /// the rest, a valid user label included.
    #[test]
    fn a_delivery_refused_for_a_user_label_waits_for_a_listing_and_goes_again() {
        use crate::outbox::StarState;
        for star in [false, true] {
            let mut actions = vec![add_labels(&["Label_dead", "Label_ok"])];
            if star {
                actions.push(crate::outbox::Action::SetStar(StarState::Starred));
            }
            let mut driven = Driven::new(&["INBOX", "STARRED", "Label_dead", "Label_ok"], actions);
            let (add, _) = driven.refuse();
            assert_eq!(add.len(), if star { 3 } else { 2 });
            assert!(driven.intents().iter().all(|(_, blocked)| !blocked));
            assert!(driven.has(&GmailRequest::ListLabels), "listed at once");
            assert!(
                !driven
                    .requests
                    .iter()
                    .any(|(_, r)| matches!(r, GmailRequest::Modify(_))),
                "nothing goes before the listing"
            );
            driven.list(&["INBOX", "STARRED", "Label_ok"]);
            let (add, _) = driven.deliver();
            let expected: &[&str] = if star {
                &["Label_ok", "STARRED"]
            } else {
                &["Label_ok"]
            };
            assert_eq!(add, ids(expected));
            assert!(driven.intents().iter().all(|(_, blocked)| !blocked));
        }
    }

    /// Refused again after the listing, the adds that may be gone are
    /// blocked and the rest goes alone; refused once more, it blocks. No
    /// loop: each refusal sends less.
    #[test]
    fn a_second_refusal_blocks_only_the_user_label_adds() {
        use crate::outbox::StarState;
        let mut driven = Driven::new(
            &["INBOX", "STARRED", "Label_ok"],
            vec![
                add_labels(&["Label_ok"]),
                crate::outbox::Action::SetStar(StarState::Starred),
            ],
        );
        driven.refuse();
        driven.list(&["INBOX", "STARRED", "Label_ok"]);
        let (add, _) = driven.refuse();
        assert_eq!(add, ids(&["Label_ok", "STARRED"]));
        assert!(!driven.has(&GmailRequest::ListLabels), "nothing is parked");
        assert_eq!(driven.intents(), [("Label_ok", true), ("STARRED", false)]);
        let (add, _) = driven.refuse();
        assert_eq!(add, ids(&["STARRED"]));
        assert_eq!(driven.intents(), [("Label_ok", true), ("STARRED", true)]);
        assert!(
            !driven
                .requests
                .iter()
                .any(|(_, r)| matches!(r, GmailRequest::Modify(_))),
            "nothing more is sent"
        );
    }

    #[test]
    fn a_new_intent_can_wait_after_the_previous_adds_were_removed_or_blocked() {
        for remove in [false, true] {
            let mut driven = Driven::new(
                &["INBOX", "Label_dead", "Label_other"],
                vec![add_labels(&["Label_dead"])],
            );
            driven.refuse();
            if remove {
                driven.list(&["INBOX", "Label_other"]);
            } else {
                driven.list(&["INBOX", "Label_dead", "Label_other"]);
                driven.refuse();
            }
            assert!(
                !driven
                    .requests
                    .iter()
                    .any(|(_, r)| matches!(r, GmailRequest::Modify(_)))
            );
            assert!(driven.engine.relisted.is_empty());
            let answer = driven.engine.handle(event(EventKind::ActionRequested {
                action: ActionRequest::Messages {
                    ids: vec![MessageId("m".into())],
                    action: add_labels(&["Label_other"]),
                },
            }));
            driven.settle(answer);
            driven.refuse();
            assert!(driven.has(&GmailRequest::ListLabels));
        }
    }

    #[test]
    fn an_incomplete_listing_releases_a_held_delivery_unchanged() {
        let mut driven = Driven::new(&["INBOX", "Label_ok"], vec![add_labels(&["Label_ok"])]);
        driven.refuse();
        driven.answer_labels(["INBOX"]);
        assert_eq!(driven.engine.status().label_listing_failures, 1);
        assert_eq!(driven.refuse().0, ids(&["Label_ok"]));
        assert_eq!(driven.intents(), [("Label_ok", true)]);
    }

    /// A delivery held for a listing goes again with what the store holds
    /// then: a change admitted meanwhile, and nothing for a message deleted
    /// meanwhile.
    #[test]
    fn a_held_delivery_follows_what_changed_meanwhile() {
        let mut driven = Driven::new(
            &["INBOX", "Label_dead", "Label_ok"],
            vec![add_labels(&["Label_dead", "Label_ok"])],
        );
        driven.refuse();
        // The user takes Label_ok back off while the delivery waits.
        driven.mirror.admit_action(
            &crate::outbox::ActionRequest::Messages {
                ids: vec![MessageId("m".into())],
                action: crate::outbox::Action::ChangeLabels(
                    crate::outbox::LabelDelta::new(Vec::new(), ids(&["Label_ok"])).expect("valid"),
                ),
            },
            Timestamp(0),
        );
        driven.list(&["INBOX", "Label_ok"]);
        let (add, remove) = driven.deliver();
        assert_eq!((add, remove), (Vec::new(), ids(&["Label_ok"])));
        assert!(driven.intents().iter().all(|(_, blocked)| !blocked));

        let mut deleted = Driven::new(&["INBOX", "Label_dead"], vec![add_labels(&["Label_dead"])]);
        deleted.refuse();
        let mut tombstone = deleted.mirror_commit();
        tombstone.messages = vec![MessageOp::Tombstone {
            id: MessageId("m".into()),
            evidence: DeletionEvidence::History { at: HistoryId(60) },
        }];
        assert!(matches!(
            deleted.mirror.apply(&tombstone),
            crate::commit::CommitOutcome::Applied { .. }
        ));
        deleted.list(&["INBOX"]);
        assert!(deleted.engine.stopped().is_none());
        assert!(deleted.mirror.intents.is_empty());
        assert!(
            !deleted
                .requests
                .iter()
                .any(|(_, r)| matches!(r, GmailRequest::Modify(_)))
        );
    }

    /// A user opens a message while the gate holds Backfill back: the
    /// store write raises its import behind the engine's back, and the
    /// engine's next claim sends it in the Foreground class.
    #[test]
    fn a_prioritised_import_is_requested_in_the_foreground_class() {
        let mut mirror =
            crate::mirror::AccountMirror::new(AccountId("alice@example.com".into()), 1);
        mirror.claim(ENGINE_VERSION).expect("claim");
        let mut engine = live(100);
        let discover = |mirror: &crate::mirror::AccountMirror, seq: u64, id: &str| Commit {
            id: CommitId {
                epoch: mirror.record.epoch,
                seq,
            },
            account: mirror.account.clone(),
            epoch: mirror.record.epoch,
            account_generation: mirror.record.generation,
            engine_version: ENGINE_VERSION,
            cursor: None,
            claim: None,
            sync: None,
            messages: vec![MessageOp::Observe(MessageObservation {
                id: MessageId(id.into()),
                thread_id: crate::ids::ThreadId(format!("t-{id}")),
                labels: LabelSet::new(),
                checkpoint: HistoryId(50),
            })],
            labels: Vec::new(),
            account_ops: Vec::new(),
            job_plan: JobPlan {
                priority: Priority::Backfill,
                import_due_at: Timestamp(0),
                draft_import_due_at: Timestamp(0),
                verify_due_at: Timestamp(0),
                cleanup_due_at: Timestamp(0),
                deliver_due_at: Timestamp(0),
                confirm_due_at: Timestamp(0),
                draft_refresh_due_at: Timestamp(0),
            },
            jobs: Vec::new(),
            intents: Vec::new(),
            drafts: Vec::new(),
        };
        mirror.apply(&discover(&mirror, 1_000, "old"));

        // The backlog's import is deferred by the gate: Backfill is held back.
        let claim = engine.claim_jobs(Timestamp(0));
        let commands = engine.handle(claim_from(&mut mirror, &claim));
        let import = commands
            .iter()
            .find(|c| matches!(&c.kind, CommandKind::Gmail(_)))
            .expect("an import")
            .id;
        let deferred = engine.handle(event(EventKind::RequestDeferred {
            command: import,
            until: Timestamp(60_000),
        }));
        mirror.apply(&first_commit(deferred).expect("the deferral's commit"));

        // A message is discovered, and the user opens it.
        mirror.apply(&discover(&mirror, 1_001, "opened"));
        let report = mirror.prioritise(&[MessageId("opened".into())], Timestamp(1_000));
        assert_eq!(report.queued, vec![MessageId("opened".into())]);

        let claim = engine.claim_jobs(Timestamp(1_000));
        assert!(
            claim.iter().any(|c| matches!(
                c.kind,
                CommandKind::ClaimJobs {
                    floor: Priority::Live,
                    ..
                }
            )),
            "Backfill is still held back: {claim:?}"
        );
        let commands = engine.handle(claim_from(&mut mirror, &claim));
        let calls: Vec<&GmailCall> = commands
            .iter()
            .filter_map(|c| match &c.kind {
                CommandKind::Gmail(call) => Some(call),
                _ => None,
            })
            .collect();
        assert_eq!(calls.len(), 1, "{commands:?}");
        assert_eq!(
            calls[0].request,
            GmailRequest::ImportMessage {
                id: MessageId("opened".into())
            }
        );
        assert_eq!(calls[0].priority, Priority::Foreground);
        assert_eq!(calls[0].cost, quota::QuotaUnits(60));
    }

    #[test]
    fn with_watches_off_no_renewal_is_scheduled_or_sent() {
        let snapshot = StoreSnapshot {
            account: AccountId("alice@example.com".into()),
            epoch: Epoch(1),
            engine_version: ENGINE_VERSION,
            cursor: Some(HistoryId(100)),
            sync: SyncState::Live,
            watch_expiration: None,
            last_activity: None,
            cooldown: None,
            rng_seed: 7,
            labels_refreshed_at: None,
            audited_at: None,
            audit_progress: None,
            audit_schedule: audit::AuditSchedule::default(),
            account_generation: 1,
            label_ids: LabelSet::new(),
            drafts_refreshed_at: None,
        };
        let config = EngineConfig {
            watch: WatchPolicy::Off,
            ..EngineConfig::default()
        };
        let mut engine = Engine::new(snapshot.validate().expect("valid"), config);
        let started = engine.start(Timestamp(1_000));
        assert!(!started.iter().any(|c| matches!(
            c.kind,
            CommandKind::Schedule {
                timer: TimerKind::WatchRenewal,
                ..
            }
        )));
        let fired = engine.handle(event(EventKind::TimerFired {
            timer: TimerKind::WatchRenewal,
        }));
        assert!(!gmail_requests(&fired).contains(&&GmailRequest::Watch));
        let mut renewing = live(100);
        let started = renewing.start(Timestamp(1_000));
        assert!(started.iter().any(|c| matches!(
            c.kind,
            CommandKind::Schedule {
                timer: TimerKind::WatchRenewal,
                ..
            }
        )));
    }

    fn live(cursor: u64) -> Engine {
        engine_with(Some(cursor), SyncState::Live)
    }

    fn event(kind: EventKind) -> Event {
        Event {
            now: Timestamp(1_000),
            kind,
        }
    }

    fn gmail_requests(commands: &[Command]) -> Vec<&GmailRequest> {
        commands
            .iter()
            .filter_map(|c| match &c.kind {
                CommandKind::Gmail(call) => Some(&call.request),
                CommandKind::Commit(_)
                | CommandKind::Schedule { .. }
                | CommandKind::ClaimJobs { .. }
                | CommandKind::DeleteBlob { .. }
                | CommandKind::ReportStatus(_)
                | CommandKind::AdvanceAudit(_) => None,
            })
            .collect()
    }

    fn first_commit(commands: Vec<Command>) -> Option<Commit> {
        commands.into_iter().find_map(|c| match c.kind {
            CommandKind::Commit(commit) => Some(*commit),
            CommandKind::Gmail(_)
            | CommandKind::Schedule { .. }
            | CommandKind::ClaimJobs { .. }
            | CommandKind::DeleteBlob { .. }
            | CommandKind::ReportStatus(_)
            | CommandKind::AdvanceAudit(_) => None,
        })
    }

    fn push(engine: &mut Engine, history_id: u64) -> Vec<Command> {
        engine.handle(event(EventKind::PushReceived {
            history_id: HistoryId(history_id),
        }))
    }

    #[test]
    fn newer_push_fetches_history_from_the_cursor() {
        let commands = push(&mut live(100), 105);
        assert_eq!(
            gmail_requests(&commands),
            vec![&GmailRequest::ListHistory {
                start: HistoryId(100),
                page_token: None
            }]
        );
    }

    #[test]
    fn stale_push_does_nothing() {
        assert_eq!(push(&mut live(100), 90), Vec::new());
    }

    #[test]
    fn a_push_during_a_pass_does_not_start_a_second_pass() {
        let mut engine = live(100);
        assert_eq!(gmail_requests(&push(&mut engine, 105)).len(), 1);
        assert!(gmail_requests(&push(&mut engine, 110)).is_empty());
    }

    #[test]
    fn a_permanent_refusal_of_a_sync_request_blocks_instead_of_looping() {
        let mut engine = engine_with(
            None,
            SyncState::Bootstrapping(ReconcileProgress::begin(ReconcileReason::Bootstrap)),
        );
        let start = engine.start(Timestamp(0));
        let labels = start
            .iter()
            .find(|c| matches!(&c.kind, CommandKind::Gmail(call) if call.request == GmailRequest::ListLabels))
            .expect("labels are listed first")
            .id;
        let refusal = BlockingFailure {
            class: Blocking::Permanent,
            detail: "400".into(),
        };
        let commands = engine.handle(event(EventKind::RequestFailed {
            command: labels,
            failure: Failure::Blocking(refusal.clone()),
        }));
        assert!(gmail_requests(&commands).is_empty(), "no retry loop");
        assert_eq!(engine.stopped(), Some(&Stopped::Blocked(refusal)));
    }

    #[test]
    fn a_result_for_another_message_leaves_the_job_waiting() {
        let mut engine = live(100);
        let job = ClaimedJob {
            work: ClaimedWork::Import(MessageId("a".into())),
            priority: Priority::Live,
            failed_attempts: 0,
            attempt: AttemptId(1),
        };
        let claim = engine.claim_jobs(Timestamp(0));
        let claim = claim.first().expect("a claim").id;
        let commands = engine.handle(event(EventKind::JobsClaimed {
            command: claim,
            jobs: vec![job],
            next_due: None,
        }));
        let import = commands
            .iter()
            .find(|c| matches!(&c.kind, CommandKind::Gmail(_)))
            .expect("the import is fetched")
            .id;
        let wrong = engine.handle(event(EventKind::MessageNotFound {
            command: import,
            message_id: MessageId("b".into()),
        }));
        assert!(
            first_commit(wrong).is_none(),
            "b's 404 must not finish a's import"
        );
        let right = engine.handle(event(EventKind::MessageNotFound {
            command: import,
            message_id: MessageId("a".into()),
        }));
        assert!(first_commit(right).is_some(), "a's own answer still lands");
    }

    #[test]
    fn a_refused_claim_stops_the_engine() {
        let mut engine = live(100);
        let claim = engine.claim_jobs(Timestamp(0));
        let claim = claim.first().expect("a claim").id;
        let commands = engine.handle(event(EventKind::JobsClaimRejected {
            command: claim,
            reason: Rejection::StaleEpoch,
        }));
        assert!(gmail_requests(&commands).is_empty());
        assert_eq!(
            engine.stopped(),
            Some(&Stopped::Superseded(Rejection::StaleEpoch))
        );
    }

    #[test]
    fn start_without_cursor_bootstraps() {
        let mut engine = engine_with(
            None,
            SyncState::Bootstrapping(ReconcileProgress::begin(ReconcileReason::Bootstrap)),
        );
        let commands = engine.start(Timestamp(0));
        assert_eq!(gmail_requests(&commands), vec![&GmailRequest::ListLabels]);
        assert_eq!(engine.status().phase, SyncPhase::Bootstrapping);
    }

    #[test]
    fn history_page_commits_ops_and_cursor_then_adopts_on_apply() {
        let mut engine = live(100);
        let command = push(&mut engine, 105)[0].id;
        let page = HistoryPage {
            records: vec![crate::gmail::HistoryRecord::MessageDeleted {
                id: HistoryId(103),
                message_id: MessageId("m".into()),
            }],
            history_id: HistoryId(105),
            next_page_token: None,
        };
        let commit =
            first_commit(engine.handle(event(EventKind::HistoryFetched { command, page })))
                .expect("page commit");
        assert_eq!(
            commit.cursor,
            Some(CursorMove {
                expected: Some(HistoryId(100)),
                to: HistoryId(105)
            })
        );
        assert_eq!(
            engine.status().cursor,
            Some(HistoryId(100)),
            "not adopted before apply"
        );
        let _ = engine.handle(event(EventKind::CommitApplied { commit: commit.id }));
        assert_eq!(engine.status().cursor, Some(HistoryId(105)));
    }

    #[test]
    fn results_from_another_epoch_are_ignored() {
        let mut engine = live(100);
        let mut command = push(&mut engine, 105)[0].id;
        command.epoch = Epoch(0);
        let page = HistoryPage {
            records: Vec::new(),
            history_id: HistoryId(105),
            next_page_token: None,
        };
        assert_eq!(
            engine.handle(event(EventKind::HistoryFetched { command, page })),
            Vec::new()
        );
    }

    #[test]
    fn a_stale_epoch_rejection_stops_the_engine() {
        let mut engine = live(100);
        let command = push(&mut engine, 105)[0].id;
        let page = HistoryPage {
            records: Vec::new(),
            history_id: HistoryId(105),
            next_page_token: None,
        };
        let commit =
            first_commit(engine.handle(event(EventKind::HistoryFetched { command, page })))
                .expect("the last page moves the cursor");
        let _ = engine.handle(event(EventKind::CommitRejected {
            commit: commit.id,
            reason: Rejection::StaleEpoch,
        }));
        assert_eq!(
            engine.stopped(),
            Some(&Stopped::Superseded(Rejection::StaleEpoch))
        );
        assert!(push(&mut engine, 200).is_empty());
    }

    #[test]
    fn expired_history_starts_reconciliation() {
        let mut engine = live(100);
        let command = push(&mut engine, 105)[0].id;
        let commit = first_commit(engine.handle(event(EventKind::HistoryExpired { command })))
            .expect("lifecycle commit");
        assert!(matches!(commit.sync, Some(SyncState::Reconciling(_))));
        assert_eq!(engine.status().phase, SyncPhase::Reconciling);
    }

    /// A refresh listing in flight when reconciliation lists the labels is
    /// dropped: answered after the reconciliation's own, it would apply an
    /// older listing over a newer one.
    #[test]
    fn a_reconciliation_listing_supersedes_a_refresh_in_flight() {
        use crate::gmail::{Label, LabelKind};
        let mut engine = live(100);
        let started = engine.start(Timestamp(1_000));
        let listing = |commands: &[Command]| {
            commands
                .iter()
                .find(|c| {
                    matches!(&c.kind, CommandKind::Gmail(call) if call.request == GmailRequest::ListLabels)
                })
                .map(|c| c.id)
        };
        let refresh = listing(&started).expect("a refresh on start");
        let command = started
            .iter()
            .find(|c| {
                matches!(&c.kind, CommandKind::Gmail(call) if matches!(call.request, GmailRequest::ListHistory { .. }))
            })
            .expect("a history pass on start")
            .id;
        let commit = first_commit(engine.handle(event(EventKind::HistoryExpired { command })))
            .expect("lifecycle commit");
        let reconciling = engine.handle(event(EventKind::CommitApplied { commit: commit.id }));
        let reconcile = listing(&reconciling).expect("reconciliation lists the labels");
        let defined = |names: &[&str]| -> Vec<Label> {
            names
                .iter()
                .map(|id| Label::plain(LabelId((*id).into()), *id, LabelKind::System))
                .collect()
        };
        let late = engine.handle(event(EventKind::LabelsListed {
            command: refresh,
            labels: defined(&["INBOX", "UNREAD", "OLD"]),
            generation: 0,
            stored_user_labels: Vec::new(),
        }));
        assert!(first_commit(late).is_none(), "the older listing is ignored");
        let applied = first_commit(engine.handle(event(EventKind::LabelsListed {
            command: reconcile,
            labels: defined(&["INBOX", "UNREAD"]),
            generation: 0,
            stored_user_labels: Vec::new(),
        })))
        .expect("the reconciliation's listing applies");
        assert!(matches!(
            applied.labels.as_slice(),
            [LabelOp::ReplaceAll { labels, .. }] if *labels == defined(&["INBOX", "UNREAD"])
        ));
    }

    #[test]
    fn audit_rate_limits_publish_shared_cooldowns() {
        for class in [Retryable::ProjectRateLimited, Retryable::AccountRateLimited] {
            let mut engine = live(100);
            let now = Timestamp(10_000);
            let request = engine.on_timer(TimerKind::Audit, now).remove(0);
            let commands = engine.on_failed(
                request.id,
                &Failure::Retryable(RetryableFailure {
                    class,
                    retry_after: Some(Duration::from_secs(300)),
                    detail: "quota".into(),
                }),
                now,
            );
            let target = if class == Retryable::ProjectRateLimited {
                crate::retry::QuotaTarget::Project
            } else {
                crate::retry::QuotaTarget::Account(engine.state.account.clone())
            };
            assert!(commands.iter().any(|command| matches!(&command.kind,
                CommandKind::Commit(commit) if commit.account_ops.iter().any(|op|
                    matches!(op, AccountOp::SetCooldown(cooldown) if
                        cooldown.target == target && cooldown.until == now.plus(Duration::from_secs(300)))))));
        }
    }

    #[test]
    fn a_late_baseline_hold_runs_its_follow_up_and_frees_four_slots() {
        let mut driven = Driven::new(&["INBOX", "Label_ok"], Vec::new());
        driven.engine.state.sync =
            SyncState::Reconciling(ReconcileProgress::begin(ReconcileReason::HistoryExpired));
        driven
            .mirror
            .record
            .sync
            .clone_from(&driven.engine.state.sync);
        driven.mirror.record.cursor = Some(HistoryId(100));
        let listing = driven
            .engine
            .list_labels(Priority::Backfill, Waiting::Labels, Timestamp(0));
        driven.settle(vec![listing]);
        let row = driven.mirror.messages[&MessageId("m".into())].clone();
        for i in 0..JOB_CONCURRENCY {
            let id = MessageId(format!("held-{i}"));
            driven.mirror.messages.insert(
                id.clone(),
                crate::mirror::MessageRecord {
                    id: id.clone(),
                    ..row.clone()
                },
            );
            driven.mirror.admit_action(
                &ActionRequest::Messages {
                    ids: vec![id],
                    action: add_labels(&["Label_ok"]),
                },
                Timestamp(0),
            );
        }
        let claim = driven.engine.claim_jobs(Timestamp(0));
        driven.settle(claim);
        for _ in 0..JOB_CONCURRENCY {
            driven.refuse();
        }
        let import = MessageId("queued-import".into());
        driven.mirror.jobs.insert(
            JobKind::Import(import.clone()),
            crate::jobs::Job {
                kind: JobKind::Import(import.clone()),
                priority: Priority::Backfill,
                failed_attempts: 0,
                state: crate::jobs::JobState::Ready {
                    due_at: Timestamp(0),
                },
            },
        );
        driven.answer_labels(["INBOX", "UNREAD", "Label_ok"]);
        assert!(!driven.has(&GmailRequest::ListLabels));
        let profile = driven
            .requests
            .iter()
            .find(|(_, request)| *request == GmailRequest::GetProfile)
            .unwrap()
            .0;
        let commands = driven.engine.handle(event(EventKind::ProfileFetched {
            command: profile,
            history_id: HistoryId(100),
        }));
        driven.settle(commands);
        assert!(
            driven.has(&GmailRequest::ListLabels),
            "the queued follow-up is sent at baseline capture"
        );
        driven.answer_labels(["INBOX", "UNREAD", "Label_ok"]);
        for _ in 0..JOB_CONCURRENCY {
            driven.deliver();
        }
        assert!(driven.engine.parked.is_empty());
        assert!(driven.has(&GmailRequest::ImportMessage { id: import }));
    }

    #[test]
    fn audit_due_time_survives_restart_and_uses_backfill_quota() {
        let mut engine = live(100);
        let completed = Timestamp(10_000);
        engine.state.audited_at = Some(completed);
        let due = completed.plus(engine.config.audit_interval);
        let snapshot = engine.state.clone().validate().expect("live");
        for now in [completed, due.minus(Duration::from_millis(1))] {
            let mut restarted = Engine::new(snapshot.clone(), engine.config.clone());
            let out = restarted.start(now);
            assert!(out.iter().any(|command| command.kind
                == CommandKind::Schedule {
                    timer: TimerKind::Audit,
                    at: due,
                }));
            assert!(
                !out.iter()
                    .any(|command| matches!(&command.kind, CommandKind::Gmail(call)
                if matches!(call.request, GmailRequest::AuditMessages { .. })))
            );
        }
        let mut restarted = Engine::new(snapshot, engine.config);
        let out = restarted.on_timer(TimerKind::Audit, due);
        assert!(
            matches!(out.as_slice(), [Command { kind: CommandKind::Gmail(call), .. }]
            if call.request == GmailRequest::AuditMessages { page_token: None }
                && call.priority == Priority::Backfill && call.cost == quota::QuotaUnits(5))
        );
    }

    #[test]
    fn an_audit_does_one_step_per_claim_and_reports_store_counts() {
        let mut engine = live(100);
        let now = Timestamp(10_000);
        let page_token = PageToken("continued".into());
        engine.state.audit_progress = Some(AuditProgress::Listing(page_token.clone()));
        let request = engine.on_timer(TimerKind::Audit, now).remove(0);
        assert!(matches!(request.kind, CommandKind::Gmail(call)
            if call.request == GmailRequest::AuditMessages { page_token: Some(page_token) }));
        let checked = engine
            .on_audit_page(request.id, vec![MessageId("missing".into())], None, now)
            .remove(0);
        assert!(
            matches!(&checked.kind, CommandKind::AdvanceAudit(AuditRequest {
            step: AuditStep::Page { ids, next: None }, ..
        }) if ids == &[MessageId("missing".into())])
        );
        let report = AuditReport {
            found_missing: 1,
            found_unlisted: 0,
            abandoned: None,
        };
        let jobs = engine.on_audit_advanced(
            checked.id,
            AuditOutcome::Applied {
                completed_at: None,
                report,
                retry_at: None,
            },
            now,
        );
        assert!(matches!(
            jobs.as_slice(),
            [Command {
                kind: CommandKind::Schedule {
                    timer: TimerKind::Jobs,
                    ..
                },
                ..
            }]
        ));
        assert_eq!(engine.status().audit, report);
        assert_eq!(engine.status().cursor, Some(HistoryId(100)));
        assert_eq!(engine.audit_next_run(), Some(now));
        assert!(engine.on_timer(TimerKind::Audit, now).is_empty());

        let mut restarted = live(100);
        restarted.state.audit_progress = Some(AuditProgress::Sweep(None));
        let request = restarted.on_timer(TimerKind::Audit, now).remove(0);
        assert!(matches!(
            request.kind,
            CommandKind::AdvanceAudit(AuditRequest {
                step: AuditStep::Continue,
                ..
            })
        ));
        let report = AuditReport {
            found_missing: 0,
            found_unlisted: 1,
            abandoned: None,
        };
        restarted.on_audit_advanced(
            request.id,
            AuditOutcome::Applied {
                completed_at: None,
                report,
                retry_at: None,
            },
            now,
        );
        assert_eq!(restarted.status().audit, report);
    }

    #[test]
    fn an_oversized_audit_page_is_never_applied() {
        let mut engine = live(100);
        let now = Timestamp(0);
        let request = engine.on_timer(TimerKind::Audit, now).remove(0);
        let ids =
            vec![MessageId("invalid oversized page".into()); crate::audit::AUDIT_PAGE_SIZE + 1];
        let commands = engine.on_audit_page(request.id, ids, None, now);
        assert!(matches!(
            commands.as_slice(),
            [Command {
                kind: CommandKind::AdvanceAudit(AuditRequest {
                    step: AuditStep::Abandon(AuditAbandonment::MalformedPage),
                    ..
                }),
                ..
            }]
        ));
    }

    #[test]
    fn an_audit_waits_for_the_live_history_pass() {
        let mut engine = live(100);
        let now = Timestamp(0);
        let history = engine.request_pass().remove(0);
        assert!(engine.on_timer(TimerKind::Audit, now).is_empty());
        let done = engine.on_history(
            history.id,
            &HistoryPage {
                history_id: HistoryId(100),
                records: Vec::new(),
                next_page_token: None,
            },
            now,
        );
        assert!(done.iter().any(|cmd| matches!(
            cmd.kind,
            CommandKind::Schedule {
                timer: TimerKind::Jobs,
                ..
            }
        )));
        let claim = engine.on_timer(TimerKind::Jobs, now).remove(0);
        let commands = engine.on_claimed(claim.id, Vec::new(), None, now);
        assert!(
            commands
                .iter()
                .any(|cmd| matches!(&cmd.kind, CommandKind::Gmail(call)
            if matches!(call.request, GmailRequest::AuditMessages { .. })))
        );
    }

    #[test]
    fn an_account_problem_blocks_with_its_reason() {
        let mut engine = live(100);
        let command = push(&mut engine, 105)[0].id;
        let reason = BlockingFailure {
            class: Blocking::Account(crate::retry::AccountProblem::DelegationRevoked),
            detail: "unauthorized_client".into(),
        };
        let commands = engine.handle(event(EventKind::RequestFailed {
            command,
            failure: Failure::Blocking(reason.clone()),
        }));
        let commit = first_commit(commands).expect("blocking is committed");
        // The store wraps whatever state it holds when the block applies.
        assert_eq!(commit.sync, None);
        assert_eq!(commit.account_ops, vec![AccountOp::Block(reason.clone())]);
        assert_eq!(engine.stopped(), Some(&Stopped::Blocked(reason)));
    }

    #[test]
    fn a_rejection_of_another_engines_commit_is_ignored() {
        let mut engine = live(100);
        let foreign = CommitId {
            epoch: Epoch(0),
            seq: 7,
        };
        let commands = engine.handle(event(EventKind::CommitRejected {
            commit: foreign,
            reason: Rejection::StaleEpoch,
        }));
        assert!(commands.is_empty());
        assert_eq!(engine.stopped(), None);
    }
}
