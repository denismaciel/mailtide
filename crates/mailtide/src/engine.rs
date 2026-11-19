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

use std::collections::BTreeMap;
use std::time::Duration;

use crate::command::{Command, CommandKind, GmailCall, GmailRequest};
use crate::commit::{
    AccountOp, Commit, CursorMove, DeletionEvidence, JobPlan, LabelOp, MessageOp, Rejection,
};
use crate::event::{Event, EventKind};
use crate::gmail::{HistoryPage, ImportedMessage, Label, ListPage, MessageObservation};
use crate::history::{self, CursorUpdate};
use crate::ids::{
    CommandId, CommitId, ENGINE_VERSION, HistoryId, LabelId, MessageId, PageToken, Timestamp,
};
use crate::jobs::{AttemptId, ClaimedJob, JobKind, JobOp, JobOutcome};
use crate::outbox::{ActionRequest, IntentOp};
use crate::quota::{self, Priority};
use crate::reconcile::{ListScope, ReconcileProgress, ReconcileReason, ReconcileStep};
use crate::retry::{
    Backoff, Blocking, BlockingFailure, Cooldown, Failure, QuotaTarget, Retryable, RetryableFailure,
};
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
    /// Renew the watch this long before it expires.
    pub watch_renew_margin: Duration,
    /// Delay before importing a draft's body, since drafts churn on save.
    pub draft_settle: Duration,
    /// After delivering an action, confirm it with a `get` this much later;
    /// until then it overlays (SPEC §7).
    pub confirm_timeout: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            poll_active: Duration::from_mins(1),
            poll_idle: Duration::from_mins(15),
            idle_after: Duration::from_hours(1),
            watch_renew_margin: Duration::from_hours(1),
            draft_settle: Duration::from_mins(2),
            confirm_timeout: Duration::from_mins(1),
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
    /// Bookkeeping (labels, watch, cooldown, blocking): nothing to continue.
    Bookkeeping,
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
    /// The highest history id a push or page reported.
    notified: Option<HistoryId>,
    /// Another pass is needed once the current one ends.
    follow_up: bool,
    /// Jobs claimed and not yet finished.
    running_jobs: u32,
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
            notified: None,
            follow_up: false,
            running_jobs: 0,
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

    /// The first commands after start: schedule timers, resume any
    /// reconciliation, catch up on history.
    #[must_use]
    pub fn start(&mut self, now: Timestamp) -> Vec<Command> {
        if let SyncState::Blocked { reason, .. } = &self.state.sync {
            self.stopped = Some(Stopped::Blocked(reason.clone()));
            return vec![self.status_command()];
        }
        let poll = self.next_poll(now);
        let renewal = schedule::next_watch_renewal(
            self.state.watch_expiration,
            now,
            self.config.watch_renew_margin,
        );
        let mut out = vec![
            self.schedule(TimerKind::Poll, poll),
            self.schedule(TimerKind::WatchRenewal, renewal),
            self.schedule_jobs(now),
        ];
        out.extend(self.resume_sync(now));
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
            EventKind::LabelsListed { command, labels } => self.on_labels(command, labels, now),
            EventKind::WatchRenewed {
                command,
                expiration,
            } => self.on_watch(command, expiration, now),
            EventKind::CommitApplied { commit } => self.on_applied(commit, now),
            EventKind::CommitRejected { commit, reason } => self.on_rejected(commit, reason, now),
            EventKind::JobsClaimed { command, jobs } => self.on_claimed(command, jobs, now),
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
            EventKind::RequestDeferred { command, until } => self.on_deferred(command, until),
            EventKind::ActionRequested { action } => self.on_action(action, now),
            EventKind::LabelsModified { command } => self.on_modified(command, now),
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
            import: self.state.sync.reconcile().map(|progress| ImportProgress {
                discovered: self.discovered,
                imported: self.imported,
                discovery_complete: matches!(
                    progress.step,
                    ReconcileStep::Verifying { .. } | ReconcileStep::Complete { .. }
                ),
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
            TimerKind::WatchRenewal => {
                vec![self.gmail(GmailRequest::Watch, Priority::Live, Waiting::Watch)]
            }
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
                out
            } // Phase 6 timers: nothing to do until the write path lands.
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
            || self.waiting.values().any(|w| match w {
                Waiting::History | Waiting::Labels | Waiting::Baseline | Waiting::Listing => true,
                Waiting::Watch | Waiting::Claim | Waiting::Job(_) => false,
            })
            || self.after.values().any(|a| match a {
                AfterCommit::HistoryPage { .. }
                | AfterCommit::Reconcile
                | AfterCommit::ReconcileDone(_) => true,
                AfterCommit::Job | AfterCommit::Bookkeeping | AfterCommit::Action => false,
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
            return self.after_history_page(None, fold.next_page, now);
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
        let progress = ReconcileProgress::begin(reason);
        let sync = match (reason, self.state.cursor) {
            (ReconcileReason::Bootstrap, None) => SyncState::Bootstrapping(progress),
            (ReconcileReason::Bootstrap, Some(_))
            | (
                ReconcileReason::HistoryExpired
                | ReconcileReason::HistoryRewound
                | ReconcileReason::Requested,
                _,
            ) => SyncState::Reconciling(progress),
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
                vec![self.gmail(
                    GmailRequest::ListLabels,
                    Priority::Backfill,
                    Waiting::Labels,
                )]
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
            ReconcileStep::Verifying { .. } | ReconcileStep::Complete { .. } => {
                self.finish_reconcile(progress, now)
            }
        }
    }

    fn on_labels(
        &mut self,
        command: CommandId,
        labels: Vec<Label>,
        now: Timestamp,
    ) -> Vec<Command> {
        if self.waiting.remove(&command) != Some(Waiting::Labels) {
            return Vec::new();
        }
        self.label_ids = labels.iter().map(|label| label.id.clone()).collect();
        let mut commit = self.commit(now, Priority::Backfill);
        commit.labels = vec![LabelOp::ReplaceAll(labels)];
        vec![
            self.send_commit(commit, AfterCommit::Bookkeeping),
            // The baseline must be captured before anything is listed.
            self.gmail(
                GmailRequest::GetProfile,
                Priority::Backfill,
                Waiting::Baseline,
            ),
        ]
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
            return vec![self.gmail(
                GmailRequest::ListLabels,
                Priority::Backfill,
                Waiting::Labels,
            )];
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

    // ------------------------------------------------------------ jobs

    fn claim_jobs(&mut self, now: Timestamp) -> Vec<Command> {
        if self.claiming || self.running_jobs >= JOB_CONCURRENCY || self.cooling_down(now) {
            return Vec::new();
        }
        self.claiming = true;
        let attempt = self.attempt();
        let id = self.command_id();
        self.waiting.insert(id, Waiting::Claim);
        vec![Command {
            id,
            kind: CommandKind::ClaimJobs {
                account: self.state.account.clone(),
                epoch: self.state.epoch,
                limit: JOB_CONCURRENCY - self.running_jobs,
                now,
                attempt,
            },
        }]
    }

    fn on_claimed(
        &mut self,
        command: CommandId,
        jobs: Vec<ClaimedJob>,
        now: Timestamp,
    ) -> Vec<Command> {
        if self.waiting.remove(&command) != Some(Waiting::Claim) {
            return Vec::new();
        }
        self.claiming = false;
        let mut out = Vec::new();
        for job in jobs {
            self.running_jobs += 1;
            let priority = job.priority;
            match &job.kind {
                JobKind::Import(id) => {
                    let request = GmailRequest::ImportMessage { id: id.clone() };
                    out.push(self.gmail(request, priority, Waiting::Job(job)));
                }
                JobKind::Verify(id) => {
                    let request = GmailRequest::GetMessage { id: id.clone() };
                    out.push(self.gmail(request, priority, Waiting::Job(job)));
                }
                JobKind::Deliver(_) => match job.modify.clone() {
                    Some(group) => {
                        out.push(self.gmail(
                            GmailRequest::Modify(group),
                            priority,
                            Waiting::Job(job),
                        ));
                    }
                    // Settled or superseded since it was enqueued.
                    None => {
                        out.push(self.finish_job(&job, JobOutcome::Done, Vec::new(), now));
                    }
                },
                JobKind::BlobCleanup(message) => {
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
            Some(Waiting::Job(job)) if answers(&job.kind) => Some(job),
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
        let versions = job
            .modify
            .as_ref()
            .map(|g| g.versions.clone())
            .unwrap_or_default();
        let intents = vec![IntentOp::Delivered {
            versions,
            at: now,
            by: job.attempt,
        }];
        vec![self.finish_job_with(&job, JobOutcome::Done, Vec::new(), intents, now)]
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
        let ops = vec![MessageOp::MarkImported {
            observation: message.observation,
            blob: message.blob,
            size: message.size,
            internal_date: message.internal_date,
            headers: Box::new(message.headers),
        }];
        vec![self.finish_job(&job, JobOutcome::Done, ops, now)]
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
        vec![self.finish_job(
            &job,
            JobOutcome::Done,
            vec![MessageOp::Observe(message)],
            now,
        )]
    }

    fn on_not_found(&mut self, command: CommandId, id: MessageId, now: Timestamp) -> Vec<Command> {
        let answers = |kind: &JobKind| match kind {
            JobKind::Import(m) | JobKind::Verify(m) | JobKind::Deliver(m) => *m == id,
            JobKind::BlobCleanup(_) => false,
        };
        let Some(job) = self.take_job(command, answers) else {
            return Vec::new();
        };
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
                out
            }
            AfterCommit::ReconcileDone(cursor) => {
                self.state.sync = SyncState::Live;
                self.adopt_cursor(cursor, now);
                let mut out = vec![self.status_command(), self.schedule_jobs(now)];
                out.extend(self.request_pass());
                out
            }
            AfterCommit::Job => {
                self.running_jobs = self.running_jobs.saturating_sub(1);
                self.claim_jobs(now)
            }
            AfterCommit::Action => self.claim_jobs(now),
            AfterCommit::Bookkeeping => Vec::new(),
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

    fn on_deferred(&mut self, command: CommandId, until: Timestamp) -> Vec<Command> {
        match self.waiting.remove(&command) {
            Some(Waiting::Job(job)) => vec![
                self.finish_job(&job, JobOutcome::Defer { due_at: until }, Vec::new(), until),
                self.schedule_jobs(until),
            ],
            Some(Waiting::Claim) => {
                self.claiming = false;
                vec![self.schedule_jobs(until)]
            }
            Some(Waiting::History | Waiting::Labels | Waiting::Baseline | Waiting::Listing) => {
                self.pass = None;
                vec![self.schedule(TimerKind::Poll, until)]
            }
            Some(Waiting::Watch) => vec![self.schedule(TimerKind::WatchRenewal, until)],
            None => Vec::new(),
        }
    }

    fn on_failed(&mut self, command: CommandId, failure: &Failure, now: Timestamp) -> Vec<Command> {
        let Some(waiting) = self.waiting.remove(&command) else {
            return Vec::new();
        };
        match failure {
            Failure::Retryable(retryable) => {
                let mut out = self.cool_down(retryable, now);
                out.extend(self.retry(waiting, retryable, now));
                out
            }
            Failure::Blocking(blocking) => self.on_refused(command, waiting, blocking, now),
            Failure::NotFound => self.on_missing(command, waiting, now),
        }
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
            // The account itself is unusable: everything stops.
            (Blocking::Account(_), _) => self.block(blocking.clone(), now),
            (Blocking::Permanent, Waiting::Job(job)) => match &job.kind {
                // This change can't be applied: block its intents (they stop
                // overlaying) and finish the job.
                JobKind::Deliver(_) => {
                    let versions = job
                        .modify
                        .as_ref()
                        .map(|g| g.versions.clone())
                        .unwrap_or_default();
                    let intents = vec![IntentOp::Block {
                        versions,
                        failure: blocking.clone(),
                    }];
                    vec![self.finish_job_with(&job, JobOutcome::Done, Vec::new(), intents, now)]
                }
                JobKind::Import(_) | JobKind::Verify(_) | JobKind::BlobCleanup(_) => {
                    vec![self.finish_job(
                        &job,
                        JobOutcome::Block(blocking.clone()),
                        Vec::new(),
                        now,
                    )]
                }
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
            (Blocking::Permanent, Waiting::Claim) => {
                // Claims go to the store, which doesn't answer with Gmail
                // failures; free the slot regardless.
                let _ = command;
                self.claiming = false;
                Vec::new()
            }
            // The watch can't be renewed: polling still finds changes; the
            // next engine tries again.
            (Blocking::Permanent, Waiting::Watch) => Vec::new(),
        }
    }

    /// Gmail answered 404.
    fn on_missing(&mut self, command: CommandId, waiting: Waiting, now: Timestamp) -> Vec<Command> {
        match waiting {
            Waiting::Job(job) => match &job.kind {
                JobKind::Import(id) | JobKind::Verify(id) | JobKind::Deliver(id) => {
                    let id = id.clone();
                    self.waiting.insert(command, Waiting::Job(job));
                    self.on_not_found(command, id, now)
                }
                // Already gone: nothing to clean up.
                JobKind::BlobCleanup(_) => {
                    vec![self.finish_job(&job, JobOutcome::Done, Vec::new(), now)]
                }
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
            Waiting::Claim => {
                self.claiming = false;
                Vec::new()
            }
            Waiting::Watch => Vec::new(),
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
                let outcome = JobOutcome::Retry {
                    failure: failure.clone(),
                    due_at,
                };
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
                vec![self.schedule(TimerKind::Poll, at)]
            }
            Waiting::Watch => {
                let at = self.backoff.next(0, failure, now, seed);
                vec![self.schedule(TimerKind::WatchRenewal, at)]
            }
        }
    }

    /// A cooldown for rate limits that apply beyond one request.
    fn cool_down(&mut self, failure: &RetryableFailure, now: Timestamp) -> Vec<Command> {
        let seed = self.jitter_seed();
        let account = QuotaTarget::Account(self.state.account.clone());
        let cooldown = match failure.class {
            Retryable::AccountRateLimited | Retryable::AccountConcurrency => Some(Cooldown {
                target: account,
                until: self.backoff.next(1, failure, now, seed),
            }),
            Retryable::AccountBandwidth => Some(Cooldown {
                target: account,
                until: now.plus(BANDWIDTH_COOLDOWN.max(failure.retry_after.unwrap_or_default())),
            }),
            Retryable::ProjectRateLimited => Some(Cooldown {
                target: QuotaTarget::Project,
                until: self.backoff.next(1, failure, now, seed),
            }),
            Retryable::Transient => None,
        };
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
            },
            jobs: Vec::new(),
            intents: Vec::new(),
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
        };
        Engine::new(
            snapshot.validate().expect("valid snapshot"),
            EngineConfig::default(),
        )
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
                | CommandKind::ReportStatus(_) => None,
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
            | CommandKind::ReportStatus(_) => None,
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
            kind: JobKind::Import(MessageId("a".into())),
            priority: Priority::Live,
            failed_attempts: 0,
            attempt: AttemptId(1),
            modify: None,
        };
        let claim = engine.claim_jobs(Timestamp(0));
        let claim = claim.first().expect("a claim").id;
        let commands = engine.handle(event(EventKind::JobsClaimed {
            command: claim,
            jobs: vec![job],
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
