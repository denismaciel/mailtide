//! The loop: build an engine from the store, feed it events, perform its
//! commands, until the account is caught up or the budget runs out. Hosts call
//! it from their scheduler (a cron job, a queue worker, a durable workflow). Each run
//! claims the account (a new epoch), so overlapping runs fence each other.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use tracing::Instrument as _;

use mailtide::command::{Command, CommandKind, GmailCall, GmailRequest};
use mailtide::commit::{AccountOp, CommitOutcome, Rejection};
use mailtide::engine::Stopped;
use mailtide::event::{Event, EventKind};
use mailtide::gmail::ImportedMessage;
use mailtide::ids::{AccountId, CommandId, ENGINE_VERSION, HistoryId, MessageId, Timestamp};
use mailtide::jobs::ClaimOutcome;
use mailtide::outbox::ActionRequest;
use mailtide::quota::Admission;
use mailtide::retry::{Blocking, BlockingFailure, Failure, Retryable, RetryableFailure};
use mailtide::schedule::TimerKind;
use mailtide::status::{AccountStatus, ForgetOutcome, ResumeOutcome, Status};
use mailtide::{Engine, EngineConfig};
use mailtide_gmail::request::{self, RequestConfig};
use mailtide_gmail::{decode, errors, headers};
use mailtide_store::{Store, StoreError};

use crate::auth::TokenSource;
use crate::blobs::{BlobStore, blob_key};
use crate::quota_gate::QuotaGate;
use crate::transport::{GmailTransport, TransportError};

/// How the runner treats `users.watch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchMode {
    /// Publish changes to this Pub/Sub topic (`projects/p/topics/t`).
    Topic(String),
    /// No push: the engine never calls `users.watch`; polling (or the
    /// host's own verified pushes) finds changes.
    Disabled,
}

impl WatchMode {
    /// The engine policy this mode implies.
    #[must_use]
    pub fn policy(&self) -> mailtide::engine::WatchPolicy {
        match self {
            Self::Topic(_) => mailtide::engine::WatchPolicy::Renew,
            Self::Disabled => mailtide::engine::WatchPolicy::Off,
        }
    }
}

/// Whether the runner may change the mailbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Access {
    /// Deliver users' actions to Gmail.
    #[default]
    ReadWrite,
    /// Shadow mode: mirror only. Admitting actions is refused, and actions
    /// admitted before the switch are blocked (visible as blocked) rather
    /// than sent. Works with the `gmail.readonly` scope.
    ReadOnly,
}

/// Runner settings.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Whether actions reach Gmail.
    pub access: Access,
    /// Engine tunables. Its `watch` policy is ignored: `watch` decides it.
    pub engine: EngineConfig,
    /// Push notifications.
    pub watch: WatchMode,
    /// How accounts appear in logs.
    pub log_accounts: LogAccounts,
}

/// How accounts (email addresses) appear in log events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogAccounts {
    /// A stable 16-hex-digit hash: correlatable, not personal data.
    #[default]
    Hashed,
    /// The address itself.
    Plain,
}

/// A running sync's progress, for heartbeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    /// Commits applied so far.
    pub commits: u32,
    /// Messages imported so far.
    pub imported: u32,
    /// Gmail requests sent so far.
    pub gmail_requests: u32,
    /// Time since the run started.
    pub elapsed: Duration,
}

/// Progress reports go out at most this often.
const PROGRESS_EVERY: Duration = Duration::from_secs(1);

impl StopReason {
    /// A stable name for logs and hosts.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Idle { .. } => "idle",
            Self::BudgetExhausted => "budget_exhausted",
            Self::CoolingDown { .. } => "cooling_down",
            Self::Blocked(_) => "blocked",
            Self::Superseded => "superseded",
        }
    }
}

/// The request kind, for logs (no ids).
fn request_kind(request: &GmailRequest) -> &'static str {
    match request {
        GmailRequest::GetProfile => "get_profile",
        GmailRequest::ListHistory { .. } => "list_history",
        GmailRequest::ListMessages { .. } => "list_messages",
        GmailRequest::GetMessage { .. } => "get_message",
        GmailRequest::ImportMessage { .. } => "import_message",
        GmailRequest::ListLabels => "list_labels",
        GmailRequest::Watch => "watch",
        GmailRequest::Modify(_) => "modify",
    }
}

/// Everything the runner needs.
#[derive(Debug)]
pub struct Runner<S, B, T, Q, A> {
    store: S,
    blobs: B,
    transport: T,
    quota: Q,
    tokens: A,
    config: RunnerConfig,
    clock: crate::Clock,
}

/// What one run did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SyncReport {
    /// The cursor before.
    pub cursor_before: Option<HistoryId>,
    /// The cursor after.
    pub cursor_after: Option<HistoryId>,
    /// Commits applied.
    pub commits: u32,
    /// Messages imported.
    pub imported: u32,
    /// Gmail requests sent.
    pub gmail_requests: u32,
    /// For [`Runner::act`]: messages the action admitted intents for.
    pub admitted: u32,
    /// The account's status at the end.
    pub status: Option<Status>,
}

/// Why a run stopped (review #16): expected outcomes, not errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    /// Nothing left to do now; the next scheduled work is at `next`, if any.
    Idle {
        /// The earliest scheduled work.
        next: Option<Timestamp>,
    },
    /// The time budget ran out with work left.
    BudgetExhausted,
    /// Gmail asked us to slow down until `until`.
    CoolingDown {
        /// When work can resume.
        until: Timestamp,
    },
    /// The account is blocked until an operator acts.
    Blocked(BlockingFailure),
    /// Another run took the account over.
    Superseded,
}

/// The outcome of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOutcome {
    /// What happened.
    pub report: SyncReport,
    /// Why it stopped.
    pub stop: StopReason,
}

/// What forgetting an account removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Forgotten {
    /// What the store removed.
    pub store: ForgetOutcome,
    /// Raw content objects deleted.
    pub blobs: u64,
}

/// Infrastructure failures (the run may be retried).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunError {
    /// The store failed or the outcome of a commit is unknown.
    Store(StoreError),
    /// The stored state didn't validate.
    Corrupt(String),
    /// The quota gate failed.
    Quota(String),
    /// Blob storage failed.
    Blobs(String),
    /// mailtide broke one of its own rules (a bug; please report it).
    Invariant(&'static str),
    /// The runner is read-only: actions are refused.
    ReadOnly,
}

impl RunError {
    /// A stable name for logs (no detail: it may carry data).
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Store(StoreError::Unavailable(_)) => "store_unavailable",
            Self::Store(StoreError::OutcomeUnknown(_)) => "outcome_unknown",
            Self::Store(StoreError::Corrupt(_)) | Self::Corrupt(_) => "corrupt",
            Self::Store(StoreError::Misconfigured(_)) => "misconfigured",
            Self::Store(StoreError::Rejected(_)) => "superseded",
            Self::Quota(_) => "quota_gate",
            Self::Blobs(_) => "blobs",
            Self::Invariant(_) => "invariant",
            Self::ReadOnly => "read_only",
        }
    }
}

/// The failure class, for logs (no detail: Gmail error text can quote data).
fn failure_kind(failure: &Failure) -> &'static str {
    match failure {
        Failure::NotFound => "not_found",
        Failure::Retryable(retryable) => match retryable.class {
            Retryable::Transient => "transient",
            Retryable::AccountRateLimited => "account_rate_limited",
            Retryable::AccountConcurrency => "account_concurrency",
            Retryable::AccountBandwidth => "account_bandwidth",
            Retryable::ProjectRateLimited => "project_rate_limited",
        },
        Failure::Blocking(blocking) => match blocking.class {
            Blocking::Permanent => "refused",
            Blocking::Account(_) => "account_problem",
        },
    }
}

/// What a push did.
#[derive(Debug, Clone)]
pub enum Notified {
    /// The mirror had already reached the pushed history id.
    Current,
    /// The mirror was behind: this run synced it.
    Ran(Box<RunOutcome>),
    /// The account isn't synced here; pushes don't start accounts.
    UnknownAccount,
}

/// One message of a drift check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checked {
    /// Both sides were read and compared.
    Compared(mailtide::drift::Verdict),
    /// The quota gate deferred the fetch: try later.
    Deferred,
    /// Gmail failed the fetch (its failure class).
    Failed(&'static str),
}

/// One Gmail response, decoded into the engine's terms.
enum Outcome {
    Event(EventKind),
    Failed(Failure),
}

impl<S, B, T, Q, A> Runner<S, B, T, Q, A>
where
    S: Store,
    B: BlobStore,
    T: GmailTransport,
    Q: QuotaGate,
    A: TokenSource,
{
    /// A runner over these resources.
    pub fn new(
        store: S,
        blobs: B,
        transport: T,
        quota: Q,
        tokens: A,
        config: RunnerConfig,
    ) -> Self {
        Self {
            store,
            blobs,
            transport,
            quota,
            tokens,
            config,
            clock: crate::Clock::system(),
        }
    }

    fn writable(&self) -> Result<(), RunError> {
        match self.config.access {
            Access::ReadWrite => Ok(()),
            Access::ReadOnly => Err(RunError::ReadOnly),
        }
    }

    /// Read the time from `clock` instead of the system.
    #[must_use]
    pub fn with_clock(mut self, clock: crate::Clock) -> Self {
        self.clock = clock;
        self
    }

    /// The store, for reads.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The blob store, for reads.
    pub fn blobs(&self) -> &B {
        &self.blobs
    }

    /// The transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Admit a user action without delivering it: the next run of the
    /// account (whichever process holds it) delivers. Returns how many
    /// messages it reached. Use this from request paths.
    pub async fn admit(
        &self,
        account: &AccountId,
        action: &ActionRequest,
    ) -> Result<u32, RunError> {
        self.writable()?;
        self.store
            .admit(account, action, self.clock.now())
            .await
            .map_err(RunError::Store)
    }

    /// What the store holds about `account`, without claiming it.
    pub async fn status(&self, account: &AccountId) -> Result<Option<AccountStatus>, RunError> {
        self.store.status(account).await.map_err(RunError::Store)
    }

    /// Every account the store holds.
    pub async fn accounts(&self) -> Result<Vec<AccountId>, RunError> {
        self.store.accounts().await.map_err(RunError::Store)
    }

    /// Compare the mirror with Gmail for `ids` (a sample the host picks),
    /// read-only: nothing is fixed, and the requests go through the quota
    /// gate like a sync's, waiting out its deferrals within `budget`. Ids the
    /// mirror doesn't have are left out.
    pub async fn drift(
        &self,
        account: &AccountId,
        ids: &[MessageId],
        budget: Duration,
    ) -> Result<Vec<(MessageId, Checked)>, RunError> {
        let deadline = Instant::now() + budget;
        let views = self
            .store
            .messages(account, ids)
            .await
            .map_err(RunError::Store)?;
        let mut report = SyncReport::default();
        let mut checked = Vec::with_capacity(views.len());
        for (seq, view) in (0_u64..).zip(views) {
            let request = GmailRequest::GetMessage {
                id: view.id.clone(),
            };
            let call = GmailCall {
                account: account.clone(),
                cost: mailtide::quota::cost(&request),
                request,
                priority: mailtide::quota::Priority::Backfill,
            };
            // Not an engine's command: epoch 0 never matches a live engine.
            let command = CommandId {
                epoch: mailtide::ids::Epoch(0),
                seq,
            };
            let mut answer = self.gmail(command, call.clone(), &mut report).await?;
            while let EventKind::RequestDeferred { until, .. } = answer {
                let wait =
                    Duration::from_millis(u64::try_from(until.0 - self.clock.now().0).unwrap_or(0));
                if !self.clock.can_wait() || Instant::now() + wait >= deadline {
                    break;
                }
                tokio::time::sleep(wait).await;
                answer = self.gmail(command, call.clone(), &mut report).await?;
            }
            let result = match answer {
                EventKind::MessageFetched { message, .. } => {
                    Checked::Compared(mailtide::drift::compare(&view, Some(&message)))
                }
                EventKind::MessageNotFound { .. } => {
                    Checked::Compared(mailtide::drift::compare(&view, None))
                }
                EventKind::RequestDeferred { .. } => Checked::Deferred,
                EventKind::RequestFailed { failure, .. } => Checked::Failed(failure_kind(&failure)),
                _ => return Err(RunError::Invariant("unexpected answer to a message get")),
            };
            checked.push((view.id, result));
        }
        Ok(checked)
    }

    /// Resume a blocked account once its cause is fixed.
    pub async fn resume(&self, account: &AccountId) -> Result<ResumeOutcome, RunError> {
        self.store
            .resume(account, self.clock.now())
            .await
            .map_err(RunError::Store)
    }

    /// Delete everything stored for `account`: rows, then raw content.
    pub async fn forget(&self, account: &AccountId) -> Result<Forgotten, RunError> {
        let store = self.store.forget(account).await.map_err(RunError::Store)?;
        let blobs = self
            .blobs
            .delete_account(account)
            .await
            .map_err(|e| RunError::Blobs(format!("{e:?}")))?;
        Ok(Forgotten { store, blobs })
    }

    /// Admit a user action and deliver it, syncing as [`Self::run`] does.
    /// Admission goes straight to the store, so it holds even when this run
    /// is superseded by a concurrent one (which then delivers it).
    pub async fn act(
        &self,
        account: &AccountId,
        action: ActionRequest,
        budget: Duration,
    ) -> Result<RunOutcome, RunError> {
        self.writable()?;
        let admitted = self
            .store
            .admit(account, &action, self.clock.now())
            .await
            .map_err(RunError::Store)?;
        let mut outcome = self.run(account, budget).await?;
        outcome.report.admitted = admitted;
        Ok(outcome)
    }

    /// How `account` appears in logs.
    fn log_account(&self, account: &AccountId) -> String {
        match self.config.log_accounts {
            LogAccounts::Hashed => format!(
                "{:016x}",
                mailtide::ids::stable_hash(account.as_str().as_bytes())
            ),
            LogAccounts::Plain => account.as_str().to_owned(),
        }
    }

    /// A verified push (the host's own, e.g. its existing Pub/Sub handler)
    /// says Gmail reached `history_id`: sync only if the mirror is behind it.
    /// Unknown accounts are not started by a push.
    pub async fn notify(
        &self,
        account: &AccountId,
        history_id: HistoryId,
        budget: Duration,
    ) -> Result<Notified, RunError> {
        let status = self.store.status(account).await.map_err(RunError::Store)?;
        match status {
            None => Ok(Notified::UnknownAccount),
            Some(status) if status.cursor.is_some_and(|cursor| cursor >= history_id) => {
                Ok(Notified::Current)
            }
            Some(_) => self
                .run(account, budget)
                .await
                .map(|outcome| Notified::Ran(Box::new(outcome))),
        }
    }

    /// Sync `account` until caught up, stopped, or `budget` elapses.
    pub async fn run(&self, account: &AccountId, budget: Duration) -> Result<RunOutcome, RunError> {
        self.run_reporting(account, budget, None).await
    }

    /// [`Self::run`], calling `progress` between commands at most once a
    /// second (for heartbeats).
    pub async fn run_reporting(
        &self,
        account: &AccountId,
        budget: Duration,
        progress: Option<&(dyn Fn(&Progress) + Sync)>,
    ) -> Result<RunOutcome, RunError> {
        let span = tracing::info_span!("mailtide.run", account = %self.log_account(account));
        let started = Instant::now();
        let outcome = self
            .run_inner(account, budget, progress)
            .instrument(span.clone())
            .await;
        let _entered = span.enter();
        match &outcome {
            Ok(RunOutcome { report, stop }) => {
                if let StopReason::Blocked(failure) = stop {
                    tracing::error!(class = ?failure.class, "account blocked; resume it once the cause is fixed");
                }
                tracing::info!(
                    stop = stop.code(),
                    commits = report.commits,
                    imported = report.imported,
                    gmail_requests = report.gmail_requests,
                    duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "run finished"
                );
            }
            Err(error) => tracing::warn!(error = error.code(), "run failed"),
        }
        outcome
    }

    /// Claim the account and build its engine; `None` when a newer engine
    /// holds it.
    async fn claim_engine(&self, account: &AccountId) -> Result<Option<Engine>, RunError> {
        let snapshot = match self.store.claim(account, ENGINE_VERSION).await {
            Ok(snapshot) => snapshot,
            Err(StoreError::Rejected(_)) => return Ok(None),
            Err(error) => return Err(RunError::Store(error)),
        };
        let snapshot = snapshot
            .validate()
            .map_err(|e| RunError::Corrupt(format!("{e:?}")))?;
        let config = EngineConfig {
            watch: self.config.watch.policy(),
            ..self.config.engine.clone()
        };
        Ok(Some(Engine::new(snapshot, config)))
    }

    /// Queued work (jobs deferred by the quota gate or backing off, a
    /// cooldown ending) due within the budget: the account isn't caught up,
    /// so the run waits for it. Polls and watch renewals end the run; the
    /// host schedules those. Only on the system clock: a host clock moves
    /// when the host moves it.
    fn due_soon(
        &self,
        timers: &BTreeMap<TimerKind, Timestamp>,
        now: Timestamp,
        deadline: Instant,
    ) -> Option<Duration> {
        if !self.clock.can_wait() {
            return None;
        }
        timers
            .iter()
            .filter(|(timer, _)| match timer {
                TimerKind::Jobs | TimerKind::CooldownOver => true,
                TimerKind::Poll | TimerKind::WatchRenewal => false,
            })
            .map(|(_, at)| Duration::from_millis(u64::try_from(at.0 - now.0).unwrap_or(0)))
            .min()
            .filter(|wait| Instant::now() + *wait < deadline)
    }

    async fn run_inner(
        &self,
        account: &AccountId,
        budget: Duration,
        progress: Option<&(dyn Fn(&Progress) + Sync)>,
    ) -> Result<RunOutcome, RunError> {
        let started = Instant::now();
        let mut reported = started;
        let deadline = started + budget;
        let Some(mut engine) = self.claim_engine(account).await? else {
            return Ok(RunOutcome {
                report: SyncReport::default(),
                stop: StopReason::Superseded,
            });
        };
        let mut report = SyncReport {
            cursor_before: engine.status().cursor,
            ..SyncReport::default()
        };
        let mut queue: VecDeque<Command> = engine.start(self.clock.now()).into();
        let mut timers: BTreeMap<TimerKind, Timestamp> = BTreeMap::new();
        loop {
            while let Some(command) = queue.pop_front() {
                if let Some(stop) = stopped(&engine) {
                    // The engine stopped (blocked or superseded) but may have
                    // queued guarded commits, e.g. the block itself: apply
                    // them so the outcome is durable; the store rejects any
                    // that are stale.
                    queue.push_front(command);
                    let refused = self.flush_commits(&mut queue, &mut report).await?;
                    // A block the store refused never happened: another
                    // engine holds the account.
                    let stop = match (stop, refused) {
                        (StopReason::Blocked(_), Some(_)) => StopReason::Superseded,
                        (stop, _) => stop,
                    };
                    return Ok(finish(report, &engine, stop));
                }
                if Instant::now() > deadline {
                    return Ok(finish(report, &engine, StopReason::BudgetExhausted));
                }
                if let Some(progress) = progress
                    && reported.elapsed() >= PROGRESS_EVERY
                {
                    reported = Instant::now();
                    progress(&Progress {
                        commits: report.commits,
                        imported: report.imported,
                        gmail_requests: report.gmail_requests,
                        elapsed: started.elapsed(),
                    });
                }
                let id = command.id;
                match command.kind {
                    CommandKind::Schedule { timer, at } => {
                        timers.insert(timer, at);
                    }
                    CommandKind::ReportStatus(status) => report.status = Some(status),
                    kind @ (CommandKind::Gmail(_)
                    | CommandKind::Commit(_)
                    | CommandKind::ClaimJobs { .. }
                    | CommandKind::DeleteBlob { .. }) => {
                        if let Some(event) = self.execute(id, kind, &mut report).await? {
                            queue.extend(engine.handle(Event {
                                now: self.clock.now(),
                                kind: event,
                            }));
                        }
                    }
                }
            }
            if let Some(stop) = stopped(&engine) {
                return Ok(finish(report, &engine, stop));
            }
            let clock = self.clock.now();
            let due: Vec<TimerKind> = timers
                .iter()
                .filter(|(_, at)| **at <= clock)
                .map(|(timer, _)| *timer)
                .collect();
            if due.is_empty() {
                if let Some(wait) = self.due_soon(&timers, clock, deadline) {
                    pause(wait, progress, &report, started).await;
                    reported = Instant::now();
                    continue;
                }
                let stop = match engine.status().cooldown {
                    Some(cooldown) if cooldown.until > clock => StopReason::CoolingDown {
                        until: cooldown.until,
                    },
                    Some(_) | None => StopReason::Idle {
                        next: timers.values().min().copied(),
                    },
                };
                return Ok(finish(report, &engine, stop));
            }
            for timer in due {
                timers.remove(&timer);
                queue.extend(engine.handle(Event {
                    now: clock,
                    kind: EventKind::TimerFired { timer },
                }));
            }
        }
    }

    /// Apply the queued commits of a stopped engine; drop everything else.
    async fn flush_commits(
        &self,
        queue: &mut VecDeque<Command>,
        report: &mut SyncReport,
    ) -> Result<Option<Rejection>, RunError> {
        let mut refused = None;
        while let Some(command) = queue.pop_front() {
            if let CommandKind::Commit(commit) = command.kind
                && let Some(EventKind::CommitRejected { reason, .. }) =
                    self.apply_commit(&commit, report).await?
            {
                refused = Some(reason);
            }
        }
        Ok(refused)
    }

    /// Perform one command; `None` if it produces no event.
    async fn execute(
        &self,
        id: CommandId,
        kind: CommandKind,
        report: &mut SyncReport,
    ) -> Result<Option<EventKind>, RunError> {
        match kind {
            CommandKind::Gmail(call) => self.gmail(id, call, report).await.map(Some),
            CommandKind::Commit(commit) => {
                // Gmail's push-back is shared: publish the engine's cooldowns
                // (account or project-wide) to the gate every account uses.
                for op in &commit.account_ops {
                    if let AccountOp::SetCooldown(cooldown) = op {
                        self.quota
                            .cool_down(cooldown.clone())
                            .await
                            .map_err(|e| RunError::Quota(e.0))?;
                    }
                }
                self.apply_commit(&commit, report).await
            }
            CommandKind::ClaimJobs {
                account,
                epoch,
                limit,
                now,
                attempt,
                floor,
            } => match self
                .store
                .claim_jobs(&account, epoch, limit, now, attempt, floor)
                .await
            {
                Ok(ClaimOutcome::Claimed { jobs, next_due }) => Ok(Some(EventKind::JobsClaimed {
                    command: id,
                    jobs,
                    next_due,
                })),
                Ok(ClaimOutcome::Rejected(reason)) => Ok(Some(EventKind::JobsClaimRejected {
                    command: id,
                    reason,
                })),
                Err(error) => Err(RunError::Store(error)),
            },
            CommandKind::DeleteBlob { account, message } => {
                if let Ok(()) = self.blobs.delete(&blob_key(&account, &message)).await {
                    Ok(Some(EventKind::BlobDeleted { command: id }))
                } else {
                    // The job stays claimed; the next run releases and
                    // retries it. No error text: it can carry the object path.
                    tracing::warn!("blob delete failed; it is retried");
                    Ok(None)
                }
            }
            CommandKind::Schedule { .. } | CommandKind::ReportStatus(_) => Ok(None),
        }
    }

    async fn apply_commit(
        &self,
        commit: &mailtide::commit::Commit,
        report: &mut SyncReport,
    ) -> Result<Option<EventKind>, RunError> {
        match self.store.apply(commit).await {
            Ok(CommitOutcome::Applied { .. }) => {
                report.commits += 1;
                Ok(Some(EventKind::CommitApplied { commit: commit.id }))
            }
            Ok(CommitOutcome::Rejected(reason)) => {
                tracing::warn!(reason = ?reason, "commit refused by the store");
                Ok(Some(EventKind::CommitRejected {
                    commit: commit.id,
                    reason,
                }))
            }
            Err(error) => Err(RunError::Store(error)),
        }
    }

    async fn gmail(
        &self,
        command: CommandId,
        call: GmailCall,
        report: &mut SyncReport,
    ) -> Result<EventKind, RunError> {
        if let (GmailRequest::Modify(_), Access::ReadOnly) = (&call.request, self.config.access) {
            // Never sent: the intents block (shown as blocked), the job ends.
            return Ok(EventKind::RequestFailed {
                command,
                failure: Failure::Blocking(BlockingFailure {
                    class: Blocking::Permanent,
                    detail: "read-only runner: action not sent".into(),
                }),
            });
        }
        let topic = match (&call.request, &self.config.watch) {
            (GmailRequest::Watch, WatchMode::Disabled) => {
                return Err(RunError::Invariant(
                    "the engine asked for a watch with watches off",
                ));
            }
            (_, WatchMode::Topic(topic)) => topic.clone(),
            (_, WatchMode::Disabled) => String::new(),
        };
        let lease = match self
            .quota
            .admit(&call.account, call.cost, call.priority, self.clock.now())
            .await
            .map_err(|e| RunError::Quota(e.0))?
        {
            Admission::Granted(lease) => lease,
            Admission::Deferred { until } => {
                tracing::debug!(
                    request = request_kind(&call.request),
                    until = until.0,
                    "deferred by the quota gate"
                );
                return Ok(EventKind::RequestDeferred { command, until });
            }
        };
        // If this future is dropped mid-request (cancellation), the guard
        // gives the lease back.
        let mut held = LeaseGuard {
            gate: &self.quota,
            lease: Some(lease),
        };
        let sent = Instant::now();
        let outcome = self
            .perform(&call.account, command, &call.request, &topic, report)
            .await;
        let lease = held.lease.take().unwrap_or(lease);
        let duration_ms = u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX);
        match &outcome {
            Outcome::Event(_) => tracing::debug!(
                request = request_kind(&call.request),
                cost = call.cost.0,
                duration_ms,
                "gmail request"
            ),
            Outcome::Failed(failure) => tracing::warn!(
                request = request_kind(&call.request),
                failure = failure_kind(failure),
                duration_ms,
                "gmail request failed"
            ),
        }
        let bytes = match &outcome {
            Outcome::Event(EventKind::MessageImported { message, .. }) => message.size,
            Outcome::Event(_) | Outcome::Failed(_) => 0,
        };
        self.quota
            .release(lease, bytes, self.clock.now())
            .await
            .map_err(|e| RunError::Quota(e.0))?;
        Ok(match outcome {
            Outcome::Event(event) => event,
            Outcome::Failed(failure) => EventKind::RequestFailed { command, failure },
        })
    }

    async fn perform(
        &self,
        account: &AccountId,
        command: CommandId,
        gmail_request: &GmailRequest,
        topic: &str,
        report: &mut SyncReport,
    ) -> Outcome {
        let token = match self.tokens.token(account).await {
            Ok(token) => token,
            Err(error) => return Outcome::Failed(error.into_failure()),
        };
        let config = RequestConfig {
            watch_topic: topic.to_owned(),
        };
        let mut last = None;
        for http in request::build(gmail_request, &config) {
            report.gmail_requests += 1;
            let response = match self.transport.send(http, &token).await {
                Ok(response) => response,
                Err(TransportError::NotSent(detail) | TransportError::OutcomeUnknown(detail)) => {
                    return Outcome::Failed(transient(detail));
                }
            };
            if response.status == 401 {
                // The token was rejected: the next call mints a fresh one.
                self.tokens.invalidate(account);
            }
            if !response.is_success() {
                return match (errors::classify(&response), gmail_request) {
                    (Failure::NotFound, GmailRequest::ListHistory { .. }) => {
                        Outcome::Event(EventKind::HistoryExpired { command })
                    }
                    (
                        Failure::NotFound,
                        GmailRequest::GetMessage { id } | GmailRequest::ImportMessage { id },
                    ) => Outcome::Event(EventKind::MessageNotFound {
                        command,
                        message_id: id.clone(),
                    }),
                    (failure, _) => Outcome::Failed(failure),
                };
            }
            last = Some(response);
        }
        let Some(response) = last else {
            return Outcome::Failed(transient("no request built".into()));
        };
        let body = response.body.as_slice();
        let decoded = match gmail_request {
            GmailRequest::GetProfile => {
                decode::profile(body).map(|history_id| EventKind::ProfileFetched {
                    command,
                    history_id,
                })
            }
            GmailRequest::ListHistory { .. } => {
                decode::history(body).map(|page| EventKind::HistoryFetched { command, page })
            }
            GmailRequest::ListMessages { .. } => {
                decode::list(body).map(|page| EventKind::MessagesListed { command, page })
            }
            GmailRequest::GetMessage { id } => match decode::message_minimal(body) {
                Ok(message) if message.id == *id => {
                    Ok(EventKind::MessageFetched { command, message })
                }
                Ok(message) => return Outcome::Failed(mismatch(id, &message.id)),
                Err(error) => Err(error),
            },
            // Check the identity before writing anything: a body naming
            // another message must not be stored under its key.
            GmailRequest::ImportMessage { id } => match decode::message_raw(body) {
                Ok(raw) if raw.observation.id == *id => {
                    return self.import(account, command, raw, report).await;
                }
                Ok(raw) => return Outcome::Failed(mismatch(id, &raw.observation.id)),
                Err(error) => Err(error),
            },
            GmailRequest::ListLabels => {
                decode::labels_list(body).map(|labels| EventKind::LabelsListed { command, labels })
            }
            GmailRequest::Watch => decode::watch(body).map(|expiration| EventKind::WatchRenewed {
                command,
                expiration,
            }),
            GmailRequest::Modify(_) => Ok(EventKind::LabelsModified { command }),
        };
        match decoded {
            Ok(event) => Outcome::Event(event),
            // A malformed body won't heal by waiting: refuse it for good
            // (a job blocks; a sync request blocks the account for an
            // operator) instead of re-fetching forever.
            Err(error) => Outcome::Failed(Failure::Blocking(BlockingFailure {
                class: Blocking::Permanent,
                detail: format!("undecodable response: {error:?}"),
            })),
        }
    }

    /// Store the raw content durably, then report it (review C2).
    async fn import(
        &self,
        account: &AccountId,
        command: CommandId,
        raw: decode::RawMessage,
        report: &mut SyncReport,
    ) -> Outcome {
        let key = blob_key(account, &raw.observation.id);
        if let Err(error) = self.blobs.put(&key, raw.rfc822.clone()).await {
            return Outcome::Failed(transient(format!("blob write failed: {error:?}")));
        }
        report.imported += 1;
        Outcome::Event(EventKind::MessageImported {
            command,
            message: Box::new(ImportedMessage {
                headers: headers::facts(&raw.rfc822, raw.snippet.as_deref()),
                blob: key,
                size: u64::try_from(raw.rfc822.len()).unwrap_or(u64::MAX),
                internal_date: raw.internal_date,
                observation: raw.observation,
            }),
        })
    }
}

/// Gives a quota lease back if the request holding it is abandoned.
struct LeaseGuard<'a, Q: QuotaGate> {
    gate: &'a Q,
    lease: Option<mailtide::quota::Lease>,
}

impl<Q: QuotaGate> Drop for LeaseGuard<'_, Q> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            self.gate.abandon(lease);
        }
    }
}

/// Gmail answered for a different message than requested: retry (never
/// store it under the requested id).
fn mismatch(requested: &MessageId, answered: &MessageId) -> Failure {
    transient(format!(
        "response for message {} answered request for {}",
        answered.0, requested.0
    ))
}

fn transient(detail: String) -> Failure {
    Failure::Retryable(RetryableFailure {
        class: Retryable::Transient,
        retry_after: None,
        detail,
    })
}

fn stopped(engine: &Engine) -> Option<StopReason> {
    engine.stopped().map(|stopped| match stopped {
        Stopped::Superseded(_) => StopReason::Superseded,
        Stopped::Blocked(reason) => StopReason::Blocked(reason.clone()),
    })
}

/// Sleep `wait`, reporting progress every second (heartbeats continue).
async fn pause(
    wait: Duration,
    progress: Option<&(dyn Fn(&Progress) + Sync)>,
    report: &SyncReport,
    started: Instant,
) {
    let until = Instant::now() + wait;
    while Instant::now() < until {
        let step = PROGRESS_EVERY.min(until.saturating_duration_since(Instant::now()));
        tokio::time::sleep(step).await;
        if let Some(progress) = progress {
            progress(&Progress {
                commits: report.commits,
                imported: report.imported,
                gmail_requests: report.gmail_requests,
                elapsed: started.elapsed(),
            });
        }
    }
}

fn finish(mut report: SyncReport, engine: &Engine, stop: StopReason) -> RunOutcome {
    let status = engine.status();
    report.cursor_after = status.cursor;
    report.status = Some(status);
    RunOutcome { report, stop }
}
