//! The loop: build an engine from the store, feed it events, perform its
//! commands, until the account is caught up or the budget runs out. Hosts call
//! it from their scheduler (a cron job, a queue worker, a durable workflow). Each run
//! claims the account (a new epoch), so overlapping runs fence each other.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use mailtide::command::{Command, CommandKind, GmailCall, GmailRequest};
use mailtide::commit::{AccountOp, CommitOutcome};
use mailtide::engine::Stopped;
use mailtide::event::{Event, EventKind};
use mailtide::gmail::ImportedMessage;
use mailtide::ids::{AccountId, CommandId, ENGINE_VERSION, HistoryId, Timestamp};
use mailtide::outbox::ActionRequest;
use mailtide::quota::Admission;
use mailtide::retry::{BlockingFailure, Failure, Retryable, RetryableFailure};
use mailtide::schedule::TimerKind;
use mailtide::status::Status;
use mailtide::{Engine, EngineConfig};
use mailtide_gmail::request::{self, RequestConfig};
use mailtide_gmail::{decode, errors, headers};
use mailtide_store::{Store, StoreError};

use crate::auth::TokenSource;
use crate::blobs::{BlobStore, blob_key};
use crate::now;
use crate::quota_gate::QuotaGate;
use crate::transport::{GmailTransport, TransportError};

/// How the runner treats `users.watch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchMode {
    /// Publish changes to this Pub/Sub topic (`projects/p/topics/t`).
    Topic(String),
    /// No push: watch requests are deferred a day; polling finds changes.
    Disabled,
}

/// Runner settings.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Engine tunables.
    pub engine: EngineConfig,
    /// Push notifications.
    pub watch: WatchMode,
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

/// Infrastructure failures (the run may be retried).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunError {
    /// The store failed or the outcome of a commit is unknown.
    Store(StoreError),
    /// The stored state didn't validate.
    Corrupt(String),
    /// The quota gate failed.
    Quota(String),
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
        }
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

    /// Admit a user action and deliver it, syncing as [`Self::run`] does.
    /// Admission goes straight to the store, so it holds even when this run
    /// is superseded by a concurrent one (which then delivers it).
    pub async fn act(
        &self,
        account: &AccountId,
        action: ActionRequest,
        budget: Duration,
    ) -> Result<RunOutcome, RunError> {
        let admitted = self
            .store
            .admit(account, &action, now())
            .await
            .map_err(RunError::Store)?;
        let mut outcome = self.run(account, budget).await?;
        outcome.report.admitted = admitted;
        Ok(outcome)
    }

    /// Sync `account` until caught up, stopped, or `budget` elapses.
    pub async fn run(&self, account: &AccountId, budget: Duration) -> Result<RunOutcome, RunError> {
        let deadline = Instant::now() + budget;
        let snapshot = match self.store.claim(account, ENGINE_VERSION).await {
            Ok(snapshot) => snapshot,
            Err(StoreError::Rejected(_)) => {
                return Ok(RunOutcome {
                    report: SyncReport::default(),
                    stop: StopReason::Superseded,
                });
            }
            Err(error) => return Err(RunError::Store(error)),
        };
        let snapshot = snapshot
            .validate()
            .map_err(|e| RunError::Corrupt(format!("{e:?}")))?;
        let mut engine = Engine::new(snapshot, self.config.engine.clone());
        let mut report = SyncReport {
            cursor_before: engine.status().cursor,
            ..SyncReport::default()
        };
        let mut queue: VecDeque<Command> = engine.start(now()).into();
        let mut timers: BTreeMap<TimerKind, Timestamp> = BTreeMap::new();
        loop {
            while let Some(command) = queue.pop_front() {
                if let Some(stop) = stopped(&engine) {
                    return Ok(finish(report, &engine, stop));
                }
                if Instant::now() > deadline {
                    return Ok(finish(report, &engine, StopReason::BudgetExhausted));
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
                                now: now(),
                                kind: event,
                            }));
                        }
                    }
                }
            }
            if let Some(stop) = stopped(&engine) {
                return Ok(finish(report, &engine, stop));
            }
            let clock = now();
            let due: Vec<TimerKind> = timers
                .iter()
                .filter(|(_, at)| **at <= clock)
                .map(|(timer, _)| *timer)
                .collect();
            if due.is_empty() {
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
            } => match self
                .store
                .claim_jobs(&account, epoch, limit, now, attempt)
                .await
            {
                Ok(jobs) => Ok(Some(EventKind::JobsClaimed { command: id, jobs })),
                Err(StoreError::Rejected(_)) => Ok(None),
                Err(error) => Err(RunError::Store(error)),
            },
            CommandKind::DeleteBlob { account, message } => {
                match self.blobs.delete(&blob_key(&account, &message)).await {
                    Ok(()) => Ok(Some(EventKind::BlobDeleted { command: id })),
                    // The job stays claimed; the next run releases and retries it.
                    Err(error) => {
                        tracing::warn!(?error, "blob delete failed");
                        Ok(None)
                    }
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
            Ok(CommitOutcome::Rejected(reason)) => Ok(Some(EventKind::CommitRejected {
                commit: commit.id,
                reason,
            })),
            Err(error) => Err(RunError::Store(error)),
        }
    }

    async fn gmail(
        &self,
        command: CommandId,
        call: GmailCall,
        report: &mut SyncReport,
    ) -> Result<EventKind, RunError> {
        let topic = match (&call.request, &self.config.watch) {
            (GmailRequest::Watch, WatchMode::Disabled) => {
                return Ok(EventKind::RequestDeferred {
                    command,
                    until: now().plus(Duration::from_hours(24)),
                });
            }
            (_, WatchMode::Topic(topic)) => topic.clone(),
            (_, WatchMode::Disabled) => String::new(),
        };
        let lease = match self
            .quota
            .admit(&call.account, call.cost, call.priority, now())
            .await
            .map_err(|e| RunError::Quota(e.0))?
        {
            Admission::Granted(lease) => lease,
            Admission::Deferred { until } => {
                return Ok(EventKind::RequestDeferred { command, until });
            }
        };
        let outcome = self
            .perform(&call.account, command, &call.request, &topic, report)
            .await;
        let bytes = match &outcome {
            Outcome::Event(EventKind::MessageImported { message, .. }) => message.size,
            Outcome::Event(_) | Outcome::Failed(_) => 0,
        };
        self.quota
            .release(lease, bytes, now())
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
            Err(failure) => return Outcome::Failed(failure),
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
            GmailRequest::GetMessage { .. } => decode::message_minimal(body)
                .map(|message| EventKind::MessageFetched { command, message }),
            GmailRequest::ImportMessage { .. } => match decode::message_raw(body) {
                Ok(raw) => return self.import(account, command, raw, report).await,
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
            Err(error) => Outcome::Failed(transient(format!("undecodable response: {error:?}"))),
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

fn finish(mut report: SyncReport, engine: &Engine, stop: StopReason) -> RunOutcome {
    let status = engine.status();
    report.cursor_after = status.cursor;
    report.status = Some(status);
    RunOutcome { report, stop }
}
