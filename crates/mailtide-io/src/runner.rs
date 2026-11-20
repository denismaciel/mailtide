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
use mailtide::gmail::{ImportedMessage, Label, MessageFacts};
use mailtide::ids::{
    AccountId, CommandId, ENGINE_VERSION, HistoryId, MessageId, ThreadId, Timestamp,
};
use mailtide::jobs::{ClaimOutcome, PRIORITISE_MAX_IDS, Prioritised};
use mailtide::outbox::ActionRequest;
use mailtide::quota::{Admission, Priority};
use mailtide::retry::{Backoff, Blocking, BlockingFailure, Failure, Retryable, RetryableFailure};
use mailtide::revision::{AckOutcome, ConsumerList, ConsumerName, FeedPosition, Retention};
use mailtide::schedule::TimerKind;
use mailtide::search::SearchScope;
use mailtide::status::{AccountStatus, ForgetOutcome, ResumeOutcome, Status, TrackOutcome};
use mailtide::thread::ThreadView;
use mailtide::{Engine, EngineConfig};
use mailtide_gmail::request::{self, RequestConfig};
use mailtide_gmail::{decode, errors, headers};
use mailtide_store::{Store, StoreError};

use crate::auth::TokenSource;
use crate::blobs::{BlobError, BlobStore, blob_key};
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
        // The request kind only: never the query, its label ids or hits.
        GmailRequest::Search(search) => match search.scope {
            SearchScope::Messages => "search_messages",
            SearchScope::Threads => "search_threads",
        },
        GmailRequest::GetLabel { .. } => "get_label",
        GmailRequest::CreateLabel(_) => "create_label",
        GmailRequest::PatchLabel { .. } => "patch_label",
        GmailRequest::DeleteLabel { .. } => "delete_label",
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
    extract: fn(&[u8]) -> MessageFacts,
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
    /// Requests that failed (token source, Gmail or blob store); they are
    /// retried after backoff, so a run can end `Idle` with work failing.
    pub failed_requests: u32,
    /// The last failure's class (`transient`, `not_found`, ...), if any.
    pub last_failure: Option<&'static str>,
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
    /// A `prioritise` call named more than [`PRIORITISE_MAX_IDS`] ids;
    /// nothing was written.
    TooManyIds {
        /// How many it named.
        given: usize,
    },
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
            Self::Store(StoreError::NotTracked) => "not_tracked",
            Self::Store(StoreError::SchemaOutOfDate { .. }) => "schema_out_of_date",
            Self::Quota(_) => "quota_gate",
            Self::Blobs(_) => "blobs",
            Self::Invariant(_) => "invariant",
            Self::ReadOnly => "read_only",
            Self::TooManyIds { .. } => "too_many_ids",
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

/// The quota gate would make a direct call wait past its budget: nothing
/// was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaDeferred {
    /// When the gate would admit it, at the earliest.
    pub until: Timestamp,
}

/// Why a direct call ([`Runner::call_gmail`]) brought back no answer.
/// Nothing here is recorded on the account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    /// The request changes the mailbox and the runner is read-only: nothing
    /// was sent.
    ReadOnly,
    /// The quota gate would make it wait past its budget: nothing was sent.
    Deferred(QuotaDeferred),
    /// Gmail has no such resource (404). A message get answers
    /// [`EventKind::MessageNotFound`] instead, as for the engine.
    NotFound,
    /// Refused for good, the caller's to report: a 400, a 403 (a missing
    /// scope or a disabled API is [`Blocking::Account`] with
    /// `ApiAccessDenied`), an account problem from Gmail or the token
    /// source, or an answer that does not decode. The account is not
    /// blocked by it.
    Refused(BlockingFailure),
    /// Worth asking again later: a 5xx, a rate limit (the gate is cooled
    /// down), an expired token, a request that certainly wasn't sent.
    Retryable(RetryableFailure),
    /// The request may have reached Gmail and its answer was lost (a
    /// timeout, a dropped connection): a write may have been applied.
    OutcomeUnknown(String),
    /// The runner's own resources failed (the quota gate, blob storage).
    Run(RunError),
}

impl From<RunError> for CallError {
    fn from(error: RunError) -> Self {
        Self::Run(error)
    }
}

impl CallError {
    /// A stable name for logs (no detail: Gmail's error text can quote
    /// data).
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::Deferred(_) => "deferred",
            Self::NotFound => failure_kind(&Failure::NotFound),
            Self::Refused(refusal) => failure_kind(&Failure::Blocking(refusal.clone())),
            Self::Retryable(failure) => failure_kind(&Failure::Retryable(failure.clone())),
            Self::OutcomeUnknown(_) => "outcome_unknown",
            Self::Run(error) => error.code(),
        }
    }
}

/// The command id of a direct call: epoch 0 never matches a live engine.
const DIRECT_CALL: CommandId = CommandId {
    epoch: mailtide::ids::Epoch(0),
    seq: 0,
};

/// One Gmail response, decoded into the engine's terms.
enum Outcome {
    Event(EventKind),
    Failed(Failure),
    /// The request may have reached Gmail, but no answer came back (a
    /// timeout, a dropped connection): a write may have been applied.
    Unanswered(String),
    /// Gmail answered but the content couldn't be stored.
    StoreFailed(BlobError),
}

/// What the quota gate made of one request.
enum Attempt {
    /// Not sent: the gate admits it at `until` at the earliest.
    Deferred(Timestamp),
    /// Sent (or failed before Gmail answered).
    Done(Outcome),
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
            extract: mailtide_mime::facts,
        }
    }

    pub(crate) fn writable(&self) -> Result<(), RunError> {
        match self.config.access {
            Access::ReadWrite => Ok(()),
            Access::ReadOnly => Err(RunError::ReadOnly),
        }
    }

    /// The time on the runner's clock.
    pub(crate) fn now(&self) -> Timestamp {
        self.clock.now()
    }

    /// Read the time from `clock` instead of the system.
    #[must_use]
    pub fn with_clock(mut self, clock: crate::Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Extract the facts stored at import with `extract` instead of
    /// [`mailtide_mime::facts`] (tests inject an extractor that fails).
    #[must_use]
    pub fn with_facts_extractor(mut self, extract: fn(&[u8]) -> MessageFacts) -> Self {
        self.extract = extract;
        self
    }

    /// The facts extractor imports and refreshes use.
    pub(crate) fn facts_extractor(&self) -> fn(&[u8]) -> MessageFacts {
        self.extract
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

    /// A user opened these messages: raise their queued body imports to the
    /// Foreground class so they are fetched ahead of the backlog, by
    /// whichever engine holds the account (call [`Self::run`] if none is
    /// running). One store write, unfenced like [`Self::admit`]; use it from
    /// request paths. Allowed read-only: an import changes nothing in Gmail.
    /// At most [`PRIORITISE_MAX_IDS`] ids per call. The feed's
    /// `MessageImported` entry (or `messages`) says when a body is stored.
    pub async fn prioritise(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Prioritised, RunError> {
        if ids.len() > PRIORITISE_MAX_IDS {
            return Err(RunError::TooManyIds { given: ids.len() });
        }
        self.store
            .prioritise(account, ids, self.clock.now())
            .await
            .map_err(RunError::Store)
    }

    /// Start mirroring `account` (a no-op if it already is). Its feed gets a
    /// new generation, the current time, so positions from an earlier
    /// incarnation (before a forget) are recognisably stale.
    pub async fn track(&self, account: &AccountId) -> Result<TrackOutcome, RunError> {
        let generation = u64::try_from(self.clock.now().0).unwrap_or(0);
        self.store
            .track(account, generation)
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

    /// Consumer `consumer` has durably processed `account`'s feed up to
    /// `at`: its cursor is set there, at the runner's clock, and the log is
    /// held from there for [`Self::compact`]. Allowed read-only: it writes
    /// mailtide's own tables, never Gmail.
    pub async fn ack(
        &self,
        account: &AccountId,
        consumer: &ConsumerName,
        at: FeedPosition,
    ) -> Result<AckOutcome, RunError> {
        self.store
            .ack(account, consumer, at, self.clock.now())
            .await
            .map_err(RunError::Store)
    }

    /// `account`'s registered consumers: who holds the log, since when, and
    /// who is lost (behind the floor).
    pub async fn consumers(&self, account: &AccountId) -> Result<ConsumerList, RunError> {
        self.store.consumers(account).await.map_err(RunError::Store)
    }

    /// Forget consumer `consumer` (decommissioned): it no longer holds the
    /// log. Whether it was registered.
    pub async fn drop_consumer(
        &self,
        account: &AccountId,
        consumer: &ConsumerName,
    ) -> Result<bool, RunError> {
        self.store
            .drop_consumer(account, consumer)
            .await
            .map_err(RunError::Store)
    }

    /// Drop whole commits of `account`'s feed: no further than `through`
    /// (when given), nor past the slowest consumer heard from within
    /// `max_idle`, at the runner's clock (see
    /// [`mailtide::revision::compaction_target`]). With neither a bound nor
    /// a registered consumer nothing goes. A `through` of another
    /// generation compacts nothing. Returns how many entries were removed.
    pub async fn compact(
        &self,
        account: &AccountId,
        through: Option<FeedPosition>,
        max_idle: Duration,
    ) -> Result<u64, RunError> {
        let mut retention = Retention::new(self.clock.now());
        retention.max_idle = max_idle;
        if let Some(through) = through {
            let status = self.status(account).await?;
            if status.is_none_or(|s| s.generation != through.generation) {
                return Ok(0);
            }
            // Whole commits only: a position inside a commit keeps that
            // commit until the reader is through it.
            retention = retention.through(through.at.compactable());
        }
        self.store
            .compact(account, &retention)
            .await
            .map_err(RunError::Store)
    }

    /// `account`'s label definitions as the mirror last synced them, in id
    /// order (empty before the first label sync); read again after a
    /// `LabelDefinitionsChanged` feed entry. Resolves label ids to names.
    pub async fn labels(&self, account: &AccountId) -> Result<Vec<Label>, RunError> {
        self.store.labels(account).await.map_err(RunError::Store)
    }

    /// `account`'s threads `ids` as the mirror holds them, in the order asked
    /// (see [`Store::threads`]): a store read that claims nothing and calls
    /// no Gmail, so it needs no token and works while the account is blocked
    /// or cooling down. Ids the mirror has no message in are left out.
    pub async fn threads(
        &self,
        account: &AccountId,
        ids: &[ThreadId],
    ) -> Result<Vec<ThreadView>, RunError> {
        self.store
            .threads(account, ids)
            .await
            .map_err(RunError::Store)
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
        let mut checked = Vec::with_capacity(views.len());
        for view in views {
            let request = GmailRequest::GetMessage {
                id: view.id.clone(),
            };
            let result = match self
                .call_gmail(account, request, Priority::Backfill, deadline)
                .await
            {
                Ok(EventKind::MessageFetched { message, .. }) => {
                    Checked::Compared(mailtide::drift::compare(&view, Some(&message)))
                }
                Ok(EventKind::MessageNotFound { .. }) => {
                    Checked::Compared(mailtide::drift::compare(&view, None))
                }
                Ok(_) => return Err(RunError::Invariant("unexpected answer to a message get")),
                Err(CallError::Deferred(_)) => Checked::Deferred,
                // An unanswered read is only a transient failure.
                Err(CallError::OutcomeUnknown(_)) => Checked::Failed("transient"),
                Err(CallError::Run(error)) => return Err(error),
                Err(error) => Checked::Failed(error.code()),
            };
            checked.push((view.id, result));
        }
        Ok(checked)
    }

    /// One Gmail request outside the engine: the shared path of every call
    /// whose answer a host needs at once (drift checks, searches, draft,
    /// label and settings writes, purges). It
    ///
    /// - refuses a request that changes the mailbox in read-only mode,
    ///   before any credential or quota is spent;
    /// - admits the request through the quota gate at `priority` with its
    ///   declared cost ([`mailtide::quota::cost`]), waits out deferrals while
    ///   that ends before `deadline`, and otherwise returns
    ///   [`CallError::Deferred`] without sending it;
    /// - sends it with the account's token (a 401 drops the token, so the
    ///   next call mints another) and decodes the answer as the engine's
    ///   commands are decoded;
    /// - cools the gate down after a rate limit, as the engine would, so
    ///   later calls and syncs wait instead of being refused too.
    ///
    /// It claims nothing and commits nothing: its command id has epoch 0,
    /// which no engine has, so it can neither supersede a sync nor be
    /// fenced by one, and it never moves the history cursor. A refusal is
    /// the caller's, never the account's: a 400, a 403 (a scope the token
    /// lacks is [`AccountProblem::ApiAccessDenied`]) or an account problem
    /// comes back as [`CallError::Refused`] and blocks nothing; a real
    /// account problem is recorded by the account's next run.
    ///
    /// A caller that keeps Gmail's answer in the mirror records it after
    /// this returns, with an unfenced store method (no epoch, as
    /// [`Store::admit`]): the answer only, never the cursor. A crash between
    /// the answer and the record loses nothing the next sync does not bring
    /// back.
    ///
    /// It is for the driver's features, not a host API promise: it sends
    /// what it is given, bypassing intents and the overlay (read-only mode
    /// still applies).
    ///
    /// [`AccountProblem::ApiAccessDenied`]: mailtide::retry::AccountProblem::ApiAccessDenied
    pub async fn call_gmail(
        &self,
        account: &AccountId,
        request: GmailRequest,
        priority: Priority,
        deadline: Instant,
    ) -> Result<EventKind, CallError> {
        if changes_mailbox(&request) && self.config.access == Access::ReadOnly {
            return Err(CallError::ReadOnly);
        }
        let call = GmailCall {
            account: account.clone(),
            cost: mailtide::quota::cost(&request),
            request,
            priority,
        };
        let mut report = SyncReport::default();
        let outcome = loop {
            match self.attempt(DIRECT_CALL, &call, &mut report).await? {
                Attempt::Done(outcome) => break outcome,
                Attempt::Deferred(until) => {
                    let wait = Duration::from_millis(
                        u64::try_from(until.0 - self.clock.now().0).unwrap_or(0),
                    );
                    if !self.clock.can_wait() || Instant::now() + wait >= deadline {
                        return Err(CallError::Deferred(QuotaDeferred { until }));
                    }
                    tokio::time::sleep(wait).await;
                }
            }
        };
        let failure = match outcome {
            Outcome::Event(event) => return Ok(event),
            Outcome::Unanswered(detail) => return Err(CallError::OutcomeUnknown(detail)),
            Outcome::Failed(failure) => failure,
            Outcome::StoreFailed(error) => blob_failure(error)?,
        };
        Err(match failure {
            Failure::NotFound => CallError::NotFound,
            Failure::Blocking(refusal) => CallError::Refused(refusal),
            Failure::Retryable(failure) => {
                let now = self.clock.now();
                let seed = mailtide::ids::stable_hash(account.as_str().as_bytes())
                    ^ u64::try_from(now.0).unwrap_or(0);
                if let Some(cooldown) = Backoff::DEFAULT.cooldown(&failure, account, now, seed) {
                    self.quota
                        .cool_down(cooldown)
                        .await
                        .map_err(|e| RunError::Quota(e.0))?;
                }
                CallError::Retryable(failure)
            }
        })
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
    /// so the run waits for it. Polls, watch renewals and label refreshes end
    /// the run; the host schedules those. Only on the system clock: a host
    /// clock moves when the host moves it.
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
                TimerKind::Poll | TimerKind::WatchRenewal | TimerKind::Labels => false,
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
        if changes_mailbox(&call.request) && self.config.access == Access::ReadOnly {
            // Never sent: the intents block (shown as blocked), the job ends.
            return Ok(EventKind::RequestFailed {
                command,
                failure: Failure::Blocking(BlockingFailure {
                    class: Blocking::Permanent,
                    detail: "read-only runner: action not sent".into(),
                }),
            });
        }
        match self.attempt(command, &call, report).await? {
            Attempt::Deferred(until) => Ok(EventKind::RequestDeferred { command, until }),
            Attempt::Done(Outcome::Event(event)) => Ok(event),
            Attempt::Done(Outcome::Failed(failure)) => {
                Ok(EventKind::RequestFailed { command, failure })
            }
            // Retried after backoff, as any transient failure.
            Attempt::Done(Outcome::Unanswered(detail)) => Ok(EventKind::RequestFailed {
                command,
                failure: transient(detail),
            }),
            Attempt::Done(Outcome::StoreFailed(error)) => Ok(EventKind::RequestFailed {
                command,
                failure: blob_failure(error)?,
            }),
        }
    }

    /// Admit `call` through the quota gate and, if granted, send it; the
    /// lease is released (or given back if this future is dropped). Shared
    /// by the engine's commands and direct calls.
    async fn attempt(
        &self,
        command: CommandId,
        call: &GmailCall,
        report: &mut SyncReport,
    ) -> Result<Attempt, RunError> {
        let topic = match (&call.request, &self.config.watch) {
            (GmailRequest::Watch, WatchMode::Disabled) => {
                return Err(RunError::Invariant(
                    "the engine asked for a watch with watches off",
                ));
            }
            (_, WatchMode::Topic(topic)) => topic.clone(),
            (_, WatchMode::Disabled) => String::new(),
        };
        // A listing reports the label generation read before it is sent:
        // a local label write after this read makes it stale (SPEC §6.6).
        let generation = match call.request {
            GmailRequest::ListLabels => self
                .store
                .label_generation(&call.account)
                .await
                .map_err(RunError::Store)?,
            _ => 0,
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
                return Ok(Attempt::Deferred(until));
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
            .perform(
                &call.account,
                command,
                &call.request,
                &topic,
                generation,
                report,
            )
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
            Outcome::Failed(failure) => {
                request_failed(report, &call.request, failure_kind(failure), duration_ms);
            }
            // For retrying, an unanswered request is a transient failure.
            Outcome::Unanswered(_) => {
                request_failed(report, &call.request, "transient", duration_ms);
            }
            // No error text: it can carry the object path.
            Outcome::StoreFailed(error) => {
                report.failed_requests += 1;
                report.last_failure = Some("blob_store");
                tracing::warn!(
                    request = request_kind(&call.request),
                    error = if matches!(error, BlobError::Misconfigured(_)) {
                        "misconfigured"
                    } else {
                        "unavailable"
                    },
                    "blob write failed"
                );
            }
        }
        let bytes = match &outcome {
            Outcome::Event(EventKind::MessageImported { message, .. }) => message.size,
            Outcome::Event(_)
            | Outcome::Failed(_)
            | Outcome::Unanswered(_)
            | Outcome::StoreFailed(_) => 0,
        };
        self.quota
            .release(lease, bytes, self.clock.now())
            .await
            .map_err(|e| RunError::Quota(e.0))?;
        Ok(Attempt::Done(outcome))
    }

    async fn perform(
        &self,
        account: &AccountId,
        command: CommandId,
        gmail_request: &GmailRequest,
        topic: &str,
        generation: u64,
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
                Err(TransportError::NotSent(detail)) => return Outcome::Failed(transient(detail)),
                Err(TransportError::OutcomeUnknown(detail)) => return Outcome::Unanswered(detail),
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
                    // `Label name exists or conflicts`: the caller's to
                    // resolve, with the label that holds the name.
                    (
                        Failure::Blocking(_),
                        GmailRequest::CreateLabel(_) | GmailRequest::PatchLabel { .. },
                    ) if response.status == 409 => {
                        Outcome::Event(EventKind::LabelNameTaken { command })
                    }
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
                decode::labels_list(body).map(|labels| EventKind::LabelsListed {
                    command,
                    labels,
                    generation,
                })
            }
            GmailRequest::Watch => decode::watch(body).map(|expiration| EventKind::WatchRenewed {
                command,
                expiration,
            }),
            GmailRequest::Modify(_) => Ok(EventKind::LabelsModified { command }),
            GmailRequest::Search(search) => decode::search(search.scope, body)
                .map(|listing| EventKind::Searched { command, listing }),
            GmailRequest::GetLabel { .. } => {
                decode::label_get(body).map(|label| EventKind::LabelFetched { command, label })
            }
            GmailRequest::CreateLabel(_) | GmailRequest::PatchLabel { .. } => {
                mailtide_gmail::labels::label(body)
                    .map(|label| EventKind::LabelSaved { command, label })
            }
            GmailRequest::DeleteLabel { .. } => Ok(EventKind::LabelDeleted { command }),
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
            return Outcome::StoreFailed(error);
        }
        report.imported += 1;
        let headers = headers::facts(&raw.rfc822, raw.snippet.as_deref());
        let size = u64::try_from(raw.rfc822.len()).unwrap_or(u64::MAX);
        let facts = self.extract_facts(raw.rfc822).await;
        Outcome::Event(EventKind::MessageImported {
            command,
            message: Box::new(ImportedMessage {
                headers,
                blob: key,
                size,
                internal_date: raw.internal_date,
                observation: raw.observation,
                facts,
            }),
        })
    }

    /// The facts of the bytes just stored, parsed off the async workers. An
    /// import never fails because of a message's shape: if the extractor
    /// gives up, the message is imported without facts.
    async fn extract_facts(&self, stored: Vec<u8>) -> Option<Box<MessageFacts>> {
        let extract = self.extract;
        let extracted = tokio::task::spawn_blocking(move || extract(&stored)).await;
        if extracted.is_err() {
            tracing::warn!("facts extraction failed; the message is imported without them");
        }
        extracted.ok().map(Box::new)
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

/// A blob write that failed after Gmail answered. A store that can't work
/// as configured fails the run: retrying each import can't fix it, and the
/// host must hear about it. Otherwise the request is retried after backoff
/// (a write can't miss an object or a range: those count as unavailable).
fn blob_failure(error: BlobError) -> Result<Failure, RunError> {
    match error {
        BlobError::Misconfigured(detail) => Err(RunError::Blobs(detail)),
        BlobError::Unavailable(detail) => Ok(transient(format!("blob write failed: {detail}"))),
        BlobError::NotFound | BlobError::OutOfRange { .. } => {
            Ok(transient("blob write failed: not found".to_owned()))
        }
    }
}

/// Count and log a failed request (its class only: Gmail's error text can
/// quote data).
fn request_failed(
    report: &mut SyncReport,
    request: &GmailRequest,
    failure: &'static str,
    duration_ms: u64,
) {
    report.failed_requests += 1;
    report.last_failure = Some(failure);
    tracing::warn!(
        request = request_kind(request),
        failure,
        duration_ms,
        "gmail request failed"
    );
}

/// Whether `request` changes the mailbox. Read-only mode refuses these
/// before any credential or quota is spent. Every request kind says which
/// it is: a new kind must choose.
fn changes_mailbox(request: &GmailRequest) -> bool {
    match request {
        GmailRequest::Modify(_)
        | GmailRequest::CreateLabel(_)
        | GmailRequest::PatchLabel { .. }
        | GmailRequest::DeleteLabel { .. } => true,
        GmailRequest::GetProfile
        | GmailRequest::ListHistory { .. }
        | GmailRequest::ListMessages { .. }
        | GmailRequest::GetMessage { .. }
        | GmailRequest::ImportMessage { .. }
        | GmailRequest::ListLabels
        | GmailRequest::Watch
        | GmailRequest::Search(_)
        | GmailRequest::GetLabel { .. } => false,
    }
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
