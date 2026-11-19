//! Python bindings for mailtide, exposed as `mailtide._core`.
//!
//! The Python surface is deliberately small and typed (review #17): a
//! `Runner` the host calls from its scheduler, the change feed, blob reads,
//! push verification and MIME parsing. Everything else stays in Rust. Calls
//! release the GIL while Rust works.

use pyo3::prelude::*;

// Safe without the GIL: every class is frozen (immutable after creation, and
// Sync, which PyO3 checks), Rust state shared across threads is behind
// Mutexes or a connection pool, and calls release the GIL/attachment while
// Rust works. On free-threaded CPython the module keeps the GIL disabled.
#[pymodule(gil_used = false)]
mod _core {
    use std::sync::Arc;
    use std::time::Duration;

    use pyo3::create_exception;
    use pyo3::exceptions::{PyException, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::PyBytes;

    use mailtide::EngineConfig;
    use mailtide::ids::{AccountId, LabelId, MessageId, ThreadId, Timestamp};
    use mailtide::outbox::{
        Action, ActionRequest, Importance, ReadState, RestoreDestination, StarState,
    };
    use mailtide::quota::Budget;
    use mailtide::retry::{
        AccountProblem, Blocking, BlockingFailure, Failure, Retryable, RetryableFailure,
    };
    use mailtide::revision::{StateSeq, StateString};
    use mailtide::state::SyncPhase;
    use mailtide_gmail::push::{self, Jwks, PushExpectations};
    use mailtide_io::auth::{AccessToken, TokenSource};
    use mailtide_io::blobs::{BlobStore, ObjectBlobs, blob_key};
    use mailtide_io::quota_gate::LocalGate;
    use mailtide_io::runner::{self, RunnerConfig, StopReason, WatchMode};
    use mailtide_io::transport::ReqwestTransport;
    use mailtide_store::Store;
    use mailtide_store::sql::SqlStore;

    create_exception!(
        _core,
        MailtideError,
        PyException,
        "Base class for mailtide errors."
    );
    create_exception!(
        _core,
        DelegationRevoked,
        MailtideError,
        "Raise from a token source when the account can no longer be accessed; the account blocks."
    );
    create_exception!(
        _core,
        InvalidPush,
        MailtideError,
        "A push request that failed verification."
    );

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        let py = m.py();
        m.add("MailtideError", py.get_type::<MailtideError>())?;
        m.add("DelegationRevoked", py.get_type::<DelegationRevoked>())?;
        m.add("InvalidPush", py.get_type::<InvalidPush>())?;
        Ok(())
    }

    fn error(what: &str, detail: impl std::fmt::Debug) -> PyErr {
        MailtideError::new_err(format!("{what}: {detail:?}"))
    }

    fn account_id(account: &str) -> PyResult<AccountId> {
        AccountId::parse(account)
            .map_err(|e| PyValueError::new_err(format!("invalid account {account:?}: {e:?}")))
    }

    /// The version of the compiled engine.
    #[pyfunction]
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    // ------------------------------------------------------------ values

    /// An account's sync status.
    #[pyclass(frozen, get_all)]
    #[derive(Clone)]
    struct Status {
        account: String,
        /// `bootstrapping`, `live`, `reconciling` or `blocked`.
        phase: &'static str,
        blocked: Option<String>,
        cursor: Option<u64>,
        cursor_advanced_at_ms: Option<i64>,
        cooldown_until_ms: Option<i64>,
        discovered: Option<u64>,
        imported: Option<u64>,
        discovery_complete: Option<bool>,
    }

    fn describe(failure: &BlockingFailure) -> String {
        let class = match failure.class {
            Blocking::Account(AccountProblem::DelegationRevoked) => "delegation_revoked",
            Blocking::Account(AccountProblem::UserDeleted) => "user_deleted",
            Blocking::Account(AccountProblem::MailDisabled) => "mail_disabled",
            Blocking::Account(AccountProblem::DomainPolicy) => "domain_policy",
            Blocking::Permanent => "permanent",
        };
        format!("{class}: {}", failure.detail)
    }

    impl From<mailtide::status::Status> for Status {
        fn from(status: mailtide::status::Status) -> Self {
            Self {
                account: status.account.as_str().to_owned(),
                phase: match status.phase {
                    SyncPhase::Bootstrapping => "bootstrapping",
                    SyncPhase::Live => "live",
                    SyncPhase::Reconciling => "reconciling",
                    SyncPhase::Blocked => "blocked",
                },
                blocked: status.blocked.as_ref().map(describe),
                cursor: status.cursor.map(|c| c.0),
                cursor_advanced_at_ms: status.cursor_advanced_at.map(|t| t.0),
                cooldown_until_ms: status.cooldown.map(|c| c.until.0),
                discovered: status.import.map(|i| i.discovered),
                imported: status.import.map(|i| i.imported),
                discovery_complete: status.import.map(|i| i.discovery_complete),
            }
        }
    }

    /// What a `Runner.run` call did and why it stopped.
    #[pyclass(frozen, get_all)]
    struct RunOutcome {
        /// `idle`, `budget_exhausted`, `cooling_down`, `blocked` or `superseded`.
        stop: &'static str,
        /// When to run next: scheduled work (idle) or the end of a cooldown.
        next_run_at_ms: Option<i64>,
        /// Why the account is blocked, when `stop == "blocked"`.
        blocked: Option<String>,
        cursor_before: Option<u64>,
        cursor_after: Option<u64>,
        commits: u32,
        imported: u32,
        gmail_requests: u32,
        /// For `act`: how many messages the action admitted changes for.
        admitted: u32,
        status: Option<Status>,
    }

    /// Changes to the mirror after a state string.
    #[pyclass(frozen, get_all)]
    struct ChangePage {
        /// Each change as a dict (`{"Message": {...}}`, `{"MessageDeleted": ...}`, ...).
        changes: Vec<Py<PyAny>>,
        /// The state string to ask from next.
        next: String,
        has_more: bool,
    }

    /// A message as the host should show it.
    #[pyclass(frozen, get_all)]
    struct MessageView {
        id: String,
        thread_id: String,
        /// Labels with pending actions applied: show these.
        labels: Vec<String>,
        /// What Gmail last showed.
        observed_labels: Vec<String>,
        checkpoint: Option<u64>,
        /// The labels await verification after history was lost.
        unverified: bool,
        pending_actions: u32,
        blocked_actions: u32,
        imported: bool,
        size: Option<u64>,
        internal_date_ms: Option<i64>,
        rfc822_message_id: Option<String>,
        in_reply_to: Option<String>,
        references: Vec<String>,
        sender: Option<String>,
        to: Vec<String>,
        cc: Vec<String>,
        subject: Option<String>,
        snippet: Option<String>,
    }

    impl From<mailtide::mirror::MessageView> for MessageView {
        fn from(view: mailtide::mirror::MessageView) -> Self {
            let names =
                |labels: &mailtide::labels::LabelSet| labels.iter().map(|l| l.0.clone()).collect();
            let headers = view
                .content
                .as_ref()
                .map(|c| c.headers.clone())
                .unwrap_or_default();
            Self {
                labels: names(&view.labels),
                observed_labels: names(&view.observed_labels),
                id: view.id.0,
                thread_id: view.thread_id.0,
                checkpoint: view.checkpoint.map(|c| c.0),
                unverified: view.unverified,
                pending_actions: view.pending_actions,
                blocked_actions: view.blocked_actions,
                imported: view.content.is_some(),
                size: view.content.as_ref().map(|c| c.size),
                internal_date_ms: view.content.as_ref().map(|c| c.internal_date.0),
                rfc822_message_id: headers.rfc822_message_id,
                in_reply_to: headers.in_reply_to,
                references: headers.references,
                sender: headers.from,
                to: headers.to,
                cc: headers.cc,
                subject: headers.subject,
                snippet: headers.snippet,
            }
        }
    }

    fn parse_action(
        action: &str,
        add_labels: Vec<String>,
        remove_labels: Vec<String>,
        destination: &str,
    ) -> PyResult<Action> {
        let destination = match destination {
            "inbox" => RestoreDestination::Inbox,
            "archive" => RestoreDestination::Archive,
            other => {
                return Err(PyValueError::new_err(format!(
                    "destination must be inbox or archive, not {other:?}"
                )));
            }
        };
        Ok(match action {
            "mark_read" => Action::SetRead(ReadState::Read),
            "mark_unread" => Action::SetRead(ReadState::Unread),
            "star" => Action::SetStar(StarState::Starred),
            "unstar" => Action::SetStar(StarState::Unstarred),
            "mark_important" => Action::SetImportance(Importance::Important),
            "mark_not_important" => Action::SetImportance(Importance::Normal),
            "archive" => Action::Archive,
            "move_to_inbox" => Action::MoveToInbox,
            "trash" => Action::MoveToTrash,
            "restore" => Action::RestoreFromTrash(destination),
            "spam" => Action::MarkSpam,
            "not_spam" => Action::MarkNotSpam(destination),
            "change_labels" => Action::ChangeLabels {
                add: add_labels.into_iter().map(LabelId).collect(),
                remove: remove_labels.into_iter().map(LabelId).collect(),
            },
            other => return Err(PyValueError::new_err(format!("unknown action {other:?}"))),
        })
    }

    /// A verified Gmail push notification.
    #[pyclass(frozen, get_all)]
    struct Notification {
        account: String,
        history_id: u64,
    }

    /// An attachment of a parsed message.
    #[pyclass(frozen, get_all)]
    struct Attachment {
        filename: Option<String>,
        content_type: String,
        content_id: Option<String>,
        data: Py<PyBytes>,
    }

    /// A parsed RFC 822 message.
    #[pyclass(frozen, get_all)]
    struct ParsedMessage {
        headers: Vec<(String, String)>,
        text: Option<String>,
        html: Option<String>,
        attachments: Vec<Py<Attachment>>,
    }

    impl From<runner::RunOutcome> for RunOutcome {
        fn from(outcome: runner::RunOutcome) -> Self {
            let (stop, next_run_at_ms, blocked) = match &outcome.stop {
                StopReason::Idle { next } => ("idle", next.map(|t| t.0), None),
                StopReason::BudgetExhausted => ("budget_exhausted", None, None),
                StopReason::CoolingDown { until } => ("cooling_down", Some(until.0), None),
                StopReason::Blocked(failure) => ("blocked", None, Some(describe(failure))),
                StopReason::Superseded => ("superseded", None, None),
            };
            let report = outcome.report;
            Self {
                stop,
                next_run_at_ms,
                blocked,
                cursor_before: report.cursor_before.map(|c| c.0),
                cursor_after: report.cursor_after.map(|c| c.0),
                commits: report.commits,
                imported: report.imported,
                gmail_requests: report.gmail_requests,
                admitted: report.admitted,
                status: report.status.map(Status::from),
            }
        }
    }

    // ------------------------------------------------------------ runner

    /// The process-wide Tokio runtime both runners share (created lazily, so
    /// create runners after forking).
    fn runtime() -> &'static tokio::runtime::Runtime {
        pyo3_async_runtimes::tokio::get_runtime()
    }

    fn seconds(value: f64, what: &str) -> PyResult<Duration> {
        Duration::try_from_secs_f64(value)
            .map_err(|e| PyValueError::new_err(format!("{what}: {e}")))
    }

    /// Tokens from a Python callable `(account) -> (token, expires_at_ms)`,
    /// sync or async. A sync callable runs on Tokio's blocking pool (minting
    /// may do network I/O); an async one is awaited on the caller's event loop
    /// (async `Runner` only).
    struct PyTokens(Py<PyAny>);

    fn token_failure(py: Python<'_>, err: &PyErr) -> Failure {
        if err.is_instance_of::<DelegationRevoked>(py) {
            Failure::Blocking(BlockingFailure {
                class: Blocking::Account(AccountProblem::DelegationRevoked),
                detail: "token source raised DelegationRevoked".into(),
            })
        } else {
            Failure::Retryable(RetryableFailure {
                class: Retryable::Transient,
                retry_after: None,
                detail: format!("token source failed: {}", err.get_type(py)),
            })
        }
    }

    fn transient(detail: &str) -> Failure {
        Failure::Retryable(RetryableFailure {
            class: Retryable::Transient,
            retry_after: None,
            detail: detail.to_owned(),
        })
    }

    impl TokenSource for PyTokens {
        async fn token(&self, account: &AccountId) -> Result<AccessToken, Failure> {
            let callable = Python::attach(|py| self.0.clone_ref(py));
            let account = account.as_str().to_owned();
            let called = tokio::task::spawn_blocking(move || {
                Python::attach(|py| {
                    callable
                        .call1(py, (account,))
                        .map_err(|e| token_failure(py, &e))
                })
            })
            .await
            .map_err(|_| transient("token source panicked"))??;
            // An async token source returned an awaitable: await it on the
            // event loop that awaits this run.
            let awaitable = Python::attach(|py| {
                let value = called.bind(py);
                if value.hasattr("__await__").unwrap_or(false) {
                    pyo3_async_runtimes::tokio::into_future(value.clone())
                        .map(Some)
                        .map_err(|_| transient("async token sources need the async Runner"))
                } else {
                    Ok(None)
                }
            })?;
            let value = match awaitable {
                Some(future) => future
                    .await
                    .map_err(|e| Python::attach(|py| token_failure(py, &e)))?,
                None => called,
            };
            Python::attach(|py| {
                value
                    .extract::<(String, i64)>(py)
                    .map(|(secret, expires_at)| AccessToken {
                        secret,
                        expires_at: Timestamp(expires_at),
                    })
                    .map_err(|_| transient("token source must return (token, expires_at_ms)"))
            })
        }
    }

    type Inner = runner::Runner<SqlStore, ObjectBlobs, ReqwestTransport, LocalGate, PyTokens>;

    /// Everything both runners share.
    struct Shared {
        inner: Inner,
    }

    #[allow(clippy::too_many_arguments)]
    fn open(
        py: Python<'_>,
        database_url: &str,
        blob_url: &str,
        token_source: Py<PyAny>,
        watch_topic: Option<String>,
        gmail_origin: Option<String>,
        request_timeout_s: f64,
        confirm_timeout_s: f64,
    ) -> PyResult<Arc<Shared>> {
        let store = py
            .detach(|| runtime().block_on(SqlStore::connect(database_url)))
            .map_err(|e| error("database", e))?;
        let blobs = if blob_url == "memory:///" {
            ObjectBlobs::memory()
        } else {
            ObjectBlobs::from_url(blob_url).map_err(|e| error("blob store", e))?
        };
        let timeout = seconds(request_timeout_s, "request_timeout_s")?;
        let transport = match gmail_origin {
            Some(origin) => ReqwestTransport::with_origin(&origin, timeout),
            None => ReqwestTransport::new(timeout),
        }
        .map_err(|e| error("http client", e))?;
        let config = RunnerConfig {
            engine: EngineConfig {
                confirm_timeout: seconds(confirm_timeout_s, "confirm_timeout_s")?,
                ..EngineConfig::default()
            },
            watch: watch_topic.map_or(WatchMode::Disabled, WatchMode::Topic),
        };
        let inner = runner::Runner::new(
            store,
            blobs,
            transport,
            LocalGate::new(Budget::GMAIL_DEFAULT),
            PyTokens(token_source),
            config,
        );
        Ok(Arc::new(Shared { inner }))
    }

    fn action_request(
        action: &str,
        message_ids: Option<Vec<String>>,
        thread_id: Option<String>,
        add_labels: Vec<String>,
        remove_labels: Vec<String>,
        destination: &str,
    ) -> PyResult<ActionRequest> {
        let action = parse_action(action, add_labels, remove_labels, destination)?;
        match (message_ids, thread_id) {
            (Some(ids), None) => Ok(ActionRequest::Messages {
                ids: ids.into_iter().map(MessageId).collect(),
                action,
            }),
            (None, Some(thread)) => Ok(ActionRequest::Thread {
                id: ThreadId(thread),
                action,
            }),
            _ => Err(PyValueError::new_err(
                "pass exactly one of message_ids or thread_id",
            )),
        }
    }

    fn parse_since(since: Option<String>) -> PyResult<StateSeq> {
        match since {
            None => Ok(StateSeq(0)),
            Some(state) => StateSeq::decode(&StateString(state.clone()))
                .map_err(|_| PyValueError::new_err(format!("invalid state string {state:?}"))),
        }
    }

    fn change_page(page: &mailtide::revision::ChangePage) -> PyResult<ChangePage> {
        Python::attach(|py| {
            let loads = py.import("json")?.getattr("loads")?;
            let changes = page
                .changes
                .iter()
                .map(|change| {
                    let text =
                        serde_json::to_string(change).map_err(|e| error("encode change", e))?;
                    Ok(loads.call1((text,))?.unbind())
                })
                .collect::<PyResult<Vec<_>>>()?;
            Ok(ChangePage {
                changes,
                next: page.next.encode().0,
                has_more: page.has_more,
            })
        })
    }

    // The operations, as futures both runners drive.
    impl Shared {
        async fn migrate(self: Arc<Self>) -> PyResult<()> {
            self.inner
                .store()
                .migrate()
                .await
                .map_err(|e| error("migrate", e))
        }

        async fn run(
            self: Arc<Self>,
            account: AccountId,
            budget: Duration,
        ) -> PyResult<RunOutcome> {
            self.inner
                .run(&account, budget)
                .await
                .map(RunOutcome::from)
                .map_err(|e| error("run", e))
        }

        async fn act(
            self: Arc<Self>,
            account: AccountId,
            request: ActionRequest,
            budget: Duration,
        ) -> PyResult<RunOutcome> {
            self.inner
                .act(&account, request, budget)
                .await
                .map(RunOutcome::from)
                .map_err(|e| error("act", e))
        }

        async fn messages(
            self: Arc<Self>,
            account: AccountId,
            ids: Vec<MessageId>,
        ) -> PyResult<Vec<MessageView>> {
            let views = self
                .inner
                .store()
                .messages(&account, &ids)
                .await
                .map_err(|e| error("messages", e))?;
            Ok(views.into_iter().map(MessageView::from).collect())
        }

        async fn changes(
            self: Arc<Self>,
            account: AccountId,
            since: StateSeq,
            limit: u32,
        ) -> PyResult<ChangePage> {
            let page = self
                .inner
                .store()
                .changes(&account, since, limit)
                .await
                .map_err(|e| error("changes", e))?;
            change_page(&page)
        }

        async fn read_blob(
            self: Arc<Self>,
            account: AccountId,
            message: MessageId,
        ) -> PyResult<Py<PyBytes>> {
            let bytes = self
                .inner
                .blobs()
                .get(&blob_key(&account, &message))
                .await
                .map_err(|e| error("read blob", e))?;
            Ok(Python::attach(|py| PyBytes::new(py, &bytes).unbind()))
        }
    }

    /// Syncs Gmail accounts into a database and blob store; every method is a
    /// coroutine. Build one per process (after forking) and share it: methods
    /// may run concurrently, from any thread or event loop (also on
    /// free-threaded Python). Cancelling an awaiting task stops the work:
    /// in-flight requests are dropped and an open transaction rolls back,
    /// which is safe because every commit is atomic and fenced. Concurrent
    /// runs of one account are safe: all but the newest stop as `superseded`.
    /// `token_source(account)` may be sync or async and may be called
    /// concurrently.
    #[pyclass(frozen)]
    struct Runner {
        shared: Arc<Shared>,
    }

    /// The same as `Runner` with blocking methods, for code without an event
    /// loop (scripts, threads). Calls release the GIL while Rust works. Its
    /// token source must be sync.
    #[pyclass(frozen)]
    struct BlockingRunner {
        shared: Arc<Shared>,
    }

    #[pymethods]
    impl Runner {
        /// `database_url`: `postgres://…` (Unix sockets:
        /// `postgres:///db?host=/socket/dir`) or `sqlite:…`. `blob_url`:
        /// `gs://…`, `s3://…`, `file:///…` or `memory:///`.
        /// `token_source(account)` returns `(access_token, expires_at_ms)`
        /// or raises `DelegationRevoked`. `watch_topic`: the Pub/Sub topic
        /// for push, or `None` to rely on polling. `gmail_origin` overrides
        /// Gmail's URL (tests). `confirm_timeout_s`: how long after
        /// delivering an action its confirmation fetch runs.
        #[new]
        #[allow(clippy::too_many_arguments)]
        #[pyo3(signature = (database_url, blob_url, token_source, *, watch_topic=None, gmail_origin=None, request_timeout_s=60.0, confirm_timeout_s=60.0))]
        fn new(
            py: Python<'_>,
            database_url: &str,
            blob_url: &str,
            token_source: Py<PyAny>,
            watch_topic: Option<String>,
            gmail_origin: Option<String>,
            request_timeout_s: f64,
            confirm_timeout_s: f64,
        ) -> PyResult<Self> {
            Ok(Self {
                shared: open(
                    py,
                    database_url,
                    blob_url,
                    token_source,
                    watch_topic,
                    gmail_origin,
                    request_timeout_s,
                    confirm_timeout_s,
                )?,
            })
        }

        /// Create or upgrade mailtide's tables (safe to call concurrently).
        fn migrate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            pyo3_async_runtimes::tokio::future_into_py(py, self.shared.clone().migrate())
        }

        /// Sync `account` until caught up, blocked or `budget_s` elapses.
        /// Call it on a schedule and whenever a push arrives.
        #[pyo3(signature = (account, budget_s=60.0))]
        fn run<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            budget_s: f64,
        ) -> PyResult<Bound<'py, PyAny>> {
            let fut = self
                .shared
                .clone()
                .run(account_id(account)?, seconds(budget_s, "budget_s")?);
            pyo3_async_runtimes::tokio::future_into_py(py, fut)
        }

        /// Act on messages (or a whole thread) as the user asked, and deliver
        /// it to Gmail. `action` is one of `mark_read`, `mark_unread`, `star`,
        /// `unstar`, `mark_important`, `mark_not_important`, `archive`,
        /// `move_to_inbox`, `trash`, `restore`, `spam`, `not_spam`,
        /// `change_labels`. The change is admitted durably before anything
        /// else, shows in `messages()` at once, and settles when Gmail
        /// confirms it.
        #[pyo3(signature = (account, action, *, message_ids=None, thread_id=None, add_labels=Vec::new(), remove_labels=Vec::new(), destination="inbox", budget_s=30.0))]
        #[allow(clippy::too_many_arguments)]
        fn act<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            action: &str,
            message_ids: Option<Vec<String>>,
            thread_id: Option<String>,
            add_labels: Vec<String>,
            remove_labels: Vec<String>,
            destination: &str,
            budget_s: f64,
        ) -> PyResult<Bound<'py, PyAny>> {
            let request = action_request(
                action,
                message_ids,
                thread_id,
                add_labels,
                remove_labels,
                destination,
            )?;
            let fut = self.shared.clone().act(
                account_id(account)?,
                request,
                seconds(budget_s, "budget_s")?,
            );
            pyo3_async_runtimes::tokio::future_into_py(py, fut)
        }

        /// How these messages should look; unknown ids are left out.
        fn messages<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            message_ids: Vec<String>,
        ) -> PyResult<Bound<'py, PyAny>> {
            let ids = message_ids.into_iter().map(MessageId).collect();
            pyo3_async_runtimes::tokio::future_into_py(
                py,
                self.shared.clone().messages(account_id(account)?, ids),
            )
        }

        /// Mirror changes for `account` after state string `since` (`None`
        /// for everything).
        #[pyo3(signature = (account, since=None, limit=500))]
        fn changes<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            since: Option<String>,
            limit: u32,
        ) -> PyResult<Bound<'py, PyAny>> {
            let fut = self
                .shared
                .clone()
                .changes(account_id(account)?, parse_since(since)?, limit);
            pyo3_async_runtimes::tokio::future_into_py(py, fut)
        }

        /// The raw RFC 822 bytes of an imported message.
        fn read_blob<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            message_id: &str,
        ) -> PyResult<Bound<'py, PyAny>> {
            let fut = self
                .shared
                .clone()
                .read_blob(account_id(account)?, MessageId(message_id.to_owned()));
            pyo3_async_runtimes::tokio::future_into_py(py, fut)
        }
    }

    #[pymethods]
    impl BlockingRunner {
        /// `database_url`: `postgres://…` (Unix sockets:
        /// `postgres:///db?host=/socket/dir`) or `sqlite:…`. `blob_url`:
        /// `gs://…`, `s3://…`, `file:///…` or `memory:///`.
        /// `token_source(account)` returns `(access_token, expires_at_ms)`
        /// or raises `DelegationRevoked`. `watch_topic`: the Pub/Sub topic
        /// for push, or `None` to rely on polling. `gmail_origin` overrides
        /// Gmail's URL (tests). `confirm_timeout_s`: how long after
        /// delivering an action its confirmation fetch runs.
        #[new]
        #[allow(clippy::too_many_arguments)]
        #[pyo3(signature = (database_url, blob_url, token_source, *, watch_topic=None, gmail_origin=None, request_timeout_s=60.0, confirm_timeout_s=60.0))]
        fn new(
            py: Python<'_>,
            database_url: &str,
            blob_url: &str,
            token_source: Py<PyAny>,
            watch_topic: Option<String>,
            gmail_origin: Option<String>,
            request_timeout_s: f64,
            confirm_timeout_s: f64,
        ) -> PyResult<Self> {
            Ok(Self {
                shared: open(
                    py,
                    database_url,
                    blob_url,
                    token_source,
                    watch_topic,
                    gmail_origin,
                    request_timeout_s,
                    confirm_timeout_s,
                )?,
            })
        }

        /// Create or upgrade mailtide's tables (safe to call concurrently).
        fn migrate(&self, py: Python<'_>) -> PyResult<()> {
            let fut = self.shared.clone().migrate();
            py.detach(|| runtime().block_on(fut))
        }

        /// Sync `account` until caught up, blocked or `budget_s` elapses.
        #[pyo3(signature = (account, budget_s=60.0))]
        fn run(&self, py: Python<'_>, account: &str, budget_s: f64) -> PyResult<RunOutcome> {
            let fut = self
                .shared
                .clone()
                .run(account_id(account)?, seconds(budget_s, "budget_s")?);
            py.detach(|| runtime().block_on(fut))
        }

        /// See `Runner.act`.
        #[pyo3(signature = (account, action, *, message_ids=None, thread_id=None, add_labels=Vec::new(), remove_labels=Vec::new(), destination="inbox", budget_s=30.0))]
        #[allow(clippy::too_many_arguments)]
        fn act(
            &self,
            py: Python<'_>,
            account: &str,
            action: &str,
            message_ids: Option<Vec<String>>,
            thread_id: Option<String>,
            add_labels: Vec<String>,
            remove_labels: Vec<String>,
            destination: &str,
            budget_s: f64,
        ) -> PyResult<RunOutcome> {
            let request = action_request(
                action,
                message_ids,
                thread_id,
                add_labels,
                remove_labels,
                destination,
            )?;
            let fut = self.shared.clone().act(
                account_id(account)?,
                request,
                seconds(budget_s, "budget_s")?,
            );
            py.detach(|| runtime().block_on(fut))
        }

        /// How these messages should look; unknown ids are left out.
        fn messages(
            &self,
            py: Python<'_>,
            account: &str,
            message_ids: Vec<String>,
        ) -> PyResult<Vec<MessageView>> {
            let ids = message_ids.into_iter().map(MessageId).collect();
            let fut = self.shared.clone().messages(account_id(account)?, ids);
            py.detach(|| runtime().block_on(fut))
        }

        /// Mirror changes for `account` after state string `since`.
        #[pyo3(signature = (account, since=None, limit=500))]
        fn changes(
            &self,
            py: Python<'_>,
            account: &str,
            since: Option<String>,
            limit: u32,
        ) -> PyResult<ChangePage> {
            let fut = self
                .shared
                .clone()
                .changes(account_id(account)?, parse_since(since)?, limit);
            py.detach(|| runtime().block_on(fut))
        }

        /// The raw RFC 822 bytes of an imported message.
        fn read_blob(
            &self,
            py: Python<'_>,
            account: &str,
            message_id: &str,
        ) -> PyResult<Py<PyBytes>> {
            let fut = self
                .shared
                .clone()
                .read_blob(account_id(account)?, MessageId(message_id.to_owned()));
            py.detach(|| runtime().block_on(fut))
        }
    }

    // ------------------------------------------------------------ functions

    /// Verify a Gmail push request (Pub/Sub OIDC token) and parse it.
    /// `jwks` is Google's certificate document (JSON bytes).
    #[pyfunction]
    #[pyo3(signature = (authorization, body, jwks, audience, service_account, now_ms=None))]
    fn verify_push(
        authorization: &str,
        body: &[u8],
        jwks: &[u8],
        audience: &str,
        service_account: &str,
        now_ms: Option<i64>,
    ) -> PyResult<Notification> {
        let jwks = Jwks::parse(jwks).map_err(|e| InvalidPush::new_err(format!("{e:?}")))?;
        let expect = PushExpectations {
            audience: audience.to_owned(),
            service_account: service_account.to_owned(),
        };
        let now = now_ms.map_or_else(mailtide_io::now, Timestamp);
        let notification = push::verify(authorization, body, &jwks, &expect, now)
            .map_err(|e| InvalidPush::new_err(format!("{e:?}")))?;
        Ok(Notification {
            account: notification.account.as_str().to_owned(),
            history_id: notification.history_id.0,
        })
    }

    /// Parse raw RFC 822 bytes into headers, bodies and attachments.
    #[pyfunction]
    fn parse_message(py: Python<'_>, raw: &[u8]) -> PyResult<ParsedMessage> {
        let parsed = py
            .detach(|| mailtide_mime::parse(raw))
            .map_err(|e| PyValueError::new_err(e.0))?;
        let attachments = parsed
            .attachments
            .into_iter()
            .map(|a| {
                Py::new(
                    py,
                    Attachment {
                        filename: a.filename,
                        content_type: a.content_type,
                        content_id: a.content_id,
                        data: PyBytes::new(py, &a.bytes).unbind(),
                    },
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(ParsedMessage {
            headers: parsed.headers,
            text: parsed.text,
            html: parsed.html,
            attachments,
        })
    }
}
