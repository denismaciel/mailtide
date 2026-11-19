//! Python bindings for mailtide, exposed as `mailtide._core`.
//!
//! The Python surface is deliberately small and typed (review #17): a
//! `Runner` the host calls from its scheduler, the change feed, blob reads,
//! push verification and MIME parsing. Everything else stays in Rust. Calls
//! release the GIL while Rust works.

use pyo3::prelude::*;

#[pymodule]
mod _core {
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
                status: report.status.map(Status::from),
            }
        }
    }

    // ------------------------------------------------------------ runner

    /// Tokens from a Python callable `(account) -> (token, expires_at_ms)`.
    struct PyTokens(Py<PyAny>);

    impl TokenSource for PyTokens {
        async fn token(&self, account: &AccountId) -> Result<AccessToken, Failure> {
            Python::attach(|py| {
                let result = self
                    .0
                    .call1(py, (account.as_str(),))
                    .and_then(|value| value.extract::<(String, i64)>(py));
                match result {
                    Ok((secret, expires_at)) => Ok(AccessToken {
                        secret,
                        expires_at: Timestamp(expires_at),
                    }),
                    Err(err) if err.is_instance_of::<DelegationRevoked>(py) => {
                        Err(Failure::Blocking(BlockingFailure {
                            class: Blocking::Account(AccountProblem::DelegationRevoked),
                            detail: err.to_string(),
                        }))
                    }
                    Err(err) => Err(Failure::Retryable(RetryableFailure {
                        class: Retryable::Transient,
                        retry_after: None,
                        detail: format!("token source failed: {err}"),
                    })),
                }
            })
        }
    }

    type Inner = runner::Runner<SqlStore, ObjectBlobs, ReqwestTransport, LocalGate, PyTokens>;

    /// Syncs Gmail accounts into a database and blob store. Build one per
    /// process; it is safe to call from several threads.
    #[pyclass(frozen)]
    struct Runner {
        inner: Inner,
        runtime: tokio::runtime::Runtime,
    }

    #[pymethods]
    impl Runner {
        /// `database_url`: `postgres://…` or `sqlite:…`. `blob_url`: `gs://…`,
        /// `s3://…`, `file:///…` or `memory:///`. `token_source(account)`
        /// returns `(access_token, expires_at_ms)` or raises
        /// `DelegationRevoked`. `watch_topic`: the Pub/Sub topic for push, or
        /// `None` to rely on polling. `gmail_origin` overrides Gmail's URL
        /// (tests, proxies). `confirm_timeout_s`: how long after delivering an
        /// action its confirmation fetch runs.
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
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| error("runtime", e))?;
            let store = py
                .detach(|| runtime.block_on(SqlStore::connect(database_url)))
                .map_err(|e| error("database", e))?;
            let blobs = if blob_url == "memory:///" {
                ObjectBlobs::memory()
            } else {
                ObjectBlobs::from_url(blob_url).map_err(|e| error("blob store", e))?
            };
            let timeout = Duration::try_from_secs_f64(request_timeout_s)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            let transport = match gmail_origin {
                Some(origin) => ReqwestTransport::with_origin(&origin, timeout),
                None => ReqwestTransport::new(timeout),
            }
            .map_err(|e| error("http client", e))?;
            let config = RunnerConfig {
                engine: EngineConfig {
                    confirm_timeout: Duration::try_from_secs_f64(confirm_timeout_s)
                        .map_err(|e| PyValueError::new_err(e.to_string()))?,
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
            Ok(Self { inner, runtime })
        }

        /// Create mailtide's tables if they don't exist.
        fn migrate(&self, py: Python<'_>) -> PyResult<()> {
            py.detach(|| self.runtime.block_on(self.inner.store().migrate()))
                .map_err(|e| error("migrate", e))
        }

        /// Sync `account` until caught up, blocked or `budget_s` elapses.
        /// Call it on a schedule and whenever a push arrives.
        #[pyo3(signature = (account, budget_s=60.0))]
        fn run(&self, py: Python<'_>, account: &str, budget_s: f64) -> PyResult<RunOutcome> {
            let account = account_id(account)?;
            let budget = Duration::try_from_secs_f64(budget_s)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            let outcome = py
                .detach(|| self.runtime.block_on(self.inner.run(&account, budget)))
                .map_err(|e| error("run", e))?;
            Ok(outcome.into())
        }

        /// Act on messages (or a whole thread) as the user asked, and deliver
        /// it to Gmail. `action` is one of `mark_read`, `mark_unread`, `star`,
        /// `unstar`, `mark_important`, `mark_not_important`, `archive`,
        /// `move_to_inbox`, `trash`, `restore`, `spam`, `not_spam`,
        /// `change_labels`. The change shows in `messages()` at once and
        /// settles when Gmail confirms it.
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
            let account = account_id(account)?;
            let action = parse_action(action, add_labels, remove_labels, destination)?;
            let request = match (message_ids, thread_id) {
                (Some(ids), None) => ActionRequest::Messages {
                    ids: ids.into_iter().map(MessageId).collect(),
                    action,
                },
                (None, Some(thread)) => ActionRequest::Thread {
                    id: ThreadId(thread),
                    action,
                },
                _ => {
                    return Err(PyValueError::new_err(
                        "pass exactly one of message_ids or thread_id",
                    ));
                }
            };
            let budget = Duration::try_from_secs_f64(budget_s)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            let outcome = py
                .detach(|| {
                    self.runtime
                        .block_on(self.inner.act(&account, request, budget))
                })
                .map_err(|e| error("act", e))?;
            Ok(outcome.into())
        }

        /// How these messages should look; unknown ids are left out.
        fn messages(
            &self,
            py: Python<'_>,
            account: &str,
            message_ids: Vec<String>,
        ) -> PyResult<Vec<MessageView>> {
            let account = account_id(account)?;
            let ids: Vec<MessageId> = message_ids.into_iter().map(MessageId).collect();
            let views = py
                .detach(|| {
                    self.runtime
                        .block_on(self.inner.store().messages(&account, &ids))
                })
                .map_err(|e| error("messages", e))?;
            Ok(views.into_iter().map(MessageView::from).collect())
        }

        /// Mirror changes for `account` after state string `since` (`None`
        /// for everything).
        #[pyo3(signature = (account, since=None, limit=500))]
        fn changes(
            &self,
            py: Python<'_>,
            account: &str,
            since: Option<String>,
            limit: u32,
        ) -> PyResult<ChangePage> {
            let account = account_id(account)?;
            let since = match since {
                None => StateSeq(0),
                Some(state) => StateSeq::decode(&StateString(state.clone())).map_err(|_| {
                    PyValueError::new_err(format!("invalid state string {state:?}"))
                })?,
            };
            let page = py
                .detach(|| {
                    self.runtime
                        .block_on(self.inner.store().changes(&account, since, limit))
                })
                .map_err(|e| error("changes", e))?;
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
        }

        /// The raw RFC 822 bytes of an imported message.
        fn read_blob<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            message_id: &str,
        ) -> PyResult<Bound<'py, PyBytes>> {
            let key = blob_key(&account_id(account)?, &MessageId(message_id.to_owned()));
            let bytes = py
                .detach(|| self.runtime.block_on(self.inner.blobs().get(&key)))
                .map_err(|e| error("read blob", e))?;
            Ok(PyBytes::new(py, &bytes))
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
