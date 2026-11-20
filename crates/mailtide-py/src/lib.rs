//! Python bindings for mailtide, exposed as `mailtide._core`.
//!
//! The Python surface is deliberately small and typed (review #17): a
//! `Runner` the host calls from its scheduler, the change feed, blob reads,
//! push verification and MIME parsing. Everything else stays in Rust. Calls
//! release the GIL while Rust works.

use pyo3::prelude::*;

mod logbridge;
mod settings;

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

    use mailtide::ids::{AccountId, LabelId, MessageId, ThreadId, Timestamp};
    use mailtide::outbox::{
        Action, ActionRequest, Importance, LabelDelta, ReadState, RestoreDestination, StarState,
    };
    use mailtide::retry::{AccountProblem, Blocking, BlockingFailure};
    use mailtide::revision::{FeedPosition, StateSeq, StateString};
    use mailtide::state::SyncPhase;
    use mailtide_gmail::push::{self, Jwks, PushExpectations};
    use mailtide_io::auth::CachedTokens;
    use mailtide_io::auth::{AccessToken, TokenError, TokenSource};
    use mailtide_io::blobs::{BlobError, BlobStore, ObjectBlobs, blob_key};
    use mailtide_io::quota_gate::LocalGate;
    use mailtide_io::runner::{self, RunError, RunnerConfig, StopReason};
    use mailtide_io::transport::ReqwestTransport;
    use mailtide_store::sql::{SchemaState, SqlStore};
    use mailtide_store::{Store, StoreError};

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

    create_exception!(
        _core,
        StoreUnavailable,
        MailtideError,
        "The database couldn't be reached or failed; nothing was applied. Retry."
    );
    create_exception!(
        _core,
        OutcomeUnknown,
        MailtideError,
        "The database failed mid-commit: it may or may not have applied. Retry; commits are fenced and idempotent."
    );
    create_exception!(
        _core,
        CorruptState,
        MailtideError,
        "Stored data failed validation when read back. Needs an operator."
    );
    create_exception!(
        _core,
        BlobUnavailable,
        MailtideError,
        "Blob storage couldn't be reached or failed. Retry."
    );
    create_exception!(
        _core,
        QuotaGateError,
        MailtideError,
        "The quota gate failed (not a deferral, which is an outcome). Retry."
    );
    create_exception!(
        _core,
        SchemaOutOfDate,
        MailtideError,
        "The database's mailtide schema doesn't match this build: run migrations (or upgrade)."
    );
    create_exception!(
        _core,
        ReadOnlyMode,
        MailtideError,
        "The runner is read-only: actions are refused."
    );
    create_exception!(
        _core,
        AccountNotTracked,
        MailtideError,
        "The account isn't tracked: call track(account) to start mirroring it."
    );
    create_exception!(
        _core,
        BlobNotFound,
        MailtideError,
        "No stored content for that message (never imported, or deleted)."
    );
    create_exception!(
        _core,
        Superseded,
        MailtideError,
        "A newer mailtide version already wrote to this account; this one must not."
    );

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        let py = m.py();
        m.add("MailtideError", py.get_type::<MailtideError>())?;
        m.add("DelegationRevoked", py.get_type::<DelegationRevoked>())?;
        m.add("InvalidPush", py.get_type::<InvalidPush>())?;
        m.add("StoreUnavailable", py.get_type::<StoreUnavailable>())?;
        m.add("OutcomeUnknown", py.get_type::<OutcomeUnknown>())?;
        m.add("CorruptState", py.get_type::<CorruptState>())?;
        m.add("BlobUnavailable", py.get_type::<BlobUnavailable>())?;
        m.add("QuotaGateError", py.get_type::<QuotaGateError>())?;
        m.add("Superseded", py.get_type::<Superseded>())?;
        m.add("SchemaOutOfDate", py.get_type::<SchemaOutOfDate>())?;
        m.add("ReadOnlyMode", py.get_type::<ReadOnlyMode>())?;
        m.add("AccountNotTracked", py.get_type::<AccountNotTracked>())?;
        m.add("BlobNotFound", py.get_type::<BlobNotFound>())?;
        m.add("SCHEMA_VERSION", mailtide_store::sql::SCHEMA_VERSION)?;
        Ok(())
    }

    /// An internal failure with no better class (encoding, runtime).
    fn error(what: &str, detail: impl std::fmt::Debug) -> PyErr {
        MailtideError::new_err(format!("{what}: {detail:?}"))
    }

    fn store_error(error: StoreError) -> PyErr {
        match error {
            StoreError::Unavailable(detail) => StoreUnavailable::new_err(detail),
            StoreError::OutcomeUnknown(detail) => OutcomeUnknown::new_err(detail),
            StoreError::Corrupt(detail) => CorruptState::new_err(detail),
            StoreError::Misconfigured(detail) => PyValueError::new_err(detail),
            StoreError::Rejected(rejection) => Superseded::new_err(format!("{rejection:?}")),
            StoreError::NotTracked => AccountNotTracked::new_err(
                "the account isn't tracked: call track(account) to start mirroring it",
            ),
        }
    }

    fn blob_error(error: BlobError) -> PyErr {
        match error {
            BlobError::Unavailable(detail) => BlobUnavailable::new_err(detail),
            BlobError::Misconfigured(detail) => PyValueError::new_err(detail),
            BlobError::NotFound => BlobNotFound::new_err("no stored content for that message"),
        }
    }

    fn run_error(error: RunError) -> PyErr {
        match error {
            RunError::Store(e) => store_error(e),
            RunError::Corrupt(detail) => CorruptState::new_err(detail),
            RunError::Quota(detail) => QuotaGateError::new_err(detail),
            RunError::Blobs(detail) => BlobUnavailable::new_err(detail),
            RunError::Invariant(detail) => MailtideError::new_err(detail),
            RunError::ReadOnly => ReadOnlyMode::new_err(
                "this runner is read-only (config access=\"read_only\"): actions are refused",
            ),
        }
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
            Blocking::Account(AccountProblem::ApiAccessDenied) => "api_access_denied",
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
        /// Requests that failed and will be retried after backoff (token
        /// source, Gmail or blob store). Nonzero with `stop == "idle"` means
        /// the run isn't caught up: it is waiting to retry.
        failed_requests: u32,
        /// The last failure's class, e.g. `transient` or `blob_store`.
        last_failure: Option<&'static str>,
        status: Option<Status>,
    }

    /// Changes to the mirror after a state string, whole commits only.
    #[pyclass(frozen, get_all)]
    struct ChangePage {
        /// The changes, oldest first: `MessageDiscovered`, `MessageImported`,
        /// `LabelsChanged`, `OverlayChanged`, `MessageDeleted` or
        /// `LabelDefinitionsChanged` (match on the class or on `kind`).
        changes: Vec<Py<PyAny>>,
        /// The state string to ask from next.
        next: String,
        has_more: bool,
    }

    /// The requested state was compacted away; rebuild from `message_ids` +
    /// `messages`, then follow the feed from `status().state`.
    #[pyclass(frozen, get_all)]
    struct StateTooOld {
        kind: &'static str,
        /// The oldest state the feed can still continue from.
        oldest: String,
    }

    fn names(labels: &mailtide::labels::LabelSet) -> Vec<String> {
        labels.iter().map(|l| l.0.clone()).collect()
    }

    /// A message row appeared, with the labels Gmail showed then.
    #[pyclass(frozen, get_all)]
    struct MessageDiscovered {
        kind: &'static str,
        state: String,
        index: u32,
        id: String,
        thread_id: String,
        labels: Vec<String>,
    }

    /// The message's raw content was stored for the first time (once per
    /// message): drive new-mail processing from this.
    #[pyclass(frozen, get_all)]
    struct MessageImported {
        kind: &'static str,
        state: String,
        index: u32,
        id: String,
        thread_id: String,
        /// Labels as of the import (`SENT` marks outgoing mail).
        labels: Vec<String>,
        internal_date_ms: i64,
        blob: String,
        size: u64,
        /// What the mirror stores for the message (its envelope), read with
        /// the feed; `None` once the message is deleted.
        content: Option<Py<MessageContent>>,
    }

    /// Gmail's labels for the message changed.
    #[pyclass(frozen, get_all)]
    struct LabelsChanged {
        kind: &'static str,
        state: String,
        index: u32,
        id: String,
        thread_id: String,
        added: Vec<String>,
        removed: Vec<String>,
    }

    /// Pending user actions changed how the message should look.
    #[pyclass(frozen, get_all)]
    struct OverlayChanged {
        kind: &'static str,
        state: String,
        index: u32,
        id: String,
        thread_id: String,
        /// The labels users should see now.
        labels: Vec<String>,
    }

    /// The message was deleted in Gmail.
    #[pyclass(frozen, get_all)]
    struct MessageDeleted {
        kind: &'static str,
        state: String,
        index: u32,
        id: String,
        /// Its thread and last labels, if the mirror held the message.
        thread_id: Option<String>,
        labels: Option<Vec<String>>,
    }

    /// The account's label definitions were replaced.
    #[pyclass(frozen, get_all)]
    struct LabelDefinitionsChanged {
        kind: &'static str,
        state: String,
        index: u32,
    }

    /// One feed entry as its Python class (exhaustive: a new change kind is a
    /// compile error here).
    #[allow(clippy::too_many_lines)] // one flat arm per change kind
    fn change_object(
        py: Python<'_>,
        generation: u64,
        entry: &mailtide::revision::ChangeEntry,
    ) -> PyResult<Py<PyAny>> {
        use mailtide::revision::Change;
        let state = position(generation, entry.state);
        let index = entry.index;
        Ok(match &entry.change {
            Change::MessageDiscovered {
                id,
                thread_id,
                labels,
            } => Py::new(
                py,
                MessageDiscovered {
                    kind: "message_discovered",
                    state,
                    index,
                    id: id.0.clone(),
                    thread_id: thread_id.0.clone(),
                    labels: names(labels),
                },
            )?
            .into_any(),
            Change::MessageImported {
                id,
                thread_id,
                labels,
                internal_date,
                blob,
                size,
            } => Py::new(
                py,
                MessageImported {
                    kind: "message_imported",
                    state,
                    index,
                    id: id.0.clone(),
                    thread_id: thread_id.0.clone(),
                    labels: names(labels),
                    internal_date_ms: internal_date.0,
                    blob: blob.0.clone(),
                    size: *size,
                    content: entry
                        .content
                        .clone()
                        .map(|stored| message_content(py, stored))
                        .transpose()?,
                },
            )?
            .into_any(),
            Change::LabelsChanged {
                id,
                thread_id,
                added,
                removed,
            } => Py::new(
                py,
                LabelsChanged {
                    kind: "labels_changed",
                    state,
                    index,
                    id: id.0.clone(),
                    thread_id: thread_id.0.clone(),
                    added: names(added),
                    removed: names(removed),
                },
            )?
            .into_any(),
            Change::OverlayChanged {
                id,
                thread_id,
                labels,
            } => Py::new(
                py,
                OverlayChanged {
                    kind: "overlay_changed",
                    state,
                    index,
                    id: id.0.clone(),
                    thread_id: thread_id.0.clone(),
                    labels: names(labels),
                },
            )?
            .into_any(),
            Change::MessageDeleted {
                id,
                thread_id,
                labels,
            } => Py::new(
                py,
                MessageDeleted {
                    kind: "message_deleted",
                    state,
                    index,
                    id: id.0.clone(),
                    thread_id: thread_id.as_ref().map(|t| t.0.clone()),
                    labels: labels.as_ref().map(names),
                },
            )?
            .into_any(),
            Change::LabelDefinitionsChanged => Py::new(
                py,
                LabelDefinitionsChanged {
                    kind: "label_definitions_changed",
                    state,
                    index,
                },
            )?
            .into_any(),
        })
    }

    /// Raw content of an imported message: where it is and its header facts.
    #[pyclass(frozen, get_all)]
    struct MessageContent {
        /// The blob key (`{account}/{id}.eml` under the blob URL's prefix).
        blob: String,
        size: u64,
        internal_date_ms: i64,
        rfc822_message_id: Option<String>,
        in_reply_to: Option<String>,
        references: Vec<String>,
        sender: Option<String>,
        to: Vec<String>,
        cc: Vec<String>,
        subject: Option<String>,
        snippet: Option<String>,
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
        /// `None` until the raw content is imported.
        content: Option<Py<MessageContent>>,
    }

    fn message_content(
        py: Python<'_>,
        stored: mailtide::mirror::StoredContent,
    ) -> PyResult<Py<MessageContent>> {
        Py::new(
            py,
            MessageContent {
                blob: stored.blob.0,
                size: stored.size,
                internal_date_ms: stored.internal_date.0,
                rfc822_message_id: stored.headers.rfc822_message_id,
                in_reply_to: stored.headers.in_reply_to,
                references: stored.headers.references,
                sender: stored.headers.from,
                to: stored.headers.to,
                cc: stored.headers.cc,
                subject: stored.headers.subject,
                snippet: stored.headers.snippet,
            },
        )
    }

    fn message_view(py: Python<'_>, view: mailtide::mirror::MessageView) -> PyResult<MessageView> {
        let content = view
            .content
            .map(|stored| message_content(py, stored))
            .transpose()?;
        Ok(MessageView {
            labels: names(&view.labels),
            observed_labels: names(&view.observed_labels),
            id: view.id.0,
            thread_id: view.thread_id.0,
            checkpoint: view.checkpoint.map(|c| c.0),
            unverified: view.unverified,
            pending_actions: view.pending_actions,
            blocked_actions: view.blocked_actions,
            content,
        })
    }

    /// A label definition: what a label id on a message is called.
    #[pyclass(frozen, get_all)]
    struct Label {
        id: String,
        name: String,
        /// `system` (`INBOX`, `UNREAD`, …) or `user`.
        kind: &'static str,
    }

    impl From<mailtide::gmail::Label> for Label {
        fn from(label: mailtide::gmail::Label) -> Self {
            Self {
                id: label.id.0,
                name: label.name,
                kind: match label.kind {
                    mailtide::gmail::LabelKind::System => "system",
                    mailtide::gmail::LabelKind::User => "user",
                },
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
            "change_labels" => Action::ChangeLabels(
                LabelDelta::new(
                    add_labels.into_iter().map(LabelId).collect(),
                    remove_labels.into_iter().map(LabelId).collect(),
                )
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
            ),
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
                failed_requests: report.failed_requests,
                last_failure: report.last_failure,
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

    fn token_failure(py: Python<'_>, err: &PyErr) -> TokenError {
        if err.is_instance_of::<DelegationRevoked>(py) {
            TokenError::Account(
                AccountProblem::DelegationRevoked,
                "token source raised DelegationRevoked".into(),
            )
        } else {
            TokenError::Transient(format!("token source failed: {}", err.get_type(py)))
        }
    }

    fn transient(detail: &str) -> TokenError {
        TokenError::Transient(detail.to_owned())
    }

    impl TokenSource for PyTokens {
        async fn token(&self, account: &AccountId) -> Result<AccessToken, TokenError> {
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

    type Inner =
        runner::Runner<SqlStore, ObjectBlobs, ReqwestTransport, LocalGate, CachedTokens<PyTokens>>;

    // ------------------------------------------------------------ configuration

    /// `mailtide.config.Database`, read by attribute.
    #[derive(FromPyObject)]
    struct DatabaseArg {
        url: String,
        max_connections: i64,
        min_connections: i64,
        acquire_timeout_s: f64,
        idle_timeout_s: Option<f64>,
        max_lifetime_s: Option<f64>,
        connect: String,
    }

    #[derive(FromPyObject)]
    struct BlobsArg {
        url: String,
        options: std::collections::BTreeMap<String, String>,
    }

    #[derive(FromPyObject)]
    struct WatchArg {
        kind: String,
        #[pyo3(default)]
        name: Option<String>,
    }

    #[derive(FromPyObject)]
    struct HttpArg {
        request_timeout_s: f64,
        connect_timeout_s: f64,
        user_agent: Option<String>,
        gmail_origin: Option<String>,
    }

    #[derive(FromPyObject)]
    #[allow(clippy::struct_field_names)] // names match the Python attributes
    struct EngineArg {
        poll_active_s: f64,
        poll_idle_s: f64,
        idle_after_s: f64,
        watch_renew_margin_s: f64,
        draft_settle_s: f64,
        confirm_timeout_s: f64,
    }

    #[derive(FromPyObject)]
    struct QuotaArg {
        account_per_minute: i64,
        project_per_minute: i64,
        project_per_day: i64,
        account_concurrency: i64,
        project_concurrency: i64,
        account_bytes_per_day: i64,
    }

    #[derive(FromPyObject)]
    struct LogArg {
        level: String,
        accounts: String,
        logger: String,
    }

    /// `mailtide.config.Config`.
    #[derive(FromPyObject)]
    struct ConfigArg {
        access: String,
        database: DatabaseArg,
        blobs: BlobsArg,
        watch: WatchArg,
        http: HttpArg,
        engine: EngineArg,
        quota: QuotaArg,
        log: LogArg,
        token_cache_margin_s: f64,
    }

    impl From<ConfigArg> for crate::settings::RawConfig {
        fn from(c: ConfigArg) -> Self {
            Self {
                access: c.access,
                database: crate::settings::RawDatabase {
                    url: c.database.url,
                    max_connections: c.database.max_connections,
                    min_connections: c.database.min_connections,
                    acquire_timeout_s: c.database.acquire_timeout_s,
                    idle_timeout_s: c.database.idle_timeout_s,
                    max_lifetime_s: c.database.max_lifetime_s,
                    connect: c.database.connect,
                },
                blobs_url: c.blobs.url,
                blobs_options: c.blobs.options,
                watch: crate::settings::RawWatch {
                    kind: c.watch.kind,
                    name: c.watch.name,
                },
                http: crate::settings::RawHttp {
                    request_timeout_s: c.http.request_timeout_s,
                    connect_timeout_s: c.http.connect_timeout_s,
                    user_agent: c.http.user_agent,
                    gmail_origin: c.http.gmail_origin,
                },
                engine: crate::settings::RawEngine {
                    poll_active_s: c.engine.poll_active_s,
                    poll_idle_s: c.engine.poll_idle_s,
                    idle_after_s: c.engine.idle_after_s,
                    watch_renew_margin_s: c.engine.watch_renew_margin_s,
                    draft_settle_s: c.engine.draft_settle_s,
                    confirm_timeout_s: c.engine.confirm_timeout_s,
                },
                quota: crate::settings::RawQuota {
                    account_per_minute: c.quota.account_per_minute,
                    project_per_minute: c.quota.project_per_minute,
                    project_per_day: c.quota.project_per_day,
                    account_concurrency: c.quota.account_concurrency,
                    project_concurrency: c.quota.project_concurrency,
                    account_bytes_per_day: c.quota.account_bytes_per_day,
                },
                log: crate::settings::RawLog {
                    level: c.log.level,
                    accounts: c.log.accounts,
                    logger: c.log.logger,
                },
                token_cache_margin_s: c.token_cache_margin_s,
            }
        }
    }

    /// Everything both runners share.
    struct Shared {
        inner: Inner,
        /// This runner's log bridge; every future it runs carries it.
        dispatch: tracing::Dispatch,
        /// Set once the schema was seen current (checked before syncing).
        schema_current: std::sync::atomic::AtomicBool,
    }

    /// Parse and validate the configuration, then build the runner. Nothing
    /// connects unless the database is configured `connect="eager"` (or is
    /// SQLite).
    fn open(
        py: Python<'_>,
        config: ConfigArg,
        token_source: Py<PyAny>,
        clock: Option<Py<PyAny>>,
    ) -> PyResult<Arc<Shared>> {
        let settings = crate::settings::validate(config.into())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let store = py
            .detach(|| {
                runtime().block_on(SqlStore::connect(&settings.database_url, &settings.pool))
            })
            .map_err(store_error)?;
        let blobs = if settings.blobs_url == "memory:///" {
            ObjectBlobs::memory()
        } else {
            ObjectBlobs::from_url_with(&settings.blobs_url, &settings.blobs_options)
                .map_err(blob_error)?
        };
        let transport =
            ReqwestTransport::with_config(&settings.http).map_err(|e| error("http client", e))?;
        let inner = runner::Runner::new(
            store,
            blobs,
            transport,
            LocalGate::new(settings.budget),
            CachedTokens::new(PyTokens(token_source), settings.token_cache_margin),
            RunnerConfig {
                access: settings.access,
                engine: settings.engine,
                watch: settings.watch,
                log_accounts: settings.log_accounts,
            },
        );
        let dispatch = crate::logbridge::dispatch(settings.log_verbosity, settings.logger);
        let inner = match clock {
            None => inner,
            Some(clock) => inner.with_clock(mailtide_io::Clock::from_fn(move || {
                Python::attach(|py| {
                    clock
                        .call0(py)
                        .and_then(|v| v.extract::<i64>(py))
                        .map_or_else(|_| mailtide_io::now(), Timestamp)
                })
            })),
        };
        Ok(Arc::new(Shared {
            inner,
            dispatch,
            schema_current: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    // ------------------------------------------------------------ results

    /// What the store holds about an account (read without claiming it).
    #[pyclass(frozen, get_all)]
    struct AccountStatus {
        account: String,
        /// `bootstrapping`, `live`, `reconciling` or `blocked`.
        phase: &'static str,
        /// Why it is blocked, when `phase == "blocked"`.
        blocked: Option<String>,
        epoch: u64,
        cursor: Option<u64>,
        watch_expiration_ms: Option<i64>,
        last_activity_ms: Option<i64>,
        cooldown_until_ms: Option<i64>,
        /// The change-feed position; pass to `changes` to read from here.
        state: String,
        messages: u64,
        pending_imports: u64,
        unverified: u64,
        jobs_ready: u64,
        jobs_running: u64,
        jobs_blocked: u64,
        actions_pending: u64,
        actions_delivered: u64,
        actions_blocked: u64,
    }

    fn phase_name(phase: SyncPhase) -> &'static str {
        match phase {
            SyncPhase::Bootstrapping => "bootstrapping",
            SyncPhase::Live => "live",
            SyncPhase::Reconciling => "reconciling",
            SyncPhase::Blocked => "blocked",
        }
    }

    impl From<mailtide::status::AccountStatus> for AccountStatus {
        fn from(s: mailtide::status::AccountStatus) -> Self {
            let blocked = match &s.sync {
                mailtide::state::SyncState::Blocked { reason, .. } => Some(describe(reason)),
                mailtide::state::SyncState::Bootstrapping(_)
                | mailtide::state::SyncState::Live
                | mailtide::state::SyncState::Reconciling(_) => None,
            };
            Self {
                account: s.account.as_str().to_owned(),
                phase: phase_name(s.sync.phase()),
                blocked,
                epoch: s.epoch.0,
                cursor: s.cursor.map(|c| c.0),
                watch_expiration_ms: s.watch_expiration.map(|t| t.0),
                last_activity_ms: s.last_activity.map(|t| t.0),
                cooldown_until_ms: s.cooldown.map(|c| c.until.0),
                state: position(s.generation, s.state),
                messages: s.messages,
                pending_imports: s.pending_imports,
                unverified: s.unverified,
                jobs_ready: s.jobs.ready,
                jobs_running: s.jobs.running,
                jobs_blocked: s.jobs.blocked,
                actions_pending: s.actions.pending,
                actions_delivered: s.actions.delivered,
                actions_blocked: s.actions.blocked,
            }
        }
    }

    /// A running sync's progress, for heartbeats.
    #[pyclass(frozen, get_all)]
    struct Progress {
        commits: u32,
        imported: u32,
        gmail_requests: u32,
        elapsed_ms: u64,
    }

    /// What `track` did.
    #[pyclass(frozen, get_all)]
    struct TrackResult {
        /// `tracked` (mirroring starts on the next run) or `already_tracked`.
        outcome: &'static str,
        /// The feed generation, when newly tracked.
        generation: Option<u64>,
    }

    /// One message of a drift check.
    #[pyclass(frozen, get_all)]
    struct DriftResult {
        message_id: String,
        /// `matches`, `explained` (a reason the mirror differs for now),
        /// `differs` (drift), `missing_in_gmail`, `deferred` (quota: check
        /// later) or `failed` (Gmail failed the fetch).
        verdict: &'static str,
        /// For `explained`: `pending_actions`, `unverified` or `mirror_behind`.
        reason: Option<&'static str>,
        /// For `differs`: what each side shows.
        mirror_labels: Option<Vec<String>>,
        gmail_labels: Option<Vec<String>>,
        /// For `failed`: the failure class.
        failure: Option<&'static str>,
    }

    impl DriftResult {
        fn new(id: MessageId, checked: runner::Checked) -> Self {
            use mailtide::drift::{Reason, Verdict};
            let mut result = Self {
                message_id: id.0,
                verdict: "",
                reason: None,
                mirror_labels: None,
                gmail_labels: None,
                failure: None,
            };
            match checked {
                runner::Checked::Compared(Verdict::Matches) => result.verdict = "matches",
                runner::Checked::Compared(Verdict::Explained(reason)) => {
                    result.verdict = "explained";
                    result.reason = Some(match reason {
                        Reason::PendingActions => "pending_actions",
                        Reason::Unverified => "unverified",
                        Reason::MirrorBehind => "mirror_behind",
                    });
                }
                runner::Checked::Compared(Verdict::Differs { mirror, gmail }) => {
                    result.verdict = "differs";
                    result.mirror_labels = Some(names(&mirror));
                    result.gmail_labels = Some(names(&gmail));
                }
                runner::Checked::Compared(Verdict::MissingInGmail) => {
                    result.verdict = "missing_in_gmail";
                }
                runner::Checked::Deferred => result.verdict = "deferred",
                runner::Checked::Failed(kind) => {
                    result.verdict = "failed";
                    result.failure = Some(kind);
                }
            }
            result
        }
    }

    /// What a push did.
    #[pyclass(frozen, get_all)]
    struct NotifyResult {
        /// `current` (already synced past it), `ran` or `unknown_account`.
        outcome: &'static str,
        /// The sync, when one ran.
        run: Option<Py<RunOutcome>>,
    }

    /// What `resume` did.
    #[pyclass(frozen, get_all)]
    struct ResumeResult {
        /// `resumed`, `not_blocked` or `unknown` (never synced).
        outcome: &'static str,
        /// The phase it resumes in, when resumed.
        phase: Option<&'static str>,
        /// Jobs made ready again.
        jobs: u64,
    }

    impl From<mailtide::status::ResumeOutcome> for ResumeResult {
        fn from(outcome: mailtide::status::ResumeOutcome) -> Self {
            match outcome {
                mailtide::status::ResumeOutcome::Resumed { phase, jobs } => Self {
                    outcome: "resumed",
                    phase: Some(phase_name(phase)),
                    jobs,
                },
                mailtide::status::ResumeOutcome::NotBlocked => Self {
                    outcome: "not_blocked",
                    phase: None,
                    jobs: 0,
                },
                mailtide::status::ResumeOutcome::Unknown => Self {
                    outcome: "unknown",
                    phase: None,
                    jobs: 0,
                },
            }
        }
    }

    /// What `forget` removed.
    #[pyclass(frozen, get_all)]
    struct ForgetResult {
        /// `forgotten`, or `unknown` (the store never held it).
        outcome: &'static str,
        messages: u64,
        blobs: u64,
    }

    impl From<runner::Forgotten> for ForgetResult {
        fn from(f: runner::Forgotten) -> Self {
            match f.store {
                mailtide::status::ForgetOutcome::Forgotten { messages } => Self {
                    outcome: "forgotten",
                    messages,
                    blobs: f.blobs,
                },
                mailtide::status::ForgetOutcome::Unknown => Self {
                    outcome: "unknown",
                    messages: 0,
                    blobs: f.blobs,
                },
            }
        }
    }

    /// Whether the runner's dependencies answer.
    #[pyclass(frozen, get_all)]
    struct Health {
        /// `ok` or `unavailable`.
        database: &'static str,
        /// Why, when not ok.
        detail: Option<String>,
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

    /// A state string: a position in one generation of an account's feed.
    fn position(generation: u64, seq: StateSeq) -> String {
        FeedPosition { generation, seq }.encode().0
    }

    fn parse_state(state: &str) -> PyResult<FeedPosition> {
        FeedPosition::decode(&StateString(state.to_owned()))
            .map_err(|_| PyValueError::new_err(format!("invalid state string {state:?}")))
    }

    fn parse_since(since: Option<&str>) -> PyResult<Option<FeedPosition>> {
        since.map(parse_state).transpose()
    }

    fn change_page(page: &mailtide::revision::ChangePage) -> PyResult<ChangePage> {
        Python::attach(|py| {
            let changes = page
                .changes
                .iter()
                .map(|entry| change_object(py, page.generation, entry))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(ChangePage {
                changes,
                next: position(page.generation, page.next),
                has_more: page.has_more,
            })
        })
    }

    fn too_old(generation: u64, oldest: StateSeq) -> PyResult<Py<PyAny>> {
        Python::attach(|py| {
            Ok(Py::new(
                py,
                StateTooOld {
                    kind: "too_old",
                    oldest: position(generation, oldest),
                },
            )?
            .into_any())
        })
    }

    /// Where the database's schema stands against this build.
    #[pyclass(frozen, get_all)]
    struct SchemaStatus {
        /// `current`, `behind` (run migrations) or `ahead` (a newer mailtide
        /// migrated it; this build must not write).
        state: &'static str,
        applied: u32,
        expected: u32,
    }

    // The operations, as futures both runners drive.
    impl Shared {
        /// Refuse to sync against a schema this build doesn't match: a
        /// forgotten migration surfaces as one clear error, not a SQL error
        /// mid-run.
        async fn ensure_schema(&self) -> PyResult<()> {
            use std::sync::atomic::Ordering;
            if self.schema_current.load(Ordering::Relaxed) {
                return Ok(());
            }
            match self.inner.store().schema().await.map_err(store_error)? {
                SchemaState::Current { .. } => {
                    self.schema_current.store(true, Ordering::Relaxed);
                    Ok(())
                }
                SchemaState::Behind { applied, expected } => {
                    Err(SchemaOutOfDate::new_err(format!(
                        "mailtide schema is at version {applied}, this build needs {expected}: run migrate() (or mailtide.migrations()) first"
                    )))
                }
                SchemaState::Ahead { applied, expected } => Err(SchemaOutOfDate::new_err(format!(
                    "mailtide schema is at version {applied}, newer than this build ({expected}): upgrade mailtide"
                ))),
            }
        }

        async fn schema(self: Arc<Self>) -> PyResult<SchemaStatus> {
            Ok(
                match self.inner.store().schema().await.map_err(store_error)? {
                    SchemaState::Current { version } => SchemaStatus {
                        state: "current",
                        applied: version,
                        expected: version,
                    },
                    SchemaState::Behind { applied, expected } => SchemaStatus {
                        state: "behind",
                        applied,
                        expected,
                    },
                    SchemaState::Ahead { applied, expected } => SchemaStatus {
                        state: "ahead",
                        applied,
                        expected,
                    },
                },
            )
        }

        async fn migrate(self: Arc<Self>) -> PyResult<()> {
            self.inner.store().migrate().await.map_err(store_error)
        }

        async fn run(
            self: Arc<Self>,
            account: AccountId,
            budget: Duration,
            on_progress: Option<Py<PyAny>>,
        ) -> PyResult<RunOutcome> {
            self.ensure_schema().await?;
            let report = |p: &runner::Progress| {
                if let Some(callback) = &on_progress {
                    Python::attach(|py| {
                        let progress = Progress {
                            commits: p.commits,
                            imported: p.imported,
                            gmail_requests: p.gmail_requests,
                            elapsed_ms: u64::try_from(p.elapsed.as_millis()).unwrap_or(u64::MAX),
                        };
                        // A failing callback must not fail the sync.
                        let _ = callback.call1(py, (progress,));
                    });
                }
            };
            let progress: Option<&(dyn Fn(&runner::Progress) + Sync)> = match on_progress {
                Some(_) => Some(&report),
                None => None,
            };
            self.inner
                .run_reporting(&account, budget, progress)
                .await
                .map(RunOutcome::from)
                .map_err(run_error)
        }

        async fn act(
            self: Arc<Self>,
            account: AccountId,
            request: ActionRequest,
            budget: Duration,
        ) -> PyResult<RunOutcome> {
            self.ensure_schema().await?;
            self.inner
                .act(&account, request, budget)
                .await
                .map(RunOutcome::from)
                .map_err(run_error)
        }

        async fn admit(
            self: Arc<Self>,
            account: AccountId,
            request: ActionRequest,
        ) -> PyResult<u32> {
            self.ensure_schema().await?;
            self.inner
                .admit(&account, &request)
                .await
                .map_err(run_error)
        }

        async fn status(self: Arc<Self>, account: AccountId) -> PyResult<Option<AccountStatus>> {
            Ok(self
                .inner
                .status(&account)
                .await
                .map_err(run_error)?
                .map(AccountStatus::from))
        }

        async fn accounts(self: Arc<Self>) -> PyResult<Vec<String>> {
            let accounts = self.inner.accounts().await.map_err(run_error)?;
            Ok(accounts
                .into_iter()
                .map(|a| a.as_str().to_owned())
                .collect())
        }

        async fn drift(
            self: Arc<Self>,
            account: AccountId,
            ids: Vec<MessageId>,
            budget: Duration,
        ) -> PyResult<Vec<DriftResult>> {
            self.ensure_schema().await?;
            let checked = self
                .inner
                .drift(&account, &ids, budget)
                .await
                .map_err(run_error)?;
            Ok(checked
                .into_iter()
                .map(|(id, checked)| DriftResult::new(id, checked))
                .collect())
        }

        async fn notify(
            self: Arc<Self>,
            account: AccountId,
            history_id: u64,
            budget: Duration,
        ) -> PyResult<NotifyResult> {
            self.ensure_schema().await?;
            let notified = self
                .inner
                .notify(&account, mailtide::ids::HistoryId(history_id), budget)
                .await
                .map_err(run_error)?;
            Ok(match notified {
                runner::Notified::Current => NotifyResult {
                    outcome: "current",
                    run: None,
                },
                runner::Notified::UnknownAccount => NotifyResult {
                    outcome: "unknown_account",
                    run: None,
                },
                runner::Notified::Ran(outcome) => NotifyResult {
                    outcome: "ran",
                    run: Some(Python::attach(|py| {
                        Py::new(py, RunOutcome::from(*outcome))
                    })?),
                },
            })
        }

        async fn resume(self: Arc<Self>, account: AccountId) -> PyResult<ResumeResult> {
            self.inner
                .resume(&account)
                .await
                .map(ResumeResult::from)
                .map_err(run_error)
        }

        async fn forget(self: Arc<Self>, account: AccountId) -> PyResult<ForgetResult> {
            self.inner
                .forget(&account)
                .await
                .map(ForgetResult::from)
                .map_err(run_error)
        }

        async fn health(self: Arc<Self>) -> PyResult<Health> {
            Ok(match self.inner.store().ping().await {
                Ok(()) => Health {
                    database: "ok",
                    detail: None,
                },
                Err(e) => Health {
                    database: "unavailable",
                    detail: Some(format!("{e:?}")),
                },
            })
        }

        async fn labels(self: Arc<Self>, account: AccountId) -> PyResult<Vec<Label>> {
            let labels = self.inner.labels(&account).await.map_err(run_error)?;
            Ok(labels.into_iter().map(Label::from).collect())
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
                .map_err(store_error)?;
            Python::attach(|py| views.into_iter().map(|v| message_view(py, v)).collect())
        }

        async fn changes(
            self: Arc<Self>,
            account: AccountId,
            since: Option<FeedPosition>,
            limit: u32,
        ) -> PyResult<Py<PyAny>> {
            let seq = since.map_or(StateSeq(0), |at| at.seq);
            let feed = self
                .inner
                .store()
                .changes(&account, seq, limit)
                .await
                .map_err(store_error)?;
            match feed {
                mailtide::revision::ChangeFeed::Page(page) => {
                    // A position from another incarnation of the account, or
                    // beyond its log, can't be resumed from: rebuild.
                    if let Some(at) = since
                        && !at.fits(page.generation, page.head)
                    {
                        return too_old(page.generation, page.head);
                    }
                    let page = change_page(&page)?;
                    Python::attach(|py| Ok(Py::new(py, page)?.into_any()))
                }
                mailtide::revision::ChangeFeed::TooOld { generation, oldest } => {
                    too_old(generation, oldest)
                }
            }
        }

        async fn compact(
            self: Arc<Self>,
            account: AccountId,
            through: FeedPosition,
        ) -> PyResult<u64> {
            let status = self
                .inner
                .store()
                .status(&account)
                .await
                .map_err(store_error)?;
            // A position from another incarnation compacts nothing.
            if status.is_none_or(|s| s.generation != through.generation) {
                return Ok(0);
            }
            self.inner
                .store()
                .compact(&account, through.seq)
                .await
                .map_err(store_error)
        }

        async fn track(self: Arc<Self>, account: AccountId) -> PyResult<TrackResult> {
            use mailtide::status::TrackOutcome;
            self.ensure_schema().await?;
            Ok(match self.inner.track(&account).await.map_err(run_error)? {
                TrackOutcome::Tracked { generation } => TrackResult {
                    outcome: "tracked",
                    generation: Some(generation),
                },
                TrackOutcome::AlreadyTracked => TrackResult {
                    outcome: "already_tracked",
                    generation: None,
                },
            })
        }

        async fn message_ids(
            self: Arc<Self>,
            account: AccountId,
            after: Option<MessageId>,
            limit: u32,
        ) -> PyResult<Vec<String>> {
            let ids = self
                .inner
                .store()
                .message_ids(&account, after.as_ref(), limit)
                .await
                .map_err(store_error)?;
            Ok(ids.into_iter().map(|id| id.0).collect())
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
                .map_err(blob_error)?;
            Ok(Python::attach(|py| PyBytes::new(py, &bytes).unbind()))
        }
    }

    /// The async runner core (`mailtide.Runner` wraps it in coroutines).
    #[pyclass(frozen)]
    struct Runner {
        shared: Arc<Shared>,
    }

    /// The same methods, blocking, for code without an event loop. Calls
    /// release the GIL while Rust works. Its token source must be sync.
    #[pyclass(frozen)]
    struct BlockingRunner {
        shared: Arc<Shared>,
    }

    fn future<'py, T>(
        py: Python<'py>,
        shared: &Shared,
        fut: impl Future<Output = PyResult<T>> + Send + 'static,
    ) -> PyResult<Bound<'py, PyAny>>
    where
        T: for<'a> IntoPyObject<'a> + Send + 'static,
    {
        use tracing::instrument::WithSubscriber as _;
        pyo3_async_runtimes::tokio::future_into_py(py, fut.with_subscriber(shared.dispatch.clone()))
    }

    fn block<T: Send>(
        py: Python<'_>,
        shared: &Shared,
        fut: impl Future<Output = PyResult<T>> + Send,
    ) -> PyResult<T> {
        use tracing::instrument::WithSubscriber as _;
        let fut = fut.with_subscriber(shared.dispatch.clone());
        py.detach(|| runtime().block_on(fut))
    }

    #[pymethods]
    impl Runner {
        /// `config`: a `mailtide.config.Config`. `token_source(account)`
        /// returns `(access_token, expires_at_ms)`, sync or async, or raises
        /// `DelegationRevoked`.
        #[new]
        #[pyo3(signature = (config, token_source, *, clock=None))]
        fn new(
            py: Python<'_>,
            config: ConfigArg,
            token_source: Py<PyAny>,
            clock: Option<Py<PyAny>>,
        ) -> PyResult<Self> {
            Ok(Self {
                shared: open(py, config, token_source, clock)?,
            })
        }

        fn migrate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            future(py, &self.shared, self.shared.clone().migrate())
        }

        fn schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            future(py, &self.shared, self.shared.clone().schema())
        }

        fn track<'py>(&self, py: Python<'py>, account: &str) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().track(account_id(account)?),
            )
        }

        #[pyo3(signature = (account, budget_s=60.0, on_progress=None))]
        fn run<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            budget_s: f64,
            on_progress: Option<Py<PyAny>>,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().run(
                    account_id(account)?,
                    seconds(budget_s, "budget_s")?,
                    on_progress,
                ),
            )
        }

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
            future(
                py,
                &self.shared,
                self.shared.clone().act(
                    account_id(account)?,
                    request,
                    seconds(budget_s, "budget_s")?,
                ),
            )
        }

        #[pyo3(signature = (account, action, *, message_ids=None, thread_id=None, add_labels=Vec::new(), remove_labels=Vec::new(), destination="inbox"))]
        #[allow(clippy::too_many_arguments)]
        fn admit<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            action: &str,
            message_ids: Option<Vec<String>>,
            thread_id: Option<String>,
            add_labels: Vec<String>,
            remove_labels: Vec<String>,
            destination: &str,
        ) -> PyResult<Bound<'py, PyAny>> {
            let request = action_request(
                action,
                message_ids,
                thread_id,
                add_labels,
                remove_labels,
                destination,
            )?;
            future(
                py,
                &self.shared,
                self.shared.clone().admit(account_id(account)?, request),
            )
        }

        fn status<'py>(&self, py: Python<'py>, account: &str) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().status(account_id(account)?),
            )
        }

        fn accounts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            future(py, &self.shared, self.shared.clone().accounts())
        }

        #[pyo3(signature = (account, message_ids, budget_s=120.0))]
        fn drift<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            message_ids: Vec<String>,
            budget_s: f64,
        ) -> PyResult<Bound<'py, PyAny>> {
            let ids = message_ids.into_iter().map(MessageId).collect();
            let budget = seconds(budget_s, "budget_s")?;
            future(
                py,
                &self.shared,
                self.shared.clone().drift(account_id(account)?, ids, budget),
            )
        }

        #[pyo3(signature = (account, history_id, budget_s=60.0))]
        fn notify<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            history_id: u64,
            budget_s: f64,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().notify(
                    account_id(account)?,
                    history_id,
                    seconds(budget_s, "budget_s")?,
                ),
            )
        }

        fn resume<'py>(&self, py: Python<'py>, account: &str) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().resume(account_id(account)?),
            )
        }

        fn forget<'py>(&self, py: Python<'py>, account: &str) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().forget(account_id(account)?),
            )
        }

        fn health<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            future(py, &self.shared, self.shared.clone().health())
        }

        fn labels<'py>(&self, py: Python<'py>, account: &str) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().labels(account_id(account)?),
            )
        }

        fn messages<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            message_ids: Vec<String>,
        ) -> PyResult<Bound<'py, PyAny>> {
            let ids = message_ids.into_iter().map(MessageId).collect();
            future(
                py,
                &self.shared,
                self.shared.clone().messages(account_id(account)?, ids),
            )
        }

        #[pyo3(signature = (account, since=None, limit=500))]
        fn changes<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            since: Option<&str>,
            limit: u32,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .changes(account_id(account)?, parse_since(since)?, limit),
            )
        }

        fn compact<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            through: &str,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .compact(account_id(account)?, parse_state(through)?),
            )
        }

        #[pyo3(signature = (account, after=None, limit=1000))]
        fn message_ids<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            after: Option<String>,
            limit: u32,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .message_ids(account_id(account)?, after.map(MessageId), limit),
            )
        }

        fn read_blob<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            message_id: &str,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .read_blob(account_id(account)?, MessageId(message_id.to_owned())),
            )
        }
    }

    #[pymethods]
    impl BlockingRunner {
        /// See `mailtide.Runner`.
        #[new]
        #[pyo3(signature = (config, token_source, *, clock=None))]
        fn new(
            py: Python<'_>,
            config: ConfigArg,
            token_source: Py<PyAny>,
            clock: Option<Py<PyAny>>,
        ) -> PyResult<Self> {
            Ok(Self {
                shared: open(py, config, token_source, clock)?,
            })
        }

        /// Create or upgrade mailtide's tables (safe to call concurrently).
        fn migrate(&self, py: Python<'_>) -> PyResult<()> {
            block(py, &self.shared, self.shared.clone().migrate())
        }

        /// Where the database's schema stands against this build.
        fn schema(&self, py: Python<'_>) -> PyResult<SchemaStatus> {
            block(py, &self.shared, self.shared.clone().schema())
        }

        /// Start mirroring `account`; see `mailtide.Runner.track`.
        fn track(&self, py: Python<'_>, account: &str) -> PyResult<TrackResult> {
            block(
                py,
                &self.shared,
                self.shared.clone().track(account_id(account)?),
            )
        }

        /// Sync `account` until caught up, blocked or `budget_s` elapses.
        /// `on_progress(Progress)` is called between commands at most once a
        /// second, from a mailtide thread.
        #[pyo3(signature = (account, budget_s=60.0, on_progress=None))]
        fn run(
            &self,
            py: Python<'_>,
            account: &str,
            budget_s: f64,
            on_progress: Option<Py<PyAny>>,
        ) -> PyResult<RunOutcome> {
            block(
                py,
                &self.shared,
                self.shared.clone().run(
                    account_id(account)?,
                    seconds(budget_s, "budget_s")?,
                    on_progress,
                ),
            )
        }

        /// Admit an action and deliver it; see `mailtide.Runner.act`.
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
            block(
                py,
                &self.shared,
                self.shared.clone().act(
                    account_id(account)?,
                    request,
                    seconds(budget_s, "budget_s")?,
                ),
            )
        }

        /// Admit an action without delivering it; see `mailtide.Runner.admit`.
        #[pyo3(signature = (account, action, *, message_ids=None, thread_id=None, add_labels=Vec::new(), remove_labels=Vec::new(), destination="inbox"))]
        #[allow(clippy::too_many_arguments)]
        fn admit(
            &self,
            py: Python<'_>,
            account: &str,
            action: &str,
            message_ids: Option<Vec<String>>,
            thread_id: Option<String>,
            add_labels: Vec<String>,
            remove_labels: Vec<String>,
            destination: &str,
        ) -> PyResult<u32> {
            let request = action_request(
                action,
                message_ids,
                thread_id,
                add_labels,
                remove_labels,
                destination,
            )?;
            block(
                py,
                &self.shared,
                self.shared.clone().admit(account_id(account)?, request),
            )
        }

        /// What the store holds about `account`, without claiming it.
        fn status(&self, py: Python<'_>, account: &str) -> PyResult<Option<AccountStatus>> {
            block(
                py,
                &self.shared,
                self.shared.clone().status(account_id(account)?),
            )
        }

        /// Every account the store holds.
        fn accounts(&self, py: Python<'_>) -> PyResult<Vec<String>> {
            block(py, &self.shared, self.shared.clone().accounts())
        }

        /// Compare the mirror with Gmail for `message_ids`; see
        /// `mailtide.Runner.drift`.
        #[pyo3(signature = (account, message_ids, budget_s=120.0))]
        fn drift(
            &self,
            py: Python<'_>,
            account: &str,
            message_ids: Vec<String>,
            budget_s: f64,
        ) -> PyResult<Vec<DriftResult>> {
            let ids = message_ids.into_iter().map(MessageId).collect();
            let budget = seconds(budget_s, "budget_s")?;
            block(
                py,
                &self.shared,
                self.shared.clone().drift(account_id(account)?, ids, budget),
            )
        }

        /// A verified push reached `history_id`; see `mailtide.Runner.notify`.
        #[pyo3(signature = (account, history_id, budget_s=60.0))]
        fn notify(
            &self,
            py: Python<'_>,
            account: &str,
            history_id: u64,
            budget_s: f64,
        ) -> PyResult<NotifyResult> {
            block(
                py,
                &self.shared,
                self.shared.clone().notify(
                    account_id(account)?,
                    history_id,
                    seconds(budget_s, "budget_s")?,
                ),
            )
        }

        /// Resume a blocked account once its cause is fixed.
        fn resume(&self, py: Python<'_>, account: &str) -> PyResult<ResumeResult> {
            block(
                py,
                &self.shared,
                self.shared.clone().resume(account_id(account)?),
            )
        }

        /// Delete everything stored for `account` (rows and raw content).
        fn forget(&self, py: Python<'_>, account: &str) -> PyResult<ForgetResult> {
            block(
                py,
                &self.shared,
                self.shared.clone().forget(account_id(account)?),
            )
        }

        /// Whether the database answers.
        fn health(&self, py: Python<'_>) -> PyResult<Health> {
            block(py, &self.shared, self.shared.clone().health())
        }

        /// The account's label definitions as last synced, in id order.
        fn labels(&self, py: Python<'_>, account: &str) -> PyResult<Vec<Label>> {
            block(
                py,
                &self.shared,
                self.shared.clone().labels(account_id(account)?),
            )
        }

        /// How these messages should look; unknown ids are left out.
        fn messages(
            &self,
            py: Python<'_>,
            account: &str,
            message_ids: Vec<String>,
        ) -> PyResult<Vec<MessageView>> {
            let ids = message_ids.into_iter().map(MessageId).collect();
            block(
                py,
                &self.shared,
                self.shared.clone().messages(account_id(account)?, ids),
            )
        }

        /// Mirror changes for `account` after state string `since`.
        #[pyo3(signature = (account, since=None, limit=500))]
        fn changes(
            &self,
            py: Python<'_>,
            account: &str,
            since: Option<&str>,
            limit: u32,
        ) -> PyResult<Py<PyAny>> {
            block(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .changes(account_id(account)?, parse_since(since)?, limit),
            )
        }

        /// Drop feed entries at or before state string `through`.
        fn compact(&self, py: Python<'_>, account: &str, through: &str) -> PyResult<u64> {
            let fut = self
                .shared
                .clone()
                .compact(account_id(account)?, parse_state(through)?);
            block(py, &self.shared, fut)
        }

        /// Up to `limit` message ids after `after`, in id order.
        #[pyo3(signature = (account, after=None, limit=1000))]
        fn message_ids(
            &self,
            py: Python<'_>,
            account: &str,
            after: Option<String>,
            limit: u32,
        ) -> PyResult<Vec<String>> {
            let fut =
                self.shared
                    .clone()
                    .message_ids(account_id(account)?, after.map(MessageId), limit);
            block(py, &self.shared, fut)
        }

        /// The raw RFC 822 bytes of an imported message.
        fn read_blob(
            &self,
            py: Python<'_>,
            account: &str,
            message_id: &str,
        ) -> PyResult<Py<PyBytes>> {
            block(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .read_blob(account_id(account)?, MessageId(message_id.to_owned())),
            )
        }
    }

    // ------------------------------------------------------------ functions

    /// One schema migration as SQL (see `migrations`).
    #[pyclass(frozen, get_all)]
    struct Migration {
        version: u32,
        statements: Vec<String>,
    }

    /// mailtide's schema migrations as SQL, in order, for hosts that run
    /// migrations with their own tool (e.g. Alembic): execute each
    /// migration's statements in order, skipping versions already in
    /// `mailtide_schema`. Each records itself there. Postgres and SQLite
    /// share it. `Runner.migrate()` does the same natively.
    #[pyfunction]
    fn migrations() -> Vec<Migration> {
        mailtide_store::sql::migrations()
            .into_iter()
            .map(|m| Migration {
                version: m.version,
                statements: m.statements,
            })
            .collect()
    }

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
