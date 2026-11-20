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
    use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::{PyBytes, PyDict};

    use mailtide::gmail::PartRef;
    use mailtide::ids::{AccountId, LabelId, MessageId, ThreadId, Timestamp};
    use mailtide::outbox::{
        Action, ActionRequest, Importance, LabelDelta, ReadState, RestoreDestination, StarState,
    };
    use mailtide::quota::Priority;
    use mailtide::retry::{AccountProblem, Blocking, BlockingFailure};
    use mailtide::revision::{
        AckOutcome, ConsumerName, FeedCursor, FeedPosition, MAX_CONSUMERS, StateSeq, StateString,
    };
    use mailtide::search::{HitState, SearchPageToken, SearchQuery};
    use mailtide::state::SyncPhase;
    use mailtide_gmail::push::{self, Jwks, PushExpectations};
    use mailtide_io::auth::CachedTokens;
    use mailtide_io::auth::{AccessToken, TokenError, TokenSource};
    use mailtide_io::blobs::{BlobError, BlobStore, ObjectBlobs, blob_key};
    use mailtide_io::labels::{LabelError, SublabelDeletion, Sublabels};
    use mailtide_io::parts::PartError;
    use mailtide_io::quota_gate::LocalGate;
    use mailtide_io::runner::{self, RunError, RunnerConfig, StopReason};
    use mailtide_io::search::{self as runner_search, SearchError};
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
        QuotaDeferred,
        MailtideError,
        "The quota gate would make this call wait past its budget; nothing was sent. `until_ms`: when the gate would admit it, at the earliest."
    );
    create_exception!(
        _core,
        WriteOutcomeUnknown,
        MailtideError,
        "A write reached Gmail, or may have, and its answer was lost (a timeout, a dropped connection): it may or may not have been applied."
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
        PartNotFound,
        MailtideError,
        "The part reference names no part of that message: unreadable, outside it, or made for other content."
    );
    create_exception!(
        _core,
        PartTooLarge,
        MailtideError,
        "The part is larger than max_bytes; refused before any read. Stream it with open_part."
    );
    create_exception!(
        _core,
        Superseded,
        MailtideError,
        "A newer mailtide version already wrote to this account; this one must not."
    );
    create_exception!(
        _core,
        ComposeError,
        PyValueError,
        "compose_message refused its input: `field`, `index` (the entry of a list field, or None) and `reason` say where and why. The message never holds the offending value."
    );
    create_exception!(
        _core,
        SearchRefused,
        MailtideError,
        "Gmail refused the search for good (a query or filter it rejects, a missing scope, an account problem): the caller's error; the account is not blocked by it."
    );
    create_exception!(
        _core,
        UnknownLabel,
        SearchRefused,
        "A search's label filter names an id Gmail does not have (deleted, or never created: its labels.get answered 404); `label_id` names it. Gmail would answer the search with no hits, so it was not sent."
    );
    create_exception!(
        _core,
        InvalidPageToken,
        MailtideError,
        "The page token was not issued by mailtide for this very search (account, scope, query, label ids, include_spam_trash and limit); nothing was sent."
    );
    create_exception!(
        _core,
        PageTokenRejected,
        InvalidPageToken,
        "Gmail refused a continuation mailtide issued: start again from the first page."
    );
    create_exception!(
        _core,
        GmailUnavailable,
        MailtideError,
        "Gmail failed or limited the call (a 5xx, a timeout, a rate limit): retry later."
    );
    create_exception!(
        _core,
        AccountBlocked,
        MailtideError,
        "The account is blocked until an operator acts (see `status`); nothing was sent."
    );
    create_exception!(
        _core,
        LabelNotFound,
        MailtideError,
        "Gmail has no label with that id."
    );
    create_exception!(
        _core,
        LabelNameTaken,
        MailtideError,
        "Another label holds the name (Gmail compares names case-insensitively): `existing` is that label, or None if a listing could not tell. Nothing was written."
    );
    create_exception!(
        _core,
        InvalidLabel,
        MailtideError,
        "Refused before any request: a blank name, a control character, a colour off Gmail's palette, a system label, or nothing to change."
    );
    create_exception!(
        _core,
        LabelHasSublabels,
        MailtideError,
        "The label has sublabels (`ids`, deepest first) and the delete was asked to refuse then: pass sublabels=\"delete\" to delete them too."
    );
    create_exception!(
        _core,
        LabelRefused,
        MailtideError,
        "Gmail refused the label write for good (another 400, or a 403 such as a scope the token lacks): the caller's error; nothing was recorded and the account is not blocked."
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
        m.add("QuotaDeferred", py.get_type::<QuotaDeferred>())?;
        m.add("WriteOutcomeUnknown", py.get_type::<WriteOutcomeUnknown>())?;
        m.add("Superseded", py.get_type::<Superseded>())?;
        m.add("SchemaOutOfDate", py.get_type::<SchemaOutOfDate>())?;
        m.add("ReadOnlyMode", py.get_type::<ReadOnlyMode>())?;
        m.add("AccountNotTracked", py.get_type::<AccountNotTracked>())?;
        m.add("BlobNotFound", py.get_type::<BlobNotFound>())?;
        m.add("ComposeError", py.get_type::<ComposeError>())?;
        m.add("SearchRefused", py.get_type::<SearchRefused>())?;
        m.add("UnknownLabel", py.get_type::<UnknownLabel>())?;
        m.add("InvalidPageToken", py.get_type::<InvalidPageToken>())?;
        m.add("PageTokenRejected", py.get_type::<PageTokenRejected>())?;
        m.add("GmailUnavailable", py.get_type::<GmailUnavailable>())?;
        m.add("AccountBlocked", py.get_type::<AccountBlocked>())?;
        m.add("PartNotFound", py.get_type::<PartNotFound>())?;
        m.add("PartTooLarge", py.get_type::<PartTooLarge>())?;
        m.add("LabelNotFound", py.get_type::<LabelNotFound>())?;
        m.add("LabelNameTaken", py.get_type::<LabelNameTaken>())?;
        m.add("InvalidLabel", py.get_type::<InvalidLabel>())?;
        m.add("LabelHasSublabels", py.get_type::<LabelHasSublabels>())?;
        m.add("LabelRefused", py.get_type::<LabelRefused>())?;
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
            StoreError::SchemaOutOfDate { applied, expected } if applied < expected => {
                SchemaOutOfDate::new_err(format!(
                    "mailtide schema is at version {applied}, this build needs {expected}: run migrate() (or mailtide.migrations()) first"
                ))
            }
            StoreError::SchemaOutOfDate { applied, expected } => SchemaOutOfDate::new_err(format!(
                "mailtide schema is at version {applied}, newer than this build ({expected}): upgrade mailtide"
            )),
        }
    }

    fn blob_error(error: BlobError) -> PyErr {
        match error {
            BlobError::Unavailable(detail) => BlobUnavailable::new_err(detail),
            BlobError::Misconfigured(detail) => PyValueError::new_err(detail),
            BlobError::NotFound => BlobNotFound::new_err("no stored content for that message"),
            BlobError::OutOfRange { total } => {
                PartNotFound::new_err(format!("outside the stored content ({total} bytes)"))
            }
        }
    }

    fn part_error(error: PartError) -> PyErr {
        match error {
            PartError::BlobNotFound => BlobNotFound::new_err("no stored content for that message"),
            PartError::BlobUnavailable(detail) => BlobUnavailable::new_err(detail),
            PartError::InvalidPart(detail) => PartNotFound::new_err(detail),
            PartError::TooLarge { size, limit } => PartTooLarge::new_err(format!(
                "the part is {size} bytes, over max_bytes={limit}: use open_part"
            )),
        }
    }

    fn part_ref(part: &str) -> PyResult<PartRef> {
        PartRef::parse(part).map_err(|e| PartNotFound::new_err(e.to_string()))
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
            RunError::TooManyIds { given } => PyValueError::new_err(format!(
                "prioritise takes at most {} message ids per call, got {given}",
                mailtide::jobs::PRIORITISE_MAX_IDS
            )),
        }
    }

    /// `error` with `name` set to `value` on the exception instance.
    fn with_attribute<'py>(
        error: PyErr,
        name: &str,
        value: impl IntoPyObject<'py>,
        py: Python<'py>,
    ) -> PyErr {
        match error.value(py).setattr(name, value) {
            Ok(()) => error,
            Err(failed) => failed,
        }
    }

    fn quota_deferred(deferred: runner::QuotaDeferred) -> PyErr {
        Python::attach(|py| {
            let error = QuotaDeferred::new_err(format!(
                "the quota gate would hold this call until {} ms; nothing was sent",
                deferred.until.0
            ));
            with_attribute(error, "until_ms", deferred.until.0, py)
        })
    }

    fn search_error(error: SearchError) -> PyErr {
        match error {
            SearchError::EmptyQuery => PyValueError::new_err(
                "search needs a non-blank query: a label-only listing is the mirror's to answer",
            ),
            SearchError::InvalidPageToken => InvalidPageToken::new_err(
                "this page token was not issued for this search: start from the first page",
            ),
            SearchError::UnknownLabel(label) => Python::attach(|py| {
                let error = UnknownLabel::new_err(format!(
                    "Gmail has no label {:?} (deleted, or never created)",
                    label.0
                ));
                with_attribute(error, "label_id", label.0, py)
            }),
            SearchError::PageTokenRejected => PageTokenRejected::new_err(
                "Gmail refused the page token: start from the first page",
            ),
            SearchError::Refused(refusal) => SearchRefused::new_err(describe(&refusal)),
            SearchError::Deferred(deferred) => quota_deferred(deferred),
            SearchError::Retryable(failure) => GmailUnavailable::new_err(failure.detail),
            SearchError::AccountBlocked(reason) => AccountBlocked::new_err(describe(&reason)),
            SearchError::NotTracked => store_error(StoreError::NotTracked),
            SearchError::Run(error) => run_error(error),
        }
    }

    /// A label write's failure as its exception; `existing` is the label a
    /// `NameTaken` names, as the host reads labels. A write that stopped
    /// half-way raises its cause with `done`, the ids already changed.
    fn label_error(error: LabelError, existing: Option<Label>) -> PyErr {
        let (error, done) = match error {
            LabelError::Partial { done, error } => (*error, Some(done)),
            other => (other, None),
        };
        Python::attach(|py| {
            let raised = match error {
                LabelError::NotFound => LabelNotFound::new_err("Gmail has no label with that id"),
                LabelError::NameTaken { .. } => with_attribute(
                    LabelNameTaken::new_err("another label holds that name"),
                    "existing",
                    existing,
                    py,
                ),
                LabelError::Invalid(detail) => InvalidLabel::new_err(detail),
                LabelError::HasSublabels { ids } => with_attribute(
                    LabelHasSublabels::new_err(
                        "the label has sublabels: pass sublabels=\"delete\" to delete them too",
                    ),
                    "ids",
                    ids.into_iter().map(|id| id.0).collect::<Vec<_>>(),
                    py,
                ),
                // A cause is never partial itself.
                LabelError::Partial { .. } => {
                    MailtideError::new_err("label write stopped half-way")
                }
                LabelError::Deferred(deferred) => quota_deferred(deferred),
                LabelError::ReadOnly => ReadOnlyMode::new_err(
                    "this runner is read-only (config access=\"read_only\"): label writes are refused",
                ),
                LabelError::Refused(refusal) => LabelRefused::new_err(describe(&refusal)),
                LabelError::Retryable(failure) => GmailUnavailable::new_err(failure.detail),
                LabelError::OutcomeUnknown(detail) => WriteOutcomeUnknown::new_err(detail),
                LabelError::Run(error) => run_error(error),
            };
            // Every label write's exception says what it changed: nothing,
            // unless it stopped half-way.
            with_attribute(
                raised,
                "done",
                done.unwrap_or_default()
                    .into_iter()
                    .map(|id| id.0)
                    .collect::<Vec<_>>(),
                py,
            )
        })
    }

    fn list_visibility(value: &str) -> PyResult<mailtide::gmail::LabelListVisibility> {
        use mailtide::gmail::LabelListVisibility;
        match value {
            "show" => Ok(LabelListVisibility::Show),
            "show_if_unread" => Ok(LabelListVisibility::ShowIfUnread),
            "hide" => Ok(LabelListVisibility::Hide),
            other => Err(PyValueError::new_err(format!(
                "list_visibility must be show, show_if_unread or hide, not {other:?}"
            ))),
        }
    }

    fn message_visibility(value: &str) -> PyResult<mailtide::gmail::MessageListVisibility> {
        use mailtide::gmail::MessageListVisibility;
        match value {
            "show" => Ok(MessageListVisibility::Show),
            "hide" => Ok(MessageListVisibility::Hide),
            other => Err(PyValueError::new_err(format!(
                "message_visibility must be show or hide, not {other:?}"
            ))),
        }
    }

    /// Refuse a keyword a label method does not take (its signature is in
    /// the stubs), as Python would.
    fn check_keywords(given: &Bound<'_, PyDict>, method: &str, takes: &[&str]) -> PyResult<()> {
        for key in given.keys() {
            let key: String = key.extract()?;
            if !takes.contains(&key.as_str()) {
                return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                    "{method}() got an unexpected keyword argument {key:?}"
                )));
            }
        }
        Ok(())
    }

    /// Keyword `key`, if given and not None.
    fn keyword<'py, T>(given: &Bound<'py, PyDict>, key: &str) -> PyResult<Option<T>>
    where
        T: for<'a> FromPyObject<'a, 'py>,
    {
        match given.get_item(key)? {
            Some(value) if !value.is_none() => value.extract().map(Some).map_err(Into::into),
            Some(_) | None => Ok(None),
        }
    }

    /// The `budget_s` keyword, 30 s by default.
    fn budget(given: &Bound<'_, PyDict>) -> PyResult<Duration> {
        seconds(keyword(given, "budget_s")?.unwrap_or(30.0), "budget_s")
    }

    fn label_color(color: &LabelColor) -> mailtide::gmail::LabelColor {
        mailtide::gmail::LabelColor {
            text: color.text.clone(),
            background: color.background.clone(),
        }
    }

    /// `create_label`'s keywords: the label to create, and the budget.
    fn new_label(
        name: String,
        given: &Bound<'_, PyDict>,
    ) -> PyResult<(mailtide::gmail::NewLabel, Duration)> {
        check_keywords(
            given,
            "create_label",
            &["list_visibility", "message_visibility", "color", "budget_s"],
        )?;
        let visibility = |key: &str| -> PyResult<String> {
            Ok(keyword::<String>(given, key)?.unwrap_or_else(|| "show".into()))
        };
        let label = mailtide::gmail::NewLabel {
            name,
            list_visibility: list_visibility(&visibility("list_visibility")?)?,
            message_visibility: message_visibility(&visibility("message_visibility")?)?,
            color: match given.get_item("color")? {
                Some(value) if !value.is_none() => {
                    let color: PyRef<'_, LabelColor> = value.extract()?;
                    Some(label_color(&color))
                }
                Some(_) | None => None,
            },
        };
        Ok((label, budget(given)?))
    }

    /// `update_label`'s keywords: the patch, what the sublabels do, and the
    /// budget. `color` is kept unless given: `None` clears it.
    fn label_patch(
        given: &Bound<'_, PyDict>,
    ) -> PyResult<(mailtide::gmail::LabelPatch, Sublabels, Duration)> {
        use mailtide::gmail::{ColorChange, LabelPatch};
        check_keywords(
            given,
            "update_label",
            &[
                "name",
                "list_visibility",
                "message_visibility",
                "color",
                "sublabels",
                "budget_s",
            ],
        )?;
        let color = match given.get_item("color")? {
            None => ColorChange::Keep,
            Some(value) if value.is_none() => ColorChange::Clear,
            Some(value) => match value.extract::<String>() {
                Ok(word) if word == "keep" => ColorChange::Keep,
                Ok(other) => {
                    return Err(PyValueError::new_err(format!(
                        "color must be a LabelColor, None (clear it) or \"keep\", not {other:?}"
                    )));
                }
                Err(_) => {
                    let color: PyRef<'_, LabelColor> = value.extract()?;
                    ColorChange::Set(label_color(&color))
                }
            },
        };
        let patch = LabelPatch {
            name: keyword(given, "name")?,
            list_visibility: keyword::<String>(given, "list_visibility")?
                .map(|v| list_visibility(&v))
                .transpose()?,
            message_visibility: keyword::<String>(given, "message_visibility")?
                .map(|v| message_visibility(&v))
                .transpose()?,
            color,
        };
        let sublabels = match keyword::<String>(given, "sublabels")?.as_deref() {
            None | Some("follow") => Sublabels::Follow,
            Some("leave") => Sublabels::Leave,
            Some(other) => {
                return Err(PyValueError::new_err(format!(
                    "sublabels must be follow or leave, not {other:?}"
                )));
            }
        };
        Ok((patch, sublabels, budget(given)?))
    }

    fn sublabel_deletion(value: &str) -> PyResult<SublabelDeletion> {
        match value {
            "refuse" => Ok(SublabelDeletion::Refuse),
            "delete" => Ok(SublabelDeletion::Delete),
            other => Err(PyValueError::new_err(format!(
                "sublabels must be refuse or delete, not {other:?}"
            ))),
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

    /// Changes to the mirror after a state string: at most the call's
    /// `limit`, whole commits unless one commit alone is larger.
    #[pyclass(frozen, get_all)]
    struct ChangePage {
        /// The changes, oldest first: `MessageDiscovered`, `MessageImported`,
        /// `LabelsChanged`, `OverlayChanged`, `MessageDeleted`,
        /// `LabelDefinitionsChanged` or `FactsRefreshed` (match on the class
        /// or on `kind`).
        changes: Vec<Py<PyAny>>,
        /// The state string to ask from next (opaque; it may point inside a
        /// commit).
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

    /// A named consumer's acknowledged position in an account's feed.
    #[pyclass(frozen, get_all)]
    struct ConsumerCursor {
        name: String,
        /// The position, as `changes()` takes it.
        state: String,
        /// When it last acknowledged, or a compaction last found it caught
        /// up.
        seen_at_ms: i64,
        /// Behind the feed's floor: its next read is `StateTooOld`; rebuild,
        /// then `ack` the position the rebuild started from.
        lost: bool,
    }

    fn consumer_cursor(
        generation: u64,
        floor: StateSeq,
        cursor: &mailtide::revision::ConsumerCursor,
    ) -> ConsumerCursor {
        ConsumerCursor {
            name: cursor.name.as_str().to_owned(),
            state: position(generation, cursor.at),
            seen_at_ms: cursor.seen_at.0,
            lost: cursor.is_lost(floor),
        }
    }

    fn consumer_name(name: &str) -> PyResult<ConsumerName> {
        ConsumerName::parse(name).map_err(|_| {
            PyValueError::new_err(format!(
                "invalid consumer name {name:?}: 1 to 64 letters, digits, '.', '-' or '_'"
            ))
        })
    }

    fn max_idle(seconds: f64) -> PyResult<Duration> {
        Duration::try_from_secs_f64(seconds).map_err(|_| {
            PyValueError::new_err(format!(
                "max_idle_s must be a non-negative number of seconds, got {seconds}"
            ))
        })
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

    /// The message's facts were brought up to date by `refresh_facts`.
    #[pyclass(frozen, get_all)]
    struct FactsRefreshed {
        kind: &'static str,
        state: String,
        index: u32,
        id: String,
        thread_id: String,
        /// What the mirror stores for the message now, facts included;
        /// `None` once the message is deleted.
        content: Option<Py<MessageContent>>,
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
            Change::FactsRefreshed { id, thread_id } => Py::new(
                py,
                FactsRefreshed {
                    kind: "facts_refreshed",
                    state,
                    index,
                    id: id.0.clone(),
                    thread_id: thread_id.0.clone(),
                    content: entry
                        .content
                        .clone()
                        .map(|stored| message_content(py, stored))
                        .transpose()?,
                },
            )?
            .into_any(),
        })
    }

    /// Raw content of an imported message: where it is, its header facts,
    /// and the facts extracted from it (envelope and attachment summary).
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
        /// The extractor version that wrote the facts below; 0 when they
        /// were not extracted (imported before they existed): then the
        /// attachment fields are `None`, never "no attachments".
        facts_version: u32,
        bcc: Vec<String>,
        reply_to: Vec<String>,
        /// The `Date` header as an instant, and the sender's UTC offset.
        sent_at_ms: Option<i64>,
        sent_at_offset_min: Option<i32>,
        /// The first `From` mailbox: decoded display name and address.
        sender_name: Option<String>,
        sender_email: Option<String>,
        list_id: Option<String>,
        list_unsubscribe: Vec<String>,
        one_click_unsubscribe: bool,
        auto_submitted: Option<String>,
        precedence: Option<String>,
        /// The `X-Mailtide-Id` token of a message mailtide wrote.
        tracking_id: Option<String>,
        /// The first 32 listed parts, in document order.
        attachments: Option<Vec<Py<AttachmentInfo>>>,
        /// Attached parts, counted beyond the list.
        attachment_count: Option<u32>,
        /// Inline parts, counted beyond the list.
        inline_count: Option<u32>,
        /// Signature and encryption parts, counted, never listed. Gmail's
        /// `has:attachment` matches when `attachment_count + inline_count +
        /// protocol_count > 0`; `has_attachment` is the narrower paperclip.
        protocol_count: Option<u32>,
        /// Whether to show a paperclip.
        has_attachment: Option<bool>,
    }

    /// One part of a message's attachment summary.
    #[pyclass(frozen, get_all)]
    struct AttachmentInfo {
        /// The part reference: read it with `read_part` or `open_part`.
        part: String,
        /// The decoded filename, as the sender wrote it: untrusted.
        filename: Option<String>,
        content_type: String,
        /// Bytes after transfer decoding: exactly what a read returns.
        size: u64,
        /// Shown inside the message rather than attached.
        inline: bool,
        /// The declared `Content-Disposition`, lowercased.
        disposition: Option<String>,
        content_id: Option<String>,
    }

    /// One leaf part of a stored message (`Runner.parts`).
    #[pyclass(frozen, get_all)]
    struct PartInfo {
        /// Its reference; `None` inside a transfer-encoded attached message.
        part: Option<String>,
        /// `text`, `html`, `attached`, `inline` or `protocol` (a signature
        /// or encryption part).
        role: &'static str,
        content_type: String,
        filename: Option<String>,
        size: u64,
        content_id: Option<String>,
        charset: Option<String>,
        /// The reference of the attached message it is in, if any.
        container: Option<String>,
        /// How many attached messages deep it is (0: the message itself).
        depth: u32,
    }

    impl From<mailtide_mime::PartInfo> for PartInfo {
        fn from(info: mailtide_mime::PartInfo) -> Self {
            use mailtide_mime::PartRole;
            Self {
                part: info.part.map(|part| part.to_string()),
                role: match info.role {
                    PartRole::Text => "text",
                    PartRole::Html => "html",
                    PartRole::Attached => "attached",
                    PartRole::Inline => "inline",
                    PartRole::Protocol => "protocol",
                },
                content_type: info.content_type,
                filename: info.filename,
                size: info.size,
                content_id: info.content_id,
                charset: info.charset,
                container: info.container.map(|part| part.to_string()),
                depth: info.depth,
            }
        }
    }

    /// What one `refresh_facts` call did.
    #[pyclass(frozen, get_all)]
    struct FactsRefresh {
        /// Messages looked at.
        examined: u32,
        /// Messages whose facts were brought up to date.
        refreshed: u32,
        /// Imported messages whose stored content is gone (left as they are).
        unreadable: Vec<String>,
        /// The message id to continue from; `None` when the pass is complete.
        next: Option<String>,
    }

    /// One part's decoded bytes, pulled one chunk at a time from a streamed
    /// ranged read (`Runner.open_part`). An async iterator (`async for`) and a
    /// plain iterator (`for`, blocking), and a context manager of both kinds
    /// that closes it. Closing it, an error, or cancelling a read in flight
    /// drops the read.
    #[pyclass(frozen)]
    struct PartStream {
        shared: Arc<Shared>,
        slot: Arc<StreamSlot>,
        /// The part's decoded size: what the chunks add up to.
        #[pyo3(get)]
        size: u64,
        /// Its reference.
        #[pyo3(get)]
        part: String,
    }

    /// Where a Python `PartStream` keeps its Rust stream. The stream is
    /// taken out for each read, so no lock is held across an await, and
    /// `close` never waits: it marks the slot closed and drops the stream if
    /// it is there; a read in flight drops it when it returns.
    struct StreamSlot {
        stream: std::sync::Mutex<Option<mailtide_io::parts::PartStream>>,
        closed: std::sync::atomic::AtomicBool,
    }

    impl StreamSlot {
        fn new(stream: mailtide_io::parts::PartStream) -> Self {
            Self {
                stream: std::sync::Mutex::new(Some(stream)),
                closed: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn is_closed(&self) -> bool {
            self.closed.load(std::sync::atomic::Ordering::SeqCst)
        }

        /// Mark it closed and drop the stream, if no read holds it.
        fn close(&self) {
            self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
            drop(self.take());
        }

        fn take(&self) -> Option<mailtide_io::parts::PartStream> {
            self.stream.lock().ok().and_then(|mut stream| stream.take())
        }

        /// Give the stream back after a read, unless it was closed meanwhile.
        fn give_back(&self, stream: mailtide_io::parts::PartStream) {
            if self.is_closed() {
                return;
            }
            if let Ok(mut slot) = self.stream.lock() {
                *slot = Some(stream);
            }
        }
    }

    /// A read holding the stream: dropped before it gives the stream back
    /// (its task was cancelled), it closes the slot, so the read is dropped.
    struct Reading {
        slot: Arc<StreamSlot>,
        stream: Option<mailtide_io::parts::PartStream>,
    }

    impl Drop for Reading {
        fn drop(&mut self) {
            if self.stream.take().is_some() {
                self.slot.close();
            }
        }
    }

    /// The next chunk of the stream in `slot`; `None` at its end or once it
    /// is closed. The read is dropped at the end and on an error.
    async fn next_chunk(slot: Arc<StreamSlot>) -> PyResult<Option<Py<PyBytes>>> {
        if slot.is_closed() {
            return Ok(None);
        }
        let Some(stream) = slot.take() else {
            return Err(MailtideError::new_err(
                "another read of this part stream is in progress",
            ));
        };
        let mut reading = Reading {
            slot,
            stream: Some(stream),
        };
        let item = match reading.stream.as_mut() {
            Some(stream) => stream.next_chunk().await,
            None => None,
        };
        match item {
            Some(Ok(chunk)) => {
                if let Some(stream) = reading.stream.take() {
                    reading.slot.give_back(stream);
                }
                Ok(Some(Python::attach(|py| PyBytes::new(py, &chunk).unbind())))
            }
            // Dropping `reading` with its stream closes the slot.
            Some(Err(error)) => Err(part_error(error)),
            None => Ok(None),
        }
    }

    #[pymethods]
    impl PartStream {
        /// Whether the read is over: ended, failed or closed.
        #[getter]
        fn closed(&self) -> bool {
            self.slot.is_closed()
        }

        /// Stop the read and drop it. Never waits, from any thread or event
        /// loop; a read in flight drops it when it returns.
        fn close(&self) {
            self.slot.close();
        }

        fn __aiter__(slf: Py<Self>) -> Py<Self> {
            slf
        }

        fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            let slot = Arc::clone(&self.slot);
            future(py, &self.shared, async move {
                next_chunk(slot)
                    .await?
                    .ok_or_else(|| pyo3::exceptions::PyStopAsyncIteration::new_err(()))
            })
        }

        fn __iter__(slf: Py<Self>) -> Py<Self> {
            slf
        }

        fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
            let slot = Arc::clone(&self.slot);
            block(py, &self.shared, next_chunk(slot))
        }

        fn __enter__(slf: Py<Self>) -> Py<Self> {
            slf
        }

        #[pyo3(signature = (*_exc))]
        fn __exit__(&self, _exc: &Bound<'_, pyo3::types::PyTuple>) -> bool {
            self.close();
            false
        }

        fn __aenter__<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
            let this = slf.clone().unbind();
            future(slf.py(), &slf.get().shared, async move { Ok(this) })
        }

        #[pyo3(signature = (*_exc))]
        fn __aexit__<'py>(
            &self,
            py: Python<'py>,
            _exc: &Bound<'_, pyo3::types::PyTuple>,
        ) -> PyResult<Bound<'py, PyAny>> {
            self.close();
            future(py, &self.shared, async move { Ok(false) })
        }
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
        let extracted = stored.facts.is_some();
        let facts = stored.facts.unwrap_or_default();
        let attachments = if extracted {
            Some(
                facts
                    .parts
                    .iter()
                    .map(|part| {
                        Py::new(
                            py,
                            AttachmentInfo {
                                part: part.part.to_string(),
                                filename: part.filename.clone(),
                                content_type: part.content_type.clone(),
                                size: part.part.size,
                                inline: part.is_inline(),
                                disposition: part.disposition.clone(),
                                content_id: part.content_id.clone(),
                            },
                        )
                    })
                    .collect::<PyResult<Vec<_>>>()?,
            )
        } else {
            None
        };
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
                facts_version: facts.version,
                attachments,
                attachment_count: extracted.then_some(facts.attached),
                inline_count: extracted.then_some(facts.inline),
                protocol_count: extracted.then_some(facts.protocol),
                has_attachment: extracted.then(|| facts.has_attachment()),
                bcc: facts.bcc,
                reply_to: facts.reply_to,
                sent_at_ms: facts.sent_at.map(|sent| sent.at.0),
                sent_at_offset_min: facts.sent_at.map(|sent| sent.offset_minutes),
                sender_name: facts.sender_name,
                sender_email: facts.sender_email,
                list_id: facts.list_id,
                list_unsubscribe: facts.list_unsubscribe,
                one_click_unsubscribe: facts.one_click_unsubscribe,
                auto_submitted: facts.auto_submitted,
                precedence: facts.precedence,
                tracking_id: facts.tracking_id,
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

    /// A thread as the host should show it, folded from its messages.
    #[pyclass(frozen, get_all)]
    struct ThreadView {
        id: String,
        /// Every member, drafts included: imported ones oldest first by
        /// `internal_date_ms` then id, then those awaiting import, by id.
        messages: Vec<Py<MessageView>>,
        /// The union of the non-draft members' labels, pending actions applied
        /// (Gmail's `threads.list` also counts a draft's labels; those are in
        /// `messages`).
        labels: Vec<String>,
        /// Some non-draft member is unread.
        unread: bool,
        /// The newest imported non-draft member: the same object as in
        /// `messages`. `None` until one is imported.
        latest: Option<Py<MessageView>>,
        message_count: u32,
        draft_count: u32,
        /// Non-draft members not imported yet: their place is provisional.
        awaiting_import: u32,
        /// `From`, `To` and `Cc` of the imported non-draft members, as
        /// written, one per address, in order of first appearance.
        participants: Vec<String>,
    }

    fn thread_view(py: Python<'_>, view: mailtide::thread::ThreadView) -> PyResult<ThreadView> {
        let mut latest = None;
        let mut messages = Vec::with_capacity(view.messages.len());
        for member in view.messages {
            let is_latest = view.latest.as_ref() == Some(&member.id);
            let member = Py::new(py, message_view(py, member)?)?;
            if is_latest {
                latest = Some(member.clone_ref(py));
            }
            messages.push(member);
        }
        Ok(ThreadView {
            id: view.id.0,
            messages,
            labels: names(&view.labels),
            unread: view.unread,
            latest,
            message_count: view.message_count,
            draft_count: view.draft_count,
            awaiting_import: view.awaiting_import,
            participants: view.participants,
        })
    }

    /// A label definition: what a label id on a message is called, and how
    /// Gmail shows it.
    #[pyclass(frozen, get_all)]
    #[derive(Clone)]
    struct Label {
        id: String,
        name: String,
        /// `system` (`INBOX`, `UNREAD`, …) or `user`.
        kind: &'static str,
        /// `show`, `show_if_unread`, `hide`, or `None` when Gmail gave none.
        list_visibility: Option<&'static str>,
        /// `show`, `hide`, or `None` when Gmail gave none.
        message_visibility: Option<&'static str>,
        color: Option<LabelColor>,
        /// The nearest label above it by name (`Work` for `Work/2026`).
        parent_id: Option<String>,
    }

    impl Label {
        /// `label`, its parent taken from `parents`.
        fn new(
            label: mailtide::gmail::Label,
            parents: &std::collections::BTreeMap<LabelId, LabelId>,
        ) -> Self {
            use mailtide::gmail::{LabelListVisibility, MessageListVisibility};
            Self {
                parent_id: parents.get(&label.id).map(|parent| parent.0.clone()),
                id: label.id.0,
                name: label.name,
                kind: match label.kind {
                    mailtide::gmail::LabelKind::System => "system",
                    mailtide::gmail::LabelKind::User => "user",
                },
                list_visibility: label.list_visibility.map(|v| match v {
                    LabelListVisibility::Show => "show",
                    LabelListVisibility::ShowIfUnread => "show_if_unread",
                    LabelListVisibility::Hide => "hide",
                }),
                message_visibility: label.message_visibility.map(|v| match v {
                    MessageListVisibility::Show => "show",
                    MessageListVisibility::Hide => "hide",
                }),
                color: label.color.map(|color| LabelColor {
                    text: color.text,
                    background: color.background,
                }),
            }
        }
    }

    /// What `update_label` changed: the label, and the sublabels moved with
    /// it.
    #[pyclass(frozen, get_all)]
    struct LabelUpdate {
        label: Label,
        sublabels: Vec<Label>,
    }

    /// What `delete_label` removed: the labels Gmail deleted (sublabels
    /// first), and those already gone.
    #[pyclass(frozen, get_all)]
    struct LabelDeletion {
        deleted: Vec<String>,
        already_gone: Vec<String>,
    }

    /// A label's colour: Gmail's text and background pair (`#rrggbb`).
    #[pyclass(frozen, get_all, eq)]
    #[derive(Clone, PartialEq, Eq)]
    struct LabelColor {
        text: String,
        background: String,
    }

    #[pymethods]
    impl LabelColor {
        #[new]
        fn new(text: String, background: String) -> Self {
            Self { text, background }
        }

        fn __repr__(&self) -> String {
            format!("LabelColor({:?}, {:?})", self.text, self.background)
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

    /// An attachment of a parsed message, or a part of one to compose.
    #[pyclass(frozen, get_all)]
    struct Attachment {
        filename: Option<String>,
        content_type: String,
        content_id: Option<String>,
        /// Its part reference in the raw message (as the attachment summary
        /// names it); `None` inside a transfer-encoded attached message, and
        /// for a part to compose.
        part: Option<String>,
        data: Py<PyBytes>,
    }

    #[pymethods]
    impl Attachment {
        #[new]
        #[pyo3(signature = (content_type, data, *, filename=None, content_id=None))]
        fn new(
            content_type: String,
            data: Py<PyBytes>,
            filename: Option<String>,
            content_id: Option<String>,
        ) -> Self {
            Self {
                filename,
                content_type,
                content_id,
                part: None,
                data,
            }
        }
    }

    impl Attachment {
        fn to_mime(&self, py: Python<'_>) -> mailtide_mime::Attachment {
            mailtide_mime::Attachment {
                filename: self.filename.clone(),
                content_type: self.content_type.clone(),
                content_id: self.content_id.clone(),
                bytes: self.data.as_bytes(py).to_vec(),
            }
        }
    }

    /// A mailbox: an optional display name and an address.
    #[pyclass(frozen, get_all, eq)]
    #[derive(Clone, PartialEq)]
    struct Address {
        name: Option<String>,
        email: String,
    }

    #[pymethods]
    impl Address {
        #[new]
        #[pyo3(signature = (email, name=None))]
        fn new(email: String, name: Option<String>) -> Self {
            Self { name, email }
        }

        fn __repr__(&self) -> String {
            format!("Address(email={:?}, name={:?})", self.email, self.name)
        }
    }

    impl From<mailtide_mime::Address> for Address {
        fn from(address: mailtide_mime::Address) -> Self {
            Self {
                name: address.name,
                email: address.email,
            }
        }
    }

    impl From<&Address> for mailtide_mime::Address {
        fn from(address: &Address) -> Self {
            Self {
                name: address.name.clone(),
                email: address.email.clone(),
            }
        }
    }

    /// An `Address`, or a bare address string.
    fn address(value: &Bound<'_, PyAny>) -> PyResult<Address> {
        if let Ok(address) = value.cast::<Address>() {
            return Ok(address.get().clone());
        }
        Ok(Address {
            name: None,
            email: value.extract()?,
        })
    }

    /// A list of `Address` or bare address strings (not one string).
    fn addresses(values: &Bound<'_, PyAny>) -> PyResult<Vec<Address>> {
        values
            .extract::<Vec<Bound<'_, PyAny>>>()?
            .iter()
            .map(address)
            .collect()
    }

    /// A message to compose (see `compose_message`). `message_id=None` and
    /// `tracking_id=None` mint one at each compose; `sent_at_ms=None` is the
    /// time of the compose.
    #[pyclass(frozen, get_all)]
    struct OutgoingMessage {
        from_: Address,
        sender: Option<Address>,
        reply_to: Vec<Address>,
        to: Vec<Address>,
        cc: Vec<Address>,
        bcc: Vec<Address>,
        subject: Option<String>,
        sent_at_ms: Option<i64>,
        message_id: Option<String>,
        tracking_id: Option<String>,
        in_reply_to: Option<String>,
        references: Vec<String>,
        headers: Vec<(String, String)>,
        text: Option<String>,
        html: Option<String>,
        inline: Vec<Py<Attachment>>,
        attachments: Vec<Py<Attachment>>,
    }

    #[pymethods]
    impl OutgoingMessage {
        /// Keyword arguments name the fields; addresses may be `Address` or
        /// a bare address string.
        #[new]
        #[pyo3(signature = (from_, **fields))]
        fn new(from_: &Bound<'_, PyAny>, fields: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
            let mut message = Self {
                from_: address(from_)?,
                sender: None,
                reply_to: Vec::new(),
                to: Vec::new(),
                cc: Vec::new(),
                bcc: Vec::new(),
                subject: None,
                sent_at_ms: None,
                message_id: None,
                tracking_id: None,
                in_reply_to: None,
                references: Vec::new(),
                headers: Vec::new(),
                text: None,
                html: None,
                inline: Vec::new(),
                attachments: Vec::new(),
            };
            for (key, value) in fields.into_iter().flatten() {
                let key: String = key.extract()?;
                match key.as_str() {
                    "sender" if value.is_none() => message.sender = None,
                    "sender" => message.sender = Some(address(&value)?),
                    "reply_to" => message.reply_to = addresses(&value)?,
                    "to" => message.to = addresses(&value)?,
                    "cc" => message.cc = addresses(&value)?,
                    "bcc" => message.bcc = addresses(&value)?,
                    "subject" => message.subject = value.extract()?,
                    "sent_at_ms" => message.sent_at_ms = value.extract()?,
                    "message_id" => message.message_id = value.extract()?,
                    "tracking_id" => message.tracking_id = value.extract()?,
                    "in_reply_to" => message.in_reply_to = value.extract()?,
                    "references" => message.references = value.extract()?,
                    "headers" => message.headers = value.extract()?,
                    "text" => message.text = value.extract()?,
                    "html" => message.html = value.extract()?,
                    "inline" => message.inline = value.extract()?,
                    "attachments" => message.attachments = value.extract()?,
                    other => {
                        return Err(PyTypeError::new_err(format!(
                            "OutgoingMessage() got an unexpected keyword argument {other:?}"
                        )));
                    }
                }
            }
            Ok(message)
        }
    }

    /// A composed message: the RFC 5322 bytes Gmail takes as `raw`.
    #[pyclass(frozen, get_all)]
    struct ComposedMessage {
        raw: Py<PyBytes>,
        message_id: Option<String>,
        tracking_id: Option<String>,
        recipients: Vec<String>,
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
        label_refresh_s: f64,
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
                    label_refresh_s: c.engine.label_refresh_s,
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
        Ok(Arc::new(Shared { inner, dispatch }))
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

    /// A message Gmail's search matched, with what the mirror holds for it.
    #[pyclass(frozen, get_all)]
    #[derive(Clone)]
    struct SearchHit {
        message_id: String,
        thread_id: String,
        /// `imported` (`messages()` has all of it), `not_imported` (its
        /// labels, not its content yet) or `unknown` (no row yet: the next
        /// sync brings it).
        state: &'static str,
    }

    /// One page of a message search.
    #[pyclass(frozen, get_all)]
    struct SearchPage {
        /// In Gmail's order; hits the mirror deleted are left out.
        hits: Vec<SearchHit>,
        /// Pass as `page_token` with the same search for the next page;
        /// `None` ends the paging run.
        next_page_token: Option<String>,
        /// Gmail's `resultSizeEstimate`: a hint, not a count.
        estimate: u64,
        /// Hits left out because the mirror deleted them.
        deleted: u32,
        /// The feed state read before Gmail was asked: `changes(account,
        /// since=mirror_state)` returning entries means the page may be
        /// stale.
        mirror_state: String,
        cursor: Option<u64>,
        phase: &'static str,
    }

    impl From<runner_search::SearchPage> for SearchPage {
        fn from(page: runner_search::SearchPage) -> Self {
            Self {
                hits: page
                    .hits
                    .into_iter()
                    .map(|hit| SearchHit {
                        message_id: hit.message_id.0,
                        thread_id: hit.thread_id.0,
                        state: match hit.state {
                            HitState::Imported => "imported",
                            HitState::NotImported => "not_imported",
                            HitState::Unknown => "unknown",
                        },
                    })
                    .collect(),
                next_page_token: page.next_page_token.map(|token| token.0),
                estimate: page.estimate,
                deleted: page.deleted,
                mirror_state: page.mirror_state.encode().0,
                cursor: page.cursor.map(|c| c.0),
                phase: phase_name(page.phase),
            }
        }
    }

    /// A thread Gmail's search matched.
    #[pyclass(frozen, get_all)]
    #[derive(Clone)]
    struct ThreadHit {
        thread_id: String,
        /// Gmail's history id for the thread: past the page's `cursor`, the
        /// mirror has not caught up with it yet.
        history_id: Option<u64>,
    }

    /// One page of a thread search.
    #[pyclass(frozen, get_all)]
    struct ThreadSearchPage {
        hits: Vec<ThreadHit>,
        next_page_token: Option<String>,
        estimate: u64,
        mirror_state: String,
        cursor: Option<u64>,
        phase: &'static str,
    }

    impl From<runner_search::ThreadSearchPage> for ThreadSearchPage {
        fn from(page: runner_search::ThreadSearchPage) -> Self {
            Self {
                hits: page
                    .hits
                    .into_iter()
                    .map(|hit| ThreadHit {
                        thread_id: hit.thread_id.0,
                        history_id: hit.history_id.map(|h| h.0),
                    })
                    .collect(),
                next_page_token: page.next_page_token.map(|token| token.0),
                estimate: page.estimate,
                mirror_state: page.mirror_state.encode().0,
                cursor: page.cursor.map(|c| c.0),
                phase: phase_name(page.phase),
            }
        }
    }

    /// A search's keyword arguments (`label_ids`, `include_spam_trash`,
    /// `limit`, `page_token`, `priority`, `budget_s`), checked: an unknown
    /// keyword is a `TypeError`, as for a Python function.
    fn search_args(
        query: String,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<(SearchQuery, Duration)> {
        let mut search = SearchQuery::new(query);
        let mut budget = Duration::from_secs(10);
        for (key, value) in options.into_iter().flat_map(PyDictMethods::iter) {
            match key.extract::<String>()?.as_str() {
                "label_ids" => {
                    search.label_ids = value
                        .extract::<Vec<String>>()?
                        .into_iter()
                        .map(LabelId)
                        .collect();
                }
                "include_spam_trash" => search.include_spam_trash = value.extract()?,
                "limit" => search.limit = value.extract()?,
                "page_token" => {
                    search.page_token = value.extract::<Option<String>>()?.map(SearchPageToken);
                }
                "priority" => {
                    search.priority = match value.extract::<String>()?.as_str() {
                        "foreground" => Priority::Foreground,
                        "live" => Priority::Live,
                        "backfill" => Priority::Backfill,
                        other => {
                            return Err(PyValueError::new_err(format!(
                                "priority must be foreground, live or backfill, not {other:?}"
                            )));
                        }
                    };
                }
                "budget_s" => budget = seconds(value.extract()?, "budget_s")?,
                other => {
                    return Err(PyTypeError::new_err(format!(
                        "search got an unexpected keyword argument {other:?}"
                    )));
                }
            }
        }
        Ok((search, budget))
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

    /// What `prioritise` did with each distinct id, in request order.
    #[pyclass(frozen, get_all)]
    struct PrioritiseResult {
        /// Fetched ahead of the backlog (raised now, or already).
        queued: Vec<String>,
        /// Already stored: read them now.
        imported: Vec<String>,
        /// The mirror has no row for them (not yet discovered, or deleted).
        unknown: Vec<String>,
        /// Their import is blocked; `resume` is the operator's tool.
        blocked: Vec<String>,
        /// The account already has the most Foreground imports; still
        /// queued in their own class. Ask again as imports land.
        over_limit: Vec<String>,
    }

    impl From<mailtide::jobs::Prioritised> for PrioritiseResult {
        fn from(report: mailtide::jobs::Prioritised) -> Self {
            let strings = |ids: Vec<MessageId>| ids.into_iter().map(|id| id.0).collect();
            Self {
                queued: strings(report.queued),
                imported: strings(report.imported),
                unknown: strings(report.unknown),
                blocked: strings(report.blocked),
                over_limit: strings(report.over_limit),
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

    /// A state string: a position in one generation of an account's feed,
    /// `mt2-<generation>.<seq>` for a whole commit or
    /// `mt2-<generation>.<seq>.<entry>` inside one.
    fn position(generation: u64, at: impl Into<FeedCursor>) -> String {
        FeedPosition {
            generation,
            at: at.into(),
        }
        .encode()
        .0
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
            self.inner
                .admit(&account, &request)
                .await
                .map_err(run_error)
        }

        async fn prioritise(
            self: Arc<Self>,
            account: AccountId,
            ids: Vec<MessageId>,
        ) -> PyResult<PrioritiseResult> {
            self.inner
                .prioritise(&account, &ids)
                .await
                .map(PrioritiseResult::from)
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

        async fn search(
            self: Arc<Self>,
            account: AccountId,
            query: SearchQuery,
            budget: Duration,
        ) -> PyResult<SearchPage> {
            self.inner
                .search(&account, &query, budget)
                .await
                .map(SearchPage::from)
                .map_err(search_error)
        }

        async fn search_threads(
            self: Arc<Self>,
            account: AccountId,
            query: SearchQuery,
            budget: Duration,
        ) -> PyResult<ThreadSearchPage> {
            self.inner
                .search_threads(&account, &query, budget)
                .await
                .map(ThreadSearchPage::from)
                .map_err(search_error)
        }

        async fn labels(self: Arc<Self>, account: AccountId) -> PyResult<Vec<Label>> {
            let labels = self.inner.labels(&account).await.map_err(run_error)?;
            let parents = mailtide::label_names::parents(&labels);
            Ok(labels
                .into_iter()
                .map(|label| Label::new(label, &parents))
                .collect())
        }

        /// `labels` as the host reads them, each with its parent among the
        /// mirror's definitions (and `labels` themselves).
        async fn described(
            &self,
            account: &AccountId,
            labels: Vec<mailtide::gmail::Label>,
        ) -> PyResult<Vec<Label>> {
            let mut known = self.inner.labels(account).await.map_err(run_error)?;
            for label in &labels {
                if known.iter().all(|stored| stored.id != label.id) {
                    known.push(label.clone());
                }
            }
            let parents = mailtide::label_names::parents(&known);
            Ok(labels
                .into_iter()
                .map(|label| Label::new(label, &parents))
                .collect())
        }

        /// A label write's failure, raised; `NameTaken`'s label described.
        async fn label_failure(&self, account: &AccountId, error: LabelError) -> PyErr {
            let cause = match &error {
                LabelError::Partial { error, .. } => error.as_ref(),
                other => other,
            };
            let taken = match cause {
                LabelError::NameTaken { existing } => existing.clone(),
                _ => None,
            };
            let existing = match taken {
                Some(label) => match self.described(account, vec![label]).await {
                    Ok(mut described) => described.pop(),
                    Err(error) => return error,
                },
                None => None,
            };
            label_error(error, existing)
        }

        async fn create_label(
            self: Arc<Self>,
            account: AccountId,
            label: mailtide::gmail::NewLabel,
            budget: Duration,
        ) -> PyResult<Label> {
            match self.inner.create_label(&account, label, budget).await {
                Ok(label) => Ok(self.described(&account, vec![label]).await?.remove(0)),
                Err(error) => Err(self.label_failure(&account, error).await),
            }
        }

        async fn update_label(
            self: Arc<Self>,
            account: AccountId,
            id: LabelId,
            patch: mailtide::gmail::LabelPatch,
            sublabels: Sublabels,
            budget: Duration,
        ) -> PyResult<LabelUpdate> {
            match self
                .inner
                .update_label(&account, &id, patch, sublabels, budget)
                .await
            {
                Ok(update) => {
                    let mut labels = self
                        .described(
                            &account,
                            std::iter::once(update.label)
                                .chain(update.sublabels)
                                .collect(),
                        )
                        .await?;
                    let label = labels.remove(0);
                    Ok(LabelUpdate {
                        label,
                        sublabels: labels,
                    })
                }
                Err(error) => Err(self.label_failure(&account, error).await),
            }
        }

        async fn delete_label(
            self: Arc<Self>,
            account: AccountId,
            id: LabelId,
            sublabels: SublabelDeletion,
            budget: Duration,
        ) -> PyResult<LabelDeletion> {
            match self
                .inner
                .delete_label(&account, &id, sublabels, budget)
                .await
            {
                Ok(deletion) => Ok(LabelDeletion {
                    deleted: deletion.deleted.into_iter().map(|id| id.0).collect(),
                    already_gone: deletion.already_gone.into_iter().map(|id| id.0).collect(),
                }),
                Err(error) => Err(self.label_failure(&account, error).await),
            }
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

        async fn threads(
            self: Arc<Self>,
            account: AccountId,
            ids: Vec<ThreadId>,
        ) -> PyResult<Vec<ThreadView>> {
            let views = self
                .inner
                .threads(&account, &ids)
                .await
                .map_err(run_error)?;
            Python::attach(|py| views.into_iter().map(|v| thread_view(py, v)).collect())
        }

        async fn changes(
            self: Arc<Self>,
            account: AccountId,
            since: Option<FeedPosition>,
            limit: u32,
        ) -> PyResult<Py<PyAny>> {
            let at = since.map_or(FeedCursor::from(StateSeq(0)), |since| since.at);
            let feed = self
                .inner
                .store()
                .changes(&account, at, limit)
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
            through: Option<FeedPosition>,
            max_idle: Duration,
        ) -> PyResult<u64> {
            self.inner
                .compact(&account, through, max_idle)
                .await
                .map_err(run_error)
        }

        async fn ack(
            self: Arc<Self>,
            account: AccountId,
            consumer: ConsumerName,
            at: FeedPosition,
        ) -> PyResult<Py<PyAny>> {
            match self
                .inner
                .ack(&account, &consumer, at)
                .await
                .map_err(run_error)?
            {
                AckOutcome::Acked(cursor) => Python::attach(|py| {
                    // Just acknowledged: the position is at or after the floor.
                    let cursor = consumer_cursor(at.generation, StateSeq(0), &cursor);
                    Ok(Py::new(py, cursor)?.into_any())
                }),
                AckOutcome::TooOld { generation, oldest } => too_old(generation, oldest),
                AckOutcome::TooManyConsumers => Err(PyValueError::new_err(format!(
                    "an account has at most {MAX_CONSUMERS} consumers: drop_consumer one first"
                ))),
            }
        }

        async fn consumers(self: Arc<Self>, account: AccountId) -> PyResult<Vec<ConsumerCursor>> {
            let list = self.inner.consumers(&account).await.map_err(run_error)?;
            Ok(list
                .cursors
                .iter()
                .map(|cursor| consumer_cursor(list.generation, list.floor, cursor))
                .collect())
        }

        async fn drop_consumer(
            self: Arc<Self>,
            account: AccountId,
            consumer: ConsumerName,
        ) -> PyResult<bool> {
            self.inner
                .drop_consumer(&account, &consumer)
                .await
                .map_err(run_error)
        }

        async fn track(self: Arc<Self>, account: AccountId) -> PyResult<TrackResult> {
            use mailtide::status::TrackOutcome;
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

        async fn parts(
            self: Arc<Self>,
            account: AccountId,
            message: MessageId,
        ) -> PyResult<Vec<PartInfo>> {
            let parts = self
                .inner
                .parts(&account, &message)
                .await
                .map_err(part_error)?;
            Ok(parts.into_iter().map(PartInfo::from).collect())
        }

        async fn refresh_facts(
            self: Arc<Self>,
            account: AccountId,
            after: Option<MessageId>,
            limit: u32,
            budget: Duration,
        ) -> PyResult<FactsRefresh> {
            let done = self
                .inner
                .refresh_facts(&account, after.as_ref(), limit, budget)
                .await
                .map_err(run_error)?;
            Ok(FactsRefresh {
                examined: done.examined,
                refreshed: done.refreshed,
                unreadable: done.unreadable.into_iter().map(|id| id.0).collect(),
                next: done.next.map(|id| id.0),
            })
        }

        async fn open_part(
            self: Arc<Self>,
            account: AccountId,
            message: MessageId,
            part: PartRef,
            chunk_size: usize,
        ) -> PyResult<PartStream> {
            let stream = self
                .inner
                .open_part(&account, &message, &part, chunk_size)
                .await
                .map_err(part_error)?;
            Ok(PartStream {
                size: stream.size(),
                part: part.to_string(),
                slot: Arc::new(StreamSlot::new(stream)),
                shared: self,
            })
        }

        async fn read_part(
            self: Arc<Self>,
            account: AccountId,
            message: MessageId,
            part: PartRef,
            max_bytes: u64,
        ) -> PyResult<Py<PyBytes>> {
            let bytes = self
                .inner
                .read_part(&account, &message, &part, max_bytes)
                .await
                .map_err(part_error)?;
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

        fn prioritise<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            message_ids: Vec<String>,
        ) -> PyResult<Bound<'py, PyAny>> {
            let ids = message_ids.into_iter().map(MessageId).collect();
            future(
                py,
                &self.shared,
                self.shared.clone().prioritise(account_id(account)?, ids),
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

        #[pyo3(signature = (account, query, **options))]
        fn search<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            query: String,
            options: Option<&Bound<'py, PyDict>>,
        ) -> PyResult<Bound<'py, PyAny>> {
            let (query, budget) = search_args(query, options)?;
            future(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .search(account_id(account)?, query, budget),
            )
        }

        #[pyo3(signature = (account, query, **options))]
        fn search_threads<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            query: String,
            options: Option<&Bound<'py, PyDict>>,
        ) -> PyResult<Bound<'py, PyAny>> {
            let (query, budget) = search_args(query, options)?;
            future(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .search_threads(account_id(account)?, query, budget),
            )
        }

        fn labels<'py>(&self, py: Python<'py>, account: &str) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().labels(account_id(account)?),
            )
        }

        /// Create a user label in Gmail and record it; see
        /// `mailtide.Runner.create_label`.
        #[pyo3(signature = (account, name, **options))]
        fn create_label<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            name: String,
            options: Option<Bound<'py, PyDict>>,
        ) -> PyResult<Bound<'py, PyAny>> {
            let options = options.unwrap_or_else(|| PyDict::new(py));
            let (label, budget) = new_label(name, &options)?;
            future(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .create_label(account_id(account)?, label, budget),
            )
        }

        /// Change a label in Gmail and record it; see
        /// `mailtide.Runner.update_label`.
        #[pyo3(signature = (account, label_id, **options))]
        fn update_label<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            label_id: String,
            options: Option<Bound<'py, PyDict>>,
        ) -> PyResult<Bound<'py, PyAny>> {
            let options = options.unwrap_or_else(|| PyDict::new(py));
            let (patch, sublabels, budget) = label_patch(&options)?;
            future(
                py,
                &self.shared,
                self.shared.clone().update_label(
                    account_id(account)?,
                    LabelId(label_id),
                    patch,
                    sublabels,
                    budget,
                ),
            )
        }

        /// Delete a label in Gmail and record it; see
        /// `mailtide.Runner.delete_label`.
        #[pyo3(signature = (account, label_id, *, sublabels="refuse", budget_s=30.0))]
        fn delete_label<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            label_id: String,
            sublabels: &str,
            budget_s: f64,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().delete_label(
                    account_id(account)?,
                    LabelId(label_id),
                    sublabel_deletion(sublabels)?,
                    seconds(budget_s, "budget_s")?,
                ),
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

        fn threads<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            thread_ids: Vec<String>,
        ) -> PyResult<Bound<'py, PyAny>> {
            let ids = thread_ids.into_iter().map(ThreadId).collect();
            future(
                py,
                &self.shared,
                self.shared.clone().threads(account_id(account)?, ids),
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

        #[pyo3(signature = (account, through=None, max_idle_s=604_800.0))]
        fn compact<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            through: Option<&str>,
            max_idle_s: f64,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().compact(
                    account_id(account)?,
                    parse_since(through)?,
                    max_idle(max_idle_s)?,
                ),
            )
        }

        fn ack<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            consumer: &str,
            through: &str,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().ack(
                    account_id(account)?,
                    consumer_name(consumer)?,
                    parse_state(through)?,
                ),
            )
        }

        fn consumers<'py>(&self, py: Python<'py>, account: &str) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().consumers(account_id(account)?),
            )
        }

        fn drop_consumer<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            consumer: &str,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .drop_consumer(account_id(account)?, consumer_name(consumer)?),
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

        fn parts<'py>(
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
                    .parts(account_id(account)?, MessageId(message_id.to_owned())),
            )
        }

        #[pyo3(signature = (account, after=None, limit=1_000, budget_s=60.0))]
        fn refresh_facts<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            after: Option<String>,
            limit: u32,
            budget_s: f64,
        ) -> PyResult<Bound<'py, PyAny>> {
            let budget = seconds(budget_s, "budget_s")?;
            future(
                py,
                &self.shared,
                self.shared.clone().refresh_facts(
                    account_id(account)?,
                    after.map(MessageId),
                    limit,
                    budget,
                ),
            )
        }

        #[pyo3(signature = (account, message_id, part, chunk_size=DEFAULT_PART_CHUNK))]
        fn open_part<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            message_id: &str,
            part: &str,
            chunk_size: usize,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().open_part(
                    account_id(account)?,
                    MessageId(message_id.to_owned()),
                    part_ref(part)?,
                    chunk_size,
                ),
            )
        }

        #[pyo3(signature = (account, message_id, part, max_bytes=DEFAULT_MAX_PART_BYTES))]
        fn read_part<'py>(
            &self,
            py: Python<'py>,
            account: &str,
            message_id: &str,
            part: &str,
            max_bytes: u64,
        ) -> PyResult<Bound<'py, PyAny>> {
            future(
                py,
                &self.shared,
                self.shared.clone().read_part(
                    account_id(account)?,
                    MessageId(message_id.to_owned()),
                    part_ref(part)?,
                    max_bytes,
                ),
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

        /// Fetch these messages' bodies ahead of the backlog; see
        /// `mailtide.Runner.prioritise`.
        fn prioritise(
            &self,
            py: Python<'_>,
            account: &str,
            message_ids: Vec<String>,
        ) -> PyResult<PrioritiseResult> {
            let ids = message_ids.into_iter().map(MessageId).collect();
            block(
                py,
                &self.shared,
                self.shared.clone().prioritise(account_id(account)?, ids),
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

        /// One page of Gmail's search, each hit marked against the mirror;
        /// see `mailtide.Runner.search`.
        #[pyo3(signature = (account, query, **options))]
        fn search(
            &self,
            py: Python<'_>,
            account: &str,
            query: String,
            options: Option<&Bound<'_, PyDict>>,
        ) -> PyResult<SearchPage> {
            let (query, budget) = search_args(query, options)?;
            block(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .search(account_id(account)?, query, budget),
            )
        }

        /// One page of Gmail's thread search; see
        /// `mailtide.Runner.search_threads`.
        #[pyo3(signature = (account, query, **options))]
        fn search_threads(
            &self,
            py: Python<'_>,
            account: &str,
            query: String,
            options: Option<&Bound<'_, PyDict>>,
        ) -> PyResult<ThreadSearchPage> {
            let (query, budget) = search_args(query, options)?;
            block(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .search_threads(account_id(account)?, query, budget),
            )
        }

        /// The account's label definitions as last synced, in id order.
        fn labels(&self, py: Python<'_>, account: &str) -> PyResult<Vec<Label>> {
            block(
                py,
                &self.shared,
                self.shared.clone().labels(account_id(account)?),
            )
        }

        /// Create a user label in Gmail and record it; see
        /// `mailtide.Runner.create_label`.
        #[pyo3(signature = (account, name, **options))]
        fn create_label(
            &self,
            py: Python<'_>,
            account: &str,
            name: String,
            options: Option<Bound<'_, PyDict>>,
        ) -> PyResult<Label> {
            let options = options.unwrap_or_else(|| PyDict::new(py));
            let (label, budget) = new_label(name, &options)?;
            block(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .create_label(account_id(account)?, label, budget),
            )
        }

        /// Change a label in Gmail and record it; see
        /// `mailtide.Runner.update_label`.
        #[pyo3(signature = (account, label_id, **options))]
        fn update_label(
            &self,
            py: Python<'_>,
            account: &str,
            label_id: String,
            options: Option<Bound<'_, PyDict>>,
        ) -> PyResult<LabelUpdate> {
            let options = options.unwrap_or_else(|| PyDict::new(py));
            let (patch, sublabels, budget) = label_patch(&options)?;
            block(
                py,
                &self.shared,
                self.shared.clone().update_label(
                    account_id(account)?,
                    LabelId(label_id),
                    patch,
                    sublabels,
                    budget,
                ),
            )
        }

        /// Delete a label in Gmail and record it; see
        /// `mailtide.Runner.delete_label`.
        #[pyo3(signature = (account, label_id, *, sublabels="refuse", budget_s=30.0))]
        fn delete_label(
            &self,
            py: Python<'_>,
            account: &str,
            label_id: String,
            sublabels: &str,
            budget_s: f64,
        ) -> PyResult<LabelDeletion> {
            block(
                py,
                &self.shared,
                self.shared.clone().delete_label(
                    account_id(account)?,
                    LabelId(label_id),
                    sublabel_deletion(sublabels)?,
                    seconds(budget_s, "budget_s")?,
                ),
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

        /// How these threads should look, in the order asked; threads the
        /// mirror has no message in are left out.
        fn threads(
            &self,
            py: Python<'_>,
            account: &str,
            thread_ids: Vec<String>,
        ) -> PyResult<Vec<ThreadView>> {
            let ids = thread_ids.into_iter().map(ThreadId).collect();
            block(
                py,
                &self.shared,
                self.shared.clone().threads(account_id(account)?, ids),
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

        /// Drop whole commits of the feed: no further than state string
        /// `through` (when given; a `through` inside a commit keeps that
        /// commit), nor past the slowest consumer heard from within
        /// `max_idle_s`. With neither a bound nor a consumer, nothing goes.
        #[pyo3(signature = (account, through=None, max_idle_s=604_800.0))]
        fn compact(
            &self,
            py: Python<'_>,
            account: &str,
            through: Option<&str>,
            max_idle_s: f64,
        ) -> PyResult<u64> {
            let fut = self.shared.clone().compact(
                account_id(account)?,
                parse_since(through)?,
                max_idle(max_idle_s)?,
            );
            block(py, &self.shared, fut)
        }

        /// Consumer `consumer` has durably processed the feed up to state
        /// string `through`: hold the log from there. A `ConsumerCursor`, or
        /// `StateTooOld` (nothing stored; rebuild).
        fn ack(
            &self,
            py: Python<'_>,
            account: &str,
            consumer: &str,
            through: &str,
        ) -> PyResult<Py<PyAny>> {
            let fut = self.shared.clone().ack(
                account_id(account)?,
                consumer_name(consumer)?,
                parse_state(through)?,
            );
            block(py, &self.shared, fut)
        }

        /// The account's registered consumers, in name order.
        fn consumers(&self, py: Python<'_>, account: &str) -> PyResult<Vec<ConsumerCursor>> {
            let fut = self.shared.clone().consumers(account_id(account)?);
            block(py, &self.shared, fut)
        }

        /// Forget a consumer: it no longer holds the log. Whether it was
        /// registered.
        fn drop_consumer(&self, py: Python<'_>, account: &str, consumer: &str) -> PyResult<bool> {
            let fut = self
                .shared
                .clone()
                .drop_consumer(account_id(account)?, consumer_name(consumer)?);
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

        /// Every leaf part of an imported message, with its reference and
        /// role (the blob is read and parsed in Rust; metadata only).
        fn parts(
            &self,
            py: Python<'_>,
            account: &str,
            message_id: &str,
        ) -> PyResult<Vec<PartInfo>> {
            block(
                py,
                &self.shared,
                self.shared
                    .clone()
                    .parts(account_id(account)?, MessageId(message_id.to_owned())),
            )
        }

        /// Bring older messages' facts up to date from their stored content,
        /// without a Gmail call; continue from `next` until it is `None`.
        #[pyo3(signature = (account, after=None, limit=1_000, budget_s=60.0))]
        fn refresh_facts(
            &self,
            py: Python<'_>,
            account: &str,
            after: Option<String>,
            limit: u32,
            budget_s: f64,
        ) -> PyResult<FactsRefresh> {
            let budget = seconds(budget_s, "budget_s")?;
            block(
                py,
                &self.shared,
                self.shared.clone().refresh_facts(
                    account_id(account)?,
                    after.map(MessageId),
                    limit,
                    budget,
                ),
            )
        }

        /// Stream one part's decoded bytes in chunks of at most
        /// `chunk_size`; iterate it with `for`, close it with `with`.
        #[pyo3(signature = (account, message_id, part, chunk_size=DEFAULT_PART_CHUNK))]
        fn open_part(
            &self,
            py: Python<'_>,
            account: &str,
            message_id: &str,
            part: &str,
            chunk_size: usize,
        ) -> PyResult<PartStream> {
            block(
                py,
                &self.shared,
                self.shared.clone().open_part(
                    account_id(account)?,
                    MessageId(message_id.to_owned()),
                    part_ref(part)?,
                    chunk_size,
                ),
            )
        }

        /// The decoded bytes of one part, by its reference: one ranged read.
        #[pyo3(signature = (account, message_id, part, max_bytes=DEFAULT_MAX_PART_BYTES))]
        fn read_part(
            &self,
            py: Python<'_>,
            account: &str,
            message_id: &str,
            part: &str,
            max_bytes: u64,
        ) -> PyResult<Py<PyBytes>> {
            block(
                py,
                &self.shared,
                self.shared.clone().read_part(
                    account_id(account)?,
                    MessageId(message_id.to_owned()),
                    part_ref(part)?,
                    max_bytes,
                ),
            )
        }
    }

    /// `read_part` refuses larger parts unless told otherwise: 25 MiB, as
    /// large as a part Gmail lets a sender attach.
    const DEFAULT_MAX_PART_BYTES: u64 = 25 * 1024 * 1024;

    /// `open_part` yields chunks of at most 256 KiB unless told otherwise.
    const DEFAULT_PART_CHUNK: usize = 256 * 1024;

    // ------------------------------------------------------------ functions

    /// `text` as one Gmail search value that matches it literally and cannot
    /// add operators or groups (for `from:`, `subject:` or a bare term).
    /// `ValueError` when no word is left.
    #[pyfunction]
    fn search_literal(text: &str) -> PyResult<String> {
        mailtide_gmail::search::literal(text)
            .ok_or_else(|| PyValueError::new_err("the text holds no word Gmail could match"))
    }

    /// The search term finding a message by the `Message-ID` Gmail stored
    /// (`rfc822msgid:`). Gmail replaces the one a client writes on an API
    /// send or draft save, so it cannot find a message mailtide just sent.
    /// `ValueError` for an id Gmail's syntax cannot carry.
    #[pyfunction]
    fn search_by_message_id(message_id: &str) -> PyResult<String> {
        mailtide_gmail::search::by_message_id(message_id).ok_or_else(|| {
            PyValueError::new_err(format!(
                "Gmail's search cannot carry the Message-ID {message_id:?}"
            ))
        })
    }

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
            .zip(parsed.attachment_parts)
            .map(|(a, part)| {
                Py::new(
                    py,
                    Attachment {
                        filename: a.filename,
                        content_type: a.content_type,
                        content_id: a.content_id,
                        part: part.map(|part| part.to_string()),
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

    /// 128 random bits, for the ids compose writes (the pure crate draws none).
    fn entropy() -> PyResult<[u8; 16]> {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).map_err(|e| error("random source", e))?;
        Ok(bytes)
    }

    fn compose_error(py: Python<'_>, e: &mailtide_mime::ComposeError) -> PyErr {
        let err = ComposeError::new_err(e.to_string());
        let value = err.value(py);
        let set = value
            .setattr("field", e.field)
            .and_then(|()| value.setattr("index", e.index))
            .and_then(|()| value.setattr("reason", e.reason.to_string()));
        match set {
            Ok(()) => err,
            Err(other) => other,
        }
    }

    fn message_id(
        py: Python<'_>,
        value: &str,
        field: &'static str,
        index: Option<usize>,
    ) -> PyResult<mailtide_mime::MessageId> {
        mailtide_mime::MessageId::parse(value).map_err(|reason| {
            compose_error(
                py,
                &mailtide_mime::ComposeError {
                    field,
                    index,
                    reason,
                },
            )
        })
    }

    /// Write `message` as RFC 5322 bytes for a draft or a send.
    ///
    /// Pure but for the defaults: a missing `message_id` is minted on the
    /// From domain, a missing `tracking_id` is minted, and a missing
    /// `sent_at_ms` is now. Gmail replaces the Message-ID and the Date on
    /// every draft save and send; `tracking_id` (the `X-Mailtide-Id` header)
    /// is what identifies the message afterwards. Fix all three to get the
    /// same bytes again. Raises `ComposeError` on invalid input.
    #[pyfunction]
    #[pyo3(signature = (message, *, purpose="send", max_bytes=None))]
    fn compose_message(
        py: Python<'_>,
        message: &Bound<'_, OutgoingMessage>,
        purpose: &str,
        max_bytes: Option<usize>,
    ) -> PyResult<ComposedMessage> {
        let purpose = match purpose {
            "send" => mailtide_mime::Purpose::Send,
            "draft" => mailtide_mime::Purpose::Draft,
            other => return Err(PyValueError::new_err(format!("unknown purpose {other:?}"))),
        };
        let message = message.get();
        let from = mailtide_mime::Address::from(&message.from_);
        let sent_at_ms = message.sent_at_ms.unwrap_or_else(|| mailtide_io::now().0);
        let mut outgoing = mailtide_mime::OutgoingMessage::new(from, sent_at_ms.div_euclid(1000));
        outgoing.message_id = Some(match &message.message_id {
            Some(id) => message_id(py, id, "message_id", None)?,
            None => mailtide_mime::MessageId::from_entropy(entropy()?, &outgoing.from).map_err(
                |reason| {
                    let e = mailtide_mime::ComposeError {
                        field: "from",
                        index: None,
                        reason,
                    };
                    compose_error(py, &e)
                },
            )?,
        });
        outgoing.tracking_id = Some(match &message.tracking_id {
            Some(token) => token.clone(),
            None => mailtide_mime::new_tracking_id(entropy()?),
        });
        outgoing.sender = message.sender.as_ref().map(Into::into);
        outgoing.reply_to = message.reply_to.iter().map(Into::into).collect();
        outgoing.to = message.to.iter().map(Into::into).collect();
        outgoing.cc = message.cc.iter().map(Into::into).collect();
        outgoing.bcc = message.bcc.iter().map(Into::into).collect();
        outgoing.subject.clone_from(&message.subject);
        outgoing.in_reply_to = message
            .in_reply_to
            .as_deref()
            .map(|id| message_id(py, id, "in_reply_to", None))
            .transpose()?;
        outgoing.references = message
            .references
            .iter()
            .enumerate()
            .map(|(index, id)| message_id(py, id, "references", Some(index)))
            .collect::<PyResult<_>>()?;
        outgoing.headers.clone_from(&message.headers);
        outgoing.text.clone_from(&message.text);
        outgoing.html.clone_from(&message.html);
        outgoing.inline = message
            .inline
            .iter()
            .map(|part| part.get().to_mime(py))
            .collect();
        outgoing.attachments = message
            .attachments
            .iter()
            .map(|part| part.get().to_mime(py))
            .collect();
        let options = mailtide_mime::ComposeOptions { purpose, max_bytes };
        let composed = py
            .detach(|| mailtide_mime::compose(&outgoing, &options))
            .map_err(|e| compose_error(py, &e))?;
        Ok(ComposedMessage {
            raw: PyBytes::new(py, &composed.raw).unbind(),
            message_id: composed.message_id,
            tracking_id: composed.tracking_id,
            recipients: composed.recipients,
        })
    }

    /// A new Message-ID, `<32 hex digits@sender's domain>`, bracketed.
    /// Gmail replaces it on send and on draft save; it serves other
    /// transports and keeps the message well formed.
    #[pyfunction]
    fn new_message_id(py: Python<'_>, sender: &str) -> PyResult<String> {
        let sender = mailtide_mime::Address {
            name: None,
            email: sender.to_owned(),
        };
        mailtide_mime::MessageId::from_entropy(entropy()?, &sender)
            .map(|id| id.to_string())
            .map_err(|reason| {
                let e = mailtide_mime::ComposeError {
                    field: "sender",
                    index: None,
                    reason,
                };
                compose_error(py, &e)
            })
    }

    /// A new tracking id: 128 random bits as base64url, 22 characters.
    #[pyfunction]
    fn new_tracking_id() -> PyResult<String> {
        Ok(mailtide_mime::new_tracking_id(entropy()?))
    }

    /// The `X-Mailtide-Id` of raw message bytes, read from the headers only;
    /// None when there is no valid one.
    #[pyfunction]
    fn tracking_id(py: Python<'_>, raw: &[u8]) -> Option<String> {
        py.detach(|| mailtide_mime::tracking_id(raw))
    }

    /// The addresses in a header value as the mirror holds it
    /// (`MessageContent.sender`, an entry of `to` or `cc`): groups flattened,
    /// display names decoded, entries without an address left out.
    #[pyfunction]
    fn parse_addresses(value: &str) -> Vec<Address> {
        mailtide_mime::parse_addresses(value)
            .into_iter()
            .map(Address::from)
            .collect()
    }

    /// An unstructured header value (a `subject`) with its RFC 2047 encoded
    /// words decoded.
    #[pyfunction]
    fn decode_header(value: &str) -> String {
        mailtide_mime::decode_header(value)
    }
}
