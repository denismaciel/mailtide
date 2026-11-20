//! A fake Gmail that deletes for good, and a runner around it, shared by
//! the purge tests. Each test file declares it `pub mod purge_host;`, so the
//! helpers a file does not use are not dead code there.
//!
//! The fake models what the purge safeguards rest on: `messages.delete` and
//! `messages.batchDelete` remove messages and write `messagesDeleted`
//! history records (or hold them back, as late evidence); a token without
//! the full scope gets Gmail's 403; another client can restore or trash a
//! message between any two calls; an answer can be lost after Gmail applied
//! the delete; and a single delete of a missing id is a 404.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::commit::{Commit, CursorMove, JobPlan, MessageOp};
use mailtide::gmail::MessageObservation;
use mailtide::ids::{AccountId, CommitId, HistoryId, MessageId, ThreadId, Timestamp};
use mailtide::labels::set;
use mailtide::mirror::AccountMirror;
use mailtide::outbox::ActionRequest;
use mailtide::purge::{Bin, EmptyOutcome, PurgeOutcome, PurgePolicy, PurgeTarget};
use mailtide::quota::{Admission, Budget, Lease, Priority, QuotaUnits};
use mailtide::retry::Cooldown;
use mailtide::revision::Change;
use mailtide::state::SyncState;
use mailtide_gmail::http::{HttpRequest, HttpResponse, Method};
use mailtide_io::auth::{AccessToken, TokenError, TokenSource};
use mailtide_io::blobs::ObjectBlobs;
use mailtide_io::purge::PurgeError;
use mailtide_io::quota_gate::{GateError, LocalGate, QuotaGate};
use mailtide_io::runner::{Access, LogAccounts, RunOutcome, Runner, RunnerConfig, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::Store as _;
use mailtide_store::memory::MemoryStore;
use serde_json::json;
use tokio::sync::Notify;

/// The mirrored account.
#[must_use]
pub fn account() -> AccountId {
    AccountId("alice@example.com".into())
}

/// Message ids from strings.
#[must_use]
pub fn ids(ids: &[&str]) -> Vec<MessageId> {
    ids.iter().map(|id| MessageId((*id).into())).collect()
}

/// A token scope that allows everything but permanent deletion.
pub const MODIFY_SCOPE: &str = "https://www.googleapis.com/auth/gmail.modify";

/// Gmail's error answer.
#[must_use]
pub fn error(status: u16, reason: &str, message: &str) -> HttpResponse {
    HttpResponse {
        status,
        headers: Vec::new(),
        body: json!({"error": {"code": status, "message": message,
            "errors": [{"reason": reason}]}})
        .to_string()
        .into_bytes(),
    }
}

/// Gmail's answer to a delete with a token lacking the full scope, as
/// recorded 2026-11-20 (`purge-scope-refusal`).
#[must_use]
pub fn scope_refusal() -> HttpResponse {
    HttpResponse {
        status: 403,
        headers: Vec::new(),
        body: json!({"error": {
            "code": 403,
            "message": "Request had insufficient authentication scopes.",
            "errors": [{"message": "Insufficient Permission", "domain": "global",
                "reason": "insufficientPermissions"}],
            "status": "PERMISSION_DENIED",
            "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                "reason": "ACCESS_TOKEN_SCOPE_INSUFFICIENT", "domain": "googleapis.com"}],
        }})
        .to_string()
        .into_bytes(),
    }
}

fn ok(body: &serde_json::Value) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: body.to_string().into_bytes(),
    }
}

fn no_content() -> HttpResponse {
    HttpResponse {
        status: 204,
        headers: Vec::new(),
        body: Vec::new(),
    }
}

fn not_found() -> HttpResponse {
    error(404, "notFound", "Requested entity was not found.")
}

/// A change another client makes to the mailbox.
pub type Elsewhere = Box<dyn FnMut(&mut Mailbox) + Send>;

/// Gmail's state, and how it misbehaves.
pub struct Mailbox {
    /// Labels, thread and history id by message id.
    pub messages: BTreeMap<String, (Vec<String>, String, u64)>,
    history: Vec<serde_json::Value>,
    /// The mailbox's history id.
    pub history_id: u64,
    /// History from before this id answers 404.
    pub history_floor: u64,
    /// History records per page.
    pub page_size: usize,
    /// The scope the token holds.
    pub scope: &'static str,
    /// Whether `messagesDeleted` records are held back from history reads
    /// (evidence arriving late) until [`Self::release_evidence`].
    pub late_evidence: bool,
    withheld: Vec<serde_json::Value>,
    /// Whether the answer to a delete call is lost after Gmail applied it.
    pub lose_delete_answers: bool,
    /// What every delete call gets instead of being applied.
    pub refuse_deletes: Option<HttpResponse>,
    /// What every delete call gets after it was applied (a 5xx, or a batch
    /// that deleted its ids and then answered 4xx).
    pub answer_after_applying: Option<HttpResponse>,
    /// How many ids a batch applies before returning its answer.
    pub batch_prefix: Option<usize>,
    /// Ids a single delete refuses as Gmail does a malformed id (400).
    pub malformed: BTreeSet<String>,
    /// Whether history reads fail (500) once a delete has been applied.
    pub history_fails_after_deletes: bool,
    deletes_applied: u32,
    /// How long a `batchDelete` takes.
    pub batch_delay: Duration,
    /// Run after each applied delete call: another client's change.
    pub after_delete: Option<Elsewhere>,
    /// Run once after taking a history response snapshot.
    pub after_history: Option<Elsewhere>,
    /// Every request, as `METHOD path` (a batch as `BATCH id,id,...`).
    pub requests: Vec<String>,
}

impl Default for Mailbox {
    fn default() -> Self {
        Self {
            messages: BTreeMap::new(),
            history: Vec::new(),
            history_id: 0,
            history_floor: 0,
            page_size: 500,
            scope: mailtide_gmail::purge::FULL_SCOPE,
            late_evidence: false,
            withheld: Vec::new(),
            lose_delete_answers: false,
            refuse_deletes: None,
            answer_after_applying: None,
            batch_prefix: None,
            malformed: BTreeSet::new(),
            history_fails_after_deletes: false,
            deletes_applied: 0,
            batch_delay: Duration::ZERO,
            after_delete: None,
            after_history: None,
            requests: Vec::new(),
        }
    }
}

impl Mailbox {
    fn record(&mut self, kind: &str, id: &str, labels: Option<&[String]>) {
        self.history_id += 1;
        let thread = self
            .messages
            .get(id)
            .map_or_else(|| format!("t-{id}"), |m| m.1.clone());
        let mut entry = json!({"message": {"id": id, "threadId": thread}});
        if let Some(labels) = labels {
            if kind == "messagesAdded" {
                entry["message"]["labelIds"] = json!(labels);
            } else {
                entry["labelIds"] = json!(labels);
            }
        }
        let record = json!({"id": self.history_id.to_string(), kind: [entry]});
        if kind == "messagesDeleted" && self.late_evidence {
            self.withheld.push(record);
        } else {
            self.history.push(record);
        }
    }

    /// A new message arrives with `labels`, in thread `t-<id>`.
    pub fn add(&mut self, id: &str, labels: &[&str]) {
        self.add_in(&format!("t-{id}"), id, labels);
    }

    /// A new message arrives with `labels`, in `thread`.
    pub fn add_in(&mut self, thread: &str, id: &str, labels: &[&str]) {
        let labels: Vec<String> = labels.iter().map(|l| (*l).to_owned()).collect();
        self.messages
            .insert(id.to_owned(), (labels.clone(), thread.to_owned(), 0));
        self.record("messagesAdded", id, Some(&labels));
        let at = self.history_id;
        if let Some(message) = self.messages.get_mut(id) {
            message.2 = at;
        }
    }

    /// Another client changes a message's labels (each change its own
    /// record, as Gmail writes them).
    pub fn modify(&mut self, id: &str, add: &[&str], remove: &[&str]) {
        let Some((labels, _, _)) = self.messages.get(id).cloned() else {
            return;
        };
        let added: Vec<String> = add
            .iter()
            .filter(|l| !labels.iter().any(|x| x == *l))
            .map(|l| (*l).to_owned())
            .collect();
        let removed: Vec<String> = remove
            .iter()
            .filter(|l| labels.iter().any(|x| x == *l))
            .map(|l| (*l).to_owned())
            .collect();
        if !added.is_empty() {
            self.record("labelsAdded", id, Some(&added));
        }
        if !removed.is_empty() {
            self.record("labelsRemoved", id, Some(&removed));
        }
        let at = self.history_id;
        if let Some(message) = self.messages.get_mut(id) {
            message.0.retain(|l| !removed.contains(l));
            message.0.extend(added);
            message.2 = at;
        }
    }

    /// Another client restores a message from the trash to the inbox.
    pub fn restore(&mut self, id: &str) {
        self.modify(id, &["INBOX"], &["TRASH"]);
    }

    /// Another client trashes a message.
    pub fn trash(&mut self, id: &str) {
        self.modify(id, &["TRASH"], &["INBOX"]);
    }

    /// Delete a message for good, with its history record.
    pub fn delete(&mut self, id: &str) -> bool {
        if !self.messages.contains_key(id) {
            return false;
        }
        self.record("messagesDeleted", id, None);
        self.messages.remove(id);
        self.deletes_applied += 1;
        true
    }

    /// Write the `messagesDeleted` record of a message already gone.
    pub fn delete_record(&mut self, id: &str) {
        self.record("messagesDeleted", id, None);
    }

    /// The held-back `messagesDeleted` records reach history.
    pub fn release_evidence(&mut self) {
        let withheld = std::mem::take(&mut self.withheld);
        self.history.extend(withheld);
        self.late_evidence = false;
    }

    /// Whether Gmail still has the message.
    #[must_use]
    pub fn has(&self, id: &str) -> bool {
        self.messages.contains_key(id)
    }

    /// The delete requests, in order: `DELETE id` or `BATCH id,id,...`.
    #[must_use]
    pub fn deletes(&self) -> Vec<String> {
        self.requests
            .iter()
            .filter(|r| r.starts_with("DELETE") || r.starts_with("BATCH"))
            .cloned()
            .collect()
    }

    fn history_page(&self, query: &BTreeMap<&str, &str>) -> HttpResponse {
        if self.history_fails_after_deletes && self.deletes_applied > 0 {
            return error(503, "backendError", "Backend Error");
        }
        let start: u64 = query["startHistoryId"].parse().expect("start");
        if start < self.history_floor {
            return not_found();
        }
        let records: Vec<&serde_json::Value> = self
            .history
            .iter()
            .filter(|h| h["id"].as_str().and_then(|s| s.parse::<u64>().ok()) > Some(start))
            .collect();
        let offset: usize = query
            .get("pageToken")
            .map_or(0, |t| t.parse().expect("token"));
        let page: Vec<&&serde_json::Value> =
            records.iter().skip(offset).take(self.page_size).collect();
        let mut body = json!({"history": page, "historyId": self.history_id.to_string()});
        if offset + self.page_size < records.len() {
            body["nextPageToken"] = json!((offset + self.page_size).to_string());
        }
        ok(&body)
    }

    fn get(&self, id: &str, query: &BTreeMap<&str, &str>) -> HttpResponse {
        let Some((labels, thread, history_id)) = self.messages.get(id) else {
            return not_found();
        };
        let mut body = json!({
            "id": id, "threadId": thread, "labelIds": labels,
            "historyId": history_id.to_string(), "internalDate": "1700000000000",
            "sizeEstimate": 40, "snippet": "hello",
        });
        if query.get("format") == Some(&"raw") {
            let raw = format!("Message-ID: <{id}@example.com>\r\nSubject: {id}\r\n\r\nBody\r\n");
            body["raw"] = json!(URL_SAFE_NO_PAD.encode(raw));
        }
        ok(&body)
    }

    /// One delete call, as Gmail answers it.
    fn delete_call(&mut self, ids: &[String], single: bool) -> HttpResponse {
        if self.scope != mailtide_gmail::purge::FULL_SCOPE {
            return scope_refusal();
        }
        if let Some(answer) = &self.refuse_deletes {
            return answer.clone();
        }
        if single {
            let id = &ids[0];
            if self.malformed.contains(id) {
                return error(400, "invalidArgument", "Invalid id value");
            }
            if !self.delete(id) {
                return not_found();
            }
        } else {
            // Ids that exist go; the answer names none
            // (`purge-batch-delete-partial`, assumed).
            for id in ids.iter().take(self.batch_prefix.unwrap_or(ids.len())) {
                self.delete(id);
            }
        }
        if let Some(mut elsewhere) = self.after_delete.take() {
            elsewhere(self);
            self.after_delete = Some(elsewhere);
        }
        self.answer_after_applying
            .clone()
            .unwrap_or_else(no_content)
    }

    fn answer(&mut self, request: &HttpRequest) -> Result<HttpResponse, TransportError> {
        let query: BTreeMap<&str, &str> = request
            .query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let path = request.path.trim_start_matches("/gmail/v1/users/me/");
        let parts: Vec<&str> = path.split('/').collect();
        let response = match (request.method, parts.as_slice()) {
            (Method::Get, ["profile"]) => ok(&json!({"historyId": self.history_id.to_string()})),
            // A reconciliation ends with a draft listing: there are none.
            (Method::Get, ["drafts"]) => ok(&json!({"resultSizeEstimate": 0})),
            (Method::Get, ["labels"]) => ok(&json!({"labels": [
                {"id": "INBOX", "name": "INBOX", "type": "system"},
                {"id": "TRASH", "name": "TRASH", "type": "system"},
                {"id": "SPAM", "name": "SPAM", "type": "system"},
            ]})),
            (Method::Get, ["history"]) => self.history_page(&query),
            (Method::Get, ["messages"]) => {
                let listed: Vec<serde_json::Value> = self
                    .messages
                    .iter()
                    .filter(|(_, (labels, _, _))| {
                        query
                            .get("labelIds")
                            .is_none_or(|l| labels.iter().any(|x| x == l))
                    })
                    .map(|(id, (_, thread, _))| json!({"id": id, "threadId": thread}))
                    .collect();
                ok(&json!({"messages": listed}))
            }
            (Method::Get, ["messages", id]) => self.get(id, &query),
            (Method::Post, ["messages", id, "modify"]) => {
                if self.has(id) {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body.as_deref().unwrap_or(b"{}"))
                            .expect("json");
                    let names = |key: &str| -> Vec<String> {
                        body[key]
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|l| l.as_str().map(str::to_owned))
                                    .collect()
                            })
                            .unwrap_or_default()
                    };
                    let (add, remove) = (names("addLabelIds"), names("removeLabelIds"));
                    let add: Vec<&str> = add.iter().map(String::as_str).collect();
                    let remove: Vec<&str> = remove.iter().map(String::as_str).collect();
                    self.modify(id, &add, &remove);
                    ok(&json!({"id": id}))
                } else {
                    not_found()
                }
            }
            (Method::Delete, ["messages", id]) => {
                let id = (*id).to_owned();
                let answer = self.delete_call(std::slice::from_ref(&id), true);
                if answer.is_success() && self.lose_delete_answers {
                    return Err(TransportError::OutcomeUnknown("timed out".into()));
                }
                answer
            }
            (Method::Post, ["messages", "batchDelete"]) => {
                let body: serde_json::Value =
                    serde_json::from_slice(request.body.as_deref().unwrap_or(b"{}")).expect("json");
                let named: Vec<String> = body["ids"]
                    .as_array()
                    .expect("ids")
                    .iter()
                    .filter_map(|id| id.as_str().map(str::to_owned))
                    .collect();
                let answer = self.delete_call(&named, false);
                if answer.is_success() && self.lose_delete_answers {
                    return Err(TransportError::OutcomeUnknown("timed out".into()));
                }
                answer
            }
            other => panic!("unexpected request {other:?}"),
        };
        Ok(response)
    }
}

/// An action a user admits on this host while a history read is answered.
type LocalAction = (std::sync::Weak<TestRunner>, ActionRequest);

/// A point where a request waits while the test changes remote or local state.
#[derive(Default)]
pub struct Pause {
    /// Signaled when the operation starts waiting.
    pub entered: Notify,
    /// Signal to let the operation continue.
    pub resume: Notify,
}

impl Pause {
    async fn wait(&self) {
        self.entered.notify_one();
        self.resume.notified().await;
    }
}

/// Gmail, served through the transport.
#[derive(Default)]
pub struct FakeGmail {
    mailbox: Mutex<Mailbox>,
    local_action: Mutex<Option<LocalAction>>,
    before_modify: Mutex<Option<Arc<Pause>>>,
    forget_after_delete: Mutex<Option<std::sync::Weak<TestRunner>>>,
}

impl FakeGmail {
    /// The mailbox, locked.
    pub fn mailbox(&self) -> std::sync::MutexGuard<'_, Mailbox> {
        self.mailbox.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl GmailTransport for FakeGmail {
    async fn send(
        &self,
        request: HttpRequest,
        token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        assert_eq!(token.secret, "test-token");
        let batch = request.path.ends_with("/batchDelete");
        if request.path.ends_with("/modify") {
            let pause = self.before_modify.lock().expect("modify hook").take();
            if let Some(pause) = pause {
                pause.wait().await;
            }
        }
        let delay = self.mailbox().batch_delay;
        if batch && !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let line = match (request.method, batch) {
            (_, true) => {
                let body: serde_json::Value =
                    serde_json::from_slice(request.body.as_deref().unwrap_or(b"{}")).expect("json");
                let named: Vec<&str> = body["ids"]
                    .as_array()
                    .expect("ids")
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect();
                format!("BATCH {}", named.join(","))
            }
            (Method::Delete, false) => format!(
                "DELETE {}",
                request
                    .path
                    .trim_start_matches("/gmail/v1/users/me/messages/")
            ),
            (method, false) => format!("{} {}", method.as_str(), request.path),
        };
        let history = request.path.ends_with("/history");
        let answer = {
            let mut mailbox = self.mailbox();
            mailbox.requests.push(line);
            let answer = mailbox.answer(&request);
            if history && let Some(mut elsewhere) = mailbox.after_history.take() {
                elsewhere(&mut mailbox);
            }
            answer
        };
        if history {
            let local = self.local_action.lock().expect("history hook").take();
            if let Some((runner, action)) = local
                && let Some(runner) = runner.upgrade()
            {
                runner
                    .store()
                    .admit(&account(), &action, Timestamp(0))
                    .await
                    .expect("admit");
            }
        }
        if batch || request.method == Method::Delete {
            let forget = self.forget_after_delete.lock().expect("forget hook").take();
            if let Some(runner) = forget.and_then(|weak| weak.upgrade()) {
                runner.forget(&account()).await.expect("forget");
            }
        }
        answer
    }
}

/// A [`LocalGate`] that defers every delete after the first `n` by a set
/// time.
pub struct Gate {
    inner: LocalGate,
    controls: Arc<Mutex<GateControls>>,
}

#[derive(Default)]
struct GateControls {
    deletes_admitted: Option<(u32, Duration)>,
    releases_before_failure: Option<u32>,
    delete_leases: BTreeSet<Lease>,
    pause_admission: Option<Arc<Pause>>,
}

impl QuotaGate for Gate {
    async fn admit(
        &self,
        account: &AccountId,
        cost: QuotaUnits,
        priority: Priority,
        now: Timestamp,
    ) -> Result<Admission, GateError> {
        let is_delete = cost == QuotaUnits(10) || cost == QuotaUnits(50);
        if is_delete {
            let pause = {
                let mut controls = self.controls.lock().expect("gate controls");
                match controls.deletes_admitted.as_mut() {
                    Some((0, by)) => {
                        return Ok(Admission::Deferred {
                            until: now.plus(*by),
                        });
                    }
                    Some((n, _)) => *n -= 1,
                    None => {}
                }
                controls.pause_admission.take()
            };
            if let Some(pause) = pause {
                pause.wait().await;
            }
        }
        let admission = self.inner.admit(account, cost, priority, now).await?;
        if is_delete && let Admission::Granted(lease) = admission {
            self.controls
                .lock()
                .expect("gate controls")
                .delete_leases
                .insert(lease);
        }
        Ok(admission)
    }

    async fn release(&self, lease: Lease, bytes: u64, now: Timestamp) -> Result<(), GateError> {
        self.inner.release(lease, bytes, now).await?;
        let mut controls = self.controls.lock().expect("gate controls");
        if controls.delete_leases.remove(&lease) {
            match controls.releases_before_failure.as_mut() {
                Some(0) => return Err(GateError("lease release failed".into())),
                Some(n) => *n -= 1,
                None => {}
            }
        }
        Ok(())
    }

    async fn cool_down(&self, cooldown: Cooldown) -> Result<(), GateError> {
        self.inner.cool_down(cooldown).await
    }

    async fn cool_down_until(&self, cooldown: Cooldown) -> Result<Timestamp, GateError> {
        self.inner.cool_down_until(cooldown).await
    }
}

type TokenPause = Arc<Mutex<Option<(u32, Arc<Pause>)>>>;

/// A token source with one token that never expires.
pub struct Tokens(TokenPause);

impl TokenSource for Tokens {
    async fn token(&self, _account: &AccountId) -> Result<AccessToken, TokenError> {
        let pause = {
            let mut blocked = self.0.lock().expect("token hook");
            match blocked.as_mut() {
                Some((0, _)) => blocked.take().map(|(_, pause)| pause),
                Some((remaining, _)) => {
                    *remaining -= 1;
                    None
                }
                None => None,
            }
        };
        if let Some(pause) = pause {
            pause.wait().await;
        }
        Ok(AccessToken {
            secret: "test-token".into(),
            expires_at: Timestamp(i64::MAX),
        })
    }

    fn invalidate(&self, _account: &AccountId) {}
}

/// The runner the tests drive.
pub type TestRunner = Runner<MemoryStore, ObjectBlobs, FakeGmail, Gate, Tokens>;

/// How long a call may wait for the gate.
pub const BUDGET: Duration = Duration::from_secs(10);

/// A runner over a memory store tracking [`account`], with a fake Gmail.
pub struct Host {
    /// The runner.
    pub runner: Arc<TestRunner>,
    controls: Arc<Mutex<GateControls>>,
    token_pause: TokenPause,
}

impl Host {
    /// A read-write runner with purge set to `policy`.
    #[must_use]
    pub fn new(policy: PurgePolicy) -> Self {
        Self::with(policy, Access::ReadWrite)
    }

    /// A runner with purge set to `policy` and `access`.
    #[must_use]
    pub fn with(policy: PurgePolicy, access: Access) -> Self {
        Self::configured(Some(policy), access)
    }

    /// A runner constructed without setting a purge policy.
    #[must_use]
    pub fn without_policy() -> Self {
        Self::configured(None, Access::ReadWrite)
    }

    fn configured(policy: Option<PurgePolicy>, access: Access) -> Self {
        let controls = Arc::new(Mutex::new(GateControls::default()));
        let token_pause = Arc::new(Mutex::new(None));
        let runner = Runner::new(
            MemoryStore::tracking(&[account()]),
            ObjectBlobs::memory(),
            FakeGmail::default(),
            Gate {
                inner: LocalGate::new(Budget::GMAIL_DEFAULT),
                controls: Arc::clone(&controls),
            },
            Tokens(token_pause.clone()),
            RunnerConfig {
                access,
                engine: EngineConfig {
                    draft_settle: Duration::ZERO,
                    confirm_timeout: Duration::ZERO,
                    ..EngineConfig::default()
                },
                watch: WatchMode::Disabled,
                log_accounts: LogAccounts::default(),
                writes: mailtide_io::runner::WriteSettings::default(),
            },
        );
        let runner = match policy {
            Some(policy) => runner.with_purge(policy),
            None => runner,
        };
        Self {
            runner: Arc::new(runner),
            controls,
            token_pause,
        }
    }

    /// The fake Gmail's mailbox, locked.
    pub fn gmail(&self) -> std::sync::MutexGuard<'_, Mailbox> {
        self.runner.transport().mailbox()
    }

    /// Whether Gmail still has the message.
    #[must_use]
    pub fn in_gmail(&self, id: &str) -> bool {
        self.gmail().has(id)
    }

    /// Defer every delete after the first `n`.
    pub fn admit_deletes(&self, n: u32) {
        self.defer_deletes_after(n, Duration::from_hours(1));
    }

    /// Defer every delete after the first `n` by `by`.
    pub fn defer_deletes_after(&self, n: u32, by: Duration) {
        self.controls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .deletes_admitted = Some((n, by));
    }

    /// Fail delete lease release after `n` successful releases.
    pub fn fail_delete_release_after(&self, n: u32) {
        self.controls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .releases_before_failure = Some(n);
    }

    /// Pause the first delete quota admission that reaches the gate.
    #[must_use]
    pub fn pause_next_delete_admission(&self) -> Arc<Pause> {
        let pause = Arc::new(Pause::default());
        self.controls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pause_admission = Some(pause.clone());
        pause
    }

    /// Pause token acquisition after `n` immediate acquisitions.
    #[must_use]
    pub fn pause_token_after(&self, n: u32) -> Arc<Pause> {
        let pause = Arc::new(Pause::default());
        *self
            .token_pause
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((n, pause.clone()));
        pause
    }

    /// Keep the next action delivery in flight until the test resumes it.
    #[must_use]
    pub fn pause_next_delivery(&self) -> Arc<Pause> {
        let pause = Arc::new(Pause::default());
        *self
            .runner
            .transport()
            .before_modify
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(pause.clone());
        pause
    }

    /// Forget this account after the next delete and before its confirmation.
    pub fn forget_after_delete(&self) {
        *self
            .runner
            .transport()
            .forget_after_delete
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Arc::downgrade(&self.runner));
    }

    /// The user admits `action` on this host while Gmail answers the next
    /// history read (between a purge's history read and its rows).
    pub fn act_during_history_read(&self, action: ActionRequest) {
        *self
            .runner
            .transport()
            .local_action
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((Arc::downgrade(&self.runner), action));
    }

    /// Mirror Gmail as it is now, as a finished sync would (one commit:
    /// every message observed, the cursor at Gmail's history id, live),
    /// without importing anything. Fast for thousands of messages.
    ///
    /// # Panics
    ///
    /// If the store fails or refuses the commit.
    pub async fn mirror_gmail(&self) {
        let (messages, cursor) = {
            let gmail = self.gmail();
            let messages: Vec<MessageOp> = gmail
                .messages
                .iter()
                .map(|(id, (labels, thread, at))| {
                    MessageOp::Observe(MessageObservation {
                        id: MessageId(id.clone()),
                        thread_id: ThreadId(thread.clone()),
                        labels: set(labels.iter().map(String::as_str)),
                        checkpoint: HistoryId(*at),
                    })
                })
                .collect();
            (messages, HistoryId(gmail.history_id))
        };
        let store = self.runner.store();
        let snapshot = store
            .claim(&account(), mailtide::ids::ENGINE_VERSION)
            .await
            .expect("claim");
        let commit = Commit {
            id: CommitId {
                epoch: snapshot.epoch,
                seq: 0,
            },
            account: account(),
            epoch: snapshot.epoch,
            engine_version: snapshot.engine_version,
            cursor: Some(CursorMove {
                expected: snapshot.cursor,
                to: cursor,
            }),
            claim: None,
            sync: Some(SyncState::Live),
            messages,
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
        };
        let outcome = store.apply(&commit).await.expect("apply");
        assert!(
            matches!(outcome, mailtide::commit::CommitOutcome::Applied { .. }),
            "{outcome:?}"
        );
    }

    /// Sync the account until it is caught up.
    ///
    /// # Panics
    ///
    /// If the run fails.
    pub async fn sync(&self) -> RunOutcome {
        self.runner
            .run(&account(), Duration::from_secs(20))
            .await
            .expect("the run")
    }

    /// Purge `messages` (not `anywhere`).
    pub async fn purge(&self, messages: &[&str]) -> Result<PurgeOutcome, PurgeError> {
        self.purge_target(&PurgeTarget::Messages(ids(messages)), false)
            .await
    }

    /// Purge `target`.
    pub async fn purge_target(
        &self,
        target: &PurgeTarget,
        anywhere: bool,
    ) -> Result<PurgeOutcome, PurgeError> {
        self.runner
            .purge(&account(), target, anywhere, BUDGET)
            .await
    }

    /// Empty `bin` within `budget`.
    pub async fn empty(&self, bin: Bin, budget: Duration) -> Result<EmptyOutcome, PurgeError> {
        self.runner.empty(&account(), bin, budget).await
    }

    /// The account's mirror.
    ///
    /// # Panics
    ///
    /// If the account is not tracked.
    #[must_use]
    pub fn mirror(&self) -> AccountMirror {
        self.runner.store().mirror(&account()).expect("tracked")
    }

    /// Whether the mirror has a row for `id`.
    #[must_use]
    pub fn mirrored(&self, id: &str) -> bool {
        self.mirror().messages.contains_key(&MessageId(id.into()))
    }

    /// The ids of the `MessageDeleted` entries in the feed, in order.
    #[must_use]
    pub fn deleted_in_feed(&self) -> Vec<String> {
        self.mirror()
            .log
            .iter()
            .filter_map(|(_, change)| match change {
                Change::MessageDeleted { id, .. } => Some(id.0.clone()),
                _ => None,
            })
            .collect()
    }
}

/// The ids of a list, as strings.
#[must_use]
pub fn names(ids: &[MessageId]) -> Vec<&str> {
    ids.iter().map(|id| id.0.as_str()).collect()
}
