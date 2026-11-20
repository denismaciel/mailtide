//! A fake Gmail with label definitions, and a runner around it, shared by
//! the label-editing tests. Each test file declares it
//! `pub mod label_support;`, so the helpers a file does not use are not
//! dead code there.
//!
//! The fake serves what a sync needs (profile, labels, history, listings,
//! minimal and raw gets, modify) and the labels API as live Gmail answered
//! it (`features/label-edit.toml`): a name another label holds, in any case
//! or with surrounding space, is a 409; a patch sends only what changes and
//! `color: null` clears; a delete removes the label from its messages with
//! one `labelsRemoved` record each (or, for a quiet deletion, none); a
//! modify adding a label the mailbox lacks is a 400 for the whole request.
//! Scripted faults answer a matching request instead.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::gmail::Label;
use mailtide::ids::{AccountId, MessageId, Timestamp};
use mailtide::mirror::MessageView;
use mailtide::quota::{Admission, Budget, Lease, Priority, QuotaUnits};
use mailtide::retry::Cooldown;
use mailtide::revision::{Change, ChangeFeed, FeedCursor, StateSeq};
use mailtide_gmail::http::{HttpRequest, HttpResponse, Method};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::ObjectBlobs;
use mailtide_io::quota_gate::{GateError, LocalGate, QuotaGate};
use mailtide_io::runner::{
    Access, LogAccounts, RunOutcome, Runner, RunnerConfig, StopReason, WatchMode,
};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::Store as _;
use mailtide_store::memory::MemoryStore;
use serde_json::{Value, json};

/// The mirrored account.
#[must_use]
pub fn account() -> AccountId {
    AccountId("alice@example.com".into())
}

/// A label definition as Gmail keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Definition {
    /// Its name.
    pub name: String,
    /// `system` or `user`.
    pub kind: &'static str,
    /// `labelListVisibility`, if set.
    pub list: Option<String>,
    /// `messageListVisibility`, if set.
    pub message: Option<String>,
    /// `textColor` and `backgroundColor`, if set.
    pub color: Option<(String, String)>,
}

impl Definition {
    fn json(&self, id: &str) -> Value {
        let mut label = json!({"id": id, "name": self.name, "type": self.kind});
        if let Some(list) = &self.list {
            label["labelListVisibility"] = json!(list);
        }
        if let Some(message) = &self.message {
            label["messageListVisibility"] = json!(message);
        }
        if let Some((text, background)) = &self.color {
            label["color"] = json!({"textColor": text, "backgroundColor": background});
        }
        label
    }
}

/// What a scripted fault does to the request it matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    /// Answer with this status and Gmail error reason, changing nothing.
    Status(u16, &'static str),
    /// Apply the request, then lose the answer (a timeout).
    LostAfterApply,
    /// Answer 200 with a body that is not JSON, changing nothing.
    Undecodable,
    /// Answer 200 with this body, changing nothing.
    Body(&'static str),
}

/// The fake mailbox.
#[derive(Debug, Default)]
pub struct Mailbox {
    /// Labels, thread and history id by message id.
    pub messages: BTreeMap<String, (Vec<String>, u64)>,
    history: Vec<Value>,
    history_id: u64,
    /// Label definitions by id.
    pub labels: BTreeMap<String, Definition>,
    next_label: u64,
    /// Faults for the next requests that match `METHOD path-suffix`.
    faults: VecDeque<(String, Fault)>,
    /// Every request, as `METHOD path` (after `/users/me/`).
    pub requests: Vec<String>,
    /// Ids whose delete answers 404 although they are listed (another
    /// client deleted them between the listing and the delete).
    pub gone_on_delete: Vec<String>,
}

impl Mailbox {
    fn next_history(&mut self) -> u64 {
        self.history_id += 1;
        self.history_id
    }

    /// A message arrives with `labels`, in a thread of its own.
    pub fn add(&mut self, id: &str, labels: &[&str]) {
        let at = self.next_history();
        let labels: Vec<String> = labels.iter().map(|l| (*l).to_owned()).collect();
        self.history.push(json!({
            "id": at.to_string(),
            "messagesAdded": [{"message": {"id": id, "threadId": format!("t-{id}"), "labelIds": labels}}],
        }));
        self.messages.insert(id.to_owned(), (labels, at));
    }

    /// Another client changes a message's labels (with history records).
    pub fn modify(&mut self, id: &str, add: &[&str], remove: &[&str]) {
        let Some((labels, _)) = self.messages.get(id).cloned() else {
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
        let at = self.next_history();
        let reference = json!({"id": id, "threadId": format!("t-{id}")});
        self.history.push(json!({
            "id": at.to_string(),
            "labelsAdded": [{"message": reference, "labelIds": added}],
            "labelsRemoved": [{"message": reference, "labelIds": removed}],
        }));
        let mut labels: Vec<String> = labels
            .into_iter()
            .filter(|l| !removed.contains(l))
            .collect();
        labels.extend(added);
        self.messages.insert(id.to_owned(), (labels, at));
    }

    /// Define a system label.
    pub fn system_label(&mut self, id: &str) {
        self.labels.insert(
            id.to_owned(),
            Definition {
                name: id.to_owned(),
                kind: "system",
                list: None,
                message: None,
                color: None,
            },
        );
    }

    /// Another client creates a user label; returns its id.
    pub fn create_label(&mut self, name: &str) -> String {
        self.next_label += 1;
        let id = format!("Label_{}", self.next_label);
        self.labels.insert(
            id.clone(),
            Definition {
                name: name.to_owned(),
                kind: "user",
                list: Some("labelShow".into()),
                message: Some("show".into()),
                color: None,
            },
        );
        id
    }

    /// Another client renames a label (no history record, as in Gmail).
    pub fn rename_label(&mut self, id: &str, name: &str) {
        if let Some(label) = self.labels.get_mut(id) {
            name.clone_into(&mut label.name);
        }
    }

    /// A label is deleted: it leaves its messages, with one `labelsRemoved`
    /// record each when `recorded` (as Gmail does), or without any.
    pub fn delete_label(&mut self, id: &str, recorded: bool) {
        self.labels.remove(id);
        let carrying: Vec<String> = self
            .messages
            .iter()
            .filter(|(_, (labels, _))| labels.iter().any(|l| l == id))
            .map(|(message, _)| message.clone())
            .collect();
        for message in carrying {
            if recorded {
                self.modify(&message, &[], &[id]);
            } else if let Some((labels, _)) = self.messages.get_mut(&message) {
                labels.retain(|l| l != id);
            }
        }
    }

    /// Answer the next request matching `request` (`METHOD path-suffix`,
    /// e.g. `PATCH labels/Label_2`) with `fault`.
    pub fn fail_next(&mut self, request: &str, fault: Fault) {
        self.faults.push_back((request.to_owned(), fault));
    }

    /// The requests that change something (not GETs), in order.
    #[must_use]
    pub fn writes(&self) -> Vec<String> {
        self.requests
            .iter()
            .filter(|r| !r.starts_with("GET "))
            .cloned()
            .collect()
    }

    fn holder(&self, name: &str, except: Option<&str>) -> Option<&String> {
        let wanted = name.trim().to_lowercase();
        self.labels
            .iter()
            .find(|(id, label)| {
                Some(id.as_str()) != except && label.name.trim().to_lowercase() == wanted
            })
            .map(|(id, _)| id)
    }

    fn defines(&self, id: &str) -> bool {
        self.labels.contains_key(id) || !id.starts_with("Label_")
    }

    fn create(&mut self, body: &Value) -> HttpResponse {
        let name = body["name"].as_str().unwrap_or_default().to_owned();
        if self.holder(&name, None).is_some() {
            return refusal(409, "aborted", "Label name exists or conflicts");
        }
        self.next_label += 1;
        let id = format!("Label_{}", self.next_label);
        let mut label = Definition {
            name,
            kind: "user",
            list: body["labelListVisibility"].as_str().map(str::to_owned),
            message: body["messageListVisibility"].as_str().map(str::to_owned),
            color: None,
        };
        set_color(&mut label, &body["color"]);
        let answer = ok(&label.json(&id));
        self.labels.insert(id, label);
        answer
    }

    fn patch(&mut self, id: &str, body: &Value) -> HttpResponse {
        if !self.labels.contains_key(id) {
            return not_found();
        }
        if let Some(name) = body["name"].as_str()
            && self.holder(name, Some(id)).is_some()
        {
            return refusal(409, "aborted", "Label name exists or conflicts");
        }
        let Some(label) = self.labels.get_mut(id) else {
            return not_found();
        };
        if let Some(name) = body["name"].as_str() {
            name.clone_into(&mut label.name);
        }
        if let Some(list) = body["labelListVisibility"].as_str() {
            label.list = Some(list.to_owned());
        }
        if let Some(message) = body["messageListVisibility"].as_str() {
            label.message = Some(message.to_owned());
        }
        if let Some(color) = body.get("color") {
            set_color(label, color);
        }
        ok(&label.json(id))
    }

    fn delete(&mut self, id: &str) -> HttpResponse {
        if !self.labels.contains_key(id) || self.gone_on_delete.iter().any(|g| g == id) {
            return not_found();
        }
        self.delete_label(id, true);
        HttpResponse {
            status: 204,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    fn modify_request(&mut self, id: &str, body: &Value) -> HttpResponse {
        let names = |key: &str| -> Vec<String> {
            body[key]
                .as_array()
                .map(|ids| {
                    ids.iter()
                        .filter_map(|l| l.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        let (add, remove) = (names("addLabelIds"), names("removeLabelIds"));
        if !self.messages.contains_key(id) {
            return not_found();
        }
        // The whole request is refused for one unknown add.
        if add.iter().any(|label| !self.defines(label)) {
            return refusal(400, "invalidArgument", "labelId not found");
        }
        let add: Vec<&str> = add.iter().map(String::as_str).collect();
        let remove: Vec<&str> = remove.iter().map(String::as_str).collect();
        self.modify(id, &add, &remove);
        ok(&json!({"id": id}))
    }

    fn answer(&mut self, request: &HttpRequest) -> HttpResponse {
        let query: BTreeMap<&str, &str> = request
            .query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let path = request.path.trim_start_matches("/gmail/v1/users/me/");
        let body: Value = request.body.as_deref().map_or(Value::Null, |b| {
            serde_json::from_slice(b).unwrap_or(Value::Null)
        });
        let parts: Vec<&str> = path.split('/').collect();
        match (request.method, parts.as_slice()) {
            (Method::Get, ["profile"]) => ok(&json!({"historyId": self.history_id.to_string()})),
            // A reconciliation ends with a draft listing: there are none.
            (Method::Get, ["drafts"]) => ok(&json!({"resultSizeEstimate": 0})),
            (Method::Get, ["labels"]) => {
                let labels: Vec<Value> = self
                    .labels
                    .iter()
                    .map(|(id, label)| label.json(id))
                    .collect();
                ok(&json!({ "labels": labels }))
            }
            (Method::Post, ["labels"]) => self.create(&body),
            (Method::Patch, ["labels", id]) => self.patch(&decoded(id), &body),
            (Method::Delete, ["labels", id]) => self.delete(&decoded(id)),
            (Method::Get, ["history"]) => {
                let start: u64 = query["startHistoryId"].parse().unwrap_or(0);
                let records: Vec<&Value> = self
                    .history
                    .iter()
                    .filter(|h| h["id"].as_str().and_then(|s| s.parse::<u64>().ok()) > Some(start))
                    .collect();
                ok(&json!({"history": records, "historyId": self.history_id.to_string()}))
            }
            (Method::Get, ["messages"]) => {
                let ids: Vec<Value> = self
                    .messages
                    .iter()
                    .filter(|(_, (labels, _))| {
                        query
                            .get("labelIds")
                            .is_none_or(|wanted| labels.iter().any(|l| l == wanted))
                    })
                    .map(|(id, _)| json!({"id": id, "threadId": format!("t-{id}")}))
                    .collect();
                ok(&json!({ "messages": ids }))
            }
            (Method::Get, ["messages", id]) => match self.messages.get(*id) {
                None => not_found(),
                Some((labels, history_id)) => {
                    let mut message = json!({
                        "id": id, "threadId": format!("t-{id}"), "labelIds": labels,
                        "historyId": history_id.to_string(), "internalDate": "1700000000000",
                        "snippet": "hello",
                    });
                    if query.get("format") == Some(&"raw") {
                        let raw = format!(
                            "Message-ID: <{id}@example.com>\r\nSubject: {id}\r\n\r\nBody\r\n"
                        );
                        message["raw"] = json!(URL_SAFE_NO_PAD.encode(raw));
                    }
                    ok(&message)
                }
            },
            (Method::Post, ["messages", id, "modify"]) => self.modify_request(id, &body),
            other => panic!("unexpected request {other:?}"),
        }
    }
}

/// A path segment as the request percent-encoded it.
fn decoded(segment: &str) -> String {
    let mut out = Vec::new();
    let bytes = segment.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match (bytes[i], segment.get(i + 1..i + 3)) {
            (b'%', Some(hex)) => {
                out.push(u8::from_str_radix(hex, 16).unwrap_or(b'?'));
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn set_color(label: &mut Definition, color: &Value) {
    label.color = match (
        color["textColor"].as_str(),
        color["backgroundColor"].as_str(),
    ) {
        (Some(text), Some(background)) => Some((text.to_owned(), background.to_owned())),
        _ => None,
    };
}

/// A 200 with `body`.
#[must_use]
pub fn ok(body: &Value) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: body.to_string().into_bytes(),
    }
}

/// Gmail's error answer.
#[must_use]
pub fn refusal(status: u16, reason: &str, message: &str) -> HttpResponse {
    HttpResponse {
        status,
        headers: Vec::new(),
        body:
            json!({"error": {"code": status, "message": message, "errors": [{"reason": reason}]}})
                .to_string()
                .into_bytes(),
    }
}

fn not_found() -> HttpResponse {
    refusal(404, "notFound", "Requested entity was not found.")
}

/// The fake Gmail, as the runner's transport.
#[derive(Debug, Default)]
pub struct FakeGmail(Mutex<Mailbox>);

impl FakeGmail {
    /// The mailbox, to change it as another client or to inspect it.
    pub fn mailbox(&self) -> MutexGuard<'_, Mailbox> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl GmailTransport for FakeGmail {
    async fn send(
        &self,
        request: HttpRequest,
        token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        assert_eq!(token.secret, "test-token");
        let mut mailbox = self.mailbox();
        let line = format!(
            "{} {}",
            request.method.as_str(),
            request.path.trim_start_matches("/gmail/v1/users/me/")
        );
        mailbox.requests.push(line.clone());
        let fault = mailbox
            .faults
            .iter()
            .position(|(matches, _)| line.ends_with(matches.as_str()) || line == *matches)
            .and_then(|at| mailbox.faults.remove(at))
            .map(|(_, fault)| fault);
        match fault {
            Some(Fault::Status(status, reason)) => Ok(refusal(status, reason, "scripted")),
            Some(Fault::LostAfterApply) => {
                let _ = mailbox.answer(&request);
                Err(TransportError::OutcomeUnknown("timed out".into()))
            }
            Some(Fault::Undecodable) => Ok(HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: b"<html>".to_vec(),
            }),
            Some(Fault::Body(body)) => Ok(HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: body.as_bytes().to_vec(),
            }),
            None => Ok(mailbox.answer(&request)),
        }
    }
}

/// What the gate saw, and whether it defers.
#[derive(Debug, Default)]
pub struct GateState {
    /// What was admitted (or deferred), in order.
    pub admitted: Mutex<Vec<(QuotaUnits, Priority)>>,
    /// Defer every admission by this long.
    pub deferring: Mutex<Option<Duration>>,
}

/// The local gate, recording each admission's cost and class, deferring
/// every admission while the state says so.
#[derive(Debug)]
pub struct RecordingGate {
    inner: LocalGate,
    state: Arc<GateState>,
}

impl QuotaGate for RecordingGate {
    async fn admit(
        &self,
        account: &AccountId,
        cost: QuotaUnits,
        priority: Priority,
        now: Timestamp,
    ) -> Result<Admission, GateError> {
        self.state
            .admitted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((cost, priority));
        let deferring = *self
            .state
            .deferring
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(by) = deferring {
            return Ok(Admission::Deferred {
                until: now.plus(by),
            });
        }
        self.inner.admit(account, cost, priority, now).await
    }

    async fn release(&self, lease: Lease, bytes: u64, now: Timestamp) -> Result<(), GateError> {
        self.inner.release(lease, bytes, now).await
    }

    async fn cool_down(&self, cooldown: Cooldown) -> Result<(), GateError> {
        self.inner.cool_down(cooldown).await
    }

    fn abandon(&self, lease: Lease) {
        self.inner.abandon(lease);
    }
}

/// The runner the label tests drive.
pub type LabelRunner = Runner<MemoryStore, ObjectBlobs, FakeGmail, RecordingGate, StaticTokens>;

/// A runner over a fake Gmail, with the clock and the gate the test moves.
pub struct Harness {
    /// The runner.
    pub runner: LabelRunner,
    /// Milliseconds on the runner's clock.
    pub clock: Arc<AtomicI64>,
    /// What the gate saw.
    pub gate: Arc<GateState>,
}

impl std::ops::Deref for Harness {
    type Target = LabelRunner;

    fn deref(&self) -> &LabelRunner {
        &self.runner
    }
}

impl Harness {
    /// Move the clock on by `by`.
    pub fn advance(&self, by: Duration) {
        self.clock.fetch_add(
            i64::try_from(by.as_millis()).unwrap_or(i64::MAX),
            Ordering::SeqCst,
        );
    }

    /// Every admission the gate saw: cost and class.
    #[must_use]
    pub fn admissions(&self) -> Vec<(QuotaUnits, Priority)> {
        self.gate
            .admitted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Defer every admission by `by` from now on (`None`: stop).
    pub fn defer(&self, by: Option<Duration>) {
        *self
            .gate
            .deferring
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = by;
    }
}

/// A runner over a fake Gmail (with `INBOX`, `UNREAD` and `STARRED`
/// defined), with `access`, on a clock the test moves.
#[must_use]
pub fn harness(access: Access) -> Harness {
    let clock = Arc::new(AtomicI64::new(1_700_000_000_000));
    let gate = Arc::new(GateState::default());
    Harness {
        runner: runner(FakeGmail::default(), access, &clock, &gate),
        clock,
        gate,
    }
}

fn runner(
    gmail: FakeGmail,
    access: Access,
    clock: &Arc<AtomicI64>,
    gate: &Arc<GateState>,
) -> LabelRunner {
    with_defaults(&gmail);
    runner_over(
        MemoryStore::tracking(&[account()]),
        gmail,
        access,
        clock,
        gate,
    )
}

/// The system labels every fake mailbox lists.
pub fn with_defaults(gmail: &FakeGmail) {
    let mut mailbox = gmail.mailbox();
    mailbox.system_label("INBOX");
    mailbox.system_label("UNREAD");
    mailbox.system_label("STARRED");
}

/// A runner over `store` and `transport`, on `clock`, through `gate`.
pub fn runner_over<S: mailtide_store::Store, T: GmailTransport>(
    store: S,
    transport: T,
    access: Access,
    clock: &Arc<AtomicI64>,
    gate: &Arc<GateState>,
) -> Runner<S, ObjectBlobs, T, RecordingGate, StaticTokens> {
    let tokens = StaticTokens(BTreeMap::from([(
        account(),
        AccessToken {
            secret: "test-token".into(),
            expires_at: Timestamp(i64::MAX),
        },
    )]));
    let now = Arc::clone(clock);
    Runner::new(
        store,
        ObjectBlobs::memory(),
        transport,
        RecordingGate {
            inner: LocalGate::new(Budget::GMAIL_DEFAULT),
            state: Arc::clone(gate),
        },
        tokens,
        RunnerConfig {
            access,
            engine: EngineConfig {
                draft_settle: Duration::ZERO,
                confirm_timeout: Duration::ZERO,
                ..EngineConfig::default()
            },
            watch: WatchMode::Disabled,
            log_accounts: LogAccounts::default(),
            drafts: mailtide_io::drafts::DraftSettings::default(),
        },
    )
    .with_clock(mailtide_io::Clock::from_fn(move || {
        Timestamp(now.load(Ordering::SeqCst))
    }))
}

/// The budget of a label call.
pub const BUDGET: Duration = Duration::from_secs(10);

/// Run the account until it is idle.
///
/// # Panics
///
/// If the run fails or stops for another reason.
pub async fn sync(runner: &LabelRunner) -> RunOutcome {
    let outcome = runner
        .run(&account(), Duration::from_secs(30))
        .await
        .expect("run");
    assert!(
        matches!(outcome.stop, StopReason::Idle { .. }),
        "{:?}",
        outcome.stop
    );
    outcome
}

/// The mirror's label definitions.
///
/// # Panics
///
/// If the store fails.
pub async fn labels(runner: &LabelRunner) -> Vec<Label> {
    runner.labels(&account()).await.expect("labels")
}

/// The names of the mirror's user labels, by id.
pub async fn user_names(runner: &LabelRunner) -> BTreeMap<String, String> {
    labels(runner)
        .await
        .into_iter()
        .filter(|label| label.id.0.starts_with("Label_"))
        .map(|label| (label.id.0, label.name))
        .collect()
}

/// How the mirror shows message `id`.
///
/// # Panics
///
/// If the store fails or has no such message.
pub async fn view(runner: &LabelRunner, id: &str) -> MessageView {
    runner
        .store()
        .messages(&account(), &[MessageId(id.into())])
        .await
        .expect("messages")
        .pop()
        .expect("a view")
}

/// The feed after `since`, every entry.
///
/// # Panics
///
/// If the store fails or `since` is too old.
pub async fn feed_since(runner: &LabelRunner, since: StateSeq) -> Vec<Change> {
    match runner
        .store()
        .changes(&account(), FeedCursor::from(since), 5_000)
        .await
        .expect("changes")
    {
        ChangeFeed::Page(page) => page.changes.into_iter().map(|e| e.change).collect(),
        ChangeFeed::TooOld { .. } => panic!("too old"),
    }
}

/// The feed's current position.
///
/// # Panics
///
/// If the store fails or the account isn't tracked.
pub async fn head(runner: &LabelRunner) -> StateSeq {
    runner
        .store()
        .status(&account())
        .await
        .expect("status")
        .expect("tracked")
        .state
}
