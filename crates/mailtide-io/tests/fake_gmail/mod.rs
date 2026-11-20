//! A fake Gmail with drafts, served through the transport, for the draft
//! tests: real request building, decoding, quota gate and store records.
//!
//! It behaves as Gmail was measured to (features/drafts.toml): a save gets a
//! new message id and a Message-ID of Gmail's own, X- headers are kept, an
//! update without a thread id leaves the thread, a send consumes the draft
//! and adds a new SENT message, and every change is in history.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::ids::{AccountId, DraftId, MessageId, ThreadId, Timestamp};
use mailtide::quota::Budget;
use mailtide_gmail::http::{HttpRequest, HttpResponse, Method};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::ObjectBlobs;
use mailtide_io::quota_gate::LocalGate;
use mailtide_io::runner::{Access, LogAccounts, Runner, RunnerConfig, WatchMode, WriteSettings};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::Store as _;
use mailtide_store::memory::MemoryStore;
use serde_json::{Value, json};

/// A message as the fake stores it.
#[derive(Debug, Clone)]
pub struct Message {
    /// Its thread.
    pub thread: String,
    /// Its labels.
    pub labels: Vec<String>,
    /// The history id that last changed it.
    pub history_id: u64,
    /// Its bytes, as Gmail stores them.
    pub raw: Vec<u8>,
}

/// How the fake answers one request a test singled out.
#[derive(Debug, Clone)]
pub enum Fault {
    /// The answer is lost (a timeout); `applied` says whether Gmail acted.
    Lost {
        /// Whether Gmail acted before the answer was lost.
        applied: bool,
    },
    /// Answer with this status, acting on nothing.
    Status(u16),
    /// Act, but answer a write with the draft id only.
    NoIds,
    /// Act, but answer a draft read without its raw content.
    NoRaw,
    /// Act and compute the answer, then hold it until the test releases
    /// it: the request reached Gmail, its answer is still on the way.
    Held(Hold),
    /// No connection was made: the request never reached Gmail.
    NotSent,
    /// Act, then answer 200 with a body that does not decode.
    Undecodable,
}

/// A request held in flight by [`Fault::Held`].
#[derive(Debug, Clone, Default)]
pub struct Hold {
    reached: Arc<Notify>,
    release: Arc<Notify>,
}

impl Hold {
    /// Wait until the held request has reached Gmail.
    pub async fn reached(&self) {
        self.reached.notified().await;
    }

    /// Let its answer go.
    pub fn release(&self) {
        self.release.notify_one();
    }
}

/// The fake's mailbox.
#[derive(Debug, Default)]
pub struct Mailbox {
    /// Messages by id.
    pub messages: BTreeMap<String, Message>,
    /// Drafts: draft id to its current message id.
    pub drafts: BTreeMap<String, String>,
    /// History records, oldest first.
    pub history: Vec<Value>,
    /// The mailbox's history id.
    pub history_id: u64,
    next_id: u64,
    /// Every request, as `METHOD path[?format]`.
    pub requests: Vec<String>,
    faults: VecDeque<(String, Fault)>,
    /// The JSON body of every draft create and update, in order.
    pub saved_bodies: Vec<Value>,
}

impl Mailbox {
    fn id(&mut self, prefix: &str) -> String {
        self.next_id += 1;
        format!("{prefix}{:04}", self.next_id)
    }

    fn record(&mut self, change: Value) {
        self.history_id += 1;
        let mut entry = change;
        entry["id"] = json!(self.history_id.to_string());
        self.history.push(entry);
    }

    /// Add a message as Gmail would: a new id and history id.
    pub fn add_message(&mut self, thread: Option<&str>, labels: &[&str], raw: Vec<u8>) -> String {
        let id = self.id("m");
        let thread = thread.map_or_else(|| format!("t-{id}"), str::to_owned);
        let labels: Vec<String> = labels.iter().map(|l| (*l).to_owned()).collect();
        self.record(json!({"messagesAdded": [{"message": {"id": id, "threadId": thread, "labelIds": labels}}]}));
        self.messages.insert(
            id.clone(),
            Message {
                thread,
                labels,
                history_id: self.history_id,
                raw,
            },
        );
        id
    }

    /// Delete a message as Gmail would (history records it).
    pub fn delete_message(&mut self, id: &str) {
        if let Some(message) = self.messages.remove(id) {
            self.record(json!({"messagesDeleted": [{"message": {"id": id, "threadId": message.thread, "labelIds": message.labels}}]}));
        }
    }

    /// Gmail's own Message-ID in place of the client's, every save.
    fn stamp(&mut self, raw: &[u8]) -> Vec<u8> {
        let gmail_id = self.id("gmail-");
        let text = String::from_utf8_lossy(raw);
        let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        let kept: Vec<&str> = head
            .split("\r\n")
            .filter(|line| !line.to_ascii_lowercase().starts_with("message-id:"))
            .collect();
        format!(
            "Message-ID: <{gmail_id}@mail.gmail.com>\r\n{}\r\n\r\n{body}",
            kept.join("\r\n")
        )
        .into_bytes()
    }

    /// Save a new draft (any client).
    pub fn create_draft(&mut self, raw: &[u8], thread: Option<&str>) -> (String, String) {
        let raw = self.stamp(raw);
        let message = self.add_message(thread, &["DRAFT"], raw);
        let draft = format!("r-{}", self.id(""));
        self.drafts.insert(draft.clone(), message.clone());
        (draft, message)
    }

    /// Replace a draft's message (any client). Without a thread id the
    /// draft leaves its thread, as Gmail does.
    pub fn update_draft(
        &mut self,
        draft: &str,
        raw: &[u8],
        thread: Option<&str>,
    ) -> Option<String> {
        let old = self.drafts.get(draft)?.clone();
        self.delete_message(&old);
        let raw = self.stamp(raw);
        let message = self.add_message(thread, &["DRAFT"], raw);
        self.drafts.insert(draft.to_owned(), message.clone());
        Some(message)
    }

    /// Delete a draft (any client); `false` if there is none.
    pub fn delete_draft(&mut self, draft: &str) -> bool {
        let Some(message) = self.drafts.remove(draft) else {
            return false;
        };
        self.delete_message(&message);
        true
    }

    /// Send a draft (any client): the draft goes, a new SENT message comes.
    pub fn send_draft(&mut self, draft: &str) -> Option<(String, String)> {
        let message = self.drafts.remove(draft)?;
        let stored = self.messages.get(&message)?.clone();
        self.delete_message(&message);
        let raw = self.stamp(&stored.raw);
        let sent = self.add_message(Some(&stored.thread), &["SENT"], raw);
        Some((sent, stored.thread))
    }

    /// Gmail's drafts, as `(draft id, message id)`.
    #[must_use]
    pub fn draft_list(&self) -> Vec<(String, String)> {
        self.drafts
            .iter()
            .map(|(draft, message)| (draft.clone(), message.clone()))
            .collect()
    }

    /// The ids of the messages carrying `label`.
    #[must_use]
    pub fn labelled(&self, label: &str) -> Vec<String> {
        self.messages
            .iter()
            .filter(|(_, m)| m.labels.iter().any(|l| l == label))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// The Message-ID Gmail's copy of a message carries.
    #[must_use]
    pub fn rfc822_message_id(&self, message: &str) -> Option<String> {
        let raw = String::from_utf8_lossy(&self.messages.get(message)?.raw).into_owned();
        raw.split("\r\n")
            .find_map(|line| line.strip_prefix("Message-ID: "))
            .map(str::to_owned)
    }

    fn draft_json(&self, draft: &str, raw: bool) -> Option<Value> {
        let id = self.drafts.get(draft)?;
        let message = self.messages.get(id)?;
        let mut body = json!({"id": draft, "message": {"id": id, "threadId": message.thread, "labelIds": message.labels}});
        if raw {
            body["message"]["raw"] = json!(URL_SAFE_NO_PAD.encode(&message.raw));
        }
        Some(body)
    }
}

/// The fake, shared with the test that drives it: the mailbox of
/// [`account`] and that of [`other_account`], told apart by access token.
#[derive(Debug, Clone, Default)]
pub struct FakeGmail {
    mailbox: Arc<Mutex<Mailbox>>,
    other: Arc<Mutex<Mailbox>>,
}

fn answer(status: u16, body: &Value) -> HttpResponse {
    HttpResponse {
        status,
        headers: Vec::new(),
        body: body.to_string().into_bytes(),
    }
}

fn not_found() -> HttpResponse {
    answer(
        404,
        &json!({"error": {"code": 404, "message": "Requested entity was not found.", "errors": [{"reason": "notFound"}]}}),
    )
}

fn decode_raw(body: &Value) -> Vec<u8> {
    URL_SAFE_NO_PAD
        .decode(body["message"]["raw"].as_str().unwrap_or_default())
        .expect("base64url raw")
}

impl FakeGmail {
    /// The mailbox.
    ///
    /// # Panics
    ///
    /// If a test panicked while holding it.
    pub fn lock(&self) -> std::sync::MutexGuard<'_, Mailbox> {
        self.mailbox.lock().expect("lock")
    }

    /// The mailbox of [`other_account`].
    ///
    /// # Panics
    ///
    /// If a test panicked while holding it.
    pub fn lock_other(&self) -> std::sync::MutexGuard<'_, Mailbox> {
        self.other.lock().expect("lock")
    }

    /// The next request starting with `key` (`"PUT drafts"`, `"POST drafts/send"`)
    /// meets `fault`.
    pub fn fail_next(&self, key: &str, fault: Fault) {
        self.lock().faults.push_back((key.to_owned(), fault));
    }

    /// The bytes of every draft create and update Gmail was sent, in order.
    #[must_use]
    pub fn saved_raw(&self) -> Vec<Vec<u8>> {
        self.lock().saved_bodies.iter().map(decode_raw).collect()
    }

    /// How many times the drafts were listed (`drafts.list`).
    #[must_use]
    pub fn listings(&self) -> usize {
        self.lock()
            .requests
            .iter()
            .filter(|r| *r == "GET drafts")
            .count()
    }

    /// Requests sent so far that start with `prefix`.
    #[must_use]
    pub fn sent(&self, prefix: &str) -> Vec<String> {
        self.lock()
            .requests
            .iter()
            .filter(|r| r.starts_with(prefix))
            .cloned()
            .collect()
    }

    fn respond(
        mailbox: &mut Mailbox,
        method: Method,
        path: &str,
        query: &BTreeMap<String, String>,
        body: &Value,
        fault: Option<&Fault>,
    ) -> HttpResponse {
        let parts: Vec<&str> = path.split('/').collect();
        match (method, parts.as_slice()) {
            (Method::Get, ["profile"]) => answer(
                200,
                &json!({"emailAddress": "alice@example.com", "historyId": mailbox.history_id.to_string()}),
            ),
            (Method::Get, ["labels"]) => answer(
                200,
                &json!({"labels": [
                    {"id": "INBOX", "name": "INBOX", "type": "system"},
                    {"id": "UNREAD", "name": "UNREAD", "type": "system"},
                    {"id": "DRAFT", "name": "DRAFT", "type": "system"},
                    {"id": "SENT", "name": "SENT", "type": "system"},
                ]}),
            ),
            (Method::Post, ["messages", "send"]) => {
                let thread = body["threadId"].as_str();
                if thread
                    .is_some_and(|thread| !mailbox.messages.values().any(|m| m.thread == thread))
                {
                    return not_found();
                }
                let raw = URL_SAFE_NO_PAD
                    .decode(body["raw"].as_str().expect("raw"))
                    .expect("base64url");
                let bytes = mailbox.stamp(&raw);
                let id = mailbox.add_message(thread, &["SENT"], bytes);
                answer(
                    200,
                    &json!({"id": id, "threadId": mailbox.messages[&id].thread}),
                )
            }
            (Method::Get, ["history"]) => {
                let start: u64 = query["startHistoryId"].parse().expect("start");
                let records: Vec<&Value> = mailbox
                    .history
                    .iter()
                    .filter(|h| h["id"].as_str().and_then(|s| s.parse::<u64>().ok()) > Some(start))
                    .collect();
                answer(
                    200,
                    &json!({"history": records, "historyId": mailbox.history_id.to_string()}),
                )
            }
            (Method::Get, ["messages"]) => {
                let ids: Vec<Value> = mailbox
                    .messages
                    .iter()
                    .filter(|(_, m)| query.get("labelIds").is_none_or(|l| m.labels.contains(l)))
                    .map(|(id, m)| json!({"id": id, "threadId": m.thread}))
                    .collect();
                answer(200, &json!({"messages": ids}))
            }
            (Method::Get, ["messages", id]) => match mailbox.messages.get(*id) {
                None => not_found(),
                Some(m) => {
                    let mut body = json!({
                        "id": id, "threadId": m.thread, "labelIds": m.labels,
                        "historyId": m.history_id.to_string(), "internalDate": "1700000000000",
                        "sizeEstimate": m.raw.len(),
                    });
                    if query.get("format").map(String::as_str) == Some("raw") {
                        body["raw"] = json!(URL_SAFE_NO_PAD.encode(&m.raw));
                    }
                    answer(200, &body)
                }
            },
            (_, ["drafts", ..]) => {
                Self::respond_drafts(mailbox, method, &parts, query, body, fault)
            }
            (method, other) => panic!("unexpected request {} {other:?}", method.as_str()),
        }
    }
    /// The `users.drafts` routes.
    fn respond_drafts(
        mailbox: &mut Mailbox,
        method: Method,
        parts: &[&str],
        query: &BTreeMap<String, String>,
        body: &Value,
        fault: Option<&Fault>,
    ) -> HttpResponse {
        let ids_only = matches!(fault, Some(Fault::NoIds));
        match (method, parts) {
            (Method::Get, ["drafts"]) => {
                let drafts: Vec<Value> = mailbox
                    .drafts
                    .iter()
                    .filter_map(|(draft, message)| {
                        let thread = &mailbox.messages.get(message)?.thread;
                        Some(json!({"id": draft, "message": {"id": message, "threadId": thread}}))
                    })
                    .collect();
                answer(
                    200,
                    &json!({"drafts": drafts, "resultSizeEstimate": drafts.len()}),
                )
            }
            (Method::Get, ["drafts", draft]) => {
                let raw = query.get("format").map(String::as_str) == Some("raw")
                    && !matches!(fault, Some(Fault::NoRaw));
                match mailbox.draft_json(draft, raw) {
                    Some(body) => answer(200, &body),
                    // An id that is a message's is not a draft's: 404.
                    None => not_found(),
                }
            }
            (Method::Post, ["drafts"]) => {
                mailbox.saved_bodies.push(body.clone());
                let raw = decode_raw(body);
                let thread = body["message"]["threadId"].as_str().map(str::to_owned);
                let (draft, _) = mailbox.create_draft(&raw, thread.as_deref());
                written(mailbox, &draft, ids_only)
            }
            (Method::Put, ["drafts", draft]) => {
                mailbox.saved_bodies.push(body.clone());
                let raw = decode_raw(body);
                let thread = body["message"]["threadId"].as_str().map(str::to_owned);
                match mailbox.update_draft(draft, &raw, thread.as_deref()) {
                    Some(_) => written(mailbox, draft, ids_only),
                    None => not_found(),
                }
            }
            (Method::Delete, ["drafts", draft]) => {
                if mailbox.delete_draft(draft) {
                    HttpResponse {
                        status: 204,
                        headers: Vec::new(),
                        body: Vec::new(),
                    }
                } else {
                    not_found()
                }
            }
            (Method::Post, ["drafts", "send"]) => {
                let draft = body["id"].as_str().unwrap_or_default().to_owned();
                match mailbox.send_draft(&draft) {
                    Some((sent, thread)) => answer(
                        200,
                        &json!({"id": sent, "threadId": thread, "labelIds": ["SENT"]}),
                    ),
                    None => not_found(),
                }
            }
            (method, other) => panic!("unexpected request {} {other:?}", method.as_str()),
        }
    }
}

fn written(mailbox: &Mailbox, draft: &str, ids_only: bool) -> HttpResponse {
    if ids_only {
        return answer(200, &json!({"id": draft}));
    }
    let body = mailbox
        .draft_json(draft, false)
        .expect("the draft just written");
    answer(200, &body)
}

impl GmailTransport for FakeGmail {
    async fn send(
        &self,
        request: HttpRequest,
        token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        let (answer, held) = {
            let mut mailbox = if token.secret == OTHER_TOKEN {
                self.lock_other()
            } else {
                self.lock()
            };
            Self::serve(&mut mailbox, &request)
        };
        if let Some(hold) = held {
            hold.reached.notify_one();
            hold.release.notified().await;
        }
        answer
    }
}

impl FakeGmail {
    /// Answer one request, and say whether the answer is held.
    fn serve(
        mailbox: &mut Mailbox,
        request: &HttpRequest,
    ) -> (Result<HttpResponse, TransportError>, Option<Hold>) {
        let query: BTreeMap<String, String> = request.query.iter().cloned().collect();
        let path = request
            .path
            .trim_start_matches("/gmail/v1/users/me/")
            .to_owned();
        let line = match query.get("format") {
            Some(format) => format!("{} {path}?{format}", request.method.as_str()),
            None => format!("{} {path}", request.method.as_str()),
        };
        mailbox.requests.push(line.clone());
        let body: Value = request.body.as_deref().map_or(Value::Null, |b| {
            serde_json::from_slice(b).expect("json body")
        });
        let fault = match mailbox.faults.front() {
            Some((key, _)) if line.starts_with(key.as_str()) => {
                mailbox.faults.pop_front().map(|(_, f)| f)
            }
            Some(_) | None => None,
        };
        match fault {
            Some(Fault::Lost { applied }) => {
                if applied {
                    let _ = Self::respond(mailbox, request.method, &path, &query, &body, None);
                }
                (
                    Err(TransportError::OutcomeUnknown("timed out".into())),
                    None,
                )
            }
            Some(Fault::Status(status)) => (
                Ok(answer(
                    status,
                    &json!({"error": {"code": status, "message": "refused", "errors": [{"reason": if status == 403 { "insufficientPermissions" } else { "badRequest" }}]}}),
                )),
                None,
            ),
            Some(Fault::NotSent) => (
                Err(TransportError::NotSent("connection refused".into())),
                None,
            ),
            Some(Fault::Undecodable) => {
                let _ = Self::respond(mailbox, request.method, &path, &query, &body, None);
                (Ok(answer(200, &json!({"unexpected": true}))), None)
            }
            Some(Fault::Held(hold)) => (
                Ok(Self::respond(
                    mailbox,
                    request.method,
                    &path,
                    &query,
                    &body,
                    None,
                )),
                Some(hold),
            ),
            fault => (
                Ok(Self::respond(
                    mailbox,
                    request.method,
                    &path,
                    &query,
                    &body,
                    fault.as_ref(),
                )),
                None,
            ),
        }
    }
}

/// The runner the draft tests drive.
pub type TestRunner = Runner<MemoryStore, ObjectBlobs, FakeGmail, LocalGate, StaticTokens>;

/// The account under test.
///
/// # Panics
///
/// Never: the address is valid.
#[must_use]
pub fn account() -> AccountId {
    AccountId::parse("alice@example.com").expect("account")
}

/// A second account, whose mailbox is [`FakeGmail::lock_other`].
///
/// # Panics
///
/// Never: the address is valid.
#[must_use]
pub fn other_account() -> AccountId {
    AccountId::parse("bob@example.com").expect("account")
}

/// The access token of [`other_account`]: the fake serves its mailbox.
const OTHER_TOKEN: &str = "other-token";

/// A token for `X-Mailtide-Id`.
#[must_use]
pub fn token(n: u32) -> String {
    format!("tracking-token-{n:04}")
}

/// A draft's bytes, carrying `token` and a Message-ID of the caller's own
/// (which Gmail replaces).
#[must_use]
pub fn raw(token: &str, subject: &str) -> Vec<u8> {
    format!(
        "X-Mailtide-Id: {token}\r\nMessage-ID: <client-{subject}@example.com>\r\nSubject: {subject}\r\n\r\nBody of {subject}\r\n"
    )
    .into_bytes()
}

/// The host clock the runner reads, moved by the test.
#[derive(Debug, Clone)]
pub struct TestClock(pub Arc<AtomicI64>);

impl TestClock {
    /// The time.
    #[must_use]
    pub fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }

    /// Move it on.
    ///
    /// # Panics
    ///
    /// For a duration beyond the clock's range.
    pub fn advance(&self, by: Duration) {
        let millis = i64::try_from(by.as_millis()).expect("millis");
        self.0.fetch_add(millis, Ordering::SeqCst);
    }
}

/// What a test holds besides the runner.
pub struct Setup {
    /// The fake, to act as another client and inspect requests.
    pub gmail: FakeGmail,
    /// The clock the runner reads.
    pub clock: TestClock,
}

/// A runner over the fake and a memory store tracking the account, on a
/// host clock (no waiting: deferrals come back at once).
#[must_use]
pub fn runner_with(access: Access, budget: Budget) -> (TestRunner, Setup) {
    runner_writing(access, budget, WriteSettings::default())
}

/// [`runner_with`] with its own write settings (a small
/// `max_message_bytes`, say).
///
/// # Panics
///
/// Never: the account addresses are valid.
#[must_use]
pub fn runner_writing(
    access: Access,
    budget: Budget,
    writes: WriteSettings,
) -> (TestRunner, Setup) {
    let gmail = FakeGmail::default();
    let clock = TestClock(Arc::new(AtomicI64::new(1_700_000_000_000)));
    let token = |secret: &str| AccessToken {
        secret: secret.into(),
        expires_at: Timestamp(i64::MAX),
    };
    let tokens = StaticTokens(BTreeMap::from([
        (account(), token("test-token")),
        (other_account(), token(OTHER_TOKEN)),
    ]));
    let shared = clock.clone();
    let runner = Runner::new(
        MemoryStore::new(),
        ObjectBlobs::memory(),
        gmail.clone(),
        LocalGate::new(budget),
        tokens,
        RunnerConfig {
            access,
            engine: EngineConfig::default(),
            watch: WatchMode::Disabled,
            log_accounts: LogAccounts::Hashed,
            writes,
        },
    )
    .with_clock(mailtide_io::Clock::from_fn(move || shared.now()));
    (runner, Setup { gmail, clock })
}

/// [`runner_with`] read-write and Gmail's default budget.
#[must_use]
pub fn runner() -> (TestRunner, Setup) {
    runner_with(Access::ReadWrite, Budget::GMAIL_DEFAULT)
}

/// Run a test's future.
///
/// # Panics
///
/// If no runtime can be built.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}

/// The time budget of every call.
pub const BUDGET: Duration = Duration::from_secs(5);

/// Track the account and sync it until idle (bootstrap included).
///
/// # Panics
///
/// If the run fails.
pub async fn synced(runner: &TestRunner) {
    runner.track(&account()).await.expect("track");
    sync(runner).await;
}

/// One run until idle.
///
/// # Panics
///
/// If the run fails or the account is blocked.
pub async fn sync(runner: &TestRunner) {
    let outcome = runner.run(&account(), BUDGET).await.expect("run");
    assert!(
        !matches!(outcome.stop, mailtide_io::runner::StopReason::Blocked(_)),
        "{outcome:?}"
    );
}

/// Move the clock past the draft listing delay and sync, so a listing the
/// sync asked for runs.
pub async fn sync_after_delay(runner: &TestRunner, setup: &Setup) {
    sync(runner).await;
    setup.clock.advance(Duration::from_secs(11));
    sync(runner).await;
}

/// A draft id.
#[must_use]
pub fn draft_id(id: &str) -> DraftId {
    DraftId(id.into())
}

/// A message id.
#[must_use]
pub fn message_id(id: &str) -> MessageId {
    MessageId(id.into())
}

/// A thread id.
#[must_use]
pub fn thread_id(id: &str) -> ThreadId {
    ThreadId(id.into())
}

/// The draft entries of the account's feed, as `(kind, draft, message)`.
///
/// # Panics
///
/// If the feed can't be read.
pub async fn draft_feed(runner: &TestRunner) -> Vec<(String, String, String)> {
    use mailtide::revision::{Change, ChangeFeed, StateSeq};
    let ChangeFeed::Page(page) = runner
        .store()
        .changes(&account(), StateSeq(0).into(), 10_000)
        .await
        .expect("changes")
    else {
        panic!("feed");
    };
    page.changes
        .into_iter()
        .filter_map(|entry| match entry.change {
            Change::DraftSaved {
                draft_id,
                message_id,
                origin,
                ..
            } => Some((
                format!("saved:{origin:?}").to_lowercase(),
                draft_id.0,
                message_id.0,
            )),
            Change::DraftRemoved {
                draft_id,
                last_message_id,
                reason,
            } => Some((
                format!("removed:{reason:?}").to_lowercase(),
                draft_id.0,
                last_message_id.0,
            )),
            Change::DraftSent {
                draft_id,
                message_id,
                ..
            } => Some(("sent".to_owned(), draft_id.0, message_id.0)),
            _ => None,
        })
        .collect()
}
