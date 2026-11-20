//! A fake Gmail and a runner around it, shared by the search tests. Each
//! test file declares it `pub mod search_support;`, so the helpers a file
//! does not use are not dead code there.
//!
//! The fake serves what a sync needs (profile, labels, history, listings,
//! minimal and raw gets) from a mailbox, and answers each search request
//! with the next scripted answer: like Gmail's index, it is not derived
//! from the mailbox, and like mailtide, it interprets no query.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::commit::{AccountOp, Commit, JobPlan, MessageOp};
use mailtide::gmail::MessageObservation;
use mailtide::ids::{AccountId, CommitId, HistoryId, MessageId, ThreadId, Timestamp};
use mailtide::labels::set;
use mailtide::quota::{Admission, Budget, Lease, Priority, QuotaUnits};
use mailtide::retry::Cooldown;
use mailtide::search::SearchQuery;
use mailtide::status::AccountStatus;
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, TokenError, TokenSource};
use mailtide_io::blobs::ObjectBlobs;
use mailtide_io::quota_gate::{GateError, LocalGate, QuotaGate};
use mailtide_io::runner::{Access, LogAccounts, RunOutcome, Runner, RunnerConfig, WatchMode};
use mailtide_io::search::{SearchError, SearchPage, ThreadSearchPage};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::Store as _;
use mailtide_store::memory::MemoryStore;
use serde_json::json;

/// The mirrored account.
#[must_use]
pub fn account() -> AccountId {
    AccountId("alice@example.com".into())
}

/// A second tracked account.
#[must_use]
pub fn other_account() -> AccountId {
    AccountId("bob@example.com".into())
}

/// A first-page search for `q` with the defaults.
#[must_use]
pub fn query(q: &str) -> SearchQuery {
    SearchQuery::new(q)
}

/// A page of `messages.list`: each id in thread `t-<id>`.
#[must_use]
pub fn page(ids: &[&str], next: Option<&str>) -> HttpResponse {
    let messages: Vec<serde_json::Value> = ids
        .iter()
        .map(|id| json!({"id": id, "threadId": format!("t-{id}")}))
        .collect();
    let mut body = json!({"messages": messages, "resultSizeEstimate": ids.len()});
    if let Some(next) = next {
        body["nextPageToken"] = json!(next);
    }
    ok(&body)
}

/// A page of `threads.list`, each thread with its history id if given.
#[must_use]
pub fn thread_page(threads: &[(&str, Option<u64>)], next: Option<&str>) -> HttpResponse {
    let threads: Vec<serde_json::Value> = threads
        .iter()
        .map(|(id, history)| {
            let mut thread = json!({"id": id, "snippet": "the snippet"});
            if let Some(history) = history {
                thread["historyId"] = json!(history.to_string());
            }
            thread
        })
        .collect();
    let mut body = json!({"threads": threads, "resultSizeEstimate": threads.len()});
    if let Some(next) = next {
        body["nextPageToken"] = json!(next);
    }
    ok(&body)
}

/// Gmail's error answer, with `Retry-After: 300`.
#[must_use]
pub fn refusal(status: u16, reason: &str) -> HttpResponse {
    HttpResponse {
        status,
        headers: vec![("retry-after".into(), "300".into())],
        body: format!(
            r#"{{"error":{{"code":{status},"message":"Invalid query: from:secret@example.com","errors":[{{"reason":"{reason}"}}]}}}}"#
        )
        .into_bytes(),
    }
}

/// A 200 with `body`.
#[must_use]
pub fn ok(body: &serde_json::Value) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: body.to_string().into_bytes(),
    }
}

fn not_found() -> HttpResponse {
    HttpResponse {
        status: 404,
        headers: Vec::new(),
        body: br#"{"error":{"code":404,"message":"Not Found","errors":[{"reason":"notFound"}]}}"#
            .to_vec(),
    }
}

#[derive(Debug, Default)]
struct Mailbox {
    /// Labels and history id by message id.
    messages: BTreeMap<String, (Vec<String>, u64)>,
    history: Vec<serde_json::Value>,
    history_id: u64,
    /// The answers to the next searches, in order.
    answers: VecDeque<Result<HttpResponse, TransportError>>,
    /// Every search request, in order.
    searches: Vec<HttpRequest>,
    /// Every request.
    requests: u32,
    /// Labels created since the start, beyond `INBOX`, `UNREAD` and `Label_1`.
    created_labels: Vec<(String, String)>,
    /// Each label id `labels.get` was asked for, in order.
    label_gets: Vec<String>,
    /// What every `labels.get` gets instead of Gmail's own answer.
    label_answer: Option<HttpResponse>,
}

/// Gmail, served through the transport.
#[derive(Debug, Default)]
pub struct FakeGmail {
    mailbox: Mutex<Mailbox>,
    /// While set, a search waits for [`Self::release`] before it is answered.
    holding: AtomicBool,
    released: tokio::sync::Notify,
}

impl FakeGmail {
    /// A new message arrives with `labels`.
    pub fn add(&self, id: &str, labels: &[&str]) {
        let mut mailbox = self.mailbox.lock().unwrap_or_else(PoisonError::into_inner);
        mailbox.history_id += 1;
        let at = mailbox.history_id;
        let labels: Vec<String> = labels.iter().map(|l| (*l).to_owned()).collect();
        mailbox.history.push(json!({
            "id": at.to_string(),
            "messagesAdded": [{"message": {"id": id, "threadId": format!("t-{id}"), "labelIds": labels}}],
        }));
        mailbox.messages.insert(id.to_owned(), (labels, at));
    }

    /// The next search gets `answer`.
    pub fn answer(&self, answer: Result<HttpResponse, TransportError>) {
        self.mailbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .answers
            .push_back(answer);
    }

    /// A user label created in Gmail: listed from now on, and found by
    /// `labels.get`.
    pub fn create_label(&self, id: &str, name: &str) {
        self.mailbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .created_labels
            .push((id.to_owned(), name.to_owned()));
    }

    /// Every `labels.get` gets `answer` (a failure, to test its mapping).
    pub fn answer_label_gets(&self, answer: HttpResponse) {
        self.mailbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .label_answer = Some(answer);
    }

    /// Hold every search until [`Self::release`].
    pub fn hold(&self) {
        self.holding.store(true, Ordering::SeqCst);
    }

    /// Answer the held search.
    pub fn release(&self) {
        self.holding.store(false, Ordering::SeqCst);
        self.released.notify_one();
    }

    fn sync_answer(mailbox: &Mailbox, request: &HttpRequest) -> HttpResponse {
        let query: BTreeMap<&str, &str> = request
            .query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let path = request.path.trim_start_matches("/gmail/v1/users/me/");
        match path.split('/').collect::<Vec<_>>().as_slice() {
            ["profile"] => ok(&json!({"historyId": mailbox.history_id.to_string()})),
            ["labels"] => ok(&json!({"labels": Self::labels(mailbox)})),
            ["labels", id] => Self::labels(mailbox)
                .into_iter()
                .find(|label| label["id"] == *id)
                .map_or_else(not_found, |label| ok(&label)),
            ["history"] => {
                let start: u64 = query["startHistoryId"].parse().expect("start");
                let records: Vec<&serde_json::Value> = mailbox
                    .history
                    .iter()
                    .filter(|h| h["id"].as_str().and_then(|s| s.parse::<u64>().ok()) > Some(start))
                    .collect();
                ok(&json!({"history": records, "historyId": mailbox.history_id.to_string()}))
            }
            ["messages"] => {
                let ids: Vec<serde_json::Value> = mailbox
                    .messages
                    .iter()
                    .filter(|(_, (labels, _))| {
                        query
                            .get("labelIds")
                            .is_none_or(|l| labels.iter().any(|x| x == l))
                    })
                    .map(|(id, _)| json!({"id": id, "threadId": format!("t-{id}")}))
                    .collect();
                ok(&json!({"messages": ids}))
            }
            ["messages", id] => match mailbox.messages.get(*id) {
                None => not_found(),
                Some((labels, history_id)) => {
                    let mut body = json!({
                        "id": id, "threadId": format!("t-{id}"), "labelIds": labels,
                        "historyId": history_id.to_string(), "internalDate": "1700000000000",
                        "sizeEstimate": 40, "snippet": "hello",
                    });
                    if query.get("format") == Some(&"raw") {
                        let raw = format!(
                            "Message-ID: <{id}@example.com>\r\nSubject: Message {id}\r\nFrom: a@example.com\r\n\r\nBody of {id}\r\n"
                        );
                        body["raw"] = json!(URL_SAFE_NO_PAD.encode(raw));
                    }
                    ok(&body)
                }
            },
            other => panic!("unexpected request {other:?}"),
        }
    }
}

impl FakeGmail {
    /// Gmail's label definitions.
    fn labels(mailbox: &Mailbox) -> Vec<serde_json::Value> {
        let mut labels = vec![
            json!({"id": "INBOX", "name": "INBOX", "type": "system"}),
            json!({"id": "UNREAD", "name": "UNREAD", "type": "system"}),
            json!({"id": "Label_1", "name": "Clients", "type": "user"}),
        ];
        labels.extend(
            mailbox
                .created_labels
                .iter()
                .map(|(id, name)| json!({"id": id, "name": name, "type": "user"})),
        );
        labels
    }
}

/// Whether `request` is a search (`q` on a listing, or any thread listing).
fn is_search(request: &HttpRequest) -> bool {
    request.path.ends_with("/threads")
        || (request.path.ends_with("/messages") && request.query.iter().any(|(k, _)| k == "q"))
}

impl GmailTransport for FakeGmail {
    async fn send(
        &self,
        request: HttpRequest,
        token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        assert_eq!(token.secret, "test-token");
        // Let a concurrent call run between any two requests.
        tokio::task::yield_now().await;
        if is_search(&request) {
            {
                let mut mailbox = self.mailbox.lock().unwrap_or_else(PoisonError::into_inner);
                mailbox.requests += 1;
                mailbox.searches.push(request);
            }
            if self.holding.load(Ordering::SeqCst) {
                self.released.notified().await;
            }
            return self
                .mailbox
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .answers
                .pop_front()
                .expect("an answer is scripted for every search");
        }
        let mut mailbox = self.mailbox.lock().unwrap_or_else(PoisonError::into_inner);
        mailbox.requests += 1;
        if let Some(id) = request.path.strip_prefix("/gmail/v1/users/me/labels/") {
            mailbox.label_gets.push(id.to_owned());
            if let Some(answer) = &mailbox.label_answer {
                return Ok(answer.clone());
            }
        }
        Ok(Self::sync_answer(&mailbox, &request))
    }
}

/// What passed the gate.
#[derive(Debug, Default)]
pub struct GateLog {
    /// Each admission asked for: cost and class.
    pub admitted: Vec<(QuotaUnits, Priority)>,
    /// Each cooldown published.
    pub cooldowns: Vec<Cooldown>,
}

/// A [`LocalGate`] that records what passes, deferring the first
/// `defer_first` admissions by `defer_by`.
#[derive(Debug)]
pub struct Gate {
    inner: LocalGate,
    defer_first: AtomicU32,
    defer_by: Duration,
    log: Arc<Mutex<GateLog>>,
}

impl QuotaGate for Gate {
    async fn admit(
        &self,
        account: &AccountId,
        cost: QuotaUnits,
        priority: Priority,
        now: Timestamp,
    ) -> Result<Admission, GateError> {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .admitted
            .push((cost, priority));
        let deferring = self
            .defer_first
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if deferring {
            return Ok(Admission::Deferred {
                until: now.plus(self.defer_by),
            });
        }
        self.inner.admit(account, cost, priority, now).await
    }

    async fn release(&self, lease: Lease, bytes: u64, now: Timestamp) -> Result<(), GateError> {
        self.inner.release(lease, bytes, now).await
    }

    async fn cool_down(&self, cooldown: Cooldown) -> Result<(), GateError> {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cooldowns
            .push(cooldown.clone());
        self.inner.cool_down(cooldown).await
    }
}

/// A token source counting the tokens it mints.
#[derive(Debug)]
pub struct Tokens(Arc<AtomicU32>);

impl TokenSource for Tokens {
    async fn token(&self, _account: &AccountId) -> Result<AccessToken, TokenError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(AccessToken {
            secret: "test-token".into(),
            expires_at: Timestamp(i64::MAX),
        })
    }

    fn invalidate(&self, _account: &AccountId) {}
}

/// How a [`Fixture`] is built.
#[derive(Debug, Clone)]
pub struct Options {
    /// Read-write or read-only.
    pub access: Access,
    /// The gate's budgets.
    pub budget: Budget,
    /// Admissions deferred before the gate decides itself.
    pub defer_first: u32,
    /// For how long each.
    pub defer_by: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            access: Access::ReadWrite,
            budget: Budget::GMAIL_DEFAULT,
            defer_first: 0,
            defer_by: Duration::ZERO,
        }
    }
}

/// How long a search may wait for the gate in these tests.
pub const BUDGET: Duration = Duration::from_secs(5);

/// A runner over a memory store tracking [`account`] and
/// [`other_account`], and the fake Gmail.
pub struct Fixture {
    /// The runner.
    pub runner: Runner<MemoryStore, ObjectBlobs, FakeGmail, Gate, Tokens>,
    gate: Arc<Mutex<GateLog>>,
    minted: Arc<AtomicU32>,
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Fixture {
    /// The defaults: read-write, Gmail's default budget, no deferrals.
    #[must_use]
    pub fn new() -> Self {
        Self::with(Options::default())
    }

    /// Built as `options` say.
    #[must_use]
    pub fn with(options: Options) -> Self {
        let gate = Gate {
            inner: LocalGate::new(options.budget),
            defer_first: AtomicU32::new(options.defer_first),
            defer_by: options.defer_by,
            log: Arc::default(),
        };
        let minted = Arc::new(AtomicU32::new(0));
        Self {
            gate: Arc::clone(&gate.log),
            minted: Arc::clone(&minted),
            runner: Runner::new(
                MemoryStore::tracking(&[account(), other_account()]),
                ObjectBlobs::memory(),
                FakeGmail::default(),
                gate,
                Tokens(minted),
                RunnerConfig {
                    access: options.access,
                    engine: EngineConfig {
                        draft_settle: Duration::ZERO,
                        ..EngineConfig::default()
                    },
                    watch: WatchMode::Disabled,
                    log_accounts: LogAccounts::default(),
                },
            ),
        }
    }

    /// The fake Gmail.
    pub fn gmail(&self) -> &FakeGmail {
        self.runner.transport()
    }

    /// The next search gets this page.
    pub fn answer_page(&self, ids: &[&str], next: Option<&str>) {
        self.gmail().answer(Ok(page(ids, next)));
    }

    /// Sync [`account`] until it is caught up.
    ///
    /// # Panics
    ///
    /// If the run fails.
    pub async fn sync(&self) -> RunOutcome {
        self.runner
            .run(&account(), Duration::from_secs(10))
            .await
            .expect("the run")
    }

    /// Search [`account`]'s messages.
    pub async fn search(&self, query: &SearchQuery) -> Result<SearchPage, SearchError> {
        self.runner.search(&account(), query, BUDGET).await
    }

    /// Search [`account`]'s threads.
    pub async fn search_threads(
        &self,
        query: &SearchQuery,
    ) -> Result<ThreadSearchPage, SearchError> {
        self.runner.search_threads(&account(), query, BUDGET).await
    }

    /// [`account`]'s status, read without claiming it.
    ///
    /// # Panics
    ///
    /// If the store fails or the account is not tracked.
    pub async fn status(&self) -> AccountStatus {
        self.runner
            .store()
            .status(&account())
            .await
            .expect("status")
            .expect("tracked")
    }

    /// Commit `messages` and `account_ops` to [`account`]'s mirror directly,
    /// as an engine would (it claims the account).
    ///
    /// # Panics
    ///
    /// If the store fails or refuses the commit.
    pub async fn seed(&self, messages: Vec<MessageOp>, account_ops: Vec<AccountOp>) {
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
            cursor: None,
            claim: None,
            sync: None,
            messages,
            labels: Vec::new(),
            account_ops,
            job_plan: JobPlan {
                priority: Priority::Live,
                import_due_at: Timestamp(0),
                draft_import_due_at: Timestamp(0),
                verify_due_at: Timestamp(0),
                cleanup_due_at: Timestamp(0),
                deliver_due_at: Timestamp(0),
                confirm_due_at: Timestamp(0),
            },
            jobs: Vec::new(),
            intents: Vec::new(),
        };
        let outcome = store.apply(&commit).await.expect("apply");
        assert!(
            matches!(outcome, mailtide::commit::CommitOutcome::Applied { .. }),
            "{outcome:?}"
        );
    }

    /// Every search request Gmail got, in order.
    pub fn searches(&self) -> Vec<HttpRequest> {
        self.gmail()
            .mailbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .searches
            .clone()
    }

    /// Each label id `labels.get` was asked for, in order.
    pub fn label_gets(&self) -> Vec<String> {
        self.gmail()
            .mailbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .label_gets
            .clone()
    }

    /// Every request Gmail got.
    pub fn requests(&self) -> u32 {
        self.gmail()
            .mailbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .requests
    }

    /// Tokens minted so far.
    pub fn tokens_minted(&self) -> u32 {
        self.minted.load(Ordering::SeqCst)
    }

    /// Each admission the gate was asked for.
    pub fn admitted(&self) -> Vec<(QuotaUnits, Priority)> {
        self.gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .admitted
            .clone()
    }

    /// Each cooldown published to the gate.
    pub fn cooldowns(&self) -> Vec<Cooldown> {
        self.gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cooldowns
            .clone()
    }
}

/// A message row without content: listed, its import queued.
#[must_use]
pub fn discovered(id: &str) -> MessageOp {
    MessageOp::Observe(MessageObservation {
        id: MessageId(id.into()),
        thread_id: ThreadId(format!("t-{id}")),
        labels: set(["INBOX"]),
        checkpoint: HistoryId(1),
    })
}

/// The ids of `page`'s hits.
#[must_use]
pub fn hit_ids(page: &SearchPage) -> Vec<&str> {
    page.hits.iter().map(|h| h.message_id.0.as_str()).collect()
}

/// The value of the query parameter `name` in `request`, unencoded.
#[must_use]
pub fn param<'a>(request: &'a HttpRequest, name: &str) -> Option<&'a str> {
    request
        .query
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}
