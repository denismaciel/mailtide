//! The runner against a fake Gmail served through the transport: real request
//! building, decoding, quota gate, blob writes and store commits.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::engine::JOB_CONCURRENCY;
use mailtide::ids::{AccountId, MessageId, ThreadId, Timestamp};
use mailtide::jobs::PRIORITISE_MAX_IDS;
use mailtide::labels::set;
use mailtide::mirror::Content;
use mailtide::outbox::{Action, ActionRequest, ReadState};
use mailtide::quota::{Admission, Budget, Lease, Priority, QuotaUnits};
use mailtide::retry::Cooldown;
use mailtide::state::SyncPhase;
use mailtide::thread::ThreadView;
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::{BlobStore, ObjectBlobs, blob_key};
use mailtide_io::quota_gate::{GateError, LocalGate, QuotaGate};
use mailtide_io::runner::{Access, RunError, Runner, RunnerConfig, StopReason, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::memory::MemoryStore;
use serde_json::json;

#[derive(Debug, Clone)]
struct Message {
    thread: String,
    labels: Vec<String>,
    history_id: u64,
    date: i64,
}

#[derive(Debug, Default)]
struct Mailbox {
    messages: BTreeMap<String, Message>,
    history: Vec<serde_json::Value>,
    history_id: u64,
    /// Every raw fetch (an import), in order.
    raw_fetches: Vec<String>,
    modified: u32,
}

impl Mailbox {
    fn add(&mut self, id: &str, labels: &[&str]) {
        self.add_in(&format!("t-{id}"), id, labels, 1_700_000_000_000);
    }

    fn add_in(&mut self, thread: &str, id: &str, labels: &[&str], date: i64) {
        self.history_id += 1;
        let labels: Vec<String> = labels.iter().map(|l| (*l).to_owned()).collect();
        self.messages.insert(
            id.to_owned(),
            Message {
                thread: thread.to_owned(),
                labels: labels.clone(),
                history_id: self.history_id,
                date,
            },
        );
        self.history.push(json!({
            "id": self.history_id.to_string(),
            "messagesAdded": [{"message": {"id": id, "threadId": thread, "labelIds": labels}}],
        }));
    }

    /// `messages.modify`: the change and its history records.
    fn modify(&mut self, id: &str, body: &serde_json::Value) {
        self.modified += 1;
        self.history_id += 1;
        let names = |key: &str| -> Vec<String> {
            body[key]
                .as_array()
                .map(|labels| {
                    labels
                        .iter()
                        .filter_map(|l| l.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default()
        };
        let (add, remove) = (names("addLabelIds"), names("removeLabelIds"));
        let message = self.messages.get_mut(id).expect("message");
        message.labels.retain(|l| !remove.contains(l));
        for label in &add {
            if !message.labels.contains(label) {
                message.labels.push(label.clone());
            }
        }
        message.history_id = self.history_id;
        self.history.push(json!({
            "id": self.history_id.to_string(),
            "labelsAdded": [{"message": {"id": id, "threadId": message.thread}, "labelIds": add}],
            "labelsRemoved": [{"message": {"id": id, "threadId": message.thread}, "labelIds": remove}],
        }));
    }

    fn remove_label(&mut self, id: &str, label: &str) {
        self.history_id += 1;
        let message = self.messages.get_mut(id).expect("message");
        message.labels.retain(|l| l != label);
        message.history_id = self.history_id;
        self.history.push(json!({
            "id": self.history_id.to_string(),
            "labelsRemoved": [{"message": {"id": id, "threadId": message.thread}, "labelIds": [label]}],
        }));
    }
}

#[derive(Debug, Default)]
struct FakeGmail(Mutex<Mailbox>);

fn ok(body: &serde_json::Value) -> HttpResponse {
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

impl GmailTransport for FakeGmail {
    async fn send(
        &self,
        request: HttpRequest,
        token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        assert_eq!(token.secret, "test-token");
        let mut mailbox = self.0.lock().expect("lock");
        let query: BTreeMap<&str, &str> = request
            .query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let path = request.path.trim_start_matches("/gmail/v1/users/me/");
        if let Some(id) = path.strip_prefix("messages/")
            && query.get("format") == Some(&"raw")
        {
            mailbox.raw_fetches.push(id.to_owned());
        }
        let response = match path.split('/').collect::<Vec<_>>().as_slice() {
            ["profile"] => ok(
                &json!({"emailAddress": "alice@example.com", "historyId": mailbox.history_id.to_string()}),
            ),
            ["labels"] => ok(&json!({"labels": [
                {"id": "INBOX", "name": "INBOX", "type": "system"},
                {"id": "UNREAD", "name": "UNREAD", "type": "system"},
            ]})),
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
                    .filter(|(_, m)| {
                        query
                            .get("labelIds")
                            .is_none_or(|l| m.labels.iter().any(|x| x == l))
                    })
                    .map(|(id, m)| json!({"id": id, "threadId": m.thread}))
                    .collect();
                ok(&json!({"messages": ids}))
            }
            ["messages", id] => match mailbox.messages.get(*id) {
                None => not_found(),
                Some(m) => {
                    let mut body = json!({
                        "id": id, "threadId": m.thread, "labelIds": m.labels,
                        "historyId": m.history_id.to_string(), "internalDate": m.date.to_string(), "sizeEstimate": 40,
                        "snippet": "hello",
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
            ["messages", id, "modify"] => {
                let body: serde_json::Value =
                    serde_json::from_slice(request.body.as_deref().unwrap_or(b"{}")).expect("json");
                mailbox.modify(id, &body);
                ok(&json!({}))
            }
            other => panic!("unexpected request {other:?}"),
        };
        Ok(response)
    }
}

fn account() -> AccountId {
    AccountId::parse("alice@example.com").expect("account")
}

fn runner(
    gmail: FakeGmail,
) -> Runner<MemoryStore, ObjectBlobs, FakeGmail, LocalGate, StaticTokens> {
    let tokens = StaticTokens(BTreeMap::from([(
        account(),
        AccessToken {
            secret: "test-token".into(),
            expires_at: Timestamp(i64::MAX),
        },
    )]));
    Runner::new(
        MemoryStore::tracking(&[account()]),
        ObjectBlobs::memory(),
        gmail,
        LocalGate::new(Budget::GMAIL_DEFAULT),
        tokens,
        RunnerConfig {
            engine: EngineConfig {
                draft_settle: Duration::ZERO,
                confirm_timeout: Duration::ZERO,
                ..EngineConfig::default()
            },
            access: mailtide_io::runner::Access::ReadWrite,
            watch: WatchMode::Disabled,
            log_accounts: mailtide_io::runner::LogAccounts::default(),
        },
    )
}

#[tokio::test]
async fn bootstraps_then_follows_history() {
    let gmail = FakeGmail::default();
    {
        let mut mailbox = gmail.0.lock().expect("lock");
        mailbox.add("m1", &["INBOX", "UNREAD"]);
        mailbox.add("m2", &["INBOX"]);
        mailbox.add("m3", &[]);
    }
    let runner = runner(gmail);
    let outcome = runner
        .run(&account(), Duration::from_secs(30))
        .await
        .expect("run");
    assert!(
        matches!(outcome.stop, StopReason::Idle { .. }),
        "{:?}",
        outcome.stop
    );
    let status = outcome.report.status.expect("status");
    assert_eq!(status.phase, SyncPhase::Live);
    let mirror = runner.store().mirror(&account()).expect("mirror");
    assert_eq!(mirror.messages.len(), 3);
    for (id, message) in &mirror.messages {
        assert!(
            matches!(message.content, Content::Imported(_)),
            "{} not imported",
            id.0
        );
        let raw = runner
            .blobs()
            .get(&blob_key(&account(), id))
            .await
            .expect("blob");
        assert!(String::from_utf8_lossy(&raw).contains(&format!("Body of {}", id.0)));
    }
    assert_eq!(
        mirror.messages[&MessageId("m1".into())].labels,
        set(["INBOX", "UNREAD"])
    );
    assert!(
        mirror.messages[&MessageId("m3".into())].labels.is_empty(),
        "archived mail with no labels is found"
    );

    {
        let mut mailbox = runner_gmail(&runner).0.lock().expect("lock");
        mailbox.add("m4", &["INBOX", "UNREAD"]);
        mailbox.remove_label("m1", "UNREAD");
    }
    let outcome = runner
        .run(&account(), Duration::from_secs(30))
        .await
        .expect("second run");
    assert!(
        matches!(outcome.stop, StopReason::Idle { .. }),
        "{:?}",
        outcome.stop
    );
    let mirror = runner.store().mirror(&account()).expect("mirror");
    assert_eq!(mirror.messages.len(), 4);
    assert_eq!(
        mirror.messages[&MessageId("m1".into())].labels,
        set(["INBOX"])
    );
    assert!(matches!(
        mirror.messages[&MessageId("m4".into())].content,
        Content::Imported(_)
    ));
    let headers = match &mirror.messages[&MessageId("m4".into())].content {
        Content::Imported(stored) => stored.headers.subject.clone(),
        Content::Pending => None,
    };
    assert_eq!(headers.as_deref(), Some("Message m4"));
    assert_eq!(outcome.report.cursor_after.map(|c| c.0), Some(5));
}

fn runner_gmail(
    runner: &Runner<MemoryStore, ObjectBlobs, FakeGmail, LocalGate, StaticTokens>,
) -> &FakeGmail {
    runner.transport()
}

/// The local gate, logging the class of every import it lets through.
#[derive(Debug)]
struct RecordingGate {
    gate: LocalGate,
    imports: Arc<Mutex<Vec<Priority>>>,
}

impl QuotaGate for RecordingGate {
    async fn admit(
        &self,
        account: &AccountId,
        cost: QuotaUnits,
        priority: Priority,
        now: Timestamp,
    ) -> Result<Admission, GateError> {
        let admission = self.gate.admit(account, cost, priority, now).await?;
        if matches!(admission, Admission::Granted(_)) && cost == QuotaUnits(60) {
            self.imports.lock().expect("lock").push(priority);
        }
        Ok(admission)
    }

    async fn release(&self, lease: Lease, bytes: u64, now: Timestamp) -> Result<(), GateError> {
        self.gate.release(lease, bytes, now).await
    }

    async fn cool_down(&self, cooldown: Cooldown) -> Result<(), GateError> {
        self.gate.cool_down(cooldown).await
    }

    fn abandon(&self, lease: Lease) {
        self.gate.abandon(lease);
    }
}

type TightRunner = Runner<MemoryStore, ObjectBlobs, FakeGmail, RecordingGate, StaticTokens>;

/// A mailbox of `count` messages behind a quota whose Backfill share admits
/// the bootstrap and two imports a minute (70% of 250 units); Foreground
/// may use all 250.
fn backlog(count: usize, access: Access) -> (TightRunner, Arc<Mutex<Vec<Priority>>>) {
    let gmail = FakeGmail::default();
    {
        let mut mailbox = gmail.0.lock().expect("lock");
        for n in 0..count {
            mailbox.add(&format!("m{n:03}"), &["INBOX"]);
        }
    }
    let imports = Arc::new(Mutex::new(Vec::new()));
    let gate = RecordingGate {
        gate: LocalGate::new(Budget {
            account_per_minute: 250,
            ..Budget::GMAIL_DEFAULT
        }),
        imports: imports.clone(),
    };
    let tokens = StaticTokens(BTreeMap::from([(
        account(),
        AccessToken {
            secret: "test-token".into(),
            expires_at: Timestamp(i64::MAX),
        },
    )]));
    let runner = Runner::new(
        MemoryStore::tracking(&[account()]),
        ObjectBlobs::memory(),
        gmail,
        gate,
        tokens,
        RunnerConfig {
            engine: EngineConfig::default(),
            access,
            watch: WatchMode::Disabled,
            log_accounts: mailtide_io::runner::LogAccounts::default(),
        },
    );
    (runner, imports)
}

/// Messages discovered but not yet imported.
fn pending(runner: &TightRunner) -> Vec<MessageId> {
    let mirror = runner.store().mirror(&account()).expect("mirror");
    mirror
        .messages
        .values()
        .filter(|m| m.content == Content::Pending)
        .map(|m| m.id.clone())
        .collect()
}

#[tokio::test]
async fn an_opened_message_jumps_the_backlog() {
    let (runner, imports) = backlog(50, Access::ReadWrite);
    // The bootstrap discovers everything; the quota holds the backlog back.
    runner
        .run(&account(), Duration::from_secs(2))
        .await
        .expect("bootstrap");
    let backlog = pending(&runner);
    assert!(backlog.len() >= 40, "{} pending", backlog.len());
    assert!(
        imports
            .lock()
            .expect("lock")
            .iter()
            .all(|p| *p == Priority::Backfill),
        "the backlog imports in its own class"
    );
    let opened = backlog[0].clone();

    let report = runner
        .prioritise(&account(), std::slice::from_ref(&opened))
        .await
        .expect("prioritise");
    assert_eq!(report.queued, vec![opened.clone()]);

    let before = {
        let mailbox = runner.transport().0.lock().expect("lock");
        mailbox.raw_fetches.len()
    };
    let classes_before = imports.lock().expect("lock").len();
    runner
        .run(&account(), Duration::from_secs(2))
        .await
        .expect("run");
    let mirror = runner.store().mirror(&account()).expect("mirror");
    assert!(
        matches!(mirror.messages[&opened].content, Content::Imported(_)),
        "the opened message is imported"
    );
    let fetched = runner.transport().0.lock().expect("lock").raw_fetches[before..].to_vec();
    let classes = imports.lock().expect("lock")[classes_before..].to_vec();
    assert_eq!(fetched.len(), classes.len(), "one admission per fetch");
    let position = fetched
        .iter()
        .position(|id| *id == opened.0)
        .expect("the opened message is fetched");
    assert_eq!(classes[position], Priority::Foreground);
    assert!(
        position <= JOB_CONCURRENCY as usize,
        "{position} backlog imports went first"
    );
}

#[tokio::test]
async fn prioritise_refuses_more_than_the_call_limit() {
    let (runner, _) = backlog(5, Access::ReadWrite);
    runner
        .run(&account(), Duration::from_secs(2))
        .await
        .expect("bootstrap");
    let before = runner.store().mirror(&account());
    let mut ids = pending(&runner);
    ids.extend((0..=PRIORITISE_MAX_IDS).map(|n| MessageId(format!("x{n}"))));
    assert!(matches!(
        runner.prioritise(&account(), &ids).await,
        Err(RunError::TooManyIds { given }) if given == ids.len()
    ));
    assert_eq!(runner.store().mirror(&account()), before, "nothing written");
    ids.truncate(PRIORITISE_MAX_IDS);
    let report = runner
        .prioritise(&account(), &ids)
        .await
        .expect("the limit itself is allowed");
    assert!(!report.queued.is_empty());
}

#[tokio::test]
async fn prioritise_works_in_read_only_mode() {
    let (runner, _) = backlog(10, Access::ReadOnly);
    runner
        .run(&account(), Duration::from_secs(2))
        .await
        .expect("bootstrap");
    let opened = pending(&runner)[0].clone();
    let report = runner
        .prioritise(&account(), std::slice::from_ref(&opened))
        .await
        .expect("an import changes nothing in Gmail");
    assert_eq!(report.queued, vec![opened.clone()]);
    let action = ActionRequest::Messages {
        ids: vec![opened],
        action: Action::Archive,
    };
    assert!(matches!(
        runner.act(&account(), action, Duration::from_secs(1)).await,
        Err(RunError::ReadOnly)
    ));
}

async fn thread(
    runner: &Runner<MemoryStore, ObjectBlobs, FakeGmail, LocalGate, StaticTokens>,
    id: &str,
) -> ThreadView {
    runner
        .threads(&account(), &[ThreadId(id.into())])
        .await
        .expect("threads")
        .pop()
        .expect("the thread")
}

async fn run_to_idle(
    runner: &Runner<MemoryStore, ObjectBlobs, FakeGmail, LocalGate, StaticTokens>,
) {
    let outcome = runner
        .run(&account(), Duration::from_secs(30))
        .await
        .expect("run");
    assert!(
        matches!(outcome.stop, StopReason::Idle { .. }),
        "{:?}",
        outcome.stop
    );
}

#[tokio::test]
async fn a_reply_synced_from_history_shows_in_its_thread_view() {
    let gmail = FakeGmail::default();
    gmail
        .0
        .lock()
        .expect("lock")
        .add_in("t", "m1", &["INBOX"], 1_700_000_000_000);
    let runner = runner(gmail);
    run_to_idle(&runner).await;
    let before = thread(&runner, "t").await;
    assert_eq!(before.latest, Some(MessageId("m1".into())));
    assert!(!before.unread);

    runner_gmail(&runner).0.lock().expect("lock").add_in(
        "t",
        "m2",
        &["INBOX", "UNREAD"],
        1_700_000_060_000,
    );
    run_to_idle(&runner).await;
    let after = thread(&runner, "t").await;
    let ids: Vec<&str> = after.messages.iter().map(|m| m.id.0.as_str()).collect();
    assert_eq!(ids, ["m1", "m2"]);
    assert!(after.unread);
    assert_eq!(after.labels, set(["INBOX", "UNREAD"]));
    assert_eq!(after.latest, Some(MessageId("m2".into())));
    assert_eq!((after.message_count, after.awaiting_import), (2, 0));
    let latest = after.messages[1].content.as_ref().expect("imported");
    assert_eq!(latest.headers.subject.as_deref(), Some("Message m2"));
    assert_eq!(latest.internal_date, Timestamp(1_700_000_060_000));
    assert_eq!(after.participants, ["a@example.com"]);
}

#[tokio::test]
async fn a_thread_action_shows_in_the_thread_view_before_it_is_delivered() {
    let gmail = FakeGmail::default();
    {
        let mut mailbox = gmail.0.lock().expect("lock");
        mailbox.add_in("t", "m1", &["INBOX", "UNREAD"], 1_700_000_000_000);
        mailbox.add_in("t", "m2", &["INBOX", "UNREAD"], 1_700_000_060_000);
    }
    let runner = runner(gmail);
    run_to_idle(&runner).await;
    assert!(thread(&runner, "t").await.unread);

    let admitted = runner
        .admit(
            &account(),
            &ActionRequest::Thread {
                id: ThreadId("t".into()),
                action: Action::SetRead(ReadState::Read),
            },
        )
        .await
        .expect("admit");
    assert_eq!(admitted, 2);
    assert_eq!(runner_gmail(&runner).0.lock().expect("lock").modified, 0);
    let admitted_view = thread(&runner, "t").await;
    assert!(!admitted_view.unread);
    assert_eq!(admitted_view.labels, set(["INBOX"]));
    assert!(
        admitted_view
            .messages
            .iter()
            .all(|m| m.pending_actions == 1 && m.observed_labels == set(["INBOX", "UNREAD"]))
    );

    run_to_idle(&runner).await;
    {
        let mailbox = runner_gmail(&runner).0.lock().expect("lock");
        assert_eq!(mailbox.modified, 2);
        assert!(mailbox.messages.values().all(|m| m.labels == ["INBOX"]));
    }
    let confirmed = thread(&runner, "t").await;
    assert!(
        confirmed
            .messages
            .iter()
            .all(|m| m.pending_actions == 0 && m.observed_labels == set(["INBOX"]))
    );
    assert_eq!(confirmed.unread, admitted_view.unread);
    assert_eq!(confirmed.labels, admitted_view.labels);
    assert_eq!(confirmed.latest, admitted_view.latest);
}
