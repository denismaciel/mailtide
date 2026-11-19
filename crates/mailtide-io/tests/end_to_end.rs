//! The runner against a fake Gmail served through the transport: real request
//! building, decoding, quota gate, blob writes and store commits.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::ids::{AccountId, MessageId, Timestamp};
use mailtide::labels::set;
use mailtide::mirror::Content;
use mailtide::quota::Budget;
use mailtide::state::SyncPhase;
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::{BlobStore, ObjectBlobs, blob_key};
use mailtide_io::quota_gate::LocalGate;
use mailtide_io::runner::{Runner, RunnerConfig, StopReason, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::memory::MemoryStore;
use serde_json::json;

#[derive(Debug, Clone)]
struct Message {
    thread: String,
    labels: Vec<String>,
    history_id: u64,
}

#[derive(Debug, Default)]
struct Mailbox {
    messages: BTreeMap<String, Message>,
    history: Vec<serde_json::Value>,
    history_id: u64,
}

impl Mailbox {
    fn add(&mut self, id: &str, labels: &[&str]) {
        self.history_id += 1;
        let labels: Vec<String> = labels.iter().map(|l| (*l).to_owned()).collect();
        self.messages.insert(
            id.to_owned(),
            Message {
                thread: format!("t-{id}"),
                labels: labels.clone(),
                history_id: self.history_id,
            },
        );
        self.history.push(json!({
            "id": self.history_id.to_string(),
            "messagesAdded": [{"message": {"id": id, "threadId": format!("t-{id}"), "labelIds": labels}}],
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
        let mailbox = self.0.lock().expect("lock");
        let query: BTreeMap<&str, &str> = request
            .query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let path = request.path.trim_start_matches("/gmail/v1/users/me/");
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
                        "historyId": m.history_id.to_string(), "internalDate": "1700000000000", "sizeEstimate": 40,
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
        MemoryStore::new(),
        ObjectBlobs::memory(),
        gmail,
        LocalGate::new(Budget::GMAIL_DEFAULT),
        tokens,
        RunnerConfig {
            engine: EngineConfig {
                draft_settle: Duration::ZERO,
                ..EngineConfig::default()
            },
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
