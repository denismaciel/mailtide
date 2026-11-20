//! Message facts end to end: the runner against a fake Gmail serving raw
//! messages with attachments, real blob writes and store commits.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::gmail::MessageFacts;
use mailtide::ids::{AccountId, MessageId, Timestamp};
use mailtide::mirror::StoredContent;
use mailtide::quota::Budget;
use mailtide::revision::{Change, ChangeFeed, StateSeq};
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::{BlobStore, ObjectBlobs, blob_key};
use mailtide_io::quota_gate::LocalGate;
use mailtide_io::runner::{Runner, RunnerConfig, StopReason, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::Store;
use mailtide_store::memory::MemoryStore;
use serde_json::json;

const REPORT: &[u8] = b"From: =?utf-8?Q?Ren=C3=A9?= <rene@example.org>\r\n\
    To: a@example.org\r\n\
    Bcc: b@example.org\r\n\
    Reply-To: \"Desk, Help\" <help@example.org>\r\n\
    Subject: Report\r\n\
    Date: Tue, 06 Oct 2026 09:00:00 +0200\r\n\
    List-Id: Reports <reports.example.org>\r\n\
    List-Unsubscribe: <https://example.org/u>\r\n\
    List-Unsubscribe-Post: List-Unsubscribe=One-Click\r\n\
    X-Mailtide-Id: k3Jd9_x-Q2mZ8pLw\r\n\
    Content-Type: multipart/mixed; boundary=\"m\"\r\n\
    \r\n\
    --m\r\n\
    Content-Type: multipart/related; boundary=\"r\"\r\n\
    \r\n\
    --r\r\n\
    Content-Type: text/html\r\n\
    \r\n\
    <p>See the chart</p><img src=\"cid:chart@x\">\r\n\
    --r\r\n\
    Content-Type: image/png\r\n\
    Content-ID: <chart@x>\r\n\
    Content-Transfer-Encoding: base64\r\n\
    \r\n\
    iVBORw0KGgo=\r\n\
    --r--\r\n\
    --m\r\n\
    Content-Type: application/pdf; name=\"report.pdf\"\r\n\
    Content-Disposition: attachment; filename=\"report.pdf\"\r\n\
    Content-Transfer-Encoding: base64\r\n\
    \r\n\
    JVBERi0xLjQK\r\n\
    --m--\r\n";

const PLAIN: &[u8] = b"From: a@example.org\r\nSubject: Plain\r\n\r\nhello\r\n";

/// An inbox of raw messages; raw fetches are counted.
#[derive(Debug, Default)]
struct FakeGmail {
    messages: BTreeMap<String, Vec<u8>>,
    raw_fetches: Mutex<u32>,
}

impl FakeGmail {
    fn with(messages: &[(&str, &[u8])]) -> Self {
        Self {
            messages: messages
                .iter()
                .map(|(id, raw)| ((*id).to_owned(), raw.to_vec()))
                .collect(),
            ..Self::default()
        }
    }

    fn raw_fetches(&self) -> u32 {
        *self.raw_fetches.lock().expect("lock")
    }
}

fn ok(body: &serde_json::Value) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: body.to_string().into_bytes(),
    }
}

impl GmailTransport for FakeGmail {
    async fn send(
        &self,
        request: HttpRequest,
        _token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        let query: BTreeMap<&str, &str> = request
            .query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let path = request.path.trim_start_matches("/gmail/v1/users/me/");
        Ok(match path.split('/').collect::<Vec<_>>().as_slice() {
            ["profile"] => ok(&json!({"emailAddress": "user@example.com", "historyId": "10"})),
            ["labels"] => {
                ok(&json!({"labels": [{"id": "INBOX", "name": "INBOX", "type": "system"}]}))
            }
            ["history"] => ok(&json!({"historyId": "10"})),
            ["messages"] => match query.get("labelIds") {
                None | Some(&"INBOX") => ok(&json!({"messages": self
                    .messages
                    .keys()
                    .map(|id| json!({"id": id, "threadId": format!("t-{id}")}))
                    .collect::<Vec<_>>()})),
                Some(_) => ok(&json!({})),
            },
            ["messages", id] => {
                let raw = self.messages.get(*id).expect("a known message");
                let mut body = json!({
                    "id": id, "threadId": format!("t-{id}"), "labelIds": ["INBOX"],
                    "historyId": "10", "internalDate": "1700000000000", "sizeEstimate": raw.len(),
                });
                if query.get("format") == Some(&"raw") {
                    *self.raw_fetches.lock().expect("lock") += 1;
                    body["raw"] = json!(URL_SAFE_NO_PAD.encode(raw));
                }
                ok(&body)
            }
            other => panic!("unexpected request {other:?}"),
        })
    }
}

fn account() -> AccountId {
    AccountId::parse("user@example.com").expect("account")
}

fn id(id: &str) -> MessageId {
    MessageId(id.into())
}

type TestRunner<B> = Runner<MemoryStore, B, FakeGmail, LocalGate, StaticTokens>;

fn runner<B: BlobStore>(gmail: FakeGmail, blobs: B) -> TestRunner<B> {
    let tokens = StaticTokens(BTreeMap::from([(
        account(),
        AccessToken {
            secret: "test".into(),
            expires_at: Timestamp(i64::MAX),
        },
    )]));
    Runner::new(
        MemoryStore::tracking(&[account()]),
        blobs,
        gmail,
        LocalGate::new(Budget::GMAIL_DEFAULT),
        tokens,
        RunnerConfig {
            engine: EngineConfig::default(),
            access: mailtide_io::runner::Access::ReadWrite,
            watch: WatchMode::Disabled,
            log_accounts: mailtide_io::runner::LogAccounts::default(),
        },
    )
}

async fn sync<B: BlobStore>(runner: &TestRunner<B>) {
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

async fn content<B: BlobStore>(runner: &TestRunner<B>, message: &str) -> StoredContent {
    runner
        .store()
        .messages(&account(), &[id(message)])
        .await
        .expect("messages")
        .remove(0)
        .content
        .expect("imported")
}

async fn import_entries<B: BlobStore>(
    runner: &TestRunner<B>,
) -> BTreeMap<String, Option<StoredContent>> {
    let ChangeFeed::Page(page) = runner
        .store()
        .changes(&account(), StateSeq(0).into(), 1_000)
        .await
        .expect("changes")
    else {
        panic!("the feed is not compacted");
    };
    page.changes
        .into_iter()
        .filter_map(|entry| match entry.change {
            Change::MessageImported { id, .. } => Some((id.0, entry.content)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn an_import_stores_envelope_facts_and_the_attachment_summary() {
    let runner = runner(
        FakeGmail::with(&[("m1", REPORT), ("m2", PLAIN)]),
        ObjectBlobs::memory(),
    );
    sync(&runner).await;
    let facts = content(&runner, "m1").await.facts.expect("facts");
    assert_eq!(facts.sender_name.as_deref(), Some("Ren\u{e9}"));
    assert_eq!(facts.sender_email.as_deref(), Some("rene@example.org"));
    assert_eq!(facts.bcc, ["b@example.org"]);
    assert_eq!(facts.reply_to, ["\"Desk, Help\" <help@example.org>"]);
    assert_eq!(facts.sent_at.map(|sent| sent.offset_minutes), Some(120));
    assert_eq!(facts.list_id.as_deref(), Some("reports.example.org"));
    assert!(facts.one_click_unsubscribe);
    assert_eq!(facts.tracking_id.as_deref(), Some("k3Jd9_x-Q2mZ8pLw"));
    assert_eq!((facts.attached, facts.inline), (1, 1));
    let parts: Vec<(&str, bool, u64)> = facts
        .parts
        .iter()
        .map(|part| (part.content_type.as_str(), part.is_inline(), part.part.size))
        .collect();
    assert_eq!(
        parts,
        [("image/png", true, 8), ("application/pdf", false, 9)]
    );
    assert_eq!(
        content(&runner, "m2").await.facts,
        Some(MessageFacts {
            sender_email: Some("a@example.org".into()),
            ..MessageFacts::empty()
        })
    );
    let entries = import_entries(&runner).await;
    assert_eq!(
        entries["m1"].as_ref().and_then(|c| c.facts.clone()),
        Some(facts),
        "the import entry shows the facts messages() shows"
    );
    assert_eq!(runner.transport().raw_fetches(), 2, "one raw fetch each");
}

#[tokio::test]
async fn facts_are_computed_from_the_bytes_that_were_stored() {
    let runner = runner(FakeGmail::with(&[("m1", REPORT)]), ObjectBlobs::memory());
    sync(&runner).await;
    let stored = runner
        .blobs()
        .get(&blob_key(&account(), &id("m1")))
        .await
        .expect("blob");
    let content = content(&runner, "m1").await;
    assert_eq!(content.size, u64::try_from(stored.len()).expect("size"));
    let facts = content.facts.expect("facts");
    assert_eq!(facts, mailtide_mime::facts(&stored));
    for part in &facts.parts {
        assert_eq!(part.part.total, content.size);
        let range = part.part.range();
        let body = &stored[usize::try_from(range.start).expect("start")
            ..usize::try_from(range.end).expect("end")];
        assert!(!body.is_empty());
    }
}

fn give_up(_: &[u8]) -> MessageFacts {
    panic!("the extractor gives up on this message")
}

#[tokio::test]
async fn a_message_the_extractor_gives_up_on_is_still_imported() {
    let runner = runner(FakeGmail::with(&[("m1", REPORT)]), ObjectBlobs::memory())
        .with_facts_extractor(give_up);
    let outcome = runner
        .run(&account(), Duration::from_secs(30))
        .await
        .expect("run");
    assert!(matches!(outcome.stop, StopReason::Idle { .. }));
    assert_eq!(outcome.report.imported, 1);
    let content = content(&runner, "m1").await;
    assert_eq!(content.facts, None);
    assert_eq!(content.headers.subject.as_deref(), Some("Report"));
    assert!(import_entries(&runner).await["m1"].is_some());
}

#[tokio::test]
async fn the_tracking_id_reaches_the_import_entry() {
    let runner = runner(
        FakeGmail::with(&[("m1", REPORT), ("m2", PLAIN)]),
        ObjectBlobs::memory(),
    );
    sync(&runner).await;
    let entries = import_entries(&runner).await;
    let tracking = |id: &str| {
        entries[id]
            .as_ref()
            .and_then(|content| content.facts.as_ref())
            .map(|facts| facts.tracking_id.clone())
    };
    assert_eq!(tracking("m1"), Some(Some("k3Jd9_x-Q2mZ8pLw".to_owned())));
    assert_eq!(
        tracking("m2"),
        Some(None),
        "a message mailtide did not write"
    );
}
