//! Message facts end to end: the runner against a fake Gmail serving raw
//! messages with attachments, real blob writes and store commits.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::commit::{Commit, CommitOutcome, DeletionEvidence, JobPlan, MessageOp};
use mailtide::gmail::{FACTS_VERSION, MessageFacts};
use mailtide::ids::{
    AccountId, BlobRef, CommitId, ENGINE_VERSION, HistoryId, MessageId, Timestamp,
};
use mailtide::mirror::StoredContent;
use mailtide::quota::Budget;
use mailtide::revision::{Change, ChangeFeed, StateSeq};
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::{BlobError, BlobStore, ObjectBlobs, RangeRead, blob_key};
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

/// An inbox of raw messages; requests and raw fetches are counted.
#[derive(Debug, Default)]
struct FakeGmail {
    messages: BTreeMap<String, Vec<u8>>,
    requests: Mutex<u32>,
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

    /// `count` plain messages, `m00` to `m{count - 1}`.
    fn plain(count: usize) -> Self {
        Self {
            messages: (0..count)
                .map(|n| (format!("m{n:02}"), PLAIN.to_vec()))
                .collect(),
            ..Self::default()
        }
    }

    fn requests(&self) -> u32 {
        *self.requests.lock().expect("lock")
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
        *self.requests.lock().expect("lock") += 1;
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
            // A reconciliation ends by listing the drafts.
            ["drafts"] => ok(&json!({"resultSizeEstimate": 0})),
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
        // Room to import every test message in one minute.
        LocalGate::new(Budget {
            account_per_minute: 100_000,
            ..Budget::GMAIL_DEFAULT
        }),
        tokens,
        RunnerConfig {
            engine: EngineConfig::default(),
            access: mailtide_io::runner::Access::ReadWrite,
            watch: WatchMode::Disabled,
            log_accounts: mailtide_io::runner::LogAccounts::default(),
            writes: mailtide_io::runner::WriteSettings::default(),
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

/// Blobs in memory that count whole reads and can hold one message's read
/// until the test lets it go.
struct Blobs {
    inner: ObjectBlobs,
    gets: Mutex<u32>,
    hold: Option<(BlobRef, tokio::sync::Notify, tokio::sync::Notify)>,
}

impl Blobs {
    fn new() -> Self {
        Self {
            inner: ObjectBlobs::memory(),
            gets: Mutex::new(0),
            hold: None,
        }
    }

    /// Reads of `message` wait, once they started, until `release`.
    fn holding(message: &str) -> Self {
        Self {
            hold: Some((
                blob_key(&account(), &id(message)),
                tokio::sync::Notify::new(),
                tokio::sync::Notify::new(),
            )),
            ..Self::new()
        }
    }

    fn gets(&self) -> u32 {
        *self.gets.lock().expect("lock")
    }
}

impl BlobStore for Blobs {
    async fn put(&self, key: &BlobRef, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &BlobRef) -> Result<Vec<u8>, BlobError> {
        *self.gets.lock().expect("lock") += 1;
        let bytes = self.inner.get(key).await;
        if let Some((held, started, release)) = &self.hold
            && held == key
        {
            started.notify_one();
            release.notified().await;
        }
        bytes
    }

    async fn get_range(&self, key: &BlobRef, range: Range<u64>) -> Result<RangeRead, BlobError> {
        self.inner.get_range(key, range).await
    }

    async fn delete(&self, key: &BlobRef) -> Result<(), BlobError> {
        self.inner.delete(key).await
    }

    async fn delete_account(&self, account: &AccountId) -> Result<u64, BlobError> {
        self.inner.delete_account(account).await
    }
}

/// A runner that imported `gmail`'s messages without facts, as a build from
/// before them did, then took the current extractor.
async fn imported_before_facts(gmail: FakeGmail, blobs: Blobs) -> TestRunner<Blobs> {
    let runner = runner(gmail, blobs).with_facts_extractor(give_up);
    sync(&runner).await;
    runner.with_facts_extractor(mailtide_mime::facts)
}

async fn facts_of<B: BlobStore>(runner: &TestRunner<B>, message: &str) -> Option<MessageFacts> {
    content(runner, message).await.facts
}

#[tokio::test]
async fn refresh_facts_fills_old_rows_without_a_gmail_call() {
    let runner = imported_before_facts(
        FakeGmail::with(&[("m1", REPORT), ("m2", PLAIN)]),
        Blobs::new(),
    )
    .await;
    assert_eq!(facts_of(&runner, "m1").await, None);
    let requests = runner.transport().requests();
    let done = runner
        .refresh_facts(&account(), None, 100, Duration::from_secs(30))
        .await
        .expect("refresh");
    assert_eq!((done.examined, done.refreshed, done.next), (2, 2, None));
    assert!(done.unreadable.is_empty());
    assert_eq!(runner.transport().requests(), requests, "no Gmail call");
    let facts = facts_of(&runner, "m1").await.expect("facts");
    assert_eq!(facts.version, FACTS_VERSION);
    assert_eq!((facts.attached, facts.inline), (1, 1));
    assert_eq!(facts.tracking_id.as_deref(), Some("k3Jd9_x-Q2mZ8pLw"));
    let ChangeFeed::Page(page) = runner
        .store()
        .changes(&account(), StateSeq(0).into(), 1_000)
        .await
        .expect("changes")
    else {
        panic!("the feed is not compacted");
    };
    let refreshed: Vec<&str> = page
        .changes
        .iter()
        .filter_map(|entry| match &entry.change {
            Change::FactsRefreshed { id, .. } => Some(id.0.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(refreshed, ["m1", "m2"]);
    assert_eq!(
        import_entries(&runner).await["m1"]
            .as_ref()
            .and_then(|c| c.facts.clone()),
        Some(facts),
        "the earlier import entry shows them too"
    );
}

#[tokio::test]
async fn refresh_facts_resumes_from_next_and_ends_with_none() {
    let runner = imported_before_facts(FakeGmail::plain(7), Blobs::new()).await;
    let mut after = None;
    let mut calls = Vec::new();
    loop {
        let done = runner
            .refresh_facts(&account(), after.as_ref(), 3, Duration::from_secs(30))
            .await
            .expect("refresh");
        calls.push((done.examined, done.refreshed));
        match done.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(calls, [(3, 3), (3, 3), (1, 1)]);
    for n in 0..7 {
        assert!(facts_of(&runner, &format!("m{n:02}")).await.is_some());
    }
}

#[tokio::test]
async fn refresh_facts_stops_at_its_budget_between_batches() {
    let runner = imported_before_facts(FakeGmail::plain(60), Blobs::new()).await;
    let done = runner
        .refresh_facts(&account(), None, 1_000, Duration::ZERO)
        .await
        .expect("refresh");
    assert_eq!((done.examined, done.refreshed), (50, 50), "one whole batch");
    assert_eq!(done.next, Some(id("m49")));
    assert!(facts_of(&runner, "m49").await.is_some());
    assert!(facts_of(&runner, "m50").await.is_none());
    let rest = runner
        .refresh_facts(&account(), done.next.as_ref(), 1_000, Duration::ZERO)
        .await
        .expect("refresh");
    assert_eq!((rest.examined, rest.refreshed, rest.next), (10, 10, None));
}

#[tokio::test]
async fn refresh_facts_skips_current_rows_without_reading_their_blobs() {
    let runner = imported_before_facts(FakeGmail::plain(5), Blobs::new()).await;
    let first = runner
        .refresh_facts(&account(), None, 100, Duration::from_secs(30))
        .await
        .expect("refresh");
    assert_eq!(first.refreshed, 5);
    let reads = runner.blobs().gets();
    let head = runner
        .store()
        .status(&account())
        .await
        .expect("status")
        .expect("tracked")
        .state;
    let second = runner
        .refresh_facts(&account(), None, 100, Duration::from_secs(30))
        .await
        .expect("refresh");
    assert_eq!((second.examined, second.refreshed), (5, 0));
    assert_eq!(runner.blobs().gets(), reads, "no blob read");
    let status = runner
        .store()
        .status(&account())
        .await
        .expect("status")
        .expect("tracked");
    assert_eq!(status.state, head, "nothing written");
}

#[tokio::test]
async fn refresh_facts_reports_a_missing_blob_and_goes_on() {
    let runner = imported_before_facts(FakeGmail::plain(3), Blobs::new()).await;
    runner
        .blobs()
        .delete(&blob_key(&account(), &id("m01")))
        .await
        .expect("delete");
    let done = runner
        .refresh_facts(&account(), None, 100, Duration::from_secs(30))
        .await
        .expect("refresh");
    assert_eq!((done.examined, done.refreshed), (3, 2));
    assert_eq!(done.unreadable, [id("m01")]);
    assert!(facts_of(&runner, "m00").await.is_some());
    assert!(
        facts_of(&runner, "m01").await.is_none(),
        "its row is untouched"
    );
    assert!(facts_of(&runner, "m02").await.is_some());
}

#[tokio::test]
async fn refresh_facts_reextracts_a_row_whose_blob_length_changed() {
    let runner = runner(FakeGmail::with(&[("m1", PLAIN)]), Blobs::new());
    sync(&runner).await;
    let before = content(&runner, "m1").await;
    assert_eq!(
        before.facts.as_ref().map(|f| f.version),
        Some(FACTS_VERSION)
    );
    // A second write of other bytes (raw content not byte-stable), its
    // import lost.
    runner
        .blobs()
        .put(&blob_key(&account(), &id("m1")), REPORT.to_vec())
        .await
        .expect("put");
    let done = runner
        .refresh_facts(&account(), None, 100, Duration::from_secs(30))
        .await
        .expect("refresh");
    assert_eq!(done.refreshed, 1);
    let after = content(&runner, "m1").await;
    assert_eq!(after.size, u64::try_from(REPORT.len()).expect("len"));
    assert_eq!(after.facts, Some(mailtide_mime::facts(REPORT)));
    assert!(
        after
            .facts
            .iter()
            .flat_map(|f| &f.parts)
            .all(|p| p.part.total == after.size)
    );
}

/// Delete `message` as a history record would, from a newer engine.
async fn tombstone<B: BlobStore>(runner: &TestRunner<B>, message: &str) {
    let snapshot = runner
        .store()
        .claim(&account(), ENGINE_VERSION)
        .await
        .expect("claim");
    let plan = JobPlan {
        priority: mailtide::quota::Priority::Live,
        import_due_at: Timestamp(0),
        draft_import_due_at: Timestamp(0),
        verify_due_at: Timestamp(0),
        cleanup_due_at: Timestamp(0),
        deliver_due_at: Timestamp(0),
        confirm_due_at: Timestamp(0),
        draft_refresh_due_at: Timestamp(0),
    };
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
        messages: vec![MessageOp::Tombstone {
            id: id(message),
            evidence: DeletionEvidence::History { at: HistoryId(11) },
        }],
        labels: Vec::new(),
        account_ops: Vec::new(),
        job_plan: plan,
        jobs: Vec::new(),
        intents: Vec::new(),
        drafts: Vec::new(),
    };
    let outcome = runner.store().apply(&commit).await.expect("apply");
    assert!(matches!(outcome, CommitOutcome::Applied { .. }));
}

#[tokio::test]
async fn a_refresh_racing_a_delete_changes_nothing() {
    let runner = imported_before_facts(FakeGmail::plain(1), Blobs::holding("m00")).await;
    let user = account();
    let refresh = runner.refresh_facts(&user, None, 100, Duration::from_secs(30));
    let delete = async {
        let (_, started, release) = runner.blobs().hold.as_ref().expect("held");
        started.notified().await;
        tombstone(&runner, "m00").await;
        release.notify_one();
    };
    let (done, ()) = tokio::join!(refresh, delete);
    let done = done.expect("no error");
    assert_eq!((done.examined, done.refreshed), (1, 0));
    assert!(
        runner
            .store()
            .messages(&account(), &[id("m00")])
            .await
            .expect("messages")
            .is_empty(),
        "no row"
    );
    let ChangeFeed::Page(page) = runner
        .store()
        .changes(&account(), StateSeq(0).into(), 1_000)
        .await
        .expect("changes")
    else {
        panic!("the feed is not compacted");
    };
    assert!(
        !page
            .changes
            .iter()
            .any(|entry| matches!(entry.change, Change::FactsRefreshed { .. })),
        "no entry"
    );
}
