//! Tests ported by name from Zero's `mail-asset-cache.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.
//!
//! Zero caches attachment bytes in R2 under hashed keys. Mailtide's
//! counterpart is the raw message blob: fetched once, written at a
//! deterministic key before the import is reported.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::ids::{AccountId, BlobRef, MessageId, Timestamp};
use mailtide::mirror::Content;
use mailtide::quota::Budget;
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::{BlobError, BlobStore, ObjectBlobs, blob_key};
use mailtide_io::quota_gate::LocalGate;
use mailtide_io::runner::{Runner, RunnerConfig, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::memory::MemoryStore;
use serde_json::json;

/// Runs a test body on a single-threaded runtime (plain `#[test]`, so
/// `scripts/zero_tests.py` counts it).
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}

const RAW: &str = "Subject: hi\r\n\r\npayload\r\n";

fn account() -> AccountId {
    AccountId::parse("user@example.com").expect("account")
}

fn m1() -> MessageId {
    MessageId("m1".into())
}

fn response(status: u16, body: &serde_json::Value) -> HttpResponse {
    HttpResponse {
        status,
        headers: Vec::new(),
        body: body.to_string().into_bytes(),
    }
}

/// One message in the inbox; raw fetches are counted and may fail.
#[derive(Debug, Default)]
struct FakeGmail {
    raw_fetches: Mutex<u32>,
    raw_status: Option<u16>,
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
        let ok = |body| response(200, &body);
        Ok(
            match request.path.trim_start_matches("/gmail/v1/users/me/") {
                "profile" => ok(json!({"emailAddress": "user@example.com", "historyId": "10"})),
                "labels" => {
                    ok(json!({"labels": [{"id": "INBOX", "name": "INBOX", "type": "system"}]}))
                }
                "history" => ok(json!({"historyId": "10"})),
                // A reconciliation ends by listing the drafts.
                "drafts" => ok(json!({"resultSizeEstimate": 0})),
                "messages" => match query.get("labelIds") {
                    None | Some(&"INBOX") => {
                        ok(json!({"messages": [{"id": "m1", "threadId": "t1"}]}))
                    }
                    Some(_) => ok(json!({})),
                },
                "messages/m1" => {
                    let mut body = json!({
                        "id": "m1", "threadId": "t1", "labelIds": ["INBOX"], "historyId": "10",
                        "internalDate": "1700000000000", "sizeEstimate": 40,
                    });
                    if query.get("format") == Some(&"raw") {
                        *self.raw_fetches.lock().expect("lock") += 1;
                        if let Some(status) = self.raw_status {
                            return Ok(response(
                                status,
                                &json!({"error": {"code": status, "message": "provider error"}}),
                            ));
                        }
                        body["raw"] = json!(URL_SAFE_NO_PAD.encode(RAW));
                    }
                    ok(body)
                }
                other => panic!("unexpected request {other}"),
            },
        )
    }
}

impl FakeGmail {
    fn raw_fetches(&self) -> u32 {
        *self.raw_fetches.lock().expect("lock")
    }
}

fn runner<B: BlobStore>(
    gmail: FakeGmail,
    blobs: B,
) -> Runner<MemoryStore, B, FakeGmail, LocalGate, StaticTokens> {
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
            drafts: mailtide_io::drafts::DraftSettings::default(),
        },
    )
}

fn imported<B: BlobStore>(
    runner: &Runner<MemoryStore, B, FakeGmail, LocalGate, StaticTokens>,
) -> bool {
    let mirror = runner.store().mirror(&account()).expect("mirror");
    matches!(mirror.messages[&m1()].content, Content::Imported(_))
}

/// A fresh directory for one test, under Cargo's per-target temp dir.
fn blob_dir(test: &str) -> String {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("zero_mail_asset_cache")
        .join(test);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    format!("file://{}", dir.display())
}

/// Zero: mail-asset-cache > immutable mailbox attachment cache > coalesces downloads and reuses R2 bytes across cache reconstruction
#[test]
fn immutable_mailbox_attachment_cache_coalesces_downloads_and_reuses_r2_bytes_across_cache_reconstruction()
 {
    block_on(async {
        let url = blob_dir("coalesces");
        let runner = runner(
            FakeGmail::default(),
            ObjectBlobs::from_url(&url).expect("blobs"),
        );
        for _ in 0..2 {
            runner
                .run(&account(), Duration::from_secs(30))
                .await
                .expect("run");
        }
        assert!(imported(&runner));
        assert_eq!(
            runner.transport().raw_fetches(),
            1,
            "content is fetched once"
        );
        // Concurrent writers of the same message land on one object.
        let key = blob_key(&account(), &m1());
        let blobs = runner.blobs();
        let (a, b, c) = tokio::join!(
            blobs.put(&key, RAW.into()),
            blobs.put(&key, RAW.into()),
            blobs.put(&key, RAW.into()),
        );
        a.and(b).and(c).expect("idempotent puts");
        // A new store over the same location reads the same bytes.
        let reopened = ObjectBlobs::from_url(&url).expect("blobs");
        assert_eq!(reopened.get(&key).await.expect("blob"), RAW.as_bytes());
    });
}

/// Zero: mail-asset-cache > immutable mailbox attachment cache > isolates mailboxes, message versions, attachment IDs and MIME metadata keys
#[test]
fn immutable_mailbox_attachment_cache_isolates_mailboxes_message_versions_attachment_ids_and_mime_metadata_keys()
 {
    block_on(async {
        // Mailtide stores one immutable raw blob per message: no versions,
        // attachments or MIME inventories to key separately.
        let a = AccountId::parse("a@example.com").expect("a");
        let b = AccountId::parse("b@example.com").expect("b");
        let keys = [
            blob_key(&a, &MessageId("m".into())),
            blob_key(&a, &MessageId("new-message".into())),
            blob_key(&b, &MessageId("m".into())),
        ];
        assert_eq!(
            keys[0],
            blob_key(
                &AccountId::parse("A@Example.com").expect("a"),
                &MessageId("m".into())
            ),
            "deterministic, and the same for the same mailbox however it is spelled"
        );
        assert_eq!(keys[0], BlobRef("a@example.com/m.eml".into()));
        let blobs = ObjectBlobs::memory();
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(keys.iter().filter(|k| *k == key).count(), 1, "{key:?}");
            blobs
                .put(key, vec![u8::try_from(i).expect("small")])
                .await
                .expect("put");
        }
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                blobs.get(key).await.expect("get"),
                vec![u8::try_from(i).expect("small")]
            );
        }
    });
}

/// Zero: mail-asset-cache > immutable mailbox attachment cache > does not cache provider errors or missing data and retries a failed download
#[test]
fn immutable_mailbox_attachment_cache_does_not_cache_provider_errors_or_missing_data_and_retries_a_failed_download()
 {
    block_on(async {
        // A 500 is retried after backoff within the run's budget; a 404
        // tombstones the message. Nothing is stored for either.
        for (status, retried) in [(500, true), (404, false)] {
            let runner = runner(
                FakeGmail {
                    raw_status: Some(status),
                    ..FakeGmail::default()
                },
                ObjectBlobs::memory(),
            );
            runner
                .run(&account(), Duration::from_secs(30))
                .await
                .expect("run");
            assert_eq!(runner.transport().raw_fetches() > 1, retried, "{status}");
            assert!(
                runner
                    .blobs()
                    .get(&blob_key(&account(), &m1()))
                    .await
                    .is_err(),
                "{status}: nothing stored"
            );
            let mirror = runner.store().mirror(&account()).expect("mirror");
            assert!(
                mirror
                    .messages
                    .get(&m1())
                    .is_none_or(|m| !matches!(m.content, Content::Imported(_))),
                "{status}: not reported as imported"
            );
        }
        // A missing key is an error, never empty content.
        assert!(
            ObjectBlobs::memory()
                .get(&blob_key(&account(), &m1()))
                .await
                .is_err()
        );
    });
}

/// Blob storage that is down.
#[derive(Debug)]
struct Unavailable;

impl BlobStore for Unavailable {
    async fn put(&self, _key: &BlobRef, _bytes: Vec<u8>) -> Result<(), BlobError> {
        Err(BlobError::Unavailable("R2 unavailable".into()))
    }

    async fn get(&self, _key: &BlobRef) -> Result<Vec<u8>, BlobError> {
        Err(BlobError::Unavailable("R2 unavailable".into()))
    }

    async fn delete(&self, _key: &BlobRef) -> Result<(), BlobError> {
        Err(BlobError::Unavailable("R2 unavailable".into()))
    }

    async fn delete_account(&self, _account: &AccountId) -> Result<u64, BlobError> {
        Err(BlobError::Unavailable("R2 unavailable".into()))
    }
}

/// Zero: mail-asset-cache > immutable mailbox attachment cache > returns provider data when R2 reads or writes fail
#[test]
fn immutable_mailbox_attachment_cache_returns_provider_data_when_r2_reads_or_writes_fail() {
    block_on(async {
        // Deliberately different from Zero: content is durable before it is
        // reported, so a failed write leaves the message pending (to retry)
        // instead of serving provider bytes; the run itself carries on.
        let runner = runner(FakeGmail::default(), Unavailable);
        runner
            .run(&account(), Duration::from_secs(30))
            .await
            .expect("a blob failure is not a run error");
        assert!(runner.transport().raw_fetches() >= 1);
        assert!(!imported(&runner));
    });
}

/// Zero: mail-asset-cache > immutable mailbox attachment cache > caches legitimate empty attachment bytes and empty MIME inventories
#[test]
fn immutable_mailbox_attachment_cache_caches_legitimate_empty_attachment_bytes_and_empty_mime_inventories()
 {
    block_on(async {
        let blobs = ObjectBlobs::memory();
        let key = blob_key(&account(), &m1());
        blobs.put(&key, Vec::new()).await.expect("put");
        assert_eq!(
            blobs.get(&key).await.expect("empty is stored"),
            Vec::<u8>::new()
        );
        blobs.delete(&key).await.expect("delete");
        blobs
            .delete(&key)
            .await
            .expect("deleting a missing key is fine");
        assert!(blobs.get(&key).await.is_err());
    });
}
