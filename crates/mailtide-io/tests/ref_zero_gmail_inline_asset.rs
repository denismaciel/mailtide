//! Tests ported by name from Zero's `gmail-inline-asset.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890), as `features/message-facts.toml`
//! maps them. Zero downloads an inline image with `attachments.get` and
//! tells a missing attachment (no image, never a missing thread) from a
//! failure to retry. We read the part from the stored blob: the missing thing
//! can only be the blob, and a read never writes to the mirror.

use std::collections::BTreeMap;
use std::ops::Range;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::ids::{AccountId, BlobRef, MessageId, Timestamp};
use mailtide::quota::Budget;
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::{BlobError, BlobStore, ObjectBlobs, RangeRead, blob_key};
use mailtide_io::parts::PartError;
use mailtide_io::quota_gate::LocalGate;
use mailtide_io::runner::{Runner, RunnerConfig, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::memory::MemoryStore;
use serde_json::json;

const RELATED: &[u8] = b"Subject: chart\r\n\
    Content-Type: multipart/related; boundary=\"r\"\r\n\
    \r\n\
    --r\r\n\
    Content-Type: text/html\r\n\
    \r\n\
    <img src=\"cid:chart@x\">\r\n\
    --r\r\n\
    Content-Type: image/png\r\n\
    Content-ID: <chart@x>\r\n\
    Content-Transfer-Encoding: base64\r\n\
    \r\n\
    aW1hZ2UtYmFzZTY0\r\n\
    --r--\r\n";

/// One message, m1, in the inbox.
struct FakeGmail;

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
        let raw = request
            .query
            .iter()
            .any(|(k, v)| k == "format" && v == "raw");
        let listing = request.query.iter().any(|(k, _)| k == "labelIds");
        Ok(
            match request.path.trim_start_matches("/gmail/v1/users/me/") {
                "profile" => ok(&json!({"emailAddress": "user@example.com", "historyId": "10"})),
                "labels" => {
                    ok(&json!({"labels": [{"id": "INBOX", "name": "INBOX", "type": "system"}]}))
                }
                "history" => ok(&json!({"historyId": "10"})),
                // A reconciliation ends by listing the drafts.
                "drafts" => ok(&json!({"resultSizeEstimate": 0})),
                "messages" if listing => ok(&json!({})),
                "messages" => ok(&json!({"messages": [{"id": "m1", "threadId": "t1"}]})),
                "messages/m1" => {
                    let mut body = json!({
                        "id": "m1", "threadId": "t1", "labelIds": ["INBOX"], "historyId": "10",
                        "internalDate": "1700000000000",
                    });
                    if raw {
                        body["raw"] = json!(URL_SAFE_NO_PAD.encode(RELATED));
                    }
                    ok(&body)
                }
                other => panic!("unexpected request {other}"),
            },
        )
    }
}

/// A blob store whose reads fail as `failure` says.
struct Failing(String);

impl BlobStore for Failing {
    async fn put(&self, _key: &BlobRef, _bytes: Vec<u8>) -> Result<(), BlobError> {
        Ok(())
    }

    async fn get(&self, _key: &BlobRef) -> Result<Vec<u8>, BlobError> {
        Err(BlobError::Unavailable(self.0.clone()))
    }

    async fn get_range(&self, _key: &BlobRef, _range: Range<u64>) -> Result<RangeRead, BlobError> {
        Err(BlobError::Unavailable(self.0.clone()))
    }

    async fn delete(&self, _key: &BlobRef) -> Result<(), BlobError> {
        Ok(())
    }

    async fn delete_account(&self, _account: &AccountId) -> Result<u64, BlobError> {
        Ok(0)
    }
}

fn account() -> AccountId {
    AccountId::parse("user@example.com").expect("account")
}

fn m1() -> MessageId {
    MessageId("m1".into())
}

fn runner<B: BlobStore>(blobs: B) -> Runner<MemoryStore, B, FakeGmail, LocalGate, StaticTokens> {
    Runner::new(
        MemoryStore::tracking(&[account()]),
        blobs,
        FakeGmail,
        LocalGate::new(Budget::GMAIL_DEFAULT),
        StaticTokens(BTreeMap::from([(
            account(),
            AccessToken {
                secret: "test".into(),
                expires_at: Timestamp(i64::MAX),
            },
        )])),
        RunnerConfig {
            engine: EngineConfig::default(),
            access: mailtide_io::runner::Access::ReadWrite,
            watch: WatchMode::Disabled,
            log_accounts: mailtide_io::runner::LogAccounts::default(),
            writes: mailtide_io::runner::WriteSettings::default(),
        },
    )
}

fn image() -> mailtide::gmail::PartRef {
    let facts = mailtide_mime::facts(RELATED);
    assert!(facts.parts[0].is_inline());
    facts.parts[0].part
}

#[tokio::test]
async fn an_inline_image_is_read_by_its_reference() {
    let runner = runner(ObjectBlobs::memory());
    runner
        .blobs()
        .put(&blob_key(&account(), &m1()), RELATED.to_vec())
        .await
        .expect("put");
    let bytes = runner
        .read_part(&account(), &m1(), &image(), 1_000)
        .await
        .expect("read");
    assert_eq!(bytes, b"image-base64");
}

#[tokio::test]
async fn a_part_whose_blob_is_gone_is_not_found_and_the_message_row_is_untouched() {
    let runner = runner(ObjectBlobs::memory());
    runner
        .run(&account(), Duration::from_secs(30))
        .await
        .expect("run");
    let before = runner.store().mirror(&account()).expect("mirror");
    assert!(before.messages.contains_key(&m1()), "imported");
    runner
        .blobs()
        .delete(&blob_key(&account(), &m1()))
        .await
        .expect("delete");
    assert_eq!(
        runner.read_part(&account(), &m1(), &image(), 1_000).await,
        Err(PartError::BlobNotFound)
    );
    assert_eq!(
        runner.parts(&account(), &m1()).await,
        Err(PartError::BlobNotFound)
    );
    assert_eq!(
        runner.store().mirror(&account()),
        Some(before),
        "missing content is not a missing message"
    );
}

#[tokio::test]
async fn a_blob_store_failure_during_a_part_read_is_unavailable_not_missing() {
    for failure in [
        "503 Service Unavailable",
        "429 Too Many Requests",
        "Connection reset",
    ] {
        let runner = runner(Failing(failure.to_owned()));
        let read = runner.read_part(&account(), &m1(), &image(), 1_000).await;
        assert_eq!(read, Err(PartError::BlobUnavailable(failure.to_owned())));
        assert_eq!(
            runner.parts(&account(), &m1()).await,
            Err(PartError::BlobUnavailable(failure.to_owned()))
        );
    }
}
