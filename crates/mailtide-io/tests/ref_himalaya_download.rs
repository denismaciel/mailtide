//! Tests ported by name from himalaya's `src/shared/attachment/download.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615), as
//! `features/message-facts.toml` maps them: a part's bytes are its
//! transfer-decoded body, of the size the listing reports, under a cap
//! checked before any work. Writing files is himalaya's, not ours.

use std::collections::BTreeMap;

use mailtide::EngineConfig;
use mailtide::ids::{AccountId, MessageId, Timestamp};
use mailtide::quota::Budget;
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::{BlobStore, ObjectBlobs, blob_key};
use mailtide_io::parts::PartError;
use mailtide_io::quota_gate::LocalGate;
use mailtide_io::runner::{Runner, RunnerConfig, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::memory::MemoryStore;

const MIXED: &[u8] = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\
    \r\n\
    --b\r\n\
    Content-Type: text/plain\r\n\
    \r\n\
    body\r\n\
    --b\r\n\
    Content-Type: application/octet-stream; name=\"blob.bin\"\r\n\
    Content-Transfer-Encoding: base64\r\n\
    \r\n\
    AAH//gAKDQ==\r\n\
    --b\r\n\
    Content-Type: text/calendar; charset=windows-1252; method=REQUEST\r\n\
    Content-Transfer-Encoding: quoted-printable\r\n\
    \r\n\
    SUMMARY:Caf=E9 =80 5\r\n\
    --b--\r\n";

struct NoGmail;

impl GmailTransport for NoGmail {
    async fn send(
        &self,
        _request: HttpRequest,
        _token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        panic!("part reads make no Gmail call")
    }
}

fn account() -> AccountId {
    AccountId::parse("user@example.com").expect("account")
}

fn m1() -> MessageId {
    MessageId("m1".into())
}

async fn runner(
    stored: bool,
) -> Runner<MemoryStore, ObjectBlobs, NoGmail, LocalGate, StaticTokens> {
    let blobs = ObjectBlobs::memory();
    if stored {
        blobs
            .put(&blob_key(&account(), &m1()), MIXED.to_vec())
            .await
            .expect("put");
    }
    Runner::new(
        MemoryStore::tracking(&[account()]),
        blobs,
        NoGmail,
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
            drafts: mailtide_io::drafts::DraftSettings::default(),
        },
    )
}

#[tokio::test]
async fn read_part_returns_the_transfer_decoded_bytes_of_any_leaf() {
    let runner = runner(true).await;
    let parts = runner.parts(&account(), &m1()).await.expect("parts");
    let mut bytes = Vec::new();
    for info in &parts {
        let part = info.part.expect("a range");
        bytes.push(
            runner
                .read_part(&account(), &m1(), &part, u64::MAX)
                .await
                .expect("read"),
        );
    }
    // A body is a part like any other; the container has no reference.
    assert_eq!(
        bytes,
        [
            b"body".to_vec(),
            b"\x00\x01\xff\xfe\x00\x0a\x0d".to_vec(),
            b"SUMMARY:Caf\xe9 \x80 5".to_vec(),
        ]
    );
    assert_eq!(parts[1].filename.as_deref(), Some("blob.bin"));
    assert_eq!(parts[2].filename, None);
}

#[tokio::test]
async fn read_part_yields_exactly_the_size_the_summary_reports() {
    let runner = runner(true).await;
    let summary = mailtide_mime::facts(MIXED).parts;
    assert_eq!(summary.len(), 2);
    for entry in &summary {
        let read = runner
            .read_part(&account(), &m1(), &entry.part, u64::MAX)
            .await
            .expect("read");
        assert_eq!(u64::try_from(read.len()).ok(), Some(entry.part.size));
    }
    for info in runner.parts(&account(), &m1()).await.expect("parts") {
        let part = info.part.expect("a range");
        let read = runner
            .read_part(&account(), &m1(), &part, u64::MAX)
            .await
            .expect("read");
        assert_eq!(u64::try_from(read.len()).ok(), Some(info.size));
    }
}

#[tokio::test]
async fn a_part_over_max_bytes_is_refused_before_the_blob_is_read() {
    let blob = mailtide_mime::facts(MIXED).parts[0].part;
    assert_eq!(blob.size, 7);
    // No blob is stored: a refusal that read anything would be BlobNotFound.
    let empty = runner(false).await;
    assert_eq!(
        empty.read_part(&account(), &m1(), &blob, 6).await,
        Err(PartError::TooLarge { size: 7, limit: 6 })
    );
    let runner = runner(true).await;
    let at_cap = runner
        .read_part(&account(), &m1(), &blob, 7)
        .await
        .expect("read");
    assert_eq!(at_cap, b"\x00\x01\xff\xfe\x00\x0a\x0d");
}
