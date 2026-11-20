//! Reading parts of stored messages: ranged reads, refusals, and blob
//! stores with and without ranges. No Gmail: the blobs are written here.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Mutex;

use mailtide::EngineConfig;
use mailtide::gmail::PartRef;
use mailtide::ids::{AccountId, BlobRef, MessageId, Timestamp};
use mailtide::quota::Budget;
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::{BlobError, BlobStore, ObjectBlobs, RangeRead, blob_key};
use mailtide_io::parts::PartError;
use mailtide_io::quota_gate::LocalGate;
use mailtide_io::runner::{Runner, RunnerConfig, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_mime::PartRole;
use mailtide_store::memory::MemoryStore;

const MIXED: &[u8] = b"From: a@example.org\r\n\
    Content-Type: multipart/mixed; boundary=\"m\"\r\n\
    \r\n\
    --m\r\n\
    Content-Type: text/plain; charset=iso-8859-1\r\n\
    Content-Transfer-Encoding: quoted-printable\r\n\
    \r\n\
    Un caf=E9 ?\r\n\
    --m\r\n\
    Content-Type: application/octet-stream; name=\"blob.bin\"\r\n\
    Content-Transfer-Encoding: base64\r\n\
    \r\n\
    AAH//gAKDQ==\r\n\
    --m\r\n\
    Content-Type: text/csv; name=\"a.csv\"\r\n\
    Content-Disposition: attachment; filename=\"a.csv\"\r\n\
    Content-Transfer-Encoding: quoted-printable\r\n\
    \r\n\
    a,b=\r\n,c\r\n\
    --m\r\n\
    Content-Type: message/rfc822\r\n\
    \r\n\
    Subject: inner\r\n\
    Content-Type: multipart/mixed; boundary=\"i\"\r\n\
    \r\n\
    --i\r\n\
    Content-Type: text/plain\r\n\
    \r\n\
    inner\r\n\
    --i\r\n\
    Content-Type: image/gif; name=\"g.gif\"\r\n\
    Content-Transfer-Encoding: base64\r\n\
    \r\n\
    R0lGODlh\r\n\
    --i--\r\n\
    --m--\r\n";

/// A transport for runners that never call Gmail.
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

/// Blobs in memory that record every read.
#[derive(Debug)]
struct Counting {
    inner: ObjectBlobs,
    gets: Mutex<u32>,
    ranges: Mutex<Vec<Range<u64>>>,
}

impl Counting {
    fn new() -> Self {
        Self {
            inner: ObjectBlobs::memory(),
            gets: Mutex::new(0),
            ranges: Mutex::new(Vec::new()),
        }
    }

    fn gets(&self) -> u32 {
        *self.gets.lock().expect("lock")
    }

    fn ranges(&self) -> Vec<Range<u64>> {
        self.ranges.lock().expect("lock").clone()
    }
}

impl BlobStore for Counting {
    async fn put(&self, key: &BlobRef, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.inner.put(key, bytes).await
    }

    async fn get(&self, key: &BlobRef) -> Result<Vec<u8>, BlobError> {
        *self.gets.lock().expect("lock") += 1;
        self.inner.get(key).await
    }

    async fn get_range(&self, key: &BlobRef, range: Range<u64>) -> Result<RangeRead, BlobError> {
        self.ranges.lock().expect("lock").push(range.clone());
        self.inner.get_range(key, range).await
    }

    async fn delete(&self, key: &BlobRef) -> Result<(), BlobError> {
        self.inner.delete(key).await
    }

    async fn delete_account(&self, account: &AccountId) -> Result<u64, BlobError> {
        self.inner.delete_account(account).await
    }
}

/// A blob store that knows only whole objects: the trait's defaults serve
/// ranges.
#[derive(Debug, Default)]
struct WholeObjects(Mutex<BTreeMap<String, Vec<u8>>>);

impl BlobStore for WholeObjects {
    async fn put(&self, key: &BlobRef, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.0.lock().expect("lock").insert(key.0.clone(), bytes);
        Ok(())
    }

    async fn get(&self, key: &BlobRef) -> Result<Vec<u8>, BlobError> {
        self.0
            .lock()
            .expect("lock")
            .get(&key.0)
            .cloned()
            .ok_or(BlobError::NotFound)
    }

    async fn delete(&self, key: &BlobRef) -> Result<(), BlobError> {
        self.0.lock().expect("lock").remove(&key.0);
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

type TestRunner<B> = Runner<MemoryStore, B, NoGmail, LocalGate, StaticTokens>;

/// A runner whose blob store holds `raw` as message m1.
async fn runner<B: BlobStore>(blobs: B, raw: &[u8]) -> TestRunner<B> {
    blobs
        .put(&blob_key(&account(), &m1()), raw.to_vec())
        .await
        .expect("put");
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
        },
    )
}

/// The references of the summary of `raw`, in document order.
fn summary(raw: &[u8]) -> Vec<PartRef> {
    mailtide_mime::facts(raw)
        .parts
        .iter()
        .map(|part| part.part)
        .collect()
}

#[tokio::test]
async fn read_part_reads_only_the_parts_range() {
    let runner = runner(Counting::new(), MIXED).await;
    let blob = summary(MIXED)[0];
    let bytes = runner
        .read_part(&account(), &m1(), &blob, 1_000)
        .await
        .expect("read");
    assert_eq!(bytes, b"\x00\x01\xff\xfe\x00\x0a\x0d");
    assert_eq!(runner.blobs().ranges(), [blob.range()]);
    assert_eq!(
        blob.range().end - blob.range().start,
        12,
        "exactly the encoded length"
    );
    assert_eq!(runner.blobs().gets(), 0, "the object is never read whole");
}

#[tokio::test]
async fn read_part_matches_parse_message_for_every_attachment() {
    let runner = runner(ObjectBlobs::memory(), MIXED).await;
    let parsed = mailtide_mime::parse(MIXED).expect("parse");
    assert!(!parsed.attachments.is_empty());
    for (attachment, part) in parsed.attachments.iter().zip(parsed.attachment_parts) {
        let part = part.expect("a range in the raw message");
        let bytes = runner
            .read_part(&account(), &m1(), &part, u64::MAX)
            .await
            .expect("read");
        assert_eq!(bytes, attachment.bytes, "{part}");
    }
}

#[tokio::test]
async fn a_blob_store_without_ranges_still_serves_parts() {
    let runner = runner(WholeObjects::default(), MIXED).await;
    for part in summary(MIXED) {
        let whole = mailtide_mime::parse(MIXED).expect("parse");
        let expected = whole
            .attachments
            .iter()
            .zip(&whole.attachment_parts)
            .find(|(_, at)| **at == Some(part))
            .map(|(attachment, _)| attachment.bytes.clone());
        let bytes = runner
            .read_part(&account(), &m1(), &part, u64::MAX)
            .await
            .expect("read");
        assert_eq!(Some(bytes), expected);
    }
    let outside = PartRef {
        offset: 10_000,
        ..summary(MIXED)[0]
    };
    assert!(matches!(
        runner
            .read_part(&account(), &m1(), &outside, u64::MAX)
            .await,
        Err(PartError::InvalidPart(_))
    ));
}

#[tokio::test]
async fn parts_lists_the_parts_of_an_attached_message_with_their_container() {
    let runner = runner(Counting::new(), MIXED).await;
    let parts = runner.parts(&account(), &m1()).await.expect("parts");
    assert_eq!(runner.blobs().gets(), 1, "the blob is read once");
    let listed: Vec<(PartRole, &str, u32)> = parts
        .iter()
        .map(|info| (info.role, info.content_type.as_str(), info.depth))
        .collect();
    assert_eq!(
        listed,
        [
            (PartRole::Text, "text/plain", 0),
            (PartRole::Attached, "application/octet-stream", 0),
            (PartRole::Attached, "text/csv", 0),
            (PartRole::Attached, "message/rfc822", 0),
            (PartRole::Text, "text/plain", 1),
            (PartRole::Attached, "image/gif", 1),
        ]
    );
    let container = parts[3].part;
    assert!(container.is_some());
    assert_eq!(parts[4].container, container);
    assert_eq!(parts[5].container, container);
    let gif = parts[5].part.expect("in the raw message");
    let bytes = runner
        .read_part(&account(), &m1(), &gif, 100)
        .await
        .expect("read");
    assert_eq!(bytes, b"GIF89a");
}
