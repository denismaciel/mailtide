//! Tests ported by name from himalaya's `tests/message_parse.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615), as
//! `features/message-facts.toml` maps them. Himalaya's `message parse`
//! reads a part by id from a file; we read it by reference from the stored
//! blob, and an import never refuses a message for its shape.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::gmail::{MessageFacts, PartRef};
use mailtide::ids::{AccountId, MessageId, Timestamp};
use mailtide::quota::Budget;
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::ObjectBlobs;
use mailtide_io::parts::PartError;
use mailtide_io::quota_gate::LocalGate;
use mailtide_io::runner::{Runner, RunnerConfig, StopReason, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_mime::PartRole;
use mailtide_store::Store;
use mailtide_store::memory::MemoryStore;
use serde_json::json;

/// A message with a nested alternative, a Latin-1 body, a base64
/// attachment, an inline image and an attached message, its line endings
/// mixed as found in the wild.
const MIXED: &[u8] = b"From: =?utf-8?Q?Ren=C3=A9?= <rene@example.org>\r\n\
    To: a@example.org, Group: b@example.org, c@example.org;\r\n\
    Subject: =?iso-8859-1?Q?Caf=E9?=\r\n\
    Date: Tue, 06 Oct 2026 09:00:00 +0200\r\n\
    Message-ID: <1@example.org>\r\n\
    Content-Type: multipart/mixed; boundary=\"m\"\r\n\
    \r\n\
    --m\r\n\
    Content-Type: multipart/related; boundary=\"r\"\r\n\
    \r\n\
    --r\r\n\
    Content-Type: multipart/alternative; boundary=\"a\"\r\n\
    \r\n\
    --a\r\n\
    Content-Type: text/plain; charset=iso-8859-1\r\n\
    Content-Transfer-Encoding: quoted-printable\r\n\
    \r\n\
    Un caf=E9 ?\r\n\
    --a\r\n\
    Content-Type: text/html; charset=utf-8\r\n\
    \r\n\
    <p>Un caf\xc3\xa9 ?</p><img src=\"cid:logo@x\">\r\n\
    --a--\r\n\
    --r\r\n\
    Content-Type: image/png\r\n\
    Content-ID: <logo@x>\r\n\
    Content-Transfer-Encoding: base64\r\n\
    \r\n\
    iVBORw0KGgo=\r\n\
    --r--\r\n\
    --m\r\n\
    Content-Type: application/octet-stream\r\n\
    Content-Disposition: attachment; filename*=utf-8''r%C3%A9sum%C3%A9.bin\r\n\
    Content-Transfer-Encoding: base64\r\n\
    \r\n\
    AAH//gAKDQ==\r\n\
    --m\r\n\
    Content-Type: message/rfc822\n\
    \n\
    Subject: Inner\n\
    \n\
    inner body\n\
    --m--\r\n";

/// One message, m1, whose raw content is `.0`.
struct FakeGmail(Vec<u8>);

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
                        body["raw"] = json!(URL_SAFE_NO_PAD.encode(&self.0));
                    }
                    ok(&body)
                }
                other => panic!("unexpected request {other}"),
            },
        )
    }
}

fn account() -> AccountId {
    AccountId::parse("user@example.com").expect("account")
}

fn m1() -> MessageId {
    MessageId("m1".into())
}

type TestRunner = Runner<MemoryStore, ObjectBlobs, FakeGmail, LocalGate, StaticTokens>;

fn runner(raw: &[u8]) -> TestRunner {
    Runner::new(
        MemoryStore::tracking(&[account()]),
        ObjectBlobs::memory(),
        FakeGmail(raw.to_vec()),
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

/// A runner that imported `raw` as m1.
async fn imported(raw: &[u8]) -> TestRunner {
    let runner = runner(raw);
    let outcome = runner
        .run(&account(), Duration::from_secs(30))
        .await
        .expect("run");
    assert!(matches!(outcome.stop, StopReason::Idle { .. }));
    assert_eq!(outcome.report.imported, 1);
    runner
}

async fn read(runner: &TestRunner, part: &PartRef) -> Result<Vec<u8>, PartError> {
    runner.read_part(&account(), &m1(), part, u64::MAX).await
}

#[tokio::test]
async fn every_part_of_a_mixed_message_reads_back_as_its_decoded_bytes() {
    let runner = imported(MIXED).await;
    let parts = runner.parts(&account(), &m1()).await.expect("parts");
    let roles: Vec<(PartRole, &str)> = parts
        .iter()
        .map(|info| (info.role, info.content_type.as_str()))
        .collect();
    assert_eq!(
        roles,
        [
            (PartRole::Text, "text/plain"),
            (PartRole::Html, "text/html"),
            (PartRole::Inline, "image/png"),
            (PartRole::Attached, "application/octet-stream"),
            (PartRole::Attached, "message/rfc822"),
            // No Content-Type: reported as unreadable, read as text.
            (PartRole::Text, "application/octet-stream"),
        ]
    );
    let mut bytes = Vec::new();
    for info in &parts {
        bytes.push(
            read(&runner, &info.part.expect("a range"))
                .await
                .expect("read"),
        );
    }
    // A Latin-1 body keeps its charset; only the transfer encoding is undone.
    assert_eq!(bytes[0], b"Un caf\xe9 ?");
    assert_eq!(bytes[2], b"\x89PNG\r\n\x1a\n");
    assert_eq!(bytes[3], b"\x00\x01\xff\xfe\x00\x0a\x0d");
    assert_eq!(bytes[4], b"Subject: Inner\n\ninner body");
    assert_eq!(bytes[5], b"inner body");
    assert_eq!(parts[3].filename.as_deref(), Some("r\u{e9}sum\u{e9}.bin"));
    assert_eq!(parts[3].size, 7);
}

#[tokio::test]
async fn a_reference_outside_the_blob_or_for_another_length_is_refused() {
    let runner = imported(MIXED).await;
    let blob = mailtide_mime::facts(MIXED)
        .parts
        .iter()
        .find(|part| part.content_type == "application/octet-stream")
        .map(|part| part.part)
        .expect("the attachment");
    assert!(read(&runner, &blob).await.is_ok());
    let total = blob.total;
    for (wrong, why) in [
        (
            PartRef {
                offset: total + 10,
                total: total + 40,
                ..blob
            },
            "outside the blob",
        ),
        (
            PartRef {
                total: total + 1,
                ..blob
            },
            "for a blob of another length",
        ),
        (
            PartRef {
                offset: blob.offset + 4,
                ..blob
            },
            "bytes that do not decode to its size",
        ),
    ] {
        assert!(
            matches!(read(&runner, &wrong).await, Err(PartError::InvalidPart(_))),
            "{why}"
        );
    }
    assert!(PartRef::parse("4.9").is_err(), "no part has no reference");
}

#[tokio::test]
async fn an_empty_message_is_imported_with_empty_facts_and_no_parts() {
    for blank in [&b""[..], b" \r\n\t\n"] {
        let runner = imported(blank).await;
        let view = runner
            .store()
            .messages(&account(), &[m1()])
            .await
            .expect("messages")
            .remove(0);
        let content = view.content.expect("imported");
        assert_eq!(content.facts, Some(MessageFacts::empty()), "{blank:?}");
        let parts = runner.parts(&account(), &m1()).await.expect("parts");
        assert!(
            parts.iter().all(|info| info.role == PartRole::Text),
            "{blank:?}"
        );
    }
}

#[tokio::test]
async fn parts_of_a_message_past_the_limits_lists_what_was_parsed() {
    let mut raw = String::from("From: a@example.org\r\n");
    for n in 0..600 {
        let _ = write!(raw, "X-Line-{n}: {n}\r\n");
    }
    for level in 0..100 {
        let _ = write!(
            raw,
            "Content-Type: multipart/mixed; boundary=\"b{level}\"\r\n\r\n--b{level}\r\n"
        );
    }
    raw.push_str("Content-Type: text/plain\r\n\r\ndeep\r\n");
    let runner = imported(raw.as_bytes()).await;
    let parts = runner.parts(&account(), &m1()).await.expect("parts");
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].role, PartRole::Attached);
    let cut = read(&runner, &parts[0].part.expect("a range"))
        .await
        .expect("read");
    assert!(String::from_utf8_lossy(&cut).ends_with("deep\r\n"));
}
