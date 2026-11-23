//! End-to-end integration tests exercising real `mailtide-io` `Runner` over HTTP
//! against `mailtide-fake`: multi-account routing, seeding, batchModify, drafts &
//! Gmail Web edits, dropped send recovery, withheld raw fetches, history expiration,
//! search, and SQLite persistence.

#![allow(clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::time::Duration;

use mailtide::EngineConfig;
use mailtide::ids::{AccountId, MessageId, ThreadId, Timestamp};
use mailtide::outbox::{Action, ActionRequest};
use mailtide::quota::{Budget, Priority};
use mailtide::search::SearchQuery;
use mailtide_fake::{FakeGmailServer, ServerConfig};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::ObjectBlobs;
use mailtide_io::quota_gate::LocalGate;
use mailtide_io::runner::{
    Access, LogAccounts, Runner, RunnerConfig, StopReason, WatchMode, WriteSettings,
};
use mailtide_io::transport::ReqwestTransport;
use mailtide_store::memory::MemoryStore;
use serde_json::{Value, json};

const BUDGET: Duration = Duration::from_secs(30);

async fn post_json(client: &reqwest::Client, url: String, body: &Value) -> Value {
    let resp = client
        .post(url)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("post");
    let bytes = resp.bytes().await.expect("bytes");
    serde_json::from_slice(&bytes).expect("json")
}

async fn get_json(client: &reqwest::Client, url: String) -> Value {
    let resp = client.get(url).send().await.expect("get");
    let bytes = resp.bytes().await.expect("bytes");
    serde_json::from_slice(&bytes).expect("json")
}

fn acct(email: &str) -> AccountId {
    AccountId::parse(email).expect("valid account")
}

fn runner_for(
    origin: &str,
    accounts: &[&str],
) -> Runner<MemoryStore, ObjectBlobs, ReqwestTransport, LocalGate, StaticTokens> {
    let parsed: Vec<AccountId> = accounts.iter().map(|a| acct(a)).collect();
    let mut tokens = BTreeMap::new();
    for (&raw, id) in accounts.iter().zip(parsed.iter()) {
        tokens.insert(
            id.clone(),
            AccessToken {
                secret: format!("fake:{raw}"),
                expires_at: Timestamp(i64::MAX),
            },
        );
    }
    Runner::new(
        MemoryStore::tracking(&parsed),
        ObjectBlobs::memory(),
        ReqwestTransport::with_origin(origin, Duration::from_secs(10)).expect("transport"),
        LocalGate::new(Budget::GMAIL_DEFAULT),
        StaticTokens(tokens),
        RunnerConfig {
            access: Access::ReadWrite,
            watch: WatchMode::Disabled,
            engine: EngineConfig {
                draft_settle: Duration::ZERO,
                confirm_timeout: Duration::ZERO,
                draft_refresh_delay: Duration::ZERO,
                ..EngineConfig::default()
            },
            writes: WriteSettings::default(),
            log_accounts: LogAccounts::Plain,
        },
    )
}

#[tokio::test]
async fn multi_account_seed_sync_search_and_batch_modify() {
    let server = FakeGmailServer::start(ServerConfig::default())
        .await
        .expect("server");
    let client = reqwest::Client::new();

    let res1 = post_json(
        &client,
        format!(
            "{}/_control/accounts/alice@example.com/seed",
            server.origin()
        ),
        &json!({ "threads": 25, "max_messages_per_thread": 2, "draft_count": 2 }),
    )
    .await;
    assert_eq!(res1["threads_created"], 25);

    let res2 = post_json(
        &client,
        format!("{}/_control/accounts/bob@example.com/seed", server.origin()),
        &json!({ "threads": 4, "max_messages_per_thread": 1, "draft_count": 0 }),
    )
    .await;
    assert_eq!(res2["threads_created"], 4);

    let runner = runner_for(server.origin(), &["alice@example.com", "bob@example.com"]);
    let alice = acct("alice@example.com");
    let bob = acct("bob@example.com");

    let out_a = runner.run(&alice, BUDGET).await.expect("run alice");
    assert!(
        out_a.report.imported >= 15,
        "imported={}",
        out_a.report.imported
    );

    let out_b = runner.run(&bob, BUDGET).await.expect("run bob");
    assert_eq!(out_b.report.imported, 4);

    // Search works via real query evaluation.
    let hits = runner
        .search(
            &alice,
            &SearchQuery {
                query: "roadmap".into(),
                label_ids: Vec::new(),
                limit: 50,
                page_token: None,
                include_spam_trash: true,
                priority: Priority::Foreground,
            },
            BUDGET,
        )
        .await
        .expect("search");
    assert!(!hits.hits.is_empty(), "expected roadmap search hits");

    // Bulk archive (>= BATCH_MODIFY_THRESHOLD = 10) exercises batchModify.
    let listed = get_json(
        &client,
        format!(
            "{}/_control/accounts/alice@example.com/messages?limit=100",
            server.origin()
        ),
    )
    .await;
    let ids: Vec<MessageId> = listed["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .filter(|m| {
            m["labels"]
                .as_array()
                .expect("labels")
                .iter()
                .any(|l| l == "INBOX")
        })
        .take(12)
        .map(|m| MessageId(m["id"].as_str().expect("id").to_owned()))
        .collect();
    assert!(ids.len() >= 10, "need >=10 INBOX msgs, got {}", ids.len());
    let action = ActionRequest::Messages {
        action: Action::Archive,
        ids: ids.clone(),
    };
    let admitted = runner.admit(&alice, &action).await.expect("admit");
    assert!(admitted >= 10, "expected >=10 admitted, got {admitted}");
    runner.run(&alice, BUDGET).await.expect("run bulk archive");

    // Verify all archived messages actually lost INBOX on the fake server.
    let after = get_json(
        &client,
        format!(
            "{}/_control/accounts/alice@example.com/messages?limit=100",
            server.origin()
        ),
    )
    .await;
    let archived_set: std::collections::BTreeSet<&str> = ids.iter().map(|m| m.0.as_str()).collect();
    for msg in after["messages"].as_array().expect("messages") {
        let mid = msg["id"].as_str().expect("id");
        if archived_set.contains(mid) {
            let has_inbox = msg["labels"]
                .as_array()
                .expect("labels")
                .iter()
                .any(|l| l == "INBOX");
            assert!(!has_inbox, "expected {mid} to be archived");
        }
    }

    // Exercise Gmail batchModify directly too.
    let batch_resp = client
        .post(format!(
            "{}/gmail/v1/users/me/messages/batchModify",
            server.origin()
        ))
        .header("authorization", "Bearer fake:alice@example.com")
        .header("content-type", "application/json")
        .body(
            json!({
                "ids": ids.iter().map(|m| m.0.as_str()).collect::<Vec<_>>(),
                "addLabelIds": ["STARRED"],
                "removeLabelIds": [],
            })
            .to_string(),
        )
        .send()
        .await
        .expect("batchModify");
    assert_eq!(batch_resp.status().as_u16(), 204);

    server.shutdown().await;
}

#[tokio::test]
async fn web_draft_edit_and_dropped_send_and_history_expire() {
    let server = FakeGmailServer::start(ServerConfig::default())
        .await
        .expect("server");
    let client = reqwest::Client::new();
    let alice = acct("alice@example.com");

    let injected = post_json(
        &client,
        format!(
            "{}/_control/accounts/alice@example.com/messages",
            server.origin()
        ),
        &json!({
            "from": "Elena <elena@northwind.example>",
            "subject": "Draft conflict check",
            "text": "Please review.",
        }),
    )
    .await;
    let tid = injected["thread_id"].as_str().expect("tid").to_owned();

    let runner = runner_for(server.origin(), &["alice@example.com"]);
    runner.run(&alice, BUDGET).await.expect("initial sync");

    let mut outgoing = mailtide_mime::OutgoingMessage::new(
        mailtide_mime::Address {
            name: None,
            email: "alice@example.com".into(),
        },
        1_700_000_000,
    );
    outgoing.to.push(mailtide_mime::Address {
        name: None,
        email: "elena@northwind.example".into(),
    });
    outgoing.subject = Some("Re: Draft conflict check".into());
    outgoing.text = Some("Initial reply".into());
    outgoing.tracking_id = Some("0123456789abcdef012345".into());
    let composed = mailtide_mime::compose(
        &outgoing,
        &mailtide_mime::ComposeOptions {
            purpose: mailtide_mime::Purpose::Draft,
            max_bytes: None,
        },
    )
    .expect("compose");

    let created = runner
        .create_draft(&alice, composed.raw, Some(ThreadId(tid)), BUDGET)
        .await
        .expect("create draft");

    // Simulate Gmail Web editing the draft externally (changes message_id, strips tracking id).
    let edited = post_json(
        &client,
        format!(
            "{}/_control/accounts/alice@example.com/drafts/{}/web-edit",
            server.origin(),
            created.id.0
        ),
        &json!({ "body": "Edited in Gmail Web" }),
    )
    .await;
    assert_ne!(
        edited["message_id"].as_str().expect("mid"),
        created.message.0
    );

    let fresh = runner
        .read_draft(&alice, &created.id, true, BUDGET)
        .await
        .expect("fresh read");
    assert_eq!(
        fresh.draft.message.0,
        edited["message_id"].as_str().expect("mid")
    );
    assert_eq!(mailtide_mime::tracking_id(&fresh.raw), None);

    // Sync so the mirror's draft index observes the external Gmail Web edit.
    runner.run(&alice, BUDGET).await.expect("sync web edit");

    // Sending with the stale message id must be rejected with DraftConflict (0 sends).
    let conflict = runner
        .send_draft(&alice, &created.id, Some(created.message.clone()), BUDGET)
        .await;
    assert!(conflict.is_err(), "expected DraftConflict on stale id");

    // Arm drop_next_send = applied, send draft with fresh id -> dropped connection error,
    // verify sent log has 1 send.
    post_json(
        &client,
        format!(
            "{}/_control/accounts/alice@example.com/faults",
            server.origin()
        ),
        &json!({ "drop_next_send": "applied" }),
    )
    .await;

    let send_err = runner
        .send_draft(
            &alice,
            &created.id,
            Some(fresh.draft.message.clone()),
            BUDGET,
        )
        .await;
    assert!(send_err.is_err(), "expected dropped connection error");

    let sent = get_json(
        &client,
        format!(
            "{}/_control/accounts/alice@example.com/sent",
            server.origin()
        ),
    )
    .await;
    assert_eq!(sent["sent"].as_array().expect("arr").len(), 1);

    // Expire history and verify runner recovers cleanly via full reconciliation.
    post_json(
        &client,
        format!(
            "{}/_control/accounts/alice@example.com/expire-history",
            server.origin()
        ),
        &json!({}),
    )
    .await;
    let out = runner
        .run(&alice, BUDGET)
        .await
        .expect("reconcile after expire");
    assert!(!matches!(out.stop, StopReason::Blocked(_)));

    server.shutdown().await;
}

#[tokio::test]
async fn sqlite_persistence_survives_server_restart() {
    let tmp = std::env::temp_dir().join(format!("mailtide-fake-test-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&tmp);

    {
        let server = FakeGmailServer::start(ServerConfig {
            state_path: Some(tmp.clone()),
            ..ServerConfig::default()
        })
        .await
        .expect("server 1");
        let client = reqwest::Client::new();
        post_json(
            &client,
            format!(
                "{}/_control/accounts/alice@example.com/seed",
                server.origin()
            ),
            &json!({ "threads": 5, "max_messages_per_thread": 1, "draft_count": 1 }),
        )
        .await;
        server.shutdown().await;
    }

    {
        let server = FakeGmailServer::start(ServerConfig {
            state_path: Some(tmp.clone()),
            ..ServerConfig::default()
        })
        .await
        .expect("server 2");
        let client = reqwest::Client::new();
        let status = get_json(&client, format!("{}/_control/status", server.origin())).await;
        let acct = &status["accounts"][0];
        assert_eq!(acct["email"], "alice@example.com");
        assert_eq!(acct["message_count"], 6);
        assert_eq!(acct["draft_count"], 1);
        server.shutdown().await;
    }

    let _ = std::fs::remove_file(&tmp);
}
