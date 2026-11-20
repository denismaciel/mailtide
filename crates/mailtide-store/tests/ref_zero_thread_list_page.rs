//! Tests ported by name from Zero's `thread-list-page.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890). Plan: `features/threads.toml`.
//!
//! Zero builds a list page from thread ids and one batch read of summaries.
//! The list (membership, order, cursor) is the host's here; what mailtide
//! keeps is the batch read, `Store::threads`, run on the memory store and
//! `SQLite`.

use mailtide::commit::{Commit, CommitOutcome, JobPlan, MessageOp};
use mailtide::gmail::{HeaderFacts, MessageObservation};
use mailtide::ids::{
    AccountId, BlobRef, CommitId, ENGINE_VERSION, HistoryId, MessageId, ThreadId, Timestamp,
};
use mailtide::labels::set;
use mailtide::quota::Priority;
use mailtide_store::Store;
use mailtide_store::memory::MemoryStore;
use mailtide_store::sqlite::SqliteStore;
use sqlx::Connection as _;

fn account() -> AccountId {
    AccountId("zero@example.com".into())
}

fn import(thread: &str, id: &str, date: i64) -> MessageOp {
    MessageOp::MarkImported {
        observation: MessageObservation {
            id: MessageId(id.into()),
            thread_id: ThreadId(thread.into()),
            labels: set(["INBOX"]),
            checkpoint: HistoryId(1),
        },
        blob: BlobRef(format!("{id}.eml")),
        size: 100,
        internal_date: Timestamp(date),
        headers: Box::new(HeaderFacts {
            from: Some("ann@example.com".into()),
            subject: Some(format!("Subject {id}")),
            ..HeaderFacts::default()
        }),
    }
}

/// Track the account and commit `messages` as one engine would.
async fn seed<S: Store>(store: &S, messages: Vec<MessageOp>) {
    store.track(&account(), 1).await.expect("track");
    let snapshot = store
        .claim(&account(), ENGINE_VERSION)
        .await
        .expect("claim");
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
        messages,
        labels: Vec::new(),
        account_ops: Vec::new(),
        job_plan: JobPlan {
            priority: Priority::Live,
            import_due_at: Timestamp(0),
            draft_import_due_at: Timestamp(0),
            verify_due_at: Timestamp(0),
            cleanup_due_at: Timestamp(0),
            deliver_due_at: Timestamp(0),
            confirm_due_at: Timestamp(0),
        },
        jobs: Vec::new(),
        intents: Vec::new(),
    };
    assert!(matches!(
        store.apply(&commit).await.expect("apply"),
        CommitOutcome::Applied { .. }
    ));
}

async fn sqlite() -> SqliteStore {
    let store = SqliteStore::connect("sqlite::memory:")
        .await
        .expect("store");
    store.migrate().await.expect("migrate");
    store
}

async fn one_call_in_order<S: Store>(store: &S) {
    seed(
        store,
        vec![
            import("a", "m1", 10),
            import("a", "m2", 20),
            import("c", "m3", 5),
        ],
    )
    .await;
    // The host's page: a, then b (not mirrored), then c.
    let page = [
        ThreadId("a".into()),
        ThreadId("b".into()),
        ThreadId("c".into()),
    ];
    let read = store.threads(&account(), &page).await.expect("threads");
    let ids: Vec<&str> = read.iter().map(|t| t.id.0.as_str()).collect();
    assert_eq!(ids, ["a", "c"]);
    let a = &read[0];
    assert_eq!(a.latest, Some(MessageId("m2".into())));
    assert_eq!(a.message_count, 2);
    // Envelopes only: the body stays in blob storage.
    let latest = a.messages[1].content.as_ref().expect("an envelope");
    assert_eq!(latest.headers.subject.as_deref(), Some("Subject m2"));
    assert_eq!(latest.blob, BlobRef("m2.eml".into()));
}

#[tokio::test]
async fn threads_come_back_from_one_call_in_the_order_asked() {
    one_call_in_order(&MemoryStore::new()).await;
    one_call_in_order(&sqlite().await).await;
}

#[tokio::test]
async fn no_thread_ids_reads_nothing() {
    let memory = MemoryStore::new();
    seed(&memory, vec![import("a", "m1", 10)]).await;
    assert!(
        memory
            .threads(&account(), &[])
            .await
            .expect("threads")
            .is_empty()
    );
    // A store that has checked its schema once, whose message table is then
    // dropped behind its back, fails any read that queries it: so an empty
    // answer proves nothing was read.
    let path =
        std::env::temp_dir().join(format!("mailtide-no-thread-ids-{}.db", std::process::id()));
    let url = format!("sqlite:{}?mode=rwc", path.display());
    let store = SqliteStore::connect(&url).await.expect("store");
    store.migrate().await.expect("migrate");
    seed(&store, vec![import("a", "m1", 10)]).await;
    assert_eq!(
        store
            .threads(&account(), &[ThreadId("a".into())])
            .await
            .expect("threads")
            .len(),
        1
    );
    let mut other = sqlx::sqlite::SqliteConnection::connect(&url)
        .await
        .expect("a second connection");
    sqlx::query("DROP TABLE mailtide_messages")
        .execute(&mut other)
        .await
        .expect("drop");
    other.close().await.expect("close");
    assert!(
        store
            .threads(&account(), &[])
            .await
            .expect("no query")
            .is_empty()
    );
    // The empty id is never a thread either, so it reads nothing too.
    assert!(
        store
            .threads(&account(), &[ThreadId(String::new())])
            .await
            .expect("no query")
            .is_empty()
    );
    assert!(
        store
            .threads(&account(), &[ThreadId("a".into())])
            .await
            .is_err(),
        "a real read queries the database"
    );
    std::fs::remove_file(&path).expect("remove the database");
}
