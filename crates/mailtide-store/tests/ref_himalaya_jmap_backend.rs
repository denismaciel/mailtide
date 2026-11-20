//! Tests ported by name from himalaya's `src/jmap/backend.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615).
//! Plan: `features/threads.toml` (this suite is shared with other plans).
//!
//! JMAP's `Thread/get` answers the ids it knows and names the others in
//! `notFound`. `Store::threads` follows `Store::messages`: unknown ids are
//! left out, so the caller compares ids; the known ones are answered either
//! way.

use mailtide::commit::{Commit, CommitOutcome, JobPlan, MessageOp};
use mailtide::gmail::MessageObservation;
use mailtide::ids::{
    AccountId, CommitId, ENGINE_VERSION, HistoryId, MessageId, ThreadId, Timestamp,
};
use mailtide::labels::set;
use mailtide::quota::Priority;
use mailtide_store::Store;
use mailtide_store::memory::MemoryStore;
use mailtide_store::sqlite::SqliteStore;

fn account() -> AccountId {
    AccountId("jmap@example.org".into())
}

async fn thread_get<S: Store>(store: &S) {
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
        messages: vec![MessageOp::Observe(MessageObservation {
            id: MessageId("E1".into()),
            thread_id: ThreadId("T1".into()),
            labels: set(["INBOX"]),
            checkpoint: HistoryId(1),
        })],
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
            draft_refresh_due_at: Timestamp(0),
        },
        jobs: Vec::new(),
        intents: Vec::new(),
        drafts: Vec::new(),
    };
    assert!(matches!(
        store.apply(&commit).await.expect("apply"),
        CommitOutcome::Applied { .. }
    ));
    let asked = [ThreadId("T1".into()), ThreadId("Tbogus".into())];
    let found = store.threads(&account(), &asked).await.expect("threads");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, ThreadId("T1".into()));
    let members: Vec<&str> = found[0].messages.iter().map(|m| m.id.0.as_str()).collect();
    assert_eq!(members, ["E1"]);
    let not_found: Vec<&ThreadId> = asked
        .iter()
        .filter(|id| !found.iter().any(|thread| &thread.id == *id))
        .collect();
    assert_eq!(not_found, [&ThreadId("Tbogus".into())]);
}

#[tokio::test]
async fn unknown_thread_ids_are_left_out_and_the_others_answered() {
    thread_get(&MemoryStore::new()).await;
    let sqlite = SqliteStore::connect("sqlite::memory:")
        .await
        .expect("store");
    sqlite.migrate().await.expect("migrate");
    thread_get(&sqlite).await;
}
