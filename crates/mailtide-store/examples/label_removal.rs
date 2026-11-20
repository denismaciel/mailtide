//! How long a label removal takes on a large mailbox:
//! `cargo run --release -p mailtide-store --example label_removal -- URL ROWS`
//! with a `sqlite::memory:` or Postgres URL (its tables are reset first).
//!
//! It mirrors `ROWS` messages carrying `Label_1` (half of them also
//! `Label_2` .. `Label_5`), then times `record_label(Remove Label_1)` (what
//! `delete_label` records) and a listing that lacks the other four labels.

use std::time::Instant;

use mailtide::commit::{Commit, JobPlan, LabelOp, MessageOp};
use mailtide::gmail::{Label, LabelKind, MessageObservation};
use mailtide::ids::{
    AccountId, BlobRef, CommitId, ENGINE_VERSION, HistoryId, LabelId, MessageId, ThreadId,
    Timestamp,
};
use mailtide::labels::LabelSet;
use mailtide::quota::Priority;
use mailtide_store::Store;
use mailtide_store::config::{DatabaseUrl, PoolConfig};
use mailtide_store::sql::SqlStore;

fn commit(account: &AccountId, epoch: mailtide::ids::Epoch, seq: u64) -> Commit {
    let now = Timestamp(0);
    Commit {
        id: CommitId { epoch, seq },
        account: account.clone(),
        epoch,
        engine_version: ENGINE_VERSION,
        cursor: None,
        claim: None,
        sync: None,
        messages: Vec::new(),
        labels: Vec::new(),
        account_ops: Vec::new(),
        job_plan: JobPlan {
            priority: Priority::Backfill,
            import_due_at: now,
            draft_import_due_at: now,
            verify_due_at: now,
            cleanup_due_at: now,
            deliver_due_at: now,
            confirm_due_at: now,
        },
        jobs: Vec::new(),
        intents: Vec::new(),
    }
}

fn label(id: &str) -> Label {
    let kind = if id.starts_with("Label_") {
        LabelKind::User
    } else {
        LabelKind::System
    };
    Label::plain(LabelId(id.into()), id, kind)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let url = args.get(1).map_or("sqlite::memory:", String::as_str);
    let rows: usize = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(50_000);
    let url = DatabaseUrl::parse(url).expect("a database URL");
    let pool = PoolConfig::default();
    let store = SqlStore::connect(&url, &pool).await.expect("connect");
    store.migrate().await.expect("migrate");
    let account = AccountId(format!("bench-{}@example.com", std::process::id()));
    store.track(&account, 1).await.expect("track");
    let snapshot = store.claim(&account, ENGINE_VERSION).await.expect("claim");
    let defined = [
        "INBOX", "Label_1", "Label_2", "Label_3", "Label_4", "Label_5",
    ];
    let mut first = commit(&account, snapshot.epoch, 0);
    first.labels = vec![LabelOp::ReplaceAll {
        labels: defined.iter().map(|id| label(id)).collect(),
        generation: 0,
        at: Timestamp(0),
    }];
    store.apply(&first).await.expect("labels");
    let started = Instant::now();
    for _ in 0..20 {
        store.ping().await.expect("ping");
    }
    println!("round trip to the database: {:?}", started.elapsed() / 20);
    let started = Instant::now();
    for (seq, chunk) in (1_u64..).zip((0..rows).collect::<Vec<_>>().chunks(1_000)) {
        let mut c = commit(&account, snapshot.epoch, seq);
        c.messages = chunk
            .iter()
            .map(|n| {
                let mut labels: LabelSet = ["INBOX", "Label_1"]
                    .into_iter()
                    .map(|id| LabelId(id.into()))
                    .collect();
                if n % 2 == 0 {
                    labels.insert(LabelId(format!("Label_{}", 2 + n % 4)));
                }
                // Imported rows: no import job to write for each.
                MessageOp::MarkImported {
                    observation: MessageObservation {
                        id: MessageId(format!("m{n:08}")),
                        thread_id: ThreadId(format!("t{n:08}")),
                        labels,
                        checkpoint: HistoryId(1),
                    },
                    blob: BlobRef(format!("m{n:08}.eml")),
                    size: 1_000,
                    internal_date: Timestamp(0),
                    headers: Box::default(),
                    facts: None,
                }
            })
            .collect();
        store.apply(&c).await.expect("rows");
    }
    println!("{rows} rows mirrored in {:?}", started.elapsed());
    let started = Instant::now();
    store
        .record_label(
            &account,
            &LabelOp::Remove {
                id: LabelId("Label_1".into()),
                at: Timestamp(1),
            },
        )
        .await
        .expect("remove");
    println!(
        "record_label(Remove Label_1), {rows} rows: {:?}",
        started.elapsed()
    );
    let generation = store.label_generation(&account).await.expect("generation");
    let mut listing = commit(&account, snapshot.epoch, 1_000_000);
    listing.labels = vec![LabelOp::ReplaceAll {
        labels: vec![label("INBOX")],
        generation,
        at: Timestamp(2),
    }];
    let started = Instant::now();
    store.apply(&listing).await.expect("listing");
    println!(
        "a listing removing Label_2..Label_5, {} rows: {:?}",
        rows / 2,
        started.elapsed()
    );
    store.forget(&account).await.expect("forget");
}
