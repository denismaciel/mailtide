//! Postgres store: mailtide's tables inside the host database.

use mailtide::commit::{Commit, CommitOutcome, DeletionEvidence, LabelOp};
use mailtide::drafts::{DraftRecorded, DraftView, DraftWrite};
use mailtide::gmail::Label;
use mailtide::ids::{
    AccountId, DraftId, EngineVersion, Epoch, LabelId, MessageId, ThreadId, Timestamp,
};
use mailtide::jobs::{AttemptId, ClaimOutcome, Prioritised};
use mailtide::mirror::{FactsUpdate, Located, MessageView};
use mailtide::outbox::ActionRequest;
use mailtide::revision::{
    AckOutcome, ChangeFeed, ConsumerList, ConsumerName, FeedCursor, FeedPosition, Retention,
};
use mailtide::state::StoreSnapshot;
use mailtide::status::{AccountStatus, ForgetOutcome, ResumeOutcome, TrackOutcome};
use mailtide::thread::ThreadView;

use crate::sql::SqlStore;
use crate::{Store, StoreError};

/// Postgres store: mailtide's tables inside the host database.
#[derive(Debug, Clone)]
pub struct PostgresStore {
    inner: SqlStore,
}

impl PostgresStore {
    /// Connect to `url`, e.g. `postgres://user:pass@host/db`.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let url = crate::config::DatabaseUrl::parse(url)
            .map_err(|e| StoreError::Misconfigured(e.to_string()))?;
        SqlStore::connect(&url, &crate::config::PoolConfig::default())
            .await
            .map(|inner| Self { inner })
    }

    /// Create mailtide's tables if they don't exist.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        self.inner.migrate().await
    }
}

impl Store for PostgresStore {
    async fn track(
        &self,
        account: &AccountId,
        generation: u64,
    ) -> Result<TrackOutcome, StoreError> {
        self.inner.track(account, generation).await
    }

    async fn track_new(&self, account: &AccountId) -> Result<TrackOutcome, StoreError> {
        crate::Store::track_new(&self.inner, account).await
    }

    async fn claim(
        &self,
        account: &AccountId,
        version: EngineVersion,
    ) -> Result<StoreSnapshot, StoreError> {
        self.inner.claim(account, version).await
    }

    async fn apply(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        self.inner.apply(commit).await
    }

    async fn advance_audit(
        &self,
        request: &mailtide::audit::AuditRequest,
    ) -> Result<mailtide::audit::AuditOutcome, StoreError> {
        self.inner.advance_audit(request).await
    }

    async fn claim_jobs(
        &self,
        account: &AccountId,
        claim: (Epoch, u64),
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
        floor: mailtide::quota::Priority,
    ) -> Result<ClaimOutcome, StoreError> {
        self.inner
            .claim_jobs(account, claim, limit, now, attempt, floor)
            .await
    }

    async fn changes(
        &self,
        account: &AccountId,
        since: FeedCursor,
        limit: u32,
    ) -> Result<ChangeFeed, StoreError> {
        self.inner.changes(account, since, limit).await
    }

    async fn compact(&self, account: &AccountId, retention: &Retention) -> Result<u64, StoreError> {
        crate::Store::compact(&self.inner, account, retention).await
    }

    async fn ack(
        &self,
        account: &AccountId,
        consumer: &ConsumerName,
        at: FeedPosition,
        now: Timestamp,
    ) -> Result<AckOutcome, StoreError> {
        crate::Store::ack(&self.inner, account, consumer, at, now).await
    }

    async fn consumers(&self, account: &AccountId) -> Result<ConsumerList, StoreError> {
        crate::Store::consumers(&self.inner, account).await
    }

    async fn drop_consumer(
        &self,
        account: &AccountId,
        consumer: &ConsumerName,
    ) -> Result<bool, StoreError> {
        crate::Store::drop_consumer(&self.inner, account, consumer).await
    }

    async fn message_ids(
        &self,
        account: &AccountId,
        after: Option<&MessageId>,
        limit: u32,
    ) -> Result<Vec<MessageId>, StoreError> {
        crate::Store::message_ids(&self.inner, account, after, limit).await
    }

    async fn status(&self, account: &AccountId) -> Result<Option<AccountStatus>, StoreError> {
        crate::Store::status(&self.inner, account).await
    }

    async fn accounts(&self) -> Result<Vec<AccountId>, StoreError> {
        crate::Store::accounts(&self.inner).await
    }

    async fn resume(
        &self,
        account: &AccountId,
        now: Timestamp,
    ) -> Result<ResumeOutcome, StoreError> {
        crate::Store::resume(&self.inner, account, now).await
    }

    async fn forget(&self, account: &AccountId) -> Result<ForgetOutcome, StoreError> {
        crate::Store::forget(&self.inner, account).await
    }

    async fn admit(
        &self,
        account: &AccountId,
        request: &ActionRequest,
        now: Timestamp,
    ) -> Result<u32, StoreError> {
        crate::Store::admit(&self.inner, account, request, now).await
    }

    async fn prioritise(
        &self,
        account: &AccountId,
        ids: &[MessageId],
        now: Timestamp,
    ) -> Result<Prioritised, StoreError> {
        crate::Store::prioritise(&self.inner, account, ids, now).await
    }

    async fn refresh_facts(
        &self,
        account: &AccountId,
        updates: &[FactsUpdate],
    ) -> Result<u32, StoreError> {
        crate::Store::refresh_facts(&self.inner, account, updates).await
    }

    async fn record_deleted(
        &self,
        account: &AccountId,
        deletions: &[(MessageId, DeletionEvidence)],
        now: Timestamp,
    ) -> Result<u32, StoreError> {
        crate::Store::record_deleted(&self.inner, account, deletions, now).await
    }

    async fn message_ids_labelled(
        &self,
        account: &AccountId,
        label: &LabelId,
        after: Option<&MessageId>,
        limit: u32,
    ) -> Result<Vec<MessageId>, StoreError> {
        crate::Store::message_ids_labelled(&self.inner, account, label, after, limit).await
    }

    async fn messages(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Vec<MessageView>, StoreError> {
        crate::Store::messages(&self.inner, account, ids).await
    }

    async fn threads(
        &self,
        account: &AccountId,
        ids: &[ThreadId],
    ) -> Result<Vec<ThreadView>, StoreError> {
        crate::Store::threads(&self.inner, account, ids).await
    }

    async fn locate(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Vec<(MessageId, Located)>, StoreError> {
        crate::Store::locate(&self.inner, account, ids).await
    }

    async fn labels(&self, account: &AccountId) -> Result<Vec<Label>, StoreError> {
        crate::Store::labels(&self.inner, account).await
    }

    async fn user_labels_oldest_first(
        &self,
        account: &AccountId,
    ) -> Result<Vec<mailtide::ids::LabelId>, StoreError> {
        crate::Store::user_labels_oldest_first(&self.inner, account).await
    }

    async fn record_label(&self, account: &AccountId, op: &LabelOp) -> Result<(), StoreError> {
        crate::Store::record_label(&self.inner, account, op).await
    }

    async fn label_generation(&self, account: &AccountId) -> Result<u64, StoreError> {
        crate::Store::label_generation(&self.inner, account).await
    }

    async fn record_draft(
        &self,
        account: &AccountId,
        write: &DraftWrite,
        now: Timestamp,
    ) -> Result<DraftRecorded, StoreError> {
        crate::Store::record_draft(&self.inner, account, write, now).await
    }

    async fn drafts(
        &self,
        account: &AccountId,
        after: Option<&DraftId>,
        limit: u32,
    ) -> Result<Vec<DraftView>, StoreError> {
        crate::Store::drafts(&self.inner, account, after, limit).await
    }

    async fn draft(
        &self,
        account: &AccountId,
        id: &DraftId,
    ) -> Result<Option<DraftView>, StoreError> {
        crate::Store::draft(&self.inner, account, id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tests share one database; the conformance suite resets it.
    static DATABASE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Runs only when `MAILTIDE_TEST_POSTGRES_URL` points at a scratch
    /// database; each case uses a fresh schema-qualified account, and the
    /// tables are created if missing.
    #[tokio::test]
    async fn passes_the_conformance_suite() {
        let Ok(url) = std::env::var("MAILTIDE_TEST_POSTGRES_URL") else {
            eprintln!("MAILTIDE_TEST_POSTGRES_URL not set; skipping");
            return;
        };
        let _database = DATABASE.lock().await;
        let results = crate::conformance::run(|| async {
            let store = PostgresStore::connect(&url).await?;
            store.migrate().await?;
            store.inner.reset_for_tests().await?;
            Ok(store)
        })
        .await;
        let failures: Vec<_> = results.iter().filter(|r| r.failure.is_some()).collect();
        assert!(failures.is_empty(), "{failures:#?}");
    }

    /// Concurrent first claims of a new account get distinct epochs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn racing_first_claims_get_distinct_epochs() {
        let Ok(url) = std::env::var("MAILTIDE_TEST_POSTGRES_URL") else {
            eprintln!("MAILTIDE_TEST_POSTGRES_URL not set; skipping");
            return;
        };
        let _database = DATABASE.lock().await;
        let setup = PostgresStore::connect(&url).await.expect("connect");
        setup.migrate().await.expect("migrate");
        for round in 0..5 {
            let account = AccountId(format!("race{round}-{}@example.com", std::process::id()));
            setup.track(&account, 1).await.expect("track");
            let mut handles = Vec::new();
            for _ in 0..4 {
                let url = url.clone();
                let account = account.clone();
                handles.push(tokio::spawn(async move {
                    let store = PostgresStore::connect(&url).await.expect("connect");
                    store
                        .claim(&account, mailtide::ids::ENGINE_VERSION)
                        .await
                        .expect("claim")
                        .epoch
                }));
            }
            let mut epochs = Vec::new();
            for handle in handles {
                epochs.push(handle.await.expect("join"));
            }
            epochs.sort();
            epochs.dedup();
            assert_eq!(epochs.len(), 4, "round {round}: epochs {epochs:?}");
        }
    }

    /// Sends of one draft begun at once from separate connections (as from
    /// separate processes): exactly one marks the row `sending`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn racing_sends_from_separate_connections_have_one_winner() {
        use mailtide::drafts::{DraftRecorded, DraftWrite, ListedDraft};
        use mailtide::ids::{DraftId, MessageId, ThreadId, Timestamp};
        let Ok(url) = std::env::var("MAILTIDE_TEST_POSTGRES_URL") else {
            eprintln!("MAILTIDE_TEST_POSTGRES_URL not set; skipping");
            return;
        };
        let _database = DATABASE.lock().await;
        let setup = PostgresStore::connect(&url).await.expect("connect");
        setup.migrate().await.expect("migrate");
        for round in 0..5 {
            let account = AccountId(format!("send{round}-{}@example.com", std::process::id()));
            setup.track(&account, 1).await.expect("track");
            let save = DraftWrite::Saved {
                draft: ListedDraft {
                    id: DraftId("r1".into()),
                    message: MessageId(format!("m{round}")),
                    thread_id: ThreadId("t1".into()),
                },
                tracking_id: format!("tracking-race-{round:010}"),
                created: true,
            };
            setup
                .record_draft(&account, &save, Timestamp(1))
                .await
                .expect("save");
            let mut handles = Vec::new();
            for _ in 0..4 {
                let url = url.clone();
                let account = account.clone();
                handles.push(tokio::spawn(async move {
                    let store = PostgresStore::connect(&url).await.expect("connect");
                    let begin = DraftWrite::BeginSend {
                        draft: DraftId("r1".into()),
                        retry_at: Timestamp(60_000),
                    };
                    store
                        .record_draft(&account, &begin, Timestamp(2))
                        .await
                        .expect("begin")
                }));
            }
            let mut winners = 0;
            for handle in handles {
                match handle.await.expect("join") {
                    DraftRecorded::Started { .. } => winners += 1,
                    DraftRecorded::InProgress { retry_at } => {
                        assert_eq!(retry_at, Timestamp(60_000));
                    }
                    other => panic!("round {round}: {other:?}"),
                }
            }
            assert_eq!(winners, 1, "round {round}");
        }
    }
}
