//! Postgres store: mailtide's tables inside the host database.

use mailtide::commit::{Commit, CommitOutcome};
use mailtide::ids::{AccountId, EngineVersion, Epoch, MessageId, Timestamp};
use mailtide::jobs::{AttemptId, ClaimedJob};
use mailtide::mirror::MessageView;
use mailtide::outbox::ActionRequest;
use mailtide::revision::{ChangePage, StateSeq};
use mailtide::state::StoreSnapshot;

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
        SqlStore::connect(url).await.map(|inner| Self { inner })
    }

    /// Create mailtide's tables if they don't exist.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        self.inner.migrate().await
    }
}

impl Store for PostgresStore {
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

    async fn claim_jobs(
        &self,
        account: &AccountId,
        epoch: Epoch,
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
    ) -> Result<Vec<ClaimedJob>, StoreError> {
        self.inner
            .claim_jobs(account, epoch, limit, now, attempt)
            .await
    }

    async fn changes(
        &self,
        account: &AccountId,
        since: StateSeq,
        limit: u32,
    ) -> Result<ChangePage, StoreError> {
        self.inner.changes(account, since, limit).await
    }

    async fn admit(
        &self,
        account: &AccountId,
        request: &ActionRequest,
        now: Timestamp,
    ) -> Result<u32, StoreError> {
        crate::Store::admit(&self.inner, account, request, now).await
    }

    async fn messages(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Vec<MessageView>, StoreError> {
        crate::Store::messages(&self.inner, account, ids).await
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
}
