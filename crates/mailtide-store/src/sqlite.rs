//! `SQLite` store: the reference store for open-source users and tests.

use mailtide::commit::{Commit, CommitOutcome};
use mailtide::ids::{AccountId, EngineVersion, Epoch, MessageId, Timestamp};
use mailtide::jobs::{AttemptId, ClaimOutcome};
use mailtide::mirror::MessageView;
use mailtide::outbox::ActionRequest;
use mailtide::revision::{ChangeFeed, StateSeq};
use mailtide::state::StoreSnapshot;
use mailtide::status::{AccountStatus, ForgetOutcome, ResumeOutcome};

use crate::sql::SqlStore;
use crate::{Store, StoreError};

/// `SQLite` store: the reference store for open-source users and tests.
#[derive(Debug, Clone)]
pub struct SqliteStore {
    inner: SqlStore,
}

impl SqliteStore {
    /// Connect to `url`, e.g. `sqlite::memory:` or `sqlite:mail.db?mode=rwc`.
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

impl Store for SqliteStore {
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
    ) -> Result<ClaimOutcome, StoreError> {
        self.inner
            .claim_jobs(account, epoch, limit, now, attempt)
            .await
    }

    async fn changes(
        &self,
        account: &AccountId,
        since: StateSeq,
        limit: u32,
    ) -> Result<ChangeFeed, StoreError> {
        self.inner.changes(account, since, limit).await
    }

    async fn compact(&self, account: &AccountId, through: StateSeq) -> Result<u64, StoreError> {
        crate::Store::compact(&self.inner, account, through).await
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

    #[tokio::test]
    async fn passes_the_conformance_suite() {
        let results = crate::conformance::run(|| async {
            let store = SqliteStore::connect("sqlite::memory:").await?;
            store.migrate().await?;
            Ok(store)
        })
        .await;
        let failures: Vec<_> = results.iter().filter(|r| r.failure.is_some()).collect();
        assert!(failures.is_empty(), "{failures:#?}");
    }

    /// A host running the exposed SQL with its own tool ends up exactly where
    /// `migrate()` would, and `migrate()` then does nothing.
    #[tokio::test]
    async fn exposed_migrations_match_migrate() {
        use crate::sql::{SCHEMA_VERSION, SchemaState, migrations};
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        assert_eq!(
            store.inner.schema().await.expect("schema"),
            SchemaState::Behind {
                applied: 0,
                expected: SCHEMA_VERSION
            }
        );
        for migration in migrations() {
            for statement in &migration.statements {
                store
                    .inner
                    .execute_for_tests(statement)
                    .await
                    .expect("statement");
            }
        }
        assert_eq!(
            store.inner.schema().await.expect("schema"),
            SchemaState::Current {
                version: SCHEMA_VERSION
            }
        );
        store.migrate().await.expect("idempotent");
        assert_eq!(
            store.inner.schema().await.expect("schema"),
            SchemaState::Current {
                version: SCHEMA_VERSION
            }
        );
    }

    /// A stored number out of range is corrupt, never read as another value.
    #[tokio::test]
    async fn a_negative_stored_cursor_is_corrupt() {
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        store.migrate().await.expect("migrate");
        let account = AccountId("a@example.com".into());
        store
            .claim(&account, EngineVersion(1))
            .await
            .expect("claim");
        store
            .inner
            .execute_for_tests("UPDATE mailtide_accounts SET cursor = -5")
            .await
            .expect("update");
        match store.claim(&account, EngineVersion(1)).await {
            Err(StoreError::Corrupt(reason)) => assert!(reason.contains("history id"), "{reason}"),
            other => panic!("expected Corrupt, got {other:?}"),
        }
    }
}
