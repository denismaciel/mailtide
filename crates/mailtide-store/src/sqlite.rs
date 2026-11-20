//! `SQLite` store: the reference store for open-source users and tests.

use mailtide::commit::{Commit, CommitOutcome};
use mailtide::gmail::Label;
use mailtide::ids::{AccountId, EngineVersion, Epoch, MessageId, Timestamp};
use mailtide::jobs::{AttemptId, ClaimOutcome, Prioritised};
use mailtide::mirror::MessageView;
use mailtide::outbox::ActionRequest;
use mailtide::revision::{ChangeFeed, StateSeq};
use mailtide::state::StoreSnapshot;
use mailtide::status::{AccountStatus, ForgetOutcome, ResumeOutcome, TrackOutcome};

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
    async fn track(
        &self,
        account: &AccountId,
        generation: u64,
    ) -> Result<TrackOutcome, StoreError> {
        self.inner.track(account, generation).await
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

    async fn claim_jobs(
        &self,
        account: &AccountId,
        epoch: Epoch,
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
        floor: mailtide::quota::Priority,
    ) -> Result<ClaimOutcome, StoreError> {
        self.inner
            .claim_jobs(account, epoch, limit, now, attempt, floor)
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

    async fn prioritise(
        &self,
        account: &AccountId,
        ids: &[MessageId],
        now: Timestamp,
    ) -> Result<Prioritised, StoreError> {
        crate::Store::prioritise(&self.inner, account, ids, now).await
    }

    async fn messages(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Vec<MessageView>, StoreError> {
        crate::Store::messages(&self.inner, account, ids).await
    }

    async fn labels(&self, account: &AccountId) -> Result<Vec<Label>, StoreError> {
        crate::Store::labels(&self.inner, account).await
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

    /// Every [`Store`] method of `store`, each expected to refuse with
    /// `SchemaOutOfDate { applied, .. }` before reading or writing.
    async fn assert_every_call_refused(store: &SqliteStore, commit: &Commit, applied: u32) {
        use crate::sql::SCHEMA_VERSION;
        use mailtide::outbox::Action;
        let account = &commit.account;
        let ids = [MessageId("m".into())];
        let action = ActionRequest::Messages {
            ids: ids.to_vec(),
            action: Action::Archive,
        };
        let refused = |what: &str, error: Option<StoreError>| {
            assert_eq!(
                error,
                Some(StoreError::SchemaOutOfDate {
                    applied,
                    expected: SCHEMA_VERSION
                }),
                "{what}"
            );
        };
        let now = Timestamp(0);
        let floor = mailtide::quota::Priority::Backfill;
        refused("track", store.track(account, 1).await.err());
        refused(
            "claim",
            store
                .claim(account, mailtide::ids::ENGINE_VERSION)
                .await
                .err(),
        );
        refused("apply", store.apply(commit).await.err());
        let jobs = store
            .claim_jobs(account, commit.epoch, 1, now, AttemptId(1), floor)
            .await;
        refused("claim_jobs", jobs.err());
        refused(
            "changes",
            store.changes(account, StateSeq(0), 10).await.err(),
        );
        refused("compact", store.compact(account, StateSeq(1)).await.err());
        refused(
            "message_ids",
            store.message_ids(account, None, 10).await.err(),
        );
        refused("admit", store.admit(account, &action, now).await.err());
        refused(
            "prioritise",
            store.prioritise(account, &ids, now).await.err(),
        );
        refused("status", store.status(account).await.err());
        refused("accounts", store.accounts().await.err());
        refused("resume", store.resume(account, now).await.err());
        refused("forget", store.forget(account).await.err());
        refused("messages", store.messages(account, &ids).await.err());
        refused("labels", store.labels(account).await.err());
    }

    /// Reads refuse a schema this build doesn't match as writes do: one
    /// that is behind would fail mid-call, one that is ahead may hold rows
    /// this build would misread. Nothing is created on the way.
    #[tokio::test]
    async fn every_read_and_write_refuses_a_schema_out_of_date() {
        use crate::sql::SCHEMA_VERSION;
        use mailtide::state::SyncState;
        let current = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        current.migrate().await.expect("migrate");
        let account = AccountId("a@example.com".into());
        current.track(&account, 1).await.expect("track");
        let snapshot = current
            .claim(&account, mailtide::ids::ENGINE_VERSION)
            .await
            .expect("claim");
        let commit = lifecycle_commit(&snapshot, 0, SyncState::Live, None, 0);

        let behind = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        assert_every_call_refused(&behind, &commit, 0).await;
        assert_eq!(
            behind
                .inner
                .text_for_tests(
                    "SELECT CAST(COUNT(*) AS TEXT) FROM sqlite_master WHERE type = 'table'"
                )
                .await
                .expect("tables"),
            "0",
            "no table was created"
        );

        let ahead = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        ahead.migrate().await.expect("migrate");
        ahead
            .inner
            .execute_for_tests(&format!(
                "INSERT INTO mailtide_schema (version) VALUES ({})",
                SCHEMA_VERSION + 1
            ))
            .await
            .expect("a newer build's migration");
        assert_every_call_refused(&ahead, &commit, SCHEMA_VERSION + 1).await;
    }

    /// The check costs one query per store, not one per call: once the
    /// schema was seen current, a clone of the store does not ask again.
    #[tokio::test]
    async fn a_current_schema_is_checked_once() {
        use crate::sql::SCHEMA_VERSION;
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        store.migrate().await.expect("migrate");
        assert_eq!(store.accounts().await, Ok(Vec::new()));
        store
            .inner
            .execute_for_tests(&format!(
                "INSERT INTO mailtide_schema (version) VALUES ({})",
                SCHEMA_VERSION + 1
            ))
            .await
            .expect("a newer build's migration");
        assert_eq!(store.clone().accounts().await, Ok(Vec::new()), "cached");
        let fresh = SqliteStore {
            inner: crate::sql::SqlStore::clone(&store.inner),
        };
        assert_eq!(fresh.accounts().await, Ok(Vec::new()), "shared by clones");
    }

    /// A commit moving `snapshot`'s account to `sync`, recording a label
    /// listing at `listed`.
    fn lifecycle_commit(
        snapshot: &StoreSnapshot,
        seq: u64,
        sync: mailtide::state::SyncState,
        cursor: Option<mailtide::commit::CursorMove>,
        listed: i64,
    ) -> Commit {
        let now = Timestamp(0);
        Commit {
            id: mailtide::ids::CommitId {
                epoch: snapshot.epoch,
                seq,
            },
            account: snapshot.account.clone(),
            epoch: snapshot.epoch,
            engine_version: mailtide::ids::ENGINE_VERSION,
            cursor,
            claim: None,
            sync: Some(sync),
            messages: Vec::new(),
            labels: Vec::new(),
            account_ops: vec![mailtide::commit::AccountOp::SetLabelsRefreshedAt(
                Timestamp(listed),
            )],
            job_plan: mailtide::commit::JobPlan {
                priority: mailtide::quota::Priority::Live,
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

    /// The `sync` column keeps the format earlier builds write and read (the
    /// bare lifecycle); the label refresh time has a column of its own, so an
    /// older build never meets a value it can't parse.
    #[tokio::test]
    async fn the_sync_column_keeps_its_format() {
        use mailtide::reconcile::{ReconcileProgress, ReconcileReason};
        use mailtide::state::SyncState;
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        store.migrate().await.expect("migrate");
        let account = AccountId("a@example.com".into());
        store.track(&account, 1).await.expect("track");
        let snapshot = store
            .claim(&account, mailtide::ids::ENGINE_VERSION)
            .await
            .expect("claim");
        let live = lifecycle_commit(
            &snapshot,
            0,
            SyncState::Live,
            Some(mailtide::commit::CursorMove {
                expected: None,
                to: mailtide::ids::HistoryId(7),
            }),
            5_000,
        );
        assert!(matches!(
            store.apply(&live).await.expect("apply"),
            CommitOutcome::Applied { .. }
        ));
        let column = |name: &str| format!("SELECT {name} FROM mailtide_accounts");
        assert_eq!(
            store
                .inner
                .text_for_tests(&column("sync"))
                .await
                .expect("sync"),
            r#""Live""#
        );
        assert_eq!(
            store
                .inner
                .text_for_tests(&column("CAST(labels_refreshed_at AS TEXT)"))
                .await
                .expect("refreshed"),
            "5000"
        );
        let reconciling = lifecycle_commit(
            &snapshot,
            1,
            SyncState::Reconciling(ReconcileProgress::begin(ReconcileReason::HistoryExpired)),
            None,
            6_000,
        );
        assert!(matches!(
            store.apply(&reconciling).await.expect("apply"),
            CommitOutcome::Applied { .. }
        ));
        assert_eq!(
            store
                .inner
                .text_for_tests(&column("sync"))
                .await
                .expect("sync"),
            r#"{"Reconciling":{"reason":"HistoryExpired","step":"CaptureBaseline"}}"#
        );
        let snapshot = store
            .claim(&account, mailtide::ids::ENGINE_VERSION)
            .await
            .expect("claim");
        assert_eq!(snapshot.labels_refreshed_at, Some(Timestamp(6_000)));
    }

    /// Migration 4 adds a nullable column: accounts from version 3 read as
    /// never refreshed. Reverting it (drop the column, forget the version) is
    /// safe: nothing else depends on it, and migrating again restores it.
    #[tokio::test]
    async fn migration_4_is_additive_and_revertible() {
        use crate::sql::{SCHEMA_VERSION, SchemaState, migrations};
        assert_eq!(SCHEMA_VERSION, 4);
        let all = migrations();
        assert_eq!(
            all[3].statements[1..],
            [
                "ALTER TABLE mailtide_accounts ADD COLUMN labels_refreshed_at BIGINT".to_owned(),
                "INSERT INTO mailtide_schema (version) VALUES (4)".to_owned(),
            ]
        );
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        let run = |statements: Vec<String>| {
            let store = &store;
            async move {
                for statement in statements {
                    store
                        .inner
                        .execute_for_tests(&statement)
                        .await
                        .expect("statement");
                }
            }
        };
        run(all[..3].iter().flat_map(|m| m.statements.clone()).collect()).await;
        // A live account as version 3 stored it.
        run(vec![
            "INSERT INTO mailtide_accounts
               (account, epoch, engine_version, cursor, sync, watch_expiration, last_activity,
                cooldown, rng_seed, seq, log_floor, generation)
             VALUES ('a@example.com', 1, 1, 7, '\"Live\"', NULL, NULL, NULL, 0, 0, 0, 1)"
                .to_owned(),
        ])
        .await;
        run(all[3].statements.clone()).await;
        let account = AccountId("a@example.com".into());
        let snapshot = store
            .claim(&account, mailtide::ids::ENGINE_VERSION)
            .await
            .expect("claim");
        assert_eq!(snapshot.sync, mailtide::state::SyncState::Live);
        assert_eq!(snapshot.labels_refreshed_at, None);
        // The documented downgrade.
        run(vec![
            "ALTER TABLE mailtide_accounts DROP COLUMN labels_refreshed_at".to_owned(),
            "DELETE FROM mailtide_schema WHERE version = 4".to_owned(),
        ])
        .await;
        assert_eq!(
            store.inner.schema().await.expect("schema"),
            SchemaState::Behind {
                applied: 3,
                expected: SCHEMA_VERSION
            }
        );
        store.migrate().await.expect("migrate again");
        let snapshot = store
            .claim(&account, mailtide::ids::ENGINE_VERSION)
            .await
            .expect("claim");
        assert_eq!(snapshot.cursor, Some(mailtide::ids::HistoryId(7)));
    }

    /// A stored number out of range is corrupt, never read as another value.
    #[tokio::test]
    async fn a_negative_stored_cursor_is_corrupt() {
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        store.migrate().await.expect("migrate");
        let account = AccountId("a@example.com".into());
        store.track(&account, 1).await.expect("track");
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
