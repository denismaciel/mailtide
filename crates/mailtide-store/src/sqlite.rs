//! `SQLite` store: the reference store for open-source users and tests.

use mailtide::commit::{Commit, CommitOutcome};
use mailtide::gmail::Label;
use mailtide::ids::{AccountId, EngineVersion, Epoch, MessageId, ThreadId, Timestamp};
use mailtide::jobs::{AttemptId, ClaimOutcome, Prioritised};
use mailtide::mirror::{Located, MessageView};
use mailtide::outbox::ActionRequest;
use mailtide::revision::{
    AckOutcome, ChangeFeed, ConsumerList, ConsumerName, FeedCursor, FeedPosition, Retention,
};
use mailtide::state::StoreSnapshot;
use mailtide::status::{AccountStatus, ForgetOutcome, ResumeOutcome, TrackOutcome};
use mailtide::thread::ThreadView;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use mailtide::revision::StateSeq;

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
            store.changes(account, StateSeq(0).into(), 10).await.err(),
        );
        let retention = Retention::new(now).through(StateSeq(1));
        refused("compact", store.compact(account, &retention).await.err());
        let consumer = ConsumerName::parse("reader").expect("a valid name");
        let position = FeedPosition {
            generation: 1,
            at: StateSeq(0).into(),
        };
        refused(
            "ack",
            store.ack(account, &consumer, position, now).await.err(),
        );
        refused("consumers", store.consumers(account).await.err());
        refused(
            "drop_consumer",
            store.drop_consumer(account, &consumer).await.err(),
        );
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
        let threads = [ThreadId("t".into())];
        refused("threads", store.threads(account, &threads).await.err());
        refused("locate", store.locate(account, &ids).await.err());
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
        let all = migrations();
        assert_eq!(all[3].version, 4);
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
        // Migration 4, and the later ones a current build expects.
        run(all[3..].iter().flat_map(|m| m.statements.clone()).collect()).await;
        let account = AccountId("a@example.com".into());
        let snapshot = store
            .claim(&account, mailtide::ids::ENGINE_VERSION)
            .await
            .expect("claim");
        assert_eq!(snapshot.sync, mailtide::state::SyncState::Live);
        assert_eq!(snapshot.labels_refreshed_at, None);
        // The documented downgrade, after forgetting the later migrations
        // (version-only, or an index that migrating again keeps).
        run(vec![
            "DELETE FROM mailtide_schema WHERE version > 4".to_owned(),
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

    /// Migration 5 has no statement of its own: it only keeps builds that
    /// can't decode a position inside a commit away from the database.
    /// Reverting it is forgetting the version.
    #[tokio::test]
    async fn migration_5_is_version_only_and_revertible() {
        use crate::sql::{SCHEMA_VERSION, SchemaState, migrations};
        let all = migrations();
        assert_eq!(all[4].version, 5);
        assert_eq!(
            all[4].statements[1..],
            ["INSERT INTO mailtide_schema (version) VALUES (5)".to_owned()]
        );
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        store.migrate().await.expect("migrate");
        // The documented downgrade, after forgetting the later migrations
        // (what they add, migrating again keeps).
        for statement in [
            "DELETE FROM mailtide_schema WHERE version > 5",
            "DELETE FROM mailtide_schema WHERE version = 5",
        ] {
            store
                .inner
                .execute_for_tests(statement)
                .await
                .expect("revert");
        }
        assert_eq!(
            store.inner.schema().await.expect("schema"),
            SchemaState::Behind {
                applied: 4,
                expected: SCHEMA_VERSION
            }
        );
        store.migrate().await.expect("migrate again");
        assert_eq!(
            store.inner.schema().await.expect("schema"),
            SchemaState::Current {
                version: SCHEMA_VERSION
            }
        );
    }

    /// The consumers migration creates an empty table beside an existing
    /// log: the log and its floor are kept, and no consumer is registered.
    /// Reverting it (drop the table, forget the version) loses only the
    /// cursors. Found by its statement: its number is assigned when it
    /// lands.
    #[tokio::test]
    async fn the_consumers_migration_applies_to_an_existing_database() {
        use crate::sql::{SchemaState, migrations};
        let all = migrations();
        let index = all
            .iter()
            .position(|m| {
                m.statements
                    .iter()
                    .any(|s| s.contains("mailtide_consumers"))
            })
            .expect("the consumers migration");
        let version = all[index].version;
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
        run(all[..index]
            .iter()
            .flat_map(|m| m.statements.clone())
            .collect())
        .await;
        // An account at the previous version: commits 1 to 4, compacted
        // through 2.
        let change = serde_json::to_string(&mailtide::revision::Change::LabelDefinitionsChanged)
            .expect("json");
        run(vec![
            "INSERT INTO mailtide_accounts
               (account, epoch, engine_version, cursor, sync, watch_expiration, last_activity,
                cooldown, rng_seed, seq, log_floor, generation)
             VALUES ('a@example.com', 1, 1, 7, '\"Live\"', NULL, NULL, NULL, 0, 4, 2, 1)"
                .to_owned(),
            format!(
                "INSERT INTO mailtide_changes (account, seq, idx, change)
                 VALUES ('a@example.com', 3, 0, '{change}'), ('a@example.com', 4, 0, '{change}')"
            ),
        ])
        .await;
        run(all[index..]
            .iter()
            .flat_map(|m| m.statements.clone())
            .collect())
        .await;
        let account = AccountId("a@example.com".into());
        let list = store.consumers(&account).await.expect("consumers");
        assert_eq!((list.generation, list.floor), (1, StateSeq(2)));
        assert!(list.cursors.is_empty(), "no cursors");
        match store
            .changes(&account, StateSeq(2).into(), 10)
            .await
            .expect("changes")
        {
            ChangeFeed::Page(page) => assert_eq!(page.changes.len(), 2, "the log is kept"),
            ChangeFeed::TooOld { .. } => panic!("the floor is kept, not raised"),
        }
        assert_eq!(
            store.compact(&account, &Retention::new(Timestamp(0))).await,
            Ok(0),
            "no consumer and no bound: nothing"
        );
        // The documented downgrade, after the later migrations.
        run(vec![
            format!("DELETE FROM mailtide_schema WHERE version > {version}"),
            "DROP TABLE mailtide_consumers".to_owned(),
            format!("DELETE FROM mailtide_schema WHERE version = {version}"),
        ])
        .await;
        assert!(matches!(
            store.inner.schema().await.expect("schema"),
            SchemaState::Behind { applied, .. } if applied == version - 1
        ));
        store.migrate().await.expect("migrate again");
        assert_eq!(
            store.consumers(&account).await.map(|list| list.cursors),
            Ok(Vec::new())
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

    /// The migration that adds the thread index, by its statement (its
    /// number is assigned when it lands), and the migrations before it.
    fn thread_index_migration() -> (Vec<crate::sql::Migration>, crate::sql::Migration) {
        let all = crate::sql::migrations();
        let index = all
            .iter()
            .position(|m| {
                m.statements
                    .iter()
                    .any(|s| s.contains("mailtide_messages_thread"))
            })
            .expect("the thread index migration");
        (all[..index].to_vec(), all[index].clone())
    }

    async fn run_statements(store: &SqliteStore, migrations: &[crate::sql::Migration]) {
        for statement in migrations.iter().flat_map(|m| &m.statements) {
            store
                .inner
                .execute_for_tests(statement)
                .await
                .expect("statement");
        }
    }

    /// Two threads, one with a draft, a message awaiting import and a
    /// pending action; returns the views of both.
    async fn populate(store: &SqliteStore) -> Vec<ThreadView> {
        use mailtide::gmail::{HeaderFacts, MessageObservation};
        use mailtide::ids::{BlobRef, HistoryId};
        use mailtide::labels::set;
        let account = AccountId("a@example.com".into());
        store.track(&account, 1).await.expect("track");
        let snapshot = store
            .claim(&account, mailtide::ids::ENGINE_VERSION)
            .await
            .expect("claim");
        let mut commit = lifecycle_commit(
            &snapshot,
            0,
            mailtide::state::SyncState::Live,
            Some(mailtide::commit::CursorMove {
                expected: None,
                to: HistoryId(7),
            }),
            0,
        );
        let observation = |thread: &str, id: &str, labels: &[&str]| MessageObservation {
            id: MessageId(id.into()),
            thread_id: ThreadId(thread.into()),
            labels: set(labels.iter().copied()),
            checkpoint: HistoryId(1),
        };
        let import = |thread: &str, id: &str, labels: &[&str], date: i64| {
            mailtide::commit::MessageOp::MarkImported {
                observation: observation(thread, id, labels),
                blob: BlobRef(format!("{id}.eml")),
                size: 10,
                internal_date: Timestamp(date),
                headers: Box::new(HeaderFacts {
                    from: Some(format!("{id}@example.com")),
                    ..HeaderFacts::default()
                }),
            }
        };
        commit.messages = vec![
            import("t1", "a", &["INBOX"], 20),
            import("t1", "b", &["INBOX", "UNREAD"], 10),
            mailtide::commit::MessageOp::Observe(observation("t1", "c", &["INBOX"])),
            import("t1", "d", &["DRAFT"], 30),
            import("t2", "e", &["INBOX"], 5),
        ];
        assert!(matches!(
            store.apply(&commit).await.expect("apply"),
            CommitOutcome::Applied { .. }
        ));
        store
            .admit(
                &account,
                &ActionRequest::Thread {
                    id: ThreadId("t2".into()),
                    action: mailtide::outbox::Action::Archive,
                },
                Timestamp(0),
            )
            .await
            .expect("admit");
        views(store).await
    }

    async fn views(store: &SqliteStore) -> Vec<ThreadView> {
        store
            .threads(
                &AccountId("a@example.com".into()),
                &[ThreadId("t1".into()), ThreadId("t2".into())],
            )
            .await
            .expect("threads")
    }

    async fn has_thread_index(store: &SqliteStore) -> bool {
        store
            .inner
            .text_for_tests(
                "SELECT CAST(COUNT(*) AS TEXT) FROM sqlite_master
                 WHERE type = 'index' AND name = 'mailtide_messages_thread'",
            )
            .await
            .expect("index count")
            == "1"
    }

    /// The documented rollback (SPEC §11) takes a populated database back to
    /// the previous version and loses nothing; migrating again restores the
    /// index, and the views read the same.
    #[tokio::test]
    async fn the_thread_index_migration_applies_to_a_populated_database() {
        use crate::sql::{SCHEMA_VERSION, SchemaState};
        let (_, migration) = thread_index_migration();
        assert_eq!(
            migration.statements[1..],
            [
                "CREATE INDEX IF NOT EXISTS mailtide_messages_thread ON mailtide_messages (account, thread_id, id)"
                    .to_owned(),
                format!(
                    "INSERT INTO mailtide_schema (version) VALUES ({})",
                    migration.version
                ),
            ]
        );
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        store.migrate().await.expect("migrate");
        let read = populate(&store).await;
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].message_count, 3);
        assert_eq!(read[1].labels.len(), 0, "the archive overlays");
        // After forgetting the later migrations (what they add, migrating
        // again keeps).
        for statement in [
            format!(
                "DELETE FROM mailtide_schema WHERE version > {}",
                migration.version
            ),
            "DROP INDEX mailtide_messages_thread".to_owned(),
            format!(
                "DELETE FROM mailtide_schema WHERE version = {}",
                migration.version
            ),
        ] {
            store
                .inner
                .execute_for_tests(&statement)
                .await
                .expect("rollback");
        }
        assert!(!has_thread_index(&store).await);
        assert_eq!(
            store.inner.schema().await.expect("schema"),
            SchemaState::Behind {
                applied: migration.version - 1,
                expected: SCHEMA_VERSION
            }
        );
        store.migrate().await.expect("migrate");
        assert!(has_thread_index(&store).await);
        assert_eq!(
            store.inner.schema().await.expect("schema"),
            SchemaState::Current {
                version: SCHEMA_VERSION
            }
        );
        assert_eq!(views(&store).await, read);
    }

    /// The thread read and the admission lookup search the thread index
    /// instead of reading every row of the account.
    #[tokio::test]
    async fn thread_reads_and_thread_actions_use_the_thread_index() {
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        store.migrate().await.expect("migrate");
        populate(&store).await;
        for (statement, binds) in [
            (
                crate::sql::thread_rows(2),
                vec!["a@example.com", "t1", "t2"],
            ),
            (
                crate::sql::THREAD_MEMBERS.to_owned(),
                vec!["a@example.com", "t1"],
            ),
        ] {
            let plan = store
                .inner
                .plan_for_tests(&statement, &binds)
                .await
                .expect("plan");
            let messages: Vec<&String> = plan
                .iter()
                .filter(|step| step.contains(" m ") || step.contains("mailtide_messages"))
                .collect();
            assert!(
                messages
                    .iter()
                    .any(|step| step.contains("mailtide_messages_thread")),
                "{statement}: {plan:?}"
            );
            assert!(
                messages.iter().all(|step| !step.starts_with("SCAN")
                    && !step.contains("sqlite_autoindex_mailtide_messages")),
                "{statement}: {plan:?}"
            );
        }
    }

    /// A host that built the index itself (on Postgres, `CREATE INDEX
    /// CONCURRENTLY` under the same name) runs the migration as a no-op.
    #[tokio::test]
    async fn the_index_a_host_created_beforehand_is_kept() {
        use crate::sql::{SCHEMA_VERSION, SchemaState};
        let (before, _) = thread_index_migration();
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        run_statements(&store, &before).await;
        store
            .inner
            .execute_for_tests(
                "CREATE INDEX mailtide_messages_thread ON mailtide_messages (account, thread_id, id)",
            )
            .await
            .expect("the host's index");
        let host_sql = store
            .inner
            .text_for_tests("SELECT sql FROM sqlite_master WHERE name = 'mailtide_messages_thread'")
            .await
            .expect("sql");
        store.migrate().await.expect("migrate");
        assert_eq!(
            store.inner.schema().await.expect("schema"),
            SchemaState::Current {
                version: SCHEMA_VERSION
            }
        );
        assert!(has_thread_index(&store).await);
        assert_eq!(
            store
                .inner
                .text_for_tests(
                    "SELECT sql FROM sqlite_master WHERE name = 'mailtide_messages_thread'",
                )
                .await
                .expect("sql"),
            host_sql
        );
        let read = populate(&store).await;
        assert_eq!(read.len(), 2);
        assert_eq!(views(&store).await, read);
    }
}
