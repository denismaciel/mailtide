//! One SQL store for Postgres and `SQLite`, over sqlx's `Any` driver.
//!
//! A commit runs in one transaction: lock the account row, load exactly the
//! rows the commit touches (its messages, tombstones, jobs and reconciliation
//! seen-set; all older messages when reconciliation finishes; the label
//! definitions when it lists them), apply the commit with the core's shared
//! rules ([`AccountMirror::apply`]), and write back the difference. The rules
//! exist once, so SQL and memory stores can't drift; the conformance suite
//! checks both.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use mailtide::commit::{Commit, CommitOutcome, DeletionEvidence, LabelOp, MessageOp, Rejection};
use mailtide::drafts::{
    DraftIndex, DraftOp, DraftRecord, DraftRecorded, DraftState, DraftView, DraftWrite, SendEnd,
};
use mailtide::gmail::{Label, LabelColor, LabelKind, LabelListVisibility, MessageListVisibility};
use mailtide::ids::{
    AccountId, DraftId, EngineVersion, Epoch, HistoryId, LabelId, MessageId, ThreadId, Timestamp,
};
use mailtide::jobs::{
    AttemptId, ClaimContext, ClaimOutcome, ClaimedJob, ClaimedWork, Job, JobKind, JobState,
    Prioritised,
};
use mailtide::labels::LabelSet;
use mailtide::mirror::{
    AccountMirror, AccountRecord, Content, FactsUpdate, Located, MessageRecord, MessageView,
    StoredContent,
};
use mailtide::outbox::{ActionRequest, Intent, IntentOp, plan_delivery};
use mailtide::quota::Priority;
use mailtide::reconcile::ListScope;
use mailtide::revision::{
    AckOutcome, Change, ChangeEntry, ChangeFeed, ChangePage, ConsumerCursor, ConsumerList,
    ConsumerName, FeedCursor, FeedPosition, Retention, StateSeq, cut_page, page_window,
};
use mailtide::state::StoreSnapshot;
use mailtide::status::{
    AccountStatus, ActionCounts, ForgetOutcome, JobCounts, ResumeOutcome, TrackOutcome,
};
use mailtide::submission::{
    SubmissionKey, SubmissionRow, SubmissionState, SubmissionWrite, SubmissionWritten,
};
use mailtide::thread::ThreadView;
use sqlx::any::AnyRow;
use sqlx::pool::PoolOptions;
use sqlx::{Any, AssertSqlSafe, Pool, Row, Transaction};

mod audit;

type AnyPool = Pool<Any>;

use crate::StoreError;
use crate::config::{Connect, DatabaseUrl, PoolConfig};

/// Which database the URL points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// Postgres: account rows are locked with `FOR UPDATE`.
    Postgres,
    /// `SQLite`: one connection serializes everything.
    Sqlite,
}

/// Schema migrations, in order; `mailtide_schema` records which ran. Never
/// edit a released migration: append a new one. Table names are prefixed so
/// the schema can live inside a host's database.
const MIGRATIONS: &[&[&str]] = &[&[
    "CREATE TABLE IF NOT EXISTS mailtide_accounts (
        account TEXT PRIMARY KEY, epoch BIGINT NOT NULL, engine_version BIGINT NOT NULL,
        cursor BIGINT, sync TEXT NOT NULL, watch_expiration BIGINT, last_activity BIGINT,
        cooldown TEXT, rng_seed BIGINT NOT NULL, seq BIGINT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS mailtide_messages (
        account TEXT NOT NULL, id TEXT NOT NULL, thread_id TEXT NOT NULL, labels TEXT NOT NULL,
        checkpoint BIGINT, unverified BIGINT, content TEXT, PRIMARY KEY (account, id))",
    "CREATE INDEX IF NOT EXISTS mailtide_messages_checkpoint ON mailtide_messages (account, checkpoint)",
    "CREATE TABLE IF NOT EXISTS mailtide_tombstones (
        account TEXT NOT NULL, id TEXT NOT NULL, checkpoint BIGINT NOT NULL, PRIMARY KEY (account, id))",
    "CREATE TABLE IF NOT EXISTS mailtide_jobs (
        account TEXT NOT NULL, kind BIGINT NOT NULL, message TEXT NOT NULL, priority BIGINT NOT NULL,
        failed_attempts BIGINT NOT NULL, state TEXT NOT NULL, due_at BIGINT, attempt BIGINT,
        failure TEXT, PRIMARY KEY (account, kind, message))",
    "CREATE INDEX IF NOT EXISTS mailtide_jobs_ready ON mailtide_jobs (account, state, priority, due_at)",
    "CREATE TABLE IF NOT EXISTS mailtide_seen (
        account TEXT NOT NULL, id TEXT NOT NULL, scope TEXT NOT NULL, PRIMARY KEY (account, id, scope))",
    "CREATE TABLE IF NOT EXISTS mailtide_labels (
        account TEXT NOT NULL, id TEXT NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL,
        PRIMARY KEY (account, id))",
    "CREATE TABLE IF NOT EXISTS mailtide_intents (
        account TEXT NOT NULL, message TEXT NOT NULL, label TEXT NOT NULL, intent TEXT NOT NULL,
        PRIMARY KEY (account, message, label))",
    "CREATE TABLE IF NOT EXISTS mailtide_changes (
        account TEXT NOT NULL, seq BIGINT NOT NULL, idx BIGINT NOT NULL, change TEXT NOT NULL,
        PRIMARY KEY (account, seq, idx))",
],
// 2: change-feed compaction.
&["ALTER TABLE mailtide_accounts ADD COLUMN log_floor BIGINT NOT NULL DEFAULT 0"],
// 3: account generations (feed positions name an incarnation).
&["ALTER TABLE mailtide_accounts ADD COLUMN generation BIGINT NOT NULL DEFAULT 0"],
// 4: when the label definitions were last listed (NULL: never). Disposable:
// dropping it only makes the next run list them again.
&["ALTER TABLE mailtide_accounts ADD COLUMN labels_refreshed_at BIGINT"],
// 5: version only. State strings may name an entry inside a commit
// (`mt2-<gen>.<seq>.<idx>`), which older builds can't decode: they must see
// the database as ahead.
&[],
// 6: thread lookups (thread views, thread action admission) read the
// thread's rows, not every row of the account. Only an index: dropping it
// makes them slow, never wrong.
&["CREATE INDEX IF NOT EXISTS mailtide_messages_thread ON mailtide_messages (account, thread_id, id)"],
// 7: consumer cursors. Older builds would compact past them, and `idx` set
// is a position inside a commit, which they can't read.
&["CREATE TABLE IF NOT EXISTS mailtide_consumers (
    account TEXT NOT NULL, name TEXT NOT NULL, seq BIGINT NOT NULL, idx BIGINT,
    seen_at BIGINT NOT NULL, PRIMARY KEY (account, name))"],

// 8: message facts, inside the existing content record, and the
// FactsRefreshed feed entry. Version only: nothing to migrate, but an older
// build would drop the facts from every row it rewrites and cannot read the
// entry, so it must see this database as ahead.
&[],
// 9: label editing. Each definition's visibility and colour (JSON; NULL:
// unknown until the next listing), and on the account the label generation
// (bumped by every local label write, it fences older listings) and the
// deleted-label memory (JSON: id to when it was deleted; NULL: none).
&[
    "ALTER TABLE mailtide_labels ADD COLUMN attrs TEXT",
    "ALTER TABLE mailtide_accounts ADD COLUMN label_generation BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE mailtide_accounts ADD COLUMN deleted_labels TEXT",
],
// 10: the draft index (SPEC §7a): a row per draft naming its current message,
// a generation that fences listings, and when the drafts were last listed.
// Raises the version so an older build refuses a database holding draft
// feed entries and RefreshDrafts jobs it cannot read.
&[
    "CREATE TABLE IF NOT EXISTS mailtide_drafts (
        account TEXT NOT NULL, id TEXT NOT NULL, message TEXT NOT NULL, thread_id TEXT NOT NULL,
        tracking_id TEXT, state TEXT NOT NULL, since BIGINT NOT NULL, PRIMARY KEY (account, id))",
    "CREATE UNIQUE INDEX IF NOT EXISTS mailtide_drafts_message ON mailtide_drafts (account, message)",
    "CREATE INDEX IF NOT EXISTS mailtide_drafts_tracking ON mailtide_drafts (account, tracking_id)",
    "ALTER TABLE mailtide_accounts ADD COLUMN drafts_generation BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE mailtide_accounts ADD COLUMN drafts_refreshed_at BIGINT",
],
// 11: audit completion time, durable continuation and temporary membership.
&[
    "ALTER TABLE mailtide_accounts ADD COLUMN audited_at BIGINT",
    "ALTER TABLE mailtide_accounts ADD COLUMN audit_progress TEXT",
    "ALTER TABLE mailtide_accounts ADD COLUMN audit_schedule TEXT",
    "ALTER TABLE mailtide_labels ADD COLUMN defined_seq BIGINT NOT NULL DEFAULT 0",
    "CREATE TABLE mailtide_generation (singleton BIGINT PRIMARY KEY, generation BIGINT NOT NULL)",
    "INSERT INTO mailtide_generation (singleton, generation)
       SELECT 1, COALESCE(MAX(CASE WHEN generation > 4611686018427387904 THEN generation ELSE 4611686018427387904 END), 4611686018427387904)
       FROM mailtide_accounts",
    "CREATE TABLE mailtide_audit_seen (
        account TEXT NOT NULL, id TEXT NOT NULL, PRIMARY KEY (account, id))",
],
// 12: durable raw submissions and the tracking tokens shared with draft sends.
&[
    "CREATE TABLE mailtide_submissions (
       account TEXT NOT NULL, key TEXT NOT NULL, tracking_id TEXT NOT NULL,
       state TEXT NOT NULL, due_at BIGINT, data TEXT NOT NULL, PRIMARY KEY(account, key))",
    "CREATE UNIQUE INDEX mailtide_submissions_tracking ON mailtide_submissions(account, tracking_id)",
    "CREATE INDEX mailtide_submissions_due ON mailtide_submissions(account, due_at, key)",
    "CREATE TABLE mailtide_send_tokens (
       account TEXT NOT NULL, tracking_id TEXT NOT NULL, owner TEXT NOT NULL,
       PRIMARY KEY(account, tracking_id))",
]];

/// The schema version this build expects.
#[allow(clippy::cast_possible_truncation)] // a handful of migrations, checked at compile time below
pub const SCHEMA_VERSION: u32 = MIGRATIONS.len() as u32;
const _: () = assert!(MIGRATIONS.len() < u32::MAX as usize);

const SCHEMA_TABLE: &str =
    "CREATE TABLE IF NOT EXISTS mailtide_schema (version BIGINT PRIMARY KEY)";

/// One schema migration as SQL, for hosts that run migrations with their own
/// tool (e.g. an Alembic revision executing each statement in order). Each
/// migration records itself in `mailtide_schema`; apply them in order,
/// skipping versions already recorded there. Postgres and `SQLite` share it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Migration {
    /// Its version (1, 2, ...).
    pub version: u32,
    /// The statements, bookkeeping included.
    pub statements: Vec<String>,
}

/// Every migration, in order.
#[must_use]
pub fn migrations() -> Vec<Migration> {
    MIGRATIONS
        .iter()
        .zip(1_u32..)
        .map(|(statements, version)| {
            let mut all = vec![SCHEMA_TABLE.to_owned()];
            all.extend(statements.iter().map(|s| (*s).to_owned()));
            all.push(format!(
                "INSERT INTO mailtide_schema (version) VALUES ({version})"
            ));
            Migration {
                version,
                statements: all,
            }
        })
        .collect()
}

/// Where a database's schema stands against this build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaState {
    /// Every migration this build knows is applied.
    Current {
        /// The version.
        version: u32,
    },
    /// Migrations are missing: run them before syncing.
    Behind {
        /// The newest applied (0: none).
        applied: u32,
        /// The newest this build expects.
        expected: u32,
    },
    /// A newer mailtide migrated this database: this build must not write.
    Ahead {
        /// The newest applied.
        applied: u32,
        /// The newest this build knows.
        expected: u32,
    },
}

/// A store in a SQL database.
#[derive(Debug, Clone)]
pub struct SqlStore {
    pool: AnyPool,
    dialect: Dialect,
    /// Set once the schema was seen current; shared by clones.
    schema_current: Arc<AtomicBool>,
}

fn unavailable(error: &sqlx::Error) -> StoreError {
    StoreError::Unavailable(error.to_string())
}

fn corrupt(what: &str, error: impl std::fmt::Display) -> StoreError {
    StoreError::Corrupt(format!("{what}: {error}"))
}

/// A number as the database stores it (`BIGINT`). A value that doesn't fit
/// fails the operation: storing a different number would silently move a
/// cursor or a checkpoint.
fn int(field: &str, value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| corrupt(field, format!("{value} exceeds BIGINT")))
}

/// A stored number back; a negative one is corrupt, never zero.
fn uint(field: &str, value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| corrupt(field, format!("negative value {value}")))
}

fn json<T: serde::Serialize>(value: &T) -> Result<String, StoreError> {
    serde_json::to_string(value).map_err(|e| corrupt("encode", e))
}

fn parse<T: serde::de::DeserializeOwned>(what: &str, text: &str) -> Result<T, StoreError> {
    serde_json::from_str(text).map_err(|e| corrupt(what, e))
}

// Column encodings shared by reads and writes.

/// A job's kind and message key; an account-level job's key is empty.
fn kind_code(kind: &JobKind) -> (i64, &str) {
    match kind {
        JobKind::Import(id) => (0, id.0.as_str()),
        JobKind::Verify(id) => (1, id.0.as_str()),
        JobKind::BlobCleanup(id) => (2, id.0.as_str()),
        JobKind::Deliver(id) => (3, id.0.as_str()),
        JobKind::RefreshDrafts => (4, ""),
        JobKind::SendSubmissions => (5, ""),
    }
}

fn kind_from(code: i64, message: String) -> Result<JobKind, StoreError> {
    let id = MessageId(message);
    match code {
        0 => Ok(JobKind::Import(id)),
        1 => Ok(JobKind::Verify(id)),
        2 => Ok(JobKind::BlobCleanup(id)),
        3 => Ok(JobKind::Deliver(id)),
        4 if id.0.is_empty() => Ok(JobKind::RefreshDrafts),
        5 if id.0.is_empty() => Ok(JobKind::SendSubmissions),
        other => Err(corrupt("job kind", other)),
    }
}

fn priority_code(priority: Priority) -> i64 {
    match priority {
        Priority::Backfill => 0,
        Priority::Live => 1,
        Priority::Foreground => 2,
    }
}

fn priority_from(code: i64) -> Result<Priority, StoreError> {
    match code {
        0 => Ok(Priority::Backfill),
        1 => Ok(Priority::Live),
        2 => Ok(Priority::Foreground),
        other => Err(corrupt("job priority", other)),
    }
}

fn scope_code(scope: &ListScope) -> String {
    match scope {
        ListScope::All => "*".to_owned(),
        ListScope::Label(label) => format!("label:{}", label.0),
    }
}

fn scope_from(text: &str) -> Result<ListScope, StoreError> {
    match text {
        "*" => Ok(ListScope::All),
        other => other
            .strip_prefix("label:")
            .map(|label| ListScope::Label(LabelId(label.to_owned())))
            .ok_or_else(|| corrupt("seen scope", other)),
    }
}

impl SqlStore {
    /// Open a store at `url` with `pool` limits (Postgres only; `SQLite`
    /// always uses one connection, opened eagerly).
    pub async fn connect(url: &DatabaseUrl, pool: &PoolConfig) -> Result<Self, StoreError> {
        sqlx::any::install_default_drivers();
        let (dialect, options, connect) = match url {
            // One connection: SQLite serializes writers, and an in-memory
            // database lives only as long as its connection.
            DatabaseUrl::SqliteMemory(_) | DatabaseUrl::SqliteFile(_) => (
                Dialect::Sqlite,
                PoolOptions::<Any>::new()
                    .max_connections(1)
                    .min_connections(1)
                    .idle_timeout(None)
                    .max_lifetime(None),
                Connect::Eager,
            ),
            DatabaseUrl::Postgres(_) => (
                Dialect::Postgres,
                PoolOptions::<Any>::new()
                    .max_connections(pool.max_connections())
                    .min_connections(pool.min_connections())
                    .acquire_timeout(pool.acquire_timeout())
                    .idle_timeout(pool.idle_timeout())
                    .max_lifetime(pool.max_lifetime()),
                pool.connect(),
            ),
        };
        let pool = match connect {
            Connect::Lazy => options.connect_lazy(url.as_str()),
            Connect::Eager => options.connect(url.as_str()).await,
        }
        .map_err(|e| unavailable(&e))?;
        Ok(Self {
            pool,
            dialect,
            schema_current: Arc::default(),
        })
    }

    /// Where this database's schema stands (read-only; works on an empty
    /// database).
    pub async fn schema(&self) -> Result<SchemaState, StoreError> {
        let exists = match self.dialect {
            Dialect::Postgres => {
                "SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_name = 'mailtide_schema' AND table_schema = ANY (current_schemas(false))"
            }
            Dialect::Sqlite => {
                "SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = 'mailtide_schema'"
            }
        };
        let row = sqlx::query(exists)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| unavailable(&e))?;
        let tables: i64 = row.try_get("n").map_err(|e| corrupt("schema", e))?;
        let applied = if tables == 0 {
            0
        } else {
            let row = sqlx::query("SELECT COALESCE(MAX(version), 0) AS v FROM mailtide_schema")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| unavailable(&e))?;
            let v: i64 = row.try_get("v").map_err(|e| corrupt("schema version", e))?;
            u32::try_from(v).map_err(|e| corrupt("schema version", e))?
        };
        Ok(match applied.cmp(&SCHEMA_VERSION) {
            std::cmp::Ordering::Equal => SchemaState::Current { version: applied },
            std::cmp::Ordering::Less => SchemaState::Behind {
                applied,
                expected: SCHEMA_VERSION,
            },
            std::cmp::Ordering::Greater => SchemaState::Ahead {
                applied,
                expected: SCHEMA_VERSION,
            },
        })
    }

    /// Refuse to read or write a database whose schema this build doesn't
    /// match: one that is behind would fail mid-operation with a SQL error,
    /// and one a newer mailtide migrated may hold rows this build would
    /// misread or drop. Every [`crate::Store`] method checks it first (in
    /// the inherent method, for those the wrappers call directly). The
    /// answer is cached once current, shared by clones: one query per
    /// store, not one per call.
    async fn ensure_schema(&self) -> Result<(), StoreError> {
        if self.schema_current.load(Ordering::Relaxed) {
            return Ok(());
        }
        match self.schema().await? {
            SchemaState::Current { .. } => {
                self.schema_current.store(true, Ordering::Relaxed);
                Ok(())
            }
            SchemaState::Behind { applied, expected }
            | SchemaState::Ahead { applied, expected } => {
                Err(StoreError::SchemaOutOfDate { applied, expected })
            }
        }
    }

    /// Whether the database answers within the pool's acquire timeout.
    pub async fn ping(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| unavailable(&e))
    }

    /// Bring the schema up to date. Safe to call on every start and from
    /// several processes at once.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        // One transaction under one lock: concurrent migrators (several
        // processes starting at once) wait instead of racing on DDL, which
        // Postgres can fail even with IF NOT EXISTS.
        let mut tx = self.begin().await?;
        if self.dialect == Dialect::Postgres {
            sqlx::query("SELECT pg_advisory_xact_lock(7366716)")
                .execute(&mut *tx)
                .await
                .map_err(|e| unavailable(&e))?;
        }
        sqlx::query(SCHEMA_TABLE)
            .execute(&mut *tx)
            .await
            .map_err(|e| unavailable(&e))?;
        for (index, migration) in MIGRATIONS.iter().enumerate() {
            let version = i64::try_from(index + 1).unwrap_or(i64::MAX);
            let applied = sqlx::query("SELECT version FROM mailtide_schema WHERE version = $1")
                .bind(version)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| unavailable(&e))?
                .is_some();
            if applied {
                continue;
            }
            for statement in *migration {
                sqlx::query(*statement)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| unavailable(&e))?;
            }
            sqlx::query("INSERT INTO mailtide_schema (version) VALUES ($1)")
                .bind(version)
                .execute(&mut *tx)
                .await
                .map_err(|e| unavailable(&e))?;
        }
        tx.commit().await.map_err(|e| unavailable(&e))
    }

    /// The text a query's first column holds in its first row (tests only).
    #[cfg(test)]
    pub(crate) async fn text_for_tests(&self, query: &str) -> Result<String, StoreError> {
        let row = sqlx::query(AssertSqlSafe(query.to_owned()))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| unavailable(&e))?;
        row.try_get(0).map_err(|e| corrupt("text", e))
    }

    /// The steps of `SQLite`'s plan for `statement` with `binds` (tests only).
    #[cfg(test)]
    pub(crate) async fn plan_for_tests(
        &self,
        statement: &str,
        binds: &[&str],
    ) -> Result<Vec<String>, StoreError> {
        let mut query = sqlx::query(AssertSqlSafe(format!("EXPLAIN QUERY PLAN {statement}")));
        for bind in binds {
            query = query.bind(*bind);
        }
        query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| unavailable(&e))?
            .iter()
            .map(|row| row.try_get("detail").map_err(|e| corrupt("detail", e)))
            .collect()
    }

    /// Run one statement (tests only).
    #[cfg(test)]
    pub(crate) async fn execute_for_tests(&self, statement: &str) -> Result<(), StoreError> {
        sqlx::query(AssertSqlSafe(statement.to_owned()))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| unavailable(&e))
    }

    /// Delete every row (tests only).
    #[cfg(test)]
    pub(crate) async fn reset_for_tests(&self) -> Result<(), StoreError> {
        for table in [
            "mailtide_accounts",
            "mailtide_messages",
            "mailtide_tombstones",
            "mailtide_jobs",
            "mailtide_seen",
            "mailtide_audit_seen",
            "mailtide_labels",
            "mailtide_intents",
            "mailtide_changes",
            "mailtide_consumers",
            "mailtide_drafts",
        ] {
            // Table names are constants, never input.
            sqlx::query(AssertSqlSafe(format!("DELETE FROM {table}")))
                .execute(&self.pool)
                .await
                .map_err(|e| unavailable(&e))?;
        }
        Ok(())
    }

    fn lock_suffix(&self) -> &'static str {
        match self.dialect {
            Dialect::Postgres => " FOR UPDATE",
            Dialect::Sqlite => "",
        }
    }

    async fn begin(&self) -> Result<Transaction<'static, Any>, StoreError> {
        self.pool.begin().await.map_err(|e| unavailable(&e))
    }

    /// The account row, locked for the transaction (Postgres).
    async fn load_account(
        &self,
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
    ) -> Result<Option<(AccountRecord, StateSeq)>, StoreError> {
        self.read_account(tx, account, self.lock_suffix()).await
    }

    /// The account row without locking it, for reads that must not wait on
    /// a running commit.
    async fn load_account_unlocked(
        &self,
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
    ) -> Result<Option<(AccountRecord, StateSeq)>, StoreError> {
        self.read_account(tx, account, "").await
    }

    async fn count(
        &self,
        tx: &mut Transaction<'static, Any>,
        sql: &'static str,
        account: &AccountId,
    ) -> Result<u64, StoreError> {
        let row = sqlx::query(sql)
            .bind(account.as_str())
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
        let n: i64 = row.try_get("n").map_err(|e| corrupt("count", e))?;
        uint("count", n)
    }

    async fn read_account(
        &self,
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
        lock: &'static str,
    ) -> Result<Option<(AccountRecord, StateSeq)>, StoreError> {
        let sql = format!(
            "SELECT epoch, engine_version, cursor, sync, watch_expiration, last_activity, cooldown, rng_seed, seq, log_floor, generation, labels_refreshed_at,
               label_generation, deleted_labels, drafts_generation, drafts_refreshed_at, audited_at, audit_progress, audit_schedule
             FROM mailtide_accounts WHERE account = $1{lock}"
        );
        // Only a constant lock clause is appended.
        let row = sqlx::query(AssertSqlSafe(sql))
            .bind(account.as_str())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let get_i = |name: &str| row.try_get::<i64, _>(name).map_err(|e| corrupt(name, e));
        let get_oi = |name: &str| {
            row.try_get::<Option<i64>, _>(name)
                .map_err(|e| corrupt(name, e))
        };
        let sync: String = row.try_get("sync").map_err(|e| corrupt("sync", e))?;
        let cooldown: Option<String> = row
            .try_get("cooldown")
            .map_err(|e| corrupt("cooldown", e))?;
        let deleted_labels: Option<String> = row
            .try_get("deleted_labels")
            .map_err(|e| corrupt("deleted_labels", e))?;
        let audit_progress: Option<String> = row
            .try_get("audit_progress")
            .map_err(|e| corrupt("audit_progress", e))?;
        let audit_schedule: Option<String> = row
            .try_get("audit_schedule")
            .map_err(|e| corrupt("audit_schedule", e))?;
        let record = AccountRecord {
            epoch: Epoch(uint("epoch", get_i("epoch")?)?),
            engine_version: EngineVersion(
                u32::try_from(get_i("engine_version")?)
                    .map_err(|e| corrupt("engine_version", e))?,
            ),
            cursor: get_oi("cursor")?
                .map(|c| uint("history id", c).map(HistoryId))
                .transpose()?,
            sync: parse("sync", &sync)?,
            watch_expiration: get_oi("watch_expiration")?.map(Timestamp),
            last_activity: get_oi("last_activity")?.map(Timestamp),
            cooldown: cooldown
                .as_deref()
                .map(|c| parse("cooldown", c))
                .transpose()?,
            rng_seed: get_i("rng_seed")?.cast_unsigned(),
            labels_refreshed_at: get_oi("labels_refreshed_at")?.map(Timestamp),
            audited_at: get_oi("audited_at")?.map(Timestamp),
            audit_progress: audit_progress
                .as_deref()
                .map(|value| parse("audit_progress", value))
                .transpose()?,
            audit_schedule: audit_schedule
                .as_deref()
                .map(|value| parse("audit_schedule", value))
                .transpose()?
                .unwrap_or_default(),
            log_floor: StateSeq(uint("log_floor", get_i("log_floor")?)?),
            generation: uint("generation", get_i("generation")?)?,
            label_generation: uint("label_generation", get_i("label_generation")?)?,
            deleted_labels: deleted_labels
                .as_deref()
                .map(|text| parse::<BTreeMap<LabelId, i64>>("deleted_labels", text))
                .transpose()?
                .unwrap_or_default()
                .into_iter()
                .map(|(id, at)| (id, Timestamp(at)))
                .collect(),
            drafts_generation: uint("drafts_generation", get_i("drafts_generation")?)?,
            drafts_refreshed_at: get_oi("drafts_refreshed_at")?.map(Timestamp),
        };
        Ok(Some((record, StateSeq(uint("seq", get_i("seq")?)?))))
    }

    async fn write_account(
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
        record: &AccountRecord,
        seq: StateSeq,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO mailtide_accounts
               (account, epoch, engine_version, cursor, sync, watch_expiration, last_activity, cooldown, rng_seed, seq, log_floor, generation, labels_refreshed_at,
                label_generation, deleted_labels, drafts_generation, drafts_refreshed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
             ON CONFLICT (account) DO UPDATE SET
               epoch = excluded.epoch, engine_version = excluded.engine_version, cursor = excluded.cursor,
               sync = excluded.sync, watch_expiration = excluded.watch_expiration,
               last_activity = excluded.last_activity, cooldown = excluded.cooldown,
               rng_seed = excluded.rng_seed, seq = excluded.seq, log_floor = excluded.log_floor,
               generation = excluded.generation, labels_refreshed_at = excluded.labels_refreshed_at,
               label_generation = excluded.label_generation, deleted_labels = excluded.deleted_labels,
               drafts_generation = excluded.drafts_generation, drafts_refreshed_at = excluded.drafts_refreshed_at",
        )
        .bind(account.as_str())
        .bind(int("epoch", record.epoch.0)?)
        .bind(i64::from(record.engine_version.0))
        .bind(record.cursor.map(|c| int("cursor", c.0)).transpose()?)
        .bind(json(&record.sync)?)
        .bind(record.watch_expiration.map(|t| t.0))
        .bind(record.last_activity.map(|t| t.0))
        .bind(record.cooldown.as_ref().map(json).transpose()?)
        .bind(record.rng_seed.cast_signed())
        .bind(int("seq", seq.0)?)
        .bind(int("log_floor", record.log_floor.0)?)
        .bind(int("generation", record.generation)?)
        .bind(record.labels_refreshed_at.map(|t| t.0))
        .bind(int("label_generation", record.label_generation)?)
        .bind(
            (!record.deleted_labels.is_empty())
                .then(|| {
                    json(
                        &record
                            .deleted_labels
                            .iter()
                            .map(|(id, at)| (id, at.0))
                            .collect::<BTreeMap<_, _>>(),
                    )
                })
                .transpose()?,
        )
        .bind(int("drafts_generation", record.drafts_generation)?)
        .bind(record.drafts_refreshed_at.map(|t| t.0))
        .execute(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
        Ok(())
    }

    /// The account row, locked, with its consumer cursors: what acks and
    /// compactions decide over. `None` for an unknown account.
    async fn load_consumers(
        &self,
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
    ) -> Result<Option<AccountMirror>, StoreError> {
        let Some((record, seq)) = self.load_account(tx, account).await? else {
            return Ok(None);
        };
        let mut mirror = AccountMirror::new(account.clone(), record.generation);
        mirror.record = record;
        mirror.seq = seq;
        for cursor in read_consumers(tx, account).await? {
            mirror.consumers.insert(cursor.name.clone(), cursor);
        }
        Ok(Some(mirror))
    }

    // ------------------------------------------------------------ partial mirror

    /// Load the rows a commit can touch into a partial mirror.
    async fn load_partial(
        &self,
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
        record: AccountRecord,
        seq: StateSeq,
        commit: &Commit,
    ) -> Result<AccountMirror, StoreError> {
        let mut mirror = AccountMirror::new(account.clone(), record.generation);
        mirror.record = record;
        mirror.seq = seq;
        let mut ids: BTreeSet<MessageId> = BTreeSet::new();
        let mut finish_baseline: Option<HistoryId> = None;
        for op in &commit.messages {
            match op {
                MessageOp::Observe(observation) | MessageOp::MarkImported { observation, .. } => {
                    ids.insert(observation.id.clone());
                }
                MessageOp::PatchLabels { id, .. }
                | MessageOp::Tombstone { id, .. }
                | MessageOp::Listed { id, .. } => {
                    ids.insert(id.clone());
                }
                MessageOp::FinishReconcile { baseline } => finish_baseline = Some(*baseline),
            }
        }
        let job_messages = commit
            .claim
            .iter()
            .map(|claim| &claim.kind)
            .chain(commit.jobs.iter().map(|op| match op {
                mailtide::jobs::JobOp::Enqueue(job) => &job.kind,
                mailtide::jobs::JobOp::Finish { claim, .. } => &claim.kind,
            }))
            .filter_map(JobKind::message);
        ids.extend(job_messages.cloned());
        // A listing is checked against the tombstones of what it names.
        for op in &commit.drafts {
            match op {
                DraftOp::Reconcile(listing) => {
                    ids.extend(listing.listed.iter().map(|d| d.message.clone()));
                }
            }
        }
        for op in &commit.intents {
            match op {
                IntentOp::Request(ActionRequest::Messages { ids: targets, .. }) => {
                    ids.extend(targets.iter().cloned());
                }
                IntentOp::Request(ActionRequest::Thread { id: thread, .. }) => {
                    let rows = sqlx::query(THREAD_MEMBERS)
                        .bind(account.as_str())
                        .bind(thread.0.as_str())
                        .fetch_all(&mut **tx)
                        .await
                        .map_err(|e| unavailable(&e))?;
                    for row in rows {
                        ids.insert(MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?));
                    }
                }
                IntentOp::Delivered { versions, .. } | IntentOp::Block { versions, .. } => {
                    ids.extend(versions.iter().map(|d| d.message.clone()));
                }
            }
        }
        // A label that goes leaves every row that carries it and every
        // intent on it: load those too (and the definitions, which a listing
        // is compared with).
        if !commit.labels.is_empty() {
            mirror.labels = load_labels(&mut **tx, account).await?;
            let removed: BTreeSet<LabelId> = commit
                .labels
                .iter()
                .flat_map(|op| {
                    mailtide::mirror::removed_labels(
                        &mirror.labels,
                        mirror.record.label_generation,
                        op,
                    )
                })
                .collect();
            ids.extend(carrying(tx, account, &removed).await?);
        }
        if let Some(baseline) = finish_baseline {
            let rows = sqlx::query(
                "SELECT id FROM mailtide_messages WHERE account = $1 AND (checkpoint IS NULL OR checkpoint < $2)
                 UNION SELECT id FROM mailtide_seen WHERE account = $1",
            )
            .bind(account.as_str())
            .bind(int("baseline", baseline.0)?)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
            for row in rows {
                ids.insert(MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?));
            }
        }
        let ids: Vec<MessageId> = ids.into_iter().collect();
        Self::load_rows(tx, account, &ids, &mut mirror).await?;
        Self::load_account_jobs(tx, account, &mut mirror).await?;
        Self::load_commit_drafts(tx, account, commit, &ids, &mut mirror).await?;
        load_import_submissions(tx, account, &commit.messages, &mut mirror).await?;
        Ok(mirror)
    }

    /// The draft rows a commit can touch: all of them for a listing;
    /// otherwise those naming its messages (`ids`), and those whose
    /// tracking id an imported message carries.
    async fn load_commit_drafts(
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
        commit: &Commit,
        ids: &[MessageId],
        mirror: &mut AccountMirror,
    ) -> Result<(), StoreError> {
        if commit.drafts.is_empty() {
            let tracking: Vec<&str> = commit
                .messages
                .iter()
                .filter_map(|op| match op {
                    MessageOp::MarkImported { facts, .. } => {
                        facts.as_ref().and_then(|f| f.tracking_id.as_deref())
                    }
                    MessageOp::Observe(_)
                    | MessageOp::PatchLabels { .. }
                    | MessageOp::Tombstone { .. }
                    | MessageOp::Listed { .. }
                    | MessageOp::FinishReconcile { .. } => None,
                })
                .collect();
            let names: Vec<&str> = ids.iter().map(|id| id.0.as_str()).collect();
            load_drafts_where(tx, account, "message", &names, mirror).await?;
            load_drafts_where(tx, account, "tracking_id", &tracking, mirror).await?;
        } else {
            mirror.drafts = load_drafts(&mut **tx, account).await?;
        }
        Ok(())
    }

    /// Load the rows of `ids` into `mirror`: five set-based queries per chunk
    /// of ids, not five per message (a mailbox's first reconcile touches every
    /// message; per-row loading took longer than any timeout on large ones).
    async fn load_rows(
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
        ids: &[MessageId],
        mirror: &mut AccountMirror,
    ) -> Result<(), StoreError> {
        for chunk in ids.chunks(LOAD_CHUNK) {
            let placeholders = (2..chunk.len() + 2)
                .map(|n| format!("${n}"))
                .collect::<Vec<_>>()
                .join(", ");
            let fetch = |sql: String| {
                let mut query = sqlx::query(AssertSqlSafe(sql)).bind(account.as_str());
                for id in chunk {
                    query = query.bind(id.0.as_str());
                }
                query
            };
            // Only numbered placeholders are interpolated; ids are bound.
            for row in fetch(format!(
                "SELECT id, thread_id, labels, checkpoint, unverified, content FROM mailtide_messages
                 WHERE account = $1 AND id IN ({placeholders})"
            ))
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?
            {
                let id = MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?);
                let record = message_from_row(&id, &row)?;
                mirror.messages.insert(id, record);
            }
            for row in fetch(format!(
                "SELECT id, checkpoint FROM mailtide_tombstones WHERE account = $1 AND id IN ({placeholders})"
            ))
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?
            {
                let id = MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?);
                let checkpoint: i64 = row
                    .try_get("checkpoint")
                    .map_err(|e| corrupt("checkpoint", e))?;
                mirror
                    .tombstones
                    .insert(id, HistoryId(uint("checkpoint", checkpoint)?));
            }
            for row in fetch(format!(
                "SELECT kind, message, priority, failed_attempts, state, due_at, attempt, failure
                 FROM mailtide_jobs WHERE account = $1 AND message IN ({placeholders})"
            ))
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?
            {
                let job = job_from_row(&row)?;
                mirror.jobs.insert(job.kind.clone(), job);
            }
            for row in fetch(format!(
                "SELECT intent FROM mailtide_intents WHERE account = $1 AND message IN ({placeholders})"
            ))
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?
            {
                let text: String = row.try_get("intent").map_err(|e| corrupt("intent", e))?;
                let intent = parse::<Intent>("intent", &text)?;
                mirror
                    .intents
                    .insert((intent.message.clone(), intent.label.clone()), intent);
            }
            for row in fetch(format!(
                "SELECT id, scope FROM mailtide_seen WHERE account = $1 AND id IN ({placeholders})"
            ))
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?
            {
                let id = MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?);
                let scope: String = row.try_get("scope").map_err(|e| corrupt("scope", e))?;
                mirror
                    .seen
                    .entry(id)
                    .or_default()
                    .insert(scope_from(&scope)?);
            }
        }
        Ok(())
    }

    /// Load the account-level jobs (an empty message key): a commit that
    /// enqueues one must see it, or it would replace a running claim.
    async fn load_account_jobs(
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
        mirror: &mut AccountMirror,
    ) -> Result<(), StoreError> {
        for row in sqlx::query(
            "SELECT kind, message, priority, failed_attempts, state, due_at, attempt, failure
             FROM mailtide_jobs WHERE account = $1 AND message = ''",
        )
        .bind(account.as_str())
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?
        {
            let job = job_from_row(&row)?;
            mirror.jobs.insert(job.kind.clone(), job);
        }
        Ok(())
    }

    /// Write what changed between `before` and `after`.
    async fn write_diff(
        tx: &mut Transaction<'static, Any>,
        before: &AccountMirror,
        after: &AccountMirror,
    ) -> Result<(), StoreError> {
        let account = after.account.as_str();
        Self::write_account(tx, &after.account, &after.record, after.seq).await?;
        let ids: BTreeSet<&MessageId> = before
            .messages
            .keys()
            .chain(after.messages.keys())
            .collect();
        let mut changed = Vec::new();
        for id in ids {
            match (before.messages.get(id), after.messages.get(id)) {
                (_, Some(record)) if before.messages.get(id) != Some(record) => {
                    changed.push(record);
                }
                (Some(_), None) => {
                    sqlx::query("DELETE FROM mailtide_messages WHERE account = $1 AND id = $2")
                        .bind(account)
                        .bind(id.0.as_str())
                        .execute(&mut **tx)
                        .await
                        .map_err(|e| unavailable(&e))?;
                }
                (_, Some(_)) | (None, None) => {}
            }
        }
        put_messages(tx, account, &changed).await?;
        for (id, checkpoint) in &after.tombstones {
            if before.tombstones.get(id) != Some(checkpoint) {
                sqlx::query(
                    "INSERT INTO mailtide_tombstones (account, id, checkpoint) VALUES ($1, $2, $3)
                     ON CONFLICT (account, id) DO UPDATE SET checkpoint = excluded.checkpoint",
                )
                .bind(account)
                .bind(id.0.as_str())
                .bind(int("checkpoint", checkpoint.0)?)
                .execute(&mut **tx)
                .await
                .map_err(|e| unavailable(&e))?;
            }
        }
        let kinds: BTreeSet<&JobKind> = before.jobs.keys().chain(after.jobs.keys()).collect();
        for kind in kinds {
            match (before.jobs.get(kind), after.jobs.get(kind)) {
                (_, Some(job)) if before.jobs.get(kind) != Some(job) => {
                    put_job(tx, account, job).await?;
                }
                (Some(_), None) => {
                    let (code, message) = kind_code(kind);
                    sqlx::query("DELETE FROM mailtide_jobs WHERE account = $1 AND kind = $2 AND message = $3")
                        .bind(account)
                        .bind(code)
                        .bind(message)
                        .execute(&mut **tx)
                        .await
                        .map_err(|e| unavailable(&e))?;
                }
                (_, Some(_)) | (None, None) => {}
            }
        }
        write_seen(tx, account, before, after).await?;
        write_intents(tx, account, before, after).await?;
        write_drafts(tx, account, before, after).await?;
        write_submission_diff(tx, account, before, after).await?;
        if before.labels != after.labels {
            write_labels(tx, account, &before.labels, &after.labels, after.seq).await?;
        }
        put_changes(tx, account, &after.log).await
    }

    // ------------------------------------------------------------ store operations

    pub(crate) async fn track(
        &self,
        account: &AccountId,
        generation: u64,
    ) -> Result<TrackOutcome, StoreError> {
        self.track_with(account, Some(generation)).await
    }

    async fn track_with(
        &self,
        account: &AccountId,
        generation: Option<u64>,
    ) -> Result<TrackOutcome, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let generation = if let Some(generation) = generation {
            sqlx::query("UPDATE mailtide_generation SET generation = $1 WHERE singleton = 1 AND generation < $1")
                .bind(int("generation", generation)?).execute(&mut *tx).await.map_err(|e| unavailable(&e))?;
            generation
        } else {
            let next = sqlx::query("UPDATE mailtide_generation SET generation = generation + 1 WHERE singleton = 1 RETURNING generation")
                .fetch_one(&mut *tx).await.map_err(|e| unavailable(&e))?;
            uint(
                "generation",
                next.try_get("generation")
                    .map_err(|e| corrupt("generation", e))?,
            )?
        };
        let initial = AccountRecord::new(account, generation);
        let inserted = sqlx::query(
            "INSERT INTO mailtide_accounts
               (account, epoch, engine_version, cursor, sync, watch_expiration, last_activity, cooldown, rng_seed, seq, log_floor, generation)
             VALUES ($1, $2, $3, NULL, $4, NULL, NULL, NULL, $5, 0, 0, $6)
             ON CONFLICT (account) DO NOTHING",
        )
        .bind(account.as_str())
        .bind(int("epoch", initial.epoch.0)?)
        .bind(i64::from(initial.engine_version.0))
        .bind(json(&initial.sync)?)
        .bind(initial.rng_seed.cast_signed())
        .bind(int("generation", generation)?)
        .execute(&mut *tx)
        .await
        .map_err(|e| unavailable(&e))?
        .rows_affected();
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(if inserted == 1 {
            TrackOutcome::Tracked { generation }
        } else {
            TrackOutcome::AlreadyTracked
        })
    }

    pub(crate) async fn claim(
        &self,
        account: &AccountId,
        version: EngineVersion,
    ) -> Result<StoreSnapshot, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        // The row exists (track created it) and is locked, so two claims
        // racing get different epochs.
        let (mut record, seq) = self
            .load_account(&mut tx, account)
            .await?
            .ok_or(StoreError::NotTracked)?;
        if version < record.engine_version {
            return Err(StoreError::Rejected(Rejection::StaleVersion));
        }
        record.epoch = Epoch(record.epoch.0 + 1);
        record.engine_version = version;
        Self::write_account(&mut tx, account, &record, seq).await?;
        let label_ids = load_labels(&mut *tx, account)
            .await?
            .into_iter()
            .map(|label| label.id)
            .collect();
        sqlx::query(
            "UPDATE mailtide_jobs SET state = 'ready', due_at = $2, attempt = NULL WHERE account = $1 AND state = 'running'",
        )
        .bind(account.as_str())
        .bind(i64::MIN)
        .execute(&mut *tx)
        .await
        .map_err(|e| unavailable(&e))?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(StoreSnapshot {
            account: account.clone(),
            epoch: record.epoch,
            engine_version: record.engine_version,
            cursor: record.cursor,
            sync: record.sync,
            watch_expiration: record.watch_expiration,
            last_activity: record.last_activity,
            cooldown: record.cooldown,
            rng_seed: record.rng_seed,
            labels_refreshed_at: record.labels_refreshed_at,
            audited_at: record.audited_at,
            audit_progress: record.audit_progress,
            audit_schedule: record.audit_schedule,
            account_generation: record.generation,
            label_ids,
            drafts_refreshed_at: record.drafts_refreshed_at,
        })
    }

    pub(crate) async fn apply(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let Some((record, seq)) = self.load_account(&mut tx, &commit.account).await? else {
            return Ok(CommitOutcome::Rejected(Rejection::StaleEpoch));
        };
        if record.epoch != commit.epoch || record.generation != commit.account_generation {
            return Ok(CommitOutcome::Rejected(Rejection::StaleEpoch));
        }
        let mut mirror = self
            .load_partial(&mut tx, &commit.account, record, seq, commit)
            .await?;
        let before = mirror.clone();
        let outcome = mirror.apply(commit);
        match outcome {
            CommitOutcome::Rejected(_) => {
                tx.rollback().await.map_err(|e| unavailable(&e))?;
            }
            CommitOutcome::Applied { .. } => {
                Self::write_diff(&mut tx, &before, &mirror).await?;
                tx.commit()
                    .await
                    .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
            }
        }
        Ok(outcome)
    }

    /// [`crate::Store::record_label`].
    pub(crate) async fn record_label(
        &self,
        account: &AccountId,
        op: &LabelOp,
    ) -> Result<(), StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let (record, seq) = self
            .load_account(&mut tx, account)
            .await?
            .ok_or(StoreError::NotTracked)?;
        // The rows the write touches, loaded as for a commit carrying it.
        let carrier = Commit {
            id: mailtide::ids::CommitId {
                epoch: record.epoch,
                seq: 0,
            },
            account: account.clone(),
            epoch: record.epoch,
            account_generation: record.generation,
            engine_version: record.engine_version,
            cursor: None,
            claim: None,
            sync: None,
            messages: Vec::new(),
            labels: vec![op.clone()],
            account_ops: Vec::new(),
            job_plan: mailtide::commit::JobPlan {
                priority: Priority::Foreground,
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
        let mut mirror = self
            .load_partial(&mut tx, account, record, seq, &carrier)
            .await?;
        let before = mirror.clone();
        mirror.record_label(op);
        Self::write_diff(&mut tx, &before, &mirror).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))
    }

    /// [`crate::Store::label_generation`].
    pub(crate) async fn label_generation(&self, account: &AccountId) -> Result<u64, StoreError> {
        self.ensure_schema().await?;
        let row = sqlx::query("SELECT label_generation FROM mailtide_accounts WHERE account = $1")
            .bind(account.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| unavailable(&e))?;
        match row {
            None => Ok(0),
            Some(row) => uint(
                "label_generation",
                row.try_get("label_generation")
                    .map_err(|e| corrupt("label_generation", e))?,
            ),
        }
    }

    pub(crate) async fn claim_jobs(
        &self,
        account: &AccountId,
        claim: (Epoch, u64),
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
        floor: mailtide::quota::Priority,
    ) -> Result<ClaimOutcome, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let record = self.load_account(&mut tx, account).await?;
        let Some((record, _)) =
            record.filter(|(record, _)| (record.epoch, record.generation) == claim)
        else {
            return Ok(ClaimOutcome::Rejected(Rejection::StaleEpoch));
        };
        let context = ClaimContext {
            drafts_generation: record.drafts_generation,
            now,
        };
        let skip_locked = match self.dialect {
            Dialect::Postgres => " FOR UPDATE SKIP LOCKED",
            Dialect::Sqlite => "",
        };
        let sql = format!(
            "SELECT kind, message, priority, failed_attempts, state, due_at, attempt, failure FROM mailtide_jobs
             WHERE account = $1 AND state IN ('ready', 'deferred') AND due_at <= $2 AND priority >= $4
             ORDER BY priority DESC, due_at ASC, kind ASC, message ASC LIMIT $3{skip_locked}"
        );
        // Only a constant lock clause is appended.
        let rows = sqlx::query(AssertSqlSafe(sql))
            .bind(account.as_str())
            .bind(now.0)
            .bind(i64::from(limit))
            .bind(priority_code(floor))
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| unavailable(&e))?;
        let mut claimed = Vec::new();
        for row in rows {
            let job = job_from_row(&row)?;
            let (code, message) = kind_code(&job.kind);
            sqlx::query(
                "UPDATE mailtide_jobs SET state = 'running', attempt = $4 WHERE account = $1 AND kind = $2 AND message = $3",
            )
            .bind(account.as_str())
            .bind(code)
            .bind(message)
            .bind(attempt.0.cast_signed())
            .execute(&mut *tx)
            .await
            .map_err(|e| unavailable(&e))?;
            let group = match &job.kind {
                JobKind::Deliver(id) => plan_delivery(&load_intents(&mut tx, account, id).await?)
                    .into_iter()
                    .next(),
                JobKind::Import(_)
                | JobKind::Verify(_)
                | JobKind::BlobCleanup(_)
                | JobKind::RefreshDrafts
                | JobKind::SendSubmissions => None,
            };
            claimed.push(ClaimedJob {
                work: ClaimedWork::plan(job.kind, context, |_| group),
                priority: job.priority,
                failed_attempts: job.failed_attempts,
                attempt,
            });
        }
        let row = sqlx::query(
            "SELECT MIN(due_at) AS due FROM mailtide_jobs WHERE account = $1 AND state IN ('ready', 'deferred')",
        )
        .bind(account.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| unavailable(&e))?;
        let next_due: Option<i64> = row.try_get("due").map_err(|e| corrupt("due_at", e))?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(ClaimOutcome::Claimed {
            jobs: claimed,
            next_due: next_due.map(Timestamp),
        })
    }

    pub(crate) async fn changes(
        &self,
        account: &AccountId,
        since: FeedCursor,
        limit: u32,
    ) -> Result<ChangeFeed, StoreError> {
        self.ensure_schema().await?;
        let row = sqlx::query(
            "SELECT log_floor, generation, seq FROM mailtide_accounts WHERE account = $1",
        )
        .bind(account.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| unavailable(&e))?;
        let (floor, generation, head) = match row {
            Some(row) => {
                let get = |name: &str| {
                    row.try_get::<i64, _>(name)
                        .map_err(|e| corrupt(name, e))
                        .and_then(|v| uint(name, v))
                };
                (
                    StateSeq(get("log_floor")?),
                    get("generation")?,
                    StateSeq(get("seq")?),
                )
            }
            None => (StateSeq(0), 0, StateSeq(0)),
        };
        if since.is_compacted(floor) {
            return Ok(ChangeFeed::TooOld {
                generation,
                oldest: floor,
            });
        }
        let mut page = self.change_page(account, since, limit).await?;
        page.generation = generation;
        page.head = head;
        Ok(ChangeFeed::Page(page))
    }

    async fn change_page(
        &self,
        account: &AccountId,
        since: FeedCursor,
        limit: u32,
    ) -> Result<ChangePage, StoreError> {
        // One keyset read on the log's key for the page's window (`limit`
        // entries and one more, which says where the last commit in the
        // window ends): the page is cut from it at the last commit boundary
        // within `limit`. A commit position reads after every entry of its
        // commit.
        let rows = sqlx::query(
            "SELECT seq, idx, change FROM mailtide_changes
             WHERE account = $1 AND (seq, idx) > ($2, $3) ORDER BY seq, idx LIMIT $4",
        )
        .bind(account.as_str())
        .bind(int("since", since.seq.0)?)
        .bind(since.entry.map_or(i64::MAX, i64::from))
        .bind(i64::from(page_window(limit)))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| unavailable(&e))?;
        let window = rows
            .iter()
            .map(|row| {
                let seq: i64 = row.try_get("seq").map_err(|e| corrupt("seq", e))?;
                let idx: i64 = row.try_get("idx").map_err(|e| corrupt("idx", e))?;
                Ok((
                    StateSeq(uint("seq", seq)?),
                    u32::try_from(idx).map_err(|e| corrupt("idx", e))?,
                ))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let cut = cut_page(&window, limit);
        let mut changes = Vec::with_capacity(cut.take);
        for (row, (state, index)) in rows.iter().zip(window).take(cut.take) {
            let change: String = row.try_get("change").map_err(|e| corrupt("change", e))?;
            changes.push(ChangeEntry {
                state,
                index,
                change: parse::<Change>("change", &change)?,
                content: None,
            });
        }
        let with_content = |change: &Change| match change {
            Change::MessageImported { id, .. } | Change::FactsRefreshed { id, .. } => {
                Some(id.clone())
            }
            _ => None,
        };
        let imported: Vec<MessageId> = changes
            .iter()
            .filter_map(|entry| with_content(&entry.change))
            .collect();
        // A page can hold a message's import and its refresh: both show the
        // content as stored now.
        let contents = self.contents(account, &imported).await?;
        for entry in &mut changes {
            if let Some(id) = with_content(&entry.change) {
                entry.content = contents.get(&id).cloned();
            }
        }
        let next = cut.next.unwrap_or(since);
        Ok(ChangePage {
            generation: 0,
            head: next.seq,
            changes,
            next,
            has_more: cut.has_more,
        })
    }

    /// The stored content of those of `ids` that are imported and still
    /// held: one query per chunk of ids (a page can hold up to
    /// `MAX_FEED_PAGE` imports).
    async fn contents(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<BTreeMap<MessageId, StoredContent>, StoreError> {
        let mut contents = BTreeMap::new();
        for chunk in ids.chunks(VIEW_CHUNK) {
            let placeholders = (2..chunk.len() + 2)
                .map(|n| format!("${n}"))
                .collect::<Vec<_>>()
                .join(", ");
            // Only numbered placeholders are interpolated; ids are bound.
            let sql = format!(
                "SELECT id, content FROM mailtide_messages
                 WHERE account = $1 AND id IN ({placeholders}) AND content IS NOT NULL"
            );
            let mut query = sqlx::query(AssertSqlSafe(sql)).bind(account.as_str());
            for id in chunk {
                query = query.bind(id.0.as_str());
            }
            for row in query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| unavailable(&e))?
            {
                let id = MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?);
                let text: String = row.try_get("content").map_err(|e| corrupt("content", e))?;
                contents.insert(id, parse::<StoredContent>("content", &text)?);
            }
        }
        Ok(contents)
    }
}

/// The messages whose row may carry one of `labels`, or that have an
/// intent on one: one statement per chunk of labels, however many a listing
/// removes. The `labels` column is matched as text, which may find more rows
/// than carry them (`SQLite` compares ASCII case-insensitively); the shared
/// rule checks each row it is given, so those are left as they are.
async fn carrying(
    tx: &mut Transaction<'static, Any>,
    account: &AccountId,
    labels: &BTreeSet<LabelId>,
) -> Result<BTreeSet<MessageId>, StoreError> {
    let labels: Vec<&LabelId> = labels.iter().collect();
    let mut ids = BTreeSet::new();
    for chunk in labels.chunks(VIEW_CHUNK / 2) {
        let patterns = chunk
            .iter()
            .map(|label| label_pattern(label))
            .collect::<Result<Vec<_>, _>>()?;
        let n = chunk.len();
        let likes = (2..n + 2)
            .map(|i| format!("labels LIKE ${i} ESCAPE '!'"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let names = (n + 2..2 * n + 2)
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(", ");
        // Only numbered placeholders are interpolated; values are bound.
        let sql = format!(
            "SELECT id FROM mailtide_messages WHERE account = $1 AND ({likes})
             UNION SELECT message AS id FROM mailtide_intents WHERE account = $1 AND label IN ({names})"
        );
        let mut query = sqlx::query(AssertSqlSafe(sql)).bind(account.as_str());
        for pattern in patterns {
            query = query.bind(pattern);
        }
        for label in chunk {
            query = query.bind(label.0.as_str());
        }
        for row in query
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?
        {
            ids.insert(MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?));
        }
    }
    Ok(ids)
}

impl SqlStore {
    /// Record mailtide's own draft call (`Store::record_draft`): one
    /// transaction under the account row's lock, so `BeginSend`'s
    /// compare-and-set has one winner across processes.
    pub(crate) async fn record_draft(
        &self,
        account: &AccountId,
        write: &DraftWrite,
        now: Timestamp,
    ) -> Result<DraftRecorded, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let Some((record, seq)) = self.load_account(&mut tx, account).await? else {
            return Err(StoreError::NotTracked);
        };
        let mut mirror = AccountMirror::new(account.clone(), record.generation);
        mirror.record = record;
        mirror.seq = seq;
        mirror.drafts = load_drafts(&mut *tx, account).await?;
        if let DraftWrite::BeginSend { draft, .. } = write
            && let Some(token) = mirror
                .drafts
                .get(draft)
                .and_then(|row| row.tracking_id.clone())
        {
            load_token(&mut tx, account, &token, &mut mirror).await?;
        }
        Self::load_account_jobs(&mut tx, account, &mut mirror).await?;
        // A sent message the mirror may not have seen gets a verification.
        if let DraftWrite::EndSend {
            end: SendEnd::Sent { message, .. },
            ..
        } = write
        {
            Self::load_rows(&mut tx, account, std::slice::from_ref(message), &mut mirror).await?;
        }
        let before = mirror.clone();
        let outcome = mirror.record_draft(write, now);
        Self::write_diff(&mut tx, &before, &mirror).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(outcome)
    }

    /// Up to `limit` drafts after `after` (`Store::drafts`), or the one
    /// named `only`, with the Message-ID of their imported messages.
    async fn draft_views(
        &self,
        account: &AccountId,
        after: Option<&DraftId>,
        only: Option<&DraftId>,
        limit: u32,
    ) -> Result<Vec<DraftView>, StoreError> {
        self.ensure_schema().await?;
        let (filter, key) = match (only, after) {
            (Some(id), _) => ("d.id = $2", id.0.as_str()),
            (None, after) => ("d.id > $2", after.map_or("", |id| id.0.as_str())),
        };
        // Only a constant filter is interpolated; values are bound.
        let sql = format!(
            "SELECT d.id, d.message, d.thread_id, d.tracking_id, d.state, d.since, m.content
             FROM mailtide_drafts d LEFT JOIN mailtide_messages m
               ON m.account = d.account AND m.id = d.message
             WHERE d.account = $1 AND {filter} ORDER BY d.id LIMIT $3"
        );
        let rows = sqlx::query(AssertSqlSafe(sql))
            .bind(account.as_str())
            .bind(key)
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| unavailable(&e))?;
        rows.iter()
            .map(|row| {
                let record = draft_from_row(row)?;
                let content: Option<String> =
                    row.try_get("content").map_err(|e| corrupt("content", e))?;
                let message_id = content
                    .as_deref()
                    .map(|text| parse::<StoredContent>("content", text))
                    .transpose()?
                    .and_then(|stored| stored.headers.rfc822_message_id);
                Ok(record.view(message_id))
            })
            .collect()
    }
}

/// Every draft row of `account`.
async fn load_drafts<'e, E>(executor: E, account: &AccountId) -> Result<DraftIndex, StoreError>
where
    E: sqlx::Executor<'e, Database = Any>,
{
    let rows = sqlx::query(
        "SELECT id, message, thread_id, tracking_id, state, since FROM mailtide_drafts WHERE account = $1",
    )
    .bind(account.as_str())
    .fetch_all(executor)
    .await
    .map_err(|e| unavailable(&e))?;
    rows.iter()
        .map(|row| draft_from_row(row).map(|record| (record.id.clone(), record)))
        .collect()
}

/// The draft rows whose `column` (`message` or `tracking_id`) is one of
/// `values`, into `mirror`.
async fn load_drafts_where(
    tx: &mut Transaction<'static, Any>,
    account: &AccountId,
    column: &'static str,
    values: &[&str],
    mirror: &mut AccountMirror,
) -> Result<(), StoreError> {
    for chunk in values.chunks(VIEW_CHUNK) {
        let placeholders = (2..chunk.len() + 2)
            .map(|n| format!("${n}"))
            .collect::<Vec<_>>()
            .join(", ");
        // Only a constant column and numbered placeholders are interpolated.
        let sql = format!(
            "SELECT id, message, thread_id, tracking_id, state, since FROM mailtide_drafts
             WHERE account = $1 AND {column} IN ({placeholders})"
        );
        let mut query = sqlx::query(AssertSqlSafe(sql)).bind(account.as_str());
        for value in chunk {
            query = query.bind(*value);
        }
        for row in query
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?
        {
            let record = draft_from_row(&row)?;
            mirror.drafts.insert(record.id.clone(), record);
        }
    }
    Ok(())
}

fn draft_from_row(row: &AnyRow) -> Result<DraftRecord, StoreError> {
    let state: String = row
        .try_get("state")
        .map_err(|e| corrupt("draft state", e))?;
    Ok(DraftRecord {
        id: DraftId(row.try_get("id").map_err(|e| corrupt("draft id", e))?),
        message: MessageId(
            row.try_get("message")
                .map_err(|e| corrupt("draft message", e))?,
        ),
        thread_id: ThreadId(
            row.try_get("thread_id")
                .map_err(|e| corrupt("draft thread_id", e))?,
        ),
        tracking_id: row
            .try_get("tracking_id")
            .map_err(|e| corrupt("tracking_id", e))?,
        state: parse::<DraftState>("draft state", &state)?,
        since: Timestamp(row.try_get("since").map_err(|e| corrupt("since", e))?),
    })
}

/// Write the draft rows that changed: rows that go or change are deleted
/// first, so a row re-pointed at a message never meets the unique index
/// while another still names it.
async fn write_drafts(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    before: &AccountMirror,
    after: &AccountMirror,
) -> Result<(), StoreError> {
    let changed: Vec<&DraftId> = before
        .drafts
        .keys()
        .chain(after.drafts.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|id| before.drafts.get(*id) != after.drafts.get(*id))
        .collect();
    for id in &changed {
        if before.drafts.contains_key(*id) {
            sqlx::query("DELETE FROM mailtide_drafts WHERE account = $1 AND id = $2")
                .bind(account)
                .bind(id.0.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| unavailable(&e))?;
        }
    }
    for id in changed {
        let Some(record) = after.drafts.get(id) else {
            continue;
        };
        sqlx::query(
            "INSERT INTO mailtide_drafts (account, id, message, thread_id, tracking_id, state, since)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(account)
        .bind(record.id.0.as_str())
        .bind(record.message.0.as_str())
        .bind(record.thread_id.0.as_str())
        .bind(record.tracking_id.as_deref())
        .bind(json(&record.state)?)
        .bind(record.since.0)
        .execute(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
    }
    Ok(())
}

async fn load_intents(
    tx: &mut Transaction<'static, Any>,
    account: &AccountId,
    id: &MessageId,
) -> Result<Vec<Intent>, StoreError> {
    let rows =
        sqlx::query("SELECT intent FROM mailtide_intents WHERE account = $1 AND message = $2")
            .bind(account.as_str())
            .bind(id.0.as_str())
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
    rows.iter()
        .map(|row| {
            let text: String = row.try_get("intent").map_err(|e| corrupt("intent", e))?;
            parse::<Intent>("intent", &text)
        })
        .collect()
}

fn message_from_row(id: &MessageId, row: &AnyRow) -> Result<MessageRecord, StoreError> {
    let labels: String = row.try_get("labels").map_err(|e| corrupt("labels", e))?;
    let content: Option<String> = row.try_get("content").map_err(|e| corrupt("content", e))?;
    let checkpoint: Option<i64> = row
        .try_get("checkpoint")
        .map_err(|e| corrupt("checkpoint", e))?;
    let unverified: Option<i64> = row
        .try_get("unverified")
        .map_err(|e| corrupt("unverified", e))?;
    Ok(MessageRecord {
        id: id.clone(),
        thread_id: ThreadId(
            row.try_get("thread_id")
                .map_err(|e| corrupt("thread_id", e))?,
        ),
        labels: parse::<LabelSet>("labels", &labels)?,
        checkpoint: checkpoint
            .map(|c| uint("history id", c).map(HistoryId))
            .transpose()?,
        unverified: unverified
            .map(|c| uint("history id", c).map(HistoryId))
            .transpose()?,
        content: match content {
            None => Content::Pending,
            Some(text) => Content::Imported(Box::new(parse::<StoredContent>("content", &text)?)),
        },
    })
}

/// Append a commit's feed entries, a chunk per statement: a label removal
/// logs one per message that carried it.
async fn put_changes(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    log: &[(StateSeq, Change)],
) -> Result<(), StoreError> {
    let entries: Vec<(i64, &(StateSeq, Change))> = (0_i64..).zip(log.iter()).collect();
    for chunk in entries.chunks(WRITE_CHUNK) {
        let values = (0..chunk.len())
            .map(|i| format!("($1, ${}, ${}, ${})", 3 * i + 2, 3 * i + 3, 3 * i + 4))
            .collect::<Vec<_>>()
            .join(", ");
        // Only numbered placeholders are interpolated; values are bound.
        let mut query = sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO mailtide_changes (account, seq, idx, change) VALUES {values}"
        )))
        .bind(account);
        for (idx, (seq, change)) in chunk {
            query = query
                .bind(int("seq", seq.0)?)
                .bind(*idx)
                .bind(json(change)?);
        }
        query
            .execute(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
    }
    Ok(())
}

/// Insert or replace `records`, a chunk of rows per statement (a label
/// removal rewrites every row that carried it).
async fn put_messages(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    records: &[&MessageRecord],
) -> Result<(), StoreError> {
    for chunk in records.chunks(WRITE_CHUNK) {
        let values = (0..chunk.len())
            .map(|i| {
                let first = 6 * i + 2;
                let numbered: Vec<String> = (first..first + 6).map(|n| format!("${n}")).collect();
                format!("($1, {})", numbered.join(", "))
            })
            .collect::<Vec<_>>()
            .join(", ");
        // Only numbered placeholders are interpolated; values are bound.
        let mut query = sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO mailtide_messages (account, id, thread_id, labels, checkpoint, unverified, content)
             VALUES {values}
             ON CONFLICT (account, id) DO UPDATE SET thread_id = excluded.thread_id, labels = excluded.labels,
               checkpoint = excluded.checkpoint, unverified = excluded.unverified, content = excluded.content"
        )))
        .bind(account);
        for record in chunk {
            let content = match &record.content {
                Content::Pending => None,
                Content::Imported(stored) => Some(json(stored.as_ref())?),
            };
            query = query
                .bind(record.id.0.as_str())
                .bind(record.thread_id.0.as_str())
                .bind(json(&record.labels)?)
                .bind(
                    record
                        .checkpoint
                        .map(|c| int("checkpoint", c.0))
                        .transpose()?,
                )
                .bind(
                    record
                        .unverified
                        .map(|c| int("unverified", c.0))
                        .transpose()?,
                )
                .bind(content);
        }
        query
            .execute(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
    }
    Ok(())
}

fn job_from_row(row: &AnyRow) -> Result<Job, StoreError> {
    let code: i64 = row.try_get("kind").map_err(|e| corrupt("kind", e))?;
    let message: String = row.try_get("message").map_err(|e| corrupt("message", e))?;
    let state: String = row.try_get("state").map_err(|e| corrupt("state", e))?;
    let due_at: Option<i64> = row.try_get("due_at").map_err(|e| corrupt("due_at", e))?;
    let attempt: Option<i64> = row.try_get("attempt").map_err(|e| corrupt("attempt", e))?;
    let failure: Option<String> = row.try_get("failure").map_err(|e| corrupt("failure", e))?;
    let state = match state.as_str() {
        "ready" => JobState::Ready {
            due_at: Timestamp(due_at.ok_or_else(|| corrupt("due_at", "missing"))?),
        },
        "deferred" => JobState::Deferred {
            due_at: Timestamp(due_at.ok_or_else(|| corrupt("due_at", "missing"))?),
        },
        "running" => JobState::Running(AttemptId(
            attempt
                .ok_or_else(|| corrupt("attempt", "missing"))?
                .cast_unsigned(),
        )),
        "blocked" => JobState::Blocked(parse(
            "failure",
            failure
                .as_deref()
                .ok_or_else(|| corrupt("failure", "missing"))?,
        )?),
        other => return Err(corrupt("state", other)),
    };
    Ok(Job {
        kind: kind_from(code, message)?,
        priority: priority_from(
            row.try_get("priority")
                .map_err(|e| corrupt("priority", e))?,
        )?,
        failed_attempts: u32::try_from(
            row.try_get::<i64, _>("failed_attempts")
                .map_err(|e| corrupt("failed_attempts", e))?,
        )
        .unwrap_or(u32::MAX),
        state,
    })
}

async fn put_job(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    job: &Job,
) -> Result<(), StoreError> {
    let (code, message) = kind_code(&job.kind);
    let (state, due_at, attempt, failure) = match &job.state {
        JobState::Ready { due_at } => ("ready", Some(due_at.0), None, None),
        JobState::Deferred { due_at } => ("deferred", Some(due_at.0), None, None),
        JobState::Running(attempt) => ("running", None, Some(attempt.0.cast_signed()), None),
        JobState::Blocked(failure) => ("blocked", None, None, Some(json(failure)?)),
    };
    sqlx::query(
        "INSERT INTO mailtide_jobs (account, kind, message, priority, failed_attempts, state, due_at, attempt, failure)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (account, kind, message) DO UPDATE SET priority = excluded.priority,
           failed_attempts = excluded.failed_attempts, state = excluded.state, due_at = excluded.due_at,
           attempt = excluded.attempt, failure = excluded.failure",
    )
    .bind(account)
    .bind(code)
    .bind(message)
    .bind(priority_code(job.priority))
    .bind(i64::from(job.failed_attempts))
    .bind(state)
    .bind(due_at)
    .bind(attempt)
    .bind(failure)
    .execute(&mut **tx)
    .await
    .map_err(|e| unavailable(&e))?;
    Ok(())
}

async fn write_intents(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    before: &AccountMirror,
    after: &AccountMirror,
) -> Result<(), StoreError> {
    let keys: BTreeSet<&(MessageId, LabelId)> =
        before.intents.keys().chain(after.intents.keys()).collect();
    for key in keys {
        match (before.intents.get(key), after.intents.get(key)) {
            (_, Some(intent)) if before.intents.get(key) != Some(intent) => {
                sqlx::query(
                    "INSERT INTO mailtide_intents (account, message, label, intent) VALUES ($1, $2, $3, $4)
                     ON CONFLICT (account, message, label) DO UPDATE SET intent = excluded.intent",
                )
                .bind(account)
                .bind(key.0.0.as_str())
                .bind(key.1.0.as_str())
                .bind(json(intent)?)
                .execute(&mut **tx)
                .await
                .map_err(|e| unavailable(&e))?;
            }
            (Some(_), None) => {
                sqlx::query("DELETE FROM mailtide_intents WHERE account = $1 AND message = $2 AND label = $3")
                    .bind(account)
                    .bind(key.0.0.as_str())
                    .bind(key.1.0.as_str())
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| unavailable(&e))?;
            }
            (_, Some(_)) | (None, None) => {}
        }
    }
    Ok(())
}

async fn write_seen(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    before: &AccountMirror,
    after: &AccountMirror,
) -> Result<(), StoreError> {
    let empty = BTreeSet::new();
    let ids: BTreeSet<&MessageId> = before.seen.keys().chain(after.seen.keys()).collect();
    let mut removed: BTreeMap<&MessageId, Vec<&ListScope>> = BTreeMap::new();
    for id in ids {
        let old = before.seen.get(id).unwrap_or(&empty);
        let new = after.seen.get(id).unwrap_or(&empty);
        for scope in new.difference(old) {
            sqlx::query("INSERT INTO mailtide_seen (account, id, scope) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
                .bind(account)
                .bind(id.0.as_str())
                .bind(scope_code(scope))
                .execute(&mut **tx)
                .await
                .map_err(|e| unavailable(&e))?;
        }
        removed.entry(id).or_default().extend(old.difference(new));
    }
    // Finishing a reconcile clears every message's marks: delete them by
    // scope in chunks, not one statement per row.
    let mut by_scope: BTreeMap<String, Vec<&MessageId>> = BTreeMap::new();
    for (id, scopes) in removed {
        for scope in scopes {
            by_scope.entry(scope_code(scope)).or_default().push(id);
        }
    }
    for (scope, ids) in by_scope {
        for chunk in ids.chunks(VIEW_CHUNK) {
            let placeholders = (3..chunk.len() + 3)
                .map(|n| format!("${n}"))
                .collect::<Vec<_>>()
                .join(", ");
            // Only numbered placeholders are interpolated; ids are bound.
            let sql = format!(
                "DELETE FROM mailtide_seen WHERE account = $1 AND scope = $2 AND id IN ({placeholders})"
            );
            let mut query = sqlx::query(AssertSqlSafe(sql))
                .bind(account)
                .bind(scope.as_str());
            for id in chunk {
                query = query.bind(id.0.as_str());
            }
            query
                .execute(&mut **tx)
                .await
                .map_err(|e| unavailable(&e))?;
        }
    }
    Ok(())
}

/// An account's consumer cursors, in name order.
async fn read_consumers(
    tx: &mut Transaction<'static, Any>,
    account: &AccountId,
) -> Result<Vec<ConsumerCursor>, StoreError> {
    let rows = sqlx::query(
        "SELECT name, seq, idx, seen_at FROM mailtide_consumers WHERE account = $1 ORDER BY name",
    )
    .bind(account.as_str())
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| unavailable(&e))?;
    rows.iter()
        .map(|row| {
            let name: String = row.try_get("name").map_err(|e| corrupt("consumer", e))?;
            let seq: i64 = row.try_get("seq").map_err(|e| corrupt("consumer seq", e))?;
            let idx: Option<i64> = row.try_get("idx").map_err(|e| corrupt("consumer idx", e))?;
            let seen_at: i64 = row
                .try_get("seen_at")
                .map_err(|e| corrupt("consumer seen_at", e))?;
            Ok(ConsumerCursor {
                name: ConsumerName::parse(&name)
                    .map_err(|_| corrupt("consumer", format!("invalid name {name:?}")))?,
                at: FeedCursor {
                    seq: StateSeq(uint("consumer seq", seq)?),
                    entry: idx
                        .map(|idx| u32::try_from(idx).map_err(|e| corrupt("consumer idx", e)))
                        .transpose()?,
                },
                seen_at: Timestamp(seen_at),
            })
        })
        .collect()
}

/// Write the consumer cursors that differ between `before` and `after`.
async fn write_consumers(
    tx: &mut Transaction<'static, Any>,
    before: &AccountMirror,
    after: &AccountMirror,
) -> Result<(), StoreError> {
    let account = after.account.as_str();
    for (name, cursor) in &after.consumers {
        if before.consumers.get(name) == Some(cursor) {
            continue;
        }
        sqlx::query(
            "INSERT INTO mailtide_consumers (account, name, seq, idx, seen_at) VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (account, name) DO UPDATE SET
               seq = excluded.seq, idx = excluded.idx, seen_at = excluded.seen_at",
        )
        .bind(account)
        .bind(name.as_str())
        .bind(int("consumer seq", cursor.at.seq.0)?)
        .bind(cursor.at.entry.map(i64::from))
        .bind(cursor.seen_at.0)
        .execute(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
    }
    for name in before.consumers.keys() {
        if !after.consumers.contains_key(name) {
            sqlx::query("DELETE FROM mailtide_consumers WHERE account = $1 AND name = $2")
                .bind(account)
                .bind(name.as_str())
                .execute(&mut **tx)
                .await
                .map_err(|e| unavailable(&e))?;
        }
    }
    Ok(())
}

impl crate::Store for SqlStore {
    async fn track(
        &self,
        account: &AccountId,
        generation: u64,
    ) -> Result<TrackOutcome, StoreError> {
        Self::track(self, account, generation).await
    }

    async fn track_new(&self, account: &AccountId) -> Result<TrackOutcome, StoreError> {
        self.track_with(account, None).await
    }

    async fn claim(
        &self,
        account: &AccountId,
        version: EngineVersion,
    ) -> Result<StoreSnapshot, StoreError> {
        Self::claim(self, account, version).await
    }

    async fn apply(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        Self::apply(self, commit).await
    }

    async fn advance_audit(
        &self,
        request: &mailtide::audit::AuditRequest,
    ) -> Result<mailtide::audit::AuditOutcome, StoreError> {
        SqlStore::advance_audit(self, request).await
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
        Self::claim_jobs(self, account, claim, limit, now, attempt, floor).await
    }

    async fn changes(
        &self,
        account: &AccountId,
        since: FeedCursor,
        limit: u32,
    ) -> Result<ChangeFeed, StoreError> {
        Self::changes(self, account, since, limit).await
    }

    async fn compact(&self, account: &AccountId, retention: &Retention) -> Result<u64, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let Some(mut mirror) = self.load_consumers(&mut tx, account).await? else {
            return Ok(0);
        };
        let before = mirror.clone();
        let Some(through) = mirror.retain(retention) else {
            // No bound and no consumer: nothing to drop, no cursor to see.
            return Ok(0);
        };
        let removed = sqlx::query("DELETE FROM mailtide_changes WHERE account = $1 AND seq <= $2")
            .bind(account.as_str())
            .bind(int("through", through.0)?)
            .execute(&mut *tx)
            .await
            .map_err(|e| unavailable(&e))?
            .rows_affected();
        Self::write_account(&mut tx, account, &mirror.record, mirror.seq).await?;
        write_consumers(&mut tx, &before, &mirror).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(removed)
    }

    async fn ack(
        &self,
        account: &AccountId,
        consumer: &ConsumerName,
        at: FeedPosition,
        now: Timestamp,
    ) -> Result<AckOutcome, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let mut mirror = self
            .load_consumers(&mut tx, account)
            .await?
            .ok_or(StoreError::NotTracked)?;
        let before = mirror.clone();
        let outcome = mirror.ack(consumer, at, now);
        write_consumers(&mut tx, &before, &mirror).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(outcome)
    }

    async fn consumers(&self, account: &AccountId) -> Result<ConsumerList, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let Some((record, _)) = self.load_account_unlocked(&mut tx, account).await? else {
            return Ok(ConsumerList {
                generation: 0,
                floor: StateSeq(0),
                cursors: Vec::new(),
            });
        };
        let cursors = read_consumers(&mut tx, account).await?;
        tx.rollback().await.map_err(|e| unavailable(&e))?;
        Ok(ConsumerList {
            generation: record.generation,
            floor: record.log_floor,
            cursors,
        })
    }

    async fn drop_consumer(
        &self,
        account: &AccountId,
        consumer: &ConsumerName,
    ) -> Result<bool, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        // The account row lock serializes this with acks and compactions.
        if self.load_account(&mut tx, account).await?.is_none() {
            return Ok(false);
        }
        let removed =
            sqlx::query("DELETE FROM mailtide_consumers WHERE account = $1 AND name = $2")
                .bind(account.as_str())
                .bind(consumer.as_str())
                .execute(&mut *tx)
                .await
                .map_err(|e| unavailable(&e))?
                .rows_affected();
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(removed > 0)
    }

    async fn message_ids(
        &self,
        account: &AccountId,
        after: Option<&MessageId>,
        limit: u32,
    ) -> Result<Vec<MessageId>, StoreError> {
        self.ensure_schema().await?;
        let rows = sqlx::query(
            "SELECT id FROM mailtide_messages WHERE account = $1 AND id > $2 ORDER BY id LIMIT $3",
        )
        .bind(account.as_str())
        .bind(after.map_or("", |id| id.0.as_str()))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| unavailable(&e))?;
        rows.iter()
            .map(|row| Ok(MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?)))
            .collect()
    }

    async fn status(&self, account: &AccountId) -> Result<Option<AccountStatus>, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let Some((record, seq)) = self.load_account_unlocked(&mut tx, account).await? else {
            return Ok(None);
        };
        let messages = self
            .count(
                &mut tx,
                "SELECT COUNT(*) AS n FROM mailtide_messages WHERE account = $1",
                account,
            )
            .await?;
        let pending_imports = self
            .count(
                &mut tx,
                "SELECT COUNT(*) AS n FROM mailtide_messages WHERE account = $1 AND content IS NULL",
                account,
            )
            .await?;
        let unverified = self
            .count(
                &mut tx,
                "SELECT COUNT(*) AS n FROM mailtide_messages WHERE account = $1 AND unverified IS NOT NULL",
                account,
            )
            .await?;
        let mut jobs = JobCounts::default();
        for row in sqlx::query(
            "SELECT kind, message, priority, failed_attempts, state, due_at, attempt, failure FROM mailtide_jobs WHERE account = $1",
        )
        .bind(account.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| unavailable(&e))?
        {
            jobs.add(&job_from_row(&row)?.state);
        }
        let mut actions = ActionCounts::default();
        for row in sqlx::query("SELECT intent FROM mailtide_intents WHERE account = $1")
            .bind(account.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| unavailable(&e))?
        {
            let text: String = row.try_get("intent").map_err(|e| corrupt("intent", e))?;
            actions.add(&parse::<Intent>("intent", &text)?.state);
        }
        tx.rollback().await.map_err(|e| unavailable(&e))?;
        Ok(Some(AccountStatus {
            account: account.clone(),
            sync: record.sync,
            epoch: record.epoch,
            cursor: record.cursor,
            watch_expiration: record.watch_expiration,
            last_activity: record.last_activity,
            cooldown: record.cooldown,
            state: seq,
            generation: record.generation,
            messages,
            pending_imports,
            unverified,
            jobs,
            actions,
        }))
    }

    async fn accounts(&self) -> Result<Vec<AccountId>, StoreError> {
        self.ensure_schema().await?;
        let rows = sqlx::query("SELECT account FROM mailtide_accounts ORDER BY account")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| unavailable(&e))?;
        rows.iter()
            .map(|row| {
                let text: String = row.try_get("account").map_err(|e| corrupt("account", e))?;
                AccountId::parse(&text).map_err(|e| corrupt("account", format!("{e:?}")))
            })
            .collect()
    }

    async fn resume(
        &self,
        account: &AccountId,
        now: Timestamp,
    ) -> Result<ResumeOutcome, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let Some((record, seq)) = self.load_account(&mut tx, account).await? else {
            return Ok(ResumeOutcome::Unknown);
        };
        let mut mirror = AccountMirror::new(account.clone(), record.generation);
        mirror.record = record;
        mirror.seq = seq;
        for row in sqlx::query(
            "SELECT kind, message, priority, failed_attempts, state, due_at, attempt, failure FROM mailtide_jobs WHERE account = $1 AND state = 'blocked'",
        )
        .bind(account.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| unavailable(&e))?
        {
            let job = job_from_row(&row)?;
            mirror.jobs.insert(job.kind.clone(), job);
        }
        let before = mirror.clone();
        let outcome = mirror.resume(now);
        Self::write_diff(&mut tx, &before, &mirror).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(outcome)
    }

    async fn forget(&self, account: &AccountId) -> Result<ForgetOutcome, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        if self.load_account(&mut tx, account).await?.is_none() {
            return Ok(ForgetOutcome::Unknown);
        }
        let messages = self
            .count(
                &mut tx,
                "SELECT COUNT(*) AS n FROM mailtide_messages WHERE account = $1",
                account,
            )
            .await?;
        for table in [
            "mailtide_messages",
            "mailtide_tombstones",
            "mailtide_jobs",
            "mailtide_seen",
            "mailtide_audit_seen",
            "mailtide_labels",
            "mailtide_intents",
            "mailtide_changes",
            "mailtide_consumers",
            "mailtide_drafts",
            "mailtide_submissions",
            "mailtide_send_tokens",
            "mailtide_accounts",
        ] {
            // Table names are constants, never input.
            sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE account = $1"
            )))
            .bind(account.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| unavailable(&e))?;
        }
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(ForgetOutcome::Forgotten { messages })
    }

    async fn admit(
        &self,
        account: &AccountId,
        request: &ActionRequest,
        now: Timestamp,
    ) -> Result<u32, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let Some((record, seq)) = self.load_account(&mut tx, account).await? else {
            // Nothing synced yet: no message can be a target.
            return Ok(0);
        };
        // The rows the action touches, loaded as for a commit carrying it.
        let carrier = Commit {
            id: mailtide::ids::CommitId {
                epoch: record.epoch,
                seq: 0,
            },
            account: account.clone(),
            epoch: record.epoch,
            account_generation: record.generation,
            engine_version: record.engine_version,
            cursor: None,
            claim: None,
            sync: None,
            messages: Vec::new(),
            labels: Vec::new(),
            account_ops: Vec::new(),
            job_plan: mailtide::commit::JobPlan {
                priority: mailtide::quota::Priority::Foreground,
                import_due_at: now,
                draft_import_due_at: now,
                verify_due_at: now,
                cleanup_due_at: now,
                deliver_due_at: now,
                confirm_due_at: now,
                draft_refresh_due_at: now,
            },
            jobs: Vec::new(),
            intents: vec![IntentOp::Request(request.clone())],
            drafts: Vec::new(),
        };
        let mut mirror = self
            .load_partial(&mut tx, account, record, seq, &carrier)
            .await?;
        let before = mirror.clone();
        let admitted = mirror.admit_action(request, now);
        Self::write_diff(&mut tx, &before, &mirror).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(admitted)
    }

    async fn refresh_facts(
        &self,
        account: &AccountId,
        updates: &[FactsUpdate],
    ) -> Result<u32, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let Some((record, seq)) = self.load_account(&mut tx, account).await? else {
            return Ok(0);
        };
        let mut mirror = AccountMirror::new(account.clone(), record.generation);
        mirror.record = record;
        mirror.seq = seq;
        let ids: Vec<MessageId> = updates.iter().map(|update| update.id.clone()).collect();
        Self::load_rows(&mut tx, account, &ids, &mut mirror).await?;
        let before = mirror.clone();
        let applied = mirror.refresh_facts(updates);
        Self::write_diff(&mut tx, &before, &mirror).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(applied)
    }

    async fn prioritise(
        &self,
        account: &AccountId,
        ids: &[MessageId],
        now: Timestamp,
    ) -> Result<Prioritised, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        // The account row lock serializes this with commits and claims.
        let Some((record, seq)) = self.load_account(&mut tx, account).await? else {
            return Ok(Prioritised::unknown(ids));
        };
        let mut mirror = AccountMirror::new(account.clone(), record.generation);
        mirror.record = record;
        mirror.seq = seq;
        let distinct: Vec<MessageId> = ids
            .iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect();
        Self::load_rows(&mut tx, account, &distinct, &mut mirror).await?;
        let row = sqlx::query(
            "SELECT COUNT(*) AS n FROM mailtide_jobs WHERE account = $1 AND kind = $2
             AND priority = $3 AND state IN ('ready', 'deferred', 'running')",
        )
        .bind(account.as_str())
        .bind(kind_code(&JobKind::Import(MessageId(String::new()))).0)
        .bind(priority_code(Priority::Foreground))
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| unavailable(&e))?;
        let foreground: i64 = row.try_get("n").map_err(|e| corrupt("count", e))?;
        let before = mirror.clone();
        let report =
            mirror.prioritise_counted(ids, now, usize::try_from(foreground).unwrap_or(usize::MAX));
        Self::write_diff(&mut tx, &before, &mirror).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(report)
    }

    async fn record_deleted(
        &self,
        account: &AccountId,
        deletions: &[(MessageId, DeletionEvidence)],
        now: Timestamp,
    ) -> Result<u32, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        // The account row lock serializes this with commits and claims; no
        // epoch is checked (deletion is final, see the trait).
        let Some((record, seq)) = self.load_account(&mut tx, account).await? else {
            return Err(StoreError::NotTracked);
        };
        let mut mirror = AccountMirror::new(account.clone(), record.generation);
        mirror.record = record;
        mirror.seq = seq;
        let ids: Vec<MessageId> = deletions
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Self::load_rows(&mut tx, account, &ids, &mut mirror).await?;
        let before = mirror.clone();
        let deleted = mirror.record_deleted(deletions, now);
        Self::write_diff(&mut tx, &before, &mirror).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(deleted)
    }

    async fn message_ids_labelled(
        &self,
        account: &AccountId,
        label: &LabelId,
        after: Option<&MessageId>,
        limit: u32,
    ) -> Result<Vec<MessageId>, StoreError> {
        self.ensure_schema().await?;
        // `labels` holds a JSON array of ids: match the label's JSON string
        // as text, then confirm on the parsed set, reading on until `limit`
        // rows hold it exactly (a text match is only a filter).
        let pattern = label_pattern(label)?;
        let wanted = usize::try_from(limit).unwrap_or(usize::MAX);
        let mut found = Vec::new();
        let mut from = after.map_or_else(String::new, |id| id.0.clone());
        while found.len() < wanted {
            let rows = sqlx::query(
                "SELECT id, labels FROM mailtide_messages
                 WHERE account = $1 AND id > $2 AND labels LIKE $3 ESCAPE '!'
                 ORDER BY id LIMIT $4",
            )
            .bind(account.as_str())
            .bind(from.as_str())
            .bind(pattern.as_str())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| unavailable(&e))?;
            let read = rows.len();
            for row in rows {
                let id: String = row.try_get("id").map_err(|e| corrupt("id", e))?;
                let labels: String = row.try_get("labels").map_err(|e| corrupt("labels", e))?;
                if parse::<LabelSet>("labels", &labels)?.contains(label) && found.len() < wanted {
                    found.push(MessageId(id.clone()));
                }
                from = id;
            }
            if read < wanted {
                break;
            }
        }
        Ok(found)
    }

    async fn messages(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Vec<MessageView>, StoreError> {
        self.ensure_schema().await?;
        // Read each row and its intents in the same statement, so a view
        // cannot combine an old observation with a newly cleared intent.
        let mut records: BTreeMap<MessageId, (MessageRecord, Vec<Intent>)> = BTreeMap::new();
        let wanted: Vec<&MessageId> = ids.iter().collect::<BTreeSet<_>>().into_iter().collect();
        for chunk in wanted.chunks(VIEW_CHUNK) {
            let placeholders = (2..chunk.len() + 2)
                .map(|n| format!("${n}"))
                .collect::<Vec<_>>()
                .join(", ");
            // Only numbered placeholders are interpolated; ids are bound.
            let sql = format!(
                "SELECT m.id, m.thread_id, m.labels, m.checkpoint, m.unverified, m.content, i.intent
                 FROM mailtide_messages m LEFT JOIN mailtide_intents i
                 ON i.account = m.account AND i.message = m.id
                 WHERE m.account = $1 AND m.id IN ({placeholders})"
            );
            let mut query = sqlx::query(AssertSqlSafe(sql)).bind(account.as_str());
            for id in chunk {
                query = query.bind(id.0.as_str());
            }
            for row in query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| unavailable(&e))?
            {
                let id = MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?);
                let (_, intents) = match records.entry(id) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let record = message_from_row(entry.key(), &row)?;
                        entry.insert((record, Vec::new()))
                    }
                };
                let text: Option<String> =
                    row.try_get("intent").map_err(|e| corrupt("intent", e))?;
                if let Some(text) = text {
                    intents.push(parse::<Intent>("intent", &text)?);
                }
            }
        }
        Ok(ids
            .iter()
            .filter_map(|id| {
                records
                    .get(id)
                    .map(|(record, intents)| record.view(intents))
            })
            .collect())
    }

    async fn threads(
        &self,
        account: &AccountId,
        ids: &[ThreadId],
    ) -> Result<Vec<ThreadView>, StoreError> {
        self.ensure_schema().await?;
        // The empty id is never a thread; each id is read once.
        let wanted: Vec<&ThreadId> = ids
            .iter()
            .filter(|id| !id.0.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut members: BTreeMap<ThreadId, BTreeMap<MessageId, (MessageRecord, Vec<Intent>)>> =
            BTreeMap::new();
        for chunk in wanted.chunks(VIEW_CHUNK) {
            // One statement per chunk: each thread's rows and their intents
            // come from one snapshot, never a row from before a commit with
            // an intent from after it.
            let mut query =
                sqlx::query(AssertSqlSafe(thread_rows(chunk.len()))).bind(account.as_str());
            for id in chunk {
                query = query.bind(id.0.as_str());
            }
            for row in query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| unavailable(&e))?
            {
                let id = MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?);
                let intent: Option<String> =
                    row.try_get("intent").map_err(|e| corrupt("intent", e))?;
                let record = message_from_row(&id, &row)?;
                let (_, intents) = members
                    .entry(record.thread_id.clone())
                    .or_default()
                    .entry(id)
                    .or_insert_with(|| (record, Vec::new()));
                if let Some(text) = intent {
                    intents.push(parse::<Intent>("intent", &text)?);
                }
            }
        }
        let views: BTreeMap<&ThreadId, ThreadView> = members
            .iter()
            .filter_map(|(id, rows)| {
                let views = rows
                    .values()
                    .map(|(record, intents)| record.view(intents))
                    .collect();
                Some((id, mailtide::thread::view(id, views)?))
            })
            .collect();
        Ok(ids.iter().filter_map(|id| views.get(id).cloned()).collect())
    }

    async fn locate(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Vec<(MessageId, Located)>, StoreError> {
        self.ensure_schema().await?;
        // Two primary-key reads per chunk, rows first: a message deleted
        // between them has a tombstone by the second, which wins, so it is
        // never reported absent while it has a row.
        let mut found: BTreeMap<MessageId, Located> = BTreeMap::new();
        for chunk in ids.chunks(VIEW_CHUNK) {
            let placeholders = (2..chunk.len() + 2)
                .map(|n| format!("${n}"))
                .collect::<Vec<_>>()
                .join(", ");
            // Only numbered placeholders are interpolated; ids are bound.
            let sql = format!(
                "SELECT id, CAST(CASE WHEN content IS NULL THEN 0 ELSE 1 END AS BIGINT) AS imported
                 FROM mailtide_messages WHERE account = $1 AND id IN ({placeholders})"
            );
            let mut query = sqlx::query(AssertSqlSafe(sql)).bind(account.as_str());
            for id in chunk {
                query = query.bind(id.0.as_str());
            }
            for row in query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| unavailable(&e))?
            {
                let id: String = row.try_get("id").map_err(|e| corrupt("id", e))?;
                let imported: i64 = row
                    .try_get("imported")
                    .map_err(|e| corrupt("imported", e))?;
                found.insert(
                    MessageId(id),
                    Located::Row {
                        imported: imported != 0,
                    },
                );
            }
            let sql = format!(
                "SELECT id FROM mailtide_tombstones WHERE account = $1 AND id IN ({placeholders})"
            );
            let mut query = sqlx::query(AssertSqlSafe(sql)).bind(account.as_str());
            for id in chunk {
                query = query.bind(id.0.as_str());
            }
            for row in query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| unavailable(&e))?
            {
                let id: String = row.try_get("id").map_err(|e| corrupt("id", e))?;
                found.insert(MessageId(id), Located::Tombstone);
            }
        }
        Ok(ids
            .iter()
            .map(|id| {
                let located = found.get(id).copied().unwrap_or(Located::Absent);
                (id.clone(), located)
            })
            .collect())
    }

    async fn labels(&self, account: &AccountId) -> Result<Vec<Label>, StoreError> {
        self.ensure_schema().await?;
        load_labels(&self.pool, account).await
    }

    async fn user_labels_oldest_first(
        &self,
        account: &AccountId,
    ) -> Result<Vec<LabelId>, StoreError> {
        self.ensure_schema().await?;
        let rows = sqlx::query(
            "SELECT id, defined_seq FROM mailtide_labels WHERE account = $1 AND kind = 'user'",
        )
        .bind(account.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| unavailable(&e))?;
        let mut ids = Vec::new();
        for row in rows {
            let id: String = row.try_get("id").map_err(|e| corrupt("id", e))?;
            if !mailtide::labels::system::is_system(&id) {
                let seq: i64 = row
                    .try_get("defined_seq")
                    .map_err(|e| corrupt("defined_seq", e))?;
                ids.push((seq, LabelId(id)));
            }
        }
        ids.sort_unstable();
        Ok(ids.into_iter().map(|(_, id)| id).collect())
    }

    async fn record_label(&self, account: &AccountId, op: &LabelOp) -> Result<(), StoreError> {
        Self::record_label(self, account, op).await
    }

    async fn label_generation(&self, account: &AccountId) -> Result<u64, StoreError> {
        Self::label_generation(self, account).await
    }

    async fn record_draft(
        &self,
        account: &AccountId,
        write: &DraftWrite,
        now: Timestamp,
    ) -> Result<DraftRecorded, StoreError> {
        Self::record_draft(self, account, write, now).await
    }

    async fn write_submission(
        &self,
        account: &AccountId,
        generation: u64,
        write: &SubmissionWrite,
        now: Timestamp,
    ) -> Result<SubmissionWritten, StoreError> {
        Self::write_submission(self, account, generation, write, now).await
    }

    async fn submission(
        &self,
        account: &AccountId,
        key: &SubmissionKey,
    ) -> Result<Option<SubmissionRow>, StoreError> {
        Self::submission(self, account, key).await
    }

    async fn submissions(
        &self,
        account: &AccountId,
        after: Option<&SubmissionKey>,
        states: &[SubmissionState],
        limit: u32,
    ) -> Result<Vec<SubmissionRow>, StoreError> {
        Self::submissions(self, account, after, states, limit).await
    }

    async fn drafts(
        &self,
        account: &AccountId,
        after: Option<&DraftId>,
        limit: u32,
    ) -> Result<Vec<DraftView>, StoreError> {
        self.draft_views(account, after, None, limit).await
    }

    async fn draft(
        &self,
        account: &AccountId,
        id: &DraftId,
    ) -> Result<Option<DraftView>, StoreError> {
        Ok(self
            .draft_views(account, None, Some(id), 1)
            .await?
            .into_iter()
            .next())
    }
}

async fn write_labels(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    before: &[Label],
    after: &[Label],
    seq: StateSeq,
) -> Result<(), StoreError> {
    let before: BTreeMap<_, _> = before.iter().map(|label| (&label.id, label)).collect();
    let after: BTreeMap<_, _> = after.iter().map(|label| (&label.id, label)).collect();
    for id in before.keys().filter(|id| !after.contains_key(*id)) {
        sqlx::query("DELETE FROM mailtide_labels WHERE account = $1 AND id = $2")
            .bind(account)
            .bind(id.0.as_str())
            .execute(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
    }
    for (id, label) in &after {
        if before.get(id) == Some(label) {
            continue;
        }
        let kind = match label.kind {
            LabelKind::System => "system",
            LabelKind::User => "user",
        };
        sqlx::query("INSERT INTO mailtide_labels (account, id, name, kind, attrs, defined_seq) VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (account, id) DO UPDATE SET name = excluded.name, kind = excluded.kind, attrs = excluded.attrs")
            .bind(account).bind(label.id.0.as_str()).bind(label.name.as_str()).bind(kind)
            .bind(LabelAttrs::of(label).map(|attrs| json(&attrs)).transpose()?)
            .bind(int("defined_seq", seq.0)?)
            .execute(&mut **tx).await.map_err(|e| unavailable(&e))?;
    }
    Ok(())
}

/// An account's label definitions, in id order.
async fn load_labels<'e, E>(executor: E, account: &AccountId) -> Result<Vec<Label>, StoreError>
where
    E: sqlx::Executor<'e, Database = Any>,
{
    let rows = sqlx::query("SELECT id, name, kind, attrs FROM mailtide_labels WHERE account = $1")
        .bind(account.as_str())
        .fetch_all(executor)
        .await
        .map_err(|e| unavailable(&e))?;
    let mut labels = rows
        .iter()
        .map(|row| {
            let kind: String = row.try_get("kind").map_err(|e| corrupt("kind", e))?;
            let attrs: Option<String> = row.try_get("attrs").map_err(|e| corrupt("attrs", e))?;
            let label = Label::plain(
                LabelId(row.try_get("id").map_err(|e| corrupt("id", e))?),
                row.try_get::<String, _>("name")
                    .map_err(|e| corrupt("name", e))?,
                match kind.as_str() {
                    "system" => LabelKind::System,
                    "user" => LabelKind::User,
                    other => return Err(corrupt("label kind", other)),
                },
            );
            match attrs {
                None => Ok(label),
                Some(text) => parse::<LabelAttrs>("label attrs", &text)?.apply(label),
            }
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    mailtide::mirror::sort_labels(&mut labels);
    Ok(labels)
}

/// A definition's visibility and colour as `mailtide_labels.attrs` holds
/// them (migration 7), in Gmail's spellings; `NULL` when none is known.
#[derive(serde::Serialize, serde::Deserialize)]
struct LabelAttrs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    list: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    background: Option<String>,
}

impl LabelAttrs {
    /// What `label` stores, or `None` when it knows neither visibility nor
    /// a colour.
    fn of(label: &Label) -> Option<Self> {
        let attrs = Self {
            list: label.list_visibility.map(|v| v.gmail_name().to_owned()),
            message: label.message_visibility.map(|v| v.gmail_name().to_owned()),
            text: label.color.as_ref().map(|c| c.text.clone()),
            background: label.color.as_ref().map(|c| c.background.clone()),
        };
        (attrs.list.is_some() || attrs.message.is_some() || attrs.text.is_some()).then_some(attrs)
    }

    /// `label` with these attributes. A spelling this build does not know
    /// is corrupt: only this build's own spellings are ever written.
    fn apply(self, label: Label) -> Result<Label, StoreError> {
        Ok(Label {
            list_visibility: self
                .list
                .map(|v| {
                    LabelListVisibility::from_gmail_name(&v)
                        .ok_or_else(|| corrupt("label list visibility", v))
                })
                .transpose()?,
            message_visibility: self
                .message
                .map(|v| {
                    MessageListVisibility::from_gmail_name(&v)
                        .ok_or_else(|| corrupt("message list visibility", v))
                })
                .transpose()?,
            color: match (self.text, self.background) {
                (Some(text), Some(background)) => Some(LabelColor { text, background }),
                (None, None) => None,
                (text, background) => {
                    return Err(corrupt(
                        "label colour",
                        format!("{text:?} on {background:?}"),
                    ));
                }
            },
            ..label
        })
    }
}

/// A `LIKE … ESCAPE '!'` pattern finding rows whose `labels` column holds
/// `label`: the id as the column's JSON spells it, `LIKE`'s wildcards and
/// the escape character escaped (`!`, not a backslash, whose meaning in a
/// Postgres string literal depends on `standard_conforming_strings`). A
/// match is only a filter (`SQLite` compares ASCII case-insensitively):
/// callers check the parsed set.
fn label_pattern(label: &LabelId) -> Result<String, StoreError> {
    let mut pattern = String::from("%");
    for c in json(&label.0)?.chars() {
        if matches!(c, '!' | '%' | '_') {
            pattern.push('!');
        }
        pattern.push(c);
    }
    pattern.push('%');
    Ok(pattern)
}

/// Ids per `IN (...)` query when reading views and feed content (well under
/// every backend's bind-parameter limit).
const VIEW_CHUNK: usize = 500;

/// Ids per `IN (...)` query when loading the rows a commit touches: a label
/// removal can touch every row of the account, and each query is a round
/// trip (under every backend's bind-parameter limit: `SQLite`'s is 32,766).
const LOAD_CHUNK: usize = 2_000;

/// Rows per multi-row `INSERT` when writing a commit's rows and feed entries
/// (six or three bound values each). Larger statements cost `SQLite` more
/// than they save it: 2,000 rows each took a 50,000-row label removal from
/// 1.6 s to 3.5 s (`examples/label_removal.rs`).
const WRITE_CHUNK: usize = 500;

/// The ids of a thread's rows, for thread action admission. Served by the
/// `mailtide_messages_thread` index (migration 6).
pub(crate) const THREAD_MEMBERS: &str =
    "SELECT id FROM mailtide_messages WHERE account = $1 AND thread_id = $2";

/// The rows of `count` threads with their intents, one row per (message,
/// intent): one statement, so one snapshot. Served by the
/// `mailtide_messages_thread` index (migration 6).
pub(crate) fn thread_rows(count: usize) -> String {
    let placeholders = (2..count + 2)
        .map(|n| format!("${n}"))
        .collect::<Vec<_>>()
        .join(", ");
    // Only numbered placeholders are interpolated; ids are bound.
    format!(
        "SELECT m.id, m.thread_id, m.labels, m.checkpoint, m.unverified, m.content, i.intent
         FROM mailtide_messages m
         LEFT JOIN mailtide_intents i ON i.account = m.account AND i.message = m.id
         WHERE m.account = $1 AND m.thread_id IN ({placeholders})"
    )
}

async fn load_token(
    tx: &mut Transaction<'static, Any>,
    account: &AccountId,
    token: &str,
    mirror: &mut AccountMirror,
) -> Result<(), StoreError> {
    if let Some(row) = sqlx::query(
        "SELECT owner FROM mailtide_send_tokens WHERE account = $1 AND tracking_id = $2",
    )
    .bind(account.as_str())
    .bind(token)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| unavailable(&e))?
    {
        let owner: String = row.try_get("owner").map_err(|e| corrupt("owner", e))?;
        mirror
            .send_tokens
            .insert(token.to_owned(), parse("owner", &owner)?);
    }
    Ok(())
}

fn submission_from_row(row: &sqlx::any::AnyRow) -> Result<SubmissionRow, StoreError> {
    let data: String = row
        .try_get("data")
        .map_err(|e| corrupt("submission data", e))?;
    parse("submission data", &data)
}

async fn load_import_submissions(
    tx: &mut Transaction<'static, Any>,
    account: &AccountId,
    ops: &[MessageOp],
    mirror: &mut AccountMirror,
) -> Result<(), StoreError> {
    for token in ops.iter().filter_map(|op| match op {
        MessageOp::MarkImported {
            facts: Some(facts), ..
        } => facts.tracking_id.as_deref(),
        _ => None,
    }) {
        if let Some(row) = sqlx::query(
            "SELECT data FROM mailtide_submissions WHERE account = $1 AND tracking_id = $2",
        )
        .bind(account.as_str())
        .bind(token)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?
        {
            let row = submission_from_row(&row)?;
            mirror.submissions.insert(row.view.key.clone(), row);
        }
    }
    Ok(())
}

async fn write_submission_diff(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    before: &AccountMirror,
    after: &AccountMirror,
) -> Result<(), StoreError> {
    for (token, owner) in &after.send_tokens {
        if before.send_tokens.get(token) == Some(owner) {
            continue;
        }
        sqlx::query(
            "INSERT INTO mailtide_send_tokens (account, tracking_id, owner) VALUES ($1, $2, $3)",
        )
        .bind(account)
        .bind(token.as_str())
        .bind(json(owner)?)
        .execute(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
    }
    for (key, row) in &after.submissions {
        if before.submissions.get(key) == Some(row) {
            continue;
        }
        sqlx::query(
            "INSERT INTO mailtide_submissions (account, key, tracking_id, state, due_at, data)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (account, key) DO UPDATE SET state = excluded.state,
                 due_at = excluded.due_at, data = excluded.data",
        )
        .bind(account)
        .bind(key.0.as_str())
        .bind(row.view.tracking_id.as_str())
        .bind(row.view.state.as_str())
        .bind(row.due_at.map(|at| at.0))
        .bind(json(row)?)
        .execute(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
    }
    Ok(())
}

impl SqlStore {
    async fn write_submission(
        &self,
        account: &AccountId,
        generation: u64,
        write: &SubmissionWrite,
        now: Timestamp,
    ) -> Result<SubmissionWritten, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let Some((record, seq)) = self.load_account(&mut tx, account).await? else {
            return Ok(SubmissionWritten::Stale);
        };
        if record.generation != generation {
            return Ok(SubmissionWritten::Stale);
        }
        let mut mirror = AccountMirror::new(account.clone(), generation);
        mirror.record = record;
        mirror.seq = seq;
        Self::load_account_jobs(&mut tx, account, &mut mirror).await?;
        let key = match write {
            SubmissionWrite::Admit(row) => {
                load_token(&mut tx, account, &row.view.tracking_id, &mut mirror).await?;
                Some(&row.view.key)
            }
            SubmissionWrite::Cancel(key)
            | SubmissionWrite::Begin(_, key)
            | SubmissionWrite::Complete(_, key, _)
            | SubmissionWrite::Prune(_, key) => Some(key),
            SubmissionWrite::Take(_) | SubmissionWrite::Release(_) | SubmissionWrite::Expire => {
                None
            }
        };
        let rows = if let Some(key) = key {
            sqlx::query("SELECT data FROM mailtide_submissions WHERE account = $1 AND key = $2")
                .bind(account.as_str())
                .bind(key.0.as_str())
                .fetch_all(&mut *tx)
                .await
        } else if matches!(write, SubmissionWrite::Expire) {
            sqlx::query(
                "SELECT data FROM mailtide_submissions WHERE account = $1 AND due_at <= $2
                        AND state IN ('sending', 'confirming')",
            )
            .bind(account.as_str())
            .bind(now.0)
            .fetch_all(&mut *tx)
            .await
        } else {
            sqlx::query(
                "SELECT data FROM mailtide_submissions WHERE account = $1 AND due_at IS NOT NULL
                         ORDER BY due_at, key LIMIT 1",
            )
            .bind(account.as_str())
            .fetch_all(&mut *tx)
            .await
        }
        .map_err(|e| unavailable(&e))?;
        for row in &rows {
            let row = submission_from_row(row)?;
            mirror.submissions.insert(row.view.key.clone(), row);
        }
        let before = mirror.clone();
        let result = mirror.write_submission(generation, write, now);
        if mirror != before {
            Self::write_diff(&mut tx, &before, &mirror).await?;
        }
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(result)
    }

    async fn submission(
        &self,
        account: &AccountId,
        key: &SubmissionKey,
    ) -> Result<Option<SubmissionRow>, StoreError> {
        self.ensure_schema().await?;
        sqlx::query("SELECT data FROM mailtide_submissions WHERE account = $1 AND key = $2")
            .bind(account.as_str())
            .bind(key.0.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| unavailable(&e))?
            .as_ref()
            .map(submission_from_row)
            .transpose()
    }

    async fn submissions(
        &self,
        account: &AccountId,
        after: Option<&SubmissionKey>,
        states: &[SubmissionState],
        limit: u32,
    ) -> Result<Vec<SubmissionRow>, StoreError> {
        self.ensure_schema().await?;
        let mut sql =
            "SELECT data FROM mailtide_submissions WHERE account = $1 AND key > $2".to_owned();
        if !states.is_empty() {
            let slots = (4..4 + states.len())
                .map(|n| format!("${n}"))
                .collect::<Vec<_>>()
                .join(", ");
            write!(sql, " AND state IN ({slots})").expect("writing to a string");
        }
        sql.push_str(" ORDER BY key LIMIT $3");
        let mut query = sqlx::query(AssertSqlSafe(sql))
            .bind(account.as_str())
            .bind(after.map_or("", |key| key.0.as_str()))
            .bind(i64::from(limit));
        for state in states {
            query = query.bind(state.as_str());
        }
        query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| unavailable(&e))?
            .iter()
            .map(submission_from_row)
            .collect()
    }
}
