//! One SQL store for Postgres and `SQLite`, over sqlx's `Any` driver.
//!
//! A commit runs in one transaction: lock the account row, load exactly the
//! rows the commit touches (its messages, tombstones, jobs and reconciliation
//! seen-set; all older messages when reconciliation finishes), apply the
//! commit with the core's shared rules ([`AccountMirror::apply`]), and write
//! back the difference. The rules exist once, so SQL and memory stores can't
//! drift; the conformance suite checks both.

use std::collections::{BTreeMap, BTreeSet};

use mailtide::commit::{Commit, CommitOutcome, LabelOp, MessageOp, Rejection};
use mailtide::gmail::LabelKind;
use mailtide::ids::{
    AccountId, EngineVersion, Epoch, HistoryId, LabelId, MessageId, ThreadId, Timestamp,
};
use mailtide::jobs::{AttemptId, ClaimOutcome, ClaimedJob, ClaimedWork, Job, JobKind, JobState};
use mailtide::labels::LabelSet;
use mailtide::mirror::{
    AccountMirror, AccountRecord, Content, MessageRecord, MessageView, StoredContent,
};
use mailtide::outbox::{ActionRequest, Intent, IntentOp, plan_delivery};
use mailtide::quota::Priority;
use mailtide::reconcile::ListScope;
use mailtide::revision::{Change, ChangeEntry, ChangeFeed, ChangePage, StateSeq};
use mailtide::state::StoreSnapshot;
use mailtide::status::{
    AccountStatus, ActionCounts, ForgetOutcome, JobCounts, ResumeOutcome, TrackOutcome,
};
use sqlx::any::AnyRow;
use sqlx::pool::PoolOptions;
use sqlx::{Any, AssertSqlSafe, Pool, Row, Transaction};

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
&["ALTER TABLE mailtide_accounts ADD COLUMN generation BIGINT NOT NULL DEFAULT 0"]];

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

fn kind_code(kind: &JobKind) -> (i64, &MessageId) {
    match kind {
        JobKind::Import(id) => (0, id),
        JobKind::Verify(id) => (1, id),
        JobKind::BlobCleanup(id) => (2, id),
        JobKind::Deliver(id) => (3, id),
    }
}

fn kind_from(code: i64, message: String) -> Result<JobKind, StoreError> {
    let id = MessageId(message);
    match code {
        0 => Ok(JobKind::Import(id)),
        1 => Ok(JobKind::Verify(id)),
        2 => Ok(JobKind::BlobCleanup(id)),
        3 => Ok(JobKind::Deliver(id)),
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
        Ok(Self { pool, dialect })
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
            "mailtide_labels",
            "mailtide_intents",
            "mailtide_changes",
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
            "SELECT epoch, engine_version, cursor, sync, watch_expiration, last_activity, cooldown, rng_seed, seq, log_floor, generation
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
            log_floor: StateSeq(uint("log_floor", get_i("log_floor")?)?),
            generation: uint("generation", get_i("generation")?)?,
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
               (account, epoch, engine_version, cursor, sync, watch_expiration, last_activity, cooldown, rng_seed, seq, log_floor, generation)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (account) DO UPDATE SET
               epoch = excluded.epoch, engine_version = excluded.engine_version, cursor = excluded.cursor,
               sync = excluded.sync, watch_expiration = excluded.watch_expiration,
               last_activity = excluded.last_activity, cooldown = excluded.cooldown,
               rng_seed = excluded.rng_seed, seq = excluded.seq, log_floor = excluded.log_floor,
               generation = excluded.generation",
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
        .execute(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
        Ok(())
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
        if let Some(claim) = &commit.claim {
            ids.insert(kind_code(&claim.kind).1.clone());
        }
        for op in &commit.jobs {
            match op {
                mailtide::jobs::JobOp::Enqueue(job) => {
                    ids.insert(kind_code(&job.kind).1.clone());
                }
                mailtide::jobs::JobOp::Finish { claim, .. } => {
                    ids.insert(kind_code(&claim.kind).1.clone());
                }
            }
        }
        for op in &commit.intents {
            match op {
                IntentOp::Request(ActionRequest::Messages { ids: targets, .. }) => {
                    ids.extend(targets.iter().cloned());
                }
                IntentOp::Request(ActionRequest::Thread { id: thread, .. }) => {
                    let rows = sqlx::query(
                        "SELECT id FROM mailtide_messages WHERE account = $1 AND thread_id = $2",
                    )
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
        for id in &ids {
            self.load_message_rows(tx, account, id, &mut mirror).await?;
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
                let id = MessageId(row.try_get("id").map_err(|e| corrupt("id", e))?);
                if !ids.contains(&id) {
                    self.load_message_rows(tx, account, &id, &mut mirror)
                        .await?;
                    ids.insert(id);
                }
            }
        }
        Ok(mirror)
    }

    async fn load_message_rows(
        &self,
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
        id: &MessageId,
        mirror: &mut AccountMirror,
    ) -> Result<(), StoreError> {
        let message = sqlx::query(
            "SELECT thread_id, labels, checkpoint, unverified, content FROM mailtide_messages WHERE account = $1 AND id = $2",
        )
        .bind(account.as_str())
        .bind(id.0.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
        if let Some(row) = message {
            mirror
                .messages
                .insert(id.clone(), message_from_row(id, &row)?);
        }
        let tombstone = sqlx::query(
            "SELECT checkpoint FROM mailtide_tombstones WHERE account = $1 AND id = $2",
        )
        .bind(account.as_str())
        .bind(id.0.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
        if let Some(row) = tombstone {
            let checkpoint: i64 = row
                .try_get("checkpoint")
                .map_err(|e| corrupt("checkpoint", e))?;
            mirror
                .tombstones
                .insert(id.clone(), HistoryId(uint("checkpoint", checkpoint)?));
        }
        let jobs = sqlx::query(
            "SELECT kind, message, priority, failed_attempts, state, due_at, attempt, failure
             FROM mailtide_jobs WHERE account = $1 AND message = $2",
        )
        .bind(account.as_str())
        .bind(id.0.as_str())
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
        for row in jobs {
            let job = job_from_row(&row)?;
            mirror.jobs.insert(job.kind.clone(), job);
        }
        for intent in load_intents(tx, account, id).await? {
            mirror
                .intents
                .insert((intent.message.clone(), intent.label.clone()), intent);
        }
        let seen = sqlx::query("SELECT scope FROM mailtide_seen WHERE account = $1 AND id = $2")
            .bind(account.as_str())
            .bind(id.0.as_str())
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
        for row in seen {
            let scope: String = row.try_get("scope").map_err(|e| corrupt("scope", e))?;
            mirror
                .seen
                .entry(id.clone())
                .or_default()
                .insert(scope_from(&scope)?);
        }
        Ok(())
    }

    /// Write what changed between `before` and `after`.
    async fn write_diff(
        tx: &mut Transaction<'static, Any>,
        before: &AccountMirror,
        after: &AccountMirror,
        labels: &[LabelOp],
    ) -> Result<(), StoreError> {
        let account = after.account.as_str();
        Self::write_account(tx, &after.account, &after.record, after.seq).await?;
        let ids: BTreeSet<&MessageId> = before
            .messages
            .keys()
            .chain(after.messages.keys())
            .collect();
        for id in ids {
            match (before.messages.get(id), after.messages.get(id)) {
                (_, Some(record)) if before.messages.get(id) != Some(record) => {
                    put_message(tx, account, record).await?;
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
                        .bind(message.0.as_str())
                        .execute(&mut **tx)
                        .await
                        .map_err(|e| unavailable(&e))?;
                }
                (_, Some(_)) | (None, None) => {}
            }
        }
        write_seen(tx, account, before, after).await?;
        write_intents(tx, account, before, after).await?;
        for op in labels {
            match op {
                LabelOp::ReplaceAll(labels) => {
                    sqlx::query("DELETE FROM mailtide_labels WHERE account = $1")
                        .bind(account)
                        .execute(&mut **tx)
                        .await
                        .map_err(|e| unavailable(&e))?;
                    for label in labels {
                        let kind = match label.kind {
                            LabelKind::System => "system",
                            LabelKind::User => "user",
                        };
                        sqlx::query("INSERT INTO mailtide_labels (account, id, name, kind) VALUES ($1, $2, $3, $4)")
                            .bind(account)
                            .bind(label.id.0.as_str())
                            .bind(label.name.as_str())
                            .bind(kind)
                            .execute(&mut **tx)
                            .await
                            .map_err(|e| unavailable(&e))?;
                    }
                }
            }
        }
        for (idx, (seq, change)) in (0_i64..).zip(after.log.iter()) {
            sqlx::query(
                "INSERT INTO mailtide_changes (account, seq, idx, change) VALUES ($1, $2, $3, $4)",
            )
            .bind(account)
            .bind(int("seq", seq.0)?)
            .bind(idx)
            .bind(json(change)?)
            .execute(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
        }
        Ok(())
    }

    // ------------------------------------------------------------ store operations

    pub(crate) async fn track(
        &self,
        account: &AccountId,
        generation: u64,
    ) -> Result<TrackOutcome, StoreError> {
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
        .execute(&self.pool)
        .await
        .map_err(|e| unavailable(&e))?
        .rows_affected();
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
        })
    }

    pub(crate) async fn apply(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        let mut tx = self.begin().await?;
        let Some((record, seq)) = self.load_account(&mut tx, &commit.account).await? else {
            return Ok(CommitOutcome::Rejected(Rejection::StaleEpoch));
        };
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
                Self::write_diff(&mut tx, &before, &mirror, &commit.labels).await?;
                tx.commit()
                    .await
                    .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
            }
        }
        Ok(outcome)
    }

    pub(crate) async fn claim_jobs(
        &self,
        account: &AccountId,
        epoch: Epoch,
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
        floor: mailtide::quota::Priority,
    ) -> Result<ClaimOutcome, StoreError> {
        let mut tx = self.begin().await?;
        let current = self
            .load_account(&mut tx, account)
            .await?
            .map(|(record, _)| record.epoch);
        if current != Some(epoch) {
            return Ok(ClaimOutcome::Rejected(Rejection::StaleEpoch));
        }
        let skip_locked = match self.dialect {
            Dialect::Postgres => " FOR UPDATE SKIP LOCKED",
            Dialect::Sqlite => "",
        };
        let sql = format!(
            "SELECT kind, message, priority, failed_attempts, state, due_at, attempt, failure FROM mailtide_jobs
             WHERE account = $1 AND state = 'ready' AND due_at <= $2 AND priority >= $4
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
            .bind(message.0.as_str())
            .bind(attempt.0.cast_signed())
            .execute(&mut *tx)
            .await
            .map_err(|e| unavailable(&e))?;
            let group = match &job.kind {
                JobKind::Deliver(id) => plan_delivery(&load_intents(&mut tx, account, id).await?)
                    .into_iter()
                    .next(),
                JobKind::Import(_) | JobKind::Verify(_) | JobKind::BlobCleanup(_) => None,
            };
            claimed.push(ClaimedJob {
                work: ClaimedWork::plan(job.kind, |_| group),
                priority: job.priority,
                failed_attempts: job.failed_attempts,
                attempt,
            });
        }
        let row = sqlx::query(
            "SELECT MIN(due_at) AS due FROM mailtide_jobs WHERE account = $1 AND state = 'ready'",
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
        since: StateSeq,
        limit: u32,
    ) -> Result<ChangeFeed, StoreError> {
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
        if since < floor {
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
        since: StateSeq,
        limit: u32,
    ) -> Result<ChangePage, StoreError> {
        // A page ends on a whole commit: a state string names a commit, so a
        // page cut inside one would lose the rest of it.
        let window = sqlx::query(
            "SELECT seq FROM mailtide_changes WHERE account = $1 AND seq > $2 ORDER BY seq, idx LIMIT $3",
        )
        .bind(account.as_str())
        .bind(int("since", since.0)?)
        .bind(i64::from(limit.max(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| unavailable(&e))?;
        let Some(last) = window.last() else {
            return Ok(ChangePage {
                generation: 0,
                head: since,
                changes: Vec::new(),
                next: since,
                has_more: false,
            });
        };
        let cutoff: i64 = last.try_get("seq").map_err(|e| corrupt("seq", e))?;
        let rows = sqlx::query(
            "SELECT seq, idx, change FROM mailtide_changes WHERE account = $1 AND seq > $2 AND seq <= $3 ORDER BY seq, idx",
        )
        .bind(account.as_str())
        .bind(int("since", since.0)?)
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| unavailable(&e))?;
        let has_more =
            sqlx::query("SELECT seq FROM mailtide_changes WHERE account = $1 AND seq > $2 LIMIT 1")
                .bind(account.as_str())
                .bind(cutoff)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| unavailable(&e))?
                .is_some();
        let mut changes = Vec::new();
        for row in rows {
            let change: String = row.try_get("change").map_err(|e| corrupt("change", e))?;
            let seq: i64 = row.try_get("seq").map_err(|e| corrupt("seq", e))?;
            let idx: i64 = row.try_get("idx").map_err(|e| corrupt("idx", e))?;
            changes.push(ChangeEntry {
                state: StateSeq(uint("seq", seq)?),
                index: u32::try_from(idx).map_err(|e| corrupt("idx", e))?,
                change: parse::<Change>("change", &change)?,
            });
        }
        let next = StateSeq(uint("seq", cutoff)?);
        Ok(ChangePage {
            generation: 0,
            head: next,
            changes,
            next,
            has_more,
        })
    }
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

async fn put_message(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    record: &MessageRecord,
) -> Result<(), StoreError> {
    let content = match &record.content {
        Content::Pending => None,
        Content::Imported(stored) => Some(json(stored.as_ref())?),
    };
    sqlx::query(
        "INSERT INTO mailtide_messages (account, id, thread_id, labels, checkpoint, unverified, content)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (account, id) DO UPDATE SET thread_id = excluded.thread_id, labels = excluded.labels,
           checkpoint = excluded.checkpoint, unverified = excluded.unverified, content = excluded.content",
    )
    .bind(account)
    .bind(record.id.0.as_str())
    .bind(record.thread_id.0.as_str())
    .bind(json(&record.labels)?)
    .bind(record.checkpoint.map(|c| int("checkpoint", c.0)).transpose()?)
    .bind(record.unverified.map(|c| int("unverified", c.0)).transpose()?)
    .bind(content)
    .execute(&mut **tx)
    .await
    .map_err(|e| unavailable(&e))?;
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
    .bind(message.0.as_str())
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
    for (id, scopes) in removed {
        for scope in scopes {
            sqlx::query("DELETE FROM mailtide_seen WHERE account = $1 AND id = $2 AND scope = $3")
                .bind(account)
                .bind(id.0.as_str())
                .bind(scope_code(scope))
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

    async fn claim_jobs(
        &self,
        account: &AccountId,
        epoch: Epoch,
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
        floor: mailtide::quota::Priority,
    ) -> Result<ClaimOutcome, StoreError> {
        Self::claim_jobs(self, account, epoch, limit, now, attempt, floor).await
    }

    async fn changes(
        &self,
        account: &AccountId,
        since: StateSeq,
        limit: u32,
    ) -> Result<ChangeFeed, StoreError> {
        Self::changes(self, account, since, limit).await
    }

    async fn compact(&self, account: &AccountId, through: StateSeq) -> Result<u64, StoreError> {
        let mut tx = self.begin().await?;
        let Some((mut record, seq)) = self.load_account(&mut tx, account).await? else {
            return Ok(0);
        };
        let through = through.min(seq);
        let removed = sqlx::query("DELETE FROM mailtide_changes WHERE account = $1 AND seq <= $2")
            .bind(account.as_str())
            .bind(int("through", through.0)?)
            .execute(&mut *tx)
            .await
            .map_err(|e| unavailable(&e))?
            .rows_affected();
        record.log_floor = record.log_floor.max(through);
        Self::write_account(&mut tx, account, &record, seq).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(removed)
    }

    async fn message_ids(
        &self,
        account: &AccountId,
        after: Option<&MessageId>,
        limit: u32,
    ) -> Result<Vec<MessageId>, StoreError> {
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
        Self::write_diff(&mut tx, &before, &mirror, &[]).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(outcome)
    }

    async fn forget(&self, account: &AccountId) -> Result<ForgetOutcome, StoreError> {
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
            "mailtide_labels",
            "mailtide_intents",
            "mailtide_changes",
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
            },
            jobs: Vec::new(),
            intents: vec![IntentOp::Request(request.clone())],
        };
        let mut mirror = self
            .load_partial(&mut tx, account, record, seq, &carrier)
            .await?;
        let before = mirror.clone();
        let admitted = mirror.admit_action(request, now);
        Self::write_diff(&mut tx, &before, &mirror, &[]).await?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(admitted)
    }

    async fn messages(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Vec<MessageView>, StoreError> {
        // A view needs only the rows and their intents: two set-based
        // queries per chunk, not a handful per message.
        let mut records: BTreeMap<MessageId, MessageRecord> = BTreeMap::new();
        let mut intents: BTreeMap<MessageId, Vec<Intent>> = BTreeMap::new();
        for chunk in ids.chunks(VIEW_CHUNK) {
            let placeholders = (2..chunk.len() + 2)
                .map(|n| format!("${n}"))
                .collect::<Vec<_>>()
                .join(", ");
            // Only numbered placeholders are interpolated; ids are bound.
            let sql = format!(
                "SELECT id, thread_id, labels, checkpoint, unverified, content FROM mailtide_messages
                 WHERE account = $1 AND id IN ({placeholders})"
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
                let record = message_from_row(&id, &row)?;
                records.insert(id, record);
            }
            let sql = format!(
                "SELECT message, intent FROM mailtide_intents WHERE account = $1 AND message IN ({placeholders})"
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
                let message: String = row.try_get("message").map_err(|e| corrupt("message", e))?;
                let text: String = row.try_get("intent").map_err(|e| corrupt("intent", e))?;
                intents
                    .entry(MessageId(message))
                    .or_default()
                    .push(parse::<Intent>("intent", &text)?);
            }
        }
        let none = Vec::new();
        Ok(ids
            .iter()
            .filter_map(|id| {
                records
                    .get(id)
                    .map(|record| record.view(intents.get(id).unwrap_or(&none)))
            })
            .collect())
    }
}

/// Ids per `IN (...)` query when reading views (well under every backend's
/// bind-parameter limit).
const VIEW_CHUNK: usize = 500;
