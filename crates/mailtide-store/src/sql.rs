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
use mailtide::jobs::{AttemptId, ClaimedJob, Job, JobKind, JobState};
use mailtide::labels::LabelSet;
use mailtide::mirror::{
    AccountMirror, AccountRecord, Content, MessageRecord, MessageView, StoredContent,
};
use mailtide::outbox::{ActionRequest, Intent, IntentOp, plan_delivery};
use mailtide::quota::Priority;
use mailtide::reconcile::ListScope;
use mailtide::revision::{Change, ChangePage, StateSeq};
use mailtide::state::StoreSnapshot;
use sqlx::any::AnyRow;
use sqlx::pool::PoolOptions;
use sqlx::{Any, AssertSqlSafe, Pool, Row, Transaction};

type AnyPool = Pool<Any>;

use crate::StoreError;

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
]];

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

fn int(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn uint(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
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
    /// Connect to `url` (`postgres://…` or `sqlite:…`).
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        sqlx::any::install_default_drivers();
        let dialect = if url.starts_with("sqlite:") {
            Dialect::Sqlite
        } else {
            Dialect::Postgres
        };
        let options = match dialect {
            // One connection: SQLite serializes writers, and an in-memory
            // database lives only as long as its connection.
            Dialect::Sqlite => PoolOptions::<Any>::new()
                .max_connections(1)
                .min_connections(1)
                .idle_timeout(None)
                .max_lifetime(None),
            Dialect::Postgres => PoolOptions::<Any>::new().max_connections(8),
        };
        let pool = options.connect(url).await.map_err(|e| unavailable(&e))?;
        Ok(Self { pool, dialect })
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
        sqlx::query("CREATE TABLE IF NOT EXISTS mailtide_schema (version BIGINT PRIMARY KEY)")
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

    async fn load_account(
        &self,
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
    ) -> Result<Option<(AccountRecord, StateSeq)>, StoreError> {
        let sql = format!(
            "SELECT epoch, engine_version, cursor, sync, watch_expiration, last_activity, cooldown, rng_seed, seq
             FROM mailtide_accounts WHERE account = $1{}",
            self.lock_suffix()
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
            epoch: Epoch(uint(get_i("epoch")?)),
            engine_version: EngineVersion(u32::try_from(get_i("engine_version")?).unwrap_or(0)),
            cursor: get_oi("cursor")?.map(|c| HistoryId(uint(c))),
            sync: parse("sync", &sync)?,
            watch_expiration: get_oi("watch_expiration")?.map(Timestamp),
            last_activity: get_oi("last_activity")?.map(Timestamp),
            cooldown: cooldown
                .as_deref()
                .map(|c| parse("cooldown", c))
                .transpose()?,
            rng_seed: get_i("rng_seed")?.cast_unsigned(),
        };
        Ok(Some((record, StateSeq(uint(get_i("seq")?)))))
    }

    async fn write_account(
        tx: &mut Transaction<'static, Any>,
        account: &AccountId,
        record: &AccountRecord,
        seq: StateSeq,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO mailtide_accounts
               (account, epoch, engine_version, cursor, sync, watch_expiration, last_activity, cooldown, rng_seed, seq)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (account) DO UPDATE SET
               epoch = excluded.epoch, engine_version = excluded.engine_version, cursor = excluded.cursor,
               sync = excluded.sync, watch_expiration = excluded.watch_expiration,
               last_activity = excluded.last_activity, cooldown = excluded.cooldown,
               rng_seed = excluded.rng_seed, seq = excluded.seq",
        )
        .bind(account.as_str())
        .bind(int(record.epoch.0))
        .bind(i64::from(record.engine_version.0))
        .bind(record.cursor.map(|c| int(c.0)))
        .bind(json(&record.sync)?)
        .bind(record.watch_expiration.map(|t| t.0))
        .bind(record.last_activity.map(|t| t.0))
        .bind(record.cooldown.as_ref().map(json).transpose()?)
        .bind(record.rng_seed.cast_signed())
        .bind(int(seq.0))
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
        let mut mirror = AccountMirror::new(account.clone());
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
            .bind(int(baseline.0))
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
                .insert(id.clone(), HistoryId(uint(checkpoint)));
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
        commit: &Commit,
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
                .bind(int(checkpoint.0))
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
        for op in &commit.labels {
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
            .bind(int(seq.0))
            .bind(idx)
            .bind(json(change)?)
            .execute(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
        }
        Ok(())
    }

    // ------------------------------------------------------------ store operations

    pub(crate) async fn claim(
        &self,
        account: &AccountId,
        version: EngineVersion,
    ) -> Result<StoreSnapshot, StoreError> {
        let mut tx = self.begin().await?;
        // Create the row first, then lock it: two first claims racing must
        // still get different epochs (an upsert of a computed epoch wouldn't).
        let initial = AccountRecord::new(account);
        sqlx::query(
            "INSERT INTO mailtide_accounts
               (account, epoch, engine_version, cursor, sync, watch_expiration, last_activity, cooldown, rng_seed, seq)
             VALUES ($1, $2, $3, NULL, $4, NULL, NULL, NULL, $5, 0)
             ON CONFLICT (account) DO NOTHING",
        )
        .bind(account.as_str())
        .bind(int(initial.epoch.0))
        .bind(i64::from(initial.engine_version.0))
        .bind(json(&initial.sync)?)
        .bind(initial.rng_seed.cast_signed())
        .execute(&mut *tx)
        .await
        .map_err(|e| unavailable(&e))?;
        let (mut record, seq) = self
            .load_account(&mut tx, account)
            .await?
            .ok_or_else(|| corrupt("account", "missing after insert"))?;
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
                Self::write_diff(&mut tx, &before, &mirror, commit).await?;
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
    ) -> Result<Vec<ClaimedJob>, StoreError> {
        let mut tx = self.begin().await?;
        let current = self
            .load_account(&mut tx, account)
            .await?
            .map(|(record, _)| record.epoch);
        if current != Some(epoch) {
            return Err(StoreError::Rejected(Rejection::StaleEpoch));
        }
        let skip_locked = match self.dialect {
            Dialect::Postgres => " FOR UPDATE SKIP LOCKED",
            Dialect::Sqlite => "",
        };
        let sql = format!(
            "SELECT kind, message, priority, failed_attempts, state, due_at, attempt, failure FROM mailtide_jobs
             WHERE account = $1 AND state = 'ready' AND due_at <= $2
             ORDER BY priority DESC, due_at ASC, kind ASC, message ASC LIMIT $3{skip_locked}"
        );
        // Only a constant lock clause is appended.
        let rows = sqlx::query(AssertSqlSafe(sql))
            .bind(account.as_str())
            .bind(now.0)
            .bind(i64::from(limit))
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
            let modify = match &job.kind {
                JobKind::Deliver(id) => plan_delivery(&load_intents(&mut tx, account, id).await?)
                    .into_iter()
                    .next(),
                JobKind::Import(_) | JobKind::Verify(_) | JobKind::BlobCleanup(_) => None,
            };
            claimed.push(ClaimedJob {
                kind: job.kind,
                priority: job.priority,
                failed_attempts: job.failed_attempts,
                attempt,
                modify,
            });
        }
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(claimed)
    }

    pub(crate) async fn changes(
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
        .bind(int(since.0))
        .bind(i64::from(limit.max(1)))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| unavailable(&e))?;
        let Some(last) = window.last() else {
            return Ok(ChangePage {
                changes: Vec::new(),
                next: since,
                has_more: false,
            });
        };
        let cutoff: i64 = last.try_get("seq").map_err(|e| corrupt("seq", e))?;
        let rows = sqlx::query(
            "SELECT seq, change FROM mailtide_changes WHERE account = $1 AND seq > $2 AND seq <= $3 ORDER BY seq, idx",
        )
        .bind(account.as_str())
        .bind(int(since.0))
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
            changes.push(parse::<Change>("change", &change)?);
        }
        let next = StateSeq(uint(cutoff));
        Ok(ChangePage {
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
        checkpoint: checkpoint.map(|c| HistoryId(uint(c))),
        unverified: unverified.map(|c| HistoryId(uint(c))),
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
    .bind(record.checkpoint.map(|c| int(c.0)))
    .bind(record.unverified.map(|c| int(c.0)))
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
    ) -> Result<Vec<ClaimedJob>, StoreError> {
        Self::claim_jobs(self, account, epoch, limit, now, attempt).await
    }

    async fn changes(
        &self,
        account: &AccountId,
        since: StateSeq,
        limit: u32,
    ) -> Result<ChangePage, StoreError> {
        Self::changes(self, account, since, limit).await
    }

    async fn messages(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Vec<MessageView>, StoreError> {
        let mut tx = self.begin().await?;
        let mut mirror = AccountMirror::new(account.clone());
        for id in ids {
            self.load_message_rows(&mut tx, account, id, &mut mirror)
                .await?;
        }
        tx.rollback().await.map_err(|e| unavailable(&e))?;
        Ok(ids.iter().filter_map(|id| mirror.view(id)).collect())
    }
}
