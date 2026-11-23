//! Optional SQLite persistence for the fake Gmail server so state survives restarts.

use std::path::Path;

use sqlx::Row as _;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

use crate::state::{
    AccountMeta, AccountState, FakeGmailState, HistoryEvent, SentRecord, StoredMessage,
    derive_metadata,
};

const SCHEMA_STMTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS fake_accounts (
        email TEXT PRIMARY KEY,
        state_json TEXT NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS fake_messages (
        email TEXT NOT NULL,
        id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        labels_json TEXT NOT NULL,
        history_id INTEGER NOT NULL,
        internal_date_ms INTEGER NOT NULL,
        snippet TEXT NOT NULL,
        raw BLOB NOT NULL,
        PRIMARY KEY (email, id)
    )",
    "CREATE TABLE IF NOT EXISTS fake_drafts (
        email TEXT NOT NULL,
        draft_id TEXT NOT NULL,
        message_id TEXT NOT NULL,
        PRIMARY KEY (email, draft_id)
    )",
    "CREATE TABLE IF NOT EXISTS fake_history (
        email TEXT NOT NULL,
        history_id INTEGER NOT NULL,
        event_json TEXT NOT NULL,
        PRIMARY KEY (email, history_id)
    )",
    "CREATE TABLE IF NOT EXISTS fake_sent (
        email TEXT NOT NULL,
        seq INTEGER NOT NULL,
        record_json TEXT NOT NULL,
        PRIMARY KEY (email, seq)
    )",
];

/// SQLite-backed persistence handle (or no-op when running in memory).
#[derive(Clone, Debug)]
pub(crate) struct Persistence {
    pool: Option<SqlitePool>,
}

impl Persistence {
    /// No-op persistence for pure in-memory runs.
    pub(crate) fn memory() -> Self {
        Self { pool: None }
    }

    /// Open (or create) a SQLite state file and ensure tables exist.
    pub(crate) async fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create state directory: {e}"))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| format!("failed to open sqlite state: {e}"))?;
        for &stmt in SCHEMA_STMTS {
            sqlx::query(stmt)
                .execute(&pool)
                .await
                .map_err(|e| format!("failed to initialize sqlite schema: {e}"))?;
        }
        Ok(Self { pool: Some(pool) })
    }

    /// Load all persisted accounts into memory on startup.
    pub(crate) async fn load_all(&self) -> Result<FakeGmailState, String> {
        let mut state = FakeGmailState::default();
        let Some(pool) = &self.pool else {
            return Ok(state);
        };

        let account_rows = sqlx::query("SELECT email, state_json FROM fake_accounts")
            .fetch_all(pool)
            .await
            .map_err(|e| format!("failed to load fake_accounts: {e}"))?;
        for row in account_rows {
            let email: String = row.get("email");
            let state_json: String = row.get("state_json");
            let meta: AccountMeta = serde_json::from_str(&state_json)
                .map_err(|e| format!("corrupt fake_accounts row for {email}: {e}"))?;
            state
                .accounts
                .insert(email.clone(), AccountState::from_meta(email, meta));
        }

        let msg_rows = sqlx::query(
            "SELECT email, id, thread_id, labels_json, history_id, internal_date_ms, snippet, raw \
             FROM fake_messages",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| format!("failed to load fake_messages: {e}"))?;
        for row in msg_rows {
            let email: String = row.get("email");
            let Some(account) = state.accounts.get_mut(&email) else {
                continue;
            };
            let id: String = row.get("id");
            let thread_id: String = row.get("thread_id");
            let labels_json: String = row.get("labels_json");
            let labels: Vec<String> = serde_json::from_str(&labels_json).unwrap_or_default();
            let history_id: i64 = row.get("history_id");
            let internal_date_ms: i64 = row.get("internal_date_ms");
            let snippet: String = row.get("snippet");
            let raw: Vec<u8> = row.get("raw");
            let derived = derive_metadata(&raw, Some(internal_date_ms));
            account.messages.insert(
                id.clone(),
                StoredMessage {
                    id,
                    thread_id,
                    labels,
                    history_id: history_id as u64,
                    internal_date_ms,
                    snippet: if snippet.is_empty() {
                        derived.snippet
                    } else {
                        snippet
                    },
                    raw,
                    from: derived.from,
                    to: derived.to,
                    cc: derived.cc,
                    subject: derived.subject,
                    rfc822_message_id: derived.rfc822_message_id,
                    has_attachment: derived.has_attachment,
                    search_text: derived.search_text,
                },
            );
        }

        let draft_rows = sqlx::query("SELECT email, draft_id, message_id FROM fake_drafts")
            .fetch_all(pool)
            .await
            .map_err(|e| format!("failed to load fake_drafts: {e}"))?;
        for row in draft_rows {
            let email: String = row.get("email");
            if let Some(account) = state.accounts.get_mut(&email) {
                let draft_id: String = row.get("draft_id");
                let message_id: String = row.get("message_id");
                account.drafts.insert(draft_id, message_id);
            }
        }

        let history_rows = sqlx::query(
            "SELECT email, history_id, event_json FROM fake_history ORDER BY history_id ASC",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| format!("failed to load fake_history: {e}"))?;
        for row in history_rows {
            let email: String = row.get("email");
            if let Some(account) = state.accounts.get_mut(&email) {
                let history_id: i64 = row.get("history_id");
                let event_json: String = row.get("event_json");
                if let Ok(record) = serde_json::from_str(&event_json) {
                    account.history.push(HistoryEvent {
                        id: history_id as u64,
                        record,
                    });
                }
            }
        }

        let sent_rows = sqlx::query("SELECT email, record_json FROM fake_sent ORDER BY seq ASC")
            .fetch_all(pool)
            .await
            .map_err(|e| format!("failed to load fake_sent: {e}"))?;
        for row in sent_rows {
            let email: String = row.get("email");
            if let Some(account) = state.accounts.get_mut(&email) {
                let record_json: String = row.get("record_json");
                if let Ok(record) = serde_json::from_str::<SentRecord>(&record_json) {
                    account.sent_log.push(record);
                }
            }
        }

        Ok(state)
    }

    /// Persist a full account snapshot in one atomic transaction (fast for seeds and resets).
    pub(crate) async fn save_account(&self, account: &AccountState) {
        let Some(pool) = &self.pool else { return };
        let Ok(mut tx) = pool.begin().await else {
            return;
        };
        let meta_json = serde_json::to_string(&account.meta()).unwrap_or_default();
        let _ = sqlx::query(
            "INSERT INTO fake_accounts(email, state_json) VALUES (?, ?) \
             ON CONFLICT(email) DO UPDATE SET state_json = excluded.state_json",
        )
        .bind(&account.email)
        .bind(&meta_json)
        .execute(&mut *tx)
        .await;

        let _ = sqlx::query("DELETE FROM fake_messages WHERE email = ?")
            .bind(&account.email)
            .execute(&mut *tx)
            .await;
        for msg in account.messages.values() {
            let labels_json = serde_json::to_string(&msg.labels).unwrap_or_default();
            let _ = sqlx::query(
                "INSERT INTO fake_messages(email, id, thread_id, labels_json, history_id, \
                 internal_date_ms, snippet, raw) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&account.email)
            .bind(&msg.id)
            .bind(&msg.thread_id)
            .bind(&labels_json)
            .bind(msg.history_id as i64)
            .bind(msg.internal_date_ms)
            .bind(&msg.snippet)
            .bind(&msg.raw)
            .execute(&mut *tx)
            .await;
        }

        let _ = sqlx::query("DELETE FROM fake_drafts WHERE email = ?")
            .bind(&account.email)
            .execute(&mut *tx)
            .await;
        for (draft_id, mid) in &account.drafts {
            let _ = sqlx::query(
                "INSERT INTO fake_drafts(email, draft_id, message_id) VALUES (?, ?, ?)",
            )
            .bind(&account.email)
            .bind(draft_id)
            .bind(mid)
            .execute(&mut *tx)
            .await;
        }

        let _ = sqlx::query("DELETE FROM fake_history WHERE email = ?")
            .bind(&account.email)
            .execute(&mut *tx)
            .await;
        for ev in &account.history {
            let event_json = serde_json::to_string(&ev.record).unwrap_or_default();
            let _ = sqlx::query(
                "INSERT INTO fake_history(email, history_id, event_json) VALUES (?, ?, ?)",
            )
            .bind(&account.email)
            .bind(ev.id as i64)
            .bind(&event_json)
            .execute(&mut *tx)
            .await;
        }

        let _ = sqlx::query("DELETE FROM fake_sent WHERE email = ?")
            .bind(&account.email)
            .execute(&mut *tx)
            .await;
        for rec in &account.sent_log {
            let rec_json = serde_json::to_string(rec).unwrap_or_default();
            let _ = sqlx::query("INSERT INTO fake_sent(email, seq, record_json) VALUES (?, ?, ?)")
                .bind(&account.email)
                .bind(rec.seq as i64)
                .bind(&rec_json)
                .execute(&mut *tx)
                .await;
        }

        let _ = tx.commit().await;
    }

    /// Persist incremental changes for one account (metadata + touched messages/history/drafts/sent).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn save_delta(
        &self,
        email: &str,
        meta: &AccountMeta,
        upsert_messages: &[StoredMessage],
        delete_messages: &[String],
        upsert_drafts: &[(String, String)],
        delete_drafts: &[String],
        append_history: &[HistoryEvent],
        append_sent: &[SentRecord],
    ) {
        let Some(pool) = &self.pool else { return };
        let Ok(mut tx) = pool.begin().await else {
            return;
        };
        let meta_json = serde_json::to_string(meta).unwrap_or_default();
        let _ = sqlx::query(
            "INSERT INTO fake_accounts(email, state_json) VALUES (?, ?) \
             ON CONFLICT(email) DO UPDATE SET state_json = excluded.state_json",
        )
        .bind(email)
        .bind(&meta_json)
        .execute(&mut *tx)
        .await;

        for mid in delete_messages {
            let _ = sqlx::query("DELETE FROM fake_messages WHERE email = ? AND id = ?")
                .bind(email)
                .bind(mid)
                .execute(&mut *tx)
                .await;
        }
        for msg in upsert_messages {
            let labels_json = serde_json::to_string(&msg.labels).unwrap_or_default();
            let _ = sqlx::query(
                "INSERT INTO fake_messages(email, id, thread_id, labels_json, history_id, \
                 internal_date_ms, snippet, raw) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT(email, id) DO UPDATE SET \
                 thread_id = excluded.thread_id, labels_json = excluded.labels_json, \
                 history_id = excluded.history_id, internal_date_ms = excluded.internal_date_ms, \
                 snippet = excluded.snippet, raw = excluded.raw",
            )
            .bind(email)
            .bind(&msg.id)
            .bind(&msg.thread_id)
            .bind(&labels_json)
            .bind(msg.history_id as i64)
            .bind(msg.internal_date_ms)
            .bind(&msg.snippet)
            .bind(&msg.raw)
            .execute(&mut *tx)
            .await;
        }

        for draft_id in delete_drafts {
            let _ = sqlx::query("DELETE FROM fake_drafts WHERE email = ? AND draft_id = ?")
                .bind(email)
                .bind(draft_id)
                .execute(&mut *tx)
                .await;
        }
        for (draft_id, mid) in upsert_drafts {
            let _ = sqlx::query(
                "INSERT INTO fake_drafts(email, draft_id, message_id) VALUES (?, ?, ?) \
                 ON CONFLICT(email, draft_id) DO UPDATE SET message_id = excluded.message_id",
            )
            .bind(email)
            .bind(draft_id)
            .bind(mid)
            .execute(&mut *tx)
            .await;
        }

        for ev in append_history {
            let event_json = serde_json::to_string(&ev.record).unwrap_or_default();
            let _ = sqlx::query(
                "INSERT OR REPLACE INTO fake_history(email, history_id, event_json) \
                 VALUES (?, ?, ?)",
            )
            .bind(email)
            .bind(ev.id as i64)
            .bind(&event_json)
            .execute(&mut *tx)
            .await;
        }

        for rec in append_sent {
            let rec_json = serde_json::to_string(rec).unwrap_or_default();
            let _ = sqlx::query(
                "INSERT OR REPLACE INTO fake_sent(email, seq, record_json) VALUES (?, ?, ?)",
            )
            .bind(email)
            .bind(rec.seq as i64)
            .bind(&rec_json)
            .execute(&mut *tx)
            .await;
        }

        let _ = tx.commit().await;
    }

    /// Delete one account or all accounts from SQLite.
    pub(crate) async fn clear(&self, email: Option<&str>) {
        let Some(pool) = &self.pool else { return };
        let Ok(mut tx) = pool.begin().await else {
            return;
        };
        if let Some(addr) = email {
            for stmt in [
                "DELETE FROM fake_messages WHERE email = ?",
                "DELETE FROM fake_drafts WHERE email = ?",
                "DELETE FROM fake_history WHERE email = ?",
                "DELETE FROM fake_sent WHERE email = ?",
                "DELETE FROM fake_accounts WHERE email = ?",
            ] {
                let _ = sqlx::query(stmt).bind(addr).execute(&mut *tx).await;
            }
        } else {
            for stmt in [
                "DELETE FROM fake_messages",
                "DELETE FROM fake_drafts",
                "DELETE FROM fake_history",
                "DELETE FROM fake_sent",
                "DELETE FROM fake_accounts",
            ] {
                let _ = sqlx::query(stmt).execute(&mut *tx).await;
            }
        }
        let _ = tx.commit().await;
    }
}
