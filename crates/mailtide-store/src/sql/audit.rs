//! Bounded audit transactions. Each page holds the account lock only while
//! reading the page's keys and recording the jobs and continuation.

use mailtide::audit::{
    self, AUDIT_PAGE_SIZE, AuditOutcome, AuditProgress, AuditReport, AuditRequest, AuditStep,
};
use mailtide::state::SyncState;

use mailtide::commit::Rejection;
use mailtide::ids::{MessageId, Timestamp};
use mailtide::jobs::JobKind;
use mailtide::quota::Priority;
use sqlx::any::AnyRow;
use sqlx::{Any, AssertSqlSafe, Row, Transaction};

use super::{SqlStore, StoreError, corrupt, json, kind_code, priority_code, unavailable};

impl SqlStore {
    pub(crate) async fn advance_audit(
        &self,
        request: &AuditRequest,
    ) -> Result<AuditOutcome, StoreError> {
        self.ensure_schema().await?;
        let mut tx = self.begin().await?;
        let Some((record, _)) = self.load_account(&mut tx, &request.account).await? else {
            return Ok(AuditOutcome::Rejected(Rejection::StaleEpoch));
        };
        if let Err(reason) = request.check(
            record.epoch,
            record.sync == SyncState::Live,
            record.audit_progress.as_ref(),
        ) {
            return Ok(AuditOutcome::Rejected(reason));
        }
        let account = request.account.as_str();
        let mut report = AuditReport::default();
        let mut completed_at = None;
        let progress = match (&request.step, request.expected.as_ref()) {
            (AuditStep::Page { ids, next }, _) => {
                let missing = found_missing(&mut tx, account, ids).await?;
                queue_checks(&mut tx, account, &missing, request.now).await?;
                report.found_missing = u32::try_from(missing.len()).unwrap_or(u32::MAX);
                Some(audit::after_listing(next.clone()))
            }
            (AuditStep::Continue, Some(AuditProgress::Sweep(after))) => {
                let (ids, unlisted) = sweep(&mut tx, account, after.as_ref()).await?;
                queue_checks(&mut tx, account, &unlisted, request.now).await?;
                report.found_unlisted = u32::try_from(unlisted.len()).unwrap_or(u32::MAX);
                Some(audit::after_sweep(&ids))
            }
            (AuditStep::Continue, Some(AuditProgress::Cleanup { after, complete })) => {
                let ids = clear_seen(&mut tx, account, after.as_ref()).await?;
                let (next, finished) = audit::after_cleanup(&ids, *complete, request.now);
                completed_at = finished;
                next
            }
            (AuditStep::Restart, _) => Some(audit::cleanup(false)),
            (AuditStep::Continue, _) => unreachable!("validated audit step"),
        };
        let audited_at = completed_at
            .map(|at| record.audited_at.map_or(at, |previous| at.max(previous)))
            .or(record.audited_at);
        sqlx::query(
            "UPDATE mailtide_accounts SET audited_at = $2, audit_progress = $3 WHERE account = $1",
        )
        .bind(account)
        .bind(audited_at.map(|at| at.0))
        .bind(progress.as_ref().map(json).transpose()?)
        .execute(&mut *tx)
        .await
        .map_err(|e| unavailable(&e))?;
        tx.commit()
            .await
            .map_err(|e| StoreError::OutcomeUnknown(e.to_string()))?;
        Ok(AuditOutcome::Applied {
            completed_at,
            report,
        })
    }
}

/// Mark what Gmail listed, then examine only newly observed audit ids. A
/// repeated id on another page will not be counted and queued twice.
async fn found_missing(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    ids: &[MessageId],
) -> Result<Vec<MessageId>, StoreError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let values = (2..ids.len() + 2)
        .map(|index| format!("($1, ${index})"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut query = sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO mailtide_audit_seen (account, id) VALUES {values}
         ON CONFLICT (account, id) DO NOTHING RETURNING id"
    )))
    .bind(account);
    for id in ids {
        query = query.bind(id.0.as_str());
    }
    let rows = query
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
    let added: Vec<_> = rows.iter().map(id_from_row).collect::<Result<_, _>>()?;
    if added.is_empty() {
        return Ok(added);
    }
    let placeholders = (2..added.len() + 2)
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut query = sqlx::query(AssertSqlSafe(format!(
        "SELECT s.id FROM mailtide_audit_seen s WHERE s.account = $1 AND s.id IN ({placeholders})
           AND NOT EXISTS (SELECT 1 FROM mailtide_messages m WHERE m.account = s.account AND m.id = s.id)
           AND NOT EXISTS (SELECT 1 FROM mailtide_tombstones t WHERE t.account = s.account AND t.id = s.id)"
    ))).bind(account);
    for id in &added {
        query = query.bind(id.0.as_str());
    }
    let rows = query
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
    rows.iter().map(id_from_row).collect()
}

fn after_key(after: Option<&MessageId>) -> &str {
    after.map_or("", |id| id.0.as_str())
}

fn id_from_row(row: &AnyRow) -> Result<MessageId, StoreError> {
    row.try_get("id")
        .map(MessageId)
        .map_err(|e| corrupt("id", e))
}

async fn sweep(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    after: Option<&MessageId>,
) -> Result<(Vec<MessageId>, Vec<MessageId>), StoreError> {
    let rows = sqlx::query(
        "SELECT page.id, s.id AS seen
         FROM (SELECT id FROM mailtide_messages WHERE account = $1 AND id > $2 ORDER BY id LIMIT $3) page
         LEFT JOIN mailtide_audit_seen s ON s.account = $1 AND s.id = page.id
         ORDER BY page.id",
    )
    .bind(account).bind(after_key(after)).bind(i64::try_from(AUDIT_PAGE_SIZE).unwrap_or(i64::MAX))
    .fetch_all(&mut **tx).await.map_err(|e| unavailable(&e))?;
    let mut ids = Vec::with_capacity(rows.len());
    let mut unlisted = Vec::new();
    for row in &rows {
        let id = id_from_row(row)?;
        let seen: Option<String> = row.try_get("seen").map_err(|e| corrupt("seen", e))?;
        if seen.is_none() {
            unlisted.push(id.clone());
        }
        ids.push(id);
    }
    Ok((ids, unlisted))
}

async fn clear_seen(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    after: Option<&MessageId>,
) -> Result<Vec<MessageId>, StoreError> {
    let after = after_key(after);
    let rows = sqlx::query(
        "SELECT id FROM mailtide_audit_seen WHERE account = $1 AND id > $2 ORDER BY id LIMIT $3",
    )
    .bind(account)
    .bind(after)
    .bind(i64::try_from(AUDIT_PAGE_SIZE).unwrap_or(i64::MAX))
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| unavailable(&e))?;
    let ids: Vec<_> = rows.iter().map(id_from_row).collect::<Result<_, _>>()?;
    if let Some(last) = ids.last() {
        sqlx::query("DELETE FROM mailtide_audit_seen WHERE account = $1 AND id > $2 AND id <= $3")
            .bind(account)
            .bind(after)
            .bind(last.0.as_str())
            .execute(&mut **tx)
            .await
            .map_err(|e| unavailable(&e))?;
    }
    Ok(ids)
}

/// Backfill is the lowest priority. An existing job keeps its priority,
/// claim, attempt count and due time just as it would on ordinary enqueue.
async fn queue_checks(
    tx: &mut Transaction<'static, Any>,
    account: &str,
    ids: &[MessageId],
    now: Timestamp,
) -> Result<(), StoreError> {
    let Some(first) = ids.first() else {
        return Ok(());
    };
    let kind = JobKind::Verify(first.clone());
    let values = (5..ids.len() + 5)
        .map(|index| format!("($1, $2, ${index}, $3, 0, 'ready', $4)"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut query = sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO mailtide_jobs (account, kind, message, priority, failed_attempts, state, due_at)
         VALUES {values} ON CONFLICT (account, kind, message) DO NOTHING"
    )))
    .bind(account).bind(kind_code(&kind).0).bind(priority_code(Priority::Backfill)).bind(now.0);
    for id in ids {
        query = query.bind(id.0.as_str());
    }
    query
        .execute(&mut **tx)
        .await
        .map_err(|e| unavailable(&e))?;
    Ok(())
}
