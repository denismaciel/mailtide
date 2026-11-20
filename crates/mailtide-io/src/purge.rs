//! Purge: permanent delete of named messages or a thread's messages, and
//! emptying trash or spam (`docs/features/purge.md`).
//!
//! The runner checks policy and refreshes eligibility before each wire
//! request. Deletes are never queued, retried or delayed by the quota gate.
//! The dispatch deadline bounds the age of the check. Gmail may still change
//! between the read and the delete, or publish a history record late.
//! Only a `messagesDeleted` history record can lead to a tombstone.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use mailtide::command::GmailRequest;
use mailtide::commit::DeletionEvidence;
use mailtide::event::EventKind;
use mailtide::ids::{AccountId, HistoryId, MessageId, Timestamp};
use mailtide::mirror::MessageView;
use mailtide::purge::{
    BATCH_DELETE_MAX, Bin, EmptyOutcome, PURGE_HISTORY_PAGES, PURGE_MAX_IDS, PurgeOutcome,
    PurgePolicy, PurgeScan, PurgeTarget, Refusal, Scope, Verdict,
};
use mailtide::quota::Priority;
use mailtide::retry::{BlockingFailure, Retryable, RetryableFailure};
use mailtide::state::SyncState;
use mailtide_store::Store;

use crate::auth::TokenSource;
use crate::blobs::BlobStore;
use crate::quota_gate::QuotaGate;
use crate::runner::{Access, CallError, DirectCallResponse, QuotaDeferred, RunError, Runner};

const MAX_CHECK_AGE: Duration = Duration::from_secs(2);
use crate::transport::GmailTransport;

/// Why the mirror can't vouch for a purge: run the account first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stale {
    /// No history cursor yet (the first sync hasn't finished).
    NoCursor,
    /// The account is bootstrapping or reconciling, not live.
    NotLive,
    /// Gmail no longer has history from the cursor (404).
    HistoryExpired,
    /// More than [`PURGE_HISTORY_PAGES`] pages of history are unsynced.
    TooFarBehind,
}

/// Why a purge or empty could not proceed before any delete may have
/// applied. Later failures are reported with the partial outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PurgeError {
    /// The runner is read-only. Wins over every other answer.
    ReadOnly,
    /// The runner's purge policy is off (the default).
    Disabled,
    /// The call asked for `anywhere` and the policy is only
    /// [`PurgePolicy::TrashAndSpam`].
    AnywhereNotAllowed,
    /// An explicit purge named more than [`PURGE_MAX_IDS`] messages (or a
    /// thread has more): split the set on purpose.
    TooMany {
        /// The most a call takes.
        limit: usize,
        /// How many it named.
        given: usize,
    },
    /// The mirror is too far from Gmail to judge eligibility: run the
    /// account, then call again.
    MirrorStale(Stale),
    /// The token lacks `https://mail.google.com/`, the only scope Gmail's
    /// deletes accept (Gmail's message). Nothing was deleted; mailtide never
    /// falls back to trash, and the account keeps syncing.
    ScopeRequired(String),
    /// The quota gate would make the check's history read wait past the
    /// budget.
    Deferred(QuotaDeferred),
    /// Gmail failed the check's history read in a way worth retrying.
    Retryable(RetryableFailure),
    /// Gmail refused the check's history read for good (the caller's: the
    /// account is not blocked by it).
    Refused(BlockingFailure),
    /// The account is blocked until an operator acts.
    AccountBlocked(BlockingFailure),
    /// The account isn't tracked.
    NotTracked,
    /// The runner's own resources failed (the store, the quota gate).
    Run(RunError),
}

impl PurgeError {
    /// A stable name for logs (no detail).
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::Disabled => "disabled",
            Self::AnywhereNotAllowed => "anywhere_not_allowed",
            Self::TooMany { .. } => "too_many",
            Self::MirrorStale(_) => "mirror_stale",
            Self::ScopeRequired(_) => "scope_required",
            Self::Deferred(_) => "deferred",
            Self::Retryable(_) => "retryable",
            Self::Refused(_) => "refused",
            Self::AccountBlocked(_) => "account_blocked",
            Self::NotTracked => "not_tracked",
            Self::Run(error) => error.code(),
        }
    }
}

fn store_error(error: mailtide_store::StoreError) -> PurgeError {
    PurgeError::Run(RunError::Store(error))
}

#[derive(Clone)]
enum Disposition {
    Purged,
    Sent,
    Refused(Refusal),
    Unsent,
}

/// State shared by all requests in a purge or empty.
struct Purging {
    scope: Scope,
    deadline: Instant,
    cursor: HistoryId,
    scan: PurgeScan,
    may_have_applied: bool,
    stopped: bool,
    retry_at: Option<Timestamp>,
    interrupted: Option<String>,
}

impl Purging {
    fn new(scope: Scope, deadline: Instant, cursor: HistoryId) -> Self {
        Self {
            scope,
            deadline,
            cursor,
            scan: PurgeScan::new(),
            may_have_applied: false,
            stopped: false,
            retry_at: None,
            interrupted: None,
        }
    }

    fn interrupt(&mut self, why: &str) {
        self.stopped = true;
        self.interrupted.get_or_insert_with(|| why.to_owned());
    }

    fn failed(&mut self, error: PurgeError) -> Result<(), PurgeError> {
        if self.may_have_applied || self.interrupted.is_some() {
            self.interrupt(error.code());
            Ok(())
        } else {
            Err(error)
        }
    }
}

impl<S, B, T, Q, A> Runner<S, B, T, Q, A>
where
    S: Store,
    B: BlobStore,
    T: GmailTransport,
    Q: QuotaGate,
    A: TokenSource,
{
    /// Permanently delete the named messages or the mirror's non-draft
    /// members of the named thread. Each wire request gets a new history
    /// check and row read. The single-message endpoint gives a definite
    /// refusal for an invalid id. The batch endpoint handles five or more
    /// eligible ids in one request.
    pub async fn purge(
        &self,
        account: &AccountId,
        target: &PurgeTarget,
        anywhere: bool,
        budget: Duration,
    ) -> Result<PurgeOutcome, PurgeError> {
        let deadline = Instant::now() + budget;
        let scope = self.purge_scope(anywhere)?;
        let ids = match target {
            PurgeTarget::Messages(ids) => mailtide::purge::distinct(ids),
            PurgeTarget::Thread(thread) => self
                .store()
                .threads(account, std::slice::from_ref(thread))
                .await
                .map_err(store_error)?
                .first()
                .map(|view| mailtide::purge::thread_targets(&view.messages))
                .unwrap_or_default(),
        };
        too_many(ids.len())?;
        let cursor = self.purge_cursor(account).await?;
        let mut run = Purging::new(scope, deadline, cursor);
        let mut dispositions = self.purge_round(account, &ids, &mut run).await?;
        let mut outcome = PurgeOutcome {
            retry_at: run.retry_at,
            interrupted: run.interrupted,
            ..PurgeOutcome::default()
        };
        for id in ids {
            match dispositions.remove(&id).unwrap_or(Disposition::Unsent) {
                Disposition::Purged => outcome.purged.push(id),
                Disposition::Sent => outcome.sent.push(id),
                Disposition::Refused(reason) => outcome.refused.push((id, reason)),
                Disposition::Unsent => outcome.unsent.push(id),
            }
        }
        tracing::info!(
            purged = outcome.purged.len(),
            sent = outcome.sent.len(),
            refused = outcome.refused.len(),
            unsent = outcome.unsent.len(),
            "purge finished"
        );
        Ok(outcome)
    }

    /// Empty the mirror's verified, non-draft members of the selected bin.
    /// Every delete gets its own fresh check and the run stops on an unread
    /// confirmation. The budget limits when new deletes can be dispatched.
    pub async fn empty(
        &self,
        account: &AccountId,
        bin: Bin,
        budget: Duration,
    ) -> Result<EmptyOutcome, PurgeError> {
        let deadline = Instant::now() + budget;
        self.purge_scope(false)?;
        let cursor = self.purge_cursor(account).await?;
        let mut run = Purging::new(Scope::Bin(bin), deadline, cursor);
        let mut outcome = EmptyOutcome::default();
        let mut after = None;
        loop {
            let page = match self
                .store()
                .message_ids_labelled(
                    account,
                    &bin.label(),
                    after.as_ref(),
                    u32::try_from(BATCH_DELETE_MAX).unwrap_or(u32::MAX),
                )
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    run.failed(store_error(error))?;
                    break;
                }
            };
            let Some(last) = page.last() else {
                break;
            };
            after = Some(last.clone());
            if run.stopped || Instant::now() >= deadline {
                outcome.remaining += u64::try_from(page.len()).unwrap_or(u64::MAX);
                continue;
            }
            for disposition in self
                .purge_round(account, &page, &mut run)
                .await?
                .into_values()
            {
                match disposition {
                    Disposition::Purged => outcome.purged += 1,
                    Disposition::Sent => outcome.sent += 1,
                    Disposition::Refused(_) => outcome.skipped += 1,
                    Disposition::Unsent => outcome.remaining += 1,
                }
            }
        }
        outcome.retry_at = run.retry_at;
        outcome.interrupted = run.interrupted;
        tracing::info!(
            purged = outcome.purged,
            sent = outcome.sent,
            skipped = outcome.skipped,
            remaining = outcome.remaining,
            "empty finished"
        );
        Ok(outcome)
    }

    /// Process explicit ids or one mirror page, assigning exactly one final
    /// disposition per id. Any targets left on a stop are unsent.
    async fn purge_round(
        &self,
        account: &AccountId,
        ids: &[MessageId],
        run: &mut Purging,
    ) -> Result<BTreeMap<MessageId, Disposition>, PurgeError> {
        let mut dispositions = BTreeMap::new();
        let mut pending = ids.to_vec();
        while !pending.is_empty() && !run.stopped && Instant::now() < run.deadline {
            let send_before = run.deadline.min(Instant::now() + MAX_CHECK_AGE);
            let views = match self.check_targets(account, &pending, run).await {
                Ok(views) => views,
                Err(error) => {
                    run.failed(error)?;
                    break;
                }
            };
            let mut gone = Vec::new();
            pending.retain(|id| match run.scan.verdict(id, views.get(id), run.scope) {
                Verdict::Eligible => true,
                Verdict::Gone { .. } => {
                    gone.push(id.clone());
                    false
                }
                Verdict::Refused(reason) => {
                    dispositions.insert(id.clone(), Disposition::Refused(reason));
                    false
                }
            });
            self.record_shown(account, &gone, run, &mut dispositions)
                .await;
            if run.stopped {
                break;
            }
            let Some(request) = mailtide::purge::delete_requests(&pending)
                .into_iter()
                .next()
            else {
                break;
            };
            let named = mailtide::purge::request_ids(&request);
            let single = matches!(request, GmailRequest::DeleteMessage { .. });
            let response = self
                .direct_call_before(account, request, Priority::Foreground, send_before)
                .await;
            let Some(disposition) = delete_disposition(response, single, run)? else {
                // Quota admission or credential acquisition can outlive the
                // check. The request never reached the transport, so check
                // again within the call budget.
                continue;
            };
            pending.drain(..named.len());
            let confirm = matches!(disposition, Disposition::Sent | Disposition::Refused(_));
            for id in &named {
                dispositions.insert(id.clone(), disposition.clone());
            }
            if confirm {
                let start = run.scan.head().unwrap_or(run.cursor);
                if let Err(error) = self
                    .catch_up(account, start, &mut run.scan, run.deadline)
                    .await
                {
                    run.interrupt(error.code());
                }
                self.record_shown(account, &named, run, &mut dispositions)
                    .await;
            }
        }
        dispositions.extend(pending.into_iter().map(|id| (id, Disposition::Unsent)));
        Ok(dispositions)
    }

    async fn check_targets(
        &self,
        account: &AccountId,
        ids: &[MessageId],
        run: &mut Purging,
    ) -> Result<BTreeMap<MessageId, MessageView>, PurgeError> {
        let start = run.scan.head().unwrap_or(run.cursor);
        self.catch_up(account, start, &mut run.scan, run.deadline)
            .await?;
        self.views(account, ids).await
    }

    /// The gate every purge passes before anything is read: read-only mode
    /// first, then the policy.
    fn purge_scope(&self, anywhere: bool) -> Result<Scope, PurgeError> {
        if self.access() == Access::ReadOnly {
            return Err(PurgeError::ReadOnly);
        }
        match (self.purge_policy(), anywhere) {
            (PurgePolicy::Off, _) => Err(PurgeError::Disabled),
            (PurgePolicy::TrashAndSpam, true) => Err(PurgeError::AnywhereNotAllowed),
            (PurgePolicy::Anywhere, true) => Ok(Scope::Anywhere),
            (PurgePolicy::TrashAndSpam | PurgePolicy::Anywhere, false) => Ok(Scope::TrashOrSpam),
        }
    }

    /// The mirror's cursor, read before any row so that every row read
    /// after it is at least as new: history from it covers them all.
    async fn purge_cursor(&self, account: &AccountId) -> Result<HistoryId, PurgeError> {
        let status = self
            .store()
            .status(account)
            .await
            .map_err(store_error)?
            .ok_or(PurgeError::NotTracked)?;
        match (&status.sync, status.cursor) {
            (SyncState::Blocked { reason, .. }, _) => {
                Err(PurgeError::AccountBlocked(reason.clone()))
            }
            (_, None) => Err(PurgeError::MirrorStale(Stale::NoCursor)),
            (SyncState::Live, Some(cursor)) => Ok(cursor),
            (SyncState::Bootstrapping(_) | SyncState::Reconciling(_), Some(_)) => {
                Err(PurgeError::MirrorStale(Stale::NotLive))
            }
        }
    }

    async fn views(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<BTreeMap<MessageId, MessageView>, PurgeError> {
        Ok(self
            .store()
            .messages(account, ids)
            .await
            .map_err(store_error)?
            .into_iter()
            .map(|view| (view.id.clone(), view))
            .collect())
    }

    /// Read history from `start` to Gmail's head into `scan`, every page,
    /// at most [`PURGE_HISTORY_PAGES`] of them. A read may wait out the
    /// quota gate within `deadline`; nothing is deleted meanwhile.
    async fn catch_up(
        &self,
        account: &AccountId,
        start: HistoryId,
        scan: &mut PurgeScan,
        deadline: Instant,
    ) -> Result<(), PurgeError> {
        let mut page_token = None;
        for _ in 0..PURGE_HISTORY_PAGES {
            let request = GmailRequest::ListHistory { start, page_token };
            match self
                .direct_call(account, request, Priority::Foreground, deadline)
                .await
            {
                Ok(EventKind::HistoryFetched { page, .. }) => {
                    scan.read(&page);
                    match page.next_page_token {
                        None => return Ok(()),
                        Some(next) => page_token = Some(next),
                    }
                }
                Ok(EventKind::HistoryExpired { .. }) => {
                    return Err(PurgeError::MirrorStale(Stale::HistoryExpired));
                }
                Ok(_) => {
                    return Err(PurgeError::Run(RunError::Invariant(
                        "unexpected answer to a history read",
                    )));
                }
                Err(error) => return Err(read_error(error)),
            }
        }
        Err(PurgeError::MirrorStale(Stale::TooFarBehind))
    }

    /// Only recorded history evidence promotes an id to purged. If a
    /// transaction fails or the account was forgotten, the evidence is
    /// unrecorded and the caller still learns about possible application.
    async fn record_shown(
        &self,
        account: &AccountId,
        checked: &[MessageId],
        run: &mut Purging,
        dispositions: &mut BTreeMap<MessageId, Disposition>,
    ) {
        let evidence = evidence(checked, &run.scan);
        if evidence.is_empty() {
            return;
        }
        let disposition = match self
            .store()
            .record_deleted(account, &evidence, self.now())
            .await
        {
            Ok(_) => Disposition::Purged,
            Err(error) => {
                run.interrupt(store_error(error).code());
                Disposition::Sent
            }
        };
        dispositions.extend(
            evidence
                .into_iter()
                .map(|(id, _)| (id, disposition.clone())),
        );
    }
}

fn delete_disposition(
    response: Result<Option<DirectCallResponse>, CallError>,
    single: bool,
    run: &mut Purging,
) -> Result<Option<Disposition>, PurgeError> {
    let response = match response {
        Ok(Some(response)) => response,
        Ok(None) => return Ok(None),
        Err(CallError::Deferred(QuotaDeferred { until })) => {
            run.stopped = true;
            run.retry_at = Some(until);
            return Ok(Some(Disposition::Unsent));
        }
        Err(error) => {
            run.failed(read_error(error))?;
            return Ok(Some(Disposition::Unsent));
        }
    };
    if let Some(error) = response.cleanup {
        run.interrupt(error.code());
    }
    run.retry_at = response.retry_at;
    let disposition = match response.outcome {
        Ok(EventKind::MessagesDeleted { .. }) | Err(CallError::NotFound) => Disposition::Sent,
        Ok(_) => {
            run.interrupt("unexpected answer to a delete");
            Disposition::Sent
        }
        Err(CallError::OutcomeUnknown(_)) => {
            run.interrupt("outcome_unknown");
            Disposition::Sent
        }
        Err(CallError::Refused(refusal)) if mailtide_gmail::purge::missing_scope(&refusal) => {
            if !run.may_have_applied {
                return Err(PurgeError::ScopeRequired(refusal.detail));
            }
            run.interrupt("scope_required");
            Disposition::Refused(Refusal::GmailRefused(refusal.detail))
        }
        Err(CallError::Refused(refusal))
            if response.not_sent
                || (single
                    && matches!(
                        refusal.detail.split_ascii_whitespace().next(),
                        Some("400" | "403")
                    )) =>
        {
            Disposition::Refused(Refusal::GmailRefused(refusal.detail))
        }
        Err(CallError::Refused(_)) => Disposition::Sent,
        Err(CallError::Retryable(failure))
            if !response.not_sent && mailtide_gmail::purge::may_have_applied(&failure) =>
        {
            run.interrupt("outcome_unknown");
            Disposition::Sent
        }
        Err(CallError::Retryable(_)) => {
            run.stopped = true;
            Disposition::Unsent
        }
        Err(error) => {
            run.failed(read_error(error))?;
            Disposition::Unsent
        }
    };
    run.may_have_applied |= matches!(disposition, Disposition::Sent);
    Ok(Some(disposition))
}

fn too_many(given: usize) -> Result<(), PurgeError> {
    if given > PURGE_MAX_IDS {
        return Err(PurgeError::TooMany {
            limit: PURGE_MAX_IDS,
            given,
        });
    }
    Ok(())
}

/// Gmail's evidence for `ids`: a `messagesDeleted` record in the scan, and
/// nothing else.
fn evidence(ids: &[MessageId], scan: &PurgeScan) -> Vec<(MessageId, DeletionEvidence)> {
    ids.iter()
        .filter_map(|id| {
            scan.deleted(id)
                .map(|at| (id.clone(), DeletionEvidence::History { at }))
        })
        .collect()
}

/// A failed history read in a purge's terms: nothing was sent.
fn read_error(error: CallError) -> PurgeError {
    match error {
        CallError::Deferred(deferred) => PurgeError::Deferred(deferred),
        CallError::Retryable(failure) => PurgeError::Retryable(failure),
        // A lost answer to a read is only a transient failure.
        CallError::OutcomeUnknown(detail) => PurgeError::Retryable(RetryableFailure {
            class: Retryable::Transient,
            retry_after: None,
            detail,
        }),
        CallError::Refused(refusal) => PurgeError::Refused(refusal),
        // A history 404 arrives as HistoryExpired.
        CallError::NotFound => PurgeError::MirrorStale(Stale::HistoryExpired),
        CallError::ReadOnly => PurgeError::Run(RunError::Invariant("a history read is a read")),
        CallError::Run(error) => PurgeError::Run(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_is_a_history_record_and_nothing_else() {
        let id = |s: &str| MessageId(s.into());
        let mut scan = PurgeScan::new();
        scan.read(&mailtide::gmail::HistoryPage {
            records: vec![mailtide::gmail::HistoryRecord::MessageDeleted {
                id: HistoryId(7),
                message_id: id("a"),
            }],
            history_id: HistoryId(9),
            next_page_token: None,
        });
        assert_eq!(
            evidence(&[id("a"), id("b")], &scan),
            vec![(id("a"), DeletionEvidence::History { at: HistoryId(7) })]
        );
    }
}
