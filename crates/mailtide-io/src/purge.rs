//! Purge: permanent delete of named messages or a thread's messages, and
//! emptying trash or spam (`docs/features/purge.md`).
//!
//! The one operation in mailtide that cannot be undone, so it is built
//! around four safeguards:
//!
//! 1. it is **off by default** ([`PurgePolicy::Off`]) and refused in
//!    read-only mode; purging outside trash and spam needs both the
//!    [`PurgePolicy::Anywhere`] ceiling and the call's own `anywhere`;
//! 2. it judges each message by **Gmail's state up to the call**: the
//!    history since the mirror's cursor is read first (committed nowhere)
//!    and folded into the mirror's rows ([`PurgeScan`]), so a message
//!    restored elsewhere a second ago is refused;
//! 3. it deletes **by message id only** and **never repeats** a delete: a
//!    purge is a call, not a queued intent, so nothing destructive survives
//!    a crash or fires later, and a lost answer is never sent again;
//! 4. a message becomes a tombstone only on **Gmail's evidence**: a
//!    `messagesDeleted` record in the history read after the delete, or a
//!    single delete answered 404. A 2xx alone is `sent`, never `purged`.
//!
//! Every call goes through `Runner::direct_call` (the quota gate, epoch 0,
//! no claim), so a refusal, a missing scope included, is the caller's and
//! never blocks the account, and the cursor never moves.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use mailtide::command::GmailRequest;
use mailtide::commit::DeletionEvidence;
use mailtide::event::EventKind;
use mailtide::ids::{AccountId, HistoryId, MessageId};
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
use crate::runner::{Access, CallError, QuotaDeferred, RunError, Runner};
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

/// Why a purge or an empty sent nothing (or, for `Run`, failed).
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
    /// deletes accept. Nothing was deleted; mailtide never falls back to
    /// trash, and the account keeps syncing.
    ScopeRequired,
    /// The quota gate would make the check wait past the budget.
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
            Self::ScopeRequired => "scope_required",
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

/// What the delete calls of one batch of eligible ids came to.
#[derive(Debug, Default)]
struct Deletes {
    /// Answered 2xx, or the answer was lost: Gmail may have deleted them.
    reached: Vec<MessageId>,
    /// A single delete answered 404: Gmail has no such message.
    missing: Vec<MessageId>,
    /// Gmail refused the call: nothing deleted.
    refused: Vec<(MessageId, Refusal)>,
    /// Not sent, or answered 429/5xx.
    unsent: Vec<MessageId>,
    /// A deferral, a rate limit or a failure ended the sending.
    stopped: bool,
}

impl Deletes {
    /// Whether any call may have changed Gmail, so history is worth
    /// reading for evidence.
    fn worth_confirming(&self) -> bool {
        !self.reached.is_empty() || !self.missing.is_empty() || !self.unsent.is_empty()
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
    /// Permanently delete `target`'s messages: named ids (at most
    /// [`PURGE_MAX_IDS`]) or the non-draft messages the mirror holds for a
    /// thread (never `threads.delete`). By default only messages in trash or
    /// spam as of the call; `anywhere` (with the
    /// [`PurgePolicy::Anywhere`] ceiling) reaches the rest.
    ///
    /// Reads history from the mirror's cursor (refusing with
    /// [`PurgeError::MirrorStale`] if that is not possible within
    /// [`PURGE_HISTORY_PAGES`] pages), decides each id, sends one
    /// `messages.delete` per id below five ids and `messages.batchDelete`
    /// otherwise, reads history again and records the deletions it shows.
    /// Quota deferrals are waited out within `budget`. mailtide never sends
    /// a delete twice: a lost answer is reported as `sent`.
    pub async fn purge(
        &self,
        account: &AccountId,
        target: &PurgeTarget,
        anywhere: bool,
        budget: Duration,
    ) -> Result<PurgeOutcome, PurgeError> {
        let deadline = Instant::now() + budget;
        let scope = self.purge_scope(anywhere)?;
        if let PurgeTarget::Messages(ids) = target {
            too_many(mailtide::purge::distinct(ids).len())?;
        }
        let cursor = self.purge_cursor(account).await?;
        let ids = match target {
            PurgeTarget::Messages(ids) => mailtide::purge::distinct(ids),
            PurgeTarget::Thread(thread) => {
                let threads = self
                    .store()
                    .threads(account, std::slice::from_ref(thread))
                    .await
                    .map_err(store_error)?;
                threads
                    .first()
                    .map(|view| mailtide::purge::thread_targets(&view.messages))
                    .unwrap_or_default()
            }
        };
        too_many(ids.len())?;
        let views = self.views(account, &ids).await?;
        let mut scan = PurgeScan::new();
        self.catch_up(account, cursor, &mut scan, deadline).await?;
        let mut eligible = Vec::new();
        let mut checked = Vec::new();
        let mut refused = Vec::new();
        for id in &ids {
            match scan.verdict(id, views.get(id), scope) {
                Verdict::Eligible => {
                    eligible.push(id.clone());
                    checked.push(id.clone());
                }
                // Recorded below, from the scan: history already shows it.
                Verdict::Gone { .. } => checked.push(id.clone()),
                Verdict::Refused(refusal) => refused.push((id.clone(), refusal)),
            }
        }
        let mut deletes = Deletes::default();
        self.send_deletes(account, &eligible, true, deadline, &mut deletes)
            .await?;
        if deletes.worth_confirming() {
            self.confirm(account, &mut scan, deadline).await;
        }
        // Only for messages the mirror holds: one already tombstoned stays
        // `not_in_mirror` although history still shows its deletion.
        let evidence = evidence(&checked, &scan, &deletes.missing);
        self.store()
            .record_deleted(account, &evidence, self.now())
            .await
            .map_err(store_error)?;
        let proven: BTreeSet<&MessageId> = evidence.iter().map(|(id, _)| id).collect();
        let unproven = |list: Vec<MessageId>| -> Vec<MessageId> {
            list.into_iter().filter(|id| !proven.contains(id)).collect()
        };
        refused.extend(deletes.refused);
        let outcome = PurgeOutcome {
            purged: ids
                .iter()
                .filter(|id| proven.contains(id))
                .cloned()
                .collect(),
            sent: unproven(deletes.reached),
            refused: refused
                .into_iter()
                .filter(|(id, _)| !proven.contains(id))
                .collect(),
            unsent: unproven(deletes.unsent),
        };
        tracing::info!(
            purged = outcome.purged.len(),
            sent = outcome.sent.len(),
            refused = outcome.refused.len(),
            unsent = outcome.unsent.len(),
            "purge finished"
        );
        Ok(outcome)
    }

    /// Permanently delete what the mirror holds in `bin` (the observed
    /// label; drafts, unverified rows and messages that left the bin are
    /// skipped), [`BATCH_DELETE_MAX`] at a time, until `budget` ends.
    ///
    /// One rolling history read from the cursor: the read after each chunk
    /// confirms it and also finds messages that left the bin meanwhile,
    /// which the chunks still to send then skip. A message trashed
    /// elsewhere that the mirror has not seen in the bin is left: this
    /// host's user never saw it there. Each chunk's deletions are recorded
    /// before the next is sent. `remaining` counts what was not reached;
    /// calling again reads the mirror afresh.
    pub async fn empty(
        &self,
        account: &AccountId,
        bin: Bin,
        budget: Duration,
    ) -> Result<EmptyOutcome, PurgeError> {
        let deadline = Instant::now() + budget;
        self.purge_scope(false)?;
        let cursor = self.purge_cursor(account).await?;
        let mut scan = PurgeScan::new();
        self.catch_up(account, cursor, &mut scan, deadline).await?;
        let mut outcome = EmptyOutcome::default();
        let mut after: Option<MessageId> = None;
        let mut stopped = false;
        let mut first = true;
        let count = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        loop {
            let page = self
                .store()
                .message_ids_labelled(
                    account,
                    &bin.label(),
                    after.as_ref(),
                    u32::try_from(BATCH_DELETE_MAX).unwrap_or(u32::MAX),
                )
                .await
                .map_err(store_error)?;
            let Some(last) = page.last() else {
                break;
            };
            after = Some(last.clone());
            if stopped || Instant::now() >= deadline {
                stopped = true;
                outcome.remaining += count(page.len());
                continue;
            }
            let views = self.views(account, &page).await?;
            let mut eligible = Vec::new();
            let mut checked = Vec::new();
            for id in &page {
                match scan.verdict(id, views.get(id), Scope::Bin(bin)) {
                    Verdict::Eligible => {
                        eligible.push(id.clone());
                        checked.push(id.clone());
                    }
                    Verdict::Gone { .. } => checked.push(id.clone()),
                    Verdict::Refused(_) => outcome.skipped += 1,
                }
            }
            let mut deletes = Deletes::default();
            self.send_deletes(account, &eligible, first, deadline, &mut deletes)
                .await?;
            first = false;
            if deletes.worth_confirming() {
                self.confirm(account, &mut scan, deadline).await;
            }
            let evidence = evidence(&checked, &scan, &deletes.missing);
            self.store()
                .record_deleted(account, &evidence, self.now())
                .await
                .map_err(store_error)?;
            let proven: BTreeSet<&MessageId> = evidence.iter().map(|(id, _)| id).collect();
            let unproven =
                |list: &[MessageId]| list.iter().filter(|id| !proven.contains(id)).count();
            outcome.purged += count(proven.len());
            outcome.sent += count(unproven(&deletes.reached));
            outcome.remaining += count(unproven(&deletes.unsent));
            outcome.skipped += count(
                deletes
                    .refused
                    .iter()
                    .filter(|(id, _)| !proven.contains(id))
                    .count(),
            );
            stopped = deletes.stopped;
        }
        tracing::info!(
            purged = outcome.purged,
            sent = outcome.sent,
            skipped = outcome.skipped,
            remaining = outcome.remaining,
            "empty finished"
        );
        Ok(outcome)
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
    /// at most [`PURGE_HISTORY_PAGES`] of them.
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

    /// The history read after deletes: where Gmail's evidence is. Best
    /// effort: when it fails, what was sent stays `sent` and the next sync
    /// finds the records.
    async fn confirm(&self, account: &AccountId, scan: &mut PurgeScan, deadline: Instant) {
        let Some(head) = scan.head() else {
            return;
        };
        if let Err(error) = self.catch_up(account, head, scan, deadline).await {
            tracing::warn!(
                error = error.code(),
                "purge confirmation unread; sent deletes stay unconfirmed"
            );
        }
    }

    /// Send the deletes for `ids`, each once. `nothing_before`: no earlier
    /// call of this purge reached Gmail, so a missing scope means nothing
    /// was deleted and the whole call is [`PurgeError::ScopeRequired`].
    async fn send_deletes(
        &self,
        account: &AccountId,
        ids: &[MessageId],
        nothing_before: bool,
        deadline: Instant,
        out: &mut Deletes,
    ) -> Result<(), PurgeError> {
        for request in mailtide::purge::delete_requests(ids) {
            let named = mailtide::purge::request_ids(&request);
            if out.stopped {
                out.unsent.extend(named);
                continue;
            }
            let single = matches!(request, GmailRequest::DeleteMessage { .. });
            let untouched = nothing_before && out.reached.is_empty() && out.missing.is_empty();
            match self
                .direct_call(account, request, Priority::Foreground, deadline)
                .await
            {
                // Answered, or the answer was lost and Gmail may have
                // applied it: never sent again either way; the
                // confirmation read tells purged from sent.
                Ok(EventKind::MessagesDeleted { .. }) | Err(CallError::OutcomeUnknown(_)) => {
                    out.reached.extend(named);
                }
                Ok(_) => {
                    return Err(PurgeError::Run(RunError::Invariant(
                        "unexpected answer to a delete",
                    )));
                }
                // Gmail has no such message: evidence for a single delete
                // (`purge-delete-missing`). A batch answers no id, so a 404
                // there proves nothing.
                Err(CallError::NotFound) if single => out.missing.extend(named),
                Err(CallError::NotFound) => {
                    refuse(out, named, "404 not found");
                }
                Err(CallError::Refused(refusal))
                    if mailtide_gmail::purge::missing_scope(&refusal) =>
                {
                    if untouched {
                        return Err(PurgeError::ScopeRequired);
                    }
                    refuse(out, named, &refusal.detail);
                    out.stopped = true;
                }
                Err(CallError::Refused(refusal)) => refuse(out, named, &refusal.detail),
                Err(CallError::Retryable(_) | CallError::Deferred(_)) => {
                    out.unsent.extend(named);
                    out.stopped = true;
                }
                Err(CallError::ReadOnly) => return Err(PurgeError::ReadOnly),
                Err(CallError::Run(error)) => {
                    if untouched && out.refused.is_empty() {
                        return Err(PurgeError::Run(error));
                    }
                    tracing::warn!(error = error.code(), "purge stopped: the runner failed");
                    out.unsent.extend(named);
                    out.stopped = true;
                }
            }
        }
        Ok(())
    }
}

fn refuse(out: &mut Deletes, ids: Vec<MessageId>, detail: &str) {
    out.refused.extend(
        ids.into_iter()
            .map(|id| (id, Refusal::GmailRefused(detail.to_owned()))),
    );
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

/// Gmail's evidence for `ids`: a `messagesDeleted` record in the scan, else
/// a single delete answered 404 (no later than the scan's head).
fn evidence(
    ids: &[MessageId],
    scan: &PurgeScan,
    missing: &[MessageId],
) -> Vec<(MessageId, DeletionEvidence)> {
    ids.iter()
        .filter_map(|id| {
            if let Some(at) = scan.deleted(id) {
                return Some((id.clone(), DeletionEvidence::History { at }));
            }
            let head = scan.head()?;
            missing.contains(id).then(|| {
                (
                    id.clone(),
                    DeletionEvidence::VerifiedMissing {
                        no_later_than: head,
                    },
                )
            })
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
    fn evidence_is_a_record_or_a_single_404() {
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
        let found = evidence(&[id("a"), id("b"), id("c")], &scan, &[id("b")]);
        assert_eq!(
            found,
            vec![
                (id("a"), DeletionEvidence::History { at: HistoryId(7) }),
                (
                    id("b"),
                    DeletionEvidence::VerifiedMissing {
                        no_later_than: HistoryId(9)
                    }
                ),
            ]
        );
    }
}
