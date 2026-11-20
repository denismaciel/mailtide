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
//!    history since the mirror's cursor is read first (committed nowhere),
//!    then the targets' rows, and the two are folded ([`PurgeScan`]); an
//!    action not yet confirmed here can only refuse a message, never make
//!    it eligible. A delete **never waits** for the quota gate, so it goes
//!    out right after its check (or not at all);
//! 3. it deletes **by message id only** and **never repeats** a delete: a
//!    purge is a call, not a queued intent, so nothing destructive survives
//!    a crash or fires later, and a lost answer is never sent again;
//! 4. a message becomes a tombstone only on **Gmail's evidence**: a
//!    `messagesDeleted` record in the history read after the delete. A 2xx,
//!    a lost answer, a 5xx or a 404 alone is `sent`, never `purged`.
//!
//! What it cannot close: a change made elsewhere between the history read
//! and the delete (milliseconds, the delete never waiting), or one Gmail
//! shows in history late.
//!
//! Every call goes through `Runner::direct_call` (the quota gate, epoch 0,
//! no claim), so a refusal, a missing scope included, is the caller's and
//! never blocks the account, and the cursor never moves.

use std::collections::{BTreeMap, BTreeSet};
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

/// Why a purge or an empty did nothing. Every error is returned before
/// any delete reached Gmail; a failure after that is reported in the
/// outcome (`interrupted`), with what was known.
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

/// What the delete calls of a whole purge or empty have done so far.
#[derive(Debug, Default)]
struct Sender {
    /// A delete reached Gmail, or may have: from now on a failure is
    /// reported in the outcome, not as an error.
    reached_gmail: bool,
    /// Nothing more is sent.
    stopped: bool,
    /// When a deferred or rate-limited delete could go.
    retry_at: Option<Timestamp>,
    /// Why the call ended early after a delete may have reached Gmail.
    interrupted: Option<String>,
}

impl Sender {
    fn interrupt(&mut self, why: &str) {
        self.stopped = true;
        self.interrupted.get_or_insert_with(|| why.to_owned());
    }
}

/// What the delete calls for one set of eligible ids came to.
#[derive(Debug, Default)]
struct Chunk {
    /// Answered 2xx, or may have been applied (a lost answer, a 5xx, a
    /// 404): history decides.
    reached: Vec<MessageId>,
    /// Gmail refused the call. A batch may still have deleted some.
    refused: Vec<(MessageId, Refusal)>,
    /// Never sent.
    unsent: Vec<MessageId>,
}

impl Chunk {
    /// Whether history is worth reading for evidence: anything sent, or
    /// refused (a refused batch may have deleted some of its ids).
    fn worth_confirming(&self) -> bool {
        !self.reached.is_empty() || !self.refused.is_empty()
    }

    fn refuse(&mut self, ids: Vec<MessageId>, detail: &str) {
        self.refused.extend(
            ids.into_iter()
                .map(|id| (id, Refusal::GmailRefused(detail.to_owned()))),
        );
    }
}

/// The state of one `empty` across its chunks.
struct Emptying {
    bin: Bin,
    deadline: Instant,
    /// The rolling history read: from the cursor, then after each chunk.
    scan: PurgeScan,
    sender: Sender,
    outcome: EmptyOutcome,
}

/// The targets of one round: what to send, and what history may prove.
#[derive(Debug, Default)]
struct Judged {
    eligible: Vec<MessageId>,
    /// Eligible or already deleted elsewhere: the ids evidence is read for.
    checked: Vec<MessageId>,
    refused: Vec<(MessageId, Refusal)>,
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
    /// [`PURGE_HISTORY_PAGES`] pages), then the targets' rows, decides each
    /// id, sends one `messages.delete` per id below five ids and
    /// `messages.batchDelete` otherwise (each at once or not at all: a
    /// delete never waits for the quota gate), reads history again and
    /// records the deletions it shows. The check's history read waits out
    /// deferrals within `budget`. mailtide never sends a delete twice.
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
        let mut scan = PurgeScan::new();
        self.catch_up(account, cursor, &mut scan, deadline).await?;
        // The rows are read after history, right before the deletes, so an
        // action admitted here while history was read is seen.
        let views = self.views(account, &ids).await?;
        let judged = judge(&ids, &views, &scan, scope);
        let mut sender = Sender::default();
        let mut chunk = Chunk::default();
        self.send_deletes(account, &judged.eligible, &mut sender, &mut chunk)
            .await?;
        if chunk.worth_confirming() {
            self.confirm(account, &mut scan, deadline).await;
        }
        let proven = self
            .record_evidence(account, &judged.checked, &scan, &mut sender)
            .await?;
        let unproven = |list: Vec<MessageId>| -> Vec<MessageId> {
            list.into_iter().filter(|id| !proven.contains(id)).collect()
        };
        let mut refused = judged.refused;
        refused.extend(chunk.refused);
        // Evidence not recorded (the store failed) is still only `sent`.
        let mut sent = unproven(chunk.reached);
        let unrecorded: Vec<MessageId> = judged
            .checked
            .iter()
            .filter(|id| scan.deleted(id).is_some() && !proven.contains(*id) && !sent.contains(id))
            .cloned()
            .collect();
        sent.extend(unrecorded);
        let outcome = PurgeOutcome {
            purged: ids
                .iter()
                .filter(|id| proven.contains(id))
                .cloned()
                .collect(),
            refused: refused
                .into_iter()
                .filter(|(id, _)| !proven.contains(id) && !sent.contains(id))
                .collect(),
            sent,
            unsent: unproven(chunk.unsent),
            retry_at: sender.retry_at,
            interrupted: sender.interrupted,
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
    /// which the chunks still to send then skip. When that read fails, no
    /// further chunk is sent (it would be judged on stale history). A
    /// message trashed elsewhere that the mirror has not seen in the bin is
    /// left: this host's user never saw it there. Each chunk's deletions
    /// are recorded before the next is sent. `remaining` counts what was
    /// not reached; calling again reads the mirror afresh.
    pub async fn empty(
        &self,
        account: &AccountId,
        bin: Bin,
        budget: Duration,
    ) -> Result<EmptyOutcome, PurgeError> {
        let deadline = Instant::now() + budget;
        self.purge_scope(false)?;
        let cursor = self.purge_cursor(account).await?;
        let mut run = Emptying {
            bin,
            deadline,
            scan: PurgeScan::new(),
            sender: Sender::default(),
            outcome: EmptyOutcome::default(),
        };
        self.catch_up(account, cursor, &mut run.scan, deadline)
            .await?;
        let mut after: Option<MessageId> = None;
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
                Err(error) if run.sender.reached_gmail => {
                    run.sender.interrupt(RunError::Store(error).code());
                    break;
                }
                Err(error) => return Err(store_error(error)),
            };
            let Some(last) = page.last() else {
                break;
            };
            after = Some(last.clone());
            if run.sender.stopped || Instant::now() >= deadline {
                run.sender.stopped = true;
                run.outcome.remaining += count(page.len());
                continue;
            }
            self.empty_chunk(account, &page, &mut run).await?;
        }
        let mut outcome = run.outcome;
        outcome.retry_at = run.sender.retry_at;
        outcome.interrupted = run.sender.interrupted;
        tracing::info!(
            purged = outcome.purged,
            sent = outcome.sent,
            skipped = outcome.skipped,
            remaining = outcome.remaining,
            "empty finished"
        );
        Ok(outcome)
    }

    /// One chunk of `empty`: judge `page` against the rolling `scan`, send,
    /// confirm, record and count.
    async fn empty_chunk(
        &self,
        account: &AccountId,
        page: &[MessageId],
        run: &mut Emptying,
    ) -> Result<(), PurgeError> {
        let Emptying {
            bin,
            deadline,
            scan,
            sender,
            outcome,
        } = run;
        let (bin, deadline) = (*bin, *deadline);
        let views = match self.views(account, page).await {
            Ok(views) => views,
            Err(PurgeError::Run(error)) if sender.reached_gmail => {
                sender.interrupt(error.code());
                outcome.remaining += count(page.len());
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let judged = judge(page, &views, scan, Scope::Bin(bin));
        outcome.skipped += count(judged.refused.len());
        let mut chunk = Chunk::default();
        self.send_deletes(account, &judged.eligible, sender, &mut chunk)
            .await?;
        if chunk.worth_confirming() && !self.confirm(account, scan, deadline).await {
            // Later chunks would be judged on history from before this
            // one's deletes: stop.
            sender.stopped = true;
        }
        let proven = self
            .record_evidence(account, &judged.checked, scan, sender)
            .await?;
        let shown = |id: &&MessageId| scan.deleted(id).is_some() && !proven.contains(*id);
        outcome.purged += count(proven.len());
        outcome.sent += count(
            chunk
                .reached
                .iter()
                .filter(|id| !proven.contains(*id))
                .count(),
        );
        // Shown by history, not recorded (the store failed): only `sent`.
        outcome.sent += count(
            judged
                .checked
                .iter()
                .filter(shown)
                .filter(|id| !chunk.reached.contains(id))
                .count(),
        );
        outcome.remaining += count(
            chunk
                .unsent
                .iter()
                .filter(|id| !proven.contains(*id))
                .count(),
        );
        outcome.skipped += count(
            chunk
                .refused
                .iter()
                .filter(|(id, _)| !proven.contains(id))
                .count(),
        );
        Ok(())
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

    /// The history read after deletes: where Gmail's evidence is. `false`
    /// when it failed: what was sent stays `sent`, the next sync finds the
    /// records, and an `empty` sends nothing more.
    async fn confirm(&self, account: &AccountId, scan: &mut PurgeScan, deadline: Instant) -> bool {
        let Some(head) = scan.head() else {
            return false;
        };
        match self.catch_up(account, head, scan, deadline).await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(
                    error = error.code(),
                    "purge confirmation unread; sent deletes stay unconfirmed"
                );
                false
            }
        }
    }

    /// Record the deletions history shows for `checked`; the ids recorded.
    /// A store failure after a delete reached Gmail interrupts the call
    /// (the next sync records them); before, it is the call's error.
    async fn record_evidence(
        &self,
        account: &AccountId,
        checked: &[MessageId],
        scan: &PurgeScan,
        sender: &mut Sender,
    ) -> Result<BTreeSet<MessageId>, PurgeError> {
        let evidence = evidence(checked, scan);
        if evidence.is_empty() {
            return Ok(BTreeSet::new());
        }
        match self
            .store()
            .record_deleted(account, &evidence, self.now())
            .await
        {
            Ok(_) => Ok(evidence.into_iter().map(|(id, _)| id).collect()),
            Err(error) if sender.reached_gmail => {
                sender.interrupt(RunError::Store(error).code());
                Ok(BTreeSet::new())
            }
            Err(error) => Err(store_error(error)),
        }
    }

    /// Send the deletes for `ids`, each once and each at once: a delete the
    /// quota gate cannot admit now is `unsent`, never sent later. A missing
    /// scope before any delete reached Gmail is the call's
    /// [`PurgeError::ScopeRequired`]; every other answer is the outcome's.
    async fn send_deletes(
        &self,
        account: &AccountId,
        ids: &[MessageId],
        sender: &mut Sender,
        out: &mut Chunk,
    ) -> Result<(), PurgeError> {
        for request in mailtide::purge::delete_requests(ids) {
            let named = mailtide::purge::request_ids(&request);
            if sender.stopped {
                out.unsent.extend(named);
                continue;
            }
            let single = matches!(request, GmailRequest::DeleteMessage { .. });
            // Never wait: a deadline of now turns any deferral into an
            // answer at once.
            let answer = self
                .direct_call(account, request, Priority::Foreground, Instant::now())
                .await;
            match answer {
                // Answered, or the answer was lost, or a 404 (which proves
                // nothing: `purge-delete-missing` is unsettled and a proxy
                // can answer 404): never sent again; history decides.
                Ok(EventKind::MessagesDeleted { .. })
                | Err(CallError::OutcomeUnknown(_) | CallError::NotFound) => {
                    out.reached.extend(named);
                    sender.reached_gmail = true;
                }
                Ok(_) => {
                    out.reached.extend(named);
                    sender.reached_gmail = true;
                    sender.interrupt("unexpected answer to a delete");
                }
                Err(CallError::Refused(refusal))
                    if mailtide_gmail::purge::missing_scope(&refusal) =>
                {
                    if !sender.reached_gmail {
                        return Err(PurgeError::ScopeRequired(refusal.detail));
                    }
                    out.refuse(named, &refusal.detail);
                    sender.stopped = true;
                }
                Err(CallError::Refused(refusal)) => {
                    out.refuse(named, &refusal.detail);
                    // A refused batch may have deleted some of its ids.
                    sender.reached_gmail |= !single;
                }
                Err(CallError::Retryable(failure))
                    if mailtide_gmail::purge::may_have_applied(&failure) =>
                {
                    out.reached.extend(named);
                    sender.reached_gmail = true;
                    sender.stopped = true;
                }
                Err(CallError::Retryable(failure)) => {
                    out.unsent.extend(named);
                    sender.stopped = true;
                    if let Some(after) = failure.retry_after {
                        sender.retry_at.get_or_insert(self.now().plus(after));
                    }
                }
                Err(CallError::Deferred(QuotaDeferred { until })) => {
                    out.unsent.extend(named);
                    sender.stopped = true;
                    sender.retry_at.get_or_insert(until);
                }
                Err(CallError::ReadOnly) => {
                    return Err(PurgeError::Run(RunError::Invariant(
                        "a purge's delete was refused as a write",
                    )));
                }
                Err(CallError::Run(error)) => {
                    if !sender.reached_gmail {
                        return Err(PurgeError::Run(error));
                    }
                    out.unsent.extend(named);
                    sender.interrupt(error.code());
                }
            }
        }
        Ok(())
    }
}

fn count(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

/// Decide each target with the pure rules.
fn judge(
    ids: &[MessageId],
    views: &BTreeMap<MessageId, MessageView>,
    scan: &PurgeScan,
    scope: Scope,
) -> Judged {
    let mut judged = Judged::default();
    for id in ids {
        match scan.verdict(id, views.get(id), scope) {
            Verdict::Eligible => {
                judged.eligible.push(id.clone());
                judged.checked.push(id.clone());
            }
            // History already shows it deleted: recorded, nothing sent.
            Verdict::Gone { .. } => judged.checked.push(id.clone()),
            Verdict::Refused(refusal) => judged.refused.push((id.clone(), refusal)),
        }
    }
    judged
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
