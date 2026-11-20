//! Draft calls: create, update, delete, send and read a Gmail draft, and the
//! mirror's draft index (SPEC §7a).
//!
//! Each write is one direct call ([`Runner::call_gmail`]) through the quota
//! gate, with Gmail's answer recorded in the index by an unfenced store
//! write ([`Store::record_draft`]). mailtide never retries a draft write on
//! its own. A send is at most once: the row is marked `sending` (a
//! compare-and-set) before the call; a retry waits out the first request
//! and reads the draft first; a lost answer is settled by the sync, never by
//! sending again.
//!
//! A draft always goes in the JSON body, whatever its size; one above
//! [`WriteSettings::max_message_bytes`](crate::runner::WriteSettings) is
//! refused before any call (there is no upload path).
//!
//! The bytes a host saves must carry exactly one `X-Mailtide-Id`: Gmail
//! replaces the Message-ID on every save and on send, and keeps that header,
//! so it is what identifies the draft's sent message.

use std::time::{Duration, Instant};

use mailtide::command::GmailRequest;
use mailtide::drafts::{
    DraftFormat, DraftRecorded, DraftStatus, DraftView, DraftWrite, ListedDraft, RemovedReason,
    Rfc822, SendEnd, WrittenDraft,
};
use mailtide::event::EventKind;
use mailtide::ids::{AccountId, DraftId, MessageId, ThreadId, Timestamp};
use mailtide::quota::Priority;
use mailtide::retry::{Blocking, BlockingFailure, Retryable, RetryableFailure};
use mailtide_store::Store;

use crate::auth::TokenSource;
use crate::blobs::BlobStore;
use crate::quota_gate::QuotaGate;
use crate::runner::{CallError, QuotaDeferred, RunError, Runner};
use crate::transport::GmailTransport;

/// A draft and the message it holds after a save: Gmail's draft id is
/// stable, the message id changes with every save.
pub type DraftRef = ListedDraft;

/// What deleting a draft found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deleted {
    /// Gmail deleted it (permanently, not to trash).
    Deleted,
    /// Gmail had no such draft (a retried delete lands here).
    AlreadyGone,
}

/// The message a draft became when it was sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentDraft {
    /// The sent message: a new id, labelled `SENT`.
    pub message_id: MessageId,
    /// Its thread.
    pub thread_id: ThreadId,
}

/// A draft's message as Gmail stores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftContent {
    /// The draft and its current message.
    pub draft: ListedDraft,
    /// The message's RFC 5322 bytes (Gmail's copy: its own Message-ID and
    /// Date).
    pub raw: Vec<u8>,
}

/// Why a draft call did not do what it was asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DraftError {
    /// Gmail has no such draft, or the index does not know it (send).
    NotFound,
    /// The draft holds another message than the caller saw: it changed.
    Conflict {
        /// The message it holds.
        current: MessageId,
    },
    /// The message is above the size limit
    /// ([`WriteSettings::max_message_bytes`](crate::runner::WriteSettings));
    /// nothing was sent.
    TooLarge {
        /// The limit, in bytes.
        limit: usize,
    },
    /// The bytes do not carry exactly one valid `X-Mailtide-Id`, or carry
    /// another than the draft's; nothing was sent.
    Invalid(String),
    /// The quota gate would hold it past the call's budget; nothing was
    /// sent.
    Deferred(QuotaDeferred),
    /// The runner is read-only; nothing was sent.
    ReadOnly,
    /// Gmail refused it for good (400, 403): the caller's to report. The
    /// account is not blocked.
    Refused(BlockingFailure),
    /// Worth trying again later; nothing was applied.
    Retryable(RetryableFailure),
    /// It may or may not have been applied (a timeout, a dropped
    /// connection, a 5xx). A send in this state is settled by the sync.
    OutcomeUnknown(String),
    /// Another send of the draft may still be in flight; a retry reads the
    /// draft from `retry_after` on.
    SendInProgress {
        /// When.
        retry_after: Timestamp,
    },
    /// A send's answer was lost and the draft is gone: it was sent at most
    /// once. `DraftSent` or `DraftRemoved{send_unconfirmed}` settles it.
    SendUnconfirmed,
    /// The runner's own resources failed.
    Run(RunError),
}

impl From<CallError> for DraftError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::ReadOnly => Self::ReadOnly,
            CallError::Deferred(deferred) => Self::Deferred(deferred),
            CallError::NotFound => Self::NotFound,
            CallError::Refused(refusal) => Self::Refused(refusal),
            CallError::Retryable(failure) => Self::Retryable(failure),
            CallError::OutcomeUnknown(detail) => Self::OutcomeUnknown(detail),
            CallError::Run(error) => Self::Run(error),
        }
    }
}

impl DraftError {
    /// A stable name for logs (no detail: it can quote data).
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::Conflict { .. } => "conflict",
            Self::TooLarge { .. } => "too_large",
            Self::Invalid(_) => "invalid",
            Self::Deferred(_) => "deferred",
            Self::ReadOnly => "read_only",
            Self::Refused(_) => "refused",
            Self::Retryable(_) => "retryable",
            Self::OutcomeUnknown(_) => "outcome_unknown",
            Self::SendInProgress { .. } => "send_in_progress",
            Self::SendUnconfirmed => "send_unconfirmed",
            Self::Run(error) => error.code(),
        }
    }
}

/// An answer that is not one of the draft answers the request can get.
fn unexpected() -> DraftError {
    DraftError::Run(RunError::Invariant("unexpected answer to a draft call"))
}

fn refused(detail: &str) -> DraftError {
    DraftError::Refused(BlockingFailure {
        class: Blocking::Permanent,
        detail: detail.to_owned(),
    })
}

/// Whether a failed send certainly sent nothing: a rate limit answers
/// before Gmail acts. Anything else retryable (a 5xx, an expired token, a
/// connection that broke) may have been applied.
fn certainly_unsent(failure: &RetryableFailure) -> bool {
    match failure.class {
        Retryable::AccountRateLimited
        | Retryable::AccountConcurrency
        | Retryable::AccountBandwidth
        | Retryable::ProjectRateLimited => true,
        Retryable::Transient => false,
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
    /// The bytes' tracking id, refusing what a draft write must not send:
    /// read-only, too large, or without exactly one valid `X-Mailtide-Id`.
    fn draft_bytes(&self, raw: &[u8]) -> Result<String, DraftError> {
        if self.config.access == crate::runner::Access::ReadOnly {
            return Err(DraftError::ReadOnly);
        }
        let limit = self.config.writes.max_message_bytes;
        if raw.len() > limit {
            return Err(DraftError::TooLarge { limit });
        }
        mailtide_mime::tracking_id(raw)
            .filter(|_| mailtide_mime::tracking_headers(raw) == 1)
            .ok_or_else(|| {
                DraftError::Invalid(format!(
                    "a draft must carry exactly one valid {} header",
                    mailtide_mime::TRACKING_HEADER
                ))
            })
    }

    fn draft_writable(&self) -> Result<(), DraftError> {
        match self.config.access {
            crate::runner::Access::ReadWrite => Ok(()),
            crate::runner::Access::ReadOnly => Err(DraftError::ReadOnly),
        }
    }

    async fn record_write(
        &self,
        account: &AccountId,
        write: &DraftWrite,
    ) -> Result<DraftRecorded, DraftError> {
        self.store()
            .record_draft(account, write, self.clock.now())
            .await
            .map_err(|e| DraftError::Run(RunError::Store(e)))
    }

    /// Record Gmail's answer after the fact. A failure loses nothing the
    /// sync does not bring back (history shows the change, and the draft
    /// listing repairs the index), so the caller still gets the answer.
    async fn record_answer(&self, account: &AccountId, write: &DraftWrite) {
        if let Err(error) = self.record_write(account, write).await {
            tracing::warn!(
                error = error.code(),
                "draft answer not recorded; the sync repairs the index"
            );
        }
    }

    /// When a listing asked for now becomes due.
    fn refresh_at(&self) -> Timestamp {
        self.clock
            .now()
            .plus(self.config.engine.draft_refresh_delay)
    }

    /// `drafts.get` (format `minimal`): the draft and its current message,
    /// `Ok(None)` when Gmail has no such draft.
    async fn get_draft(
        &self,
        account: &AccountId,
        id: &DraftId,
        deadline: Instant,
    ) -> Result<Option<ListedDraft>, DraftError> {
        let request = GmailRequest::GetDraft {
            id: id.clone(),
            format: DraftFormat::Minimal,
        };
        match self
            .call_gmail(account, request, Priority::Foreground, deadline)
            .await?
        {
            EventKind::DraftFetched { draft, .. } => Ok(Some(draft.draft)),
            EventKind::DraftNotFound { .. } => Ok(None),
            _ => Err(unexpected()),
        }
    }

    /// A write's answer with both message ids, reading the draft for any
    /// Gmail left out.
    async fn written(
        &self,
        account: &AccountId,
        answer: &WrittenDraft,
        deadline: Instant,
    ) -> Result<ListedDraft, DraftError> {
        if let Some(complete) = answer.complete() {
            return Ok(complete);
        }
        match self.get_draft(account, &answer.id, deadline).await {
            Ok(Some(draft)) => Ok(draft),
            Ok(None) => Err(DraftError::OutcomeUnknown(
                "the saved draft could not be read back".into(),
            )),
            Err(error) => Err(DraftError::OutcomeUnknown(format!(
                "the saved draft could not be read back: {}",
                error.code()
            ))),
        }
    }

    /// Save a new draft from RFC 5322 bytes carrying exactly one
    /// `X-Mailtide-Id`; `thread_id` puts a reply in its thread. Gmail's
    /// draft id comes back at once and the index has it before history
    /// arrives (`DraftSaved{local}`). A lost answer is
    /// [`DraftError::OutcomeUnknown`], never an invented id: a listing is
    /// scheduled, so a draft Gmail did create enters the index. Retrying
    /// may leave a second draft, never a second send.
    pub async fn create_draft(
        &self,
        account: &AccountId,
        raw: Vec<u8>,
        thread_id: Option<ThreadId>,
        budget: Duration,
    ) -> Result<DraftRef, DraftError> {
        let tracking_id = self.draft_bytes(&raw)?;
        let deadline = Instant::now() + budget;
        let request = GmailRequest::CreateDraft {
            raw: Rfc822(raw),
            thread_id,
        };
        let answer = self
            .call_gmail(account, request, Priority::Foreground, deadline)
            .await;
        let saved = match answer {
            Ok(EventKind::DraftSaved { draft, .. }) => {
                self.written(account, &draft, deadline).await
            }
            // Gmail does not know the thread given.
            Ok(EventKind::DraftNotFound { .. }) | Err(CallError::NotFound) => {
                Err(refused("no such thread"))
            }
            Ok(_) => Err(unexpected()),
            Err(error) => Err(error.into()),
        };
        match saved {
            Ok(draft) => {
                let write = DraftWrite::Saved {
                    draft: draft.clone(),
                    tracking_id,
                };
                self.record_answer(account, &write).await;
                Ok(draft)
            }
            Err(DraftError::OutcomeUnknown(detail)) => {
                let write = DraftWrite::Refresh {
                    at: self.refresh_at(),
                };
                self.record_answer(account, &write).await;
                Err(DraftError::OutcomeUnknown(detail))
            }
            Err(error) => Err(error),
        }
    }

    /// Replace a draft's message (Gmail gives it a new message id; the
    /// draft id stays). The thread id is always sent: the caller's, else the
    /// index's, else one read with `drafts.get` (an update without one moves
    /// a reply draft out of its thread). With `if_message_id` the update is
    /// refused ([`DraftError::Conflict`]) if the draft holds another message:
    /// from the index if it already shows one, else after reading the
    /// draft. The bytes must carry the draft's `X-Mailtide-Id` (any, for a
    /// draft mailtide never saved). A lost answer marks the row stale and
    /// lists the drafts; retrying is safe, an update is a replace.
    pub async fn update_draft(
        &self,
        account: &AccountId,
        draft_id: &DraftId,
        raw: Vec<u8>,
        thread_id: Option<ThreadId>,
        if_message_id: Option<MessageId>,
        budget: Duration,
    ) -> Result<DraftRef, DraftError> {
        let tracking_id = self.draft_bytes(&raw)?;
        let deadline = Instant::now() + budget;
        let row = self
            .draft(account, draft_id)
            .await
            .map_err(DraftError::Run)?;
        if let Some(row) = &row {
            check_unsent(row)?;
            if row.tracking_id.as_ref().is_some_and(|t| *t != tracking_id) {
                return Err(DraftError::Invalid(format!(
                    "the bytes carry another {} than the draft's",
                    mailtide_mime::TRACKING_HEADER
                )));
            }
            // A stale row may be behind Gmail: Gmail is asked below.
            if let Some(expected) = &if_message_id
                && row.state == DraftStatus::Saved
                && *expected != row.message_id
            {
                return Err(DraftError::Conflict {
                    current: row.message_id.clone(),
                });
            }
        }
        let mut thread_id = thread_id.or_else(|| row.as_ref().map(|r| r.thread_id.clone()));
        if if_message_id.is_some() || thread_id.is_none() {
            let Some(current) = self.get_draft(account, draft_id, deadline).await? else {
                return Err(self.gone(account, draft_id).await);
            };
            if let Some(expected) = &if_message_id
                && *expected != current.message
            {
                // The index is behind Gmail: mark the row stale (a listing
                // repoints it), so a retry naming Gmail's message is
                // checked against Gmail, not refused from the index.
                if row
                    .as_ref()
                    .is_some_and(|r| r.message_id != current.message)
                {
                    let write = DraftWrite::Stale {
                        draft: draft_id.clone(),
                        refresh_at: self.refresh_at(),
                    };
                    self.record_answer(account, &write).await;
                }
                return Err(DraftError::Conflict {
                    current: current.message,
                });
            }
            thread_id = thread_id.or(Some(current.thread_id));
        }
        let Some(thread_id) = thread_id else {
            return Err(DraftError::Run(RunError::Invariant(
                "an update without a thread id",
            )));
        };
        let request = GmailRequest::UpdateDraft {
            id: draft_id.clone(),
            raw: Rfc822(raw),
            thread_id,
        };
        let saved = match self
            .call_gmail(account, request, Priority::Foreground, deadline)
            .await
        {
            Ok(EventKind::DraftSaved { draft, .. }) => {
                self.written(account, &draft, deadline).await
            }
            Ok(EventKind::DraftNotFound { .. }) | Err(CallError::NotFound) => {
                return Err(self.gone(account, draft_id).await);
            }
            Ok(_) => Err(unexpected()),
            Err(error) => Err(error.into()),
        };
        match saved {
            Ok(draft) => {
                let write = DraftWrite::Saved {
                    draft: draft.clone(),
                    tracking_id,
                };
                self.record_answer(account, &write).await;
                Ok(draft)
            }
            Err(DraftError::OutcomeUnknown(detail)) => {
                let write = DraftWrite::Stale {
                    draft: draft_id.clone(),
                    refresh_at: self.refresh_at(),
                };
                self.record_answer(account, &write).await;
                Err(DraftError::OutcomeUnknown(detail))
            }
            Err(error) => Err(error),
        }
    }

    /// Gmail has no such draft: it leaves the index (`DraftRemoved{gone}`).
    async fn gone(&self, account: &AccountId, draft_id: &DraftId) -> DraftError {
        let write = DraftWrite::Removed {
            draft: draft_id.clone(),
            reason: RemovedReason::Gone,
        };
        self.record_answer(account, &write).await;
        DraftError::NotFound
    }

    /// Delete a draft, permanently (Gmail does not move it to trash). A
    /// draft Gmail no longer has is [`Deleted::AlreadyGone`], so a retry
    /// after a lost answer is safe. A draft being sent is not deleted.
    pub async fn delete_draft(
        &self,
        account: &AccountId,
        draft_id: &DraftId,
        budget: Duration,
    ) -> Result<Deleted, DraftError> {
        self.draft_writable()?;
        let deadline = Instant::now() + budget;
        if let Some(row) = self
            .draft(account, draft_id)
            .await
            .map_err(DraftError::Run)?
        {
            check_unsent(&row)?;
        }
        let request = GmailRequest::DeleteDraft {
            id: draft_id.clone(),
        };
        let deleted = match self
            .call_gmail(account, request, Priority::Foreground, deadline)
            .await
        {
            Ok(EventKind::DraftDeleted { .. }) => Deleted::Deleted,
            Ok(EventKind::DraftNotFound { .. }) | Err(CallError::NotFound) => Deleted::AlreadyGone,
            Ok(_) => return Err(unexpected()),
            Err(CallError::OutcomeUnknown(detail)) => {
                let write = DraftWrite::Stale {
                    draft: draft_id.clone(),
                    refresh_at: self.refresh_at(),
                };
                self.record_answer(account, &write).await;
                return Err(DraftError::OutcomeUnknown(detail));
            }
            Err(error) => return Err(error.into()),
        };
        let write = DraftWrite::Removed {
            draft: draft_id.clone(),
            reason: RemovedReason::Deleted,
        };
        self.record_answer(account, &write).await;
        Ok(deleted)
    }

    /// Send a draft of the index as it was last saved (`drafts.send` names
    /// only the draft id), at most once:
    ///
    /// 1. the row is marked `sending` first, a compare-and-set: a second
    ///    call, from this process or another, gets
    ///    [`DraftError::SendInProgress`] and calls nothing;
    /// 2. once the first request has certainly ended (its timeout), a retry
    ///    reads the draft before sending: gone is
    ///    [`DraftError::SendUnconfirmed`] (no send), another message is a
    ///    conflict, the same draft is sent;
    /// 3. a lost answer is never resent: the sync settles it (the imported
    ///    `SENT` message carrying the draft's `X-Mailtide-Id` emits
    ///    `DraftSent`; a listing that still shows the draft returns it to
    ///    `saved`).
    ///
    /// With `if_message_id` the draft is read first and the send refused if
    /// it holds another message.
    pub async fn send_draft(
        &self,
        account: &AccountId,
        draft_id: &DraftId,
        if_message_id: Option<MessageId>,
        budget: Duration,
    ) -> Result<SentDraft, DraftError> {
        self.draft_writable()?;
        let deadline = Instant::now() + budget;
        let begin = DraftWrite::BeginSend {
            draft: draft_id.clone(),
            if_message: if_message_id.clone(),
            retry_at: self.clock.now().plus(self.config.writes.request_timeout),
        };
        let (row, retry) = match self.record_write(account, &begin).await? {
            DraftRecorded::Started { draft, retry } => (draft, retry),
            DraftRecorded::InProgress { retry_at } => {
                return Err(DraftError::SendInProgress {
                    retry_after: retry_at,
                });
            }
            DraftRecorded::Conflict { current } => return Err(DraftError::Conflict { current }),
            DraftRecorded::Unconfirmed => return Err(DraftError::SendUnconfirmed),
            DraftRecorded::NotIndexed | DraftRecorded::Done | DraftRecorded::Superseded => {
                return Err(DraftError::NotFound);
            }
        };
        let Some(started) = row.state.attempt().map(|attempt| attempt.started) else {
            return Err(DraftError::Run(RunError::Invariant(
                "a started send without an attempt",
            )));
        };
        let end = |end: SendEnd| DraftWrite::EndSend {
            draft: draft_id.clone(),
            started,
            end,
        };
        if retry || if_message_id.is_some() {
            self.read_before_send(account, &row, started, retry, deadline)
                .await?;
        }
        let request = GmailRequest::SendDraft {
            id: draft_id.clone(),
        };
        match self
            .call_gmail(account, request, Priority::Foreground, deadline)
            .await
        {
            Ok(EventKind::DraftSent {
                message, thread_id, ..
            }) => {
                let sent = SendEnd::Sent {
                    message: message.clone(),
                    thread_id: thread_id.clone(),
                };
                self.record_answer(account, &end(sent)).await;
                Ok(SentDraft {
                    message_id: message,
                    thread_id,
                })
            }
            Ok(EventKind::DraftNotFound { .. }) | Err(CallError::NotFound) if retry => {
                self.record_answer(account, &end(SendEnd::Unconfirmed))
                    .await;
                Err(DraftError::SendUnconfirmed)
            }
            Ok(EventKind::DraftNotFound { .. }) | Err(CallError::NotFound) => {
                self.record_answer(account, &end(SendEnd::Gone)).await;
                Err(DraftError::NotFound)
            }
            Ok(_) => Err(unexpected()),
            Err(CallError::Retryable(failure)) if !certainly_unsent(&failure) => {
                Err(DraftError::OutcomeUnknown(failure.detail))
            }
            Err(error @ (CallError::OutcomeUnknown(_) | CallError::Run(_))) => Err(error.into()),
            Err(
                error @ (CallError::ReadOnly
                | CallError::Deferred(_)
                | CallError::Refused(_)
                | CallError::Retryable(_)),
            ) => {
                self.record_answer(account, &end(SendEnd::Unsent)).await;
                Err(error.into())
            }
        }
    }

    /// Before a retry (or a send with `if_message_id`), read the draft: a
    /// draft gone after a lost answer may have been sent, so it is not sent
    /// again; another message is a conflict. Records how the attempt ended
    /// unless the draft is there as the row says.
    async fn read_before_send(
        &self,
        account: &AccountId,
        row: &mailtide::drafts::DraftRecord,
        started: Timestamp,
        retry: bool,
        deadline: Instant,
    ) -> Result<(), DraftError> {
        let end = |end: SendEnd| DraftWrite::EndSend {
            draft: row.id.clone(),
            started,
            end,
        };
        match self.get_draft(account, &row.id, deadline).await {
            Ok(Some(current)) if current.message == row.message => Ok(()),
            Ok(Some(current)) => {
                let changed = SendEnd::Changed {
                    message: current.message.clone(),
                    thread_id: current.thread_id,
                };
                self.record_answer(account, &end(changed)).await;
                Err(DraftError::Conflict {
                    current: current.message,
                })
            }
            Ok(None) if retry => {
                self.record_answer(account, &end(SendEnd::Unconfirmed))
                    .await;
                Err(DraftError::SendUnconfirmed)
            }
            Ok(None) => {
                self.record_answer(account, &end(SendEnd::Gone)).await;
                Err(DraftError::NotFound)
            }
            // A read changes nothing: whatever happened to it, the
            // draft was not sent by this call.
            Err(error) => {
                self.record_answer(account, &end(SendEnd::Unsent)).await;
                Err(match error {
                    DraftError::OutcomeUnknown(detail) => DraftError::Retryable(RetryableFailure {
                        class: Retryable::Transient,
                        retry_after: None,
                        detail,
                    }),
                    other => other,
                })
            }
        }
    }

    /// The drafts of the index after `after`, in draft id order (no Gmail
    /// call).
    pub async fn drafts(
        &self,
        account: &AccountId,
        after: Option<&DraftId>,
        limit: u32,
    ) -> Result<Vec<DraftView>, RunError> {
        self.store()
            .drafts(account, after, limit)
            .await
            .map_err(RunError::Store)
    }

    /// One draft of the index (no Gmail call).
    pub async fn draft(
        &self,
        account: &AccountId,
        draft_id: &DraftId,
    ) -> Result<Option<DraftView>, RunError> {
        self.store()
            .draft(account, draft_id)
            .await
            .map_err(RunError::Store)
    }

    /// A draft's message as Gmail stores it. Content never changes for a
    /// message id, so once the index's current message is imported its blob
    /// is read and Gmail is not called; otherwise, or with `fresh`, the
    /// draft is read with `drafts.get` (60 units). A sent or deleted draft,
    /// or an id that is not a draft's, is [`DraftError::NotFound`]. Allowed
    /// read-only.
    pub async fn read_draft(
        &self,
        account: &AccountId,
        draft_id: &DraftId,
        fresh: bool,
        budget: Duration,
    ) -> Result<DraftContent, DraftError> {
        if !fresh && let Some(content) = self.stored_draft(account, draft_id).await? {
            return Ok(content);
        }
        let request = GmailRequest::GetDraft {
            id: draft_id.clone(),
            format: DraftFormat::Raw,
        };
        match self
            .call_gmail(
                account,
                request,
                Priority::Foreground,
                Instant::now() + budget,
            )
            .await?
        {
            EventKind::DraftFetched { draft, .. } => match draft.raw {
                Some(raw) => Ok(DraftContent {
                    draft: draft.draft,
                    raw: raw.0,
                }),
                // Never a partial draft: the content is the point.
                None => Err(refused("the draft answer carries no raw content")),
            },
            EventKind::DraftNotFound { .. } => Err(DraftError::NotFound),
            _ => Err(unexpected()),
        }
    }

    /// The index's draft read from the blob of its current message, if
    /// that is imported.
    async fn stored_draft(
        &self,
        account: &AccountId,
        draft_id: &DraftId,
    ) -> Result<Option<DraftContent>, DraftError> {
        let Some(row) = self
            .draft(account, draft_id)
            .await
            .map_err(DraftError::Run)?
        else {
            return Ok(None);
        };
        let view = self
            .store()
            .messages(account, std::slice::from_ref(&row.message_id))
            .await
            .map_err(|e| DraftError::Run(RunError::Store(e)))?;
        let Some(content) = view.into_iter().next().and_then(|v| v.content) else {
            return Ok(None);
        };
        let raw = self
            .blobs()
            .get(&content.blob)
            .await
            .map_err(|e| DraftError::Run(RunError::Blobs(format!("{e:?}"))))?;
        Ok(Some(DraftContent {
            draft: ListedDraft {
                id: row.draft_id,
                message: row.message_id,
                thread_id: row.thread_id,
            },
            raw,
        }))
    }
}

/// A draft being sent is neither updated nor deleted: its send settles it.
fn check_unsent(row: &DraftView) -> Result<(), DraftError> {
    match row.state {
        DraftStatus::Sending { retry_at } => Err(DraftError::SendInProgress {
            retry_after: retry_at,
        }),
        DraftStatus::SendUnconfirmed => Err(DraftError::SendUnconfirmed),
        DraftStatus::Saved | DraftStatus::Stale => Ok(()),
    }
}
