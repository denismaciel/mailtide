//! Admission and bounded delivery of raw submissions.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use mailtide::command::GmailRequest;
use mailtide::event::EventKind;
use mailtide::ids::{AccountId, LabelId, MessageId, ThreadId, Timestamp};
use mailtide::jobs::AttemptId;
use mailtide::quota::Priority;
use mailtide::submission::{
    Disposition, Submission, SubmissionKey, SubmissionRow, SubmissionState, SubmissionWrite,
    SubmissionWritten,
};
use mailtide_store::{Store, StoreError};
use sha2::{Digest, Sha256};

use crate::auth::TokenSource;
use crate::blobs::{BlobError, BlobStore, outbox_key};
use crate::quota_gate::QuotaGate;
use crate::runner::{Access, CallError, RunError, Runner, SyncReport};
use crate::transport::GmailTransport;

const LOCAL_RETRY: Duration = Duration::from_secs(5);

/// Settings for a raw submission.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SendOptions {
    /// A specific Gmail thread to join.
    pub thread_id: Option<ThreadId>,
    /// Resolve a live, non-draft original to its thread before admission.
    pub thread_of: Option<MessageId>,
    /// The first eligible time. Omit to allow immediate delivery.
    pub send_at: Option<Timestamp>,
}

/// Why a raw submission operation could not complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendError {
    /// Write access is disabled.
    ReadOnly,
    /// Missing, invalid or duplicate X-Mailtide-Id.
    InvalidTrackingId,
    /// The bytes name no readable recipient.
    NoRecipients,
    /// The bytes exceed this limit.
    TooLarge {
        /// Maximum size in RFC 5322 bytes.
        limit: usize,
    },
    /// The original is absent or a draft.
    UnknownOriginal,
    /// Supply just one thread selector and a nonempty key.
    InvalidOptions,
    /// The row already names a different request.
    KeyConflict,
    /// Another raw submission or draft owns the token.
    DuplicateTrackingId,
    /// There is no such submission.
    UnknownSubmission,
    /// The first claim closed the cancel window.
    CannotUnsend(SubmissionState),
    /// The original has no readable stored content.
    OriginalUnavailable,
    /// A store, blob or runner failure.
    Run(RunError),
}

fn digest(raw: &[u8]) -> String {
    let mut hex = String::with_capacity(64);
    for byte in Sha256::digest(raw) {
        write!(hex, "{byte:02x}").expect("writing to a string");
    }
    hex
}

fn key_and_token(
    raw: &[u8],
    key: Option<SubmissionKey>,
) -> Result<(SubmissionKey, String), SendError> {
    let token = mailtide_mime::tracking_id(raw)
        .filter(|_| mailtide_mime::tracking_headers(raw) == 1)
        .ok_or(SendError::InvalidTrackingId)?;
    let key = key.unwrap_or_else(|| SubmissionKey(token.clone()));
    if key.0.is_empty() {
        return Err(SendError::InvalidOptions);
    }
    Ok((key, token))
}

fn fingerprint(content: &str, options: &SendOptions) -> String {
    // The tuple's shape distinguishes the two thread selectors and absent
    // values. Its byte digest is calculated separately for staging checks.
    let data = serde_json::to_vec(&(
        content,
        &options.thread_id,
        &options.thread_of,
        options.send_at,
    ))
    .expect("submission options serialize");
    digest(&data)
}

impl<S, B, T, Q, A> Runner<S, B, T, Q, A>
where
    S: Store,
    B: BlobStore,
    T: GmailTransport,
    Q: QuotaGate,
    A: TokenSource,
{
    /// Stage and admit raw bytes without calling Gmail.
    pub async fn submit(
        &self,
        account: &AccountId,
        key: Option<SubmissionKey>,
        raw: Vec<u8>,
        options: SendOptions,
    ) -> Result<Submission, SendError> {
        if self.config.access == Access::ReadOnly {
            return Err(SendError::ReadOnly);
        }
        let generation = self.submission_generation(account).await?;
        let (key, tracking_id) = key_and_token(&raw, key)?;
        let content_digest = digest(&raw);
        let fingerprint = fingerprint(&content_digest, &options);
        if let Some(row) = self
            .store()
            .submission(account, &key)
            .await
            .map_err(RunError::Store)
            .map_err(SendError::Run)?
        {
            return if row.fingerprint == fingerprint {
                Ok(row.view)
            } else {
                Err(SendError::KeyConflict)
            };
        }
        let limit = self.config.writes.max_message_bytes;
        if raw.len() > limit {
            return Err(SendError::TooLarge { limit });
        }
        if !mailtide_mime::has_send_recipient(&raw) {
            return Err(SendError::NoRecipients);
        }
        let target_thread = self.resolve_send_thread(account, &options).await?;
        let mut entropy = [0; 16];
        getrandom::fill(&mut entropy)
            .map_err(|error| SendError::Run(RunError::Blobs(error.to_string())))?;
        let blob = outbox_key(account, &mailtide_mime::new_tracking_id(entropy));
        self.blobs()
            .put(&blob, raw)
            .await
            .map_err(|error| SendError::Run(RunError::Blobs(format!("{error:?}"))))?;
        let now = self.clock.now();
        let send_at = options.send_at.unwrap_or(now);
        let row = SubmissionRow {
            view: Submission {
                key,
                tracking_id,
                state: SubmissionState::Pending,
                send_at,
                confirmation_deadline: None,
                message_id: None,
                thread_id: None,
                refusal: None,
                retry_at: None,
            },
            content_digest,
            fingerprint,
            target_thread,
            blob: Some(blob.clone()),
            due_at: Some(send_at),
            dispatch: None,
            terminal_at: None,
        };
        let result = self
            .store()
            .write_submission(
                account,
                generation,
                &SubmissionWrite::Admit(Box::new(row)),
                now,
            )
            .await;
        // An ambiguous commit may own this candidate. A definite loser can
        // remove only its unique candidate, even when racing another writer.
        let keep = match &result {
            Ok(SubmissionWritten::Row(row)) => row.blob.as_ref() == Some(&blob),
            Err(StoreError::OutcomeUnknown(_)) => true,
            _ => false,
        };
        if !keep && self.blobs().delete(&blob).await.is_err() {
            tracing::warn!("unused outbox candidate could not be removed");
        }
        write_result(result)
    }

    /// Cancel before the first worker claims a submission.
    pub async fn cancel_submission(
        &self,
        account: &AccountId,
        key: &SubmissionKey,
    ) -> Result<Submission, SendError> {
        let generation = self.submission_generation(account).await?;
        write_result(
            self.store()
                .write_submission(
                    account,
                    generation,
                    &SubmissionWrite::Cancel(key.clone()),
                    self.clock.now(),
                )
                .await,
        )
    }

    /// Read one raw submission.
    pub async fn submission(
        &self,
        account: &AccountId,
        key: &SubmissionKey,
    ) -> Result<Option<Submission>, SendError> {
        Ok(self
            .store()
            .submission(account, key)
            .await
            .map_err(RunError::Store)
            .map_err(SendError::Run)?
            .map(|row| row.view))
    }

    /// Page raw submissions in key order with an optional state filter.
    pub async fn submissions(
        &self,
        account: &AccountId,
        after: Option<&SubmissionKey>,
        states: &[SubmissionState],
        limit: u32,
    ) -> Result<Vec<Submission>, SendError> {
        Ok(self
            .store()
            .submissions(account, after, states, limit.clamp(1, 500))
            .await
            .map_err(RunError::Store)
            .map_err(SendError::Run)?
            .into_iter()
            .map(|row| row.view)
            .collect())
    }

    /// Read the staged bytes while they are retained.
    pub async fn submission_content(
        &self,
        account: &AccountId,
        key: &SubmissionKey,
    ) -> Result<Option<Vec<u8>>, SendError> {
        let row = self
            .store()
            .submission(account, key)
            .await
            .map_err(RunError::Store)
            .map_err(SendError::Run)?;
        let Some(blob) = row.and_then(|row| row.blob) else {
            return Ok(None);
        };
        match self.blobs().get(&blob).await {
            Ok(raw) => Ok(Some(raw)),
            Err(BlobError::NotFound) => Ok(None),
            Err(error) => Err(SendError::Run(RunError::Blobs(format!("{error:?}")))),
        }
    }

    async fn submission_generation(&self, account: &AccountId) -> Result<u64, SendError> {
        self.status(account)
            .await
            .map_err(SendError::Run)?
            .map(|status| status.generation)
            .ok_or(SendError::Run(RunError::Store(StoreError::NotTracked)))
    }

    async fn resolve_send_thread(
        &self,
        account: &AccountId,
        options: &SendOptions,
    ) -> Result<Option<ThreadId>, SendError> {
        let Some(original) = &options.thread_of else {
            return Ok(options.thread_id.clone());
        };
        if options.thread_id.is_some() {
            return Err(SendError::InvalidOptions);
        }
        let found = self
            .store()
            .messages(account, std::slice::from_ref(original))
            .await
            .map_err(RunError::Store)
            .map_err(SendError::Run)?;
        let view = found.into_iter().next().ok_or(SendError::UnknownOriginal)?;
        if view
            .observed_labels
            .contains(&LabelId(mailtide::labels::system::DRAFT.into()))
        {
            return Err(SendError::UnknownOriginal);
        }
        Ok(Some(view.thread_id))
    }

    /// Do at most one item under the already-held account job.
    pub(crate) async fn run_submission_work(
        &self,
        account: &AccountId,
        generation: u64,
        attempt: AttemptId,
        deadline: Instant,
        report: &mut SyncReport,
    ) -> Result<bool, RunError> {
        let now = self.clock.now();
        let taken = self
            .store()
            .write_submission(account, generation, &SubmissionWrite::Take(attempt), now)
            .await
            .map_err(RunError::Store)?;
        let mut refresh = false;
        let mut cleanup = None;
        if let SubmissionWritten::Row(row) = taken {
            match row.view.state {
                SubmissionState::Claimed => {
                    let (refresh_history, gate_cleanup) = self
                        .deliver_submission(account, generation, attempt, &row, deadline, report)
                        .await?;
                    refresh = refresh_history;
                    cleanup = gate_cleanup;
                }
                SubmissionState::Confirming => refresh = true,
                SubmissionState::Sent
                | SubmissionState::Unknown
                | SubmissionState::Refused
                | SubmissionState::Canceled
                    if row.due_at.is_some_and(|at| at <= now) =>
                {
                    if let Some(blob) = &row.blob {
                        // A cleanup failure ends this run. The next claim
                        // retries the idempotent delete, without a busy loop.
                        tokio::time::timeout_at(deadline.into(), self.blobs().delete(blob))
                            .await
                            .map_err(|_| {
                                RunError::Blobs("outbox cleanup exceeded the budget".into())
                            })?
                            .map_err(|e| RunError::Blobs(format!("{e:?}")))?;
                        self.store()
                            .write_submission(
                                account,
                                generation,
                                &SubmissionWrite::Prune(attempt, row.view.key),
                                now,
                            )
                            .await
                            .map_err(RunError::Store)?;
                    }
                }
                _ => {}
            }
        }
        self.store()
            .write_submission(
                account,
                generation,
                &SubmissionWrite::Release(attempt),
                self.clock.now(),
            )
            .await
            .map_err(RunError::Store)?;
        if let Some(error) = cleanup {
            return Err(error);
        }
        Ok(refresh)
    }

    async fn deliver_submission(
        &self,
        account: &AccountId,
        generation: u64,
        attempt: AttemptId,
        row: &SubmissionRow,
        deadline: Instant,
        report: &mut SyncReport,
    ) -> Result<(bool, Option<RunError>), RunError> {
        let now = self.clock.now();
        let raw = match self.prepare_submission(row, deadline).await {
            Ok(raw) => raw,
            Err(disposition) => {
                self.store()
                    .write_submission(
                        account,
                        generation,
                        &SubmissionWrite::Complete(attempt, row.view.key.clone(), disposition),
                        now,
                    )
                    .await
                    .map_err(RunError::Store)?;
                return Ok((false, None));
            }
        };
        let marker = self
            .store()
            .write_submission(
                account,
                generation,
                &SubmissionWrite::Begin(attempt, row.view.key.clone()),
                self.clock.now(),
            )
            .await
            .map_err(RunError::Store)?;
        let SubmissionWritten::Row(marked) = marker else {
            return Ok((false, None));
        };
        let Some(confirm_by) = marked.view.confirmation_deadline else {
            return Err(RunError::Invariant("submission marker has no deadline"));
        };
        let remaining = confirm_by.since(self.clock.now());
        let issue_by = Instant::now() + remaining;
        let request = GmailRequest::MessagesSend {
            raw,
            thread_id: row.target_thread.clone(),
        };
        let response = self
            .direct_call_before(account, request, Priority::Foreground, issue_by, deadline)
            .await;
        let retry = self.clock.now().plus(LOCAL_RETRY);
        let (disposition, cleanup, refresh) = match response {
            Ok(Some(response)) if response.not_sent => (
                Disposition::NotSent(response.retry_at.unwrap_or(retry)),
                response.cleanup,
                false,
            ),
            Ok(Some(response)) => {
                report.gmail_requests += 1;
                let disposition = match response.outcome {
                    Ok(EventKind::MessageSent {
                        message_id,
                        thread_id,
                        ..
                    }) => Disposition::Accepted(message_id, thread_id),
                    Err(CallError::Refused(refusal)) => Disposition::Refused {
                        reason: refusal.detail,
                        retry_at: response.retry_at,
                    },
                    Err(CallError::Retryable(refusal)) => Disposition::Refused {
                        reason: refusal.detail,
                        retry_at: response.retry_at,
                    },
                    _ => Disposition::Unanswered,
                };
                let refresh = matches!(
                    disposition,
                    Disposition::Accepted(_, _) | Disposition::Unanswered
                );
                (disposition, response.cleanup, refresh)
            }
            Err(CallError::Deferred(deferred)) => {
                (Disposition::NotSent(deferred.until.max(retry)), None, false)
            }
            Ok(None) => (Disposition::NotSent(retry), None, false),
            Err(CallError::ReadOnly) => (local_refusal("read_only"), None, false),
            Err(CallError::OutcomeUnknown(_)) => (Disposition::Unanswered, None, true),
            // Direct-call errors here precede transport. Any response or
            // ambiguous transport outcome is wrapped in the detailed result.
            Err(error) => (
                Disposition::NotSent(retry),
                Some(RunError::Quota(error.code().into())),
                false,
            ),
        };
        self.store()
            .write_submission(
                account,
                generation,
                &SubmissionWrite::Complete(attempt, row.view.key.clone(), disposition),
                self.clock.now(),
            )
            .await
            .map_err(RunError::Store)?;
        Ok((refresh, cleanup))
    }

    async fn prepare_submission(
        &self,
        row: &SubmissionRow,
        deadline: Instant,
    ) -> Result<Vec<u8>, Disposition> {
        if self.config.access == Access::ReadOnly {
            return Err(local_refusal("read_only"));
        }
        let Some(blob) = &row.blob else {
            return Err(local_refusal("staged content missing"));
        };
        match tokio::time::timeout_at(deadline.into(), self.blobs().get(blob)).await {
            Ok(Ok(raw)) if digest(&raw) == row.content_digest => Ok(raw),
            Ok(Ok(_)) => Err(local_refusal("staged content fingerprint changed")),
            Ok(Err(BlobError::NotFound)) => Err(local_refusal("staged content missing")),
            Ok(Err(_)) | Err(_) => Err(Disposition::NotSent(self.clock.now().plus(LOCAL_RETRY))),
        }
    }
}

fn write_result(result: Result<SubmissionWritten, StoreError>) -> Result<Submission, SendError> {
    match result.map_err(RunError::Store).map_err(SendError::Run)? {
        SubmissionWritten::Row(row) => Ok(row.view),
        SubmissionWritten::KeyConflict => Err(SendError::KeyConflict),
        SubmissionWritten::DuplicateTrackingId => Err(SendError::DuplicateTrackingId),
        SubmissionWritten::CannotUnsend(state) => Err(SendError::CannotUnsend(state)),
        SubmissionWritten::Missing => Err(SendError::UnknownSubmission),
        SubmissionWritten::Stale => Err(SendError::Run(RunError::Store(StoreError::NotTracked))),
        SubmissionWritten::Done => Err(SendError::Run(RunError::Invariant(
            "submission mutation returned no row",
        ))),
    }
}

fn local_refusal(reason: &str) -> Disposition {
    Disposition::Refused {
        reason: reason.to_owned(),
        retry_at: None,
    }
}
