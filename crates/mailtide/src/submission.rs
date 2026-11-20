//! Durable raw submissions and their store transitions.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::ids::{BlobRef, DraftId, MessageId, ThreadId, Timestamp};
use crate::jobs::AttemptId;

/// A host's idempotency key, scoped to an account.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SubmissionKey(pub String);

/// A tracking token has one owner within an account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenOwner {
    /// A submission of raw bytes.
    Raw(SubmissionKey),
    /// A Gmail draft.
    Draft(DraftId),
}

/// The state exposed to callers and feed consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionState {
    /// Waiting for the initial claim and still cancellable.
    Pending,
    /// Claimed. The worker can still prove the request has not left.
    Claimed,
    /// The dispatch marker is durable. The request might have left.
    Sending,
    /// Waiting for a matching import.
    Confirming,
    /// A matching SENT message was imported.
    Sent,
    /// The import deadline elapsed. A late import can still settle it.
    Unknown,
    /// Definitively refused.
    Refused,
    /// Canceled before claim.
    Canceled,
}

impl SubmissionState {
    /// The stable state name used in bindings.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::Sending => "sending",
            Self::Confirming => "confirming",
            Self::Sent => "sent",
            Self::Unknown => "unknown",
            Self::Refused => "refused",
            Self::Canceled => "canceled",
        }
    }
}

/// A submission's public fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Submission {
    /// The idempotency key.
    pub key: SubmissionKey,
    /// The token on the outgoing message.
    pub tracking_id: String,
    /// Current lifecycle state.
    pub state: SubmissionState,
    /// When the submission first becomes eligible.
    pub send_at: Timestamp,
    /// The reporting deadline once dispatch is marked.
    pub confirmation_deadline: Option<Timestamp>,
    /// A provisional acceptance receipt or the confirmed message.
    pub message_id: Option<MessageId>,
    /// Gmail's thread, when it supplied a receipt or an import confirmed it.
    pub thread_id: Option<ThreadId>,
    /// The definitive refusal, if any.
    pub refusal: Option<String>,
    /// Gmail's cooldown, if known.
    pub retry_at: Option<Timestamp>,
}

/// Private staging and dispatch data, stored with the public state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionRow {
    /// Data exposed in the feed.
    pub view: Submission,
    /// Digest of the raw bytes and caller options for safe replay.
    pub fingerprint: String,
    /// Digest of the staged bytes.
    pub content_digest: String,
    /// The thread selected and resolved at admission.
    pub target_thread: Option<ThreadId>,
    /// The staged bytes, while retained.
    pub blob: Option<BlobRef>,
    /// When the next operation on this row becomes due.
    pub due_at: Option<Timestamp>,
    /// The job claim that wrote the dispatch marker.
    pub dispatch: Option<AttemptId>,
    /// When the row first became terminal.
    pub terminal_at: Option<Timestamp>,
}

/// A result from the bounded send call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    /// It was proved that no request went out. Retry at this time.
    NotSent(Timestamp),
    /// Gmail explicitly refused the request.
    Refused {
        /// Gmail's reason, or a local definitive refusal.
        reason: String,
        /// When the gate may reopen for other work.
        retry_at: Option<Timestamp>,
    },
    /// The answer was lost or did not carry a usable receipt.
    Unanswered,
    /// The HTTP response acknowledges acceptance.
    Accepted(MessageId, ThreadId),
}

/// Changes made under the account lock.
#[derive(Debug, Clone)]
pub enum SubmissionWrite {
    /// Admit a new raw submission or return an identical replay.
    Admit(Box<SubmissionRow>),
    /// Cancel a still-pending submission.
    Cancel(SubmissionKey),
    /// Take the earliest due row under a claimed job.
    Take(AttemptId),
    /// Persist a dispatch marker before a write can go out.
    Begin(AttemptId, SubmissionKey),
    /// Record the disposition under the current claim.
    Complete(AttemptId, SubmissionKey, Disposition),
    /// Drop a staged blob reference after idempotent cleanup.
    Prune(AttemptId, SubmissionKey),
    /// Release the account job and schedule the next due row.
    Release(AttemptId),
    /// Apply elapsed confirmation deadlines even when sync is blocked.
    Expire,
}

impl SubmissionWrite {
    /// The job attempt this operation must still hold.
    #[must_use]
    pub fn attempt(&self) -> Option<AttemptId> {
        match self {
            Self::Take(attempt)
            | Self::Begin(attempt, _)
            | Self::Complete(attempt, _, _)
            | Self::Prune(attempt, _)
            | Self::Release(attempt) => Some(*attempt),
            Self::Admit(_) | Self::Cancel(_) | Self::Expire => None,
        }
    }
}

/// The result of an account-locked operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionWritten {
    /// A row was read or changed.
    Row(Box<SubmissionRow>),
    /// There is no row or no due work.
    Missing,
    /// The key names a different request.
    KeyConflict,
    /// Another send owns its token.
    DuplicateTrackingId,
    /// The first claim closed the cancel window.
    CannotUnsend(SubmissionState),
    /// A superseded claim or incarnation tried to change a row.
    Stale,
    /// The mutation completed without a row to return.
    Done,
}

impl SubmissionRow {
    /// Whether normal import is allowed to confirm this submission.
    #[must_use]
    pub fn can_confirm(&self) -> bool {
        matches!(
            self.view.state,
            SubmissionState::Sending | SubmissionState::Confirming | SubmissionState::Unknown
        )
    }

    /// Record a final state and set the retention time once.
    pub fn terminal(&mut self, state: SubmissionState, now: Timestamp) {
        self.view.state = state;
        self.dispatch = None;
        let since = *self.terminal_at.get_or_insert(now);
        self.due_at = self
            .blob
            .as_ref()
            .map(|_| since.plus(Duration::from_hours(168)));
    }

    /// Take due work. A recovered dispatch marker can only reconcile.
    pub fn take(&mut self, now: Timestamp) {
        match self.view.state {
            SubmissionState::Pending => self.view.state = SubmissionState::Claimed,
            SubmissionState::Sending => {
                self.view.state = SubmissionState::Confirming;
                self.dispatch = None;
                self.due_at = self.view.confirmation_deadline;
                self.expire(now);
            }
            SubmissionState::Confirming => self.expire(now),
            _ => {}
        }
    }

    /// Set a marker and deadline before making the request.
    pub fn begin(&mut self, attempt: AttemptId, now: Timestamp) -> bool {
        if self.view.state != SubmissionState::Claimed {
            return false;
        }
        self.view.state = SubmissionState::Sending;
        self.view.confirmation_deadline = Some(now.plus(Duration::from_mins(10)));
        self.dispatch = Some(attempt);
        // A successor must pick up the marker promptly after a crash.
        self.due_at = Some(now);
        true
    }

    /// Apply an answer from the current attempt. A local preflight refusal
    /// or retry can also complete claimed work before its marker is set.
    pub fn complete(
        &mut self,
        attempt: AttemptId,
        disposition: &Disposition,
        now: Timestamp,
    ) -> bool {
        let preflight = self.view.state == SubmissionState::Claimed
            && matches!(
                disposition,
                Disposition::Refused { .. } | Disposition::NotSent(_)
            );
        if self.dispatch != Some(attempt) && !preflight {
            return false;
        }
        self.dispatch = None;
        match disposition {
            Disposition::NotSent(at) => {
                self.view.state = SubmissionState::Claimed;
                self.view.confirmation_deadline = None;
                self.due_at = Some(*at);
            }
            Disposition::Refused { reason, retry_at } => {
                self.view.refusal = Some(reason.clone());
                self.view.retry_at = *retry_at;
                self.terminal(SubmissionState::Refused, now);
            }
            Disposition::Accepted(_, _) | Disposition::Unanswered => {
                if let Disposition::Accepted(message, thread) = disposition {
                    self.view.message_id = Some(message.clone());
                    self.view.thread_id = Some(thread.clone());
                }
                self.view.state = SubmissionState::Confirming;
                self.due_at = self.view.confirmation_deadline;
            }
        }
        true
    }

    /// Expire only work that might have sent and missed its deadline.
    pub fn expire(&mut self, now: Timestamp) {
        if matches!(
            self.view.state,
            SubmissionState::Sending | SubmissionState::Confirming
        ) && self
            .view
            .confirmation_deadline
            .is_some_and(|deadline| deadline <= now)
        {
            self.terminal(SubmissionState::Unknown, now);
        }
    }
}

use crate::commit::MessageOp;
use crate::jobs::{JobKind, JobState, NewJob};
use crate::mirror::{AccountMirror, enqueue};
use crate::quota::Priority;
use crate::revision::Change;
use crate::revision::StateSeq;

impl AccountMirror {
    /// Apply a submission transition under the account lock.
    pub fn write_submission(
        &mut self,
        generation: u64,
        write: &SubmissionWrite,
        now: Timestamp,
    ) -> SubmissionWritten {
        if self.record.generation != generation
            || write.attempt().is_some_and(|attempt| {
                self.jobs
                    .get(&JobKind::SendSubmissions)
                    .map(|job| &job.state)
                    != Some(&JobState::Running(attempt))
            })
        {
            return SubmissionWritten::Stale;
        }
        match write {
            SubmissionWrite::Admit(row) => self.admit_submission(row),
            SubmissionWrite::Cancel(key) => {
                let Some(row) = self.submissions.get(key) else {
                    return SubmissionWritten::Missing;
                };
                match row.view.state {
                    SubmissionState::Pending => self.change_submission(key, |row| {
                        row.terminal(SubmissionState::Canceled, now);
                        true
                    }),
                    SubmissionState::Canceled => SubmissionWritten::Row(Box::new(row.clone())),
                    state => SubmissionWritten::CannotUnsend(state),
                }
            }
            SubmissionWrite::Take(_) => {
                let Some((at, key)) = self.next_submission() else {
                    return SubmissionWritten::Missing;
                };
                if at > now {
                    return SubmissionWritten::Missing;
                }
                let key = key.clone();
                self.change_submission(&key, |row| {
                    row.take(now);
                    true
                })
            }
            SubmissionWrite::Begin(attempt, key) => {
                self.change_submission(key, |row| row.begin(*attempt, now))
            }
            SubmissionWrite::Complete(attempt, key, disposition) => {
                self.change_submission(key, |row| row.complete(*attempt, disposition, now))
            }
            SubmissionWrite::Prune(_, key) => self.change_submission(key, |row| {
                if row.terminal_at.is_none() || row.due_at.is_none_or(|at| at > now) {
                    return false;
                }
                row.blob = None;
                row.due_at = None;
                true
            }),
            SubmissionWrite::Release(_) => {
                self.jobs.remove(&JobKind::SendSubmissions);
                if let Some((at, _)) = self.next_submission() {
                    self.enqueue_submission(at);
                }
                SubmissionWritten::Done
            }
            SubmissionWrite::Expire => {
                let mut changes = Vec::new();
                for row in self.submissions.values_mut() {
                    let before = row.view.state;
                    row.expire(now);
                    if before != row.view.state {
                        changes.push(row.view.clone());
                    }
                }
                if !changes.is_empty() {
                    self.seq.0 += 1;
                    self.log.extend(
                        changes
                            .into_iter()
                            .map(|submission| (self.seq, Change::SubmissionChanged { submission })),
                    );
                }
                SubmissionWritten::Done
            }
        }
    }

    fn next_submission(&self) -> Option<(Timestamp, &SubmissionKey)> {
        self.submissions
            .iter()
            .filter_map(|(key, row)| row.due_at.map(|at| (at, key)))
            .min()
    }

    fn enqueue_submission(&mut self, at: Timestamp) {
        let kind = JobKind::SendSubmissions;
        if let Some(crate::jobs::Job {
            state: JobState::Ready { due_at },
            ..
        }) = self.jobs.get_mut(&kind)
        {
            *due_at = (*due_at).min(at);
            return;
        }
        let job = enqueue(
            self.jobs.get(&kind),
            &NewJob {
                kind: kind.clone(),
                priority: Priority::Foreground,
                due_at: at,
            },
        );
        self.jobs.insert(kind, job);
    }

    fn admit_submission(&mut self, row: &SubmissionRow) -> SubmissionWritten {
        let key = &row.view.key;
        if let Some(existing) = self.submissions.get(key) {
            return if existing.fingerprint == row.fingerprint {
                SubmissionWritten::Row(Box::new(existing.clone()))
            } else {
                SubmissionWritten::KeyConflict
            };
        }
        if self.send_tokens.contains_key(&row.view.tracking_id) {
            return SubmissionWritten::DuplicateTrackingId;
        }
        self.send_tokens
            .insert(row.view.tracking_id.clone(), TokenOwner::Raw(key.clone()));
        self.submissions.insert(key.clone(), row.clone());
        self.enqueue_submission(row.view.send_at);
        self.seq.0 += 1;
        self.log.push((
            self.seq,
            Change::SubmissionChanged {
                submission: row.view.clone(),
            },
        ));
        SubmissionWritten::Row(Box::new(row.clone()))
    }

    fn change_submission(
        &mut self,
        key: &SubmissionKey,
        change: impl FnOnce(&mut SubmissionRow) -> bool,
    ) -> SubmissionWritten {
        let Some(row) = self.submissions.get_mut(key) else {
            return SubmissionWritten::Missing;
        };
        let before = row.view.clone();
        if !change(row) {
            return SubmissionWritten::Stale;
        }
        if before != row.view {
            self.seq.0 += 1;
            self.log.push((
                self.seq,
                Change::SubmissionChanged {
                    submission: row.view.clone(),
                },
            ));
        }
        SubmissionWritten::Row(Box::new(row.clone()))
    }

    /// Consume first-import evidence for a raw token. Return whether a raw
    /// submission owns it, so draft confirmation cannot settle that token.
    pub(crate) fn confirm_submission(
        &mut self,
        op: &MessageOp,
        now: Timestamp,
        seq: StateSeq,
    ) -> bool {
        let MessageOp::MarkImported {
            observation,
            facts: Some(facts),
            ..
        } = op
        else {
            return false;
        };
        let Some(token) = &facts.tracking_id else {
            return false;
        };
        let Some(row) = self
            .submissions
            .values_mut()
            .find(|row| &row.view.tracking_id == token)
        else {
            return false;
        };
        if row.can_confirm()
            && self.messages.get(&observation.id).is_some_and(|message| {
                message
                    .labels
                    .iter()
                    .any(|label| label.0 == crate::labels::system::SENT)
            })
        {
            row.view.message_id = Some(observation.id.clone());
            row.view.thread_id = Some(observation.thread_id.clone());
            row.terminal(SubmissionState::Sent, now);
            self.log.push((
                seq,
                Change::SubmissionChanged {
                    submission: row.view.clone(),
                },
            ));
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::AccountId;

    const NOW: Timestamp = Timestamp(100);
    const GENERATION: u64 = 42;
    const ATTEMPT: AttemptId = AttemptId(1);

    fn candidate(key: &str) -> SubmissionRow {
        SubmissionRow {
            view: Submission {
                key: SubmissionKey(key.into()),
                tracking_id: "tracking-token-0001".into(),
                state: SubmissionState::Pending,
                send_at: NOW,
                confirmation_deadline: None,
                message_id: None,
                thread_id: None,
                refusal: None,
                retry_at: None,
            },
            fingerprint: "sample".into(),
            content_digest: "content".into(),
            target_thread: Some(ThreadId("thread".into())),
            blob: Some(BlobRef("staged.eml".into())),
            due_at: Some(NOW),
            dispatch: None,
            terminal_at: None,
        }
    }

    fn admitted() -> (AccountMirror, SubmissionKey) {
        let mut mirror = AccountMirror::new(AccountId("a@example.org".into()), GENERATION);
        let row = candidate("key");
        let key = row.view.key.clone();
        assert!(matches!(
            write(&mut mirror, &SubmissionWrite::Admit(Box::new(row))),
            SubmissionWritten::Row(_)
        ));
        (mirror, key)
    }

    fn write(mirror: &mut AccountMirror, op: &SubmissionWrite) -> SubmissionWritten {
        mirror.write_submission(GENERATION, op, NOW)
    }

    fn claim(mirror: &mut AccountMirror, attempt: AttemptId) {
        let jobs = mirror
            .claim_jobs(mirror.record.epoch, 1, NOW, attempt)
            .unwrap();
        assert_eq!(jobs[0].kind(), JobKind::SendSubmissions);
    }

    fn begun() -> (AccountMirror, SubmissionKey) {
        let (mut mirror, key) = admitted();
        claim(&mut mirror, ATTEMPT);
        write(&mut mirror, &SubmissionWrite::Take(ATTEMPT));
        assert!(matches!(
            write(&mut mirror, &SubmissionWrite::Begin(ATTEMPT, key.clone())),
            SubmissionWritten::Row(_)
        ));
        (mirror, key)
    }

    #[test]
    fn admission_is_idempotent_per_key() {
        let (mut mirror, key) = admitted();
        let mut replay = candidate("key");
        replay.blob = Some(BlobRef("loser.eml".into()));
        assert_eq!(
            write(&mut mirror, &SubmissionWrite::Admit(Box::new(replay))),
            SubmissionWritten::Row(Box::new(mirror.submissions[&key].clone()))
        );
        assert_eq!(mirror.log.len(), 1);
    }

    #[test]
    fn a_key_reused_with_other_content_or_options_is_a_conflict() {
        let (mut mirror, _) = admitted();
        let mut replay = candidate("key");
        replay.fingerprint = "different".into();
        assert_eq!(
            write(&mut mirror, &SubmissionWrite::Admit(Box::new(replay))),
            SubmissionWritten::KeyConflict
        );
    }

    #[test]
    fn a_token_can_only_belong_to_one_send_in_an_account() {
        let (mut mirror, _) = admitted();
        let row = candidate("different key");
        assert_eq!(
            write(&mut mirror, &SubmissionWrite::Admit(Box::new(row))),
            SubmissionWritten::DuplicateTrackingId
        );
    }

    #[test]
    fn a_claim_closes_cancellation_for_a_due_submission() {
        let (mut mirror, key) = admitted();
        claim(&mut mirror, ATTEMPT);
        write(&mut mirror, &SubmissionWrite::Take(ATTEMPT));
        assert_eq!(
            write(&mut mirror, &SubmissionWrite::Cancel(key)),
            SubmissionWritten::CannotUnsend(SubmissionState::Claimed)
        );
    }

    #[test]
    fn cancel_wins_only_while_pending() {
        let (mut mirror, key) = admitted();
        let canceled = write(&mut mirror, &SubmissionWrite::Cancel(key.clone()));
        assert_eq!(
            canceled,
            write(&mut mirror, &SubmissionWrite::Cancel(key.clone()))
        );
        claim(&mut mirror, ATTEMPT);
        assert_eq!(
            write(&mut mirror, &SubmissionWrite::Begin(ATTEMPT, key)),
            SubmissionWritten::Stale
        );
    }

    #[test]
    fn a_dispatch_requires_the_current_claim_and_committed_marker() {
        let (mut mirror, key) = begun();
        assert_eq!(mirror.submissions[&key].dispatch, Some(ATTEMPT));
        assert_eq!(
            write(
                &mut mirror,
                &SubmissionWrite::Complete(AttemptId(2), key, Disposition::Unanswered)
            ),
            SubmissionWritten::Stale
        );
    }

    #[test]
    fn a_claim_recovering_an_unresolved_dispatch_can_only_confirm() {
        let (mut mirror, key) = begun();
        mirror.claim(crate::ids::ENGINE_VERSION).unwrap();
        let successor = AttemptId(2);
        claim(&mut mirror, successor);
        write(&mut mirror, &SubmissionWrite::Take(successor));
        assert_eq!(
            mirror.submissions[&key].view.state,
            SubmissionState::Confirming
        );
        assert_eq!(
            write(&mut mirror, &SubmissionWrite::Begin(successor, key)),
            SubmissionWritten::Stale
        );
    }

    #[test]
    fn only_proof_of_no_wire_request_reschedules_the_claimed_row() {
        let (mut mirror, key) = begun();
        write(
            &mut mirror,
            &SubmissionWrite::Complete(ATTEMPT, key.clone(), Disposition::NotSent(Timestamp(200))),
        );
        assert_eq!(mirror.submissions[&key].due_at, Some(Timestamp(200)));
        assert_eq!(
            write(&mut mirror, &SubmissionWrite::Cancel(key)),
            SubmissionWritten::CannotUnsend(SubmissionState::Claimed)
        );
    }

    #[test]
    fn an_accepted_send_waits_for_import_confirmation() {
        let (mut mirror, key) = begun();
        write(
            &mut mirror,
            &SubmissionWrite::Complete(
                ATTEMPT,
                key.clone(),
                Disposition::Accepted(MessageId("gmail".into()), ThreadId("actual".into())),
            ),
        );
        let view = &mirror.submissions[&key].view;
        assert_eq!(view.state, SubmissionState::Confirming);
        assert_eq!(view.message_id, Some(MessageId("gmail".into())));
    }

    #[test]
    fn the_confirmation_deadline_leaves_an_unknown_receipt() {
        let (mut mirror, key) = begun();
        let at = mirror.submissions[&key].view.confirmation_deadline.unwrap();
        mirror.write_submission(GENERATION, &SubmissionWrite::Expire, at);
        assert_eq!(
            mirror.submissions[&key].view.state,
            SubmissionState::Unknown
        );
        assert_eq!(
            write(&mut mirror, &SubmissionWrite::Begin(ATTEMPT, key)),
            SubmissionWritten::Stale
        );
    }

    #[test]
    fn a_refusal_is_final_and_keeps_gmails_reason() {
        let (mut mirror, key) = begun();
        write(
            &mut mirror,
            &SubmissionWrite::Complete(
                ATTEMPT,
                key.clone(),
                Disposition::Refused {
                    reason: "429 rate limited".into(),
                    retry_at: Some(Timestamp(900)),
                },
            ),
        );
        let view = &mirror.submissions[&key].view;
        assert_eq!(view.state, SubmissionState::Refused);
        assert_eq!(view.refusal.as_deref(), Some("429 rate limited"));
        assert_eq!(view.retry_at, Some(Timestamp(900)));
    }

    #[test]
    fn blob_retention_preserves_the_terminal_receipt_and_token() {
        let (mut mirror, key) = admitted();
        write(&mut mirror, &SubmissionWrite::Cancel(key.clone()));
        claim(&mut mirror, ATTEMPT);
        let at = mirror.submissions[&key].due_at.unwrap();
        mirror.write_submission(
            GENERATION,
            &SubmissionWrite::Prune(ATTEMPT, key.clone()),
            at,
        );
        assert!(mirror.submissions[&key].blob.is_none());
        assert_eq!(
            mirror.submissions[&key].view.state,
            SubmissionState::Canceled
        );
        assert!(mirror.send_tokens.contains_key("tracking-token-0001"));
    }

    fn import(mirror: &mut AccountMirror, label: &str, token: &str) {
        use crate::commit::{Commit, JobPlan};
        use crate::gmail::{HeaderFacts, MessageFacts, MessageObservation};
        let id = MessageId(label.into());
        let op = MessageOp::MarkImported {
            observation: MessageObservation {
                id,
                thread_id: ThreadId("observed".into()),
                labels: crate::labels::set([label]),
                checkpoint: crate::ids::HistoryId(1),
            },
            blob: BlobRef("imported.eml".into()),
            size: 3,
            internal_date: NOW,
            headers: Box::new(HeaderFacts::default()),
            facts: Some(Box::new(MessageFacts {
                tracking_id: Some(token.into()),
                ..MessageFacts::empty()
            })),
        };
        let commit = Commit {
            id: crate::ids::CommitId {
                epoch: mirror.record.epoch,
                seq: 1,
            },
            account: mirror.account.clone(),
            epoch: mirror.record.epoch,
            account_generation: GENERATION,
            engine_version: mirror.record.engine_version,
            cursor: None,
            claim: None,
            sync: None,
            messages: vec![op],
            labels: Vec::new(),
            account_ops: Vec::new(),
            job_plan: JobPlan {
                priority: Priority::Live,
                import_due_at: NOW,
                draft_import_due_at: NOW,
                verify_due_at: NOW,
                cleanup_due_at: NOW,
                deliver_due_at: NOW,
                confirm_due_at: NOW,
                draft_refresh_due_at: NOW,
            },
            jobs: Vec::new(),
            intents: Vec::new(),
            drafts: Vec::new(),
        };
        assert!(matches!(
            mirror.apply(&commit),
            crate::commit::CommitOutcome::Applied { .. }
        ));
    }

    #[test]
    fn a_sent_import_with_the_tracking_id_confirms_in_the_same_commit() {
        let (mut mirror, key) = begun();
        import(&mut mirror, "SENT", "tracking-token-0001");
        assert_eq!(mirror.submissions[&key].view.state, SubmissionState::Sent);
        let seq = mirror.seq;
        assert!(
            mirror
                .log
                .iter()
                .any(|(s, change)| *s == seq && matches!(change, Change::MessageImported { .. }))
        );
        assert!(
            mirror
                .log
                .iter()
                .any(|(s, change)| *s == seq && matches!(change, Change::SubmissionChanged { .. }))
        );
    }

    #[test]
    fn only_a_sent_import_for_the_token_owner_can_confirm() {
        let (mut mirror, key) = begun();
        import(&mut mirror, "INBOX", "tracking-token-0001");
        import(&mut mirror, "DRAFT", "tracking-token-0001");
        import(&mut mirror, "SENT", "other-token");
        assert_eq!(
            mirror.submissions[&key].view.state,
            SubmissionState::Sending
        );
    }

    #[test]
    fn a_late_result_cannot_undo_import_confirmation() {
        let (mut mirror, key) = begun();
        import(&mut mirror, "SENT", "tracking-token-0001");
        assert_eq!(
            write(
                &mut mirror,
                &SubmissionWrite::Complete(ATTEMPT, key.clone(), Disposition::Unanswered)
            ),
            SubmissionWritten::Stale
        );
        assert_eq!(mirror.submissions[&key].view.state, SubmissionState::Sent);
    }

    #[test]
    fn a_late_sent_import_turns_unknown_into_sent() {
        let (mut mirror, key) = begun();
        let at = mirror.submissions[&key].view.confirmation_deadline.unwrap();
        mirror.write_submission(GENERATION, &SubmissionWrite::Expire, at);
        import(&mut mirror, "SENT", "tracking-token-0001");
        assert_eq!(mirror.submissions[&key].view.state, SubmissionState::Sent);
    }

    #[test]
    fn deleting_the_original_does_not_cancel_a_reply() {
        let (mut mirror, key) = admitted();
        import(&mut mirror, "INBOX", "unrelated-token");
        let result = mirror.record_deleted(
            &[(
                MessageId("INBOX".into()),
                crate::commit::DeletionEvidence::History {
                    at: crate::ids::HistoryId(2),
                },
            )],
            NOW,
        );
        assert!(result > 0);
        assert_eq!(
            mirror.submissions[&key].target_thread,
            Some(ThreadId("thread".into()))
        );
    }
}
