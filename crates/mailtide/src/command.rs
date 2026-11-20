//! Requests the engine returns for the host to perform (SPEC §10.2).

use crate::commit::Commit;
use crate::drafts::{DraftFormat, Rfc822};
use crate::gmail::{LabelPatch, NewLabel};
use crate::ids::{
    AccountId, CommandId, DraftId, HistoryId, LabelId, MessageId, PageToken, ThreadId, Timestamp,
};
use crate::outbox::ModifyGroup;
use crate::quota::{Priority, QuotaUnits};
use crate::schedule::TimerKind;
use crate::status::Status;

/// A request for the host to perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    /// Matches the result event to this command.
    pub id: CommandId,
    /// What to do.
    pub kind: CommandKind,
}

/// What the host should do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandKind {
    /// Call Gmail. The host admits it through the quota gate first.
    Gmail(GmailCall),
    /// Apply a commit atomically, then report `CommitApplied` or
    /// `CommitRejected`.
    Commit(Box<Commit>),
    /// Apply a bounded mailbox audit step and report its outcome.
    AdvanceAudit(crate::audit::AuditRequest),
    /// Fire `TimerFired` at `at`. A later schedule of the same timer replaces
    /// this one.
    Schedule {
        /// Which timer.
        timer: TimerKind,
        /// When.
        at: Timestamp,
    },
    /// Claim up to `limit` due jobs for `attempt`; report `JobsClaimed`.
    ClaimJobs {
        /// The account.
        account: AccountId,
        /// The claiming engine's epoch; claims from older epochs are refused.
        epoch: crate::ids::Epoch,
        /// Account incarnation at claim time.
        account_generation: u64,
        /// At most this many.
        limit: u32,
        /// Due at or before this time.
        now: Timestamp,
        /// The attempt the claims belong to.
        attempt: crate::jobs::AttemptId,
        /// Only jobs of at least this priority: lower classes are held back
        /// by the quota gate.
        floor: crate::quota::Priority,
    },
    /// Delete a message's raw content at its deterministic key; report
    /// `BlobDeleted`.
    DeleteBlob {
        /// The account.
        account: AccountId,
        /// The message.
        message: MessageId,
    },
    /// Publish the account's sync status.
    ReportStatus(Status),
}

/// A Gmail request with its declared cost and priority (SPEC §8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GmailCall {
    /// The account to act as.
    pub account: AccountId,
    /// The request.
    pub request: GmailRequest,
    /// Admission class.
    pub priority: Priority,
    /// Declared quota cost.
    pub cost: QuotaUnits,
}

/// The Gmail requests the engine issues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GmailRequest {
    /// `getProfile`, for the reconciliation baseline.
    GetProfile,
    /// `history.list` from `start`. Never filtered by type or label.
    ListHistory {
        /// The committed cursor.
        start: HistoryId,
        /// The page to read.
        page_token: Option<PageToken>,
    },
    /// `messages.list` over a scope, including spam and trash.
    ListMessages {
        /// All mail, or one label (SPEC §6.3).
        scope: crate::reconcile::ListScope,
        /// The page to read.
        page_token: Option<PageToken>,
    },
    /// An all-mail audit page with just message ids and the next token.
    AuditMessages {
        /// The page to read.
        page_token: Option<PageToken>,
    },
    /// `messages.get?format=minimal`: labels and checkpoint only.
    GetMessage {
        /// The message.
        id: MessageId,
    },
    /// `messages.get?format=raw`; the host stores the content at the
    /// account's deterministic blob key, then reports `MessageImported`.
    ImportMessage {
        /// The message.
        id: MessageId,
    },
    /// `labels.list`.
    ListLabels,
    /// `users.watch` on all labels.
    Watch,
    /// `messages.modify` or `batchModify` for one group of identical label
    /// changes (phase 6).
    Modify(ModifyGroup),
    /// `messages.list` or `threads.list` with the host's query: one page of
    /// a search. Issued by the driver (`Runner::search`), never by the
    /// engine; answered with [`crate::event::EventKind::Searched`].
    Search(crate::search::SearchRequest),
    /// `labels.get`: whether Gmail has a label the mirror's definitions
    /// lack or a listing omitted, answered with
    /// [`crate::event::EventKind::LabelFetched`].
    GetLabel {
        /// The label.
        id: crate::ids::LabelId,
    },
    /// `labels.create`. Issued by the driver for a host, never by the
    /// engine; answered by `LabelSaved` (or `LabelNameTaken` for a 409).
    CreateLabel(NewLabel),
    /// `labels.patch` of label `id`, sending only what `patch` changes.
    /// Issued by the driver; answered by `LabelSaved` (or `LabelNameTaken`).
    PatchLabel {
        /// The label.
        id: LabelId,
        /// What changes.
        patch: LabelPatch,
    },
    /// `labels.delete` of label `id`. Issued by the driver; answered by
    /// `LabelDeleted`.
    DeleteLabel {
        /// The label.
        id: LabelId,
    },
    /// `messages.delete`: removes one message for good. Issued by the
    /// driver's purge ([`crate::purge`]), never by the engine.
    DeleteMessage {
        /// The message.
        id: MessageId,
    },
    /// `messages.batchDelete`: removes these messages for good, by id only
    /// (never a query). Issued by the driver's purge, never by the engine.
    BatchDeleteMessages {
        /// At most [`crate::purge::BATCH_DELETE_MAX`] ids.
        ids: Vec<MessageId>,
    },
    /// `drafts.list`, including spam and trash: the draft index refresh.
    ListDrafts {
        /// The page to read.
        page_token: Option<PageToken>,
    },
    /// `drafts.get` (a direct call).
    GetDraft {
        /// The draft.
        id: DraftId,
        /// How much of its message to return.
        format: DraftFormat,
    },
    /// `drafts.create` from raw bytes (a direct call).
    CreateDraft {
        /// The message.
        raw: Rfc822,
        /// The thread to put it in, for a reply.
        thread_id: Option<ThreadId>,
    },
    /// `drafts.update`: replace the draft's message (a direct call). The
    /// thread id is never left out: Gmail moves a reply draft updated
    /// without one out of its thread.
    UpdateDraft {
        /// The draft.
        id: DraftId,
        /// The new message.
        raw: Rfc822,
        /// The draft's thread.
        thread_id: ThreadId,
    },
    /// `drafts.delete`: permanent, not to trash (a direct call).
    DeleteDraft {
        /// The draft.
        id: DraftId,
    },
    /// `drafts.send`: send the stored draft as is (a direct call).
    SendDraft {
        /// The draft.
        id: DraftId,
    },
}
