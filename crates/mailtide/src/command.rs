//! Requests the engine returns for the host to perform (SPEC §10.2).

use crate::commit::Commit;
use crate::ids::{AccountId, CommandId, HistoryId, MessageId, PageToken, Timestamp};
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
    /// lack. Issued by the driver (a search's label check), never by the
    /// engine; answered with [`crate::event::EventKind::LabelFetched`].
    GetLabel {
        /// The label.
        id: crate::ids::LabelId,
    },
}
