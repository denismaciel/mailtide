//! Facts the host reports to the engine (SPEC §10.1).
//!
//! Every event carries the host's clock, so the engine itself never reads the
//! time and stays deterministic (invariant I10).
//!
//! Zero counterpart: the RPC and queue inputs of `routes/agent/index.ts` and
//! `gmail-push.ts`.

use crate::drafts::{DraftPage, FetchedDraft, WrittenDraft};
use crate::gmail::{HistoryPage, ImportedMessage, Label, ListPage, MessageObservation};
use crate::ids::{CommandId, CommitId, DraftId, HistoryId, MessageId, ThreadId, Timestamp};
use crate::outbox::ActionRequest;
use crate::retry::Failure;
use crate::schedule::TimerKind;

/// A fact the host observed, stamped with when it observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// The host's clock when the event happened.
    pub now: Timestamp,
    /// What happened.
    pub kind: EventKind,
}

/// What the host observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    /// A Gmail push notification arrived. A hint, never data (SPEC §6.2).
    PushReceived {
        /// The mailbox revision the notification reported.
        history_id: HistoryId,
    },
    /// A timer the engine scheduled fired.
    TimerFired {
        /// Which timer.
        timer: TimerKind,
    },
    /// `getProfile` returned.
    ProfileFetched {
        /// The command it answers.
        command: CommandId,
        /// The mailbox's current history id.
        history_id: HistoryId,
    },
    /// A `history.list` page arrived.
    HistoryFetched {
        /// The command it answers.
        command: CommandId,
        /// The page.
        page: HistoryPage,
    },
    /// `history.list` returned 404: the cursor is too old (SPEC §6.3).
    HistoryExpired {
        /// The command it answers.
        command: CommandId,
    },
    /// A `messages.list` page arrived.
    MessagesListed {
        /// The command it answers.
        command: CommandId,
        /// The page.
        page: ListPage,
    },
    /// `messages.get?format=minimal` returned.
    MessageFetched {
        /// The command it answers.
        command: CommandId,
        /// The message's labels and checkpoint.
        message: MessageObservation,
    },
    /// The host fetched a message's raw content and stored it durably.
    MessageImported {
        /// The command it answers.
        command: CommandId,
        /// The imported message.
        message: Box<ImportedMessage>,
    },
    /// Gmail returned 404 for a message (SPEC §6.3 step 3, §6.4).
    MessageNotFound {
        /// The command it answers.
        command: CommandId,
        /// The missing message.
        message_id: MessageId,
    },
    /// `labels.list` returned.
    LabelsListed {
        /// The command it answers.
        command: CommandId,
        /// All label definitions.
        labels: Vec<Label>,
        /// The account's label generation, read from the store before the
        /// request was sent (`Store::label_generation`): the listing is
        /// applied only if no local label write happened since.
        generation: u64,
        /// Non-system definitions in the store, read after the generation
        /// and before sending the request. A write racing either read is
        /// covered by the generation fence.
        stored_user_labels: crate::labels::LabelSet,
    },
    /// `users.watch` succeeded.
    WatchRenewed {
        /// The command it answers.
        command: CommandId,
        /// When the watch expires.
        expiration: Timestamp,
    },
    /// The store applied a commit.
    CommitApplied {
        /// The commit.
        commit: CommitId,
    },
    /// The store claimed due jobs for this engine.
    JobsClaimed {
        /// The command it answers.
        command: CommandId,
        /// The claimed jobs, each now held by the command's attempt.
        jobs: Vec<crate::jobs::ClaimedJob>,
        /// When the earliest job left ready is due.
        next_due: Option<crate::ids::Timestamp>,
    },
    /// The store refused the claim: another engine holds the account.
    JobsClaimRejected {
        /// The command it answers.
        command: CommandId,
        /// Why.
        reason: crate::commit::Rejection,
    },
    /// The host deleted a message's raw content.
    BlobDeleted {
        /// The command it answers.
        command: CommandId,
    },
    /// The store rejected a commit (stale epoch, cursor mismatch, …).
    CommitRejected {
        /// The commit.
        commit: CommitId,
        /// Why.
        reason: crate::commit::Rejection,
    },
    /// A Gmail request failed.
    RequestFailed {
        /// The command it answers.
        command: CommandId,
        /// The classified failure.
        failure: Failure,
    },
    /// The quota gate deferred a request; it was not sent.
    RequestDeferred {
        /// The command.
        command: CommandId,
        /// When to try again.
        until: Timestamp,
    },
    /// The user asked for an action in the host's UI (phase 6, SPEC §7).
    ActionRequested {
        /// The request.
        action: ActionRequest,
    },
    /// `batchModify` or `modify` succeeded (phase 6).
    LabelsModified {
        /// The command it answers.
        command: CommandId,
    },
    /// A page of a search arrived. It answers the driver's direct call,
    /// never an engine command: the engine ignores it.
    Searched {
        /// The command it answers.
        command: CommandId,
        /// The page.
        listing: crate::search::SearchListing,
    },
    /// `labels.get` answered with the label. It answers the driver's direct
    /// call, never an engine command: the engine ignores it.
    LabelFetched {
        /// The command it answers.
        command: CommandId,
        /// The label's definition.
        label: Label,
    },
    /// `labels.create` or `labels.patch` succeeded: the label as it is now.
    /// The answer of a driver call; the engine issues none.
    LabelSaved {
        /// The command it answers.
        command: CommandId,
        /// Gmail's answer.
        label: Label,
    },
    /// `labels.create` or `labels.patch` answered 409: another label holds
    /// the name (Gmail compares names case-insensitively).
    LabelNameTaken {
        /// The command it answers.
        command: CommandId,
    },
    /// `labels.delete` succeeded.
    LabelDeleted {
        /// The command it answers.
        command: CommandId,
    },
    /// `messages.delete` or `messages.batchDelete` answered 2xx. Gmail's
    /// answer names no id and proves nothing: only a history record is
    /// evidence of the deletion ([`crate::purge`]). Answers the driver's
    /// purge, never an engine command.
    MessagesDeleted {
        /// The command it answers.
        command: CommandId,
        /// The ids the request named.
        ids: Vec<MessageId>,
    },
    /// A `drafts.list` page arrived.
    DraftsListed {
        /// The command it answers.
        command: CommandId,
        /// The page.
        page: DraftPage,
    },
    /// `drafts.get` returned a draft.
    DraftFetched {
        /// The command it answers.
        command: CommandId,
        /// The draft.
        draft: FetchedDraft,
    },
    /// `drafts.create` or `drafts.update` succeeded.
    DraftSaved {
        /// The command it answers.
        command: CommandId,
        /// Gmail's answer.
        draft: WrittenDraft,
    },
    /// `drafts.delete` succeeded.
    DraftDeleted {
        /// The command it answers.
        command: CommandId,
    },
    /// `drafts.send` succeeded: the draft is gone and a new message left.
    DraftSent {
        /// The command it answers.
        command: CommandId,
        /// The sent message (a new id, labelled `SENT`).
        message: MessageId,
        /// Its thread.
        thread_id: ThreadId,
    },
    /// Gmail has no such draft (404), or what it returned is not a draft.
    DraftNotFound {
        /// The command it answers.
        command: CommandId,
        /// The draft asked for.
        id: DraftId,
    },
}
