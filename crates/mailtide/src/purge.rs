//! Purge: permanent delete, empty trash and empty spam (the pure rules).
//!
//! A purge is the one operation in mailtide that cannot be undone, so it is
//! a driver call (`mailtide_io::Runner::purge`), never a queued intent, and
//! these rules decide everything that matters before Gmail is asked:
//!
//! - **Eligibility** is judged on Gmail's state up to the moment of the
//!   call, not on a mirror that may lag: [`PurgeScan`] folds the history
//!   read since the mirror's cursor into each message's observed labels
//!   with the mirror's own checkpoint rule; the actions not yet confirmed
//!   may only take labels away (a restore asked for here refuses), never
//!   add `TRASH` or `SPAM`. By default only a message in `TRASH` or `SPAM`
//!   is eligible ([`Scope`]).
//! - **By id only**: [`delete_requests`] turns eligible ids into
//!   `messages.delete` calls (fewer than
//!   [`PURGE_BATCH_THRESHOLD`](crate::quota::PURGE_BATCH_THRESHOLD)) or
//!   `messages.batchDelete` calls of at most [`BATCH_DELETE_MAX`]; never
//!   `threads.delete`, never a query.
//! - **Evidence**: a delete's answer proves nothing, a 2xx or a 404. Only a
//!   `messagesDeleted` history record makes a tombstone;
//!   [`PurgeScan::deleted`] finds the records.

use std::collections::{BTreeMap, BTreeSet};

use crate::command::GmailRequest;
use crate::gmail::{HistoryPage, HistoryRecord};
use crate::ids::{HistoryId, LabelId, MessageId, Timestamp};
use crate::labels::{self, LabelSet};
use crate::mirror::MessageView;
use crate::quota::PURGE_BATCH_THRESHOLD;

/// An explicit purge names at most this many messages (a thread at most
/// this many members); a larger set is split by the caller on purpose.
pub const PURGE_MAX_IDS: usize = 1_000;

/// At most this many ids per `messages.batchDelete`.
pub const BATCH_DELETE_MAX: usize = 1_000;

/// A mirror more than this many history pages behind Gmail is stale for a
/// purge: the account should sync first (`MirrorStale`).
pub const PURGE_HISTORY_PAGES: u32 = 20;

/// The deployment's ceiling on purges (`Config.purge`). Off unless a host
/// switches it on: the one operation that destroys mail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PurgePolicy {
    /// Every purge is refused.
    #[default]
    Off,
    /// Only messages in trash or spam may be purged.
    TrashAndSpam,
    /// A call may also ask (`anywhere`) to purge messages wherever they are.
    Anywhere,
}

/// What an explicit purge names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PurgeTarget {
    /// These messages.
    Messages(Vec<MessageId>),
    /// The non-draft messages the mirror holds for this thread at the call.
    Thread(crate::ids::ThreadId),
}

/// The bin `empty` empties.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bin {
    /// `TRASH`.
    Trash,
    /// `SPAM`.
    Spam,
}

impl Bin {
    /// The bin's label id.
    #[must_use]
    pub fn label(self) -> LabelId {
        LabelId(
            match self {
                Self::Trash => labels::system::TRASH,
                Self::Spam => labels::system::SPAM,
            }
            .to_owned(),
        )
    }
}

/// Which messages a purge may reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Messages in trash or spam (the default policy).
    TrashOrSpam,
    /// Messages in this bin (`empty`).
    Bin(Bin),
    /// Messages wherever they are (the `anywhere` policy and flag).
    Anywhere,
}

/// Why an id was not sent. Nothing reached Gmail for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The mirror has no such message (never seen, or already deleted).
    NotInMirror,
    /// A draft: `delete_draft` owns those.
    Draft,
    /// Its labels await verification, so where it is can't be told.
    Unverified,
    /// Not in trash or spam (or not in the bin being emptied), as of the
    /// call: it may have been restored elsewhere since the last sync.
    NotInTrashOrSpam,
    /// Gmail refused the delete (its message): nothing was deleted.
    GmailRefused(String),
}

impl Refusal {
    /// A stable name for hosts and logs.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotInMirror => "not_in_mirror",
            Self::Draft => "draft",
            Self::Unverified => "unverified",
            Self::NotInTrashOrSpam => "not_in_trash_or_spam",
            Self::GmailRefused(_) => "gmail_refused",
        }
    }
}

/// What the rules make of one target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Send the delete.
    Eligible,
    /// History already shows it deleted (elsewhere): record the evidence,
    /// send nothing.
    Gone {
        /// The `messagesDeleted` record's id.
        at: HistoryId,
    },
    /// Not sent.
    Refused(Refusal),
}

/// What an explicit purge did, per id (each distinct id in exactly one
/// list).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PurgeOutcome {
    /// Gmail's history shows the deletion: the tombstone is written and
    /// `MessageDeleted` is in the feed.
    pub purged: Vec<MessageId>,
    /// The delete reached Gmail, or may have (its answer was lost, a 5xx,
    /// a 404), and history has not shown the deletion yet: the row stays
    /// until a sync sees it. mailtide never sends it again on its own.
    pub sent: Vec<MessageId>,
    /// Nothing was deleted for these, and why.
    pub refused: Vec<(MessageId, Refusal)>,
    /// Never sent: the quota gate could not admit the delete at once (a
    /// delete never waits), Gmail limited the rate (429), the token was
    /// rejected, or an earlier failure ended the call. Call again; the
    /// call checks them afresh.
    pub unsent: Vec<MessageId>,
    /// When a deferred or rate-limited delete could go, at the earliest.
    pub retry_at: Option<Timestamp>,
    /// Why the call ended early after deletes may have reached Gmail (the
    /// store or the quota gate failed): what it reports is what was known.
    pub interrupted: Option<String>,
}

/// What emptying a bin did, in counts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EmptyOutcome {
    /// Deleted, with Gmail's evidence recorded.
    pub purged: u64,
    /// Sent (or may have been), not yet shown by history.
    pub sent: u64,
    /// Held in the bin by the mirror but not deleted: restored since,
    /// drafts, unverified, or refused by Gmail.
    pub skipped: u64,
    /// Not reached: the budget, the quota gate, a rate limit, a failing
    /// Gmail or an unread confirmation ended the call first. Call again;
    /// it reads the mirror afresh.
    pub remaining: u64,
    /// When a deferred or rate-limited delete could go, at the earliest.
    pub retry_at: Option<Timestamp>,
    /// Why the call ended early after deletes may have reached Gmail.
    pub interrupted: Option<String>,
}

/// One message's history since the cursor, as a purge reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Delta {
    Added(LabelSet),
    Removed(LabelSet),
    Observed(LabelSet),
    Deleted,
}

/// History read since the mirror's cursor, kept per message: the private
/// catch-up a purge checks eligibility against and later finds Gmail's
/// evidence in. Committed nowhere; the cursor never moves. Pages are read
/// in order and appended; a later read starts at [`Self::head`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PurgeScan {
    records: BTreeMap<MessageId, Vec<(HistoryId, Delta)>>,
    head: Option<HistoryId>,
}

impl PurgeScan {
    /// An empty scan.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one `history.list` page in. The last page of a read (no next
    /// page) sets [`Self::head`].
    pub fn read(&mut self, page: &HistoryPage) {
        for record in &page.records {
            let (id, at, delta) = match record {
                HistoryRecord::MessageAdded { id, message } => {
                    (&message.id, *id, Delta::Observed(message.labels.clone()))
                }
                HistoryRecord::MessageDeleted { id, message_id } => {
                    (message_id, *id, Delta::Deleted)
                }
                HistoryRecord::LabelsAdded {
                    id,
                    message_id,
                    labels,
                } => (message_id, *id, Delta::Added(labels.clone())),
                HistoryRecord::LabelsRemoved {
                    id,
                    message_id,
                    labels,
                } => (message_id, *id, Delta::Removed(labels.clone())),
            };
            self.records
                .entry(id.clone())
                .or_default()
                .push((at, delta));
        }
        if page.next_page_token.is_none() {
            self.head = Some(
                self.head
                    .map_or(page.history_id, |h| h.max(page.history_id)),
            );
        }
    }

    /// The history id Gmail reported with the last complete read: where the
    /// next read starts.
    #[must_use]
    pub fn head(&self) -> Option<HistoryId> {
        self.head
    }

    /// The `messagesDeleted` record for `id`, if history shows one: the
    /// evidence a tombstone needs.
    #[must_use]
    pub fn deleted(&self, id: &MessageId) -> Option<HistoryId> {
        self.records
            .get(id)?
            .iter()
            .find(|(_, delta)| *delta == Delta::Deleted)
            .map(|(at, _)| *at)
    }

    /// The labels Gmail holds for the message now, as far as history shows:
    /// the row's observed labels with every record at or after its
    /// checkpoint applied, as the mirror's own rule applies them (records
    /// older than the row's checkpoint are already in it).
    fn folded(&self, view: &MessageView) -> LabelSet {
        let mut current = view.observed_labels.clone();
        for (at, delta) in self.records.get(&view.id).into_iter().flatten() {
            if !labels::accepts(view.checkpoint, *at) {
                continue;
            }
            match delta {
                Delta::Added(added) => current.extend(added.iter().cloned()),
                Delta::Removed(removed) => current.retain(|label| !removed.contains(label)),
                Delta::Observed(observed) => current.clone_from(observed),
                Delta::Deleted => {}
            }
        }
        current
    }

    /// The labels the rules judge `view` by: Gmail's labels now
    /// ([`Self::folded`]), less what the actions not yet confirmed here
    /// remove. The overlay only ever makes a message *less* eligible: a
    /// restore, a move to the inbox or a not-spam asked for here (pending,
    /// or delivered and awaiting confirmation) refuses it. It never adds a
    /// label: a trash or spam asked for here counts once Gmail's state
    /// shows it (the observed labels, or history since the checkpoint).
    /// The view cannot tell a pending action from a delivered one, and a
    /// delivered trash may since have been undone elsewhere and synced, so
    /// adding the overlay's `TRASH` would purge a message the user restored.
    #[must_use]
    pub fn current_labels(&self, view: &MessageView) -> LabelSet {
        let mut current = self.folded(view);
        current
            .retain(|label| !view.observed_labels.contains(label) || view.labels.contains(label));
        current
    }

    /// The verdict on target `id`, whose mirror view is `view` (`None`: the
    /// mirror has no row).
    #[must_use]
    pub fn verdict(&self, id: &MessageId, view: Option<&MessageView>, scope: Scope) -> Verdict {
        let Some(view) = view else {
            return Verdict::Refused(Refusal::NotInMirror);
        };
        if let Some(at) = self.deleted(id) {
            return Verdict::Gone { at };
        }
        if labels::is_draft(&view.labels) || labels::is_draft(&view.observed_labels) {
            return Verdict::Refused(Refusal::Draft);
        }
        if view.unverified {
            return Verdict::Refused(Refusal::Unverified);
        }
        let current = self.current_labels(view);
        if labels::is_draft(&current) {
            return Verdict::Refused(Refusal::Draft);
        }
        let in_scope = match scope {
            Scope::Anywhere => true,
            Scope::TrashOrSpam => current.iter().any(is_bin),
            Scope::Bin(bin) => current.contains(&bin.label()),
        };
        if in_scope {
            Verdict::Eligible
        } else {
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        }
    }
}

fn is_bin(label: &LabelId) -> bool {
    label.0 == labels::system::TRASH || label.0 == labels::system::SPAM
}

/// The ids a thread target reaches: its non-draft members, as the mirror
/// holds them at the call (never `threads.delete`, which would also destroy
/// a reply the mirror has not seen).
#[must_use]
pub fn thread_targets(members: &[MessageView]) -> Vec<MessageId> {
    members
        .iter()
        .filter(|view| !labels::is_draft(&view.labels))
        .map(|view| view.id.clone())
        .collect()
}

/// Each distinct id once, in the order given.
#[must_use]
pub fn distinct(ids: &[MessageId]) -> Vec<MessageId> {
    let mut seen = BTreeSet::new();
    ids.iter().filter(|id| seen.insert(*id)).cloned().collect()
}

/// The delete calls for `ids`: one `messages.delete` each below
/// [`PURGE_BATCH_THRESHOLD`] (per-id answers, at most 40 units), otherwise
/// `messages.batchDelete` calls of at most [`BATCH_DELETE_MAX`] ids.
#[must_use]
pub fn delete_requests(ids: &[MessageId]) -> Vec<GmailRequest> {
    let threshold = usize::try_from(PURGE_BATCH_THRESHOLD).unwrap_or(usize::MAX);
    if ids.len() < threshold {
        return ids
            .iter()
            .map(|id| GmailRequest::DeleteMessage { id: id.clone() })
            .collect();
    }
    ids.chunks(BATCH_DELETE_MAX)
        .map(|chunk| GmailRequest::BatchDeleteMessages {
            ids: chunk.to_vec(),
        })
        .collect()
}

/// The ids a delete request names.
#[must_use]
pub fn request_ids(request: &GmailRequest) -> Vec<MessageId> {
    match request {
        GmailRequest::DeleteMessage { id } => vec![id.clone()],
        GmailRequest::BatchDeleteMessages { ids } => ids.clone(),
        GmailRequest::GetProfile
        | GmailRequest::ListHistory { .. }
        | GmailRequest::ListMessages { .. }
        | GmailRequest::GetMessage { .. }
        | GmailRequest::ImportMessage { .. }
        | GmailRequest::ListLabels
        | GmailRequest::Watch
        | GmailRequest::Modify(_)
        | GmailRequest::Search(_)
        | GmailRequest::GetLabel { .. }
        | GmailRequest::CreateLabel(_)
        | GmailRequest::PatchLabel { .. }
        | GmailRequest::DeleteLabel { .. } => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gmail::MessageRef;
    use crate::ids::{AccountId, ThreadId, Timestamp};
    use crate::labels::set;
    use crate::mirror::{AccountMirror, Content, MessageRecord};
    use crate::outbox::{Action, ActionRequest, RestoreDestination};

    fn id(id: &str) -> MessageId {
        MessageId(id.into())
    }

    /// A mirror holding `rows`: (id, labels, checkpoint).
    fn mirror(rows: &[(&str, &[&str], u64)]) -> AccountMirror {
        let mut mirror = AccountMirror::new(AccountId("a@example.com".into()), 1);
        for (row, labels, checkpoint) in rows {
            mirror.messages.insert(
                id(row),
                MessageRecord {
                    id: id(row),
                    thread_id: ThreadId(format!("t-{row}")),
                    labels: set(labels.iter().copied()),
                    checkpoint: Some(HistoryId(*checkpoint)),
                    unverified: None,
                    content: Content::Pending,
                },
            );
        }
        mirror
    }

    fn act(mirror: &mut AccountMirror, row: &str, action: Action) {
        let admitted = mirror.admit_action(
            &ActionRequest::Messages {
                ids: vec![id(row)],
                action,
            },
            Timestamp(0),
        );
        assert_eq!(admitted, 1, "the action is admitted");
    }

    fn removed(at: u64, row: &str, labels: &[&str]) -> HistoryRecord {
        HistoryRecord::LabelsRemoved {
            id: HistoryId(at),
            message_id: id(row),
            labels: set(labels.iter().copied()),
        }
    }

    fn added(at: u64, row: &str, labels: &[&str]) -> HistoryRecord {
        HistoryRecord::LabelsAdded {
            id: HistoryId(at),
            message_id: id(row),
            labels: set(labels.iter().copied()),
        }
    }

    fn scan(records: Vec<HistoryRecord>, head: u64) -> PurgeScan {
        let mut scan = PurgeScan::new();
        scan.read(&HistoryPage {
            records,
            history_id: HistoryId(head),
            next_page_token: None,
        });
        scan
    }

    fn verdict(mirror: &AccountMirror, scan: &PurgeScan, row: &str, scope: Scope) -> Verdict {
        scan.verdict(&id(row), mirror.view(&id(row)).as_ref(), scope)
    }

    #[test]
    fn a_trashed_message_with_user_labels_is_purged() {
        let mirror = mirror(&[
            ("t", &["TRASH", "Label_1", "STARRED"], 10),
            ("s", &["SPAM", "Label_2"], 10),
            ("i", &["INBOX", "Label_1"], 10),
        ]);
        let scan = scan(Vec::new(), 10);
        assert_eq!(
            verdict(&mirror, &scan, "t", Scope::TrashOrSpam),
            Verdict::Eligible
        );
        assert_eq!(
            verdict(&mirror, &scan, "s", Scope::TrashOrSpam),
            Verdict::Eligible
        );
        assert_eq!(
            verdict(&mirror, &scan, "i", Scope::TrashOrSpam),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
        // Emptying one bin reaches only that bin.
        assert_eq!(
            verdict(&mirror, &scan, "s", Scope::Bin(Bin::Trash)),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
        assert_eq!(
            verdict(&mirror, &scan, "s", Scope::Bin(Bin::Spam)),
            Verdict::Eligible
        );
    }

    #[test]
    fn a_restore_seen_in_history_makes_a_message_ineligible() {
        let mirror = mirror(&[("m", &["TRASH"], 10)]);
        // Restored on a phone after the mirror last saw the message.
        let restored = scan(
            vec![removed(12, "m", &["TRASH"]), added(12, "m", &["INBOX"])],
            12,
        );
        assert_eq!(
            verdict(&mirror, &restored, "m", Scope::TrashOrSpam),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
    }

    /// Every intent of the mirror delivered (awaiting its confirmation).
    fn deliver_all(mirror: &mut AccountMirror) {
        for intent in mirror.intents.values_mut() {
            intent.state = crate::outbox::IntentState::Delivered {
                at: Timestamp(0),
                attempt: crate::jobs::AttemptId(1),
            };
        }
    }

    #[test]
    fn a_delivered_trash_undone_elsewhere_and_synced_is_not_eligible() {
        // Trashed here and delivered (history 11), restored on a phone
        // (history 13), both synced: the row is observed in the inbox at 13
        // while the delivered trash still overlays until its confirmation.
        let mut synced = mirror(&[("d", &["INBOX"], 13)]);
        act(&mut synced, "d", Action::MoveToTrash);
        deliver_all(&mut synced);
        let view = synced.view(&id("d")).expect("view");
        assert!(
            view.labels.contains(&LabelId("TRASH".into())),
            "the view shows it trashed"
        );
        assert_eq!(
            verdict(&synced, &scan(Vec::new(), 13), "d", Scope::TrashOrSpam),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
        // Not yet synced either: history since the checkpoint shows both.
        let mut unsynced = mirror(&[("d", &["INBOX"], 10)]);
        act(&mut unsynced, "d", Action::MoveToTrash);
        deliver_all(&mut unsynced);
        let history = scan(
            vec![
                added(11, "d", &["TRASH"]),
                removed(11, "d", &["INBOX"]),
                removed(13, "d", &["TRASH"]),
                added(13, "d", &["INBOX"]),
            ],
            13,
        );
        assert_eq!(
            verdict(&unsynced, &history, "d", Scope::TrashOrSpam),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
    }

    #[test]
    fn a_trash_whose_answer_was_lost_and_then_undone_is_not_eligible() {
        // The delivery was applied and its answer lost, so the intent is
        // still pending; a phone restored the message; a sync applied both.
        let mut mirror = mirror(&[("m", &["INBOX"], 13)]);
        act(&mut mirror, "m", Action::MoveToTrash);
        assert_eq!(
            verdict(&mirror, &scan(Vec::new(), 13), "m", Scope::TrashOrSpam),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
    }

    #[test]
    fn a_trash_seen_only_in_history_makes_a_message_eligible() {
        let mirror = mirror(&[("m", &["INBOX"], 10), ("n", &["INBOX"], 10)]);
        let history = scan(
            vec![
                added(11, "m", &["TRASH"]),
                removed(11, "m", &["INBOX"]),
                // Trashed and restored again: in the inbox now.
                added(12, "n", &["TRASH"]),
                removed(13, "n", &["TRASH"]),
            ],
            13,
        );
        assert_eq!(
            verdict(&mirror, &history, "m", Scope::TrashOrSpam),
            Verdict::Eligible
        );
        assert_eq!(
            verdict(&mirror, &history, "n", Scope::TrashOrSpam),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
    }

    #[test]
    fn a_pending_restore_makes_a_message_ineligible() {
        let mut mirror = mirror(&[("r", &["TRASH"], 10), ("i", &["SPAM"], 10)]);
        act(
            &mut mirror,
            "r",
            Action::RestoreFromTrash(RestoreDestination::Inbox),
        );
        act(&mut mirror, "i", Action::MoveToInbox);
        let scan = scan(Vec::new(), 10);
        assert_eq!(
            verdict(&mirror, &scan, "r", Scope::TrashOrSpam),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
        assert_eq!(
            verdict(&mirror, &scan, "i", Scope::TrashOrSpam),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
    }

    #[test]
    fn a_trash_asked_here_counts_only_once_gmail_shows_it() {
        let mut mirror = mirror(&[("m", &["INBOX", "UNREAD"], 10)]);
        act(&mut mirror, "m", Action::MoveToTrash);
        // Not delivered yet: Gmail still has it in the inbox.
        assert_eq!(
            verdict(&mirror, &scan(Vec::new(), 10), "m", Scope::TrashOrSpam),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
        // Delivered: history shows the trash, and the message is eligible
        // although the action awaits its confirmation.
        deliver_all(&mut mirror);
        let delivered = scan(
            vec![added(11, "m", &["TRASH"]), removed(11, "m", &["INBOX"])],
            11,
        );
        assert_eq!(
            verdict(&mirror, &delivered, "m", Scope::TrashOrSpam),
            Verdict::Eligible
        );
        assert_eq!(
            verdict(&mirror, &delivered, "m", Scope::Bin(Bin::Trash)),
            Verdict::Eligible
        );
    }

    #[test]
    fn drafts_unverified_rows_and_unknown_ids_are_refused() {
        let mut mirror = mirror(&[("d", &["DRAFT", "TRASH"], 10), ("u", &["TRASH"], 10)]);
        if let Some(row) = mirror.messages.get_mut(&id("u")) {
            row.unverified = Some(HistoryId(10));
        }
        let scan = scan(Vec::new(), 10);
        for scope in [Scope::TrashOrSpam, Scope::Anywhere] {
            assert_eq!(
                verdict(&mirror, &scan, "d", scope),
                Verdict::Refused(Refusal::Draft)
            );
            assert_eq!(
                verdict(&mirror, &scan, "u", scope),
                Verdict::Refused(Refusal::Unverified)
            );
            assert_eq!(
                verdict(&mirror, &scan, "unknown", scope),
                Verdict::Refused(Refusal::NotInMirror)
            );
        }
        assert_eq!(Refusal::Draft.code(), "draft");
        assert_eq!(Refusal::Unverified.code(), "unverified");
        assert_eq!(Refusal::NotInMirror.code(), "not_in_mirror");
    }

    #[test]
    fn history_older_than_a_rows_checkpoint_is_not_folded_twice() {
        // The cursor is at 10; the row was observed in full at 20 (a fetch
        // newer than the cursor), in the trash. A restore at 15 and the
        // trash at 18 are already in that observation.
        let mirror = mirror(&[("m", &["TRASH"], 20)]);
        let history = scan(
            vec![
                removed(15, "m", &["TRASH"]),
                added(15, "m", &["INBOX"]),
                added(18, "m", &["TRASH"]),
            ],
            20,
        );
        assert_eq!(
            verdict(&mirror, &history, "m", Scope::TrashOrSpam),
            Verdict::Eligible
        );
        // A record after the checkpoint still counts.
        let later = scan(vec![removed(21, "m", &["TRASH"])], 21);
        assert_eq!(
            verdict(&mirror, &later, "m", Scope::TrashOrSpam),
            Verdict::Refused(Refusal::NotInTrashOrSpam)
        );
    }

    #[test]
    fn anywhere_skips_the_label_rule_and_nothing_else() {
        let mut mirror = mirror(&[
            ("i", &["INBOX"], 10),
            ("a", &[], 10),
            ("d", &["DRAFT"], 10),
            ("u", &["INBOX"], 10),
        ]);
        if let Some(row) = mirror.messages.get_mut(&id("u")) {
            row.unverified = Some(HistoryId(10));
        }
        let scan = scan(Vec::new(), 10);
        assert_eq!(
            verdict(&mirror, &scan, "i", Scope::Anywhere),
            Verdict::Eligible
        );
        assert_eq!(
            verdict(&mirror, &scan, "a", Scope::Anywhere),
            Verdict::Eligible
        );
        assert_eq!(
            verdict(&mirror, &scan, "d", Scope::Anywhere),
            Verdict::Refused(Refusal::Draft)
        );
        assert_eq!(
            verdict(&mirror, &scan, "u", Scope::Anywhere),
            Verdict::Refused(Refusal::Unverified)
        );
        assert_eq!(
            verdict(&mirror, &scan, "nope", Scope::Anywhere),
            Verdict::Refused(Refusal::NotInMirror)
        );
    }

    #[test]
    fn a_thread_expands_to_the_messages_the_mirror_holds() {
        let mut mirror = mirror(&[]);
        for (row, labels) in [
            ("a", &["TRASH"][..]),
            ("b", &["TRASH", "UNREAD"][..]),
            ("draft", &["DRAFT"][..]),
            ("other", &["TRASH"][..]),
        ] {
            mirror.messages.insert(
                id(row),
                MessageRecord {
                    id: id(row),
                    thread_id: ThreadId(if row == "other" { "t2" } else { "t1" }.into()),
                    labels: set(labels.iter().copied()),
                    checkpoint: Some(HistoryId(10)),
                    unverified: None,
                    content: Content::Pending,
                },
            );
        }
        let thread = mirror.thread(&ThreadId("t1".into())).expect("thread");
        assert_eq!(thread_targets(&thread.messages), vec![id("a"), id("b")]);
        // Only message ids are ever sent: never threads.delete.
        let requests = delete_requests(&thread_targets(&thread.messages));
        assert_eq!(
            requests,
            vec![
                GmailRequest::DeleteMessage { id: id("a") },
                GmailRequest::DeleteMessage { id: id("b") },
            ]
        );
    }

    #[test]
    fn only_a_messages_deleted_record_confirms_a_purge() {
        let mirror = mirror(&[("a", &["TRASH"], 10), ("b", &["TRASH"], 10)]);
        let mut scan = scan(Vec::new(), 10);
        assert_eq!(
            verdict(&mirror, &scan, "a", Scope::TrashOrSpam),
            Verdict::Eligible
        );
        // The confirmation read after the delete: one record, for `a`.
        scan.read(&HistoryPage {
            records: vec![
                HistoryRecord::MessageDeleted {
                    id: HistoryId(11),
                    message_id: id("a"),
                },
                HistoryRecord::MessageAdded {
                    id: HistoryId(12),
                    message: MessageRef {
                        id: id("c"),
                        thread_id: ThreadId("t".into()),
                        labels: set(["INBOX"]),
                    },
                },
            ],
            history_id: HistoryId(12),
            next_page_token: None,
        });
        assert_eq!(scan.head(), Some(HistoryId(12)));
        assert_eq!(scan.deleted(&id("a")), Some(HistoryId(11)));
        assert_eq!(scan.deleted(&id("b")), None, "sent, not confirmed");
        assert_eq!(
            verdict(&mirror, &scan, "a", Scope::TrashOrSpam),
            Verdict::Gone { at: HistoryId(11) }
        );
    }
}
