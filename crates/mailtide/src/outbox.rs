//! Pending user actions: intents, delivery, confirmation (SPEC §7, phase 6).
//!
//! One versioned intent per (message, label). An undelivered intent is always
//! delivered: Gmail label changes are absolute, so delivery is idempotent
//! (review M3). At most one unresolved delivery per (message, label), so
//! retries can't reorder (review M4). An intent clears only when history or a
//! `get` issued after delivery shows its effect (invariant I7). Drafts are
//! never targets; INBOX/SPAM/TRASH are mutually exclusive in one change.
//!
//! Zero counterpart: the outbox in `lib/mail-label-store.ts`.

use std::collections::BTreeMap;

use crate::ids::{HistoryId, LabelId, MessageId, ThreadId, Timestamp};
use crate::labels::{LabelSet, is_draft, system};

/// Add or remove one label.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum LabelChange {
    /// Add the label.
    Add,
    /// Remove the label.
    Remove,
}

/// The version of an intent; a new toggle on the same (message, label) gets a
/// higher one.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct IntentVersion(pub u64);

/// A pending change to one label on one message.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Intent {
    /// The message.
    pub message: MessageId,
    /// The label.
    pub label: LabelId,
    /// Add or remove.
    pub change: LabelChange,
    /// Its version.
    pub version: IntentVersion,
    /// The message's checkpoint when the user acted.
    pub base_checkpoint: Option<HistoryId>,
    /// Where it is.
    pub state: IntentState,
}

impl Intent {
    /// Whether the intent is applied on top of observed labels.
    #[must_use]
    pub fn overlays(&self) -> bool {
        match self.state {
            IntentState::Pending | IntentState::Delivered { .. } => true,
            IntentState::Blocked(_) => false,
        }
    }
}

/// An intent's lifecycle, as users see it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IntentState {
    /// Admitted, not yet sent.
    Pending,
    /// Sent; Gmail accepted it at `at`; awaiting confirmation by a fetch
    /// claimed after `attempt` (the delivery's claim).
    Delivered {
        /// When Gmail accepted it.
        at: Timestamp,
        /// The delivery's job claim.
        attempt: crate::jobs::AttemptId,
    },
    /// Gmail can't apply it until an operator acts.
    Blocked(crate::retry::BlockingFailure),
}

/// What the user asked for, before expansion into intents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionRequest {
    /// Act on these messages.
    Messages {
        /// The messages.
        ids: Vec<MessageId>,
        /// The action.
        action: Action,
    },
    /// Act on the non-draft messages a thread has when the action is admitted.
    Thread {
        /// The thread.
        id: ThreadId,
        /// The action.
        action: Action,
    },
}

/// A user action, with every policy explicit (review #14).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Mark read or unread.
    SetRead(ReadState),
    /// Star or unstar.
    SetStar(StarState),
    /// Mark important or not.
    SetImportance(Importance),
    /// Remove from the inbox.
    Archive,
    /// Move into the inbox (out of trash or spam, if there).
    MoveToInbox,
    /// Move to trash.
    MoveToTrash,
    /// Restore from trash.
    RestoreFromTrash(RestoreDestination),
    /// Report as spam.
    MarkSpam,
    /// Not spam.
    MarkNotSpam(RestoreDestination),
    /// Add and remove labels.
    ChangeLabels(LabelDelta),
}

/// Labels to add and remove, valid by construction: no read-only label, at
/// most one location added, and no label both added and removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelDelta {
    add: Vec<LabelId>,
    remove: Vec<LabelId>,
}

impl LabelDelta {
    /// Validate a change, as the caller gave it.
    pub fn new(add: Vec<LabelId>, remove: Vec<LabelId>) -> Result<Self, AdmissionError> {
        for label in add.iter().chain(&remove) {
            if READ_ONLY.contains(&label.0.as_str()) {
                return Err(AdmissionError::ReadOnlyLabel(label.clone()));
            }
        }
        if let Some(both) = add.iter().find(|label| remove.contains(label)) {
            return Err(AdmissionError::AddedAndRemoved(both.clone()));
        }
        let locations = add
            .iter()
            .filter(|label| LOCATIONS.contains(&label.0.as_str()))
            .count();
        if locations > 1 {
            return Err(AdmissionError::ConflictingLocations);
        }
        Ok(Self { add, remove })
    }

    /// Labels to add.
    #[must_use]
    pub fn add(&self) -> &[LabelId] {
        &self.add
    }

    /// Labels to remove.
    #[must_use]
    pub fn remove(&self) -> &[LabelId] {
        &self.remove
    }
}

/// Read or unread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadState {
    /// Read.
    Read,
    /// Unread.
    Unread,
}

/// Starred or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StarState {
    /// Starred.
    Starred,
    /// Not starred.
    Unstarred,
}

/// Important or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Importance {
    /// Important.
    Important,
    /// Not important.
    Normal,
}

/// Where a restored message goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreDestination {
    /// Into the inbox.
    Inbox,
    /// Into the archive (no INBOX).
    Archive,
}

/// Why an action can't be admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionError {
    /// Drafts can't carry other labels.
    DraftTarget(MessageId),
    /// `SENT`, `DRAFT` and `CHAT` can't be applied.
    ReadOnlyLabel(LabelId),
    /// Only one of INBOX, SPAM, TRASH may be added at once.
    ConflictingLocations,
    /// The same label is both added and removed.
    AddedAndRemoved(LabelId),
}

impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DraftTarget(id) => write!(f, "message {} is a draft", id.0),
            Self::ReadOnlyLabel(label) => write!(f, "label {} can't be added or removed", label.0),
            Self::ConflictingLocations => {
                f.write_str("only one of INBOX, SPAM and TRASH may be added at once")
            }
            Self::AddedAndRemoved(label) => {
                write!(f, "label {} is both added and removed", label.0)
            }
        }
    }
}

impl std::error::Error for AdmissionError {}

/// Label changes an action implies, including implicit ones: trash and spam
/// remove INBOX; restoring goes where the action says. Every implied change is
/// kept even when the mirror already shows it: the mirror may lag Gmail, and
/// Gmail's label changes are absolute, so sending one that is already true is
/// harmless while dropping one could lose the user's action. `labels` (the
/// effective labels) only decide whether the message may be acted on.
pub fn expand(
    message: &MessageId,
    action: &Action,
    labels: &LabelSet,
) -> Result<Vec<(LabelId, LabelChange)>, AdmissionError> {
    if is_draft(labels) {
        return Err(AdmissionError::DraftTarget(message.clone()));
    }
    let wanted: Vec<(&str, LabelChange)> = match action {
        Action::SetRead(ReadState::Read) => vec![(system::UNREAD, LabelChange::Remove)],
        Action::SetRead(ReadState::Unread) => vec![(system::UNREAD, LabelChange::Add)],
        Action::SetStar(StarState::Starred) => vec![(system::STARRED, LabelChange::Add)],
        Action::SetStar(StarState::Unstarred) => vec![(system::STARRED, LabelChange::Remove)],
        Action::SetImportance(Importance::Important) => vec![(system::IMPORTANT, LabelChange::Add)],
        Action::SetImportance(Importance::Normal) => vec![(system::IMPORTANT, LabelChange::Remove)],
        Action::Archive => vec![(system::INBOX, LabelChange::Remove)],
        Action::MoveToInbox => vec![
            (system::INBOX, LabelChange::Add),
            (system::TRASH, LabelChange::Remove),
            (system::SPAM, LabelChange::Remove),
        ],
        Action::MoveToTrash => vec![
            (system::TRASH, LabelChange::Add),
            (system::INBOX, LabelChange::Remove),
        ],
        Action::RestoreFromTrash(destination) => restore(system::TRASH, *destination),
        Action::MarkSpam => vec![
            (system::SPAM, LabelChange::Add),
            (system::INBOX, LabelChange::Remove),
        ],
        Action::MarkNotSpam(destination) => restore(system::SPAM, *destination),
        Action::ChangeLabels(delta) => {
            let (add, remove) = (delta.add(), delta.remove());
            let mut changes: Vec<(LabelId, LabelChange)> = add
                .iter()
                .map(|label| (label.clone(), LabelChange::Add))
                .chain(
                    remove
                        .iter()
                        .map(|label| (label.clone(), LabelChange::Remove)),
                )
                .collect();
            // Like MoveToTrash/MarkSpam: trash and spam leave the inbox.
            let leaves_inbox = add
                .iter()
                .any(|label| label.0 == system::TRASH || label.0 == system::SPAM);
            let inbox = LabelId(system::INBOX.to_owned());
            if leaves_inbox && !remove.contains(&inbox) {
                changes.push((inbox, LabelChange::Remove));
            }
            return Ok(changes);
        }
    };
    Ok(wanted
        .into_iter()
        .map(|(label, change)| (LabelId(label.to_owned()), change))
        .collect())
}

/// Labels Gmail doesn't let clients set or clear.
const READ_ONLY: [&str; 3] = [system::SENT, system::DRAFT, system::CHAT];

/// Labels that are locations: only one may be added at a time.
const LOCATIONS: [&str; 3] = [system::INBOX, system::SPAM, system::TRASH];

/// The most label ids one `modify`/`batchModify` add or remove list may name.
pub const MAX_LABELS_PER_CALL: usize = 100;

/// The most messages one `batchModify` call may name.
pub const MAX_BATCH_MODIFY: usize = 1_000;

fn restore(
    from: &'static str,
    destination: RestoreDestination,
) -> Vec<(&'static str, LabelChange)> {
    match destination {
        RestoreDestination::Inbox => vec![
            (from, LabelChange::Remove),
            (system::INBOX, LabelChange::Add),
        ],
        RestoreDestination::Archive => vec![(from, LabelChange::Remove)],
    }
}

/// One delivery: messages that need exactly the same label changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyGroup {
    /// At most 1,000 messages; a group under 10 uses `messages.modify`.
    pub messages: Vec<MessageId>,
    /// Labels to add.
    pub add: Vec<LabelId>,
    /// Labels to remove.
    pub remove: Vec<LabelId>,
    /// The intent versions this delivery carries, for fencing acks (I8).
    pub versions: Vec<Delivery>,
}

/// One intent version carried by a delivery, for fencing acknowledgments.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Delivery {
    /// The message.
    pub message: MessageId,
    /// The label.
    pub label: LabelId,
    /// The intent version sent.
    pub version: IntentVersion,
}

/// Group deliverable intents into the fewest Gmail calls (pure).
#[must_use]
pub fn plan_delivery(intents: &[Intent]) -> Vec<ModifyGroup> {
    let mut per_message: BTreeMap<&MessageId, MessageChange> = BTreeMap::new();
    for intent in intents
        .iter()
        .filter(|intent| intent.state == IntentState::Pending)
    {
        let entry = per_message.entry(&intent.message).or_default();
        match intent.change {
            LabelChange::Add => entry.labels.add.push(intent.label.clone()),
            LabelChange::Remove => entry.labels.remove.push(intent.label.clone()),
        }
        entry.deliveries.push(Delivery {
            message: intent.message.clone(),
            label: intent.label.clone(),
            version: intent.version,
        });
    }
    // Gmail takes at most MAX_LABELS_PER_CALL ids per add/remove list: a
    // message with more changes is split, the rest go in later calls.
    let per_message: Vec<(&MessageId, MessageChange)> = per_message
        .into_iter()
        .flat_map(|(message, change)| {
            change
                .deliveries
                .chunks(MAX_LABELS_PER_CALL)
                .map(|chunk| {
                    let mut part = MessageChange::default();
                    for delivery in chunk {
                        if change.labels.add.contains(&delivery.label) {
                            part.labels.add.push(delivery.label.clone());
                        } else {
                            part.labels.remove.push(delivery.label.clone());
                        }
                        part.deliveries.push(delivery.clone());
                    }
                    (message, part)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    // Messages needing identical changes share a call.
    let mut by_change: BTreeMap<LabelChanges, Vec<(MessageId, Vec<Delivery>)>> = BTreeMap::new();
    for (message, mut change) in per_message {
        change.labels.add.sort();
        change.labels.remove.sort();
        by_change
            .entry(change.labels)
            .or_default()
            .push((message.clone(), change.deliveries));
    }
    by_change
        .into_iter()
        .flat_map(|(labels, messages)| {
            messages
                .chunks(MAX_BATCH_MODIFY)
                .map(|chunk| ModifyGroup {
                    messages: chunk.iter().map(|(message, _)| message.clone()).collect(),
                    add: labels.add.clone(),
                    remove: labels.remove.clone(),
                    versions: chunk
                        .iter()
                        .flat_map(|(_, deliveries)| deliveries.clone())
                        .collect(),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The labels one delivery adds and removes.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct LabelChanges {
    add: Vec<LabelId>,
    remove: Vec<LabelId>,
}

/// One message's pending changes and the versions they carry.
#[derive(Debug, Default)]
struct MessageChange {
    labels: LabelChanges,
    deliveries: Vec<Delivery>,
}

/// Whether observed labels confirm an intent.
#[must_use]
pub fn confirms(intent: &Intent, observed: &LabelSet) -> bool {
    match intent.change {
        LabelChange::Add => observed.contains(&intent.label),
        LabelChange::Remove => !observed.contains(&intent.label),
    }
}

/// An intent change inside a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentOp {
    /// Admit a user action: expand it against the messages' effective
    /// labels into versioned intents, and enqueue their delivery. Targets
    /// that can't take it (drafts, unknown messages) are skipped.
    Request(ActionRequest),
    /// Gmail accepted these versions at `at`; they now await confirmation by
    /// a fetch claimed after `by`, the delivery job's claim.
    Delivered {
        /// The versions delivered.
        versions: Vec<Delivery>,
        /// When.
        at: Timestamp,
        /// The claim of the job that delivered them.
        by: crate::jobs::AttemptId,
    },
    /// Gmail refused these versions for good.
    Block {
        /// The versions.
        versions: Vec<Delivery>,
        /// Why.
        failure: crate::retry::BlockingFailure,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::set;

    fn m(id: &str) -> MessageId {
        MessageId(id.into())
    }

    fn label(id: &str) -> LabelId {
        LabelId(id.into())
    }

    fn pending(message: &str, label_id: &str, change: LabelChange, version: u64) -> Intent {
        Intent {
            message: m(message),
            label: label(label_id),
            change,
            version: IntentVersion(version),
            base_checkpoint: None,
            state: IntentState::Pending,
        }
    }

    #[test]
    fn trash_removes_inbox_and_restore_re_adds_it() {
        let inbox = set(["INBOX"]);
        assert_eq!(
            expand(&m("a"), &Action::MoveToTrash, &inbox),
            Ok(vec![
                (label("TRASH"), LabelChange::Add),
                (label("INBOX"), LabelChange::Remove)
            ])
        );
        let trashed = set(["TRASH"]);
        assert_eq!(
            expand(
                &m("a"),
                &Action::RestoreFromTrash(RestoreDestination::Inbox),
                &trashed
            ),
            Ok(vec![
                (label("TRASH"), LabelChange::Remove),
                (label("INBOX"), LabelChange::Add)
            ])
        );
    }

    #[test]
    fn restoring_to_the_archive_does_not_add_inbox() {
        assert_eq!(
            expand(
                &m("a"),
                &Action::RestoreFromTrash(RestoreDestination::Archive),
                &set(["TRASH"])
            ),
            Ok(vec![(label("TRASH"), LabelChange::Remove)])
        );
    }

    #[test]
    fn implied_changes_are_kept_even_when_the_mirror_shows_them() {
        // The mirror may lag Gmail: archiving what looks archived still sends.
        assert_eq!(
            expand(&m("a"), &Action::SetRead(ReadState::Read), &set(["INBOX"])),
            Ok(vec![(label("UNREAD"), LabelChange::Remove)])
        );
        assert_eq!(
            expand(&m("a"), &Action::MarkSpam, &LabelSet::new()),
            Ok(vec![
                (label("SPAM"), LabelChange::Add),
                (label("INBOX"), LabelChange::Remove)
            ])
        );
    }

    #[test]
    fn drafts_and_read_only_labels_are_rejected() {
        assert_eq!(
            expand(
                &m("d"),
                &Action::SetStar(StarState::Starred),
                &set(["DRAFT"])
            ),
            Err(AdmissionError::DraftTarget(m("d")))
        );
        assert_eq!(
            LabelDelta::new(vec![label("SENT")], Vec::new()),
            Err(AdmissionError::ReadOnlyLabel(label("SENT")))
        );
        assert_eq!(
            LabelDelta::new(vec![label("SPAM"), label("TRASH")], Vec::new()),
            Err(AdmissionError::ConflictingLocations)
        );
        assert_eq!(
            LabelDelta::new(vec![label("Label_1")], vec![label("Label_1")]),
            Err(AdmissionError::AddedAndRemoved(label("Label_1")))
        );
    }

    #[test]
    fn identical_changes_share_one_group() {
        let intents = [
            pending("a", "INBOX", LabelChange::Remove, 1),
            pending("b", "INBOX", LabelChange::Remove, 2),
            pending("c", "STARRED", LabelChange::Add, 3),
        ];
        let groups = plan_delivery(&intents);
        assert_eq!(groups.len(), 2);
        let archive = groups
            .iter()
            .find(|g| g.remove == vec![label("INBOX")])
            .unwrap();
        assert_eq!(archive.messages, vec![m("a"), m("b")]);
        assert_eq!(archive.versions.len(), 2);
    }

    #[test]
    fn only_pending_intents_are_delivered() {
        let mut delivered = pending("a", "INBOX", LabelChange::Remove, 1);
        delivered.state = IntentState::Delivered {
            at: Timestamp(0),
            attempt: crate::jobs::AttemptId(0),
        };
        assert!(plan_delivery(&[delivered]).is_empty());
    }

    #[test]
    fn confirmation_matches_observed_labels() {
        let archive = pending("a", "INBOX", LabelChange::Remove, 1);
        assert!(confirms(&archive, &set(["UNREAD"])));
        assert!(!confirms(&archive, &set(["INBOX"])));
    }
}
