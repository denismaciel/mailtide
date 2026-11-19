//! Message label state: label sets, checkpoints, effective state (SPEC §6.5, §6.6).
//!
//! Read, starred, archived, spam and trash are views of a label set, never
//! separate fields. The store keeps the *observed* labels (Gmail's state at the
//! message's checkpoint); the *effective* labels overlay pending intents
//! ([`crate::outbox`]) and are what users see.
//!
//! Zero counterpart: `lib/mail-label-store.ts`, `lib/mail-label-policy.ts`.

use std::collections::BTreeSet;

use crate::ids::{HistoryId, LabelId};
use crate::outbox::{Intent, LabelChange};

/// A set of Gmail label ids, ordered for determinism.
pub type LabelSet = BTreeSet<LabelId>;

/// Gmail system labels the engine reasons about.
pub mod system {
    /// In the inbox.
    pub const INBOX: &str = "INBOX";
    /// Unread.
    pub const UNREAD: &str = "UNREAD";
    /// Starred.
    pub const STARRED: &str = "STARRED";
    /// Marked important.
    pub const IMPORTANT: &str = "IMPORTANT";
    /// In spam.
    pub const SPAM: &str = "SPAM";
    /// In trash.
    pub const TRASH: &str = "TRASH";
    /// Sent by the account.
    pub const SENT: &str = "SENT";
    /// A draft. Drafts carry no other user-modifiable labels.
    pub const DRAFT: &str = "DRAFT";
    /// A Chat message.
    pub const CHAT: &str = "CHAT";
}

/// Whether an update observed at `incoming` may replace state stored at
/// `stored` (invariant I3). Equal checkpoints are accepted: label records are
/// absolute, so replaying them converges.
#[must_use]
pub fn accepts(stored: Option<HistoryId>, incoming: HistoryId) -> bool {
    stored.is_none_or(|stored| incoming >= stored)
}

/// Observed labels with pending intents applied, in version order (SPEC §7).
#[must_use]
pub fn effective(observed: &LabelSet, pending: &[Intent]) -> LabelSet {
    let mut labels = observed.clone();
    let mut ordered: Vec<&Intent> = pending.iter().filter(|intent| intent.overlays()).collect();
    ordered.sort_by_key(|intent| intent.version);
    for intent in ordered {
        match intent.change {
            LabelChange::Add => {
                labels.insert(intent.label.clone());
            }
            LabelChange::Remove => {
                labels.remove(&intent.label);
            }
        }
    }
    labels
}

/// A thread's labels: the union of its non-draft messages' effective labels.
#[must_use]
pub fn thread_labels<'a>(messages: impl IntoIterator<Item = &'a LabelSet>) -> LabelSet {
    messages
        .into_iter()
        .filter(|labels| !is_draft(labels))
        .flat_map(|labels| labels.iter().cloned())
        .collect()
}

/// Whether a label set reads as unread.
#[must_use]
pub fn is_unread(labels: &LabelSet) -> bool {
    has(labels, system::UNREAD)
}

/// Whether the message is a draft, whose ids churn on every save.
#[must_use]
pub fn is_draft(labels: &LabelSet) -> bool {
    has(labels, system::DRAFT)
}

/// Whether `labels` contains the label with id `id`.
#[must_use]
pub fn has(labels: &LabelSet, id: &str) -> bool {
    labels.iter().any(|label| label.0 == id)
}

/// A label set from ids, for tests and fixtures.
#[must_use]
pub fn set<'a>(ids: impl IntoIterator<Item = &'a str>) -> LabelSet {
    ids.into_iter().map(|id| LabelId(id.to_owned())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{MessageId, Timestamp};
    use crate::outbox::{IntentState, IntentVersion};

    fn intent(label: &str, change: LabelChange, version: u64, state: IntentState) -> Intent {
        Intent {
            message: MessageId("m".into()),
            label: LabelId(label.into()),
            change,
            version: IntentVersion(version),
            base_checkpoint: None,
            state,
        }
    }

    #[test]
    fn newer_or_equal_checkpoints_are_accepted() {
        assert!(accepts(None, HistoryId(5)));
        assert!(accepts(Some(HistoryId(5)), HistoryId(5)));
        assert!(accepts(Some(HistoryId(5)), HistoryId(6)));
        assert!(!accepts(Some(HistoryId(5)), HistoryId(4)));
    }

    #[test]
    fn pending_and_delivered_intents_overlay_observed_labels() {
        let observed = set(["INBOX", "UNREAD"]);
        let intents = [
            intent("INBOX", LabelChange::Remove, 1, IntentState::Pending),
            intent(
                "STARRED",
                LabelChange::Add,
                2,
                IntentState::Delivered {
                    at: Timestamp(0),
                    attempt: crate::jobs::AttemptId(0),
                },
            ),
        ];
        assert_eq!(effective(&observed, &intents), set(["STARRED", "UNREAD"]));
    }

    #[test]
    fn blocked_intents_do_not_overlay() {
        let observed = set(["INBOX"]);
        let blocked = intent(
            "INBOX",
            LabelChange::Remove,
            1,
            IntentState::Blocked(crate::retry::BlockingFailure {
                class: crate::retry::Blocking::Permanent,
                detail: String::new(),
            }),
        );
        assert_eq!(effective(&observed, &[blocked]), observed);
    }

    #[test]
    fn thread_labels_union_non_draft_messages() {
        let a = set(["INBOX"]);
        let b = set(["UNREAD", "STARRED"]);
        let draft = set(["DRAFT", "Label_1"]);
        assert_eq!(
            thread_labels([&a, &b, &draft]),
            set(["INBOX", "STARRED", "UNREAD"])
        );
    }
}
