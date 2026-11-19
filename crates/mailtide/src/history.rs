//! Incremental sync from Gmail history (SPEC §6.1).
//!
//! One commit per page (review M2), with the cursor at the page's last record,
//! so a long catch-up is bounded and restartable. Whether a page is the last
//! one is read from the page itself, never passed in (review #13). Label
//! records for messages the store doesn't know make the store enqueue a
//! verification; deletions of unknown messages still tombstone. Empty pages
//! with repeated tokens back off instead of spinning.
//!
//! Records at the same history id are applied in order; an operation at a
//! checkpoint equal to the stored one is accepted (label records are absolute,
//! so replaying them converges).
//!
//! Zero counterpart: `lib/mail-history-store.ts`.

use crate::commit::{DeletionEvidence, MessageOp};
use crate::gmail::{HistoryPage, HistoryRecord};
use crate::ids::{HistoryId, PageToken};
use crate::labels::LabelSet;

/// What a page does to the cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorUpdate {
    /// The cursor stays (an empty page with more to come).
    Unchanged,
    /// The cursor advances to this id.
    AdvanceTo(HistoryId),
}

/// What one history page turns into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageFold {
    /// Conditional message operations, in record order.
    pub messages: Vec<MessageOp>,
    /// How many messages the page added.
    pub added: usize,
    /// Where the cursor may move after this page.
    pub cursor: CursorUpdate,
    /// The next page to read, if any.
    pub next_page: Option<PageToken>,
}

/// Fold a page's records into conditional operations (pure).
///
/// A page with a continuation token moves the cursor only to its last
/// record; the last page moves it to the history id Gmail reported.
#[must_use]
pub fn fold_page(page: &HistoryPage) -> PageFold {
    let messages = page.records.iter().map(record_op).collect();
    let added = page
        .records
        .iter()
        .filter(|record| matches!(record, HistoryRecord::MessageAdded { .. }))
        .count();
    let cursor = match &page.next_page_token {
        None => CursorUpdate::AdvanceTo(page.history_id),
        Some(_) => page
            .records
            .iter()
            .map(record_id)
            .max()
            .map_or(CursorUpdate::Unchanged, CursorUpdate::AdvanceTo),
    };
    PageFold {
        messages,
        added,
        cursor,
        next_page: page.next_page_token.clone(),
    }
}

/// One record's operation.
#[must_use]
pub fn record_op(record: &HistoryRecord) -> MessageOp {
    match record {
        HistoryRecord::MessageAdded { id, message } => MessageOp::Observe(message.observed_at(*id)),
        HistoryRecord::MessageDeleted { id, message_id } => MessageOp::Tombstone {
            id: message_id.clone(),
            evidence: DeletionEvidence::History { at: *id },
        },
        HistoryRecord::LabelsAdded {
            id,
            message_id,
            labels,
        } => MessageOp::PatchLabels {
            id: message_id.clone(),
            add: labels.clone(),
            remove: LabelSet::new(),
            checkpoint: *id,
        },
        HistoryRecord::LabelsRemoved {
            id,
            message_id,
            labels,
        } => MessageOp::PatchLabels {
            id: message_id.clone(),
            add: LabelSet::new(),
            remove: labels.clone(),
            checkpoint: *id,
        },
    }
}

/// A record's own history id.
#[must_use]
pub fn record_id(record: &HistoryRecord) -> HistoryId {
    match record {
        HistoryRecord::MessageAdded { id, .. }
        | HistoryRecord::MessageDeleted { id, .. }
        | HistoryRecord::LabelsAdded { id, .. }
        | HistoryRecord::LabelsRemoved { id, .. } => *id,
    }
}

/// Whether a page token repeats the previous one (an empty-page loop).
#[must_use]
pub fn is_repeated_token(previous: Option<&PageToken>, next: Option<&PageToken>) -> bool {
    matches!((previous, next), (Some(a), Some(b)) if a == b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gmail::MessageRef;
    use crate::ids::{MessageId, ThreadId};
    use crate::labels::set;

    fn added(id: u64, message: &str) -> HistoryRecord {
        HistoryRecord::MessageAdded {
            id: HistoryId(id),
            message: MessageRef {
                id: MessageId(message.into()),
                thread_id: ThreadId("t".into()),
                labels: set(["INBOX", "UNREAD"]),
            },
        }
    }

    fn page(records: Vec<HistoryRecord>, history_id: u64, more: bool) -> HistoryPage {
        HistoryPage {
            records,
            history_id: HistoryId(history_id),
            next_page_token: more.then(|| PageToken("next".into())),
        }
    }

    #[test]
    fn records_become_conditional_ops_in_order() {
        let records = vec![
            added(11, "a"),
            HistoryRecord::LabelsRemoved {
                id: HistoryId(12),
                message_id: MessageId("a".into()),
                labels: set(["UNREAD"]),
            },
            HistoryRecord::MessageDeleted {
                id: HistoryId(13),
                message_id: MessageId("b".into()),
            },
        ];
        let fold = fold_page(&page(records, 20, false));
        assert!(matches!(
            &fold.messages[0],
            MessageOp::Observe(observation) if observation.checkpoint == HistoryId(11)
        ));
        assert!(matches!(
            &fold.messages[1],
            MessageOp::PatchLabels { remove, checkpoint: HistoryId(12), .. } if *remove == set(["UNREAD"])
        ));
        assert!(matches!(
            &fold.messages[2],
            MessageOp::Tombstone {
                evidence: DeletionEvidence::History { at: HistoryId(13) },
                ..
            }
        ));
        assert_eq!(fold.added, 1);
    }

    #[test]
    fn last_page_moves_cursor_to_reported_history_id() {
        let fold = fold_page(&page(vec![added(11, "a")], 20, false));
        assert_eq!(fold.cursor, CursorUpdate::AdvanceTo(HistoryId(20)));
        assert_eq!(fold.next_page, None);
    }

    #[test]
    fn middle_page_moves_cursor_only_to_its_last_record() {
        let fold = fold_page(&page(vec![added(11, "a"), added(15, "b")], 20, true));
        assert_eq!(fold.cursor, CursorUpdate::AdvanceTo(HistoryId(15)));
    }

    #[test]
    fn empty_middle_page_keeps_the_cursor() {
        assert_eq!(
            fold_page(&page(Vec::new(), 20, true)).cursor,
            CursorUpdate::Unchanged
        );
    }

    #[test]
    fn repeated_tokens_are_detected() {
        let token = PageToken("x".into());
        assert!(is_repeated_token(Some(&token), Some(&token)));
        assert!(!is_repeated_token(None, Some(&token)));
        assert!(!is_repeated_token(Some(&token), None));
    }
}
