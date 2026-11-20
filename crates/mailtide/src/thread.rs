//! Thread views: a thread's messages folded into what a host shows (SPEC §4).
//!
//! A thread is derived, never stored: Gmail's `threadId` groups messages, and
//! [`view`] folds the members' [`MessageView`]s into one [`ThreadView`] with
//! one rule every store shares. Listing threads, sorting, paging and counting
//! them is the host's read model, fed by the change feed.
//!
//! Zero counterpart: `MAILBOX_SYNC.md` (thread state is the union over
//! non-draft messages with pending intent overlaid) and
//! `lib/thread-list-summary.ts` (the list row: latest message, unread, counts).

use crate::ids::{MessageId, ThreadId};
use crate::labels::{self, LabelSet};
use crate::mirror::MessageView;

/// A thread as hosts read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadView {
    /// The thread.
    pub id: ThreadId,
    /// Every member the mirror holds, drafts included: imported members by
    /// (`internal_date`, id), oldest first, then members awaiting import, by
    /// id.
    pub messages: Vec<MessageView>,
    /// The union of the non-draft members' effective labels. Gmail's own
    /// `threads.list` also lists a thread under a label only its draft
    /// carries; a host wanting that adds the drafts' labels from `messages`.
    pub labels: LabelSet,
    /// Whether some non-draft member is effectively unread.
    pub unread: bool,
    /// The newest imported non-draft member: it always has an envelope.
    pub latest: Option<MessageId>,
    /// Non-draft members.
    pub message_count: u32,
    /// Draft members.
    pub draft_count: u32,
    /// Non-draft members whose content is not imported yet: their place in
    /// `messages` is provisional.
    pub awaiting_import: u32,
    /// `From`, `To` and `Cc` of the imported non-draft members, oldest first,
    /// as the header facts hold them; one entry per address (see
    /// [`address_key`]), keeping the first spelling seen.
    pub participants: Vec<String>,
}

/// Fold a thread's members into its view. `None` when there are none (a
/// deleted thread and an unknown id read the same) and for the empty thread
/// id, which is never a thread. Members of other threads are ignored, and the
/// order members come in does not matter.
#[must_use]
pub fn view(id: &ThreadId, members: Vec<MessageView>) -> Option<ThreadView> {
    if id.0.is_empty() {
        return None;
    }
    let mut messages: Vec<MessageView> = members
        .into_iter()
        .filter(|member| &member.thread_id == id)
        .collect();
    if messages.is_empty() {
        return None;
    }
    messages.sort_by(|a, b| order_key(a).cmp(&order_key(b)));
    let non_draft: Vec<&MessageView> = messages
        .iter()
        .filter(|member| !labels::is_draft(&member.labels))
        .collect();
    let labels = labels::thread_labels(messages.iter().map(|member| &member.labels));
    let count = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
    let imported: Vec<&MessageView> = non_draft
        .iter()
        .copied()
        .filter(|member| member.content.is_some())
        .collect();
    Some(ThreadView {
        id: id.clone(),
        unread: labels::is_unread(&labels),
        labels,
        latest: imported.last().map(|member| member.id.clone()),
        message_count: count(non_draft.len()),
        draft_count: count(messages.len() - non_draft.len()),
        awaiting_import: count(non_draft.len() - imported.len()),
        participants: participants(&imported),
        messages,
    })
}

/// Where a member sorts: imported ones by date, then the rest; ties by id.
fn order_key(member: &MessageView) -> (bool, i64, &MessageId) {
    match &member.content {
        Some(content) => (false, content.internal_date.0, &member.id),
        None => (true, 0, &member.id),
    }
}

/// The participants of `members`, in order.
fn participants(members: &[&MessageView]) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for member in members {
        let Some(content) = &member.content else {
            continue;
        };
        let headers = &content.headers;
        for value in headers.from.iter().chain(&headers.to).chain(&headers.cc) {
            let key = address_key(value);
            if !key.is_empty() && seen.insert(key) {
                out.push(value.trim().to_owned());
            }
        }
    }
    out
}

/// What two header values are compared by: the text inside the last `<…>`,
/// else the whole value, trimmed and lower-cased.
#[must_use]
pub fn address_key(value: &str) -> String {
    let address = value
        .rfind('<')
        .and_then(|start| {
            let rest = &value[start + 1..];
            rest.find('>').map(|end| &rest[..end])
        })
        .unwrap_or(value);
    address.trim().to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gmail::HeaderFacts;
    use crate::ids::{BlobRef, HistoryId, LabelId, Timestamp};
    use crate::labels::set;
    use crate::mirror::StoredContent;
    use crate::outbox::{Intent, IntentState, IntentVersion, LabelChange};

    fn t() -> ThreadId {
        ThreadId("t".into())
    }

    /// A member awaiting import.
    fn pending(id: &str, labels: &[&str]) -> MessageView {
        MessageView {
            id: MessageId(id.into()),
            thread_id: t(),
            labels: set(labels.iter().copied()),
            observed_labels: set(labels.iter().copied()),
            checkpoint: Some(HistoryId(1)),
            unverified: false,
            content: None,
            pending_actions: 0,
            blocked_actions: 0,
        }
    }

    /// An imported member dated `date`, from `id@x`.
    fn imported(id: &str, labels: &[&str], date: i64) -> MessageView {
        with_headers(
            id,
            labels,
            date,
            HeaderFacts {
                from: Some(format!("{id}@x")),
                ..HeaderFacts::default()
            },
        )
    }

    fn with_headers(id: &str, labels: &[&str], date: i64, headers: HeaderFacts) -> MessageView {
        MessageView {
            content: Some(StoredContent {
                blob: BlobRef(format!("{id}.eml")),
                size: 10,
                internal_date: Timestamp(date),
                headers,
                facts: None,
            }),
            ..pending(id, labels)
        }
    }

    fn ids(view: &ThreadView) -> Vec<&str> {
        view.messages.iter().map(|m| m.id.0.as_str()).collect()
    }

    fn fold(members: Vec<MessageView>) -> ThreadView {
        view(&t(), members).expect("a thread")
    }

    #[test]
    fn messages_are_ordered_by_internal_date_then_id() {
        let thread = fold(vec![
            imported("c", &["INBOX"], 20),
            imported("b", &["INBOX"], 10),
            imported("a", &["INBOX"], 20),
            imported("z", &["INBOX"], 5),
        ]);
        assert_eq!(ids(&thread), ["z", "b", "a", "c"]);
        assert_eq!(thread.latest, Some(MessageId("c".into())));
    }

    #[test]
    fn messages_awaiting_import_follow_the_dated_ones_and_are_counted() {
        let thread = fold(vec![
            pending("b", &["INBOX"]),
            imported("y", &["INBOX"], 30),
            pending("a", &["INBOX"]),
            imported("x", &["INBOX"], 10),
        ]);
        assert_eq!(ids(&thread), ["x", "y", "a", "b"]);
        assert_eq!(thread.awaiting_import, 2);
        assert_eq!(thread.message_count, 4);
    }

    #[test]
    fn latest_is_the_newest_imported_non_draft_message() {
        let thread = fold(vec![
            imported("old", &["INBOX"], 10),
            imported("mid", &["INBOX"], 20),
            imported("draft", &["DRAFT"], 30),
            pending("new", &["INBOX", "UNREAD"]),
        ]);
        assert_eq!(thread.latest, Some(MessageId("mid".into())));
        assert!(thread.unread);
        let only_pending = fold(vec![pending("new", &["INBOX"])]);
        assert_eq!(only_pending.latest, None);
    }

    #[test]
    fn a_thread_of_drafts_only_has_no_labels_no_latest_and_a_draft_count() {
        let thread = fold(vec![imported("d", &["DRAFT"], 10)]);
        assert_eq!(thread.message_count, 0);
        assert_eq!(thread.draft_count, 1);
        assert!(thread.labels.is_empty());
        assert!(!thread.unread);
        assert_eq!(thread.latest, None);
        assert!(thread.participants.is_empty());
        assert_eq!(ids(&thread), ["d"]);
    }

    #[test]
    fn a_drafts_labels_never_reach_the_thread() {
        let thread = fold(vec![
            imported("m", &["INBOX"], 10),
            imported("d", &["DRAFT", "STARRED", "UNREAD"], 20),
        ]);
        assert_eq!(thread.labels, set(["INBOX"]));
        assert!(!thread.unread);
        assert_eq!(ids(&thread), ["m", "d"]);
        assert_eq!((thread.message_count, thread.draft_count), (1, 1));
    }

    #[test]
    fn a_blocked_action_no_longer_shows_in_the_thread_labels() {
        let intent = |label: &str, change, version, state| Intent {
            message: MessageId("m".into()),
            label: LabelId(label.into()),
            change,
            version: IntentVersion(version),
            base_checkpoint: None,
            state,
        };
        let observed = set(["INBOX", "UNREAD"]);
        let member = |intents: &[Intent]| MessageView {
            labels: labels::effective(&observed, intents),
            ..imported("m", &["INBOX", "UNREAD"], 10)
        };
        let pending = [intent(
            "UNREAD",
            LabelChange::Remove,
            1,
            IntentState::Pending,
        )];
        let delivered = [intent(
            "UNREAD",
            LabelChange::Remove,
            1,
            IntentState::Delivered {
                at: Timestamp(0),
                attempt: crate::jobs::AttemptId(1),
            },
        )];
        let blocked = [intent(
            "UNREAD",
            LabelChange::Remove,
            1,
            IntentState::Blocked(crate::retry::BlockingFailure {
                class: crate::retry::Blocking::Permanent,
                detail: String::new(),
            }),
        )];
        let other = imported("o", &["INBOX"], 5);
        for (intents, unread) in [(&pending, false), (&delivered, false), (&blocked, true)] {
            let thread = fold(vec![member(intents), other.clone()]);
            assert_eq!(thread.unread, unread, "{:?}", intents[0].state);
            assert_eq!(labels::is_unread(&thread.labels), unread);
        }
    }

    #[test]
    fn trashed_and_spam_members_stay_in_the_view() {
        let thread = fold(vec![
            imported("a", &["INBOX"], 10),
            imported("b", &["TRASH"], 20),
            imported("c", &["SPAM", "UNREAD"], 30),
        ]);
        assert_eq!(thread.message_count, 3);
        assert_eq!(thread.labels, set(["INBOX", "SPAM", "TRASH", "UNREAD"]));
        assert!(thread.unread);
        assert_eq!(thread.latest, Some(MessageId("c".into())));
    }

    #[test]
    fn participants_keep_first_appearance_order_and_are_deduplicated_by_address() {
        let first = with_headers(
            "a",
            &["INBOX"],
            10,
            HeaderFacts {
                from: Some("Ann <ann@x>".into()),
                to: vec!["me@x".into(), "Bob <bob@x>".into()],
                cc: vec!["cat@x".into()],
                ..HeaderFacts::default()
            },
        );
        let reply = with_headers(
            "b",
            &["SENT"],
            20,
            HeaderFacts {
                from: Some("Me <me@x>".into()),
                to: vec!["ann@x".into()],
                cc: vec!["Dan <dan@x>".into(), "bob@x".into()],
                ..HeaderFacts::default()
            },
        );
        // Given newest first: the order comes from the dates, not the input.
        let thread = fold(vec![reply, first]);
        assert_eq!(
            thread.participants,
            ["Ann <ann@x>", "me@x", "Bob <bob@x>", "cat@x", "Dan <dan@x>"]
        );
    }

    #[test]
    fn an_address_is_compared_without_its_display_name_or_case() {
        assert_eq!(address_key("\"Doe, Jane\" <Jane@X>"), "jane@x");
        assert_eq!(address_key("jane@x"), "jane@x");
        assert_eq!(address_key(" JANE@X "), "jane@x");
        assert_eq!(address_key("Not <an> address <real@x>"), "real@x");
        assert_eq!(
            address_key("undisclosed-recipients:;"),
            "undisclosed-recipients:;"
        );
        let thread = fold(vec![
            with_headers(
                "a",
                &["INBOX"],
                10,
                HeaderFacts {
                    from: Some("\"Doe, Jane\" <Jane@X>".into()),
                    ..HeaderFacts::default()
                },
            ),
            with_headers(
                "b",
                &["INBOX"],
                20,
                HeaderFacts {
                    from: Some("jane@x".into()),
                    ..HeaderFacts::default()
                },
            ),
        ]);
        assert_eq!(thread.participants, ["\"Doe, Jane\" <Jane@X>"]);
    }

    #[test]
    fn participants_ignore_drafts_and_messages_awaiting_import() {
        let draft = with_headers(
            "d",
            &["DRAFT"],
            30,
            HeaderFacts {
                from: Some("me@x".into()),
                to: vec!["secret@x".into()],
                ..HeaderFacts::default()
            },
        );
        let thread = fold(vec![
            imported("a", &["INBOX"], 10),
            pending("b", &["INBOX"]),
            draft,
        ]);
        assert_eq!(thread.participants, ["a@x"]);
    }

    #[test]
    fn the_view_does_not_depend_on_the_order_members_are_given_in() {
        let members = vec![
            imported("a", &["INBOX"], 10),
            imported("b", &["UNREAD"], 10),
            imported("c", &["DRAFT"], 5),
            pending("d", &["STARRED"]),
            pending("e", &["INBOX"]),
        ];
        let expected = fold(members.clone());
        // Every permutation, by Heap's algorithm.
        let mut items = members;
        let n = items.len();
        let mut c = vec![0; n];
        let mut i = 0;
        while i < n {
            if c[i] < i {
                items.swap(if i % 2 == 0 { 0 } else { c[i] }, i);
                assert_eq!(fold(items.clone()), expected);
                c[i] += 1;
                i = 0;
            } else {
                c[i] = 0;
                i += 1;
            }
        }
    }

    #[test]
    fn no_members_is_no_thread() {
        assert_eq!(view(&t(), Vec::new()), None);
        // Members of another thread are not this thread's.
        let mut other = imported("a", &["INBOX"], 10);
        other.thread_id = ThreadId("other".into());
        assert_eq!(view(&t(), vec![other]), None);
        // The empty id is never a thread.
        let mut orphan = imported("b", &["INBOX"], 10);
        orphan.thread_id = ThreadId(String::new());
        assert_eq!(view(&ThreadId(String::new()), vec![orphan]), None);
    }
}
