//! The scoping rule of himalaya's JMAP delete, `src/jmap/backend.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615), as a pure
//! eligibility test. Plan: `features/purge.toml` (the suite is shared with
//! other plans).
//!
//! himalaya destroys only what the mailbox holds: an email another mailbox
//! holds is not in it. A purge reaches only what is in trash or spam.

use mailtide::gmail::HistoryPage;
use mailtide::ids::{AccountId, HistoryId, MessageId, ThreadId};
use mailtide::labels::set;
use mailtide::mirror::{AccountMirror, Content, MessageRecord};
use mailtide::purge::{PurgeScan, Refusal, Scope, Verdict};

#[test]
fn a_message_outside_trash_and_spam_is_not_eligible() {
    let mut mirror = AccountMirror::new(AccountId("jmap@example.org".into()), 1);
    for (id, labels) in [
        ("E1", &["TRASH"][..]),
        ("E2", &["INBOX", "Label_1"][..]),
        ("E3", &["TRASH", "Label_1"][..]),
    ] {
        mirror.messages.insert(
            MessageId(id.into()),
            MessageRecord {
                id: MessageId(id.into()),
                thread_id: ThreadId(format!("t-{id}")),
                labels: set(labels.iter().copied()),
                checkpoint: Some(HistoryId(5)),
                unverified: None,
                content: Content::Pending,
            },
        );
    }
    let mut scan = PurgeScan::new();
    scan.read(&HistoryPage {
        records: Vec::new(),
        history_id: HistoryId(5),
        next_page_token: None,
    });
    let verdict = |id: &str| {
        let id = MessageId(id.into());
        scan.verdict(&id, mirror.view(&id).as_ref(), Scope::TrashOrSpam)
    };
    assert_eq!(verdict("E1"), Verdict::Eligible);
    assert_eq!(verdict("E2"), Verdict::Refused(Refusal::NotInTrashOrSpam));
    assert_eq!(verdict("E3"), Verdict::Eligible);
}
