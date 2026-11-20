//! himalaya's live write lifecycle, `tests/google.rs` (pimalaya/himalaya,
//! MIT OR Apache-2.0, commit a5623dcac615), against the fake Gmail instead
//! of Google. Plan: `features/purge.toml`.
//!
//! The reference trashes a message, then deletes it for good beside an
//! unknown id; the other live checks there need real credentials.

pub mod purge_host;

use mailtide::outbox::{Action, ActionRequest};
use mailtide::purge::PurgePolicy;
use purge_host::{Host, account, ids, names};

#[tokio::test]
async fn a_message_is_trashed_then_purged_and_an_unknown_id_is_reported() {
    let host = Host::new(PurgePolicy::TrashAndSpam);
    host.gmail().add("m", &["INBOX"]);
    host.sync().await;
    // In the inbox: not purged, not even sent.
    let outcome = host.purge(&["m"]).await.expect("purge");
    assert_eq!(outcome.refused.len(), 1);
    assert!(host.gmail().deletes().is_empty());
    // Trash it through an action, delivered by the run.
    host.runner
        .act(
            &account(),
            ActionRequest::Messages {
                ids: ids(&["m"]),
                action: Action::MoveToTrash,
            },
            purge_host::BUDGET,
        )
        .await
        .expect("act");
    assert!(host.gmail().messages["m"].0.contains(&"TRASH".to_owned()));
    let outcome = host.purge(&["m", "18c0ffee00000000"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["m"]);
    assert_eq!(outcome.refused.len(), 1);
    assert_eq!(outcome.refused[0].0.0, "18c0ffee00000000");
    assert_eq!(outcome.refused[0].1.code(), "not_in_mirror");
    assert!(!host.in_gmail("m"));
    assert_eq!(host.gmail().deletes(), ["DELETE m"]);
}
