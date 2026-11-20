//! The delete-scoping tests of himalaya's JMAP backend, `src/jmap/backend.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615), ported by
//! behaviour. Plan: `features/purge.toml` (the suite is shared with other
//! plans).
//!
//! JMAP's `Email/set` destroy answers per id: what was destroyed, and a
//! reason for each id that was not. A purge answers the same way: ids the
//! mirror does not hold are refused and the others are purged.

pub mod purge_host;

use mailtide::purge::{PurgePolicy, Refusal};
use purge_host::{Host, error, names};

async fn mirrored(messages: &[(&str, &[&str])]) -> Host {
    let host = Host::new(PurgePolicy::TrashAndSpam);
    {
        let mut gmail = host.gmail();
        for (id, labels) in messages {
            gmail.add(id, labels);
        }
    }
    host.mirror_gmail().await;
    host
}

#[tokio::test]
async fn an_id_the_mirror_does_not_hold_is_refused_and_the_others_are_purged() {
    let host = mirrored(&[("E1", &["TRASH"]), ("E2", &["INBOX"])]).await;
    let outcome = host.purge(&["E1", "E2", "E9"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["E1"]);
    let refused: Vec<(&str, &str)> = outcome
        .refused
        .iter()
        .map(|(id, refusal)| (id.0.as_str(), refusal.code()))
        .collect();
    assert_eq!(
        refused,
        [("E2", "not_in_trash_or_spam"), ("E9", "not_in_mirror")]
    );
    assert_eq!(
        host.gmail().deletes(),
        ["DELETE E1"],
        "E9 never reaches Gmail"
    );
}

#[tokio::test]
async fn a_purge_applied_in_part_names_what_was_purged_and_what_was_refused() {
    let host = mirrored(&[
        ("E1", &["TRASH"]),
        ("E2", &["TRASH"]),
        ("Egone", &["TRASH"]),
    ])
    .await;
    // Gmail refuses one id and deletes the others.
    host.gmail().malformed.insert("Egone".into());
    let outcome = host.purge(&["E1", "E2", "Egone"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["E1", "E2"]);
    assert!(matches!(
        outcome.refused.as_slice(),
        [(id, Refusal::GmailRefused(_))] if id.0 == "Egone"
    ));
    assert!(outcome.sent.is_empty() && outcome.unsent.is_empty());
    // This fake applies none of the batch. Gmail's 400 gives the caller
    // no per-id proof of non-application.
    let host = mirrored(&[
        ("a", &["TRASH"]),
        ("b", &["TRASH"]),
        ("c", &["TRASH"]),
        ("d", &["TRASH"]),
        ("e", &["TRASH"]),
    ])
    .await;
    host.gmail().refuse_deletes = Some(error(400, "invalidArgument", "Too many ids"));
    let outcome = host.purge(&["a", "b", "c", "d", "e"]).await.expect("purge");
    assert!(outcome.purged.is_empty());
    assert_eq!(names(&outcome.sent), ["a", "b", "c", "d", "e"]);
    assert!(outcome.refused.is_empty() && outcome.unsent.is_empty());
    assert_eq!(host.gmail().deletes(), ["BATCH a,b,c,d,e"]);
    assert!(host.mirrored("a"));
}
