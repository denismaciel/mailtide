//! Test ported from himalaya's mailbox lifecycle, `tests/mailbox.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615). Plan:
//! `features/label-edit.toml`.
//!
//! The reference runs its binary against a Maildir; this runs the same
//! lifecycle through the runner against the fake Gmail, where nesting is a
//! name: the definitions and the feed say what happened.

pub mod label_support;

use label_support::{BUDGET, account, feed_since, harness, head, labels, sync, user_names};
use mailtide::gmail::{LabelListVisibility, LabelPatch, MessageListVisibility, NewLabel};
use mailtide::label_names;
use mailtide::revision::Change;
use mailtide_io::labels::{SublabelDeletion, Sublabels};
use mailtide_io::runner::Access;

#[tokio::test]
async fn a_label_is_created_under_a_parent_renamed_there_and_deleted() {
    let runner = harness(Access::ReadWrite);
    let archive = runner.transport().mailbox().create_label("Archive");
    sync(&runner).await;
    let before = head(&runner).await;

    let created = runner
        .create_label(
            &account(),
            NewLabel {
                name: label_names::child("Archive", "2025"),
                list_visibility: LabelListVisibility::Show,
                message_visibility: MessageListVisibility::Show,
                color: None,
            },
            BUDGET,
        )
        .await
        .expect("created");
    assert_eq!(created.name, "Archive/2025");
    let parents = label_names::parents(&labels(&runner).await);
    assert_eq!(
        parents.get(&created.id).map(|id| id.0.as_str()),
        Some(archive.as_str())
    );

    let renamed = runner
        .update_label(
            &account(),
            &created.id,
            LabelPatch {
                name: Some(label_names::renamed(&created.name, "2026")),
                ..LabelPatch::default()
            },
            Sublabels::Follow,
            BUDGET,
        )
        .await
        .expect("renamed")
        .label;
    assert_eq!(
        (renamed.id.clone(), renamed.name.as_str()),
        (created.id.clone(), "Archive/2026")
    );
    assert_eq!(
        user_names(&runner).await.into_values().collect::<Vec<_>>(),
        ["Archive", "Archive/2026"]
    );

    let deletion = runner
        .delete_label(&account(), &created.id, SublabelDeletion::Refuse, BUDGET)
        .await
        .expect("deleted");
    assert_eq!(deletion.deleted, [created.id]);
    assert_eq!(
        user_names(&runner).await.into_values().collect::<Vec<_>>(),
        ["Archive"]
    );
    assert_eq!(
        feed_since(&runner, before).await,
        vec![Change::LabelDefinitionsChanged; 3],
        "each write is in the feed"
    );
}
