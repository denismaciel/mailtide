//! Test ported by behaviour from Zero's IMAP sync engine,
//! `apps/server/src/lib/imap/sync.test.ts` (Mail-0/Zero, MIT, commit
//! 4b642dfd9890). Plan: `features/label-edit.toml`; the suite's other tests
//! are IMAP-only.
//!
//! Zero forgets a folder that disappears from LIST, with what it knew of
//! its messages. In Gmail a folder is a label: one missing from a later
//! listing leaves the definitions and every mirrored message.

pub mod label_support;

use std::time::Duration;

use label_support::{harness, labels, sync, view};
use mailtide::labels::set;
use mailtide_io::runner::Access;

#[tokio::test]
async fn a_label_missing_from_a_later_listing_leaves_the_definitions_and_every_message() {
    let runner = harness(Access::ReadWrite);
    {
        let mut mailbox = runner.transport().mailbox();
        let projects = mailbox.create_label("Projects");
        mailbox.add("p", &[&projects]);
    }
    sync(&runner).await;
    assert_eq!(view(&runner, "p").await.labels, set(["Label_1"]));
    runner.transport().mailbox().delete_label("Label_1", false);
    // The next refresh lists the labels again.
    runner.advance(Duration::from_mins(16));
    sync(&runner).await;
    assert!(labels(&runner).await.iter().all(|l| l.name != "Projects"));
    assert_eq!(view(&runner, "p").await.labels, set([]));
}
