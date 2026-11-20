//! Test ported from himalaya's Gmail client, `src/gmail/client.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615). Plan:
//! `features/label-edit.toml`.
//!
//! himalaya answers a label it cannot resolve from its name index, without a
//! request. mailtide addresses labels by id and asks Gmail, whose 404 is
//! `LabelError::NotFound`; nothing is recorded.

pub mod label_support;

use label_support::{BUDGET, account, harness, labels, sync};
use mailtide::gmail::{LabelListVisibility, LabelPatch};
use mailtide::ids::LabelId;
use mailtide_io::labels::{LabelError, Sublabels};
use mailtide_io::runner::Access;

#[tokio::test]
async fn a_label_gmail_does_not_know_is_label_not_found() {
    let runner = harness(Access::ReadWrite);
    sync(&runner).await;
    let before = labels(&runner).await;
    let patch = LabelPatch {
        list_visibility: Some(LabelListVisibility::Hide),
        ..LabelPatch::default()
    };
    let missing = runner
        .update_label(
            &account(),
            &LabelId("Label_404".into()),
            patch,
            Sublabels::Follow,
            BUDGET,
        )
        .await;
    assert_eq!(missing, Err(LabelError::NotFound));
    assert_eq!(missing.map_err(|e| e.code()), Err("not_found"));
    assert_eq!(labels(&runner).await, before);
}
