//! Test ported from himalaya's `labels.get` command,
//! `src/gmail/labels/get.rs` (pimalaya/himalaya, MIT OR Apache-2.0,
//! commit a5623dcac615). Plan: `features/label-edit.toml`.
//!
//! The reference prints Gmail's label whole, a property it does not model
//! kept; mailtide decodes it once, at the boundary, into a [`Label`].

use mailtide::gmail::{Label, LabelKind};
use mailtide::ids::LabelId;
use mailtide_gmail::labels;
use serde_json::json;

#[test]
fn a_label_answer_decodes_with_unknown_fields_and_without_colour_or_visibility() {
    let answer = json!({"id": "L1", "name": "Work", "type": "user", "x-vendor": 1});
    assert_eq!(
        labels::label(answer.to_string().as_bytes()),
        Ok(Label::plain(LabelId("L1".into()), "Work", LabelKind::User))
    );
}
