//! Test ported from himalaya's Gmail backend, `src/gmail/backend.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615). Plan:
//! `features/label-edit.toml`: the pure sublabel rule, here; the duplicate
//! create is a driver test (`crates/mailtide-io/tests`).

use mailtide::gmail::{Label, LabelKind};
use mailtide::ids::LabelId;
use mailtide::label_names::sublabel_renames;

fn label(id: &str, name: &str) -> Label {
    Label::plain(LabelId(id.into()), name, LabelKind::User)
}

#[test]
fn a_rename_moves_the_sublabels_and_only_them() {
    let labels = [
        label("1", "A/Sub"),
        label("2", "A/Sub/Deep"),
        label("3", "A/Sub/Deep/Er"),
        label("4", "A/Subway"),
        label("5", "B/A/Sub/X"),
    ];
    assert_eq!(
        sublabel_renames(&labels, "A/Sub", "A/Sub2"),
        [
            (LabelId("2".into()), "A/Sub2/Deep".to_owned()),
            (LabelId("3".into()), "A/Sub2/Deep/Er".to_owned()),
        ]
    );
}
