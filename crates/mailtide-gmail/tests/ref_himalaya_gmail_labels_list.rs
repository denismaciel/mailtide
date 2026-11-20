//! Test ported from himalaya's `labels.list` command,
//! `src/gmail/labels/list.rs` (pimalaya/himalaya, MIT OR Apache-2.0,
//! commit a5623dcac615). Plan: `features/label-edit.toml`.
//!
//! The reference prints the listing as Gmail sent it; mailtide keeps what it
//! models of each label: the id, name and kind, both visibilities and the
//! colour, and reads a field it does not model as nothing.

use mailtide::gmail::{Label, LabelColor, LabelKind, LabelListVisibility, MessageListVisibility};
use mailtide::ids::LabelId;
use mailtide_gmail::decode;
use serde_json::json;

#[test]
fn a_label_listing_keeps_visibility_and_colour_of_each_label() {
    let answer = json!({"labels": [
        {"id": "INBOX", "name": "INBOX", "type": "system",
         "messageListVisibility": "hide", "labelListVisibility": "labelShow"},
        {"id": "L1", "name": "Work", "type": "user", "x-vendor": 1,
         "messageListVisibility": "show", "labelListVisibility": "labelShowIfUnread",
         "color": {"textColor": "#ffffff", "backgroundColor": "#16a766"}},
        {"id": "L2", "name": "Work/Old", "labelListVisibility": "labelHide",
         "messageListVisibility": "a value of a later API"},
    ]});
    assert_eq!(
        decode::labels_list(answer.to_string().as_bytes()),
        Ok(vec![
            Label {
                list_visibility: Some(LabelListVisibility::Show),
                message_visibility: Some(MessageListVisibility::Hide),
                ..Label::plain(LabelId("INBOX".into()), "INBOX", LabelKind::System)
            },
            Label {
                list_visibility: Some(LabelListVisibility::ShowIfUnread),
                message_visibility: Some(MessageListVisibility::Show),
                color: Some(LabelColor {
                    text: "#ffffff".into(),
                    background: "#16a766".into(),
                }),
                ..Label::plain(LabelId("L1".into()), "Work", LabelKind::User)
            },
            Label {
                list_visibility: Some(LabelListVisibility::Hide),
                ..Label::plain(LabelId("L2".into()), "Work/Old", LabelKind::User)
            },
        ])
    );
}
