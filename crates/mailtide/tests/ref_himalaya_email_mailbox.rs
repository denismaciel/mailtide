//! Tests ported from himalaya's mailbox name rules, `src/email/mailbox.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615). Plan:
//! `features/label-edit.toml`. Gmail's delimiter is always `/`.

use mailtide::label_names::{InvalidName, child, renamed, validate, validate_leaf};

#[test]
fn a_label_name_is_not_blank_and_holds_no_control_character() {
    assert_eq!(validate("Été 2026"), Ok(()));
    assert_eq!(validate("Work/2026"), Ok(()), "a nested name");
    assert_eq!(validate(""), Err(InvalidName::Blank));
    assert_eq!(validate(" "), Err(InvalidName::Blank));
    assert_eq!(validate("a\nb"), Err(InvalidName::ControlCharacter));
    assert_eq!(validate("a\u{7f}"), Err(InvalidName::ControlCharacter));
}

#[test]
fn a_child_name_holds_no_slash() {
    assert_eq!(validate_leaf("a.b"), Ok(()));
    assert_eq!(validate_leaf("a/b"), Err(InvalidName::Delimiter));
    assert_eq!(validate_leaf(" "), Err(InvalidName::Blank));
}

#[test]
fn a_renamed_leaf_keeps_its_parent_path() {
    assert_eq!(child("Archive", "2026"), "Archive/2026");
    assert_eq!(renamed("Archive/2025", "2026"), "Archive/2026");
    assert_eq!(renamed("A/B/Old", "New"), "A/B/New");
    assert_eq!(renamed("Top", "Renamed"), "Renamed");
}
