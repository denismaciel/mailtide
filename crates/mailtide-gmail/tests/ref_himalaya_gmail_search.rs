//! Tests ported from himalaya's Gmail query translator, `src/gmail/search.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615).
//!
//! himalaya translates its own filter tree into Gmail's `q`; mailtide takes
//! `q` as the host wrote it and keeps only the two value helpers, so the
//! translator's tests are ported onto `literal()` and `by_message_id()`.
//! The `from:`/`subject:` prefixes the reference adds are the host's here.

use mailtide_gmail::search::{by_message_id, literal};

#[test]
fn a_message_id_is_searched_bare_with_or_without_its_angle_brackets() {
    assert_eq!(
        by_message_id("id123@example.org").as_deref(),
        Some("rfc822msgid:id123@example.org")
    );
    assert_eq!(
        by_message_id("<id123@example.org>").as_deref(),
        Some("rfc822msgid:id123@example.org")
    );
    let negated = by_message_id("id123@example.org").map(|term| format!("-{term}"));
    assert_eq!(
        negated.as_deref(),
        Some("-rfc822msgid:id123@example.org"),
        "the host writes the negation"
    );
}

#[test]
fn a_value_with_spaces_is_quoted_and_a_bare_word_is_not() {
    assert_eq!(literal("john doe").as_deref(), Some("\"john doe\""));
    assert_eq!(
        literal("alice@example.org").as_deref(),
        Some("alice@example.org")
    );
}

#[test]
fn a_value_cannot_add_operators_or_groups() {
    assert_eq!(
        literal("rapport OR kangaroo").as_deref(),
        Some("\"rapport or kangaroo\"")
    );
    assert_eq!(
        literal("rapport} OR {kangaroo").as_deref(),
        Some("\"rapport or kangaroo\"")
    );
    assert_eq!(
        literal("alice) AND (dave").as_deref(),
        Some("\"alice and dave\"")
    );
    assert_eq!(
        literal("zebrafish\" OR kangaroo").as_deref(),
        Some("\"zebrafish or kangaroo\"")
    );
}

#[test]
fn a_value_that_looks_like_an_operator_is_quoted() {
    assert_eq!(literal("from:alice").as_deref(), Some("\"from:alice\""));
    assert_eq!(literal("-draft").as_deref(), Some("\"-draft\""));
}

#[test]
fn a_value_with_no_word_is_refused() {
    assert_eq!(literal("()"), None);
    assert_eq!(literal(" \"{}\" "), None);
}
