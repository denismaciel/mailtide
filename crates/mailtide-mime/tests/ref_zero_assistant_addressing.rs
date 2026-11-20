//! Cases ported by name from Zero's `apps/server/src/lib/assistant-addressing.test.ts`
//! (MIT, commit 4b642dfd9890). The pure plan suggests addresses for the host to edit.

use mailtide_mime::{Address, reply_plan};

fn emails(list: &[Address]) -> Vec<&str> {
    list.iter().map(|address| address.email.as_str()).collect()
}

#[test]
fn a_reply_answers_reply_to_and_copies_nobody() {
    let plan = reply_plan(
        b"From: author@example.com\r\nReply-To: Support <help@example.com>\r\nTo: me@example.com\r\nCc: copy@example.com\r\nBcc: hidden@example.com\r\n\r\nOriginal",
        &["me@example.com"],
        false,
    )
    .unwrap();
    assert_eq!(emails(&plan.to), ["help@example.com"]);
    assert_eq!(plan.to[0].name.as_deref(), Some("Support"));
    assert!(plan.cc.is_empty());
}

#[test]
fn reply_all_keeps_other_to_and_cc_once_ignoring_case() {
    let plan = reply_plan(
        b"From: author@example.com\r\nTo: me@example.com, another@example.com\r\nCc: ANOTHER@example.com, copy@example.com, COPY@example.com\r\nBcc: hidden@example.com\r\n\r\nOriginal",
        &["ME@example.com"],
        true,
    )
    .unwrap();
    assert_eq!(
        emails(&plan.to),
        ["author@example.com", "another@example.com"]
    );
    assert_eq!(emails(&plan.cc), ["copy@example.com"]);
}

#[test]
fn a_reply_to_ones_own_message_answers_its_recipients() {
    let plan = reply_plan(
        b"From: alias@example.com\r\nReply-To: other@example.com\r\nTo: saved@example.com\r\nCc: copy@example.com\r\n\r\nOriginal",
        &["me@example.com", "ALIAS@example.com"],
        false,
    )
    .unwrap();
    assert_eq!(emails(&plan.to), ["saved@example.com"]);
    assert!(plan.cc.is_empty());
}

#[test]
fn a_reply_plan_never_copies_ones_own_addresses() {
    let plan = reply_plan(
        b"From: author@example.com\r\nTo: alias@example.com\r\nCc: ME@example.com, reader@example.com\r\nReply-To: another@example.com, alias@example.com\r\n\r\nOriginal",
        &[" me@example.com ", "alias@example.com"],
        true,
    )
    .unwrap();
    assert_eq!(emails(&plan.to), ["another@example.com"]);
    assert_eq!(emails(&plan.cc), ["reader@example.com"]);
}

#[test]
fn a_reply_plan_with_nobody_left_has_an_empty_to() {
    let plan = reply_plan(
        b"From: me@example.com\r\nTo: alias@example.com\r\nBcc: hidden@example.com\r\n\r\nOriginal",
        &["me@example.com", "alias@example.com"],
        true,
    )
    .unwrap();
    assert!(plan.to.is_empty());
    assert!(plan.cc.is_empty());
}
