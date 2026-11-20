//! Cases from pimalaya core's `email/src/email/message/template/reply/mod.rs`
//! (MIT, commit b3a9640a2b2e). The host owns template layout.

use mailtide_mime::reply_plan;

fn recipients(headers: &str, own: &[&str]) -> Vec<String> {
    let raw = format!("{headers}\r\nOriginal");
    reply_plan(raw.as_bytes(), own, false)
        .unwrap()
        .to
        .into_iter()
        .map(|address| address.email)
        .collect()
}

#[test]
fn a_reply_to_ones_own_message_goes_to_its_to_recipients() {
    assert_eq!(
        recipients(
            "From: me@example.com\r\nTo: them@example.com\r\n",
            &["me@example.com"],
        ),
        ["them@example.com"],
    );
}

#[test]
fn a_message_with_sender_and_no_from_is_answered_to_sender() {
    assert_eq!(
        recipients("Sender: list@example.com\r\n", &[]),
        ["list@example.com"],
    );
}

#[test]
fn a_message_without_reply_to_is_answered_to_from() {
    assert_eq!(
        recipients(
            "From: author@example.com\r\nSender: list@example.com\r\n",
            &[]
        ),
        ["author@example.com"],
    );
}

#[test]
fn reply_to_wins_over_from() {
    assert_eq!(
        recipients(
            "From: author@example.com\r\nReply-To: reply@example.com\r\n",
            &[]
        ),
        ["reply@example.com"],
    );
}

#[test]
fn every_from_address_is_answered() {
    assert_eq!(
        recipients(
            "From: Alice <alice@example.com>, Bob <bob@example.com>\r\n",
            &[]
        ),
        ["alice@example.com", "bob@example.com"],
    );
}

#[test]
fn a_chain_of_re_prefixes_is_trimmed_to_the_subject() {
    let plan = reply_plan(b"Subject: RE: re : Re: Project\r\n\r\nOriginal", &[], false).unwrap();
    assert_eq!(plan.subject, "Re: Project");
}
