//! Tests ported by name from Zero's `outgoing-mime.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890), as
//! `features/compose-mime.toml` maps them: one composer for drafts and sends
//! over structured input, the recipients returned beside the bytes.

use mailtide_mime::{
    Address, ComposeOptions, MessageId, OutgoingMessage, Purpose, compose, parse_addresses,
};

fn bare(email: &str) -> Address {
    Address {
        name: None,
        email: email.to_owned(),
    }
}

#[test]
fn recipients_are_collected_once_and_a_given_message_id_is_written() {
    let mut message = OutgoingMessage::new(bare("me@x.com"), 0);
    // Zero splits `Alice <a@x.com>` itself; here parse_addresses does.
    message.to = parse_addresses("Alice <a@x.com>, a@x.com");
    message.cc = vec![bare("c@x.com")];
    message.bcc = vec![bare("b@x.com")];
    message.subject = Some("Hi".into());
    message.html = Some("<p>Hello</p>".into());
    message.message_id = Some(MessageId::parse("<fixed@x.com>").expect("id"));
    message.references = MessageId::parse_list("r1@x r2@x").expect("ids");

    let composed = compose(&message, &ComposeOptions::default()).expect("compose");
    let raw = String::from_utf8(composed.raw).expect("ASCII");

    assert_eq!(composed.recipients, ["a@x.com", "c@x.com", "b@x.com"]);
    assert_eq!(composed.message_id.as_deref(), Some("<fixed@x.com>"));
    assert!(raw.contains("\r\nMessage-ID: <fixed@x.com>\r\n"), "{raw}");
    assert!(raw.contains("\r\nReferences: <r1@x> <r2@x>\r\n"), "{raw}");
    assert!(raw.starts_with("From: <me@x.com>\r\n"), "{raw}");
    // NOTE: Zero also drops the repeated address from To; we write the
    // headers as given and deduplicate only the recipients.
    assert!(
        raw.contains("\r\nTo: \"Alice\" <a@x.com>, <a@x.com>\r\n"),
        "{raw}"
    );
}

#[test]
fn a_draft_is_composed_without_recipients() {
    let mut message = OutgoingMessage::new(bare("me@x.com"), 0);
    message.subject = Some("Draft".into());
    message.html = Some("<p>x</p>".into());
    message.message_id = Some(MessageId::parse("<d@x.com>").expect("id"));
    let draft = ComposeOptions {
        purpose: Purpose::Draft,
        max_bytes: None,
    };

    let composed = compose(&message, &draft).expect("compose");
    let raw = String::from_utf8(composed.raw).expect("ASCII");

    assert!(raw.contains("\r\nSubject: Draft\r\n"), "{raw}");
    assert!(raw.contains("\r\nMessage-ID: <d@x.com>\r\n"), "{raw}");
    assert!(!raw.contains("\r\nTo:"), "{raw}");
    assert!(composed.recipients.is_empty());
}

#[test]
fn a_message_id_list_is_parsed_with_or_without_angle_brackets() {
    let ids = MessageId::parse_list("a@b  <c@d>").expect("ids");
    let shown: Vec<String> = ids.iter().map(ToString::to_string).collect();
    assert_eq!(shown.join(" "), "<a@b> <c@d>");
    let ids = MessageId::parse_list("<a@b><c@d>\r\n <e@f>").expect("ids");
    assert_eq!(
        ids.iter().map(MessageId::as_str).collect::<Vec<_>>(),
        ["a@b", "c@d", "e@f"]
    );
    assert!(MessageId::parse_list("").expect("ids").is_empty());
}
