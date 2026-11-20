//! The mirror's own header parse (`mailtide_gmail::headers::facts`, what the
//! import stores) reads back the ids compose wrote. Through Gmail the
//! Message-ID is replaced; In-Reply-To and References are kept as written.

use mailtide_gmail::headers::facts;
use mailtide_mime::{Address, ComposeOptions, MessageId, OutgoingMessage, compose};

#[test]
fn the_mirror_reads_back_the_ids_compose_wrote() {
    let mut message = OutgoingMessage::new(
        Address {
            name: Some("Ann".into()),
            email: "ann@example.com".into(),
        },
        0,
    );
    message.to = vec![Address {
        name: None,
        email: "bob@example.org".into(),
    }];
    message.text = Some("reply".into());
    message.message_id = Some(MessageId::parse("<m1@example.com>").expect("id"));
    message.in_reply_to = Some(MessageId::parse("<p9@example.org>").expect("id"));
    // Long enough that References folds over several lines.
    let references: Vec<String> = (0..12)
        .map(|n| format!("<CAF{n:02}-0123456789abcdef-thread@mail.example.org>"))
        .collect();
    message.references = references
        .iter()
        .map(|id| MessageId::parse(id).expect("id"))
        .collect();

    let composed = compose(&message, &ComposeOptions::default()).expect("compose");
    let raw = String::from_utf8_lossy(&composed.raw);
    let folded = raw
        .split("\r\nReferences: ")
        .nth(1)
        .and_then(|rest| rest.split("\r\nMIME-Version").next())
        .expect("References");
    assert!(folded.contains("\r\n "), "not folded: {folded:?}");

    let facts = facts(&composed.raw, None);
    assert_eq!(facts.rfc822_message_id, composed.message_id);
    assert_eq!(facts.in_reply_to.as_deref(), Some("<p9@example.org>"));
    assert_eq!(facts.references, references);
}
