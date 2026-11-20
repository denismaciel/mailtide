//! Tests ported by behaviour from pimalaya core's
//! `mml/src/message/compiler.rs` (pimalaya/core, MIT, commit b3a9640a2b2e),
//! as `features/compose-mime.toml` maps them. MML compiles a text template
//! with mail-builder; compose takes the same headers as structured input.

use mail_parser::MessageParser;
use mailtide_mime::{Address, ComposeOptions, MessageId, OutgoingMessage, compose};

fn hugo() -> Address {
    Address {
        name: Some("Hugo Osvaldo Barrera".into()),
        email: "hugo@localhost".into(),
    }
}

fn with_id(id: &str) -> String {
    let mut message = OutgoingMessage::new(hugo(), 0);
    message.to = vec![hugo()];
    message.subject = Some("Blah".into());
    message.text = Some("Test message\n".into());
    message.message_id = Some(MessageId::parse(id).expect("id"));
    let raw = compose(&message, &ComposeOptions::default())
        .expect("compose")
        .raw;
    String::from_utf8(raw).expect("ASCII")
}

#[test]
fn non_ascii_from_to_and_subject_parse_back_unchanged() {
    let mut message = OutgoingMessage::new(
        Address {
            name: Some("Fr\u{0227}m".into()),
            email: "from@localhost".into(),
        },
        0,
    );
    message.to = vec![Address {
        name: Some("T\u{f3}".into()),
        email: "to@localhost".into(),
    }];
    message.subject = Some("Subj\u{ea}ct".into());
    message.text = Some("Hello, world!\n".into());
    message.message_id = Some(MessageId::parse("<id@localhost>").expect("id"));

    let raw = compose(&message, &ComposeOptions::default())
        .expect("compose")
        .raw;
    assert!(raw.is_ascii());
    let parsed = MessageParser::new().parse(&raw).expect("parse");
    let first = |list: Option<mail_parser::AddressList<'_>>| {
        let mailbox = list.and_then(|list| list.first()).expect("a mailbox");
        (
            mailbox.name().map(str::to_owned),
            mailbox.address().map(str::to_owned),
        )
    };
    assert_eq!(
        first(parsed.from()),
        (Some("Fr\u{0227}m".into()), Some("from@localhost".into()))
    );
    assert_eq!(
        first(parsed.to()),
        (Some("T\u{f3}".into()), Some("to@localhost".into()))
    );
    assert_eq!(parsed.subject(), Some("Subj\u{ea}ct"));
    assert_eq!(parsed.body_text(0).as_deref(), Some("Hello, world!\r\n"));
}

#[test]
fn a_message_id_given_in_angle_brackets_is_written_bracketed_once() {
    let raw = with_id("<bfb64e12-b7d4-474c-a658-8a221365f8ca@localhost>");
    assert!(
        raw.contains("\r\nMessage-ID: <bfb64e12-b7d4-474c-a658-8a221365f8ca@localhost>\r\n"),
        "{raw}"
    );
    assert!(!raw.contains("<<"));
}

#[test]
fn a_message_id_given_bare_gains_angle_brackets() {
    let raw = with_id("bfb64e12-b7d4-474c-a658-8a221365f8ca@localhost");
    assert!(
        raw.contains("\r\nMessage-ID: <bfb64e12-b7d4-474c-a658-8a221365f8ca@localhost>\r\n"),
        "{raw}"
    );
}
