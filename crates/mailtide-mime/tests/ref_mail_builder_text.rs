//! Tests ported by name from mail-builder's `src/headers/text.rs`
//! (stalwartlabs/mail-builder, Apache-2.0 OR MIT, commit 2fc4a942754f), as
//! `features/compose-mime.toml` maps them: the subject, through `compose`.
//! Where the library encodes a control character, compose refuses it.

use mail_parser::MessageParser;
use mailtide_mime::{
    Address, ComposeError, ComposeOptions, OutgoingMessage, Purpose, Reason, compose,
};

fn with_subject(subject: &str) -> Result<String, ComposeError> {
    let mut message = OutgoingMessage::new(
        Address {
            name: None,
            email: "a@example.org".into(),
        },
        0,
    );
    message.subject = Some(subject.to_owned());
    let draft = ComposeOptions {
        purpose: Purpose::Draft,
        max_bytes: None,
    };
    compose(&message, &draft).map(|composed| String::from_utf8(composed.raw).expect("ASCII"))
}

/// The Subject header as written, folding kept.
fn subject_header(raw: &str) -> &str {
    let start = raw.find("\r\nSubject: ").expect("a subject") + 2;
    let end = raw[start..].find("\r\nDate: ").expect("then a date") + start;
    &raw[start..end]
}

fn round_trip(raw: &str) -> String {
    MessageParser::new()
        .parse(raw.as_bytes())
        .expect("parse")
        .subject()
        .expect("a subject")
        .to_owned()
}

#[test]
fn a_long_q_encoded_subject_never_splits_a_character() {
    let input = format!(
        "{}{}x{}",
        "x".repeat(20_000),
        "δ".repeat(600),
        "δ".repeat(600)
    );
    let raw = with_subject(&input).expect("compose");
    let header = subject_header(&raw);
    for line in header.split("\r\n") {
        assert!(line.trim().len() <= 78, "line of {}: {line}", line.len());
    }
    assert_eq!(round_trip(&raw), input);
    assert!(header.starts_with("Subject: =?utf-8?Q?xxx"), "{header}");
    assert!(!header.contains("CE?="));
    assert!(!header.contains("=?utf-8?Q?=B4"));
}

#[test]
fn a_long_base64_encoded_subject_never_splits_a_character() {
    let input = format!("{}x{}", "δ".repeat(600), "δ".repeat(600));
    let raw = with_subject(&input).expect("compose");
    let header = subject_header(&raw);
    for line in header.split("\r\n") {
        assert!(line.trim().len() <= 78, "line of {}: {line}", line.len());
    }
    assert_eq!(round_trip(&raw), input);
    assert!(
        header.starts_with("Subject: =?utf-8?B?zrTOtM60zrTOtM60"),
        "{header}"
    );
    assert!(!header.contains("zg==?="));
    assert!(!header.contains("?B?tM60zr"));
}

#[test]
fn a_control_character_in_a_subject_is_refused() {
    for input in [
        "ring \x07 the bell",
        "nul \0 byte",
        "\x1b[1mbold",
        "tab\tstays",
        "a\r\nBcc: e@x",
    ] {
        assert_eq!(
            with_subject(input),
            Err(ComposeError {
                field: "subject",
                index: None,
                reason: Reason::ControlCharacter
            }),
            "{input:?}"
        );
    }
    let raw = with_subject("tab  stays").expect("compose");
    assert_eq!(subject_header(&raw), "Subject: tab  stays");
}
