//! Tests ported by name from mail-builder's `src/headers/content_type.rs`
//! (stalwartlabs/mail-builder, Apache-2.0 OR MIT, commit 2fc4a942754f), as
//! `features/compose-mime.toml` maps them: parameters, as attachment file
//! names written by `compose`. Where the library percent-encodes a control
//! character, compose refuses it.

use mail_parser::MessageParser;
use mailtide_mime::{
    Address, Attachment, ComposeError, ComposeOptions, OutgoingMessage, Purpose, Reason, compose,
};

fn with_file(name: &str) -> Result<String, ComposeError> {
    let mut message = OutgoingMessage::new(
        Address {
            name: None,
            email: "a@example.org".into(),
        },
        0,
    );
    message.text = Some("see attached".into());
    message.attachments = vec![Attachment {
        filename: Some(name.to_owned()),
        content_type: "application/pdf".into(),
        content_id: None,
        bytes: b"%PDF".to_vec(),
    }];
    let draft = ComposeOptions {
        purpose: Purpose::Draft,
        max_bytes: None,
    };
    compose(&message, &draft).map(|composed| String::from_utf8(composed.raw).expect("ASCII"))
}

/// The Content-Disposition value of the attachment, folding kept.
fn disposition(raw: &str) -> &str {
    let start = raw.find("Content-Disposition: ").expect("a disposition") + 21;
    let end = raw[start..]
        .find("\r\nContent-Transfer-Encoding")
        .expect("then the encoding")
        + start;
    &raw[start..end]
}

fn parsed_filename(raw: &str) -> String {
    let message = MessageParser::new().parse(raw.as_bytes()).expect("parse");
    let part = message.attachments().next().expect("an attachment");
    part.attachment_name().expect("a name").to_owned()
}

#[test]
fn a_non_ascii_file_name_is_an_rfc_2231_parameter() {
    let raw = with_file("Jahresabschlu\u{df}, 2024.pdf").expect("compose");
    assert_eq!(
        // Folded after `attachment;`: the header name is longer here.
        disposition(&raw).replace("\r\n ", " "),
        "attachment; filename*=UTF-8''Jahresabschlu%C3%9F%2C%202024.pdf"
    );
    assert_eq!(parsed_filename(&raw), "Jahresabschlu\u{df}, 2024.pdf");
}

#[test]
fn a_long_non_ascii_file_name_is_split_into_sections() {
    let name = "R\u{e9}union d'\u{e9}quipe: r\u{e9}sum\u{e9} des d\u{e9}cisions du trimestre et prochaines \u{e9}tapes du projet (version finale).pdf";
    let raw = with_file(name).expect("compose");
    let header = disposition(&raw);
    assert!(
        header.contains("filename*0*=UTF-8''R%C3%A9union"),
        "{header:?}"
    );
    assert!(header.contains(" filename*1*="), "{header:?}");
    assert!(!header.contains("%C3;"), "{header:?}");
    assert!(!header.contains("=?"), "{header:?}");
    for line in format!("Content-Disposition: {header}").split("\r\n") {
        assert!(line.len() <= 78, "{line:?}");
    }
    assert_eq!(parsed_filename(&raw), name);
}

// ASCII values over 78 characters use RFC 2231 quoted continuations (#51).
#[test]
fn a_long_ascii_file_name_uses_quoted_continuations() {
    for name in [
        "long ".repeat(200) + ".png",
        "quote\"and\\backslash ".repeat(8) + ".txt",
        "x".repeat(79),
    ] {
        let raw = with_file(&name).expect("compose");
        let header = disposition(&raw);
        assert!(header.contains("filename*0=\""), "{header:?}");
        assert!(header.contains(" filename*1=\""), "{header:?}");
        assert!(!header.contains("filename*0*"), "{header:?}");
        assert!(!header.contains("=?"), "{header:?}");
        for line in format!("Content-Disposition: {header}").split("\r\n") {
            assert!(line.len() <= 78, "{line:?}");
        }
        assert_eq!(parsed_filename(&raw), name);
    }

    let short = "y".repeat(50);
    let raw = with_file(&short).expect("compose");
    assert!(
        disposition(&raw).contains(&format!("filename=\"{short}\"")),
        "{raw}"
    );
}

#[test]
fn a_control_character_in_a_file_name_is_refused() {
    for name in ["a\r\nb\u{1}.pdf", "a\nb.pdf", "nul\0.pdf", "tab\t.pdf"] {
        assert_eq!(
            with_file(name),
            Err(ComposeError {
                field: "attachments",
                index: Some(0),
                reason: Reason::ControlCharacter
            }),
            "{name:?}"
        );
    }
}

#[test]
fn a_file_name_with_quotes_is_quoted_and_escaped() {
    let raw = with_file("report \"final\".pdf").expect("compose");
    assert!(
        disposition(&raw).contains(r#""report \"final\".pdf""#),
        "{raw}"
    );
    assert_eq!(parsed_filename(&raw), "report \"final\".pdf");
}
