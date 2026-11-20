//! Tests ported by name from mail-builder's `src/mime.rs`
//! (stalwartlabs/mail-builder, Apache-2.0 OR MIT, commit 2fc4a942754f), as
//! `features/compose-mime.toml` maps them: the multipart cases compose can
//! reach. compose passes every boundary in, so the library's random
//! boundary generator is never used.

use std::collections::HashSet;

use mail_parser::MessageParser;
use mailtide_mime::{Address, Attachment, ComposeOptions, MessageId, OutgoingMessage, compose};

fn message() -> OutgoingMessage {
    let mut message = OutgoingMessage::new(
        Address {
            name: None,
            email: "a@example.org".into(),
        },
        0,
    );
    message.to = vec![Address {
        name: None,
        email: "b@example.org".into(),
    }];
    message.message_id = Some(MessageId::parse("id@example.org").expect("id"));
    message
}

#[test]
fn the_boundary_compose_chose_is_written_and_delimits_the_parts() {
    let mut message = message();
    message.text = Some("hello".into());
    message.attachments = vec![Attachment {
        filename: None,
        content_type: "application/octet-stream".into(),
        content_id: None,
        bytes: vec![1, 2, 3],
    }];
    let raw = compose(&message, &ComposeOptions::default())
        .expect("compose")
        .raw;
    let raw = String::from_utf8(raw).expect("ASCII");
    let boundary = raw
        .split("boundary=\"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .expect("a boundary");
    assert!(
        raw.contains(&format!(
            "Content-Type: multipart/mixed; boundary=\"{boundary}\"\r\n\r\n--{boundary}\r\n"
        )),
        "{raw}"
    );
    assert_eq!(
        raw.matches(&format!("\r\n--{boundary}\r\n")).count(),
        2,
        "{raw}"
    );
    assert!(raw.ends_with(&format!("\r\n--{boundary}--\r\n")), "{raw}");
}

// Multipart headers are followed by exactly one empty line before the first boundary (#14).
#[test]
fn multipart_headers_are_followed_by_exactly_one_empty_line() {
    let mut message = message();
    message.text = Some("plain".into());
    message.html = Some("<p>html</p>".into());
    message.attachments = vec![Attachment {
        filename: Some("hello.txt".into()),
        content_type: "text/plain".into(),
        content_id: None,
        bytes: b"Hello".to_vec(),
    }];
    let raw = compose(&message, &ComposeOptions::default())
        .expect("compose")
        .raw;
    let message = String::from_utf8(raw).expect("ASCII");

    let boundaries = message
        .lines()
        .filter_map(|line| line.strip_prefix("--"))
        .filter(|boundary| !boundary.ends_with("--"))
        .collect::<HashSet<_>>();
    assert_eq!(boundaries.len(), 2, "{message:?}");
    for boundary in boundaries {
        let first = format!("\r\n--{boundary}\r\n");
        let (headers, _) = message.split_once(&first).expect("delimiter");
        assert!(headers.ends_with("\"\r\n"), "{message:?}");
        assert!(!headers.ends_with("\r\n\r\n"), "{message:?}");
    }

    let parsed = MessageParser::new()
        .parse(message.as_bytes())
        .expect("parse");
    assert_eq!(
        parsed
            .parts()
            .filter(mail_parser::MessagePart::is_text)
            .map(|part| part.raw_body())
            .collect::<Vec<_>>(),
        [&b"plain"[..], b"<p>html</p>", b"Hello"]
    );
}
