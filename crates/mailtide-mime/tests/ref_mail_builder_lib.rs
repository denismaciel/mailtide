//! Tests ported by name from mail-builder's `src/lib.rs`
//! (stalwartlabs/mail-builder, Apache-2.0 OR MIT, commit 2fc4a942754f), as
//! `features/compose-mime.toml` maps them: whole messages, run through
//! `compose` and `mailtide_mime::parse` rather than the library's builder,
//! so a dependency upgrade that changes our bytes fails here.

use mail_parser::MessageParser;
use mailtide_mime::{
    Address, Attachment, ComposeOptions, MessageId, OutgoingMessage, compose, parse,
};

fn named(name: &str, email: &str) -> Address {
    Address {
        name: Some(name.to_owned()),
        email: email.to_owned(),
    }
}

fn file(content_type: &str, name: Option<&str>, bytes: &[u8]) -> Attachment {
    Attachment {
        filename: name.map(str::to_owned),
        content_type: content_type.to_owned(),
        content_id: None,
        bytes: bytes.to_vec(),
    }
}

#[test]
fn the_fullest_layout_parses_back_part_for_part() {
    let mut message = OutgoingMessage::new(named("John Doe", "john@doe.com"), 0);
    message.to = vec![named("Jane Doe", "jane@doe.com")];
    message.subject = Some("RFC 8621 Section 4.1.4 test".into());
    message.message_id = Some(MessageId::parse("<nested@doe.com>").expect("id"));
    message.text = Some("Part B contents go here...".into());
    message.html = Some("Part E contents go here...".into());
    message.inline = vec![Attachment {
        content_id: Some("part-f".into()),
        ..file("image/jpeg", None, b"Part F contents go here...")
    }];
    message.attachments = vec![
        file(
            "image/jpeg",
            Some("image_G.jpg"),
            b"Part G contents go here...",
        ),
        file("application/x-excel", None, b"Part H contents go here..."),
        file("x-message/rfc822", None, b"Part J contents go here..."),
    ];

    let raw = compose(&message, &ComposeOptions::default())
        .expect("compose")
        .raw;
    let parsed = MessageParser::new().parse(&raw).expect("parse");

    // mixed > [alternative > [text, related > [html, image]], G, H, J]
    let shape: Vec<(String, usize)> = parsed
        .parts()
        .map(|part| {
            let content_type = part.content_type().map_or_else(String::new, |ct| {
                format!("{}/{}", ct.ctype(), ct.subtype().unwrap_or_default())
            });
            (content_type, part.children().len())
        })
        .collect();
    let expected = [
        ("multipart/mixed", 4),
        ("multipart/alternative", 2),
        ("text/plain", 0),
        ("multipart/related", 2),
        ("text/html", 0),
        ("image/jpeg", 0),
        ("image/jpeg", 0),
        ("application/x-excel", 0),
        ("x-message/rfc822", 0),
    ];
    assert_eq!(
        shape,
        expected
            .iter()
            .map(|(ct, n)| ((*ct).to_owned(), *n))
            .collect::<Vec<_>>()
    );
    let ours = parse(&raw).expect("parse");
    assert_eq!(ours.text, message.text);
    assert_eq!(ours.html, message.html);
    let mut parts = message.inline.clone();
    parts.extend(message.attachments.iter().cloned());
    assert_eq!(ours.attachments, parts);
}

#[test]
fn non_ascii_names_bodies_and_file_names_parse_back() {
    let to = [
        ("Antoine de Saint-Exup\u{e9}ry", "antoine@exupery.com"),
        (
            "\u{c548}\u{b155}\u{d558}\u{c138}\u{c694} \u{c138}\u{acc4}",
            "test@test.com",
        ),
        ("Xin ch\u{e0}o", "addr@addr.com"),
    ];
    let bcc = [
        ("ASCII recipient", "addr1@addr7.com"),
        (
            "\u{30cf}\u{30ed}\u{30fc}\u{30fb}\u{30ef}\u{30fc}\u{30eb}\u{30c9}",
            "addr2@addr6.com",
        ),
        ("\u{e1}\u{e9}\u{ed}\u{f3}\u{fa}", "addr3@addr5.com"),
        (
            "\u{393}\u{3b5}\u{3b9}\u{3ac} \u{3c3}\u{3bf}\u{3c5} \u{39a}\u{3cc}\u{3c3}\u{3bc}\u{3b5}",
            "addr4@addr4.com",
        ),
        (
            "\u{5e9}\u{5dc}\u{5d5}\u{5dd} \u{5e2}\u{5d5}\u{5dc}\u{5dd}",
            "addr5@addr3.com",
        ),
        (
            "\u{a1}El \u{f1}and\u{fa} comi\u{f3} \u{f1}oquis!",
            "addr6@addr2.com",
        ),
    ];
    let mut message = OutgoingMessage::new(named("John Doe", "john@doe.com"), 0);
    message.to = to.iter().map(|(n, e)| named(n, e)).collect();
    message.bcc = bcc.iter().map(|(n, e)| named(n, e)).collect();
    message.subject = Some("Hello world!".into());
    message.text = Some("Hello, world!\r\n".repeat(20));
    message.html = Some("<p>\u{a1}Hola Mundo!</p>".repeat(20));
    message.inline = vec![Attachment {
        content_id: Some("image".into()),
        ..file("image/png", None, &[0, 1, 2, 3, 4, 5])
    }];
    let korean = "\u{c548}\u{b155}\u{d558}\u{c138}\u{c694} \u{c138}\u{acc4}".repeat(20);
    let japanese = "\u{30cf}\u{30ed}\u{30fc}\u{30fb}\u{30ef}\u{30fc}\u{30eb}\u{30c9}";
    message.attachments = vec![
        file("text/plain", Some("my f\u{ed}le.txt"), korean.as_bytes()),
        file("text/plain", Some(japanese), japanese.repeat(20).as_bytes()),
    ];

    let raw = compose(&message, &ComposeOptions::default())
        .expect("compose")
        .raw;
    assert!(raw.is_ascii());
    let parsed = MessageParser::new().parse(&raw).expect("parse");
    let names = |list: Option<mail_parser::AddressList<'_>>| -> Vec<(String, String)> {
        list.map(|list| {
            list.mailboxes()
                .map(|m| {
                    (
                        m.name().unwrap_or_default().to_owned(),
                        m.address().unwrap_or_default().to_owned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
    };
    let owned = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, e)| ((*n).to_owned(), (*e).to_owned()))
            .collect()
    };
    assert_eq!(names(parsed.to()), owned(&to));
    assert_eq!(names(parsed.bcc()), owned(&bcc));
    let ours = parse(&raw).expect("parse");
    assert_eq!(ours.text, message.text);
    assert_eq!(ours.html, message.html);
    let mut parts = message.inline.clone();
    parts.extend(message.attachments.iter().cloned());
    assert_eq!(ours.attachments, parts);
}
