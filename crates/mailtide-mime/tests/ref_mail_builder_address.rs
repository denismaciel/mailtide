//! Tests ported by name from mail-builder's `src/headers/address.rs`
//! (stalwartlabs/mail-builder, Apache-2.0 OR MIT, commit 2fc4a942754f), as
//! `features/compose-mime.toml` maps them: address headers written by
//! `compose`. Where the library strips or encodes a control character,
//! compose refuses the message. Address groups are not an input.

use mail_parser::MessageParser;
use mailtide_mime::{Address, ComposeError, ComposeOptions, OutgoingMessage, Reason, compose};

type Parsed = Vec<(Option<String>, Option<String>)>;

fn address(name: Option<&str>, email: &str) -> Address {
    Address {
        name: name.map(str::to_owned),
        email: email.to_owned(),
    }
}

fn message(to: Vec<Address>) -> OutgoingMessage {
    let mut message = OutgoingMessage::new(address(None, "sender@example.com"), 0);
    message.to = to;
    message.text = Some("b".into());
    message
}

fn written(to: Vec<Address>) -> Result<String, ComposeError> {
    compose(&message(to), &ComposeOptions::default())
        .map(|composed| String::from_utf8(composed.raw).expect("ASCII"))
}

/// The To header as written, folding kept, without its name.
fn to_header(raw: &str) -> &str {
    let start = raw.find("\r\nTo: ").expect("a To") + 6;
    let end = raw[start..].find("\r\nDate: ").expect("then a date") + start;
    &raw[start..end]
}

fn parse(raw: &str) -> Parsed {
    let message = MessageParser::new().parse(raw.as_bytes()).expect("parse");
    message
        .to()
        .expect("To")
        .mailboxes()
        .map(|addr| {
            (
                addr.name().map(str::to_string),
                addr.address().map(str::to_string),
            )
        })
        .collect()
}

fn one(name: &str, email: &str) -> Parsed {
    vec![(Some(name.to_owned()), Some(email.to_owned()))]
}

#[test]
fn an_encoded_display_name_keeps_its_lines_within_76_characters() {
    let mut bcc = message(vec![]);
    bcc.bcc = vec![address(
        Some("\u{a1}El \u{f1}and\u{fa} comi\u{f3} \u{f1}oquis!"),
        "addr1@example.com",
    )];
    let raw = String::from_utf8(
        compose(&bcc, &ComposeOptions::default())
            .expect("compose")
            .raw,
    )
    .expect("ASCII");
    let start = raw.find("Bcc: ").expect("Bcc");
    let end = raw[start..].find("\r\nDate: ").expect("then a date") + start;
    let header = &raw[start..end];
    for line in header.split("\r\n") {
        assert!(
            !line.contains("=?") || line.len() <= 76,
            "line of {}: {line:?}",
            line.len()
        );
    }
    assert_eq!(
        header,
        "Bcc: =?utf-8?B?wqFFbCDDsWFuZMO6IGNvbWnDsyDDsW9xdWlzIQ==?=\r\n <addr1@example.com>"
    );
}

#[test]
fn a_line_break_in_an_address_is_refused_and_injects_nothing() {
    let evil = "victim@example.com>\r\nBcc: injected@evil.test\r\nX-Injected: <x";
    let long = format!("{evil}{}", "a".repeat(80));
    for (to, index) in [
        (vec![address(None, evil)], 0),
        (vec![address(Some("Name"), evil)], 0),
        (
            vec![address(None, "ok@example.com"), address(None, &long)],
            1,
        ),
        (vec![address(None, "a\0b\u{7f}@x.test\t")], 0),
    ] {
        assert_eq!(
            written(to),
            Err(ComposeError {
                field: "to",
                index: Some(index),
                reason: Reason::ControlCharacter
            })
        );
    }
}

// Control characters in display names are refused, never written raw (#53).
#[test]
fn a_control_character_in_a_display_name_is_refused() {
    for name in [
        "with \x07 bel",
        "nul \0 byte",
        "esc\x1b[0m",
        "\x01",
        "line\r\nBcc: e@x",
    ] {
        assert_eq!(
            written(vec![address(Some(name), "a@example.com")]),
            Err(ComposeError {
                field: "to",
                index: Some(0),
                reason: Reason::ControlCharacter
            }),
            "{name:?}"
        );
    }
}

#[test]
fn a_non_ascii_display_name_is_an_unquoted_encoded_word() {
    let raw =
        written(vec![address(Some("Anna M\u{fc}ller"), "anna@example.com")]).expect("compose");
    let header = to_header(&raw);
    assert!(
        !header.contains("\"=?"),
        "quoted encoded-word in {header:?}"
    );
    assert!(header.contains("=?utf-8?"), "not encoded in {header:?}");
    assert_eq!(parse(&raw), one("Anna M\u{fc}ller", "anna@example.com"));
}

#[test]
fn a_base64_encoded_display_name_is_not_quoted() {
    let name = "\u{394}\u{3bf}\u{3ba}\u{3b9}\u{3bc}\u{3ae}, \u{395}\u{3c4}\u{3b1}\u{3b9}\u{3c1}\u{3b5}\u{3af}\u{3b1}";
    let raw = written(vec![address(Some(name), "info@example.org")]).expect("compose");
    let header = to_header(&raw);
    assert!(header.contains("=?utf-8?B?"), "not base64 in {header:?}");
    assert!(
        !header.contains("\"=?"),
        "quoted encoded-word in {header:?}"
    );
    assert_eq!(parse(&raw), one(name, "info@example.org"));
}

#[test]
fn display_name_containing_quotes_and_comma_round_trips() {
    let name = "\"Steuerberater, Wirtschaftspr\u{fc}fer\"";
    let raw = written(vec![
        address(Some("Anna M\u{fc}ller"), "anna@example.com"),
        address(Some(name), "kanzlei@example.org"),
    ])
    .expect("compose");
    assert!(!to_header(&raw).contains("\"=?"), "{raw:?}");
    assert_eq!(
        parse(&raw),
        vec![
            (
                Some("Anna M\u{fc}ller".into()),
                Some("anna@example.com".into())
            ),
            (Some(name.into()), Some("kanzlei@example.org".into())),
        ]
    );
}

#[test]
fn comma_in_encoded_display_name_does_not_split_list() {
    let raw = written(vec![
        address(Some("M\u{fc}ller, Anna"), "anna@example.com"),
        address(Some("Beispiel GmbH"), "info@example.org"),
    ])
    .expect("compose");
    let header = to_header(&raw);
    assert_eq!(
        header.matches(',').count(),
        1,
        "comma inside encoded-word in {header:?}"
    );
    let parsed = parse(&raw);
    assert_eq!(parsed.len(), 2, "{header:?}");
    assert_eq!(
        parsed[0].0.as_deref(),
        Some("M\u{fc}ller, Anna"),
        "{header:?}"
    );
}

#[test]
fn phrase_encoded_word_escapes_specials() {
    let name = "Meier (Kanzlei) <x>; [y] \"z\" @ w\\v: u, t\u{fc}";
    let raw = written(vec![address(Some(name), "info@example.org")]).expect("compose");
    let header = to_header(&raw);
    assert!(header.contains("?Q?"), "not Q encoded in {header:?}");
    let encoded = header
        .split_once("?Q?")
        .and_then(|(_, rest)| rest.split_once("?="))
        .map(|(word, _)| word)
        .expect("an encoded word");
    for ch in [
        '(', ')', '<', '>', '[', ']', ':', ';', '@', '\\', ',', '"', '.',
    ] {
        assert!(!encoded.contains(ch), "raw {ch:?} in {encoded:?}");
    }
    assert_eq!(parse(&raw), one(name, "info@example.org"));
}

#[test]
fn ascii_display_name_with_comma_uses_quoted_string() {
    let raw = written(vec![address(Some("Doe, John"), "john@example.com")]).expect("compose");
    assert!(to_header(&raw).contains("\"Doe, John\""), "{raw:?}");
    assert_eq!(parse(&raw), one("Doe, John", "john@example.com"));
}

#[test]
fn ascii_display_name_with_quotes_is_escaped() {
    let name = "John \"JD\" Doe";
    let raw = written(vec![address(Some(name), "john@example.com")]).expect("compose");
    assert_eq!(parse(&raw), one(name, "john@example.com"), "{raw:?}");
}
