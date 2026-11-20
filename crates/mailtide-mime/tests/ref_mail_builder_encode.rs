//! Tests ported by name from mail-builder's `src/encoders/encode.rs`
//! (stalwartlabs/mail-builder, Apache-2.0 OR MIT, commit 2fc4a942754f), as
//! `features/compose-mime.toml` maps them: the body transfer encoding,
//! through `compose`.

use mailtide_mime::{Address, ComposeOptions, OutgoingMessage, Purpose, compose};

/// The text part of a message holding only `text`, from its
/// Content-Transfer-Encoding header on.
fn written(text: &str) -> String {
    let mut message = OutgoingMessage::new(
        Address {
            name: None,
            email: "a@example.org".into(),
        },
        0,
    );
    message.text = Some(text.to_owned());
    let draft = ComposeOptions {
        purpose: Purpose::Draft,
        max_bytes: None,
    };
    let raw = String::from_utf8(compose(&message, &draft).expect("compose").raw).expect("ASCII");
    let at = raw.find("Content-Transfer-Encoding").expect("encoding");
    raw[at..].to_owned()
}

#[test]
fn text_is_7bit_when_clean_quoted_printable_past_78_columns_and_always_crlf() {
    assert_eq!(
        written("a b c\r\n"),
        "Content-Transfer-Encoding: 7bit\r\n\r\na b c\r\n"
    );

    let line = "a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a";
    assert_eq!(
        written(&format!("{line}\r\n")),
        format!("Content-Transfer-Encoding: 7bit\r\n\r\n{line}\r\n")
    );

    let long_line = format!("{line} a");
    assert_eq!(
        written(&format!("{long_line}\r\n")),
        format!(
            "Content-Transfer-Encoding: quoted-printable\r\n\r\n{}=\r\n{}\r\n",
            &long_line[..75],
            &long_line[75..]
        )
    );

    // A message of one part ends with a line break.
    let long_line = "a".repeat(100);
    assert_eq!(
        written(&long_line),
        format!(
            "Content-Transfer-Encoding: quoted-printable\r\n\r\n{}=\r\n{}\r\n",
            "a".repeat(75),
            "a".repeat(25)
        )
    );

    // Bare LF and bare CR both become CRLF.
    assert_eq!(
        written("one\ntwo\r\nthree\rfour\n"),
        "Content-Transfer-Encoding: 7bit\r\n\r\none\r\ntwo\r\nthree\r\nfour\r\n"
    );
}
