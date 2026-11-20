//! Tests ported by behaviour from pimalaya core's
//! `mml/src/message/body/compiler/mod.rs` (pimalaya/core, MIT, commit
//! b3a9640a2b2e), as `features/compose-mime.toml` maps them: exact bytes,
//! pinned by fixing the Message-ID and the Date.

use mailtide_mime::{Address, ComposeOptions, MessageId, OutgoingMessage, Purpose, compose};

fn exact(text: Option<&str>, html: Option<&str>) -> String {
    let mut message = OutgoingMessage::new(
        Address {
            name: None,
            email: "from@localhost".into(),
        },
        0,
    );
    message.message_id = Some(MessageId::parse("id@localhost").expect("id"));
    message.text = text.map(str::to_owned);
    message.html = html.map(str::to_owned);
    let draft = ComposeOptions {
        purpose: Purpose::Draft,
        max_bytes: None,
    };
    String::from_utf8(compose(&message, &draft).expect("compose").raw).expect("ASCII")
}

#[test]
fn a_plain_text_message_is_exactly_these_bytes() {
    assert_eq!(
        exact(Some("Hello, world!\n"), None),
        "From: <from@localhost>\r\n\
         Date: Thu, 1 Jan 1970 00:00:00 +0000\r\n\
         Message-ID: <id@localhost>\r\n\
         MIME-Version: 1.0\r\n\
         Content-Type: text/plain; charset=\"utf-8\"\r\n\
         Content-Transfer-Encoding: 7bit\r\n\
         \r\n\
         Hello, world!\r\n"
    );
}

#[test]
fn an_html_message_is_exactly_these_bytes() {
    assert_eq!(
        exact(None, Some("<h1>Hello, world!</h1>\n")),
        "From: <from@localhost>\r\n\
         Date: Thu, 1 Jan 1970 00:00:00 +0000\r\n\
         Message-ID: <id@localhost>\r\n\
         MIME-Version: 1.0\r\n\
         Content-Type: text/html; charset=\"utf-8\"\r\n\
         Content-Transfer-Encoding: 7bit\r\n\
         \r\n\
         <h1>Hello, world!</h1>\r\n"
    );
}
