//! Tests ported by name from Zero's `imap/imap-store.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890), as `features/message-facts.toml`
//! maps them. Message-ID, References and Subject stay the existing header
//! facts, as written; the Date is the `sent_at` fact and encoded words
//! decode through `decode_header` and the sender name.

use mailtide::Timestamp;
use mailtide_mime::{decode_header, facts};

#[test]
fn a_date_is_parsed_and_encoded_words_decode() {
    let raw = b"Message-ID: <a@x>\r\nReferences: <r1@x>\r\n <r2@x>\r\n\
        Subject: =?UTF-8?B?SMOpbGxv?= world\r\nDate: Thu, 01 Jan 2026 10:00:00 +0000\r\n\
        From: =?iso-8859-1?Q?caf=E9_ok?= <cafe@x>\r\n\r\nbody\r\n";
    let facts = facts(raw);
    // 2026-01-01T10:00:00Z
    assert_eq!(
        facts.sent_at.map(|sent| sent.at),
        Some(Timestamp(1_767_261_600_000))
    );
    assert_eq!(
        decode_header("=?UTF-8?B?SMOpbGxv?= world"),
        "H\u{e9}llo world"
    );
    assert_eq!(decode_header("=?iso-8859-1?Q?caf=E9_ok?="), "caf\u{e9} ok");
    assert_eq!(facts.sender_name.as_deref(), Some("caf\u{e9} ok"));
}
