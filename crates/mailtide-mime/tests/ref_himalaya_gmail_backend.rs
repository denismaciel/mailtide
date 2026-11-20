//! Tests ported by name from himalaya's `src/gmail/backend.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615): the two
//! envelope cases `features/message-facts.toml` maps here. Himalaya reads
//! the headers from `format=metadata`; we read the same values from the raw
//! message.

use mailtide::Timestamp;
use mailtide_mime::{Address, facts, parse_addresses};

#[test]
fn a_quoted_name_with_a_comma_is_one_address_and_an_empty_group_is_none() {
    let addresses: Vec<(Option<String>, String)> =
        parse_addresses("\"Doe, John\" <john@x.org>, Team: a@x.org, b@x.org;")
            .into_iter()
            .map(|Address { name, email }| (name, email))
            .collect();
    assert_eq!(
        addresses,
        [
            (Some("Doe, John".to_owned()), "john@x.org".to_owned()),
            (None, "a@x.org".to_owned()),
            (None, "b@x.org".to_owned()),
        ]
    );
    assert!(parse_addresses("undisclosed-recipients:;").is_empty());
    let facts = facts(b"From: \"Doe, John\" <john@x.org>\r\n\r\nbody\r\n");
    assert_eq!(facts.sender_name.as_deref(), Some("Doe, John"));
    assert_eq!(facts.sender_email.as_deref(), Some("john@x.org"));
}

#[test]
fn a_date_from_gmail_with_a_wrong_weekday_is_kept() {
    // 2026-11-15 is a Sunday.
    let facts = facts(b"Date: Mon, 15 Nov 2026 11:00:00 +0000\r\nSubject: hi\r\n\r\nbody\r\n");
    let sent = facts.sent_at.expect("a date");
    assert_eq!(sent.at, Timestamp(1_794_740_400_000));
    assert_eq!(sent.offset_minutes, 0);
}
