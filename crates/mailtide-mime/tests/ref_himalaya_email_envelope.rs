//! Tests ported by name from himalaya's `src/email/envelope.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615): its Date
//! cases, as `features/message-facts.toml` maps them, read as the `sent_at`
//! fact of a message carrying that `Date`.

use mailtide::Timestamp;
use mailtide::gmail::SentAt;
use mailtide_mime::facts;

fn sent_at(date: &str) -> Option<SentAt> {
    facts(format!("Date: {date}\r\nSubject: x\r\n\r\nbody\r\n").as_bytes()).sent_at
}

/// 2026-07-17T10:00:00Z.
const JULY_17: i64 = 1_784_282_400_000;

#[test]
fn a_well_formed_date_keeps_its_instant_and_offset() {
    assert_eq!(
        sent_at("Fri, 17 Jul 2026 10:00:00 +0000"),
        Some(SentAt {
            at: Timestamp(JULY_17),
            offset_minutes: 0
        })
    );
    assert_eq!(
        sent_at("Fri, 17 Jul 2026 12:00:00 +0200"),
        Some(SentAt {
            at: Timestamp(JULY_17),
            offset_minutes: 120
        })
    );
}

#[test]
fn a_wrong_weekday_keeps_its_date() {
    assert_eq!(
        sent_at("Thu, 17 Jul 2026 10:00:00 +0000").map(|sent| sent.at),
        Some(Timestamp(JULY_17))
    );
}

#[test]
fn a_date_without_a_weekday_is_parsed() {
    assert_eq!(
        sent_at("17 Jul 2026 10:00:00 +0000").map(|sent| sent.at),
        Some(Timestamp(JULY_17))
    );
}

#[test]
fn an_empty_or_unreadable_date_is_absent() {
    assert_eq!(sent_at("   "), None);
    assert_eq!(sent_at("not a date"), None);
    assert_eq!(facts(b"Subject: no date\r\n\r\nbody\r\n").sent_at, None);
}
