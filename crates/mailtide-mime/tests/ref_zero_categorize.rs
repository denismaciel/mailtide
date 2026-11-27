//! Tests ported by name from Zero's `imap/categorize.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890), as `features/message-facts.toml`
//! maps them. Zero classifies a message from its headers; we check that the
//! headers its classifier reads are stored facts. The category is the
//! host's.

use mailtide_mime::{facts, parse_addresses};

fn message(lines: &[&str]) -> Vec<u8> {
    format!("{}\r\n\r\nbody\r\n", lines.join("\r\n")).into_bytes()
}

#[test]
fn the_headers_a_classifier_reads_are_stored_facts() {
    let rows: [(&[&str], &str); 10] = [
        (&["From: Alice <alice@example.com>"], "personal"),
        (&["From: info@smallbiz.com"], "personal"),
        (
            &[
                "From: Shop <news@shop.com>",
                "List-Unsubscribe: <https://shop.com/u>",
            ],
            "promotions",
        ),
        (&["From: deals@shop.com", "Precedence: bulk"], "promotions"),
        (&["From: x@esp.com", "Feedback-ID: 1:2:3:esp"], "promotions"),
        (
            &[
                "From: no-reply@bank.com",
                "List-Unsubscribe: <mailto:u@bank.com>",
            ],
            "updates",
        ),
        (&["From: GitHub <notifications@github.com>"], "updates"),
        (
            &["From: robot@ci.dev", "Auto-Submitted: auto-generated"],
            "updates",
        ),
        (
            &["From: bob@lists.dev", "List-Id: <dev.lists.dev>"],
            "updates",
        ),
        (
            &[
                "From: bob@lists.dev",
                "List-Id: <dev.lists.dev>",
                "Precedence: list",
                "List-Unsubscribe: <x>",
            ],
            "updates",
        ),
    ];
    let mut seen = Vec::new();
    for (lines, _category) in rows {
        let facts = facts(&message(lines));
        let header = |name: &str| {
            lines
                .iter()
                .find_map(|line| line.strip_prefix(name)?.strip_prefix(": "))
        };
        let from = header("From").expect("every row has a sender");
        assert!(from.contains(facts.sender_email.as_deref().expect("sender")));
        assert_eq!(
            facts.list_unsubscribe,
            header("List-Unsubscribe")
                .map_or_default(|value| vec![value.trim_matches(['<', '>']).to_owned()])
        );
        assert_eq!(facts.precedence.as_deref(), header("Precedence"));
        assert_eq!(facts.auto_submitted.as_deref(), header("Auto-Submitted"));
        assert_eq!(
            facts.list_id.as_deref(),
            header("List-Id").map(|value| value.trim_matches(['<', '>']))
        );
        seen.push(facts);
    }
    assert_eq!(seen[1].sender_name, None);
    assert_eq!(seen[6].sender_name.as_deref(), Some("GitHub"));
}

#[test]
fn the_sender_address_is_read_from_a_named_from_and_lists_are_split() {
    let facts = facts(&message(&["From: \"Alice A.\" <Alice@Example.com>"]));
    assert_eq!(facts.sender_name.as_deref(), Some("Alice A."));
    assert_eq!(facts.sender_email.as_deref(), Some("Alice@Example.com"));
    let emails: Vec<String> = ["a@x.com, \"B\" <b@y.com>", "c@z.com"]
        .iter()
        .flat_map(|value| parse_addresses(value))
        .map(|address| address.email)
        .collect();
    assert_eq!(emails, ["a@x.com", "b@y.com", "c@z.com"]);
}
