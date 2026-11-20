//! Tests ported by name from himalaya's `src/shared/message/read.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615): the header
//! and part cases of its read view, as `features/message-facts.toml` maps
//! them. Himalaya renders a view; we check the facts and the part listing
//! the same fixtures give. Its `src/shared/message/header.rs` suite (its own
//! RFC 2047 decoder) is not applicable here.

use mailtide::Timestamp;
use mailtide::gmail::SentAt;
use mailtide_mime::{Address, PartRole, decode_header, facts, parse_addresses, parts};

fn address(name: Option<&str>, email: &str) -> Address {
    Address {
        name: name.map(str::to_owned),
        email: email.to_owned(),
    }
}

#[test]
fn a_sender_name_in_encoded_words_is_decoded_and_adjacent_words_joined() {
    let raw = b"From: =?ISO-8859-1?Q?Andr=E9?= Dupont <andre@example.com>\r\n\
        To: =?UTF-8?B?w4lsb2RpZQ==?= <elodie@example.com>\r\n\
        Subject: =?UTF-8?B?w4l0w6k=?= =?UTF-8?Q?_=C3=A0_Paris?=\r\n\
        Content-Type: text/plain\r\n\
        \r\n\
        body\r\n";

    let facts = facts(raw);

    assert_eq!(facts.sender_name.as_deref(), Some("Andr\u{e9} Dupont"));
    assert_eq!(facts.sender_email.as_deref(), Some("andre@example.com"));
    assert_eq!(
        decode_header("=?UTF-8?B?w4l0w6k=?= =?UTF-8?Q?_=C3=A0_Paris?="),
        "\u{c9}t\u{e9} \u{e0} Paris"
    );
    assert_eq!(
        parse_addresses("=?UTF-8?B?w4lsb2RpZQ==?= <elodie@example.com>"),
        [address(Some("\u{c9}lodie"), "elodie@example.com")]
    );
}

#[test]
fn a_quoted_sender_name_holding_an_address_is_a_name() {
    let raw = b"From: \"Doe, John <john@x.org>\" <john@x.org>\r\n\
        To: \"Doe, John\" <john@x.org>\r\n\
        Content-Type: text/plain\r\n\
        \r\n\
        body\r\n";

    let facts = facts(raw);

    assert_eq!(facts.sender_name.as_deref(), Some("Doe, John <john@x.org>"));
    assert_eq!(facts.sender_email.as_deref(), Some("john@x.org"));
    assert_eq!(
        parse_addresses("\"Doe, John\" <john@x.org>"),
        [address(Some("Doe, John"), "john@x.org")]
    );
}

#[test]
fn parse_addresses_flattens_groups_and_drops_unreadable_entries() {
    assert_eq!(
        parse_addresses("Friends: Bob <bob@example.com>, carol@example.com;, dave@example.org"),
        [
            address(Some("Bob"), "bob@example.com"),
            address(None, "carol@example.com"),
            address(None, "dave@example.org"),
        ]
    );
    assert_eq!(parse_addresses("Undisclosed recipients:;"), []);
    // NOTE: himalaya also drops `frank@localhost`; a single-label domain is
    // still an address, so we keep it.
    assert_eq!(
        parse_addresses(
            "nobody, Eve <eve@>, \"x\" <@example.com>, Frank <frank@localhost>, <grace@example.net>"
        ),
        [
            address(Some("Frank"), "frank@localhost"),
            address(None, "grace@example.net"),
        ]
    );
    assert_eq!(
        parse_addresses("\"Support, Team\" <support@example.com>"),
        [address(Some("Support, Team"), "support@example.com")]
    );
}

#[test]
fn sent_at_keeps_the_offset_the_sender_wrote() {
    let at = |date: &str| {
        let raw = format!("Date: {date}\r\nContent-Type: text/plain\r\n\r\nbody\r\n");
        facts(raw.as_bytes()).sent_at
    };
    // 2025-07-24T10:00:00Z
    let utc = 1_753_351_200_000;
    let hour = 3_600_000;
    assert_eq!(
        at("Thu, 24 Jul 2025 10:00:00 -0500"),
        Some(SentAt {
            at: Timestamp(utc + 5 * hour),
            offset_minutes: -300
        })
    );
    assert_eq!(
        at("Thu, 24 Jul 2025 10:00:00 +0530"),
        Some(SentAt {
            at: Timestamp(utc - 5 * hour - hour / 2),
            offset_minutes: 330
        })
    );
    let zero = Some(SentAt {
        at: Timestamp(utc),
        offset_minutes: 0,
    });
    assert_eq!(at("Thu, 24 Jul 2025 10:00:00 +0000"), zero);
    assert_eq!(at("Thu, 24 Jul 2025 10:00:00 GMT"), zero);
    assert_eq!(at("not a date"), None);
    assert_eq!(at("Mon, 31 Feb 2025 10:00:00 +0000"), None);
}

#[test]
fn list_id_unsubscribe_precedence_and_auto_submitted_are_facts() {
    let raw = b"List-ID: =?UTF-8?Q?Caf=C3=A9?= news <news.example.com>\r\n\
        List-Unsubscribe: <mailto:leave@example.com>,\r\n <https://example.com/leave>\r\n\
        Precedence: bulk\r\n\
        Auto-Submitted: auto-generated\r\n\
        Content-Type: text/plain\r\n\
        \r\n\
        body\r\n";

    let facts = facts(raw);

    assert_eq!(facts.list_id.as_deref(), Some("news.example.com"));
    assert_eq!(
        facts.list_unsubscribe,
        ["mailto:leave@example.com", "https://example.com/leave"]
    );
    assert_eq!(facts.precedence.as_deref(), Some("bulk"));
    assert_eq!(facts.auto_submitted.as_deref(), Some("auto-generated"));
    assert!(!facts.one_click_unsubscribe);
}

#[test]
fn a_nested_alternative_in_mixed_lists_only_the_attachment() {
    let raw = b"Content-Type: multipart/mixed; boundary=\"outer\"\r\n\
        \r\n\
        --outer\r\n\
        Content-Type: multipart/alternative; boundary=\"inner\"\r\n\
        \r\n\
        --inner\r\n\
        Content-Type: text/plain; charset=utf-8\r\n\
        \r\n\
        plain body\r\n\
        --inner\r\n\
        Content-Type: text/html; charset=utf-8\r\n\
        \r\n\
        <p>html body</p>\r\n\
        --inner--\r\n\
        \r\n\
        --outer\r\n\
        Content-Type: application/pdf; name=\"doc.pdf\"\r\n\
        Content-Disposition: attachment; filename=\"doc.pdf\"\r\n\
        Content-Transfer-Encoding: base64\r\n\
        \r\n\
        JVBERi0=\r\n\
        --outer--\r\n";

    let facts = facts(raw);

    assert_eq!((facts.attached, facts.inline), (1, 0));
    assert_eq!(facts.parts.len(), 1);
    assert_eq!(facts.parts[0].content_type, "application/pdf");
    assert_eq!(facts.parts[0].filename.as_deref(), Some("doc.pdf"));
    assert_eq!(facts.parts[0].size, 5);
    let roles: Vec<(PartRole, String)> = parts(raw)
        .into_iter()
        .map(|info| (info.role, info.content_type))
        .collect();
    assert_eq!(
        roles,
        [
            (PartRole::Text, "text/plain".to_owned()),
            (PartRole::Html, "text/html".to_owned()),
            (PartRole::Attached, "application/pdf".to_owned()),
        ]
    );
}

#[test]
fn an_attached_message_is_one_listed_part_and_its_own_parts_are_not() {
    let raw = b"Subject: Outer\r\n\
        Content-Type: multipart/mixed; boundary=\"b\"\r\n\
        \r\n\
        --b\r\n\
        Content-Type: text/plain\r\n\
        \r\n\
        see attached\r\n\
        --b\r\n\
        Content-Type: message/rfc822\r\n\
        \r\n\
        Subject: Inner\r\n\
        Content-Type: text/plain\r\n\
        \r\n\
        inner body\r\n\
        --b--\r\n";

    let facts = facts(raw);

    assert_eq!((facts.attached, facts.inline), (1, 0));
    let message = &facts.parts[0];
    assert_eq!(message.content_type, "message/rfc822");
    assert_eq!(message.filename, None);
    let inner = b"Subject: Inner\r\nContent-Type: text/plain\r\n\r\ninner body";
    assert_eq!(message.size, u64::try_from(inner.len()).expect("size"));
    let all = parts(raw);
    assert_eq!(
        all.len(),
        3,
        "the inner text is a part of the attached message"
    );
    assert_eq!(all[2].container, Some(message.part));
}

#[test]
fn rfc_2231_filenames_are_decoded() {
    let raw = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\
        \r\n\
        --b\r\n\
        Content-Type: application/pdf\r\n\
        Content-Disposition: attachment;\r\n filename*0*=utf-8''r%C3%A9sum;\r\n filename*1*=%C3%A9.pdf\r\n\
        \r\n\
        x\r\n\
        --b\r\n\
        Content-Type: text/plain\r\n\
        Content-Disposition: attachment; filename*=iso-8859-1'fr'caf%E9.txt\r\n\
        \r\n\
        y\r\n\
        --b\r\n\
        Content-Type: application/octet-stream; name=\"=?UTF-8?B?w6l0w6kucG5n?=\"\r\n\
        \r\n\
        z\r\n\
        --b--\r\n";

    let facts = facts(raw);

    let names: Vec<Option<&str>> = facts
        .parts
        .iter()
        .map(|part| part.filename.as_deref())
        .collect();
    assert_eq!(
        names,
        [
            Some("r\u{e9}sum\u{e9}.pdf"),
            Some("caf\u{e9}.txt"),
            Some("\u{e9}t\u{e9}.png")
        ]
    );
    assert_eq!((facts.attached, facts.inline), (3, 0));
}

#[test]
fn roles_follow_our_rule_on_himalayas_fixture() {
    let raw = b"Content-Type: multipart/related; boundary=\"b\"\r\n\
        \r\n\
        --b\r\n\
        Content-Type: text/html\r\n\
        \r\n\
        <img src=\"cid:logo@example.com\">\r\n\
        --b\r\n\
        Content-Type: image/png\r\n\
        Content-ID: <logo@example.com>\r\n\
        Content-Transfer-Encoding: base64\r\n\
        \r\n\
        iVBORw==\r\n\
        --b\r\n\
        Content-Type: image/gif\r\n\
        Content-Disposition: inline\r\n\
        \r\n\
        GIF\r\n\
        --b\r\n\
        Content-Type: image/jpeg\r\n\
        \r\n\
        JPG\r\n\
        --b\r\n\
        Content-Type: text/plain; name=\"notes.txt\"\r\n\
        \r\n\
        notes\r\n\
        --b\r\n\
        Content-Type: text/calendar; charset=UTF-8; method=request\r\n\
        \r\n\
        BEGIN:VCALENDAR\r\n\
        END:VCALENDAR\r\n\
        --b\r\n\
        Content-Type: image/png\r\n\
        Content-ID: <photo@example.com>\r\n\
        Content-Disposition: attachment; filename=\"photo.png\"\r\n\
        \r\n\
        PNG\r\n\
        --b--\r\n";

    let roles: Vec<PartRole> = parts(raw).iter().map(|info| info.role).collect();
    assert_eq!(
        roles,
        [
            PartRole::Html,
            PartRole::Inline,
            PartRole::Inline,
            PartRole::Inline,
            PartRole::Attached,
            PartRole::Attached,
            PartRole::Attached,
        ]
    );
    let facts = facts(raw);
    assert_eq!((facts.attached, facts.inline), (3, 3));
    assert_eq!(
        facts.parts[0].content_id.as_deref(),
        Some("logo@example.com")
    );
    assert_eq!(facts.parts[0].size, 4);
    assert_eq!(facts.parts[4].content_type, "text/calendar");
}

#[test]
fn a_name_split_across_encoded_words_is_rebuilt() {
    let raw = b"From: =?utf-8?B?w4lsb2Rp?= =?utf-8?B?ZSBD?= =?utf-8?B?w6k=?= <e@example.com>\r\n\
        Content-Type: multipart/mixed; boundary=\"b\"\r\n\
        \r\n\
        --b\r\n\
        Content-Type: application/pdf; name=\"=?utf-8?B?w6k=?= =?utf-8?B?dMOp?=.pdf\"\r\n\
        \r\n\
        x\r\n\
        --b--\r\n";

    let facts = facts(raw);

    assert_eq!(facts.sender_name.as_deref(), Some("\u{c9}lodie C\u{e9}"));
    assert_eq!(
        decode_header("=?utf-8?B?w4lsb2Rp?= =?utf-8?B?ZSBD?= =?utf-8?B?w6k=?="),
        "\u{c9}lodie C\u{e9}"
    );
    // NOTE: inside a quoted parameter, where RFC 2047 does not apply, the
    // parser decodes each word but keeps the space between them.
    assert_eq!(
        facts.parts[0].filename.as_deref(),
        Some("\u{e9} t\u{e9}.pdf")
    );
}

#[test]
fn parts_lists_every_leaf_in_document_order() {
    let raw = b"From: Alice <alice@example.com>\r\n\
        Subject: Mixed\r\n\
        Content-Type: multipart/mixed; boundary=\"b\"\r\n\
        \r\n\
        --b\r\n\
        Content-Type: text/plain\r\n\
        \r\n\
        plain body\r\n\
        --b\r\n\
        Content-Type: text/html\r\n\
        \r\n\
        <p>html body</p>\r\n\
        --b\r\n\
        Content-Type: application/pdf; name=\"doc.pdf\"\r\n\
        Content-Disposition: attachment; filename=\"doc.pdf\"\r\n\
        Content-Transfer-Encoding: base64\r\n\
        \r\n\
        JVBERi0=\r\n\
        --b--\r\n";

    let parts = parts(raw);

    let listed: Vec<(PartRole, &str, Option<&str>, u64)> = parts
        .iter()
        .map(|info| {
            (
                info.role,
                info.content_type.as_str(),
                info.filename.as_deref(),
                info.size,
            )
        })
        .collect();
    assert_eq!(
        listed,
        [
            (PartRole::Text, "text/plain", None, 10),
            (PartRole::Html, "text/html", None, 16),
            (PartRole::Attached, "application/pdf", Some("doc.pdf"), 5),
        ]
    );
    let offsets: Vec<u64> = parts
        .iter()
        .map(|info| info.part.expect("range").offset)
        .collect();
    assert!(offsets.is_sorted());
}

#[test]
fn content_id_and_list_id_lose_their_angle_brackets() {
    let raw = b"List-Id: <list.example.com>\r\n\
        Message-ID: <3@example.com>\r\n\
        Content-Type: multipart/mixed; boundary=\"b\"\r\n\
        \r\n\
        --b\r\n\
        Content-Type: image/png\r\n\
        Content-ID: <logo@example.com>\r\n\
        Content-Disposition: attachment\r\n\
        \r\n\
        PNG\r\n\
        --b--\r\n";

    let facts = facts(raw);

    assert_eq!(facts.list_id.as_deref(), Some("list.example.com"));
    assert_eq!(
        facts.parts[0].content_id.as_deref(),
        Some("logo@example.com")
    );
    assert_eq!(
        parts(raw)[0].content_id.as_deref(),
        Some("logo@example.com")
    );
}
