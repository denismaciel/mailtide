//! The facts the driver stores with a message at import: envelope fields
//! the header facts don't cover, and a bounded attachment summary whose
//! entries carry a byte range into the stored blob.

use mail_parser::{DateTime, HeaderName, Headers};
use mailtide::gmail::{
    AttachmentFact, MAX_FILENAME_BYTES, MAX_LISTED_PARTS, MAX_UNSUBSCRIBE_URI_BYTES,
    MAX_UNSUBSCRIBE_URIS, MessageFacts, SentAt, split_addresses,
};
use mailtide::ids::Timestamp;

use crate::parts::{PartRole, leaves};

/// Extract the facts of a raw message. Never fails: input that does not
/// parse as a message has empty facts.
#[must_use]
pub fn facts(raw: &[u8]) -> MessageFacts {
    let Some(message) = crate::parser().parse(raw) else {
        return MessageFacts::empty();
    };
    let root = message.root();
    let headers = root.headers();
    let sender = headers.all_from().next().and_then(|from| from.first());
    let list_unsubscribe = first_value(&headers, HeaderName::ListUnsubscribe)
        .map_or_default(|value| unsubscribe_uris(&value));
    let one_click = first_value(&headers, HeaderName::ListUnsubscribePost).is_some_and(|value| {
        value
            .trim()
            .eq_ignore_ascii_case("List-Unsubscribe=One-Click")
    }) && list_unsubscribe.iter().any(|uri| {
        uri.get(..8)
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
    });
    let mut facts = MessageFacts {
        bcc: addresses(&headers, HeaderName::Bcc),
        reply_to: addresses(&headers, HeaderName::ReplyTo),
        sent_at: headers
            .all(HeaderName::Date)
            .next()
            .and_then(|date| date.value().as_datetime())
            .and_then(sent_at),
        sender_name: sender.and_then(|mailbox| mailbox.name()).map(str::to_owned),
        sender_email: sender
            .and_then(|mailbox| mailbox.address())
            .map(str::to_owned),
        list_id: first_value(&headers, HeaderName::ListId).and_then(|value| list_id(&value)),
        list_unsubscribe,
        one_click_unsubscribe: one_click,
        auto_submitted: first_value(&headers, HeaderName::AutoSubmitted)
            .and_then(|value| keyword(&value)),
        precedence: first_value(&headers, HeaderName::Precedence).and_then(|value| keyword(&value)),
        tracking_id: crate::tracking_id(raw),
        ..MessageFacts::empty()
    };
    for leaf in leaves(&message, root) {
        match leaf.role {
            PartRole::Attached => facts.attached = facts.attached.saturating_add(1),
            PartRole::Inline => facts.inline = facts.inline.saturating_add(1),
            PartRole::Protocol => {
                facts.protocol = facts.protocol.saturating_add(1);
                continue;
            }
            PartRole::Text | PartRole::Html => continue,
        }
        // Every part of the top-level message has a range in the raw input.
        let Some(part) = leaf.reference else { continue };
        if facts.parts.len() < MAX_LISTED_PARTS {
            facts.parts.push(AttachmentFact {
                part,
                filename: leaf
                    .part
                    .attachment_name()
                    .map(|name| cut(name, MAX_FILENAME_BYTES).to_owned()),
                content_type: leaf.content_type,
                content_id: leaf.part.content_id().map(str::to_owned),
                disposition: leaf.disposition,
                referenced: leaf.referenced,
                related: leaf.related,
            });
        }
    }
    facts
}

/// Every field named `name`, unfolded and split like the header facts'
/// `to` and `cc`.
fn addresses(headers: &Headers<'_>, name: HeaderName<'_>) -> Vec<String> {
    headers
        .all(name)
        .flat_map(|header| split_addresses(&crate::unfold(header.raw_value())))
        .collect()
}

/// The first field named `name`, unfolded, as written.
fn first_value(headers: &Headers<'_>, name: HeaderName<'_>) -> Option<String> {
    headers
        .all(name)
        .next()
        .map(|header| crate::unfold(header.raw_value()))
}

/// A readable `Date`: in range, a day that exists in its month. The weekday
/// is not checked.
fn sent_at(date: DateTime) -> Option<SentAt> {
    if !date.is_valid() || date.day > days_in_month(date.year, date.month) {
        return None;
    }
    let minutes = i32::from(date.tz_hour) * 60 + i32::from(date.tz_minute);
    Some(SentAt {
        at: Timestamp(date.to_timestamp().checked_mul(1000)?),
        offset_minutes: if date.tz_before_gmt {
            -minutes
        } else {
            minutes
        },
    })
}

fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// RFC 2919: an optional phrase, then the identifier in angle brackets.
fn list_id(value: &str) -> Option<String> {
    let id = match (value.rfind('<'), value.rfind('>')) {
        (Some(open), Some(close)) if open < close => value.get(open + 1..close)?,
        _ => value,
    }
    .trim();
    (!id.is_empty()).then(|| id.to_owned())
}

/// RFC 2369: URIs in angle brackets, separated by commas. At most
/// [`MAX_UNSUBSCRIBE_URIS`] are kept; a longer one than
/// [`MAX_UNSUBSCRIBE_URI_BYTES`] is left out, never cut.
fn unsubscribe_uris(value: &str) -> Vec<String> {
    value
        .split('<')
        .skip(1)
        .filter_map(|rest| rest.split_once('>').map(|(uri, _)| uri.trim()))
        .filter(|uri| !uri.is_empty() && uri.len() <= MAX_UNSUBSCRIBE_URI_BYTES)
        .take(MAX_UNSUBSCRIBE_URIS)
        .map(str::to_owned)
        .collect()
}

/// The keyword of `Auto-Submitted` or `Precedence`, lowercased, without
/// parameters or comments.
fn keyword(value: &str) -> Option<String> {
    let word = value.split([';', '(']).next().unwrap_or_default().trim();
    (!word.is_empty()).then(|| word.to_ascii_lowercase())
}

/// `text` cut to at most `max` bytes, never inside a character.
fn cut(text: &str, max: usize) -> &str {
    text.get(..text.floor_char_boundary(max)).unwrap_or(text)
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;
    use mailtide::gmail::FACTS_VERSION;

    fn message(headers: &str) -> Vec<u8> {
        format!("{headers}Content-Type: text/plain\r\n\r\nbody\r\n").into_bytes()
    }

    /// A text/html body plus one image, in `layout` (`related` or `mixed`).
    fn with_image(layout: &str, html: &str, image_headers: &str) -> Vec<u8> {
        format!(
            "Content-Type: multipart/{layout}; boundary=\"b\"\r\n\r\n\
             --b\r\nContent-Type: text/html\r\n\r\n{html}\r\n\
             --b\r\nContent-Type: image/png\r\n{image_headers}Content-Transfer-Encoding: base64\r\n\r\n\
             iVBORw0KGgo=\r\n--b--\r\n"
        )
        .into_bytes()
    }

    #[test]
    fn bcc_and_reply_to_are_listed_as_written() {
        let facts = facts(&message(
            "Bcc: a@x.org, \"Doe, Jane\" <jane@x.org>\r\nBcc: =?utf-8?Q?Ren=C3=A9?= <rene@x.org>\r\n\
             Reply-To: \"Support, Team\" <help@x.org>,\r\n <other@x.org>\r\n",
        ));
        assert_eq!(
            facts.bcc,
            [
                "a@x.org",
                "\"Doe, Jane\" <jane@x.org>",
                "=?utf-8?Q?Ren=C3=A9?= <rene@x.org>"
            ]
        );
        assert_eq!(
            facts.reply_to,
            ["\"Support, Team\" <help@x.org>", "<other@x.org>"]
        );
    }

    #[test]
    fn a_from_without_a_display_name_has_only_an_address() {
        let facts = facts(&message("From: ann@x.org\r\n"));
        assert_eq!(facts.sender_name, None);
        assert_eq!(facts.sender_email.as_deref(), Some("ann@x.org"));
        let facts = super::facts(&message("From: Ann <ann@x.org>, Bob <bob@x.org>\r\n"));
        assert_eq!(facts.sender_name.as_deref(), Some("Ann"));
        assert_eq!(facts.sender_email.as_deref(), Some("ann@x.org"));
    }

    #[test]
    fn list_id_is_the_identifier_in_angle_brackets() {
        let id = |value: &str| facts(&message(&format!("List-Id: {value}\r\n"))).list_id;
        assert_eq!(
            id("Dev list <dev.lists.example.org>").as_deref(),
            Some("dev.lists.example.org")
        );
        assert_eq!(
            id("<news.example.org>").as_deref(),
            Some("news.example.org")
        );
        assert_eq!(
            id("  bare.example.org  ").as_deref(),
            Some("bare.example.org")
        );
    }

    #[test]
    fn one_click_unsubscribe_needs_the_post_header_and_an_https_uri() {
        let unsubscribe = "List-Unsubscribe: <mailto:leave@x.org>, <https://x.org/u?id=1>\r\n";
        let post = "List-Unsubscribe-Post: List-Unsubscribe=One-Click\r\n";
        let both = facts(&message(&format!("{unsubscribe}{post}")));
        assert_eq!(
            both.list_unsubscribe,
            ["mailto:leave@x.org", "https://x.org/u?id=1"]
        );
        assert!(both.one_click_unsubscribe);
        assert!(!facts(&message(unsubscribe)).one_click_unsubscribe);
        let mail_only = format!("List-Unsubscribe: <mailto:leave@x.org>\r\n{post}");
        assert!(!facts(&message(&mail_only)).one_click_unsubscribe);
        let plain_http = format!("List-Unsubscribe: <http://x.org/u>\r\n{post}");
        assert!(!facts(&message(&plain_http)).one_click_unsubscribe);
    }

    #[test]
    fn an_overlong_unsubscribe_uri_is_left_out() {
        let long = format!("https://x.org/{}", "a".repeat(MAX_UNSUBSCRIBE_URI_BYTES));
        let value = format!(
            "List-Unsubscribe: <mailto:1@x.org>, <{long}>, <mailto:2@x.org>, <mailto:3@x.org>, <mailto:4@x.org>, <mailto:5@x.org>\r\n"
        );
        let facts = facts(&message(&value));
        assert_eq!(
            facts.list_unsubscribe,
            [
                "mailto:1@x.org",
                "mailto:2@x.org",
                "mailto:3@x.org",
                "mailto:4@x.org"
            ]
        );
    }

    #[test]
    fn auto_submitted_and_precedence_are_lowercased_keywords() {
        let facts = facts(&message(
            "Auto-Submitted: Auto-Replied; owner-email=x@x.org\r\nPrecedence: BULK (mass mailing)\r\n",
        ));
        assert_eq!(facts.auto_submitted.as_deref(), Some("auto-replied"));
        assert_eq!(facts.precedence.as_deref(), Some("bulk"));
    }

    #[test]
    fn a_message_without_the_optional_headers_has_empty_facts_at_the_current_version() {
        let facts = facts(&message("Subject: hi\r\n"));
        assert_eq!(facts, MessageFacts::empty());
        assert_eq!(facts.version, FACTS_VERSION);
        assert_eq!((facts.attached, facts.inline), (0, 0));
        assert!(!facts.has_attachment());
    }

    #[test]
    fn a_referenced_image_disposed_as_attachment_is_inline() {
        let facts = facts(&with_image(
            "related",
            "<img src=\"cid:logo@x\">",
            "Content-ID: <logo@x>\r\nContent-Disposition: attachment; filename=\"logo.png\"\r\n",
        ));
        let part = &facts.parts[0];
        assert!(part.referenced);
        assert_eq!(part.disposition.as_deref(), Some("attachment"));
        assert!(part.is_inline());
        assert_eq!((facts.attached, facts.inline), (0, 1));
    }

    #[test]
    fn an_unreferenced_image_disposed_inline_is_inline() {
        let facts = facts(&with_image(
            "mixed",
            "<p>no image here</p>",
            "Content-Disposition: inline\r\n",
        ));
        let part = &facts.parts[0];
        assert!(!part.referenced);
        assert_eq!(part.disposition.as_deref(), Some("inline"));
        assert!(part.is_inline());
        assert_eq!((facts.attached, facts.inline), (0, 1));
    }

    #[test]
    fn a_cid_reference_matches_the_whole_percent_decoded_id() {
        let referenced = |html: &str, id: &str| {
            facts(&with_image(
                "mixed",
                html,
                &format!("Content-ID: <{id}>\r\n"),
            ))
            .parts[0]
                .referenced
        };
        assert!(referenced("<img src=\"cid:logo%40x\">", "logo@x"));
        assert!(referenced("<img src='CID:logo@x'>", "logo@x"));
        assert!(!referenced("<img src=\"cid:logo\">", "logo2@x"));
        assert!(!referenced("<img src=\"cid:logo@x.org\">", "logo@x"));
        let in_text = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n\
            --b\r\nContent-Type: text/plain\r\n\r\nsee cid:logo@x\r\n\
            --b\r\nContent-Type: image/png\r\nContent-ID: <logo@x>\r\n\r\nPNG\r\n--b--\r\n";
        let facts = facts(in_text);
        assert!(!facts.parts[0].referenced);
        assert!(!facts.parts[0].is_inline());
    }

    #[test]
    fn a_non_text_part_with_an_unknown_disposition_is_attached() {
        let raw = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n\
            --b\r\nContent-Type: text/plain\r\n\r\nhello\r\n\
            --b\r\nContent-Type: application/pdf\r\nContent-Disposition: x-foo\r\n\r\nPDF\r\n--b--\r\n";
        let facts = facts(raw);
        assert_eq!(facts.parts[0].disposition.as_deref(), Some("x-foo"));
        assert!(!facts.parts[0].is_inline());
        assert_eq!((facts.attached, facts.inline), (1, 0));
    }

    #[test]
    fn a_calendar_alternative_is_attached() {
        let raw = b"Content-Type: multipart/alternative; boundary=\"b\"\r\n\r\n\
            --b\r\nContent-Type: text/plain\r\n\r\nInvitation\r\n\
            --b\r\nContent-Type: text/html\r\n\r\n<p>Invitation</p>\r\n\
            --b\r\nContent-Type: text/calendar; method=REQUEST\r\n\r\nBEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n--b--\r\n";
        let facts = facts(raw);
        assert_eq!(facts.parts.len(), 1);
        assert_eq!(facts.parts[0].content_type, "text/calendar");
        assert_eq!((facts.attached, facts.inline), (1, 0));
        assert!(facts.has_attachment());
    }

    #[test]
    fn more_than_32_listed_parts_keep_exact_counts() {
        let mut raw = String::from("Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n");
        for n in 0..40 {
            let disposition = if n % 4 == 0 { "inline" } else { "attachment" };
            let _ = write!(
                raw,
                "--b\r\nContent-Type: application/pdf\r\nContent-Disposition: {disposition}; filename=\"f{n}.pdf\"\r\n\r\nPDF {n}\r\n"
            );
        }
        raw.push_str("--b--\r\n");
        let facts = facts(raw.as_bytes());
        assert_eq!(facts.parts.len(), MAX_LISTED_PARTS);
        assert_eq!((facts.attached, facts.inline), (30, 10));
        let names: Vec<_> = facts
            .parts
            .iter()
            .map(|part| part.filename.clone().unwrap_or_default())
            .collect();
        assert_eq!(names.first().map(String::as_str), Some("f0.pdf"));
        assert_eq!(names.last().map(String::as_str), Some("f31.pdf"));
    }

    #[test]
    fn a_long_filename_is_cut_at_a_character_boundary() {
        let mut encoded = String::new();
        for byte in format!("{}.pdf", "é".repeat(200)).bytes() {
            let _ = write!(encoded, "%{byte:02X}");
        }
        let raw = format!(
            "Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n\
             --b\r\nContent-Type: application/pdf\r\n\
             Content-Disposition: attachment; filename*=utf-8''{encoded}\r\n\r\nPDF\r\n--b--\r\n"
        );
        let facts = facts(raw.as_bytes());
        let filename = facts.parts[0].filename.clone().unwrap_or_default();
        assert_eq!(filename.len(), 254);
        assert_eq!(filename, "é".repeat(127));
    }

    #[test]
    fn a_part_without_a_readable_type_is_octet_stream() {
        let raw = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n\
            --b\r\nContent-Type: text/plain\r\n\r\nhello\r\n\
            --b\r\nContent-Disposition: attachment; filename=\"a\"\r\n\r\nA\r\n\
            --b\r\nContent-Type: Image/PNG\r\nContent-Disposition: attachment\r\n\r\nB\r\n\
            --b\r\nContent-Type: image\r\nContent-Disposition: attachment\r\n\r\nC\r\n--b--\r\n";
        let facts = facts(raw);
        let types: Vec<&str> = facts
            .parts
            .iter()
            .map(|part| part.content_type.as_str())
            .collect();
        assert_eq!(
            types,
            [
                "application/octet-stream",
                "image/png",
                "application/octet-stream"
            ]
        );
    }

    const TOKEN: &str = "k3Jd9_x-Q2mZ8pLw";

    #[test]
    fn a_tracking_id_is_read_from_the_first_well_formed_x_mailtide_id() {
        let read = |headers: &str| {
            let raw = message(headers);
            let token = crate::tracking_id(&raw);
            assert_eq!(token, facts(&raw).tracking_id, "{headers:?}");
            token
        };
        let long = "A".repeat(64);
        assert_eq!(
            read(&format!("X-Mailtide-Id: {TOKEN}\r\n")).as_deref(),
            Some(TOKEN)
        );
        assert_eq!(
            read(&format!("x-MAILTIDE-id:   {TOKEN} \r\n")).as_deref(),
            Some(TOKEN)
        );
        assert_eq!(
            read(&format!("X-Mailtide-Id:\r\n {TOKEN}\r\n")).as_deref(),
            Some(TOKEN)
        );
        assert_eq!(
            read(&format!("X-Mailtide-Id: {long}\r\n")),
            Some(long.clone())
        );
        assert_eq!(
            read(&format!(
                "X-Mailtide-Id: {TOKEN}\r\nX-Mailtide-Id: {long}\r\n"
            ))
            .as_deref(),
            Some(TOKEN),
            "the first one wins"
        );
        assert_eq!(
            read(&format!(
                "X-Mailtide-Id: not valid\r\nX-Mailtide-Id: {TOKEN}\r\n"
            ))
            .as_deref(),
            Some(TOKEN),
            "a malformed one is passed over"
        );
        assert_eq!(crate::TRACKING_HEADER, "X-Mailtide-Id");
    }

    #[test]
    fn a_malformed_or_missing_tracking_id_is_none() {
        let read = |headers: &str| facts(&message(headers)).tracking_id;
        assert_eq!(read("Subject: no token\r\n"), None);
        for bad in [
            "",
            "short_token_15c",
            &"A".repeat(65),
            "k3Jd9 x-Q2mZ8pLw",
            "k3Jd9.x-Q2mZ8pLw",
            "k3Jd9_x-Q2mZ8pL\u{e9}",
            "<k3Jd9_x-Q2mZ8pLw>",
        ] {
            assert_eq!(read(&format!("X-Mailtide-Id: {bad}\r\n")), None, "{bad:?}");
        }
        assert_eq!(crate::tracking_id(b""), None);
    }

    #[test]
    fn every_non_body_part_counts_for_gmail_has_attachment() {
        let signed = b"Content-Type: multipart/signed; protocol=\"application/pgp-signature\"; boundary=\"b\"\r\n\r\n\
            --b\r\nContent-Type: text/html\r\n\r\n<p>hi</p>\r\n\
            --b\r\nContent-Type: application/pgp-signature\r\n\r\nsig\r\n--b--\r\n";
        let calendar = b"Content-Type: multipart/alternative; boundary=\"b\"\r\n\r\n\
            --b\r\nContent-Type: text/html\r\n\r\n<p>invite</p>\r\n\
            --b\r\nContent-Type: text/calendar\r\n\r\nBEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n--b--\r\n";
        let html = "<img src=\"cid:i@x\">";
        // The shapes of message-facts-has-attachment-parity, (a) to (g):
        // Gmail's has:attachment matched all seven.
        let shapes: [(&str, Vec<u8>, bool); 7] = [
            (
                "a",
                with_image(
                    "related",
                    html,
                    "Content-ID: <i@x>\r\nContent-Disposition: inline\r\n",
                ),
                false,
            ),
            (
                "b",
                with_image(
                    "related",
                    html,
                    "Content-ID: <i@x>\r\nContent-Disposition: attachment\r\n",
                ),
                false,
            ),
            (
                "c",
                with_image("mixed", "<p>x</p>", "Content-Disposition: inline\r\n"),
                false,
            ),
            ("d", with_image("mixed", "<p>x</p>", ""), true),
            (
                "e",
                with_image("related", "<p>x</p>", "Content-ID: <i@x>\r\n"),
                false,
            ),
            ("f", signed.to_vec(), false),
            ("g", calendar.to_vec(), true),
        ];
        for (shape, raw, paperclip) in shapes {
            let facts = facts(&raw);
            assert!(facts.gmail_has_attachment(), "({shape})");
            assert_eq!(facts.has_attachment(), paperclip, "({shape})");
        }
        let facts = facts(signed);
        assert_eq!((facts.attached, facts.inline, facts.protocol), (0, 0, 1));
        assert!(
            facts.parts.is_empty(),
            "a signature is counted, never listed"
        );
        let control = super::facts(b"Content-Type: text/html\r\n\r\n<p>only a body</p>\r\n");
        assert!(!control.gmail_has_attachment() && !control.has_attachment());
    }

    /// splitmix64, for inputs that differ on every run of the loop.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: usize) -> usize {
            usize::try_from(self.next() % u64::try_from(n.max(1)).unwrap_or(1)).unwrap_or(0)
        }
    }

    #[test]
    fn facts_of_arbitrary_bytes_never_panic_and_stay_bounded() {
        const PIECES: [&str; 24] = [
            "Content-Type: multipart/mixed; boundary=\"b\"\r\n",
            "Content-Type: multipart/related; boundary=\"b\"\r\n",
            "Content-Type: multipart/signed; boundary=\"b\"\r\n",
            "Content-Type: message/rfc822\r\n",
            "Content-Type: image/png; name=\"x.png\"\r\n",
            "Content-Type: text/html\r\n",
            "Content-Disposition: attachment; filename*=utf-8''%C3%A9%C3\r\n",
            "Content-Disposition: inline\r\n",
            "Content-Transfer-Encoding: base64\r\n",
            "Content-Transfer-Encoding: quoted-printable\r\n",
            "Content-ID: <i@x>\r\n",
            "List-Unsubscribe: <https://x.org/u>, <mailto:u@x.org>\r\n",
            "Bcc: a@x.org, \"b, c\" <b@x.org>\r\n",
            "Date: Thu, 24 Jul 2025 10:00:00 +0530\r\n",
            "From: =?utf-8?B?w4l0w6k=?= <e@x.org>\r\n",
            "\r\n",
            "--b\r\n",
            "--b--\r\n",
            "<img src=\"cid:i@x\">",
            "iVBORw0KGgo=",
            "=E9=\r\n",
            "\u{e9}\0\u{7f}",
            "\n",
            "x",
        ];
        let mut rng = Rng(0x5eed_f00d_cafe_0001);
        for _ in 0..3_000 {
            let mut raw = Vec::new();
            for _ in 0..rng.below(80) {
                if rng.below(8) == 0 {
                    raw.push(u8::try_from(rng.below(256)).unwrap_or(0));
                } else {
                    raw.extend_from_slice(PIECES[rng.below(PIECES.len())].as_bytes());
                }
            }
            let facts = facts(&raw);
            assert!(facts.parts.len() <= MAX_LISTED_PARTS);
            assert!(facts.list_unsubscribe.len() <= MAX_UNSUBSCRIBE_URIS);
            assert!(facts.parts.iter().all(|part| {
                part.filename
                    .as_ref()
                    .is_none_or(|name| name.len() <= MAX_FILENAME_BYTES)
                    && part.part.range().end <= part.part.total
            }));
            let counted = usize::try_from(facts.attached + facts.inline).unwrap_or(usize::MAX);
            assert!(facts.parts.len() <= counted);
            let json = serde_json::to_string(&facts).unwrap_or_default();
            assert!(json.len() <= 16 * 1024 + 6 * raw.len(), "{}", json.len());
        }
    }
}
