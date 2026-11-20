//! Tests ported by name from mail-parser's `tests/issues.rs`
//! (stalwartlabs/mail-parser, Apache-2.0 OR MIT, commit 1d449bb5ef17), as
//! `features/message-facts.toml` maps them: the cases the summary and the
//! part ranges rest on. The fixtures are mail-parser's, under
//! `fixtures/mail-parser` with their notice.

use mailtide_mime::{PartDecoder, PartRole, facts, parts};

/// The fixture with LF line endings and with CRLF line endings, with the
/// line ending used.
fn line_endings(raw: &[u8]) -> [(Vec<u8>, &'static str); 2] {
    let lf: Vec<u8> = raw.iter().copied().filter(|&byte| byte != b'\r').collect();
    let mut crlf = Vec::with_capacity(lf.len() * 2);
    for &byte in &lf {
        if byte == b'\n' {
            crlf.push(b'\r');
        }
        crlf.push(byte);
    }
    [(lf, "\n"), (crlf, "\r\n")]
}

fn read(raw: &[u8], part: &mailtide::gmail::PartRef) -> Vec<u8> {
    let start = usize::try_from(part.offset).expect("offset");
    let end = usize::try_from(part.offset + part.length).expect("end");
    let mut decoder = PartDecoder::new(part);
    let mut out = Vec::new();
    decoder.feed(raw.get(start..end).expect("range"), &mut out);
    decoder.finish(&mut out);
    out
}

#[test]
fn inline_images_in_related_are_listed_inline_and_count_no_attachment() {
    for (raw, eol) in line_endings(include_bytes!("fixtures/mail-parser/issues/70.eml")) {
        let facts = facts(&raw);
        assert_eq!((facts.attached, facts.inline), (0, 2));
        assert!(!facts.has_attachment());
        for (part, body) in facts.parts.iter().zip(["A", "B"]) {
            assert!(part.referenced, "shown by the HTML through cid:");
            assert!(part.related);
            assert_eq!(part.filename.as_deref(), Some("image.jpg"));
            assert_eq!(read(&raw, &part.part), format!("{body}{eol}").as_bytes());
            assert_eq!(
                part.size,
                u64::try_from(body.len() + eol.len()).expect("size")
            );
        }
    }
}

#[test]
fn an_inline_image_beside_an_alternative_is_listed_inline() {
    for (raw, _) in line_endings(include_bytes!(
        "fixtures/mail-parser/issues/67-alternative.eml"
    )) {
        let facts = facts(&raw);
        assert_eq!((facts.attached, facts.inline), (0, 1));
        assert_eq!(facts.parts[0].content_type, "image/png");
        assert_eq!(facts.parts[0].filename.as_deref(), Some("version.png"));
        let roles: Vec<PartRole> = parts(&raw).iter().map(|info| info.role).collect();
        assert_eq!(roles, [PartRole::Text, PartRole::Inline]);
    }
}

#[test]
fn a_part_range_is_the_body_as_written_without_the_line_break_before_the_delimiter() {
    let nested_headers = "From: Nested Sender <nested@example.com>\n\
        Subject: Nested message\n\
        MIME-Version: 1.0\n\
        Content-Type: multipart/mixed; boundary=\"nested-mixed\"\n\n";
    let nested_body = "--nested-mixed\n\
        Content-Type: text/plain; charset=us-ascii\n\n\
        Nested text.\n\
        --nested-mixed\n\
        Content-Type: application/octet-stream\n\
        Content-Transfer-Encoding: base64\n\n\
        AAECAw==\n\
        --nested-mixed--";
    let expected = [
        "Plain version.".to_owned(),
        "<p>HTML version.</p>".to_owned(),
        format!("{nested_headers}{nested_body}"),
        "Nested text.".to_owned(),
        "AAECAw==".to_owned(),
    ];
    for (raw, eol) in line_endings(include_bytes!("fixtures/mail-parser/issues/127.eml")) {
        let ranges: Vec<String> = parts(&raw)
            .iter()
            .map(|info| {
                let part = info.part.expect("every part is in the raw message");
                let start = usize::try_from(part.offset).expect("offset");
                let end = usize::try_from(part.offset + part.length).expect("end");
                String::from_utf8_lossy(raw.get(start..end).expect("range")).into_owned()
            })
            .collect();
        let expected: Vec<String> = expected
            .iter()
            .map(|body| body.replace('\n', eol))
            .collect();
        assert_eq!(ranges, expected);
    }
}
