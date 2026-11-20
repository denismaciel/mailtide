//! Tests ported by name from mail-parser's `src/parser/tests.rs`
//! (stalwartlabs/mail-parser, Apache-2.0 OR MIT, commit 1d449bb5ef17), as
//! `features/message-facts.toml` maps them: the cases the part ranges and
//! the limits rest on. The fixtures are mail-parser's, under
//! `fixtures/mail-parser` with their notice.

use std::fmt::Write as _;

use mailtide::gmail::{PartRef, TransferEncoding};
use mailtide_mime::{PartDecoder, PartRole, facts, parts};

fn with_crlf(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len() * 2);
    for &byte in raw.iter().filter(|&&byte| byte != b'\r') {
        if byte == b'\n' {
            out.push(b'\r');
        }
        out.push(byte);
    }
    out
}

fn body<'r>(raw: &'r [u8], part: &PartRef) -> &'r [u8] {
    let start = usize::try_from(part.offset).expect("offset");
    let end = usize::try_from(part.offset + part.length).expect("end");
    raw.get(start..end).expect("range inside the message")
}

fn read(raw: &[u8], part: &PartRef) -> Vec<u8> {
    let mut decoder = PartDecoder::new(part);
    let mut out = Vec::new();
    decoder.feed(body(raw, part), &mut out);
    decoder.finish(&mut out);
    out
}

#[test]
fn a_single_part_message_has_one_range_from_its_body_to_its_end() {
    let raw = include_bytes!("fixtures/mail-parser/regressions/single-part-offsets.eml");
    let parts = parts(raw);
    assert_eq!(parts.len(), 1);
    let part = parts[0].part.expect("range");
    assert_eq!(part.to_string(), format!("23.5.n.5.{}", raw.len()));
    assert_eq!(body(raw, &part), b"body\n");
    assert_eq!(parts[0].role, PartRole::Text);
}

#[test]
fn parts_of_an_attached_message_have_ranges_in_the_raw_message() {
    let fixture = include_bytes!("fixtures/mail-parser/regressions/nested-inline-message.eml");
    for raw in [fixture.to_vec(), with_crlf(fixture)] {
        let parts = parts(&raw);
        assert_eq!(parts.len(), 3);
        let container = parts[1].part.expect("the attached message");
        let inner = &parts[2];
        assert_eq!(inner.depth, 1);
        assert_eq!(inner.container, Some(container));
        let range = inner.part.expect("a range in the raw message");
        assert_eq!(read(&raw, &range), b"nested body");
        assert!(range.offset > container.offset);
        assert!(range.range().end <= container.range().end);
    }
}

#[test]
fn parts_of_a_transfer_encoded_attached_message_have_no_range() {
    let fixture = include_bytes!("fixtures/mail-parser/regressions/nested-encoded-message.eml");
    for raw in [fixture.to_vec(), with_crlf(fixture)] {
        let parts = parts(&raw);
        let summary: Vec<(PartRole, bool, u32)> = parts
            .iter()
            .map(|info| (info.role, info.part.is_some(), info.depth))
            .collect();
        assert_eq!(
            summary,
            [
                (PartRole::Attached, true, 0),
                (PartRole::Text, false, 1),
                (PartRole::Text, true, 0),
            ]
        );
        let container = parts[0].part.expect("the attached message");
        assert_eq!(container.encoding, TransferEncoding::Base64);
        assert_eq!(parts[1].container, Some(container));
        assert_eq!(read(&raw, &container), b"Subject: inner\n\ninner body\n");
        assert_eq!(parts[1].size, 11, "the size is still known");
    }
}

#[test]
fn a_part_cut_by_a_limit_is_one_leaf_with_a_range() {
    let mut deep = String::new();
    for level in 0..200 {
        let _ = write!(
            deep,
            "Content-Type: multipart/mixed; boundary=\"b{level}\"\n\n--b{level}\n"
        );
    }
    let raw = deep.as_bytes();
    let parts = parts(raw);
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].role, PartRole::Attached);
    assert_eq!(parts[0].content_type, "multipart/mixed");
    let cut = parts[0].part.expect("range");
    assert!(cut.range().end <= u64::try_from(raw.len()).expect("len"));
    let facts = facts(raw);
    assert_eq!((facts.attached, facts.parts.len()), (1, 1));
    assert_eq!(facts.parts[0].part, cut);
}

#[test]
fn a_part_with_a_malformed_encoding_is_marked_irregular() {
    let raw = include_bytes!("fixtures/mail-parser/regressions/decode-problems.eml");
    let codes: Vec<String> = parts(raw)
        .iter()
        .map(|info| {
            let part = info.part.expect("range");
            part.to_string()
                .split('.')
                .nth(2)
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    // base64, broken base64, broken quoted-printable, then bodies without a
    // transfer encoding (x-uuencode is unknown: the body as written).
    assert_eq!(codes, ["b", "B", "Q", "n", "n", "n", "n", "n"]);
    let parts = parts(raw);
    assert_eq!(
        read(raw, &parts[0].part.expect("range")),
        "caf\u{e9}".as_bytes()
    );
    assert_eq!(
        read(raw, &parts[1].part.expect("range")),
        "caf\u{e9}".as_bytes()
    );
    assert_eq!(read(raw, &parts[2].part.expect("range")), b"caf\xe9 =ZZ");
    for info in &parts {
        let part = info.part.expect("range");
        assert_eq!(
            u64::try_from(read(raw, &part).len()).ok(),
            Some(info.size),
            "{part}"
        );
    }
}

#[test]
fn facts_and_parts_never_panic_on_truncated_messages() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mail-parser");
    let mut dirs = vec![root];
    let mut files = Vec::new();
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).expect("fixtures").flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|ext| ext == "eml") {
                files.push(path);
            }
        }
    }
    assert!(files.len() > 150);
    for file in files {
        let raw = with_crlf(&std::fs::read(&file).expect("fixture"));
        // Every 7th byte through the header blocks and first parts, where
        // the structure is decided; every 61st after.
        let ends = (0..raw.len().min(2_048))
            .step_by(7)
            .chain((2_048..raw.len()).step_by(61));
        for end in ends {
            let cut = raw.get(..end).unwrap_or_default();
            let facts = facts(cut);
            for part in &facts.parts {
                assert!(part.part.range().end <= u64::try_from(cut.len()).expect("len"));
            }
            for info in parts(cut) {
                if let Some(part) = info.part {
                    assert_eq!(u64::try_from(read(cut, &part).len()).ok(), Some(info.size));
                }
            }
        }
    }
}
