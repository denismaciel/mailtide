//! Tests ported by name from mail-parser's `tests/classification.rs`
//! (stalwartlabs/mail-parser, Apache-2.0 OR MIT, commit 1d449bb5ef17), as
//! `features/message-facts.toml` maps them. The fixtures are mail-parser's,
//! under `fixtures/mail-parser` with their notice.

use mailtide_mime::{PartDecoder, PartRole, facts, parts};

/// The fixture with LF line endings and with CRLF line endings.
fn line_endings(raw: &[u8]) -> [Vec<u8>; 2] {
    let lf: Vec<u8> = raw.iter().copied().filter(|&byte| byte != b'\r').collect();
    let mut crlf = Vec::with_capacity(lf.len() * 2);
    for &byte in &lf {
        if byte == b'\n' {
            crlf.push(b'\r');
        }
        crlf.push(byte);
    }
    [lf, crlf]
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
fn the_rfc_8621_example_lists_its_attachments_with_ranges() {
    for raw in line_endings(include_bytes!("fixtures/mail-parser/rfc/007.eml")) {
        let facts = facts(&raw);
        // RFC 8621's attachments: C, F, G, H and J. C is declared inline and
        // F is an undeclared component of multipart/related.
        let listed: Vec<(&str, bool, Vec<u8>)> = facts
            .parts
            .iter()
            .map(|part| {
                (
                    part.content_type.as_str(),
                    part.is_inline(),
                    read(&raw, &part.part),
                )
            })
            .collect();
        let eol: &[u8] = if raw.contains(&b'\r') { b"\r\n" } else { b"\n" };
        let message = [b"Subject: J".as_slice(), eol, eol, b"J"].concat();
        assert_eq!(
            listed,
            [
                ("image/jpeg", true, b"C".to_vec()),
                ("image/jpeg", true, b"F".to_vec()),
                ("image/jpeg", false, b"G".to_vec()),
                ("application/x-excel", false, b"H".to_vec()),
                ("message/rfc822", false, message),
            ]
        );
        assert_eq!((facts.attached, facts.inline), (3, 2));
        let nested: Vec<PartRole> = parts(&raw)
            .iter()
            .filter(|info| info.depth == 1)
            .map(|info| info.role)
            .collect();
        assert_eq!(nested, [PartRole::Text], "J's own body");
    }
}

#[test]
fn an_attached_message_counts_once_and_its_own_attachments_do_not() {
    for raw in line_endings(include_bytes!("fixtures/mail-parser/issues/127.eml")) {
        let facts = facts(&raw);
        // Declared inline, but an attached message is always attached.
        assert_eq!((facts.attached, facts.inline), (1, 0));
        assert_eq!(facts.parts.len(), 1);
        assert_eq!(facts.parts[0].content_type, "message/rfc822");
        assert_eq!(facts.parts[0].disposition.as_deref(), Some("inline"));
        let inner: Vec<(PartRole, u32)> = parts(&raw)
            .iter()
            .filter(|info| info.content_type == "application/octet-stream")
            .map(|info| (info.role, info.depth))
            .collect();
        assert_eq!(
            inner,
            [(PartRole::Attached, 1)],
            "attached in its own message"
        );
    }
}
