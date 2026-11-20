//! Tests ported by name from himalaya's `src/shared/attachment/list.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615), as
//! `features/message-facts.toml` maps them. Himalaya shares one walk so a
//! part id means the same in two commands; ours: a summary entry carries the
//! reference `parts` gives the same part.

use mailtide_mime::{PartRole, facts, parts};

#[test]
fn summary_references_are_those_of_the_part_listing() {
    let raw = b"Content-Type: multipart/mixed; boundary=\"m\"\r\n\
        \r\n\
        --m\r\n\
        Content-Type: multipart/related; boundary=\"r\"\r\n\
        \r\n\
        --r\r\n\
        Content-Type: text/html\r\n\
        \r\n\
        <img src=\"cid:logo@example.com\">\r\n\
        --r\r\n\
        Content-Type: image/png\r\n\
        Content-ID: <logo@example.com>\r\n\
        Content-Disposition: inline\r\n\
        \r\n\
        PNG\r\n\
        --r--\r\n\
        --m\r\n\
        Content-Type: application/pdf; name=\"doc.pdf\"\r\n\
        Content-Transfer-Encoding: base64\r\n\
        \r\n\
        JVBERi0=\r\n\
        --m\r\n\
        Content-Type: message/rfc822\r\n\
        \r\n\
        Subject: inner\r\n\
        \r\n\
        inner\r\n\
        --m--\r\n";

    let summary = facts(raw).parts;
    let listing = parts(raw);

    assert_eq!(summary.len(), 3);
    for entry in &summary {
        let part = listing
            .iter()
            .find(|info| info.part == Some(entry.part))
            .expect("the summary's reference in the listing");
        assert_eq!(part.content_type, entry.content_type);
        assert_eq!(part.size, entry.size);
        let role = if entry.is_inline() {
            PartRole::Inline
        } else {
            PartRole::Attached
        };
        assert_eq!(part.role, role);
    }
    let listed: Vec<_> = listing
        .iter()
        .filter(|info| {
            matches!(info.role, PartRole::Inline | PartRole::Attached) && info.depth == 0
        })
        .filter_map(|info| info.part)
        .collect();
    let summarised: Vec<_> = summary.iter().map(|entry| entry.part).collect();
    assert_eq!(listed, summarised);
}
