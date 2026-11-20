//! Cases from himalaya's `tests/message_forward.rs`
//! (MIT OR Apache-2.0, commit a5623dcac615), exercised on a forward plan.

use mailtide_mime::forward_plan;

fn source(headers: &str, body: &str) -> String {
    format!(
        "Subject: Original\r\nContent-Type: multipart/mixed; boundary=m\r\n\r\n\
         --m\r\nContent-Type: text/plain\r\n\r\nOriginal text\r\n\
         --m\r\n{headers}\r\n{body}\r\n--m--\r\n"
    )
}

#[test]
fn a_forward_plan_lists_the_originals_attachment() {
    let raw = source(
        "Content-Type: application/pdf\r\nContent-Disposition: attachment; filename=data.pdf\r\n",
        "attachment body",
    );
    let plan = forward_plan(raw.as_bytes()).unwrap();
    let [part] = plan.parts.as_slice() else {
        panic!("one attachment");
    };
    assert_eq!(part.attachment.filename.as_deref(), Some("data.pdf"));
    assert_eq!(part.attachment.bytes, b"attachment body");
    assert_eq!(plan.quote_text.as_deref(), Some("Original text"));
}

#[test]
fn a_forward_plan_of_a_plain_message_lists_no_parts() {
    let plan = forward_plan(b"Subject: Original\r\n\r\nPlain body").unwrap();
    assert!(plan.parts.is_empty());
    assert_eq!(plan.quote_text.as_deref(), Some("Plain body"));
    assert!(plan.html.is_none());
}

#[test]
fn a_nameless_part_is_named_from_its_media_type() {
    for (kind, name) in [
        ("application/octet-stream", "attachment.bin"),
        ("application/pdf", "attachment.pdf"),
        ("application/x-unknown", "attachment"),
    ] {
        let raw = source(&format!("Content-Type: {kind}\r\n"), "nameless bytes");
        let plan = forward_plan(raw.as_bytes()).unwrap();
        assert_eq!(plan.parts[0].attachment.filename.as_deref(), Some(name));
        assert_eq!(plan.parts[0].attachment.content_type, kind);
    }
}

#[test]
fn an_attached_message_is_named_from_its_subject() {
    for (subject, name) in [
        ("Subject: Quarterly report\r\n", "Quarterly report.eml"),
        ("Subject: ../report\\draft:Q1?\r\n", "_report_draft_Q1_.eml"),
        ("Subject: =?utf-8?Q?caf=C3=A9?=\r\n", "café.eml"),
        ("", "attachment.eml"),
        ("Subject: ...\r\n", "attachment.eml"),
    ] {
        let inner = format!("From: a@example.com\r\n{subject}\r\nNested body");
        let raw = source("Content-Type: message/rfc822\r\n", &inner);
        let plan = forward_plan(raw.as_bytes()).unwrap();
        let attachment = &plan.parts[0].attachment;
        assert_eq!(attachment.filename.as_deref(), Some(name));
        assert_eq!(attachment.content_type, "message/rfc822");
        assert_eq!(attachment.bytes, inner.as_bytes());
    }
}

#[test]
fn a_multipart_container_is_never_a_forward_part() {
    let plan = forward_plan(
        b"Content-Type: multipart/mixed; boundary=outer\r\n\r\n\
          --outer\r\nContent-Type: multipart/mixed; boundary=inner; name=container\r\n\
          Content-Disposition: attachment; filename=container\r\n\r\n\
          --inner\r\nContent-Type: text/plain\r\n\r\nInside\r\n--inner--\r\n--outer--\r\n",
    )
    .unwrap();
    assert!(plan.parts.is_empty());
    assert_eq!(plan.quote_text.as_deref(), Some("Inside"));
}
