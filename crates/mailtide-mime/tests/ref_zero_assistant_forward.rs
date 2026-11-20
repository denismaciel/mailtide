//! Cases ported by name from Zero's `apps/server/src/lib/assistant-forward.test.ts`
//! (MIT, commit 4b642dfd9890). The host uses the original HTML and parts to compose.

use mailtide_mime::forward_plan;

#[test]
fn a_forward_plan_strips_old_prefixes_and_never_lists_bcc() {
    let plan = forward_plan(
        b"From: Author <author@example.com>\r\nTo: main@example.com\r\nCc: copy@example.com\r\nBcc: hidden@example.com\r\nSubject: re: FW: Fwd: Report\r\nContent-Type: text/html\r\n\r\n<p><a href=\"https://example.com/\">Report</a></p>",
    )
    .unwrap();
    assert_eq!(plan.subject, "Fwd: Report");
    assert_eq!(plan.original.to[0].email, "main@example.com");
    assert_eq!(plan.original.cc[0].email, "copy@example.com");
    assert_eq!(
        plan.html.as_deref(),
        Some(r#"<p><a href="https://example.com/">Report</a></p>"#)
    );
    assert!(!format!("{plan:?}").contains("hidden@example.com"));
}

#[test]
fn a_forward_plan_with_an_unreadable_part_is_an_error() {
    for bytes in ["", "aGV!sbG8="] {
        let raw = format!(
            "Content-Type: multipart/mixed; boundary=b\r\n\r\n\
             --b\r\nContent-Type: text/plain\r\n\r\nhello\r\n\
             --b\r\nContent-Type: application/pdf\r\nContent-Transfer-Encoding: base64\r\n\r\n\
             {bytes}\r\n--b--\r\n"
        );
        assert!(forward_plan(raw.as_bytes()).is_err(), "{bytes}");
    }
}

#[test]
fn a_forward_plan_carries_files_and_inline_parts_with_their_content_ids() {
    let plan = forward_plan(
        b"Content-Type: multipart/mixed; boundary=m\r\n\r\n\
          --m\r\nContent-Type: multipart/related; boundary=r\r\n\r\n\
          --r\r\nContent-Type: text/html\r\n\r\n<img src=\"cid:logo\">\r\n\
          --r\r\nContent-Type: image/png\r\nContent-ID: <logo>\r\nContent-Transfer-Encoding: base64\r\n\r\neHl6\r\n--r--\r\n\
          --m\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename=report.pdf\r\n\r\nPDF\r\n--m--\r\n",
    )
    .unwrap();
    let [image, file] = plan.parts.as_slice() else {
        panic!("expected an image and a file");
    };
    assert!(image.inline);
    assert_eq!(image.attachment.content_id.as_deref(), Some("logo"));
    assert_eq!(image.attachment.bytes, b"xyz");
    assert!(!file.inline);
    assert_eq!(file.attachment.filename.as_deref(), Some("report.pdf"));
    assert_eq!(file.attachment.bytes, b"PDF");
}
