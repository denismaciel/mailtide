//! Case from himalaya's `tests/message_reply.rs`
//! (MIT OR Apache-2.0, commit a5623dcac615), exercised without its CLI.

use mailtide_mime::reply_plan;

#[test]
fn reply_all_from_raw_bytes_lists_the_other_recipients() {
    let raw = b"From: Alice <alice@example.com>\r\n\
                To: Bob <bob@example.com>, Carol <carol@example.com>\r\n\
                Cc: Dan <dan@example.com>, Alice <alice@example.com>\r\n\
                Subject: Trip\r\n\r\nPlans";
    let plan = reply_plan(raw, &["bob@example.com"], true).unwrap();
    assert_eq!(plan.to.len(), 2);
    assert_eq!(plan.to[0].email, "alice@example.com");
    assert_eq!(plan.to[1].email, "carol@example.com");
    assert_eq!(plan.cc.len(), 1);
    assert_eq!(plan.cc[0].email, "dan@example.com");
}
