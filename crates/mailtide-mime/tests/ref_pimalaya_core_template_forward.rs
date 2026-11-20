//! Case from pimalaya core's `email/src/email/message/template/forward/mod.rs`
//! (MIT, commit b3a9640a2b2e). The host owns template layout.

use mailtide_mime::forward_plan;

#[test]
fn a_chain_of_fwd_prefixes_is_trimmed_to_the_subject() {
    let plan = forward_plan(b"Subject: FWD: fw : Fwd: Project\r\n\r\nOriginal").unwrap();
    assert_eq!(plan.subject, "Fwd: Project");
}
