//! The test of himalaya's `src/gmail/drafts/get.rs`, ported by behaviour
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615).
//!
//! himalaya prints the draft as Gmail sent it; mailtide decodes it. What
//! carries over: the request names the draft by id, and an answer with
//! properties mailtide does not model still decodes.

use mailtide::drafts::{DraftFormat, ListedDraft};
use mailtide::ids::{DraftId, MessageId, ThreadId};
use mailtide::labels::set;
use mailtide_gmail::drafts::{fetched, get};
use mailtide_gmail::http::Method;

#[test]
fn a_draft_get_asks_for_the_draft_by_id_and_tolerates_unknown_fields() {
    let request = get(&DraftId("d1".into()), DraftFormat::Minimal);
    assert_eq!(request.method, Method::Get);
    assert_eq!(
        request.path_and_query(),
        "/gmail/v1/users/me/drafts/d1?format=minimal"
    );
    let answer = br#"{"id":"d1","x-vendor":1,"message":{"id":"m1","threadId":"t1","labelIds":["DRAFT"],"x-other":true}}"#;
    let draft = fetched(answer).expect("decodes").expect("a draft");
    assert_eq!(
        draft.draft,
        ListedDraft {
            id: DraftId("d1".into()),
            message: MessageId("m1".into()),
            thread_id: ThreadId("t1".into()),
        }
    );
    assert_eq!(draft.labels, set(["DRAFT"]));
}
