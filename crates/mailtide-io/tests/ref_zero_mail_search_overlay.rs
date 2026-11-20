//! The one test of Zero's `mail-search-overlay.test.ts` (Mail-0/Zero, MIT,
//! commit 4b642dfd9890) that applies to mailtide.
//!
//! Zero parses the Gmail query to drop results that pending local label
//! changes make false; mailtide parses no query. What carries over is the
//! hydration: hits read through `messages` show the overlay for the
//! messages a user just changed, and Gmail's labels for the others.

pub mod search_support;

use mailtide::ids::MessageId;
use mailtide::labels::set;
use mailtide::outbox::{Action, ActionRequest};
use mailtide_store::Store as _;
use search_support::{Fixture, account, query};

#[tokio::test]
async fn only_hits_with_unsettled_actions_hydrate_with_overlaid_labels() {
    let fixture = Fixture::new();
    fixture.gmail().add("m1", &["INBOX", "UNREAD"]);
    fixture.gmail().add("m2", &["INBOX", "UNREAD"]);
    fixture.sync().await;
    let archive = ActionRequest::Messages {
        ids: vec![MessageId("m1".into())],
        action: Action::Archive,
    };
    assert_eq!(fixture.runner.admit(&account(), &archive).await, Ok(1));
    fixture.answer_page(&["m1", "m2"], None);

    let page = fixture.search(&query("in:inbox")).await.expect("a page");
    let ids: Vec<MessageId> = page.hits.iter().map(|h| h.message_id.clone()).collect();
    let views = fixture
        .runner
        .store()
        .messages(&account(), &ids)
        .await
        .expect("views");

    let [changed, untouched] = views.as_slice() else {
        panic!("{views:?}");
    };
    assert_eq!(changed.id.0, "m1");
    assert_eq!(changed.labels, set(["UNREAD"]), "the archive shows at once");
    assert_eq!(changed.observed_labels, set(["INBOX", "UNREAD"]));
    assert_eq!(changed.pending_actions, 1);
    assert_eq!(untouched.labels, untouched.observed_labels);
    assert_eq!(untouched.pending_actions, 0);
}
