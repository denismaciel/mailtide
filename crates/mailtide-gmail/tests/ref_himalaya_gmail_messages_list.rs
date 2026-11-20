//! Test ported from himalaya's `messages.list` command,
//! `src/gmail/messages/list.rs` (pimalaya/himalaya, MIT OR Apache-2.0,
//! commit a5623dcac615).
//!
//! The reference prints a page as Gmail sent it; mailtide decodes it into a
//! [`SearchListing`] once, at the boundary.

use mailtide::ids::{MessageId, PageToken, ThreadId};
use mailtide::search::{ListedHits, SearchListing, SearchScope};
use mailtide_gmail::decode;
use serde_json::json;

#[test]
fn a_search_page_keeps_ids_estimate_and_token_and_a_page_without_messages_is_empty() {
    let page = json!({
        "messages": [{"id": "m1", "threadId": "t1"}],
        "nextPageToken": "p2",
        "resultSizeEstimate": 3,
    });
    assert_eq!(
        decode::search(SearchScope::Messages, page.to_string().as_bytes()),
        Ok(SearchListing {
            hits: ListedHits::Messages(vec![(MessageId("m1".into()), ThreadId("t1".into()))]),
            estimate: 3,
            next_page_token: Some(PageToken("p2".into())),
        })
    );
    // Gmail sends an empty page without `messages`.
    assert_eq!(
        decode::search(SearchScope::Messages, br#"{"resultSizeEstimate": 0}"#),
        Ok(SearchListing {
            hits: ListedHits::Messages(Vec::new()),
            estimate: 0,
            next_page_token: None,
        })
    );
}
