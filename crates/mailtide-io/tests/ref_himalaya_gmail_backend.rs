//! The paging tests of himalaya's Gmail backend, `src/gmail/backend.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615).
//!
//! himalaya emulates numbered pages by listing ids; mailtide hands back
//! Gmail's token, one call per page.

pub mod search_support;

use mailtide::search::SearchQuery;
use search_support::{Fixture, param, query};

#[tokio::test]
async fn a_token_after_an_exactly_full_last_page_leads_to_an_empty_final_page() {
    let fixture = Fixture::new();
    let first = SearchQuery {
        limit: 2,
        ..query("from:alice")
    };
    fixture.answer_page(&["m1", "m2"], Some("n1"));
    fixture.answer_page(&[], None);

    let full = fixture.search(&first).await.expect("page 1");
    let last = fixture
        .search(&SearchQuery {
            page_token: full.next_page_token.clone(),
            ..first
        })
        .await
        .expect("page 2");

    assert_eq!(full.hits.len(), 2);
    assert!(full.next_page_token.is_some(), "Gmail's token is returned");
    assert!(last.hits.is_empty());
    assert_eq!(last.next_page_token, None, "the empty page ends the run");
    let searches = fixture.searches();
    assert_eq!(param(&searches[1], "maxResults"), Some("2"));
    assert_eq!(param(&searches[1], "pageToken"), Some("n1"));
}

#[tokio::test]
async fn a_limit_above_gmails_maximum_is_one_page_of_500_with_a_token() {
    let fixture = Fixture::new();
    let ids: Vec<String> = (0..500).map(|n| format!("m{n}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    fixture.answer_page(&ids, Some("n500"));

    let page = fixture
        .search(&SearchQuery {
            limit: 501,
            ..query("in:anywhere")
        })
        .await
        .expect("a page");

    assert_eq!(page.hits.len(), 500);
    assert!(page.next_page_token.is_some());
    let searches = fixture.searches();
    let [request] = searches.as_slice() else {
        panic!("one call: {searches:?}");
    };
    assert_eq!(param(request, "maxResults"), Some("500"));
}
