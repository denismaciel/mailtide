//! The token-paging tests of himalaya's local search,
//! `tests/pimdir_search.rs` (pimalaya/himalaya, MIT OR Apache-2.0, commit
//! a5623dcac615), as the paging contract: a page, a token, the next page,
//! no token on the last; a token is refused under another query.

pub mod search_support;

use mailtide::search::SearchQuery;
use mailtide_io::search::SearchError;
use search_support::{Fixture, param, query};

#[tokio::test]
async fn paging_follows_next_page_token_until_there_is_none() {
    let fixture = Fixture::new();
    fixture.answer_page(&["m1", "m2"], Some("p2"));
    fixture.answer_page(&["m3", "m4"], Some("p3"));
    fixture.answer_page(&["m5"], None);
    let first = SearchQuery {
        limit: 2,
        ..query("has:attachment")
    };

    let mut ids = Vec::new();
    let mut next = first.clone();
    loop {
        let page = fixture.search(&next).await.expect("a page");
        ids.extend(page.hits.iter().map(|h| h.message_id.0.clone()));
        match page.next_page_token {
            Some(token) => {
                next = SearchQuery {
                    page_token: Some(token),
                    ..first.clone()
                };
            }
            None => break,
        }
    }

    assert_eq!(ids, ["m1", "m2", "m3", "m4", "m5"]);
    let searches = fixture.searches();
    assert_eq!(searches.len(), 3, "one call per page");
    assert_eq!(param(&searches[0], "pageToken"), None);
    assert_eq!(param(&searches[1], "pageToken"), Some("p2"));
    assert_eq!(param(&searches[2], "pageToken"), Some("p3"));
}

#[tokio::test]
async fn a_page_token_of_another_query_is_refused_before_gmail_is_called() {
    let fixture = Fixture::new();
    fixture.answer_page(&["m1"], Some("p2"));
    let token = fixture
        .search(&query("from:alice"))
        .await
        .expect("page 1")
        .next_page_token;

    let other = fixture
        .search(&SearchQuery {
            page_token: token,
            ..query("from:bob")
        })
        .await;

    assert_eq!(other, Err(SearchError::InvalidPageToken));
    assert_eq!(fixture.searches().len(), 1);
}
