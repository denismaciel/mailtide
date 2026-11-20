//! Tests ported from Zero's `mail-search-store.test.ts` (Mail-0/Zero, MIT,
//! commit 4b642dfd9890).
//!
//! Zero caches pages of search results for 30 s, keyed by query, label set
//! and page token, fenced by its history position. mailtide keeps no cache:
//! the cache key's rules become request and page-token rules, and the fence
//! is the page's `mirror_state`, which a host cache would key on.

pub mod search_support;

use std::time::Duration;

use mailtide::ids::LabelId;
use mailtide::revision::ChangeFeed;
use mailtide::search::SearchQuery;
use mailtide_io::search::SearchError;
use mailtide_store::Store as _;
use search_support::{
    Fixture, Options, account, hit_ids, ok, other_account, param, query, refusal,
};

fn labels(ids: &[&str]) -> Vec<LabelId> {
    ids.iter().map(|id| LabelId((*id).into())).collect()
}

#[tokio::test]
async fn label_ids_are_canonical_and_the_query_and_gmails_page_token_go_out_byte_for_byte() {
    let fixture = Fixture::new();
    fixture.sync().await;
    let raw = "  subject:\"hello  world\"  ";
    let first = SearchQuery {
        label_ids: labels(&["Label_1", "INBOX", "Label_1"]),
        ..query(raw)
    };
    fixture.answer_page(&["m1"], Some("opaque+/=token"));
    let page = fixture.search(&first).await.expect("page 1");

    // The same label set in another order continues the same run.
    fixture.answer_page(&["m2"], None);
    let next = SearchQuery {
        label_ids: labels(&["INBOX", "Label_1"]),
        page_token: page.next_page_token.clone(),
        ..query(raw)
    };
    fixture.search(&next).await.expect("page 2");

    let searches = fixture.searches();
    for request in &searches {
        assert_eq!(param(request, "q"), Some(raw), "untrimmed, unfolded");
        let sent: Vec<&str> = request
            .query
            .iter()
            .filter(|(k, _)| k == "labelIds")
            .map(|(_, v)| v.as_str())
            .collect();
        assert_eq!(sent, ["INBOX", "Label_1"], "a sorted set");
    }
    assert_eq!(param(&searches[1], "pageToken"), Some("opaque+/=token"));

    // Unlike Zero, mailtide does not trim: a trimmed query is another one.
    let trimmed = SearchQuery {
        page_token: page.next_page_token,
        ..query(raw.trim())
    };
    assert_eq!(
        fixture.search(&trimmed).await,
        Err(SearchError::InvalidPageToken)
    );
}

#[tokio::test]
async fn a_page_token_is_bound_to_its_account_query_filters_and_page_size() {
    let fixture = Fixture::new();
    fixture.sync().await;
    let first = SearchQuery {
        label_ids: labels(&["INBOX"]),
        limit: 50,
        ..query("from:alice")
    };
    fixture.answer_page(&["m1"], Some("n1"));
    let token = fixture
        .search(&first)
        .await
        .expect("page 1")
        .next_page_token;
    let next = SearchQuery {
        page_token: token,
        ..first
    };

    let elsewhere = fixture
        .runner
        .search(&other_account(), &next, Duration::from_secs(1))
        .await;
    assert_eq!(elsewhere, Err(SearchError::InvalidPageToken), "account");
    let threads = fixture.search_threads(&next).await;
    assert_eq!(threads, Err(SearchError::InvalidPageToken), "scope");
    for (what, other) in [
        (
            "query",
            SearchQuery {
                query: "from:bob".into(),
                ..next.clone()
            },
        ),
        (
            "label filter",
            SearchQuery {
                label_ids: labels(&["INBOX", "Label_1"]),
                ..next.clone()
            },
        ),
        (
            "spam and trash",
            SearchQuery {
                include_spam_trash: true,
                ..next.clone()
            },
        ),
        (
            "page size",
            SearchQuery {
                limit: 51,
                ..next.clone()
            },
        ),
    ] {
        assert_eq!(
            fixture.search(&other).await,
            Err(SearchError::InvalidPageToken),
            "{what}"
        );
    }
    assert_eq!(fixture.searches().len(), 1, "Gmail saw only page 1");
}

#[tokio::test]
async fn mirror_state_is_read_before_gmail_is_asked_so_a_change_during_the_call_marks_the_page_stale()
 {
    let fixture = Fixture::new();
    fixture.gmail().add("m1", &["INBOX"]);
    fixture.sync().await;
    fixture.gmail().add("m2", &["INBOX"]);
    fixture.answer_page(&["m1"], None);
    fixture.gmail().hold();
    let hello = query("hello");

    // A sync commits m2 while the search waits for Gmail's answer.
    let (searched, _) = tokio::join!(fixture.search(&hello), async {
        let outcome = fixture.sync().await;
        fixture.gmail().release();
        outcome
    });

    let page = searched.expect("a page");
    let after = fixture.status().await;
    assert!(page.mirror_state.at < after.state.into());
    let ChangeFeed::Page(changes) = fixture
        .runner
        .store()
        .changes(&account(), page.mirror_state.at, 100)
        .await
        .expect("feed")
    else {
        panic!("the feed is not compacted");
    };
    assert!(
        !changes.changes.is_empty(),
        "changes since the page's mirror_state say it may be stale"
    );
}

#[tokio::test]
async fn a_failed_or_deferred_search_is_an_error_never_an_empty_or_older_page() {
    let fixture = Fixture::with(Options {
        defer_first: 1,
        defer_by: Duration::from_mins(1),
        ..Options::default()
    });
    let deferred = fixture
        .runner
        .search(&account(), &query("x"), Duration::from_secs(1))
        .await;
    assert!(
        matches!(deferred, Err(SearchError::Deferred(_))),
        "{deferred:?}"
    );

    fixture.answer_page(&["m1"], None);
    assert_eq!(
        hit_ids(&fixture.search(&query("x")).await.expect("a page")),
        ["m1"]
    );
    fixture.gmail().answer(Ok(refusal(503, "backendError")));
    fixture.gmail().answer(Ok(ok(&serde_json::json!({}))));
    fixture
        .gmail()
        .answer(Ok(ok(&serde_json::json!({"error": "x"}))));
    for failure in ["5xx", "an empty object", "not a page"] {
        let answer = fixture.search(&query("x")).await;
        assert!(answer.is_err(), "{failure}: {answer:?}");
    }

    // Nothing was kept: the next answer is Gmail's next page, not m1's.
    fixture.answer_page(&["m2"], None);
    assert_eq!(
        hit_ids(&fixture.search(&query("x")).await.expect("a page")),
        ["m2"]
    );
    assert_eq!(fixture.searches().len(), 5);
}
