//! Tests of himalaya's Gmail backend, `src/gmail/backend.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615): the paging
//! tests (`features/search.toml`) and the duplicate create
//! (`features/label-edit.toml`).
//!
//! himalaya emulates numbered pages by listing ids; mailtide hands back
//! Gmail's token, one call per page.

pub mod label_support;
pub mod search_support;

use label_support::{BUDGET, Fault, account, harness, sync};
use mailtide::gmail::{LabelListVisibility, MessageListVisibility, NewLabel};
use mailtide::retry::{AccountProblem, Blocking};
use mailtide::search::SearchQuery;
use mailtide_io::labels::LabelError;
use mailtide_io::runner::Access;
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

/// The reference codes Gmail's 409 to a create as a duplicate and keeps
/// Gmail's words; mailtide answers `NameTaken` with the label that holds
/// the name. Another refusal keeps its own code.
#[tokio::test]
async fn a_create_of_an_existing_label_is_name_taken_and_another_refusal_keeps_its_own_code() {
    let runner = harness(Access::ReadWrite);
    runner.transport().mailbox().create_label("Work");
    sync(&runner).await;
    let work = |name: &str| NewLabel {
        name: name.into(),
        list_visibility: LabelListVisibility::Show,
        message_visibility: MessageListVisibility::Show,
        color: None,
    };
    let duplicate = runner.create_label(&account(), work("WORK"), BUDGET).await;
    let Err(LabelError::NameTaken {
        existing: Some(existing),
    }) = &duplicate
    else {
        panic!("{duplicate:?}");
    };
    assert_eq!(existing.name, "Work");
    assert_eq!(
        duplicate.as_ref().map_err(LabelError::code),
        Err("name_taken")
    );

    runner
        .transport()
        .mailbox()
        .fail_next("POST labels", Fault::Status(403, "forbidden"));
    let forbidden = runner.create_label(&account(), work("Other"), BUDGET).await;
    assert!(
        matches!(&forbidden, Err(LabelError::Refused(f))
            if f.class == Blocking::Account(AccountProblem::ApiAccessDenied)),
        "{forbidden:?}"
    );
    assert_eq!(forbidden.map_err(|e| e.code()), Err("refused"));
}
