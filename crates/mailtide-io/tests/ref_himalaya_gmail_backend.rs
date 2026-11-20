//! Tests of himalaya's Gmail backend, `src/gmail/backend.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615): the paging
//! tests (`features/search.toml`), the duplicate create
//! (`features/label-edit.toml`) and the permanent delete
//! (`features/purge.toml`).
//!
//! himalaya emulates numbered pages by listing ids; mailtide hands back
//! Gmail's token, one call per page. Its permanent delete reads each id
//! first and deletes only those carrying the mailbox label; mailtide's
//! purge (plan `features/purge.toml`) checks the mirror against history
//! since its cursor instead.

pub mod label_support;
pub mod purge_host;
pub mod search_support;

use label_support::{BUDGET, Fault, account, harness, sync};
use mailtide::gmail::{LabelListVisibility, MessageListVisibility, NewLabel};
use mailtide::purge::PurgePolicy;
use mailtide::retry::{AccountProblem, Blocking};
use mailtide::search::SearchQuery;
use mailtide_io::labels::LabelError;
use mailtide_io::runner::Access;
use purge_host::{Host, names};
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

/// A purge host whose mirror holds `messages` (id, labels).
async fn purge_host(messages: &[(&str, &[&str])]) -> Host {
    let host = Host::new(PurgePolicy::TrashAndSpam);
    {
        let mut gmail = host.gmail();
        for (id, labels) in messages {
            gmail.add(id, labels);
        }
    }
    host.mirror_gmail().await;
    host
}

#[tokio::test]
async fn a_message_restored_elsewhere_since_the_last_sync_is_not_purged() {
    let host = purge_host(&[("in", &["TRASH"]), ("restored", &["TRASH"])]).await;
    // The mirror still shows both in the trash; a phone restored one since.
    host.gmail().restore("restored");
    let outcome = host.purge(&["in", "restored"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["in"]);
    let refused: Vec<(&str, &str)> = outcome
        .refused
        .iter()
        .map(|(id, refusal)| (id.0.as_str(), refusal.code()))
        .collect();
    assert_eq!(refused, [("restored", "not_in_trash_or_spam")]);
    assert_eq!(host.gmail().deletes(), ["DELETE in"], "no delete names it");
    assert!(host.in_gmail("restored"));
}

#[tokio::test]
async fn a_single_delete_answered_404_is_already_gone() {
    let host = purge_host(&[("gone", &["TRASH"])]).await;
    // Gmail no longer has it, and history has no record of that (yet).
    host.gmail().messages.remove("gone");
    let outcome = host.purge(&["gone"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["gone"], "a 404 is proof");
    assert!(!host.mirrored("gone"));
    let mirror = host.mirror();
    assert!(
        mirror
            .tombstones
            .contains_key(&mailtide::ids::MessageId("gone".into())),
        "tombstoned as a verified miss"
    );
    assert_eq!(host.deleted_in_feed(), ["gone"]);
}

#[tokio::test]
async fn a_malformed_id_is_refused_by_gmail_and_tombstones_nothing() {
    let host = purge_host(&[("bad", &["TRASH"]), ("good", &["TRASH"])]).await;
    host.gmail().malformed.insert("bad".into());
    let outcome = host.purge(&["bad", "good"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["good"]);
    assert!(matches!(
        outcome.refused.as_slice(),
        [(id, mailtide::purge::Refusal::GmailRefused(detail))]
            if id.0 == "bad" && detail.contains("Invalid id value")
    ));
    assert!(host.mirrored("bad"), "a refusal is not a deletion");
    assert_eq!(host.deleted_in_feed(), ["good"]);
}
