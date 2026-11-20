//! The draft calls against a fake Gmail: lifecycle, conflicts, lost
//! answers, drafts changed elsewhere (SPEC §7a).

pub mod fake_gmail;

use std::time::Duration;

use fake_gmail::*;
use mailtide::drafts::{DraftRecorded, DraftStatus, DraftView, DraftWrite, SendEnd};
use mailtide::ids::{DraftId, MessageId, Timestamp};
use mailtide::quota::Budget;
use mailtide_io::drafts::{Deleted, DraftError, DraftRef, LATE_APPLY_MARGIN};
use mailtide_io::runner::{Access, WriteSettings};
use mailtide_store::Store as _;

/// A send's request may be in flight this long (the default timeout).
const REQUEST_TIMEOUT: Duration = Duration::from_mins(1);

/// From a send's start until its request has certainly ended: the latest
/// time it may be issued (the budget, under the timeout), the request
/// timeout and the late-apply margin.
fn send_window() -> Duration {
    BUDGET.min(REQUEST_TIMEOUT) + REQUEST_TIMEOUT + LATE_APPLY_MARGIN
}

/// A draft's message is imported this long after history shows it.
const DRAFT_SETTLE: Duration = Duration::from_mins(2);

async fn create(runner: &TestRunner, n: u32, subject: &str) -> DraftRef {
    runner
        .create_draft(&account(), raw(&token(n), subject), None, BUDGET)
        .await
        .expect("create")
}

async fn update(runner: &TestRunner, draft: &DraftRef, subject: &str) -> DraftRef {
    runner
        .update_draft(
            &account(),
            &draft.id,
            raw(&token(1), subject),
            None,
            None,
            BUDGET,
        )
        .await
        .expect("update")
}

async fn view(runner: &TestRunner, draft: &DraftId) -> Option<DraftView> {
    runner.draft(&account(), draft).await.expect("draft")
}

/// Gmail's drafts.
fn gmail_drafts(setup: &Setup) -> Vec<(String, String)> {
    setup.gmail.lock().draft_list()
}

/// The messages Gmail sent.
fn sent_messages(setup: &Setup) -> Vec<String> {
    setup.gmail.lock().labelled("SENT")
}

fn requests(setup: &Setup) -> Vec<String> {
    setup.gmail.lock().requests.clone()
}

fn entry(kind: &str, draft: &DraftId, message: &MessageId) -> (String, String, String) {
    (kind.to_owned(), draft.0.clone(), message.0.clone())
}

/// A message to the account in a thread of its own; returns its thread.
fn inbound(setup: &Setup) -> String {
    let mut gmail = setup.gmail.lock();
    let id = gmail.add_message(
        None,
        &["INBOX"],
        b"Message-ID: <inbound@example.org>\r\nSubject: Hello\r\n\r\nHi\r\n".to_vec(),
    );
    gmail.messages[&id].thread.clone()
}

#[test]
fn a_draft_update_keeps_its_draft_id() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let first = create(&runner, 1, "a").await;
        let second = update(&runner, &first, "b").await;
        assert_eq!(second.id, first.id);
        assert_ne!(second.message, first.message);
        assert_eq!(
            gmail_drafts(&setup),
            vec![(first.id.0.clone(), second.message.0.clone())]
        );
        let row = view(&runner, &first.id).await.expect("row");
        assert_eq!(row.message_id, second.message, "the index follows");
        assert_eq!(row.state, DraftStatus::Saved);
    });
}

#[test]
fn a_created_draft_is_in_the_index_before_history_arrives() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let history_reads = setup.gmail.sent("GET history").len();
        let draft = create(&runner, 1, "a").await;
        let views = runner.drafts(&account(), None, 10).await.expect("drafts");
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].draft_id, draft.id);
        assert_eq!(views[0].message_id, draft.message);
        assert_eq!(views[0].thread_id, draft.thread_id);
        assert_eq!(views[0].tracking_id.as_deref(), Some(token(1).as_str()));
        assert_eq!(views[0].state, DraftStatus::Saved);
        assert_eq!(
            draft_feed(&runner).await,
            vec![entry("saved:local", &draft.id, &draft.message)]
        );
        assert_eq!(
            setup.gmail.sent("GET history").len(),
            history_reads,
            "no sync ran"
        );
    });
}

#[test]
fn a_draft_without_recipients_is_saved() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let bytes = format!(
            "X-Mailtide-Id: {}\r\nFrom: alice@example.com\r\nSubject: Note to self\r\n\r\nNo recipient yet\r\n",
            token(1)
        )
        .into_bytes();
        let draft = runner
            .create_draft(&account(), bytes.clone(), None, BUDGET)
            .await
            .expect("a draft needs no recipient");
        assert_eq!(setup.gmail.saved_raw(), vec![bytes], "sent as given");
        assert_eq!(
            gmail_drafts(&setup),
            vec![(draft.id.0.clone(), draft.message.0.clone())]
        );
        assert!(view(&runner, &draft.id).await.is_some());
    });
}

#[test]
fn an_update_keeps_the_draft_in_its_thread() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let thread = inbound(&setup);
        let draft = runner
            .create_draft(
                &account(),
                raw(&token(1), "re"),
                Some(thread_id(&thread)),
                BUDGET,
            )
            .await
            .expect("create");
        assert_eq!(draft.thread_id.0, thread);

        // No thread from the caller: the index's is sent.
        let updated = update(&runner, &draft, "re-2").await;
        let thread_sent = |setup: &Setup| {
            let gmail = setup.gmail.lock();
            gmail.saved_bodies.last().expect("body")["message"]["threadId"].clone()
        };
        assert_eq!(thread_sent(&setup), thread.as_str());
        assert_eq!(updated.thread_id.0, thread);
        assert!(
            setup.gmail.sent("GET drafts/").is_empty(),
            "the index knew it"
        );
        let in_gmail = |setup: &Setup, message: &MessageId| {
            setup.gmail.lock().messages[&message.0].thread.clone()
        };
        assert_eq!(in_gmail(&setup, &updated.message), thread);

        // The caller's thread comes first.
        let other = inbound(&setup);
        let moved = runner
            .update_draft(
                &account(),
                &draft.id,
                raw(&token(1), "re-3"),
                Some(thread_id(&other)),
                None,
                BUDGET,
            )
            .await
            .expect("update");
        assert_eq!(thread_sent(&setup), other.as_str());
        assert_eq!(moved.thread_id.0, other);
        assert_eq!(in_gmail(&setup, &moved.message), other);
    });
}

#[test]
fn an_update_with_a_stale_message_id_is_refused() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let first = create(&runner, 1, "a").await;
        let second = update(&runner, &first, "b").await;

        // The index already shows a newer message: still, Gmail is read
        // (the index may be behind) and refuses, nothing written.
        let before = requests(&setup).len();
        let error = runner
            .update_draft(
                &account(),
                &first.id,
                raw(&token(1), "c"),
                None,
                Some(first.message.clone()),
                BUDGET,
            )
            .await
            .expect_err("stale");
        assert_eq!(
            error,
            DraftError::Conflict {
                current: second.message.clone()
            }
        );
        assert_eq!(
            requests(&setup)[before..],
            [format!("GET drafts/{}?minimal", first.id.0)]
        );

        // The index is behind (saved elsewhere): Gmail is read first.
        let before = requests(&setup).len();
        let elsewhere = setup
            .gmail
            .lock()
            .update_draft(
                &first.id.0,
                &raw(&token(1), "elsewhere"),
                Some(&first.thread_id.0),
            )
            .expect("draft");
        let error = runner
            .update_draft(
                &account(),
                &first.id,
                raw(&token(1), "d"),
                None,
                Some(second.message.clone()),
                BUDGET,
            )
            .await
            .expect_err("stale");
        assert_eq!(
            error,
            DraftError::Conflict {
                current: message_id(&elsewhere)
            }
        );
        assert_eq!(
            requests(&setup)[before..],
            [format!("GET drafts/{}?minimal", first.id.0)]
        );
        assert_eq!(
            gmail_drafts(&setup),
            vec![(first.id.0.clone(), elsewhere.clone())]
        );
        assert_eq!(
            view(&runner, &first.id).await.expect("row").state,
            DraftStatus::Stale,
            "the index learned it is behind"
        );

        // The conflict's message is current: an update naming it goes.
        let saved = runner
            .update_draft(
                &account(),
                &first.id,
                raw(&token(1), "e"),
                None,
                Some(message_id(&elsewhere)),
                BUDGET,
            )
            .await
            .expect("update");
        assert_eq!(
            gmail_drafts(&setup),
            vec![(first.id.0.clone(), saved.message.0.clone())]
        );
    });
}

#[test]
fn a_send_with_a_stale_message_id_is_refused() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "reviewed").await;
        let edited = setup
            .gmail
            .lock()
            .update_draft(
                &draft.id.0,
                &raw(&token(1), "edited"),
                Some(&draft.thread_id.0),
            )
            .expect("draft");

        // The index still shows the reviewed message: Gmail is read first.
        let error = runner
            .send_draft(&account(), &draft.id, Some(draft.message.clone()), BUDGET)
            .await
            .expect_err("changed");
        assert_eq!(
            error,
            DraftError::Conflict {
                current: message_id(&edited)
            }
        );
        assert!(setup.gmail.sent("POST drafts/send").is_empty());
        assert!(sent_messages(&setup).is_empty());
        assert_eq!(
            gmail_drafts(&setup),
            vec![(draft.id.0.clone(), edited.clone())]
        );
        let row = view(&runner, &draft.id).await.expect("row");
        assert_eq!(row.state, DraftStatus::Saved, "sendable again");
        assert_eq!(row.message_id, message_id(&edited));

        // Now the index shows the change: Gmail is read, refused, not sent.
        let before = requests(&setup).len();
        let error = runner
            .send_draft(&account(), &draft.id, Some(draft.message.clone()), BUDGET)
            .await
            .expect_err("changed");
        assert_eq!(
            error,
            DraftError::Conflict {
                current: message_id(&edited)
            }
        );
        assert_eq!(
            requests(&setup)[before..],
            [format!("GET drafts/{}?minimal", draft.id.0)]
        );
        let row = view(&runner, &draft.id).await.expect("row");
        assert_eq!(
            row.state,
            DraftStatus::Saved,
            "the refusal ends the attempt"
        );
        assert!(sent_messages(&setup).is_empty());
    });
}

#[test]
fn a_failed_update_leaves_the_draft_and_index_unchanged() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        // A 5xx last: its cool-down would defer the calls after it.
        for status in [400, 403, 503] {
            setup.gmail.fail_next("PUT drafts", Fault::Status(status));
            let error = runner
                .update_draft(
                    &account(),
                    &draft.id,
                    raw(&token(1), "b"),
                    None,
                    None,
                    BUDGET,
                )
                .await
                .expect_err("failed");
            if status == 503 {
                assert!(matches!(error, DraftError::Retryable(_)), "{error:?}");
            } else {
                assert!(matches!(error, DraftError::Refused(_)), "{error:?}");
            }
            let row = view(&runner, &draft.id).await.expect("row");
            assert_eq!(row.message_id, draft.message, "after {status}");
            assert_eq!(row.state, DraftStatus::Saved);
            assert_eq!(
                gmail_drafts(&setup),
                vec![(draft.id.0.clone(), draft.message.0.clone())]
            );
        }
        assert_eq!(
            draft_feed(&runner).await,
            vec![entry("saved:local", &draft.id, &draft.message)]
        );
    });
}

#[test]
fn an_update_retried_after_a_timeout_leaves_one_draft() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        setup
            .gmail
            .fail_next("PUT drafts", Fault::Lost { applied: true });
        let error = runner
            .update_draft(
                &account(),
                &draft.id,
                raw(&token(1), "b"),
                None,
                None,
                BUDGET,
            )
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Stale
        );

        let retried = update(&runner, &draft, "b").await;
        assert_eq!(retried.id, draft.id);
        assert_eq!(
            gmail_drafts(&setup),
            vec![(draft.id.0.clone(), retried.message.0.clone())],
            "one draft"
        );
        let stored = {
            let gmail = setup.gmail.lock();
            assert_eq!(gmail.labelled("DRAFT"), vec![retried.message.0.clone()]);
            String::from_utf8_lossy(&gmail.messages[&retried.message.0].raw).into_owned()
        };
        assert!(stored.contains("Subject: b\r\n"), "{stored}");
        let row = view(&runner, &draft.id).await.expect("row");
        assert_eq!(row.message_id, retried.message);
        assert_eq!(row.state, DraftStatus::Saved);
    });
}

#[test]
fn a_create_that_times_out_returns_no_id_and_refreshes_the_index() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        setup
            .gmail
            .fail_next("POST drafts", Fault::Lost { applied: true });
        let error = runner
            .create_draft(&account(), raw(&token(1), "a"), None, BUDGET)
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        assert!(
            runner
                .drafts(&account(), None, 10)
                .await
                .expect("drafts")
                .is_empty()
        );
        assert!(draft_feed(&runner).await.is_empty());
        // Drop the history record, so only the create's own refresh can
        // find the draft.
        setup.gmail.lock().history.pop();
        let (created, message) = gmail_drafts(&setup).pop().expect("Gmail created it");
        let listings = setup.gmail.listings();

        sync_after_delay(&runner, &setup).await;
        assert_eq!(setup.gmail.listings(), listings + 1);
        let row = view(&runner, &draft_id(&created)).await.expect("adopted");
        assert_eq!(row.message_id.0, message);
        assert_eq!(
            draft_feed(&runner).await,
            vec![entry(
                "saved:remote",
                &draft_id(&created),
                &message_id(&message)
            )]
        );
    });
}

#[test]
fn deleting_a_missing_draft_reports_already_gone() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        setup
            .gmail
            .fail_next("DELETE drafts", Fault::Lost { applied: true });
        let error = runner
            .delete_draft(&account(), &draft.id, BUDGET)
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        assert!(gmail_drafts(&setup).is_empty());

        let retried = runner
            .delete_draft(&account(), &draft.id, BUDGET)
            .await
            .expect("retry");
        assert_eq!(retried, Deleted::AlreadyGone);
        assert!(view(&runner, &draft.id).await.is_none());
        // This call deleted nothing: the feed says gone, not deleted.
        assert_eq!(
            draft_feed(&runner).await,
            vec![
                entry("saved:local", &draft.id, &draft.message),
                entry("removed:gone", &draft.id, &draft.message),
            ]
        );
        // A draft Gmail never had.
        let never = runner
            .delete_draft(&account(), &draft_id("r-never"), BUDGET)
            .await
            .expect("delete");
        assert_eq!(never, Deleted::AlreadyGone);
    });
}

#[test]
fn sending_returns_the_new_message_and_drops_the_draft() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        let sent = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect("send");
        assert_ne!(sent.message_id, draft.message);
        assert_eq!(sent_messages(&setup), vec![sent.message_id.0.clone()]);
        assert_eq!(
            setup.gmail.lock().messages[&sent.message_id.0].thread,
            sent.thread_id.0
        );
        assert!(gmail_drafts(&setup).is_empty());
        assert!(view(&runner, &draft.id).await.is_none());
        assert_eq!(
            draft_feed(&runner).await,
            vec![
                entry("saved:local", &draft.id, &draft.message),
                entry("sent", &draft.id, &sent.message_id),
            ]
        );
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 1);
        assert!(
            setup.gmail.sent("GET drafts/").is_empty(),
            "a first send reads nothing"
        );
    });
}

#[test]
fn a_send_that_times_out_then_finds_the_draft_gone_is_unconfirmed_and_never_resent() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Lost { applied: true });
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        assert!(matches!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Sending { .. }
        ));

        setup.clock.advance(send_window());
        let before = requests(&setup).len();
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("gone");
        assert_eq!(error, DraftError::SendUnconfirmed);
        assert_eq!(
            requests(&setup)[before..],
            [format!("GET drafts/{}?minimal", draft.id.0)]
        );
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 1);
        assert_eq!(sent_messages(&setup).len(), 1, "sent once");
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::SendUnconfirmed
        );

        // And again: still nothing sent.
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("unconfirmed");
        assert_eq!(error, DraftError::SendUnconfirmed);
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 1);
    });
}

#[test]
fn a_send_retried_after_a_request_that_never_arrived_sends_once() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Lost { applied: false });
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        assert!(sent_messages(&setup).is_empty());

        setup.clock.advance(send_window());
        let sent = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect("the draft is still there");
        assert_eq!(sent_messages(&setup), vec![sent.message_id.0.clone()]);
        assert!(gmail_drafts(&setup).is_empty());
        assert!(view(&runner, &draft.id).await.is_none());
        assert_eq!(
            draft_feed(&runner).await.last(),
            Some(&entry("sent", &draft.id, &sent.message_id))
        );
    });
}

#[test]
fn a_send_whose_answer_was_lost_is_confirmed_by_the_sync() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Lost { applied: true });
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        let sent = sent_messages(&setup).pop().expect("Gmail sent it");
        assert!(matches!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Sending { .. }
        ));

        sync(&runner).await;
        assert!(view(&runner, &draft.id).await.is_none());
        assert_eq!(
            draft_feed(&runner).await,
            vec![
                entry("saved:local", &draft.id, &draft.message),
                entry("sent", &draft.id, &message_id(&sent)),
            ]
        );
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 1);
    });
}

#[test]
fn reading_a_sent_draft_reports_not_found() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        let sent = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect("send");
        for fresh in [true, false] {
            let error = runner
                .read_draft(&account(), &draft.id, fresh, BUDGET)
                .await
                .expect_err("sent");
            assert_eq!(error, DraftError::NotFound);
        }
        // The sent message's id is not a draft's.
        let error = runner
            .read_draft(&account(), &DraftId(sent.message_id.0), true, BUDGET)
            .await
            .expect_err("a message");
        assert_eq!(error, DraftError::NotFound);

        let deleted = create(&runner, 2, "b").await;
        runner
            .delete_draft(&account(), &deleted.id, BUDGET)
            .await
            .expect("delete");
        let error = runner
            .read_draft(&account(), &deleted.id, false, BUDGET)
            .await
            .expect_err("deleted");
        assert_eq!(error, DraftError::NotFound);
        assert_eq!(setup.gmail.sent("GET drafts/").len(), 4);
    });
}

#[test]
fn reading_a_draft_uses_the_blob_once_its_message_is_imported() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        let stored = setup.gmail.lock().messages[&draft.message.0].raw.clone();
        let reads = |setup: &Setup| setup.gmail.sent("GET drafts/");
        let raw_read = format!("GET drafts/{}?raw", draft.id.0);

        // Not imported yet: Gmail is asked, in full (60 units).
        let content = runner
            .read_draft(&account(), &draft.id, false, BUDGET)
            .await
            .expect("read");
        assert_eq!(content.raw, stored);
        assert_eq!(content.draft, draft);
        assert_eq!(reads(&setup), vec![raw_read.clone()]);

        // Imported: the blob, no call.
        sync(&runner).await;
        setup.clock.advance(DRAFT_SETTLE);
        sync(&runner).await;
        assert!(
            setup
                .gmail
                .sent(&format!("GET messages/{}?raw", draft.message.0))
                .len()
                == 1,
            "imported"
        );
        let content = runner
            .read_draft(&account(), &draft.id, false, BUDGET)
            .await
            .expect("read");
        assert_eq!(content.raw, stored);
        assert_eq!(content.draft, draft);
        assert_eq!(reads(&setup).len(), 1, "no Gmail call");

        // Fresh asks Gmail anyway.
        runner
            .read_draft(&account(), &draft.id, true, BUDGET)
            .await
            .expect("read");
        assert_eq!(reads(&setup), vec![raw_read.clone(), raw_read.clone()]);

        // A new message, not imported: Gmail again.
        let updated = update(&runner, &draft, "b").await;
        let content = runner
            .read_draft(&account(), &draft.id, false, BUDGET)
            .await
            .expect("read");
        assert_eq!(content.draft, updated);
        assert!(String::from_utf8_lossy(&content.raw).contains("Subject: b\r\n"));
        assert_eq!(reads(&setup).len(), 3);
    });
}

#[test]
fn a_draft_edited_elsewhere_is_repointed_after_a_refresh() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        sync(&runner).await;
        let edited = setup
            .gmail
            .lock()
            .update_draft(
                &draft.id.0,
                &raw(&token(1), "elsewhere"),
                Some(&draft.thread_id.0),
            )
            .expect("draft");

        sync(&runner).await;
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Stale,
            "its message was deleted"
        );
        setup.clock.advance(Duration::from_secs(11));
        sync(&runner).await;
        let row = view(&runner, &draft.id).await.expect("row");
        assert_eq!(row.message_id, message_id(&edited));
        assert_eq!(row.state, DraftStatus::Saved);
        assert_eq!(
            draft_feed(&runner).await,
            vec![
                entry("saved:local", &draft.id, &draft.message),
                entry("saved:remote", &draft.id, &message_id(&edited)),
            ]
        );
    });
}

#[test]
fn a_draft_deleted_elsewhere_is_removed_after_a_refresh() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let deleted = create(&runner, 1, "deleted").await;
        let sent = create(&runner, 2, "sent").await;
        sync(&runner).await;
        {
            let mut gmail = setup.gmail.lock();
            assert!(gmail.delete_draft(&deleted.id.0));
            gmail.send_draft(&sent.id.0).expect("sent elsewhere");
        }

        sync_after_delay(&runner, &setup).await;
        assert!(
            runner
                .drafts(&account(), None, 10)
                .await
                .expect("drafts")
                .is_empty()
        );
        let feed = draft_feed(&runner).await;
        assert_eq!(
            feed[2..],
            [
                entry("removed:gone", &deleted.id, &deleted.message),
                entry("removed:gone", &sent.id, &sent.message),
            ]
        );
    });
}

#[test]
fn two_writers_on_one_draft_converge_after_a_refresh() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        sync(&runner).await;

        // Gmail applies our update, then the other writer's; our answer
        // arrives, and is recorded, after both.
        let hold = Hold::default();
        setup
            .gmail
            .fail_next("PUT drafts", Fault::Held(hold.clone()));
        let (ours, theirs) = tokio::join!(update(&runner, &draft, "ours"), async {
            hold.reached().await;
            let theirs = setup
                .gmail
                .lock()
                .update_draft(
                    &draft.id.0,
                    &raw(&token(1), "theirs"),
                    Some(&draft.thread_id.0),
                )
                .expect("draft");
            hold.release();
            theirs
        });
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").message_id,
            ours.message,
            "the late record"
        );
        assert_eq!(
            gmail_drafts(&setup),
            vec![(draft.id.0.clone(), theirs.clone())]
        );

        sync(&runner).await;
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Stale,
            "the losing message's tombstone"
        );
        setup.clock.advance(Duration::from_secs(11));
        sync(&runner).await;
        let row = view(&runner, &draft.id).await.expect("row");
        assert_eq!(row.message_id, message_id(&theirs));
        assert_eq!(row.state, DraftStatus::Saved);
        assert_eq!(
            draft_feed(&runner).await.last(),
            Some(&entry("saved:remote", &draft.id, &message_id(&theirs)))
        );
    });
}

#[test]
fn history_expiry_refreshes_the_draft_index() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        sync_after_delay(&runner, &setup).await;

        // Nothing changed, yet the reconciliation ends with a listing.
        let listings = setup.gmail.listings();
        setup.gmail.fail_next("GET history", Fault::Status(404));
        sync_after_delay(&runner, &setup).await;
        assert_eq!(setup.gmail.listings(), listings + 1);

        // Changes in a window whose history Gmail no longer has.
        let (remote, message) = {
            let mut gmail = setup.gmail.lock();
            gmail.delete_draft(&draft.id.0);
            let created = gmail.create_draft(&raw(&token(2), "elsewhere"), None);
            gmail.history.clear();
            created
        };
        setup.gmail.fail_next("GET history", Fault::Status(404));
        sync_after_delay(&runner, &setup).await;
        let views = runner.drafts(&account(), None, 10).await.expect("drafts");
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].draft_id.0, remote);
        assert_eq!(views[0].message_id.0, message);
        let feed = draft_feed(&runner).await;
        assert!(
            feed.contains(&entry("removed:gone", &draft.id, &draft.message)),
            "{feed:?}"
        );
        assert!(
            feed.contains(&entry(
                "saved:remote",
                &draft_id(&remote),
                &message_id(&message)
            )),
            "{feed:?}"
        );
    });
}

#[test]
fn draft_writes_are_refused_in_read_only_mode() {
    block_on(async {
        let (runner, setup) = runner_with(Access::ReadOnly, Budget::GMAIL_DEFAULT);
        synced(&runner).await;
        let (remote, message) = setup
            .gmail
            .lock()
            .create_draft(&raw(&token(1), "elsewhere"), None);
        sync_after_delay(&runner, &setup).await;
        let row = view(&runner, &draft_id(&remote))
            .await
            .expect("still mirrored");
        assert_eq!(row.message_id.0, message);

        let before = requests(&setup);
        let remote = draft_id(&remote);
        let created = runner
            .create_draft(&account(), raw(&token(2), "a"), None, BUDGET)
            .await;
        assert_eq!(created, Err(DraftError::ReadOnly));
        let updated = runner
            .update_draft(&account(), &remote, raw(&token(1), "b"), None, None, BUDGET)
            .await;
        assert_eq!(updated, Err(DraftError::ReadOnly));
        let deleted = runner.delete_draft(&account(), &remote, BUDGET).await;
        assert_eq!(deleted, Err(DraftError::ReadOnly));
        let sent = runner.send_draft(&account(), &remote, None, BUDGET).await;
        assert_eq!(sent, Err(DraftError::ReadOnly));
        assert_eq!(requests(&setup), before, "no Gmail call");
        assert_eq!(
            view(&runner, &remote).await.expect("row").state,
            DraftStatus::Saved
        );
        // Reading is allowed.
        runner
            .read_draft(&account(), &remote, true, BUDGET)
            .await
            .expect("read");
    });
}

#[test]
fn a_deferred_draft_write_sends_nothing() {
    block_on(async {
        let budget = Budget {
            account_per_minute: 1,
            ..Budget::GMAIL_DEFAULT
        };
        let (runner, setup) = runner_with(Access::ReadWrite, budget);
        runner.track(&account()).await.expect("track");
        let deferred = |error: DraftError| match error {
            DraftError::Deferred(deferred) => deferred.until,
            other => panic!("{other:?}"),
        };
        let created = runner
            .create_draft(&account(), raw(&token(1), "a"), None, BUDGET)
            .await
            .expect_err("deferred");
        assert!(deferred(created) > Timestamp(0));
        let updated = runner
            .update_draft(
                &account(),
                &draft_id("r-1"),
                raw(&token(1), "b"),
                Some(thread_id("t-1")),
                None,
                BUDGET,
            )
            .await
            .expect_err("deferred");
        assert!(deferred(updated) > Timestamp(0));
        let deleted = runner
            .delete_draft(&account(), &draft_id("r-1"), BUDGET)
            .await
            .expect_err("deferred");
        assert!(deferred(deleted) > Timestamp(0));

        assert!(requests(&setup).is_empty(), "no request");
        assert!(
            runner
                .drafts(&account(), None, 10)
                .await
                .expect("drafts")
                .is_empty(),
            "no record"
        );
        assert!(draft_feed(&runner).await.is_empty());
    });
}

#[test]
fn a_message_over_the_size_limit_is_refused_before_calling_gmail() {
    block_on(async {
        let limit = 256;
        let (runner, setup) = runner_writing(
            Access::ReadWrite,
            Budget::GMAIL_DEFAULT,
            WriteSettings {
                max_message_bytes: limit,
                ..WriteSettings::default()
            },
        );
        synced(&runner).await;
        let sized = |size: usize| {
            let mut bytes = raw(&token(1), "sized");
            bytes.resize(size, b'x');
            bytes
        };

        let error = runner
            .create_draft(&account(), sized(limit + 1), None, BUDGET)
            .await
            .expect_err("too large");
        assert_eq!(error, DraftError::TooLarge { limit });
        assert!(setup.gmail.sent("POST drafts").is_empty(), "no call");

        // Up to the limit it goes, in the JSON body.
        let draft = runner
            .create_draft(&account(), sized(limit), None, BUDGET)
            .await
            .expect("at the limit");
        assert_eq!(setup.gmail.saved_raw(), vec![sized(limit)]);

        let error = runner
            .update_draft(&account(), &draft.id, sized(limit + 1), None, None, BUDGET)
            .await
            .expect_err("too large");
        assert_eq!(error, DraftError::TooLarge { limit });
        assert!(setup.gmail.sent("PUT drafts").is_empty(), "no call");
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").message_id,
            draft.message
        );
    });
}

#[test]
fn a_refused_draft_write_does_not_block_the_account() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        setup.gmail.fail_next("POST drafts", Fault::Status(403));
        let error = runner
            .create_draft(&account(), raw(&token(1), "a"), None, BUDGET)
            .await
            .expect_err("no compose scope");
        assert!(matches!(error, DraftError::Refused(_)), "{error:?}");

        let draft = create(&runner, 1, "a").await;
        setup.gmail.fail_next("PUT drafts", Fault::Status(400));
        let error = runner
            .update_draft(
                &account(),
                &draft.id,
                raw(&token(1), "b"),
                None,
                None,
                BUDGET,
            )
            .await
            .expect_err("bad request");
        assert!(matches!(error, DraftError::Refused(_)), "{error:?}");
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Status(403));
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("refused");
        assert!(matches!(error, DraftError::Refused(_)), "{error:?}");
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Saved,
            "nothing was sent"
        );

        // The account keeps syncing (`sync` fails on a blocked account).
        let thread = inbound(&setup);
        sync(&runner).await;
        let message = setup
            .gmail
            .lock()
            .messages
            .iter()
            .find_map(|(id, m)| (m.thread == thread).then(|| message_id(id)));
        let mirrored = runner
            .store()
            .messages(&account(), &[message.expect("inbound")])
            .await
            .expect("messages");
        assert_eq!(mirrored.len(), 1, "the new message is mirrored");
    });
}

#[test]
fn a_second_send_while_one_is_in_flight_is_refused() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        let started = setup.clock.now();
        let hold = Hold::default();
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Held(hold.clone()));
        let account = account();
        let (first, second) = tokio::join!(
            runner.send_draft(&account, &draft.id, None, BUDGET),
            async {
                hold.reached().await;
                let second = runner.send_draft(&account, &draft.id, None, BUDGET).await;
                hold.release();
                second
            }
        );
        assert_eq!(
            second,
            Err(DraftError::SendInProgress {
                retry_after: started.plus(send_window())
            })
        );
        let first = first.expect("the first send");
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 1);
        assert!(setup.gmail.sent("GET drafts/").is_empty());
        assert_eq!(sent_messages(&setup), vec![first.message_id.0]);
    });
}

#[test]
fn a_send_retry_waits_out_the_first_request_and_reads_the_draft_before_sending() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        let started = setup.clock.now();
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Lost { applied: false });
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");

        // The first request may still be in flight.
        setup.clock.advance(send_window() / 2);
        let before = requests(&setup).len();
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("in flight");
        assert_eq!(
            error,
            DraftError::SendInProgress {
                retry_after: started.plus(send_window())
            }
        );
        assert_eq!(requests(&setup).len(), before, "no call");

        // It has timed out: the draft is read, then sent.
        setup.clock.advance(send_window() / 2);
        let sent = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect("send");
        assert_eq!(
            requests(&setup)[before..],
            [
                format!("GET drafts/{}?minimal", draft.id.0),
                "POST drafts/send".to_owned()
            ]
        );
        assert_eq!(sent_messages(&setup), vec![sent.message_id.0.clone()]);

        // Edited meanwhile: not sent.
        let changed = create(&runner, 2, "b").await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Lost { applied: false });
        let error = runner
            .send_draft(&account(), &changed.id, None, BUDGET)
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        let edited = setup
            .gmail
            .lock()
            .update_draft(
                &changed.id.0,
                &raw(&token(2), "edited"),
                Some(&changed.thread_id.0),
            )
            .expect("draft");
        setup.clock.advance(send_window());
        let error = runner
            .send_draft(&account(), &changed.id, None, BUDGET)
            .await
            .expect_err("changed");
        assert_eq!(
            error,
            DraftError::Conflict {
                current: message_id(&edited)
            }
        );
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 3);
        assert_eq!(sent_messages(&setup).len(), 1);
        let row = view(&runner, &changed.id).await.expect("row");
        assert_eq!(row.message_id, message_id(&edited));
        assert_eq!(row.state, DraftStatus::Saved);
    });
}

#[test]
fn an_update_reads_the_thread_when_neither_caller_nor_index_knows_it() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let thread = inbound(&setup);
        let (remote, _) = setup
            .gmail
            .lock()
            .create_draft(&raw(&token(1), "reply"), Some(&thread));
        let before = requests(&setup).len();

        let updated = runner
            .update_draft(
                &account(),
                &draft_id(&remote),
                raw(&token(1), "reply-2"),
                None,
                None,
                BUDGET,
            )
            .await
            .expect("update");
        assert_eq!(
            requests(&setup)[before..],
            [
                format!("GET drafts/{remote}?minimal"),
                format!("PUT drafts/{remote}")
            ]
        );
        assert_eq!(updated.thread_id.0, thread);
        let gmail = setup.gmail.lock();
        assert_eq!(
            gmail.saved_bodies.last().expect("body")["message"]["threadId"],
            thread.as_str()
        );
        assert_eq!(gmail.messages[&updated.message.0].thread, thread);
    });
}

#[test]
fn a_write_answer_without_ids_is_completed_by_a_get() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        setup.gmail.fail_next("POST drafts", Fault::NoIds);
        let before = requests(&setup).len();
        let draft = create(&runner, 1, "a").await;
        assert_eq!(
            requests(&setup)[before..],
            [
                "POST drafts".to_owned(),
                format!("GET drafts/{}?minimal", draft.id.0)
            ]
        );
        assert_eq!(
            gmail_drafts(&setup),
            vec![(draft.id.0.clone(), draft.message.0.clone())]
        );
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").message_id,
            draft.message
        );

        // The read fails too: the outcome is unknown, and a refresh runs.
        setup.gmail.fail_next("PUT drafts", Fault::NoIds);
        setup.gmail.fail_next("GET drafts/", Fault::Status(500));
        let error = runner
            .update_draft(
                &account(),
                &draft.id,
                raw(&token(1), "b"),
                None,
                None,
                BUDGET,
            )
            .await
            .expect_err("unknown");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Stale
        );
        let (_, current) = gmail_drafts(&setup).pop().expect("draft");
        sync_after_delay(&runner, &setup).await;
        let row = view(&runner, &draft.id).await.expect("row");
        assert_eq!(row.message_id.0, current);
        assert_eq!(row.state, DraftStatus::Saved);
    });
}

#[test]
fn a_periodic_refresh_adopts_a_draft_history_never_showed() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        // The bootstrap ends with a listing.
        sync_after_delay(&runner, &setup).await;
        assert_eq!(setup.gmail.listings(), 1);
        let (remote, message) = {
            let mut gmail = setup.gmail.lock();
            let created = gmail.create_draft(&raw(&token(1), "unseen"), None);
            gmail.history.pop();
            created
        };
        sync_after_delay(&runner, &setup).await;
        assert!(
            view(&runner, &draft_id(&remote)).await.is_none(),
            "nothing triggered a refresh"
        );

        setup.clock.advance(Duration::from_mins(15));
        sync(&runner).await;
        let row = view(&runner, &draft_id(&remote)).await.expect("adopted");
        assert_eq!(row.message_id.0, message);
        assert_eq!(
            draft_feed(&runner).await,
            vec![entry(
                "saved:remote",
                &draft_id(&remote),
                &message_id(&message)
            )]
        );
    });
}

#[test]
fn a_draft_write_without_one_valid_tracking_id_is_refused() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        let valid = token(1);
        let refused = [
            "Subject: none\r\n\r\nbody\r\n".to_owned(),
            format!(
                "X-Mailtide-Id: {valid}\r\nX-Mailtide-Id: {}\r\n\r\nbody\r\n",
                token(2)
            ),
            format!("X-Mailtide-Id: {valid}\r\nX-Mailtide-Id: short\r\n\r\nbody\r\n"),
            "X-Mailtide-Id: short\r\n\r\nbody\r\n".to_owned(),
            "X-Mailtide-Id: not/a+valid=token!\r\n\r\nbody\r\n".to_owned(),
            format!("Subject: in the body\r\n\r\nX-Mailtide-Id: {valid}\r\n"),
        ];
        let before = requests(&setup).len();
        for bytes in refused {
            let created = runner
                .create_draft(&account(), bytes.clone().into_bytes(), None, BUDGET)
                .await
                .expect_err(&bytes);
            assert!(matches!(created, DraftError::Invalid(_)), "{bytes}");
            let updated = runner
                .update_draft(
                    &account(),
                    &draft.id,
                    bytes.clone().into_bytes(),
                    None,
                    None,
                    BUDGET,
                )
                .await
                .expect_err(&bytes);
            assert!(matches!(updated, DraftError::Invalid(_)), "{bytes}");
        }
        assert_eq!(requests(&setup).len(), before, "no Gmail call");
        assert_eq!(setup.gmail.saved_raw().len(), 1);
        let views = runner.drafts(&account(), None, 10).await.expect("drafts");
        assert_eq!(views.len(), 1, "no record");
        assert_eq!(views[0].message_id, draft.message);
        assert_eq!(draft_feed(&runner).await.len(), 1);
    });
}

#[test]
fn an_update_carrying_another_tracking_id_is_refused() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        let error = runner
            .update_draft(
                &account(),
                &draft.id,
                raw(&token(2), "b"),
                None,
                None,
                BUDGET,
            )
            .await
            .expect_err("another token");
        assert!(matches!(error, DraftError::Invalid(_)), "{error:?}");
        assert!(setup.gmail.sent("PUT drafts").is_empty(), "no call");
        let row = view(&runner, &draft.id).await.expect("row");
        assert_eq!(row.message_id, draft.message);
        assert_eq!(row.tracking_id.as_deref(), Some(token(1).as_str()));
        // Its own token goes.
        update(&runner, &draft, "b").await;
    });
}

#[test]
fn a_drafts_message_id_is_read_from_gmails_copy() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        let rfc822 = |setup: &Setup, message: &MessageId| {
            setup
                .gmail
                .lock()
                .rfc822_message_id(&message.0)
                .expect("Gmail's Message-ID")
        };
        let imported = async |runner: &TestRunner| {
            sync(runner).await;
            setup.clock.advance(DRAFT_SETTLE);
            sync(runner).await;
        };
        assert_eq!(
            view(&runner, &draft.id)
                .await
                .expect("row")
                .rfc822_message_id,
            None,
            "unknown until imported"
        );
        imported(&runner).await;
        let first = rfc822(&setup, &draft.message);
        assert_ne!(first, "<client-a@example.com>", "Gmail replaced it");
        assert_eq!(
            view(&runner, &draft.id)
                .await
                .expect("row")
                .rfc822_message_id,
            Some(first.clone())
        );

        let updated = update(&runner, &draft, "b").await;
        assert_eq!(
            view(&runner, &draft.id)
                .await
                .expect("row")
                .rfc822_message_id,
            None,
            "a new message, not imported"
        );
        imported(&runner).await;
        let second = rfc822(&setup, &updated.message);
        assert_ne!(second, first);
        assert_ne!(second, "<client-b@example.com>");
        assert_eq!(
            view(&runner, &draft.id)
                .await
                .expect("row")
                .rfc822_message_id,
            Some(second)
        );
    });
}

// ------------------------------------------------------------- review fixes

/// The state of a row being sent: its attempt's start.
async fn sending_since(runner: &TestRunner, draft: &DraftId) -> Timestamp {
    match view(runner, draft).await.expect("row").state {
        DraftStatus::Sending { started, .. } => started,
        other => panic!("not sending: {other:?}"),
    }
}

#[test]
fn a_send_held_before_its_request_is_fenced_until_it_has_certainly_ended() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        let started = setup.clock.now();
        // A short budget: the request must be issued within 50 ms or never.
        let budget = Duration::from_millis(50);
        // The send's pre-read is held: its request is not issued yet.
        let hold = Hold::default();
        setup
            .gmail
            .fail_next("GET drafts/", Fault::Held(hold.clone()));
        let account = account();
        let (first, (second, listed)) = tokio::join!(
            runner.send_draft(&account, &draft.id, Some(draft.message.clone()), budget),
            async {
                hold.reached().await;
                // Past the time the request may be issued...
                std::thread::sleep(Duration::from_millis(100));
                // ...and past the request timeout counted from the start.
                setup
                    .clock
                    .advance(REQUEST_TIMEOUT + Duration::from_secs(1));
                let second = runner.send_draft(&account, &draft.id, None, BUDGET).await;
                // A listing in that window still shows the draft: it proves
                // nothing yet.
                let now = setup.clock.now();
                runner
                    .store()
                    .record_draft(&account, &DraftWrite::Refresh { at: now }, now)
                    .await
                    .expect("refresh");
                let listings = setup.gmail.listings();
                sync(&runner).await;
                assert_eq!(setup.gmail.listings(), listings + 1, "listed");
                let listed = view(&runner, &draft.id).await.expect("row").state;
                hold.release();
                (second, listed)
            }
        );
        assert_eq!(
            second,
            Err(DraftError::SendInProgress {
                retry_after: started
                    .plus(budget)
                    .plus(REQUEST_TIMEOUT)
                    .plus(LATE_APPLY_MARGIN)
            })
        );
        assert!(matches!(listed, DraftStatus::Sending { .. }), "{listed:?}");
        // The first call's request would go out after its time: not issued.
        assert!(matches!(first, Err(DraftError::Deferred(_))), "{first:?}");
        assert!(setup.gmail.sent("POST drafts/send").is_empty());
        assert!(sent_messages(&setup).is_empty());
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Saved
        );
    });
}

#[test]
fn a_send_answered_with_an_undecodable_success_is_an_unknown_outcome() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Undecodable);
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("unknown");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        assert_eq!(sent_messages(&setup).len(), 1, "Gmail did send it");
        sending_since(&runner, &draft.id).await;
        // Never resent; the sync confirms it.
        sync(&runner).await;
        assert!(view(&runner, &draft.id).await.is_none());
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 1);
    });
}

#[test]
fn a_send_answered_5xx_or_not_at_all_stays_sending() {
    for fault in [
        Fault::Status(500),
        Fault::Status(503),
        Fault::Lost { applied: false },
    ] {
        block_on(async {
            let (runner, setup) = runner();
            synced(&runner).await;
            let draft = create(&runner, 1, "a").await;
            setup.gmail.fail_next("POST drafts/send", fault.clone());
            let error = runner
                .send_draft(&account(), &draft.id, None, BUDGET)
                .await
                .expect_err("failed");
            assert!(
                matches!(error, DraftError::OutcomeUnknown(_)),
                "{fault:?}: {error:?}"
            );
            sending_since(&runner, &draft.id).await;
            let again = runner.send_draft(&account(), &draft.id, None, BUDGET).await;
            assert!(
                matches!(again, Err(DraftError::SendInProgress { .. })),
                "{fault:?}: {again:?}"
            );
        });
    }
}

#[test]
fn a_send_rate_limited_or_deferred_returns_to_saved() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Status(429));
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("rate limited");
        assert!(matches!(error, DraftError::Retryable(_)), "{error:?}");
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Saved
        );
        // The rate limit's cool-down defers the next send at the gate.
        let before = setup.gmail.sent("POST drafts/send").len();
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("deferred");
        assert!(matches!(error, DraftError::Deferred(_)), "{error:?}");
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), before);
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Saved
        );
        assert!(sent_messages(&setup).is_empty());
    });
}

#[test]
fn a_send_that_never_connected_returns_to_saved() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        setup.gmail.fail_next("POST drafts/send", Fault::NotSent);
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("not sent");
        assert!(matches!(error, DraftError::Retryable(_)), "{error:?}");
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Saved
        );
        let sent = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect("sent at once");
        assert_eq!(sent_messages(&setup), vec![sent.message_id.0]);
    });
}

#[test]
fn after_an_unknown_send_the_draft_can_be_edited_or_deleted_once_its_request_ended() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        // Not sent after all: an update reads the draft, finds it, goes on.
        let draft = create(&runner, 1, "a").await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Lost { applied: false });
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        let early = runner
            .update_draft(
                &account(),
                &draft.id,
                raw(&token(1), "b"),
                None,
                None,
                BUDGET,
            )
            .await;
        assert!(
            matches!(early, Err(DraftError::SendInProgress { .. })),
            "{early:?}"
        );
        setup.clock.advance(send_window());
        let before = requests(&setup).len();
        let updated = update(&runner, &draft, "b").await;
        assert_eq!(
            requests(&setup)[before..],
            [
                format!("GET drafts/{}?minimal", draft.id.0),
                format!("PUT drafts/{}", draft.id.0)
            ]
        );
        let row = view(&runner, &draft.id).await.expect("row");
        assert_eq!(row.state, DraftStatus::Saved);
        assert_eq!(row.message_id, updated.message);

        // Sent after all: a delete reads the draft, finds it gone.
        let other = create(&runner, 2, "c").await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Lost { applied: true });
        let error = runner
            .send_draft(&account(), &other.id, None, BUDGET)
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        setup.clock.advance(send_window());
        let before = requests(&setup).len();
        let error = runner
            .delete_draft(&account(), &other.id, BUDGET)
            .await
            .expect_err("unconfirmed");
        assert_eq!(error, DraftError::SendUnconfirmed);
        assert_eq!(
            requests(&setup)[before..],
            [format!("GET drafts/{}?minimal", other.id.0)]
        );
        assert_eq!(
            view(&runner, &other.id).await.expect("row").state,
            DraftStatus::SendUnconfirmed
        );
    });
}

#[test]
fn a_stale_row_is_read_before_it_is_sent() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        let elsewhere = setup
            .gmail
            .lock()
            .update_draft(
                &draft.id.0,
                &raw(&token(1), "elsewhere"),
                Some(&draft.thread_id.0),
            )
            .expect("draft");
        sync(&runner).await;
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").state,
            DraftStatus::Stale
        );
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("changed");
        assert_eq!(
            error,
            DraftError::Conflict {
                current: message_id(&elsewhere)
            }
        );
        assert!(setup.gmail.sent("POST drafts/send").is_empty());
        let row = view(&runner, &draft.id).await.expect("row");
        assert_eq!(row.state, DraftStatus::Saved);
        assert_eq!(row.message_id, message_id(&elsewhere));
        // Now the host has seen it: sent.
        runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect("sent");

        // Deleted elsewhere: the read's 404 is NotFound, nothing sent.
        let gone = create(&runner, 2, "b").await;
        assert!(setup.gmail.lock().delete_draft(&gone.id.0));
        sync(&runner).await;
        assert_eq!(
            view(&runner, &gone.id).await.expect("row").state,
            DraftStatus::Stale
        );
        let error = runner
            .send_draft(&account(), &gone.id, None, BUDGET)
            .await
            .expect_err("gone");
        assert_eq!(error, DraftError::NotFound);
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 1);
        assert!(view(&runner, &gone.id).await.is_none());
    });
}

#[test]
fn a_token_another_draft_holds_is_refused() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        create(&runner, 1, "a").await;
        // A draft made elsewhere: its row has no token until its import.
        let (made, _) = setup
            .gmail
            .lock()
            .create_draft(&raw(&token(3), "elsewhere"), None);
        sync_after_delay(&runner, &setup).await;
        let made = draft_id(&made);
        assert_eq!(view(&runner, &made).await.expect("row").tracking_id, None);
        let before = requests(&setup).len();
        let error = runner
            .create_draft(&account(), raw(&token(1), "b"), None, BUDGET)
            .await
            .expect_err("taken");
        assert!(matches!(error, DraftError::Invalid(_)), "{error:?}");
        let error = runner
            .update_draft(&account(), &made, raw(&token(1), "c"), None, None, BUDGET)
            .await
            .expect_err("taken");
        assert!(matches!(error, DraftError::Invalid(_)), "{error:?}");
        assert_eq!(requests(&setup).len(), before, "no call");
        runner
            .update_draft(&account(), &made, raw(&token(3), "d"), None, None, BUDGET)
            .await
            .expect("its own token");
    });
}

#[test]
fn an_empty_thread_id_counts_as_absent() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = runner
            .create_draft(&account(), raw(&token(1), "a"), Some(thread_id("")), BUDGET)
            .await
            .expect("create");
        assert!(!draft.thread_id.0.is_empty());
        let updated = runner
            .update_draft(
                &account(),
                &draft.id,
                raw(&token(1), "b"),
                Some(thread_id("")),
                None,
                BUDGET,
            )
            .await
            .expect("update");
        assert_eq!(updated.thread_id, draft.thread_id, "kept its thread");
        let bodies = setup.gmail.lock().saved_bodies.clone();
        assert_eq!(bodies[0]["message"].get("threadId"), None);
        assert_eq!(bodies[1]["message"]["threadId"], draft.thread_id.0.as_str());
    });
}

#[test]
fn an_update_naming_gmails_message_is_not_refused_by_an_index_behind() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        // The host learned Gmail's message before the index did.
        let elsewhere = setup
            .gmail
            .lock()
            .update_draft(
                &draft.id.0,
                &raw(&token(1), "elsewhere"),
                Some(&draft.thread_id.0),
            )
            .expect("draft");
        assert_eq!(
            view(&runner, &draft.id).await.expect("row").message_id,
            draft.message
        );
        runner
            .update_draft(
                &account(),
                &draft.id,
                raw(&token(1), "b"),
                None,
                Some(message_id(&elsewhere)),
                BUDGET,
            )
            .await
            .expect("Gmail holds the message named");
    });
}

#[test]
fn late_ends_of_an_attempt_a_retry_took_over_are_ignored() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = create(&runner, 1, "a").await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Lost { applied: false });
        let error = runner
            .send_draft(&account(), &draft.id, None, BUDGET)
            .await
            .expect_err("lost");
        assert!(matches!(error, DraftError::OutcomeUnknown(_)), "{error:?}");
        let first = sending_since(&runner, &draft.id).await;
        setup.clock.advance(send_window());
        // The retry takes the row over; its request is held in flight
        // while the first attempt's late records arrive.
        let hold = Hold::default();
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Held(hold.clone()));
        let account = account();
        let (sent, ()) = tokio::join!(
            runner.send_draft(&account, &draft.id, None, BUDGET),
            async {
                hold.reached().await;
                let retry = sending_since(&runner, &draft.id).await;
                assert!(retry > first);
                let ends = [
                    SendEnd::Changed {
                        message: message_id("m-other"),
                        thread_id: draft.thread_id.clone(),
                    },
                    SendEnd::Unconfirmed,
                    SendEnd::Unsent,
                ];
                for end in ends {
                    let late = DraftWrite::EndSend {
                        draft: draft.id.clone(),
                        started: first,
                        end: end.clone(),
                    };
                    let now = setup.clock.now();
                    let recorded = runner
                        .store()
                        .record_draft(&account, &late, now)
                        .await
                        .expect("record");
                    assert_eq!(recorded, DraftRecorded::Superseded, "{end:?}");
                    assert_eq!(sending_since(&runner, &draft.id).await, retry, "{end:?}");
                }
                hold.release();
            }
        );
        let sent = sent.expect("the retry sends");
        assert_eq!(sent_messages(&setup), vec![sent.message_id.0.clone()]);
        assert!(view(&runner, &draft.id).await.is_none());
        assert_eq!(
            draft_feed(&runner).await,
            vec![
                entry("saved:local", &draft.id, &draft.message),
                entry("sent", &draft.id, &sent.message_id),
            ]
        );
    });
}
