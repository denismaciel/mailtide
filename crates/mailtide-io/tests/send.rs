//! Raw submission delivery through the real Runner and a scripted Gmail.

pub mod fake_gmail;

use std::time::Duration;

use fake_gmail::{
    BUDGET, Fault, Hold, Setup, TestRunner, account, block_on, raw, runner, runner_writing, sync,
    synced, token,
};
use mailtide::ids::{MessageId, ThreadId};
use mailtide::quota::Budget;
use mailtide::revision::{Change, ChangeFeed, FeedCursor};
use mailtide::submission::{Submission, SubmissionKey, SubmissionState};
use mailtide_io::runner::{Access, StopReason, WriteSettings};
use mailtide_io::send::{SendError, SendOptions};
use mailtide_store::Store as _;

fn bytes(n: u32) -> Vec<u8> {
    [
        b"To: recipient@example.org\r\n".as_slice(),
        &raw(&token(n), "Hello"),
    ]
    .concat()
}

async fn queue(r: &TestRunner, n: u32, options: SendOptions) -> Submission {
    r.submit(&account(), None, bytes(n), options).await.unwrap()
}

async fn state(r: &TestRunner, sub: &Submission) -> Submission {
    r.submission(&account(), &sub.key).await.unwrap().unwrap()
}

fn sends(setup: &Setup) -> usize {
    setup.gmail.sent("POST messages/send").len()
}

#[test]
fn a_send_returns_a_receipt_and_import_confirms_by_token() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        assert_eq!(sub.state, SubmissionState::Pending);
        sync(&r).await;
        let result = state(&r, &sub).await;
        assert_eq!(result.state, SubmissionState::Sent, "{result:?}");
        let id = result.message_id.unwrap();
        assert_eq!(sends(&setup), 1);
        assert_ne!(
            setup.gmail.lock().rfc822_message_id(&id.0).as_deref(),
            Some("<client-Hello@example.com>")
        );
        let ChangeFeed::Page(page) = r
            .store()
            .changes(
                &account(),
                FeedCursor::from(mailtide::revision::StateSeq(0)),
                500,
            )
            .await
            .unwrap()
        else {
            panic!("feed page");
        };
        assert!(page.changes.iter().any(|entry| matches!(&entry.change,
            Change::SubmissionChanged { submission } if submission.key == sub.key && submission.state == SubmissionState::Confirming)));
        let confirmed = page
            .changes
            .iter()
            .find(|entry| {
                matches!(&entry.change,
            Change::SubmissionChanged { submission } if submission.state == SubmissionState::Sent)
            })
            .unwrap();
        assert!(page.changes.iter().any(|entry| entry.state == confirmed.state && matches!(&entry.change, Change::MessageImported { id: imported, .. } if imported == &id)));
    });
}

#[test]
fn invalid_or_repeated_tracking_headers_are_refused_before_staging() {
    block_on(async {
        let (r, _) = runner();
        r.track(&account()).await.unwrap();
        for raw in [
            b"To: a@example.org\r\n\r\nbody".to_vec(),
            [b"X-Mailtide-Id: too-short\r\n".as_slice(), &bytes(1)].concat(),
        ] {
            assert_eq!(
                r.submit(&account(), None, raw, SendOptions::default())
                    .await,
                Err(SendError::InvalidTrackingId)
            );
        }
        assert!(
            r.submissions(&account(), None, &[], 10)
                .await
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn an_omitted_key_defaults_to_the_tracking_id() {
    block_on(async {
        let (r, _) = runner();
        r.track(&account()).await.unwrap();
        let sub = queue(&r, 1, SendOptions::default()).await;
        assert_eq!(sub.key.0, token(1));
        assert_eq!(queue(&r, 1, SendOptions::default()).await, sub);
    });
}

#[test]
fn concurrent_admission_never_overwrites_the_winning_blob() {
    block_on(async {
        let (r, _) = runner();
        r.track(&account()).await.unwrap();
        let key = SubmissionKey("same-key".into());
        let acc = account();
        let (a, b) = tokio::join!(
            r.submit(&acc, Some(key.clone()), bytes(1), SendOptions::default()),
            r.submit(&acc, Some(key.clone()), bytes(2), SendOptions::default()),
        );
        let winner = match (a, b) {
            (Ok(_), Err(SendError::KeyConflict)) => bytes(1),
            (Err(SendError::KeyConflict), Ok(_)) => bytes(2),
            other => panic!("one winner: {other:?}"),
        };
        assert_eq!(
            r.submission_content(&acc, &key).await.unwrap(),
            Some(winner)
        );
    });
}

#[test]
fn an_unreadable_or_changed_staged_blob_is_never_dispatched() {
    block_on(async {
        use mailtide_io::blobs::BlobStore as _;
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        let blob = r
            .store()
            .submission(&account(), &sub.key)
            .await
            .unwrap()
            .unwrap()
            .blob
            .unwrap();
        r.blobs().put(&blob, bytes(2)).await.unwrap();
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Refused);
        assert_eq!(sends(&setup), 0);
    });
}

#[test]
fn a_send_whose_answer_was_lost_is_confirmed_by_the_imported_token() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Lost { applied: true });
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Sent);
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn a_lost_send_with_no_import_becomes_unknown_and_never_redispatches() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Lost { applied: false });
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Confirming);
        setup.clock.advance(Duration::from_mins(10));
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Unknown);
        setup.clock.advance(Duration::from_hours(169));
        sync(&r).await;
        assert_eq!(
            r.submission_content(&account(), &sub.key).await.unwrap(),
            None
        );
        assert_eq!(
            queue(&r, 1, SendOptions::default()).await.state,
            SubmissionState::Unknown
        );
        assert_eq!(
            r.submit(
                &account(),
                Some(SubmissionKey("new-key".into())),
                bytes(1),
                SendOptions::default()
            )
            .await,
            Err(SendError::DuplicateTrackingId)
        );
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn an_engine_superseded_mid_send_is_confirmed_by_its_successor() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        let hold = Hold::default();
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Held(hold.clone()));
        let acc = account();
        let mut first = std::pin::pin!(r.run(&acc, BUDGET));
        tokio::select! { () = hold.reached() => (), other = &mut first => panic!("{other:?}") }
        sync(&r).await;
        hold.release();
        first.await.unwrap();
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Sent);
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn dropping_the_send_future_after_dispatch_leaves_it_closed() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        let hold = Hold::default();
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Held(hold.clone()));
        let acc = account();
        {
            let mut call = std::pin::pin!(r.run(&acc, BUDGET));
            tokio::select! { () = hold.reached() => (), other = &mut call => panic!("{other:?}") }
        }
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Sent);
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn a_pre_dispatch_deferral_requeues_without_reopening_undo() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        setup.gmail.fail_next("POST messages/send", Fault::NotSent);
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Claimed);
        assert_eq!(
            r.cancel_submission(&account(), &sub.key).await,
            Err(SendError::CannotUnsend(SubmissionState::Claimed))
        );
        setup.clock.advance(Duration::from_secs(30));
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Sent);
        assert_eq!(setup.gmail.lock().labelled("SENT").len(), 1);
    });
}

#[test]
fn an_http_rate_limit_consumes_the_wire_request() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Status(429));
        sync(&r).await;
        let row = state(&r, &sub).await;
        assert_eq!(row.state, SubmissionState::Refused);
        assert!(row.refusal.unwrap().starts_with("429"));
        assert!(row.retry_at.is_some());
        setup.clock.advance(Duration::from_mins(10));
        sync(&r).await;
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn a_message_over_the_maximum_size_is_refused_before_staging() {
    block_on(async {
        assert_eq!(WriteSettings::default().max_message_bytes, 35_651_584);
        let limit = bytes(1).len() - 1;
        let (r, setup) = runner_writing(
            Access::ReadWrite,
            Budget::GMAIL_DEFAULT,
            WriteSettings {
                max_message_bytes: limit,
                ..WriteSettings::default()
            },
        );
        r.track(&account()).await.unwrap();
        assert_eq!(
            r.submit(&account(), None, bytes(1), SendOptions::default())
                .await,
            Err(SendError::TooLarge { limit })
        );
        assert!(
            r.submissions(&account(), None, &[], 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(sends(&setup), 0);
    });
}

#[test]
fn a_message_at_the_size_limit_uses_one_json_send() {
    block_on(async {
        let (r, setup) = runner_writing(
            Access::ReadWrite,
            Budget::GMAIL_DEFAULT,
            WriteSettings {
                max_message_bytes: bytes(1).len(),
                ..WriteSettings::default()
            },
        );
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Sent);
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn a_reply_uses_the_thread_id_with_unrelated_subject_and_no_reply_headers() {
    block_on(async {
        let (r, setup) = runner();
        let parent = setup.gmail.lock().add_message(
            Some("thread"),
            &["INBOX"],
            b"Subject: Original\r\n\r\nParent".to_vec(),
        );
        synced(&r).await;
        let sub = queue(
            &r,
            1,
            SendOptions {
                thread_of: Some(MessageId(parent)),
                ..SendOptions::default()
            },
        )
        .await;
        sync(&r).await;
        assert_eq!(
            state(&r, &sub).await.thread_id,
            Some(ThreadId("thread".into()))
        );
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn thread_of_a_draft_a_tombstone_or_an_unknown_message_is_refused() {
    block_on(async {
        let (r, setup) = runner();
        let draft = setup.gmail.lock().add_message(None, &["DRAFT"], bytes(2));
        let dead = setup.gmail.lock().add_message(None, &["INBOX"], bytes(3));
        synced(&r).await;
        setup.gmail.lock().delete_message(&dead);
        sync(&r).await;
        for id in [draft, dead, "absent".into()] {
            assert_eq!(
                r.submit(
                    &account(),
                    None,
                    bytes(1),
                    SendOptions {
                        thread_of: Some(MessageId(id)),
                        ..SendOptions::default()
                    }
                )
                .await,
                Err(SendError::UnknownOriginal)
            );
        }
    });
}

#[test]
fn a_gone_gmail_thread_is_refused() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(
            &r,
            1,
            SendOptions {
                thread_id: Some(ThreadId("gone".into())),
                ..SendOptions::default()
            },
        )
        .await;
        sync(&r).await;
        let row = state(&r, &sub).await;
        assert_eq!(row.state, SubmissionState::Refused);
        assert!(row.refusal.unwrap().starts_with("404 notFound"));
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn an_undone_send_never_reaches_gmail() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(
            &r,
            1,
            SendOptions {
                send_at: Some(setup.clock.now().plus(Duration::from_mins(1))),
                ..SendOptions::default()
            },
        )
        .await;
        assert_eq!(
            r.cancel_submission(&account(), &sub.key)
                .await
                .unwrap()
                .state,
            SubmissionState::Canceled
        );
        setup.clock.advance(Duration::from_mins(2));
        sync(&r).await;
        assert_eq!(sends(&setup), 0);
        assert_eq!(
            r.submission_content(&account(), &sub.key).await.unwrap(),
            Some(bytes(1))
        );
    });
}

#[test]
fn undo_after_the_claim_reports_cannot_unsend() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        let hold = Hold::default();
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Held(hold.clone()));
        let acc = account();
        let mut call = std::pin::pin!(r.run(&acc, BUDGET));
        tokio::select! { () = hold.reached() => (), other = &mut call => panic!("{other:?}") }
        assert_eq!(
            r.cancel_submission(&account(), &sub.key).await,
            Err(SendError::CannotUnsend(SubmissionState::Sending))
        );
        hold.release();
        call.await.unwrap();
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn a_scheduled_send_survives_a_restart_and_goes_out_when_due() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(
            &r,
            1,
            SendOptions {
                send_at: Some(setup.clock.now().plus(Duration::from_secs(30))),
                ..SendOptions::default()
            },
        )
        .await;
        sync(&r).await;
        assert_eq!(sends(&setup), 0);
        setup.clock.advance(Duration::from_secs(30));
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Sent);
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn run_reports_the_next_scheduled_send_as_its_due_time() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let due = setup.clock.now().plus(Duration::from_secs(1));
        queue(
            &r,
            1,
            SendOptions {
                send_at: Some(due),
                ..SendOptions::default()
            },
        )
        .await;
        let stop = r.run(&account(), BUDGET).await.unwrap().stop;
        assert!(matches!(stop, StopReason::Idle { next: Some(next) } if next <= due));
        assert_eq!(sends(&setup), 0);
    });
}

#[test]
fn sends_are_refused_in_read_only_mode() {
    block_on(async {
        let (r, setup) = runner_writing(
            Access::ReadOnly,
            Budget::GMAIL_DEFAULT,
            WriteSettings::default(),
        );
        synced(&r).await;
        assert_eq!(
            r.submit(&account(), None, bytes(1), SendOptions::default())
                .await,
            Err(SendError::ReadOnly)
        );
        assert_eq!(sends(&setup), 0);
    });
}

#[test]
fn a_refused_send_does_not_block_the_account() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Status(403));
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Refused);
    });
}

#[test]
fn forget_deletes_submissions_and_staged_bytes() {
    block_on(async {
        let (r, _) = runner();
        r.track(&account()).await.unwrap();
        let sub = queue(&r, 1, SendOptions::default()).await;
        assert_eq!(r.forget(&account()).await.unwrap().blobs, 1);
        assert_eq!(r.submission(&account(), &sub.key).await.unwrap(), None);
    });
}

async fn block_account(r: &TestRunner, setup: &Setup) {
    setup.gmail.fail_next("GET history", Fault::Status(403));
    assert!(matches!(
        r.run(&account(), BUDGET).await.unwrap().stop,
        StopReason::Blocked(_)
    ));
}

#[test]
fn a_send_while_the_account_is_blocked_waits() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        block_account(&r, &setup).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        assert!(matches!(
            r.run(&account(), BUDGET).await.unwrap().stop,
            StopReason::Blocked(_)
        ));
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Pending);
        assert_eq!(sends(&setup), 0);
        r.resume(&account()).await.unwrap();
        sync(&r).await;
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Sent);
    });
}

#[test]
fn a_blocked_account_still_reports_a_due_unknown_send() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Lost { applied: false });
        sync(&r).await;
        block_account(&r, &setup).await;
        setup.clock.advance(Duration::from_mins(10));
        assert!(matches!(
            r.run(&account(), BUDGET).await.unwrap().stop,
            StopReason::Blocked(_)
        ));
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Unknown);
        assert_eq!(sends(&setup), 1);
    });
}

#[test]
fn possible_dispatch_and_recovery_wake_ordinary_history() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Lost { applied: true });
        sync(&r).await;
        let sent_position = setup
            .gmail
            .lock()
            .requests
            .iter()
            .position(|request| request == "POST messages/send")
            .unwrap();
        assert!(
            setup.gmail.lock().requests[sent_position + 1..]
                .iter()
                .any(|request| request.starts_with("GET history"))
        );
        assert_eq!(state(&r, &sub).await.state, SubmissionState::Sent);
    });
}

#[test]
fn submission_job_due_time_includes_confirmation_and_cleanup() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let sub = queue(&r, 1, SendOptions::default()).await;
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Lost { applied: false });
        sync(&r).await;
        let row = r
            .store()
            .submission(&account(), &sub.key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.due_at, row.view.confirmation_deadline);
        setup.clock.advance(Duration::from_mins(10));
        sync(&r).await;
        let row = r
            .store()
            .submission(&account(), &sub.key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.due_at,
            Some(setup.clock.now().plus(Duration::from_hours(168)))
        );
        setup.clock.advance(Duration::from_hours(168));
        sync(&r).await;
        let row = r
            .store()
            .submission(&account(), &sub.key)
            .await
            .unwrap()
            .unwrap();
        assert!(row.blob.is_none() && row.due_at.is_none());
    });
}

async fn draft_source(r: &TestRunner) -> mailtide_io::send::SendSource {
    let saved = r
        .create_draft(&account(), bytes(2), None, BUDGET)
        .await
        .unwrap();
    mailtide_io::send::SendSource::Draft {
        draft_id: saved.id,
        if_message_id: Some(saved.message),
    }
}

#[test]
fn sending_an_existing_draft_uses_only_the_drafts_path() {
    use mailtide_io::send::SendOutcome;
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let source = draft_source(&r).await;
        let result = r.send(&account(), source, BUDGET).await.unwrap();
        assert!(matches!(result, SendOutcome::Draft(_)));
        assert!(
            r.submissions(&account(), None, &[], 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 1);
        assert_eq!(sends(&setup), 0);
    });
}

#[test]
fn a_draft_unknown_uses_drafts_fences_and_confirmation() {
    use mailtide_io::drafts::DraftError;
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let source = draft_source(&r).await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Lost { applied: true });
        assert!(matches!(
            r.send(&account(), source.clone(), BUDGET).await,
            Err(SendError::Draft(DraftError::OutcomeUnknown(_)))
        ));
        setup.clock.advance(Duration::from_mins(5));
        assert_eq!(
            r.send(&account(), source, BUDGET).await,
            Err(SendError::Draft(DraftError::SendUnconfirmed))
        );
        sync(&r).await;
        assert!(
            fake_gmail::draft_feed(&r)
                .await
                .iter()
                .any(|entry| entry.0 == "sent")
        );
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 1);
        assert_eq!(sends(&setup), 0);
    });
}

#[test]
fn a_draft_http_refusal_preserves_drafts_retry_rules() {
    use mailtide_io::drafts::DraftError;
    use mailtide_io::send::SendOutcome;
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let source = draft_source(&r).await;
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Status(400));
        assert!(matches!(
            r.send(&account(), source.clone(), BUDGET).await,
            Err(SendError::Draft(DraftError::Refused(_)))
        ));
        assert!(matches!(
            r.send(&account(), source, BUDGET).await,
            Ok(SendOutcome::Draft(_))
        ));
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 2);
        assert_eq!(setup.gmail.lock().labelled("SENT").len(), 1);
    });
}

#[test]
fn a_changed_or_gone_draft_uses_drafts_own_error() {
    use mailtide_io::drafts::DraftError;
    use mailtide_io::send::SendSource;
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let source = draft_source(&r).await;
        let SendSource::Draft { draft_id, .. } = &source else {
            panic!("draft");
        };
        r.update_draft(&account(), draft_id, bytes(2), None, None, BUDGET)
            .await
            .unwrap();
        assert!(matches!(
            r.send(&account(), source.clone(), BUDGET).await,
            Err(SendError::Draft(DraftError::Conflict { .. }))
        ));
        r.delete_draft(&account(), draft_id, BUDGET).await.unwrap();
        assert_eq!(
            r.send(&account(), source, BUDGET).await,
            Err(SendError::Draft(DraftError::NotFound))
        );
        assert_eq!(setup.gmail.sent("POST drafts/send").len(), 0);
        assert_eq!(sends(&setup), 0);
    });
}

#[test]
fn a_reply_plan_for_an_original_not_imported_is_refused() {
    block_on(async {
        let (r, _) = runner();
        r.track(&account()).await.unwrap();
        let id = MessageId("not-imported".into());
        assert_eq!(
            r.reply_plan(&account(), &id, &[], false).await,
            Err(SendError::OriginalUnavailable)
        );
        assert_eq!(
            r.forward_plan(&account(), &id).await,
            Err(SendError::OriginalUnavailable)
        );
    });
}
