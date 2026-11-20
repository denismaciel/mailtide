//! Cases from himalaya's `src/jmap/backend.rs` (MIT OR Apache-2.0,
//! commit a5623dcac615), exercising refusal on the two Gmail send routes.

pub mod fake_gmail;

use fake_gmail::*;
use mailtide::submission::SubmissionState;
use mailtide_io::drafts::DraftError;
use mailtide_io::send::{SendError, SendOptions, SendSource};

#[test]
fn a_refused_draft_submission_leaves_the_draft_in_place() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let draft = r
            .create_draft(&account(), raw(&token(2), "Draft"), None, BUDGET)
            .await
            .unwrap();
        setup
            .gmail
            .fail_next("POST drafts/send", Fault::Status(403));
        assert!(matches!(
            r.send(
                &account(),
                SendSource::Draft {
                    draft_id: draft.id.clone(),
                    if_message_id: Some(draft.message.clone()),
                },
                BUDGET
            )
            .await,
            Err(SendError::Draft(DraftError::Refused(_)))
        ));
        assert_eq!(
            setup.gmail.lock().draft_list(),
            vec![(draft.id.0, draft.message.0)]
        );
        assert!(
            r.submissions(&account(), None, &[], 10)
                .await
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn a_refusal_for_its_recipients_carries_gmails_message() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        let bytes = [
            b"To: somebody@example.org\r\n".as_slice(),
            &raw(&token(1), "Hello"),
        ]
        .concat();
        let row = r
            .submit(&account(), None, bytes, SendOptions::default())
            .await
            .unwrap();
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Status(400));
        sync(&r).await;
        let row = r.submission(&account(), &row.key).await.unwrap().unwrap();
        assert_eq!(row.state, SubmissionState::Refused);
        assert_eq!(row.refusal.as_deref(), Some("400 badRequest refused"));
    });
}
