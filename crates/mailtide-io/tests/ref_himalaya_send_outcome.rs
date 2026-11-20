//! By behavior from himalaya's `tests/send_outcome.rs` (MIT OR Apache-2.0,
//! commit a5623dcac615). Exercise Gmail's refusal in the durable queue.

pub mod fake_gmail;

use fake_gmail::*;
use mailtide::submission::SubmissionState;
use mailtide_io::send::SendOptions;

#[test]
fn a_send_gmail_refuses_is_the_submissions_result() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Status(400));
        let bytes = [
            b"To: bob@example.org\r\n".as_slice(),
            &raw(&token(1), "Hello"),
        ]
        .concat();
        let row = r
            .submit(&account(), None, bytes, SendOptions::default())
            .await
            .unwrap();
        sync(&r).await;
        let result = r.submission(&account(), &row.key).await.unwrap().unwrap();
        assert_eq!(result.state, SubmissionState::Refused);
        assert_eq!(result.refusal.as_deref(), Some("400 badRequest refused"));
        sync(&r).await;
        assert_eq!(setup.gmail.sent("POST messages/send").len(), 1);
    });
}
