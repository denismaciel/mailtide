//! Queue uncertainty from himalaya's `src/pimdir/queue/show.rs`
//! (MIT OR Apache-2.0, commit a5623dcac615).

pub mod fake_gmail;

use fake_gmail::*;
use mailtide::submission::SubmissionState;
use mailtide_io::send::SendOptions;

#[test]
fn an_unknown_submission_reads_as_maybe_sent() {
    block_on(async {
        let (r, setup) = runner();
        synced(&r).await;
        setup
            .gmail
            .fail_next("POST messages/send", Fault::Lost { applied: false });
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
        setup.clock.advance(std::time::Duration::from_mins(10));
        sync(&r).await;
        let rows = r
            .submissions(&account(), None, &[SubmissionState::Unknown], 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, row.key);
        assert_eq!(rows[0].state, SubmissionState::Unknown);
        assert!(rows[0].message_id.is_none());
    });
}
