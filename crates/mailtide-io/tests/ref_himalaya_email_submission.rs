//! Recipient guard from himalaya's src/email/submission.rs (MIT OR Apache-2.0,
//! commit a5623dcac615).

pub mod fake_gmail;

use fake_gmail::*;
use mailtide_io::send::{SendError, SendOptions};

#[test]
fn a_message_without_recipients_is_refused() {
    block_on(async {
        let (r, setup) = runner();
        r.track(&account()).await.unwrap();
        assert_eq!(
            r.submit(
                &account(),
                None,
                raw(&token(1), "Missing recipient"),
                SendOptions::default()
            )
            .await,
            Err(SendError::NoRecipients)
        );
        assert!(
            r.submissions(&account(), None, &[], 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(setup.gmail.sent("POST messages/send").is_empty());
    });
}
