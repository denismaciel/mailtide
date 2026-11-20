//! Tests ported by behaviour from Zero's `driver/imap.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890). Plan: `features/drafts.toml`.
//!
//! Only the suite's two provider-neutral draft tests: a draft keeps one id
//! through its life, and a message that is not a draft is never handed out
//! as one. They run against the fake Gmail instead of a fake IMAP server.

pub mod fake_gmail;

use fake_gmail::*;
use mailtide_io::drafts::{Deleted, DraftError};

#[test]
fn creates_updates_reads_and_deletes_drafts_with_a_stable_id() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft_bytes = |version: &str| {
            format!(
                "X-Mailtide-Id: {}\r\nTo: bob@example.com\r\nSubject: Draft\r\nContent-Type: text/html\r\n\r\n<p>{version}</p>\r\n",
                token(1)
            )
            .into_bytes()
        };
        let created = runner
            .create_draft(&account(), draft_bytes("v1"), None, BUDGET)
            .await
            .expect("create");
        assert_eq!(setup.gmail.lock().labelled("DRAFT").len(), 1);

        let updated = runner
            .update_draft(
                &account(),
                &created.id,
                draft_bytes("v2"),
                None,
                None,
                BUDGET,
            )
            .await
            .expect("update");
        assert_eq!(updated.id, created.id);
        assert_eq!(
            setup.gmail.lock().labelled("DRAFT"),
            vec![updated.message.0.clone()],
            "still one draft message"
        );

        let read = runner
            .read_draft(&account(), &created.id, false, BUDGET)
            .await
            .expect("read");
        assert_eq!(read.draft.id, created.id);
        let text = String::from_utf8_lossy(&read.raw).into_owned();
        assert!(text.contains("To: bob@example.com\r\n"), "{text}");
        assert!(text.contains("Subject: Draft\r\n"), "{text}");
        assert!(text.contains("<p>v2</p>"), "{text}");
        let listed = runner.drafts(&account(), None, 10).await.expect("drafts");
        assert_eq!(
            listed
                .iter()
                .map(|d| d.draft_id.clone())
                .collect::<Vec<_>>(),
            vec![created.id.clone()]
        );

        let deleted = runner
            .delete_draft(&account(), &created.id, BUDGET)
            .await
            .expect("delete");
        assert_eq!(deleted, Deleted::Deleted);
        assert!(setup.gmail.lock().labelled("DRAFT").is_empty());
        assert!(
            runner
                .drafts(&account(), None, 10)
                .await
                .expect("drafts")
                .is_empty()
        );
    });
}

#[test]
fn does_not_expose_an_inbox_message_as_an_editable_draft() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let message = setup.gmail.lock().add_message(
            None,
            &["INBOX"],
            b"Message-ID: <not-a-draft@example.org>\r\nSubject: Incoming\r\n\r\nHi\r\n".to_vec(),
        );
        sync(&runner).await;
        let as_draft = draft_id(&message);

        let read = runner
            .read_draft(&account(), &as_draft, false, BUDGET)
            .await
            .expect_err("not a draft");
        assert_eq!(read, DraftError::NotFound);
        assert!(
            runner
                .draft(&account(), &as_draft)
                .await
                .expect("draft")
                .is_none()
        );
        let thread = setup.gmail.lock().messages[&message].thread.clone();
        let updated = runner
            .update_draft(
                &account(),
                &as_draft,
                raw(&token(1), "overwrite"),
                Some(thread_id(&thread)),
                None,
                BUDGET,
            )
            .await
            .expect_err("not a draft");
        assert_eq!(updated, DraftError::NotFound);
        let sent = runner
            .send_draft(&account(), &as_draft, None, BUDGET)
            .await
            .expect_err("not a draft");
        assert_eq!(sent, DraftError::NotFound);

        // The message is as it was.
        assert!(setup.gmail.sent("POST drafts/send").is_empty());
        let gmail = setup.gmail.lock();
        assert_eq!(gmail.messages[&message].labels, vec!["INBOX".to_owned()]);
        assert!(gmail.draft_list().is_empty());
    });
}

// Zero's SMTP case maps to Gmail's wire, SENT and threading behavior in send.
#[test]
fn a_reply_goes_out_once_with_its_bcc_lands_in_sent_and_joins_the_thread() {
    use mailtide::ids::MessageId;
    use mailtide::submission::SubmissionState;
    use mailtide_io::send::SendOptions;
    block_on(async {
        let (r, setup) = runner();
        let parent =
            setup
                .gmail
                .lock()
                .add_message(Some("thread"), &["INBOX"], raw(&token(9), "Parent"));
        synced(&r).await;
        let bytes = [
            b"To: reader@example.org\r\nBcc: blind@example.org\r\n".as_slice(),
            &raw(&token(1), "Reply"),
        ]
        .concat();
        let row = r
            .submit(
                &account(),
                None,
                bytes,
                SendOptions {
                    thread_of: Some(MessageId(parent)),
                    ..SendOptions::default()
                },
            )
            .await
            .unwrap();
        sync(&r).await;
        let sent = r.submission(&account(), &row.key).await.unwrap().unwrap();
        assert_eq!(sent.state, SubmissionState::Sent);
        assert_eq!(sent.thread_id.unwrap().0, "thread");
        assert_eq!(setup.gmail.sent("POST messages/send").len(), 1);
        let gmail = setup.gmail.lock();
        let stored = &gmail.messages[&sent.message_id.unwrap().0];
        assert!(stored.labels.iter().any(|label| label == "SENT"));
        assert!(String::from_utf8_lossy(&stored.raw).contains("Bcc: blind@example.org"));
    });
}
