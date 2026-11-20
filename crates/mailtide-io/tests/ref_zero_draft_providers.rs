//! Tests ported by behaviour from Zero's `driver/draft-providers.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890). Plan: `features/drafts.toml`.
//!
//! Zero builds a draft's MIME and parses Gmail's `format=full` answer into
//! fields. mailtide takes and returns RFC 5322 bytes: what a host saves is
//! what Gmail is sent, and a read returns Gmail's copy whole, or nothing.

pub mod fake_gmail;

use fake_gmail::*;
use mailtide::drafts::DraftStatus;
use mailtide_io::drafts::DraftError;

#[test]
fn provider_draft_safety_writes_the_selected_gmail_from_address_into_the_real_saved_mime() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let bytes = format!(
            "X-Mailtide-Id: {}\r\nFrom: Alias <alias@example.com>\r\nTo: recipient@example.com\r\nSubject: Hello\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<p>Hi</p>\r\n",
            token(1)
        )
        .into_bytes();
        let draft = runner
            .create_draft(&account(), bytes.clone(), None, BUDGET)
            .await
            .expect("create");

        let saved = setup.gmail.saved_raw();
        assert_eq!(saved, vec![bytes], "the bytes as given, From included");
        let text = String::from_utf8_lossy(&saved[0]).into_owned();
        assert!(
            text.split("\r\n")
                .any(|line| line.starts_with("From:") && line.contains("alias@example.com")),
            "{text}"
        );
        // Gmail's stored copy keeps it.
        let stored = runner
            .read_draft(&account(), &draft.id, true, BUDGET)
            .await
            .expect("read");
        assert!(
            String::from_utf8_lossy(&stored.raw).contains("From: Alias <alias@example.com>\r\n")
        );
    });
}

#[test]
fn provider_draft_safety_reads_nested_gmail_mime_content_and_retains_threading_and_quoted_recipients()
 {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let thread = {
            let mut gmail = setup.gmail.lock();
            let source = gmail.add_message(
                None,
                &["INBOX"],
                b"Message-ID: <source@example.com>\r\nSubject: Hello\r\n\r\nHi\r\n".to_vec(),
            );
            gmail.messages[&source].thread.clone()
        };
        let headers = [
            "To: \"Doe, Sam\" <sam@example.com>",
            "Subject: Re: Hello",
            "In-Reply-To: <source@example.com>",
            "References: <prior@example.com> <source@example.com>",
        ];
        let body = "Content-Type: multipart/mixed; boundary=\"outer\"\r\n\r\n--outer\r\nContent-Type: multipart/alternative; boundary=\"inner\"\r\n\r\n--inner\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<p>Stored body</p>\r\n--inner--\r\n--outer--\r\n";
        let bytes = format!(
            "X-Mailtide-Id: {}\r\n{}\r\nMIME-Version: 1.0\r\n{body}",
            token(1),
            headers.join("\r\n")
        );
        // Saved by another client, in the thread it replies to.
        let (draft, _) = setup
            .gmail
            .lock()
            .create_draft(bytes.as_bytes(), Some(&thread));

        let content = runner
            .read_draft(&account(), &draft_id(&draft), true, BUDGET)
            .await
            .expect("read");
        assert_eq!(content.draft.id.0, draft);
        assert_eq!(content.draft.thread_id.0, thread);
        let text = String::from_utf8_lossy(&content.raw).into_owned();
        for header in headers {
            assert!(
                text.contains(&format!("{header}\r\n")),
                "{header} in {text}"
            );
        }
        assert!(text.ends_with(body), "the nested parts whole: {text}");
    });
}

#[test]
fn a_draft_read_without_its_raw_content_is_refused() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = runner
            .create_draft(&account(), raw(&token(1), "a"), None, BUDGET)
            .await
            .expect("create");
        setup.gmail.fail_next("GET drafts/", Fault::NoRaw);
        let error = runner
            .read_draft(&account(), &draft.id, true, BUDGET)
            .await
            .expect_err("no content");
        assert!(matches!(error, DraftError::Refused(_)), "{error:?}");
        assert_eq!(
            setup.gmail.sent("GET drafts/"),
            vec![format!("GET drafts/{}?raw", draft.id.0)]
        );
        // The draft itself is untouched, and the next read is whole.
        let row = runner
            .draft(&account(), &draft.id)
            .await
            .expect("draft")
            .expect("row");
        assert_eq!(row.message_id, draft.message);
        assert_eq!(row.state, DraftStatus::Saved);
        let content = runner
            .read_draft(&account(), &draft.id, true, BUDGET)
            .await
            .expect("read");
        assert!(String::from_utf8_lossy(&content.raw).contains("Subject: a\r\n"));
    });
}

// Zero's forward fail-closed case. The mirror's blob is the original.
#[test]
fn a_forward_plan_whose_original_cannot_be_read_is_refused() {
    use mailtide::ids::MessageId;
    use mailtide_io::blobs::BlobStore as _;
    use mailtide_io::send::SendError;
    block_on(async {
        let (r, setup) = runner();
        let id = MessageId(setup.gmail.lock().add_message(
            None,
            &["INBOX"],
            raw(&token(3), "Original"),
        ));
        synced(&r).await;
        assert!(r.forward_plan(&account(), &id).await.is_ok());
        r.blobs()
            .delete(&mailtide_io::blobs::blob_key(&account(), &id))
            .await
            .unwrap();
        assert_eq!(
            r.forward_plan(&account(), &id).await,
            Err(SendError::OriginalUnavailable)
        );
    });
}
