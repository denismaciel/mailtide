//! Tests ported by behaviour from Zero's `mail-draft-store.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890). Plan: `features/drafts.toml`.
//!
//! Zero caches draft reads for seconds behind a revision every local write
//! bumps. mailtide has no read cache: the draft index is durable, and the
//! same three properties are rules of the index. A local write fences an
//! older listing out, history touching a draft schedules a listing whose
//! failure keeps the index as it was, and one account never reads another's
//! rows.

pub mod fake_gmail;

use std::time::Duration;

use fake_gmail::*;
use mailtide::drafts::DraftStatus;

/// A listing the sync asks for runs this much later.
const REFRESH_DELAY: Duration = Duration::from_secs(11);

#[test]
fn a_listing_claimed_before_a_local_draft_write_never_overwrites_it() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let account = account();
        let draft = runner
            .create_draft(&account, raw(&token(1), "a"), None, BUDGET)
            .await
            .expect("create");
        // The bootstrap's listing.
        sync_after_delay(&runner, &setup).await;
        let listings = setup.gmail.listings();

        // A listing reads Gmail before the update and answers after it.
        let hold = Hold::default();
        setup
            .gmail
            .fail_next("GET drafts", Fault::Held(hold.clone()));
        setup.clock.advance(Duration::from_mins(15));
        let (run, updated) = tokio::join!(runner.run(&account, BUDGET), async {
            hold.reached().await;
            let updated = runner
                .update_draft(&account, &draft.id, raw(&token(1), "b"), None, None, BUDGET)
                .await;
            hold.release();
            updated
        });
        run.expect("run");
        let updated = updated.expect("update");
        assert_eq!(setup.gmail.listings(), listings + 1, "the held listing");
        let row = runner
            .draft(&account, &draft.id)
            .await
            .expect("draft")
            .expect("row");
        assert_eq!(row.message_id, updated.message, "the write stands");
        assert_eq!(row.state, DraftStatus::Saved);
        assert!(
            !draft_feed(&runner)
                .await
                .iter()
                .any(|(kind, _, _)| kind == "saved:remote"),
            "the old listing applied nothing"
        );

        // It is listed again, and that listing agrees.
        sync_after_delay(&runner, &setup).await;
        assert_eq!(setup.gmail.listings(), listings + 2);
        let row = runner
            .draft(&account, &draft.id)
            .await
            .expect("draft")
            .expect("row");
        assert_eq!(row.message_id, updated.message);
    });
}

#[test]
fn history_touching_a_draft_schedules_a_refresh_and_a_failed_refresh_keeps_the_index() {
    block_on(async {
        let (runner, setup) = runner();
        synced(&runner).await;
        let draft = runner
            .create_draft(&account(), raw(&token(1), "a"), None, BUDGET)
            .await
            .expect("create");
        sync_after_delay(&runner, &setup).await;
        let listings = setup.gmail.listings();

        let edited = setup
            .gmail
            .lock()
            .update_draft(
                &draft.id.0,
                &raw(&token(1), "elsewhere"),
                Some(&draft.thread_id.0),
            )
            .expect("draft");
        setup.gmail.fail_next("GET drafts", Fault::Status(500));
        sync_after_delay(&runner, &setup).await;
        assert_eq!(setup.gmail.listings(), listings + 1, "history asked for it");
        let row = runner
            .draft(&account(), &draft.id)
            .await
            .expect("draft")
            .expect("kept");
        assert_eq!(row.message_id, draft.message, "nothing from the failure");
        assert_eq!(row.state, DraftStatus::Stale);

        // The listing is retried, and repoints the draft.
        for _ in 0..10 {
            setup.clock.advance(Duration::from_mins(1));
            sync(&runner).await;
        }
        assert!(setup.gmail.listings() > listings + 1);
        let row = runner
            .draft(&account(), &draft.id)
            .await
            .expect("draft")
            .expect("row");
        assert_eq!(row.message_id, message_id(&edited));
        assert_eq!(row.state, DraftStatus::Saved);
    });
}

#[test]
fn draft_rows_are_isolated_per_account() {
    block_on(async {
        let (runner, setup) = runner();
        let (alice, bob) = (account(), other_account());
        for account in [&alice, &bob] {
            runner.track(account).await.expect("track");
            runner.run(account, BUDGET).await.expect("run");
        }
        let mine = runner
            .create_draft(&alice, raw(&token(1), "alice"), None, BUDGET)
            .await
            .expect("create");
        let theirs = runner
            .create_draft(&bob, raw(&token(2), "bob"), None, BUDGET)
            .await
            .expect("create");
        // Each mailbox numbers its own drafts: the ids are the same.
        assert_eq!(mine.id, theirs.id);
        assert_eq!(setup.gmail.lock().draft_list().len(), 1);
        assert_eq!(setup.gmail.lock_other().draft_list().len(), 1);

        let row = |account| {
            let runner = &runner;
            let id = mine.id.clone();
            async move {
                runner
                    .draft(account, &id)
                    .await
                    .expect("draft")
                    .expect("row")
            }
        };
        assert_eq!(row(&alice).await.tracking_id, Some(token(1)));
        assert_eq!(row(&bob).await.tracking_id, Some(token(2)));

        // Deleting one account's draft and listing the other's leave the
        // first account's rows alone.
        runner
            .delete_draft(&bob, &theirs.id, BUDGET)
            .await
            .expect("delete");
        setup.clock.advance(REFRESH_DELAY);
        runner.run(&bob, BUDGET).await.expect("run");
        assert!(
            runner
                .drafts(&bob, None, 10)
                .await
                .expect("drafts")
                .is_empty()
        );
        let views = runner.drafts(&alice, None, 10).await.expect("drafts");
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].message_id, mine.message);
        assert_eq!(views[0].tracking_id, Some(token(1)));
    });
}
