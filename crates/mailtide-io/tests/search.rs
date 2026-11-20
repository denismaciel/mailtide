//! `Runner::search` against a fake Gmail: one quota-gated `messages.list`
//! per page, the hits marked with what the mirror holds, and nothing
//! written.

pub mod search_support;

use std::time::Duration;

use mailtide::commit::{AccountOp, DeletionEvidence, MessageOp};
use mailtide::ids::{AccountId, Epoch, HistoryId, LabelId, MessageId, ThreadId};
use mailtide::quota::{Budget, Priority, QuotaUnits};
use mailtide::retry::{AccountProblem, Blocking, BlockingFailure, QuotaTarget, Retryable};
use mailtide::revision::FeedPosition;
use mailtide::search::{HitState, SearchQuery, ThreadHit};
use mailtide::state::{SyncPhase, SyncState};
use mailtide_io::runner::{Access, QuotaDeferred, StopReason};
use mailtide_io::search::SearchError;
use mailtide_io::transport::TransportError;
use mailtide_store::Store as _;
use search_support::{
    Fixture, Options, account, discovered, hit_ids, param, query, refusal, thread_page,
};

#[tokio::test]
async fn a_search_returns_gmails_hits_with_their_mirror_state() {
    let fixture = Fixture::new();
    fixture.gmail().add("m1", &["INBOX"]);
    fixture.sync().await;
    fixture.seed(vec![discovered("m3")], Vec::new()).await;
    let status = fixture.status().await;
    fixture.answer_page(&["m3", "m1", "x9"], Some("next-1"));

    let page = fixture.search(&query("from:alice")).await.expect("a page");

    let hits: Vec<(&str, &str, HitState)> = page
        .hits
        .iter()
        .map(|h| (h.message_id.0.as_str(), h.thread_id.0.as_str(), h.state))
        .collect();
    assert_eq!(
        hits,
        [
            ("m3", "t-m3", HitState::NotImported),
            ("m1", "t-m1", HitState::Imported),
            ("x9", "t-x9", HitState::Unknown),
        ]
    );
    assert_eq!(page.estimate, 3);
    assert_eq!(page.deleted, 0);
    let token = page.next_page_token.expect("a next page");
    assert!(
        token.0.starts_with("s1.") && token.0.ends_with(".next-1"),
        "{token:?}"
    );
    assert_eq!(
        page.mirror_state,
        FeedPosition {
            generation: status.generation,
            at: status.state.into(),
        }
    );
    assert_eq!(page.cursor, status.cursor);
    assert!(page.cursor.is_some());
    assert_eq!(page.phase, SyncPhase::Live);
    let searches = fixture.searches();
    let [request] = searches.as_slice() else {
        panic!("one messages.list: {searches:?}");
    };
    assert_eq!(request.path, "/gmail/v1/users/me/messages");
    assert_eq!(param(request, "q"), Some("from:alice"));
    assert_eq!(param(request, "maxResults"), Some("100"));
    assert_eq!(param(request, "includeSpamTrash"), None);
}

#[tokio::test]
async fn a_search_writes_nothing() {
    let fixture = Fixture::new();
    fixture.gmail().add("m1", &["INBOX"]);
    fixture.sync().await;
    let before = fixture.status().await;
    let feed_before = fixture
        .runner
        .store()
        .changes(&account(), before.state.into(), 100)
        .await
        .expect("feed");
    fixture.answer_page(&["m1", "unknown-1", "unknown-2"], None);

    let page = fixture.search(&query("is:unread")).await.expect("a page");

    assert_eq!(page.hits[1].state, HitState::Unknown);
    let after = fixture.status().await;
    assert_eq!(before, after, "no commit, row, job, epoch or feed entry");
    let feed_after = fixture
        .runner
        .store()
        .changes(&account(), before.state.into(), 100)
        .await
        .expect("feed");
    assert_eq!(feed_before, feed_after);
    let views = fixture
        .runner
        .store()
        .messages(&account(), &[MessageId("unknown-1".into())])
        .await
        .expect("views");
    assert!(views.is_empty(), "an unknown hit gets no row");
}

#[tokio::test]
async fn a_hit_gmail_has_and_the_mirror_does_not_is_unknown_until_the_next_sync() {
    let fixture = Fixture::new();
    fixture.gmail().add("m1", &["INBOX"]);
    fixture.sync().await;
    fixture.gmail().add("m2", &["INBOX"]);
    fixture.answer_page(&["m2"], None);
    let ahead = fixture.search(&query("newer")).await.expect("a page");
    assert_eq!(ahead.hits[0].state, HitState::Unknown);

    fixture.sync().await;
    fixture.answer_page(&["m2"], None);
    let caught_up = fixture.search(&query("newer")).await.expect("a page");
    assert_eq!(caught_up.hits[0].state, HitState::Imported);
    assert!(caught_up.cursor > ahead.cursor, "history brought it");
}

#[tokio::test]
async fn a_hit_the_mirror_tombstoned_is_not_returned() {
    let fixture = Fixture::new();
    fixture.gmail().add("m1", &["INBOX"]);
    fixture.gmail().add("m2", &["INBOX"]);
    fixture.sync().await;
    fixture
        .seed(
            vec![MessageOp::Tombstone {
                id: MessageId("m2".into()),
                evidence: DeletionEvidence::History { at: HistoryId(100) },
            }],
            Vec::new(),
        )
        .await;
    fixture.answer_page(&["m2", "m1"], None);

    let page = fixture.search(&query("invoice")).await.expect("a page");

    assert_eq!(hit_ids(&page), ["m1"]);
    assert_eq!(page.deleted, 1);
    let views = fixture
        .runner
        .store()
        .messages(&account(), &[MessageId("m2".into())])
        .await
        .expect("views");
    assert!(views.is_empty(), "never resurrected");
}

#[tokio::test]
async fn a_search_during_bootstrap_reports_the_phase() {
    let fixture = Fixture::new();
    fixture.gmail().add("m1", &["INBOX"]);
    fixture.answer_page(&["m1"], None);

    let page = fixture.search(&query("hello")).await.expect("a page");

    assert_eq!(page.phase, SyncPhase::Bootstrapping);
    assert_eq!(page.cursor, None);
    assert_eq!(page.hits[0].state, HitState::Unknown);
}

#[tokio::test]
async fn a_search_does_not_supersede_a_running_sync() {
    let fixture = Fixture::new();
    for id in ["m1", "m2", "m3"] {
        fixture.gmail().add(id, &["INBOX"]);
    }
    fixture.answer_page(&["m1"], None);
    fixture.gmail().hold();
    let hello = query("hello");

    // The run claims first; the search starts during it and is answered
    // only once the run is over.
    let (outcome, searched) = tokio::join!(
        async {
            let outcome = fixture.sync().await;
            fixture.gmail().release();
            outcome
        },
        fixture.search(&hello),
    );

    assert!(
        matches!(outcome.stop, StopReason::Idle { .. }),
        "{:?}",
        outcome.stop
    );
    assert_eq!(outcome.report.imported, 3, "the run finished its work");
    assert!(searched.is_ok(), "{searched:?}");
    assert_eq!(
        fixture.status().await.epoch,
        Epoch(1),
        "only the run claimed"
    );
}

#[tokio::test]
async fn a_deferred_search_waits_within_its_budget() {
    let waited = Fixture::with(Options {
        defer_first: 1,
        defer_by: Duration::from_millis(50),
        ..Options::default()
    });
    waited.answer_page(&["m1"], None);
    assert!(waited.search(&query("x")).await.is_ok());
    assert_eq!(waited.admitted().len(), 2, "asked again after the wait");
    assert_eq!(waited.searches().len(), 1);

    let deferred = Fixture::with(Options {
        defer_first: 1,
        defer_by: Duration::from_mins(1),
        ..Options::default()
    });
    let before = mailtide_io::now();
    let error = deferred
        .runner
        .search(&account(), &query("x"), Duration::from_secs(1))
        .await
        .expect_err("deferred");
    let SearchError::Deferred(QuotaDeferred { until }) = error else {
        panic!("{error:?}");
    };
    assert!(until >= before.plus(Duration::from_secs(59)), "{until:?}");
    assert!(deferred.searches().is_empty(), "nothing sent");
    assert_eq!(deferred.tokens_minted(), 0);
}

#[tokio::test]
async fn a_background_search_yields_to_foreground_work() {
    // 10 units a minute: Backfill may use 7, Foreground all 10.
    let fixture = Fixture::with(Options {
        budget: Budget {
            account_per_minute: 10,
            ..Budget::GMAIL_DEFAULT
        },
        ..Options::default()
    });
    let background = SearchQuery {
        priority: Priority::Backfill,
        ..query("older:1y")
    };
    fixture.answer_page(&["m1"], None);
    fixture.answer_page(&["m2"], None);
    assert!(fixture.search(&background).await.is_ok());

    let held_back = fixture
        .runner
        .search(&account(), &background, Duration::from_millis(100))
        .await;
    assert!(
        matches!(held_back, Err(SearchError::Deferred(_))),
        "{held_back:?}"
    );
    let foreground = fixture.search(&query("from:alice")).await;
    assert!(foreground.is_ok(), "{foreground:?}");
    assert_eq!(
        fixture.admitted(),
        [
            (QuotaUnits(5), Priority::Backfill),
            (QuotaUnits(5), Priority::Backfill),
            (QuotaUnits(5), Priority::Foreground),
        ]
    );
    assert_eq!(fixture.searches().len(), 2);
}

#[tokio::test]
async fn a_rate_limited_search_cools_the_gate_down() {
    let fixture = Fixture::new();
    fixture
        .gmail()
        .answer(Ok(refusal(403, "userRateLimitExceeded")));
    let before = mailtide_io::now();

    let error = fixture.search(&query("x")).await.expect_err("limited");

    assert!(
        matches!(&error, SearchError::Retryable(f) if f.class == Retryable::AccountRateLimited),
        "{error:?}"
    );
    let cooldowns = fixture.cooldowns();
    let [cooldown] = cooldowns.as_slice() else {
        panic!("{cooldowns:?}");
    };
    assert_eq!(cooldown.target, QuotaTarget::Account(account()));
    assert!(cooldown.until >= before.plus(Duration::from_secs(300)));
    let next = fixture
        .runner
        .search(&account(), &query("x"), Duration::from_secs(1))
        .await;
    assert!(matches!(next, Err(SearchError::Deferred(_))), "{next:?}");
    assert_eq!(fixture.searches().len(), 1, "the second never left");

    let unlimited = Fixture::new();
    unlimited
        .gmail()
        .answer(Ok(refusal(429, "rateLimitExceeded")));
    let error = unlimited.search(&query("x")).await.expect_err("limited");
    assert!(
        matches!(&error, SearchError::Retryable(f) if f.class == Retryable::AccountRateLimited),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_refused_search_does_not_block_the_account() {
    let fixture = Fixture::new();
    fixture.gmail().add("m1", &["INBOX"]);
    fixture.sync().await;
    fixture.gmail().answer(Ok(refusal(400, "invalidArgument")));
    fixture
        .gmail()
        .answer(Ok(refusal(403, "insufficientPermissions")));

    let bad_query = fixture.search(&query("x")).await;
    assert!(
        matches!(&bad_query, Err(SearchError::Refused(r)) if r.class == Blocking::Permanent),
        "{bad_query:?}"
    );
    let metadata_scope = fixture.search(&query("x")).await;
    assert!(
        matches!(
            &metadata_scope,
            Err(SearchError::Refused(BlockingFailure {
                class: Blocking::Account(AccountProblem::ApiAccessDenied),
                ..
            }))
        ),
        "{metadata_scope:?}"
    );

    assert!(!matches!(
        fixture.status().await.sync,
        SyncState::Blocked { .. }
    ));
    fixture.gmail().add("m2", &["INBOX"]);
    let outcome = fixture.sync().await;
    assert!(
        matches!(outcome.stop, StopReason::Idle { .. }),
        "{:?}",
        outcome.stop
    );
    assert_eq!(outcome.report.imported, 1, "the account still syncs");
}

#[tokio::test]
async fn a_continuation_gmail_rejects_is_a_rejected_page_token() {
    let fixture = Fixture::new();
    let first = query("from:alice");
    for rejection in [refusal(400, "invalidArgument"), refusal(404, "notFound")] {
        fixture.answer_page(&["m1"], Some("n1"));
        let page = fixture.search(&first).await.expect("the first page");
        fixture.gmail().answer(Ok(rejection));
        let next = SearchQuery {
            page_token: page.next_page_token,
            ..first.clone()
        };
        assert_eq!(
            fixture.search(&next).await,
            Err(SearchError::PageTokenRejected)
        );
    }
    let searches = fixture.searches();
    assert_eq!(param(&searches[1], "pageToken"), Some("n1"));

    // The same answer to a first page is the query's, not a token's.
    fixture.gmail().answer(Ok(refusal(404, "notFound")));
    let first_page = fixture.search(&first).await;
    assert!(
        matches!(first_page, Err(SearchError::Refused(_))),
        "{first_page:?}"
    );
}

#[tokio::test]
async fn search_works_in_read_only_mode() {
    let fixture = Fixture::with(Options {
        access: Access::ReadOnly,
        ..Options::default()
    });
    fixture.gmail().add("m1", &["INBOX"]);
    fixture.sync().await;
    fixture.answer_page(&["m1"], None);

    let page = fixture.search(&query("hello")).await.expect("a page");

    assert_eq!(page.hits[0].state, HitState::Imported);
}

#[tokio::test]
async fn search_on_a_blocked_account_makes_no_credential_or_gmail_call() {
    let fixture = Fixture::new();
    let reason = BlockingFailure {
        class: Blocking::Account(AccountProblem::DelegationRevoked),
        detail: "unauthorized_client".into(),
    };
    fixture
        .seed(Vec::new(), vec![AccountOp::Block(reason.clone())])
        .await;

    assert_eq!(
        fixture.search(&query("x")).await,
        Err(SearchError::AccountBlocked(reason))
    );
    assert_eq!(fixture.tokens_minted(), 0);
    assert_eq!(fixture.requests(), 0);
    assert!(fixture.admitted().is_empty());
}

#[tokio::test]
async fn search_on_an_untracked_account_is_refused() {
    let fixture = Fixture::new();
    let stranger = AccountId::parse("carol@example.com").expect("account");

    let error = fixture
        .runner
        .search(&stranger, &query("x"), Duration::from_secs(1))
        .await;

    assert_eq!(error, Err(SearchError::NotTracked));
    assert_eq!(fixture.requests(), 0);
    assert!(
        fixture
            .runner
            .store()
            .status(&stranger)
            .await
            .expect("status")
            .is_none(),
        "search never starts mirroring an account"
    );
}

#[tokio::test]
async fn pages_of_a_changing_mailbox_may_repeat_or_skip_ids() {
    let fixture = Fixture::new();
    let first = query("label:clients");
    // A message arriving between the pages pushes m2 onto the second page
    // again; a deleted one can make Gmail skip another.
    fixture.answer_page(&["m1", "m2"], Some("p2"));
    fixture.answer_page(&["m2", "m3"], None);

    let one = fixture.search(&first).await.expect("page 1");
    let two = fixture
        .search(&SearchQuery {
            page_token: one.next_page_token.clone(),
            ..first
        })
        .await
        .expect("page 2");

    assert_eq!(hit_ids(&one), ["m1", "m2"]);
    assert_eq!(hit_ids(&two), ["m2", "m3"], "the repeat is Gmail's, kept");
    assert_eq!(two.next_page_token, None);
}

fn filtered(labels: &[&str]) -> SearchQuery {
    SearchQuery {
        label_ids: labels.iter().map(|l| LabelId((*l).into())).collect(),
        ..query("invoice")
    }
}

#[tokio::test]
async fn a_label_id_gmail_does_not_have_is_unknown_and_never_searched() {
    let fixture = Fixture::new();
    fixture.sync().await;
    let synced = fixture.admitted().len();

    // Deleted or never created: labels.get answers 404.
    assert_eq!(
        fixture
            .search(&filtered(&["INBOX", "Label_9", "Label_9"]))
            .await,
        Err(SearchError::UnknownLabel(LabelId("Label_9".into())))
    );
    assert_eq!(
        fixture.label_gets(),
        ["Label_9"],
        "once, and only the missing id"
    );
    assert!(fixture.searches().is_empty(), "no search was sent");
    assert_eq!(
        fixture.admitted()[synced..],
        [(QuotaUnits(1), Priority::Foreground)],
        "labels.get is charged 1 unit at the search's priority"
    );

    // Any other failure of the check maps as the search's own would.
    fixture
        .gmail()
        .answer_label_gets(refusal(503, "backendError"));
    let failed = fixture.search(&filtered(&["Label_9"])).await;
    assert!(
        matches!(&failed, Err(SearchError::Retryable(f)) if f.class == Retryable::Transient),
        "{failed:?}"
    );
    assert!(fixture.searches().is_empty());
}

#[tokio::test]
async fn a_label_id_newer_than_the_mirrors_listing_is_checked_with_gmail_and_searched() {
    let fixture = Fixture::new();
    // No label listing yet: the mirror holds no definitions at all.
    fixture.answer_page(&["m1"], None);
    let page = fixture
        .search(&filtered(&["INBOX"]))
        .await
        .expect("Gmail has INBOX");
    assert_eq!(page.hits.len(), 1);
    assert_eq!(fixture.label_gets(), ["INBOX"]);

    // Created in Gmail after the mirror's last listing.
    fixture.sync().await;
    fixture.gmail().create_label("Label_2", "Projects");
    fixture.answer_page(&[], None);
    fixture
        .search(&filtered(&["Label_2", "Label_1", "INBOX", "Label_2"]))
        .await
        .expect("Gmail has Label_2");
    assert_eq!(
        fixture.label_gets(),
        ["INBOX", "Label_2"],
        "one get per distinct id the mirror lacks; none for the ones it holds"
    );
    let searches = fixture.searches();
    let labels: Vec<&str> = searches[1]
        .query
        .iter()
        .filter(|(k, _)| k == "labelIds")
        .map(|(_, v)| v.as_str())
        .collect();
    assert_eq!(labels, ["INBOX", "Label_1", "Label_2"]);
    let views = fixture.runner.labels(&account()).await.expect("labels");
    assert!(
        views.iter().all(|label| label.id.0 != "Label_2"),
        "search records nothing; the engine's refresh will"
    );
}

/// A lost answer is a transient failure: a search is a read.
#[tokio::test]
async fn a_lost_answer_is_retryable() {
    let fixture = Fixture::new();
    fixture
        .gmail()
        .answer(Err(TransportError::OutcomeUnknown("timed out".into())));
    let error = fixture.search(&query("x")).await;
    assert!(
        matches!(&error, Err(SearchError::Retryable(f)) if f.class == Retryable::Transient),
        "{error:?}"
    );
}

#[tokio::test]
async fn search_is_charged_five_units_and_thread_search_ten() {
    let fixture = Fixture::new();
    fixture.answer_page(&["m1"], None);
    fixture
        .gmail()
        .answer(Ok(thread_page(&[("t-m1", Some(7))], None)));

    fixture.search(&query("x")).await.expect("messages");
    fixture.search_threads(&query("x")).await.expect("threads");

    assert_eq!(
        fixture.admitted(),
        [
            (QuotaUnits(5), Priority::Foreground),
            (QuotaUnits(10), Priority::Foreground),
        ]
    );
}

#[tokio::test]
async fn thread_search_returns_thread_ids_with_gmails_history_ids() {
    let fixture = Fixture::new();
    fixture.gmail().add("m1", &["INBOX"]);
    fixture.sync().await;
    let status = fixture.status().await;
    fixture.gmail().answer(Ok(thread_page(
        &[("t-m1", Some(1)), ("t-new", Some(99)), ("t-bare", None)],
        Some("tn"),
    )));

    let page = fixture
        .search_threads(&query("has:attachment"))
        .await
        .expect("a page");

    assert_eq!(
        page.hits,
        [
            ThreadHit {
                thread_id: ThreadId("t-m1".into()),
                history_id: Some(HistoryId(1)),
            },
            ThreadHit {
                thread_id: ThreadId("t-new".into()),
                history_id: Some(HistoryId(99)),
            },
            ThreadHit {
                thread_id: ThreadId("t-bare".into()),
                history_id: None,
            },
        ]
    );
    // The mirror trails Gmail for a thread whose history id is past it.
    assert_eq!(page.cursor, status.cursor);
    assert!(page.hits[1].history_id > page.cursor);
    assert_eq!(page.estimate, 3);
    assert_eq!(page.phase, SyncPhase::Live);
    fixture.gmail().answer(Ok(thread_page(&[], None)));
    let last = fixture
        .search_threads(&SearchQuery {
            page_token: page.next_page_token,
            ..query("has:attachment")
        })
        .await
        .expect("the next page");
    assert!(last.hits.is_empty() && last.next_page_token.is_none());
    let searches = fixture.searches();
    assert_eq!(searches[0].path, "/gmail/v1/users/me/threads");
    assert_eq!(param(&searches[1], "pageToken"), Some("tn"));
}
