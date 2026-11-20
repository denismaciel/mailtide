//! Purge and empty against a fake Gmail that deletes for good: the policy,
//! the history check, lost answers, scopes and emptying a bin.
//! Plan: `features/purge.toml`.

pub mod purge_host;

use std::time::{Duration, Instant};

use mailtide::command::GmailRequest;
use mailtide::ids::{HistoryId, MessageId, ThreadId};
use mailtide::outbox::{Action, ActionRequest, RestoreDestination, StarState};
use mailtide::purge::{Bin, PURGE_MAX_IDS, PurgePolicy, PurgeTarget, Refusal};
use mailtide::quota::Priority;
use mailtide::reconcile::{ReconcileProgress, ReconcileReason};
use mailtide::state::SyncState;
use mailtide_io::purge::{PurgeError, Stale};
use mailtide_io::runner::{Access, CallError, QuotaDeferred, StopReason};
use mailtide_store::Store as _;
use purge_host::{BUDGET, Host, MODIFY_SCOPE, account, error, ids, names};

/// A host whose mirror holds `messages` (id, labels), synced.
async fn mirrored(policy: PurgePolicy, messages: &[(&str, &[&str])]) -> Host {
    let host = Host::new(policy);
    {
        let mut gmail = host.gmail();
        for (id, labels) in messages {
            gmail.add(id, labels);
        }
    }
    host.mirror_gmail().await;
    host
}

/// `n` messages in `label`, named `<prefix>0000`, `<prefix>0001`, ...
fn many(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}{i:04}")).collect()
}

async fn mirrored_many(policy: PurgePolicy, prefix: &str, n: usize, label: &str) -> Host {
    let host = Host::new(policy);
    {
        let mut gmail = host.gmail();
        for id in many(prefix, n) {
            gmail.add(&id, &[label]);
        }
    }
    host.mirror_gmail().await;
    host
}

fn refusals(refused: &[(MessageId, Refusal)]) -> Vec<(&str, &'static str)> {
    refused
        .iter()
        .map(|(id, refusal)| (id.0.as_str(), refusal.code()))
        .collect()
}

#[tokio::test]
async fn purge_is_off_by_default() {
    let host = Host::without_policy();
    host.gmail().add("t", &["TRASH"]);
    host.mirror_gmail().await;
    assert_eq!(host.purge(&["t"]).await, Err(PurgeError::Disabled));
    assert_eq!(
        host.empty(Bin::Trash, BUDGET).await,
        Err(PurgeError::Disabled)
    );
    assert!(host.gmail().requests.is_empty(), "nothing reached Gmail");
    assert!(host.in_gmail("t"));

    let on = mirrored(PurgePolicy::TrashAndSpam, &[("t", &["TRASH"])]).await;
    let outcome = on.purge(&["t"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["t"]);
}

#[tokio::test]
async fn purge_is_refused_in_read_only_mode() {
    let host = Host::with(PurgePolicy::Anywhere, Access::ReadOnly);
    host.gmail().add("t", &["TRASH"]);
    host.mirror_gmail().await;
    assert_eq!(host.purge(&["t"]).await, Err(PurgeError::ReadOnly));
    assert_eq!(
        host.purge_target(&PurgeTarget::Messages(ids(&["t"])), true)
            .await,
        Err(PurgeError::ReadOnly)
    );
    assert_eq!(
        host.empty(Bin::Trash, BUDGET).await,
        Err(PurgeError::ReadOnly)
    );
    assert!(host.gmail().requests.is_empty());
}

#[tokio::test]
async fn anywhere_needs_the_policy_and_the_flag() {
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("i", &["INBOX"])]).await;
    let anywhere = PurgeTarget::Messages(ids(&["i"]));
    assert_eq!(
        host.purge_target(&anywhere, true).await,
        Err(PurgeError::AnywhereNotAllowed)
    );
    assert!(host.gmail().requests.is_empty(), "nothing is sent");

    let host = mirrored(PurgePolicy::Anywhere, &[("i", &["INBOX"])]).await;
    // The ceiling alone is not enough: the call must ask.
    let outcome = host.purge(&["i"]).await.expect("purge");
    assert_eq!(refusals(&outcome.refused), [("i", "not_in_trash_or_spam")]);
    assert!(host.gmail().deletes().is_empty());
    let outcome = host
        .purge_target(&anywhere, true)
        .await
        .expect("purge anywhere");
    assert_eq!(names(&outcome.purged), ["i"]);
    assert!(!host.in_gmail("i"));
}

#[tokio::test]
async fn a_purged_message_is_a_tombstone_before_the_next_sync() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[
            ("t", &["TRASH", "Label_1"]),
            ("s", &["SPAM"]),
            ("i", &["INBOX"]),
        ],
    )
    .await;
    let cursor = host.mirror().record.cursor;
    let outcome = host.purge(&["t", "s"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["t", "s"]);
    assert!(outcome.sent.is_empty() && outcome.refused.is_empty() && outcome.unsent.is_empty());
    assert!(!host.in_gmail("t") && !host.in_gmail("s"));
    // Recorded when purge returns, not at the next sync.
    assert!(!host.mirrored("t") && !host.mirrored("s"));
    assert_eq!(host.deleted_in_feed(), ["t", "s"]);
    let mirror = host.mirror();
    assert!(mirror.tombstones.contains_key(&MessageId("t".into())));
    assert_eq!(mirror.record.cursor, cursor, "the cursor never moves");
    // The sync reads the same records and logs nothing more.
    host.sync().await;
    assert_eq!(host.deleted_in_feed(), ["t", "s"]);
}

#[tokio::test]
async fn a_stale_mirror_refuses_the_whole_purge() {
    // No cursor: the first sync hasn't finished.
    let host = Host::new(PurgePolicy::TrashAndSpam);
    host.gmail().add("t", &["TRASH"]);
    assert_eq!(
        host.purge(&["t"]).await,
        Err(PurgeError::MirrorStale(Stale::NoCursor))
    );
    assert!(host.gmail().requests.is_empty());

    // Not live: reconciling after history was lost.
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("t", &["TRASH"])]).await;
    let snapshot = host
        .runner
        .store()
        .claim(&account(), mailtide::ids::ENGINE_VERSION)
        .await
        .expect("claim");
    let mut reconcile = mailtide::commit::Commit {
        id: mailtide::ids::CommitId {
            epoch: snapshot.epoch,
            seq: 0,
        },
        account: account(),
        epoch: snapshot.epoch,
        engine_version: snapshot.engine_version,
        cursor: None,
        claim: None,
        sync: Some(SyncState::Reconciling(ReconcileProgress::begin(
            ReconcileReason::HistoryExpired,
        ))),
        messages: Vec::new(),
        labels: Vec::new(),
        account_ops: Vec::new(),
        job_plan: mailtide::commit::JobPlan {
            priority: mailtide::quota::Priority::Live,
            import_due_at: mailtide::ids::Timestamp(0),
            draft_import_due_at: mailtide::ids::Timestamp(0),
            verify_due_at: mailtide::ids::Timestamp(0),
            cleanup_due_at: mailtide::ids::Timestamp(0),
            deliver_due_at: mailtide::ids::Timestamp(0),
            confirm_due_at: mailtide::ids::Timestamp(0),
        },
        jobs: Vec::new(),
        intents: Vec::new(),
    };
    host.runner.store().apply(&reconcile).await.expect("apply");
    assert_eq!(
        host.purge(&["t"]).await,
        Err(PurgeError::MirrorStale(Stale::NotLive))
    );
    reconcile.sync = Some(SyncState::Live);
    reconcile.id.seq = 1;
    host.runner.store().apply(&reconcile).await.expect("apply");

    // History from the cursor is gone (404).
    let head = host.gmail().history_id;
    host.gmail().history_floor = head + 1;
    assert_eq!(
        host.purge(&["t"]).await,
        Err(PurgeError::MirrorStale(Stale::HistoryExpired))
    );
    host.gmail().history_floor = 0;

    // More than PURGE_HISTORY_PAGES pages behind.
    {
        let mut gmail = host.gmail();
        gmail.page_size = 1;
        for n in 0..21 {
            gmail.add(&format!("new{n}"), &["INBOX"]);
        }
    }
    assert_eq!(
        host.purge(&["t"]).await,
        Err(PurgeError::MirrorStale(Stale::TooFarBehind))
    );
    assert!(host.gmail().deletes().is_empty(), "nothing is sent");
    assert!(host.in_gmail("t"));
}

#[tokio::test]
async fn a_token_without_the_full_scope_is_scope_required_and_blocks_nothing() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[("t", &["TRASH"]), ("i", &["INBOX"])],
    )
    .await;
    host.gmail().scope = MODIFY_SCOPE;
    let refused = host.purge(&["t"]).await;
    assert!(
        matches!(&refused, Err(PurgeError::ScopeRequired(detail)) if detail.contains("insufficientPermissions")),
        "Gmail's refusal is kept: {refused:?}"
    );
    assert_eq!(
        mailtide_gmail::purge::FULL_SCOPE,
        "https://mail.google.com/",
        "the scope the error names"
    );
    // Nothing deleted, and no fallback to trash: no label change was sent.
    assert!(host.in_gmail("t"));
    assert!(
        !host
            .gmail()
            .requests
            .iter()
            .any(|r| r.contains("modify") || r.contains("trash")),
        "{:?}",
        host.gmail().requests
    );
    assert!(host.mirrored("t"));
    // The account keeps syncing.
    host.gmail().add("n", &["INBOX"]);
    let outcome = host.sync().await;
    assert!(
        matches!(outcome.stop, StopReason::Idle { .. }),
        "{:?}",
        outcome.stop
    );
    assert!(host.mirrored("n"));
    assert!(matches!(host.mirror().record.sync, SyncState::Live));
    // Emptying is refused the same way.
    assert!(matches!(
        host.empty(Bin::Trash, BUDGET).await,
        Err(PurgeError::ScopeRequired(_))
    ));
}

#[tokio::test]
async fn few_ids_use_delete_and_many_use_batch_delete() {
    let four = ["a", "b", "c", "d"];
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &four.map(|id| (id, &["TRASH"][..])),
    )
    .await;
    host.purge(&four).await.expect("purge");
    assert_eq!(
        host.gmail().deletes(),
        ["DELETE a", "DELETE b", "DELETE c", "DELETE d"]
    );

    let five = ["a", "b", "c", "d", "e"];
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &five.map(|id| (id, &["TRASH"][..])),
    )
    .await;
    let outcome = host.purge(&five).await.expect("purge");
    assert_eq!(host.gmail().deletes(), ["BATCH a,b,c,d,e"]);
    assert_eq!(outcome.purged.len(), 5);

    let host = mirrored_many(PurgePolicy::TrashAndSpam, "m", PURGE_MAX_IDS, "TRASH").await;
    let all = many("m", PURGE_MAX_IDS);
    let all: Vec<&str> = all.iter().map(String::as_str).collect();
    let outcome = host.purge(&all).await.expect("purge");
    let deletes = host.gmail().deletes();
    assert_eq!(deletes.len(), 1, "one batchDelete of 1,000 ids");
    assert_eq!(deletes[0].split(',').count(), PURGE_MAX_IDS);
    assert_eq!(outcome.purged.len(), PURGE_MAX_IDS);
}

#[tokio::test]
async fn more_than_a_thousand_ids_are_refused() {
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("t", &["TRASH"])]).await;
    let named = many("x", PURGE_MAX_IDS + 1);
    let named: Vec<&str> = named.iter().map(String::as_str).collect();
    assert_eq!(
        host.purge(&named).await,
        Err(PurgeError::TooMany {
            limit: PURGE_MAX_IDS,
            given: PURGE_MAX_IDS + 1
        })
    );
    assert!(host.gmail().requests.is_empty(), "not even history is read");
    // Duplicates count once.
    let twice = ["t"; PURGE_MAX_IDS + 1];
    let outcome = host.purge(&twice).await.expect("one distinct id");
    assert_eq!(names(&outcome.purged), ["t"]);
}

#[tokio::test]
async fn a_delete_whose_answer_was_lost_is_never_sent_again() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[("a", &["TRASH"]), ("b", &["TRASH"]), ("c", &["TRASH"])],
    )
    .await;
    {
        let mut gmail = host.gmail();
        gmail.lose_delete_answers = true;
        gmail.late_evidence = true;
    }
    let outcome = host.purge(&["a", "b", "c"]).await.expect("purge");
    assert_eq!(names(&outcome.sent), ["a"]);
    assert_eq!(names(&outcome.unsent), ["b", "c"]);
    assert_eq!(outcome.interrupted.as_deref(), Some("outcome_unknown"));
    assert_eq!(host.gmail().deletes(), ["DELETE a"]);
    assert!(host.mirrored("a") && host.in_gmail("b") && host.in_gmail("c"));
    host.gmail().release_evidence();
    host.sync().await;
    assert!(!host.mirrored("a"));
    assert_eq!(host.gmail().deletes(), ["DELETE a"]);
}

#[tokio::test]
async fn a_delete_gmail_has_not_shown_in_history_is_reported_as_sent() {
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
    host.gmail().late_evidence = true;
    let outcome = host.purge(&["a"]).await.expect("purge");
    assert!(outcome.purged.is_empty());
    assert_eq!(names(&outcome.sent), ["a"]);
    assert!(!host.in_gmail("a"), "Gmail deleted it");
    assert!(host.mirrored("a"), "the row stays until the evidence");
    assert!(host.deleted_in_feed().is_empty());
    host.gmail().release_evidence();
    host.sync().await;
    assert!(!host.mirrored("a"));
    assert_eq!(host.deleted_in_feed(), ["a"], "one MessageDeleted");
}

#[tokio::test]
async fn a_crash_after_the_delete_heals_at_the_next_sync() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[("a", &["TRASH"]), ("b", &["TRASH"])],
    )
    .await;
    // The confirmation read never comes back: as if the host died after
    // Gmail deleted and before anything was recorded.
    host.gmail().history_fails_after_deletes = true;
    let outcome = host.purge(&["a", "b"]).await.expect("purge");
    assert_eq!(names(&outcome.sent), ["a"]);
    assert_eq!(names(&outcome.unsent), ["b"]);
    assert!(!host.in_gmail("a") && host.in_gmail("b"));
    assert!(host.mirrored("a") && host.mirrored("b"));
    host.gmail().history_fails_after_deletes = false;
    host.sync().await;
    assert!(!host.mirrored("a") && host.mirrored("b"));
    assert_eq!(host.deleted_in_feed(), ["a"]);
    assert_eq!(host.gmail().deletes(), ["DELETE a"]);
}

#[tokio::test]
async fn a_deferred_purge_sends_nothing_more() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[("a", &["TRASH"]), ("b", &["TRASH"]), ("c", &["TRASH"])],
    )
    .await;
    host.admit_deletes(1);
    let outcome = host.purge(&["a", "b", "c"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["a"], "the chunk sent is confirmed");
    assert_eq!(names(&outcome.unsent), ["b", "c"]);
    assert!(outcome.retry_at.is_some(), "the time it could go");
    assert_eq!(host.gmail().deletes(), ["DELETE a"]);
    assert!(host.in_gmail("b") && host.in_gmail("c"));
    // Deferred before the first delete: nothing is sent at all.
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
    host.admit_deletes(0);
    let outcome = host.purge(&["a"]).await.expect("purge");
    assert_eq!(names(&outcome.unsent), ["a"]);
    assert!(outcome.retry_at.is_some());
    assert!(host.gmail().deletes().is_empty());
}

#[tokio::test]
async fn a_delete_never_waits_for_the_quota_gate() {
    // The gate would admit the delete 50 ms later, well within the budget:
    // waiting would send it long after its check, so it is not sent.
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
    host.defer_deletes_after(0, Duration::from_millis(50));
    let outcome = host.purge(&["a"]).await.expect("purge");
    assert_eq!(names(&outcome.unsent), ["a"]);
    assert!(outcome.retry_at.is_some());
    assert!(host.gmail().deletes().is_empty(), "nothing waited and went");
    assert!(host.in_gmail("a"));
    // The same in every chunk of an empty.
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
    host.defer_deletes_after(0, Duration::from_millis(50));
    let outcome = host.empty(Bin::Trash, BUDGET).await.expect("empty");
    assert_eq!((outcome.purged, outcome.remaining), (0, 1));
    assert!(host.gmail().deletes().is_empty());
}

#[tokio::test]
async fn a_restore_admitted_here_during_the_history_read_refuses_the_id() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[("a", &["TRASH"]), ("b", &["TRASH"])],
    )
    .await;
    host.act_during_history_read(ActionRequest::Messages {
        ids: ids(&["a"]),
        action: Action::RestoreFromTrash(RestoreDestination::Inbox),
    });
    let outcome = host.purge(&["a", "b"]).await.expect("purge");
    assert_eq!(refusals(&outcome.refused), [("a", "not_in_trash_or_spam")]);
    assert_eq!(names(&outcome.purged), ["b"]);
    assert_eq!(host.gmail().deletes(), ["DELETE b"]);
}

#[tokio::test]
async fn a_429_or_a_5xx_on_a_delete_is_never_resent() {
    let three = [
        ("a", &["TRASH"][..]),
        ("b", &["TRASH"][..]),
        ("c", &["TRASH"][..]),
    ];
    // A rate limit: nothing was applied, and nothing more is sent.
    let host = mirrored(PurgePolicy::TrashAndSpam, &three).await;
    let mut limited = error(429, "rateLimitExceeded", "Rate Limit Exceeded");
    limited.headers.push(("retry-after".into(), "30".into()));
    host.gmail().refuse_deletes = Some(limited);
    let outcome = host.purge(&["a", "b", "c"]).await.expect("purge");
    assert_eq!(names(&outcome.unsent), ["a", "b", "c"]);
    assert!(outcome.retry_at.is_some(), "Retry-After says when");
    assert_eq!(host.gmail().deletes(), ["DELETE a"]);
    // A 5xx may have been applied: sent, never resent, and the call stops.
    let host = mirrored(PurgePolicy::TrashAndSpam, &three).await;
    host.gmail().refuse_deletes = Some(error(500, "backendError", "Backend Error"));
    let outcome = host.purge(&["a", "b", "c"]).await.expect("purge");
    assert_eq!(names(&outcome.sent), ["a"]);
    assert_eq!(names(&outcome.unsent), ["b", "c"]);
    assert!(host.mirrored("a"), "no evidence, no tombstone");
    assert_eq!(host.gmail().deletes(), ["DELETE a"]);
    // A 5xx after Gmail applied it: history proves it.
    let host = mirrored(PurgePolicy::TrashAndSpam, &three).await;
    host.gmail().answer_after_applying = Some(error(503, "backendError", "Backend Error"));
    let outcome = host.purge(&["a", "b", "c"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["a"]);
    assert_eq!(names(&outcome.unsent), ["b", "c"]);
    assert_eq!(host.gmail().deletes(), ["DELETE a"]);
}

#[tokio::test]
async fn a_lost_answer_on_a_batch_is_never_resent() {
    let five = ["a", "b", "c", "d", "e"];
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &five.map(|id| (id, &["TRASH"][..])),
    )
    .await;
    {
        let mut gmail = host.gmail();
        gmail.lose_delete_answers = true;
        gmail.late_evidence = true;
    }
    let outcome = host.purge(&five).await.expect("purge");
    assert_eq!(outcome.sent.len(), 5);
    assert!(outcome.purged.is_empty() && outcome.unsent.is_empty());
    assert_eq!(host.gmail().deletes(), ["BATCH a,b,c,d,e"], "sent once");
    assert!(host.mirrored("a"));
}

#[tokio::test]
async fn a_404_on_a_delete_tombstones_nothing() {
    let five = ["a", "b", "c", "d", "e"];
    // A batch answered 404 with Gmail's body.
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &five.map(|id| (id, &["TRASH"][..])),
    )
    .await;
    host.gmail().refuse_deletes = Some(error(404, "notFound", "Requested entity was not found."));
    let outcome = host.purge(&five).await.expect("purge");
    assert_eq!(outcome.sent.len(), 5);
    assert!(outcome.purged.is_empty());
    assert!(host.mirror().tombstones.is_empty());
    // A single delete answered 404 by something that is not Gmail (no
    // body): the message is live, and stays mirrored.
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
    host.gmail().refuse_deletes = Some(mailtide_gmail::http::HttpResponse {
        status: 404,
        headers: Vec::new(),
        body: Vec::new(),
    });
    let outcome = host.purge(&["a"]).await.expect("purge");
    assert_eq!(names(&outcome.sent), ["a"]);
    assert!(host.mirrored("a") && host.in_gmail("a"));
    assert!(host.mirror().tombstones.is_empty());
    assert!(host.deleted_in_feed().is_empty());
}

#[tokio::test]
async fn a_missing_scope_after_a_delete_succeeded_stops_the_call() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[("a", &["TRASH"]), ("b", &["TRASH"]), ("c", &["TRASH"])],
    )
    .await;
    host.gmail().after_delete = Some(Box::new(|gmail| {
        gmail.scope = MODIFY_SCOPE;
    }));
    let outcome = host.purge(&["a", "b", "c"]).await.expect("an outcome");
    assert_eq!(names(&outcome.purged), ["a"]);
    assert_eq!(refusals(&outcome.refused), [("b", "gmail_refused")]);
    assert_eq!(names(&outcome.unsent), ["c"]);
    assert_eq!(host.gmail().deletes(), ["DELETE a", "DELETE b"]);
}

#[tokio::test]
async fn a_refused_batch_is_checked_against_history() {
    let five = ["a", "b", "c", "d", "e"];
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &five.map(|id| (id, &["TRASH"][..])),
    )
    .await;
    {
        let mut gmail = host.gmail();
        gmail.batch_prefix = Some(2);
        gmail.answer_after_applying = Some(error(400, "invalidArgument", "Invalid ids"));
    }
    let outcome = host.purge(&five).await.expect("purge");
    assert_eq!(
        names(&outcome.purged),
        ["a", "b"],
        "history proves the deletions"
    );
    assert_eq!(names(&outcome.sent), ["c", "d", "e"]);
    assert!(outcome.refused.is_empty() && outcome.unsent.is_empty());
    assert_eq!(host.deleted_in_feed(), ["a", "b"]);
}

#[tokio::test]
async fn purging_twice_is_harmless() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[("a", &["TRASH"]), ("b", &["SPAM"])],
    )
    .await;
    host.purge(&["a", "b"]).await.expect("purge");
    let again = host.purge(&["a", "b"]).await.expect("again");
    assert_eq!(
        refusals(&again.refused),
        [("a", "not_in_mirror"), ("b", "not_in_mirror")]
    );
    assert!(again.purged.is_empty() && again.sent.is_empty());
    assert_eq!(host.gmail().deletes().len(), 2, "nothing more is sent");
    assert_eq!(host.deleted_in_feed(), ["a", "b"]);
}

#[tokio::test]
async fn a_purged_message_with_pending_actions_drops_them() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[("a", &["TRASH"]), ("b", &["TRASH"])],
    )
    .await;
    for id in ["a", "b"] {
        host.runner
            .admit(
                &account(),
                &ActionRequest::Messages {
                    ids: ids(&[id]),
                    action: Action::SetStar(StarState::Starred),
                },
            )
            .await
            .expect("admit");
    }
    host.purge(&["a"]).await.expect("purge");
    let mirror = host.mirror();
    assert!(mirror.intents_of(&MessageId("a".into())).is_empty());
    assert!(
        !mirror
            .jobs
            .contains_key(&mailtide::jobs::JobKind::Deliver(MessageId("a".into())))
    );
    let pause = host.pause_next_delivery();
    let syncing = tokio::spawn({
        let runner = host.runner.clone();
        async move { runner.run(&account(), BUDGET).await.expect("sync") }
    });
    tokio::time::timeout(BUDGET, pause.entered.notified())
        .await
        .expect("delivery in flight");
    let outcome = host.purge(&["b"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["b"]);
    assert!(host.mirror().intents.is_empty());
    pause.resume.notify_one();
    syncing.await.expect("sync joined");
    let modifies: Vec<String> = host
        .gmail()
        .requests
        .iter()
        .filter(|request| request.ends_with("/modify"))
        .cloned()
        .collect();
    assert_eq!(modifies, ["POST /gmail/v1/users/me/messages/b/modify"]);
    assert!(!host.mirrored("b"));
    assert!(host.mirror().intents.is_empty());
    assert_eq!(host.deleted_in_feed(), ["a", "b"]);
}

#[tokio::test]
async fn empty_trash_purges_what_the_mirror_holds_in_trash_and_nothing_else() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[
            ("t1", &["TRASH"]),
            ("t2", &["TRASH", "Label_1"]),
            ("d", &["DRAFT", "TRASH"]),
            ("s", &["SPAM"]),
            ("i", &["INBOX"]),
        ],
    )
    .await;
    let outcome = host.empty(Bin::Trash, BUDGET).await.expect("empty");
    assert_eq!(outcome.purged, 2);
    assert_eq!(outcome.skipped, 1, "the draft");
    assert_eq!((outcome.sent, outcome.remaining), (0, 0));
    {
        let gmail = host.gmail();
        assert!(!gmail.has("t1") && !gmail.has("t2"));
        assert!(gmail.has("d") && gmail.has("s") && gmail.has("i"));
    }
    let outcome = host.empty(Bin::Spam, BUDGET).await.expect("empty spam");
    assert_eq!(outcome.purged, 1);
    assert!(host.in_gmail("i"));
}

#[tokio::test]
async fn empty_leaves_a_message_trashed_elsewhere_after_the_last_sync() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[("i", &["INBOX"]), ("t", &["TRASH"])],
    )
    .await;
    // Trashed on a phone after the mirror last synced.
    host.gmail().trash("i");
    let outcome = host.empty(Bin::Trash, BUDGET).await.expect("empty");
    assert_eq!(outcome.purged, 1);
    assert!(
        host.in_gmail("i"),
        "the user never saw it in the trash here"
    );
    assert_eq!(host.gmail().deletes(), ["DELETE t"]);
}

#[tokio::test]
async fn empty_skips_messages_restored_while_it_runs() {
    let host = mirrored_many(PurgePolicy::TrashAndSpam, "m", 1_002, "TRASH").await;
    // While the first chunk (m0000..m0999) is deleted, a phone restores
    // m1001, which is in the chunk still to send.
    host.gmail().after_delete = Some(Box::new(|gmail| {
        if gmail.has("m1001") {
            gmail.restore("m1001");
        }
    }));
    let outcome = host.empty(Bin::Trash, BUDGET).await.expect("empty");
    assert_eq!(outcome.purged, 1_001);
    assert_eq!(outcome.skipped, 1);
    assert!(host.in_gmail("m1001"));
    let deletes = host.gmail().deletes();
    assert_eq!(deletes.len(), 2);
    assert_eq!(deletes[1], "DELETE m1000");
}

#[tokio::test]
async fn empty_stops_after_a_failed_confirmation() {
    let host = mirrored_many(PurgePolicy::TrashAndSpam, "m", 2_500, "TRASH").await;
    {
        let mut gmail = host.gmail();
        // After the first chunk history answers 503; a phone restores a
        // message of the second chunk meanwhile.
        gmail.history_fails_after_deletes = true;
        gmail.after_delete = Some(Box::new(|gmail| {
            if gmail.has("m1500") {
                gmail.restore("m1500");
            }
        }));
    }
    let outcome = host.empty(Bin::Trash, BUDGET).await.expect("empty");
    assert_eq!(outcome.sent, 1_000, "unconfirmed");
    assert_eq!(outcome.purged, 0);
    assert_eq!(outcome.remaining, 1_500);
    assert_eq!(outcome.interrupted.as_deref(), Some("retryable"));
    assert_eq!(host.gmail().deletes().len(), 1, "nothing further is sent");
    assert!(host.in_gmail("m1500"));
}

#[tokio::test]
async fn empty_reports_a_missing_scope_whenever_nothing_was_deleted_yet() {
    // The first page holds only drafts: nothing to delete there.
    let host = Host::new(PurgePolicy::TrashAndSpam);
    {
        let mut gmail = host.gmail();
        for id in many("d", 1_000) {
            gmail.add(&id, &["DRAFT", "TRASH"]);
        }
        gmail.add("z", &["TRASH"]);
        gmail.scope = MODIFY_SCOPE;
    }
    host.mirror_gmail().await;
    let refused = host.empty(Bin::Trash, BUDGET).await;
    assert!(
        matches!(refused, Err(PurgeError::ScopeRequired(_))),
        "{refused:?}"
    );
    assert!(host.in_gmail("z"));
}

#[tokio::test]
async fn empty_stops_at_its_budget_and_reports_the_rest() {
    let host = mirrored_many(PurgePolicy::TrashAndSpam, "m", 1_003, "TRASH").await;
    host.gmail().batch_delay = Duration::from_millis(300);
    let outcome = host
        .empty(Bin::Trash, Duration::from_millis(100))
        .await
        .expect("empty");
    assert_eq!(outcome.purged, 1_000);
    assert_eq!(outcome.remaining, 3);
    assert_eq!(host.gmail().deletes().len(), 1);
    // A second call is a new request: it reads the mirror again.
    let outcome = host.empty(Bin::Trash, BUDGET).await.expect("again");
    assert_eq!((outcome.purged, outcome.remaining), (3, 0));
    assert_eq!(
        host.empty(Bin::Trash, BUDGET).await.expect("empty bin"),
        mailtide::purge::EmptyOutcome::default()
    );
}

#[tokio::test]
async fn a_purge_refusal_does_not_block_the_account() {
    for refusal in [
        error(403, "accessNotConfigured", "Gmail API has not been used"),
        error(400, "invalidArgument", "Invalid id value"),
    ] {
        let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
        host.gmail().refuse_deletes = Some(refusal);
        let outcome = host.purge(&["a"]).await.expect("purge");
        assert_eq!(refusals(&outcome.refused), [("a", "gmail_refused")]);
        assert!(host.mirrored("a"), "a refusal tombstones nothing");
        let mirror = host.mirror();
        assert!(matches!(mirror.record.sync, SyncState::Live), "not blocked");
        host.gmail().add("n", &["INBOX"]);
        let run = host.sync().await;
        assert!(
            matches!(run.stop, StopReason::Idle { .. }),
            "{:?}",
            run.stop
        );
        assert!(host.mirror().record.cursor > Some(HistoryId(1)));
    }
}

#[tokio::test]
async fn a_thread_expanding_past_the_cap_sends_nothing() {
    let host = Host::new(PurgePolicy::TrashAndSpam);
    for id in many("m", PURGE_MAX_IDS + 1) {
        host.gmail().add_in("big", &id, &["TRASH"]);
    }
    host.mirror_gmail().await;
    let outcome = host
        .purge_target(&PurgeTarget::Thread(ThreadId("big".into())), false)
        .await;
    assert_eq!(
        outcome,
        Err(PurgeError::TooMany {
            limit: PURGE_MAX_IDS,
            given: PURGE_MAX_IDS + 1,
        }),
    );
    assert!(host.gmail().requests.is_empty());
}

#[tokio::test]
async fn a_restore_during_the_previous_single_delete_is_left_in_gmail() {
    for empty in [false, true] {
        let host = mirrored(
            PurgePolicy::TrashAndSpam,
            &[("a", &["TRASH"]), ("b", &["TRASH"])],
        )
        .await;
        host.gmail().after_delete = Some(Box::new(|gmail| gmail.restore("b")));
        if empty {
            let outcome = host.empty(Bin::Trash, BUDGET).await.expect("empty");
            assert_eq!((outcome.purged, outcome.skipped), (1, 1));
        } else {
            let outcome = host.purge(&["a", "b"]).await.expect("purge");
            assert_eq!(names(&outcome.purged), ["a"]);
            assert_eq!(refusals(&outcome.refused), [("b", "not_in_trash_or_spam")]);
        }
        assert_eq!(host.gmail().deletes(), ["DELETE a"]);
        assert!(host.in_gmail("b"));
    }
}

#[tokio::test]
async fn a_restore_after_confirmation_is_seen_before_the_next_delete() {
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &[("a", &["TRASH"]), ("b", &["TRASH"])],
    )
    .await;
    host.gmail().after_delete = Some(Box::new(|gmail| {
        gmail.after_history = Some(Box::new(|gmail| gmail.restore("b")));
    }));
    let outcome = host.purge(&["a", "b"]).await.expect("purge");
    assert_eq!(names(&outcome.purged), ["a"]);
    assert_eq!(refusals(&outcome.refused), [("b", "not_in_trash_or_spam")]);
    assert_eq!(host.gmail().deletes(), ["DELETE a"]);
    assert!(host.in_gmail("b"));
}

#[tokio::test]
async fn empty_checks_again_after_a_page_without_a_delete() {
    let host = Host::new(PurgePolicy::TrashAndSpam);
    for draft in many("draft", 1_000) {
        host.gmail().add(&draft, &["DRAFT", "TRASH"]);
    }
    host.gmail().add("z", &["TRASH"]);
    host.mirror_gmail().await;
    host.gmail().after_history = Some(Box::new(|gmail| gmail.restore("z")));
    let outcome = host.empty(Bin::Trash, BUDGET).await.expect("empty");
    assert_eq!((outcome.purged, outcome.skipped), (0, 1_001));
    assert!(host.in_gmail("z"));
    assert!(host.gmail().deletes().is_empty());
}

#[tokio::test]
async fn a_delete_rechecks_after_admission_outlives_eligibility() {
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
    let pause = host.pause_next_delete_admission();
    let change = async {
        pause.entered.notified().await;
        tokio::time::sleep(Duration::from_millis(2_050)).await;
        host.gmail().restore("a");
        pause.resume.notify_one();
    };
    let (outcome, ()) =
        tokio::time::timeout(BUDGET, async { tokio::join!(host.purge(&["a"]), change) })
            .await
            .expect("test completed");
    let outcome = outcome.expect("purge");
    assert_eq!(refusals(&outcome.refused), [("a", "not_in_trash_or_spam")]);
    assert!(host.gmail().deletes().is_empty() && host.in_gmail("a"));
}

#[tokio::test]
async fn a_delete_rechecks_after_credentials_outlive_eligibility() {
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
    let pause = host.pause_token_after(1);
    let change = async {
        pause.entered.notified().await;
        tokio::time::sleep(Duration::from_millis(2_050)).await;
        host.gmail().restore("a");
        pause.resume.notify_one();
    };
    let (outcome, ()) =
        tokio::time::timeout(BUDGET, async { tokio::join!(host.purge(&["a"]), change) })
            .await
            .expect("test completed");
    let outcome = outcome.expect("purge");
    assert_eq!(refusals(&outcome.refused), [("a", "not_in_trash_or_spam")]);
    assert!(host.gmail().deletes().is_empty() && host.in_gmail("a"));
}

#[tokio::test]
async fn an_expired_budget_sends_no_delete_even_after_admission() {
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
    let target = PurgeTarget::Messages(ids(&["a"]));
    let zero = host
        .runner
        .purge(&account(), &target, false, Duration::ZERO)
        .await
        .expect("zero budget");
    assert_eq!(names(&zero.unsent), ["a"]);
    assert!(host.gmail().requests.is_empty());
    let pause = host.pause_next_delete_admission();
    let budget = Duration::from_millis(50);
    let let_budget_expire = async {
        pause.entered.notified().await;
        tokio::time::sleep(budget).await;
        pause.resume.notify_one();
    };
    let account = account();
    let (outcome, ()) = tokio::time::timeout(BUDGET, async {
        tokio::join!(
            host.runner.purge(&account, &target, false, budget),
            let_budget_expire
        )
    })
    .await
    .expect("test completed");
    assert_eq!(names(&outcome.expect("purge").unsent), ["a"]);
    assert!(host.gmail().deletes().is_empty() && host.in_gmail("a"));
}

#[tokio::test]
async fn release_failure_preserves_a_first_or_later_applied_delete() {
    for fails_after in [0, 1] {
        let host = mirrored(
            PurgePolicy::TrashAndSpam,
            &[("a", &["TRASH"]), ("b", &["TRASH"]), ("c", &["TRASH"])],
        )
        .await;
        host.fail_delete_release_after(fails_after);
        let outcome = host.purge(&["a", "b", "c"]).await.expect("outcome");
        let split = usize::try_from(fails_after + 1).expect("small count");
        assert_eq!(names(&outcome.purged), ["a", "b", "c"][..split]);
        assert_eq!(names(&outcome.unsent), ["a", "b", "c"][split..]);
        assert_eq!(outcome.interrupted.as_deref(), Some("quota_gate"));
        assert_eq!(host.gmail().deletes().len(), split);
    }
}

#[tokio::test]
async fn a_408_may_have_applied_and_a_401_did_not() {
    for (status, sent) in [(408, true), (401, false)] {
        let host = mirrored(
            PurgePolicy::TrashAndSpam,
            &[("a", &["TRASH"]), ("b", &["TRASH"])],
        )
        .await;
        host.gmail().refuse_deletes = Some(error(status, "", ""));
        let outcome = host.purge(&["a", "b"]).await.expect("outcome");
        if sent {
            assert_eq!(names(&outcome.sent), ["a"]);
            assert_eq!(names(&outcome.unsent), ["b"]);
        } else {
            assert!(outcome.sent.is_empty());
            assert_eq!(names(&outcome.unsent), ["a", "b"]);
        }
        assert_eq!(host.gmail().deletes(), ["DELETE a"]);
        assert!(outcome.retry_at.is_none());
    }
}

#[tokio::test]
async fn retry_at_matches_the_gate_without_retry_after() {
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
    host.gmail().refuse_deletes = Some(error(429, "rateLimitExceeded", ""));
    let outcome = host.purge(&["a"]).await.expect("outcome");
    let Err(CallError::Deferred(QuotaDeferred { until })) = host
        .runner
        .call_gmail(
            &account(),
            GmailRequest::GetProfile,
            Priority::Foreground,
            Instant::now(),
        )
        .await
    else {
        panic!("the gate is cooling down");
    };
    assert_eq!(outcome.retry_at, Some(until));
    assert_eq!(names(&outcome.unsent), ["a"]);
}

#[tokio::test]
async fn a_partly_applied_batch_with_late_evidence_stays_sent() {
    let five = ["a", "b", "c", "d", "e"];
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &five.map(|id| (id, &["TRASH"][..])),
    )
    .await;
    {
        let mut gmail = host.gmail();
        gmail.batch_prefix = Some(2);
        gmail.answer_after_applying = Some(error(400, "invalidArgument", "bad id"));
        gmail.late_evidence = true;
    }
    let outcome = host.empty(Bin::Trash, BUDGET).await.expect("empty");
    assert_eq!(
        (
            outcome.purged,
            outcome.sent,
            outcome.skipped,
            outcome.remaining
        ),
        (0, 5, 0, 0)
    );
    assert!(host.mirrored("a") && !host.in_gmail("a") && host.in_gmail("c"));
}

#[tokio::test]
async fn forgetting_an_account_before_recording_reports_unrecorded_evidence() {
    let five = ["a", "b", "c", "d", "e"];
    let host = mirrored(
        PurgePolicy::TrashAndSpam,
        &five.map(|id| (id, &["TRASH"][..])),
    )
    .await;
    host.gmail().answer_after_applying = Some(error(400, "invalidArgument", "ambiguous"));
    host.forget_after_delete();
    let outcome = host.empty(Bin::Trash, BUDGET).await.expect("empty");
    assert_eq!(
        (
            outcome.purged,
            outcome.sent,
            outcome.skipped,
            outcome.remaining
        ),
        (0, 5, 0, 0)
    );
    assert_eq!(outcome.interrupted.as_deref(), Some("not_tracked"));
    assert!(
        host.runner
            .store()
            .status(&account())
            .await
            .expect("status")
            .is_none()
    );
}
