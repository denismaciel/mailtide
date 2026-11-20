//! Purge and empty against a fake Gmail that deletes for good: the policy,
//! the history check, lost answers, scopes and emptying a bin.
//! Plan: `features/purge.toml`.

pub mod purge_host;

use std::time::Duration;

use mailtide::ids::{HistoryId, MessageId};
use mailtide::outbox::{Action, ActionRequest, StarState};
use mailtide::purge::{Bin, PURGE_MAX_IDS, PurgePolicy, PurgeTarget, Refusal};
use mailtide::reconcile::{ReconcileProgress, ReconcileReason};
use mailtide::state::SyncState;
use mailtide_io::purge::{PurgeError, Stale};
use mailtide_io::runner::{Access, StopReason};
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
    let host = Host::new(PurgePolicy::default());
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
    assert_eq!(host.purge(&["t"]).await, Err(PurgeError::ScopeRequired));
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
    assert_eq!(
        host.empty(Bin::Trash, BUDGET).await,
        Err(PurgeError::ScopeRequired)
    );
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
        // b's record is late; a's and c's are there at once.
        gmail.after_delete = Some(Box::new(|gmail| {
            gmail.late_evidence = gmail.has("b");
        }));
    }
    let outcome = host.purge(&["a", "b", "c"]).await.expect("purge");
    // a: applied, answer lost, record read: purged. b: same, no record
    // yet: sent. c: same as a.
    assert_eq!(names(&outcome.purged), ["a", "c"]);
    assert_eq!(names(&outcome.sent), ["b"]);
    assert_eq!(
        host.gmail().deletes(),
        ["DELETE a", "DELETE b", "DELETE c"],
        "each delete is sent exactly once"
    );
    assert!(host.mirrored("b"), "no tombstone without evidence");
    // The evidence arrives; the next sync records it. Still no new delete.
    host.gmail().release_evidence();
    host.sync().await;
    assert!(!host.mirrored("b"));
    assert_eq!(host.gmail().deletes().len(), 3);
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
    assert_eq!(names(&outcome.sent), ["a", "b"]);
    assert!(!host.in_gmail("a") && !host.in_gmail("b"));
    assert!(host.mirrored("a") && host.mirrored("b"));
    host.gmail().history_fails_after_deletes = false;
    host.sync().await;
    assert!(!host.mirrored("a") && !host.mirrored("b"));
    assert_eq!(host.deleted_in_feed(), ["a", "b"]);
    assert_eq!(host.gmail().deletes().len(), 2, "nothing is resent");
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
    assert_eq!(host.gmail().deletes(), ["DELETE a"]);
    assert!(host.in_gmail("b") && host.in_gmail("c"));
    // Deferred before the first delete: nothing is sent at all.
    let host = mirrored(PurgePolicy::TrashAndSpam, &[("a", &["TRASH"])]).await;
    host.admit_deletes(0);
    let outcome = host.purge(&["a"]).await.expect("purge");
    assert_eq!(names(&outcome.unsent), ["a"]);
    assert!(host.gmail().deletes().is_empty());
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
    // b's delivery is under way when b is purged, and b is not recorded
    // (Gmail's record is late): the delivery gets Gmail's 404 and changes
    // nothing; history tombstones b.
    host.gmail().late_evidence = true;
    let outcome = host.purge(&["b"]).await.expect("purge");
    assert_eq!(names(&outcome.sent), ["b"]);
    host.gmail().release_evidence();
    host.sync().await;
    let modifies: Vec<String> = host
        .gmail()
        .requests
        .iter()
        .filter(|r| r.ends_with("/modify"))
        .cloned()
        .collect();
    assert!(
        modifies.iter().all(|r| !r.contains("/a/")),
        "a's delivery was cancelled: {modifies:?}"
    );
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
