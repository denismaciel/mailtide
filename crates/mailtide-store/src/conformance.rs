//! Behaviour every store must show, as a reusable suite.
//!
//! Covers atomic commits, epoch/version/cursor fencing, conditional message
//! operations (checkpoints, tombstones, unknown ids), job claims by attempt,
//! and the change log. Each case runs against a fresh store from the factory.

use std::future::Future;

use mailtide::commit::{
    Commit, CommitOutcome, CursorMove, DeletionEvidence, JobPlan, LabelOp, MessageOp, Rejection,
};
use mailtide::gmail::{HeaderFacts, Label, LabelKind, MessageObservation};
use mailtide::ids::{
    AccountId, BlobRef, CommitId, ENGINE_VERSION, EngineVersion, HistoryId, LabelId, MessageId,
    ThreadId, Timestamp,
};
use mailtide::jobs::{AttemptId, FOREGROUND_IMPORT_CAP, JobKind, JobOp, JobOutcome, Prioritised};
use mailtide::labels::{LabelSet, set};
use mailtide::mirror::MessageView;
use mailtide::outbox::{Action, ActionRequest, IntentOp, ReadState};
use mailtide::quota::Priority;
use mailtide::revision::{Change, ChangeEntry, FeedCursor, StateSeq};
use mailtide::state::StoreSnapshot;
use mailtide::thread::ThreadView;

use crate::{Store, StoreError};

/// The outcome of one conformance case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseResult {
    /// The case.
    pub name: &'static str,
    /// `None` if it passed, else why it failed.
    pub failure: Option<String>,
}

type CaseFn<S> =
    for<'a> fn(&'a S) -> std::pin::Pin<Box<dyn Future<Output = Result<(), String>> + 'a>>;

/// Run every case against fresh stores from `make`.
pub async fn run<S, F, Fut>(make: F) -> Vec<CaseResult>
where
    S: Store,
    F: Fn() -> Fut,
    Fut: Future<Output = Result<S, StoreError>>,
{
    let mut cases: Vec<(&'static str, CaseFn<S>)> = vec![
        ("claims bump the epoch and fence older writers", |s| {
            Box::pin(epoch_fencing(s))
        }),
        ("older engine versions can't claim", |s| {
            Box::pin(version_fencing(s))
        }),
        (
            "cursor moves only from the expected value, atomically",
            |s| Box::pin(cursor_fencing(s)),
        ),
        ("older observations never overwrite newer ones", |s| {
            Box::pin(checkpoints(s))
        }),
        ("tombstones are final", |s| Box::pin(tombstones(s))),
        ("unknown label patches enqueue verification", |s| {
            Box::pin(unknown_patch(s))
        }),
        ("job results apply only while the claim holds", |s| {
            Box::pin(claims(s))
        }),
        ("the change log pages in order", |s| Box::pin(change_log(s))),
        ("claims from an older epoch are refused", |s| {
            Box::pin(stale_claims(s))
        }),
        (
            "actions overlay, deliver, and settle on confirmation",
            |s| Box::pin(actions(s)),
        ),
        ("thread actions reach every non-draft message", |s| {
            Box::pin(thread_actions(s))
        }),
        ("unverified labels wait for a fresh full observation", |s| {
            Box::pin(unverified(s))
        }),
        ("the feed records what each commit changed", |s| {
            Box::pin(feed_records_changes(s))
        }),
        (
            "compaction makes older states too old; ids enumerate the mirror",
            |s| Box::pin(compaction(s)),
        ),
        ("admitted actions survive a newer engine's claim", |s| {
            Box::pin(admission_is_unfenced(s))
        }),
        ("status reads without claiming", |s| {
            Box::pin(status_does_not_claim(s))
        }),
        ("resume restores a blocked account", |s| {
            Box::pin(resume_restores(s))
        }),
        ("forget removes an account and fences its engine", |s| {
            Box::pin(forget_fences(s))
        }),
        (
            "blocking wraps the stored state, not the engine's view",
            |s| Box::pin(block_wraps_stored_state(s)),
        ),
        ("claims follow priority, then due time, then kind", |s| {
            Box::pin(claim_order(s))
        }),
        ("claims report when the next ready job is due", |s| {
            Box::pin(claims_report_next_due(s))
        }),
        ("only tracked accounts are mirrored", |s| {
            Box::pin(tracking(s))
        }),
        ("claims skip classes below the floor", |s| {
            Box::pin(claims_respect_the_floor(s))
        }),
        ("label definitions read back as last synced", |s| {
            Box::pin(label_definitions(s))
        }),
        (
            "label refreshes persist their time; unchanged ones log nothing",
            |s| Box::pin(label_refreshes(s)),
        ),
        ("imported entries carry the stored envelope", |s| {
            Box::pin(imports_carry_envelopes(s))
        }),
    ];
    cases.extend(prioritise_cases());
    cases.extend(feed_page_cases());
    cases.extend(thread_cases());
    let mut results = Vec::new();
    for (name, case) in cases {
        let failure = match make().await {
            Ok(store) => case(&store).await.err(),
            Err(error) => Some(format!("store setup failed: {error:?}")),
        };
        results.push(CaseResult { name, failure });
    }
    results
}

/// The cases of `Store::prioritise`.
fn prioritise_cases<S: Store>() -> Vec<(&'static str, CaseFn<S>)> {
    vec![
        ("prioritise raises queued imports to Foreground", |s| {
            Box::pin(prioritise_raises_queued_imports(s))
        }),
        ("prioritise needs no epoch", |s| {
            Box::pin(prioritise_is_unfenced(s))
        }),
        ("prioritise reports the ids it could not raise", |s| {
            Box::pin(prioritise_reports_what_it_could_not_raise(s))
        }),
        ("prioritise keeps a failed import's backoff", |s| {
            Box::pin(prioritise_keeps_a_failed_imports_backoff(s))
        }),
        ("a running claim finishes after prioritise", |s| {
            Box::pin(prioritise_survives_a_running_claim(s))
        }),
        ("prioritise stops at the Foreground import cap", |s| {
            Box::pin(prioritise_respects_the_foreground_cap(s))
        }),
        ("prioritise writes nothing to the feed", |s| {
            Box::pin(prioritise_leaves_the_feed_alone(s))
        }),
    ]
}

/// The cases of feed pages bounded by their limit.
fn feed_page_cases<S: Store>() -> Vec<(&'static str, CaseFn<S>)> {
    vec![
        ("change pages split inside a commit at the limit", |s| {
            Box::pin(pages_split_inside_a_commit(s))
        }),
        ("commit positions read as before", |s| {
            Box::pin(commit_positions_read_as_before(s))
        }),
        ("compaction keeps the commit a reader is inside", |s| {
            Box::pin(compaction_keeps_a_commit_a_reader_is_inside(s))
        }),
        ("a split read survives writes between pages", |s| {
            Box::pin(a_split_read_survives_writes_between_pages(s))
        }),
        ("a large commit arrives in bounded, hydrated pages", |s| {
            Box::pin(a_large_commit_arrives_in_bounded_pages(s))
        }),
        ("a page hydrates only its own imports", |s| {
            Box::pin(a_page_hydrates_only_its_own_imports(s))
        }),
    ]
}

fn account() -> AccountId {
    AccountId("conformance@example.com".into())
}

fn plan() -> JobPlan {
    JobPlan {
        priority: Priority::Live,
        import_due_at: Timestamp(0),
        draft_import_due_at: Timestamp(0),
        verify_due_at: Timestamp(0),
        cleanup_due_at: Timestamp(0),
        deliver_due_at: Timestamp(0),
        confirm_due_at: Timestamp(0),
    }
}

fn commit(snapshot: &StoreSnapshot, seq: u64) -> Commit {
    Commit {
        id: CommitId {
            epoch: snapshot.epoch,
            seq,
        },
        account: snapshot.account.clone(),
        epoch: snapshot.epoch,
        engine_version: snapshot.engine_version,
        cursor: None,
        claim: None,
        sync: None,
        messages: Vec::new(),
        labels: Vec::new(),
        account_ops: Vec::new(),
        job_plan: plan(),
        jobs: Vec::new(),
        intents: Vec::new(),
    }
}

/// The import of `id` with header facts derived from it.
fn import(id: &str, labels: &[&str], checkpoint: u64) -> MessageOp {
    import_in("t", id, labels, checkpoint, 1_700_000_000_000)
}

/// The import of `id` into `thread`, dated `date`.
fn import_in(thread: &str, id: &str, labels: &[&str], checkpoint: u64, date: i64) -> MessageOp {
    MessageOp::MarkImported {
        observation: MessageObservation {
            id: MessageId(id.into()),
            thread_id: ThreadId(thread.into()),
            labels: set(labels.iter().copied()),
            checkpoint: HistoryId(checkpoint),
        },
        blob: BlobRef(format!("{id}.eml")),
        size: 100,
        internal_date: Timestamp(date),
        headers: Box::new(HeaderFacts {
            rfc822_message_id: Some(format!("<{id}@example.com>")),
            from: Some(format!("{id}@example.com")),
            subject: Some(format!("Subject {id}")),
            ..HeaderFacts::default()
        }),
    }
}

fn observe(id: &str, labels: &[&str], checkpoint: u64) -> MessageOp {
    observe_in("t", id, labels, checkpoint)
}

fn observe_in(thread: &str, id: &str, labels: &[&str], checkpoint: u64) -> MessageOp {
    MessageOp::Observe(MessageObservation {
        id: MessageId(id.into()),
        thread_id: ThreadId(thread.into()),
        labels: set(labels.iter().copied()),
        checkpoint: HistoryId(checkpoint),
    })
}

/// The generation conformance accounts are tracked as.
const GENERATION: u64 = 1;

/// Track the account (idempotent), then claim it.
async fn claim<S: Store>(store: &S) -> Result<StoreSnapshot, String> {
    store
        .track(&account(), GENERATION)
        .await
        .map_err(|e| format!("{e:?}"))?;
    store
        .claim(&account(), ENGINE_VERSION)
        .await
        .map_err(|e| format!("{e:?}"))
}

async fn apply<S: Store>(store: &S, commit: &Commit) -> Result<CommitOutcome, String> {
    store.apply(commit).await.map_err(|e| format!("{e:?}"))
}

fn expect(condition: bool, message: &str) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(message.to_owned())
    }
}

async fn epoch_fencing<S: Store>(store: &S) -> Result<(), String> {
    let first = claim(store).await?;
    let second = claim(store).await?;
    expect(second.epoch > first.epoch, "a claim must bump the epoch")?;
    let outcome = apply(store, &commit(&first, 0)).await?;
    expect(
        outcome == CommitOutcome::Rejected(Rejection::StaleEpoch),
        "an older epoch must be rejected",
    )?;
    let outcome = apply(store, &commit(&second, 0)).await?;
    expect(
        matches!(outcome, CommitOutcome::Applied { .. }),
        "the current epoch must apply",
    )
}

async fn version_fencing<S: Store>(store: &S) -> Result<(), String> {
    claim(store).await?;
    let older = EngineVersion(ENGINE_VERSION.0.saturating_sub(1));
    if older == ENGINE_VERSION {
        return Ok(());
    }
    let result = store.claim(&account(), older).await;
    expect(
        result == Err(StoreError::Rejected(Rejection::StaleVersion)),
        "an older engine version must not claim",
    )
}

async fn cursor_fencing<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut wrong = commit(&snapshot, 0);
    wrong.cursor = Some(CursorMove {
        expected: Some(HistoryId(1)),
        to: HistoryId(2),
    });
    wrong.messages = vec![observe("a", &["INBOX"], 1)];
    let outcome = apply(store, &wrong).await?;
    expect(
        outcome == CommitOutcome::Rejected(Rejection::CursorMismatch),
        "a wrong expected cursor must be rejected",
    )?;
    // A cursor alone would contradict the bootstrapping lifecycle.
    let mut contradictory = commit(&snapshot, 1);
    contradictory.cursor = Some(CursorMove {
        expected: snapshot.cursor,
        to: HistoryId(2),
    });
    expect(
        apply(store, &contradictory).await?
            == CommitOutcome::Rejected(Rejection::InvalidTransition),
        "a cursor without leaving bootstrapping must be refused",
    )?;
    // The first cursor arrives with the move to Live, as reconciliation
    // finishes.
    let mut right = commit(&snapshot, 2);
    right.cursor = Some(CursorMove {
        expected: snapshot.cursor,
        to: HistoryId(2),
    });
    right.sync = Some(mailtide::state::SyncState::Live);
    let outcome = apply(store, &right).await?;
    expect(
        matches!(outcome, CommitOutcome::Applied { .. }),
        "the expected cursor must apply",
    )?;
    let page = store
        .changes(&account(), StateSeq(0).into(), 100)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(
        !page
            .changes
            .iter()
            .any(|entry| entry.change.message() == Some(&MessageId("a".into()))),
        "a rejected commit must not change anything",
    )?;
    let after = claim(store).await?;
    expect(
        after.cursor == Some(HistoryId(2)),
        "the cursor must be stored",
    )
}

async fn checkpoints<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![observe("a", &["INBOX"], 20), observe("a", &["SPAM"], 10)];
    let outcome = apply(store, &c).await?;
    expect(
        matches!(outcome, CommitOutcome::Applied { skipped: 1, .. }),
        "the older observation must be skipped",
    )
}

async fn tombstones<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![
        MessageOp::Tombstone {
            id: MessageId("a".into()),
            evidence: DeletionEvidence::History { at: HistoryId(30) },
        },
        observe("a", &["INBOX"], 40),
    ];
    let outcome = apply(store, &c).await?;
    expect(
        matches!(outcome, CommitOutcome::Applied { skipped: 1, .. }),
        "an observation of a tombstoned message must be skipped",
    )?;
    let jobs = store
        .claim_jobs(
            &account(),
            snapshot.epoch,
            10,
            Timestamp(0),
            AttemptId(1),
            Priority::Backfill,
        )
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(claimed)?;
    expect(
        !jobs
            .iter()
            .any(|job| job.kind() == JobKind::Import(MessageId("a".into()))),
        "a tombstoned message must not be imported",
    )
}

async fn unknown_patch<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![MessageOp::PatchLabels {
        id: MessageId("x".into()),
        add: set(["STARRED"]),
        remove: LabelSet::new(),
        checkpoint: HistoryId(5),
    }];
    apply(store, &c).await?;
    let jobs = store
        .claim_jobs(
            &account(),
            snapshot.epoch,
            10,
            Timestamp(0),
            AttemptId(1),
            Priority::Backfill,
        )
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(claimed)?;
    expect(
        jobs.iter()
            .any(|job| job.kind() == JobKind::Verify(MessageId("x".into()))),
        "an unknown message must get a verification job",
    )
}

async fn claims<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![observe("a", &["INBOX"], 1)];
    apply(store, &c).await?;
    let jobs = store
        .claim_jobs(
            &account(),
            snapshot.epoch,
            10,
            Timestamp(0),
            AttemptId(7),
            Priority::Backfill,
        )
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(claimed)?;
    let job = jobs.first().ok_or("the import must be claimable")?;
    let again = store
        .claim_jobs(
            &account(),
            snapshot.epoch,
            10,
            Timestamp(0),
            AttemptId(8),
            Priority::Backfill,
        )
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(claimed)?;
    expect(again.is_empty(), "a running job must not be claimed twice")?;
    let mut stale = commit(&snapshot, 1);
    stale.claim = Some(mailtide::jobs::JobClaim {
        kind: job.kind().clone(),
        attempt: AttemptId(99),
    });
    stale.jobs = vec![JobOp::Finish {
        claim: stale.claim.clone().ok_or("claim")?,
        outcome: JobOutcome::Done,
    }];
    let outcome = apply(store, &stale).await?;
    expect(
        outcome == CommitOutcome::Rejected(Rejection::StaleClaim),
        "a stale claim must be rejected",
    )?;
    let mut done = commit(&snapshot, 2);
    done.claim = Some(job.claim());
    done.jobs = vec![JobOp::Finish {
        claim: job.claim(),
        outcome: JobOutcome::Done,
    }];
    let outcome = apply(store, &done).await?;
    expect(
        matches!(outcome, CommitOutcome::Applied { .. }),
        "the current claim must apply",
    )
}

async fn change_log<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    for (seq, id) in ["a", "b", "c"].iter().enumerate() {
        let mut c = commit(&snapshot, seq as u64);
        c.messages = vec![observe(id, &["INBOX"], 1)];
        apply(store, &c).await?;
    }
    let first = store
        .changes(&account(), StateSeq(0).into(), 2)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(
        first.changes.len() == 2 && first.has_more,
        "the first page must hold two changes and more",
    )?;
    let rest = store
        .changes(&account(), first.next, 10)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(
        rest.changes
            .iter()
            .filter_map(|e| e.change.message())
            .collect::<Vec<_>>()
            == vec![&MessageId("c".into())]
            && !rest.has_more,
        "the second page must hold the rest",
    )
}

async fn stale_claims<S: Store>(store: &S) -> Result<(), String> {
    let old = claim(store).await?;
    let mut c = commit(&old, 0);
    c.messages = vec![observe("a", &["INBOX"], 1)];
    apply(store, &c).await?;
    let new = claim(store).await?;
    let stale = store
        .claim_jobs(
            &account(),
            old.epoch,
            10,
            Timestamp(0),
            AttemptId(1),
            Priority::Backfill,
        )
        .await;
    expect(
        stale
            == Ok(mailtide::jobs::ClaimOutcome::Rejected(
                Rejection::StaleEpoch,
            )),
        "a claim from an older epoch must be refused",
    )?;
    let current = store
        .claim_jobs(
            &account(),
            new.epoch,
            10,
            Timestamp(0),
            AttemptId(2),
            Priority::Backfill,
        )
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(claimed)?;
    expect(
        current.len() == 1,
        "the current epoch must still claim the job",
    )
}

async fn view<S: Store>(store: &S, id: &str) -> Result<MessageView, String> {
    store
        .messages(&account(), &[MessageId(id.into())])
        .await
        .map_err(|e| format!("{e:?}"))?
        .pop()
        .ok_or_else(|| format!("{id} must be readable"))
}

async fn claim_all<S: Store>(
    store: &S,
    snapshot: &StoreSnapshot,
    attempt: u64,
) -> Result<Vec<mailtide::jobs::ClaimedJob>, String> {
    store
        .claim_jobs(
            &account(),
            snapshot.epoch,
            100,
            Timestamp(0),
            AttemptId(attempt),
            Priority::Backfill,
        )
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(claimed)
}

async fn actions<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![observe("a", &["INBOX", "UNREAD"], 10)];
    apply(store, &c).await?;
    let mut act = commit(&snapshot, 1);
    act.intents = vec![IntentOp::Request(ActionRequest::Messages {
        ids: vec![MessageId("a".into())],
        action: Action::SetRead(ReadState::Read),
    })];
    apply(store, &act).await?;
    let seen = view(store, "a").await?;
    expect(
        seen.labels == set(["INBOX"]),
        "the action must overlay at once",
    )?;
    expect(
        seen.observed_labels == set(["INBOX", "UNREAD"]),
        "observed labels stay Gmail's",
    )?;
    expect(seen.pending_actions == 1, "one pending action")?;
    let jobs = claim_all(store, &snapshot, 1).await?;
    let deliver = jobs
        .iter()
        .find(|job| job.kind() == JobKind::Deliver(MessageId("a".into())))
        .ok_or("the action must be deliverable")?;
    let mailtide::jobs::ClaimedWork::Deliver(_, mailtide::jobs::Delivery::Send(group)) =
        &deliver.work
    else {
        return Err("delivery carries the change".into());
    };
    expect(
        group.remove == vec![mailtide::ids::LabelId("UNREAD".into())] && group.add.is_empty(),
        "delivery removes UNREAD",
    )?;
    let mut delivered = commit(&snapshot, 2);
    delivered.claim = Some(deliver.claim());
    delivered.jobs = vec![JobOp::Finish {
        claim: deliver.claim(),
        outcome: JobOutcome::Done,
    }];
    delivered.intents = vec![IntentOp::Delivered {
        versions: group.versions.clone(),
        at: Timestamp(5),
        by: deliver.attempt,
    }];
    apply(store, &delivered).await?;
    expect(
        view(store, "a").await?.labels == set(["INBOX"]),
        "a delivered action keeps overlaying until confirmed",
    )?;
    // History alone can't settle it: a record may predate the delivery.
    let mut history = commit(&snapshot, 3);
    history.messages = vec![MessageOp::PatchLabels {
        id: MessageId("a".into()),
        add: LabelSet::new(),
        remove: set(["UNREAD"]),
        checkpoint: HistoryId(11),
    }];
    apply(store, &history).await?;
    expect(
        view(store, "a").await?.pending_actions == 1,
        "history doesn't settle a delivered action",
    )?;
    // A confirmation fetch claimed after the delivery does.
    let jobs = claim_all(store, &snapshot, 2).await?;
    let verify = jobs
        .iter()
        .find(|job| job.kind() == JobKind::Verify(MessageId("a".into())))
        .ok_or("delivery must queue a confirmation fetch")?;
    let mut confirmed = commit(&snapshot, 4);
    confirmed.claim = Some(verify.claim());
    confirmed.messages = vec![observe("a", &["INBOX"], 11)];
    confirmed.jobs = vec![JobOp::Finish {
        claim: verify.claim(),
        outcome: JobOutcome::Done,
    }];
    apply(store, &confirmed).await?;
    let seen = view(store, "a").await?;
    expect(
        seen.pending_actions == 0,
        "the confirmation fetch settles the action",
    )?;
    expect(seen.labels == set(["INBOX"]), "labels match Gmail")
}

async fn thread_actions<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![
        observe_in("t1", "a", &["INBOX"], 10),
        observe_in("t1", "b", &["INBOX", "UNREAD"], 11),
        observe_in("t1", "d", &["DRAFT"], 12),
        observe_in("t2", "c", &["INBOX"], 13),
    ];
    apply(store, &c).await?;
    let mut act = commit(&snapshot, 1);
    act.intents = vec![IntentOp::Request(ActionRequest::Thread {
        id: ThreadId("t1".into()),
        action: Action::Archive,
    })];
    apply(store, &act).await?;
    for id in ["a", "b"] {
        expect(
            !view(store, id)
                .await?
                .labels
                .contains(&mailtide::ids::LabelId("INBOX".into())),
            "thread members are archived",
        )?;
    }
    expect(
        view(store, "d").await?.pending_actions == 0,
        "drafts are not targets",
    )?;
    expect(
        view(store, "c").await?.pending_actions == 0,
        "other threads are untouched",
    )
}

/// The thread view cases.
fn thread_cases<S: Store>() -> Vec<(&'static str, CaseFn<S>)> {
    vec![
        (
            "thread views fold their members as messages() shows them",
            |s| Box::pin(thread_views(s)),
        ),
        ("thread views span statement chunks whole", |s| {
            Box::pin(thread_views_span_statement_chunks(s))
        }),
        ("thread views never mix accounts", |s| {
            Box::pin(thread_views_never_mix_accounts(s))
        }),
    ]
}

async fn threads<S: Store>(
    store: &S,
    account: &AccountId,
    ids: &[&str],
) -> Result<Vec<ThreadView>, String> {
    let ids: Vec<ThreadId> = ids.iter().map(|id| ThreadId((*id).into())).collect();
    store
        .threads(account, &ids)
        .await
        .map_err(|e| format!("{e:?}"))
}

fn members(thread: &ThreadView) -> Vec<&str> {
    thread.messages.iter().map(|m| m.id.0.as_str()).collect()
}

async fn thread_views<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![
        import_in("t1", "a", &["INBOX"], 10, 20),
        import_in("t1", "b", &["INBOX", "UNREAD"], 11, 10),
        observe_in("t1", "c", &["INBOX"], 12),
        observe_in("t1", "d", &["DRAFT"], 13),
        import_in("t2", "e", &["INBOX"], 14, 30),
        observe_in("t3", "f", &["INBOX"], 15),
    ];
    apply(store, &c).await?;
    expect(
        threads(store, &account(), &[]).await?.is_empty(),
        "no ids read nothing",
    )?;
    let read = threads(store, &account(), &["t2", "unknown", "", "t1"]).await?;
    let ids: Vec<&str> = read.iter().map(|t| t.id.0.as_str()).collect();
    expect(
        ids == ["t2", "t1"],
        &format!("answers come in the order asked, unknown and empty ids left out: {ids:?}"),
    )?;
    let t1 = &read[1];
    expect(
        members(t1) == ["b", "a", "c", "d"],
        &format!(
            "members by date, then those awaiting import: {:?}",
            members(t1)
        ),
    )?;
    expect(t1.labels == set(["INBOX", "UNREAD"]) && t1.unread, "labels")?;
    expect(t1.latest == Some(MessageId("a".into())), "latest")?;
    expect(
        (t1.message_count, t1.draft_count, t1.awaiting_import) == (3, 1, 1),
        "counts",
    )?;
    expect(
        t1.participants == ["b@example.com", "a@example.com"],
        "participants",
    )?;
    let ids: Vec<MessageId> = t1.messages.iter().map(|m| m.id.clone()).collect();
    let views = store
        .messages(&account(), &ids)
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(views == t1.messages, "each member is its messages() view")?;

    // A pending action shows at once.
    store
        .admit(
            &account(),
            &ActionRequest::Thread {
                id: ThreadId("t1".into()),
                action: Action::SetRead(ReadState::Read),
            },
            Timestamp(0),
        )
        .await
        .map_err(|e| format!("{e:?}"))?;
    let t1 = threads(store, &account(), &["t1"])
        .await?
        .pop()
        .ok_or("t1")?;
    expect(!t1.unread && t1.labels == set(["INBOX"]), "the overlay")?;

    // A deleted member leaves; a thread whose members are all deleted goes.
    let mut d = commit(&snapshot, 1);
    d.messages = ["a", "e"]
        .iter()
        .map(|id| MessageOp::Tombstone {
            id: MessageId((*id).into()),
            evidence: DeletionEvidence::History { at: HistoryId(20) },
        })
        .collect();
    apply(store, &d).await?;
    let read = threads(store, &account(), &["t1", "t2", "t3"]).await?;
    let ids: Vec<&str> = read.iter().map(|t| t.id.0.as_str()).collect();
    expect(ids == ["t1", "t3"], "a thread with no members is gone")?;
    expect(
        members(&read[0]) == ["b", "c", "d"] && read[0].latest == Some(MessageId("b".into())),
        "the member before the deleted one is latest",
    )?;
    expect(
        threads(store, &AccountId("stranger@example.com".into()), &["t1"])
            .await?
            .is_empty(),
        "an untracked account reads as empty",
    )
}

async fn thread_views_span_statement_chunks<S: Store>(store: &S) -> Result<(), String> {
    // More thread ids than one statement takes, and one thread with more
    // rows (each joined with two intents) than a chunk of ids.
    const THREADS: u64 = 1_200;
    const BIG: u64 = 600;
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = (0..THREADS)
        .map(|n| {
            import_in(
                &format!("t{n:04}"),
                &format!("m{n:04}"),
                &["INBOX"],
                n + 1,
                1,
            )
        })
        .chain((0..BIG).map(|n| {
            import_in(
                "big",
                &format!("b{n:04}"),
                &["INBOX", "UNREAD"],
                THREADS + n + 1,
                i64::try_from(n).unwrap_or(i64::MAX),
            )
        }))
        .collect();
    apply(store, &c).await?;
    let admitted = store
        .admit(
            &account(),
            &ActionRequest::Thread {
                id: ThreadId("big".into()),
                action: Action::MarkSpam,
            },
            Timestamp(0),
        )
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(u64::from(admitted) == BIG, "every member of the big thread")?;
    let mut ids: Vec<String> = (0..THREADS).rev().map(|n| format!("t{n:04}")).collect();
    ids.insert(ids.len() / 2, "big".into());
    let asked: Vec<&str> = ids.iter().map(String::as_str).collect();
    let read = threads(store, &account(), &asked).await?;
    let answered: Vec<&str> = read.iter().map(|t| t.id.0.as_str()).collect();
    expect(answered == asked, "every thread once, in the order asked")?;
    expect(
        read.iter()
            .filter(|t| t.id.0 != "big")
            .all(|t| t.message_count == 1 && t.messages.len() == 1),
        "each small thread whole",
    )?;
    let big = read
        .iter()
        .find(|t| t.id.0 == "big")
        .ok_or("the big thread")?;
    expect(
        u64::from(big.message_count) == BIG
            && big
                .messages
                .iter()
                .all(|m| m.pending_actions == 2 && !m.labels.contains(&LabelId("INBOX".into()))),
        "the big thread whole, each member with both intents",
    )?;
    expect(
        big.labels == set(["SPAM", "UNREAD"]),
        "the big thread's labels with the overlay",
    )?;
    expect(
        big.messages.windows(2).all(|w| {
            w[0].content.as_ref().map(|c| c.internal_date)
                < w[1].content.as_ref().map(|c| c.internal_date)
        }),
        "the big thread in date order",
    )
}

async fn thread_views_never_mix_accounts<S: Store>(store: &S) -> Result<(), String> {
    let other = AccountId("delegate@example.com".into());
    let mine = claim(store).await?;
    store
        .track(&other, GENERATION)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let theirs = store
        .claim(&other, ENGINE_VERSION)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut c = commit(&mine, 0);
    c.messages = vec![import_in("t", "a", &["INBOX"], 1, 10)];
    apply(store, &c).await?;
    let mut c = commit(&theirs, 0);
    c.messages = vec![
        import_in("t", "a", &["STARRED"], 1, 10),
        import_in("t", "b", &["INBOX", "UNREAD"], 2, 20),
    ];
    apply(store, &c).await?;
    let read = threads(store, &account(), &["t"]).await?;
    expect(
        read.len() == 1 && members(&read[0]) == ["a"] && read[0].labels == set(["INBOX"]),
        "only the account's own rows",
    )?;
    let read = threads(store, &other, &["t"]).await?;
    expect(
        read.len() == 1
            && members(&read[0]) == ["a", "b"]
            && read[0].labels == set(["INBOX", "STARRED", "UNREAD"]),
        "the other account's own rows",
    )
}

async fn unverified<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![observe("a", &["INBOX", "STARRED"], 10)];
    apply(store, &c).await?;
    // History was lost: reconciliation finishes past the row's checkpoint.
    let mut finish = commit(&snapshot, 1);
    finish.messages = vec![MessageOp::FinishReconcile {
        baseline: HistoryId(20),
    }];
    apply(store, &finish).await?;
    expect(
        view(store, "a").await?.unverified,
        "older rows become unverified",
    )?;
    let mut patch = commit(&snapshot, 2);
    patch.messages = vec![MessageOp::PatchLabels {
        id: MessageId("a".into()),
        add: set(["IMPORTANT"]),
        remove: LabelSet::new(),
        checkpoint: HistoryId(25),
    }];
    apply(store, &patch).await?;
    let seen = view(store, "a").await?;
    expect(seen.unverified, "a delta can't verify an unverified row")?;
    expect(
        seen.checkpoint == Some(HistoryId(10)),
        "nor advance its checkpoint",
    )?;
    // A fetch from before the delta must not settle it ...
    let mut early = commit(&snapshot, 3);
    early.messages = vec![observe("a", &["INBOX"], 22)];
    apply(store, &early).await?;
    expect(
        view(store, "a").await?.unverified,
        "a fetch older than the delta is not enough",
    )?;
    // ... one from after it does.
    let mut late = commit(&snapshot, 4);
    late.messages = vec![observe("a", &["INBOX", "IMPORTANT"], 25)];
    apply(store, &late).await?;
    let seen = view(store, "a").await?;
    expect(!seen.unverified, "a fresh full observation settles the row")?;
    expect(
        seen.labels == set(["INBOX", "IMPORTANT"]),
        "with Gmail's labels",
    )
}

/// Read from `since` to the end with `limit`: every entry, and each page's
/// size.
async fn read_all<S: Store>(
    store: &S,
    since: FeedCursor,
    limit: u32,
) -> Result<(Vec<ChangeEntry>, Vec<usize>), String> {
    let (entries, sizes, _) = read_pages(store, since, limit).await?;
    Ok((entries, sizes))
}

/// Like [`read_all`], also returning each page's `next`.
async fn read_pages<S: Store>(
    store: &S,
    since: FeedCursor,
    limit: u32,
) -> Result<(Vec<ChangeEntry>, Vec<usize>, Vec<FeedCursor>), String> {
    let (mut entries, mut sizes, mut nexts, mut since) =
        (Vec::new(), Vec::new(), Vec::new(), since);
    loop {
        let page = store
            .changes(&account(), since, limit)
            .await
            .map_err(|e| format!("{e:?}"))
            .and_then(page)?;
        sizes.push(page.changes.len());
        nexts.push(page.next);
        entries.extend(page.changes);
        since = page.next;
        if !page.has_more {
            return Ok((entries, sizes, nexts));
        }
    }
}

/// The messages discovered by `entries`, in order.
fn discovered(entries: &[ChangeEntry]) -> Vec<&str> {
    entries
        .iter()
        .filter_map(|entry| match &entry.change {
            Change::MessageDiscovered { id, .. } => Some(id.0.as_str()),
            Change::MessageImported { .. }
            | Change::LabelsChanged { .. }
            | Change::OverlayChanged { .. }
            | Change::MessageDeleted { .. }
            | Change::LabelDefinitionsChanged => None,
        })
        .collect()
}

fn identities(entries: &[ChangeEntry]) -> Vec<(StateSeq, u32)> {
    entries.iter().map(|e| (e.state, e.index)).collect()
}

/// The account's feed position now (the last commit's state).
async fn state<S: Store>(store: &S) -> Result<StateSeq, String> {
    Ok(store
        .status(&account())
        .await
        .map_err(|e| format!("{e:?}"))?
        .ok_or("status")?
        .state)
}

/// Apply one commit discovering each of `ids`; its state.
async fn discover<S: Store>(
    store: &S,
    snapshot: &StoreSnapshot,
    seq: u64,
    ids: &[&str],
) -> Result<StateSeq, String> {
    let mut c = commit(snapshot, seq);
    c.messages = ids.iter().map(|id| observe(id, &["INBOX"], 1)).collect();
    apply(store, &c).await?;
    state(store).await
}

async fn pages_split_inside_a_commit<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    discover(store, &snapshot, 0, &["a", "b", "c"]).await?;
    discover(store, &snapshot, 1, &["d"]).await?;
    let (entries, sizes) = read_all(store, StateSeq(0).into(), 2).await?;
    expect(
        sizes == [2, 2],
        "two pages of two, the first ending inside a commit",
    )?;
    expect(
        discovered(&entries) == ["a", "b", "c", "d"],
        "every change arrives exactly once",
    )?;
    let (whole, _) = read_all(store, StateSeq(0).into(), 100).await?;
    expect(
        identities(&entries) == identities(&whole),
        "identities are those of an unbounded read",
    )
}

async fn commit_positions_read_as_before<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let first = discover(store, &snapshot, 0, &["a", "b", "c"]).await?;
    let second = discover(store, &snapshot, 1, &["d"]).await?;
    let third = discover(store, &snapshot, 2, &["e", "f"]).await?;
    // From each commit position, exactly what followed that commit.
    for (since, rest) in [
        (StateSeq(0), &["a", "b", "c", "d", "e", "f"][..]),
        (first, &["d", "e", "f"][..]),
        (second, &["e", "f"][..]),
        (third, &[][..]),
    ] {
        let page = store
            .changes(&account(), since.into(), 100)
            .await
            .map_err(|e| format!("{e:?}"))
            .and_then(page)?;
        expect(
            discovered(&page.changes) == rest && !page.has_more,
            &format!("from {since:?}: the commits after it"),
        )?;
        expect(
            page.next == FeedCursor::from(page.head),
            "a page that reads to the head ends on its commit",
        )?;
    }
    // No commit is larger than the limit: every page holds whole commits
    // and every position is a commit position, as before. Where the limit
    // falls on a commit boundary the pages are the ones a whole-commit reader
    // saw; elsewhere a page now ends at the last boundary before the limit
    // (the old rule went past it to finish the commit).
    for limit in [3, 4, 5, 6] {
        let (entries, sizes, nexts) = read_pages(store, StateSeq(0).into(), limit).await?;
        expect(
            discovered(&entries) == ["a", "b", "c", "d", "e", "f"]
                && nexts.iter().all(|next| next.entry.is_none()),
            &format!("limit {limit}: whole commits, commit positions"),
        )?;
        let expected: &[usize] = match limit {
            3 => &[3, 3],
            4 | 5 => &[4, 2],
            _ => &[6],
        };
        expect(
            sizes == expected,
            &format!("limit {limit}: pages {sizes:?}"),
        )?;
    }
    let page_one = store
        .changes(&account(), StateSeq(0).into(), 4)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(
        discovered(&page_one.changes) == ["a", "b", "c", "d"]
            && page_one.has_more
            && page_one.next == FeedCursor::from(second),
        "the first page ends on the second commit",
    )?;
    let page_two = store
        .changes(&account(), page_one.next, 4)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(
        discovered(&page_two.changes) == ["e", "f"]
            && !page_two.has_more
            && page_two.next == FeedCursor::from(third),
        "the second page is the third commit",
    )
}

async fn compaction_keeps_a_commit_a_reader_is_inside<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let first = discover(store, &snapshot, 0, &["a"]).await?;
    let second = discover(store, &snapshot, 1, &["b", "c", "d"]).await?;
    discover(store, &snapshot, 2, &["e"]).await?;
    // The second commit is larger than the limit: the reader is split
    // inside it.
    let reading = store
        .changes(&account(), first.into(), 2)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    let inside = reading.next;
    expect(
        inside
            == FeedCursor {
                seq: second,
                entry: Some(1),
            },
        "the reader is inside the second commit",
    )?;
    expect(
        inside.compactable() == first,
        "it holds the commit it is inside",
    )?;
    let removed = store
        .compact(&account(), inside.compactable())
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(removed == 1, "only the first commit is removed")?;
    let (rest, _) = read_all(store, inside, 100).await?;
    expect(
        discovered(&rest) == ["d", "e"],
        "the reader continues where it was",
    )?;
    store
        .compact(&account(), second)
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(
        store
            .changes(&account(), inside, 100)
            .await
            .map_err(|e| format!("{e:?}"))?
            == mailtide::revision::ChangeFeed::TooOld {
                generation: GENERATION,
                oldest: second,
            },
        "compacting the commit a reader is inside makes it too old",
    )
}

async fn a_split_read_survives_writes_between_pages<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let first = discover(store, &snapshot, 0, &["a", "b", "c", "d"]).await?;
    let one = store
        .changes(&account(), StateSeq(0).into(), 2)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(
        one.head == first && one.next.seq == first && one.has_more,
        "the first page ends inside the commit",
    )?;
    // Between the pages: a commit and an admitted action.
    discover(store, &snapshot, 1, &["e"]).await?;
    let admitted = store
        .admit(
            &account(),
            &ActionRequest::Messages {
                ids: vec![MessageId("a".into())],
                action: Action::Archive,
            },
            Timestamp(0),
        )
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(admitted == 1, "the action is admitted")?;
    let two = store
        .changes(&account(), one.next, 2)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(
        discovered(&two.changes) == ["c", "d"] && two.has_more,
        "the rest of the commit comes first",
    )?;
    expect(
        two.head > first && two.next == FeedCursor::from(first),
        "head moves; next ends the commit and does not jump",
    )?;
    let (rest, _) = read_all(store, two.next, 2).await?;
    expect(
        discovered(&rest) == ["e"]
            && rest
                .iter()
                .any(|entry| matches!(entry.change, Change::OverlayChanged { .. })),
        "then the commit and the admission",
    )?;
    let mut paged = one.changes;
    paged.extend(two.changes);
    paged.extend(rest);
    let (whole, _) = read_all(store, StateSeq(0).into(), 100).await?;
    expect(
        identities(&paged) == identities(&whole),
        "no entry skipped or repeated",
    )
}

async fn block_wraps_stored_state<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    // The store has moved on (cursor and Live) while the engine still
    // thought it was bootstrapping; its block must not corrupt that.
    let mut live = commit(&snapshot, 0);
    live.cursor = Some(CursorMove {
        expected: None,
        to: HistoryId(10),
    });
    live.sync = Some(mailtide::state::SyncState::Live);
    apply(store, &live).await?;
    let mut block = commit(&snapshot, 1);
    block.account_ops = vec![mailtide::commit::AccountOp::Block(
        mailtide::retry::BlockingFailure {
            class: mailtide::retry::Blocking::Account(mailtide::retry::AccountProblem::UserDeleted),
            detail: "gone".into(),
        },
    )];
    apply(store, &block).await?;
    let snapshot = claim(store).await?;
    expect(
        snapshot.clone().validate().is_ok(),
        "the stored state stays valid",
    )?;
    expect(
        matches!(&snapshot.sync, mailtide::state::SyncState::Blocked { resume, .. } if **resume == mailtide::state::SyncState::Live),
        "blocked, resuming into the stored Live state",
    )
}

async fn admission_is_unfenced<S: Store>(store: &S) -> Result<(), String> {
    let old = claim(store).await?;
    let mut c = commit(&old, 0);
    c.messages = vec![observe("a", &["INBOX"], 10)];
    apply(store, &c).await?;
    // A newer engine claims the account; the user's action still lands.
    let new = claim(store).await?;
    let admitted = store
        .admit(
            &account(),
            &ActionRequest::Messages {
                ids: vec![MessageId("a".into()), MessageId("unknown".into())],
                action: Action::Archive,
            },
            Timestamp(0),
        )
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(admitted == 1, "one known message admitted")?;
    expect(
        view(store, "a").await?.labels.is_empty(),
        "the action overlays at once",
    )?;
    let jobs = claim_all(store, &new, 1).await?;
    expect(
        jobs.iter()
            .any(|job| job.kind() == JobKind::Deliver(MessageId("a".into()))),
        "the current engine delivers it",
    )
}

fn block_commit(snapshot: &StoreSnapshot, seq: u64) -> Commit {
    let mut c = commit(snapshot, seq);
    c.account_ops = vec![mailtide::commit::AccountOp::Block(
        mailtide::retry::BlockingFailure {
            class: mailtide::retry::Blocking::Account(
                mailtide::retry::AccountProblem::DelegationRevoked,
            ),
            detail: "revoked".into(),
        },
    )];
    c
}

async fn status_does_not_claim<S: Store>(store: &S) -> Result<(), String> {
    expect(
        store
            .status(&account())
            .await
            .map_err(|e| format!("{e:?}"))?
            .is_none(),
        "an unknown account has no status",
    )?;
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![observe("a", &["INBOX"], 1), observe("b", &["INBOX"], 2)];
    apply(store, &c).await?;
    let status = store
        .status(&account())
        .await
        .map_err(|e| format!("{e:?}"))?
        .ok_or("status of a known account")?;
    expect(
        status.epoch == snapshot.epoch,
        "reading status must not bump the epoch",
    )?;
    expect(status.messages == 2, "two messages")?;
    expect(status.pending_imports == 2, "both await import")?;
    expect(status.jobs.ready == 2, "two import jobs ready")?;
    let accounts = store.accounts().await.map_err(|e| format!("{e:?}"))?;
    expect(accounts == vec![account()], "the account is listed")?;
    // The running engine's commits still apply: nothing was claimed.
    let mut later = commit(&snapshot, 1);
    later.messages = vec![observe("c", &["INBOX"], 3)];
    expect(
        matches!(apply(store, &later).await?, CommitOutcome::Applied { .. }),
        "the engine keeps its claim",
    )
}

async fn resume_restores<S: Store>(store: &S) -> Result<(), String> {
    expect(
        store
            .resume(&account(), Timestamp(0))
            .await
            .map_err(|e| format!("{e:?}"))?
            == mailtide::status::ResumeOutcome::Unknown,
        "unknown accounts can't resume",
    )?;
    let snapshot = claim(store).await?;
    expect(
        store
            .resume(&account(), Timestamp(0))
            .await
            .map_err(|e| format!("{e:?}"))?
            == mailtide::status::ResumeOutcome::NotBlocked,
        "a running account isn't blocked",
    )?;
    let mut c = commit(&snapshot, 0);
    c.cursor = Some(CursorMove {
        expected: None,
        to: HistoryId(5),
    });
    c.sync = Some(mailtide::state::SyncState::Live);
    apply(store, &c).await?;
    apply(store, &block_commit(&snapshot, 1)).await?;
    let outcome = store
        .resume(&account(), Timestamp(0))
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(
        matches!(
            outcome,
            mailtide::status::ResumeOutcome::Resumed {
                phase: mailtide::state::SyncPhase::Live,
                ..
            }
        ),
        "resumes into the phase it was blocked in",
    )?;
    let status = store
        .status(&account())
        .await
        .map_err(|e| format!("{e:?}"))?
        .ok_or("status")?;
    expect(
        status.sync == mailtide::state::SyncState::Live,
        "no longer blocked",
    )
}

async fn forget_fences<S: Store>(store: &S) -> Result<(), String> {
    expect(
        store
            .forget(&account())
            .await
            .map_err(|e| format!("{e:?}"))?
            == mailtide::status::ForgetOutcome::Unknown,
        "nothing to forget",
    )?;
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![observe("a", &["INBOX"], 1)];
    apply(store, &c).await?;
    expect(
        store
            .forget(&account())
            .await
            .map_err(|e| format!("{e:?}"))?
            == mailtide::status::ForgetOutcome::Forgotten { messages: 1 },
        "one message forgotten",
    )?;
    expect(
        store
            .status(&account())
            .await
            .map_err(|e| format!("{e:?}"))?
            .is_none(),
        "the account is gone",
    )?;
    let mut late = commit(&snapshot, 1);
    late.messages = vec![observe("b", &["INBOX"], 2)];
    expect(
        matches!(apply(store, &late).await?, CommitOutcome::Rejected(_)),
        "the old engine is fenced out",
    )
}

/// The jobs of a claim that must not be refused.
fn claimed(
    outcome: mailtide::jobs::ClaimOutcome,
) -> Result<Vec<mailtide::jobs::ClaimedJob>, String> {
    match outcome {
        mailtide::jobs::ClaimOutcome::Claimed { jobs, .. } => Ok(jobs),
        mailtide::jobs::ClaimOutcome::Rejected(reason) => Err(format!("claim refused: {reason:?}")),
    }
}

async fn feed_records_changes<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![observe("a", &["INBOX", "UNREAD"], 1)];
    apply(store, &c).await?;
    let mut patch = commit(&snapshot, 1);
    patch.messages = vec![MessageOp::PatchLabels {
        id: MessageId("a".into()),
        add: set(["STARRED"]),
        remove: set(["UNREAD"]),
        checkpoint: HistoryId(2),
    }];
    apply(store, &patch).await?;
    let mut deleted = commit(&snapshot, 2);
    deleted.messages = vec![MessageOp::Tombstone {
        id: MessageId("a".into()),
        evidence: DeletionEvidence::History { at: HistoryId(3) },
    }];
    apply(store, &deleted).await?;
    let page = store
        .changes(&account(), StateSeq(0).into(), 100)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    let kinds: Vec<&Change> = page.changes.iter().map(|e| &e.change).collect();
    expect(kinds.len() == 3, "one change per commit")?;
    expect(
        matches!(kinds[0], Change::MessageDiscovered { labels, .. } if *labels == set(["INBOX", "UNREAD"])),
        "discovery carries the labels then",
    )?;
    expect(
        matches!(kinds[1], Change::LabelsChanged { added, removed, .. }
            if *added == set(["STARRED"]) && *removed == set(["UNREAD"])),
        "label changes carry what was added and removed",
    )?;
    expect(
        matches!(kinds[2], Change::MessageDeleted { thread_id: Some(_), labels: Some(labels), .. }
            if *labels == set(["INBOX", "STARRED"])),
        "deletion carries the last thread and labels",
    )?;
    expect(
        page.changes.iter().all(|e| e.index == 0)
            && page.changes.windows(2).all(|w| w[0].state < w[1].state),
        "entries are identified by state and index",
    )
}

/// The page of a feed read that must not be too old.
fn page(feed: mailtide::revision::ChangeFeed) -> Result<mailtide::revision::ChangePage, String> {
    match feed {
        mailtide::revision::ChangeFeed::Page(page) => Ok(page),
        mailtide::revision::ChangeFeed::TooOld { oldest, .. } => {
            Err(format!("too old (oldest {oldest:?})"))
        }
    }
}

async fn compaction<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    for (seq, id) in [(0, "a"), (1, "b"), (2, "c")] {
        let mut c = commit(&snapshot, seq);
        c.messages = vec![observe(id, &["INBOX"], seq + 1)];
        apply(store, &c).await?;
    }
    let first = store
        .changes(&account(), StateSeq(0).into(), 100)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    let middle = first.changes[1].state;
    let removed = store
        .compact(&account(), middle)
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(removed == 2, "the first two commits' entries are removed")?;
    expect(
        store
            .changes(&account(), StateSeq(0).into(), 100)
            .await
            .map_err(|e| format!("{e:?}"))?
            == mailtide::revision::ChangeFeed::TooOld {
                generation: GENERATION,
                oldest: middle,
            },
        "reading from before the floor is too old",
    )?;
    let rest = store
        .changes(&account(), middle.into(), 100)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(rest.changes.len() == 1, "the remaining entry is kept")?;
    let ids = store
        .message_ids(&account(), None, 2)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let more = store
        .message_ids(&account(), ids.last(), 2)
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(
        ids == vec![MessageId("a".into()), MessageId("b".into())]
            && more == vec![MessageId("c".into())],
        "ids page in order",
    )
}

fn enqueue(kind: JobKind, priority: mailtide::quota::Priority, due_at: i64) -> JobOp {
    JobOp::Enqueue(mailtide::jobs::NewJob {
        kind,
        priority,
        due_at: Timestamp(due_at),
    })
}

/// SQL orders claims in its query and memory in `pick_next`: pin the order
/// so they can't drift.
async fn claim_order<S: Store>(store: &S) -> Result<(), String> {
    use mailtide::quota::Priority::{Backfill, Foreground, Live};
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    let m = |id: &str| MessageId(id.into());
    c.jobs = vec![
        enqueue(JobKind::Import(m("late")), Live, 5),
        enqueue(JobKind::Import(m("early")), Live, 1),
        enqueue(JobKind::Import(m("backfill")), Backfill, 0),
        enqueue(JobKind::BlobCleanup(m("early")), Live, 1),
        enqueue(JobKind::Verify(m("early")), Live, 1),
        enqueue(JobKind::Import(m("user")), Foreground, 9),
        enqueue(JobKind::Import(m("future")), Foreground, 100),
    ];
    apply(store, &c).await?;
    let mut order = Vec::new();
    for attempt in 1..=7 {
        let jobs = store
            .claim_jobs(
                &account(),
                snapshot.epoch,
                1,
                Timestamp(10),
                AttemptId(attempt),
                Priority::Backfill,
            )
            .await
            .map_err(|e| format!("{e:?}"))
            .and_then(claimed)?;
        order.extend(jobs.iter().map(mailtide::jobs::ClaimedJob::kind));
    }
    let expected = vec![
        JobKind::Import(m("user")),
        JobKind::Import(m("early")),
        JobKind::Verify(m("early")),
        JobKind::BlobCleanup(m("early")),
        JobKind::Import(m("late")),
        JobKind::Import(m("backfill")),
    ];
    expect(order == expected, &format!("claim order {order:?}"))
}

async fn claims_report_next_due<S: Store>(store: &S) -> Result<(), String> {
    use mailtide::jobs::ClaimOutcome;
    use mailtide::quota::Priority::Live;
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.jobs = vec![
        enqueue(JobKind::Import(MessageId("now".into())), Live, 0),
        enqueue(JobKind::Verify(MessageId("later".into())), Live, 60_000),
        enqueue(JobKind::Verify(MessageId("latest".into())), Live, 90_000),
    ];
    apply(store, &c).await?;
    let outcome = store
        .claim_jobs(
            &account(),
            snapshot.epoch,
            10,
            Timestamp(0),
            AttemptId(1),
            Priority::Backfill,
        )
        .await
        .map_err(|e| format!("{e:?}"))?;
    let ClaimOutcome::Claimed { jobs, next_due } = outcome else {
        return Err("the claim must succeed".into());
    };
    expect(jobs.len() == 1, "only the due job is claimed")?;
    expect(
        next_due == Some(Timestamp(60_000)),
        &format!("the running job doesn't count; the next is at 60s, got {next_due:?}"),
    )
}

async fn claims_respect_the_floor<S: Store>(store: &S) -> Result<(), String> {
    use mailtide::quota::Priority::{Backfill, Live};
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.jobs = vec![
        enqueue(JobKind::Import(MessageId("old".into())), Backfill, 0),
        enqueue(JobKind::Import(MessageId("new".into())), Live, 0),
    ];
    apply(store, &c).await?;
    let jobs = store
        .claim_jobs(
            &account(),
            snapshot.epoch,
            10,
            Timestamp(0),
            AttemptId(1),
            Live,
        )
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(claimed)?;
    let kinds: Vec<_> = jobs.iter().map(mailtide::jobs::ClaimedJob::kind).collect();
    expect(
        kinds == vec![JobKind::Import(MessageId("new".into()))],
        &format!("only live work above a backfill floor, got {kinds:?}"),
    )
}

async fn tracking<S: Store>(store: &S) -> Result<(), String> {
    use mailtide::status::TrackOutcome;
    let stranger = AccountId("stranger@example.com".into());
    expect(
        matches!(
            store.claim(&stranger, ENGINE_VERSION).await,
            Err(StoreError::NotTracked)
        ),
        "claiming an untracked account is refused",
    )?;
    expect(
        store
            .status(&stranger)
            .await
            .map_err(|e| format!("{e:?}"))?
            .is_none(),
        "a refused claim creates nothing",
    )?;
    let first = store
        .track(&stranger, 5)
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(first == TrackOutcome::Tracked { generation: 5 }, "tracked")?;
    let again = store
        .track(&stranger, 6)
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(
        again == TrackOutcome::AlreadyTracked,
        "tracking twice is a no-op",
    )?;
    store
        .claim(&stranger, ENGINE_VERSION)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let feed = page(
        store
            .changes(&stranger, StateSeq(0).into(), 10)
            .await
            .map_err(|e| format!("{e:?}"))?,
    )?;
    expect(feed.generation == 5, "pages name the generation")?;
    let status = store
        .status(&stranger)
        .await
        .map_err(|e| format!("{e:?}"))?
        .ok_or("status")?;
    expect(status.generation == 5, "status names the generation")
}

fn label(id: &str, name: &str, kind: LabelKind) -> Label {
    Label {
        id: LabelId(id.into()),
        name: name.into(),
        kind,
    }
}

async fn labels<S: Store>(store: &S, account: &AccountId) -> Result<Vec<Label>, String> {
    store.labels(account).await.map_err(|e| format!("{e:?}"))
}

async fn label_definitions<S: Store>(store: &S) -> Result<(), String> {
    let stranger = AccountId("stranger@example.com".into());
    expect(
        labels(store, &stranger).await?.is_empty(),
        "an untracked account has no labels",
    )?;
    let snapshot = claim(store).await?;
    expect(
        labels(store, &account()).await?.is_empty(),
        "no labels before the first label sync",
    )?;
    let mut first = commit(&snapshot, 0);
    first.labels = vec![LabelOp::ReplaceAll(vec![
        label("Label_2", "Receipts", LabelKind::User),
        label("INBOX", "INBOX", LabelKind::System),
        label("Label_1", "Travel", LabelKind::User),
    ])];
    apply(store, &first).await?;
    expect(
        labels(store, &account()).await?
            == vec![
                label("INBOX", "INBOX", LabelKind::System),
                label("Label_1", "Travel", LabelKind::User),
                label("Label_2", "Receipts", LabelKind::User),
            ],
        "labels round-trip in id order",
    )?;
    let mut refresh = commit(&snapshot, 1);
    refresh.labels = vec![LabelOp::ReplaceAll(vec![
        label("INBOX", "INBOX", LabelKind::System),
        label("Label_2", "Invoices", LabelKind::User),
    ])];
    apply(store, &refresh).await?;
    expect(
        labels(store, &account()).await?
            == vec![
                label("INBOX", "INBOX", LabelKind::System),
                label("Label_2", "Invoices", LabelKind::User),
            ],
        "a refresh replaces the definitions: renames show, removed labels go",
    )?;
    let page = store
        .changes(&account(), StateSeq(0).into(), 100)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(
        page.changes
            .iter()
            .filter(|e| e.change == Change::LabelDefinitionsChanged)
            .count()
            == 2,
        "each refresh is a feed entry",
    )?;
    expect(
        labels(store, &stranger).await?.is_empty(),
        "labels belong to their account",
    )
}

/// When labels were last listed, and which ids are defined, reach the next
/// engine (SPEC §6.6); a listing equal to the stored one changes nothing.
async fn label_refreshes<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    expect(
        snapshot.labels_refreshed_at.is_none() && snapshot.label_ids.is_empty(),
        "never listed",
    )?;
    let listed = vec![
        label("INBOX", "INBOX", LabelKind::System),
        label("Label_1", "Travel", LabelKind::User),
    ];
    let mut first = commit(&snapshot, 0);
    first.labels = vec![LabelOp::ReplaceAll(listed.clone())];
    first.account_ops = vec![mailtide::commit::AccountOp::SetLabelsRefreshedAt(
        Timestamp(5_000),
    )];
    apply(store, &first).await?;
    let snapshot = claim(store).await?;
    expect(
        snapshot.labels_refreshed_at == Some(Timestamp(5_000)),
        "the refresh time survives a hand-over",
    )?;
    expect(
        snapshot.label_ids == set(["INBOX", "Label_1"]),
        "the next engine knows the defined ids",
    )?;
    let mut same = commit(&snapshot, 0);
    let mut reordered = listed.clone();
    reordered.reverse();
    same.labels = vec![LabelOp::ReplaceAll(reordered)];
    same.account_ops = vec![mailtide::commit::AccountOp::SetLabelsRefreshedAt(
        Timestamp(9_000),
    )];
    apply(store, &same).await?;
    // An older time never moves it back.
    let mut late = commit(&snapshot, 1);
    late.account_ops = vec![mailtide::commit::AccountOp::SetLabelsRefreshedAt(
        Timestamp(7_000),
    )];
    apply(store, &late).await?;
    let page = store
        .changes(&account(), StateSeq(0).into(), 100)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(
        page.changes
            .iter()
            .filter(|e| e.change == Change::LabelDefinitionsChanged)
            .count()
            == 1,
        "an unchanged listing is no feed entry",
    )?;
    expect(
        labels(store, &account()).await? == listed,
        "an unchanged listing keeps the definitions",
    )?;
    let snapshot = claim(store).await?;
    expect(
        snapshot.labels_refreshed_at == Some(Timestamp(9_000)),
        "an unchanged listing still records its time, which never moves back",
    )
}

/// The feed's import entries, by message, with their content.
fn imports(entries: &[ChangeEntry]) -> Vec<(&str, Option<&mailtide::mirror::StoredContent>)> {
    entries
        .iter()
        .filter_map(|entry| match &entry.change {
            Change::MessageImported { id, .. } => Some((id.0.as_str(), entry.content.as_ref())),
            Change::MessageDiscovered { .. }
            | Change::LabelsChanged { .. }
            | Change::OverlayChanged { .. }
            | Change::MessageDeleted { .. }
            | Change::LabelDefinitionsChanged => None,
        })
        .collect()
}

async fn imports_carry_envelopes<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![import("a", &["INBOX"], 1), import("b", &["INBOX"], 2)];
    apply(store, &c).await?;
    let mut deleted = commit(&snapshot, 1);
    deleted.messages = vec![MessageOp::Tombstone {
        id: MessageId("b".into()),
        evidence: DeletionEvidence::History { at: HistoryId(3) },
    }];
    apply(store, &deleted).await?;
    let page = store
        .changes(&account(), StateSeq(0).into(), 100)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    let views = store
        .messages(&account(), &[MessageId("a".into())])
        .await
        .map_err(|e| format!("{e:?}"))?;
    let stored = views.first().and_then(|view| view.content.as_ref());
    expect(
        stored.is_some_and(|content| content.headers.subject.as_deref() == Some("Subject a")),
        "the import is stored with its header facts",
    )?;
    expect(
        imports(&page.changes) == vec![("a", stored), ("b", None)],
        "an import carries what the mirror stores, nothing once deleted",
    )?;
    expect(
        page.changes
            .iter()
            .filter(|entry| !matches!(entry.change, Change::MessageImported { .. }))
            .all(|entry| entry.content.is_none()),
        "other kinds carry no content",
    )
}

async fn a_large_commit_arrives_in_bounded_pages<S: Store>(store: &S) -> Result<(), String> {
    // More imports than one chunk of ids, in one commit (of imports only:
    // the messages were discovered before).
    const COUNT: u64 = 1_200;
    let snapshot = claim(store).await?;
    let ids: Vec<String> = (0..COUNT).map(|n| format!("m{n:04}")).collect();
    let mut seen = commit(&snapshot, 0);
    seen.messages = ids.iter().map(|id| observe(id, &["INBOX"], 1)).collect();
    apply(store, &seen).await?;
    let since = state(store).await?;
    let mut c = commit(&snapshot, 1);
    c.messages = ids.iter().map(|id| import(id, &["INBOX"], 1)).collect();
    apply(store, &c).await?;
    let (entries, sizes) = read_all(store, since.into(), 500).await?;
    expect(sizes == [500, 500, 200], "pages of at most 500")?;
    let entries = imports(&entries);
    expect(
        u64::try_from(entries.len()) == Ok(COUNT)
            && entries
                .iter()
                .map(|(id, _)| *id)
                .eq(ids.iter().map(String::as_str)),
        "every import arrives once, in order",
    )?;
    expect(
        entries.iter().all(|(id, content)| {
            content.is_some_and(|content| {
                content.headers.subject.as_deref() == Some(format!("Subject {id}").as_str())
                    && content.blob.0 == format!("{id}.eml")
            })
        }),
        "every import carries its own envelope",
    )
}

fn ids(ids: &[&str]) -> Vec<MessageId> {
    ids.iter().map(|id| MessageId((*id).into())).collect()
}

/// Discover `names` with their imports queued at Backfill, due at once.
async fn pending_backfill<S: Store>(
    store: &S,
    snapshot: &StoreSnapshot,
    seq: u64,
    names: &[&str],
) -> Result<(), String> {
    let mut c = commit(snapshot, seq);
    c.job_plan.priority = Priority::Backfill;
    c.messages = names.iter().map(|id| observe(id, &["INBOX"], 1)).collect();
    let outcome = apply(store, &c).await?;
    expect(
        matches!(outcome, CommitOutcome::Applied { .. }),
        "the discovery must apply",
    )
}

async fn prioritise<S: Store>(
    store: &S,
    account: &AccountId,
    names: &[&str],
    now: i64,
) -> Result<Prioritised, String> {
    store
        .prioritise(account, &ids(names), Timestamp(now))
        .await
        .map_err(|e| format!("{e:?}"))
}

/// Claim up to `limit` jobs of at least `floor` due at `now`.
async fn claim_at<S: Store>(
    store: &S,
    snapshot: &StoreSnapshot,
    limit: u32,
    now: i64,
    attempt: u64,
    floor: Priority,
) -> Result<Vec<mailtide::jobs::ClaimedJob>, String> {
    store
        .claim_jobs(
            &account(),
            snapshot.epoch,
            limit,
            Timestamp(now),
            AttemptId(attempt),
            floor,
        )
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(claimed)
}

/// The claimed jobs as (message, class).
fn classes(jobs: &[mailtide::jobs::ClaimedJob]) -> Vec<(String, Priority)> {
    jobs.iter()
        .map(|job| (job.kind().message().0.clone(), job.priority))
        .collect()
}

async fn prioritise_raises_queued_imports<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    pending_backfill(store, &snapshot, 0, &["a", "b", "c"]).await?;
    let report = prioritise(store, &account(), &["b"], 0).await?;
    expect(report.queued == ids(&["b"]), &format!("{report:?}"))?;
    let first = claim_at(store, &snapshot, 1, 0, 1, Priority::Backfill).await?;
    expect(
        classes(&first) == vec![("b".to_owned(), Priority::Foreground)],
        &format!("the opened message is claimed first, got {first:?}"),
    )?;
    let rest = claim_at(store, &snapshot, 10, 0, 2, Priority::Backfill).await?;
    expect(
        classes(&rest)
            == vec![
                ("a".to_owned(), Priority::Backfill),
                ("c".to_owned(), Priority::Backfill),
            ],
        &format!("the others keep their class, got {rest:?}"),
    )
}

async fn prioritise_is_unfenced<S: Store>(store: &S) -> Result<(), String> {
    let first = claim(store).await?;
    pending_backfill(store, &first, 0, &["a"]).await?;
    let held = claim_at(store, &first, 10, 0, 1, Priority::Backfill).await?;
    expect(held.len() == 1, "engine 1 holds the import")?;
    let second = claim(store).await?;
    let report = prioritise(store, &account(), &["a"], 0).await?;
    expect(
        report.queued == ids(&["a"]),
        "prioritise needs no epoch, and a released job is still queued",
    )?;
    let jobs = claim_at(store, &second, 10, 0, 2, Priority::Backfill).await?;
    expect(
        classes(&jobs) == vec![("a".to_owned(), Priority::Foreground)],
        &format!("engine 2 runs it in the Foreground class, got {jobs:?}"),
    )?;
    let stale = store
        .claim_jobs(
            &account(),
            first.epoch,
            10,
            Timestamp(0),
            AttemptId(3),
            Priority::Backfill,
        )
        .await;
    expect(
        stale
            == Ok(mailtide::jobs::ClaimOutcome::Rejected(
                Rejection::StaleEpoch,
            )),
        "engine 1's claims are still refused",
    )
}

async fn prioritise_reports_what_it_could_not_raise<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![
        import("done", &["INBOX"], 1),
        observe("gone", &["INBOX"], 1),
        observe("stuck", &["INBOX"], 1),
    ];
    apply(store, &c).await?;
    let mut deleted = commit(&snapshot, 1);
    deleted.messages = vec![MessageOp::Tombstone {
        id: MessageId("gone".into()),
        evidence: DeletionEvidence::History { at: HistoryId(2) },
    }];
    apply(store, &deleted).await?;
    let stuck = claim_all(store, &snapshot, 1)
        .await?
        .into_iter()
        .find(|job| job.kind() == JobKind::Import(MessageId("stuck".into())))
        .ok_or("the import of stuck is claimable")?;
    let mut block = commit(&snapshot, 2);
    block.claim = Some(stuck.claim());
    block.jobs = vec![JobOp::Finish {
        claim: stuck.claim(),
        outcome: JobOutcome::Block(mailtide::retry::BlockingFailure {
            class: mailtide::retry::Blocking::Permanent,
            detail: "malformed".into(),
        }),
    }];
    apply(store, &block).await?;
    let jobs = |status: Option<mailtide::status::AccountStatus>| status.map(|s| s.jobs);
    let before = jobs(
        store
            .status(&account())
            .await
            .map_err(|e| format!("{e:?}"))?,
    );
    let report = prioritise(store, &account(), &["done", "never", "gone", "stuck"], 0).await?;
    expect(
        report
            == Prioritised {
                imported: ids(&["done"]),
                unknown: ids(&["never", "gone"]),
                blocked: ids(&["stuck"]),
                ..Prioritised::default()
            },
        &format!("{report:?}"),
    )?;
    let after = jobs(
        store
            .status(&account())
            .await
            .map_err(|e| format!("{e:?}"))?,
    );
    expect(before == after, "no job is created, removed or unblocked")
}

async fn prioritise_keeps_a_failed_imports_backoff<S: Store>(store: &S) -> Result<(), String> {
    const BACKOFF: i64 = 60_000;
    let snapshot = claim(store).await?;
    pending_backfill(store, &snapshot, 0, &["a"]).await?;
    let [job] = <[_; 1]>::try_from(claim_all(store, &snapshot, 1).await?)
        .map_err(|jobs| format!("one job, got {jobs:?}"))?;
    let mut failed = commit(&snapshot, 1);
    failed.claim = Some(job.claim());
    failed.jobs = vec![JobOp::Finish {
        claim: job.claim(),
        outcome: JobOutcome::Retry {
            due_at: Timestamp(BACKOFF),
        },
    }];
    apply(store, &failed).await?;
    let report = prioritise(store, &account(), &["a"], 1_000).await?;
    expect(
        report.queued == ids(&["a"]),
        "a backing-off import is raised",
    )?;
    let early = claim_at(store, &snapshot, 10, BACKOFF - 1, 2, Priority::Backfill).await?;
    expect(early.is_empty(), "the backoff stands")?;
    let due = claim_at(store, &snapshot, 10, BACKOFF, 3, Priority::Backfill).await?;
    expect(
        classes(&due) == vec![("a".to_owned(), Priority::Foreground)]
            && due.first().map(|job| job.failed_attempts) == Some(1),
        &format!("raised, with its failed attempt counted, got {due:?}"),
    )
}

async fn prioritise_survives_a_running_claim<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    pending_backfill(store, &snapshot, 0, &["a", "b"]).await?;
    let running = claim_all(store, &snapshot, 1).await?;
    let report = prioritise(store, &account(), &["a", "b"], 0).await?;
    expect(
        report.queued == ids(&["a", "b"]),
        "running imports are raised for their next attempt",
    )?;
    for (seq, job) in (1..).zip(&running) {
        let outcome = if job.kind() == JobKind::Import(MessageId("a".into())) {
            JobOutcome::Done
        } else {
            JobOutcome::Retry {
                due_at: Timestamp(0),
            }
        };
        let mut finish = commit(&snapshot, seq);
        finish.claim = Some(job.claim());
        finish.jobs = vec![JobOp::Finish {
            claim: job.claim(),
            outcome,
        }];
        let applied = apply(store, &finish).await?;
        expect(
            matches!(applied, CommitOutcome::Applied { skipped: 0, .. }),
            "the claim still holds after prioritise",
        )?;
    }
    let next = claim_all(store, &snapshot, 2).await?;
    expect(
        classes(&next) == vec![("b".to_owned(), Priority::Foreground)],
        &format!("the finished job is gone, the retried one is Foreground, got {next:?}"),
    )
}

async fn prioritise_respects_the_foreground_cap<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let names: Vec<String> = (0..FOREGROUND_IMPORT_CAP + 5)
        .map(|n| format!("m{n:02}"))
        .collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    pending_backfill(store, &snapshot, 0, &names).await?;
    let report = prioritise(store, &account(), &names, 0).await?;
    expect(
        report.queued == ids(&names[..FOREGROUND_IMPORT_CAP])
            && report.over_limit == ids(&names[FOREGROUND_IMPORT_CAP..]),
        &format!("the first {FOREGROUND_IMPORT_CAP} are raised, got {report:?}"),
    )?;
    let raised = claim_at(store, &snapshot, 100, 0, 1, Priority::Foreground).await?;
    expect(
        raised.len() == FOREGROUND_IMPORT_CAP,
        &format!(
            "{FOREGROUND_IMPORT_CAP} Foreground imports, got {}",
            raised.len()
        ),
    )?;
    let rest = claim_at(store, &snapshot, 100, 0, 2, Priority::Backfill).await?;
    expect(
        rest.len() == 5 && rest.iter().all(|job| job.priority == Priority::Backfill),
        &format!("the rest stay in their class, got {rest:?}"),
    )?;
    let stranger = AccountId("stranger@example.com".into());
    let report = prioritise(store, &stranger, &["a", "a", "b"], 0).await?;
    expect(
        report == Prioritised::unknown(&ids(&["a", "b"])),
        "an untracked account answers every id as unknown",
    )
}

async fn prioritise_leaves_the_feed_alone<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    pending_backfill(store, &snapshot, 0, &["a", "b"]).await?;
    let state = |status: Option<mailtide::status::AccountStatus>| status.map(|s| s.state);
    let before = state(
        store
            .status(&account())
            .await
            .map_err(|e| format!("{e:?}"))?,
    );
    let feed_before = store
        .changes(&account(), StateSeq(0).into(), 100)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let report = prioritise(store, &account(), &["a"], 0).await?;
    expect(report.queued == ids(&["a"]), "raised")?;
    let after = state(
        store
            .status(&account())
            .await
            .map_err(|e| format!("{e:?}"))?,
    );
    let feed_after = store
        .changes(&account(), StateSeq(0).into(), 100)
        .await
        .map_err(|e| format!("{e:?}"))?;
    expect(before == after, "the state string does not move")?;
    expect(feed_before == feed_after, "no feed entry is written")
}

/// The feed's import entries, by message, with whether they carry content.
fn has_content(entries: &[ChangeEntry]) -> Vec<(&str, bool)> {
    imports(entries)
        .into_iter()
        .map(|(id, content)| (id, content.is_some()))
        .collect()
}

async fn a_page_hydrates_only_its_own_imports<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    // Each new import logs a discovery and an import: eight entries.
    let mut c = commit(&snapshot, 0);
    c.messages = ["a", "b", "c", "d"]
        .iter()
        .zip(1..)
        .map(|(id, checkpoint)| import(id, &["INBOX"], checkpoint))
        .collect();
    apply(store, &c).await?;
    let mut deleted = commit(&snapshot, 1);
    deleted.messages = vec![MessageOp::Tombstone {
        id: MessageId("c".into()),
        evidence: DeletionEvidence::History { at: HistoryId(9) },
    }];
    apply(store, &deleted).await?;
    let first = store
        .changes(&account(), StateSeq(0).into(), 4)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    let second = store
        .changes(&account(), first.next, 4)
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(page)?;
    expect(
        has_content(&first.changes) == [("a", true), ("b", true)],
        "the first page's imports carry their content",
    )?;
    expect(
        has_content(&second.changes) == [("c", false), ("d", true)],
        "a deleted import has none; its neighbour in the page keeps its own",
    )?;
    expect(
        first
            .changes
            .iter()
            .chain(&second.changes)
            .filter(|entry| !matches!(entry.change, Change::MessageImported { .. }))
            .all(|entry| entry.content.is_none()),
        "other kinds carry no content",
    )
}
