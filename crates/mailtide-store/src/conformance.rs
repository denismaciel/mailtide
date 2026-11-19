//! Behaviour every store must show, as a reusable suite.
//!
//! Covers atomic commits, epoch/version/cursor fencing, conditional message
//! operations (checkpoints, tombstones, unknown ids), job claims by attempt,
//! and the change log. Each case runs against a fresh store from the factory.

use std::future::Future;

use mailtide::commit::{
    Commit, CommitOutcome, CursorMove, DeletionEvidence, JobPlan, MessageOp, Rejection,
};
use mailtide::gmail::MessageObservation;
use mailtide::ids::{
    AccountId, CommitId, ENGINE_VERSION, EngineVersion, HistoryId, MessageId, ThreadId, Timestamp,
};
use mailtide::jobs::{AttemptId, JobKind, JobOp, JobOutcome};
use mailtide::labels::{LabelSet, set};
use mailtide::mirror::MessageView;
use mailtide::outbox::{Action, ActionRequest, IntentOp, ReadState};
use mailtide::quota::Priority;
use mailtide::revision::{Change, StateSeq};
use mailtide::state::StoreSnapshot;

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
    let cases: Vec<(&'static str, CaseFn<S>)> = vec![
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
        ("change pages never split a commit", |s| {
            Box::pin(whole_commit_pages(s))
        }),
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
    ];
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

async fn claim<S: Store>(store: &S) -> Result<StoreSnapshot, String> {
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
        .changes(&account(), StateSeq(0), 100)
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
        .claim_jobs(&account(), snapshot.epoch, 10, Timestamp(0), AttemptId(1))
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
        .claim_jobs(&account(), snapshot.epoch, 10, Timestamp(0), AttemptId(1))
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
        .claim_jobs(&account(), snapshot.epoch, 10, Timestamp(0), AttemptId(7))
        .await
        .map_err(|e| format!("{e:?}"))
        .and_then(claimed)?;
    let job = jobs.first().ok_or("the import must be claimable")?;
    let again = store
        .claim_jobs(&account(), snapshot.epoch, 10, Timestamp(0), AttemptId(8))
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
        .changes(&account(), StateSeq(0), 2)
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
        .claim_jobs(&account(), old.epoch, 10, Timestamp(0), AttemptId(1))
        .await;
    expect(
        stale
            == Ok(mailtide::jobs::ClaimOutcome::Rejected(
                Rejection::StaleEpoch,
            )),
        "a claim from an older epoch must be refused",
    )?;
    let current = store
        .claim_jobs(&account(), new.epoch, 10, Timestamp(0), AttemptId(2))
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

async fn whole_commit_pages<S: Store>(store: &S) -> Result<(), String> {
    let snapshot = claim(store).await?;
    let mut c = commit(&snapshot, 0);
    c.messages = vec![
        observe("a", &[], 1),
        observe("b", &[], 2),
        observe("c", &[], 3),
    ];
    apply(store, &c).await?;
    let mut d = commit(&snapshot, 1);
    d.messages = vec![observe("d", &[], 4)];
    apply(store, &d).await?;
    let mut since = StateSeq(0);
    let mut seen = Vec::new();
    for _ in 0..10 {
        let page = store
            .changes(&account(), since, 2)
            .await
            .map_err(|e| format!("{e:?}"))
            .and_then(page)?;
        seen.extend(page.changes);
        since = page.next;
        if !page.has_more {
            break;
        }
    }
    let ids: Vec<&str> = seen
        .iter()
        .filter_map(|entry| match &entry.change {
            Change::MessageDiscovered { id, .. } => Some(id.0.as_str()),
            Change::MessageImported { .. }
            | Change::LabelsChanged { .. }
            | Change::OverlayChanged { .. }
            | Change::MessageDeleted { .. }
            | Change::LabelDefinitionsChanged => None,
        })
        .collect();
    expect(
        ids == ["a", "b", "c", "d"],
        "every change arrives exactly once",
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
        .changes(&account(), StateSeq(0), 100)
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
        mailtide::revision::ChangeFeed::TooOld { oldest } => {
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
        .changes(&account(), StateSeq(0), 100)
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
            .changes(&account(), StateSeq(0), 100)
            .await
            .map_err(|e| format!("{e:?}"))?
            == mailtide::revision::ChangeFeed::TooOld { oldest: middle },
        "reading from before the floor is too old",
    )?;
    let rest = store
        .changes(&account(), middle, 100)
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
        .claim_jobs(&account(), snapshot.epoch, 10, Timestamp(0), AttemptId(1))
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
