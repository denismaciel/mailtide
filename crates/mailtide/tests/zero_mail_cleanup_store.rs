//! Tests ported by name from Zero's `mail-cleanup-store.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.
//!
//! Zero's spam cleanup enumerates SPAM threads, then trashes them, and a
//! durable action clock makes any user move newer than the cleanup win. In
//! mailtide a bulk cleanup is an ordinary admitted action: per (message,
//! label) intents with versions, so a later user action supersedes it label
//! by label, durably, and stale deliveries can't clear newer intents.
//! Enumeration resumes through reconciliation's persisted progress.

use common::{Host, apply, commit, imported, live_mirror, plan};
use mailtide::command::GmailRequest;
use mailtide::commit::{CommitOutcome, MessageOp};
use mailtide::event::EventKind;
use mailtide::gmail::{
    HeaderFacts, HistoryPage, HistoryRecord, ImportedMessage, ListPage, MessageObservation,
};
use mailtide::ids::{
    AccountId, BlobRef, CommandId, ENGINE_VERSION, HistoryId, LabelId, MessageId, PageToken,
    ThreadId, Timestamp,
};
use mailtide::jobs::{AttemptId, ClaimedJob, JobKind, JobOp, JobOutcome, JobState};
use mailtide::labels::{LabelSet, set};
use mailtide::mirror::AccountMirror;
use mailtide::outbox::{
    Action, ActionRequest, Delivery, IntentOp, IntentState, IntentVersion, LabelChange, LabelDelta,
    ModifyGroup,
};
use mailtide::quota::Priority;
use mailtide::reconcile::ListScope;
use mailtide::retry::{Blocking, BlockingFailure, Failure, Retryable, RetryableFailure};

mod common {
    use std::collections::{BTreeMap, VecDeque};

    use mailtide::command::{Command, CommandKind, GmailRequest};
    use mailtide::commit::{Commit, CommitOutcome, CursorMove, JobPlan, MessageOp};
    use mailtide::event::{Event, EventKind};
    use mailtide::gmail::{HeaderFacts, MessageObservation};
    use mailtide::ids::{
        AccountId, BlobRef, CommandId, CommitId, ENGINE_VERSION, HistoryId, MessageId, ThreadId,
        Timestamp,
    };
    use mailtide::labels::LabelSet;
    use mailtide::mirror::AccountMirror;
    use mailtide::quota::Priority;
    use mailtide::schedule::TimerKind;
    use mailtide::state::SyncState;
    use mailtide::{Engine, EngineConfig};

    /// Job timing for commits a test writes itself: everything due at `now`.
    pub fn plan(now: i64) -> JobPlan {
        let now = Timestamp(now);
        JobPlan {
            priority: Priority::Live,
            import_due_at: now,
            draft_import_due_at: now,
            verify_due_at: now,
            cleanup_due_at: now,
            deliver_due_at: now,
            confirm_due_at: now,
        }
    }

    /// An empty commit from the mirror's current writer.
    pub fn commit(mirror: &AccountMirror) -> Commit {
        Commit {
            id: CommitId {
                epoch: mirror.record.epoch,
                seq: mirror.seq.0,
            },
            account: mirror.account.clone(),
            epoch: mirror.record.epoch,
            engine_version: ENGINE_VERSION,
            cursor: None,
            claim: None,
            sync: None,
            messages: Vec::new(),
            labels: Vec::new(),
            account_ops: Vec::new(),
            job_plan: plan(0),
            jobs: Vec::new(),
            intents: Vec::new(),
        }
    }

    pub fn apply(mirror: &mut AccountMirror, commit: &Commit) {
        assert!(
            matches!(mirror.apply(commit), CommitOutcome::Applied { .. }),
            "commit applies"
        );
    }

    /// A store following history from `cursor`, owned by a current engine.
    pub fn live_mirror(account: AccountId, cursor: u64) -> AccountMirror {
        let mut mirror = AccountMirror::new(account);
        mirror.claim(ENGINE_VERSION).expect("current version");
        let mut c = commit(&mirror);
        c.sync = Some(SyncState::Live);
        c.cursor = Some(CursorMove {
            expected: None,
            to: HistoryId(cursor),
        });
        apply(&mut mirror, &c);
        mirror
    }

    /// A stored message `id` in `thread`.
    pub fn imported(id: &str, thread: &str, labels: &LabelSet, checkpoint: u64) -> MessageOp {
        MessageOp::MarkImported {
            observation: MessageObservation {
                id: MessageId(id.into()),
                thread_id: ThreadId(thread.into()),
                labels: labels.clone(),
                checkpoint: HistoryId(checkpoint),
            },
            blob: BlobRef(format!("blobs/{id}")),
            size: 1,
            internal_date: Timestamp(0),
            headers: Box::new(HeaderFacts::default()),
        }
    }

    /// A host: performs store commands against the mirror at once and
    /// collects Gmail calls and timers for the test to answer.
    pub struct Host {
        pub mirror: AccountMirror,
        pub engine: Engine,
        pub now: Timestamp,
        pub timers: BTreeMap<TimerKind, Timestamp>,
        pub gmail: Vec<(CommandId, GmailRequest)>,
    }

    fn build(mirror: &mut AccountMirror) -> Engine {
        let snapshot = mirror.claim(ENGINE_VERSION).expect("current version");
        Engine::new(
            snapshot.validate().expect("valid snapshot"),
            EngineConfig::default(),
        )
    }

    impl Host {
        pub fn new(mut mirror: AccountMirror, now: i64) -> Self {
            let engine = build(&mut mirror);
            Self {
                mirror,
                engine,
                now: Timestamp(now),
                timers: BTreeMap::new(),
                gmail: Vec::new(),
            }
        }

        pub fn start(&mut self) {
            let commands = self.engine.start(self.now);
            self.pump(commands);
        }

        /// Crash and rebuild the engine from the store.
        pub fn restart(&mut self) {
            self.engine = build(&mut self.mirror);
            self.timers.clear();
            self.gmail.clear();
            self.start();
        }

        pub fn at(&mut self, now: i64) -> &mut Self {
            self.now = Timestamp(now);
            self
        }

        pub fn handle(&mut self, kind: EventKind) {
            let commands = self.engine.handle(Event {
                now: self.now,
                kind,
            });
            self.pump(commands);
        }

        pub fn fire(&mut self, timer: TimerKind) {
            self.timers.remove(&timer);
            self.handle(EventKind::TimerFired { timer });
        }

        pub fn take_gmail(&mut self) -> Vec<(CommandId, GmailRequest)> {
            std::mem::take(&mut self.gmail)
        }

        fn pump(&mut self, commands: Vec<Command>) {
            let mut queue: VecDeque<Command> = commands.into();
            while let Some(command) = queue.pop_front() {
                let id = command.id;
                let event = match command.kind {
                    CommandKind::Gmail(call) => {
                        self.gmail.push((id, call.request));
                        None
                    }
                    CommandKind::Commit(commit) => Some(match self.mirror.apply(&commit) {
                        CommitOutcome::Applied { .. } => {
                            EventKind::CommitApplied { commit: commit.id }
                        }
                        CommitOutcome::Rejected(reason) => EventKind::CommitRejected {
                            commit: commit.id,
                            reason,
                        },
                    }),
                    CommandKind::Schedule { timer, at } => {
                        self.timers.insert(timer, at);
                        None
                    }
                    CommandKind::ClaimJobs {
                        epoch,
                        limit,
                        now,
                        attempt,
                        ..
                    } => Some(EventKind::JobsClaimed {
                        command: id,
                        jobs: self
                            .mirror
                            .claim_jobs(epoch, limit, now, attempt)
                            .unwrap_or_default(),
                    }),
                    CommandKind::DeleteBlob { .. } => Some(EventKind::BlobDeleted { command: id }),
                    CommandKind::ReportStatus(_) => None,
                };
                if let Some(kind) = event {
                    queue.extend(self.engine.handle(Event {
                        now: self.now,
                        kind,
                    }));
                }
            }
        }
    }
}

fn account() -> AccountId {
    AccountId("alice@example.com".into())
}

fn m(id: &str) -> MessageId {
    MessageId(id.into())
}

fn label(id: &str) -> LabelId {
    LabelId(id.into())
}

fn labels(ids: &[&str]) -> Vec<LabelId> {
    ids.iter().map(|id| label(id)).collect()
}

/// The cleanup: out of spam (and the inbox), into the trash.
fn cleanup() -> Action {
    Action::ChangeLabels(
        LabelDelta::new(labels(&["TRASH"]), labels(&["SPAM", "INBOX"])).expect("a valid change"),
    )
}

/// The user moving a thread back to spam.
fn back_to_spam() -> Action {
    Action::ChangeLabels(
        LabelDelta::new(labels(&["SPAM"]), labels(&["TRASH"])).expect("a valid change"),
    )
}

fn on_thread(thread: &str, action: Action) -> IntentOp {
    IntentOp::Request(ActionRequest::Thread {
        id: ThreadId(thread.into()),
        action,
    })
}

/// Admit actions with a plain store commit.
fn admit(mirror: &mut AccountMirror, intents: Vec<IntentOp>) {
    let mut c = commit(mirror);
    c.intents = intents;
    apply(mirror, &c);
}

fn spam_mirror(messages: &[(&str, &str)]) -> AccountMirror {
    let mut mirror = live_mirror(account(), 10);
    let mut c = commit(&mirror);
    c.messages = messages
        .iter()
        .map(|(id, thread)| imported(id, thread, &set(["SPAM"]), 5))
        .collect();
    apply(&mut mirror, &c);
    mirror
}

fn effective(mirror: &AccountMirror, id: &str) -> LabelSet {
    mirror.view(&m(id)).expect("message").labels
}

fn intent_of(
    mirror: &AccountMirror,
    id: &str,
    l: &str,
) -> (LabelChange, IntentVersion, IntentState) {
    let intent = &mirror.intents[&(m(id), label(l))];
    (intent.change, intent.version, intent.state.clone())
}

fn claim(mirror: &mut AccountMirror, limit: u32, now: i64, attempt: u64) -> Vec<ClaimedJob> {
    mirror
        .claim_jobs(
            mirror.record.epoch,
            limit,
            Timestamp(now),
            AttemptId(attempt),
        )
        .expect("current epoch")
}

/// The modify groups among Gmail calls, with their command ids.
fn modifies(calls: Vec<(CommandId, GmailRequest)>) -> Vec<(CommandId, ModifyGroup)> {
    calls
        .into_iter()
        .filter_map(|(id, request)| match request {
            GmailRequest::Modify(group) => Some((id, group)),
            _ => None,
        })
        .collect()
}

fn group_for(groups: &[(CommandId, ModifyGroup)], id: &str) -> (CommandId, ModifyGroup) {
    let found: Vec<_> = groups
        .iter()
        .filter(|(_, group)| group.messages.contains(&m(id)))
        .cloned()
        .collect();
    assert_eq!(found.len(), 1, "one delivery for {id}");
    found[0].clone()
}

fn transient() -> Failure {
    Failure::Retryable(RetryableFailure {
        class: Retryable::Transient,
        retry_after: None,
        detail: "connection reset".into(),
    })
}

/// Zero: mail-cleanup-store > durable spam cleanup > protects a real no-op restore after discovery and survives reconstruction
#[test]
fn durable_spam_cleanup_protects_a_real_no_op_restore_after_discovery_and_survives_reconstruction()
{
    let mut mirror = spam_mirror(&[("m", "a")]);
    admit(&mut mirror, vec![on_thread("a", cleanup())]);
    // Gmail already lacks TRASH, so this restore changes nothing there; it
    // must still be recorded, newer than the cleanup.
    admit(
        &mut mirror,
        vec![on_thread(
            "a",
            Action::ChangeLabels(
                LabelDelta::new(Vec::new(), labels(&["TRASH"])).expect("a valid change"),
            ),
        )],
    );
    assert!(
        !mirror
            .view(&m("m"))
            .expect("m")
            .observed_labels
            .contains(&label("TRASH"))
    );

    // Reconstruction: a new engine takes the store over.
    mirror.claim(ENGINE_VERSION).expect("current version");
    assert_eq!(
        intent_of(&mirror, "m", "TRASH"),
        (LabelChange::Remove, IntentVersion(2), IntentState::Pending)
    );
    assert!(!effective(&mirror, "m").contains(&label("TRASH")));
    // What goes to Gmail is the restore, never the cleanup's trash.
    let jobs = claim(&mut mirror, 4, 0, 1);
    assert_eq!(jobs.len(), 1);
    let group = jobs[0].modify.clone().expect("pending changes");
    assert!(!group.add.contains(&label("TRASH")));
    assert!(group.remove.contains(&label("TRASH")));
    assert!(group.versions.contains(&Delivery {
        message: m("m"),
        label: label("TRASH"),
        version: IntentVersion(2),
    }));
}

/// The first delivery attempt after a crash: both messages' cleanup
/// changes go out; `m1`'s fails after Gmail applied it, `m2`'s is in flight.
fn crash_then_partial_delivery(host: &mut Host) -> (CommandId, Vec<Delivery>) {
    host.handle(EventKind::ActionRequested {
        action: ActionRequest::Thread {
            id: ThreadId("a".into()),
            action: cleanup(),
        },
    });
    // Crash before anything was delivered: the admitted intents and their
    // delivery jobs survive.
    host.restart();
    assert_eq!(host.mirror.intents.len(), 6);
    for id in ["m1", "m2"] {
        assert!(host.mirror.jobs.contains_key(&JobKind::Deliver(m(id))));
    }
    let pass = host.take_gmail().remove(0).0;
    host.fire(mailtide::schedule::TimerKind::Jobs);
    let old = modifies(host.take_gmail());
    assert_eq!(old.len(), 2);
    let (m1_call, _) = group_for(&old, "m1");
    let (m2_call, _) = group_for(&old, "m2");
    let old_versions: Vec<Delivery> = old.iter().flat_map(|(_, g)| g.versions.clone()).collect();
    assert!(old_versions.iter().all(|d| d.version == IntentVersion(1)));
    host.handle(EventKind::RequestFailed {
        command: m1_call,
        failure: transient(),
    });
    // History shows Gmail did apply m1's change.
    let record = |id: u64, change: &str, l: &str| {
        let (message_id, labels) = (m("m1"), set([l]));
        match change {
            "add" => HistoryRecord::LabelsAdded {
                id: HistoryId(id),
                message_id,
                labels,
            },
            _ => HistoryRecord::LabelsRemoved {
                id: HistoryId(id),
                message_id,
                labels,
            },
        }
    };
    host.handle(EventKind::HistoryFetched {
        command: pass,
        page: HistoryPage {
            records: vec![record(11, "add", "TRASH"), record(12, "remove", "SPAM")],
            history_id: HistoryId(12),
            next_page_token: None,
        },
    });
    (m2_call, old_versions)
}

/// Zero: mail-cleanup-store > durable spam cleanup > recovers admitted cleanup intent and fences partial-delivery retries against a newer restore
#[test]
fn durable_spam_cleanup_recovers_admitted_cleanup_intent_and_fences_partial_delivery_retries_against_a_newer_restore()
 {
    let mut host = Host::new(spam_mirror(&[("m1", "a"), ("m2", "a")]), 0);
    host.start();
    let (m2_call, old_versions) = crash_then_partial_delivery(&mut host);

    // The user moves the thread back to spam, then the old m2 delivery
    // succeeds: it must not clear or override the newer restore.
    host.at(5_000).handle(EventKind::ActionRequested {
        action: ActionRequest::Thread {
            id: ThreadId("a".into()),
            action: back_to_spam(),
        },
    });
    host.handle(EventKind::LabelsModified { command: m2_call });
    for id in ["m1", "m2"] {
        let labels = effective(&host.mirror, id);
        assert!(labels.contains(&label("SPAM")), "{id}: {labels:?}");
        assert!(!labels.contains(&label("TRASH")), "{id}: {labels:?}");
    }

    // The retries carry the restore's versions for SPAM and TRASH, never
    // the cleanup's.
    let newer = modifies(host.take_gmail());
    let (_, m1) = group_for(&newer, "m1");
    let (_, m2) = group_for(&newer, "m2");
    for group in [&m1, &m2] {
        assert!(group.add.contains(&label("SPAM")));
        assert!(group.remove.contains(&label("TRASH")));
        assert!(!group.add.contains(&label("TRASH")));
        for delivery in &group.versions {
            if delivery.label != label("INBOX") {
                assert_eq!(delivery.version, IntentVersion(2), "{delivery:?}");
                assert!(!old_versions.contains(delivery));
            }
        }
    }
    // m2's old delivery was acknowledged in full, so nothing of it repeats.
    assert!(m2.versions.iter().all(|d| !old_versions.contains(d)));
}

/// Zero: mail-cleanup-store > durable spam cleanup > resumes discovery and delivery without changing the enumerated set
#[test]
fn durable_spam_cleanup_resumes_discovery_and_delivery_without_changing_the_enumerated_set() {
    let mut host = Host::new(AccountMirror::new(account()), 0);
    host.start();
    let call = |host: &mut Host| host.take_gmail().remove(0).0;
    let listed = |ids: &[&str], next: Option<&str>| ListPage {
        messages: ids
            .iter()
            .map(|id| (m(id), ThreadId(format!("thread-{id}"))))
            .collect(),
        next_page_token: next.map(|t| PageToken(t.into())),
    };
    let command = call(&mut host);
    host.handle(EventKind::LabelsListed {
        command,
        labels: Vec::new(),
    });
    let command = call(&mut host);
    host.handle(EventKind::ProfileFetched {
        command,
        history_id: HistoryId(100),
    });
    let command = call(&mut host);
    host.handle(EventKind::MessagesListed {
        command,
        page: listed(&["a", "b"], Some("page2")),
    });

    // Restarting resumes the enumeration where it was rather than over.
    host.restart();
    let resumed = host.take_gmail();
    assert_eq!(
        resumed.iter().map(|(_, r)| r).collect::<Vec<_>>(),
        vec![&GmailRequest::ListMessages {
            scope: ListScope::All,
            page_token: Some(PageToken("page2".into())),
        }]
    );
    host.handle(EventKind::MessagesListed {
        command: resumed[0].0,
        page: listed(&["b", "c"], None),
    });
    assert_eq!(host.mirror.record.cursor, Some(HistoryId(100)));
    assert_eq!(host.mirror.messages.len(), 3);
    let imports = |host: &mut Host| -> Vec<(CommandId, MessageId)> {
        host.take_gmail()
            .into_iter()
            .filter_map(|(command, request)| match request {
                GmailRequest::ImportMessage { id } => Some((command, id)),
                _ => None,
            })
            .collect()
    };
    host.fire(mailtide::schedule::TimerKind::Jobs);
    let first = imports(&mut host);
    assert_eq!(
        first.iter().map(|(_, id)| id.clone()).collect::<Vec<_>>(),
        vec![m("a"), m("b"), m("c")]
    );
    host.handle(EventKind::MessageImported {
        command: first[0].0,
        message: Box::new(ImportedMessage {
            observation: mailtide::gmail::MessageObservation {
                id: m("a"),
                thread_id: ThreadId("thread-a".into()),
                labels: set(["SPAM"]),
                checkpoint: HistoryId(100),
            },
            blob: BlobRef("blobs/a".into()),
            size: 1,
            internal_date: Timestamp(0),
            headers: HeaderFacts::default(),
        }),
    });
    // Delivery resumes after a restart with exactly what is left.
    host.restart();
    host.fire(mailtide::schedule::TimerKind::Jobs);
    let rest = imports(&mut host);
    assert_eq!(
        rest.iter().map(|(_, id)| id.clone()).collect::<Vec<_>>(),
        vec![m("b"), m("c")]
    );
}

/// Zero: mail-cleanup-store > durable spam cleanup > preserves newer user moves even after their provider acknowledgment and no-op restores
#[test]
fn durable_spam_cleanup_preserves_newer_user_moves_even_after_their_provider_acknowledgment_and_no_op_restores()
 {
    let mut mirror = spam_mirror(&[("o", "old"), ("r", "restored")]);
    // A move older than the cleanup is overridden by it…
    admit(
        &mut mirror,
        vec![on_thread(
            "old",
            Action::MarkNotSpam(mailtide::outbox::RestoreDestination::Inbox),
        )],
    );
    admit(
        &mut mirror,
        vec![
            on_thread("old", cleanup()),
            on_thread("restored", cleanup()),
        ],
    );
    // …a newer one overrides the cleanup.
    admit(&mut mirror, vec![on_thread("restored", back_to_spam())]);
    mirror.claim(ENGINE_VERSION).expect("current version");
    assert_eq!(effective(&mirror, "o"), set(["TRASH"]));
    assert_eq!(effective(&mirror, "r"), set(["SPAM"]));

    // Gmail acknowledges the user's move and history confirms it.
    let jobs = claim(&mut mirror, 2, 0, 1);
    let restored = jobs
        .iter()
        .find(|job| job.kind == JobKind::Deliver(m("r")))
        .expect("r is delivered");
    let group = restored.modify.clone().expect("pending changes");
    assert!(group.add.contains(&label("SPAM")) && !group.add.contains(&label("TRASH")));
    let mut c = commit(&mirror);
    c.claim = Some(restored.claim());
    c.intents = vec![IntentOp::Delivered {
        versions: group.versions,
        at: Timestamp(0),
        by: restored.attempt,
    }];
    c.jobs = vec![JobOp::Finish {
        claim: restored.claim(),
        outcome: JobOutcome::Done,
    }];
    apply(&mut mirror, &c);
    // The confirmation fetch after delivery shows the move.
    let verify = claim(&mut mirror, 1, 0, 2).remove(0);
    assert_eq!(verify.kind, JobKind::Verify(m("r")));
    let mut confirm = commit(&mirror);
    confirm.claim = Some(verify.claim());
    confirm.messages = vec![MessageOp::Observe(MessageObservation {
        id: m("r"),
        thread_id: ThreadId("restored".into()),
        labels: set(["SPAM"]),
        checkpoint: HistoryId(20),
    })];
    confirm.jobs = vec![JobOp::Finish {
        claim: verify.claim(),
        outcome: JobOutcome::Done,
    }];
    apply(&mut mirror, &confirm);

    // The acknowledged move stays; the cleanup has nothing left to send.
    assert!(mirror.intents_of(&m("r")).is_empty());
    assert!(!mirror.jobs.contains_key(&JobKind::Deliver(m("r"))));
    assert_eq!(effective(&mirror, "r"), set(["SPAM"]));
    mirror.claim(ENGINE_VERSION).expect("current version");
    assert_eq!(effective(&mirror, "r"), set(["SPAM"]));
}

/// Zero: mail-cleanup-store > durable spam cleanup > rolls back target removal alongside intent admission and persists deferrals
#[test]
fn durable_spam_cleanup_rolls_back_target_removal_alongside_intent_admission_and_persists_deferrals()
 {
    let mut mirror = spam_mirror(&[("a", "a")]);
    admit(&mut mirror, vec![on_thread("a", cleanup())]);
    let kind = JobKind::Deliver(m("a"));
    let job = claim(&mut mirror, 1, 0, 1).remove(0);
    let versions = job.modify.clone().expect("pending").versions;

    // Finishing the delivery and admitting more intents in one commit that
    // fails applies neither.
    let before = mirror.clone();
    let mut interrupted = commit(&mirror);
    let mut stale = job.claim();
    stale.attempt = AttemptId(99);
    interrupted.claim = Some(stale.clone());
    interrupted.intents = vec![
        IntentOp::Delivered {
            versions: versions.clone(),
            at: Timestamp(0),
            by: AttemptId(99),
        },
        on_thread("a", Action::SetRead(mailtide::outbox::ReadState::Read)),
    ];
    interrupted.jobs = vec![JobOp::Finish {
        claim: stale,
        outcome: JobOutcome::Done,
    }];
    assert!(matches!(
        mirror.apply(&interrupted),
        CommitOutcome::Rejected(_)
    ));
    assert_eq!(mirror, before);

    // A quota deferral is persisted without counting an attempt.
    let mut deferred = commit(&mirror);
    deferred.claim = Some(job.claim());
    deferred.jobs = vec![JobOp::Finish {
        claim: job.claim(),
        outcome: JobOutcome::Defer {
            due_at: Timestamp(60_000),
        },
    }];
    apply(&mut mirror, &deferred);
    mirror.claim(ENGINE_VERSION).expect("current version");
    assert_eq!(mirror.jobs[&kind].failed_attempts, 0);
    assert_eq!(
        mirror.jobs[&kind].state,
        JobState::Ready {
            due_at: Timestamp(60_000)
        }
    );
    assert!(claim(&mut mirror, 1, 59_999, 2).is_empty());

    // Gmail refuses it for good: the intents block and stop overlaying.
    let job = claim(&mut mirror, 1, 60_000, 3).remove(0);
    let mut refused = commit(&mirror);
    refused.claim = Some(job.claim());
    refused.intents = vec![IntentOp::Block {
        versions: job.modify.clone().expect("pending").versions,
        failure: BlockingFailure {
            class: Blocking::Permanent,
            detail: "invalid label".into(),
        },
    }];
    refused.jobs = vec![JobOp::Finish {
        claim: job.claim(),
        outcome: JobOutcome::Done,
    }];
    apply(&mut mirror, &refused);
    let view = mirror.view(&m("a")).expect("a");
    assert_eq!((view.blocked_actions, view.labels), (3, set(["SPAM"])));
    assert!(!mirror.jobs.contains_key(&kind));

    // Retrying the action starts afresh: due now, no failed attempts.
    let mut retry = commit(&mirror);
    retry.job_plan = plan(61_000);
    retry.intents = vec![on_thread("a", cleanup())];
    apply(&mut mirror, &retry);
    assert_eq!(mirror.jobs[&kind].failed_attempts, 0);
    assert_eq!(
        mirror.jobs[&kind].state,
        JobState::Ready {
            due_at: Timestamp(61_000)
        }
    );
    assert_eq!(mirror.view(&m("a")).expect("a").labels, set(["TRASH"]));
    assert_eq!(mirror.jobs[&kind].priority, Priority::Foreground);
}
