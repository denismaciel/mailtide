//! Tests ported by name from Zero's `mail-history-store.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.
//!
//! Zero's `MailHistoryStore` keeps the history cursor, the per-thread import
//! queue and browser notifications in Durable Object SQLite. In mailtide the
//! same rules live in the engine (cursor, passes, timers), the job table of
//! `AccountMirror` (dedupe, priority, claims, deferrals) and its change log
//! (what clients are notified of).

use std::time::Duration;

use common::{
    Host, ImportProgress, account, apply, claim_finish, commit, import_jobs, imported, live_mirror,
    message_ids, observe, plan, progress, requests,
};
use mailtide::command::{CommandKind, GmailRequest};
use mailtide::commit::{
    AccountOp, CommitOutcome, CursorMove, DeletionEvidence, MessageOp, Rejection,
};
use mailtide::event::{Event, EventKind};
use mailtide::gmail::{
    HeaderFacts, HistoryPage, HistoryRecord, ImportedMessage, ListPage, MessageRef,
};
use mailtide::ids::{
    BlobRef, ENGINE_VERSION, HistoryId, LabelId, MessageId, PageToken, ThreadId, Timestamp,
};
use mailtide::jobs::{
    AttemptId, Job, JobClaim, JobKind, JobOp, JobOutcome, JobState, NewJob, pick_next,
};
use mailtide::labels::set;
use mailtide::mirror::AccountMirror;
use mailtide::outbox::{Action, ActionRequest};
use mailtide::quota::{self, Admission, Budget, Ledger, Priority};
use mailtide::reconcile::ListScope;
use mailtide::retry::{
    Blocking, BlockingFailure, Cooldown, Failure, QuotaTarget, Retryable, RetryableFailure,
};
use mailtide::revision::{Change, StateSeq};
use mailtide::schedule::{self, TimerKind};

mod common {
    use std::collections::{BTreeMap, VecDeque};

    use mailtide::command::{Command, CommandKind, GmailRequest};
    use mailtide::commit::{AccountOp, Commit, CommitOutcome, CursorMove, JobPlan, MessageOp};
    use mailtide::event::{Event, EventKind};
    use mailtide::gmail::{HeaderFacts, MessageObservation};
    use mailtide::ids::{
        AccountId, BlobRef, CommandId, CommitId, ENGINE_VERSION, HistoryId, MessageId, ThreadId,
        Timestamp,
    };
    use mailtide::jobs::{AttemptId, JobKind, JobOp, JobOutcome};
    use mailtide::labels::LabelSet;
    use mailtide::mirror::{AccountMirror, Content};
    use mailtide::quota::Priority;
    use mailtide::schedule::TimerKind;
    use mailtide::state::SyncState;
    use mailtide::{Engine, EngineConfig};

    pub fn account() -> AccountId {
        AccountId("alice@example.com".into())
    }

    /// Job timing for commits a test writes itself: everything due at `now`.
    pub fn plan(priority: Priority, now: i64) -> JobPlan {
        let now = Timestamp(now);
        JobPlan {
            priority,
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
            job_plan: plan(Priority::Live, 0),
            jobs: Vec::new(),
            intents: Vec::new(),
        }
    }

    pub fn apply(mirror: &mut AccountMirror, commit: &Commit) {
        assert!(
            matches!(mirror.apply(commit), CommitOutcome::Applied { .. }),
            "seed commit applies"
        );
    }

    /// A store following history from `cursor`, owned by a current engine,
    /// whose label definitions were listed at time 0 (so no refresh is due
    /// until 15 minutes in).
    pub fn live_mirror(cursor: u64) -> AccountMirror {
        let mut mirror = AccountMirror::new(account(), 1);
        mirror.claim(ENGINE_VERSION).expect("current version");
        let mut c = commit(&mirror);
        c.sync = Some(SyncState::Live);
        c.cursor = Some(CursorMove {
            expected: None,
            to: HistoryId(cursor),
        });
        c.account_ops = vec![AccountOp::SetLabelsRefreshedAt(Timestamp(0))];
        apply(&mut mirror, &c);
        mirror
    }

    pub fn observation(id: &str, labels: &LabelSet, checkpoint: u64) -> MessageObservation {
        MessageObservation {
            id: MessageId(id.into()),
            thread_id: ThreadId(format!("thread-{id}")),
            labels: labels.clone(),
            checkpoint: HistoryId(checkpoint),
        }
    }

    pub fn observe(id: &str, labels: &LabelSet, checkpoint: u64) -> MessageOp {
        MessageOp::Observe(observation(id, labels, checkpoint))
    }

    pub fn imported(id: &str, labels: &LabelSet, checkpoint: u64) -> MessageOp {
        MessageOp::MarkImported {
            observation: observation(id, labels, checkpoint),
            blob: BlobRef(format!("blobs/{id}")),
            size: 1,
            internal_date: Timestamp(0),
            headers: Box::new(HeaderFacts::default()),
        }
    }

    /// Claim exactly `kind` (it must be the next due job) and finish it.
    pub fn claim_finish(
        mirror: &mut AccountMirror,
        kind: &JobKind,
        attempt: u64,
        outcome: JobOutcome,
        messages: Vec<MessageOp>,
    ) -> CommitOutcome {
        let claimed = mirror
            .claim_jobs(
                mirror.record.epoch,
                1,
                Timestamp(i64::MAX),
                AttemptId(attempt),
            )
            .expect("current epoch");
        assert_eq!(
            claimed
                .iter()
                .map(mailtide::jobs::ClaimedJob::kind)
                .collect::<Vec<_>>(),
            vec![kind.clone()]
        );
        let claim = claimed[0].claim();
        let mut c = commit(mirror);
        c.claim = Some(claim.clone());
        c.messages = messages;
        c.jobs = vec![JobOp::Finish { claim, outcome }];
        mirror.apply(&c)
    }

    pub fn import_jobs(mirror: &AccountMirror) -> Vec<MessageId> {
        mirror
            .jobs
            .keys()
            .filter_map(|kind| match kind {
                JobKind::Import(id) => Some(id.clone()),
                JobKind::Verify(_) | JobKind::BlobCleanup(_) | JobKind::Deliver(_) => None,
            })
            .collect()
    }

    pub fn message_ids(mirror: &AccountMirror) -> Vec<MessageId> {
        mirror.messages.keys().cloned().collect()
    }

    #[derive(Debug, PartialEq, Eq)]
    pub struct ImportProgress {
        pub imported: usize,
        pub remaining: usize,
    }

    /// Import progress as the store's rows tell it: stored content counts
    /// as imported, a row still waiting for content as remaining.
    pub fn progress(mirror: &AccountMirror) -> ImportProgress {
        let stored = mirror
            .messages
            .values()
            .filter(|r| matches!(r.content, Content::Imported(_)))
            .count();
        ImportProgress {
            imported: stored,
            remaining: mirror.messages.len() - stored,
        }
    }

    pub fn requests(gmail: &[(CommandId, GmailRequest)]) -> Vec<&GmailRequest> {
        gmail.iter().map(|(_, request)| request).collect()
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

        /// The one outstanding Gmail call, which must be `expected`.
        pub fn only_call(&mut self, expected: &GmailRequest) -> CommandId {
            let calls = self.take_gmail();
            assert_eq!(requests(&calls), vec![expected]);
            calls[0].0
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
                        next_due: self.mirror.next_due(),
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

fn m(id: &str) -> MessageId {
    MessageId(id.into())
}

fn history(start: u64) -> GmailRequest {
    GmailRequest::ListHistory {
        start: HistoryId(start),
        page_token: None,
    }
}

fn added(record: u64, id: &str) -> HistoryRecord {
    HistoryRecord::MessageAdded {
        id: HistoryId(record),
        message: MessageRef {
            id: m(id),
            thread_id: ThreadId(format!("thread-{id}")),
            labels: set(["INBOX"]),
        },
    }
}

fn page(records: Vec<HistoryRecord>, history_id: u64) -> HistoryPage {
    HistoryPage {
        records,
        history_id: HistoryId(history_id),
        next_page_token: None,
    }
}

fn job(id: &str, priority: Priority, state: JobState) -> Job {
    Job {
        kind: JobKind::Import(m(id)),
        priority,
        failed_attempts: 0,
        state,
    }
}

fn ready(due_at: i64) -> JobState {
    JobState::Ready {
        due_at: Timestamp(due_at),
    }
}

fn permanent() -> BlockingFailure {
    BlockingFailure {
        class: Blocking::Permanent,
        detail: "bad request".into(),
    }
}

fn transient() -> RetryableFailure {
    RetryableFailure {
        class: Retryable::Transient,
        retry_after: None,
        detail: "503".into(),
    }
}

/// Seed rows and jobs with a plain commit.
fn seed(mirror: &mut AccountMirror, priority: Priority, messages: Vec<MessageOp>) {
    let mut c = commit(mirror);
    c.job_plan = plan(priority, 0);
    c.messages = messages;
    apply(mirror, &c);
}

fn enqueue(mirror: &mut AccountMirror, kind: JobKind, priority: Priority, due_at: i64) {
    let mut c = commit(mirror);
    c.jobs = vec![JobOp::Enqueue(NewJob {
        kind,
        priority,
        due_at: Timestamp(due_at),
    })];
    apply(mirror, &c);
}

fn claim_ids(mirror: &mut AccountMirror, limit: u32, now: i64, attempt: u64) -> Vec<JobKind> {
    mirror
        .claim_jobs(
            mirror.record.epoch,
            limit,
            Timestamp(now),
            AttemptId(attempt),
        )
        .expect("current epoch")
        .into_iter()
        .map(|j| j.kind())
        .collect()
}

/// Zero: mail-history-store > durable Gmail history checkpoints > counts only unimported threads toward import progress, including blocked work but excluding verification duplicates and deletions
#[test]
fn durable_gmail_history_checkpoints_counts_only_unimported_threads_toward_import_progress_including_blocked_work_but_excluding_verification_duplicates_and_deletions()
 {
    let mut mirror = live_mirror(10);
    let inbox = set(["INBOX"]);
    // `older` is a historical import whose job gets blocked.
    seed(
        &mut mirror,
        Priority::Backfill,
        vec![observe("older", &inbox, 3)],
    );
    let outcome = claim_finish(
        &mut mirror,
        &JobKind::Import(m("older")),
        1,
        JobOutcome::Block(permanent()),
        Vec::new(),
    );
    assert!(matches!(outcome, CommitOutcome::Applied { .. }));
    // `stored` has content and a verification queued; `deleted` had content
    // and now has only a cleanup job; `live` just arrived.
    seed(
        &mut mirror,
        Priority::Live,
        vec![
            imported("stored", &inbox, 1),
            imported("deleted", &inbox, 1),
        ],
    );
    enqueue(&mut mirror, JobKind::Verify(m("stored")), Priority::Live, 0);
    seed(
        &mut mirror,
        Priority::Live,
        vec![
            MessageOp::Tombstone {
                id: m("deleted"),
                evidence: DeletionEvidence::History { at: HistoryId(4) },
            },
            observe("live", &inbox, 5),
        ],
    );
    assert!(
        mirror
            .jobs
            .contains_key(&JobKind::BlobCleanup(m("deleted")))
    );

    assert_eq!(
        progress(&mirror),
        ImportProgress {
            imported: 1,
            remaining: 2
        }
    );
    // Remaining work is exactly the import jobs, blocked ones included;
    // verifications and cleanups are not imports.
    assert_eq!(import_jobs(&mirror), vec![m("live"), m("older")]);
    assert!(matches!(
        mirror.jobs[&JobKind::Import(m("older"))].state,
        JobState::Blocked(_)
    ));

    seed(
        &mut mirror,
        Priority::Live,
        vec![imported("older", &inbox, 6)],
    );
    assert_eq!(
        progress(&mirror),
        ImportProgress {
            imported: 2,
            remaining: 1
        }
    );
}

/// Zero: mail-history-store > durable Gmail history checkpoints > pauses backfill without blocking newly arrived live work or counting a quota denial as a failed request
#[test]
fn durable_gmail_history_checkpoints_pauses_backfill_without_blocking_newly_arrived_live_work_or_counting_a_quota_denial_as_a_failed_request()
 {
    // The quota gate holds back historical work before live work.
    let budget = Budget {
        account_per_minute: 100,
        ..Budget::GMAIL_DEFAULT
    };
    let import = quota::cost(&GmailRequest::ImportMessage { id: m("old") });
    let mut ledger = Ledger::default();
    let who = account();
    assert!(matches!(
        ledger.admit(&budget, &who, import, Priority::Backfill, Timestamp(0)),
        Admission::Granted(_)
    ));
    assert!(matches!(
        ledger.admit(&budget, &who, import, Priority::Backfill, Timestamp(1)),
        Admission::Deferred { .. }
    ));
    let live_cost = quota::cost(&history(100));
    assert!(matches!(
        ledger.admit(&budget, &who, live_cost, Priority::Live, Timestamp(1)),
        Admission::Granted(_)
    ));

    // The engine parks a deferred import without counting a failure, and
    // live mail arriving meanwhile runs at once.
    let mut mirror = live_mirror(100);
    seed(
        &mut mirror,
        Priority::Backfill,
        vec![observe("old", &set(["INBOX"]), 50)],
    );
    let mut host = Host::new(mirror, 0);
    host.start();
    let pass = host.only_call(&history(100));
    host.fire(TimerKind::Jobs);
    let import_old = host.only_call(&GmailRequest::ImportMessage { id: m("old") });
    host.handle(EventKind::RequestDeferred {
        command: import_old,
        until: Timestamp(60_000),
    });
    let old = &host.mirror.jobs[&JobKind::Import(m("old"))];
    assert_eq!(old.failed_attempts, 0, "a quota denial is not a failure");
    assert_eq!(old.state, ready(60_000));
    assert_eq!(host.timers.get(&TimerKind::Jobs), Some(&Timestamp(60_000)));

    host.at(1_000).handle(EventKind::HistoryFetched {
        command: pass,
        page: page(vec![added(105, "live")], 105),
    });
    host.fire(TimerKind::Jobs);
    host.only_call(&GmailRequest::ImportMessage { id: m("live") });
    assert_eq!(
        host.mirror.jobs[&JobKind::Import(m("old"))].state,
        ready(60_000)
    );
}

/// Zero: mail-history-store > durable Gmail history checkpoints > rolls back both import jobs and the cursor when applying a history page fails
#[test]
fn durable_gmail_history_checkpoints_rolls_back_both_import_jobs_and_the_cursor_when_applying_a_history_page_fails()
 {
    let mut host = Host::new(live_mirror(10), 0);
    let before = host.mirror.seq;
    host.start();
    let pass = host.only_call(&history(10));
    // Take the page's commit before the store sees it.
    let commands = host.engine.handle(Event {
        now: Timestamp(0),
        kind: EventKind::HistoryFetched {
            command: pass,
            page: page(vec![added(15, "t")], 20),
        },
    });
    let page_commit = commands
        .into_iter()
        .find_map(|c| match c.kind {
            CommandKind::Commit(commit) => Some(*commit),
            _ => None,
        })
        .expect("the page is committed");
    assert_eq!(
        page_commit.cursor,
        Some(CursorMove {
            expected: Some(HistoryId(10)),
            to: HistoryId(20)
        })
    );
    assert!(!page_commit.messages.is_empty());
    // The store's transaction fails (here: a guard): none of it applies —
    // no row, no import job, no cursor move, no change-log entry.
    let mut failing = page_commit.clone();
    failing.cursor = Some(CursorMove {
        expected: Some(HistoryId(5)),
        to: HistoryId(20),
    });
    assert_eq!(
        host.mirror.apply(&failing),
        CommitOutcome::Rejected(Rejection::CursorMismatch)
    );
    assert_eq!(host.mirror.record.cursor, Some(HistoryId(10)));
    assert!(host.mirror.jobs.is_empty());
    assert!(host.mirror.messages.is_empty());
    assert_eq!(host.mirror.seq, before);
    // The engine doesn't adopt the cursor of a commit that didn't apply.
    host.handle(EventKind::CommitRejected {
        commit: page_commit.id,
        reason: Rejection::CursorMismatch,
    });
    assert_eq!(host.engine.status().cursor, Some(HistoryId(10)));
}

/// Zero: mail-history-store > durable Gmail history checkpoints > retains a notification arriving during a sync and coalesces duplicate thread jobs
#[test]
fn durable_gmail_history_checkpoints_retains_a_notification_arriving_during_a_sync_and_coalesces_duplicate_thread_jobs()
 {
    let mut host = Host::new(live_mirror(10), 0);
    host.start();
    let pass = host.only_call(&history(10));
    // Pushes during the pass don't start another one…
    for history_id in [20, 30] {
        host.handle(EventKind::PushReceived {
            history_id: HistoryId(history_id),
        });
    }
    assert!(host.take_gmail().is_empty());
    // …the page (read before the second push) mentions `t` three times…
    let starred = HistoryRecord::LabelsAdded {
        id: HistoryId(16),
        message_id: m("t"),
        labels: set(["STARRED"]),
    };
    host.handle(EventKind::HistoryFetched {
        command: pass,
        page: page(vec![added(15, "t"), starred, added(17, "t")], 20),
    });
    // …and the newer push is not lost: another pass follows from the new cursor.
    assert_eq!(host.mirror.record.cursor, Some(HistoryId(20)));
    host.only_call(&history(20));
    assert_eq!(
        host.mirror.jobs.keys().collect::<Vec<_>>(),
        vec![&JobKind::Import(m("t"))],
        "one job per message"
    );
    host.fire(TimerKind::Jobs);
    let import = host.only_call(&GmailRequest::ImportMessage { id: m("t") });
    host.handle(EventKind::MessageImported {
        command: import,
        message: Box::new(ImportedMessage {
            observation: common::observation("t", &set(["INBOX"]), 20),
            blob: BlobRef("blobs/t".into()),
            size: 1,
            internal_date: Timestamp(0),
            headers: HeaderFacts::default(),
        }),
    });
    assert!(host.mirror.jobs.is_empty());
}

/// Zero: mail-history-store > durable Gmail history checkpoints > does not delete a newly re-enqueued import when an older import finishes
#[test]
fn durable_gmail_history_checkpoints_does_not_delete_a_newly_re_enqueued_import_when_an_older_import_finishes()
 {
    let mut mirror = live_mirror(10);
    seed(
        &mut mirror,
        Priority::Live,
        vec![observe("t", &set(["INBOX"]), 11)],
    );
    let kind = JobKind::Import(m("t"));
    let old_epoch = mirror.record.epoch;
    assert_eq!(claim_ids(&mut mirror, 1, 0, 1), vec![kind.clone()]);
    let old_claim = JobClaim {
        kind: kind.clone(),
        attempt: AttemptId(1),
    };
    // A new engine takes over and the import is claimed again.
    mirror.claim(ENGINE_VERSION).expect("current version");
    assert_eq!(claim_ids(&mut mirror, 1, 0, 2), vec![kind.clone()]);

    // The older attempt finishing removes nothing, whether it is fenced by
    // its epoch or only by its claim.
    let mut late = commit(&mirror);
    late.epoch = old_epoch;
    late.claim = Some(old_claim.clone());
    late.jobs = vec![JobOp::Finish {
        claim: old_claim,
        outcome: JobOutcome::Done,
    }];
    assert_eq!(
        mirror.apply(&late),
        CommitOutcome::Rejected(Rejection::StaleEpoch)
    );
    late.epoch = mirror.record.epoch;
    assert_eq!(
        mirror.apply(&late),
        CommitOutcome::Rejected(Rejection::StaleClaim)
    );
    late.claim = None;
    assert!(matches!(
        mirror.apply(&late),
        CommitOutcome::Applied { skipped: 1, .. }
    ));
    assert_eq!(mirror.jobs[&kind].state, JobState::Running(AttemptId(2)));
}

/// Zero: mail-history-store > durable Gmail history checkpoints > resumes full reconciliation after restart and preserves signals received during it
#[test]
fn durable_gmail_history_checkpoints_resumes_full_reconciliation_after_restart_and_preserves_signals_received_during_it()
 {
    let mut mirror = live_mirror(50);
    let inbox = set(["INBOX"]);
    seed(
        &mut mirror,
        Priority::Live,
        vec![imported("keep", &inbox, 40), imported("gone", &inbox, 40)],
    );
    let mut host = Host::new(mirror, 0);
    host.start();
    let pass = host.only_call(&history(50));
    host.handle(EventKind::HistoryExpired { command: pass });
    let labels = host.only_call(&GmailRequest::ListLabels);
    host.handle(EventKind::LabelsListed {
        command: labels,
        labels: Vec::new(),
    });
    let profile = host.only_call(&GmailRequest::GetProfile);
    host.handle(EventKind::ProfileFetched {
        command: profile,
        history_id: HistoryId(100),
    });
    let list = host.only_call(&GmailRequest::ListMessages {
        scope: ListScope::All,
        page_token: None,
    });
    let page_2 = PageToken("page-2".into());
    host.handle(EventKind::MessagesListed {
        command: list,
        page: ListPage {
            messages: vec![(m("keep"), ThreadId("thread-keep".into()))],
            next_page_token: Some(page_2.clone()),
        },
    });
    let _ = host.take_gmail();
    host.handle(EventKind::PushReceived {
        history_id: HistoryId(120),
    });

    host.restart();
    let progress = host.mirror.record.sync.reconcile().expect("reconciling");
    assert_eq!(progress.listing(), Some((&ListScope::All, Some(&page_2))));
    let list = host.only_call(&GmailRequest::ListMessages {
        scope: ListScope::All,
        page_token: Some(page_2.clone()),
    });
    host.handle(EventKind::MessagesListed {
        command: list,
        page: ListPage {
            messages: vec![(m("new"), ThreadId("thread-new".into()))],
            next_page_token: None,
        },
    });
    assert_eq!(host.mirror.record.cursor, Some(HistoryId(100)));
    // Unlisted messages are verified, not deleted.
    assert_eq!(
        message_ids(&host.mirror),
        vec![m("gone"), m("keep"), m("new")]
    );
    assert!(host.mirror.jobs.contains_key(&JobKind::Verify(m("gone"))));
    // History is followed from the baseline right away, so the push that
    // arrived during reconciliation is acted on.
    host.only_call(&history(100));
}

/// Zero: mail-history-store > durable Gmail history checkpoints > does not let notification storms bypass a persisted retry cooldown
#[test]
fn durable_gmail_history_checkpoints_does_not_let_notification_storms_bypass_a_persisted_retry_cooldown()
 {
    let mut mirror = live_mirror(100);
    let mut c = commit(&mirror);
    c.account_ops = vec![AccountOp::SetCooldown(Cooldown {
        target: QuotaTarget::Account(account()),
        until: Timestamp(60_000),
    })];
    apply(&mut mirror, &c);
    let mut host = Host::new(mirror, 0);
    host.start();
    let none: Vec<&GmailRequest> = Vec::new();
    assert_eq!(
        requests(&host.take_gmail()),
        none,
        "start waits out the cooldown"
    );
    for (at, history_id) in [(1_000, 105), (2_000, 110), (3_000, 120)] {
        host.at(at).handle(EventKind::PushReceived {
            history_id: HistoryId(history_id),
        });
    }
    assert_eq!(requests(&host.take_gmail()), none, "pushes wait it out too");
    host.at(59_999).fire(TimerKind::Poll);
    assert_eq!(requests(&host.take_gmail()), none);
    host.at(60_000).fire(TimerKind::Poll);
    host.only_call(&history(100));
}

/// Zero: mail-history-store > durable Gmail history checkpoints > uses a longer polling interval when the mailbox is inactive
#[test]
fn durable_gmail_history_checkpoints_uses_a_longer_polling_interval_when_the_mailbox_is_inactive() {
    let minute = Duration::from_mins(1);
    let hour = Timestamp(3_600_000);
    let (active, idle, idle_after) = (minute, 15 * minute, 60 * minute);
    let who = account();
    let idle_poll = schedule::next_poll(&who, hour, None, active, idle, idle_after);
    assert!(idle_poll.since(hour) >= idle);
    let active_poll = schedule::next_poll(&who, hour, Some(hour), active, idle, idle_after);
    assert!(active_poll.since(hour) >= active && active_poll.since(hour) < idle);

    // An inactive mailbox polls at the idle interval…
    let mut host = Host::new(live_mirror(10), 3_600_000);
    host.start();
    assert!(host.timers[&TimerKind::Poll].since(hour) >= idle);
    // …and once the user acts, at the active one.
    host.handle(EventKind::ActionRequested {
        action: ActionRequest::Messages {
            ids: Vec::new(),
            action: Action::Archive,
        },
    });
    host.fire(TimerKind::Poll);
    let next = host.timers[&TimerKind::Poll].since(hour);
    assert!(next < idle, "polls {next:?} after user activity");
}

/// Zero: mail-history-store > durable Gmail history checkpoints > deduplicates browser cache misses without resetting a failed import cooldown
#[test]
fn durable_gmail_history_checkpoints_deduplicates_browser_cache_misses_without_resetting_a_failed_import_cooldown()
 {
    let mut mirror = live_mirror(10);
    seed(
        &mut mirror,
        Priority::Backfill,
        vec![observe("t", &set(["INBOX"]), 11)],
    );
    let kind = JobKind::Import(m("t"));
    let retry = JobOutcome::Retry {
        due_at: Timestamp(60_000),
    };
    let outcome = claim_finish(&mut mirror, &kind, 1, retry, Vec::new());
    assert!(matches!(outcome, CommitOutcome::Applied { .. }));
    // A user opens the message twice: the job is shared, not reset.
    enqueue(&mut mirror, kind.clone(), Priority::Foreground, 0);
    enqueue(&mut mirror, kind.clone(), Priority::Foreground, 0);
    assert_eq!(mirror.jobs.len(), 1);
    assert!(claim_ids(&mut mirror, 4, 59_999, 2).is_empty());
    let claimed = mirror
        .claim_jobs(mirror.record.epoch, 4, Timestamp(60_000), AttemptId(3))
        .expect("current epoch");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].kind(), kind);
    assert_eq!(claimed[0].failed_attempts, 1);
    assert_eq!(claimed[0].priority, Priority::Foreground);
}

/// Zero: mail-history-store > durable Gmail history checkpoints > delivers label notifications independently of a backfill for the same thread
#[test]
fn durable_gmail_history_checkpoints_delivers_label_notifications_independently_of_a_backfill_for_the_same_thread()
 {
    let mut mirror = live_mirror(10);
    seed(
        &mut mirror,
        Priority::Backfill,
        vec![observe("t", &set(["INBOX"]), 11)],
    );
    let import = mirror.jobs[&JobKind::Import(m("t"))].clone();
    let consumed = mirror.seq;
    seed(
        &mut mirror,
        Priority::Live,
        vec![MessageOp::PatchLabels {
            id: m("t"),
            add: set(["STARRED"]),
            remove: set([]),
            checkpoint: HistoryId(12),
        }],
    );
    // Clients learn of the label change before the body is imported…
    let changes = mirror.changes(consumed, 100);
    assert!(matches!(
        changes.changes.as_slice(),
        [entry] if matches!(&entry.change, Change::LabelsChanged { id, .. } if *id == m("t"))
    ));
    let view = mirror.view(&m("t")).expect("row");
    assert!(view.labels.contains(&LabelId("STARRED".into())));
    // …and acknowledging it leaves the import untouched.
    assert!(mirror.changes(changes.next, 100).changes.is_empty());
    assert_eq!(mirror.jobs[&JobKind::Import(m("t"))], import);
    assert_eq!(import.priority, Priority::Backfill);
}

/// Zero: mail-history-store > durable Gmail history checkpoints > prioritizes live changes and opened threads ahead of historical imports
#[test]
fn durable_gmail_history_checkpoints_prioritizes_live_changes_and_opened_threads_ahead_of_historical_imports()
 {
    let mut mirror = live_mirror(10);
    let inbox = set(["INBOX"]);
    seed(
        &mut mirror,
        Priority::Backfill,
        vec![observe("old", &inbox, 1), observe("opened", &inbox, 2)],
    );
    seed(
        &mut mirror,
        Priority::Live,
        vec![observe("changed", &inbox, 11)],
    );
    let outcome = claim_finish(
        &mut mirror,
        &JobKind::Import(m("changed")),
        1,
        JobOutcome::Done,
        vec![imported("changed", &inbox, 11)],
    );
    assert!(matches!(outcome, CommitOutcome::Applied { .. }));
    // The user opens `opened`: its import jumps the historical queue.
    enqueue(
        &mut mirror,
        JobKind::Import(m("opened")),
        Priority::Foreground,
        0,
    );
    assert_eq!(
        claim_ids(&mut mirror, 1, 0, 2),
        vec![JobKind::Import(m("opened"))]
    );
}

/// Zero: mail-history-store > durable Gmail history checkpoints > retains a newer notification when an earlier broadcast is acknowledged
#[test]
fn durable_gmail_history_checkpoints_retains_a_newer_notification_when_an_earlier_broadcast_is_acknowledged()
 {
    let mut mirror = live_mirror(10);
    let patch = |checkpoint: u64, label: &str| MessageOp::PatchLabels {
        id: m("t"),
        add: set([label]),
        remove: set([]),
        checkpoint: HistoryId(checkpoint),
    };
    seed(
        &mut mirror,
        Priority::Live,
        vec![imported("t", &set(["INBOX"]), 11)],
    );
    let start = mirror.seq;
    seed(&mut mirror, Priority::Live, vec![patch(12, "STARRED")]);
    let broadcast = mirror.changes(start, 100);
    assert!(matches!(
        broadcast.changes.as_slice(),
        [entry] if matches!(&entry.change, Change::LabelsChanged { id, .. } if *id == m("t"))
    ));
    seed(&mut mirror, Priority::Live, vec![patch(13, "IMPORTANT")]);
    // Acknowledging the earlier broadcast keeps the newer change.
    let after_ack = mirror.changes(broadcast.next, 100);
    assert!(matches!(
        after_ack.changes.as_slice(),
        [entry] if matches!(&entry.change, Change::LabelsChanged { id, .. } if *id == m("t"))
    ));
    assert!(after_ack.next > broadcast.next);
    // Three commits: the import (discovered + imported) and two label changes.
    let all = mirror.changes(StateSeq(0), 100).changes;
    let commits: std::collections::BTreeSet<_> = all.iter().map(|e| e.state).collect();
    assert_eq!((all.len(), commits.len()), (4, 3));
}

/// Zero: mail-history-store > durable Gmail history checkpoints > picks the earliest due job within the highest priority and skips blocked work
#[test]
fn durable_gmail_history_checkpoints_picks_the_earliest_due_job_within_the_highest_priority_and_skips_blocked_work()
 {
    let jobs = [
        job("late-live", Priority::Live, ready(500)),
        job("early-live", Priority::Live, ready(100)),
        job("backfill", Priority::Backfill, ready(0)),
        job("blocked", Priority::Live, JobState::Blocked(permanent())),
    ];
    let pick = |now: i64| pick_next(&jobs, Timestamp(now)).map(|job| job.kind.clone());
    assert_eq!(pick(1_000), Some(JobKind::Import(m("early-live"))));
    assert_eq!(pick(200), Some(JobKind::Import(m("early-live"))));
    assert_eq!(pick(50), Some(JobKind::Import(m("backfill"))));
}

/// Zero: mail-history-store > durable Gmail history checkpoints > reports the next due job or notification, delaying paused backfill
#[test]
fn durable_gmail_history_checkpoints_reports_the_next_due_job_or_notification_delaying_paused_backfill()
 {
    let mut mirror = live_mirror(100);
    seed(
        &mut mirror,
        Priority::Live,
        vec![observe("live", &set(["INBOX"]), 50)],
    );
    let mut host = Host::new(mirror, 0);
    host.start();
    let _ = host.take_gmail();
    host.fire(TimerKind::Jobs);
    let import = host.only_call(&GmailRequest::ImportMessage { id: m("live") });
    host.handle(EventKind::RequestFailed {
        command: import,
        failure: Failure::Retryable(transient()),
    });
    let JobState::Ready { due_at } = host.mirror.jobs[&JobKind::Import(m("live"))].state else {
        panic!("the import waits for its retry");
    };
    assert!(due_at > Timestamp(0));
    // The host is told to wake the engine when the job is due again.
    assert_eq!(host.timers.get(&TimerKind::Jobs), Some(&due_at));
}

/// Zero: mail-history-store > durable Gmail history checkpoints > uses a supplied thread count for import progress
#[test]
#[ignore = "zero-na: mailtide's import progress comes from its own discovered/imported counters; there is no host-supplied total to override it"]
fn durable_gmail_history_checkpoints_uses_a_supplied_thread_count_for_import_progress() {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-history-store > durable Gmail history checkpoints > AI runs > dispatches only after the import lands, and survives a restart
#[test]
#[ignore = "zero-na: Zero's AI thread workflow; not part of a sync engine"]
fn durable_gmail_history_checkpoints_ai_runs_dispatches_only_after_the_import_lands_and_survives_a_restart()
 {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-history-store > durable Gmail history checkpoints > AI runs > re-dispatches a lost run when its lease expires
#[test]
#[ignore = "zero-na: Zero's AI thread workflow; not part of a sync engine"]
fn durable_gmail_history_checkpoints_ai_runs_re_dispatches_a_lost_run_when_its_lease_expires() {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-history-store > durable Gmail history checkpoints > AI runs > keeps the row when newer mail arrives during a run
#[test]
#[ignore = "zero-na: Zero's AI thread workflow; not part of a sync engine"]
fn durable_gmail_history_checkpoints_ai_runs_keeps_the_row_when_newer_mail_arrives_during_a_run() {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-history-store > durable Gmail history checkpoints > AI runs > retries with backoff, never repeats an attempted draft, and gives up
#[test]
#[ignore = "zero-na: Zero's AI thread workflow; not part of a sync engine"]
fn durable_gmail_history_checkpoints_ai_runs_retries_with_backoff_never_repeats_an_attempted_draft_and_gives_up()
 {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-history-store > durable Gmail history checkpoints > AI runs > caps concurrent runs and waits for a lease instead of spinning
#[test]
#[ignore = "zero-na: Zero's AI thread workflow; not part of a sync engine"]
fn durable_gmail_history_checkpoints_ai_runs_caps_concurrent_runs_and_waits_for_a_lease_instead_of_spinning()
 {
    todo!("port from Zero 4b642dfd9890")
}
