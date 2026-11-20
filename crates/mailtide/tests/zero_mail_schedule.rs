//! Tests ported by name from Zero's `mail-schedule.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.
//!
//! Zero repairs its Durable Object alarms so each callback keeps exactly one
//! future continuation. mailtide's engine has one timer per [`TimerKind`]:
//! every `Schedule` replaces that kind's pending instance and leaves the
//! others alone, and there is no cancel. The tests drive the engine against
//! a host that keeps one deadline per kind, and check the commands it emits.

use std::collections::BTreeMap;

use mailtide::command::{Command, CommandKind, GmailRequest};
use mailtide::commit::{Commit, CommitOutcome, CursorMove, JobPlan, MessageOp};
use mailtide::event::{Event, EventKind};
use mailtide::gmail::MessageObservation;
use mailtide::ids::{
    AccountId, CommandId, CommitId, ENGINE_VERSION, HistoryId, MessageId, ThreadId, Timestamp,
};
use mailtide::labels::set;
use mailtide::mirror::AccountMirror;
use mailtide::quota::Priority;
use mailtide::retry::{Failure, Retryable, RetryableFailure};
use mailtide::schedule::TimerKind;
use mailtide::state::SyncState;
use mailtide::{Engine, EngineConfig};

/// A host keeping one deadline per timer kind; store commands are applied
/// to the mirror at once, Gmail calls are collected for the test.
struct Host {
    mirror: AccountMirror,
    engine: Engine,
    now: Timestamp,
    timers: BTreeMap<TimerKind, Timestamp>,
    gmail: Vec<(CommandId, GmailRequest)>,
}

impl Host {
    fn new(mut mirror: AccountMirror) -> Self {
        let snapshot = mirror.claim(ENGINE_VERSION).expect("current version");
        let engine = Engine::new(
            snapshot.validate().expect("valid snapshot"),
            EngineConfig::default(),
        );
        Self {
            mirror,
            engine,
            now: Timestamp(0),
            timers: BTreeMap::new(),
            gmail: Vec::new(),
        }
    }

    fn start(&mut self) {
        let commands = self.engine.start(self.now);
        self.pump(commands);
    }

    /// Handle one event; returns the schedules it emitted directly.
    fn handle(&mut self, now: i64, kind: EventKind) -> Vec<(TimerKind, Timestamp)> {
        self.now = Timestamp(now);
        let commands = self.engine.handle(Event {
            now: self.now,
            kind,
        });
        let schedules = commands
            .iter()
            .filter_map(|c| match c.kind {
                CommandKind::Schedule { timer, at } => Some((timer, at)),
                _ => None,
            })
            .collect();
        self.pump(commands);
        schedules
    }

    fn fire(&mut self, now: i64, timer: TimerKind) -> Vec<(TimerKind, Timestamp)> {
        self.timers.remove(&timer);
        self.handle(now, EventKind::TimerFired { timer })
    }

    fn take_gmail(&mut self) -> Vec<(CommandId, GmailRequest)> {
        std::mem::take(&mut self.gmail)
    }

    fn pump(&mut self, commands: Vec<Command>) {
        let mut queue: std::collections::VecDeque<Command> = commands.into();
        while let Some(command) = queue.pop_front() {
            let id = command.id;
            let event = match command.kind {
                CommandKind::Gmail(call) => {
                    self.gmail.push((id, call.request));
                    None
                }
                CommandKind::Commit(commit) => Some(match self.mirror.apply(&commit) {
                    CommitOutcome::Applied { .. } => EventKind::CommitApplied { commit: commit.id },
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
                CommandKind::ReportStatus(_) | CommandKind::AdvanceAudit(_) => None,
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

fn commit(mirror: &AccountMirror) -> Commit {
    let now = Timestamp(0);
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
        job_plan: JobPlan {
            priority: Priority::Live,
            import_due_at: now,
            draft_import_due_at: now,
            verify_due_at: now,
            cleanup_due_at: now,
            deliver_due_at: now,
            confirm_due_at: now,
            draft_refresh_due_at: now,
        },
        jobs: Vec::new(),
        intents: Vec::new(),
        drafts: Vec::new(),
    }
}

/// A store following history from `cursor`, with `pending` messages to import.
fn live_mirror(cursor: u64, pending: &[&str]) -> AccountMirror {
    let mut mirror = AccountMirror::new(AccountId("alice@example.com".into()), 1);
    mirror.claim(ENGINE_VERSION).expect("current version");
    let mut c = commit(&mirror);
    c.sync = Some(SyncState::Live);
    c.cursor = Some(CursorMove {
        expected: None,
        to: HistoryId(cursor),
    });
    c.messages = pending
        .iter()
        .map(|id| {
            MessageOp::Observe(MessageObservation {
                id: MessageId((*id).into()),
                thread_id: ThreadId(format!("thread-{id}")),
                labels: set(["INBOX"]),
                checkpoint: HistoryId(cursor),
            })
        })
        .collect();
    assert!(matches!(mirror.apply(&c), CommitOutcome::Applied { .. }));
    mirror
}

fn history_calls(gmail: &[(CommandId, GmailRequest)]) -> Vec<CommandId> {
    gmail
        .iter()
        .filter(|(_, request)| matches!(request, GmailRequest::ListHistory { .. }))
        .map(|(id, _)| *id)
        .collect()
}

/// Zero: mail-schedule > mailbox continuation repair > coalesces repeated cron wakeups and replaces obsolete retry deadlines
#[test]
fn mailbox_continuation_repair_coalesces_repeated_cron_wakeups_and_replaces_obsolete_retry_deadlines()
 {
    let mut host = Host::new(live_mirror(100, &[]));
    host.start();
    let renewal = host.timers[&TimerKind::WatchRenewal];
    let pass = history_calls(&host.take_gmail());
    assert_eq!(pass.len(), 1);
    // Repeated wakeups while the pass runs: one continuation each, no new pass.
    for tick in 1..=5 {
        let schedules = host.fire(tick * 1_000, TimerKind::Poll);
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].0, TimerKind::Poll);
        assert!(host.take_gmail().is_empty());
    }
    assert_eq!(
        host.timers
            .keys()
            .filter(|t| **t == TimerKind::Poll)
            .count(),
        1
    );

    // A failure sets a retry deadline for the same continuation…
    host.handle(
        6_000,
        EventKind::RequestFailed {
            command: pass[0],
            failure: Failure::Retryable(RetryableFailure {
                class: Retryable::Transient,
                retry_after: None,
                detail: "503".into(),
            }),
        },
    );
    let retry_at = host.timers[&TimerKind::Poll];
    assert!(retry_at > Timestamp(6_000) && retry_at < Timestamp(60_000));
    // …and a deferral moves it rather than adding another.
    host.fire(retry_at.0, TimerKind::Poll);
    let deferred = history_calls(&host.take_gmail());
    assert_eq!(deferred.len(), 1);
    host.handle(
        retry_at.0,
        EventKind::RequestDeferred {
            command: deferred[0],
            until: Timestamp(120_000),
        },
    );
    assert_eq!(host.timers.get(&TimerKind::Poll), Some(&Timestamp(120_000)));
    // The independent watch renewal was never touched.
    assert_eq!(host.timers.get(&TimerKind::WatchRenewal), Some(&renewal));
}

/// Zero: mail-schedule > mailbox continuation repair > keeps independent callback schedules and leaves executing callbacks alone
#[test]
fn mailbox_continuation_repair_keeps_independent_callback_schedules_and_leaves_executing_callbacks_alone()
 {
    let mut host = Host::new(live_mirror(100, &["t"]));
    host.start();
    let _ = host.take_gmail();
    let before = host.timers.clone();
    assert!(before.contains_key(&TimerKind::Poll));
    assert!(before.contains_key(&TimerKind::WatchRenewal));

    // The Jobs callback runs; its work is deferred to 3s.
    let schedules = host.fire(1_000, TimerKind::Jobs);
    assert!(
        schedules.is_empty(),
        "running a callback reschedules nothing else"
    );
    let import = host
        .take_gmail()
        .into_iter()
        .find(|(_, r)| matches!(r, GmailRequest::ImportMessage { .. }))
        .expect("the import runs")
        .0;
    let schedules = host.handle(
        1_000,
        EventKind::RequestDeferred {
            command: import,
            until: Timestamp(3_000),
        },
    );
    assert_eq!(schedules, vec![(TimerKind::Jobs, Timestamp(3_000))]);
    // Only that callback's continuation was created; the others are intact.
    assert_eq!(host.timers.get(&TimerKind::Jobs), Some(&Timestamp(3_000)));
    for timer in [TimerKind::Poll, TimerKind::WatchRenewal] {
        assert_eq!(host.timers.get(&timer), before.get(&timer));
    }
}
