//! Label definitions are refreshed while an account is live (SPEC §6.6):
//! on start and periodically, decided from the persisted refresh time; soon
//! after an unknown label id, coalesced and throttled; quietly when nothing
//! changed; and never in the way of mail.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use mailtide::command::{Command, CommandKind, GmailCall, GmailRequest};
use mailtide::commit::{AccountOp, Commit, CommitOutcome, CursorMove, JobPlan, LabelOp};
use mailtide::event::{Event, EventKind};
use mailtide::gmail::{
    HeaderFacts, HistoryPage, HistoryRecord, ImportedMessage, Label, LabelKind, MessageObservation,
    MessageRef,
};
use mailtide::ids::{
    AccountId, BlobRef, CommandId, CommitId, ENGINE_VERSION, HistoryId, LabelId, MessageId,
    ThreadId, Timestamp,
};
use mailtide::jobs::{JobKind, JobOp, NewJob};
use mailtide::labels::set;
use mailtide::mirror::AccountMirror;
use mailtide::quota::{Priority, QuotaUnits};
use mailtide::reconcile::{ReconcileProgress, ReconcileReason};
use mailtide::retry::{Blocking, BlockingFailure, Failure, Retryable, RetryableFailure};
use mailtide::revision::Change;
use mailtide::schedule::{TimerKind, UNKNOWN_LABEL_GAP};
use mailtide::state::SyncState;
use mailtide::{Engine, EngineConfig};

const MINUTE: i64 = 60_000;
/// When the stored definitions were last listed.
const LISTED: i64 = 10 * MINUTE;
/// The default refresh interval.
const INTERVAL: i64 = 15 * MINUTE;

fn account() -> AccountId {
    AccountId("alice@example.com".into())
}

fn label(id: &str, name: &str) -> Label {
    Label::plain(
        LabelId(id.into()),
        name,
        if id.starts_with("Label_") {
            LabelKind::User
        } else {
            LabelKind::System
        },
    )
}

fn stored_labels() -> Vec<Label> {
    vec![
        label("INBOX", "INBOX"),
        label("UNREAD", "UNREAD"),
        label("Label_1", "Travel"),
    ]
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

/// A live account at cursor 100 whose definitions were listed at `LISTED`.
fn live_mirror() -> AccountMirror {
    let mut mirror = AccountMirror::new(account(), 1);
    mirror.claim(ENGINE_VERSION).expect("current version");
    let mut c = commit(&mirror);
    c.sync = Some(SyncState::Live);
    c.cursor = Some(CursorMove {
        expected: None,
        to: HistoryId(100),
    });
    c.labels = vec![LabelOp::ReplaceAll {
        labels: stored_labels(),
        generation: 0,
        at: Timestamp(0),
    }];
    c.account_ops = vec![AccountOp::SetLabelsRefreshedAt(Timestamp(LISTED))];
    assert!(matches!(mirror.apply(&c), CommitOutcome::Applied { .. }));
    mirror
}

/// A host running the engine against an in-memory store. Gmail calls and
/// timers wait for the test; store commands apply at once.
struct Host {
    mirror: AccountMirror,
    engine: Engine,
    now: Timestamp,
    timers: BTreeMap<TimerKind, Timestamp>,
    gmail: Vec<(CommandId, GmailCall)>,
    commits: Vec<Commit>,
}

impl Host {
    fn new(mut mirror: AccountMirror, now: i64) -> Self {
        let engine = build(&mut mirror);
        let mut host = Self {
            mirror,
            engine,
            now: Timestamp(now),
            timers: BTreeMap::new(),
            gmail: Vec::new(),
            commits: Vec::new(),
        };
        host.start();
        host
    }

    fn start(&mut self) {
        let commands = self.engine.start(self.now);
        self.pump(commands);
    }

    /// The host ends this run; a later one builds a new engine.
    fn restart(&mut self, now: i64) {
        self.now = Timestamp(now);
        self.engine = build(&mut self.mirror);
        self.timers.clear();
        self.gmail.clear();
        self.start();
    }

    fn at(&mut self, now: i64) -> &mut Self {
        self.now = Timestamp(now);
        self
    }

    fn handle(&mut self, kind: EventKind) {
        let commands = self.engine.handle(Event {
            now: self.now,
            kind,
        });
        self.pump(commands);
    }

    fn fire(&mut self, timer: TimerKind) {
        self.timers.remove(&timer);
        self.handle(EventKind::TimerFired { timer });
    }

    /// The outstanding `labels.list` calls.
    fn label_calls(&self) -> Vec<CommandId> {
        self.gmail
            .iter()
            .filter(|(_, call)| call.request == GmailRequest::ListLabels)
            .map(|(id, _)| *id)
            .collect()
    }

    /// The one outstanding `labels.list`, taken.
    fn take_label_call(&mut self) -> (CommandId, GmailCall) {
        let calls = self.label_calls();
        assert_eq!(calls.len(), 1, "one label listing: {:?}", self.gmail);
        let at = self
            .gmail
            .iter()
            .position(|(id, _)| *id == calls[0])
            .expect("listed");
        self.gmail.remove(at)
    }

    /// The outstanding history call, taken.
    fn take_history_call(&mut self) -> CommandId {
        let at = self
            .gmail
            .iter()
            .position(|(_, call)| matches!(call.request, GmailRequest::ListHistory { .. }))
            .expect("a history pass");
        self.gmail.remove(at).0
    }

    fn definitions_changes(&self) -> usize {
        self.mirror
            .log
            .iter()
            .filter(|(_, change)| *change == Change::LabelDefinitionsChanged)
            .count()
    }

    fn pump(&mut self, commands: Vec<Command>) {
        let mut queue: VecDeque<Command> = commands.into();
        while let Some(command) = queue.pop_front() {
            let id = command.id;
            let event = match command.kind {
                CommandKind::Gmail(call) => {
                    self.gmail.push((id, call));
                    None
                }
                CommandKind::Commit(commit) => {
                    let outcome = self.mirror.apply(&commit);
                    self.commits.push(*commit.clone());
                    Some(match outcome {
                        CommitOutcome::Applied { .. } => {
                            EventKind::CommitApplied { commit: commit.id }
                        }
                        CommitOutcome::Rejected(reason) => EventKind::CommitRejected {
                            commit: commit.id,
                            reason,
                        },
                    })
                }
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

fn build(mirror: &mut AccountMirror) -> Engine {
    let snapshot = mirror.claim(ENGINE_VERSION).expect("current version");
    Engine::new(
        snapshot.validate().expect("valid snapshot"),
        EngineConfig::default(),
    )
}

fn added(record: u64, id: &str, labels: &[&str]) -> HistoryRecord {
    HistoryRecord::MessageAdded {
        id: HistoryId(record),
        message: MessageRef {
            id: MessageId(id.into()),
            thread_id: ThreadId(format!("thread-{id}")),
            labels: set(labels.iter().copied()),
        },
    }
}

fn labelled(record: u64, id: &str, labels: &[&str]) -> HistoryRecord {
    HistoryRecord::LabelsAdded {
        id: HistoryId(record),
        message_id: MessageId(id.into()),
        labels: set(labels.iter().copied()),
    }
}

fn page(records: Vec<HistoryRecord>, history_id: u64) -> HistoryPage {
    HistoryPage {
        records,
        history_id: HistoryId(history_id),
        next_page_token: None,
    }
}

fn transient() -> Failure {
    Failure::Retryable(RetryableFailure {
        class: Retryable::Transient,
        retry_after: None,
        detail: "503".into(),
    })
}

#[test]
fn a_refresh_is_due_from_the_persisted_refresh_time_across_restarts() {
    // A host starting a fresh run every few minutes lists labels only once
    // they are `label_refresh` old.
    let mut host = Host::new(live_mirror(), LISTED + MINUTE);
    assert!(host.label_calls().is_empty(), "listed a minute ago");
    assert_eq!(
        host.timers.get(&TimerKind::Labels),
        Some(&Timestamp(LISTED + INTERVAL))
    );
    for minutes in [2, 5, 14] {
        host.restart(LISTED + minutes * MINUTE);
        assert!(host.label_calls().is_empty(), "{minutes} min: not due");
    }
    // An account idle for an hour refreshes on its next run, at once.
    host.restart(LISTED + 60 * MINUTE);
    let (call, request) = host.take_label_call();
    assert_eq!(request.priority, Priority::Live);
    assert_eq!(request.cost, QuotaUnits(1));
    let renamed = vec![
        label("INBOX", "INBOX"),
        label("UNREAD", "UNREAD"),
        label("Label_1", "Trips"),
        label("Label_2", "Receipts"),
    ];
    host.handle(EventKind::LabelsListed {
        command: call,
        labels: renamed.clone(),
        generation: 0,
    });
    assert_eq!(host.mirror.label_definitions(), {
        let mut sorted = renamed;
        mailtide::mirror::sort_labels(&mut sorted);
        sorted
    });
    let refreshed = LISTED + 60 * MINUTE;
    assert_eq!(
        host.mirror.record.labels_refreshed_at,
        Some(Timestamp(refreshed))
    );
    assert_eq!(host.definitions_changes(), 2, "the seed and the rename");
    assert_eq!(
        host.timers.get(&TimerKind::Labels),
        Some(&Timestamp(refreshed + INTERVAL))
    );
    // The next run right after doesn't list them again.
    host.restart(refreshed + MINUTE);
    assert!(host.label_calls().is_empty());
}

#[test]
fn a_long_running_engine_refreshes_when_its_timer_fires() {
    let mut host = Host::new(live_mirror(), LISTED + MINUTE);
    let due = host.timers[&TimerKind::Labels];
    host.at(due.0 - 1).fire(TimerKind::Labels);
    assert!(host.label_calls().is_empty(), "a timer firing early waits");
    assert_eq!(host.timers.get(&TimerKind::Labels), Some(&due));
    host.at(due.0).fire(TimerKind::Labels);
    let (call, _) = host.take_label_call();
    host.handle(EventKind::LabelsListed {
        command: call,
        labels: stored_labels(),
        generation: 0,
    });
    assert_eq!(host.mirror.record.labels_refreshed_at, Some(due));
}

#[test]
fn an_unchanged_listing_adds_no_feed_entry() {
    let mut host = Host::new(live_mirror(), LISTED + INTERVAL);
    let before = host.mirror.log.len();
    let seq = host.mirror.seq;
    let (call, _) = host.take_label_call();
    let mut reordered = stored_labels();
    reordered.reverse();
    host.handle(EventKind::LabelsListed {
        command: call,
        labels: reordered,
        generation: 0,
    });
    assert_eq!(
        host.mirror.log.len(),
        before,
        "nothing changed, nothing logged"
    );
    assert!(host.mirror.seq > seq, "the refresh time is still recorded");
    assert_eq!(
        host.mirror.record.labels_refreshed_at,
        Some(Timestamp(LISTED + INTERVAL))
    );
    assert_eq!(host.mirror.label_definitions(), {
        let mut sorted = stored_labels();
        mailtide::mirror::sort_labels(&mut sorted);
        sorted
    });
}

#[test]
fn reconciliation_listing_unchanged_labels_adds_no_feed_entry_either() {
    let mut host = Host::new(live_mirror(), LISTED + MINUTE);
    let pass = host.take_history_call();
    host.handle(EventKind::HistoryExpired { command: pass });
    let before = host.definitions_changes();
    let (call, request) = host.take_label_call();
    assert_eq!(
        request.priority,
        Priority::Backfill,
        "the reconciliation's own"
    );
    host.handle(EventKind::LabelsListed {
        command: call,
        labels: stored_labels(),
        generation: 0,
    });
    assert_eq!(host.definitions_changes(), before);
    assert_eq!(
        host.mirror.record.labels_refreshed_at,
        Some(Timestamp(LISTED + MINUTE)),
        "a reconciliation's listing counts as a refresh"
    );
    assert_eq!(
        host.timers.get(&TimerKind::Labels),
        Some(&Timestamp(LISTED + MINUTE + INTERVAL))
    );
}

#[test]
fn a_bootstrap_lists_labels_once() {
    let mut mirror = AccountMirror::new(account(), 1);
    assert!(matches!(
        mirror.record.sync,
        SyncState::Bootstrapping(ReconcileProgress {
            reason: ReconcileReason::Bootstrap,
            ..
        })
    ));
    mirror.claim(ENGINE_VERSION).expect("current version");
    let host = Host::new(mirror, 0);
    assert_eq!(
        host.label_calls().len(),
        1,
        "the reconciliation's listing counts"
    );
}

#[test]
fn unknown_label_ids_coalesce_into_one_throttled_refresh() {
    let start = LISTED + 10_000;
    let mut host = Host::new(live_mirror(), start);
    let pass = host.take_history_call();
    // Two new labels on three records, ten seconds after the last listing:
    // one refresh, once the gap since that listing has passed.
    host.handle(EventKind::HistoryFetched {
        command: pass,
        page: page(
            vec![
                added(101, "a", &["INBOX", "Label_7"]),
                added(102, "b", &["INBOX", "Label_8"]),
                labelled(103, "a", &["Label_8"]),
            ],
            103,
        ),
    });
    assert_eq!(host.mirror.record.cursor, Some(HistoryId(103)));
    assert!(host.label_calls().is_empty(), "throttled");
    let soon = Timestamp(LISTED).plus(UNKNOWN_LABEL_GAP);
    assert_eq!(host.timers.get(&TimerKind::Labels), Some(&soon));
    host.at(start + 1_000).handle(EventKind::PushReceived {
        history_id: HistoryId(104),
    });
    let pass = host.take_history_call();
    host.handle(EventKind::HistoryFetched {
        command: pass,
        page: page(vec![labelled(104, "b", &["Label_7", "Label_9"])], 104),
    });
    assert!(host.label_calls().is_empty(), "coalesced");
    assert_eq!(host.timers.get(&TimerKind::Labels), Some(&soon));
    host.at(soon.0).fire(TimerKind::Labels);
    let (call, request) = host.take_label_call();
    assert_eq!(request.priority, Priority::Live);
    // Gmail lists two of them; it never lists `Label_9`.
    let mut listed = stored_labels();
    listed.extend([label("Label_7", "Seven"), label("Label_8", "Eight")]);
    host.handle(EventKind::LabelsListed {
        command: call,
        labels: listed,
        generation: 0,
    });
    assert_eq!(
        host.timers.get(&TimerKind::Labels),
        Some(&soon.plus(Duration::from_mins(15))),
        "back to the periodic refresh"
    );
    // `Label_9` keeps appearing: no refresh loop.
    for (n, at) in [(105_u64, 2), (106, 5), (107, 9)] {
        host.at(LISTED + at * MINUTE)
            .handle(EventKind::PushReceived {
                history_id: HistoryId(n),
            });
        let pass = host.take_history_call();
        host.handle(EventKind::HistoryFetched {
            command: pass,
            page: page(vec![labelled(n, "a", &["Label_9"])], n),
        });
        assert!(host.label_calls().is_empty(), "minute {at}");
    }
    assert_eq!(
        host.timers.get(&TimerKind::Labels),
        Some(&soon.plus(Duration::from_mins(15)))
    );
    // A new engine (a later run) asks once more, throttled from the last
    // persisted listing, then not again.
    host.restart(LISTED + 10 * MINUTE);
    let pass = host.take_history_call();
    host.handle(EventKind::HistoryFetched {
        command: pass,
        page: page(vec![labelled(108, "b", &["Label_9"])], 108),
    });
    let (call, _) = host.take_label_call();
    let mut listed = stored_labels();
    listed.extend([label("Label_7", "Seven"), label("Label_8", "Eight")]);
    host.handle(EventKind::LabelsListed {
        command: call,
        labels: listed,
        generation: 0,
    });
    host.at(LISTED + 11 * MINUTE)
        .handle(EventKind::PushReceived {
            history_id: HistoryId(109),
        });
    let pass = host.take_history_call();
    host.handle(EventKind::HistoryFetched {
        command: pass,
        page: page(vec![labelled(109, "b", &["Label_9"])], 109),
    });
    assert!(host.label_calls().is_empty());
}

#[test]
fn an_unknown_label_long_after_the_last_listing_refreshes_at_once() {
    let mut host = Host::new(live_mirror(), LISTED + 5 * MINUTE);
    let pass = host.take_history_call();
    host.handle(EventKind::HistoryFetched {
        command: pass,
        page: page(vec![added(101, "a", &["INBOX", "Label_7"])], 101),
    });
    host.take_label_call();
}

#[test]
fn an_imported_message_with_an_unknown_label_asks_for_a_refresh() {
    let mut mirror = live_mirror();
    let mut c = commit(&mirror);
    c.jobs = vec![JobOp::Enqueue(NewJob {
        kind: JobKind::Import(MessageId("m".into())),
        priority: Priority::Live,
        due_at: Timestamp(0),
    })];
    assert!(matches!(mirror.apply(&c), CommitOutcome::Applied { .. }));
    let mut host = Host::new(mirror, LISTED + 5 * MINUTE);
    host.fire(TimerKind::Jobs);
    let import = host
        .gmail
        .iter()
        .find(|(_, call)| matches!(call.request, GmailRequest::ImportMessage { .. }))
        .expect("the import runs")
        .0;
    host.handle(EventKind::MessageImported {
        command: import,
        message: Box::new(ImportedMessage {
            observation: MessageObservation {
                id: MessageId("m".into()),
                thread_id: ThreadId("t".into()),
                labels: set(["INBOX", "Label_new"]),
                checkpoint: HistoryId(100),
            },
            blob: BlobRef("blobs/m".into()),
            size: 1,
            internal_date: Timestamp(0),
            headers: HeaderFacts::default(),
            facts: None,
        }),
    });
    host.take_label_call();
}

#[test]
fn a_failed_refresh_neither_blocks_sync_nor_moves_the_cursor() {
    let start = LISTED + INTERVAL;
    let mut host = Host::new(live_mirror(), start);
    // The refresh and the history pass go out together.
    let (labels, _) = host.take_label_call();
    let pass = host.take_history_call();
    host.handle(EventKind::RequestFailed {
        command: labels,
        failure: transient(),
    });
    assert_eq!(host.engine.stopped(), None);
    let retry = host.timers[&TimerKind::Labels];
    assert!(retry > Timestamp(start), "backs off");
    assert!(
        retry < Timestamp(start + MINUTE),
        "with the engine's backoff"
    );
    // History goes on meanwhile.
    host.handle(EventKind::HistoryFetched {
        command: pass,
        page: page(vec![added(101, "a", &["INBOX"])], 101),
    });
    assert_eq!(host.mirror.record.cursor, Some(HistoryId(101)));
    // A deferral by the quota gate waits as long as it says.
    host.at(retry.0).fire(TimerKind::Labels);
    let (labels, _) = host.take_label_call();
    host.handle(EventKind::RequestDeferred {
        command: labels,
        until: Timestamp(start + 2 * MINUTE),
    });
    assert_eq!(
        host.timers.get(&TimerKind::Labels),
        Some(&Timestamp(start + 2 * MINUTE))
    );
    // A permanent refusal blocks nothing either: the next periodic refresh
    // tries again.
    host.at(start + 2 * MINUTE).fire(TimerKind::Labels);
    let (labels, _) = host.take_label_call();
    host.handle(EventKind::RequestFailed {
        command: labels,
        failure: Failure::Blocking(BlockingFailure {
            class: Blocking::Permanent,
            detail: "400".into(),
        }),
    });
    assert_eq!(host.engine.stopped(), None);
    assert_eq!(
        host.timers.get(&TimerKind::Labels),
        Some(&Timestamp(start + 2 * MINUTE + INTERVAL))
    );
    host.at(start + 2 * MINUTE + INTERVAL)
        .fire(TimerKind::Labels);
    let (labels, _) = host.take_label_call();
    host.handle(EventKind::LabelsListed {
        command: labels,
        labels: stored_labels(),
        generation: 0,
    });
    // No label commit ever touched the cursor.
    for commit in &host.commits {
        if !commit.labels.is_empty() {
            assert_eq!(commit.cursor, None);
            assert_eq!(commit.sync, None);
        }
    }
    assert_eq!(host.mirror.record.cursor, Some(HistoryId(101)));
    assert_eq!(host.mirror.record.sync, SyncState::Live);
}

#[test]
fn a_refresh_in_flight_does_not_hold_back_history() {
    let mut host = Host::new(live_mirror(), LISTED + INTERVAL);
    let (_labels, _) = host.take_label_call();
    let pass = host.take_history_call();
    host.handle(EventKind::HistoryFetched {
        command: pass,
        page: page(vec![added(101, "a", &["INBOX"])], 101),
    });
    host.handle(EventKind::PushReceived {
        history_id: HistoryId(102),
    });
    host.take_history_call();
    assert_eq!(host.mirror.record.cursor, Some(HistoryId(101)));
}

#[test]
fn an_unknown_label_seen_during_a_listing_is_asked_about_once_more() {
    let start = LISTED + INTERVAL;
    let mut host = Host::new(live_mirror(), start);
    let (labels, _) = host.take_label_call();
    let pass = host.take_history_call();
    // The id may postdate the listing in flight.
    host.handle(EventKind::HistoryFetched {
        command: pass,
        page: page(vec![added(101, "a", &["INBOX", "Label_7"])], 101),
    });
    assert!(host.label_calls().is_empty(), "one listing at a time");
    host.handle(EventKind::LabelsListed {
        command: labels,
        labels: stored_labels(),
        generation: 0,
    });
    let again = Timestamp(start).plus(UNKNOWN_LABEL_GAP);
    assert_eq!(host.timers.get(&TimerKind::Labels), Some(&again));
    host.at(again.0).fire(TimerKind::Labels);
    host.take_label_call();
}

#[test]
fn a_cooldown_holds_the_refresh_until_it_ends() {
    let mut mirror = live_mirror();
    let mut c = commit(&mirror);
    let until = Timestamp(LISTED + INTERVAL + MINUTE);
    c.account_ops = vec![AccountOp::SetCooldown(mailtide::retry::Cooldown {
        target: mailtide::retry::QuotaTarget::Account(account()),
        until,
    })];
    assert!(matches!(mirror.apply(&c), CommitOutcome::Applied { .. }));
    let mut host = Host::new(mirror, LISTED + INTERVAL);
    assert!(host.label_calls().is_empty(), "Gmail asked us to slow down");
    host.at(until.0).fire(TimerKind::CooldownOver);
    host.take_label_call();
}
