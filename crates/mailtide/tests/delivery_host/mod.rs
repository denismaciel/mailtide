//! A host for engine tests of deliveries: it applies store commands to an
//! [`AccountMirror`] at once and leaves Gmail calls and timers to the test.

use std::collections::{BTreeMap, VecDeque};

use mailtide::command::{Command, CommandKind, GmailRequest};
use mailtide::commit::{AccountOp, Commit, CommitOutcome, CursorMove, JobPlan, MessageOp};
use mailtide::event::{Event, EventKind};
use mailtide::gmail::{HeaderFacts, HistoryPage, MessageObservation};
use mailtide::ids::{
    AccountId, BlobRef, CommandId, CommitId, ENGINE_VERSION, HistoryId, LabelId, MessageId,
    ThreadId, Timestamp,
};
use mailtide::labels::{LabelSet, set};
use mailtide::mirror::AccountMirror;
use mailtide::outbox::{Action, ActionRequest, IntentState, ModifyGroup};
use mailtide::quota::Priority;
use mailtide::schedule::TimerKind;
use mailtide::state::SyncState;
use mailtide::{Engine, EngineConfig};

pub fn m(id: &str) -> MessageId {
    MessageId(id.into())
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
        draft_refresh_due_at: Timestamp(0),
    }
}

fn commit(mirror: &AccountMirror) -> Commit {
    Commit {
        id: CommitId {
            epoch: mirror.record.epoch,
            seq: mirror.seq.0,
        },
        account: mirror.account.clone(),
        epoch: mirror.record.epoch,
        account_generation: mirror.record.generation,
        engine_version: ENGINE_VERSION,
        cursor: None,
        claim: None,
        sync: None,
        messages: Vec::new(),
        labels: Vec::new(),
        account_ops: Vec::new(),
        job_plan: plan(),
        jobs: Vec::new(),
        intents: Vec::new(),
        drafts: Vec::new(),
    }
}

/// A store following history from 10, holding `ids` imported with
/// `labels`, its label definitions listed at time 0.
pub fn imported_mirror(ids: &[&str], labels: &[&str]) -> AccountMirror {
    let mut mirror = AccountMirror::new(AccountId("alice@example.com".into()), 1);
    mirror.claim(ENGINE_VERSION).expect("current version");
    let mut c = commit(&mirror);
    c.sync = Some(SyncState::Live);
    c.cursor = Some(CursorMove {
        expected: None,
        to: HistoryId(10),
    });
    c.account_ops = vec![AccountOp::SetLabelsRefreshedAt(Timestamp(0))];
    c.messages = ids
        .iter()
        .map(|id| MessageOp::MarkImported {
            observation: MessageObservation {
                id: m(id),
                thread_id: ThreadId(format!("thread-{id}")),
                labels: set(labels.iter().copied()),
                checkpoint: HistoryId(5),
            },
            blob: BlobRef(format!("blobs/{id}")),
            size: 1,
            internal_date: Timestamp(0),
            headers: Box::new(HeaderFacts::default()),
            facts: None,
        })
        .collect();
    assert!(matches!(mirror.apply(&c), CommitOutcome::Applied { .. }));
    mirror
}

/// The modify calls among `calls`, with their command ids.
pub fn modifies(calls: Vec<(CommandId, GmailRequest)>) -> Vec<(CommandId, ModifyGroup)> {
    calls
        .into_iter()
        .filter_map(|(id, request)| match request {
            GmailRequest::Modify(group) => Some((id, group)),
            _ => None,
        })
        .collect()
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

impl Host {
    /// An engine on `mirror`, started at 0, its first history pass
    /// answered with nothing new.
    pub fn new(mut mirror: AccountMirror) -> Self {
        let snapshot = mirror.claim(ENGINE_VERSION).expect("current version");
        let engine = Engine::new(
            snapshot.validate().expect("valid snapshot"),
            EngineConfig::default(),
        );
        let mut host = Self {
            mirror,
            engine,
            now: Timestamp(0),
            timers: BTreeMap::new(),
            gmail: Vec::new(),
        };
        let commands = host.engine.start(host.now);
        host.pump(commands);
        let calls = host.take_gmail();
        assert!(matches!(calls[..], [(_, GmailRequest::ListHistory { .. })]));
        host.handle(EventKind::HistoryFetched {
            command: calls[0].0,
            page: HistoryPage {
                records: Vec::new(),
                history_id: HistoryId(10),
                next_page_token: None,
            },
        });
        host
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

    /// Move the clock from job or cooldown timer to the next, firing
    /// each, until a delivery goes out.
    pub fn advance_to_retry(&mut self) {
        while !self
            .gmail
            .iter()
            .any(|(_, request)| matches!(request, GmailRequest::Modify(_)))
        {
            let (timer, at) = [TimerKind::CooldownOver, TimerKind::Jobs]
                .into_iter()
                .filter_map(|timer| self.timers.get(&timer).map(|at| (timer, *at)))
                .min_by_key(|(_, at)| *at)
                .expect("a retry is scheduled");
            self.now = self.now.max(at);
            self.fire(timer);
        }
    }

    pub fn take_gmail(&mut self) -> Vec<(CommandId, GmailRequest)> {
        std::mem::take(&mut self.gmail)
    }

    /// The user acts on `ids`.
    pub fn act(&mut self, ids: &[&str], action: Action) {
        self.handle(EventKind::ActionRequested {
            action: ActionRequest::Messages {
                ids: ids.iter().map(|id| m(id)).collect(),
                action,
            },
        });
    }

    /// The one delivery sent since the last look, for `id`.
    pub fn delivery(&mut self, id: &str) -> (CommandId, ModifyGroup) {
        let calls = modifies(self.take_gmail());
        assert_eq!(calls.len(), 1, "one delivery: {calls:?}");
        assert_eq!(calls[0].1.messages, vec![m(id)]);
        calls[0].clone()
    }

    /// The intent on (`id`, `label`).
    pub fn intent(&self, id: &str, label: &str) -> IntentState {
        self.mirror.intents[&(m(id), LabelId(label.into()))]
            .state
            .clone()
    }

    /// The labels `id` shows, pending changes applied.
    pub fn effective(&self, id: &str) -> LabelSet {
        self.mirror.view(&m(id)).expect("row").labels
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
                    floor,
                    ..
                } => Some(EventKind::JobsClaimed {
                    command: id,
                    jobs: self
                        .mirror
                        .claim_jobs_from(epoch, limit, now, attempt, floor)
                        .unwrap_or_default(),
                    next_due: self.mirror.next_due(),
                }),
                CommandKind::DeleteBlob { .. } => Some(EventKind::BlobDeleted { command: id }),
                CommandKind::ReportStatus(_)
                | CommandKind::AdvanceAudit(_)
                | CommandKind::RunSubmissions { .. } => None,
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
