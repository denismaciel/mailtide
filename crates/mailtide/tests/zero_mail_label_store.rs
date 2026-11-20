//! Tests ported by name from Zero's `mail-label-store.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.
//!
//! Zero's `MailLabelStore` maps onto mailtide's [`AccountMirror`]: imports and
//! provider deltas are `Observe`/`PatchLabels` commits, `setThreadLabels` and
//! friends are `IntentOp::Request`s, `pendingBatches` is a claim of `Deliver`
//! jobs, `acknowledge` is `IntentOp::Delivered`, and reopening the store is a
//! new engine claiming the account.

use mailtide::command::CommandKind;
use mailtide::commit::{AccountOp, Commit, CommitOutcome, DeletionEvidence, JobPlan, MessageOp};
use mailtide::event::{Event, EventKind};
use mailtide::gmail::MessageObservation;
use mailtide::ids::{
    AccountId, CommitId, ENGINE_VERSION, HistoryId, LabelId, MessageId, ThreadId, Timestamp,
};
use mailtide::jobs::{AttemptId, ClaimedJob, JobClaim, JobKind, JobOp, JobOutcome};
use mailtide::labels::{self, LabelSet, set};
use mailtide::mirror::{AccountMirror, MessageView};
use mailtide::outbox::{
    Action, ActionRequest, Importance, IntentOp, IntentState, LabelChange, LabelDelta, ModifyGroup,
    ReadState, RestoreDestination, StarState, plan_delivery,
};
use mailtide::quota::Priority;
use mailtide::reconcile::ListScope;
use mailtide::retry::{Cooldown, QuotaTarget};
use mailtide::schedule::TimerKind;
use mailtide::state::StoreSnapshot;
use mailtide::{Command, Engine, EngineConfig};

const ACCOUNT: &str = "zero@example.com";

fn m(id: &str) -> MessageId {
    MessageId(id.into())
}

fn t(id: &str) -> ThreadId {
    ThreadId(id.into())
}

fn ids(labels: &[&str]) -> Vec<LabelId> {
    labels.iter().map(|id| LabelId((*id).into())).collect()
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

fn observation(thread: &str, id: &str, labels: &[&str], checkpoint: u64) -> MessageOp {
    MessageOp::Observe(MessageObservation {
        id: m(id),
        thread_id: t(thread),
        labels: set(labels.iter().copied()),
        checkpoint: HistoryId(checkpoint),
    })
}

fn change_labels(add: &[&str], remove: &[&str]) -> Action {
    Action::ChangeLabels(LabelDelta::new(ids(add), ids(remove)).expect("a valid change"))
}

fn has(labels: &LabelSet, id: &str) -> bool {
    labels::has(labels, id)
}

fn applied(outcome: &CommitOutcome) -> bool {
    matches!(outcome, CommitOutcome::Applied { .. })
}

/// One account's store, driven the way the engine drives it.
struct Mailbox {
    mirror: AccountMirror,
    snapshot: StoreSnapshot,
    seq: u64,
    attempt: u64,
}

impl Mailbox {
    fn new() -> Self {
        let mut mirror = AccountMirror::new(AccountId(ACCOUNT.into()), 1);
        let snapshot = mirror.claim(ENGINE_VERSION).expect("claim");
        Self {
            mirror,
            snapshot,
            seq: 0,
            attempt: 0,
        }
    }

    /// A new engine takes over the stored state (Zero's `reopen()`).
    fn restart(&mut self) {
        self.snapshot = self.mirror.claim(ENGINE_VERSION).expect("reclaim");
    }

    fn commit(&mut self) -> Commit {
        self.seq += 1;
        Commit {
            id: CommitId {
                epoch: self.snapshot.epoch,
                seq: self.seq,
            },
            account: self.snapshot.account.clone(),
            epoch: self.snapshot.epoch,
            engine_version: self.snapshot.engine_version,
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

    fn apply(&mut self, commit: &Commit) {
        let outcome = self.mirror.apply(commit);
        assert!(applied(&outcome), "commit must apply: {outcome:?}");
    }

    fn messages(&mut self, ops: Vec<MessageOp>) {
        let mut commit = self.commit();
        commit.messages = ops;
        self.apply(&commit);
    }

    fn observe(&mut self, thread: &str, id: &str, labels: &[&str], checkpoint: u64) {
        self.messages(vec![observation(thread, id, labels, checkpoint)]);
    }

    fn patch(&mut self, id: &str, add: &[&str], remove: &[&str], checkpoint: u64) {
        self.messages(vec![MessageOp::PatchLabels {
            id: m(id),
            add: set(add.iter().copied()),
            remove: set(remove.iter().copied()),
            checkpoint: HistoryId(checkpoint),
        }]);
    }

    fn request(&mut self, request: ActionRequest) {
        let mut commit = self.commit();
        commit.intents = vec![IntentOp::Request(request)];
        self.apply(&commit);
    }

    fn act(&mut self, messages: &[&str], action: Action) {
        self.request(ActionRequest::Messages {
            ids: messages.iter().map(|id| m(id)).collect(),
            action,
        });
    }

    fn act_thread(&mut self, thread: &str, action: Action) {
        self.request(ActionRequest::Thread {
            id: t(thread),
            action,
        });
    }

    /// Claim every due job; keep those `wanted`, put the rest back.
    fn claim(&mut self, now: i64, wanted: impl Fn(&JobKind) -> bool) -> Vec<ClaimedJob> {
        self.attempt += 1;
        let claimed = self
            .mirror
            .claim_jobs(
                self.snapshot.epoch,
                100_000,
                Timestamp(now),
                AttemptId(self.attempt),
            )
            .expect("claim jobs");
        let (keep, release): (Vec<_>, Vec<_>) =
            claimed.into_iter().partition(|job| wanted(&job.kind()));
        if !release.is_empty() {
            let mut commit = self.commit();
            commit.jobs = release
                .iter()
                .map(|job| JobOp::Finish {
                    claim: job.claim(),
                    outcome: JobOutcome::Defer {
                        due_at: Timestamp(now),
                        at: Timestamp(now),
                        priority: job.priority,
                    },
                })
                .collect();
            self.apply(&commit);
        }
        keep
    }

    /// Claim the due deliveries (Zero's `pendingBatches`).
    fn deliveries(&mut self, now: i64) -> Vec<ClaimedJob> {
        self.claim(now, |kind| matches!(kind, JobKind::Deliver(_)))
    }

    /// The label changes that would go out at `now`, without claiming them.
    fn peek(&self, now: i64) -> Vec<ModifyGroup> {
        let mut probe = Self {
            mirror: self.mirror.clone(),
            snapshot: self.snapshot.clone(),
            seq: self.seq,
            attempt: self.attempt,
        };
        probe
            .deliveries(now)
            .into_iter()
            .filter_map(|job| job.group().cloned())
            .collect()
    }

    /// Gmail accepted a delivery (Zero's `acknowledge`).
    fn ack(&mut self, job: &ClaimedJob, at: i64) -> CommitOutcome {
        let mut commit = self.commit();
        commit.claim = Some(job.claim());
        commit.jobs = vec![JobOp::Finish {
            claim: job.claim(),
            outcome: JobOutcome::Done,
        }];
        commit.intents = vec![IntentOp::Delivered {
            versions: job
                .group()
                .as_ref()
                .map(|group| group.versions.clone())
                .unwrap_or_default(),
            at: Timestamp(at),
            by: job.attempt,
        }];
        self.mirror.apply(&commit)
    }

    /// A delivery failed transiently; retry at `due_at` (Zero's `defer`).
    fn retry(&mut self, job: &ClaimedJob, due_at: i64) {
        let mut commit = self.commit();
        commit.claim = Some(job.claim());
        commit.jobs = vec![JobOp::Finish {
            claim: job.claim(),
            outcome: JobOutcome::Retry {
                due_at: Timestamp(due_at),
            },
        }];
        self.apply(&commit);
    }

    fn view(&self, id: &str) -> MessageView {
        self.mirror.view(&m(id)).expect("message must be readable")
    }

    fn labels(&self, id: &str) -> LabelSet {
        self.view(id).labels
    }

    /// A thread's labels as users see them.
    fn thread(&self, thread: &str) -> LabelSet {
        let messages: Vec<LabelSet> = self
            .mirror
            .messages
            .values()
            .filter(|record| record.thread_id == t(thread))
            .map(|record| self.labels(&record.id.0))
            .collect();
        labels::thread_labels(messages.iter())
    }

    /// A confirmation fetch, claimed after the deliveries so far, observes
    /// Gmail's state of `id`.
    fn confirm(&mut self, now: i64, thread: &str, id: &str, labels: &[&str], checkpoint: u64) {
        let verify = self
            .claim(now, |kind| *kind == JobKind::Verify(m(id)))
            .pop()
            .expect("a delivery queues a confirmation fetch");
        let mut fetched = self.commit();
        fetched.claim = Some(verify.claim());
        fetched.messages = vec![observation(thread, id, labels, checkpoint)];
        fetched.jobs = vec![JobOp::Finish {
            claim: verify.claim(),
            outcome: JobOutcome::Done,
        }];
        self.apply(&fetched);
    }

    fn deliver_jobs(&self) -> Vec<MessageId> {
        self.mirror
            .jobs
            .keys()
            .filter_map(|kind| match kind {
                JobKind::Deliver(id) => Some(id.clone()),
                _ => None,
            })
            .collect()
    }
}

/// Zero: mail-label-store > durable message label outbox > preserves app MUTE state without dispatching it as a Gmail label
#[test]
#[ignore = "zero-na: MUTE is Zero app-only thread state; mailtide mirrors only Gmail labels and keeps no synthetic labels"]
fn durable_message_label_outbox_preserves_app_mute_state_without_dispatching_it_as_a_gmail_label() {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-label-store > durable message label outbox > preserves snooze through history refresh, restart and durable custom-label actions
#[test]
#[ignore = "zero-na: snooze is Zero app-only thread state; mailtide has no synthetic labels (pending custom-label intents across refresh/restart are covered by the other ports)"]
fn durable_message_label_outbox_preserves_snooze_through_history_refresh_restart_and_durable_custom_label_actions()
 {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-label-store > durable message label outbox > migrates only recognized app state and never resurrects removed state from stale summaries
#[test]
#[ignore = "zero-na: migration of Zero's synthetic SNOOZED/MUTE labels out of its threads summary table; mailtide has neither synthetic labels nor that table"]
fn durable_message_label_outbox_migrates_only_recognized_app_state_and_never_resurrects_removed_state_from_stale_summaries()
 {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-label-store > durable message label outbox > migrates synthetic labels from stored threads only once
#[test]
#[ignore = "zero-na: one-shot migration of Zero's synthetic labels from its threads table; mailtide has no synthetic labels or legacy schema"]
fn durable_message_label_outbox_migrates_synthetic_labels_from_stored_threads_only_once() {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-label-store > durable message label outbox > keeps thread state across snapshot message replacement and clears it on final deletion
#[test]
#[ignore = "zero-na: protects app-only per-thread state (SNOOZED); mailtide keeps no thread-level state, thread labels derive from its messages"]
fn durable_message_label_outbox_keeps_thread_state_across_snapshot_message_replacement_and_clears_it_on_final_deletion()
 {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-label-store > durable message label outbox > derives unread from every message and preserves pending intent across imports
#[test]
fn durable_message_label_outbox_derives_unread_from_every_message_and_preserves_pending_intent_across_imports()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "old", &["INBOX", "UNREAD"], 1);
    mailbox.observe("t", "new", &["INBOX"], 1);
    assert!(has(&mailbox.thread("t"), "UNREAD"), "any unread message");
    mailbox.act_thread("t", Action::SetRead(ReadState::Read));
    // Re-import both messages: Gmail hasn't applied the change yet.
    mailbox.messages(vec![
        observation("t", "old", &["INBOX", "UNREAD"], 2),
        observation("t", "new", &["INBOX"], 2),
    ]);
    assert!(!has(&mailbox.thread("t"), "UNREAD"), "the intent overlays");
    assert!(
        has(&mailbox.view("old").observed_labels, "UNREAD"),
        "observed labels stay Gmail's"
    );
    let groups = mailbox.peek(1000);
    assert!(
        groups
            .iter()
            .any(|g| g.messages == vec![m("old")] && g.remove == ids(&["UNREAD"])),
        "the unread message is still marked read: {groups:?}"
    );
}

/// Zero: mail-label-store > durable message label outbox > does not clear a newer toggle when an older batch completes
#[test]
fn durable_message_label_outbox_does_not_clear_a_newer_toggle_when_an_older_batch_completes() {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["UNREAD"], 1);
    mailbox.act(&["m"], Action::SetRead(ReadState::Read));
    let batch = mailbox.deliveries(1000).remove(0);
    mailbox.act(&["m"], Action::SetRead(ReadState::Unread));
    assert!(applied(&mailbox.ack(&batch, 1000)));
    assert!(has(&mailbox.labels("m"), "UNREAD"));
    let next = mailbox.peek(1000);
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].add, ids(&["UNREAD"]));
    assert!(next[0].remove.is_empty());
}

/// Zero: mail-label-store > durable message label outbox > coalesces toggles, survives store reconstruction, and respects retry times
#[test]
fn durable_message_label_outbox_coalesces_toggles_survives_store_reconstruction_and_respects_retry_times()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["UNREAD"], 1);
    mailbox.act(&["m"], Action::SetRead(ReadState::Read));
    mailbox.act(&["m"], Action::SetRead(ReadState::Unread));
    mailbox.act(&["m"], Action::SetRead(ReadState::Read));
    let batch = mailbox.deliveries(1000).remove(0);
    assert_eq!(batch.group().unwrap().versions.len(), 1);
    mailbox.retry(&batch, 5000);
    assert!(mailbox.peek(4999).is_empty(), "not before the retry time");
    mailbox.restart();
    assert!(mailbox.peek(4999).is_empty(), "the retry time is durable");
    let due = mailbox.peek(5000);
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].remove, ids(&["UNREAD"]));
    assert!(due[0].add.is_empty());
    assert_eq!(mailbox.mirror.intents.len(), 1, "one intent per label");
}

/// Zero: mail-label-store > durable message label outbox > rolls back local state and intent together on failure
#[test]
fn durable_message_label_outbox_rolls_back_local_state_and_intent_together_on_failure() {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["UNREAD"], 1);
    let before = mailbox.mirror.clone();
    // A commit whose guard fails: the claim it finishes isn't held.
    let mut commit = mailbox.commit();
    commit.claim = Some(JobClaim {
        kind: JobKind::Deliver(m("m")),
        attempt: AttemptId(99),
    });
    commit.messages = vec![MessageOp::PatchLabels {
        id: m("m"),
        add: set(["STARRED"]),
        remove: LabelSet::new(),
        checkpoint: HistoryId(2),
    }];
    commit.intents = vec![IntentOp::Request(ActionRequest::Messages {
        ids: vec![m("m")],
        action: Action::SetRead(ReadState::Read),
    })];
    assert!(matches!(
        mailbox.mirror.apply(&commit),
        CommitOutcome::Rejected(_)
    ));
    assert_eq!(mailbox.mirror, before, "nothing applies");
    assert!(has(&mailbox.thread("t"), "UNREAD"));
    assert!(mailbox.mirror.intents.is_empty());
}

/// Zero: mail-label-store > durable message label outbox > applies provider changes per message without producing outbound writes
#[test]
fn durable_message_label_outbox_applies_provider_changes_per_message_without_producing_outbound_writes()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "a", &["UNREAD"], 1);
    mailbox.observe("t", "b", &["UNREAD"], 1);
    mailbox.patch("a", &[], &["UNREAD"], 2);
    assert!(has(&mailbox.thread("t"), "UNREAD"));
    mailbox.patch("b", &[], &["UNREAD"], 3);
    assert!(!has(&mailbox.thread("t"), "UNREAD"));
    assert!(mailbox.mirror.intents.is_empty());
    assert!(mailbox.deliver_jobs().is_empty());
    assert!(mailbox.peek(1000).is_empty());
}

/// Zero: mail-label-store > durable message label outbox > limits batches to 1000 messages and enforces a persisted mailbox cooldown
#[test]
fn durable_message_label_outbox_limits_batches_to_1000_messages_and_enforces_a_persisted_mailbox_cooldown()
 {
    let mut mailbox = Mailbox::new();
    let ops = (0..1001)
        .map(|i| observation("t", &i.to_string(), &["UNREAD"], 1))
        .collect();
    mailbox.messages(ops);
    mailbox.act_thread("t", Action::SetRead(ReadState::Read));
    let intents: Vec<_> = mailbox.mirror.intents.values().cloned().collect();
    let groups = plan_delivery(&intents);
    let mut sizes: Vec<usize> = groups.iter().map(|g| g.messages.len()).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![1, 1000]);

    // The cooldown is stored with the account and survives a restart.
    let mut cool = mailbox.commit();
    cool.account_ops = vec![AccountOp::SetCooldown(Cooldown {
        target: QuotaTarget::Account(AccountId(ACCOUNT.into())),
        until: Timestamp(2000),
    })];
    mailbox.apply(&cool);
    mailbox.restart();
    let snapshot = mailbox.snapshot.clone();
    assert_eq!(snapshot.cooldown.as_ref().unwrap().until, Timestamp(2000));
    let mut engine = Engine::new(snapshot.validate().unwrap(), EngineConfig::default());
    let claims = |commands: &[Command]| {
        commands
            .iter()
            .filter(|c| matches!(c.kind, CommandKind::ClaimJobs { .. }))
            .count()
    };
    let jobs_timer = |now| Event {
        now: Timestamp(now),
        kind: EventKind::TimerFired {
            timer: TimerKind::Jobs,
        },
    };
    assert_eq!(claims(&engine.handle(jobs_timer(1999))), 0, "cooling down");
    assert_eq!(claims(&engine.handle(jobs_timer(2000))), 1, "cooldown over");
}

/// Zero: mail-label-store > durable message label outbox > does not mutate drafts when marking a thread unread
#[test]
fn durable_message_label_outbox_does_not_mutate_drafts_when_marking_a_thread_unread() {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &[], 1);
    mailbox.observe("t", "draft", &["DRAFT"], 1);
    mailbox.act_thread("t", Action::SetRead(ReadState::Unread));
    mailbox.act(&["draft"], Action::SetRead(ReadState::Unread));
    let groups = mailbox.peek(1000);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].messages, vec![m("m")]);
    assert_eq!(groups[0].add, ids(&["UNREAD"]));
    assert_eq!(mailbox.labels("draft"), set(["DRAFT"]));
    assert!(mailbox.mirror.intents_of(&m("draft")).is_empty());
}

/// Zero: mail-label-store > durable message label outbox > avoids provider writes when the requested state is already observed
#[test]
#[ignore = "zero-na: deliberate divergence: outbox::expand keeps implied changes the mirror already shows (the mirror may lag Gmail; changes are absolute), so mailtide does write"]
fn durable_message_label_outbox_avoids_provider_writes_when_the_requested_state_is_already_observed()
 {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-label-store > durable message label outbox > retains acknowledged intent until history confirms it, then accepts a later external edit
#[test]
fn durable_message_label_outbox_retains_acknowledged_intent_until_history_confirms_it_then_accepts_a_later_external_edit()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["UNREAD"], 10);
    mailbox.act(&["m"], Action::SetRead(ReadState::Read));
    let batch = mailbox.deliveries(1000).remove(0);
    assert!(applied(&mailbox.ack(&batch, 1000)));
    // History can't confirm: a record fetched after delivery may predate it.
    mailbox.patch("m", &["UNREAD"], &[], 11);
    assert!(!has(&mailbox.thread("t"), "UNREAD"), "still overlaid");
    mailbox.patch("m", &[], &["UNREAD"], 12);
    assert_eq!(mailbox.view("m").pending_actions, 1, "still unconfirmed");
    mailbox.confirm(1000, "t", "m", &[], 12);
    assert_eq!(mailbox.view("m").pending_actions, 0, "confirmed");
    assert!(mailbox.mirror.intents.is_empty());
    mailbox.patch("m", &["UNREAD"], &[], 13);
    assert!(has(&mailbox.thread("t"), "UNREAD"), "external edit wins");
}

/// Zero: mail-label-store > durable message label outbox > ignores history older than a fresh snapshot using numeric Gmail IDs
#[test]
fn durable_message_label_outbox_ignores_history_older_than_a_fresh_snapshot_using_numeric_gmail_ids()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &[], 100);
    // "99" > "100" as strings; checkpoints compare numerically.
    mailbox.patch("m", &["UNREAD"], &[], 99);
    assert!(!has(&mailbox.thread("t"), "UNREAD"));
    assert_eq!(mailbox.view("m").checkpoint, Some(HistoryId(100)));
}

/// Zero: mail-label-store > durable message label outbox > does not resurrect a deleted message from stale R2 contents
#[test]
fn durable_message_label_outbox_does_not_resurrect_a_deleted_message_from_stale_r2_contents() {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["UNREAD"], 10);
    mailbox.act(&["m"], Action::SetRead(ReadState::Read));
    mailbox.messages(vec![MessageOp::Tombstone {
        id: m("m"),
        evidence: DeletionEvidence::History { at: HistoryId(11) },
    }]);
    mailbox.observe("t", "m", &["UNREAD"], 10);
    mailbox.observe("t", "m", &["UNREAD"], 12);
    assert!(mailbox.mirror.view(&m("m")).is_none());
    assert!(mailbox.mirror.intents.is_empty());
    assert!(mailbox.deliver_jobs().is_empty());
    assert!(mailbox.peek(1000).is_empty());
}

/// Zero: mail-label-store > durable message label outbox > reconciles delivered intent against a fresh post-delivery snapshot
#[test]
fn durable_message_label_outbox_reconciles_delivered_intent_against_a_fresh_post_delivery_snapshot()
{
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["UNREAD"], 10);
    mailbox.act(&["m"], Action::SetRead(ReadState::Read));
    let batch = mailbox.deliveries(1000).remove(0);
    assert!(applied(&mailbox.ack(&batch, 1000)));
    // The confirmation fetch, issued after delivery, sees Gmail's state.
    let verify = mailbox
        .claim(1000, |kind| *kind == JobKind::Verify(m("m")))
        .pop()
        .expect("delivery schedules a confirmation fetch");
    let mut fetched = mailbox.commit();
    fetched.claim = Some(verify.claim());
    fetched.messages = vec![observation("t", "m", &["UNREAD"], 20)];
    fetched.jobs = vec![JobOp::Finish {
        claim: verify.claim(),
        outcome: JobOutcome::Done,
    }];
    mailbox.apply(&fetched);
    assert!(mailbox.mirror.intents.is_empty(), "Gmail's state wins");
    assert_eq!(mailbox.view("m").pending_actions, 0);
    assert!(has(&mailbox.thread("t"), "UNREAD"));
}

/// Zero: mail-label-store > durable message label outbox > prunes messages absent from an authoritative snapshot after an expired history cursor
#[test]
fn durable_message_label_outbox_prunes_messages_absent_from_an_authoritative_snapshot_after_an_expired_history_cursor()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "old", &["UNREAD"], 10);
    mailbox.observe("t", "keep", &[], 10);
    mailbox.act_thread("t", Action::SetRead(ReadState::Read));
    // History expired: reconciliation lists only `keep`.
    mailbox.messages(vec![
        MessageOp::Listed {
            id: m("keep"),
            thread_id: t("t"),
            scope: ListScope::All,
            baseline: HistoryId(20),
        },
        MessageOp::FinishReconcile {
            baseline: HistoryId(20),
        },
    ]);
    assert!(
        mailbox.mirror.jobs.contains_key(&JobKind::Verify(m("old"))),
        "an unlisted message is verified"
    );
    // The verification fetch finds it gone.
    mailbox.messages(vec![MessageOp::Tombstone {
        id: m("old"),
        evidence: DeletionEvidence::VerifiedMissing {
            no_later_than: HistoryId(20),
        },
    }]);
    assert!(mailbox.mirror.view(&m("old")).is_none());
    assert!(mailbox.mirror.tombstones.contains_key(&m("old")));
    assert!(!has(&mailbox.thread("t"), "UNREAD"));
    assert!(mailbox.mirror.intents_of(&m("old")).is_empty());
    assert!(!mailbox.deliver_jobs().contains(&m("old")));
    assert!(
        mailbox
            .peek(1000)
            .iter()
            .all(|g| !g.messages.contains(&m("old")))
    );
}

/// Zero: mail-label-store > durable message label outbox > migrates legacy unread retries and delivered intents without losing their versions
#[test]
#[ignore = "zero-na: migration of Zero's legacy mail_label_outbox table into mail_label_intents; mailtide has no legacy outbox schema"]
fn durable_message_label_outbox_migrates_legacy_unread_retries_and_delivered_intents_without_losing_their_versions()
 {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-label-store > durable message label outbox > coalesces archive, star and read intent into one compatible Gmail modification
#[test]
fn durable_message_label_outbox_coalesces_archive_star_and_read_intent_into_one_compatible_gmail_modification()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["INBOX", "UNREAD"], 1);
    mailbox.act_thread("t", change_labels(&["STARRED"], &["INBOX"]));
    mailbox.act_thread("t", Action::SetRead(ReadState::Read));
    assert_eq!(mailbox.labels("m"), set(["STARRED"]));
    let groups = mailbox.peek(1000);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].messages, vec![m("m")]);
    assert_eq!(groups[0].add, ids(&["STARRED"]));
    assert_eq!(groups[0].remove, ids(&["INBOX", "UNREAD"]));
    assert_eq!(groups[0].versions.len(), 3);
}

/// Zero: mail-label-store > durable message label outbox > preserves a newer spam-to-inbox restore while older trash delivery and history complete
#[test]
fn durable_message_label_outbox_preserves_a_newer_spam_to_inbox_restore_while_older_trash_delivery_and_history_complete()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["SPAM", "UNREAD"], 10);
    mailbox.act_thread(
        "t",
        change_labels(&["TRASH", "IMPORTANT", "Label_123"], &["SPAM"]),
    );
    let old = mailbox.deliveries(1000).remove(0);
    mailbox.act_thread("t", Action::MoveToInbox);
    mailbox.restart();
    // The old engine's acknowledgment is fenced off; its changes stay pending
    // and go out again (Gmail's label changes are absolute).
    assert!(!applied(&mailbox.ack(&old, 1000)));
    mailbox.patch("m", &["TRASH", "IMPORTANT", "Label_123"], &["SPAM"], 11);
    assert_eq!(
        mailbox.thread("t"),
        set(["INBOX", "UNREAD", "IMPORTANT", "Label_123"])
    );
    let restore = mailbox.deliveries(1100).remove(0);
    let group = restore.group().cloned().unwrap();
    let label = |id: &str| LabelId(id.into());
    assert!(group.add.contains(&label("INBOX")));
    assert!(group.remove.contains(&label("TRASH")));
    assert!(!group.add.contains(&label("TRASH")));
    assert!(!group.remove.contains(&label("INBOX")));
    assert!(applied(&mailbox.ack(&restore, 1100)));
    mailbox.patch("m", &["INBOX"], &["TRASH", "SPAM"], 12);
    assert_eq!(
        mailbox.thread("t"),
        set(["INBOX", "UNREAD", "IMPORTANT", "Label_123"])
    );
    mailbox.confirm(
        1100,
        "t",
        "m",
        &["INBOX", "UNREAD", "IMPORTANT", "Label_123"],
        12,
    );
    assert!(mailbox.mirror.intents.is_empty());
    assert_eq!(
        mailbox.thread("t"),
        set(["INBOX", "UNREAD", "IMPORTANT", "Label_123"])
    );
}

/// Zero: mail-label-store > durable message label outbox > queues an inbox restore after trash even before Gmail confirms implicit inbox removal
#[test]
fn durable_message_label_outbox_queues_an_inbox_restore_after_trash_even_before_gmail_confirms_implicit_inbox_removal()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["INBOX"], 5);
    mailbox.act_thread("t", Action::MoveToTrash);
    let trash = mailbox.deliveries(0).remove(0);
    assert!(applied(&mailbox.ack(&trash, 0)));
    assert_eq!(mailbox.thread("t"), set(["TRASH"]));
    mailbox.act_thread("t", Action::RestoreFromTrash(RestoreDestination::Inbox));
    let restore = mailbox.deliveries(3000).remove(0);
    let group = restore.group().cloned().unwrap();
    assert_eq!(group.add, ids(&["INBOX"]));
    assert_eq!(group.remove, ids(&["TRASH"]));
    assert!(applied(&mailbox.ack(&restore, 3000)));
    // A record from before the trash reached Gmail confirms nothing.
    mailbox.patch("m", &["STARRED"], &[], 10);
    assert_eq!(mailbox.view("m").pending_actions, 2);
    // The trash lands, then the restore.
    mailbox.patch("m", &["TRASH"], &["INBOX"], 11);
    assert!(has(&mailbox.thread("t"), "INBOX"));
    assert!(!has(&mailbox.thread("t"), "TRASH"));
    mailbox.patch("m", &["INBOX"], &["TRASH"], 12);
    assert_eq!(
        mailbox.view("m").pending_actions,
        2,
        "history confirms nothing"
    );
    mailbox.confirm(3000, "t", "m", &["INBOX", "STARRED"], 12);
    assert!(mailbox.mirror.intents.is_empty());
    assert_eq!(mailbox.thread("t"), set(["INBOX", "STARRED"]));
}

/// Zero: mail-label-store > durable message label outbox > does not resend an already delivered custom-label intent when the same action is repeated
#[test]
fn durable_message_label_outbox_does_not_resend_an_already_delivered_custom_label_intent_when_the_same_action_is_repeated()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &[], 1);
    mailbox.act_thread("t", change_labels(&["Label_123"], &[]));
    let batch = mailbox.deliveries(1000).remove(0);
    assert!(applied(&mailbox.ack(&batch, 1000)));
    mailbox.act_thread("t", change_labels(&["Label_123"], &[]));
    assert!(mailbox.peek(3000).is_empty(), "nothing is resent");
    let intents = mailbox.mirror.intents_of(&m("m"));
    assert_eq!(intents.len(), 1);
    assert!(matches!(intents[0].state, IntentState::Delivered { .. }));
    assert!(has(&mailbox.labels("m"), "Label_123"));
    mailbox.patch("m", &["Label_123"], &[], 10);
    mailbox.confirm(3000, "t", "m", &["Label_123"], 10);
    assert!(mailbox.mirror.intents.is_empty());
    assert!(mailbox.peek(3000).is_empty());
}

/// Zero: mail-label-store > durable message label outbox > retries custom-label and importance intent independently without modifying drafts
#[test]
fn durable_message_label_outbox_retries_custom_label_and_importance_intent_independently_without_modifying_drafts()
 {
    // mailtide retries per message, not per label: a newer change to one label
    // rides the message's next delivery. What carries over: a retry neither
    // loses nor reorders intents, keeps its attempt count, never targets drafts.
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "a", &["INBOX"], 1);
    mailbox.observe("t", "b", &["INBOX", "IMPORTANT"], 1);
    mailbox.observe("t", "draft", &["DRAFT"], 1);
    mailbox.act_thread("t", change_labels(&["IMPORTANT", "Label_123"], &[]));
    let batches = mailbox.deliveries(1000);
    let targets: Vec<_> = batches.iter().map(|job| job.kind().clone()).collect();
    assert_eq!(
        targets,
        vec![JobKind::Deliver(m("a")), JobKind::Deliver(m("b"))]
    );
    for batch in &batches {
        mailbox.retry(batch, 5000);
    }
    mailbox.act_thread("t", Action::SetImportance(Importance::Normal));
    mailbox.restart();
    let retried = mailbox.deliveries(5000);
    assert_eq!(retried.len(), 2);
    for job in &retried {
        assert_eq!(job.failed_attempts, 1);
        let group = job.group().unwrap();
        assert_eq!(group.add, ids(&["Label_123"]));
        assert_eq!(group.remove, ids(&["IMPORTANT"]));
    }
    assert_eq!(mailbox.labels("draft"), set(["DRAFT"]));
    assert!(mailbox.mirror.intents_of(&m("draft")).is_empty());
    assert!(
        mailbox
            .mirror
            .intents
            .values()
            .filter(|intent| intent.label == LabelId("IMPORTANT".into()))
            .all(|intent| intent.change == LabelChange::Remove),
        "the newer importance change wins"
    );
}

/// Zero: mail-label-store > durable message label outbox > protects a rapid unstar from an older star acknowledgment without losing archive intent
#[test]
fn durable_message_label_outbox_protects_a_rapid_unstar_from_an_older_star_acknowledgment_without_losing_archive_intent()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["INBOX"], 1);
    mailbox.act_thread("t", change_labels(&["STARRED"], &["INBOX"]));
    let older = mailbox.deliveries(1000).remove(0);
    mailbox.act_thread("t", Action::SetStar(StarState::Unstarred));
    assert!(applied(&mailbox.ack(&older, 1000)));
    let labels = mailbox.thread("t");
    assert!(!has(&labels, "STARRED"));
    assert!(!has(&labels, "INBOX"));
    let next = mailbox.peek(1000);
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].remove, ids(&["STARRED"]));
    assert!(next[0].add.is_empty());
    mailbox.patch("m", &["STARRED"], &["INBOX"], 10);
    assert!(!has(&mailbox.thread("t"), "STARRED"));
    assert!(!has(&mailbox.thread("t"), "INBOX"));
    let next = mailbox.peek(1000);
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].remove, ids(&["STARRED"]));
}

/// Zero: mail-label-store > durable message label outbox > confirms archive and star independently, then accepts a Gmail-side restore
#[test]
fn durable_message_label_outbox_confirms_archive_and_star_independently_then_accepts_a_gmail_side_restore()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &["INBOX"], 1);
    mailbox.act_thread("t", change_labels(&["STARRED"], &["INBOX"]));
    let batch = mailbox.deliveries(1000).remove(0);
    assert!(applied(&mailbox.ack(&batch, 1000)));
    // Only the confirmation fetch settles (both labels at once); history
    // showing part of the change keeps the overlay.
    mailbox.patch("m", &["STARRED"], &[], 10);
    assert_eq!(mailbox.thread("t"), set(["STARRED"]));
    assert_eq!(mailbox.view("m").pending_actions, 2);
    mailbox.patch("m", &[], &["INBOX"], 11);
    mailbox.confirm(1000, "t", "m", &["STARRED"], 11);
    assert_eq!(mailbox.view("m").pending_actions, 0);
    mailbox.patch("m", &["INBOX"], &[], 12);
    assert_eq!(mailbox.thread("t"), set(["STARRED", "INBOX"]));
}

/// Zero: mail-label-store > durable message label outbox > groups only messages with identical add/remove sets and deduplicates bulk IDs
#[test]
fn durable_message_label_outbox_groups_only_messages_with_identical_add_remove_sets_and_deduplicates_bulk_ids()
 {
    // mailtide keeps implied changes the mirror already shows, so `b1` gets a
    // different change set by being starred only.
    let mut mailbox = Mailbox::new();
    mailbox.observe("a", "a1", &["INBOX"], 1);
    mailbox.observe("a", "a2", &["INBOX"], 1);
    mailbox.observe("b", "b1", &[], 1);
    mailbox.act_thread("a", change_labels(&["STARRED"], &["INBOX"]));
    mailbox.act_thread("a", change_labels(&["STARRED"], &["INBOX"]));
    mailbox.act(&["b1", "b1"], Action::SetStar(StarState::Starred));
    assert_eq!(mailbox.mirror.intents.len(), 5, "one intent per label");
    let intents: Vec<_> = mailbox.mirror.intents.values().cloned().collect();
    let groups = plan_delivery(&intents);
    assert_eq!(groups.len(), 2);
    let archive = groups
        .iter()
        .find(|g| g.remove.contains(&LabelId("INBOX".into())))
        .unwrap();
    assert_eq!(archive.messages, vec![m("a1"), m("a2")]);
    let star = groups.iter().find(|g| g.remove.is_empty()).unwrap();
    assert_eq!(star.messages, vec![m("b1")]);
    assert_eq!(star.versions.len(), 1);
}

/// Zero: mail-label-store > durable message label outbox > rolls back an entire bulk action and its local summaries if any thread is missing
#[test]
#[ignore = "zero-na: deliberate divergence: IntentOp::Request admits per message and skips unknown targets instead of failing the bulk action; commit atomicity is ported in rolls_back_local_state_and_intent_together_on_failure"]
fn durable_message_label_outbox_rolls_back_an_entire_bulk_action_and_its_local_summaries_if_any_thread_is_missing()
 {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-label-store > durable message label outbox > respects per-request Gmail label-array limits without losing remaining intent
#[test]
fn durable_message_label_outbox_respects_per_request_gmail_label_array_limits_without_losing_remaining_intent()
 {
    let mut mailbox = Mailbox::new();
    mailbox.observe("t", "m", &[], 1);
    let many: Vec<String> = (0..101).map(|i| format!("Label_{i}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    mailbox.act_thread("t", change_labels(&many, &[]));
    let batch = mailbox.deliveries(1000).remove(0);
    assert_eq!(batch.group().unwrap().add.len(), 100);
    assert!(applied(&mailbox.ack(&batch, 1000)));
    let rest = mailbox.peek(1000);
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].add.len(), 1);
    assert_eq!(mailbox.mirror.intents.len(), 101);
}

/// Zero: mail-label-store > app-kept thread labels > survive re-imports and provider deltas and are cleared with the thread
#[test]
#[ignore = "zero-na: app-kept K_* thread labels are Zero app-only state never sent to Gmail; mailtide stores only Gmail labels"]
fn app_kept_thread_labels_survive_re_imports_and_provider_deltas_and_are_cleared_with_the_thread() {
    todo!("port from Zero 4b642dfd9890")
}
