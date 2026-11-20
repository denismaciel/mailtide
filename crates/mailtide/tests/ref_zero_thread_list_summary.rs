//! Tests ported by name from Zero's `thread-list-summary.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890). Plan: `features/threads.toml`.
//!
//! Zero stores a body-free summary per thread and corrects it from the label
//! store on every read. mailtide stores none: a [`ThreadView`] is folded from
//! the mirror's rows at read time ([`AccountMirror::thread`]), so the tests
//! about keeping a snapshot honest become rules of the fold.

use mailtide::commit::{Commit, CommitOutcome, DeletionEvidence, JobPlan, MessageOp};
use mailtide::gmail::{HeaderFacts, MessageObservation};
use mailtide::ids::{
    AccountId, BlobRef, CommitId, ENGINE_VERSION, HistoryId, MessageId, ThreadId, Timestamp,
};
use mailtide::labels::{self, set};
use mailtide::mirror::AccountMirror;
use mailtide::quota::Priority;
use mailtide::thread::ThreadView;

fn m(id: &str) -> MessageId {
    MessageId(id.into())
}

fn t() -> ThreadId {
    ThreadId("t".into())
}

/// One account's mirror, driven with commits as the engine drives it.
struct Mailbox {
    mirror: AccountMirror,
    seq: u64,
    checkpoint: u64,
}

impl Mailbox {
    fn new() -> Self {
        let mut mirror = AccountMirror::new(AccountId("zero@example.com".into()), 1);
        mirror.claim(ENGINE_VERSION).expect("claim");
        Self {
            mirror,
            seq: 0,
            checkpoint: 0,
        }
    }

    fn apply(&mut self, ops: Vec<MessageOp>) {
        self.seq += 1;
        let commit = Commit {
            id: CommitId {
                epoch: self.mirror.record.epoch,
                seq: self.seq,
            },
            account: self.mirror.account.clone(),
            epoch: self.mirror.record.epoch,
            engine_version: ENGINE_VERSION,
            cursor: None,
            claim: None,
            sync: None,
            messages: ops,
            labels: Vec::new(),
            account_ops: Vec::new(),
            job_plan: JobPlan {
                priority: Priority::Live,
                import_due_at: Timestamp(0),
                draft_import_due_at: Timestamp(0),
                verify_due_at: Timestamp(0),
                cleanup_due_at: Timestamp(0),
                deliver_due_at: Timestamp(0),
                confirm_due_at: Timestamp(0),
            },
            jobs: Vec::new(),
            intents: Vec::new(),
        };
        assert!(matches!(
            self.mirror.apply(&commit),
            CommitOutcome::Applied { .. }
        ));
    }

    fn next(&mut self) -> HistoryId {
        self.checkpoint += 1;
        HistoryId(self.checkpoint)
    }

    fn observation(&mut self, id: &str, labels: &[&str]) -> MessageObservation {
        MessageObservation {
            id: m(id),
            thread_id: t(),
            labels: set(labels.iter().copied()),
            checkpoint: self.next(),
        }
    }

    /// Import `id` as Zero's fixture message: Ann writes to me.
    fn import(&mut self, id: &str, labels: &[&str], date: i64, headers: HeaderFacts) {
        let observation = self.observation(id, labels);
        self.apply(vec![MessageOp::MarkImported {
            observation,
            blob: BlobRef(format!("{id}.eml")),
            size: 100,
            internal_date: Timestamp(date),
            headers: Box::new(headers),
            facts: None,
        }]);
    }

    fn discover(&mut self, id: &str, labels: &[&str]) {
        let observation = self.observation(id, labels);
        self.apply(vec![MessageOp::Observe(observation)]);
    }

    fn relabel(&mut self, id: &str, labels: &[&str]) {
        let observation = self.observation(id, labels);
        self.apply(vec![MessageOp::Observe(observation)]);
    }

    fn delete(&mut self, ids: &[&str]) {
        let at = self.next();
        self.apply(
            ids.iter()
                .map(|id| MessageOp::Tombstone {
                    id: m(id),
                    evidence: DeletionEvidence::History { at },
                })
                .collect(),
        );
    }

    fn thread(&self) -> Option<ThreadView> {
        self.mirror.thread(&t())
    }
}

/// Zero's fixture message.
fn ann(cc: &[&str]) -> HeaderFacts {
    HeaderFacts {
        rfc822_message_id: Some("<m@example.com>".into()),
        from: Some("Ann <ann@example.com>".into()),
        to: vec!["me@example.com".into()],
        cc: cc.iter().map(|c| (*c).to_owned()).collect(),
        subject: Some("Subject".into()),
        snippet: Some("snippet".into()),
        ..HeaderFacts::default()
    }
}

/// Zero's `thread()`: m1 and m2 labelled Work, and a draft.
fn thread() -> Mailbox {
    let mut mailbox = Mailbox::new();
    mailbox.import("m1", &["INBOX", "Label_1"], 10, ann(&[]));
    mailbox.import("m2", &["INBOX", "Label_1"], 20, ann(&[]));
    mailbox.import("d1", &["DRAFT"], 30, ann(&[]));
    mailbox
}

#[test]
fn a_thread_view_carries_the_latest_envelope_the_counts_and_no_body() {
    let mut mailbox = Mailbox::new();
    mailbox.import("m1", &["INBOX"], 10, ann(&[]));
    mailbox.import("m2", &["INBOX", "UNREAD"], 20, ann(&[]));
    mailbox.import("d", &["DRAFT"], 30, ann(&[]));
    let view = mailbox.thread().expect("a thread");
    assert_eq!(view.latest, Some(m("m2")));
    let latest = view
        .messages
        .iter()
        .find(|message| Some(&message.id) == view.latest.as_ref())
        .and_then(|message| message.content.as_ref())
        .expect("the latest has an envelope");
    assert_eq!(latest.headers.subject.as_deref(), Some("Subject"));
    assert_eq!(
        latest.headers.from.as_deref(),
        Some("Ann <ann@example.com>")
    );
    // The body stays in blob storage: the view holds only its reference.
    assert_eq!(latest.blob, BlobRef("m2.eml".into()));
    assert!(view.unread);
    assert_eq!(view.message_count, 2);
    assert_eq!(view.draft_count, 1);
    assert_eq!(view.labels, set(["INBOX", "UNREAD"]));
}

#[test]
fn participants_tell_a_group_thread_from_a_two_party_one() {
    // Zero's flag: the latest message has more than one recipient.
    let is_group = |view: &ThreadView| {
        let latest = view
            .messages
            .iter()
            .find(|message| Some(&message.id) == view.latest.as_ref())
            .and_then(|message| message.content.as_ref())
            .expect("a latest");
        latest.headers.to.len() + latest.headers.cc.len() > 1
    };
    let mut group = Mailbox::new();
    group.import("m", &["INBOX"], 10, ann(&["b@example.com"]));
    let view = group.thread().expect("a thread");
    assert!(is_group(&view));
    assert_eq!(
        view.participants,
        ["Ann <ann@example.com>", "me@example.com", "b@example.com"]
    );
    let mut pair = Mailbox::new();
    pair.import("m", &["INBOX"], 10, ann(&[]));
    let view = pair.thread().expect("a thread");
    assert!(!is_group(&view));
    assert_eq!(view.participants.len(), 2);
}

#[test]
fn a_thread_with_nothing_imported_has_no_latest() {
    let mut mailbox = Mailbox::new();
    assert_eq!(mailbox.thread(), None);
    mailbox.discover("m", &["INBOX", "UNREAD"]);
    let view = mailbox.thread().expect("a thread");
    assert_eq!(view.latest, None);
    assert_eq!(view.awaiting_import, 1);
    assert_eq!(view.message_count, 1);
    assert!(view.unread);
    assert!(view.participants.is_empty());
}

#[test]
fn labels_unread_and_counts_follow_the_current_message_labels() {
    let mut mailbox = thread();
    mailbox.relabel("m2", &["INBOX", "UNREAD", "Label_1"]);
    let view = mailbox.thread().expect("a thread");
    assert!(view.unread);
    assert_eq!(view.latest, Some(m("m2")));
    let latest = &view.messages[1];
    assert_eq!(latest.id, m("m2"));
    assert!(labels::has(&latest.labels, "Label_1"));
    assert!(labels::is_unread(&latest.labels));
    assert_eq!(view.labels, set(["INBOX", "Label_1", "UNREAD"]));
    assert_eq!(view.message_count, 2);
    assert_eq!(view.draft_count, 1);
}

#[test]
fn a_deleted_thread_is_absent_and_deleted_messages_leave_the_counts() {
    let mut mailbox = thread();
    mailbox.delete(&["m1", "d1"]);
    let view = mailbox.thread().expect("a thread");
    assert_eq!(view.message_count, 1);
    assert_eq!(view.draft_count, 0);
    // Zero hides a thread whose remaining member is a draft. Here a draft is
    // a member like any other (a new draft is a thread of its own), so the
    // view stays, with nothing a list row would show.
    let mut drafts_left = thread();
    drafts_left.delete(&["m1", "m2"]);
    let view = drafts_left.thread().expect("the draft's thread");
    assert_eq!((view.message_count, view.draft_count), (0, 1));
    assert_eq!(view.latest, None);
    assert!(view.labels.is_empty());
    // Every member deleted: no thread.
    let mut gone = thread();
    gone.delete(&["m1", "m2", "d1"]);
    assert_eq!(gone.thread(), None);
}

#[test]
fn deleting_the_latest_message_makes_the_one_before_it_latest() {
    let mut mailbox = thread();
    assert_eq!(mailbox.thread().expect("a thread").latest, Some(m("m2")));
    mailbox.delete(&["m2"]);
    let view = mailbox.thread().expect("a thread");
    assert_eq!(view.latest, Some(m("m1")));
    assert_eq!(view.message_count, 1);
}
