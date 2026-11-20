//! Tests ported by name from Zero's `mail-count-store.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890). Plan: `features/threads.toml`.
//!
//! Per-label counts are a whole-mailbox aggregate, which mailtide leaves to
//! the host with listing. These two tests check what a thread view must carry
//! for a host to count: per-member effective labels with pending actions
//! applied, drafts told apart, deleted messages gone. Zero's counting rule
//! (`MailCountStore.countThreads`) is restated in the test as a host would
//! write it, over [`ThreadView`]s.

use std::collections::BTreeMap;

use mailtide::commit::{Commit, CommitOutcome, DeletionEvidence, JobPlan, MessageOp};
use mailtide::gmail::MessageObservation;
use mailtide::ids::{
    AccountId, CommitId, ENGINE_VERSION, HistoryId, MessageId, ThreadId, Timestamp,
};
use mailtide::labels::{self, set};
use mailtide::mirror::AccountMirror;
use mailtide::outbox::{Action, ActionRequest, Importance, ReadState, RestoreDestination};
use mailtide::quota::Priority;
use mailtide::thread::ThreadView;

fn t(id: &str) -> ThreadId {
    ThreadId(id.into())
}

fn mirror() -> AccountMirror {
    let mut mirror = AccountMirror::new(AccountId("zero@example.com".into()), 1);
    mirror.claim(ENGINE_VERSION).expect("claim");
    mirror
}

fn apply(mirror: &mut AccountMirror, ops: Vec<MessageOp>) {
    let commit = Commit {
        id: CommitId {
            epoch: mirror.record.epoch,
            seq: 0,
        },
        account: mirror.account.clone(),
        epoch: mirror.record.epoch,
        account_generation: mirror.record.generation,
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
            draft_refresh_due_at: Timestamp(0),
        },
        jobs: Vec::new(),
        intents: Vec::new(),
        drafts: Vec::new(),
    };
    assert!(matches!(
        mirror.apply(&commit),
        CommitOutcome::Applied { .. }
    ));
}

/// Zero's `importMessages(thread, messages)`.
fn import_messages(mirror: &mut AccountMirror, thread: &str, messages: &[(&str, &[&str])]) {
    let ops = messages
        .iter()
        .map(|(id, labels)| {
            MessageOp::Observe(MessageObservation {
                id: MessageId((*id).into()),
                thread_id: t(thread),
                labels: set(labels.iter().copied()),
                checkpoint: HistoryId(1),
            })
        })
        .collect();
    apply(mirror, ops);
}

fn act(mirror: &mut AccountMirror, thread: &str, action: Action) {
    mirror.admit_action(
        &ActionRequest::Thread {
            id: t(thread),
            action,
        },
        Timestamp(0),
    );
}

/// The views of every thread the mirror holds, as a host's read model would
/// hold them after following the feed.
fn views(mirror: &AccountMirror) -> Vec<ThreadView> {
    let threads: std::collections::BTreeSet<ThreadId> = mirror
        .messages
        .values()
        .map(|record| record.thread_id.clone())
        .collect();
    threads
        .iter()
        .filter_map(|thread| mirror.thread(thread))
        .collect()
}

/// Zero's `countThreads`, written by a host over thread views: labels count
/// distinct threads; DRAFT and SENT count any member, other labels only an
/// unread non-draft member carrying the label itself; archive counts threads
/// with a non-draft member and no INBOX, SPAM or TRASH on any member.
fn count_threads(views: &[ThreadView]) -> BTreeMap<String, u32> {
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();
    for view in views {
        let mut counted = std::collections::BTreeSet::new();
        let mut foldered = false;
        for message in &view.messages {
            let draft = labels::is_draft(&message.labels);
            let unread = labels::is_unread(&message.labels);
            for label in &message.labels {
                let id = label.0.as_str();
                if matches!(id, "INBOX" | "SPAM" | "TRASH") {
                    foldered = true;
                }
                if id == "DRAFT" || id == "SENT" || (!draft && unread) {
                    counted.insert(id.to_owned());
                }
            }
        }
        if view.message_count > 0 && !foldered {
            counted.insert("archive".to_owned());
        }
        for label in counted {
            *counts.entry(label).or_default() += 1;
        }
    }
    counts
}

/// Zero's `localCounts()` over its catalogue: one entry per catalogue label.
fn local_counts(mirror: &AccountMirror, catalogue: &[(&str, &str)]) -> BTreeMap<String, u32> {
    let counts = count_threads(&views(mirror));
    catalogue
        .iter()
        .map(|(id, name)| ((*name).to_owned(), counts.get(*id).copied().unwrap_or(0)))
        .chain(std::iter::once((
            "archive".to_owned(),
            counts.get("archive").copied().unwrap_or(0),
        )))
        .collect()
}

const CATALOGUE: &[(&str, &str)] = &[
    ("INBOX", "inbox"),
    ("UNREAD", "unread"),
    ("STARRED", "starred"),
    ("IMPORTANT", "important"),
    ("SPAM", "spam"),
    ("TRASH", "trash"),
    ("DRAFT", "drafts"),
    ("SENT", "sent"),
];

fn expected(rows: &[(&str, u32)]) -> BTreeMap<String, u32> {
    rows.iter()
        .map(|(name, count)| ((*name).to_owned(), *count))
        .collect()
}

#[test]
fn a_hosts_label_counts_can_be_folded_from_thread_views() {
    let mut mirror = mirror();
    import_messages(
        &mut mirror,
        "a",
        &[("a1", &["INBOX", "Label_123"]), ("a2", &["SENT", "UNREAD"])],
    );
    import_messages(
        &mut mirror,
        "b",
        &[
            ("b1", &["INBOX", "UNREAD", "Label_123"]),
            ("b2", &["INBOX", "UNREAD", "Label_123"]),
        ],
    );
    import_messages(&mut mirror, "archive", &[("c", &["SENT"])]);
    import_messages(&mut mirror, "draft", &[("d", &["DRAFT", "UNREAD"])]);
    import_messages(&mut mirror, "spam", &[("s", &["SPAM", "UNREAD"])]);
    import_messages(&mut mirror, "trash", &[("t", &["TRASH", "UNREAD"])]);
    let mut catalogue = CATALOGUE.to_vec();
    catalogue.push(("Label_123", "work/projects"));
    assert_eq!(
        local_counts(&mirror, &catalogue),
        expected(&[
            ("inbox", 1),
            ("unread", 4),
            ("starred", 0),
            ("important", 0),
            ("spam", 1),
            ("trash", 1),
            ("drafts", 1),
            ("sent", 2),
            ("work/projects", 1),
            ("archive", 1),
        ])
    );
    // What the views carry for it: the draft's thread has no labels of its
    // own, and is told apart by its draft count.
    let draft = mirror.thread(&t("draft")).expect("the draft's thread");
    assert!(draft.labels.is_empty());
    assert_eq!((draft.message_count, draft.draft_count), (0, 1));
}

#[test]
fn pending_actions_and_deletions_show_in_the_thread_view_at_once() {
    let mut mirror = mirror();
    import_messages(&mut mirror, "a", &[("m", &["INBOX", "UNREAD"])]);
    act(&mut mirror, "a", Action::SetRead(ReadState::Read));
    let view = mirror.thread(&t("a")).expect("a");
    assert!(!view.unread);
    assert_eq!(local_counts(&mirror, CATALOGUE)["inbox"], 0);

    act(&mut mirror, "a", Action::MarkSpam);
    act(
        &mut mirror,
        "a",
        Action::SetImportance(Importance::Important),
    );
    act(&mut mirror, "a", Action::SetRead(ReadState::Unread));
    let view = mirror.thread(&t("a")).expect("a");
    assert!(view.unread);
    assert_eq!(view.labels, set(["IMPORTANT", "SPAM", "UNREAD"]));
    let counts = local_counts(&mirror, CATALOGUE);
    assert_eq!(
        (
            counts["inbox"],
            counts["spam"],
            counts["important"],
            counts["archive"]
        ),
        (0, 1, 1, 0)
    );

    act(
        &mut mirror,
        "a",
        Action::MarkNotSpam(RestoreDestination::Archive),
    );
    assert_eq!(local_counts(&mirror, CATALOGUE)["archive"], 1);

    apply(
        &mut mirror,
        vec![MessageOp::Tombstone {
            id: MessageId("m".into()),
            evidence: DeletionEvidence::History { at: HistoryId(2) },
        }],
    );
    assert_eq!(mirror.thread(&t("a")), None);
    let counts = local_counts(&mirror, CATALOGUE);
    assert_eq!(
        (counts["spam"], counts["important"], counts["archive"]),
        (0, 0, 0)
    );
}
