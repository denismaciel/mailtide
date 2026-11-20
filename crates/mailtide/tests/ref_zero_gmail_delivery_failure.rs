//! Tests ported by name from Zero's `gmail-delivery-failure.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890). Plan:
//! `features/delivery-retries.toml`.
//!
//! Zero classifies a failed label delivery in one pure function and returns
//! the delay or the block. mailtide splits it: `mailtide-gmail` classifies
//! the answer (its unit tests map two of Zero's four tests), and the engine
//! decides what the class does to a `Deliver` job. These two tests drive a
//! delivery through the engine and the shared mirror rules.

use delivery_host::{Host, imported_mirror, m, modifies};
use mailtide::command::GmailRequest;
use mailtide::event::EventKind;
use mailtide::ids::Timestamp;
use mailtide::jobs::{JobKind, JobState};
use mailtide::labels::set;
use mailtide::outbox::{Action, IntentState, ReadState};
use mailtide::retry::{Blocking, BlockingFailure, Failure, Retryable, RetryableFailure};
use mailtide::schedule::TimerKind;
use mailtide::state::SyncState;

mod delivery_host;

fn transient(detail: &str) -> Failure {
    Failure::Retryable(RetryableFailure {
        class: Retryable::Transient,
        retry_after: None,
        detail: detail.into(),
    })
}

fn deliver_job(host: &Host, id: &str) -> (u32, JobState) {
    let job = &host.mirror.jobs[&JobKind::Deliver(m(id))];
    (job.failed_attempts, job.state.clone())
}

/// Zero: gmail-delivery-failure > Gmail durable retry classification > unwraps Effect failures and honors local admission retry times
#[test]
fn a_quota_deferral_of_a_delivery_is_no_failed_attempt_and_waits_as_long_as_the_gate_says() {
    let mut host = Host::new(imported_mirror(&["a"], &["INBOX", "UNREAD"]));
    host.act(&["a"], Action::Archive);
    // A delivery that has failed many times already.
    let mut failed = 0;
    let (command, sent) = loop {
        let (command, group) = host.delivery("a");
        if failed == 6 {
            break (command, group);
        }
        host.handle(EventKind::RequestFailed {
            command,
            failure: transient("connection reset"),
        });
        failed += 1;
        assert_eq!(deliver_job(&host, "a").0, failed);
        host.advance_to_retry();
    };

    // The gate says not for five minutes: longer than any backoff (capped
    // at a minute), and not a failure.
    let gate_reopens = Timestamp(host.now.0 + 300_000);
    host.handle(EventKind::RequestDeferred {
        command,
        until: gate_reopens,
    });
    assert_eq!(
        deliver_job(&host, "a"),
        (
            6,
            JobState::Deferred {
                due_at: gate_reopens
            }
        ),
        "the attempt count is unchanged and the job is due when the gate says"
    );
    assert_eq!(host.timers.get(&TimerKind::Jobs), Some(&gate_reopens));
    assert_eq!(host.intent("a", "INBOX"), IntentState::Pending);
    assert_eq!(host.effective("a"), set(["UNREAD"]), "still shown");

    // Nothing goes out before then; at that time the same change does.
    host.now = Timestamp(gate_reopens.0 - 1);
    host.handle(EventKind::TimerFired {
        timer: TimerKind::Jobs,
    });
    assert!(modifies(host.take_gmail()).is_empty());
    host.now = gate_reopens;
    host.fire(TimerKind::Jobs);
    let (command, again) = host.delivery("a");
    assert_eq!(again, sent);
    host.handle(EventKind::LabelsModified { command });
    assert!(matches!(
        host.intent("a", "INBOX"),
        IntentState::Delivered { .. }
    ));
}

/// Zero: gmail-delivery-failure > Gmail durable retry classification > retries network and server failures, but blocks invalid requests
#[test]
fn a_delivery_retries_a_network_failure_and_a_503_and_a_400_blocks_only_its_intents() {
    let mut host = Host::new(imported_mirror(&["a", "b"], &["INBOX", "UNREAD"]));
    host.act(&["a"], Action::Archive);
    let (first, sent) = host.delivery("a");
    host.act(&["b"], Action::SetRead(ReadState::Read));
    let (other, _) = host.delivery("b");
    host.handle(EventKind::LabelsModified { command: other });

    // A network failure, then a 503: each retried after a backoff that
    // grows, the change still pending and shown.
    let first_failed_at = host.now;
    host.handle(EventKind::RequestFailed {
        command: first,
        failure: transient("connection reset"),
    });
    let (attempts, JobState::Ready { due_at: first_due }) = deliver_job(&host, "a") else {
        panic!("retried");
    };
    assert_eq!(attempts, 1);
    assert!(first_due > host.now);
    host.advance_to_retry();
    let (second, again) = host.delivery("a");
    assert_eq!(again, sent, "the same change is sent again");
    let failed_at = host.now;
    host.handle(EventKind::RequestFailed {
        command: second,
        failure: transient("503 backendError"),
    });
    let (attempts, JobState::Ready { due_at: second_due }) = deliver_job(&host, "a") else {
        panic!("retried");
    };
    assert_eq!(attempts, 2);
    assert!(
        second_due.0 - failed_at.0 >= first_due.0 - first_failed_at.0,
        "backoff grows"
    );
    assert_eq!(host.intent("a", "INBOX"), IntentState::Pending);
    assert_eq!(host.effective("a"), set(["UNREAD"]));
    assert_eq!(host.mirror.record.cooldown, None, "no cooldown for these");

    // A 400: Gmail will never accept this change. Its intents are blocked
    // and stop overlaying, the job is finished, and nothing else stops.
    host.advance_to_retry();
    let (third, _) = host.delivery("a");
    host.handle(EventKind::RequestFailed {
        command: third,
        failure: Failure::Blocking(BlockingFailure {
            class: Blocking::Permanent,
            detail: "400 invalidArgument".into(),
        }),
    });
    assert!(matches!(
        host.intent("a", "INBOX"),
        IntentState::Blocked(BlockingFailure {
            class: Blocking::Permanent,
            ..
        })
    ));
    assert!(!host.mirror.jobs.contains_key(&JobKind::Deliver(m("a"))));
    assert_eq!(host.effective("a"), set(["INBOX", "UNREAD"]));
    assert!(matches!(
        host.intent("b", "UNREAD"),
        IntentState::Delivered { .. }
    ));
    assert_eq!(host.mirror.record.sync, SyncState::Live);
    assert_eq!(host.engine.status().blocked, None);
    host.now = host.now.max(host.timers[&TimerKind::Poll]);
    host.fire(TimerKind::Poll);
    assert!(
        host.take_gmail()
            .iter()
            .any(|(_, request)| matches!(request, GmailRequest::ListHistory { .. })),
        "the account keeps syncing"
    );
}
