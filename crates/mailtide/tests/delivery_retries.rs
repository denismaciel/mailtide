//! A throttled delivery, through the engine and the shared mirror rules.
//! Plan: `features/delivery-retries.toml`.
//!
//! A `Deliver` job sends one message's pending changes in one call, answered
//! whole: a rate limit cools the account down and the job sends the whole
//! change again, as often as it takes. Resending is harmless because label
//! changes are absolute.

use std::time::Duration;

use delivery_host::{Host, imported_mirror, m, modifies};
use mailtide::event::EventKind;
use mailtide::ids::{LabelId, Timestamp};
use mailtide::jobs::{JobKind, JobState};
use mailtide::labels::set;
use mailtide::outbox::{Action, IntentState, LabelDelta};
use mailtide::retry::{Cooldown, Failure, QuotaTarget, Retryable, RetryableFailure};
use mailtide::schedule::TimerKind;
use mailtide::state::SyncState;

mod delivery_host;

fn rate_limited(retry_after: Option<Duration>, detail: &str) -> Failure {
    Failure::Retryable(RetryableFailure {
        class: Retryable::AccountRateLimited,
        retry_after,
        detail: detail.into(),
    })
}

fn labels(ids: &[&str]) -> Vec<LabelId> {
    ids.iter().map(|id| LabelId((*id).into())).collect()
}

/// The account's cooldown, which must end at or after `at_least`.
fn cooldown_until(host: &Host, at_least: Timestamp) -> Timestamp {
    let Some(Cooldown { target, until }) = host.mirror.record.cooldown.clone() else {
        panic!("the account cools down");
    };
    assert_eq!(target, QuotaTarget::Account(host.mirror.account.clone()));
    assert!(until >= at_least, "{until:?} is before {at_least:?}");
    assert_eq!(host.timers.get(&TimerKind::CooldownOver), Some(&until));
    until
}

#[test]
fn a_throttled_modify_retries_the_whole_delivery_after_the_cooldown() {
    let mut host = Host::new(imported_mirror(&["a"], &["INBOX", "UNREAD"]));
    // An earlier change was delivered and awaits its confirmation.
    host.act(&["a"], Action::Archive);
    let (command, _) = host.delivery("a");
    host.handle(EventKind::LabelsModified { command });
    let archived = host.intent("a", "INBOX");
    assert!(matches!(archived, IntentState::Delivered { .. }));

    host.act(
        &["a"],
        Action::ChangeLabels(
            LabelDelta::new(labels(&["STARRED"]), labels(&["UNREAD"])).expect("a valid change"),
        ),
    );
    let (command, sent) = host.delivery("a");
    assert_eq!(
        (sent.add.clone(), sent.remove.clone()),
        (labels(&["STARRED"]), labels(&["UNREAD"]))
    );
    assert_eq!(sent.versions.len(), 2, "only the changes not delivered yet");

    // 429 with Retry-After: the account pauses at least that long, and the
    // delivery waits for it.
    let throttled_at = host.now;
    host.handle(EventKind::RequestFailed {
        command,
        failure: rate_limited(Some(Duration::from_secs(30)), "429 rateLimitExceeded"),
    });
    let until = cooldown_until(&host, Timestamp(throttled_at.0 + 30_000));
    let job = &host.mirror.jobs[&JobKind::Deliver(m("a"))];
    assert_eq!(job.failed_attempts, 1);
    assert!(matches!(job.state, JobState::Ready { due_at } if due_at.0 >= throttled_at.0 + 30_000));
    for label in ["STARRED", "UNREAD"] {
        assert_eq!(host.intent("a", label), IntentState::Pending);
    }
    assert_eq!(host.effective("a"), set(["STARRED"]));
    host.now = Timestamp(until.0 - 1);
    host.handle(EventKind::TimerFired {
        timer: TimerKind::Jobs,
    });
    assert!(
        modifies(host.take_gmail()).is_empty(),
        "nothing goes out while the account cools down"
    );

    // The whole change goes again; this time a 403 userRateLimitExceeded,
    // the other form of the same limit.
    host.advance_to_retry();
    assert!(host.now >= until);
    let (command, again) = host.delivery("a");
    assert_eq!(again, sent, "the same messages, labels and versions");
    let throttled_at = host.now;
    host.handle(EventKind::RequestFailed {
        command,
        failure: rate_limited(Some(Duration::from_secs(45)), "403 userRateLimitExceeded"),
    });
    let until = cooldown_until(&host, Timestamp(throttled_at.0 + 45_000));
    host.advance_to_retry();
    assert!(host.now >= until);
    let (command, again) = host.delivery("a");
    assert_eq!(again, sent);

    // Gmail takes it: exactly those versions are acknowledged, once; the
    // earlier delivery keeps its own acknowledgement.
    host.handle(EventKind::LabelsModified { command });
    for label in ["STARRED", "UNREAD"] {
        assert!(matches!(
            host.intent("a", label),
            IntentState::Delivered { .. }
        ));
    }
    assert_eq!(host.intent("a", "INBOX"), archived);
    assert!(!host.mirror.jobs.contains_key(&JobKind::Deliver(m("a"))));
    assert_eq!(host.mirror.record.cooldown, None);
}

#[test]
fn a_delivery_throttled_again_and_again_keeps_backing_off_and_is_never_dropped() {
    let mut host = Host::new(imported_mirror(&["a"], &["INBOX", "UNREAD"]));
    host.act(&["a"], Action::Archive);
    let (mut command, sent) = host.delivery("a");
    let mut delays = Vec::new();
    for attempt in 1..=15 {
        let failed_at = host.now;
        host.handle(EventKind::RequestFailed {
            command,
            failure: rate_limited(None, "429 rateLimitExceeded"),
        });
        let job = &host.mirror.jobs[&JobKind::Deliver(m("a"))];
        assert_eq!(job.failed_attempts, attempt);
        let JobState::Ready { due_at } = job.state else {
            panic!("attempt {attempt}: the job waits for its retry");
        };
        delays.push(due_at.0 - failed_at.0);
        assert!(
            host.timers
                .get(&TimerKind::Jobs)
                .is_some_and(|at| *at <= due_at),
            "attempt {attempt}: the retry is scheduled"
        );
        assert_eq!(host.intent("a", "INBOX"), IntentState::Pending);
        assert_eq!(host.effective("a"), set(["UNREAD"]), "still overlaid");
        host.advance_to_retry();
        let (next, again) = host.delivery("a");
        assert_eq!(again, sent);
        command = next;
    }
    // The delay grows to its cap (a minute, half of it jittered) and stays
    // there; no attempt is the last.
    assert!(delays[0] <= 1_000, "{delays:?}");
    assert!(
        delays.windows(2).take(5).all(|w| w[0] <= w[1]),
        "{delays:?}"
    );
    assert!(delays.iter().all(|d| *d <= 60_000), "{delays:?}");
    assert!(delays[7..].iter().all(|d| *d >= 30_000), "{delays:?}");
    assert_eq!(host.mirror.record.sync, SyncState::Live);
    assert_eq!(host.engine.status().blocked, None);
    assert!(
        host.mirror
            .intents
            .values()
            .all(|intent| intent.state == IntentState::Pending)
    );
}
