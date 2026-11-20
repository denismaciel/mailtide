//! Auditing through the runner, with a clock and a programmable quota gate.

pub mod label_support;

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};
use std::time::Duration;

use label_support::{BUDGET, FakeGmail, Fault, GateState, account, runner_over, with_defaults};
use mailtide::ids::{HistoryId, MessageId, Timestamp};
use mailtide::outbox::{Action, ActionRequest, StarState};
use mailtide::quota::{Priority, QuotaUnits};
use mailtide::retry::QuotaTarget;
use mailtide_io::runner::{Access, Notified, StopReason};
use mailtide_store::{Store as _, sqlite::SqliteStore};

struct AuditHarness {
    runner: mailtide_io::runner::Runner<
        SqliteStore,
        mailtide_io::blobs::ObjectBlobs,
        FakeGmail,
        label_support::RecordingGate,
        mailtide_io::auth::StaticTokens,
    >,
    clock: Arc<AtomicI64>,
    gate: Arc<GateState>,
}

impl AuditHarness {
    async fn ready() -> Self {
        let store = SqliteStore::connect("sqlite::memory:")
            .await
            .expect("store");
        store.migrate().await.expect("migrate");
        store.track(&account(), 1).await.expect("track");
        let clock = Arc::new(AtomicI64::new(1_700_000_000_000));
        let gate = Arc::new(GateState::default());
        let gmail = FakeGmail::default();
        with_defaults(&gmail);
        gmail.mailbox().add("m1", &["INBOX"]);
        let runner = runner_over(store, gmail, Access::ReadWrite, &clock, &gate);
        for _ in 0..3 {
            runner
                .run(&account(), BUDGET)
                .await
                .expect("finish first audit");
        }
        clock.fetch_add(86_400_000, Ordering::SeqCst);
        Self {
            runner,
            clock,
            gate,
        }
    }

    fn listings(&self) -> usize {
        self.runner
            .transport()
            .mailbox()
            .requests
            .iter()
            .filter(|request| request.as_str() == "GET messages")
            .count()
    }
}

#[tokio::test]
async fn audit_retry_after_survives_a_push_and_explicit_claims() {
    for (status, reason, target) in [
        (403, "rateLimitExceeded", QuotaTarget::Project),
        (
            429,
            "userRateLimitExceeded",
            QuotaTarget::Account(account()),
        ),
    ] {
        let harness = AuditHarness::ready().await;
        let runner = &harness.runner;
        let now = Timestamp(harness.clock.load(Ordering::SeqCst));
        let until = now.plus(Duration::from_secs(300));
        runner
            .transport()
            .mailbox()
            .fail_next("GET messages", Fault::RateLimit(status, reason, 300));
        let failed = runner.run(&account(), BUDGET).await.expect("limited audit");
        assert_eq!(failed.stop, StopReason::CoolingDown { until });
        let published = harness.gate.cooldowns.lock().expect("gate").clone();
        assert!(
            published
                .iter()
                .any(|cooldown| cooldown.target == target && cooldown.until == until)
        );

        let snapshot = runner
            .store()
            .claim(&account(), mailtide::ids::ENGINE_VERSION)
            .await
            .expect("restart");
        assert_eq!(snapshot.audit_schedule.retry_at, Some(until));
        let sent = harness.listings();
        harness.clock.fetch_add(299_000, Ordering::SeqCst);
        assert!(matches!(
            runner
                .notify(&account(), HistoryId(10_000), BUDGET)
                .await
                .expect("push"),
            Notified::Ran(_)
        ));
        runner
            .run(&account(), BUDGET)
            .await
            .expect("explicit claim");
        assert_eq!(harness.listings(), sent);
        harness.clock.fetch_add(1_000, Ordering::SeqCst);
        runner
            .run(&account(), BUDGET)
            .await
            .expect("retry at deadline");
        assert_eq!(harness.listings(), sent + 1);
    }
}

#[tokio::test]
async fn live_confirmation_rechecks_a_deferred_backfill_verify() {
    let harness = AuditHarness::ready().await;
    let runner = &harness.runner;
    // Gmail temporarily omits m1, so the sweep queues its verification.
    runner
        .transport()
        .mailbox()
        .fail_next("GET messages", Fault::Body(r#"{"messages":[]}"#));
    runner.run(&account(), BUDGET).await.expect("list");
    *harness.gate.backfill_verify_deferral.lock().expect("gate") = Some(Duration::from_secs(300));
    let swept = runner.run(&account(), BUDGET).await.expect("sweep");
    assert_eq!(swept.report.status.expect("status").audit.found_unlisted, 1);
    assert!(
        harness
            .gate
            .admitted
            .lock()
            .expect("gate")
            .contains(&(QuotaUnits(20), Priority::Backfill))
    );
    runner
        .act(
            &account(),
            ActionRequest::Messages {
                ids: vec![MessageId("m1".into())],
                action: Action::SetStar(StarState::Starred),
            },
            BUDGET,
        )
        .await
        .expect("deliver and confirm");
    assert!(
        harness
            .gate
            .admitted
            .lock()
            .expect("gate")
            .contains(&(QuotaUnits(20), Priority::Live))
    );
    assert!(
        runner
            .transport()
            .mailbox()
            .requests
            .iter()
            .any(|request| request == "GET messages/m1")
    );
}

#[tokio::test]
async fn an_unusable_continuation_reports_abandonment_and_waits_after_cleanup() {
    use mailtide::audit::AuditAbandonment;
    for (fault, reason) in [
        (Fault::Undecodable, AuditAbandonment::MalformedPage),
        (
            Fault::Status(400, "invalidArgument"),
            AuditAbandonment::Rejected,
        ),
    ] {
        let harness = AuditHarness::ready().await;
        let runner = &harness.runner;
        let now = Timestamp(harness.clock.load(Ordering::SeqCst));
        runner.transport().mailbox().fail_next(
            "GET messages",
            Fault::Body(r#"{"messages":[{"id":"m1"}],"nextPageToken":"next"}"#),
        );
        let page = runner.run(&account(), BUDGET).await.expect("first page");
        assert_eq!(page.stop, StopReason::Idle { next: Some(now) });
        runner
            .transport()
            .mailbox()
            .fail_next("GET messages", fault);
        let abandoned = runner
            .run(&account(), BUDGET)
            .await
            .expect("unusable continuation");
        assert_eq!(
            abandoned.report.status.expect("status").audit.abandoned,
            Some(reason)
        );
        assert_eq!(abandoned.stop, StopReason::Idle { next: Some(now) });
        let cleanup = runner.run(&account(), BUDGET).await.expect("cleanup");
        let next = now.plus(Duration::from_secs(60));
        assert_eq!(cleanup.stop, StopReason::Idle { next: Some(next) });
        let sent = harness.listings();
        runner.run(&account(), BUDGET).await.expect("early claim");
        assert_eq!(sent, harness.listings());
        harness.clock.fetch_add(60_000, Ordering::SeqCst);
        runner
            .run(&account(), BUDGET)
            .await
            .expect("replacement audit");
        assert_eq!(sent + 1, harness.listings());
    }
}

#[tokio::test]
async fn an_audit_quota_deferral_survives_early_claims_after_the_gate_reopens() {
    let harness = AuditHarness::ready().await;
    let runner = &harness.runner;
    let sent = harness.listings();
    let now = Timestamp(harness.clock.load(Ordering::SeqCst));
    let delay = Duration::from_secs(300);
    *harness.gate.audit_deferral.lock().expect("gate") = Some(delay);
    let deferred = runner
        .run(&account(), BUDGET)
        .await
        .expect("deferred audit");
    assert_eq!(
        deferred.stop,
        StopReason::Idle {
            next: Some(now.plus(delay))
        }
    );
    assert_eq!(harness.listings(), sent);
    *harness.gate.audit_deferral.lock().expect("gate") = None;
    harness.clock.fetch_add(299_000, Ordering::SeqCst);
    runner
        .run(&account(), BUDGET)
        .await
        .expect("early explicit claim");
    assert_eq!(harness.listings(), sent);
    harness.clock.fetch_add(1_000, Ordering::SeqCst);
    runner
        .run(&account(), BUDGET)
        .await
        .expect("retry at deadline");
    assert_eq!(harness.listings(), sent + 1);
}

#[tokio::test]
async fn a_late_rate_limit_updates_the_gate_and_store_for_audit_and_history() {
    for (suffix, request) in [("/messages", "GET messages"), ("/history", "GET history")] {
        let harness = AuditHarness::ready().await;
        let runner = &harness.runner;
        let now = Timestamp(harness.clock.load(Ordering::SeqCst));
        let until = now.plus(Duration::from_secs(300));
        runner
            .transport()
            .delay_responses(suffix, Duration::from_millis(400));
        runner
            .transport()
            .mailbox()
            .fail_next(request, Fault::RateLimit(429, "userRateLimitExceeded", 300));
        let budget = Duration::from_millis(200);
        let outcome = if suffix == "/history" {
            let Notified::Ran(run) = runner
                .notify(&account(), HistoryId(10_000), budget)
                .await
                .expect("history push")
            else {
                panic!("the history push should run");
            };
            *run
        } else {
            runner.run(&account(), budget).await.expect("audit run")
        };
        assert_eq!(outcome.stop, StopReason::BudgetExhausted);
        let published = harness.gate.cooldowns.lock().expect("gate").clone();
        assert!(published.iter().any(|cooldown| {
            cooldown.target == QuotaTarget::Account(account()) && cooldown.until == until
        }));
        let snapshot = runner
            .store()
            .claim(&account(), mailtide::ids::ENGINE_VERSION)
            .await
            .expect("restart");
        assert_eq!(
            snapshot.cooldown.map(|cooldown| cooldown.until),
            Some(until)
        );
        if suffix == "/messages" {
            assert_eq!(snapshot.audit_schedule.retry_at, Some(until));
        }
        let sent = runner.transport().mailbox().requests.len();
        let push = runner
            .notify(&account(), HistoryId(20_000), BUDGET)
            .await
            .expect("push before retry");
        assert!(
            matches!(push, Notified::Ran(run) if run.stop == StopReason::CoolingDown { until })
        );
        assert_eq!(runner.transport().mailbox().requests.len(), sent);
    }
}

#[tokio::test]
async fn an_audit_page_finishes_its_store_step_after_the_run_budget() {
    let harness = AuditHarness::ready().await;
    let runner = &harness.runner;
    runner
        .transport()
        .delay_responses("/messages", Duration::from_millis(400));
    runner.transport().mailbox().fail_next(
        "GET messages",
        Fault::Body(r#"{"messages":[{"id":"m1"}],"nextPageToken":"next"}"#),
    );
    let outcome = runner
        .run(&account(), Duration::from_millis(200))
        .await
        .expect("slow page");
    assert_eq!(outcome.stop, StopReason::BudgetExhausted);
    let snapshot = runner
        .store()
        .claim(&account(), mailtide::ids::ENGINE_VERSION)
        .await
        .expect("restart");
    assert_eq!(
        snapshot.audit_progress,
        Some(mailtide::audit::AuditProgress::Listing(
            mailtide::ids::PageToken("next".into())
        ))
    );
}

#[tokio::test]
async fn a_persistently_failing_continuation_is_abandoned_with_its_retry_floor() {
    use mailtide::audit::{AUDIT_FAILURE_LIMIT, AuditAbandonment, AuditProgress};

    let harness = AuditHarness::ready().await;
    let runner = &harness.runner;
    runner.transport().mailbox().fail_next(
        "GET messages",
        Fault::Body(r#"{"messages":[{"id":"m1"}],"nextPageToken":"next"}"#),
    );
    runner.run(&account(), BUDGET).await.expect("first page");

    for attempt in 1..=AUDIT_FAILURE_LIMIT {
        let now = Timestamp(harness.clock.load(Ordering::SeqCst));
        let floor = now.plus(Duration::from_secs(300));
        runner
            .transport()
            .mailbox()
            .fail_next("GET messages", Fault::RateLimit(503, "backendError", 300));
        let failure = runner.run(&account(), BUDGET).await.expect("failed page");
        let snapshot = runner
            .store()
            .claim(&account(), mailtide::ids::ENGINE_VERSION)
            .await
            .expect("restart");
        let retry_at = snapshot.audit_schedule.retry_at.expect("durable retry");
        assert!(retry_at >= floor);
        let abandoned = failure.report.status.expect("status").audit.abandoned;
        if attempt < AUDIT_FAILURE_LIMIT {
            assert_eq!(
                snapshot.audit_progress,
                Some(AuditProgress::Listing(mailtide::ids::PageToken(
                    "next".into()
                )))
            );
            assert_eq!(snapshot.audit_schedule.failures(), attempt);
            assert_eq!(abandoned, None);
            harness.clock.store(retry_at.0, Ordering::SeqCst);
        } else {
            assert_eq!(abandoned, Some(AuditAbandonment::RetryLimit));
            assert_eq!(
                snapshot.audit_progress,
                Some(AuditProgress::Cleanup {
                    after: None,
                    complete: false
                })
            );
            assert_eq!(snapshot.audit_schedule.failures(), 0);
            let cleanup = runner.run(&account(), BUDGET).await.expect("cleanup");
            assert_eq!(
                cleanup.stop,
                StopReason::Idle {
                    next: Some(retry_at)
                }
            );
            let sent = harness.listings();
            runner.run(&account(), BUDGET).await.expect("early claim");
            assert_eq!(harness.listings(), sent);
        }
    }
}
