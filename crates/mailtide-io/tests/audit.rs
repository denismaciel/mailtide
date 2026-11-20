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
