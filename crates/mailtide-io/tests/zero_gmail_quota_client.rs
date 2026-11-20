//! Tests ported by name from Zero's `gmail-quota-client.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mailtide::EngineConfig;
use mailtide::ids::{AccountId, Timestamp};
use mailtide::quota::{Admission, Budget, Lease, Priority, QuotaUnits};
use mailtide::retry::Cooldown;
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, StaticTokens};
use mailtide_io::blobs::ObjectBlobs;
use mailtide_io::quota_gate::{GateError, LocalGate, QuotaGate};
use mailtide_io::runner::{Runner, RunnerConfig, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::memory::MemoryStore;

/// Runs a test body on a single-threaded runtime (plain `#[test]`, so
/// `scripts/zero_tests.py` counts it).
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(future)
}

fn account() -> AccountId {
    AccountId::parse("USER@example.com").expect("account")
}

async fn admit(gate: &LocalGate, units: u32, priority: Priority, at: Timestamp) -> Option<Lease> {
    match gate
        .admit(&account(), QuotaUnits(units), priority, at)
        .await
        .expect("gate")
    {
        Admission::Granted(lease) => Some(lease),
        Admission::Deferred { .. } => None,
    }
}

/// Zero: gmail-quota-client > Gmail request admission with persisted quota storage > defers saturated backfill without sending it, admits live and foreground work, then resumes
#[test]
fn gmail_request_admission_with_persisted_quota_storage_defers_saturated_backfill_without_sending_it_admits_live_and_foreground_work_then_resumes()
 {
    block_on(async {
        // That a deferral sends nothing is covered at the runner level in
        // zero_gmail_quota_auth; here, the gate's decisions over a 400-unit
        // minute with 40-unit requests.
        let gate = LocalGate::new(Budget {
            account_per_minute: 400,
            project_per_minute: 400,
            ..Budget::GMAIL_DEFAULT
        });
        let t0 = Timestamp(0);
        let run = async |priority| match admit(&gate, 40, priority, t0).await {
            Some(lease) => {
                gate.release(lease, 0, t0).await.expect("release");
                true
            }
            None => false,
        };
        for _ in 0..7 {
            assert!(run(Priority::Backfill).await);
        }
        assert!(!run(Priority::Backfill).await, "backfill stops at 70%");
        assert!(run(Priority::Live).await);
        assert!(run(Priority::Live).await);
        assert!(!run(Priority::Live).await, "live stops at 90%");
        assert!(run(Priority::Foreground).await);
        assert!(!run(Priority::Foreground).await, "the minute is spent");
        let Admission::Deferred { until } = gate
            .admit(&account(), QuotaUnits(40), Priority::Backfill, t0)
            .await
            .expect("gate")
        else {
            panic!("deferred");
        };
        // No lease leaked: every slot is free for zero-cost foreground work.
        let mut leases = Vec::new();
        for _ in 0..Budget::GMAIL_DEFAULT.account_concurrency {
            leases.push(
                admit(&gate, 0, Priority::Foreground, t0)
                    .await
                    .expect("a free slot"),
            );
        }
        for lease in leases {
            gate.release(lease, 0, t0).await.expect("release");
        }
        assert_eq!(until, Timestamp(60_000));
        assert!(admit(&gate, 40, Priority::Backfill, until).await.is_some());
    });
}

/// Every request fails before reaching Gmail.
#[derive(Debug)]
struct Offline;

impl GmailTransport for Offline {
    async fn send(
        &self,
        _request: HttpRequest,
        _token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        Err(TransportError::NotSent("Network failure".into()))
    }
}

#[derive(Debug, Default)]
struct Leases {
    granted: Vec<Lease>,
    released: Vec<Lease>,
}

#[derive(Debug)]
struct RecordingGate(LocalGate, Arc<Mutex<Leases>>);

impl QuotaGate for RecordingGate {
    async fn admit(
        &self,
        account: &AccountId,
        cost: QuotaUnits,
        priority: Priority,
        now: Timestamp,
    ) -> Result<Admission, GateError> {
        let admission = self.0.admit(account, cost, priority, now).await?;
        if let Admission::Granted(lease) = admission {
            self.1.lock().expect("lock").granted.push(lease);
        }
        Ok(admission)
    }

    async fn release(&self, lease: Lease, bytes: u64, now: Timestamp) -> Result<(), GateError> {
        self.1.lock().expect("lock").released.push(lease);
        self.0.release(lease, bytes, now).await
    }

    async fn cool_down(&self, cooldown: Cooldown) -> Result<(), GateError> {
        self.0.cool_down(cooldown).await
    }
}

/// Zero: gmail-quota-client > Gmail request admission with persisted quota storage > keeps live and foreground slots available during backfill and releases leases on failure
#[test]
fn gmail_request_admission_with_persisted_quota_storage_keeps_live_and_foreground_slots_available_during_backfill_and_releases_leases_on_failure()
 {
    block_on(async {
        // One-unit requests isolate concurrency from the unit budget; nothing is
        // released until the end, as if all were in flight.
        let gate = LocalGate::new(Budget::GMAIL_DEFAULT);
        let t0 = Timestamp(0);
        let mut held = Vec::new();
        for (priority, slots) in [
            (Priority::Backfill, 4),
            (Priority::Live, 2),
            (Priority::Foreground, 2),
        ] {
            for _ in 0..slots {
                held.push(admit(&gate, 1, priority, t0).await.expect("a slot"));
            }
            assert_eq!(
                admit(&gate, 1, priority, t0).await,
                None,
                "{priority:?} slots are bounded"
            );
        }
        assert_eq!(held.len(), 8);
        gate.release(held.remove(0), 0, t0).await.expect("release");
        assert!(
            admit(&gate, 1, Priority::Foreground, t0).await.is_some(),
            "a freed slot admits waiting foreground work"
        );

        // A request that fails still releases its lease.
        let leases = Arc::new(Mutex::new(Leases::default()));
        let tokens = StaticTokens(BTreeMap::from([(
            account(),
            AccessToken {
                secret: "test".into(),
                expires_at: Timestamp(i64::MAX),
            },
        )]));
        let runner = Runner::new(
            MemoryStore::tracking(&[account()]),
            ObjectBlobs::memory(),
            Offline,
            RecordingGate(LocalGate::new(Budget::GMAIL_DEFAULT), Arc::clone(&leases)),
            tokens,
            RunnerConfig {
                engine: EngineConfig::default(),
                access: mailtide_io::runner::Access::ReadWrite,
                watch: WatchMode::Disabled,
                log_accounts: mailtide_io::runner::LogAccounts::default(),
                drafts: mailtide_io::drafts::DraftSettings::default(),
            },
        );
        runner
            .run(&account(), Duration::from_secs(30))
            .await
            .expect("a failed request is not a run error");
        let leases = leases.lock().expect("lock");
        assert!(!leases.granted.is_empty());
        assert_eq!(leases.released, leases.granted);
    });
}
