//! Tests ported by name from Zero's `gmail-quota-auth.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.
//!
//! Zero gates each HTTP call in its Google auth client; mailtide's runner
//! admits every Gmail command through the [`QuotaGate`] before sending it.
//! These tests drive the runner against a fake Gmail and a recording gate.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::EngineConfig;
use mailtide::ids::{AccountId, Timestamp};
use mailtide::quota::{Admission, Budget, Lease, Priority, QuotaUnits};
use mailtide::retry::{Cooldown, Failure, QuotaTarget};
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, TokenSource};
use mailtide_io::blobs::ObjectBlobs;
use mailtide_io::quota_gate::{GateError, LocalGate, QuotaGate};
use mailtide_io::runner::{RunOutcome, Runner, RunnerConfig, StopReason, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::memory::MemoryStore;
use serde_json::json;

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
    AccountId::parse("user@example.com").expect("account")
}

fn ok(body: &serde_json::Value) -> HttpResponse {
    HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: body.to_string().into_bytes(),
    }
}

/// One message in the inbox; every response can be overridden.
#[derive(Debug, Default)]
struct FakeGmail {
    sent: Mutex<Vec<String>>,
    answer: Option<HttpResponse>,
}

impl GmailTransport for FakeGmail {
    async fn send(
        &self,
        request: HttpRequest,
        _token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        let query: BTreeMap<&str, &str> = request
            .query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let path = request.path.trim_start_matches("/gmail/v1/users/me/");
        let format = query.get("format").copied().unwrap_or_default();
        self.sent.lock().expect("lock").push(format!(
            "{path}{}{format}",
            if format.is_empty() { "" } else { "?" }
        ));
        if let Some(answer) = &self.answer {
            return Ok(answer.clone());
        }
        Ok(match path {
            "profile" => ok(&json!({"emailAddress": "user@example.com", "historyId": "10"})),
            "labels" => {
                ok(&json!({"labels": [{"id": "INBOX", "name": "INBOX", "type": "system"}]}))
            }
            "history" => ok(&json!({"historyId": "10"})),
            "messages" => match query.get("labelIds") {
                None | Some(&"INBOX") => ok(&json!({"messages": [{"id": "m1", "threadId": "t1"}]})),
                Some(_) => ok(&json!({})),
            },
            "messages/m1" => {
                let mut body = json!({
                    "id": "m1", "threadId": "t1", "labelIds": ["INBOX"], "historyId": "10",
                    "internalDate": "1700000000000", "sizeEstimate": 40,
                });
                if format == "raw" {
                    body["raw"] = json!(URL_SAFE_NO_PAD.encode("Subject: hi\r\n\r\nbody\r\n"));
                }
                ok(&body)
            }
            other => panic!("unexpected request {other}"),
        })
    }
}

#[derive(Debug, Default)]
struct GateLog {
    admitted: Vec<(AccountId, QuotaUnits, Priority)>,
    granted: Vec<Lease>,
    released: Vec<Lease>,
    cooldowns: Vec<Cooldown>,
}

/// A [`LocalGate`] that records what passes through it, or denies everything.
#[derive(Debug)]
struct RecordingGate {
    inner: LocalGate,
    deny: bool,
    log: Arc<Mutex<GateLog>>,
}

impl RecordingGate {
    fn new(deny: bool) -> Self {
        Self {
            inner: LocalGate::new(Budget::GMAIL_DEFAULT),
            deny,
            log: Arc::default(),
        }
    }
}

impl QuotaGate for RecordingGate {
    async fn admit(
        &self,
        account: &AccountId,
        cost: QuotaUnits,
        priority: Priority,
        now: Timestamp,
    ) -> Result<Admission, GateError> {
        self.log
            .lock()
            .expect("lock")
            .admitted
            .push((account.clone(), cost, priority));
        if self.deny {
            return Ok(Admission::Deferred {
                until: now.plus(Duration::from_mins(1)),
            });
        }
        let admission = self.inner.admit(account, cost, priority, now).await?;
        if let Admission::Granted(lease) = admission {
            self.log.lock().expect("lock").granted.push(lease);
        }
        Ok(admission)
    }

    async fn release(&self, lease: Lease, bytes: u64, now: Timestamp) -> Result<(), GateError> {
        self.log.lock().expect("lock").released.push(lease);
        self.inner.release(lease, bytes, now).await
    }

    async fn cool_down(&self, cooldown: Cooldown) -> Result<(), GateError> {
        self.log
            .lock()
            .expect("lock")
            .cooldowns
            .push(cooldown.clone());
        self.inner.cool_down(cooldown).await
    }
}

/// Tokens minted outside the Gmail budget, counted.
#[derive(Debug, Default)]
struct CountingTokens(Arc<AtomicU32>);

impl TokenSource for CountingTokens {
    async fn token(&self, _account: &AccountId) -> Result<AccessToken, Failure> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(AccessToken {
            secret: "test".into(),
            expires_at: Timestamp(i64::MAX),
        })
    }
}

struct TestRunner {
    runner: Runner<MemoryStore, ObjectBlobs, FakeGmail, RecordingGate, CountingTokens>,
    gate: Arc<Mutex<GateLog>>,
    tokens: Arc<AtomicU32>,
}

impl TestRunner {
    fn gate(&self) -> MutexGuard<'_, GateLog> {
        self.gate.lock().expect("lock")
    }

    fn tokens(&self) -> u32 {
        self.tokens.load(Ordering::SeqCst)
    }

    fn sent(&self) -> Vec<String> {
        self.runner.transport().sent.lock().expect("lock").clone()
    }
}

fn runner(gmail: FakeGmail, gate: RecordingGate) -> TestRunner {
    let tokens = CountingTokens::default();
    TestRunner {
        gate: Arc::clone(&gate.log),
        tokens: Arc::clone(&tokens.0),
        runner: Runner::new(
            MemoryStore::new(),
            ObjectBlobs::memory(),
            gmail,
            gate,
            tokens,
            RunnerConfig {
                engine: EngineConfig::default(),
                watch: WatchMode::Disabled,
            },
        ),
    }
}

async fn run(runner: &TestRunner) -> RunOutcome {
    runner
        .runner
        .run(&account(), Duration::from_secs(30))
        .await
        .expect("run")
}

/// Zero: gmail-quota-auth > Gmail transport quota gate > releases in-flight capacity after a successful request
#[test]
fn gmail_transport_quota_gate_releases_in_flight_capacity_after_a_successful_request() {
    block_on(async {
        let runner = runner(FakeGmail::default(), RecordingGate::new(false));
        let outcome = run(&runner).await;
        assert!(
            matches!(outcome.stop, StopReason::Idle { .. }),
            "{:?}",
            outcome.stop
        );
        let log = runner.gate();
        assert!(!log.granted.is_empty());
        assert_eq!(log.released, log.granted, "every lease released, in order");
    });
}

/// Zero: gmail-quota-auth > Gmail transport quota gate > charges every SDK call with its method cost and ambient priority
#[test]
fn gmail_transport_quota_gate_charges_every_sdk_call_with_its_method_cost_and_ambient_priority() {
    block_on(async {
        let runner = runner(FakeGmail::default(), RecordingGate::new(false));
        run(&runner).await;
        let sent = runner.sent();
        let log = runner.gate();
        // No modify groups here, so each admission sends exactly one request.
        assert_eq!(log.admitted.len(), sent.len(), "{sent:?}");
        for ((who, cost, priority), request) in log.admitted.iter().zip(&sent) {
            assert_eq!(who, &account());
            let expected = match request.as_str() {
                "profile" | "labels" => 1,
                "history" => 2,
                "messages" => 5,
                "messages/m1?minimal" => 20,
                "messages/m1?raw" => 60,
                other => panic!("unexpected request {other}"),
            };
            assert_eq!(*cost, QuotaUnits(expected), "{request}");
            assert_ne!(*priority, Priority::Foreground, "no user is waiting");
            if request == "messages/m1?raw" {
                assert_eq!(
                    *priority,
                    Priority::Backfill,
                    "bootstrap imports are backfill"
                );
            }
        }
        assert!(sent.iter().any(|r| r == "messages/m1?raw"), "{sent:?}");
    });
}

/// Zero: gmail-quota-auth > Gmail transport quota gate > does not send an HTTP request when shared capacity is exhausted
#[test]
fn gmail_transport_quota_gate_does_not_send_an_http_request_when_shared_capacity_is_exhausted() {
    block_on(async {
        let runner = runner(FakeGmail::default(), RecordingGate::new(true));
        let outcome = run(&runner).await;
        assert!(!runner.gate().admitted.is_empty());
        assert_eq!(runner.sent(), Vec::<String>::new());
        assert_eq!(runner.tokens(), 0, "no token minted");
        assert!(
            matches!(outcome.stop, StopReason::Idle { next: Some(_) }),
            "deferred, not failed: {:?}",
            outcome.stop
        );
    });
}

/// Zero: gmail-quota-auth > Gmail transport quota gate > leaves People and OAuth requests outside the Gmail budget
#[test]
fn gmail_transport_quota_gate_leaves_people_and_oauth_requests_outside_the_gmail_budget() {
    block_on(async {
        // Mailtide makes no People calls; tokens come from the host's
        // TokenSource, which is never charged against the Gmail budget.
        let runner = runner(FakeGmail::default(), RecordingGate::new(false));
        run(&runner).await;
        let tokens = runner.tokens();
        assert!(tokens > 0);
        assert_eq!(runner.gate().admitted.len(), runner.sent().len());
    });
}

/// Zero: gmail-quota-auth > Gmail transport quota gate > keeps priority isolated between concurrent asynchronous requests
#[test]
#[ignore = "zero-na: mailtide has no ambient (AsyncLocalStorage) priority; each GmailCall carries its priority explicitly to QuotaGate::admit"]
fn gmail_transport_quota_gate_keeps_priority_isolated_between_concurrent_asynchronous_requests() {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: gmail-quota-auth > Gmail transport quota gate > publishes provider Retry-After to a shared mailbox cooldown
#[test]
fn gmail_transport_quota_gate_publishes_provider_retry_after_to_a_shared_mailbox_cooldown() {
    block_on(async {
        let limited = HttpResponse {
        status: 429,
        headers: vec![("retry-after".into(), "300".into())],
        body: br#"{"error":{"code":429,"message":"Rate limited","errors":[{"reason":"userRateLimitExceeded"}]}}"#.to_vec(),
    };
        let runner = runner(
            FakeGmail {
                answer: Some(limited),
                ..FakeGmail::default()
            },
            RecordingGate::new(false),
        );
        let before = mailtide_io::now();
        let outcome = run(&runner).await;
        let floor = before.plus(Duration::from_secs(290));
        match outcome.stop {
            StopReason::CoolingDown { until } => assert!(until > floor, "{until:?}"),
            other => panic!("expected a cooldown, got {other:?}"),
        }
        let log = runner.gate();
        let shared = log
            .cooldowns
            .iter()
            .find(|c| c.target == QuotaTarget::Account(account()))
            .unwrap_or_else(|| panic!("no mailbox cooldown published to the gate: {log:?}"));
        assert!(shared.until > floor, "{shared:?}");
    });
}
