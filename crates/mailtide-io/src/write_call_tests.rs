//! Direct calls that change the mailbox, which only the crate can make
//! (`Runner::direct_call`; the public `call_gmail` only reads, see
//! `tests/direct_calls.rs`): what a write's answer means when it cannot be
//! trusted, and a send's issue deadline at the gate.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mailtide::EngineConfig;
use mailtide::command::GmailRequest;
use mailtide::drafts::{DraftStatus, DraftWrite, ListedDraft};
use mailtide::event::EventKind;
use mailtide::ids::{AccountId, DraftId, MessageId, ThreadId, Timestamp};
use mailtide::quota::{Admission, Budget, Lease, Priority, QuotaUnits};
use mailtide::retry::Cooldown;
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_store::Store as _;
use mailtide_store::memory::MemoryStore;

use crate::auth::{AccessToken, TokenError, TokenSource};
use crate::blobs::ObjectBlobs;
use crate::drafts::DraftError;
use crate::quota_gate::{GateError, LocalGate, QuotaGate};
use crate::runner::{
    Access, CallError, LogAccounts, Runner, RunnerConfig, WatchMode, WriteSettings,
};
use crate::transport::{GmailTransport, TransportError};

fn account() -> AccountId {
    AccountId("alice@example.com".into())
}

fn answer(status: u16, body: &str) -> HttpResponse {
    HttpResponse {
        status,
        headers: vec![("retry-after".into(), "300".into())],
        body: body.as_bytes().to_vec(),
    }
}

#[derive(Debug, Default)]
struct Scripted {
    answers: Mutex<VecDeque<Result<HttpResponse, TransportError>>>,
    sent: Mutex<Vec<String>>,
}

impl GmailTransport for Scripted {
    async fn send(
        &self,
        request: HttpRequest,
        _token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        self.sent.lock().expect("lock").push(format!(
            "{} {}",
            request.method.as_str(),
            request.path_and_query()
        ));
        self.answers
            .lock()
            .expect("lock")
            .pop_front()
            .expect("an answer is scripted")
    }
}

/// A [`LocalGate`] that defers the first `defer_first` admissions by
/// `defer_by`, and fails every release once `fail_release` is set.
#[derive(Debug)]
struct Gate {
    inner: LocalGate,
    defer_first: AtomicU32,
    defer_by: Duration,
    fail_release: Arc<AtomicBool>,
}

impl QuotaGate for Gate {
    async fn admit(
        &self,
        account: &AccountId,
        cost: QuotaUnits,
        priority: Priority,
        now: Timestamp,
    ) -> Result<Admission, GateError> {
        let deferring = self
            .defer_first
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if deferring {
            return Ok(Admission::Deferred {
                until: now.plus(self.defer_by),
            });
        }
        self.inner.admit(account, cost, priority, now).await
    }

    async fn release(&self, lease: Lease, bytes: u64, now: Timestamp) -> Result<(), GateError> {
        if self.fail_release.load(Ordering::SeqCst) {
            return Err(GateError("the gate is down".into()));
        }
        self.inner.release(lease, bytes, now).await
    }

    async fn cool_down(&self, cooldown: Cooldown) -> Result<(), GateError> {
        self.inner.cool_down(cooldown).await
    }
}

#[derive(Debug, Default)]
struct Tokens {
    minted: AtomicU32,
    dropped: AtomicU32,
}

#[derive(Debug)]
struct Shared(Arc<Tokens>);

impl TokenSource for Shared {
    async fn token(&self, _account: &AccountId) -> Result<AccessToken, TokenError> {
        self.0.minted.fetch_add(1, Ordering::SeqCst);
        Ok(AccessToken {
            secret: "test".into(),
            expires_at: Timestamp(i64::MAX),
        })
    }

    fn invalidate(&self, _account: &AccountId) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct Setup {
    runner: Runner<MemoryStore, ObjectBlobs, Scripted, Gate, Shared>,
    fail_release: Arc<AtomicBool>,
}

impl Setup {
    fn new(
        answers: Vec<Result<HttpResponse, TransportError>>,
        defer_first: u32,
        defer_by: Duration,
        writes: WriteSettings,
    ) -> Self {
        let fail_release = Arc::new(AtomicBool::new(false));
        let gate = Gate {
            inner: LocalGate::new(Budget::GMAIL_DEFAULT),
            defer_first: AtomicU32::new(defer_first),
            defer_by,
            fail_release: Arc::clone(&fail_release),
        };
        Self {
            fail_release,
            runner: Runner::new(
                MemoryStore::tracking(&[account()]),
                ObjectBlobs::memory(),
                Scripted {
                    answers: Mutex::new(answers.into()),
                    ..Scripted::default()
                },
                gate,
                Shared(Arc::new(Tokens::default())),
                RunnerConfig {
                    access: Access::ReadWrite,
                    engine: EngineConfig::default(),
                    watch: WatchMode::Disabled,
                    log_accounts: LogAccounts::default(),
                    writes,
                },
            ),
        }
    }

    async fn call(&self, request: GmailRequest) -> Result<EventKind, CallError> {
        self.runner
            .direct_call(
                &account(),
                request,
                Priority::Foreground,
                Instant::now() + BUDGET,
            )
            .await
    }

    fn sent(&self) -> Vec<String> {
        self.runner.transport().sent.lock().expect("lock").clone()
    }
}

const BUDGET: Duration = Duration::from_secs(5);

const SENT: &str = r#"{"id": "s1", "threadId": "t1", "labelIds": ["SENT"]}"#;

fn get_m1() -> GmailRequest {
    GmailRequest::GetMessage {
        id: MessageId("m1".into()),
    }
}

fn send_r1() -> GmailRequest {
    GmailRequest::SendDraft {
        id: DraftId("r1".into()),
    }
}

/// A write answered 2xx with a body that does not decode may have changed
/// the mailbox: an unknown outcome, never a refusal. A read is refused.
#[tokio::test]
async fn an_undecodable_write_answer_is_unknown() {
    let setup = Setup::new(
        vec![Ok(answer(200, "{}")), Ok(answer(200, "{}"))],
        0,
        Duration::ZERO,
        WriteSettings::default(),
    );
    let unknown = setup.call(send_r1()).await;
    assert!(
        matches!(unknown, Err(CallError::OutcomeUnknown(_))),
        "{unknown:?}"
    );
    let read = setup.call(get_m1()).await;
    assert!(matches!(read, Err(CallError::Refused(_))), "{read:?}");
}

/// A draft in the index, saved through mailtide.
async fn saved_draft(setup: &Setup) {
    let save = DraftWrite::Saved {
        draft: ListedDraft {
            id: DraftId("r1".into()),
            message: MessageId("m1".into()),
            thread_id: ThreadId("t1".into()),
        },
        tracking_id: "tracking-0123456789".into(),
        created: true,
    };
    setup
        .runner
        .store()
        .record_draft(&account(), &save, crate::now())
        .await
        .expect("save");
}

/// Gmail answered 200 and the gate's release failed after it: the send
/// happened and is reported as sent, the row settled; never "not sent".
#[tokio::test]
async fn a_send_answered_200_is_sent_though_the_gate_release_fails() {
    let setup = Setup::new(
        vec![Ok(answer(200, SENT))],
        0,
        Duration::ZERO,
        WriteSettings::default(),
    );
    saved_draft(&setup).await;
    setup.fail_release.store(true, Ordering::SeqCst);
    let sent = setup
        .runner
        .send_draft(&account(), &DraftId("r1".into()), None, BUDGET)
        .await
        .expect("sent");
    assert_eq!(sent.message_id, MessageId("s1".into()));
    assert_eq!(setup.sent().len(), 1);
    let row = setup
        .runner
        .store()
        .draft(&account(), &DraftId("r1".into()))
        .await
        .expect("draft");
    assert_eq!(row, None, "settled as sent");
}

/// The gate would admit a send only after the latest time its request may
/// be issued (and so after the row's fence starts counting): nothing is
/// sent, the row returns to `saved`. Waiting would have sent it while a
/// retry could already take the row over.
#[tokio::test]
async fn a_send_the_gate_defers_past_its_issue_time_sends_nothing() {
    let writes = WriteSettings {
        request_timeout: Duration::from_millis(50),
        ..WriteSettings::default()
    };
    let setup = Setup::new(
        vec![Ok(answer(200, SENT))],
        1,
        Duration::from_millis(300),
        writes,
    );
    saved_draft(&setup).await;
    let store = setup.runner.store();
    let error = setup
        .runner
        .send_draft(&account(), &DraftId("r1".into()), None, BUDGET)
        .await
        .expect_err("deferred");
    assert!(matches!(error, DraftError::Deferred(_)), "{error:?}");
    assert!(setup.sent().is_empty(), "never issued");
    let row = store
        .draft(&account(), &DraftId("r1".into()))
        .await
        .expect("draft")
        .expect("row");
    assert_eq!(row.state, DraftStatus::Saved);
}
