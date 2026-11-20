//! Direct calls: one Gmail request outside the engine, through the quota
//! gate. The public `Runner::call_gmail` only reads (drift checks,
//! searches); the features' writes (label edits, drafts, purges) take the
//! same path inside the crate, after their own checks.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mailtide::EngineConfig;
use mailtide::command::GmailRequest;
use mailtide::event::EventKind;
use mailtide::ids::{AccountId, LabelId, MessageId, Timestamp};
use mailtide::outbox::ModifyGroup;
use mailtide::quota::{Admission, Budget, Lease, Priority, QuotaUnits};
use mailtide::retry::{AccountProblem, Blocking, Cooldown, QuotaTarget, Retryable};
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, TokenError, TokenSource};
use mailtide_io::blobs::ObjectBlobs;
use mailtide_io::quota_gate::{GateError, LocalGate, QuotaGate};
use mailtide_io::runner::{
    Access, CallError, LogAccounts, QuotaDeferred, Runner, RunnerConfig, WatchMode,
};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::Store as _;
use mailtide_store::memory::MemoryStore;

fn account() -> AccountId {
    AccountId::parse("user@example.com").expect("account")
}

fn answer(status: u16, body: &str) -> HttpResponse {
    HttpResponse {
        status,
        headers: vec![("retry-after".into(), "300".into())],
        body: body.as_bytes().to_vec(),
    }
}

fn refusal(status: u16, reason: &str) -> HttpResponse {
    answer(
        status,
        &format!(
            r#"{{"error":{{"code":{status},"message":"x","errors":[{{"reason":"{reason}"}}]}}}}"#
        ),
    )
}

fn fetched() -> HttpResponse {
    answer(
        200,
        r#"{"id":"m1","threadId":"t1","labelIds":["INBOX"],"historyId":"10"}"#,
    )
}

/// Answers each request with the next scripted answer, recording the
/// request line.
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

#[derive(Debug, Default)]
struct GateLog {
    admitted: Vec<(QuotaUnits, Priority)>,
    released: u32,
    cooldowns: Vec<Cooldown>,
}

/// A [`LocalGate`] that records what passes, deferring the first
/// `defer_first` admissions by `defer_by`.
#[derive(Debug)]
struct Gate {
    inner: LocalGate,
    defer_first: AtomicU32,
    defer_by: Duration,
    log: Arc<Mutex<GateLog>>,
}

impl QuotaGate for Gate {
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
            .push((cost, priority));
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
        self.log.lock().expect("lock").released += 1;
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

/// Counts tokens minted and dropped.
#[derive(Debug, Default)]
struct Tokens {
    minted: AtomicU32,
    dropped: AtomicU32,
}

/// The runner's handle on [`Tokens`].
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
    gate: Arc<Mutex<GateLog>>,
    tokens: Arc<Tokens>,
}

impl Setup {
    fn new(
        access: Access,
        answers: Vec<Result<HttpResponse, TransportError>>,
        defer_first: u32,
        defer_by: Duration,
    ) -> Self {
        let gate = Gate {
            inner: LocalGate::new(Budget::GMAIL_DEFAULT),
            defer_first: AtomicU32::new(defer_first),
            defer_by,
            log: Arc::default(),
        };
        let tokens = Arc::new(Tokens::default());
        Self {
            gate: Arc::clone(&gate.log),
            tokens: Arc::clone(&tokens),
            runner: Runner::new(
                MemoryStore::tracking(&[account()]),
                ObjectBlobs::memory(),
                Scripted {
                    answers: Mutex::new(answers.into()),
                    ..Scripted::default()
                },
                gate,
                Shared(tokens),
                RunnerConfig {
                    access,
                    engine: EngineConfig::default(),
                    watch: WatchMode::Disabled,
                    log_accounts: LogAccounts::default(),
                    writes: mailtide_io::runner::WriteSettings::default(),
                },
            ),
        }
    }

    fn answering(answers: Vec<Result<HttpResponse, TransportError>>) -> Self {
        Self::new(Access::ReadWrite, answers, 0, Duration::ZERO)
    }

    async fn call(&self, request: GmailRequest, budget: Duration) -> Result<EventKind, CallError> {
        self.runner
            .call_gmail(
                &account(),
                request,
                Priority::Foreground,
                Instant::now() + budget,
            )
            .await
    }

    fn sent(&self) -> Vec<String> {
        self.runner.transport().sent.lock().expect("lock").clone()
    }

    fn admitted(&self) -> Vec<(QuotaUnits, Priority)> {
        self.gate.lock().expect("lock").admitted.clone()
    }
}

fn get_m1() -> GmailRequest {
    GmailRequest::GetMessage {
        id: MessageId("m1".into()),
    }
}

fn archive_m1() -> GmailRequest {
    GmailRequest::Modify(ModifyGroup {
        messages: vec![MessageId("m1".into())],
        add: Vec::new(),
        remove: vec![LabelId("INBOX".into())],
        versions: Vec::new(),
    })
}

const BUDGET: Duration = Duration::from_secs(5);

#[tokio::test]
async fn a_call_is_admitted_at_its_cost_and_priority_and_answered() {
    let setup = Setup::answering(vec![Ok(fetched())]);
    let answer = setup.call(get_m1(), BUDGET).await.expect("answered");
    assert!(
        matches!(&answer, EventKind::MessageFetched { message, .. } if message.id.0 == "m1"),
        "{answer:?}"
    );
    assert_eq!(setup.admitted(), [(QuotaUnits(20), Priority::Foreground)]);
    assert_eq!(setup.gate.lock().expect("lock").released, 1);
    assert_eq!(
        setup.sent(),
        ["GET /gmail/v1/users/me/messages/m1?format=minimal"]
    );
}

#[tokio::test]
async fn a_call_claims_nothing_and_writes_nothing() {
    let setup = Setup::answering(vec![Ok(fetched()), Ok(refusal(400, "invalidArgument"))]);
    let before = setup
        .runner
        .store()
        .status(&account())
        .await
        .expect("status");
    setup.call(get_m1(), BUDGET).await.expect("answered");
    let refused = setup.call(get_m1(), BUDGET).await;
    assert!(matches!(refused, Err(CallError::Refused(_))), "{refused:?}");
    let after = setup
        .runner
        .store()
        .status(&account())
        .await
        .expect("status");
    assert_eq!(before, after, "no claim, no commit, no epoch");
}

/// The public direct call only reads: a write is refused before any token
/// or quota, read-write or read-only, so a host cannot bypass the checks of
/// the features that write (they use the crate's `direct_call`).
#[tokio::test]
async fn the_public_call_refuses_every_write_before_any_token_or_quota() {
    use mailtide::drafts::Rfc822;
    use mailtide::gmail::{LabelListVisibility, LabelPatch, MessageListVisibility, NewLabel};
    use mailtide::ids::{DraftId, ThreadId};
    let label = LabelId("Label_1".into());
    let draft = DraftId("r1".into());
    let writes = [
        archive_m1(),
        GmailRequest::CreateLabel(NewLabel {
            name: "Work".into(),
            list_visibility: LabelListVisibility::Show,
            message_visibility: MessageListVisibility::Show,
            color: None,
        }),
        GmailRequest::PatchLabel {
            id: label.clone(),
            patch: LabelPatch {
                name: Some("Job".into()),
                ..LabelPatch::default()
            },
        },
        GmailRequest::DeleteLabel { id: label },
        GmailRequest::DeleteMessage {
            id: MessageId("m1".into()),
        },
        GmailRequest::BatchDeleteMessages {
            ids: vec![MessageId("m1".into())],
        },
        GmailRequest::CreateDraft {
            raw: Rfc822(b"Subject: x\r\n\r\nx".to_vec()),
            thread_id: None,
        },
        GmailRequest::UpdateDraft {
            id: draft.clone(),
            raw: Rfc822(b"Subject: x\r\n\r\nx".to_vec()),
            thread_id: ThreadId("t1".into()),
        },
        GmailRequest::DeleteDraft { id: draft.clone() },
        GmailRequest::SendDraft { id: draft },
    ];
    for access in [Access::ReadWrite, Access::ReadOnly] {
        let setup = Setup::new(access, Vec::new(), 0, Duration::ZERO);
        for write in writes.clone() {
            let refused = setup.call(write.clone(), BUDGET).await;
            assert!(
                matches!(&refused, Err(CallError::Refused(f)) if f.class == Blocking::Permanent),
                "{write:?}: {refused:?}"
            );
        }
        assert!(setup.admitted().is_empty());
        assert!(setup.sent().is_empty());
        assert_eq!(setup.tokens.minted.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn read_only_mode_allows_a_read() {
    let setup = Setup::new(Access::ReadOnly, vec![Ok(fetched())], 0, Duration::ZERO);
    assert!(setup.call(get_m1(), BUDGET).await.is_ok());
}

#[tokio::test]
async fn a_deferral_past_the_budget_sends_nothing_and_says_when() {
    let setup = Setup::new(Access::ReadWrite, Vec::new(), 1, Duration::from_mins(1));
    let before = mailtide_io::now();
    let error = setup
        .call(get_m1(), Duration::from_secs(1))
        .await
        .expect_err("deferred");
    let CallError::Deferred(QuotaDeferred { until }) = error else {
        panic!("{error:?}");
    };
    assert!(until >= before.plus(Duration::from_secs(59)), "{until:?}");
    assert!(setup.sent().is_empty());
    assert_eq!(setup.tokens.minted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_deferral_within_the_budget_is_waited_out() {
    let setup = Setup::new(
        Access::ReadWrite,
        vec![Ok(fetched())],
        1,
        Duration::from_millis(50),
    );
    assert!(setup.call(get_m1(), BUDGET).await.is_ok());
    assert_eq!(setup.admitted().len(), 2, "asked again after the wait");
    assert_eq!(setup.sent().len(), 1);
}

/// A token without a scope the call needs (purge's full scope, identities'
/// settings scope) is the caller's refusal: the sync of the account goes on.
#[tokio::test]
async fn a_missing_scope_is_refused_without_blocking_the_account() {
    let setup = Setup::answering(vec![Ok(refusal(403, "insufficientPermissions"))]);
    let error = setup.call(get_m1(), BUDGET).await.expect_err("refused");
    let CallError::Refused(refusal) = &error else {
        panic!("{error:?}");
    };
    assert_eq!(
        refusal.class,
        Blocking::Account(AccountProblem::ApiAccessDenied)
    );
    assert_eq!(error.code(), "account_problem");
    let status = setup
        .runner
        .store()
        .status(&account())
        .await
        .expect("status")
        .expect("tracked");
    assert!(
        !matches!(status.sync, mailtide::state::SyncState::Blocked { .. }),
        "{:?}",
        status.sync
    );
}

#[tokio::test]
async fn a_rate_limit_cools_the_gate_down_for_later_calls() {
    let setup = Setup::answering(vec![Ok(refusal(429, "userRateLimitExceeded"))]);
    let before = mailtide_io::now();
    let error = setup.call(get_m1(), BUDGET).await.expect_err("limited");
    assert!(
        matches!(&error, CallError::Retryable(f) if f.class == Retryable::AccountRateLimited),
        "{error:?}"
    );
    let cooldowns = setup.gate.lock().expect("lock").cooldowns.clone();
    let [cooldown] = cooldowns.as_slice() else {
        panic!("{cooldowns:?}");
    };
    assert_eq!(cooldown.target, QuotaTarget::Account(account()));
    assert!(cooldown.until >= before.plus(Duration::from_secs(300)));
    let next = setup.call(get_m1(), Duration::from_secs(1)).await;
    assert!(matches!(next, Err(CallError::Deferred(_))), "{next:?}");
    assert_eq!(setup.sent().len(), 1, "the second call never left");
}

#[tokio::test]
async fn a_lost_answer_is_an_unknown_outcome_and_an_unsent_request_is_retryable() {
    let setup = Setup::answering(vec![
        Err(TransportError::OutcomeUnknown("timed out".into())),
        Err(TransportError::NotSent("refused".into())),
    ]);
    assert_eq!(
        setup.call(get_m1(), BUDGET).await,
        Err(CallError::OutcomeUnknown("timed out".into()))
    );
    let unsent = setup.call(get_m1(), BUDGET).await;
    assert!(
        matches!(&unsent, Err(CallError::Retryable(f)) if f.class == Retryable::Transient),
        "{unsent:?}"
    );
    assert!(setup.gate.lock().expect("lock").cooldowns.is_empty());
}

#[tokio::test]
async fn a_missing_resource_is_not_found_and_an_expired_token_is_dropped() {
    let setup = Setup::answering(vec![
        Ok(refusal(404, "notFound")),
        Ok(refusal(401, "authError")),
    ]);
    assert_eq!(
        setup.call(GmailRequest::ListLabels, BUDGET).await,
        Err(CallError::NotFound)
    );
    let expired = setup.call(GmailRequest::ListLabels, BUDGET).await;
    assert!(
        matches!(&expired, Err(CallError::Retryable(f)) if f.class == Retryable::Transient),
        "{expired:?}"
    );
    assert_eq!(setup.tokens.dropped.load(Ordering::SeqCst), 1);
}
