//! Tests ported by name from Zero's `mailbox-reader.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890). Plan: `features/threads.toml`.
//!
//! Zero opens its mailbox agent without provider auth for a cached read. A
//! mailtide thread read is a store read: the runner asks the token source
//! for nothing and sends no request.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use mailtide::EngineConfig;
use mailtide::commit::{Commit, CommitOutcome, JobPlan, MessageOp};
use mailtide::gmail::MessageObservation;
use mailtide::ids::{
    AccountId, CommitId, ENGINE_VERSION, HistoryId, MessageId, ThreadId, Timestamp,
};
use mailtide::labels::set;
use mailtide::quota::{Budget, Priority};
use mailtide_gmail::http::{HttpRequest, HttpResponse};
use mailtide_io::auth::{AccessToken, TokenError, TokenSource};
use mailtide_io::blobs::ObjectBlobs;
use mailtide_io::quota_gate::LocalGate;
use mailtide_io::runner::{Access, LogAccounts, Runner, RunnerConfig, WatchMode};
use mailtide_io::transport::{GmailTransport, TransportError};
use mailtide_store::Store;
use mailtide_store::memory::MemoryStore;

fn account() -> AccountId {
    AccountId::parse("mailbox@example.com").expect("account")
}

/// A token source whose provider is down: counts every call.
#[derive(Debug, Default)]
struct NoAuth(Arc<AtomicU32>);

impl TokenSource for NoAuth {
    async fn token(&self, _account: &AccountId) -> Result<AccessToken, TokenError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TokenError::Transient("provider auth unavailable".into()))
    }
}

/// A Gmail that can't be reached: counts every request.
#[derive(Debug, Default)]
struct NoGmail(AtomicU32);

impl GmailTransport for NoGmail {
    async fn send(
        &self,
        _request: HttpRequest,
        _token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::NotSent("no network".into()))
    }
}

/// A store holding one synced thread.
async fn synced() -> MemoryStore {
    let store = MemoryStore::tracking(&[account()]);
    let snapshot = store
        .claim(&account(), ENGINE_VERSION)
        .await
        .expect("claim");
    let commit = Commit {
        id: CommitId {
            epoch: snapshot.epoch,
            seq: 0,
        },
        account: account(),
        epoch: snapshot.epoch,
        engine_version: snapshot.engine_version,
        cursor: None,
        claim: None,
        sync: None,
        messages: vec![MessageOp::Observe(MessageObservation {
            id: MessageId("m".into()),
            thread_id: ThreadId("thread".into()),
            labels: set(["INBOX", "UNREAD"]),
            checkpoint: HistoryId(1),
        })],
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
        store.apply(&commit).await.expect("apply"),
        CommitOutcome::Applied { .. }
    ));
    store
}

#[tokio::test]
async fn a_thread_read_asks_for_no_token_and_calls_no_gmail() {
    let token_calls = Arc::new(AtomicU32::new(0));
    let runner = Runner::new(
        synced().await,
        ObjectBlobs::memory(),
        NoGmail::default(),
        LocalGate::new(Budget::GMAIL_DEFAULT),
        NoAuth(Arc::clone(&token_calls)),
        RunnerConfig {
            access: Access::ReadWrite,
            engine: EngineConfig::default(),
            watch: WatchMode::Disabled,
            log_accounts: LogAccounts::default(),
            drafts: mailtide_io::drafts::DraftSettings::default(),
        },
    );
    let epoch = runner
        .status(&account())
        .await
        .expect("status")
        .expect("tracked")
        .epoch;
    let threads = runner
        .threads(&account(), &[ThreadId("thread".into())])
        .await
        .expect("a cached read");
    assert_eq!(threads.len(), 1);
    assert!(threads[0].unread);
    assert_eq!(token_calls.load(Ordering::SeqCst), 0);
    assert_eq!(runner.transport().0.load(Ordering::SeqCst), 0);
    // The read claimed nothing: a running sync would not be preempted.
    let after = runner
        .status(&account())
        .await
        .expect("status")
        .expect("tracked")
        .epoch;
    assert_eq!(after, epoch);
}
