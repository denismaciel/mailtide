//! In-memory store for tests, the simulator and single-process hosts. Applies
//! commits with the core's shared rules ([`mailtide::mirror`]).

use std::collections::BTreeMap;
use std::sync::Mutex;

use mailtide::commit::{Commit, CommitOutcome};
use mailtide::gmail::Label;
use mailtide::ids::{AccountId, EngineVersion, Epoch, MessageId, ThreadId, Timestamp};
use mailtide::jobs::{AttemptId, ClaimOutcome, Prioritised};
use mailtide::mirror::{AccountMirror, MessageView};
use mailtide::outbox::ActionRequest;
use mailtide::revision::{
    AckOutcome, ChangeFeed, ConsumerList, ConsumerName, FeedCursor, FeedPosition, Retention,
    StateSeq,
};
use mailtide::state::StoreSnapshot;
use mailtide::status::{AccountStatus, ForgetOutcome, ResumeOutcome, TrackOutcome};
use mailtide::thread::ThreadView;

use crate::{Store, StoreError};

/// A store held in memory.
#[derive(Debug, Default)]
pub struct MemoryStore {
    accounts: Mutex<BTreeMap<AccountId, AccountMirror>>,
}

impl MemoryStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A store already tracking `accounts` (generation 1), for tests and
    /// tools that build a store synchronously.
    #[must_use]
    pub fn tracking(accounts: &[AccountId]) -> Self {
        let store = Self::default();
        if let Ok(mut map) = store.lock() {
            for account in accounts {
                map.insert(account.clone(), AccountMirror::new(account.clone(), 1));
            }
        }
        store
    }

    /// A copy of one account's mirror, for inspection.
    #[must_use]
    pub fn mirror(&self, account: &AccountId) -> Option<AccountMirror> {
        self.lock()
            .ok()
            .and_then(|accounts| accounts.get(account).cloned())
    }

    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, BTreeMap<AccountId, AccountMirror>>, StoreError> {
        self.accounts
            .lock()
            .map_err(|_| StoreError::Unavailable("memory store lock poisoned".into()))
    }

    /// Run `f` on a tracked account's mirror.
    fn with_account<T>(
        &self,
        account: &AccountId,
        f: impl FnOnce(&mut AccountMirror) -> T,
    ) -> Result<T, StoreError> {
        let mut accounts = self.lock()?;
        let mirror = accounts.get_mut(account).ok_or(StoreError::NotTracked)?;
        Ok(f(mirror))
    }

    /// Run `f` on a tracked account's mirror, or answer `untracked` (what the
    /// SQL store answers for a missing row).
    fn on_account<T>(
        &self,
        account: &AccountId,
        untracked: T,
        f: impl FnOnce(&mut AccountMirror) -> T,
    ) -> Result<T, StoreError> {
        let mut accounts = self.lock()?;
        Ok(accounts.get_mut(account).map_or(untracked, f))
    }
}

impl Store for MemoryStore {
    async fn track(
        &self,
        account: &AccountId,
        generation: u64,
    ) -> Result<TrackOutcome, StoreError> {
        let mut accounts = self.lock()?;
        if accounts.contains_key(account) {
            return Ok(TrackOutcome::AlreadyTracked);
        }
        accounts.insert(
            account.clone(),
            AccountMirror::new(account.clone(), generation),
        );
        Ok(TrackOutcome::Tracked { generation })
    }

    async fn claim(
        &self,
        account: &AccountId,
        version: EngineVersion,
    ) -> Result<StoreSnapshot, StoreError> {
        self.with_account(account, |mirror| mirror.claim(version))?
            .map_err(StoreError::Rejected)
    }

    async fn apply(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        self.on_account(
            &commit.account,
            CommitOutcome::Rejected(mailtide::commit::Rejection::StaleEpoch),
            |mirror| mirror.apply(commit),
        )
    }

    async fn claim_jobs(
        &self,
        account: &AccountId,
        epoch: Epoch,
        limit: u32,
        now: Timestamp,
        attempt: AttemptId,
        floor: mailtide::quota::Priority,
    ) -> Result<ClaimOutcome, StoreError> {
        let untracked = ClaimOutcome::Rejected(mailtide::commit::Rejection::StaleEpoch);
        self.on_account(account, untracked, |mirror| {
            match mirror.claim_jobs_from(epoch, limit, now, attempt, floor) {
                Ok(jobs) => ClaimOutcome::Claimed {
                    jobs,
                    next_due: mirror.next_due(),
                },
                Err(rejection) => ClaimOutcome::Rejected(rejection),
            }
        })
    }

    async fn changes(
        &self,
        account: &AccountId,
        since: FeedCursor,
        limit: u32,
    ) -> Result<ChangeFeed, StoreError> {
        let untracked = ChangeFeed::Page(mailtide::revision::ChangePage {
            generation: 0,
            head: since.seq,
            changes: Vec::new(),
            next: since,
            has_more: false,
        });
        self.on_account(account, untracked, |mirror| mirror.feed(since, limit))
    }

    async fn compact(&self, account: &AccountId, retention: &Retention) -> Result<u64, StoreError> {
        self.on_account(account, 0, |mirror| mirror.compact(retention))
    }

    async fn ack(
        &self,
        account: &AccountId,
        consumer: &ConsumerName,
        at: FeedPosition,
        now: Timestamp,
    ) -> Result<AckOutcome, StoreError> {
        self.with_account(account, |mirror| mirror.ack(consumer, at, now))
    }

    async fn consumers(&self, account: &AccountId) -> Result<ConsumerList, StoreError> {
        let untracked = ConsumerList {
            generation: 0,
            floor: StateSeq(0),
            cursors: Vec::new(),
        };
        self.on_account(account, untracked, |mirror| mirror.consumer_list())
    }

    async fn drop_consumer(
        &self,
        account: &AccountId,
        consumer: &ConsumerName,
    ) -> Result<bool, StoreError> {
        self.on_account(account, false, |mirror| mirror.drop_consumer(consumer))
    }

    async fn message_ids(
        &self,
        account: &AccountId,
        after: Option<&MessageId>,
        limit: u32,
    ) -> Result<Vec<MessageId>, StoreError> {
        Ok(self
            .lock()?
            .get(account)
            .map(|mirror| mirror.message_ids(after, limit))
            .unwrap_or_default())
    }

    async fn status(&self, account: &AccountId) -> Result<Option<AccountStatus>, StoreError> {
        Ok(self.lock()?.get(account).map(AccountMirror::status))
    }

    async fn accounts(&self) -> Result<Vec<AccountId>, StoreError> {
        Ok(self.lock()?.keys().cloned().collect())
    }

    async fn resume(
        &self,
        account: &AccountId,
        now: Timestamp,
    ) -> Result<ResumeOutcome, StoreError> {
        Ok(self
            .lock()?
            .get_mut(account)
            .map_or(ResumeOutcome::Unknown, |mirror| mirror.resume(now)))
    }

    async fn forget(&self, account: &AccountId) -> Result<ForgetOutcome, StoreError> {
        Ok(self
            .lock()?
            .remove(account)
            .map_or(ForgetOutcome::Unknown, |mirror| ForgetOutcome::Forgotten {
                messages: mirror.messages.len() as u64,
            }))
    }

    async fn admit(
        &self,
        account: &AccountId,
        request: &ActionRequest,
        now: Timestamp,
    ) -> Result<u32, StoreError> {
        self.on_account(account, 0, |mirror| mirror.admit_action(request, now))
    }

    async fn prioritise(
        &self,
        account: &AccountId,
        ids: &[MessageId],
        now: Timestamp,
    ) -> Result<Prioritised, StoreError> {
        self.on_account(account, Prioritised::unknown(ids), |mirror| {
            mirror.prioritise(ids, now)
        })
    }

    async fn messages(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Vec<MessageView>, StoreError> {
        self.on_account(account, Vec::new(), |mirror| {
            ids.iter().filter_map(|id| mirror.view(id)).collect()
        })
    }

    async fn threads(
        &self,
        account: &AccountId,
        ids: &[ThreadId],
    ) -> Result<Vec<ThreadView>, StoreError> {
        self.on_account(account, Vec::new(), |mirror| {
            ids.iter().filter_map(|id| mirror.thread(id)).collect()
        })
    }

    async fn labels(&self, account: &AccountId) -> Result<Vec<Label>, StoreError> {
        self.on_account(account, Vec::new(), |mirror| mirror.label_definitions())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passes_the_conformance_suite() {
        let results =
            pollster::block_on(crate::conformance::run(|| async { Ok(MemoryStore::new()) }));
        let failures: Vec<_> = results.iter().filter(|r| r.failure.is_some()).collect();
        assert!(failures.is_empty(), "{failures:#?}");
        assert!(results.len() >= 6);
    }
}
