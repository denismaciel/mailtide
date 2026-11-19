//! In-memory store for tests, the simulator and single-process hosts. Applies
//! commits with the core's shared rules ([`mailtide::mirror`]).

use std::collections::BTreeMap;
use std::sync::Mutex;

use mailtide::commit::{Commit, CommitOutcome};
use mailtide::ids::{AccountId, EngineVersion, Epoch, MessageId, Timestamp};
use mailtide::jobs::{AttemptId, ClaimOutcome};
use mailtide::mirror::{AccountMirror, MessageView};
use mailtide::outbox::ActionRequest;
use mailtide::revision::{ChangeFeed, StateSeq};
use mailtide::state::StoreSnapshot;
use mailtide::status::{AccountStatus, ForgetOutcome, ResumeOutcome};

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

    fn with_account<T>(
        &self,
        account: &AccountId,
        f: impl FnOnce(&mut AccountMirror) -> T,
    ) -> Result<T, StoreError> {
        let mut accounts = self.lock()?;
        let mirror = accounts
            .entry(account.clone())
            .or_insert_with(|| AccountMirror::new(account.clone()));
        Ok(f(mirror))
    }
}

impl Store for MemoryStore {
    async fn claim(
        &self,
        account: &AccountId,
        version: EngineVersion,
    ) -> Result<StoreSnapshot, StoreError> {
        self.with_account(account, |mirror| mirror.claim(version))?
            .map_err(StoreError::Rejected)
    }

    async fn apply(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        self.with_account(&commit.account, |mirror| mirror.apply(commit))
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
        self.with_account(account, |mirror| {
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
        since: StateSeq,
        limit: u32,
    ) -> Result<ChangeFeed, StoreError> {
        self.with_account(account, |mirror| mirror.feed(since, limit))
    }

    async fn compact(&self, account: &AccountId, through: StateSeq) -> Result<u64, StoreError> {
        Ok(self
            .lock()?
            .get_mut(account)
            .map_or(0, |mirror| mirror.compact(through)))
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
        self.with_account(account, |mirror| mirror.admit_action(request, now))
    }

    async fn messages(
        &self,
        account: &AccountId,
        ids: &[MessageId],
    ) -> Result<Vec<MessageView>, StoreError> {
        self.with_account(account, |mirror| {
            ids.iter().filter_map(|id| mirror.view(id)).collect()
        })
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
