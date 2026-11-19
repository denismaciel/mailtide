//! In-memory store for tests, the simulator and single-process hosts. Applies
//! commits with the core's shared rules ([`mailtide::mirror`]).

use std::collections::BTreeMap;
use std::sync::Mutex;

use mailtide::commit::{Commit, CommitOutcome};
use mailtide::ids::{AccountId, EngineVersion, Epoch, MessageId, Timestamp};
use mailtide::jobs::{AttemptId, ClaimedJob};
use mailtide::mirror::{AccountMirror, MessageView};
use mailtide::revision::{ChangePage, StateSeq};
use mailtide::state::StoreSnapshot;

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
    ) -> Result<Vec<ClaimedJob>, StoreError> {
        self.with_account(account, |mirror| {
            mirror.claim_jobs(epoch, limit, now, attempt)
        })?
        .map_err(StoreError::Rejected)
    }

    async fn changes(
        &self,
        account: &AccountId,
        since: StateSeq,
        limit: u32,
    ) -> Result<ChangePage, StoreError> {
        self.with_account(account, |mirror| mirror.changes(since, limit))
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
