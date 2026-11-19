//! The quota gate: wraps the core [`mailtide::quota::Ledger`] for the
//! processes sharing a Gmail project.

use std::future::Future;
use std::sync::Mutex;

use mailtide::ids::{AccountId, Timestamp};
use mailtide::quota::{Admission, Budget, Lease, Ledger, Priority, QuotaUnits};
use mailtide::retry::Cooldown;

/// A gate failure, distinct from a deferral.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateError(pub String);

/// Admits Gmail requests across accounts.
pub trait QuotaGate: Send + Sync {
    /// Admit a request, or defer it.
    fn admit(
        &self,
        account: &AccountId,
        cost: QuotaUnits,
        priority: Priority,
        now: Timestamp,
    ) -> impl Future<Output = Result<Admission, GateError>> + Send;
    /// Release a lease and record downloaded bytes.
    fn release(
        &self,
        lease: Lease,
        bytes: u64,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), GateError>> + Send;
    /// Record that Gmail pushed back, for an account or the whole project.
    fn cool_down(&self, cooldown: Cooldown) -> impl Future<Output = Result<(), GateError>> + Send;
}

/// A gate held in this process (single-process hosts and tests).
#[derive(Debug)]
pub struct LocalGate {
    budget: Budget,
    ledger: Mutex<Ledger>,
}

impl LocalGate {
    /// A gate with `budget`.
    #[must_use]
    pub fn new(budget: Budget) -> Self {
        Self {
            budget,
            ledger: Mutex::new(Ledger::default()),
        }
    }

    fn ledger(&self) -> Result<std::sync::MutexGuard<'_, Ledger>, GateError> {
        self.ledger
            .lock()
            .map_err(|_| GateError("quota ledger poisoned".into()))
    }
}

impl QuotaGate for LocalGate {
    async fn admit(
        &self,
        account: &AccountId,
        cost: QuotaUnits,
        priority: Priority,
        now: Timestamp,
    ) -> Result<Admission, GateError> {
        Ok(self
            .ledger()?
            .admit(&self.budget, account, cost, priority, now))
    }

    async fn release(&self, lease: Lease, bytes: u64, now: Timestamp) -> Result<(), GateError> {
        self.ledger()?.release(lease, bytes, now);
        Ok(())
    }

    async fn cool_down(&self, cooldown: Cooldown) -> Result<(), GateError> {
        self.ledger()?.cool_down(cooldown);
        Ok(())
    }
}
