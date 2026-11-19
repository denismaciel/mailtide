//! Access tokens per account, supplied by the host (e.g. Google domain-wide
//! delegation; tokens last at most an hour and are minted, not refreshed).

use std::collections::BTreeMap;
use std::future::Future;

use mailtide::ids::{AccountId, Timestamp};
use mailtide::retry::Failure;

/// A bearer token for one account.
#[derive(Clone, PartialEq, Eq)]
pub struct AccessToken {
    /// The secret. Never logged.
    pub secret: String,
    /// When it expires.
    pub expires_at: Timestamp,
}

impl std::fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessToken")
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// Mints access tokens.
pub trait TokenSource: Send + Sync {
    /// A valid token for `account`, or a classified failure (delegation
    /// revoked, user deleted, …).
    fn token(
        &self,
        account: &AccountId,
    ) -> impl Future<Output = Result<AccessToken, Failure>> + Send;
}

/// Fixed tokens per account (tests and development).
#[derive(Debug, Clone, Default)]
pub struct StaticTokens(pub BTreeMap<AccountId, AccessToken>);

impl TokenSource for StaticTokens {
    async fn token(&self, account: &AccountId) -> Result<AccessToken, Failure> {
        self.0.get(account).cloned().ok_or_else(|| {
            Failure::Blocking(mailtide::retry::BlockingFailure {
                class: mailtide::retry::Blocking::Account(
                    mailtide::retry::AccountProblem::DelegationRevoked,
                ),
                detail: "no token configured".into(),
            })
        })
    }
}
