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

    /// Gmail rejected `account`'s current token (401): drop any cached copy
    /// so the next call mints a fresh one.
    fn invalidate(&self, account: &AccountId) {
        let _ = account;
    }
}

/// Reuse each account's token until shortly before it expires, so the host's
/// source is asked once per token, not once per request.
#[derive(Debug)]
pub struct CachedTokens<A> {
    source: A,
    margin: std::time::Duration,
    cache: std::sync::Mutex<BTreeMap<AccountId, AccessToken>>,
}

impl<A> CachedTokens<A> {
    /// Cache `source`'s tokens until `margin` before they expire.
    pub fn new(source: A, margin: std::time::Duration) -> Self {
        Self {
            source,
            margin,
            cache: std::sync::Mutex::new(BTreeMap::new()),
        }
    }

    fn cached(&self, account: &AccountId) -> Option<AccessToken> {
        let now = crate::now();
        let cache = self.cache.lock().ok()?;
        cache
            .get(account)
            .filter(|token| token.expires_at > now.plus(self.margin))
            .cloned()
    }
}

impl<A: TokenSource> TokenSource for CachedTokens<A> {
    async fn token(&self, account: &AccountId) -> Result<AccessToken, Failure> {
        if let Some(token) = self.cached(account) {
            return Ok(token);
        }
        let token = self.source.token(account).await?;
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(account.clone(), token.clone());
        }
        Ok(token)
    }

    fn invalidate(&self, account: &AccountId) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.remove(account);
        }
        self.source.invalidate(account);
    }
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
