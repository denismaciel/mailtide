//! Error classes and backoff (SPEC §6.4, §8).
//!
//! Classes come from `mailtide-gmail`'s classifier. Rate limits arrive as 403
//! or 429; bandwidth 429s pause the account for hours. Account problems
//! (delegation removed, user deleted or suspended, domain policy) block rather
//! than retry. Jitter comes from the account's stored seed (I10).
//!
//! Zero counterpart: `lib/gmail-quota-error.ts`, `lib/gmail-rate-limit.ts`,
//! `lib/driver/errors.ts`.

use std::time::Duration;

use crate::ids::{Timestamp, splitmix64};

/// A classified failure: what to do about it is part of its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// Try again later.
    Retryable(RetryableFailure),
    /// Gmail returned 404 for the requested resource.
    NotFound,
    /// Stop until an operator acts.
    Blocking(BlockingFailure),
}

/// A failure worth retrying.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryableFailure {
    /// The class.
    pub class: Retryable,
    /// Gmail's `Retry-After`, a lower bound.
    pub retry_after: Option<Duration>,
    /// Short diagnostic, never message content.
    pub detail: String,
}

/// Why a retry is worthwhile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retryable {
    /// Network, 5xx, 408.
    Transient,
    /// Per-account rate limit (403/429 `userRateLimitExceeded`): cool down the account.
    AccountRateLimited,
    /// Too many concurrent requests for the account (429): cool down the account.
    AccountConcurrency,
    /// Account bandwidth cap reached (429): pause the account for hours.
    AccountBandwidth,
    /// Project rate or quota limit: cool down the project.
    ProjectRateLimited,
}

/// A failure that stops work until an operator acts.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockingFailure {
    /// The class.
    pub class: Blocking,
    /// Short diagnostic, never message content.
    pub detail: String,
}

/// Why work stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Blocking {
    /// The account can't be used at all.
    Account(AccountProblem),
    /// A request Gmail will never accept as sent (400 and the like).
    Permanent,
}

/// Why an account can't be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AccountProblem {
    /// Domain-wide delegation removed or scope missing (`unauthorized_client`).
    DelegationRevoked,
    /// The user no longer exists (`invalid_grant`).
    UserDeleted,
    /// Gmail is not enabled for the user (suspended or unlicensed).
    MailDisabled,
    /// The domain blocks Gmail API apps (`domainPolicy`).
    DomainPolicy,
}

/// What a cooldown applies to.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum QuotaTarget {
    /// One account.
    Account(crate::ids::AccountId),
    /// The whole project.
    Project,
}

/// An active pause.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Cooldown {
    /// What it applies to.
    pub target: QuotaTarget,
    /// Until when.
    pub until: Timestamp,
}

/// Backoff policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// First delay.
    pub base: Duration,
    /// Largest delay before jitter.
    pub cap: Duration,
}

impl Backoff {
    /// Zero's defaults: one second, doubling, capped at a minute (SPEC §6.4).
    pub const DEFAULT: Self = Self {
        base: Duration::from_secs(1),
        cap: Duration::from_mins(1),
    };

    /// When to retry attempt `attempt` (0-based) after `failure`, with jitter
    /// drawn from `seed`. Never earlier than `Retry-After`. The delay is
    /// "equal jitter": half the capped exponential delay, plus a random part
    /// of the other half.
    #[must_use]
    pub fn next(
        &self,
        attempt: u32,
        failure: &RetryableFailure,
        now: Timestamp,
        seed: u64,
    ) -> Timestamp {
        let exponential = self
            .base
            .saturating_mul(2u32.saturating_pow(attempt.min(31)))
            .min(self.cap);
        let half = exponential / 2;
        let half_millis = u64::try_from(half.as_millis()).unwrap_or(u64::MAX);
        let jitter = Duration::from_millis(match half_millis {
            0 => 0,
            span => splitmix64(seed ^ u64::from(attempt)) % (span + 1),
        });
        let delay = (half + jitter).max(failure.retry_after.unwrap_or_default());
        now.plus(delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(retry_after: Option<Duration>) -> RetryableFailure {
        RetryableFailure {
            class: Retryable::Transient,
            retry_after,
            detail: String::new(),
        }
    }

    #[test]
    fn delay_grows_and_is_capped() {
        let backoff = Backoff::DEFAULT;
        let now = Timestamp(0);
        let first = backoff.next(0, &failure(None), now, 1).0;
        let tenth = backoff.next(10, &failure(None), now, 1).0;
        assert!((500..=1000).contains(&first), "{first}");
        assert!((30_000..=60_000).contains(&tenth), "{tenth}");
    }

    #[test]
    fn retry_after_is_a_lower_bound() {
        let at = Backoff::DEFAULT.next(0, &failure(Some(Duration::from_secs(90))), Timestamp(0), 1);
        assert_eq!(at, Timestamp(90_000));
    }

    #[test]
    fn jitter_is_deterministic_per_seed() {
        let backoff = Backoff::DEFAULT;
        let a = backoff.next(3, &failure(None), Timestamp(0), 42);
        let b = backoff.next(3, &failure(None), Timestamp(0), 42);
        assert_eq!(a, b);
    }
}
