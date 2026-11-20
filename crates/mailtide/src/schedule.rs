//! When to sync: push hints, polling, watch renewal (SPEC §6.2).
//!
//! A push only raises the notification revision. Accounts poll as a safety net,
//! staggered per account. Watch renewal never moves the cursor (invariant I2).
//! History can expire within hours, so polling never stops.
//!
//! Zero counterpart: `lib/gmail-push.ts`, `lib/mail-schedule.ts`.

use std::time::Duration;

use crate::ids::{AccountId, Timestamp, stable_hash};

/// The engine's timers. One pending instance of each per account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TimerKind {
    /// Poll history.
    Poll,
    /// Renew the Gmail watch.
    WatchRenewal,
    /// Run the next due job.
    Jobs,
    /// Retry after a cooldown.
    CooldownOver,
    /// Refresh the label definitions (SPEC §6.6).
    Labels,
    /// Enqueue the periodic draft listing (SPEC §7a).
    Drafts,
}

/// How soon after a listing a label id the definitions lack may list them
/// again: a burst of new ids costs one listing, and an id Gmail never lists
/// can't make the engine list labels over and over (SPEC §6.6).
pub const UNKNOWN_LABEL_GAP: Duration = Duration::from_mins(1);

/// When the label definitions are due for their periodic refresh:
/// `interval` after the last listing, or now if they were never listed.
#[must_use]
pub fn label_refresh_due(
    refreshed_at: Option<Timestamp>,
    now: Timestamp,
    interval: Duration,
) -> Timestamp {
    refreshed_at.map_or(now, |at| at.plus(interval).max(now))
}

/// When the drafts are due for their periodic listing: by the rule of
/// [`label_refresh_due`], `interval` after the last listing, or now if they
/// were never listed.
#[must_use]
pub fn draft_refresh_due(
    refreshed_at: Option<Timestamp>,
    now: Timestamp,
    interval: Duration,
) -> Timestamp {
    label_refresh_due(refreshed_at, now, interval)
}

/// When an unknown label id may refresh the definitions: now, unless they
/// were listed less than [`UNKNOWN_LABEL_GAP`] (or `interval`, if shorter)
/// ago.
#[must_use]
pub fn unknown_label_refresh(
    refreshed_at: Option<Timestamp>,
    now: Timestamp,
    interval: Duration,
) -> Timestamp {
    label_refresh_due(refreshed_at, now, interval.min(UNKNOWN_LABEL_GAP))
}

/// When to poll next, staggered by account so accounts don't poll together.
#[must_use]
pub fn next_poll(
    account: &AccountId,
    now: Timestamp,
    last_activity: Option<Timestamp>,
    active: Duration,
    idle: Duration,
    idle_after: Duration,
) -> Timestamp {
    let active_now = last_activity.is_some_and(|at| now.since(at) < idle_after);
    let interval = if active_now { active } else { idle };
    let interval_millis = u64::try_from(interval.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    // Up to a tenth of the interval, fixed per account.
    let stagger = stable_hash(account.0.as_bytes()) % (interval_millis / 10 + 1);
    now.plus(interval).plus(Duration::from_millis(stagger))
}

/// How long a Gmail watch lasts at most; renew daily (SPEC §6.2).
pub const RENEW_AT_LEAST_EVERY: Duration = Duration::from_hours(24);

/// When to renew the watch.
#[must_use]
pub fn next_watch_renewal(
    expiration: Option<Timestamp>,
    now: Timestamp,
    margin: Duration,
) -> Timestamp {
    match expiration {
        None => now,
        Some(expiration) => expiration
            .minus(margin)
            .min(now.plus(RENEW_AT_LEAST_EVERY))
            .max(now),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: Duration = Duration::from_mins(1);

    #[test]
    fn active_accounts_poll_sooner_than_idle_ones() {
        let account = AccountId("alice@example.com".into());
        let now = Timestamp(10_000_000);
        let active = next_poll(&account, now, Some(now), MINUTE, 15 * MINUTE, 60 * MINUTE);
        let idle = next_poll(&account, now, None, MINUTE, 15 * MINUTE, 60 * MINUTE);
        assert!(active.since(now) >= MINUTE && active.since(now) <= MINUTE + MINUTE / 10);
        assert!(idle.since(now) >= 15 * MINUTE);
    }

    #[test]
    fn labels_refresh_an_interval_after_the_last_listing() {
        let now = Timestamp(10_000_000);
        let interval = 15 * MINUTE;
        assert_eq!(label_refresh_due(None, now, interval), now, "never listed");
        let recent = now.minus(MINUTE);
        assert_eq!(
            label_refresh_due(Some(recent), now, interval),
            recent.plus(interval)
        );
        let long_ago = now.minus(Duration::from_hours(1));
        assert_eq!(label_refresh_due(Some(long_ago), now, interval), now);
    }

    #[test]
    fn unknown_labels_refresh_at_most_once_a_gap() {
        let now = Timestamp(10_000_000);
        let interval = 15 * MINUTE;
        let just_now = now.minus(Duration::from_secs(10));
        assert_eq!(
            unknown_label_refresh(Some(just_now), now, interval),
            just_now.plus(UNKNOWN_LABEL_GAP)
        );
        let earlier = now.minus(2 * MINUTE);
        assert_eq!(unknown_label_refresh(Some(earlier), now, interval), now);
        let short = Duration::from_secs(5);
        assert_eq!(
            unknown_label_refresh(Some(just_now), now, short),
            now,
            "a shorter interval wins"
        );
    }

    #[test]
    fn missing_watch_renews_now() {
        assert_eq!(next_watch_renewal(None, Timestamp(5), MINUTE), Timestamp(5));
    }

    #[test]
    fn watch_renews_before_expiry_and_at_least_daily() {
        let now = Timestamp(0);
        let soon = now.plus(2 * MINUTE);
        assert_eq!(
            next_watch_renewal(Some(soon), now, MINUTE),
            now.plus(MINUTE)
        );
        let far = now.plus(Duration::from_hours(168));
        assert_eq!(
            next_watch_renewal(Some(far), now, MINUTE),
            now.plus(RENEW_AT_LEAST_EVERY)
        );
        let past = Timestamp(-1);
        assert_eq!(next_watch_renewal(Some(past), now, MINUTE), now);
    }
}
