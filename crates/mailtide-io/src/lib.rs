//! Async driver: runs the [`mailtide`] engine against the real world.
//!
//! Executes engine commands over HTTP via [`mailtide_gmail`], admits them
//! through the shared quota gate, writes raw content to blob storage at a
//! deterministic key before reporting it, applies commits through
//! [`mailtide_store`], and feeds results back as events stamped with the
//! time. Credentials come from a [`auth::TokenSource`] the host provides.
//!
//! SPEC §3, §6.2, §8. Zero counterpart: `routes/agent/index.ts`,
//! `lib/gmail-quota-store.ts`, `lib/gmail-quota-client.ts`,
//! `lib/mail-asset-cache.ts`.

pub mod auth;
pub mod blobs;
pub mod quota_gate;
pub mod runner;
pub mod transport;

/// The host's clock, as the engine's time type.
#[must_use]
pub fn now() -> mailtide::Timestamp {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    mailtide::Timestamp(i64::try_from(millis).unwrap_or(i64::MAX))
}

/// Where the driver reads the time: the system clock, or a host-supplied one
/// (tests advance it to reach retries, cleanups and confirmations without
/// sleeping). Budgets still use real elapsed time.
#[derive(Clone)]
pub struct Clock(Option<std::sync::Arc<dyn Fn() -> mailtide::Timestamp + Send + Sync>>);

impl Clock {
    /// The system clock.
    #[must_use]
    pub fn system() -> Self {
        Self(None)
    }

    /// A clock that asks `now`.
    pub fn from_fn(now: impl Fn() -> mailtide::Timestamp + Send + Sync + 'static) -> Self {
        Self(Some(std::sync::Arc::new(now)))
    }

    /// The current time.
    #[must_use]
    pub fn now(&self) -> mailtide::Timestamp {
        match &self.0 {
            Some(now) => now(),
            None => now(),
        }
    }
}

impl std::fmt::Debug for Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.0 {
            Some(_) => "Clock(host)",
            None => "Clock(system)",
        })
    }
}
