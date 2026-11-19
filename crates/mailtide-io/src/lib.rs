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
