//! Identifiers and scalar types, parsed once at the boundary (SPEC §2).

use std::time::Duration;

/// A Gmail mailbox revision (`historyId`). Monotonically increasing per account.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct HistoryId(pub u64);

/// The mirrored mailbox: the user's email address, lowercased.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct AccountId(pub String);

/// An account address that isn't one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidAccount(pub String);

impl AccountId {
    /// Parse an email address into its canonical (trimmed, lowercased) form.
    pub fn parse(raw: &str) -> Result<Self, InvalidAccount> {
        let canonical = raw.trim().to_lowercase();
        match canonical.split_once('@') {
            Some((local, domain)) if !local.is_empty() && domain.contains('.') => {
                Ok(Self(canonical))
            }
            Some(_) | None => Err(InvalidAccount(raw.to_owned())),
        }
    }

    /// The address.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A Gmail message id.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct MessageId(pub String);

/// A Gmail draft id: stable for the life of a draft, while the message it
/// holds gets a new id on every save. Not a message id (SPEC §4).
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct DraftId(pub String);

/// A Gmail thread id. Gmail's threading is authoritative.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ThreadId(pub String);

/// A Gmail label id, system (`INBOX`, `UNREAD`, …) or user (`Label_…`).
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct LabelId(pub String);

/// An opaque Gmail pagination token.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PageToken(pub String);

/// Milliseconds since the Unix epoch, as reported by the host on each event.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// This instant plus `duration`, saturating.
    #[must_use]
    pub fn plus(self, duration: Duration) -> Self {
        let millis = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
        Self(self.0.saturating_add(millis))
    }

    /// This instant minus `duration`, saturating.
    #[must_use]
    pub fn minus(self, duration: Duration) -> Self {
        let millis = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
        Self(self.0.saturating_sub(millis))
    }

    /// The time from `earlier` to this instant, zero if `earlier` is later.
    #[must_use]
    pub fn since(self, earlier: Self) -> Duration {
        Duration::from_millis(u64::try_from(self.0.saturating_sub(earlier.0)).unwrap_or(0))
    }
}

/// A stable 64-bit hash (FNV-1a), for staggering and jitter. Unlike
/// `std::hash`, it is the same across builds and platforms (invariant I10).
#[must_use]
pub fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// One step of `SplitMix64`: a deterministic pseudo-random value from a seed.
#[must_use]
pub fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A fencing token: bumped by every engine instance that takes over an
/// account. The store rejects commits from older epochs (SPEC §10.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(pub u64);

/// The engine build that wrote a commit. A store refuses writes from a
/// version older than the newest it has seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EngineVersion(pub u32);

/// The engine version of this build.
pub const ENGINE_VERSION: EngineVersion = EngineVersion(1);

/// Identifies one command so its result can be matched. Carries the issuing
/// engine's epoch: a result for an older engine is ignored, never mistaken
/// for one of this engine's own commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandId {
    /// The issuing engine's epoch.
    pub epoch: Epoch,
    /// Its sequence within the epoch.
    pub seq: u64,
}

/// Identifies one commit, like [`CommandId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitId {
    /// The issuing engine's epoch.
    pub epoch: Epoch,
    /// Its sequence within the epoch.
    pub seq: u64,
}

/// A host-supplied key under which a message's raw content is stored.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct BlobRef(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_ids_are_canonical() {
        assert_eq!(
            AccountId::parse("  Alice@Example.com ").map(|a| a.0),
            Ok("alice@example.com".to_owned())
        );
        assert!(AccountId::parse("alice").is_err());
        assert!(AccountId::parse("@example.com").is_err());
    }
}
