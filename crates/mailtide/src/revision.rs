//! State strings and the change log for consumers (SPEC §4).
//!
//! Every applied commit appends change-log entries with a monotonic sequence,
//! in the same transaction. Hosts ask "what changed since state S", in the
//! spirit of JMAP `/changes` (RFC 8620).
//!
//! Zero counterpart: `lib/mail-data-revision.ts`.

use crate::ids::{MessageId, ThreadId};

/// A position in an account's change log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateSeq(pub u64);

/// The opaque form of a [`StateSeq`] handed to consumers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateString(pub String);

impl StateSeq {
    /// Encode for consumers.
    #[must_use]
    pub fn encode(self) -> StateString {
        StateString(format!("{PREFIX}{:x}", self.0))
    }

    /// Decode a consumer's state string.
    pub fn decode(state: &StateString) -> Result<Self, InvalidState> {
        let hex = state.0.strip_prefix(PREFIX).ok_or(InvalidState)?;
        u64::from_str_radix(hex, 16)
            .map(Self)
            .map_err(|_| InvalidState)
    }
}

const PREFIX: &str = "mt1-";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_strings_round_trip() {
        let seq = StateSeq(48_879);
        assert_eq!(seq.encode(), StateString("mt1-beef".into()));
        assert_eq!(StateSeq::decode(&seq.encode()), Ok(seq));
    }

    #[test]
    fn foreign_state_strings_are_rejected() {
        assert_eq!(
            StateSeq::decode(&StateString("beef".into())),
            Err(InvalidState)
        );
        assert_eq!(
            StateSeq::decode(&StateString("mt1-zz".into())),
            Err(InvalidState)
        );
    }
}

/// A state string that isn't one of ours.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidState;

/// What changed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Change {
    /// A message was created, relabelled or imported.
    Message(MessageId),
    /// A message was deleted.
    MessageDeleted(MessageId),
    /// A thread's derived state changed.
    Thread(ThreadId),
    /// Label definitions changed.
    Labels,
}

/// One page of changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangePage {
    /// Changes after the requested state.
    pub changes: Vec<Change>,
    /// The state to ask from next.
    pub next: StateSeq,
    /// Whether more changes are waiting.
    pub has_more: bool,
}
