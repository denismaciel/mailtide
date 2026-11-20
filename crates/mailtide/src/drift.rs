//! Drift checks: does the mirror agree with Gmail for a sample of messages?
//!
//! Pure: the host fetches both sides; this decides. A difference is only
//! drift when the mirror has no reason to differ: pending actions overlay
//! Gmail on purpose, unverified rows are waiting for a fetch, and a Gmail
//! copy newer than the mirror's checkpoint is lag the next sync closes.

use crate::gmail::MessageObservation;
use crate::labels::LabelSet;
use crate::mirror::MessageView;

/// One message's comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Both sides show the same labels.
    Matches,
    /// The mirror differs on purpose or for now (pending actions, awaiting
    /// verification, or Gmail changed after the mirror's last observation).
    Explained(Reason),
    /// The mirror shows different labels with no reason to: drift.
    Differs {
        /// What the mirror shows.
        mirror: LabelSet,
        /// What Gmail shows.
        gmail: LabelSet,
    },
    /// Gmail no longer has a message the mirror has.
    MissingInGmail,
}

/// Why a difference isn't drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// Actions the user took are not yet confirmed.
    PendingActions,
    /// The mirror is waiting to verify the row.
    Unverified,
    /// Gmail changed the message after the mirror last saw it.
    MirrorBehind,
}

/// Compare the mirror's view with Gmail's (`None`: Gmail returned 404).
#[must_use]
pub fn compare(mirror: &MessageView, gmail: Option<&MessageObservation>) -> Verdict {
    let Some(gmail) = gmail else {
        return Verdict::MissingInGmail;
    };
    if mirror.labels == gmail.labels {
        return Verdict::Matches;
    }
    if mirror.pending_actions > 0 {
        return Verdict::Explained(Reason::PendingActions);
    }
    if mirror.unverified {
        return Verdict::Explained(Reason::Unverified);
    }
    match mirror.checkpoint {
        Some(seen) if gmail.checkpoint <= seen => Verdict::Differs {
            mirror: mirror.labels.clone(),
            gmail: gmail.labels.clone(),
        },
        Some(_) | None => Verdict::Explained(Reason::MirrorBehind),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{HistoryId, MessageId, ThreadId};
    use crate::labels::set;

    fn view(labels: &[&str], checkpoint: u64) -> MessageView {
        MessageView {
            id: MessageId("m".into()),
            thread_id: ThreadId("t".into()),
            labels: set(labels.iter().copied()),
            observed_labels: set(labels.iter().copied()),
            bin_removals: LabelSet::new(),
            checkpoint: Some(HistoryId(checkpoint)),
            unverified: false,
            content: None,
            pending_actions: 0,
            blocked_actions: 0,
        }
    }

    fn gmail(labels: &[&str], checkpoint: u64) -> MessageObservation {
        MessageObservation {
            id: MessageId("m".into()),
            thread_id: ThreadId("t".into()),
            labels: set(labels.iter().copied()),
            checkpoint: HistoryId(checkpoint),
        }
    }

    #[test]
    fn verdicts() {
        let mirror = view(&["INBOX"], 10);
        assert_eq!(
            compare(&mirror, Some(&gmail(&["INBOX"], 12))),
            Verdict::Matches
        );
        assert_eq!(compare(&mirror, None), Verdict::MissingInGmail);
        assert_eq!(
            compare(&mirror, Some(&gmail(&["STARRED"], 10))),
            Verdict::Differs {
                mirror: set(["INBOX"]),
                gmail: set(["STARRED"]),
            }
        );
        assert_eq!(
            compare(&mirror, Some(&gmail(&["STARRED"], 11))),
            Verdict::Explained(Reason::MirrorBehind)
        );
        let pending = MessageView {
            pending_actions: 1,
            ..view(&["INBOX"], 10)
        };
        assert_eq!(
            compare(&pending, Some(&gmail(&[], 10))),
            Verdict::Explained(Reason::PendingActions)
        );
        let unverified = MessageView {
            unverified: true,
            ..view(&["INBOX"], 10)
        };
        assert_eq!(
            compare(&unverified, Some(&gmail(&[], 10))),
            Verdict::Explained(Reason::Unverified)
        );
    }
}
