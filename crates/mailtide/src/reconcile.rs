//! Full reconciliation and the initial import (SPEC §6.3).
//!
//! Capture the baseline before listing, enumerate per label (Gmail's list
//! returns ids only; per-label listing rebuilds membership cheaply), verify
//! absences with a `get` before tombstoning, then commit the baseline as the
//! cursor. Long enumerations keep the cursor moving so the baseline can't
//! expire (review C4).
//!
//! Zero counterpart: the reconciliation path in `lib/mail-history-store.ts`.

use crate::ids::{HistoryId, LabelId, PageToken};

/// What one enumeration pass lists.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum ListScope {
    /// Every message, including spam and trash. Finds messages with no
    /// labels at all (archived mail), which no per-label listing returns.
    All,
    /// Messages carrying one label: rebuilds membership cheaply.
    Label(LabelId),
}

/// Durable reconciliation progress, stored with each commit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReconcileProgress {
    /// Why reconciliation started.
    pub reason: ReconcileReason,
    /// Where it is.
    pub step: ReconcileStep,
}

/// Why reconciliation started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReconcileReason {
    /// The account has no cursor yet.
    Bootstrap,
    /// History returned 404.
    HistoryExpired,
    /// An operator asked for it.
    Requested,
}

/// The reconciliation steps, in order.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReconcileStep {
    /// Waiting for `getProfile` to fix the baseline.
    CaptureBaseline,
    /// Listing message ids label by label.
    Enumerating {
        /// The history id captured before listing.
        baseline: HistoryId,
        /// Scopes still to list, current first.
        labels_left: Vec<ListScope>,
        /// The page within the current scope.
        page_token: Option<PageToken>,
    },
    /// Confirming messages the listing did not return.
    Verifying {
        /// The history id captured before listing.
        baseline: HistoryId,
    },
}

impl ReconcileProgress {
    /// Start reconciling.
    #[must_use]
    pub fn begin(reason: ReconcileReason) -> Self {
        Self {
            reason,
            step: ReconcileStep::CaptureBaseline,
        }
    }

    /// The baseline, once captured.
    #[must_use]
    pub fn baseline(&self) -> Option<HistoryId> {
        match &self.step {
            ReconcileStep::CaptureBaseline => None,
            ReconcileStep::Enumerating { baseline, .. } | ReconcileStep::Verifying { baseline } => {
                Some(*baseline)
            }
        }
    }

    /// The step after the baseline is captured: list all mail, then every
    /// label in `labels`.
    #[must_use]
    pub fn with_baseline(self, baseline: HistoryId, labels: Vec<LabelId>) -> Self {
        Self {
            step: ReconcileStep::Enumerating {
                baseline,
                labels_left: std::iter::once(ListScope::All)
                    .chain(labels.into_iter().map(ListScope::Label))
                    .collect(),
                page_token: None,
            },
            ..self
        }
    }

    /// The step after a list page: the next page of the same label, the next
    /// label, or verification once every label is listed.
    #[must_use]
    pub fn after_page(self, next_page_token: Option<PageToken>) -> Self {
        let step = match self.step {
            ReconcileStep::Enumerating {
                baseline,
                mut labels_left,
                ..
            } => {
                if let Some(token) = next_page_token {
                    ReconcileStep::Enumerating {
                        baseline,
                        labels_left,
                        page_token: Some(token),
                    }
                } else {
                    if !labels_left.is_empty() {
                        labels_left.remove(0);
                    }
                    if labels_left.is_empty() {
                        ReconcileStep::Verifying { baseline }
                    } else {
                        ReconcileStep::Enumerating {
                            baseline,
                            labels_left,
                            page_token: None,
                        }
                    }
                }
            }
            step @ (ReconcileStep::CaptureBaseline | ReconcileStep::Verifying { .. }) => step,
        };
        Self { step, ..self }
    }

    /// The scope currently being listed and its page.
    #[must_use]
    pub fn listing(&self) -> Option<(&ListScope, Option<&PageToken>)> {
        match &self.step {
            ReconcileStep::Enumerating {
                labels_left,
                page_token,
                ..
            } => labels_left
                .first()
                .map(|label| (label, page_token.as_ref())),
            ReconcileStep::CaptureBaseline | ReconcileStep::Verifying { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(ids: &[&str]) -> Vec<LabelId> {
        ids.iter().map(|id| LabelId((*id).to_owned())).collect()
    }

    #[test]
    fn walks_labels_and_pages_then_verifies() {
        let progress = ReconcileProgress::begin(ReconcileReason::Bootstrap);
        assert_eq!(progress.baseline(), None);
        let progress = progress.with_baseline(HistoryId(100), labels(&["INBOX", "SENT"]));
        assert_eq!(progress.listing(), Some((&ListScope::All, None)));
        let progress = progress.after_page(None);
        assert_eq!(
            progress.listing(),
            Some((&ListScope::Label(LabelId("INBOX".into())), None))
        );
        let progress = progress.after_page(Some(PageToken("p2".into())));
        assert_eq!(
            progress.listing(),
            Some((
                &ListScope::Label(LabelId("INBOX".into())),
                Some(&PageToken("p2".into()))
            ))
        );
        let progress = progress.after_page(None);
        assert_eq!(
            progress.listing(),
            Some((&ListScope::Label(LabelId("SENT".into())), None))
        );
        let progress = progress.after_page(None);
        assert_eq!(
            progress.step,
            ReconcileStep::Verifying {
                baseline: HistoryId(100)
            }
        );
        assert_eq!(progress.baseline(), Some(HistoryId(100)));
    }
}
