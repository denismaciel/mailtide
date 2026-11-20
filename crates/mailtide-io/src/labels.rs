//! Label definitions written through Gmail's labels API: create, rename,
//! restyle and delete, with sublabels (SPEC §6.6, `docs/features/label-edit.md`).
//!
//! Each write is one direct call (the crate's `direct_call`, what
//! [`Runner::call_gmail`] does for reads) through the quota
//! gate at Foreground, and Gmail's answer is recorded in the mirror at once
//! by [`Store::record_label`], unfenced like an admission: the host gets
//! Gmail's id back, `labels()` and the feed show the change before any sync,
//! and a listing sent before the write is not applied over it. mailtide
//! never repeats a write on its own; every write is safe for the host to
//! repeat.
//!
//! Gmail nests labels by name alone, and its API renames or deletes only the
//! label it is asked to (`label-edit-rename-parent`,
//! `label-edit-delete-parent`): moving or deleting sublabels is mailtide's,
//! in an order a failure half-way can resume from (sublabels first, the
//! label last).

use std::time::{Duration, Instant};

use mailtide::command::GmailRequest;
use mailtide::commit::LabelOp;
use mailtide::event::EventKind;
use mailtide::gmail::{ColorChange, Label, LabelColor, LabelKind, LabelPatch, NewLabel};
use mailtide::ids::{AccountId, LabelId};
use mailtide::label_names::{self, RenameConflict};
use mailtide::quota::Priority;
use mailtide::retry::{BlockingFailure, Retryable, RetryableFailure};
use mailtide_store::Store;

use crate::auth::TokenSource;
use crate::blobs::BlobStore;
use crate::quota_gate::QuotaGate;
use crate::runner::{CallError, QuotaDeferred, RunError, Runner};
use crate::transport::GmailTransport;

/// What a rename does to the label's sublabels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Sublabels {
    /// Move them with it (`A/x` becomes `B/x`), as Gmail's web interface
    /// and IMAP's `RENAME` do.
    #[default]
    Follow,
    /// Rename the label alone; its sublabels keep their names.
    Leave,
}

/// What a delete does when the label has sublabels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SublabelDeletion {
    /// Refuse with [`LabelError::HasSublabels`] (JMAP's `mailboxHasChild`).
    #[default]
    Refuse,
    /// Delete them too, deepest first, the label last.
    Delete,
}

/// What [`Runner::update_label`] changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelUpdate {
    /// The label as Gmail answered.
    pub label: Label,
    /// The sublabels moved with it, as Gmail answered each.
    pub sublabels: Vec<Label>,
}

/// What [`Runner::delete_label`] removed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LabelDeletion {
    /// Labels Gmail deleted, sublabels first.
    pub deleted: Vec<LabelId>,
    /// Labels already gone: a listing lacked them, or the delete found
    /// none (404). Recorded as removals all the same.
    pub already_gone: Vec<LabelId>,
}

/// Why a label write did not happen, or did only in part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelError {
    /// Gmail has no label with that id.
    NotFound,
    /// Another label holds the name (Gmail compares names case-insensitively),
    /// found by listing the labels; `None` if the listing found none or
    /// failed. A host retrying a create whose answer was lost can adopt it.
    NameTaken {
        /// The label holding the name.
        existing: Option<Label>,
    },
    /// Refused before any request: a blank name, a control character, a
    /// colour off Gmail's palette, a system label, a patch changing nothing.
    Invalid(String),
    /// The label has sublabels and the delete was asked to refuse then.
    HasSublabels {
        /// The sublabels, deepest first.
        ids: Vec<LabelId>,
    },
    /// Some of a rename's or a delete's labels changed before `error`: the
    /// same call again finishes what is left.
    Partial {
        /// The labels already renamed or deleted.
        done: Vec<LabelId>,
        /// What stopped the rest.
        error: Box<LabelError>,
    },
    /// The quota gate would hold the call past its budget: nothing was
    /// sent, nothing recorded.
    Deferred(QuotaDeferred),
    /// The runner is read-only: nothing was sent.
    ReadOnly,
    /// Gmail refused it for good (another 400, or a 403 such as a scope
    /// the token lacks): the caller's error; the account is not blocked.
    Refused(BlockingFailure),
    /// Gmail failed or limited it (a 5xx, a rate limit): try again later.
    Retryable(RetryableFailure),
    /// The write reached Gmail, or may have, and its answer was lost: it
    /// may or may not have been applied. The next listing shows which.
    OutcomeUnknown(String),
    /// The runner's own resources failed (the store, the quota gate).
    Run(RunError),
}

impl LabelError {
    /// A stable name for logs and hosts.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::NameTaken { .. } => "name_taken",
            Self::Invalid(_) => "invalid",
            Self::HasSublabels { .. } => "has_sublabels",
            Self::Partial { .. } => "partial",
            Self::Deferred(_) => "deferred",
            Self::ReadOnly => "read_only",
            Self::Refused(_) => "refused",
            Self::Retryable(_) => "retryable",
            Self::OutcomeUnknown(_) => "outcome_unknown",
            Self::Run(error) => error.code(),
        }
    }

    /// `self`, after `done` changed: [`LabelError::Partial`] unless nothing
    /// did.
    fn after(self, done: Vec<LabelId>) -> Self {
        if done.is_empty() {
            self
        } else {
            Self::Partial {
                done,
                error: Box::new(self),
            }
        }
    }
}

impl From<CallError> for LabelError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::ReadOnly => Self::ReadOnly,
            CallError::Deferred(deferred) => Self::Deferred(deferred),
            CallError::NotFound => Self::NotFound,
            CallError::Refused(refusal) => Self::Refused(refusal),
            CallError::Retryable(failure) => Self::Retryable(failure),
            CallError::OutcomeUnknown(detail) => Self::OutcomeUnknown(detail),
            CallError::Run(error) => Self::Run(error),
        }
    }
}

/// Whether `id` is one of Gmail's fixed system label ids.
fn fixed_system(id: &LabelId) -> bool {
    mailtide::labels::system::is_system(&id.0)
}

fn store_error(error: mailtide_store::StoreError) -> LabelError {
    LabelError::Run(RunError::Store(error))
}

fn short_listing() -> LabelError {
    LabelError::Retryable(RetryableFailure {
        class: Retryable::Transient,
        retry_after: None,
        detail: "a label listing omitted a live label or a core system label".into(),
    })
}

fn unexpected() -> LabelError {
    LabelError::Run(RunError::Invariant("unexpected answer to a label request"))
}

/// A name the caller gave, checked as Gmail would take it.
fn checked_name(name: &str) -> Result<(), LabelError> {
    label_names::validate(name).map_err(|e| LabelError::Invalid(e.to_string()))
}

/// A colour the caller gave, checked against Gmail's palette and
/// lower-cased.
fn checked_color(color: &LabelColor) -> Result<LabelColor, LabelError> {
    mailtide_gmail::labels::palette_color(&color.text, &color.background)
        .map_err(|e| LabelError::Invalid(e.to_string()))
}

impl<S, B, T, Q, A> Runner<S, B, T, Q, A>
where
    S: Store,
    B: BlobStore,
    T: GmailTransport,
    Q: QuotaGate,
    A: TokenSource,
{
    /// Create a user label and record it: the returned label carries
    /// Gmail's id, ready to use in an action. Both visibilities are sent.
    /// A name another label holds is [`LabelError::NameTaken`] with that
    /// label (after one listing), so a host retrying after a lost answer
    /// can adopt the label its first attempt created.
    pub async fn create_label(
        &self,
        account: &AccountId,
        label: NewLabel,
        budget: Duration,
    ) -> Result<Label, LabelError> {
        self.writable().map_err(|_| LabelError::ReadOnly)?;
        checked_name(&label.name)?;
        self.tracked(account).await?;
        let label = NewLabel {
            color: label.color.as_ref().map(checked_color).transpose()?,
            ..label
        };
        let deadline = Instant::now() + budget;
        let name = label.name.clone();
        match self
            .direct_call(
                account,
                GmailRequest::CreateLabel(label),
                Priority::Foreground,
                deadline,
            )
            .await?
        {
            EventKind::LabelSaved { label, .. } => {
                self.record(account, LabelOp::Upsert(label.clone())).await?;
                Ok(label)
            }
            EventKind::LabelNameTaken { .. } => {
                Err(self.name_taken(account, &name, None, deadline).await)
            }
            _ => Err(unexpected()),
        }
    }

    /// Change label `id`: only what `patch` names is sent (a rename keeps
    /// the colour and visibility, a restyle the name). A rename with
    /// [`Sublabels::Follow`] lists the labels, checks every name it needs
    /// is free, then moves each sublabel and the label last; a failure
    /// half-way is [`LabelError::Partial`], and the same call again
    /// finishes it. A system label the mirror knows is refused before any
    /// request; an id it does not know is left to Gmail.
    pub async fn update_label(
        &self,
        account: &AccountId,
        id: &LabelId,
        patch: LabelPatch,
        sublabels: Sublabels,
        budget: Duration,
    ) -> Result<LabelUpdate, LabelError> {
        self.writable().map_err(|_| LabelError::ReadOnly)?;
        if patch.is_empty() {
            return Err(LabelError::Invalid("the patch changes nothing".into()));
        }
        if let Some(name) = &patch.name {
            checked_name(name)?;
        }
        let patch = LabelPatch {
            color: match patch.color {
                ColorChange::Set(color) => ColorChange::Set(checked_color(&color)?),
                other => other,
            },
            ..patch
        };
        self.tracked(account).await?;
        self.refuse_system(account, id, None).await?;
        let deadline = Instant::now() + budget;
        let renames = match (&patch.name, sublabels) {
            (Some(name), Sublabels::Follow) => {
                let listed = self.list(account, deadline).await?;
                self.refuse_system(account, id, Some(&listed)).await?;
                match label_names::plan_rename(&listed, id, name) {
                    Ok(renames) => renames,
                    Err(RenameConflict::NotFound) => return Err(LabelError::NotFound),
                    Err(RenameConflict::UnderItself) => {
                        return Err(LabelError::Invalid(format!(
                            "{name:?} is under the label's own name: its sublabels would move into themselves"
                        )));
                    }
                    Err(RenameConflict::Taken(holder)) => {
                        return Err(LabelError::NameTaken {
                            existing: Some(holder),
                        });
                    }
                }
            }
            (Some(_) | None, _) => Vec::new(),
        };
        let mut moved = Vec::with_capacity(renames.len());
        for (sublabel, name) in renames {
            let rename = LabelPatch {
                name: Some(name),
                ..LabelPatch::default()
            };
            match self.patch_label(account, &sublabel, rename, deadline).await {
                Ok(label) => moved.push(label),
                Err(error) => {
                    return Err(error.after(moved.into_iter().map(|l| l.id).collect()));
                }
            }
        }
        match self.patch_label(account, id, patch, deadline).await {
            Ok(label) => Ok(LabelUpdate {
                label,
                sublabels: moved,
            }),
            Err(error) => Err(error.after(moved.into_iter().map(|l| l.id).collect())),
        }
    }

    /// Delete label `id` and record it: it leaves every mirrored message at
    /// once. The labels are listed first: a label the listing lacks is
    /// checked with a get. Its 404 records the removal without a write, as
    /// does a 404 from the delete. With sublabels, refused unless
    /// `sublabels` is [`SublabelDeletion::Delete`], which deletes them
    /// deepest first and the label last.
    pub async fn delete_label(
        &self,
        account: &AccountId,
        id: &LabelId,
        sublabels: SublabelDeletion,
        budget: Duration,
    ) -> Result<LabelDeletion, LabelError> {
        self.writable().map_err(|_| LabelError::ReadOnly)?;
        self.tracked(account).await?;
        self.refuse_system(account, id, None).await?;
        let deadline = Instant::now() + budget;
        let listed = self.list(account, deadline).await?;
        let mut deletion = LabelDeletion::default();
        let Some(label) = listed.iter().find(|label| &label.id == id) else {
            // Gone if the mirror defined it; an id nobody ever had is not
            // found, and nothing is remembered for it.
            let known = self
                .store()
                .labels(account)
                .await
                .map_err(store_error)?
                .iter()
                .any(|label| &label.id == id);
            if !known {
                return Err(LabelError::NotFound);
            }
            match self
                .direct_call(
                    account,
                    GmailRequest::GetLabel { id: id.clone() },
                    Priority::Foreground,
                    deadline,
                )
                .await
            {
                Err(CallError::NotFound) => {}
                Ok(EventKind::LabelFetched { .. } | EventKind::LabelExists { .. }) => {
                    return Err(short_listing());
                }
                Ok(_) => return Err(unexpected()),
                Err(error) => return Err(error.into()),
            }
            self.removed(account, id).await?;
            deletion.already_gone.push(id.clone());
            return Ok(deletion);
        };
        self.refuse_system(account, id, Some(&listed)).await?;
        let nested: Vec<LabelId> = label_names::sublabels(&listed, &label.name)
            .into_iter()
            .map(|label| label.id.clone())
            .collect();
        if !nested.is_empty() && sublabels == SublabelDeletion::Refuse {
            return Err(LabelError::HasSublabels { ids: nested });
        }
        for target in nested.into_iter().chain([id.clone()]) {
            let done = |deletion: &LabelDeletion| -> Vec<LabelId> {
                deletion
                    .deleted
                    .iter()
                    .chain(&deletion.already_gone)
                    .cloned()
                    .collect()
            };
            match self
                .direct_call(
                    account,
                    GmailRequest::DeleteLabel { id: target.clone() },
                    Priority::Foreground,
                    deadline,
                )
                .await
            {
                Ok(EventKind::LabelDeleted { .. }) => {
                    self.removed(account, &target).await?;
                    deletion.deleted.push(target);
                }
                Err(CallError::NotFound) => {
                    self.removed(account, &target).await?;
                    deletion.already_gone.push(target);
                }
                Ok(_) => return Err(unexpected().after(done(&deletion))),
                Err(error) => return Err(LabelError::from(error).after(done(&deletion))),
            }
        }
        Ok(deletion)
    }

    /// Refuse an account the store does not track (or a database whose
    /// schema this build does not match) before anything reaches Gmail: a
    /// write whose answer could not be recorded must not be sent.
    async fn tracked(&self, account: &AccountId) -> Result<(), LabelError> {
        match self.store().status(account).await.map_err(store_error)? {
            Some(_) => Ok(()),
            None => Err(store_error(mailtide_store::StoreError::NotTracked)),
        }
    }

    /// `labels.list`, for a write that needs Gmail's current definitions.
    /// A listing that lacks Gmail's system labels (cut short) proves
    /// nothing about what it lacks: it is asked again later, never used.
    async fn list(&self, account: &AccountId, deadline: Instant) -> Result<Vec<Label>, LabelError> {
        let EventKind::LabelsListed { labels: listed, .. } = self
            .direct_call(
                account,
                GmailRequest::ListLabels,
                Priority::Foreground,
                deadline,
            )
            .await?
        else {
            return Err(unexpected());
        };
        if !mailtide::mirror::complete_listing(&listed) {
            return Err(short_listing());
        }
        Ok(listed)
    }

    /// One `labels.patch`, recorded.
    async fn patch_label(
        &self,
        account: &AccountId,
        id: &LabelId,
        patch: LabelPatch,
        deadline: Instant,
    ) -> Result<Label, LabelError> {
        let name = patch.name.clone();
        match self
            .direct_call(
                account,
                GmailRequest::PatchLabel {
                    id: id.clone(),
                    patch,
                },
                Priority::Foreground,
                deadline,
            )
            .await?
        {
            EventKind::LabelSaved { label, .. } => {
                self.record(account, LabelOp::Upsert(label.clone())).await?;
                Ok(label)
            }
            EventKind::LabelNameTaken { .. } => Err(self
                .name_taken(
                    account,
                    name.as_deref().unwrap_or_default(),
                    Some(id),
                    deadline,
                )
                .await),
            _ => Err(unexpected()),
        }
    }

    /// Gmail answered 409 for `name`: the label holding it, from one
    /// listing (not `except`, the label being renamed).
    async fn name_taken(
        &self,
        account: &AccountId,
        name: &str,
        except: Option<&LabelId>,
        deadline: Instant,
    ) -> LabelError {
        let existing = self.list(account, deadline).await.ok().and_then(|labels| {
            labels.into_iter().find(|label| {
                Some(&label.id) != except && label_names::same_name(&label.name, name)
            })
        });
        LabelError::NameTaken { existing }
    }

    /// Refuse a system label: one of Gmail's fixed ids whatever anyone
    /// lists, or one `listed` (or else the mirror's definitions) calls a
    /// system label; any other id is left to Gmail.
    async fn refuse_system(
        &self,
        account: &AccountId,
        id: &LabelId,
        listed: Option<&[Label]>,
    ) -> Result<(), LabelError> {
        let system = |labels: &[Label]| {
            labels
                .iter()
                .any(|label| &label.id == id && label.kind == LabelKind::System)
        };
        let refused = fixed_system(id)
            || match listed {
                Some(labels) => system(labels),
                None => system(&self.store().labels(account).await.map_err(store_error)?),
            };
        if refused {
            return Err(LabelError::Invalid(format!(
                "{} is a system label: Gmail's own, not to be renamed or deleted",
                id.0
            )));
        }
        Ok(())
    }

    /// Record that label `id` is gone, now.
    async fn removed(&self, account: &AccountId, id: &LabelId) -> Result<(), LabelError> {
        self.record(
            account,
            LabelOp::Remove {
                id: id.clone(),
                at: self.now(),
            },
        )
        .await
    }

    /// Record Gmail's answer to a write in the mirror.
    async fn record(&self, account: &AccountId, op: LabelOp) -> Result<(), LabelError> {
        self.store()
            .record_label(account, &op)
            .await
            .map_err(store_error)
    }
}
