//! Label writes through the runner against a fake Gmail
//! (`features/label-edit.toml`): Gmail's answer is in the mirror at once,
//! lost answers and half-done renames are safe to repeat, labels changed
//! elsewhere reach every message, and nothing blocks the account.

pub mod label_support;

use std::time::Duration;

use label_support::{
    BUDGET, Definition, Fault, Harness, LabelRunner, Mailbox, account, feed_since, harness, head,
    labels, sync, user_names, view,
};
use mailtide::gmail::{
    ColorChange, LabelColor, LabelListVisibility, LabelPatch, MessageListVisibility, NewLabel,
};
use mailtide::ids::{LabelId, MessageId};
use mailtide::labels::set;
use mailtide::outbox::{Action, ActionRequest, LabelDelta};
use mailtide::quota::{Priority, QuotaUnits};
use mailtide::retry::{AccountProblem, Blocking, Retryable};
use mailtide::revision::Change;
use mailtide::state::SyncPhase;
use mailtide_io::labels::{LabelError, SublabelDeletion, Sublabels};
use mailtide_io::runner::Access;

/// A label refresh is due after this long (`label_refresh` is 15 minutes).
const REFRESH: Duration = Duration::from_mins(16);

/// A synced runner over a mailbox `setup` fills.
async fn synced(access: Access, setup: impl FnOnce(&mut Mailbox)) -> Harness {
    let harness = harness(access);
    setup(&mut harness.transport().mailbox());
    sync(&harness).await;
    harness
}

fn new_label(name: &str) -> NewLabel {
    NewLabel {
        name: name.into(),
        list_visibility: LabelListVisibility::Show,
        message_visibility: MessageListVisibility::Show,
        color: None,
    }
}

fn renamed(name: &str) -> LabelPatch {
    LabelPatch {
        name: Some(name.into()),
        ..LabelPatch::default()
    }
}

fn green() -> LabelColor {
    LabelColor {
        text: "#ffffff".into(),
        background: "#16a766".into(),
    }
}

fn id(id: &str) -> LabelId {
    LabelId(id.into())
}

fn requests(runner: &LabelRunner) -> usize {
    runner.transport().mailbox().requests.len()
}

fn writes(runner: &LabelRunner) -> Vec<String> {
    runner.transport().mailbox().writes()
}

fn removed(change: &Change) -> Option<(&str, Vec<&str>)> {
    match change {
        Change::LabelsChanged { id, removed, .. } => Some((
            id.0.as_str(),
            removed.iter().map(|l| l.0.as_str()).collect(),
        )),
        _ => None,
    }
}

#[tokio::test]
async fn a_created_label_is_in_the_definitions_before_any_sync() {
    let runner = synced(Access::ReadWrite, |_| {}).await;
    let before = head(&runner).await;
    let sent = requests(&runner);
    let label = runner
        .create_label(&account(), new_label("Receipts"), BUDGET)
        .await
        .expect("created");
    assert_eq!(
        (label.id.0.as_str(), label.name.as_str()),
        ("Label_1", "Receipts")
    );
    assert_eq!(label.list_visibility, Some(LabelListVisibility::Show));
    assert!(labels(&runner).await.contains(&label));
    assert_eq!(
        feed_since(&runner, before).await,
        vec![Change::LabelDefinitionsChanged]
    );
    assert_eq!(
        runner.transport().mailbox().requests[sent..],
        ["POST labels"],
        "one request, no sync"
    );
    let admitted = runner.admissions();
    assert_eq!(
        admitted.last(),
        Some(&(QuotaUnits(5), Priority::Foreground))
    );
}

#[tokio::test]
async fn a_new_label_can_be_applied_to_a_message_at_once() {
    let runner = synced(Access::ReadWrite, |mailbox| mailbox.add("m1", &["INBOX"])).await;
    let label = runner
        .create_label(&account(), new_label("Travel"), BUDGET)
        .await
        .expect("created");
    let action = ActionRequest::Messages {
        ids: vec![MessageId("m1".into())],
        action: Action::ChangeLabels(
            LabelDelta::new(vec![label.id.clone()], Vec::new()).expect("valid"),
        ),
    };
    runner
        .act(&account(), action, Duration::from_secs(30))
        .await
        .expect("act");
    let gmail = runner.transport().mailbox().messages["m1"].0.clone();
    assert_eq!(gmail, ["INBOX", label.id.0.as_str()]);
    assert_eq!(view(&runner, "m1").await.labels, set(["INBOX", "Label_1"]));
}

#[tokio::test]
async fn a_restyle_keeps_the_name_and_a_rename_keeps_the_style() {
    let runner = synced(Access::ReadWrite, |_| {}).await;
    let label = runner
        .create_label(
            &account(),
            NewLabel {
                list_visibility: LabelListVisibility::ShowIfUnread,
                message_visibility: MessageListVisibility::Hide,
                color: Some(green()),
                ..new_label("Work")
            },
            BUDGET,
        )
        .await
        .expect("created");
    let restyled = runner
        .update_label(
            &account(),
            &label.id,
            LabelPatch {
                // Gmail takes lower case only: the colour is sent lowered.
                color: ColorChange::Set(LabelColor {
                    text: "#FB4C2F".into(),
                    background: "#16A766".into(),
                }),
                ..LabelPatch::default()
            },
            Sublabels::Follow,
            BUDGET,
        )
        .await
        .expect("restyled")
        .label;
    assert_eq!(restyled.name, "Work");
    assert_eq!(
        restyled.color.as_ref().map(|c| c.text.as_str()),
        Some("#fb4c2f")
    );
    let renamed = runner
        .update_label(
            &account(),
            &label.id,
            renamed("Job"),
            Sublabels::Follow,
            BUDGET,
        )
        .await
        .expect("renamed")
        .label;
    assert_eq!(renamed.name, "Job");
    assert_eq!(renamed.color, restyled.color);
    assert_eq!(
        renamed.list_visibility,
        Some(LabelListVisibility::ShowIfUnread)
    );
    assert_eq!(
        renamed.message_visibility,
        Some(MessageListVisibility::Hide)
    );
    assert!(labels(&runner).await.contains(&renamed));
    let cleared = runner
        .update_label(
            &account(),
            &label.id,
            LabelPatch {
                color: ColorChange::Clear,
                ..LabelPatch::default()
            },
            Sublabels::Follow,
            BUDGET,
        )
        .await
        .expect("cleared")
        .label;
    assert_eq!((cleared.name.as_str(), cleared.color), ("Job", None));
}

#[tokio::test]
async fn a_create_that_times_out_then_finds_the_name_taken_reports_the_existing_label() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.fail_next("POST labels", Fault::LostAfterApply);
    })
    .await;
    let sent = requests(&runner);
    let lost = runner
        .create_label(&account(), new_label("Receipts"), BUDGET)
        .await;
    assert!(
        matches!(lost, Err(LabelError::OutcomeUnknown(_))),
        "{lost:?}"
    );
    let retried = runner
        .create_label(&account(), new_label("receipts "), BUDGET)
        .await;
    let Err(LabelError::NameTaken {
        existing: Some(existing),
    }) = retried
    else {
        panic!("{retried:?}");
    };
    assert_eq!(
        (existing.id.0.as_str(), existing.name.as_str()),
        ("Label_1", "Receipts")
    );
    assert_eq!(
        runner.transport().mailbox().requests[sent..],
        ["POST labels", "POST labels", "GET labels"],
        "the lost create, the retry, one listing"
    );
}

#[tokio::test]
async fn a_rename_renames_sublabels_first_and_a_retry_finishes_it() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.create_label("A");
        mailbox.create_label("A/x");
        mailbox.create_label("A/y");
    })
    .await;
    runner
        .transport()
        .mailbox()
        .fail_next("PATCH labels/Label_3", Fault::Status(500, "backendError"));
    let first = runner
        .update_label(
            &account(),
            &id("Label_1"),
            renamed("B"),
            Sublabels::Follow,
            BUDGET,
        )
        .await;
    let Err(LabelError::Partial { done, error }) = first else {
        panic!("{first:?}");
    };
    assert_eq!(done, [id("Label_2")]);
    assert!(matches!(*error, LabelError::Retryable(_)), "{error:?}");
    assert_eq!(
        user_names(&runner).await["Label_1"],
        "A",
        "the label moves last"
    );
    let update = runner
        .update_label(
            &account(),
            &id("Label_1"),
            renamed("B"),
            Sublabels::Follow,
            BUDGET,
        )
        .await
        .expect("finished");
    assert_eq!(update.label.name, "B");
    assert_eq!(
        update
            .sublabels
            .iter()
            .map(|l| l.name.as_str())
            .collect::<Vec<_>>(),
        ["B/y"],
        "only what was left"
    );
    assert_eq!(
        writes(&runner),
        [
            "PATCH labels/Label_2",
            "PATCH labels/Label_3",
            "PATCH labels/Label_3",
            "PATCH labels/Label_1"
        ]
    );
    assert_eq!(
        user_names(&runner).await.into_values().collect::<Vec<_>>(),
        ["B", "B/x", "B/y"]
    );
}

#[tokio::test]
async fn a_rename_onto_a_taken_name_changes_nothing() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.create_label("A");
        mailbox.create_label("A/x");
        mailbox.create_label("b/x");
        mailbox.create_label("C");
    })
    .await;
    let sublabel_target = runner
        .update_label(
            &account(),
            &id("Label_1"),
            renamed("B"),
            Sublabels::Follow,
            BUDGET,
        )
        .await;
    assert!(
        matches!(&sublabel_target, Err(LabelError::NameTaken { existing: Some(l) }) if l.name == "b/x"),
        "{sublabel_target:?}"
    );
    let own_target = runner
        .update_label(
            &account(),
            &id("Label_1"),
            renamed("c"),
            Sublabels::Follow,
            BUDGET,
        )
        .await;
    assert!(
        matches!(&own_target, Err(LabelError::NameTaken { existing: Some(l) }) if l.name == "C"),
        "{own_target:?}"
    );
    assert!(writes(&runner).is_empty(), "nothing was sent");
    // Without the pre-check, Gmail's own 409 says the same.
    let gmail_says = runner
        .update_label(
            &account(),
            &id("Label_1"),
            renamed("C"),
            Sublabels::Leave,
            BUDGET,
        )
        .await;
    assert!(
        matches!(&gmail_says, Err(LabelError::NameTaken { existing: Some(l) }) if l.id == id("Label_4")),
        "{gmail_says:?}"
    );
    assert_eq!(
        user_names(&runner).await.into_values().collect::<Vec<_>>(),
        ["A", "A/x", "b/x", "C"]
    );
}

#[tokio::test]
async fn a_rename_can_leave_sublabels_alone() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.create_label("A");
        mailbox.create_label("A/x");
    })
    .await;
    let sent = requests(&runner);
    let update = runner
        .update_label(
            &account(),
            &id("Label_1"),
            renamed("B"),
            Sublabels::Leave,
            BUDGET,
        )
        .await
        .expect("renamed");
    assert!(update.sublabels.is_empty());
    assert_eq!(
        runner.transport().mailbox().requests[sent..],
        ["PATCH labels/Label_1"],
        "one patch, no listing"
    );
    assert_eq!(
        user_names(&runner).await.into_values().collect::<Vec<_>>(),
        ["B", "A/x"]
    );
}

#[tokio::test]
async fn deleting_a_label_with_sublabels_is_refused_unless_asked() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.create_label("A");
        mailbox.create_label("A/x");
        mailbox.create_label("A/x/y");
        mailbox.create_label("Ab");
    })
    .await;
    let refused = runner
        .delete_label(&account(), &id("Label_1"), SublabelDeletion::Refuse, BUDGET)
        .await;
    assert_eq!(
        refused,
        Err(LabelError::HasSublabels {
            ids: vec![id("Label_3"), id("Label_2")]
        })
    );
    assert!(writes(&runner).is_empty());
    let deletion = runner
        .delete_label(&account(), &id("Label_1"), SublabelDeletion::Delete, BUDGET)
        .await
        .expect("deleted");
    assert_eq!(
        deletion.deleted,
        [id("Label_3"), id("Label_2"), id("Label_1")]
    );
    assert!(deletion.already_gone.is_empty());
    assert_eq!(
        writes(&runner),
        [
            "DELETE labels/Label_3",
            "DELETE labels/Label_2",
            "DELETE labels/Label_1"
        ],
        "deepest first, the label last"
    );
    assert_eq!(
        user_names(&runner).await.into_values().collect::<Vec<_>>(),
        ["Ab"]
    );
}

#[tokio::test]
async fn a_deleted_label_leaves_every_message_at_once() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let label = mailbox.create_label("Old");
        mailbox.add("m1", &["INBOX", &label]);
        mailbox.add("m2", &[&label]);
        mailbox.add("m3", &["INBOX"]);
    })
    .await;
    let before = head(&runner).await;
    let deletion = runner
        .delete_label(&account(), &id("Label_1"), SublabelDeletion::Refuse, BUDGET)
        .await
        .expect("deleted");
    assert_eq!(deletion.deleted, [id("Label_1")]);
    assert_eq!(view(&runner, "m1").await.labels, set(["INBOX"]));
    assert_eq!(view(&runner, "m2").await.labels, set([]));
    let feed = feed_since(&runner, before).await;
    assert_eq!(feed[0], Change::LabelDefinitionsChanged);
    assert_eq!(
        feed.iter().filter_map(removed).collect::<Vec<_>>(),
        [("m1", vec!["Label_1"]), ("m2", vec!["Label_1"])],
        "before any history is read"
    );
    // Gmail's own labelsRemoved records then change nothing.
    let after = head(&runner).await;
    sync(&runner).await;
    assert!(
        feed_since(&runner, after)
            .await
            .iter()
            .all(|c| removed(c).is_none())
    );
}

#[tokio::test]
async fn deleting_a_missing_label_reports_already_gone() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let label = mailbox.create_label("Old");
        mailbox.add("m1", &[&label]);
        mailbox.create_label("Racing");
        mailbox.fail_next("DELETE labels/Label_1", Fault::LostAfterApply);
    })
    .await;
    let lost = runner
        .delete_label(&account(), &id("Label_1"), SublabelDeletion::Refuse, BUDGET)
        .await;
    assert!(
        matches!(lost, Err(LabelError::OutcomeUnknown(_))),
        "{lost:?}"
    );
    let retried = runner
        .delete_label(&account(), &id("Label_1"), SublabelDeletion::Refuse, BUDGET)
        .await
        .expect("safe to retry");
    assert_eq!(
        retried.already_gone,
        [id("Label_1")],
        "the listing lacks it"
    );
    assert_eq!(view(&runner, "m1").await.labels, set([]));
    // Listed, then gone by the time of the delete: the 404 is proof too.
    runner
        .transport()
        .mailbox()
        .gone_on_delete
        .push("Label_2".into());
    let raced = runner
        .delete_label(&account(), &id("Label_2"), SublabelDeletion::Refuse, BUDGET)
        .await
        .expect("a 404 is no failure");
    assert_eq!(raced.already_gone, [id("Label_2")]);
    assert!(user_names(&runner).await.is_empty());
}

#[tokio::test]
async fn a_label_deleted_elsewhere_without_history_records_still_leaves_every_message() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let label = mailbox.create_label("Old");
        mailbox.add("m1", &["INBOX", &label]);
    })
    .await;
    runner.transport().mailbox().delete_label("Label_1", false);
    runner.advance(REFRESH);
    sync(&runner).await;
    assert_eq!(view(&runner, "m1").await.labels, set(["INBOX"]));
    assert!(
        runner
            .transport()
            .mailbox()
            .requests
            .iter()
            .any(|r| r == "GET labels/Label_1")
    );
    assert!(user_names(&runner).await.is_empty());
}

#[tokio::test]
async fn history_from_before_a_deletion_cannot_bring_the_label_back() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.create_label("Old");
        mailbox.add("m1", &["INBOX"]);
    })
    .await;
    // Another client applies the label; the mirror has not read it yet.
    runner.transport().mailbox().modify("m1", &["Label_1"], &[]);
    runner
        .delete_label(&account(), &id("Label_1"), SublabelDeletion::Refuse, BUDGET)
        .await
        .expect("deleted");
    let before = head(&runner).await;
    // The cursor is behind the deletion: history replays the labelsAdded.
    sync(&runner).await;
    assert_eq!(view(&runner, "m1").await.labels, set(["INBOX"]));
    assert!(
        feed_since(&runner, before).await.iter().all(|c| !matches!(
            c,
            Change::LabelsChanged { added, .. } if added.contains(&id("Label_1"))
        )),
        "the dead id never shows on the row"
    );
}

#[tokio::test]
async fn a_pending_action_on_a_label_deleted_elsewhere_is_dropped_not_blocked() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.create_label("Old");
        mailbox.add("m1", &["INBOX"]);
    })
    .await;
    let action = ActionRequest::Messages {
        ids: vec![MessageId("m1".into())],
        action: Action::ChangeLabels(
            LabelDelta::new(vec![id("Label_1")], Vec::new()).expect("valid"),
        ),
    };
    runner.admit(&account(), &action).await.expect("admitted");
    assert_eq!(view(&runner, "m1").await.pending_actions, 1);
    runner.transport().mailbox().delete_label("Label_1", true);
    runner.advance(REFRESH);
    sync(&runner).await;
    let shown = view(&runner, "m1").await;
    assert_eq!(
        (shown.labels, shown.pending_actions, shown.blocked_actions),
        (set(["INBOX"]), 0, 0)
    );
    assert!(writes(&runner).is_empty());
}

#[tokio::test]
async fn a_label_renamed_elsewhere_shows_under_the_same_id() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let label = mailbox.create_label("Old");
        mailbox.add("m1", &[&label]);
    })
    .await;
    let before = head(&runner).await;
    runner.transport().mailbox().rename_label("Label_1", "New");
    runner.advance(REFRESH);
    sync(&runner).await;
    assert_eq!(user_names(&runner).await["Label_1"], "New");
    assert_eq!(
        feed_since(&runner, before).await,
        vec![Change::LabelDefinitionsChanged],
        "no message row changes"
    );
    assert_eq!(view(&runner, "m1").await.labels, set(["Label_1"]));
}

#[tokio::test]
async fn a_failed_listing_keeps_the_definitions() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let label = mailbox.create_label("Old");
        mailbox.add("m1", &[&label]);
    })
    .await;
    runner.transport().mailbox().rename_label("Label_1", "New");
    for fault in [Fault::Status(500, "backendError"), Fault::Undecodable] {
        runner.transport().mailbox().fail_next("GET labels", fault);
        runner.advance(REFRESH);
        let outcome = sync(&runner).await;
        assert_eq!(
            outcome.report.status.map(|s| s.phase),
            Some(SyncPhase::Live),
            "never blocked"
        );
        assert_eq!(user_names(&runner).await["Label_1"], "Old");
        assert_eq!(view(&runner, "m1").await.labels, set(["Label_1"]));
    }
}

#[tokio::test]
async fn a_system_label_cannot_be_renamed_or_deleted() {
    let runner = synced(Access::ReadWrite, |_| {}).await;
    let sent = requests(&runner);
    let rename = runner
        .update_label(
            &account(),
            &id("INBOX"),
            renamed("Mine"),
            Sublabels::Follow,
            BUDGET,
        )
        .await;
    assert!(matches!(rename, Err(LabelError::Invalid(_))), "{rename:?}");
    let delete = runner
        .delete_label(&account(), &id("INBOX"), SublabelDeletion::Delete, BUDGET)
        .await;
    assert!(matches!(delete, Err(LabelError::Invalid(_))), "{delete:?}");
    assert_eq!(requests(&runner), sent, "refused before any call");
    // An id the mirror doesn't know is left to Gmail.
    let restyle = LabelPatch {
        color: ColorChange::Set(green()),
        ..LabelPatch::default()
    };
    let unknown = runner
        .update_label(
            &account(),
            &id("Label_9"),
            restyle,
            Sublabels::Follow,
            BUDGET,
        )
        .await;
    assert_eq!(unknown, Err(LabelError::NotFound));
    assert_eq!(writes(&runner), ["PATCH labels/Label_9"]);
}

#[tokio::test]
async fn label_writes_are_refused_in_read_only_mode() {
    let runner = synced(Access::ReadOnly, |mailbox| {
        let label = mailbox.create_label("Styled");
        if let Some(definition) = mailbox.labels.get_mut(&label) {
            definition.color = Some(("#ffffff".into(), "#16a766".into()));
        }
    })
    .await;
    let styled = labels(&runner)
        .await
        .into_iter()
        .find(|l| l.name == "Styled")
        .expect("mirrored");
    assert_eq!(
        styled.color,
        Some(green()),
        "definitions are still mirrored"
    );
    assert_eq!(styled.list_visibility, Some(LabelListVisibility::Show));
    let sent = requests(&runner);
    assert_eq!(
        runner
            .create_label(&account(), new_label("New"), BUDGET)
            .await,
        Err(LabelError::ReadOnly)
    );
    assert_eq!(
        runner
            .update_label(
                &account(),
                &styled.id,
                renamed("X"),
                Sublabels::Follow,
                BUDGET
            )
            .await,
        Err(LabelError::ReadOnly)
    );
    assert_eq!(
        runner
            .delete_label(&account(), &styled.id, SublabelDeletion::Refuse, BUDGET)
            .await,
        Err(LabelError::ReadOnly)
    );
    assert_eq!(requests(&runner), sent, "before any call");
}

#[tokio::test]
async fn a_refused_label_write_does_not_block_the_account() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.add("m1", &["INBOX"]);
        mailbox.fail_next("POST labels", Fault::Status(403, "insufficientPermissions"));
        mailbox.fail_next("POST labels", Fault::Status(400, "invalidArgument"));
    })
    .await;
    let scope = runner
        .create_label(&account(), new_label("A"), BUDGET)
        .await;
    assert!(
        matches!(&scope, Err(LabelError::Refused(f)) if f.class == Blocking::Account(AccountProblem::ApiAccessDenied)),
        "{scope:?}"
    );
    let name = runner
        .create_label(&account(), new_label("B"), BUDGET)
        .await;
    assert!(
        matches!(&name, Err(LabelError::Refused(f)) if f.class == Blocking::Permanent),
        "{name:?}"
    );
    runner.transport().mailbox().add("m2", &["INBOX"]);
    let outcome = sync(&runner).await;
    assert_eq!(
        outcome.report.status.map(|s| s.phase),
        Some(SyncPhase::Live)
    );
    assert_eq!(
        view(&runner, "m2").await.labels,
        set(["INBOX"]),
        "sync goes on"
    );
    let retryable = {
        runner
            .transport()
            .mailbox()
            .fail_next("POST labels", Fault::Status(503, "backendError"));
        runner
            .create_label(&account(), new_label("C"), BUDGET)
            .await
    };
    assert!(
        matches!(&retryable, Err(LabelError::Retryable(f)) if f.class == Retryable::Transient),
        "{retryable:?}"
    );
}

#[tokio::test]
async fn a_deferred_label_write_sends_nothing() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.create_label("Old");
    })
    .await;
    let sent = requests(&runner);
    let before = labels(&runner).await;
    runner.defer(Some(Duration::from_mins(1)));
    let created = runner
        .create_label(&account(), new_label("New"), BUDGET)
        .await;
    assert!(
        matches!(created, Err(LabelError::Deferred(_))),
        "{created:?}"
    );
    let deleted = runner
        .delete_label(&account(), &id("Label_1"), SublabelDeletion::Refuse, BUDGET)
        .await;
    assert!(
        matches!(deleted, Err(LabelError::Deferred(_))),
        "{deleted:?}"
    );
    assert_eq!(requests(&runner), sent, "no request");
    assert_eq!(labels(&runner).await, before, "no record");
}

#[tokio::test]
async fn the_first_refresh_after_the_migration_fills_colour_and_visibility() {
    // The first listing knows names only, as rows stored before the
    // migration do.
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.labels.insert(
            "Label_1".into(),
            Definition {
                name: "Old".into(),
                kind: "user",
                list: None,
                message: None,
                color: None,
            },
        );
    })
    .await;
    let old = labels(&runner).await;
    assert!(
        old.iter()
            .all(|l| l.list_visibility.is_none() && l.color.is_none())
    );
    let before = head(&runner).await;
    {
        let mut mailbox = runner.transport().mailbox();
        let definition = mailbox.labels.get_mut("Label_1").expect("defined");
        definition.list = Some("labelHide".into());
        definition.message = Some("show".into());
        definition.color = Some(("#ffffff".into(), "#16a766".into()));
    }
    runner.advance(REFRESH);
    sync(&runner).await;
    let filled = labels(&runner)
        .await
        .into_iter()
        .find(|l| l.id == id("Label_1"))
        .expect("defined");
    assert_eq!(
        (
            filled.list_visibility,
            filled.message_visibility,
            filled.color
        ),
        (
            Some(LabelListVisibility::Hide),
            Some(MessageListVisibility::Show),
            Some(green())
        )
    );
    assert_eq!(
        feed_since(&runner, before).await,
        vec![Change::LabelDefinitionsChanged]
    );
    let after = head(&runner).await;
    runner.advance(REFRESH);
    sync(&runner).await;
    assert!(feed_since(&runner, after).await.is_empty(), "then quiet");
}

/// What the account's mirror remembers as deleted.
fn remembered(runner: &LabelRunner) -> Vec<String> {
    runner
        .store()
        .mirror(&account())
        .expect("tracked")
        .record
        .deleted_labels
        .keys()
        .map(|id| id.0.clone())
        .collect()
}

#[tokio::test]
async fn an_empty_or_truncated_listing_strips_no_label() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let label = mailbox.create_label("Kept");
        mailbox.add("m1", &["INBOX", "UNREAD", &label]);
    })
    .await;
    // No labels at all, then a listing cut short (no INBOX, no UNREAD).
    for body in [
        "{}",
        r#"{"labels":[]}"#,
        r#"{"labels":[{"id":"STARRED","name":"STARRED","type":"system"}]}"#,
    ] {
        runner
            .transport()
            .mailbox()
            .fail_next("GET labels", Fault::Body(body));
        runner.advance(REFRESH);
        sync(&runner).await;
        assert_eq!(
            view(&runner, "m1").await.labels,
            set(["INBOX", "Label_1", "UNREAD"]),
            "{body}"
        );
        assert_eq!(user_names(&runner).await["Label_1"], "Kept", "{body}");
        assert!(remembered(&runner).is_empty(), "{body}");
    }
    // A delete whose own listing is cut short sends nothing.
    runner.transport().mailbox().fail_next(
        "GET labels",
        Fault::Body(r#"{"labels":[{"id":"Label_1","name":"Kept","type":"user"}]}"#),
    );
    let deleted = runner
        .delete_label(&account(), &id("Label_1"), SublabelDeletion::Refuse, BUDGET)
        .await;
    assert!(
        matches!(deleted, Err(LabelError::Retryable(_))),
        "{deleted:?}"
    );
    assert!(writes(&runner).is_empty());
    assert_eq!(
        view(&runner, "m1").await.labels,
        set(["INBOX", "Label_1", "UNREAD"])
    );
}

#[tokio::test]
async fn delete_checks_an_omission_even_when_all_system_labels_are_listed() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let kept = mailbox.create_label("Kept");
        mailbox.add("m1", &[&kept]);
    })
    .await;
    runner
        .transport()
        .mailbox()
        .fail_next("GET labels", Fault::Body(CORE_LABELS));
    let result = runner
        .delete_label(&account(), &id("Label_1"), SublabelDeletion::Refuse, BUDGET)
        .await;
    assert!(matches!(result, Err(LabelError::Retryable(_))));
    assert!(
        runner
            .transport()
            .mailbox()
            .requests
            .iter()
            .any(|request| request == "GET labels/Label_1")
    );
    assert!(writes(&runner).is_empty());
    assert_eq!(view(&runner, "m1").await.labels, set(["Label_1"]));
}

const CORE_LABELS: &str = r#"{"labels":[{"id":"INBOX","name":"INBOX","type":"system"},{"id":"UNREAD","name":"UNREAD","type":"system"},{"id":"STARRED","name":"STARRED","type":"system"}]}"#;

#[tokio::test]
async fn a_single_cut_that_contains_every_system_label_keeps_live_uses() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let label = mailbox.create_label("Kept");
        mailbox.add("m1", &[&label]);
    })
    .await;
    let before = head(&runner).await;
    runner
        .transport()
        .mailbox()
        .fail_next("GET labels", Fault::Body(CORE_LABELS));
    runner.advance(REFRESH);
    let run = sync(&runner).await;
    assert_eq!(run.report.status.expect("status").label_listing_failures, 1);
    assert_eq!(view(&runner, "m1").await.labels, set(["Label_1"]));
    assert_eq!(user_names(&runner).await["Label_1"], "Kept");
    assert!(feed_since(&runner, before).await.is_empty());
}

#[tokio::test]
async fn a_retired_system_definition_allows_refresh_and_missing_core_is_reported() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.system_label("YELLOW_STAR");
        let label = mailbox.create_label("Old");
        mailbox.add("m1", &[&label, "YELLOW_STAR"]);
    })
    .await;
    runner.transport().mailbox().labels.remove("YELLOW_STAR");
    runner.transport().mailbox().rename_label("Label_1", "New");
    runner.advance(REFRESH);
    sync(&runner).await;
    assert_eq!(user_names(&runner).await["Label_1"], "New");
    assert!(
        view(&runner, "m1")
            .await
            .labels
            .contains(&id("YELLOW_STAR"))
    );
    let refreshed = runner
        .store()
        .mirror(&account())
        .unwrap()
        .record
        .labels_refreshed_at;
    runner.transport().mailbox().fail_next(
        "GET labels",
        Fault::Body(r#"{"labels":[{"id":"INBOX","name":"INBOX","type":"system"}]}"#),
    );
    runner.advance(REFRESH);
    let outcome = sync(&runner).await;
    assert_eq!(outcome.report.failed_requests, 1);
    assert_eq!(
        runner
            .store()
            .mirror(&account())
            .unwrap()
            .record
            .labels_refreshed_at,
        refreshed
    );
}

#[tokio::test]
async fn a_rename_into_its_own_subtree_changes_nothing() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        mailbox.create_label("A");
        mailbox.create_label("A/k");
    })
    .await;
    let into_itself = runner
        .update_label(
            &account(),
            &id("Label_1"),
            renamed("A/x"),
            Sublabels::Follow,
            BUDGET,
        )
        .await;
    assert!(
        matches!(into_itself, Err(LabelError::Invalid(_))),
        "{into_itself:?}"
    );
    assert!(writes(&runner).is_empty());
    assert_eq!(
        user_names(&runner).await.into_values().collect::<Vec<_>>(),
        ["A", "A/k"]
    );
}

#[tokio::test]
async fn deleting_an_unknown_or_system_label_id_is_refused() {
    let runner = synced(Access::ReadWrite, |_| {}).await;
    let unknown = runner
        .delete_label(
            &account(),
            &id("Label_404"),
            SublabelDeletion::Refuse,
            BUDGET,
        )
        .await;
    assert_eq!(unknown, Err(LabelError::NotFound));
    assert!(remembered(&runner).is_empty(), "nothing remembered for it");
    let sent = requests(&runner);
    // A fixed system id is refused whatever the mirror holds.
    let category = runner
        .delete_label(
            &account(),
            &id("CATEGORY_SOCIAL"),
            SublabelDeletion::Refuse,
            BUDGET,
        )
        .await;
    assert!(
        matches!(category, Err(LabelError::Invalid(_))),
        "{category:?}"
    );
    assert_eq!(requests(&runner), sent, "before any call");
    // A system label only Gmail's listing names: refused from it.
    runner.transport().mailbox().system_label("YELLOW_STAR");
    let star = runner
        .delete_label(
            &account(),
            &id("YELLOW_STAR"),
            SublabelDeletion::Refuse,
            BUDGET,
        )
        .await;
    assert!(matches!(star, Err(LabelError::Invalid(_))), "{star:?}");
    assert!(writes(&runner).is_empty());
}

#[tokio::test]
async fn a_write_to_an_untracked_account_sends_nothing() {
    let runner = synced(Access::ReadWrite, |_| {}).await;
    let sent = requests(&runner);
    let stranger = mailtide::ids::AccountId("bob@example.com".into());
    let created = runner
        .create_label(&stranger, new_label("Work"), BUDGET)
        .await;
    assert!(
        matches!(
            created,
            Err(LabelError::Run(mailtide_io::runner::RunError::Store(
                mailtide_store::StoreError::NotTracked
            )))
        ),
        "{created:?}"
    );
    assert_eq!(requests(&runner), sent, "Gmail never saw it");
}

/// The fake Gmail, with another client of the store writing a label while
/// a listing is on its way: it answers the listing as it stood when the
/// request arrived.
struct Racing {
    gmail: label_support::FakeGmail,
    store: mailtide_store::sqlite::SqliteStore,
}

impl mailtide_io::transport::GmailTransport for Racing {
    async fn send(
        &self,
        request: mailtide_gmail::http::HttpRequest,
        token: &mailtide_io::auth::AccessToken,
    ) -> Result<mailtide_gmail::http::HttpResponse, mailtide_io::transport::TransportError> {
        let listing = request.path.ends_with("/labels")
            && request.method == mailtide_gmail::http::Method::Get;
        let answer = self.gmail.send(request, token).await;
        if listing && !self.gmail.mailbox().labels.contains_key("Label_1") {
            // A create elsewhere lands and is recorded before the answer.
            self.gmail.mailbox().create_label("Created meanwhile");
            let label = mailtide::gmail::Label {
                list_visibility: Some(LabelListVisibility::Show),
                message_visibility: Some(MessageListVisibility::Show),
                ..mailtide::gmail::Label::plain(
                    id("Label_1"),
                    "Created meanwhile",
                    mailtide::gmail::LabelKind::User,
                )
            };
            mailtide_store::Store::record_label(
                &self.store,
                &account(),
                &mailtide::commit::LabelOp::Upsert(label),
            )
            .await
            .expect("recorded");
        }
        answer
    }
}

/// The generation is read before the listing is sent: a local write that
/// lands while it is on its way makes it stale, so it cannot remove the
/// label just written (read after the answer, it would).
#[tokio::test]
async fn a_listing_answered_after_a_local_write_is_not_applied() {
    use mailtide_store::Store as _;
    let store = mailtide_store::sqlite::SqliteStore::connect("sqlite::memory:")
        .await
        .expect("store");
    store.migrate().await.expect("migrate");
    store.track(&account(), 1).await.expect("track");
    let gmail = label_support::FakeGmail::default();
    label_support::with_defaults(&gmail);
    let clock = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(1_700_000_000_000));
    let gate = std::sync::Arc::new(label_support::GateState::default());
    let runner = label_support::runner_over(
        store.clone(),
        Racing { gmail, store },
        Access::ReadWrite,
        &clock,
        &gate,
    );
    runner
        .run(&account(), Duration::from_secs(30))
        .await
        .expect("run");
    let defined = runner.labels(&account()).await.expect("labels");
    assert!(
        defined.iter().any(|label| label.id == id("Label_1")),
        "{defined:?}"
    );
    // The next listing names it: nothing was remembered as deleted.
    clock.fetch_add(
        i64::try_from(REFRESH.as_millis()).unwrap_or(i64::MAX),
        std::sync::atomic::Ordering::SeqCst,
    );
    runner
        .run(&account(), Duration::from_secs(30))
        .await
        .expect("run");
    assert!(
        runner
            .labels(&account())
            .await
            .expect("labels")
            .iter()
            .any(|label| label.id == id("Label_1"))
    );
}

const LISTING_WITH_NEW_LABEL: &str = r#"{"labels":[{"id":"INBOX","name":"INBOX","type":"system"},{"id":"UNREAD","name":"UNREAD","type":"system"},{"id":"Label_3","name":"New","type":"user"}]}"#;

#[tokio::test]
async fn a_live_label_disproves_the_listing_even_after_an_earlier_404() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let gone = mailbox.create_label("Gone");
        let live = mailbox.create_label("Live");
        mailbox.add("m1", &[&gone, &live]);
    })
    .await;
    let before = head(&runner).await;
    {
        let mut gmail = runner.transport().mailbox();
        gmail.delete_label("Label_1", false);
        gmail.create_label("New");
        gmail.fail_next("GET labels", Fault::Body(LISTING_WITH_NEW_LABEL));
        gmail.fail_next("GET labels/Label_2", Fault::Undecodable);
    }
    runner.advance(REFRESH);
    let run = sync(&runner).await;
    assert_eq!(run.report.status.expect("status").label_listing_failures, 1);
    assert_eq!(
        view(&runner, "m1").await.labels,
        set(["Label_1", "Label_2"])
    );
    assert_eq!(user_names(&runner).await.len(), 2);
    assert_eq!(
        runner
            .transport()
            .mailbox()
            .requests
            .iter()
            .filter(|request| request.starts_with("GET labels/"))
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["GET labels/Label_1", "GET labels/Label_2"]
    );
    assert!(feed_since(&runner, before).await.is_empty());
}

#[tokio::test]
async fn a_transient_label_lookup_keeps_that_label_and_applies_the_other_proof() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let live = mailbox.create_label("Live");
        let gone = mailbox.create_label("Gone");
        mailbox.add("m1", &[&live, &gone]);
    })
    .await;
    {
        let mut gmail = runner.transport().mailbox();
        gmail.delete_label("Label_2", false);
        gmail.create_label("New");
        gmail.fail_next("GET labels", Fault::Body(LISTING_WITH_NEW_LABEL));
        gmail.fail_next("GET labels/Label_1", Fault::Status(503, "backendError"));
    }
    runner.advance(REFRESH);
    sync(&runner).await;
    assert_eq!(view(&runner, "m1").await.labels, set(["Label_1"]));
    let names = user_names(&runner).await;
    assert_eq!(names.len(), 2);
    assert_eq!(names["Label_1"], "Live");
    assert_eq!(names["Label_3"], "New");
}

#[tokio::test]
async fn a_run_ending_mid_proof_discards_the_listing_and_its_completed_lookups() {
    let runner = synced(Access::ReadWrite, |mailbox| {
        let first = mailbox.create_label("First");
        let second = mailbox.create_label("Second");
        mailbox.add("m1", &[&first, &second]);
    })
    .await;
    {
        let mut gmail = runner.transport().mailbox();
        gmail.delete_label("Label_1", false);
        gmail.delete_label("Label_2", false);
    }
    runner
        .transport()
        .delay_responses("/labels/Label_1", Duration::from_millis(400));
    runner.advance(REFRESH);
    let run = runner
        .run(&account(), Duration::from_millis(200))
        .await
        .expect("interrupted proofs");
    assert_eq!(run.stop, mailtide_io::runner::StopReason::BudgetExhausted);
    assert_eq!(
        view(&runner, "m1").await.labels,
        set(["Label_1", "Label_2"])
    );
    {
        let gmail = runner.transport().mailbox();
        assert!(
            gmail
                .requests
                .iter()
                .any(|request| request == "GET labels/Label_1")
        );
        assert!(
            !gmail
                .requests
                .iter()
                .any(|request| request == "GET labels/Label_2")
        );
    }

    runner
        .transport()
        .delay_responses("/labels/Label_1", Duration::ZERO);
    sync(&runner).await;
    assert!(view(&runner, "m1").await.labels.is_empty());
    assert_eq!(
        runner
            .transport()
            .mailbox()
            .requests
            .iter()
            .filter(|request| request.as_str() == "GET labels/Label_1")
            .count(),
        2
    );
}
