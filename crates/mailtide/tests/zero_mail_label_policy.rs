//! Tests ported by name from Zero's `mail-label-policy.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.
//!
//! Zero's routing policy maps onto [`mailtide::outbox::expand`]: what a user
//! action may change, including Gmail's implicit changes.

use mailtide::ids::{LabelId, MessageId};
use mailtide::labels::{LabelSet, set};
use mailtide::outbox::{Action, AdmissionError, LabelChange, RestoreDestination, expand};

fn m() -> MessageId {
    MessageId("m".into())
}

fn label(id: &str) -> LabelId {
    LabelId(id.into())
}

fn change_labels(add: &[&str], remove: &[&str]) -> Action {
    Action::ChangeLabels {
        add: add.iter().map(|id| label(id)).collect(),
        remove: remove.iter().map(|id| label(id)).collect(),
    }
}

fn changes(pairs: &[(&str, LabelChange)]) -> Vec<(LabelId, LabelChange)> {
    pairs
        .iter()
        .map(|(id, change)| (label(id), *change))
        .collect()
}

/// The effective labels after applying an expansion.
fn after(labels: &LabelSet, expanded: &[(LabelId, LabelChange)]) -> LabelSet {
    let mut labels = labels.clone();
    for (id, change) in expanded {
        match change {
            LabelChange::Add => labels.insert(id.clone()),
            LabelChange::Remove => labels.remove(id),
        };
    }
    labels
}

/// Zero: mail-label-policy > durable label routing > splits snooze and unsnooze while retaining provider custom IDs and unknown names
#[test]
#[ignore = "zero-na: splits Zero's app-only synthetic labels (SNOOZED/MUTE) from Gmail ones; mailtide has no synthetic labels"]
fn durable_label_routing_splits_snooze_and_unsnooze_while_retaining_provider_custom_ids_and_unknown_names()
 {
    todo!("port from Zero 4b642dfd9890")
}

/// Zero: mail-label-policy > durable label routing > records Gmail implicit inbox removal without mutating caller arrays
#[test]
fn durable_label_routing_records_gmail_implicit_inbox_removal_without_mutating_caller_arrays() {
    let inbox = set(["INBOX", "Label_123"]);
    for (location, action) in [("TRASH", Action::MoveToTrash), ("SPAM", Action::MarkSpam)] {
        let expanded = expand(&m(), &action, &inbox).unwrap();
        assert_eq!(
            expanded,
            changes(&[(location, LabelChange::Add), ("INBOX", LabelChange::Remove)])
        );
        // Zero: normalizeDurableLabelMutation([label, 'INBOX'], ['Label_123']).
        let raw = change_labels(&[location, "INBOX"], &["Label_123"]);
        let copy = raw.clone();
        match expand(&m(), &raw, &inbox) {
            Err(AdmissionError::ConflictingLocations) => {}
            Ok(expanded) => assert!(!after(&inbox, &expanded).contains(&label("INBOX"))),
            Err(other) => panic!("unexpected {other:?}"),
        }
        assert_eq!(raw, copy, "the caller's action is untouched");
        // Zero: normalizeDurableLabelMutation([label], ['Label_123']).
        let raw = change_labels(&[location], &["Label_123"]);
        let expanded = expand(&m(), &raw, &inbox).unwrap();
        assert_eq!(after(&inbox, &expanded), set([location]));
    }
}

/// Zero: mail-label-policy > durable label routing > routes reversible system moves and custom-label combinations together
#[test]
fn durable_label_routing_routes_reversible_system_moves_and_custom_label_combinations_together() {
    use LabelChange::{Add, Remove};
    let labels = set(["INBOX"]);
    for (add, remove) in [
        (&["TRASH"][..], &["INBOX", "SPAM"][..]),
        (&["SPAM"], &["TRASH", "INBOX"]),
        (&["INBOX"], &["TRASH", "SPAM"]),
        (&["IMPORTANT", "Label_123"], &["Label_456"]),
    ] {
        let expected: Vec<_> = add
            .iter()
            .map(|id| (*id, Add))
            .chain(remove.iter().map(|id| (*id, Remove)))
            .collect();
        assert_eq!(
            expand(&m(), &change_labels(add, remove), &labels),
            Ok(changes(&expected)),
            "{add:?} / {remove:?}"
        );
    }
    // The named moves route the same changes.
    assert_eq!(
        expand(&m(), &Action::MoveToInbox, &set(["TRASH"])),
        Ok(changes(&[
            ("INBOX", Add),
            ("TRASH", Remove),
            ("SPAM", Remove)
        ]))
    );
    assert_eq!(
        expand(
            &m(),
            &Action::MarkNotSpam(RestoreDestination::Inbox),
            &set(["SPAM"])
        ),
        Ok(changes(&[("SPAM", Remove), ("INBOX", Add)]))
    );
}

/// Zero: mail-label-policy > durable label routing > keeps synthetic names, immutable labels and mixed operations out of the provider-ID outbox
#[test]
fn durable_label_routing_keeps_synthetic_names_immutable_labels_and_mixed_operations_out_of_the_provider_id_outbox()
 {
    // mailtide has no synthetic labels and treats label ids as opaque Gmail
    // ids (an id Gmail rejects blocks at delivery), so only the immutable
    // system labels and the empty change carry over.
    let labels = set(["INBOX"]);
    for immutable in ["DRAFT", "SENT", "CHAT"] {
        assert_eq!(
            expand(&m(), &change_labels(&[immutable], &["INBOX"]), &labels),
            Err(AdmissionError::ReadOnlyLabel(label(immutable)))
        );
        assert_eq!(
            expand(&m(), &change_labels(&["Label_123"], &[immutable]), &labels),
            Err(AdmissionError::ReadOnlyLabel(label(immutable)))
        );
    }
    assert_eq!(expand(&m(), &change_labels(&[], &[]), &labels), Ok(vec![]));
}
