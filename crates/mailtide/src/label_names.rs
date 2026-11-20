//! Label names and nesting (pure).
//!
//! Gmail nests labels by name alone: `Work/2026` is under `Work` because its
//! name says so, and a label has no parent field. So a parent is derived
//! from the names whenever it is read, a rename moves the sublabels by
//! renaming each, and a name is checked against the others the way Gmail
//! compares them: case-insensitively, surrounding space ignored
//! (`label-edit-duplicate-name`).
//!
//! The name rules follow himalaya's `src/email/mailbox.rs` (pimalaya/himalaya,
//! MIT OR Apache-2.0, commit a5623dcac615): not blank, no control character,
//! one level holds no delimiter, a renamed path keeps its parent.

use std::collections::BTreeMap;

use crate::gmail::Label;
use crate::ids::LabelId;

/// What separates the levels of a nested label name.
pub const DELIMITER: char = '/';

/// Why a name can't be a label's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidName {
    /// Empty, or only white space.
    Blank,
    /// Holds a control character.
    ControlCharacter,
    /// One level of a nested name holds the delimiter.
    Delimiter,
}

impl std::fmt::Display for InvalidName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Blank => "a label name must not be blank",
            Self::ControlCharacter => "a label name must not hold a control character",
            Self::Delimiter => "one level of a label name must not hold `/`",
        })
    }
}

impl std::error::Error for InvalidName {}

/// Refuse a name no label can take: blank, or holding a control character.
/// Length and reserved names (a system label's) are left to Gmail.
pub fn validate(name: &str) -> Result<(), InvalidName> {
    if name.trim().is_empty() {
        return Err(InvalidName::Blank);
    }
    if name.chars().any(char::is_control) {
        return Err(InvalidName::ControlCharacter);
    }
    Ok(())
}

/// Refuse a name for one level of a nested name: as [`validate`], and
/// without the delimiter.
pub fn validate_leaf(name: &str) -> Result<(), InvalidName> {
    validate(name)?;
    if name.contains(DELIMITER) {
        return Err(InvalidName::Delimiter);
    }
    Ok(())
}

/// The name of the level above `name` (`A/B` for `A/B/C`), if it is nested.
#[must_use]
pub fn parent(name: &str) -> Option<&str> {
    name.rsplit_once(DELIMITER).map(|(parent, _)| parent)
}

/// The last level of `name` (`C` for `A/B/C`).
#[must_use]
pub fn leaf(name: &str) -> &str {
    name.rsplit_once(DELIMITER).map_or(name, |(_, leaf)| leaf)
}

/// The name of `leaf` under `parent`.
#[must_use]
pub fn child(parent: &str, leaf: &str) -> String {
    format!("{parent}{DELIMITER}{leaf}")
}

/// `name` with its last level renamed `leaf`, its parent kept.
#[must_use]
pub fn renamed(name: &str, leaf: &str) -> String {
    match parent(name) {
        Some(parent) => child(parent, leaf),
        None => leaf.to_owned(),
    }
}

/// Whether Gmail treats two names as the same: it refuses a name that
/// differs from another label's only in case or by surrounding space.
#[must_use]
pub fn same_name(a: &str, b: &str) -> bool {
    a.trim().to_lowercase() == b.trim().to_lowercase()
}

/// Whether `name` is nested, at any depth, under `ancestor`.
#[must_use]
pub fn is_under(name: &str, ancestor: &str) -> bool {
    name.strip_prefix(ancestor)
        .is_some_and(|rest| rest.starts_with(DELIMITER))
}

/// The labels nested, at any depth, under the label named `name`, deepest
/// first (then by name): the order a delete takes them in.
#[must_use]
pub fn sublabels<'a>(labels: &'a [Label], name: &str) -> Vec<&'a Label> {
    let mut nested: Vec<&Label> = labels
        .iter()
        .filter(|label| is_under(&label.name, name))
        .collect();
    nested.sort_by(|a, b| {
        let depth = |label: &Label| label.name.matches(DELIMITER).count();
        depth(b).cmp(&depth(a)).then_with(|| a.name.cmp(&b.name))
    });
    nested
}

/// The sublabel renames a rename of `old` to `new` implies: every label
/// nested under `old` moves to the same place under `new`, and nothing else
/// (`A/Subway` is not under `A/Sub`). In the order of `labels`.
#[must_use]
pub fn sublabel_renames(labels: &[Label], old: &str, new: &str) -> Vec<(LabelId, String)> {
    labels
        .iter()
        .filter_map(|label| {
            let rest = label.name.strip_prefix(old)?;
            rest.starts_with(DELIMITER)
                .then(|| (label.id.clone(), format!("{new}{rest}")))
        })
        .collect()
}

/// Why a rename can't go ahead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameConflict {
    /// The definitions have no label with that id.
    NotFound,
    /// Another label already holds a name the rename needs.
    Taken(Label),
}

/// The sublabel renames renaming label `id` to `new` takes, checked before
/// anything is sent: every name the rename needs (its own and each
/// sublabel's) must be free of every label that does not move, as Gmail
/// compares names. A label may take another case of its own name. Renames
/// that change nothing are left out, so a rename that failed half-way and is
/// made again finds only what is left.
pub fn plan_rename(
    labels: &[Label],
    id: &LabelId,
    new: &str,
) -> Result<Vec<(LabelId, String)>, RenameConflict> {
    let label = labels
        .iter()
        .find(|label| &label.id == id)
        .ok_or(RenameConflict::NotFound)?;
    let renames = sublabel_renames(labels, &label.name, new);
    let moving = |candidate: &Label| {
        candidate.id == label.id || renames.iter().any(|(id, _)| id == &candidate.id)
    };
    for target in std::iter::once(new).chain(renames.iter().map(|(_, name)| name.as_str())) {
        if let Some(holder) = labels
            .iter()
            .find(|other| !moving(other) && same_name(&other.name, target))
        {
            return Err(RenameConflict::Taken(holder.clone()));
        }
    }
    Ok(renames
        .into_iter()
        .filter(|(id, name)| labels.iter().any(|l| &l.id == id && &l.name != name))
        .collect())
}

/// Each label's parent: the label whose name is the longest prefix of its
/// name ending at a delimiter (`A/B/C` is under `A/B` if that label exists,
/// else under `A`, else at the top). Labels at the top are left out.
#[must_use]
pub fn parents(labels: &[Label]) -> BTreeMap<LabelId, LabelId> {
    let by_name: BTreeMap<&str, &LabelId> = labels
        .iter()
        .map(|label| (label.name.as_str(), &label.id))
        .collect();
    labels
        .iter()
        .filter_map(|label| {
            let mut name = label.name.as_str();
            while let Some(above) = parent(name) {
                if let Some(id) = by_name.get(above) {
                    return Some((label.id.clone(), (*id).clone()));
                }
                name = above;
            }
            None
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gmail::LabelKind;

    fn label(id: &str, name: &str) -> Label {
        Label::plain(LabelId(id.into()), name, LabelKind::User)
    }

    fn id(id: &str) -> LabelId {
        LabelId(id.into())
    }

    #[test]
    fn a_parent_is_the_longest_existing_label_at_a_slash_boundary() {
        let labels = [
            label("1", "A"),
            label("2", "A/B"),
            label("3", "A/B/C"),
            label("4", "A/X/Y"),
            label("5", "Top/Orphan"),
            label("6", "A/Sub"),
            label("7", "A/Subway"),
        ];
        assert_eq!(
            parents(&labels),
            BTreeMap::from([
                (id("2"), id("1")),
                (id("3"), id("2")),
                // A/X does not exist: the nearest one above it does.
                (id("4"), id("1")),
                (id("6"), id("1")),
                // Not under A/Sub: the prefix must end at a slash.
                (id("7"), id("1")),
            ])
        );
    }

    #[test]
    fn sublabel_renames_never_collide_with_an_existing_name() {
        let labels = [
            label("1", "A"),
            label("2", "A/x"),
            label("3", "B"),
            label("4", "c/x"),
            label("5", "D/x"),
        ];
        assert_eq!(
            plan_rename(&labels, &id("1"), "B"),
            Err(RenameConflict::Taken(label("3", "B"))),
            "the label's own target is taken"
        );
        assert_eq!(
            plan_rename(&labels, &id("1"), " b "),
            Err(RenameConflict::Taken(label("3", "B"))),
            "in another case, with surrounding space"
        );
        assert_eq!(
            plan_rename(&labels, &id("1"), "C"),
            Err(RenameConflict::Taken(label("4", "c/x"))),
            "a sublabel's target is taken, in another case"
        );
        assert_eq!(
            plan_rename(&labels, &id("1"), "a"),
            Ok(vec![(id("2"), "a/x".into())]),
            "a label may take another case of its own name"
        );
        assert_eq!(
            plan_rename(&labels, &id("1"), "D"),
            Err(RenameConflict::Taken(label("5", "D/x")))
        );
        assert_eq!(
            plan_rename(&labels, &id("1"), "E"),
            Ok(vec![(id("2"), "E/x".into())])
        );
        assert_eq!(
            plan_rename(&labels, &id("9"), "E"),
            Err(RenameConflict::NotFound)
        );
        // Half-way: the sublabel moved, the label did not. Only the label
        // is left, and its moved sublabel does not block it.
        let half = [label("1", "A"), label("2", "E/x")];
        assert_eq!(plan_rename(&half, &id("1"), "E"), Ok(Vec::new()));
    }
}
