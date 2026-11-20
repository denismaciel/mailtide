//! Gmail's label resource (`users.labels`): definitions with their
//! visibility and colour, decoded once at the boundary.

use mailtide::gmail::{Label, LabelColor, LabelKind, LabelListVisibility, MessageListVisibility};
use mailtide::ids::LabelId;
use serde::Deserialize;

use crate::decode::{DecodeError, json};

/// A label as Gmail sends it, in `labels.list` and as the answer to
/// `labels.create`, `labels.get` and `labels.patch`.
#[derive(Deserialize)]
pub(crate) struct WireLabel {
    id: Option<String>,
    name: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    #[serde(rename = "labelListVisibility")]
    list_visibility: Option<String>,
    #[serde(rename = "messageListVisibility")]
    message_visibility: Option<String>,
    color: Option<WireColor>,
}

#[derive(Deserialize)]
struct WireColor {
    #[serde(rename = "textColor")]
    text: Option<String>,
    #[serde(rename = "backgroundColor")]
    background: Option<String>,
}

impl WireLabel {
    /// The definition, or why there is none: a label without an id or a
    /// name is never stored (an empty id would read as a label of its own).
    /// A visibility Gmail spells in a way this build does not know reads as
    /// unknown, not as an error.
    pub(crate) fn decode(self) -> Result<Label, DecodeError> {
        let id = self
            .id
            .filter(|id| !id.is_empty())
            .ok_or(DecodeError::MissingField("id"))?;
        let name = self.name.ok_or(DecodeError::MissingField("name"))?;
        Ok(Label {
            id: LabelId(id),
            name,
            kind: match self.kind.as_deref() {
                Some("system") => LabelKind::System,
                Some(_) | None => LabelKind::User,
            },
            list_visibility: self
                .list_visibility
                .as_deref()
                .and_then(LabelListVisibility::from_gmail_name),
            message_visibility: self
                .message_visibility
                .as_deref()
                .and_then(MessageListVisibility::from_gmail_name),
            color: self.color.and_then(|color| {
                Some(LabelColor {
                    text: color.text?,
                    background: color.background?,
                })
            }),
        })
    }
}

/// One label: the answer to `labels.create`, `labels.get` or
/// `labels.patch`.
pub fn label(body: &[u8]) -> Result<Label, DecodeError> {
    json::<WireLabel>(body)?.decode()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_label_answer_without_an_id_is_refused() {
        assert_eq!(
            label(br#"{"name":"Work","type":"user"}"#),
            Err(DecodeError::MissingField("id"))
        );
        assert_eq!(
            label(br#"{"id":"","name":"Work","type":"user"}"#),
            Err(DecodeError::MissingField("id"))
        );
        assert_eq!(
            label(br#"{"id":"Label_1","type":"user"}"#),
            Err(DecodeError::MissingField("name"))
        );
        assert!(matches!(
            crate::decode::labels_list(
                br#"{"labels":[{"id":"INBOX","name":"INBOX"},{"name":"Work"}]}"#
            ),
            Err(DecodeError::MissingField("id"))
        ));
    }
}
