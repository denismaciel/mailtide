//! Gmail's label resource (`users.labels`): definitions with their
//! visibility and colour, decoded once at the boundary; the create, patch
//! and delete requests; Google's colour palette.

use mailtide::gmail::{
    ColorChange, Label, LabelColor, LabelKind, LabelListVisibility, LabelPatch,
    MessageListVisibility, NewLabel,
};
use mailtide::ids::LabelId;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::decode::{DecodeError, json as decode_json};
use crate::http::{HttpRequest, Method, encode};
use crate::request::USER;

/// The colours Gmail accepts for a label, as text or as background (both
/// lists are the same), in the lower case Gmail matches them in: `#FFFFFF`
/// is refused (`label-edit-color-rules`). From the `users.labels` reference,
/// read 2026-11-20.
pub const PALETTE: [&str; 102] = [
    "#000000", "#434343", "#666666", "#999999", "#cccccc", "#efefef", "#f3f3f3", "#ffffff",
    "#fb4c2f", "#ffad47", "#fad165", "#16a766", "#43d692", "#4a86e8", "#a479e2", "#f691b3",
    "#f6c5be", "#ffe6c7", "#fef1d1", "#b9e4d0", "#c6f3de", "#c9daf8", "#e4d7f5", "#fcdee8",
    "#efa093", "#ffd6a2", "#fce8b3", "#89d3b2", "#a0eac9", "#a4c2f4", "#d0bcf1", "#fbc8d9",
    "#e66550", "#ffbc6b", "#fcda83", "#44b984", "#68dfa9", "#6d9eeb", "#b694e8", "#f7a7c0",
    "#cc3a21", "#eaa041", "#f2c960", "#149e60", "#3dc789", "#3c78d8", "#8e63ce", "#e07798",
    "#ac2b16", "#cf8933", "#d5ae49", "#0b804b", "#2a9c68", "#285bac", "#653e9b", "#b65775",
    "#822111", "#a46a21", "#aa8831", "#076239", "#1a764d", "#1c4587", "#41236d", "#83334c",
    "#464646", "#e7e7e7", "#0d3472", "#b6cff5", "#0d3b44", "#98d7e4", "#3d188e", "#e3d7ff",
    "#711a36", "#fbd3e0", "#8a1c0a", "#f2b2a8", "#7a2e0b", "#ffc8af", "#7a4706", "#ffdeb5",
    "#594c05", "#fbe983", "#684e07", "#fdedc1", "#0b4f30", "#b3efd3", "#04502e", "#a2dcc1",
    "#c2c2c2", "#4986e7", "#2da2bb", "#b99aff", "#994a64", "#f691b2", "#ff7537", "#ffad46",
    "#662e37", "#ebdbde", "#cca6ac", "#094228", "#42d692", "#16a765",
];

/// A colour Gmail would refuse: not one of [`PALETTE`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffPalette {
    /// The value given.
    pub value: String,
}

impl std::fmt::Display for OffPalette {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "label colour {:?} is not on Gmail's palette", self.value)
    }
}

impl std::error::Error for OffPalette {}

/// The pair as Gmail takes it: both values from [`PALETTE`], lower-cased
/// (any pair of palette values is accepted). Checked before any request, so
/// a colour Gmail would refuse is never sent.
pub fn palette_color(text: &str, background: &str) -> Result<LabelColor, OffPalette> {
    let checked = |value: &str| {
        let lower = value.trim().to_ascii_lowercase();
        if PALETTE.contains(&lower.as_str()) {
            Ok(lower)
        } else {
            Err(OffPalette {
                value: value.to_owned(),
            })
        }
    };
    Ok(LabelColor {
        text: checked(text)?,
        background: checked(background)?,
    })
}

fn color_json(color: &LabelColor) -> Value {
    json!({ "textColor": color.text, "backgroundColor": color.background })
}

/// `labels.create`: the name, both visibilities, and the colour if any.
pub(crate) fn create(label: &NewLabel) -> HttpRequest {
    let mut body = json!({
        "name": label.name,
        "labelListVisibility": label.list_visibility.gmail_name(),
        "messageListVisibility": label.message_visibility.gmail_name(),
    });
    if let Some(color) = &label.color {
        body["color"] = color_json(color);
    }
    with_body(Method::Post, format!("{USER}/labels"), &body)
}

/// `labels.patch`: only the fields that change (a rename sends the name
/// alone, so colour and visibility survive); `color: null` clears it.
pub(crate) fn patch(id: &LabelId, patch: &LabelPatch) -> HttpRequest {
    let mut body = Map::new();
    if let Some(name) = &patch.name {
        body.insert("name".into(), json!(name));
    }
    if let Some(visibility) = patch.list_visibility {
        body.insert("labelListVisibility".into(), json!(visibility.gmail_name()));
    }
    if let Some(visibility) = patch.message_visibility {
        body.insert(
            "messageListVisibility".into(),
            json!(visibility.gmail_name()),
        );
    }
    match &patch.color {
        ColorChange::Keep => {}
        ColorChange::Set(color) => {
            body.insert("color".into(), color_json(color));
        }
        ColorChange::Clear => {
            body.insert("color".into(), Value::Null);
        }
    }
    with_body(
        Method::Patch,
        format!("{USER}/labels/{}", encode(&id.0)),
        &Value::Object(body),
    )
}

/// `labels.delete`: no body; Gmail answers 204.
pub(crate) fn delete(id: &LabelId) -> HttpRequest {
    HttpRequest {
        method: Method::Delete,
        path: format!("{USER}/labels/{}", encode(&id.0)),
        query: Vec::new(),
        body: None,
    }
}

fn with_body(method: Method, path: String, body: &Value) -> HttpRequest {
    HttpRequest {
        method,
        path,
        query: Vec::new(),
        body: Some(body.to_string().into_bytes()),
    }
}

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
    decode_json::<WireLabel>(body)?.decode()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(request: &HttpRequest) -> Value {
        serde_json::from_slice(request.body.as_deref().unwrap_or_default()).expect("json")
    }

    fn colour() -> LabelColor {
        LabelColor {
            text: "#ffffff".into(),
            background: "#16a766".into(),
        }
    }

    #[test]
    fn create_posts_the_name_with_explicit_visibility() {
        let mut label = NewLabel {
            name: "Work/2026".into(),
            list_visibility: LabelListVisibility::ShowIfUnread,
            message_visibility: MessageListVisibility::Hide,
            color: None,
        };
        let request = create(&label);
        assert_eq!(request.method, Method::Post);
        assert_eq!(request.path_and_query(), "/gmail/v1/users/me/labels");
        assert_eq!(
            body(&request),
            json!({
                "name": "Work/2026",
                "labelListVisibility": "labelShowIfUnread",
                "messageListVisibility": "hide",
            })
        );
        label.color = Some(colour());
        assert_eq!(
            body(&create(&label))["color"],
            json!({"textColor": "#ffffff", "backgroundColor": "#16a766"})
        );
    }

    #[test]
    fn patch_sends_only_the_fields_that_change() {
        let id = LabelId("Label 1/x".into());
        let restyle = patch(
            &id,
            &LabelPatch {
                list_visibility: Some(LabelListVisibility::Hide),
                color: ColorChange::Set(colour()),
                ..LabelPatch::default()
            },
        );
        assert_eq!(restyle.method, Method::Patch);
        assert_eq!(
            restyle.path_and_query(),
            "/gmail/v1/users/me/labels/Label%201%2Fx"
        );
        assert_eq!(
            body(&restyle),
            json!({
                "labelListVisibility": "labelHide",
                "color": {"textColor": "#ffffff", "backgroundColor": "#16a766"},
            }),
            "a restyle sends no name"
        );
        let rename = patch(
            &id,
            &LabelPatch {
                name: Some("Renamed".into()),
                ..LabelPatch::default()
            },
        );
        assert_eq!(
            body(&rename),
            json!({"name": "Renamed"}),
            "a rename no colour"
        );
        let clear = patch(
            &id,
            &LabelPatch {
                message_visibility: Some(MessageListVisibility::Show),
                color: ColorChange::Clear,
                ..LabelPatch::default()
            },
        );
        assert_eq!(
            body(&clear),
            json!({"messageListVisibility": "show", "color": null})
        );
    }

    #[test]
    fn delete_sends_delete_to_the_label_id() {
        let request = delete(&LabelId("Label_7".into()));
        assert_eq!(request.method, Method::Delete);
        assert_eq!(
            request.path_and_query(),
            "/gmail/v1/users/me/labels/Label_7"
        );
        assert_eq!(request.body, None);
    }

    #[test]
    fn a_colour_outside_the_palette_is_refused_before_any_request() {
        // Any pair of palette values, even one Gmail's picker never offers.
        assert_eq!(
            palette_color("#fb4c2f", "#16a766"),
            Ok(LabelColor {
                text: "#fb4c2f".into(),
                background: "#16a766".into(),
            })
        );
        // Gmail matches lower case only: an upper-case value is sent lower.
        assert_eq!(palette_color("#FFFFFF", "#16A766"), Ok(colour()));
        assert_eq!(
            palette_color("#123456", "#16a766"),
            Err(OffPalette {
                value: "#123456".into()
            })
        );
        assert_eq!(
            palette_color("#ffffff", "red"),
            Err(OffPalette {
                value: "red".into()
            })
        );
        assert!(
            PALETTE
                .iter()
                .all(|c| c.len() == 7 && *c == c.to_ascii_lowercase())
        );
        let unique: std::collections::BTreeSet<&&str> = PALETTE.iter().collect();
        assert_eq!(unique.len(), PALETTE.len());
    }

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
