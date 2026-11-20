//! Test ported from himalaya's `labels update` command,
//! `src/gmail/labels/update.rs` (pimalaya/himalaya, MIT OR Apache-2.0,
//! commit a5623dcac615). Plan: `features/label-edit.toml`.

use mailtide::command::GmailRequest;
use mailtide::gmail::LabelPatch;
use mailtide::ids::LabelId;
use mailtide_gmail::http::Method;
use mailtide_gmail::request::{self, RequestConfig};
use serde_json::json;

/// A rename sends the name alone through `labels.patch`, so the label keeps
/// the colour and the visibility a `labels.update` naming neither would
/// clear.
#[test]
fn a_rename_patches_the_name_alone() {
    let rename = GmailRequest::PatchLabel {
        id: LabelId("Label_1".into()),
        patch: LabelPatch {
            name: Some("Renamed".into()),
            ..LabelPatch::default()
        },
    };
    let config = RequestConfig {
        watch_topic: String::new(),
    };
    let [request] = request::build(&rename, &config)
        .try_into()
        .expect("one request");
    assert_eq!(request.method, Method::Patch);
    assert_eq!(
        request.path_and_query(),
        "/gmail/v1/users/me/labels/Label_1"
    );
    let body: serde_json::Value =
        serde_json::from_slice(request.body.as_deref().unwrap_or_default()).expect("json");
    assert_eq!(body, json!({"name": "Renamed"}));
}
