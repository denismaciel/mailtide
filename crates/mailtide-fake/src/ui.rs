//! Embedded zero-build web inspector served at `/_ui`.

use http::StatusCode;

use crate::server::{HttpReply, text_reply};

const UI_HTML: &str = include_str!("ui.html");

/// Serve the single-page web inspector.
pub(crate) fn index() -> HttpReply {
    text_reply(
        StatusCode::OK,
        "text/html; charset=utf-8",
        UI_HTML.to_owned(),
    )
}
