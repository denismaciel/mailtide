//! The single JSON `messages.send` request and Gmail's acceptance receipt.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::ids::{MessageId, ThreadId};
use serde::{Deserialize, Serialize};

use crate::decode::{self, DecodeError};
use crate::http::{HttpRequest, Method};
use crate::request::USER;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SendBody<'a> {
    raw: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_id: Option<&'a str>,
}

pub(crate) fn request(raw: &[u8], thread_id: Option<&ThreadId>) -> HttpRequest {
    let body = SendBody {
        raw: URL_SAFE_NO_PAD.encode(raw),
        thread_id: thread_id.map(|id| id.0.as_str()),
    };
    HttpRequest {
        method: Method::Post,
        path: format!("{USER}/messages/send"),
        query: Vec::new(),
        body: Some(serde_json::to_vec(&body).expect("string fields serialize")),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Receipt {
    id: Option<String>,
    thread_id: Option<String>,
}

/// Read Gmail's receipt. A response needs both ids to acknowledge acceptance.
pub fn receipt(body: &[u8]) -> Result<(MessageId, ThreadId), DecodeError> {
    fn id(value: Option<String>, field: &'static str) -> Result<String, DecodeError> {
        value
            .filter(|value| !value.trim().is_empty())
            .ok_or(DecodeError::MissingField(field))
    }

    let wire: Receipt = decode::json(body)?;
    Ok((
        MessageId(id(wire.id, "id")?),
        ThreadId(id(wire.thread_id, "threadId")?),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mailtide::command::GmailRequest;

    #[test]
    fn send_posts_base64url_raw_with_the_thread_id() {
        for thread_id in [Some(ThreadId("t".into())), None] {
            let [http] = crate::request::build(
                &GmailRequest::MessagesSend {
                    raw: vec![0xfb, 0xff],
                    thread_id: thread_id.clone(),
                },
                &crate::request::RequestConfig {
                    watch_topic: String::new(),
                },
            )
            .try_into()
            .expect("one request");
            assert_eq!(http.method, Method::Post);
            assert_eq!(http.path_and_query(), "/gmail/v1/users/me/messages/send");
            let body: serde_json::Value =
                serde_json::from_slice(&http.body.expect("body")).expect("JSON");
            if thread_id.is_some() {
                assert_eq!(body, serde_json::json!({"raw": "-_8", "threadId": "t"}));
            } else {
                assert_eq!(body, serde_json::json!({"raw": "-_8"}));
            }
        }
    }

    #[test]
    fn a_send_receipt_requires_a_message_and_thread_id() {
        assert_eq!(
            receipt(br#"{"id":"m","threadId":"t","labelIds":["SENT"]}"#),
            Ok((MessageId("m".into()), ThreadId("t".into())))
        );
        for body in [
            "",
            "{}",
            r#"{"id":"m"}"#,
            r#"{"threadId":"t"}"#,
            r#"{"id":"m","threadId":" "}"#,
        ] {
            assert!(receipt(body.as_bytes()).is_err(), "{body}");
        }
    }
}
