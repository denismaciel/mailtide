//! Gmail's `users.drafts` methods: requests and answers.
//!
//! A draft write carries the raw message as unpadded base64url in the JSON
//! body (the metadata URI): mailtide refuses content above its inline limit
//! before it gets here. An update always names the thread: Gmail moves a
//! reply draft updated without one out of its thread.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mailtide::drafts::{DraftFormat, DraftPage, FetchedDraft, ListedDraft, Rfc822, WrittenDraft};
use mailtide::ids::{DraftId, MessageId, PageToken, ThreadId};
use mailtide::labels::{self, LabelSet};
use serde::Deserialize;
use serde_json::json;

use crate::decode::{BASE64URL, DecodeError};
use crate::http::{HttpRequest, Method, encode};
use crate::request::PAGE_SIZE;

const DRAFTS: &str = "/gmail/v1/users/me/drafts";

fn request(
    method: Method,
    path: String,
    query: Vec<(String, String)>,
    body: Option<serde_json::Value>,
) -> HttpRequest {
    HttpRequest {
        method,
        path,
        query,
        body: body.map(|body| body.to_string().into_bytes()),
    }
}

fn message(raw: &Rfc822, thread_id: Option<&ThreadId>) -> serde_json::Value {
    let mut message = json!({ "raw": URL_SAFE_NO_PAD.encode(&raw.0) });
    if let Some(thread) = thread_id {
        message["threadId"] = json!(thread.0);
    }
    message
}

/// `drafts.create`: `POST /drafts` with `{message: {raw, threadId?}}`.
#[must_use]
pub fn create(raw: &Rfc822, thread_id: Option<&ThreadId>) -> HttpRequest {
    request(
        Method::Post,
        DRAFTS.to_owned(),
        Vec::new(),
        Some(json!({ "message": message(raw, thread_id) })),
    )
}

/// `drafts.update`: `PUT /drafts/{id}`, replacing the message whole; the
/// thread id is always sent.
#[must_use]
pub fn update(id: &DraftId, raw: &Rfc822, thread_id: &ThreadId) -> HttpRequest {
    request(
        Method::Put,
        format!("{DRAFTS}/{}", encode(&id.0)),
        Vec::new(),
        Some(json!({ "id": id.0, "message": message(raw, Some(thread_id)) })),
    )
}

/// `drafts.send`: `POST /drafts/send` with the draft id only, so the
/// stored draft is sent as it was last saved.
#[must_use]
pub fn send(id: &DraftId) -> HttpRequest {
    request(
        Method::Post,
        format!("{DRAFTS}/send"),
        Vec::new(),
        Some(json!({ "id": id.0 })),
    )
}

/// `drafts.get` in `format`.
#[must_use]
pub fn get(id: &DraftId, format: DraftFormat) -> HttpRequest {
    let format = match format {
        DraftFormat::Minimal => "minimal",
        DraftFormat::Raw => "raw",
    };
    request(
        Method::Get,
        format!("{DRAFTS}/{}", encode(&id.0)),
        vec![("format".to_owned(), format.to_owned())],
        None,
    )
}

/// `drafts.list`: full pages, spam and trash included (a draft can be in
/// neither, but a listing must never miss one for a filter it didn't ask
/// for).
#[must_use]
pub fn list(page_token: Option<&PageToken>) -> HttpRequest {
    let mut query = vec![
        ("maxResults".to_owned(), PAGE_SIZE.to_string()),
        ("includeSpamTrash".to_owned(), "true".to_owned()),
    ];
    if let Some(token) = page_token {
        query.push(("pageToken".to_owned(), token.0.clone()));
    }
    request(Method::Get, DRAFTS.to_owned(), query, None)
}

/// `drafts.delete`: permanent; Gmail answers 204.
#[must_use]
pub fn delete(id: &DraftId) -> HttpRequest {
    request(
        Method::Delete,
        format!("{DRAFTS}/{}", encode(&id.0)),
        Vec::new(),
        None,
    )
}

// ------------------------------------------------------------ answers

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireMessage {
    id: Option<String>,
    thread_id: Option<String>,
    label_ids: Option<Vec<String>>,
    raw: Option<String>,
}

#[derive(Deserialize)]
struct WireDraft {
    id: Option<String>,
    message: Option<WireMessage>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireDraftList {
    #[serde(default)]
    drafts: Vec<WireDraft>,
    next_page_token: Option<String>,
}

fn json<'a, T: Deserialize<'a>>(body: &'a [u8]) -> Result<T, DecodeError> {
    serde_json::from_slice(body).map_err(|e| DecodeError::InvalidJson(e.to_string()))
}

/// A field that must be present and not empty: an empty id would become a
/// row no call can address.
fn required(value: Option<String>, field: &'static str) -> Result<String, DecodeError> {
    value
        .filter(|v| !v.is_empty())
        .ok_or(DecodeError::MissingField(field))
}

fn listed(draft: WireDraft) -> Result<(ListedDraft, WireMessage), DecodeError> {
    let id = required(draft.id, "id")?;
    let mut message = draft.message.ok_or(DecodeError::MissingField("message"))?;
    let listed = ListedDraft {
        id: DraftId(id),
        message: MessageId(required(message.id.take(), "message.id")?),
        thread_id: ThreadId(required(message.thread_id.take(), "message.threadId")?),
    };
    Ok((listed, message))
}

/// `drafts.list`. Every entry must name its draft, message and thread.
pub fn list_page(body: &[u8]) -> Result<DraftPage, DecodeError> {
    let wire: WireDraftList = json(body)?;
    Ok(DraftPage {
        drafts: wire
            .drafts
            .into_iter()
            .map(|draft| listed(draft).map(|(listed, _)| listed))
            .collect::<Result<_, _>>()?,
        next_page_token: wire.next_page_token.map(PageToken),
    })
}

/// `drafts.get`. `None` when the message Gmail returned carries labels but
/// not `DRAFT`: whatever it is, it is not an editable draft.
pub fn fetched(body: &[u8]) -> Result<Option<FetchedDraft>, DecodeError> {
    let wire: WireDraft = json(body)?;
    let (draft, message) = listed(wire)?;
    let labels: LabelSet = message
        .label_ids
        .as_ref()
        .map_or_default(|ids| labels::set(ids.iter().map(String::as_str)));
    if message.label_ids.is_some() && !labels::is_draft(&labels) {
        return Ok(None);
    }
    let raw = message
        .raw
        .map(|raw| {
            BASE64URL
                .decode(raw.as_bytes())
                .map(Rfc822)
                .map_err(|e| DecodeError::InvalidField {
                    field: "message.raw",
                    reason: e.to_string(),
                })
        })
        .transpose()?;
    Ok(Some(FetchedDraft { draft, labels, raw }))
}

/// `drafts.create` and `drafts.update`: the draft id is required; the new
/// message's ids are taken when present.
pub fn written(body: &[u8]) -> Result<WrittenDraft, DecodeError> {
    let wire: WireDraft = json(body)?;
    let id = DraftId(required(wire.id, "id")?);
    let (message, thread_id) = match wire.message {
        Some(message) => (
            message.id.filter(|v| !v.is_empty()).map(MessageId),
            message.thread_id.filter(|v| !v.is_empty()).map(ThreadId),
        ),
        None => (None, None),
    };
    Ok(WrittenDraft {
        id,
        message,
        thread_id,
    })
}

/// `drafts.send`: the sent message (a new id) and its thread.
pub fn sent(body: &[u8]) -> Result<(MessageId, ThreadId), DecodeError> {
    let wire: WireMessage = json(body)?;
    Ok((
        MessageId(required(wire.id, "id")?),
        ThreadId(required(wire.thread_id, "threadId")?),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(request: &HttpRequest) -> serde_json::Value {
        serde_json::from_slice(request.body.as_deref().expect("a body")).expect("json")
    }

    const RAW: &[u8] = b"X-Mailtide-Id: 0123456789abcdef\r\nSubject: hi\r\n\r\nbody?>~";

    #[test]
    fn create_posts_base64url_raw_with_the_thread_id() {
        let request = create(&Rfc822(RAW.to_vec()), Some(&ThreadId("t1".into())));
        assert_eq!(request.method, Method::Post);
        assert_eq!(request.path_and_query(), "/gmail/v1/users/me/drafts");
        let body = body(&request);
        let raw = body["message"]["raw"].as_str().expect("raw");
        assert!(!raw.contains('='), "unpadded");
        assert!(!raw.contains('+') && !raw.contains('/'), "url-safe");
        assert_eq!(URL_SAFE_NO_PAD.decode(raw).expect("base64url"), RAW);
        assert_eq!(body["message"]["threadId"], "t1");
        let standalone = create(&Rfc822(RAW.to_vec()), None);
        assert!(body_has_no_thread(&standalone));
    }

    fn body_has_no_thread(request: &HttpRequest) -> bool {
        body(request)["message"].get("threadId").is_none()
    }

    #[test]
    fn update_puts_to_the_draft_id() {
        let request = update(
            &DraftId("r-12/3".into()),
            &Rfc822(RAW.to_vec()),
            &ThreadId("t1".into()),
        );
        assert_eq!(request.method, Method::Put);
        assert_eq!(
            request.path_and_query(),
            "/gmail/v1/users/me/drafts/r-12%2F3"
        );
        let body = body(&request);
        assert_eq!(body["message"]["threadId"], "t1");
        assert_eq!(
            URL_SAFE_NO_PAD
                .decode(body["message"]["raw"].as_str().expect("raw"))
                .expect("base64url"),
            RAW
        );
    }

    #[test]
    fn send_posts_only_the_draft_id() {
        let request = send(&DraftId("r1".into()));
        assert_eq!(request.method, Method::Post);
        assert_eq!(request.path_and_query(), "/gmail/v1/users/me/drafts/send");
        assert_eq!(
            body(&request),
            json!({"id": "r1"}),
            "no message: sent as stored"
        );
    }

    #[test]
    fn list_asks_for_full_pages_including_spam_and_trash() {
        assert_eq!(
            list(None).path_and_query(),
            "/gmail/v1/users/me/drafts?maxResults=500&includeSpamTrash=true"
        );
        assert_eq!(
            list(Some(&PageToken("next".into()))).path_and_query(),
            "/gmail/v1/users/me/drafts?maxResults=500&includeSpamTrash=true&pageToken=next"
        );
        let page = list_page(
            br#"{"drafts":[{"id":"r1","message":{"id":"m1","threadId":"t1"}}],"nextPageToken":"n","resultSizeEstimate":1}"#,
        )
        .expect("decodes");
        assert_eq!(
            page.drafts,
            vec![ListedDraft {
                id: DraftId("r1".into()),
                message: MessageId("m1".into()),
                thread_id: ThreadId("t1".into()),
            }]
        );
        assert_eq!(page.next_page_token, Some(PageToken("n".into())));
        assert_eq!(
            list_page(br#"{"resultSizeEstimate":0}"#)
                .expect("empty")
                .drafts,
            Vec::new()
        );
        let delete = delete(&DraftId("r1".into()));
        assert_eq!(delete.method, Method::Delete);
        assert_eq!(delete.body, None);
    }

    #[test]
    fn a_draft_answer_without_a_message_id_is_refused() {
        for body in [
            br#"{"message":{"id":"m1","threadId":"t1"}}"#.as_slice(),
            br#"{"id":"","message":{"id":"m1","threadId":"t1"}}"#,
            br#"{"id":"r1","message":{"threadId":"t1"}}"#,
            br#"{"id":"r1"}"#,
        ] {
            assert!(
                matches!(fetched(body), Err(DecodeError::MissingField(_))),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
        assert!(matches!(
            list_page(br#"{"drafts":[{"id":"r1","message":{"threadId":"t1"}}]}"#),
            Err(DecodeError::MissingField("message.id"))
        ));
        assert_eq!(
            written(br#"{"message":{"id":"m1","threadId":"t1"}}"#),
            Err(DecodeError::MissingField("id"))
        );
        // A write answer may lack the message ids: the driver completes it.
        assert_eq!(written(br#"{"id":"r1"}"#).map(|w| w.complete()), Ok(None));
    }

    #[test]
    fn a_draft_answer_whose_message_lacks_the_draft_label_is_not_a_draft() {
        let inbox = br#"{"id":"r1","message":{"id":"m1","threadId":"t1","labelIds":["INBOX"]}}"#;
        assert_eq!(fetched(inbox), Ok(None));
        let draft = br#"{"id":"r1","message":{"id":"m1","threadId":"t1","labelIds":["DRAFT"]}}"#;
        let found = fetched(draft).expect("decodes").expect("a draft");
        assert_eq!(found.draft.message, MessageId("m1".into()));
        assert_eq!(found.raw, None);
    }
}
