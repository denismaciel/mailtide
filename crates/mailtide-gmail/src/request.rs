//! Engine commands → HTTP requests.

use mailtide::command::GmailRequest;
use mailtide::outbox::ModifyGroup;
use mailtide::reconcile::ListScope;
use serde_json::json;

use crate::http::{HttpRequest, Method, encode};

const USER: &str = "/gmail/v1/users/me";

/// Page size for list calls (Gmail's maximum).
pub const PAGE_SIZE: u32 = 500;

/// Settings that requests need from the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestConfig {
    /// The Pub/Sub topic Gmail publishes to, e.g. `projects/p/topics/gmail`.
    pub watch_topic: String,
}

/// The HTTP requests for a Gmail command: one, except a small modify group,
/// which is sent as one `messages.modify` per message (cheaper than one
/// `batchModify` below [`mailtide::quota::BATCH_MODIFY_THRESHOLD`]).
///
/// `history.list` is never filtered by history type or label (both silently
/// narrow the stream); `messages.list` includes spam and trash.
#[must_use]
pub fn build(request: &GmailRequest, config: &RequestConfig) -> Vec<HttpRequest> {
    match request {
        GmailRequest::GetProfile => vec![get(format!("{USER}/profile"), Vec::new())],
        GmailRequest::ListHistory { start, page_token } => {
            let mut query = vec![
                ("startHistoryId".to_owned(), start.0.to_string()),
                ("maxResults".to_owned(), PAGE_SIZE.to_string()),
            ];
            if let Some(token) = page_token {
                query.push(("pageToken".to_owned(), token.0.clone()));
            }
            vec![get(format!("{USER}/history"), query)]
        }
        GmailRequest::ListMessages { scope, page_token } => {
            let mut query = vec![
                ("includeSpamTrash".to_owned(), "true".to_owned()),
                ("maxResults".to_owned(), PAGE_SIZE.to_string()),
            ];
            match scope {
                ListScope::All => {}
                ListScope::Label(label) => query.push(("labelIds".to_owned(), label.0.clone())),
            }
            if let Some(token) = page_token {
                query.push(("pageToken".to_owned(), token.0.clone()));
            }
            vec![get(format!("{USER}/messages"), query)]
        }
        GmailRequest::GetMessage { id } => vec![get(
            format!("{USER}/messages/{}", encode(&id.0)),
            vec![("format".to_owned(), "minimal".to_owned())],
        )],
        GmailRequest::ImportMessage { id } => vec![get(
            format!("{USER}/messages/{}", encode(&id.0)),
            vec![("format".to_owned(), "raw".to_owned())],
        )],
        GmailRequest::ListLabels => vec![get(format!("{USER}/labels"), Vec::new())],
        GmailRequest::Watch => vec![post(
            format!("{USER}/watch"),
            // No label filter: every change in the mailbox is published.
            &json!({ "topicName": config.watch_topic }),
        )],
        GmailRequest::Modify(group) => modify(group),
    }
}

fn modify(group: &ModifyGroup) -> Vec<HttpRequest> {
    let add: Vec<&str> = group.add.iter().map(|l| l.0.as_str()).collect();
    let remove: Vec<&str> = group.remove.iter().map(|l| l.0.as_str()).collect();
    let threshold = usize::try_from(mailtide::quota::BATCH_MODIFY_THRESHOLD).unwrap_or(usize::MAX);
    if group.messages.len() < threshold {
        group
            .messages
            .iter()
            .map(|id| {
                post(
                    format!("{USER}/messages/{}/modify", encode(&id.0)),
                    &json!({ "addLabelIds": add, "removeLabelIds": remove }),
                )
            })
            .collect()
    } else {
        let ids: Vec<&str> = group.messages.iter().map(|m| m.0.as_str()).collect();
        vec![post(
            format!("{USER}/messages/batchModify"),
            &json!({ "ids": ids, "addLabelIds": add, "removeLabelIds": remove }),
        )]
    }
}

fn get(path: String, query: Vec<(String, String)>) -> HttpRequest {
    HttpRequest {
        method: Method::Get,
        path,
        query,
        body: None,
    }
}

fn post(path: String, body: &serde_json::Value) -> HttpRequest {
    HttpRequest {
        method: Method::Post,
        path,
        query: Vec::new(),
        body: Some(body.to_string().into_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mailtide::ids::{HistoryId, LabelId, MessageId, PageToken};

    fn config() -> RequestConfig {
        RequestConfig {
            watch_topic: "projects/p/topics/gmail".into(),
        }
    }

    #[test]
    fn history_is_never_filtered() {
        let [request] = build(
            &GmailRequest::ListHistory {
                start: HistoryId(42),
                page_token: Some(PageToken("next".into())),
            },
            &config(),
        )
        .try_into()
        .expect("one request");
        assert_eq!(
            request.path_and_query(),
            "/gmail/v1/users/me/history?startHistoryId=42&maxResults=500&pageToken=next"
        );
    }

    #[test]
    fn label_listing_includes_spam_and_trash() {
        let [request] = build(
            &GmailRequest::ListMessages {
                scope: ListScope::Label(LabelId("INBOX".into())),
                page_token: None,
            },
            &config(),
        )
        .try_into()
        .expect("one request");
        assert_eq!(
            request.path_and_query(),
            "/gmail/v1/users/me/messages?includeSpamTrash=true&maxResults=500&labelIds=INBOX"
        );
    }

    #[test]
    fn raw_and_minimal_fetches() {
        let id = MessageId("18c2a".into());
        let [raw] = build(&GmailRequest::ImportMessage { id: id.clone() }, &config())
            .try_into()
            .expect("one request");
        assert_eq!(
            raw.path_and_query(),
            "/gmail/v1/users/me/messages/18c2a?format=raw"
        );
        let [minimal] = build(&GmailRequest::GetMessage { id }, &config())
            .try_into()
            .expect("one request");
        assert_eq!(
            minimal.path_and_query(),
            "/gmail/v1/users/me/messages/18c2a?format=minimal"
        );
    }

    #[test]
    fn small_modify_groups_become_individual_modifies() {
        let group = ModifyGroup {
            messages: vec![MessageId("a".into()), MessageId("b".into())],
            add: Vec::new(),
            remove: vec![LabelId("INBOX".into())],
            versions: Vec::new(),
        };
        let requests = build(&GmailRequest::Modify(group), &config());
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/gmail/v1/users/me/messages/a/modify");
    }

    #[test]
    fn watch_covers_all_labels() {
        let [watch] = build(&GmailRequest::Watch, &config())
            .try_into()
            .expect("one request");
        let body: serde_json::Value =
            serde_json::from_slice(watch.body.as_deref().unwrap_or_default()).expect("json");
        assert_eq!(body["topicName"], "projects/p/topics/gmail");
        assert!(body.get("labelIds").is_none(), "no label filter");
    }
}
