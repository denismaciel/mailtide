//! Permanent deletion on the wire: `messages.delete` and
//! `messages.batchDelete`, by message id only, and telling Gmail's
//! missing-scope refusal from its other answers.
//!
//! Both calls list [`FULL_SCOPE`] as their only scope: `gmail.modify` is not
//! enough. Settled against Gmail 2026-11-20 (`purge-scope-refusal`): with a
//! `gmail.modify` or `gmail.readonly` token `messages.delete` answers 403,
//! `PERMISSION_DENIED`, reason `insufficientPermissions`
//! (`ACCESS_TOKEN_SCOPE_INSUFFICIENT`), and deletes nothing.

use mailtide::ids::MessageId;
use mailtide::retry::{AccountProblem, Blocking, BlockingFailure, Retryable, RetryableFailure};
use serde_json::json;

use crate::http::{HttpRequest, Method, encode};

const USER: &str = "/gmail/v1/users/me";

/// The OAuth scope permanent deletion needs.
pub const FULL_SCOPE: &str = "https://mail.google.com/";

/// `DELETE /messages/{id}`: no body; Gmail answers 204.
#[must_use]
pub fn delete(id: &MessageId) -> HttpRequest {
    HttpRequest {
        method: Method::Delete,
        path: format!("{USER}/messages/{}", encode(&id.0)),
        query: Vec::new(),
        body: None,
    }
}

/// `POST /messages/batchDelete` with `{ids}` and nothing else: never a
/// query, never a label. Gmail's answer is empty and names no id.
#[must_use]
pub fn batch_delete(ids: &[MessageId]) -> HttpRequest {
    let ids: Vec<&str> = ids.iter().map(|id| id.0.as_str()).collect();
    HttpRequest {
        method: Method::Post,
        path: format!("{USER}/messages/batchDelete"),
        query: Vec::new(),
        body: Some(json!({ "ids": ids }).to_string().into_bytes()),
    }
}

/// Whether Gmail refused a delete because the token lacks [`FULL_SCOPE`].
///
/// [`crate::errors::classify`] preserves the structured scope reason in
/// the detail of an [`AccountProblem::ApiAccessDenied`] refusal.
#[must_use]
pub fn missing_scope(refusal: &BlockingFailure) -> bool {
    let mut detail = refusal.detail.split_ascii_whitespace();
    refusal.class == Blocking::Account(AccountProblem::ApiAccessDenied)
        && detail.next() == Some("403")
        && matches!(
            detail.next(),
            Some("insufficientPermissions" | "ACCESS_TOKEN_SCOPE_INSUFFICIENT")
        )
}

/// Whether a transient failure of a delete came with Gmail's answer, so the
/// delete may have been applied (a 5xx or a 408): it is `sent`, not
/// `unsent`. A 401 (the token was refused before anything ran), a rate
/// limit and a request that never left are not. [`crate::errors::classify`]
/// leads the detail with the status it answered.
#[must_use]
pub fn may_have_applied(failure: &RetryableFailure) -> bool {
    let status = failure
        .detail
        .get(..3)
        .filter(|_| failure.detail.as_bytes().get(3).is_none_or(|b| *b == b' '))
        .and_then(|digits| digits.parse::<u16>().ok());
    failure.class == Retryable::Transient && matches!(status, Some(408 | 500..=599))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::classify;
    use crate::http::HttpResponse;
    use crate::request::{RequestConfig, build};
    use mailtide::command::GmailRequest;
    use mailtide::retry::Failure;

    fn config() -> RequestConfig {
        RequestConfig {
            watch_topic: String::new(),
        }
    }

    #[test]
    fn delete_sends_delete_to_the_message_id() {
        let [request] = build(
            &GmailRequest::DeleteMessage {
                id: MessageId("18c/2a b".into()),
            },
            &config(),
        )
        .try_into()
        .expect("one request");
        assert_eq!(request.method, Method::Delete);
        assert_eq!(
            request.path_and_query(),
            "/gmail/v1/users/me/messages/18c%2F2a%20b"
        );
        assert_eq!(request.body, None);
    }

    #[test]
    fn batch_delete_posts_the_ids_and_nothing_else() {
        let ids = vec![MessageId("a".into()), MessageId("b".into())];
        let [request] = build(&GmailRequest::BatchDeleteMessages { ids }, &config())
            .try_into()
            .expect("one request");
        assert_eq!(request.method, Method::Post);
        assert_eq!(
            request.path_and_query(),
            "/gmail/v1/users/me/messages/batchDelete"
        );
        let body: serde_json::Value =
            serde_json::from_slice(request.body.as_deref().unwrap_or_default()).expect("json");
        assert_eq!(body, json!({ "ids": ["a", "b"] }));
    }

    fn answer(status: u16, body: &serde_json::Value) -> HttpResponse {
        HttpResponse {
            status,
            headers: Vec::new(),
            body: body.to_string().into_bytes(),
        }
    }

    fn refusal(failure: &Failure) -> Option<&BlockingFailure> {
        match failure {
            Failure::Blocking(refusal) => Some(refusal),
            Failure::NotFound | Failure::Retryable(_) => None,
        }
    }

    #[test]
    fn a_missing_scope_refusal_is_told_from_a_rate_limit() {
        // Gmail's answer to messages.delete with a gmail.modify token, as
        // recorded 2026-11-20.
        let scope = answer(
            403,
            &json!({"error": {
                "code": 403,
                "message": "Request had insufficient authentication scopes.",
                "errors": [{
                    "message": "Insufficient Permission",
                    "domain": "global",
                    "reason": "insufficientPermissions",
                }],
                "status": "PERMISSION_DENIED",
                "details": [{
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "ACCESS_TOKEN_SCOPE_INSUFFICIENT",
                    "domain": "googleapis.com",
                    "metadata": {
                        "method": "caribou.api.proto.MailboxService.DeleteMessage",
                        "service": "gmail.googleapis.com",
                    },
                }],
            }}),
        );
        let failure = classify(&scope);
        assert!(refusal(&failure).is_some_and(missing_scope), "{failure:?}");

        let throttled = answer(
            403,
            &json!({"error": {"code": 403, "message": "User-rate limit exceeded.",
                "errors": [{"reason": "userRateLimitExceeded"}]}}),
        );
        let failure = classify(&throttled);
        assert!(
            matches!(&failure, Failure::Retryable(r) if r.class == Retryable::AccountRateLimited),
            "a rate limit stays a rate limit: {failure:?}"
        );
        assert!(refusal(&failure).is_none());

        // A 5xx may have been applied; a 401 and a 429 were not.
        let retryable = |status: u16, reason: &str| match classify(&answer(
            status,
            &json!({"error": {"code": status, "message": "x", "errors": [{"reason": reason}]}}),
        )) {
            Failure::Retryable(failure) => failure,
            other => panic!("{other:?}"),
        };
        assert!(may_have_applied(&retryable(500, "backendError")));
        assert!(may_have_applied(&retryable(503, "backendError")));
        assert!(!may_have_applied(&retryable(401, "authError")));
        assert!(!may_have_applied(&retryable(429, "rateLimitExceeded")));
        assert!(!may_have_applied(&RetryableFailure {
            class: Retryable::Transient,
            retry_after: None,
            detail: "connection refused".into(),
        }));

        // A disabled API and a bad id are refusals, not a missing scope.
        for (status, reason) in [(403, "accessNotConfigured"), (400, "invalidArgument")] {
            let failure = classify(&answer(
                status,
                &json!({"error": {"code": status, "message": "Invalid id value",
                    "errors": [{"reason": reason}]}}),
            ));
            let refused = refusal(&failure).expect("a refusal");
            assert!(!missing_scope(refused), "{status} {reason}");
        }
    }

    #[test]
    fn the_structured_scope_reason_works_without_a_legacy_reason() {
        let response = answer(
            403,
            &json!({"error": {
                "status": "PERMISSION_DENIED",
                "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "ACCESS_TOKEN_SCOPE_INSUFFICIENT"}]
            }}),
        );
        assert!(refusal(&classify(&response)).is_some_and(missing_scope));
    }
}
