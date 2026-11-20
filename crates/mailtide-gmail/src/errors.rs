//! Gmail and OAuth errors → engine failures.
//!
//! Gmail reports rate limits as 403 or 429. A 429 can mean the per-user
//! concurrency limit or the per-user bandwidth cap; the message says which.
//! Account problems (domain policy, mail disabled, delegation revoked, user
//! deleted) block instead of retrying.

use std::time::Duration;

use mailtide::retry::{
    AccountProblem, Blocking, BlockingFailure, Failure, Retryable, RetryableFailure,
};
use serde::Deserialize;

use crate::http::HttpResponse;

#[derive(Deserialize, Default)]
struct WireErrorBody {
    #[serde(default)]
    error: WireError,
}

#[derive(Deserialize, Default)]
struct WireError {
    #[serde(default)]
    message: String,
    #[serde(default)]
    errors: Vec<WireReason>,
    #[serde(default)]
    status: String,
    #[serde(default)]
    details: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct WireReason {
    #[serde(default)]
    reason: String,
}

/// Classify a failed Gmail response.
#[must_use]
pub fn classify(response: &HttpResponse) -> Failure {
    let body: WireErrorBody = serde_json::from_slice(&response.body).unwrap_or_default();
    let reasons: Vec<&str> = body
        .error
        .errors
        .iter()
        .map(|r| r.reason.as_str())
        .collect();
    let message = body.error.message.to_lowercase();
    let scope_reason = if response.status == 403 {
        reasons
            .iter()
            .copied()
            .find(|reason| *reason == "insufficientPermissions")
            .or_else(|| {
                body.error
                    .details
                    .iter()
                    .any(|detail| {
                        detail.get("reason").and_then(serde_json::Value::as_str)
                            == Some("ACCESS_TOKEN_SCOPE_INSUFFICIENT")
                    })
                    .then_some("ACCESS_TOKEN_SCOPE_INSUFFICIENT")
            })
    } else {
        None
    };
    let detail = format!(
        "{} {} {}",
        response.status,
        scope_reason
            .or_else(|| reasons.first().copied())
            .unwrap_or(body.error.status.as_str()),
        body.error.message.chars().take(120).collect::<String>()
    )
    .trim()
    .to_owned();
    let retry_after = response
        .header("retry-after")
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    let retryable = |class| {
        Failure::Retryable(RetryableFailure {
            class,
            retry_after,
            detail: detail.clone(),
        })
    };
    let blocking = |class| {
        Failure::Blocking(BlockingFailure {
            class,
            detail: detail.clone(),
        })
    };
    let has = |reason: &str| reasons.contains(&reason);
    match response.status {
        404 => Failure::NotFound,
        // 401: the token expired between minting and use; retrying mints a new one.
        401 | 408 | 500..=599 => retryable(Retryable::Transient),
        429 if message.contains("concurrent") => retryable(Retryable::AccountConcurrency),
        429 if message.contains("bandwidth") => retryable(Retryable::AccountBandwidth),
        429 => retryable(Retryable::AccountRateLimited),
        403 if has("domainPolicy") => blocking(Blocking::Account(AccountProblem::DomainPolicy)),
        403 if scope_reason.is_some() || has("accessNotConfigured") || has("forbidden") => {
            blocking(Blocking::Account(AccountProblem::ApiAccessDenied))
        }
        403 if has("userRateLimitExceeded") => retryable(Retryable::AccountRateLimited),
        403 if has("rateLimitExceeded") || has("quotaExceeded") || has("dailyLimitExceeded") => {
            retryable(Retryable::ProjectRateLimited)
        }
        400 if message.contains("mail service not enabled")
            || body.error.status == "FAILED_PRECONDITION" =>
        {
            blocking(Blocking::Account(AccountProblem::MailDisabled))
        }
        _ => blocking(Blocking::Permanent),
    }
}

/// Classify an error from minting a delegated access token.
#[must_use]
pub fn classify_token_error(error: &str, description: Option<&str>) -> Failure {
    let detail = format!("{error}: {}", description.unwrap_or_default());
    let blocking = |problem| {
        Failure::Blocking(BlockingFailure {
            class: Blocking::Account(problem),
            detail: detail.clone(),
        })
    };
    let description = description.unwrap_or_default().to_lowercase();
    match error {
        "unauthorized_client" | "access_denied" => blocking(AccountProblem::DelegationRevoked),
        "invalid_grant"
            if description.contains("invalid email") || description.contains("user id") =>
        {
            blocking(AccountProblem::UserDeleted)
        }
        _ => Failure::Retryable(RetryableFailure {
            class: Retryable::Transient,
            retry_after: None,
            detail,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(status: u16, reason: &str, message: &str) -> HttpResponse {
        HttpResponse {
            status,
            headers: vec![("retry-after".into(), "7".into())],
            body: format!(r#"{{"error":{{"code":{status},"message":"{message}","errors":[{{"reason":"{reason}"}}]}}}}"#)
                .into_bytes(),
        }
    }

    fn class(failure: &Failure) -> Option<Retryable> {
        match failure {
            Failure::Retryable(retryable) => Some(retryable.class),
            Failure::NotFound | Failure::Blocking(_) => None,
        }
    }

    #[test]
    fn missing_scopes_and_disabled_apis_are_account_problems() {
        for reason in [
            "insufficientPermissions",
            "accessNotConfigured",
            "forbidden",
        ] {
            assert!(
                matches!(
                    classify(&response(403, reason, "x")),
                    Failure::Blocking(BlockingFailure {
                        class: Blocking::Account(AccountProblem::ApiAccessDenied),
                        ..
                    })
                ),
                "{reason}"
            );
        }
    }

    #[test]
    fn rate_limits_come_as_403_or_429() {
        assert_eq!(
            class(&classify(&response(403, "userRateLimitExceeded", "x"))),
            Some(Retryable::AccountRateLimited)
        );
        assert_eq!(
            class(&classify(&response(403, "rateLimitExceeded", "x"))),
            Some(Retryable::ProjectRateLimited)
        );
        assert_eq!(
            class(&classify(&response(
                429,
                "rateLimitExceeded",
                "Too many concurrent requests for user"
            ))),
            Some(Retryable::AccountConcurrency)
        );
        assert_eq!(
            class(&classify(&response(
                429,
                "rateLimitExceeded",
                "User-rate limit exceeded: bandwidth"
            ))),
            Some(Retryable::AccountBandwidth)
        );
    }

    #[test]
    fn retry_after_is_kept() {
        let Failure::Retryable(failure) = classify(&response(429, "x", "y")) else {
            panic!("retryable");
        };
        assert_eq!(failure.retry_after, Some(Duration::from_secs(7)));
    }

    #[test]
    fn account_problems_block() {
        assert!(matches!(
            classify(&response(403, "domainPolicy", "disabled")),
            Failure::Blocking(BlockingFailure {
                class: Blocking::Account(AccountProblem::DomainPolicy),
                ..
            })
        ));
        assert!(matches!(
            classify(&response(
                400,
                "failedPrecondition",
                "Mail service not enabled"
            )),
            Failure::Blocking(BlockingFailure {
                class: Blocking::Account(AccountProblem::MailDisabled),
                ..
            })
        ));
        assert!(matches!(
            classify_token_error("unauthorized_client", None),
            Failure::Blocking(BlockingFailure {
                class: Blocking::Account(AccountProblem::DelegationRevoked),
                ..
            })
        ));
        assert!(matches!(
            classify_token_error("invalid_grant", Some("Invalid email or User ID")),
            Failure::Blocking(BlockingFailure {
                class: Blocking::Account(AccountProblem::UserDeleted),
                ..
            })
        ));
    }

    #[test]
    fn not_found_and_server_errors() {
        assert_eq!(classify(&response(404, "notFound", "x")), Failure::NotFound);
        assert_eq!(
            class(&classify(&response(503, "backendError", "x"))),
            Some(Retryable::Transient)
        );
        assert!(matches!(
            classify(&response(400, "invalidArgument", "x")),
            Failure::Blocking(_)
        ));
    }

    /// PUT, PATCH and DELETE answers are classified like any other: a
    /// conflict or a bad field is refused for good, a 404 is the resource
    /// missing, and a 5xx may be retried.
    #[test]
    fn answers_to_writes_classify_like_reads() {
        assert_eq!(classify(&response(404, "notFound", "x")), Failure::NotFound);
        for (status, reason) in [(409, "alreadyExists"), (400, "invalidArgument")] {
            assert!(
                matches!(
                    classify(&response(status, reason, "x")),
                    Failure::Blocking(BlockingFailure {
                        class: Blocking::Permanent,
                        ..
                    })
                ),
                "{status}"
            );
        }
        assert_eq!(
            class(&classify(&response(500, "backendError", "x"))),
            Some(Retryable::Transient)
        );
    }
}
