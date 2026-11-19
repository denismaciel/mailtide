//! Sends Gmail HTTP requests.

use std::future::Future;
use std::time::Duration;

use mailtide_gmail::http::{HttpRequest, HttpResponse, Method, ORIGIN};

use crate::auth::AccessToken;

/// A transport failure before Gmail answered (review #10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// The request certainly didn't reach Gmail.
    NotSent(String),
    /// It may or may not have been applied (timeout, dropped connection).
    OutcomeUnknown(String),
}

/// Executes HTTP requests against Gmail.
pub trait GmailTransport: Send + Sync {
    /// Send one request with `token`.
    fn send(
        &self,
        request: HttpRequest,
        token: &AccessToken,
    ) -> impl Future<Output = Result<HttpResponse, TransportError>> + Send;
}

/// HTTP settings for the Gmail transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpConfig {
    /// Gmail's origin; override only for tests (it receives the bearer token).
    pub origin: String,
    /// The whole request, connect to last byte.
    pub request_timeout: Duration,
    /// Establishing the connection.
    pub connect_timeout: Duration,
    /// Sent as `User-Agent`.
    pub user_agent: String,
}

impl HttpConfig {
    /// Gmail's origin, `timeout` per request, a 10 s connect timeout, and
    /// `mailtide/<version>` as the user agent.
    #[must_use]
    pub fn new(timeout: Duration) -> Self {
        Self {
            origin: ORIGIN.to_owned(),
            request_timeout: timeout,
            connect_timeout: Duration::from_secs(10),
            user_agent: format!("mailtide/{}", env!("CARGO_PKG_VERSION")),
        }
    }
}

/// The production transport (reqwest, rustls).
#[derive(Debug, Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
    origin: String,
}

impl ReqwestTransport {
    /// A transport to Gmail with `timeout` per request.
    pub fn new(timeout: Duration) -> Result<Self, TransportError> {
        Self::with_config(&HttpConfig::new(timeout))
    }

    /// A transport to another origin (tests, proxies).
    pub fn with_origin(origin: &str, timeout: Duration) -> Result<Self, TransportError> {
        Self::with_config(&HttpConfig {
            origin: origin.to_owned(),
            ..HttpConfig::new(timeout)
        })
    }

    /// A transport with every HTTP setting explicit.
    pub fn with_config(config: &HttpConfig) -> Result<Self, TransportError> {
        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .user_agent(config.user_agent.as_str())
            .build()
            .map_err(|e| TransportError::NotSent(e.to_string()))?;
        let origin = config.origin.as_str();
        Ok(Self {
            client,
            origin: origin.trim_end_matches('/').to_owned(),
        })
    }
}

impl GmailTransport for ReqwestTransport {
    async fn send(
        &self,
        request: HttpRequest,
        token: &AccessToken,
    ) -> Result<HttpResponse, TransportError> {
        let url = format!("{}{}", self.origin, request.path_and_query());
        let builder = match request.method {
            Method::Get => self.client.get(url),
            Method::Post => self.client.post(url),
        }
        .bearer_auth(&token.secret);
        let builder = match request.body {
            Some(body) => builder
                .header("content-type", "application/json")
                .body(body),
            None => builder,
        };
        let response = builder.send().await.map_err(|e| {
            if e.is_connect() || e.is_builder() || e.is_request() && !e.is_timeout() {
                TransportError::NotSent(e.to_string())
            } else {
                TransportError::OutcomeUnknown(e.to_string())
            }
        })?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_ascii_lowercase(),
                    v.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        let body = response
            .bytes()
            .await
            .map_err(|e| TransportError::OutcomeUnknown(e.to_string()))?
            .to_vec();
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}
