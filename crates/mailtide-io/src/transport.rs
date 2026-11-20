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
    /// Send one request with `token`. Make at most one wire attempt, with
    /// no automatic retry or redirect. The caller decides what to retry.
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
        let client = client_builder(config)
            .build()
            .map_err(|e| TransportError::NotSent(e.to_string()))?;
        let origin = config.origin.as_str();
        Ok(Self {
            client,
            origin: origin.trim_end_matches('/').to_owned(),
        })
    }
}

fn client_builder(config: &HttpConfig) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .timeout(config.request_timeout)
        .connect_timeout(config.connect_timeout)
        .user_agent(config.user_agent.as_str())
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
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
            Method::Put => self.client.put(url),
            Method::Patch => self.client.patch(url),
            Method::Delete => self.client.delete(url),
        }
        .bearer_auth(&token.secret);
        let builder = match request.body {
            Some(body) => builder
                .header("content-type", "application/json")
                .body(body),
            None => builder,
        };
        let response = builder.send().await.map_err(|e| {
            // Only a request that never got a connection is certainly
            // unsent. Every other failure (reqwest calls a connection
            // dropped after the request went out a request error too) may
            // have reached Gmail: a write may have been applied.
            if e.is_connect() || e.is_builder() {
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

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;

    fn token() -> AccessToken {
        AccessToken {
            secret: "t0ken".into(),
            expires_at: mailtide::Timestamp(i64::MAX),
        }
    }

    fn request(method: Method, body: Option<&str>) -> HttpRequest {
        HttpRequest {
            method,
            path: "/gmail/v1/users/me/labels/Label_1".into(),
            query: Vec::new(),
            body: body.map(|b| b.as_bytes().to_vec()),
        }
    }

    /// A one-connection server: reads one request (head and body), then
    /// writes `answer` (or nothing) and closes. Returns the origin and the
    /// request as received.
    async fn serve_once(answer: Option<&'static str>) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let origin = format!("http://{}", listener.local_addr().expect("addr"));
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut received = Vec::new();
            let mut buffer = [0_u8; 4096];
            loop {
                let n = socket.read(&mut buffer).await.expect("read");
                received.extend_from_slice(&buffer[..n]);
                let text = String::from_utf8_lossy(&received).to_string();
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    if body.len() >= length {
                        break;
                    }
                }
                if n == 0 {
                    break;
                }
            }
            if let Some(answer) = answer {
                socket.write_all(answer.as_bytes()).await.expect("write");
            }
            String::from_utf8_lossy(&received).to_string()
        });
        (origin, task)
    }

    fn transport(origin: &str) -> ReqwestTransport {
        ReqwestTransport::with_origin(origin, Duration::from_secs(5)).expect("transport")
    }

    #[tokio::test]
    async fn writes_go_out_with_their_method_token_and_body() {
        for (method, body) in [
            (Method::Put, Some("{\"name\":\"a\"}")),
            (Method::Patch, Some("{\"name\":\"b\"}")),
            (Method::Delete, None),
        ] {
            let (origin, server) =
                serve_once(Some("HTTP/1.1 204 No Content\r\ncontent-length: 0\r\n\r\n")).await;
            let response = transport(&origin)
                .send(request(method, body), &token())
                .await
                .expect("answered");
            assert_eq!((response.status, response.body.len()), (204, 0));
            assert!(response.is_success());
            let received = server.await.expect("server");
            let lower = received.to_ascii_lowercase();
            assert!(
                received.starts_with(&format!(
                    "{} /gmail/v1/users/me/labels/Label_1 HTTP/1.1\r\n",
                    method.as_str()
                )),
                "{received}"
            );
            assert!(
                lower.contains("authorization: bearer t0ken\r\n"),
                "{received}"
            );
            match body {
                Some(json) => {
                    assert!(lower.contains("content-type: application/json\r\n"));
                    assert!(received.ends_with(json), "{received}");
                }
                None => assert!(!lower.contains("content-type:"), "{received}"),
            }
        }
    }

    #[tokio::test]
    async fn an_error_answer_to_a_write_is_a_response_not_a_transport_error() {
        let (origin, server) = serve_once(Some(
            "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{}",
        ))
        .await;
        let response = transport(&origin)
            .send(request(Method::Delete, None), &token())
            .await
            .expect("answered");
        assert_eq!(response.status, 404);
        assert_eq!(response.header("Content-Type"), Some("application/json"));
        server.await.expect("server");
    }

    #[tokio::test]
    async fn redirects_do_not_issue_a_second_wire_request() {
        for redirect in [
            "HTTP/1.1 307 Temporary Redirect",
            "HTTP/1.1 308 Permanent Redirect",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let origin = format!("http://{}", listener.local_addr().expect("addr"));
            let expected = redirect[9..12].parse::<u16>().expect("status");
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.expect("first request");
                let mut request = [0; 4096];
                assert!(socket.read(&mut request).await.expect("request") > 0);
                let response = format!(
                    "{redirect}\r\nlocation: /other\r\nconnection: close\r\ncontent-length: 0\r\n\r\n"
                );
                socket.write_all(response.as_bytes()).await.expect("write");
                drop(socket);
                if let Ok(Ok((mut next, _))) =
                    tokio::time::timeout(Duration::from_millis(100), listener.accept()).await
                {
                    let _ = next
                        .write_all(b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\n\r\n")
                        .await;
                    return true;
                }
                false
            });
            let response = transport(&origin)
                .send(request(Method::Delete, None), &token())
                .await
                .expect("answered");
            assert!(!server.await.expect("server"), "follow-up request");
            assert_eq!(response.status, expected);
        }
    }

    #[tokio::test]
    async fn an_http2_refused_stream_does_not_issue_a_second_wire_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let origin = format!("http://{}", listener.local_addr().expect("addr"));
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("first connection");
            let mut h2 = h2::server::handshake(socket).await.expect("handshake");
            let (_, mut response) = h2.accept().await.expect("stream").expect("request");
            response.send_reset(h2::Reason::REFUSED_STREAM);
            tokio::time::timeout(Duration::from_millis(200), async {
                tokio::select! {
                    stream = h2.accept() => stream.is_some(),
                    connection = listener.accept() => connection.is_ok(),
                }
            })
            .await
            .unwrap_or(false)
        });
        let client = client_builder(&HttpConfig::new(Duration::from_secs(5)))
            .http2_prior_knowledge()
            .build()
            .expect("HTTP/2 client");
        let result = ReqwestTransport { client, origin }
            .send(request(Method::Delete, None), &token())
            .await;
        assert!(!server.await.expect("server"), "follow-up request");
        assert!(result.is_err(), "{result:?}");
    }

    /// A write Gmail received but never answered may have been applied: it
    /// must not be reported as certainly unsent.
    #[tokio::test]
    async fn a_write_whose_connection_drops_has_an_unknown_outcome() {
        let (origin, server) = serve_once(None).await;
        let error = transport(&origin)
            .send(request(Method::Patch, Some("{}")), &token())
            .await
            .expect_err("no answer");
        server.await.expect("server");
        assert!(
            matches!(error, TransportError::OutcomeUnknown(_)),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_refused_connection_is_not_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let origin = format!("http://{}", listener.local_addr().expect("addr"));
        drop(listener);
        let error = transport(&origin)
            .send(request(Method::Delete, None), &token())
            .await
            .expect_err("nobody listens");
        assert!(matches!(error, TransportError::NotSent(_)), "{error:?}");
    }
}
