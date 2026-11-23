//! Loopback HTTP server hosting the fake Gmail REST API, control API, and web inspector.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt as _, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde::Serialize;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::AbortHandle;

use crate::persist::Persistence;
use crate::push::PushNotifier;
use crate::state::FakeGmailState;
use crate::{control, gmail, ui};

/// Configuration for starting a [`FakeGmailServer`].
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Host to bind to (must be loopback: `127.0.0.1`, `::1`, or `localhost`).
    pub host: String,
    /// TCP port to bind (`0` chooses an ephemeral free port).
    pub port: u16,
    /// Optional SQLite file path for persisting mailbox state across restarts.
    pub state_path: Option<PathBuf>,
    /// Optional webhook URL receiving debounced `{emailAddress, historyId}` POSTs on mutations.
    pub push_url: Option<String>,
    /// Coalescing window for debounced push notifications.
    pub push_debounce: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 0,
            state_path: None,
            push_url: None,
            push_debounce: Duration::from_millis(150),
        }
    }
}

/// Why [`FakeGmailServer::start`] failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError(pub String);

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ServerError {}

/// Shared server context passed into HTTP handlers.
pub(crate) struct AppContext {
    pub(crate) state: Mutex<FakeGmailState>,
    pub(crate) persist: Persistence,
    pub(crate) push: PushNotifier,
    pub(crate) bursts: Mutex<BTreeMap<String, AbortHandle>>,
}

/// Simple buffered HTTP reply built by handlers.
pub(crate) struct HttpReply {
    pub(crate) status: StatusCode,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Bytes,
}

pub(crate) fn json_reply<T: Serialize + ?Sized>(status: StatusCode, body: &T) -> HttpReply {
    let data = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    HttpReply {
        status,
        headers: vec![("content-type".to_owned(), "application/json".to_owned())],
        body: Bytes::from(data),
    }
}

pub(crate) fn text_reply(status: StatusCode, content_type: &str, body: String) -> HttpReply {
    HttpReply {
        status,
        headers: vec![("content-type".to_owned(), content_type.to_owned())],
        body: Bytes::from(body),
    }
}

pub(crate) fn no_content_reply() -> HttpReply {
    HttpReply {
        status: StatusCode::NO_CONTENT,
        headers: Vec::new(),
        body: Bytes::new(),
    }
}

/// Running fake Gmail HTTP server.
pub struct FakeGmailServer {
    addr: SocketAddr,
    origin: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
    ctx: Arc<AppContext>,
}

impl FakeGmailServer {
    /// Start the server on a loopback address.
    pub async fn start(config: ServerConfig) -> Result<Self, ServerError> {
        let ip = parse_loopback_host(&config.host)?;
        let persist = match &config.state_path {
            Some(path) => Persistence::open(path).await.map_err(ServerError)?,
            None => Persistence::memory(),
        };
        let loaded = persist.load_all().await.map_err(ServerError)?;
        let ctx = Arc::new(AppContext {
            state: Mutex::new(loaded),
            persist,
            push: PushNotifier::new(config.push_url, config.push_debounce),
            bursts: Mutex::new(BTreeMap::new()),
        });

        let listener = TcpListener::bind(SocketAddr::new(ip, config.port))
            .await
            .map_err(|e| {
                ServerError(format!(
                    "failed to bind {}:{}: {e}",
                    config.host, config.port
                ))
            })?;
        let addr = listener
            .local_addr()
            .map_err(|e| ServerError(format!("failed to inspect local addr: {e}")))?;
        let origin = match addr {
            SocketAddr::V4(v4) => format!("http://{}:{}", v4.ip(), v4.port()),
            SocketAddr::V6(v6) => format!("http://[{}]:{}", v6.ip(), v6.port()),
        };

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let server_ctx = Arc::clone(&ctx);
        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let conn_ctx = Arc::clone(&server_ctx);
                        tokio::spawn(async move {
                            let io = TokioIo::new(stream);
                            let svc = service_fn(move |req: Request<Incoming>| {
                                let req_ctx = Arc::clone(&conn_ctx);
                                async move { dispatch(req_ctx, req).await }
                            });
                            let _ = http1::Builder::new().serve_connection(io, svc).await;
                        });
                    }
                }
            }
            let mut bursts = server_ctx.bursts.lock().expect("bursts lock");
            for (_, handle) in std::mem::take(&mut *bursts) {
                handle.abort();
            }
        });

        Ok(Self {
            addr,
            origin,
            shutdown_tx: Some(shutdown_tx),
            join: Some(join),
            ctx,
        })
    }

    /// Base HTTP origin (e.g. `http://127.0.0.1:8200`).
    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Bound socket address.
    #[must_use]
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Bound TCP port.
    #[must_use]
    pub const fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Inspect or mutate the in-memory state directly (useful in Rust tests).
    pub fn with_state<R>(&self, f: impl FnOnce(&mut FakeGmailState) -> R) -> R {
        let mut guard = self.ctx.state.lock().expect("state lock");
        f(&mut guard)
    }

    /// Shut down the server cleanly and stop any active burst tasks.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

impl Drop for FakeGmailServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

fn parse_loopback_host(host: &str) -> Result<IpAddr, ServerError> {
    let trimmed = host.trim();
    if trimmed.eq_ignore_ascii_case("localhost") {
        return Ok(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    }
    let ip: IpAddr = trimmed
        .parse()
        .map_err(|_| ServerError(format!("invalid host {host:?}: expected loopback IP")))?;
    if !ip.is_loopback() {
        return Err(ServerError(format!(
            "refusing to bind non-loopback address {host:?}"
        )));
    }
    Ok(ip)
}

async fn dispatch(
    ctx: Arc<AppContext>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, std::io::Error> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path().to_owned();
    let query = uri.query().unwrap_or("").to_owned();
    let auth = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let body_bytes = req
        .into_body()
        .collect()
        .await
        .map_or_else(|_| Bytes::new(), http_body_util::Collected::to_bytes);

    let reply = if path.starts_with("/gmail/v1/users/") {
        match gmail::handle(&ctx, method, &path, &query, auth.as_deref(), body_bytes).await {
            Ok(reply) => reply,
            Err(()) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "injected connection drop",
                ));
            }
        }
    } else if path.starts_with("/_control") {
        control::handle(&ctx, method, &path, &query, body_bytes).await
    } else if path == "/_health" {
        let count = ctx.state.lock().expect("state lock").accounts.len();
        json_reply(
            StatusCode::OK,
            &json!({ "status": "ok", "accounts": count }),
        )
    } else if path == "/_ui" || path == "/_ui/" {
        ui::index()
    } else if path == "/" {
        HttpReply {
            status: StatusCode::FOUND,
            headers: vec![("location".to_owned(), "/_ui".to_owned())],
            body: Bytes::new(),
        }
    } else {
        json_reply(StatusCode::NOT_FOUND, &json!({ "error": "not found" }))
    };

    let mut builder = Response::builder().status(reply.status);
    for (k, v) in reply.headers {
        builder = builder.header(k, v);
    }
    let response = builder
        .body(Full::new(reply.body))
        .expect("valid HTTP response");
    Ok(response)
}
