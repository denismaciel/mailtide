//! Stateful multi-account fake Gmail HTTP server for local development,
//! UI testing, stress testing, and fault injection.
//!
//! Point a `mailtide` runner at the server's loopback origin via
//! `HttpConfig::origin` (or `mailtide.config.Http(gmail_origin=...)`) and
//! mint tokens of the form `fake:<account_email>` to route each account to
//! its own isolated mailbox.
//!
//! Out-of-band control endpoints live under `/_control/...`, health under
//! `/_health`, and a built-in web inspector under `/_ui`.

#![allow(
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss
)]

mod control;
mod gmail;
mod persist;
mod push;
pub mod seed;
mod server;
pub mod state;
mod ui;

pub use server::{FakeGmailServer, ServerConfig, ServerError};
pub use state::{
    AccountFaults, AccountSummary, DropSendMode, FakeGmailState, GlobalFaults, InjectAttachment,
    InjectMessage, RefusalFault, RequestLogEntry, SentRecord,
};
