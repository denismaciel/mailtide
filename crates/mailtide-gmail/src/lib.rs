//! Gmail API wire layer, pure: no network.
//!
//! Builds HTTP requests for engine commands, decodes Gmail responses into
//! engine types once at the boundary, classifies errors, batches requests,
//! extracts header facts, and verifies push notifications. Hand written over
//! the endpoints mailtide needs (no generated client).
//!
//! SPEC §5. Zero counterpart: `lib/driver/google.ts`, `lib/gmail-push.ts`,
//! `lib/gmail-quota-error.ts`.

pub mod batch;
pub mod decode;
pub mod drafts;
pub mod errors;
pub mod headers;
pub mod http;
pub mod labels;
pub mod purge;
pub mod push;
pub mod request;
pub mod search;
pub mod send;
