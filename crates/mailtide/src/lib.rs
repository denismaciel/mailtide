//! mailtide: a sans-IO Gmail sync engine.
//!
//! The engine never performs I/O and never reads the clock. The host feeds it
//! [`Event`]s (facts it observed, stamped with the time) and performs the
//! [`Command`]s it returns. All durable state lives in the store, which the
//! engine reaches only through [`commit::Commit`]s and the snapshot it is
//! built from ([`state::StoreSnapshot`]). See `SPEC.md`.
//!
//! Module layout follows Zero's mailbox sync (Mail-0/Zero, MIT,
//! `apps/server/MAILBOX_SYNC.md`), restated as a pure state machine.

pub mod command;
pub mod commit;
pub mod drift;
pub mod engine;
pub mod event;
pub mod gmail;
pub mod history;
pub mod ids;
pub mod invariants;
pub mod jobs;
pub mod label_names;
pub mod labels;
pub mod mirror;
pub mod outbox;
pub mod purge;
pub mod quota;
pub mod reconcile;
pub mod retry;
pub mod revision;
pub mod schedule;
pub mod search;
pub mod state;
pub mod status;
pub mod thread;

pub use command::Command;
pub use engine::{Engine, EngineConfig};
pub use event::Event;
pub use ids::{AccountId, HistoryId, LabelId, MessageId, ThreadId, Timestamp};
