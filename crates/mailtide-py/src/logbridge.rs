//! mailtide's `tracing` events into the host's Python `logging`.
//!
//! Each runner gets its own dispatcher (never a process-global subscriber):
//! events are filtered by level in Rust, so disabled ones cost nothing; the
//! rest go into a bounded queue and a drain thread hands them to
//! `logging.getLogger(<logger>)` with their fields under
//! `extra={"mailtide": {...}}`, where the host's handlers and JSON
//! formatters pick them up. A full queue drops events (counted and reported)
//! rather than ever slowing a commit. Dependencies' events (sqlx, hyper) are
//! not forwarded.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};

use pyo3::prelude::*;
use pyo3::types::PyDict;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::{FilterExt as _, LevelFilter, filter_fn};
use tracing_subscriber::layer::{Context, SubscriberExt as _};
use tracing_subscriber::registry::{LookupSpan, Registry};

/// Events waiting for the drain thread at most.
const QUEUE: usize = 4096;

/// A field value, kept typed so JSON handlers emit numbers as numbers.
#[derive(Debug, Clone)]
enum Value {
    Str(String),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

#[derive(Default)]
struct Fields(BTreeMap<String, Value>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_owned(), Value::Str(format!("{value:?}")));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0
            .insert(field.name().to_owned(), Value::Str(value.to_owned()));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_owned(), Value::I64(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), Value::U64(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().to_owned(), Value::F64(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().to_owned(), Value::Bool(value));
    }
}

/// One event on its way to Python.
struct Record {
    level: Level,
    message: String,
    fields: BTreeMap<String, Value>,
}

struct PyLogLayer {
    queue: SyncSender<Record>,
    dropped: Arc<AtomicU64>,
}

impl<S> Layer<S> for PyLogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        // Span fields first (outermost first), so the event's own win.
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(span_fields) = span.extensions().get::<Fields>() {
                    for (k, v) in &span_fields.0 {
                        fields.0.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        event.record(&mut fields);
        let message = match fields.0.remove("message") {
            Some(Value::Str(message)) => message,
            Some(other) => format!("{other:?}"),
            None => String::new(),
        };
        let record = Record {
            level: *event.metadata().level(),
            message,
            fields: fields.0,
        };
        match self.queue.try_send(record) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// How verbose the bridge is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verbosity {
    /// Nothing is logged.
    Off,
    /// Up to this level.
    Up(Level),
}

fn python_level(level: Level) -> i32 {
    match level {
        Level::ERROR => 40,
        Level::WARN => 30,
        Level::INFO => 20,
        Level::DEBUG => 10,
        Level::TRACE => 5,
    }
}

fn to_python<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match value {
        Value::Str(s) => s.into_pyobject(py)?.into_any(),
        Value::I64(n) => n.into_pyobject(py)?.into_any(),
        Value::U64(n) => n.into_pyobject(py)?.into_any(),
        Value::F64(n) => n.into_pyobject(py)?.into_any(),
        Value::Bool(b) => pyo3::types::PyBool::new(py, *b).to_owned().into_any(),
    })
}

fn emit(py: Python<'_>, logger: &str, record: &Record) -> PyResult<()> {
    let logger = py
        .import("logging")?
        .getattr("getLogger")?
        .call1((logger,))?;
    let level = python_level(record.level);
    if !logger.call_method1("isEnabledFor", (level,))?.is_truthy()? {
        return Ok(());
    }
    let fields = PyDict::new(py);
    for (k, v) in &record.fields {
        fields.set_item(k, to_python(py, v)?)?;
    }
    let extra = PyDict::new(py);
    extra.set_item("mailtide", fields)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("extra", extra)?;
    logger.call_method("log", (level, record.message.as_str()), Some(&kwargs))?;
    Ok(())
}

fn drain(records: &Receiver<Record>, logger: &str, dropped: &AtomicU64) {
    while let Ok(record) = records.recv() {
        Python::attach(|py| {
            // A broken handler must not stop the bridge.
            let _ = emit(py, logger, &record);
            let lost = dropped.swap(0, Ordering::Relaxed);
            if lost > 0 {
                let note = Record {
                    level: Level::WARN,
                    message: format!("mailtide dropped {lost} log events (queue full)"),
                    fields: BTreeMap::new(),
                };
                let _ = emit(py, logger, &note);
            }
        });
    }
}

/// A dispatcher sending this runner's events to Python `logger`.
pub fn dispatch(verbosity: Verbosity, logger: String) -> tracing::Dispatch {
    let filter = match verbosity {
        Verbosity::Off => LevelFilter::OFF,
        Verbosity::Up(level) => LevelFilter::from_level(level),
    };
    let (queue, records) = sync_channel(QUEUE);
    let dropped = Arc::new(AtomicU64::new(0));
    let layer = PyLogLayer {
        queue,
        dropped: dropped.clone(),
    };
    if verbosity != Verbosity::Off {
        // Ends when the runner (and with it the sender) is dropped.
        let spawned = std::thread::Builder::new()
            .name("mailtide-log".into())
            .spawn(move || drain(&records, &logger, &dropped));
        drop(spawned);
    }
    // Only mailtide's own events: its dependencies (sqlx, hyper) log every
    // query and connection, which is noise for the host (and query text).
    let ours = filter_fn(|meta| meta.target().starts_with("mailtide"));
    tracing::Dispatch::new(Registry::default().with(layer.with_filter(filter.and(ours))))
}
