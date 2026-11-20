//! The search's log events: the request kind, cost and duration, never the
//! query, its label ids, the hits or Gmail's error text.
//!
//! In a process of its own: tracing caches each callsite's interest
//! globally, so a subscriber scoped to one test can miss events while tests
//! on other threads register the same callsites.

pub mod search_support;

use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};

use mailtide::ids::LabelId;
use mailtide::search::SearchQuery;
use search_support::{Fixture, query, refusal};

/// Every span and event field, as `name=value` text.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<String>>>);

struct Fields(String);

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let _ = write!(self.0, "{}={value:?} ", field.name());
    }
}

impl Captured {
    fn push(&self, record: impl FnOnce(&mut Fields)) {
        let mut fields = Fields(String::new());
        record(&mut fields);
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(fields.0);
    }

    fn lines(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl tracing::Subscriber for Captured {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        self.push(|fields| span.record(fields));
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        self.push(|fields| values.record(fields));
    }

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        self.push(|fields| {
            let _ = write!(fields.0, "message={} ", event.metadata().name());
            event.record(fields);
        });
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test]
async fn the_query_never_reaches_the_logs() {
    let captured = Captured::default();
    let _guard = tracing::subscriber::set_default(captured.clone());
    let fixture = Fixture::new();
    fixture.gmail().add("m1", &["INBOX"]);
    fixture.sync().await;
    let secret = SearchQuery {
        label_ids: vec![LabelId("Label_1".into())],
        ..query("from:secret@example.com")
    };
    fixture.answer_page(&["msg-secret-1"], None);
    fixture.gmail().answer(Ok(refusal(400, "invalidArgument")));

    fixture.search(&secret).await.expect("a page");
    fixture.search(&secret).await.expect_err("refused");

    let lines = captured.lines();
    let searches: Vec<&String> = lines
        .iter()
        .filter(|line| line.contains("search_messages"))
        .collect();
    assert_eq!(searches.len(), 2, "{lines:#?}");
    assert!(
        searches[0].contains("cost=5") && searches[0].contains("duration_ms="),
        "{searches:?}"
    );
    assert!(searches[1].contains("failure=\"refused\""), "{searches:?}");
    for leak in ["secret", "Label_1", "msg-secret", "Invalid query"] {
        assert!(
            lines.iter().all(|line| !line.contains(leak)),
            "{leak} logged: {lines:#?}"
        );
    }
}
