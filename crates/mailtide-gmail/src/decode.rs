//! Gmail JSON → engine types, parsed once at the boundary.

use base64::Engine as _;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use mailtide::gmail::{
    HistoryPage, HistoryRecord, Label, LabelKind, ListPage, MessageObservation, MessageRef,
};
use mailtide::ids::{HistoryId, LabelId, MessageId, PageToken, ThreadId, Timestamp};
use mailtide::labels::LabelSet;
use mailtide::search::{ListedHits, SearchListing, SearchScope, ThreadHit};
use serde::Deserialize;

/// A response body that isn't what Gmail documents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Not JSON, or not the expected shape.
    InvalidJson(String),
    /// A required field is absent.
    MissingField(&'static str),
    /// A field has an unusable value.
    InvalidField {
        /// The field.
        field: &'static str,
        /// Why.
        reason: String,
    },
}

fn json<'a, T: Deserialize<'a>>(body: &'a [u8]) -> Result<T, DecodeError> {
    serde_json::from_slice(body).map_err(|e| DecodeError::InvalidJson(e.to_string()))
}

/// Gmail sends 64-bit ids as decimal strings; pushes may send numbers.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Number {
    Text(String),
    Int(u64),
}

impl Number {
    fn parse(self, field: &'static str) -> Result<u64, DecodeError> {
        match self {
            Self::Int(n) => Ok(n),
            Self::Text(text) => text.parse().map_err(|_| DecodeError::InvalidField {
                field,
                reason: format!("{text:?} is not a number"),
            }),
        }
    }
}

fn history_id(value: Option<Number>, field: &'static str) -> Result<HistoryId, DecodeError> {
    value
        .ok_or(DecodeError::MissingField(field))?
        .parse(field)
        .map(HistoryId)
}

fn labels(ids: Option<Vec<String>>) -> LabelSet {
    ids.unwrap_or_default().into_iter().map(LabelId).collect()
}

// ------------------------------------------------------------ history

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireMessage {
    id: String,
    thread_id: Option<String>,
    label_ids: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct WireMessageChange {
    message: WireMessage,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireLabelChange {
    message: WireMessage,
    label_ids: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireHistory {
    id: Option<Number>,
    #[serde(default)]
    messages_added: Vec<WireMessageChange>,
    #[serde(default)]
    messages_deleted: Vec<WireMessageChange>,
    #[serde(default)]
    labels_added: Vec<WireLabelChange>,
    #[serde(default)]
    labels_removed: Vec<WireLabelChange>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireHistoryList {
    #[serde(default)]
    history: Vec<WireHistory>,
    history_id: Option<Number>,
    next_page_token: Option<String>,
}

/// `history.list`. Each entry's changes become records at the entry's id, in
/// the order added, labels added, labels removed, deleted.
pub fn history(body: &[u8]) -> Result<HistoryPage, DecodeError> {
    let wire: WireHistoryList = json(body)?;
    let mut records = Vec::new();
    for entry in wire.history {
        let id = history_id(entry.id, "history[].id")?;
        for change in entry.messages_added {
            records.push(HistoryRecord::MessageAdded {
                id,
                message: MessageRef {
                    thread_id: ThreadId(change.message.thread_id.unwrap_or_default()),
                    labels: labels(change.message.label_ids),
                    id: MessageId(change.message.id),
                },
            });
        }
        for change in entry.labels_added {
            records.push(HistoryRecord::LabelsAdded {
                id,
                message_id: MessageId(change.message.id),
                labels: labels(change.label_ids),
            });
        }
        for change in entry.labels_removed {
            records.push(HistoryRecord::LabelsRemoved {
                id,
                message_id: MessageId(change.message.id),
                labels: labels(change.label_ids),
            });
        }
        for change in entry.messages_deleted {
            records.push(HistoryRecord::MessageDeleted {
                id,
                message_id: MessageId(change.message.id),
            });
        }
    }
    Ok(HistoryPage {
        records,
        history_id: history_id(wire.history_id, "historyId")?,
        next_page_token: wire.next_page_token.map(PageToken),
    })
}

// ------------------------------------------------------------ messages

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireListEntry {
    id: String,
    thread_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireList {
    #[serde(default)]
    messages: Vec<WireListEntry>,
    next_page_token: Option<String>,
}

/// `messages.list` (ids only).
pub fn list(body: &[u8]) -> Result<ListPage, DecodeError> {
    let wire: WireList = json(body)?;
    Ok(ListPage {
        messages: wire
            .messages
            .into_iter()
            .map(|m| (MessageId(m.id), ThreadId(m.thread_id)))
            .collect(),
        next_page_token: wire.next_page_token.map(PageToken),
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireFullMessage {
    id: String,
    thread_id: String,
    label_ids: Option<Vec<String>>,
    history_id: Option<Number>,
    internal_date: Option<Number>,
    size_estimate: Option<u64>,
    snippet: Option<String>,
    raw: Option<String>,
}

impl WireFullMessage {
    fn observation(&mut self) -> Result<MessageObservation, DecodeError> {
        Ok(MessageObservation {
            id: MessageId(std::mem::take(&mut self.id)),
            thread_id: ThreadId(std::mem::take(&mut self.thread_id)),
            labels: labels(self.label_ids.take()),
            checkpoint: history_id(self.history_id.take(), "historyId")?,
        })
    }
}

/// `messages.get?format=minimal`; the checkpoint is the message's `historyId`.
pub fn message_minimal(body: &[u8]) -> Result<MessageObservation, DecodeError> {
    let mut wire: WireFullMessage = json(body)?;
    wire.observation()
}

/// A raw message: its observation, `internalDate`, size, snippet, and the
/// decoded RFC 822 bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMessage {
    /// Labels and checkpoint.
    pub observation: MessageObservation,
    /// Gmail's `internalDate`.
    pub internal_date: Timestamp,
    /// Gmail's `sizeEstimate`.
    pub size_estimate: u64,
    /// Gmail's snippet.
    pub snippet: Option<String>,
    /// The base64url-decoded message.
    pub rfc822: Vec<u8>,
}

/// Gmail's base64url, with or without padding.
const BASE64URL: GeneralPurpose = GeneralPurpose::new(
    &alphabet::URL_SAFE,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// `messages.get?format=raw`.
pub fn message_raw(body: &[u8]) -> Result<RawMessage, DecodeError> {
    let mut wire: WireFullMessage = json(body)?;
    let observation = wire.observation()?;
    let raw = wire.raw.take().ok_or(DecodeError::MissingField("raw"))?;
    let rfc822 = BASE64URL
        .decode(raw.as_bytes())
        .map_err(|e| DecodeError::InvalidField {
            field: "raw",
            reason: e.to_string(),
        })?;
    let internal_date = wire
        .internal_date
        .take()
        .ok_or(DecodeError::MissingField("internalDate"))?
        .parse("internalDate")?;
    Ok(RawMessage {
        observation,
        internal_date: Timestamp(i64::try_from(internal_date).unwrap_or(i64::MAX)),
        size_estimate: wire
            .size_estimate
            .unwrap_or_else(|| u64::try_from(rfc822.len()).unwrap_or(u64::MAX)),
        snippet: wire.snippet.take(),
        rfc822,
    })
}

// ------------------------------------------------------------ search

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireThreadEntry {
    id: String,
    history_id: Option<Number>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireSearch {
    #[serde(default)]
    messages: Vec<WireListEntry>,
    #[serde(default)]
    threads: Vec<WireThreadEntry>,
    result_size_estimate: Option<Number>,
    next_page_token: Option<String>,
}

/// A page of `messages.list` or `threads.list` with `q`: the ids in
/// Gmail's order, `resultSizeEstimate` and `nextPageToken`. Gmail sends
/// the estimate on every page, an empty one included (it leaves out the
/// list), so a body without it is not a page and is refused: an answer
/// that is not a page must never read as "no hits". A thread's snippet is
/// ignored; its `historyId` is kept when present.
pub fn search(scope: SearchScope, body: &[u8]) -> Result<SearchListing, DecodeError> {
    let wire: WireSearch = json(body)?;
    let estimate = wire
        .result_size_estimate
        .ok_or(DecodeError::MissingField("resultSizeEstimate"))?
        .parse("resultSizeEstimate")?;
    let hits = match scope {
        SearchScope::Messages => ListedHits::Messages(
            wire.messages
                .into_iter()
                .map(|m| (MessageId(m.id), ThreadId(m.thread_id)))
                .collect(),
        ),
        SearchScope::Threads => ListedHits::Threads(
            wire.threads
                .into_iter()
                .map(|t| {
                    Ok(ThreadHit {
                        thread_id: ThreadId(t.id),
                        history_id: t
                            .history_id
                            .map(|h| h.parse("threads[].historyId").map(HistoryId))
                            .transpose()?,
                    })
                })
                .collect::<Result<_, DecodeError>>()?,
        ),
    };
    Ok(SearchListing {
        hits,
        estimate,
        next_page_token: wire.next_page_token.map(PageToken),
    })
}

// ------------------------------------------------------------ account

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireProfile {
    history_id: Option<Number>,
}

/// `getProfile`'s history id.
pub fn profile(body: &[u8]) -> Result<HistoryId, DecodeError> {
    let wire: WireProfile = json(body)?;
    history_id(wire.history_id, "historyId")
}

#[derive(Deserialize)]
struct WireLabel {
    id: String,
    name: String,
    #[serde(rename = "type")]
    kind: Option<String>,
}

#[derive(Deserialize)]
struct WireLabels {
    #[serde(default)]
    labels: Vec<WireLabel>,
}

fn label(wire: WireLabel) -> Label {
    Label {
        kind: match wire.kind.as_deref() {
            Some("system") => LabelKind::System,
            Some(_) | None => LabelKind::User,
        },
        id: LabelId(wire.id),
        name: wire.name,
    }
}

/// `labels.get`: one label's definition.
pub fn label_get(body: &[u8]) -> Result<Label, DecodeError> {
    json::<WireLabel>(body).map(label)
}

/// `labels.list`.
pub fn labels_list(body: &[u8]) -> Result<Vec<Label>, DecodeError> {
    let wire: WireLabels = json(body)?;
    Ok(wire.labels.into_iter().map(label).collect())
}

#[derive(Deserialize)]
struct WireWatch {
    expiration: Option<Number>,
}

/// `users.watch`'s expiration.
pub fn watch(body: &[u8]) -> Result<Timestamp, DecodeError> {
    let wire: WireWatch = json(body)?;
    let expiration = wire
        .expiration
        .ok_or(DecodeError::MissingField("expiration"))?
        .parse("expiration")?;
    Ok(Timestamp(i64::try_from(expiration).unwrap_or(i64::MAX)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mailtide::labels::set;

    #[test]
    fn history_entries_become_ordered_records() {
        let body = br#"{
          "history": [
            {"id": "101", "messages": [{"id": "m1", "threadId": "t1"}],
             "messagesAdded": [{"message": {"id": "m1", "threadId": "t1", "labelIds": ["INBOX", "UNREAD"]}}]},
            {"id": "102", "labelsRemoved": [{"message": {"id": "m1", "threadId": "t1"}, "labelIds": ["UNREAD"]}]},
            {"id": "103", "messagesDeleted": [{"message": {"id": "m0", "threadId": "t0"}}]}
          ],
          "historyId": "105",
          "nextPageToken": "abc"
        }"#;
        let page = history(body).expect("decodes");
        assert_eq!(page.history_id, HistoryId(105));
        assert_eq!(page.next_page_token, Some(PageToken("abc".into())));
        assert_eq!(page.records.len(), 3);
        assert!(matches!(
            &page.records[0],
            HistoryRecord::MessageAdded { id: HistoryId(101), message } if message.labels == set(["INBOX", "UNREAD"])
        ));
        assert!(matches!(
            &page.records[1],
            HistoryRecord::LabelsRemoved {
                id: HistoryId(102),
                ..
            }
        ));
        assert!(matches!(
            &page.records[2],
            HistoryRecord::MessageDeleted {
                id: HistoryId(103),
                ..
            }
        ));
    }

    #[test]
    fn an_empty_history_page_still_carries_the_history_id() {
        let page = history(br#"{"historyId": "9"}"#).expect("decodes");
        assert!(page.records.is_empty());
        assert_eq!(page.history_id, HistoryId(9));
    }

    #[test]
    fn minimal_messages_use_their_own_history_id_as_checkpoint() {
        let message = message_minimal(br#"{"id":"m1","threadId":"t1","labelIds":["INBOX"],"historyId":"77","sizeEstimate":10}"#)
            .expect("decodes");
        assert_eq!(message.checkpoint, HistoryId(77));
        assert_eq!(message.labels, set(["INBOX"]));
    }

    #[test]
    fn messages_without_labels_have_an_empty_set() {
        let message =
            message_minimal(br#"{"id":"m1","threadId":"t1","historyId":"5"}"#).expect("decodes");
        assert!(message.labels.is_empty());
    }

    #[test]
    fn raw_messages_decode_base64url_with_or_without_padding() {
        let encoded = BASE64URL.encode(b"Subject: hi\r\n\r\nbody?>");
        let body = format!(
            r#"{{"id":"m1","threadId":"t1","historyId":"5","internalDate":"1700000000000","sizeEstimate":20,"raw":"{encoded}"}}"#
        );
        let raw = message_raw(body.as_bytes()).expect("decodes");
        assert_eq!(raw.rfc822, b"Subject: hi\r\n\r\nbody?>");
        assert_eq!(raw.internal_date, Timestamp(1_700_000_000_000));
    }

    #[test]
    fn missing_fields_are_named() {
        assert_eq!(
            message_minimal(br#"{"id":"m1","threadId":"t1"}"#),
            Err(DecodeError::MissingField("historyId"))
        );
        assert!(matches!(
            profile(b"not json"),
            Err(DecodeError::InvalidJson(_))
        ));
    }

    #[test]
    fn profile_labels_list_and_watch() {
        assert_eq!(
            profile(br#"{"emailAddress":"a@b.c","historyId":"123"}"#),
            Ok(HistoryId(123))
        );
        let labels = labels_list(br#"{"labels":[{"id":"INBOX","name":"INBOX","type":"system"},{"id":"Label_1","name":"Clients","type":"user"}]}"#)
            .expect("decodes");
        assert_eq!(labels[1].kind, LabelKind::User);
        let fetched =
            label_get(br#"{"id":"Label_1","name":"Clients","type":"user","messagesTotal":3}"#)
                .expect("decodes");
        assert_eq!(
            (fetched.id.0.as_str(), fetched.kind),
            ("Label_1", LabelKind::User)
        );
        assert!(label_get(br#"{"error":{"code":404}}"#).is_err());
        assert_eq!(
            watch(br#"{"historyId":"1","expiration":"1700000000000"}"#),
            Ok(Timestamp(1_700_000_000_000))
        );
    }

    #[test]
    fn a_thread_search_page_decodes_ids_and_history_ids() {
        let page = search(
            SearchScope::Threads,
            br#"{"threads":[
                {"id":"t1","snippet":"Hello there","historyId":"812"},
                {"id":"t2","snippet":"x"}
              ],
              "nextPageToken":"n2","resultSizeEstimate":201}"#,
        )
        .expect("decodes");
        assert_eq!(
            page.hits,
            ListedHits::Threads(vec![
                ThreadHit {
                    thread_id: ThreadId("t1".into()),
                    history_id: Some(HistoryId(812)),
                },
                ThreadHit {
                    thread_id: ThreadId("t2".into()),
                    history_id: None,
                },
            ])
        );
        assert_eq!(page.estimate, 201);
        assert_eq!(page.next_page_token, Some(PageToken("n2".into())));
    }

    #[test]
    fn a_search_answer_that_is_not_a_page_is_refused() {
        for body in [
            &b"not json"[..],
            b"[]",
            b"{}",
            br#"{"messages":[]}"#,
            br#"{"error":{"code":400,"message":"Invalid"}}"#,
            br#"{"messages":[{"id":"m1"}],"resultSizeEstimate":1}"#,
            br#"{"resultSizeEstimate":"many"}"#,
        ] {
            assert!(
                search(SearchScope::Messages, body).is_err(),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
        assert!(
            search(
                SearchScope::Threads,
                br#"{"threads":[{"id":"t1","historyId":"x"}],"resultSizeEstimate":1}"#
            )
            .is_err()
        );
        let empty = search(SearchScope::Messages, br#"{"resultSizeEstimate":0}"#)
            .expect("an empty page is a page");
        assert_eq!(empty.hits, ListedHits::Messages(Vec::new()));
    }
}
