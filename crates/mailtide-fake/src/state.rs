//! Multi-account fake Gmail state, Gmail semantics, query matching, and fault configuration.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STD;
use mailtide_mime::{Address, Attachment, ComposeOptions, OutgoingMessage, Purpose};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Default account used when a request presents `Bearer test-token`.
pub const DEFAULT_ACCOUNT: &str = "alice@example.com";

/// Prefix for multi-account fake bearer tokens (`Authorization: Bearer fake:<email>`).
pub const TOKEN_PREFIX: &str = "fake:";

const MAX_REQUEST_LOG: usize = 500;

/// System labels created on every new account.
const SYSTEM_LABELS: &[&str] = &[
    "INBOX",
    "UNREAD",
    "SENT",
    "DRAFT",
    "STARRED",
    "IMPORTANT",
    "TRASH",
    "SPAM",
    "CATEGORY_PERSONAL",
    "CATEGORY_SOCIAL",
    "CATEGORY_PROMOTIONS",
    "CATEGORY_UPDATES",
    "CATEGORY_FORUMS",
];

/// Current wall-clock time in milliseconds since the Unix epoch.
#[must_use]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Extract the account email from an `Authorization` header value.
#[must_use]
pub fn account_from_auth(header: Option<&str>) -> Option<String> {
    let raw = header?.trim();
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))?
        .trim();
    if token.is_empty() {
        return None;
    }
    if let Some(email) = token.strip_prefix(TOKEN_PREFIX) {
        let normalized = email.trim().to_ascii_lowercase();
        return (!normalized.is_empty()).then_some(normalized);
    }
    if token == "test-token" {
        return Some(DEFAULT_ACCOUNT.to_owned());
    }
    // Accept bare email-shaped tokens for convenience when testing manually.
    if token.contains('@') {
        return Some(token.to_ascii_lowercase());
    }
    None
}

/// A stored Gmail label definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelDef {
    /// Gmail label id (e.g. `INBOX` or `Label_1`).
    pub id: String,
    /// Display name (supports `/` nesting).
    pub name: String,
    /// `"system"` or `"user"`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Optional `labelListVisibility`.
    #[serde(
        rename = "labelListVisibility",
        skip_serializing_if = "Option::is_none"
    )]
    pub list_visibility: Option<String>,
    /// Optional `messageListVisibility`.
    #[serde(
        rename = "messageListVisibility",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_visibility: Option<String>,
    /// Optional Gmail palette color `{textColor, backgroundColor}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<Value>,
}

/// A stored RFC 5322 message with Gmail metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    /// Gmail message id.
    pub id: String,
    /// Gmail thread id.
    pub thread_id: String,
    /// Labels currently on this message.
    pub labels: Vec<String>,
    /// History id that last modified this message.
    pub history_id: u64,
    /// Gmail `internalDate` in Unix milliseconds.
    pub internal_date_ms: i64,
    /// Short preview snippet derived from the body.
    pub snippet: String,
    /// Raw RFC 5322 bytes.
    #[serde(skip)]
    pub raw: Vec<u8>,
    /// Derived metadata (recomputed on load).
    pub from: String,
    /// Derived `To` header.
    pub to: Vec<String>,
    /// Derived `Cc` header.
    pub cc: Vec<String>,
    /// Decoded `Subject` header.
    pub subject: String,
    /// `Message-ID` header value.
    pub rfc822_message_id: String,
    /// Whether the message has non-body attachments.
    pub has_attachment: bool,
    /// Plain text searchable cache.
    #[serde(skip)]
    pub search_text: String,
}

/// One Gmail history entry stored in the account log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEvent {
    /// Monotonic history id.
    pub id: u64,
    /// Wire JSON payload (`messagesAdded`, `messagesDeleted`, `labelsAdded`, `labelsRemoved`).
    pub record: Value,
}

/// A sent message recorded for inspection via `/_control/accounts/{email}/sent`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentRecord {
    /// Sequence number within the account.
    pub seq: u64,
    /// Resulting Gmail message id.
    pub message_id: String,
    /// Resulting Gmail thread id.
    pub thread_id: String,
    /// How it was sent (`messages.send`, `drafts.send`, `web.send`).
    pub via: String,
    /// Timestamp in milliseconds.
    pub sent_at_ms: i64,
    /// Derived sender.
    pub from: String,
    /// Derived `To` recipients.
    pub to: Vec<String>,
    /// Derived `Subject`.
    pub subject: String,
    /// Derived snippet.
    pub snippet: String,
    /// Optional `X-Mailtide-Id` tracking header.
    pub tracking_id: Option<String>,
}

/// A structured Gmail API error fault.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RefusalFault {
    /// HTTP status code (e.g. `429`, `403`, `401`, `503`).
    pub status: u16,
    /// Gmail error reason (e.g. `userRateLimitExceeded`, `domainPolicy`, `backendError`).
    pub reason: String,
    /// Human-readable message.
    #[serde(default = "default_refusal_message")]
    pub message: String,
    /// Optional `Retry-After` header in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_s: Option<u64>,
    /// Optional countdown; `None` means active until cleared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining: Option<u32>,
}

fn default_refusal_message() -> String {
    "Injected fake Gmail refusal".to_owned()
}

/// How to drop the next send response (`messages.send` or `drafts.send`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DropSendMode {
    /// Store the sent message in Gmail, then drop the TCP connection before answering.
    Applied,
    /// Drop the TCP connection before storing the sent message.
    NotApplied,
}

/// Per-account fault injection configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AccountFaults {
    /// Base artificial latency for Gmail API requests in milliseconds.
    #[serde(default)]
    pub delay_ms: u64,
    /// Additional uniform jitter up to this many milliseconds.
    #[serde(default)]
    pub jitter_ms: u64,
    /// Extra delay specifically on `messages.get?format=raw` (tests body-syncing / prioritise).
    #[serde(default)]
    pub raw_delay_ms: u64,
    /// Message ids whose `format=raw` fetch returns HTTP 503.
    #[serde(default)]
    pub withheld_messages: BTreeSet<String>,
    /// Automatically withhold `format=raw` for the next N newly added messages.
    #[serde(default)]
    pub withhold_next_raw: u32,
    /// Blanket refusal applied to Gmail API requests on this account.
    #[serde(default)]
    pub refuse: Option<RefusalFault>,
    /// Refusal applied to `messages.send` and `drafts.send`.
    #[serde(default)]
    pub send_refusal: Option<RefusalFault>,
    /// One-shot dropped connection on the next send call.
    #[serde(default)]
    pub drop_next_send: Option<DropSendMode>,
    /// Refusal applied to `messages.delete` and `messages.batchDelete`.
    #[serde(default)]
    pub delete_failure: Option<RefusalFault>,
    /// Whether the token has `https://mail.google.com/` scope (needed for permanent deletes).
    #[serde(default = "default_true")]
    pub full_scope: bool,
    /// Suppress push webhook notifications for this account.
    #[serde(default)]
    pub push_paused: bool,
}

fn default_true() -> bool {
    true
}

impl AccountFaults {
    /// Fresh defaults (no active faults, full scope enabled).
    #[must_use]
    pub fn clean() -> Self {
        Self {
            full_scope: true,
            ..Self::default()
        }
    }
}

/// Global fault injection configuration applied across all accounts.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalFaults {
    /// Base latency added to every Gmail API request.
    #[serde(default)]
    pub delay_ms: u64,
    /// Uniform jitter added to every Gmail API request.
    #[serde(default)]
    pub jitter_ms: u64,
    /// Suppress push webhooks globally.
    #[serde(default)]
    pub push_paused: bool,
}

/// One logged Gmail HTTP request for inspection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestLogEntry {
    /// Timestamp in milliseconds.
    pub at_ms: i64,
    /// Resolved account email (if authenticated).
    pub account: Option<String>,
    /// HTTP method.
    pub method: String,
    /// Path and query string.
    pub path: String,
    /// Response HTTP status (`0` when connection dropped).
    pub status: u16,
    /// Duration in milliseconds.
    pub duration_ms: u64,
}

/// Summary of an account returned by `/_control/status` and `/_control/accounts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSummary {
    /// Account email.
    pub email: String,
    /// Current `historyId`.
    pub history_id: u64,
    /// Minimum valid `startHistoryId`.
    pub history_floor: u64,
    /// Number of stored messages.
    pub message_count: usize,
    /// Number of distinct threads.
    pub thread_count: usize,
    /// Number of active drafts.
    pub draft_count: usize,
    /// Number of labels.
    pub label_count: usize,
    /// Number of sent messages logged.
    pub sent_count: usize,
    /// Active faults on this account.
    pub faults: AccountFaults,
    /// Whether a background burst is currently running.
    pub burst_active: bool,
}

/// Serialized metadata persisted in `fake_accounts.state_json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AccountMeta {
    pub history_id: u64,
    pub history_floor: u64,
    pub next_seq: u64,
    pub next_label_seq: u64,
    pub next_sent_seq: u64,
    pub labels: BTreeMap<String, LabelDef>,
    pub faults: AccountFaults,
    pub search_overrides: BTreeMap<String, Vec<String>>,
    pub watch_topic: Option<String>,
}

/// Complete in-memory state of one Gmail account.
#[derive(Debug, Clone)]
pub struct AccountState {
    /// Normalized lowercase account email.
    pub email: String,
    /// Monotonic Gmail history id.
    pub history_id: u64,
    /// History ids strictly below this value answer 404.
    pub history_floor: u64,
    /// Sequence counter for message/draft ids.
    pub next_seq: u64,
    /// Sequence counter for user label ids.
    pub next_label_seq: u64,
    /// Sequence counter for sent records.
    pub next_sent_seq: u64,
    /// Stored messages keyed by Gmail message id.
    pub messages: BTreeMap<String, StoredMessage>,
    /// Active drafts: `draft_id -> current_message_id`.
    pub drafts: BTreeMap<String, String>,
    /// Labels keyed by Gmail label id.
    pub labels: BTreeMap<String, LabelDef>,
    /// History records ordered oldest first.
    pub history: Vec<HistoryEvent>,
    /// Sent message log.
    pub sent_log: Vec<SentRecord>,
    /// Active faults on this account.
    pub faults: AccountFaults,
    /// Optional explicit query overrides (`q -> [message_id, ...]`).
    pub search_overrides: BTreeMap<String, Vec<String>>,
    /// Topic registered by `users.watch`, if any.
    pub watch_topic: Option<String>,
}

impl AccountState {
    /// Create a fresh account with standard Gmail system labels.
    #[must_use]
    pub fn new(email: String) -> Self {
        let mut labels = BTreeMap::new();
        for &lid in SYSTEM_LABELS {
            labels.insert(
                lid.to_owned(),
                LabelDef {
                    id: lid.to_owned(),
                    name: lid.to_owned(),
                    kind: "system".to_owned(),
                    list_visibility: None,
                    message_visibility: None,
                    color: None,
                },
            );
        }
        Self {
            email,
            history_id: 1,
            history_floor: 0,
            next_seq: 0,
            next_label_seq: 0,
            next_sent_seq: 0,
            messages: BTreeMap::new(),
            drafts: BTreeMap::new(),
            labels,
            history: Vec::new(),
            sent_log: Vec::new(),
            faults: AccountFaults::clean(),
            search_overrides: BTreeMap::new(),
            watch_topic: None,
        }
    }

    pub(crate) fn meta(&self) -> AccountMeta {
        AccountMeta {
            history_id: self.history_id,
            history_floor: self.history_floor,
            next_seq: self.next_seq,
            next_label_seq: self.next_label_seq,
            next_sent_seq: self.next_sent_seq,
            labels: self.labels.clone(),
            faults: self.faults.clone(),
            search_overrides: self.search_overrides.clone(),
            watch_topic: self.watch_topic.clone(),
        }
    }

    pub(crate) fn from_meta(email: String, meta: AccountMeta) -> Self {
        Self {
            email,
            history_id: meta.history_id,
            history_floor: meta.history_floor,
            next_seq: meta.next_seq,
            next_label_seq: meta.next_label_seq,
            next_sent_seq: meta.next_sent_seq,
            messages: BTreeMap::new(),
            drafts: BTreeMap::new(),
            labels: meta.labels,
            history: Vec::new(),
            sent_log: Vec::new(),
            faults: meta.faults,
            search_overrides: meta.search_overrides,
            watch_topic: meta.watch_topic,
        }
    }

    /// Summary statistics for control APIs and UI.
    #[must_use]
    pub fn summary(&self, burst_active: bool) -> AccountSummary {
        let threads: BTreeSet<&str> = self
            .messages
            .values()
            .map(|m| m.thread_id.as_str())
            .collect();
        AccountSummary {
            email: self.email.clone(),
            history_id: self.history_id,
            history_floor: self.history_floor,
            message_count: self.messages.len(),
            thread_count: threads.len(),
            draft_count: self.drafts.len(),
            label_count: self.labels.len(),
            sent_count: self.sent_log.len(),
            faults: self.faults.clone(),
            burst_active,
        }
    }

    fn next_id(&mut self, prefix: &str) -> String {
        self.next_seq += 1;
        format!("{prefix}{:06x}", self.next_seq)
    }

    fn record_history(&mut self, mut record: Value) -> HistoryEvent {
        self.history_id += 1;
        record["id"] = json!(self.history_id.to_string());
        let event = HistoryEvent {
            id: self.history_id,
            record,
        };
        self.history.push(event.clone());
        event
    }

    /// Add a message directly (external delivery or seed).
    pub fn add_message(
        &mut self,
        explicit_id: Option<String>,
        thread_id: Option<String>,
        labels: Vec<String>,
        date_ms: Option<i64>,
        raw: Vec<u8>,
    ) -> (StoredMessage, HistoryEvent) {
        let id = explicit_id.unwrap_or_else(|| self.next_id("msg-"));
        let thread_id = thread_id.unwrap_or_else(|| format!("thr-{id}"));
        let dedup_labels = dedup_vec(labels);
        let parsed = derive_metadata(&raw, date_ms);
        let event = self.record_history(json!({
            "messagesAdded": [{
                "message": {
                    "id": id,
                    "threadId": thread_id,
                    "labelIds": dedup_labels,
                }
            }]
        }));
        let msg = StoredMessage {
            id: id.clone(),
            thread_id,
            labels: dedup_labels,
            history_id: self.history_id,
            internal_date_ms: parsed.internal_date_ms,
            snippet: parsed.snippet,
            raw,
            from: parsed.from,
            to: parsed.to,
            cc: parsed.cc,
            subject: parsed.subject,
            rfc822_message_id: parsed.rfc822_message_id,
            has_attachment: parsed.has_attachment,
            search_text: parsed.search_text,
        };
        self.messages.insert(id.clone(), msg.clone());
        if self.faults.withhold_next_raw > 0 {
            self.faults.withhold_next_raw -= 1;
            self.faults.withheld_messages.insert(id);
        }
        (msg, event)
    }

    /// Store a Gmail-processed message (replaces `Message-ID`; optionally strips `X-Mailtide-Id`
    /// to emulate Gmail Web).
    pub fn store_stamped(
        &mut self,
        raw: &[u8],
        labels: Vec<String>,
        thread_id: Option<String>,
        strip_tracking: bool,
    ) -> (StoredMessage, HistoryEvent) {
        self.next_seq += 1;
        let seq = self.next_seq;
        let id = format!("msg-{seq:06x}");
        let stamped = stamp_gmail_headers(raw, seq, strip_tracking);
        self.add_message(Some(id), thread_id, labels, Some(now_ms()), stamped)
    }

    /// Create a draft (returns `draft_id`, stored message, history event).
    pub fn create_draft(
        &mut self,
        raw: &[u8],
        thread_id: Option<String>,
        web_client: bool,
    ) -> (String, StoredMessage, HistoryEvent) {
        let (msg, event) = self.store_stamped(raw, vec!["DRAFT".to_owned()], thread_id, web_client);
        let draft_id = format!("r-{:06x}", self.next_seq);
        self.drafts.insert(draft_id.clone(), msg.id.clone());
        (draft_id, msg, event)
    }

    /// Update an existing draft (replaces its message id, deletes old draft message).
    pub fn update_draft(
        &mut self,
        draft_id: &str,
        raw: &[u8],
        thread_id: Option<String>,
        web_client: bool,
    ) -> Option<(String, StoredMessage, Vec<HistoryEvent>)> {
        let old_mid = self.drafts.get(draft_id)?.clone();
        let mut events = Vec::new();
        if let Some(del_ev) = self.delete_message(&old_mid) {
            events.push(del_ev);
        }
        let (msg, add_ev) =
            self.store_stamped(raw, vec!["DRAFT".to_owned()], thread_id, web_client);
        events.push(add_ev);
        self.drafts.insert(draft_id.to_owned(), msg.id.clone());
        Some((old_mid, msg, events))
    }

    /// Delete a draft permanently.
    pub fn delete_draft(&mut self, draft_id: &str) -> Option<(String, Option<HistoryEvent>)> {
        let old_mid = self.drafts.remove(draft_id)?;
        let ev = self.delete_message(&old_mid);
        Some((old_mid, ev))
    }

    /// Send an existing draft (consumes draft, adds new `SENT` message in same thread).
    pub fn send_draft(
        &mut self,
        draft_id: &str,
        web_client: bool,
    ) -> Option<(String, StoredMessage, Vec<HistoryEvent>, SentRecord)> {
        let old_mid = self.drafts.remove(draft_id)?;
        let old = self.messages.get(&old_mid)?.clone();
        let mut events = Vec::new();
        if let Some(del_ev) = self.delete_message(&old_mid) {
            events.push(del_ev);
        }
        let (sent_msg, add_ev) = self.store_stamped(
            &old.raw,
            vec!["SENT".to_owned()],
            Some(old.thread_id),
            web_client,
        );
        events.push(add_ev);
        let via = if web_client {
            "web.send"
        } else {
            "drafts.send"
        };
        let record = self.record_sent(&sent_msg, via);
        Some((old_mid, sent_msg, events, record))
    }

    /// Record a sent message in the account's sent log.
    pub fn record_sent(&mut self, msg: &StoredMessage, via: &str) -> SentRecord {
        self.next_sent_seq += 1;
        let record = SentRecord {
            seq: self.next_sent_seq,
            message_id: msg.id.clone(),
            thread_id: msg.thread_id.clone(),
            via: via.to_owned(),
            sent_at_ms: msg.internal_date_ms,
            from: msg.from.clone(),
            to: msg.to.clone(),
            subject: msg.subject.clone(),
            snippet: msg.snippet.clone(),
            tracking_id: mailtide_mime::tracking_id(&msg.raw),
        };
        self.sent_log.push(record.clone());
        record
    }

    /// Permanently delete a message and append a `messagesDeleted` history record.
    pub fn delete_message(&mut self, mid: &str) -> Option<HistoryEvent> {
        let removed = self.messages.remove(mid)?;
        self.faults.withheld_messages.remove(mid);
        let event = self.record_history(json!({
            "messagesDeleted": [{
                "message": {
                    "id": mid,
                    "threadId": removed.thread_id,
                    "labelIds": removed.labels,
                }
            }]
        }));
        Some(event)
    }

    /// Modify labels on a message and emit `labelsAdded` / `labelsRemoved` history records.
    pub fn modify_labels(
        &mut self,
        mid: &str,
        add: &[String],
        remove: &[String],
    ) -> Option<(StoredMessage, Vec<HistoryEvent>)> {
        let current = self.messages.get(mid)?.clone();
        let added: Vec<String> = dedup_vec(
            add.iter()
                .filter(|l| !current.labels.contains(l))
                .cloned()
                .collect(),
        );
        let removed: Vec<String> = dedup_vec(
            remove
                .iter()
                .filter(|l| current.labels.contains(l))
                .cloned()
                .collect(),
        );
        let mut events = Vec::new();
        let ref_obj = json!({ "id": mid, "threadId": current.thread_id });
        if !added.is_empty() {
            events.push(self.record_history(json!({
                "labelsAdded": [{ "message": ref_obj, "labelIds": added }]
            })));
        }
        if !removed.is_empty() {
            events.push(self.record_history(json!({
                "labelsRemoved": [{ "message": ref_obj, "labelIds": removed }]
            })));
        }
        let msg = self.messages.get_mut(mid)?;
        for label in &added {
            if !msg.labels.contains(label) {
                msg.labels.push(label.clone());
            }
        }
        msg.labels.retain(|l| !removed.contains(l));
        msg.history_id = self.history_id;
        Some((msg.clone(), events))
    }

    /// Whether a label id is defined on this account.
    #[must_use]
    pub fn defines_label(&self, lid: &str) -> bool {
        self.labels.contains_key(lid) || !lid.starts_with("Label_")
    }

    /// Existing label id holding `name` (case-insensitive, trimmed), excluding `other_than`.
    #[must_use]
    pub fn label_holder(&self, name: &str, other_than: Option<&str>) -> Option<String> {
        let wanted = name.trim().to_ascii_lowercase();
        self.labels
            .iter()
            .find(|(lid, def)| {
                Some(lid.as_str()) != other_than && def.name.trim().to_ascii_lowercase() == wanted
            })
            .map(|(lid, _)| lid.clone())
    }

    /// Create a user label (returns its id).
    pub fn create_label(
        &mut self,
        name: String,
        list_visibility: Option<String>,
        message_visibility: Option<String>,
        color: Option<Value>,
    ) -> LabelDef {
        loop {
            self.next_label_seq += 1;
            let lid = format!("Label_{}", self.next_label_seq);
            if let std::collections::btree_map::Entry::Vacant(entry) =
                self.labels.entry(lid.clone())
            {
                let def = LabelDef {
                    id: lid,
                    name,
                    kind: "user".to_owned(),
                    list_visibility: list_visibility.or_else(|| Some("labelShow".to_owned())),
                    message_visibility: message_visibility.or_else(|| Some("show".to_owned())),
                    color,
                };
                entry.insert(def.clone());
                return def;
            }
        }
    }

    /// Ensure a user label with `name` exists (idempotent), returning its id.
    pub fn ensure_label(&mut self, name: &str, color: Option<Value>) -> String {
        if let Some(lid) = self.label_holder(name, None) {
            return lid;
        }
        self.create_label(name.to_owned(), None, None, color).id
    }

    /// Delete a label and strip it from all messages, emitting `labelsRemoved` history events.
    pub fn delete_label(&mut self, lid: &str) -> Option<Vec<(StoredMessage, Vec<HistoryEvent>)>> {
        self.labels.remove(lid)?;
        let affected: Vec<String> = self
            .messages
            .iter()
            .filter(|(_, m)| m.labels.iter().any(|l| l == lid))
            .map(|(mid, _)| mid.clone())
            .collect();
        let mut updates = Vec::new();
        for mid in affected {
            if let Some(updated) = self.modify_labels(&mid, &[], &[lid.to_owned()]) {
                updates.push(updated);
            }
        }
        Some(updates)
    }

    /// Expire history so any prior `startHistoryId` returns 404.
    pub fn expire_history(&mut self) {
        self.history_id += 1;
        self.history_floor = self.history_id;
    }

    /// Find matching messages for a Gmail `q=` search query (newest first).
    #[must_use]
    pub fn search_messages(
        &self,
        q: &str,
        label_ids: &[String],
        include_spam_trash: bool,
    ) -> Vec<&StoredMessage> {
        if let Some(scripted) = self.search_overrides.get(q) {
            return scripted
                .iter()
                .filter_map(|id| self.messages.get(id))
                .collect();
        }
        let tokens = tokenize_query(q);
        let include_all = include_spam_trash
            || tokens.iter().any(|t| {
                matches!(
                    t.as_str(),
                    "in:anywhere" | "in:trash" | "in:spam" | "label:trash" | "label:spam"
                )
            });
        let mut matches: Vec<&StoredMessage> = self
            .messages
            .values()
            .filter(|msg| {
                if !include_all && msg.labels.iter().any(|l| l == "TRASH" || l == "SPAM") {
                    return false;
                }
                if !label_ids.is_empty() && !label_ids.iter().all(|l| msg.labels.contains(l)) {
                    return false;
                }
                tokens
                    .iter()
                    .all(|tok| matches_token(msg, tok, &self.labels))
            })
            .collect();
        matches.sort_by(|a, b| {
            b.internal_date_ms
                .cmp(&a.internal_date_ms)
                .then_with(|| b.id.cmp(&a.id))
        });
        matches
    }
}

/// Top-level shared state of the fake Gmail server.
#[derive(Debug, Default)]
pub struct FakeGmailState {
    /// Accounts keyed by normalized lowercase email.
    pub accounts: BTreeMap<String, AccountState>,
    /// Global fault configuration.
    pub global_faults: GlobalFaults,
    /// Recent Gmail API request log.
    pub request_log: VecDeque<RequestLogEntry>,
}

impl FakeGmailState {
    /// Ensure an account exists and return a mutable reference.
    pub fn ensure_account(&mut self, email: &str) -> (&mut AccountState, bool) {
        let key = email.trim().to_ascii_lowercase();
        let created = !self.accounts.contains_key(&key);
        let entry = self
            .accounts
            .entry(key.clone())
            .or_insert_with(|| AccountState::new(key));
        (entry, created)
    }

    /// Append a request entry to the bounded ring buffer.
    pub fn log_request(&mut self, entry: RequestLogEntry) {
        if self.request_log.len() >= MAX_REQUEST_LOG {
            self.request_log.pop_front();
        }
        self.request_log.push_back(entry);
    }
}

/// Attachment payload for `/_control/accounts/{email}/messages`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InjectAttachment {
    /// Filename (optional for inline parts).
    #[serde(default)]
    pub filename: Option<String>,
    /// MIME media type (e.g. `application/pdf`, `image/png`).
    pub content_type: String,
    /// Base64-encoded bytes.
    pub data_base64: String,
    /// Optional `Content-ID` for inline HTML images.
    #[serde(default)]
    pub content_id: Option<String>,
}

/// Payload accepted by `/_control/accounts/{email}/messages` (either raw or structured).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InjectMessage {
    /// Optional explicit message id.
    #[serde(default)]
    pub id: Option<String>,
    /// Optional thread id to append to.
    #[serde(default)]
    pub thread_id: Option<String>,
    /// Labels to assign (defaults to `["INBOX", "UNREAD"]`).
    #[serde(default)]
    pub labels: Option<Vec<String>>,
    /// Explicit timestamp in Unix milliseconds.
    #[serde(default)]
    pub date_ms: Option<i64>,
    /// Raw RFC 822 text directly.
    #[serde(default)]
    pub raw: Option<String>,
    /// Raw RFC 822 bytes encoded as standard base64.
    #[serde(default)]
    pub raw_base64: Option<String>,
    /// Sender address (`"Alice <alice@example.com>"` or `"alice@example.com"`).
    #[serde(default)]
    pub from: Option<String>,
    /// Recipients (`To`).
    #[serde(default)]
    pub to: Vec<String>,
    /// Carbon copy (`Cc`).
    #[serde(default)]
    pub cc: Vec<String>,
    /// Blind carbon copy (`Bcc`).
    #[serde(default)]
    pub bcc: Vec<String>,
    /// Subject line.
    #[serde(default)]
    pub subject: Option<String>,
    /// Plain-text body.
    #[serde(default)]
    pub text: Option<String>,
    /// HTML body.
    #[serde(default)]
    pub html: Option<String>,
    /// Optional `In-Reply-To` Message-ID.
    #[serde(default)]
    pub in_reply_to: Option<String>,
    /// Optional `References` Message-IDs.
    #[serde(default)]
    pub references: Vec<String>,
    /// Optional `X-Mailtide-Id`.
    #[serde(default)]
    pub tracking_id: Option<String>,
    /// Optional extra headers (`[name, value]`).
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// Optional attachments / inline parts.
    #[serde(default)]
    pub attachments: Vec<InjectAttachment>,
}

/// Build raw RFC 5322 bytes from an [`InjectMessage`].
pub fn build_raw_message(account_email: &str, input: &InjectMessage) -> Result<Vec<u8>, String> {
    if let Some(raw_b64) = &input.raw_base64 {
        return BASE64_STD
            .decode(raw_b64.trim())
            .map_err(|e| format!("invalid raw_base64: {e}"));
    }
    if let Some(raw) = &input.raw {
        return Ok(normalize_crlf(raw).into_bytes());
    }
    let from = parse_single_address(input.from.as_deref().unwrap_or("sender@example.com"));
    let sent_at_s = input.date_ms.unwrap_or_else(now_ms).div_euclid(1000);
    let mut outgoing = OutgoingMessage::new(from, sent_at_s);
    if input.to.is_empty() {
        outgoing.to.push(parse_single_address(account_email));
    } else {
        outgoing.to = input.to.iter().map(|s| parse_single_address(s)).collect();
    }
    outgoing.cc = input.cc.iter().map(|s| parse_single_address(s)).collect();
    outgoing.bcc = input.bcc.iter().map(|s| parse_single_address(s)).collect();
    outgoing.subject = Some(
        input
            .subject
            .clone()
            .unwrap_or_else(|| "(no subject)".to_owned()),
    );
    outgoing.text.clone_from(&input.text);
    outgoing.html.clone_from(&input.html);
    if outgoing.text.is_none() && outgoing.html.is_none() {
        outgoing.text = Some(String::new());
    }
    if let Some(irt) = input.in_reply_to.as_deref()
        && let Ok(parsed) = mailtide_mime::MessageId::parse(irt)
    {
        outgoing.in_reply_to = Some(parsed);
    }
    for r in &input.references {
        if let Ok(parsed) = mailtide_mime::MessageId::parse(r) {
            outgoing.references.push(parsed);
        }
    }
    outgoing.tracking_id.clone_from(&input.tracking_id);
    outgoing.headers.clone_from(&input.headers);
    for att in &input.attachments {
        let bytes = BASE64_STD
            .decode(att.data_base64.trim())
            .map_err(|e| format!("invalid attachment base64: {e}"))?;
        let part = Attachment {
            filename: att.filename.clone(),
            content_type: att.content_type.clone(),
            content_id: att.content_id.clone(),
            bytes,
        };
        if part.content_id.is_some() && outgoing.html.is_some() {
            outgoing.inline.push(part);
        } else {
            outgoing.attachments.push(part);
        }
    }
    let options = ComposeOptions {
        purpose: Purpose::Draft,
        max_bytes: None,
    };
    mailtide_mime::compose(&outgoing, &options)
        .map(|c| c.raw)
        .map_err(|e| format!("compose failed: {e}"))
}

/// Parse `"Display Name <email@example.com>"` or `"email@example.com"` into a `mailtide_mime::Address`.
#[must_use]
pub fn parse_single_address(raw: &str) -> Address {
    let parsed = mailtide_mime::parse_addresses(raw);
    if let Some(first) = parsed.into_iter().next() {
        return first;
    }
    Address {
        name: None,
        email: raw.trim().to_owned(),
    }
}

/// Normalize LF line endings to CRLF for RFC 5322.
#[must_use]
pub fn normalize_crlf(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 32);
    let mut prev_cr = false;
    for ch in text.chars() {
        if ch == '\n' && !prev_cr {
            out.push('\r');
        }
        out.push(ch);
        prev_cr = ch == '\r';
    }
    out
}

/// Stamp Gmail's own `Message-ID` (and optionally strip `X-Mailtide-Id` to mimic Gmail Web).
#[must_use]
pub fn stamp_gmail_headers(raw: &[u8], seq: u64, strip_tracking: bool) -> Vec<u8> {
    let (head_bytes, sep, body_bytes) = split_headers_body(raw);
    let head = String::from_utf8_lossy(head_bytes);
    let mut kept = Vec::new();
    let mut skipping = false;
    for line in head.split("\r\n") {
        let is_continuation = line.starts_with(' ') || line.starts_with('\t');
        if is_continuation {
            if !skipping {
                kept.push(line.to_owned());
            }
            continue;
        }
        let lower = line.to_ascii_lowercase();
        let drop_line = lower.starts_with("message-id:")
            || (strip_tracking && lower.starts_with("x-mailtide-id:"));
        skipping = drop_line;
        if !drop_line && !line.is_empty() {
            kept.push(line.to_owned());
        }
    }
    let mut out = format!("Message-ID: <gmail-{seq:06x}@mail.gmail.com>\r\n").into_bytes();
    out.extend_from_slice(kept.join("\r\n").as_bytes());
    out.extend_from_slice(sep);
    out.extend_from_slice(body_bytes);
    out
}

fn split_headers_body(raw: &[u8]) -> (&[u8], &[u8], &[u8]) {
    if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
        return (&raw[..pos], b"\r\n\r\n", &raw[pos + 4..]);
    }
    if let Some(pos) = raw.windows(2).position(|w| w == b"\n\n") {
        return (&raw[..pos], b"\r\n\r\n", &raw[pos + 2..]);
    }
    (raw, b"\r\n\r\n", b"")
}

pub(crate) struct DerivedMeta {
    pub internal_date_ms: i64,
    pub snippet: String,
    pub from: String,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub subject: String,
    pub rfc822_message_id: String,
    pub has_attachment: bool,
    pub search_text: String,
}

/// Derive Gmail-style metadata and search text from raw RFC 5322 bytes using `mailtide-mime`.
pub(crate) fn derive_metadata(raw: &[u8], explicit_date_ms: Option<i64>) -> DerivedMeta {
    let facts = mailtide_mime::facts(raw);
    let parsed = mailtide_mime::parse(raw).ok();
    let header_first = |name: &str| -> String {
        parsed
            .as_ref()
            .and_then(|p| {
                p.headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v.clone())
            })
            .unwrap_or_default()
    };
    let header_all = |name: &str| -> Vec<String> {
        parsed.as_ref().map_or_default(|p| {
            p.headers
                .iter()
                .filter(|(k, _)| k.eq_ignore_ascii_case(name))
                .flat_map(|(_, v)| {
                    mailtide_mime::parse_addresses(v)
                        .into_iter()
                        .map(|a| a.email)
                })
                .collect()
        })
    };

    let internal_date_ms = explicit_date_ms
        .or_else(|| facts.sent_at.map(|s| s.at.0))
        .unwrap_or_else(now_ms);
    let from = match (&facts.sender_name, &facts.sender_email) {
        (Some(name), Some(email)) if !name.is_empty() => format!("{name} <{email}>"),
        (_, Some(email)) => email.clone(),
        _ => header_first("From"),
    };
    let to = header_all("To");
    let cc = header_all("Cc");
    let subject = mailtide_mime::decode_header(&header_first("Subject"));
    let rfc822_message_id = header_first("Message-ID");
    let plain_body = parsed
        .as_ref()
        .and_then(|p| p.text.as_deref())
        .map(str::to_owned)
        .or_else(|| {
            parsed
                .as_ref()
                .and_then(|p| p.html.as_deref())
                .map(strip_html_tags)
        })
        .unwrap_or_default();
    let snippet = build_snippet(&plain_body);
    let has_attachment = facts.attached > 0;

    let mut search_text = String::new();
    search_text.push_str(&from);
    search_text.push(' ');
    search_text.push_str(&to.join(" "));
    search_text.push(' ');
    search_text.push_str(&cc.join(" "));
    search_text.push(' ');
    search_text.push_str(&subject);
    search_text.push(' ');
    search_text.push_str(&rfc822_message_id);
    search_text.push(' ');
    search_text.push_str(&plain_body.chars().take(4000).collect::<String>());
    let search_text = search_text.to_ascii_lowercase();

    DerivedMeta {
        internal_date_ms,
        snippet,
        from,
        to,
        cc,
        subject,
        rfc822_message_id,
        has_attachment,
        search_text,
    }
}

fn build_snippet(body: &str) -> String {
    let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = collapsed.chars().take(160).collect();
    if collapsed.chars().count() > 160 {
        out.push_str("...");
    }
    out
}

fn strip_html_tags(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => {
                in_tag = true;
                out.push(' ');
            }
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

fn dedup_vec(items: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    items
        .into_iter()
        .filter(|item| seen.insert(item.clone()))
        .collect()
}

fn tokenize_query(q: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for ch in q.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            c if c.is_whitespace() && !in_quotes => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current).to_ascii_lowercase());
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        tokens.push(current.to_ascii_lowercase());
    }
    tokens
}

fn matches_token(msg: &StoredMessage, token: &str, labels: &BTreeMap<String, LabelDef>) -> bool {
    let (negated, tok) = token
        .strip_prefix('-')
        .map_or((false, token), |rest| (true, rest));
    let matched = if let Some(val) = tok.strip_prefix("from:") {
        msg.from.to_ascii_lowercase().contains(val)
    } else if let Some(val) = tok.strip_prefix("to:") {
        msg.to.iter().any(|t| t.to_ascii_lowercase().contains(val))
    } else if let Some(val) = tok.strip_prefix("cc:") {
        msg.cc.iter().any(|t| t.to_ascii_lowercase().contains(val))
    } else if let Some(val) = tok.strip_prefix("subject:") {
        msg.subject.to_ascii_lowercase().contains(val)
    } else if let Some(val) = tok.strip_prefix("rfc822msgid:") {
        let needle = val.trim_matches(|c| c == '<' || c == '>');
        let hay = msg
            .rfc822_message_id
            .to_ascii_lowercase()
            .trim_matches(|c| c == '<' || c == '>')
            .to_owned();
        hay == needle || hay.contains(needle)
    } else if let Some(val) = tok.strip_prefix("is:") {
        match val {
            "unread" => msg.labels.iter().any(|l| l == "UNREAD"),
            "read" => !msg.labels.iter().any(|l| l == "UNREAD"),
            "starred" => msg.labels.iter().any(|l| l == "STARRED"),
            "important" => msg.labels.iter().any(|l| l == "IMPORTANT"),
            _ => true,
        }
    } else if let Some(val) = tok.strip_prefix("has:") {
        match val {
            "attachment" => msg.has_attachment,
            _ => true,
        }
    } else if let Some(val) = tok.strip_prefix("in:") {
        match val {
            "anywhere" => true,
            "inbox" => msg.labels.iter().any(|l| l == "INBOX"),
            "sent" => msg.labels.iter().any(|l| l == "SENT"),
            "trash" => msg.labels.iter().any(|l| l == "TRASH"),
            "spam" => msg.labels.iter().any(|l| l == "SPAM"),
            "draft" | "drafts" => msg.labels.iter().any(|l| l == "DRAFT"),
            other => has_label_named(msg, other, labels),
        }
    } else if let Some(val) = tok.strip_prefix("label:") {
        has_label_named(msg, val, labels)
    } else {
        msg.search_text.contains(tok)
    };
    if negated { !matched } else { matched }
}

fn has_label_named(msg: &StoredMessage, wanted: &str, labels: &BTreeMap<String, LabelDef>) -> bool {
    let normalized = wanted.replace('-', " ");
    msg.labels.iter().any(|lid| {
        if lid.eq_ignore_ascii_case(wanted) {
            return true;
        }
        labels.get(lid).is_some_and(|def| {
            let name = def.name.to_ascii_lowercase();
            name == wanted || name == normalized || name.replace('/', "-") == wanted
        })
    })
}
