//! Out-of-band JSON control API (`/_control/...`) for injecting messages,
//! simulating external Gmail Web edits/sends/discards, running bursts,
//! seeding synthetic corpora, toggling faults, and inspecting state.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode};
use mailtide_mime::{Address, ComposeOptions, MessageId, OutgoingMessage, Purpose};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::gmail::{parse_query, pct_decode, query_first};
use crate::seed::{SeedSpec, import_eml_dir, populate};
use crate::server::{AppContext, HttpReply, json_reply, text_reply};
use crate::state::{
    AccountFaults, AccountState, DropSendMode, GlobalFaults, InjectMessage, RefusalFault,
    build_raw_message, now_ms, parse_single_address,
};

/// Dispatch `/_control/...` requests.
pub(crate) async fn handle(
    ctx: &Arc<AppContext>,
    method: Method,
    path: &str,
    query_str: &str,
    body_bytes: Bytes,
) -> HttpReply {
    let subpath = path.strip_prefix("/_control").unwrap_or(path);
    let parts: Vec<&str> = subpath.split('/').filter(|s| !s.is_empty()).collect();
    let query = parse_query(query_str);

    match (method.as_str(), parts.as_slice()) {
        ("GET", ["status"]) => handle_status(ctx),
        ("POST", ["reset"]) => handle_reset(ctx, &body_bytes).await,
        ("GET", ["requests"]) => handle_requests(ctx, &query),
        ("POST", ["faults"]) => handle_global_faults(ctx, &body_bytes),
        ("DELETE", ["faults"]) => handle_clear_global_faults(ctx),
        ("GET", ["accounts"]) => handle_accounts_list(ctx),
        ("POST", ["accounts"]) => handle_account_create(ctx, &body_bytes).await,
        ("GET", ["accounts", email]) => handle_account_get(ctx, &pct_decode(email)),
        ("GET", ["accounts", email, "messages"]) => {
            handle_account_messages(ctx, &pct_decode(email), &query)
        }
        ("GET", ["accounts", email, "messages", mid, "raw"]) => {
            handle_account_message_raw(ctx, &pct_decode(email), &pct_decode(mid))
        }
        ("POST", ["accounts", email, "messages"]) => {
            handle_inject_message(ctx, &pct_decode(email), &body_bytes).await
        }
        ("POST", ["accounts", email, "messages", mid, "modify"]) => {
            handle_external_modify(ctx, &pct_decode(email), &pct_decode(mid), &body_bytes).await
        }
        ("DELETE", ["accounts", email, "messages", mid]) => {
            handle_external_delete(ctx, &pct_decode(email), &pct_decode(mid)).await
        }
        ("POST", ["accounts", email, "threads", tid, "reply"]) => {
            handle_thread_reply(ctx, &pct_decode(email), &pct_decode(tid), &body_bytes).await
        }
        ("POST", ["accounts", email, "seed"]) => {
            handle_seed(ctx, &pct_decode(email), &body_bytes).await
        }
        ("POST", ["accounts", email, "import-eml"]) => {
            handle_import_eml(ctx, &pct_decode(email), &body_bytes).await
        }
        ("POST", ["accounts", email, "burst"]) => {
            handle_burst_start(ctx, &pct_decode(email), &body_bytes)
        }
        ("DELETE", ["accounts", email, "burst"]) => handle_burst_stop(ctx, &pct_decode(email)),
        ("GET", ["accounts", email, "drafts"]) => handle_drafts_list(ctx, &pct_decode(email)),
        ("POST", ["accounts", email, "drafts"]) => {
            handle_external_draft_create(ctx, &pct_decode(email), &body_bytes).await
        }
        ("POST", ["accounts", email, "drafts", did, "web-edit"]) => {
            handle_web_draft_edit(ctx, &pct_decode(email), &pct_decode(did), &body_bytes).await
        }
        ("POST", ["accounts", email, "drafts", did, "web-send"]) => {
            handle_web_draft_send(ctx, &pct_decode(email), &pct_decode(did)).await
        }
        ("POST", ["accounts", email, "drafts", did, "web-discard"]) => {
            handle_web_draft_discard(ctx, &pct_decode(email), &pct_decode(did)).await
        }
        ("POST", ["accounts", email, "labels"]) => {
            handle_external_label_create(ctx, &pct_decode(email), &body_bytes).await
        }
        ("POST", ["accounts", email, "expire-history"]) => {
            handle_expire_history(ctx, &pct_decode(email)).await
        }
        ("POST", ["accounts", email, "faults"]) => {
            handle_account_faults(ctx, &pct_decode(email), &body_bytes).await
        }
        ("DELETE", ["accounts", email, "faults"]) => {
            handle_clear_account_faults(ctx, &pct_decode(email)).await
        }
        ("GET", ["accounts", email, "sent"]) => handle_sent_list(ctx, &pct_decode(email)),
        ("POST", ["accounts", email, "push"]) => handle_push_now(ctx, &pct_decode(email)).await,
        _ => err_reply(StatusCode::NOT_FOUND, "Unknown control endpoint"),
    }
}

fn handle_status(ctx: &Arc<AppContext>) -> HttpReply {
    let bursts = ctx.bursts.lock().expect("bursts lock");
    let guard = ctx.state.lock().expect("state lock");
    let accounts: Vec<_> = guard
        .accounts
        .values()
        .map(|a| a.summary(bursts.contains_key(&a.email)))
        .collect();
    json_reply(
        StatusCode::OK,
        &json!({
            "push_url": ctx.push.url(),
            "global_faults": guard.global_faults,
            "request_count": guard.request_log.len(),
            "accounts": accounts,
        }),
    )
}

async fn handle_reset(ctx: &Arc<AppContext>, body_bytes: &Bytes) -> HttpReply {
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let target = body
        .get("account")
        .and_then(Value::as_str)
        .map(|s| s.trim().to_ascii_lowercase());

    {
        let mut bursts = ctx.bursts.lock().expect("bursts lock");
        if let Some(ref email) = target {
            if let Some(handle) = bursts.remove(email) {
                handle.abort();
            }
        } else {
            for (_, handle) in std::mem::take(&mut *bursts) {
                handle.abort();
            }
        }
    }

    {
        let mut guard = ctx.state.lock().expect("state lock");
        if let Some(ref email) = target {
            guard.accounts.remove(email);
        } else {
            guard.accounts.clear();
            guard.global_faults = GlobalFaults::default();
            guard.request_log.clear();
        }
    }

    ctx.persist.clear(target.as_deref()).await;
    json_reply(
        StatusCode::OK,
        &json!({ "reset": target.unwrap_or_else(|| "all".to_owned()) }),
    )
}

fn handle_requests(
    ctx: &Arc<AppContext>,
    query: &std::collections::BTreeMap<String, Vec<String>>,
) -> HttpReply {
    let limit = query_first(query, "limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100)
        .min(500);
    let guard = ctx.state.lock().expect("state lock");
    let items: Vec<_> = guard
        .request_log
        .iter()
        .rev()
        .take(limit)
        .cloned()
        .collect();
    json_reply(StatusCode::OK, &json!({ "requests": items }))
}

#[derive(Deserialize, Default)]
struct GlobalFaultsPatch {
    delay_ms: Option<u64>,
    jitter_ms: Option<u64>,
    push_paused: Option<bool>,
}

fn handle_global_faults(ctx: &Arc<AppContext>, body_bytes: &Bytes) -> HttpReply {
    let Ok(patch) = serde_json::from_slice::<GlobalFaultsPatch>(body_bytes) else {
        return err_reply(StatusCode::BAD_REQUEST, "Invalid global faults payload");
    };
    let mut guard = ctx.state.lock().expect("state lock");
    if let Some(v) = patch.delay_ms {
        guard.global_faults.delay_ms = v;
    }
    if let Some(v) = patch.jitter_ms {
        guard.global_faults.jitter_ms = v;
    }
    if let Some(v) = patch.push_paused {
        guard.global_faults.push_paused = v;
    }
    json_reply(StatusCode::OK, &guard.global_faults)
}

fn handle_clear_global_faults(ctx: &Arc<AppContext>) -> HttpReply {
    let mut guard = ctx.state.lock().expect("state lock");
    guard.global_faults = GlobalFaults::default();
    json_reply(StatusCode::OK, &guard.global_faults)
}

fn handle_accounts_list(ctx: &Arc<AppContext>) -> HttpReply {
    let bursts = ctx.bursts.lock().expect("bursts lock");
    let guard = ctx.state.lock().expect("state lock");
    let accounts: Vec<_> = guard
        .accounts
        .values()
        .map(|a| a.summary(bursts.contains_key(&a.email)))
        .collect();
    json_reply(StatusCode::OK, &json!({ "accounts": accounts }))
}

async fn handle_account_create(ctx: &Arc<AppContext>, body_bytes: &Bytes) -> HttpReply {
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let Some(email) = body
        .get("email")
        .and_then(Value::as_str)
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
    else {
        return err_reply(StatusCode::BAD_REQUEST, "email is required");
    };
    let (summary, meta) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let (acc, _) = guard.ensure_account(&email);
        (acc.summary(false), acc.meta())
    };
    ctx.persist
        .save_delta(&email, &meta, &[], &[], &[], &[], &[], &[])
        .await;
    json_reply(StatusCode::OK, &summary)
}

fn handle_account_get(ctx: &Arc<AppContext>, raw_email: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let bursts = ctx.bursts.lock().expect("bursts lock");
    let guard = ctx.state.lock().expect("state lock");
    let Some(acc) = guard.accounts.get(&email) else {
        return err_reply(StatusCode::NOT_FOUND, "Account not found");
    };
    json_reply(
        StatusCode::OK,
        &json!({
            "summary": acc.summary(bursts.contains_key(&email)),
            "labels": acc.labels.values().collect::<Vec<_>>(),
            "watch_topic": acc.watch_topic,
        }),
    )
}

fn handle_account_messages(
    ctx: &Arc<AppContext>,
    raw_email: &str,
    query: &std::collections::BTreeMap<String, Vec<String>>,
) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let thread_filter = query_first(query, "thread_id");
    let q_filter = query_first(query, "q");
    let limit = query_first(query, "limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 500);
    let offset = query_first(query, "offset")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    let guard = ctx.state.lock().expect("state lock");
    let Some(acc) = guard.accounts.get(&email) else {
        return json_reply(StatusCode::OK, &json!({ "messages": [], "total": 0 }));
    };

    let mut msgs: Vec<&crate::state::StoredMessage> = if let Some(q) = q_filter {
        acc.search_messages(q, &[], true)
    } else {
        let mut all: Vec<_> = acc
            .messages
            .values()
            .filter(|m| thread_filter.is_none_or(|t| m.thread_id == t))
            .collect();
        all.sort_by(|a, b| {
            b.internal_date_ms
                .cmp(&a.internal_date_ms)
                .then_with(|| b.id.cmp(&a.id))
        });
        all
    };
    if let Some(tid) = thread_filter
        && q_filter.is_some()
    {
        msgs.retain(|m| m.thread_id == tid);
    }

    let total = msgs.len();
    let end = offset.saturating_add(limit).min(total);
    let slice = if offset < total {
        &msgs[offset..end]
    } else {
        &[]
    };
    let items: Vec<Value> = slice
        .iter()
        .map(|m| {
            json!({
                "id": m.id,
                "thread_id": m.thread_id,
                "labels": m.labels,
                "history_id": m.history_id,
                "internal_date_ms": m.internal_date_ms,
                "from": m.from,
                "to": m.to,
                "cc": m.cc,
                "subject": m.subject,
                "snippet": m.snippet,
                "rfc822_message_id": m.rfc822_message_id,
                "has_attachment": m.has_attachment,
                "withheld": acc.faults.withheld_messages.contains(&m.id),
            })
        })
        .collect();
    json_reply(
        StatusCode::OK,
        &json!({ "messages": items, "total": total }),
    )
}

fn handle_account_message_raw(ctx: &Arc<AppContext>, raw_email: &str, mid: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let guard = ctx.state.lock().expect("state lock");
    let Some(msg) = guard.accounts.get(&email).and_then(|a| a.messages.get(mid)) else {
        return err_reply(StatusCode::NOT_FOUND, "Message not found");
    };
    text_reply(
        StatusCode::OK,
        "text/plain; charset=utf-8",
        String::from_utf8_lossy(&msg.raw).into_owned(),
    )
}

async fn handle_inject_message(
    ctx: &Arc<AppContext>,
    raw_email: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let Ok(input) = serde_json::from_slice::<InjectMessage>(body_bytes) else {
        return err_reply(StatusCode::BAD_REQUEST, "Invalid InjectMessage JSON");
    };
    let raw = match build_raw_message(&email, &input) {
        Ok(raw) => raw,
        Err(msg) => return err_reply(StatusCode::BAD_REQUEST, &msg),
    };
    let labels = input
        .labels
        .unwrap_or_else(|| vec!["INBOX".to_owned(), "UNREAD".to_owned()]);

    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let (acc, _) = guard.ensure_account(&email);
        let (msg, ev) = acc.add_message(input.id, input.thread_id, labels, input.date_ms, raw);
        let should_push = !paused && !acc.faults.push_paused;
        (acc.meta(), msg, ev, acc.history_id, should_push)
    };
    ctx.persist
        .save_delta(
            &email,
            &saved.0,
            std::slice::from_ref(&saved.1),
            &[],
            &[],
            &[],
            &[saved.2],
            &[],
        )
        .await;
    if saved.4 {
        ctx.push.notify(email, saved.3);
    }
    json_reply(
        StatusCode::OK,
        &json!({
            "id": saved.1.id,
            "thread_id": saved.1.thread_id,
            "history_id": saved.3,
            "labels": saved.1.labels,
        }),
    )
}

#[derive(Deserialize, Default)]
struct ExternalModifyBody {
    #[serde(default)]
    add_labels: Vec<String>,
    #[serde(default)]
    remove_labels: Vec<String>,
}

async fn handle_external_modify(
    ctx: &Arc<AppContext>,
    raw_email: &str,
    mid: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let Ok(body) = serde_json::from_slice::<ExternalModifyBody>(body_bytes) else {
        return err_reply(StatusCode::BAD_REQUEST, "Invalid modify body");
    };
    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let Some(acc) = guard.accounts.get_mut(&email) else {
            return err_reply(StatusCode::NOT_FOUND, "Account not found");
        };
        let Some((msg, evs)) = acc.modify_labels(mid, &body.add_labels, &body.remove_labels) else {
            return err_reply(StatusCode::NOT_FOUND, "Message not found");
        };
        let should_push = !paused && !acc.faults.push_paused;
        (acc.meta(), msg, evs, acc.history_id, should_push)
    };
    ctx.persist
        .save_delta(
            &email,
            &saved.0,
            std::slice::from_ref(&saved.1),
            &[],
            &[],
            &[],
            &saved.2,
            &[],
        )
        .await;
    if saved.4 {
        ctx.push.notify(email, saved.3);
    }
    json_reply(
        StatusCode::OK,
        &json!({
            "id": saved.1.id,
            "labels": saved.1.labels,
            "history_id": saved.3,
        }),
    )
}

async fn handle_external_delete(ctx: &Arc<AppContext>, raw_email: &str, mid: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let Some(acc) = guard.accounts.get_mut(&email) else {
            return err_reply(StatusCode::NOT_FOUND, "Account not found");
        };
        let Some(ev) = acc.delete_message(mid) else {
            return err_reply(StatusCode::NOT_FOUND, "Message not found");
        };
        let should_push = !paused && !acc.faults.push_paused;
        (acc.meta(), ev, acc.history_id, should_push)
    };
    ctx.persist
        .save_delta(
            &email,
            &saved.0,
            &[],
            &[mid.to_owned()],
            &[],
            &[],
            &[saved.1],
            &[],
        )
        .await;
    if saved.3 {
        ctx.push.notify(email, saved.2);
    }
    json_reply(
        StatusCode::OK,
        &json!({ "deleted": mid, "history_id": saved.2 }),
    )
}

#[derive(Deserialize, Default)]
struct ThreadReplyBody {
    from: Option<String>,
    body: Option<String>,
    html: Option<String>,
    subject: Option<String>,
    labels: Option<Vec<String>>,
}

async fn handle_thread_reply(
    ctx: &Arc<AppContext>,
    raw_email: &str,
    tid: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let Ok(input) = serde_json::from_slice::<ThreadReplyBody>(body_bytes) else {
        return err_reply(StatusCode::BAD_REQUEST, "Invalid reply payload");
    };
    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let Some(acc) = guard.accounts.get_mut(&email) else {
            return err_reply(StatusCode::NOT_FOUND, "Account not found");
        };
        let mut thread_msgs: Vec<_> = acc
            .messages
            .values()
            .filter(|m| m.thread_id == tid && !m.labels.iter().any(|l| l == "DRAFT"))
            .cloned()
            .collect();
        if thread_msgs.is_empty() {
            return err_reply(StatusCode::NOT_FOUND, "Thread not found");
        }
        thread_msgs.sort_by_key(|m| m.internal_date_ms);
        let latest = thread_msgs.last().expect("non-empty");

        let sender_raw = input.from.clone().unwrap_or_else(|| {
            thread_msgs
                .iter()
                .rev()
                .find(|m| !m.from.to_ascii_lowercase().contains(&email))
                .map_or_else(|| "contact@example.com".to_owned(), |m| m.from.clone())
        });
        let from_addr = parse_single_address(&sender_raw);
        let mut outgoing = OutgoingMessage::new(from_addr, now_ms().div_euclid(1000));
        outgoing.to.push(Address {
            name: None,
            email: email.clone(),
        });
        let subject = input.subject.unwrap_or_else(|| {
            if latest.subject.to_ascii_lowercase().starts_with("re:") {
                latest.subject.clone()
            } else {
                format!("Re: {}", latest.subject)
            }
        });
        outgoing.subject = Some(subject);
        if let Ok(irt) = MessageId::parse(&latest.rfc822_message_id) {
            outgoing.in_reply_to = Some(irt);
        }
        for m in &thread_msgs {
            if let Ok(parsed) = MessageId::parse(&m.rfc822_message_id) {
                outgoing.references.push(parsed);
            }
        }
        outgoing.text = Some(
            input
                .body
                .unwrap_or_else(|| "Thanks for the update! Following up here.".to_owned()),
        );
        outgoing.html = input.html;
        let raw = mailtide_mime::compose(
            &outgoing,
            &ComposeOptions {
                purpose: Purpose::Draft,
                max_bytes: None,
            },
        )
        .map_err(|e| format!("compose failed: {e}"));
        let raw = match raw {
            Ok(c) => c.raw,
            Err(e) => return err_reply(StatusCode::BAD_REQUEST, &e),
        };
        let labels = input
            .labels
            .unwrap_or_else(|| vec!["INBOX".to_owned(), "UNREAD".to_owned()]);
        let (msg, ev) = acc.add_message(None, Some(tid.to_owned()), labels, Some(now_ms()), raw);
        let should_push = !paused && !acc.faults.push_paused;
        (acc.meta(), msg, ev, acc.history_id, should_push)
    };

    ctx.persist
        .save_delta(
            &email,
            &saved.0,
            std::slice::from_ref(&saved.1),
            &[],
            &[],
            &[],
            &[saved.2],
            &[],
        )
        .await;
    if saved.4 {
        ctx.push.notify(email, saved.3);
    }
    json_reply(
        StatusCode::OK,
        &json!({
            "id": saved.1.id,
            "thread_id": saved.1.thread_id,
            "history_id": saved.3,
        }),
    )
}

async fn handle_seed(ctx: &Arc<AppContext>, raw_email: &str, body_bytes: &Bytes) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let spec: SeedSpec = if body_bytes.is_empty() {
        SeedSpec::default()
    } else {
        match serde_json::from_slice(body_bytes) {
            Ok(s) => s,
            Err(e) => return err_reply(StatusCode::BAD_REQUEST, &format!("Invalid SeedSpec: {e}")),
        }
    };
    let (result, snapshot, should_push) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let (acc, _) = guard.ensure_account(&email);
        let res = populate(acc, &spec);
        let should_push = !paused && !acc.faults.push_paused;
        (res, acc.clone(), should_push)
    };
    ctx.persist.save_account(&snapshot).await;
    if should_push {
        ctx.push.notify(email, result.history_id);
    }
    json_reply(StatusCode::OK, &result)
}

#[derive(Deserialize)]
struct ImportEmlBody {
    dir: String,
    #[serde(default)]
    labels: Vec<String>,
}

async fn handle_import_eml(
    ctx: &Arc<AppContext>,
    raw_email: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let Ok(body) = serde_json::from_slice::<ImportEmlBody>(body_bytes) else {
        return err_reply(StatusCode::BAD_REQUEST, "dir is required");
    };
    let dir = PathBuf::from(&body.dir);
    let (result, snapshot, should_push) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let (acc, _) = guard.ensure_account(&email);
        match import_eml_dir(acc, &dir, &body.labels) {
            Ok(res) => {
                let should_push = !paused && !acc.faults.push_paused;
                (res, acc.clone(), should_push)
            }
            Err(e) => return err_reply(StatusCode::BAD_REQUEST, &e),
        }
    };
    ctx.persist.save_account(&snapshot).await;
    if should_push && result.imported > 0 {
        ctx.push.notify(email, result.history_id);
    }
    json_reply(StatusCode::OK, &result)
}

#[derive(Deserialize)]
struct BurstBody {
    #[serde(default = "default_burst_count")]
    count: usize,
    #[serde(default = "default_burst_interval")]
    interval_ms: u64,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    subject_prefix: Option<String>,
}

fn default_burst_count() -> usize {
    25
}
fn default_burst_interval() -> u64 {
    200
}

fn handle_burst_start(ctx: &Arc<AppContext>, raw_email: &str, body_bytes: &Bytes) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let body: BurstBody = if body_bytes.is_empty() {
        BurstBody {
            count: default_burst_count(),
            interval_ms: default_burst_interval(),
            thread_id: None,
            from: None,
            subject_prefix: None,
        }
    } else {
        match serde_json::from_slice(body_bytes) {
            Ok(b) => b,
            Err(e) => return err_reply(StatusCode::BAD_REQUEST, &format!("Invalid burst: {e}")),
        }
    };
    let count = body.count.clamp(1, 5000);
    let interval_ms = body.interval_ms.max(10);

    // Cancel any existing burst on this account.
    {
        let mut bursts = ctx.bursts.lock().expect("bursts lock");
        if let Some(prev) = bursts.remove(&email) {
            prev.abort();
        }
    }

    let ctx_clone = Arc::clone(ctx);
    let email_clone = email.clone();
    let thread_id = body.thread_id;
    let from = body
        .from
        .unwrap_or_else(|| "Burst Sender <burst@example.com>".to_owned());
    let subject_prefix = body
        .subject_prefix
        .unwrap_or_else(|| "Burst update".to_owned());

    let handle = tokio::spawn(async move {
        for idx in 1..=count {
            let input = InjectMessage {
                thread_id: thread_id.clone(),
                from: Some(from.clone()),
                to: vec![email_clone.clone()],
                subject: Some(format!("{subject_prefix} #{idx}/{count}")),
                text: Some(format!(
                    "Live burst message {idx} of {count} delivered at {}.",
                    now_ms()
                )),
                date_ms: Some(now_ms()),
                ..InjectMessage::default()
            };
            if let Ok(raw) = build_raw_message(&email_clone, &input) {
                let saved = {
                    let mut guard = ctx_clone.state.lock().expect("state lock");
                    let paused = guard.global_faults.push_paused;
                    let (acc, _) = guard.ensure_account(&email_clone);
                    let (msg, ev) = acc.add_message(
                        None,
                        input.thread_id,
                        vec!["INBOX".to_owned(), "UNREAD".to_owned()],
                        input.date_ms,
                        raw,
                    );
                    let should_push = !paused && !acc.faults.push_paused;
                    (acc.meta(), msg, ev, acc.history_id, should_push)
                };
                ctx_clone
                    .persist
                    .save_delta(
                        &email_clone,
                        &saved.0,
                        &[saved.1],
                        &[],
                        &[],
                        &[],
                        &[saved.2],
                        &[],
                    )
                    .await;
                if saved.4 {
                    ctx_clone.push.notify(email_clone.clone(), saved.3);
                }
            }
            if idx < count {
                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
            }
        }
        let mut bursts = ctx_clone.bursts.lock().expect("bursts lock");
        bursts.remove(&email_clone);
    });

    {
        let mut bursts = ctx.bursts.lock().expect("bursts lock");
        bursts.insert(email.clone(), handle.abort_handle());
    }

    json_reply(
        StatusCode::ACCEPTED,
        &json!({
            "started": true,
            "account": email,
            "count": count,
            "interval_ms": interval_ms,
        }),
    )
}

fn handle_burst_stop(ctx: &Arc<AppContext>, raw_email: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let stopped = {
        let mut bursts = ctx.bursts.lock().expect("bursts lock");
        bursts.remove(&email).is_some_and(|h| {
            h.abort();
            true
        })
    };
    json_reply(StatusCode::OK, &json!({ "stopped": stopped }))
}

fn handle_drafts_list(ctx: &Arc<AppContext>, raw_email: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let guard = ctx.state.lock().expect("state lock");
    let Some(acc) = guard.accounts.get(&email) else {
        return json_reply(StatusCode::OK, &json!({ "drafts": [] }));
    };
    let drafts: Vec<Value> = acc
        .drafts
        .iter()
        .filter_map(|(did, mid)| {
            let msg = acc.messages.get(mid)?;
            let parsed = mailtide_mime::parse(&msg.raw).ok();
            Some(json!({
                "draft_id": did,
                "message_id": mid,
                "thread_id": msg.thread_id,
                "subject": msg.subject,
                "to": msg.to,
                "cc": msg.cc,
                "snippet": msg.snippet,
                "text": parsed.as_ref().and_then(|p| p.text.as_deref()),
                "has_html": parsed.as_ref().and_then(|p| p.html.as_ref()).is_some(),
                "tracking_id": mailtide_mime::tracking_id(&msg.raw),
            }))
        })
        .collect();
    json_reply(StatusCode::OK, &json!({ "drafts": drafts }))
}

#[derive(Deserialize, Default)]
struct ExternalDraftCreate {
    #[serde(flatten)]
    message: InjectMessage,
    /// If true (default), emulate Gmail Web (no `X-Mailtide-Id`).
    #[serde(default = "default_true")]
    web_client: bool,
}

fn default_true() -> bool {
    true
}

async fn handle_external_draft_create(
    ctx: &Arc<AppContext>,
    raw_email: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let Ok(mut input) = serde_json::from_slice::<ExternalDraftCreate>(body_bytes) else {
        return err_reply(StatusCode::BAD_REQUEST, "Invalid draft payload");
    };
    if input.message.from.is_none() {
        input.message.from = Some(email.clone());
    }
    let raw = match build_raw_message(&email, &input.message) {
        Ok(r) => r,
        Err(e) => return err_reply(StatusCode::BAD_REQUEST, &e),
    };
    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let (acc, _) = guard.ensure_account(&email);
        let (did, msg, ev) = acc.create_draft(&raw, input.message.thread_id, input.web_client);
        let should_push = !paused && !acc.faults.push_paused;
        (acc.meta(), did, msg, ev, acc.history_id, should_push)
    };
    ctx.persist
        .save_delta(
            &email,
            &saved.0,
            std::slice::from_ref(&saved.2),
            &[],
            &[(saved.1.clone(), saved.2.id.clone())],
            &[],
            &[saved.3],
            &[],
        )
        .await;
    if saved.5 {
        ctx.push.notify(email, saved.4);
    }
    json_reply(
        StatusCode::OK,
        &json!({
            "draft_id": saved.1,
            "message_id": saved.2.id,
            "thread_id": saved.2.thread_id,
            "history_id": saved.4,
        }),
    )
}

#[derive(Deserialize, Default)]
struct WebDraftEditBody {
    body: Option<String>,
    html: Option<String>,
    subject: Option<String>,
    to: Option<Vec<String>>,
    cc: Option<Vec<String>>,
    bcc: Option<Vec<String>>,
    /// If true, keep `X-Mailtide-Id` (default false: Gmail Web strips custom headers).
    #[serde(default)]
    keep_tracking_id: bool,
}

async fn handle_web_draft_edit(
    ctx: &Arc<AppContext>,
    raw_email: &str,
    did: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let Ok(edit) = serde_json::from_slice::<WebDraftEditBody>(body_bytes) else {
        return err_reply(StatusCode::BAD_REQUEST, "Invalid web-edit payload");
    };
    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let Some(acc) = guard.accounts.get_mut(&email) else {
            return err_reply(StatusCode::NOT_FOUND, "Account not found");
        };
        let Some(old_mid) = acc.drafts.get(did).cloned() else {
            return err_reply(StatusCode::NOT_FOUND, "Draft not found");
        };
        let Some(old_msg) = acc.messages.get(&old_mid).cloned() else {
            return err_reply(StatusCode::NOT_FOUND, "Draft message not found");
        };

        let mime_parsed = mailtide_mime::parse(&old_msg.raw).ok();
        let mut outgoing = OutgoingMessage::new(
            Address {
                name: None,
                email: email.clone(),
            },
            now_ms().div_euclid(1000),
        );
        let to_list = edit.to.unwrap_or_else(|| old_msg.to.clone());
        let cc_list = edit.cc.unwrap_or_else(|| old_msg.cc.clone());
        outgoing.to = to_list.iter().map(|s| parse_single_address(s)).collect();
        outgoing.cc = cc_list.iter().map(|s| parse_single_address(s)).collect();
        if let Some(bcc_list) = edit.bcc {
            outgoing.bcc = bcc_list.iter().map(|s| parse_single_address(s)).collect();
        }
        outgoing.subject = Some(edit.subject.unwrap_or_else(|| old_msg.subject.clone()));

        // Preserve In-Reply-To and References just as Gmail Web does.
        if let Some(ref p) = mime_parsed {
            for (k, v) in &p.headers {
                if k.eq_ignore_ascii_case("In-Reply-To")
                    && let Ok(mid) = MessageId::parse(v)
                {
                    outgoing.in_reply_to = Some(mid);
                } else if k.eq_ignore_ascii_case("References") {
                    for token in v.split_whitespace() {
                        if let Ok(mid) = MessageId::parse(token) {
                            outgoing.references.push(mid);
                        }
                    }
                }
            }
        }
        if edit.keep_tracking_id {
            outgoing.tracking_id = mailtide_mime::tracking_id(&old_msg.raw);
        }
        let existing_text = mime_parsed
            .as_ref()
            .and_then(|p| p.text.clone())
            .unwrap_or_default();
        outgoing.text = Some(
            edit.body
                .unwrap_or_else(|| format!("{existing_text}\n\n[Edited externally in Gmail Web]")),
        );
        outgoing.html = edit.html.or_else(|| mime_parsed.and_then(|p| p.html));

        let raw = match mailtide_mime::compose(
            &outgoing,
            &ComposeOptions {
                purpose: Purpose::Draft,
                max_bytes: None,
            },
        ) {
            Ok(c) => c.raw,
            Err(e) => return err_reply(StatusCode::BAD_REQUEST, &format!("compose failed: {e}")),
        };

        let Some((deleted_mid, new_msg, evs)) =
            acc.update_draft(did, &raw, Some(old_msg.thread_id), !edit.keep_tracking_id)
        else {
            return err_reply(StatusCode::NOT_FOUND, "Draft not found");
        };
        let should_push = !paused && !acc.faults.push_paused;
        (
            acc.meta(),
            deleted_mid,
            new_msg,
            evs,
            acc.history_id,
            should_push,
        )
    };
    ctx.persist
        .save_delta(
            &email,
            &saved.0,
            std::slice::from_ref(&saved.2),
            &[saved.1],
            &[(did.to_owned(), saved.2.id.clone())],
            &[],
            &saved.3,
            &[],
        )
        .await;
    if saved.5 {
        ctx.push.notify(email, saved.4);
    }
    json_reply(
        StatusCode::OK,
        &json!({
            "draft_id": did,
            "message_id": saved.2.id,
            "thread_id": saved.2.thread_id,
            "history_id": saved.4,
        }),
    )
}

async fn handle_web_draft_send(ctx: &Arc<AppContext>, raw_email: &str, did: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let Some(acc) = guard.accounts.get_mut(&email) else {
            return err_reply(StatusCode::NOT_FOUND, "Account not found");
        };
        let Some((old_mid, sent_msg, evs, rec)) = acc.send_draft(did, true) else {
            return err_reply(StatusCode::NOT_FOUND, "Draft not found");
        };
        let should_push = !paused && !acc.faults.push_paused;
        (
            acc.meta(),
            old_mid,
            sent_msg,
            evs,
            rec,
            acc.history_id,
            should_push,
        )
    };
    ctx.persist
        .save_delta(
            &email,
            &saved.0,
            std::slice::from_ref(&saved.2),
            &[saved.1],
            &[],
            &[did.to_owned()],
            &saved.3,
            &[saved.4],
        )
        .await;
    if saved.6 {
        ctx.push.notify(email, saved.5);
    }
    json_reply(
        StatusCode::OK,
        &json!({
            "sent_message_id": saved.2.id,
            "thread_id": saved.2.thread_id,
            "history_id": saved.5,
        }),
    )
}

async fn handle_web_draft_discard(ctx: &Arc<AppContext>, raw_email: &str, did: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let Some(acc) = guard.accounts.get_mut(&email) else {
            return err_reply(StatusCode::NOT_FOUND, "Account not found");
        };
        let Some((old_mid, ev)) = acc.delete_draft(did) else {
            return err_reply(StatusCode::NOT_FOUND, "Draft not found");
        };
        let should_push = !paused && !acc.faults.push_paused;
        (acc.meta(), old_mid, ev, acc.history_id, should_push)
    };
    let hist: Vec<_> = saved.2.into_iter().collect();
    ctx.persist
        .save_delta(
            &email,
            &saved.0,
            &[],
            &[saved.1],
            &[],
            &[did.to_owned()],
            &hist,
            &[],
        )
        .await;
    if saved.4 {
        ctx.push.notify(email, saved.3);
    }
    json_reply(
        StatusCode::OK,
        &json!({ "discarded": did, "history_id": saved.3 }),
    )
}

async fn handle_external_label_create(
    ctx: &Arc<AppContext>,
    raw_email: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let Some(name) = body.get("name").and_then(Value::as_str) else {
        return err_reply(StatusCode::BAD_REQUEST, "name is required");
    };
    let color = body.get("color").filter(|v| !v.is_null()).cloned();
    let (def, meta) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let (acc, _) = guard.ensure_account(&email);
        let lid = acc.ensure_label(name, color);
        (acc.labels[&lid].clone(), acc.meta())
    };
    ctx.persist
        .save_delta(&email, &meta, &[], &[], &[], &[], &[], &[])
        .await;
    json_reply(StatusCode::OK, &def)
}

async fn handle_expire_history(ctx: &Arc<AppContext>, raw_email: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let (history_id, floor, meta, should_push) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let paused = guard.global_faults.push_paused;
        let (acc, _) = guard.ensure_account(&email);
        acc.expire_history();
        let should_push = !paused && !acc.faults.push_paused;
        (acc.history_id, acc.history_floor, acc.meta(), should_push)
    };
    ctx.persist
        .save_delta(&email, &meta, &[], &[], &[], &[], &[], &[])
        .await;
    if should_push {
        ctx.push.notify(email, history_id);
    }
    json_reply(
        StatusCode::OK,
        &json!({ "history_id": history_id, "history_floor": floor }),
    )
}

#[derive(Default)]
enum PatchOpt<T> {
    #[default]
    Unset,
    Set(Option<T>),
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for PatchOpt<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<T>::deserialize(deserializer).map(Self::Set)
    }
}

#[derive(Deserialize, Default)]
struct AccountFaultsPatch {
    delay_ms: Option<u64>,
    jitter_ms: Option<u64>,
    raw_delay_ms: Option<u64>,
    withhold_next_raw: Option<u32>,
    withhold_message_ids: Option<Vec<String>>,
    release_withheld: Option<bool>,
    #[serde(default)]
    refuse: PatchOpt<RefusalFault>,
    #[serde(default)]
    send_refusal: PatchOpt<RefusalFault>,
    #[serde(default)]
    drop_next_send: PatchOpt<DropSendMode>,
    #[serde(default)]
    delete_failure: PatchOpt<RefusalFault>,
    full_scope: Option<bool>,
    push_paused: Option<bool>,
}

async fn handle_account_faults(
    ctx: &Arc<AppContext>,
    raw_email: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let Ok(patch) = serde_json::from_slice::<AccountFaultsPatch>(body_bytes) else {
        return err_reply(StatusCode::BAD_REQUEST, "Invalid faults patch JSON");
    };
    let (faults, meta) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let (acc, _) = guard.ensure_account(&email);
        apply_fault_patch(acc, patch);
        (acc.faults.clone(), acc.meta())
    };
    ctx.persist
        .save_delta(&email, &meta, &[], &[], &[], &[], &[], &[])
        .await;
    json_reply(StatusCode::OK, &faults)
}

fn apply_fault_patch(acc: &mut AccountState, patch: AccountFaultsPatch) {
    if let Some(v) = patch.delay_ms {
        acc.faults.delay_ms = v;
    }
    if let Some(v) = patch.jitter_ms {
        acc.faults.jitter_ms = v;
    }
    if let Some(v) = patch.raw_delay_ms {
        acc.faults.raw_delay_ms = v;
    }
    if let Some(v) = patch.withhold_next_raw {
        acc.faults.withhold_next_raw = v;
    }
    if patch.release_withheld == Some(true) {
        acc.faults.withheld_messages.clear();
        acc.faults.withhold_next_raw = 0;
    }
    if let Some(ids) = patch.withhold_message_ids {
        for id in ids {
            acc.faults.withheld_messages.insert(id);
        }
    }
    if let PatchOpt::Set(v) = patch.refuse {
        acc.faults.refuse = v;
    }
    if let PatchOpt::Set(v) = patch.send_refusal {
        acc.faults.send_refusal = v;
    }
    if let PatchOpt::Set(v) = patch.drop_next_send {
        acc.faults.drop_next_send = v;
    }
    if let PatchOpt::Set(v) = patch.delete_failure {
        acc.faults.delete_failure = v;
    }
    if let Some(v) = patch.full_scope {
        acc.faults.full_scope = v;
    }
    if let Some(v) = patch.push_paused {
        acc.faults.push_paused = v;
    }
}

async fn handle_clear_account_faults(ctx: &Arc<AppContext>, raw_email: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let (faults, meta) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let (acc, _) = guard.ensure_account(&email);
        acc.faults = AccountFaults::clean();
        (acc.faults.clone(), acc.meta())
    };
    ctx.persist
        .save_delta(&email, &meta, &[], &[], &[], &[], &[], &[])
        .await;
    json_reply(StatusCode::OK, &faults)
}

fn handle_sent_list(ctx: &Arc<AppContext>, raw_email: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let guard = ctx.state.lock().expect("state lock");
    let sent: Vec<_> = guard
        .accounts
        .get(&email)
        .map(|a| a.sent_log.iter().rev().cloned().collect())
        .unwrap_or_default();
    json_reply(StatusCode::OK, &json!({ "sent": sent }))
}

async fn handle_push_now(ctx: &Arc<AppContext>, raw_email: &str) -> HttpReply {
    let email = raw_email.trim().to_ascii_lowercase();
    let hid = {
        let guard = ctx.state.lock().expect("state lock");
        guard.accounts.get(&email).map_or(1, |a| a.history_id)
    };
    match ctx.push.push_now(&email, hid).await {
        Ok(status) => json_reply(
            StatusCode::OK,
            &json!({ "pushed": true, "status": status, "history_id": hid }),
        ),
        Err(msg) => err_reply(StatusCode::BAD_REQUEST, &msg),
    }
}

fn err_reply(status: StatusCode, message: &str) -> HttpReply {
    json_reply(status, &json!({ "error": message }))
}
