//! Gmail REST API v1 wire handler (`/gmail/v1/users/{user}/...`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use bytes::Bytes;
use http::{Method, StatusCode};
use serde_json::{Value, json};

use crate::server::{AppContext, HttpReply, json_reply, no_content_reply};
use crate::state::{DropSendMode, RefusalFault, RequestLogEntry, account_from_auth, now_ms};

/// Handle any `/gmail/v1/users/...` request. Returns `Err(())` to drop the TCP connection
/// without answering (used for `drop_next_send` fault injection).
pub(crate) async fn handle(
    ctx: &Arc<AppContext>,
    method: Method,
    path: &str,
    query_str: &str,
    auth_header: Option<&str>,
    body_bytes: Bytes,
) -> Result<HttpReply, ()> {
    let started = Instant::now();
    let full_path = if query_str.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{query_str}")
    };

    let Some(account_email) = resolve_account(path, auth_header) else {
        let reply = gmail_error(401, "authError", "Invalid or missing bearer token", None);
        log_req(ctx, None, &method, &full_path, 401, started);
        return Ok(reply);
    };

    let subpath = strip_user_prefix(path);
    let query = parse_query(query_str);
    let is_raw_fetch = method == Method::GET
        && subpath.starts_with("messages/")
        && query_first(&query, "format") == Some("raw");

    // Compute artificial latency before taking the state lock for execution.
    let (delay_ms, jitter_ms, raw_delay_ms, meta_if_created) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let g_delay = guard.global_faults.delay_ms;
        let g_jitter = guard.global_faults.jitter_ms;
        let (acc, created) = guard.ensure_account(&account_email);
        let a_delay = acc.faults.delay_ms;
        let a_jitter = acc.faults.jitter_ms;
        let a_raw = if is_raw_fetch {
            acc.faults.raw_delay_ms
        } else {
            0
        };
        let meta_if_created = created.then(|| acc.meta());
        (
            g_delay.saturating_add(a_delay),
            g_jitter.saturating_add(a_jitter),
            a_raw,
            meta_if_created,
        )
    };
    if let Some(meta) = meta_if_created {
        ctx.persist
            .save_delta(&account_email, &meta, &[], &[], &[], &[], &[], &[])
            .await;
    }

    let total_sleep = delay_ms
        .saturating_add(raw_delay_ms)
        .saturating_add(jitter_for(jitter_ms));
    if total_sleep > 0 {
        tokio::time::sleep(Duration::from_millis(total_sleep)).await;
    }

    // Check blanket account refusal fault.
    if let Some(fault) = take_refusal(ctx, &account_email, RefusalSlot::Blanket).await {
        let reply = refusal_reply(&fault);
        log_req(
            ctx,
            Some(account_email),
            &method,
            &full_path,
            fault.status,
            started,
        );
        return Ok(reply);
    }

    let parts: Vec<&str> = subpath.split('/').filter(|s| !s.is_empty()).collect();

    let result = match (method.as_str(), parts.as_slice()) {
        ("GET", ["profile"]) => Ok(handle_profile(ctx, &account_email)),
        ("POST", ["watch"]) => Ok(handle_watch(ctx, &account_email, &body_bytes).await),
        ("GET", ["history"]) => Ok(handle_history(ctx, &account_email, &query)),
        ("GET", ["labels"]) => Ok(handle_labels_list(ctx, &account_email)),
        ("GET", ["labels", id]) => Ok(handle_label_get(ctx, &account_email, &pct_decode(id))),
        ("POST", ["labels"]) => Ok(handle_label_create(ctx, &account_email, &body_bytes).await),
        ("PATCH", ["labels", id]) => {
            Ok(handle_label_patch(ctx, &account_email, &pct_decode(id), &body_bytes).await)
        }
        ("DELETE", ["labels", id]) => {
            Ok(handle_label_delete(ctx, &account_email, &pct_decode(id)).await)
        }
        ("GET", ["messages"]) => Ok(handle_messages_list(ctx, &account_email, &query)),
        ("GET", ["threads"]) => Ok(handle_threads_list(ctx, &account_email, &query)),
        ("GET", ["messages", id]) => Ok(handle_message_get(
            ctx,
            &account_email,
            &pct_decode(id),
            &query,
        )),
        ("POST", ["messages", "send"]) => {
            handle_message_send(ctx, &account_email, &body_bytes).await
        }
        ("POST", ["messages", "batchModify"]) => {
            Ok(handle_batch_modify(ctx, &account_email, &body_bytes).await)
        }
        ("POST", ["messages", "batchDelete"]) => {
            Ok(handle_batch_delete(ctx, &account_email, &body_bytes).await)
        }
        ("POST", ["messages", id, "modify"]) => {
            Ok(handle_message_modify(ctx, &account_email, &pct_decode(id), &body_bytes).await)
        }
        ("DELETE", ["messages", id]) => {
            Ok(handle_message_delete(ctx, &account_email, &pct_decode(id)).await)
        }
        ("GET", ["drafts"]) => Ok(handle_drafts_list(ctx, &account_email, &query)),
        ("GET", ["drafts", id]) => Ok(handle_draft_get(
            ctx,
            &account_email,
            &pct_decode(id),
            &query,
        )),
        ("POST", ["drafts"]) => Ok(handle_draft_create(ctx, &account_email, &body_bytes).await),
        ("PUT", ["drafts", id]) => {
            Ok(handle_draft_update(ctx, &account_email, &pct_decode(id), &body_bytes).await)
        }
        ("DELETE", ["drafts", id]) => {
            Ok(handle_draft_delete(ctx, &account_email, &pct_decode(id)).await)
        }
        ("POST", ["drafts", "send"]) => handle_draft_send(ctx, &account_email, &body_bytes).await,
        _ => Ok(gmail_error(404, "notFound", "Unknown Gmail route", None)),
    };

    let status = match &result {
        Ok(reply) => reply.status.as_u16(),
        Err(()) => 0,
    };
    log_req(
        ctx,
        Some(account_email),
        &method,
        &full_path,
        status,
        started,
    );
    result
}

fn log_req(
    ctx: &Arc<AppContext>,
    account: Option<String>,
    method: &Method,
    path: &str,
    status: u16,
    started: Instant,
) {
    let mut guard = ctx.state.lock().expect("state lock");
    guard.log_request(RequestLogEntry {
        at_ms: now_ms(),
        account,
        method: method.as_str().to_owned(),
        path: path.to_owned(),
        status,
        duration_ms: started.elapsed().as_millis() as u64,
    });
}

fn resolve_account(path: &str, auth_header: Option<&str>) -> Option<String> {
    if let Some(email) = account_from_auth(auth_header) {
        return Some(email);
    }
    // If path names an explicit email instead of `me`, require an Authorization header
    // but allow routing by path user.
    if auth_header.is_some() {
        let rest = path.strip_prefix("/gmail/v1/users/")?;
        let user = rest.split('/').next()?;
        if user != "me" && user.contains('@') {
            return Some(pct_decode(user).to_ascii_lowercase());
        }
    }
    None
}

fn strip_user_prefix(path: &str) -> &str {
    let rest = path.strip_prefix("/gmail/v1/users/").unwrap_or(path);
    match rest.find('/') {
        Some(idx) => &rest[idx + 1..],
        None => "",
    }
}

fn handle_profile(ctx: &Arc<AppContext>, email: &str) -> HttpReply {
    let guard = ctx.state.lock().expect("state lock");
    let acc = &guard.accounts[email];
    let threads: BTreeSet<&str> = acc
        .messages
        .values()
        .map(|m| m.thread_id.as_str())
        .collect();
    json_reply(
        StatusCode::OK,
        &json!({
            "emailAddress": acc.email,
            "messagesTotal": acc.messages.len(),
            "threadsTotal": threads.len(),
            "historyId": acc.history_id.to_string(),
        }),
    )
}

async fn handle_watch(ctx: &Arc<AppContext>, email: &str, body_bytes: &Bytes) -> HttpReply {
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let topic = body
        .get("topicName")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let (history_id, meta) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        acc.watch_topic = topic;
        (acc.history_id, acc.meta())
    };
    ctx.persist
        .save_delta(email, &meta, &[], &[], &[], &[], &[], &[])
        .await;
    let expiration = now_ms() + 7 * 86_400_000;
    json_reply(
        StatusCode::OK,
        &json!({
            "historyId": history_id.to_string(),
            "expiration": expiration.to_string(),
        }),
    )
}

fn handle_history(
    ctx: &Arc<AppContext>,
    email: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> HttpReply {
    let Some(start_str) = query_first(query, "startHistoryId") else {
        return gmail_error(400, "invalidArgument", "startHistoryId is required", None);
    };
    let Ok(start) = start_str.parse::<u64>() else {
        return gmail_error(400, "invalidArgument", "Invalid startHistoryId", None);
    };
    let max_results = query_first(query, "maxResults")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(500)
        .clamp(1, 500);
    let offset = query_first(query, "pageToken")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    let guard = ctx.state.lock().expect("state lock");
    let acc = &guard.accounts[email];
    if start < acc.history_floor {
        return gmail_error(404, "notFound", "History id expired", None);
    }
    let matching: Vec<&Value> = acc
        .history
        .iter()
        .filter(|h| h.id > start)
        .map(|h| &h.record)
        .collect();
    let end = offset.saturating_add(max_results).min(matching.len());
    let slice: Vec<&Value> = if offset < matching.len() {
        matching[offset..end].to_vec()
    } else {
        Vec::new()
    };
    let mut resp = json!({
        "historyId": acc.history_id.to_string(),
    });
    if !slice.is_empty() {
        resp["history"] = json!(slice);
    }
    if end < matching.len() {
        resp["nextPageToken"] = json!(end.to_string());
    }
    json_reply(StatusCode::OK, &resp)
}

fn handle_labels_list(ctx: &Arc<AppContext>, email: &str) -> HttpReply {
    let guard = ctx.state.lock().expect("state lock");
    let acc = &guard.accounts[email];
    let labels: Vec<&crate::state::LabelDef> = acc.labels.values().collect();
    json_reply(StatusCode::OK, &json!({ "labels": labels }))
}

fn handle_label_get(ctx: &Arc<AppContext>, email: &str, lid: &str) -> HttpReply {
    let guard = ctx.state.lock().expect("state lock");
    let acc = &guard.accounts[email];
    match acc.labels.get(lid) {
        Some(label) => json_reply(StatusCode::OK, label),
        None => gmail_error(404, "notFound", "Label not found", None),
    }
}

async fn handle_label_create(ctx: &Arc<AppContext>, email: &str, body_bytes: &Bytes) -> HttpReply {
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let Some(name) = body.get("name").and_then(Value::as_str) else {
        return gmail_error(400, "invalidArgument", "Label name required", None);
    };
    let list_vis = body
        .get("labelListVisibility")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let msg_vis = body
        .get("messageListVisibility")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let color = body.get("color").filter(|v| !v.is_null()).cloned();

    let created = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        if acc.label_holder(name, None).is_some() {
            return gmail_error(409, "aborted", "Label name exists or conflicts", None);
        }
        let def = acc.create_label(name.to_owned(), list_vis, msg_vis, color);
        (def, acc.meta())
    };
    ctx.persist
        .save_delta(email, &created.1, &[], &[], &[], &[], &[], &[])
        .await;
    json_reply(StatusCode::OK, &created.0)
}

async fn handle_label_patch(
    ctx: &Arc<AppContext>,
    email: &str,
    lid: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let updated = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        if !acc.labels.contains_key(lid) {
            return gmail_error(404, "notFound", "Label not found", None);
        }
        if let Some(name) = body.get("name").and_then(Value::as_str)
            && acc.label_holder(name, Some(lid)).is_some()
        {
            return gmail_error(409, "aborted", "Label name exists or conflicts", None);
        }
        let label = acc.labels.get_mut(lid).expect("label");
        if let Some(name) = body.get("name").and_then(Value::as_str) {
            name.clone_into(&mut label.name);
        }
        if let Some(vis) = body.get("labelListVisibility").and_then(Value::as_str) {
            label.list_visibility = Some(vis.to_owned());
        }
        if let Some(vis) = body.get("messageListVisibility").and_then(Value::as_str) {
            label.message_visibility = Some(vis.to_owned());
        }
        if let Some(color) = body.get("color") {
            if color.is_null() {
                label.color = None;
            } else {
                label.color = Some(color.clone());
            }
        }
        let def = label.clone();
        (def, acc.meta())
    };
    ctx.persist
        .save_delta(email, &updated.1, &[], &[], &[], &[], &[], &[])
        .await;
    json_reply(StatusCode::OK, &updated.0)
}

async fn handle_label_delete(ctx: &Arc<AppContext>, email: &str, lid: &str) -> HttpReply {
    let delta = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        let Some(updates) = acc.delete_label(lid) else {
            return gmail_error(404, "notFound", "Label not found", None);
        };
        let mut msgs = Vec::new();
        let mut hist = Vec::new();
        for (m, evs) in updates {
            msgs.push(m);
            hist.extend(evs);
        }
        (acc.meta(), msgs, hist)
    };
    ctx.persist
        .save_delta(email, &delta.0, &delta.1, &[], &[], &[], &delta.2, &[])
        .await;
    no_content_reply()
}

fn handle_messages_list(
    ctx: &Arc<AppContext>,
    email: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> HttpReply {
    let max_results = query_first(query, "maxResults")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 500);
    let include_spam_trash = query_first(query, "includeSpamTrash") == Some("true");
    let label_ids = query.get("labelIds").cloned().unwrap_or_default();

    let guard = ctx.state.lock().expect("state lock");
    let acc = &guard.accounts[email];

    // Audit partial listing (`fields=messages(id),nextPageToken`).
    if query_first(query, "fields") == Some("messages(id),nextPageToken") {
        let cursor = query_first(query, "pageToken");
        let mut ids: Vec<&String> = acc
            .messages
            .keys()
            .filter(|id| cursor.is_none_or(|c| id.as_str() < c))
            .collect();
        ids.sort_by(|a, b| b.cmp(a));
        let has_more = ids.len() > max_results;
        let page_ids = &ids[..ids.len().min(max_results)];
        let mut resp = json!({
            "messages": page_ids.iter().map(|id| json!({"id": id})).collect::<Vec<_>>(),
        });
        if has_more && let Some(last) = page_ids.last() {
            resp["nextPageToken"] = json!(last);
        }
        return json_reply(StatusCode::OK, &resp);
    }

    let offset = query_first(query, "pageToken")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    let matches: Vec<(&str, &str)> = if let Some(q) = query_first(query, "q") {
        acc.search_messages(q, &label_ids, include_spam_trash)
            .into_iter()
            .map(|m| (m.id.as_str(), m.thread_id.as_str()))
            .collect()
    } else {
        let mut msgs: Vec<&crate::state::StoredMessage> = acc
            .messages
            .values()
            .filter(|m| {
                if !include_spam_trash && m.labels.iter().any(|l| l == "TRASH" || l == "SPAM") {
                    return false;
                }
                label_ids.iter().all(|l| m.labels.contains(l))
            })
            .collect();
        msgs.sort_by(|a, b| {
            b.internal_date_ms
                .cmp(&a.internal_date_ms)
                .then_with(|| b.id.cmp(&a.id))
        });
        msgs.into_iter()
            .map(|m| (m.id.as_str(), m.thread_id.as_str()))
            .collect()
    };

    let end = offset.saturating_add(max_results).min(matches.len());
    let slice = if offset < matches.len() {
        &matches[offset..end]
    } else {
        &[]
    };
    let mut resp = json!({
        "messages": slice.iter().map(|(id, tid)| json!({"id": id, "threadId": tid})).collect::<Vec<_>>(),
        "resultSizeEstimate": matches.len(),
    });
    if end < matches.len() {
        resp["nextPageToken"] = json!(end.to_string());
    }
    json_reply(StatusCode::OK, &resp)
}

fn handle_threads_list(
    ctx: &Arc<AppContext>,
    email: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> HttpReply {
    let max_results = query_first(query, "maxResults")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 500);
    let offset = query_first(query, "pageToken")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let include_spam_trash = query_first(query, "includeSpamTrash") == Some("true");
    let label_ids = query.get("labelIds").cloned().unwrap_or_default();
    let q = query_first(query, "q").unwrap_or("");

    let guard = ctx.state.lock().expect("state lock");
    let acc = &guard.accounts[email];
    let msgs = acc.search_messages(q, &label_ids, include_spam_trash);
    let mut seen = BTreeSet::new();
    let mut threads = Vec::new();
    for m in msgs {
        if seen.insert(m.thread_id.as_str()) {
            threads.push((m.thread_id.as_str(), m.snippet.as_str(), m.history_id));
        }
    }
    let end = offset.saturating_add(max_results).min(threads.len());
    let slice = if offset < threads.len() {
        &threads[offset..end]
    } else {
        &[]
    };
    let mut resp = json!({
        "threads": slice.iter().map(|(tid, snip, hid)| json!({
            "id": tid,
            "snippet": snip,
            "historyId": hid.to_string(),
        })).collect::<Vec<_>>(),
        "resultSizeEstimate": threads.len(),
    });
    if end < threads.len() {
        resp["nextPageToken"] = json!(end.to_string());
    }
    json_reply(StatusCode::OK, &resp)
}

fn handle_message_get(
    ctx: &Arc<AppContext>,
    email: &str,
    mid: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> HttpReply {
    let format = query_first(query, "format").unwrap_or("full");
    let guard = ctx.state.lock().expect("state lock");
    let acc = &guard.accounts[email];
    let Some(msg) = acc.messages.get(mid) else {
        return gmail_error(404, "notFound", "Message not found", None);
    };
    if format == "raw" && acc.faults.withheld_messages.contains(mid) {
        return gmail_error(503, "backendError", "Injected withheld raw fetch", None);
    }
    let mut resp = json!({
        "id": msg.id,
        "threadId": msg.thread_id,
        "labelIds": msg.labels,
        "historyId": msg.history_id.to_string(),
        "internalDate": msg.internal_date_ms.to_string(),
        "sizeEstimate": msg.raw.len(),
        "snippet": msg.snippet,
    });
    if format == "raw" {
        resp["raw"] = json!(URL_SAFE_NO_PAD.encode(&msg.raw));
    }
    json_reply(StatusCode::OK, &resp)
}

async fn handle_message_send(
    ctx: &Arc<AppContext>,
    email: &str,
    body_bytes: &Bytes,
) -> Result<HttpReply, ()> {
    if let Some(fault) = take_refusal(ctx, email, RefusalSlot::Send).await {
        return Ok(refusal_reply(&fault));
    }
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let Some(raw_b64) = body.get("raw").and_then(Value::as_str) else {
        return Ok(gmail_error(400, "invalidArgument", "raw is required", None));
    };
    let Ok(raw) = decode_base64url(raw_b64) else {
        return Ok(gmail_error(
            400,
            "invalidArgument",
            "Invalid base64url raw",
            None,
        ));
    };
    let thread_id = body
        .get("threadId")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let (drop_mode, drop_meta) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        let mode = acc.faults.drop_next_send.take();
        let meta = (mode == Some(DropSendMode::NotApplied)).then(|| acc.meta());
        (mode, meta)
    };
    if let Some(meta) = drop_meta {
        ctx.persist
            .save_delta(email, &meta, &[], &[], &[], &[], &[], &[])
            .await;
        return Err(());
    }

    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        if let Some(ref tid) = thread_id
            && !acc.messages.values().any(|m| m.thread_id == *tid)
        {
            return Ok(gmail_error(
                404,
                "notFound",
                "Requested entity was not found.",
                None,
            ));
        }
        let (msg, event) = acc.store_stamped(&raw, vec!["SENT".to_owned()], thread_id, false);
        let rec = acc.record_sent(&msg, "messages.send");
        (acc.meta(), msg, event, rec)
    };

    ctx.persist
        .save_delta(
            email,
            &saved.0,
            std::slice::from_ref(&saved.1),
            &[],
            &[],
            &[],
            &[saved.2],
            &[saved.3],
        )
        .await;

    if drop_mode == Some(DropSendMode::Applied) {
        return Err(());
    }

    Ok(json_reply(
        StatusCode::OK,
        &json!({
            "id": saved.1.id,
            "threadId": saved.1.thread_id,
            "labelIds": saved.1.labels,
        }),
    ))
}

async fn handle_message_modify(
    ctx: &Arc<AppContext>,
    email: &str,
    mid: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let add = string_array(body.get("addLabelIds"));
    let remove = string_array(body.get("removeLabelIds"));

    let updated = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        if !add.iter().all(|l| acc.defines_label(l)) {
            return gmail_error(400, "invalidArgument", "labelId not found", None);
        }
        let Some((msg, events)) = acc.modify_labels(mid, &add, &remove) else {
            return gmail_error(404, "notFound", "Message not found", None);
        };
        (acc.meta(), msg, events)
    };
    ctx.persist
        .save_delta(
            email,
            &updated.0,
            std::slice::from_ref(&updated.1),
            &[],
            &[],
            &[],
            &updated.2,
            &[],
        )
        .await;
    json_reply(
        StatusCode::OK,
        &json!({
            "id": updated.1.id,
            "threadId": updated.1.thread_id,
            "labelIds": updated.1.labels,
        }),
    )
}

async fn handle_batch_modify(ctx: &Arc<AppContext>, email: &str, body_bytes: &Bytes) -> HttpReply {
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let ids = string_array(body.get("ids"));
    let add = string_array(body.get("addLabelIds"));
    let remove = string_array(body.get("removeLabelIds"));

    let delta = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        if !add.iter().all(|l| acc.defines_label(l)) {
            return gmail_error(400, "invalidArgument", "labelId not found", None);
        }
        let mut msgs = Vec::new();
        let mut hist = Vec::new();
        for mid in &ids {
            if let Some((msg, evs)) = acc.modify_labels(mid, &add, &remove) {
                msgs.push(msg);
                hist.extend(evs);
            }
        }
        (acc.meta(), msgs, hist)
    };
    ctx.persist
        .save_delta(email, &delta.0, &delta.1, &[], &[], &[], &delta.2, &[])
        .await;
    no_content_reply()
}

async fn handle_message_delete(ctx: &Arc<AppContext>, email: &str, mid: &str) -> HttpReply {
    if let Some(reply) = check_delete_guards(ctx, email).await {
        return reply;
    }
    let delta = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        let Some(event) = acc.delete_message(mid) else {
            return gmail_error(404, "notFound", "Message not found", None);
        };
        (acc.meta(), event)
    };
    ctx.persist
        .save_delta(
            email,
            &delta.0,
            &[],
            &[mid.to_owned()],
            &[],
            &[],
            &[delta.1],
            &[],
        )
        .await;
    no_content_reply()
}

async fn handle_batch_delete(ctx: &Arc<AppContext>, email: &str, body_bytes: &Bytes) -> HttpReply {
    if let Some(reply) = check_delete_guards(ctx, email).await {
        return reply;
    }
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let ids = string_array(body.get("ids"));
    let delta = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        let mut deleted = Vec::new();
        let mut hist = Vec::new();
        for mid in ids {
            if let Some(ev) = acc.delete_message(&mid) {
                deleted.push(mid);
                hist.push(ev);
            }
        }
        (acc.meta(), deleted, hist)
    };
    ctx.persist
        .save_delta(email, &delta.0, &[], &delta.1, &[], &[], &delta.2, &[])
        .await;
    no_content_reply()
}

async fn check_delete_guards(ctx: &Arc<AppContext>, email: &str) -> Option<HttpReply> {
    let full_scope = {
        let guard = ctx.state.lock().expect("state lock");
        guard.accounts[email].faults.full_scope
    };
    if !full_scope {
        return Some(json_reply(
            StatusCode::FORBIDDEN,
            &json!({
                "error": {
                    "code": 403,
                    "message": "Request had insufficient authentication scopes.",
                    "errors": [{ "reason": "insufficientPermissions" }],
                    "status": "PERMISSION_DENIED",
                    "details": [{ "reason": "ACCESS_TOKEN_SCOPE_INSUFFICIENT" }],
                }
            }),
        ));
    }
    if let Some(fault) = take_refusal(ctx, email, RefusalSlot::Delete).await {
        return Some(refusal_reply(&fault));
    }
    None
}

fn handle_drafts_list(
    ctx: &Arc<AppContext>,
    email: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> HttpReply {
    let max_results = query_first(query, "maxResults")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(500)
        .clamp(1, 500);
    let offset = query_first(query, "pageToken")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let include_spam_trash = query_first(query, "includeSpamTrash") == Some("true");

    let guard = ctx.state.lock().expect("state lock");
    let acc = &guard.accounts[email];
    let items: Vec<Value> = acc
        .drafts
        .iter()
        .filter_map(|(did, mid)| {
            let msg = acc.messages.get(mid)?;
            if !include_spam_trash && msg.labels.iter().any(|l| l == "TRASH" || l == "SPAM") {
                return None;
            }
            Some(json!({
                "id": did,
                "message": { "id": mid, "threadId": msg.thread_id },
            }))
        })
        .collect();
    let end = offset.saturating_add(max_results).min(items.len());
    let slice = if offset < items.len() {
        &items[offset..end]
    } else {
        &[]
    };
    let mut resp = json!({
        "drafts": slice,
        "resultSizeEstimate": items.len(),
    });
    if end < items.len() {
        resp["nextPageToken"] = json!(end.to_string());
    }
    json_reply(StatusCode::OK, &resp)
}

fn handle_draft_get(
    ctx: &Arc<AppContext>,
    email: &str,
    did: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> HttpReply {
    let raw = query_first(query, "format") == Some("raw");
    let guard = ctx.state.lock().expect("state lock");
    let acc = &guard.accounts[email];
    draft_reply(acc, did, raw)
}

async fn handle_draft_create(ctx: &Arc<AppContext>, email: &str, body_bytes: &Bytes) -> HttpReply {
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let message = body.get("message").cloned().unwrap_or_else(|| json!({}));
    let Some(raw_b64) = message.get("raw").and_then(Value::as_str) else {
        return gmail_error(400, "invalidArgument", "message.raw is required", None);
    };
    let Ok(raw) = decode_base64url(raw_b64) else {
        return gmail_error(400, "invalidArgument", "Invalid base64url raw", None);
    };
    let thread_id = message
        .get("threadId")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        let (did, msg, ev) = acc.create_draft(&raw, thread_id, false);
        let reply = draft_reply(acc, &did, false);
        (acc.meta(), did, msg, ev, reply)
    };
    ctx.persist
        .save_delta(
            email,
            &saved.0,
            std::slice::from_ref(&saved.2),
            &[],
            &[(saved.1, saved.2.id.clone())],
            &[],
            &[saved.3],
            &[],
        )
        .await;
    saved.4
}

async fn handle_draft_update(
    ctx: &Arc<AppContext>,
    email: &str,
    did: &str,
    body_bytes: &Bytes,
) -> HttpReply {
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let message = body.get("message").cloned().unwrap_or_else(|| json!({}));
    let Some(raw_b64) = message.get("raw").and_then(Value::as_str) else {
        return gmail_error(400, "invalidArgument", "message.raw is required", None);
    };
    let Ok(raw) = decode_base64url(raw_b64) else {
        return gmail_error(400, "invalidArgument", "Invalid base64url raw", None);
    };
    let thread_id = message
        .get("threadId")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        let Some((old_mid, msg, evs)) = acc.update_draft(did, &raw, thread_id, false) else {
            return gmail_error(404, "notFound", "Requested entity was not found.", None);
        };
        let reply = draft_reply(acc, did, false);
        (acc.meta(), old_mid, msg, evs, reply)
    };
    ctx.persist
        .save_delta(
            email,
            &saved.0,
            std::slice::from_ref(&saved.2),
            &[saved.1],
            &[(did.to_owned(), saved.2.id.clone())],
            &[],
            &saved.3,
            &[],
        )
        .await;
    saved.4
}

async fn handle_draft_delete(ctx: &Arc<AppContext>, email: &str, did: &str) -> HttpReply {
    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        let Some((old_mid, ev)) = acc.delete_draft(did) else {
            return gmail_error(404, "notFound", "Requested entity was not found.", None);
        };
        (acc.meta(), old_mid, ev)
    };
    let hist: Vec<_> = saved.2.into_iter().collect();
    ctx.persist
        .save_delta(
            email,
            &saved.0,
            &[],
            &[saved.1],
            &[],
            &[did.to_owned()],
            &hist,
            &[],
        )
        .await;
    no_content_reply()
}

async fn handle_draft_send(
    ctx: &Arc<AppContext>,
    email: &str,
    body_bytes: &Bytes,
) -> Result<HttpReply, ()> {
    if let Some(fault) = take_refusal(ctx, email, RefusalSlot::Send).await {
        return Ok(refusal_reply(&fault));
    }
    let body: Value = serde_json::from_slice(body_bytes).unwrap_or_else(|_| json!({}));
    let did = body
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();

    let (drop_mode, drop_meta) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        let mode = acc.faults.drop_next_send.take();
        let meta = (mode == Some(DropSendMode::NotApplied)).then(|| acc.meta());
        (mode, meta)
    };
    if let Some(meta) = drop_meta {
        ctx.persist
            .save_delta(email, &meta, &[], &[], &[], &[], &[], &[])
            .await;
        return Err(());
    }

    let saved = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email).expect("account");
        let Some((old_mid, sent_msg, evs, rec)) = acc.send_draft(&did, false) else {
            return Ok(gmail_error(
                404,
                "notFound",
                "Requested entity was not found.",
                None,
            ));
        };
        (acc.meta(), old_mid, sent_msg, evs, rec)
    };
    ctx.persist
        .save_delta(
            email,
            &saved.0,
            std::slice::from_ref(&saved.2),
            &[saved.1],
            &[],
            &[did],
            &saved.3,
            &[saved.4],
        )
        .await;

    if drop_mode == Some(DropSendMode::Applied) {
        return Err(());
    }

    Ok(json_reply(
        StatusCode::OK,
        &json!({
            "id": saved.2.id,
            "threadId": saved.2.thread_id,
            "labelIds": saved.2.labels,
        }),
    ))
}

fn draft_reply(acc: &crate::state::AccountState, did: &str, include_raw: bool) -> HttpReply {
    let Some(mid) = acc.drafts.get(did) else {
        return gmail_error(404, "notFound", "Requested entity was not found.", None);
    };
    let Some(msg) = acc.messages.get(mid) else {
        return gmail_error(404, "notFound", "Requested entity was not found.", None);
    };
    let mut message = json!({
        "id": msg.id,
        "threadId": msg.thread_id,
        "labelIds": msg.labels,
    });
    if include_raw {
        message["raw"] = json!(URL_SAFE_NO_PAD.encode(&msg.raw));
    }
    json_reply(StatusCode::OK, &json!({ "id": did, "message": message }))
}

enum RefusalSlot {
    Blanket,
    Send,
    Delete,
}

async fn take_refusal(
    ctx: &Arc<AppContext>,
    email: &str,
    slot: RefusalSlot,
) -> Option<RefusalFault> {
    let (fault, changed_meta) = {
        let mut guard = ctx.state.lock().expect("state lock");
        let acc = guard.accounts.get_mut(email)?;
        let target = match slot {
            RefusalSlot::Blanket => &mut acc.faults.refuse,
            RefusalSlot::Send => &mut acc.faults.send_refusal,
            RefusalSlot::Delete => &mut acc.faults.delete_failure,
        };
        let current = target.clone()?;
        let mut changed = false;
        if let Some(rem) = current.remaining {
            changed = true;
            if rem <= 1 {
                *target = None;
            } else if let Some(slot_mut) = target.as_mut() {
                slot_mut.remaining = Some(rem - 1);
            }
        }
        (Some(current), changed.then(|| acc.meta()))
    };
    if let Some(meta) = changed_meta {
        ctx.persist
            .save_delta(email, &meta, &[], &[], &[], &[], &[], &[])
            .await;
    }
    fault
}

fn refusal_reply(fault: &RefusalFault) -> HttpReply {
    gmail_error(
        fault.status,
        &fault.reason,
        &fault.message,
        fault.retry_after_s,
    )
}

pub(crate) fn gmail_error(
    status: u16,
    reason: &str,
    message: &str,
    retry_after_s: Option<u64>,
) -> HttpReply {
    let mut reply = json_reply(
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        &json!({
            "error": {
                "code": status,
                "message": message,
                "errors": [{ "reason": reason, "message": message }],
            }
        }),
    );
    if let Some(secs) = retry_after_s {
        reply
            .headers
            .push(("retry-after".to_owned(), secs.to_string()));
    }
    reply
}

fn decode_base64url(raw: &str) -> Result<Vec<u8>, base64::DecodeError> {
    URL_SAFE_NO_PAD
        .decode(raw.trim())
        .or_else(|_| URL_SAFE.decode(raw.trim()))
}

fn string_array(val: Option<&Value>) -> Vec<String> {
    val.and_then(Value::as_array).map_or_default(|arr| {
        arr.iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect()
    })
}

pub(crate) fn parse_query(query: &str) -> BTreeMap<String, Vec<String>> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
        map.entry(k.into_owned()).or_default().push(v.into_owned());
    }
    map
}

pub(crate) fn query_first<'a>(
    query: &'a BTreeMap<String, Vec<String>>,
    key: &str,
) -> Option<&'a str> {
    query.get(key).and_then(|v| v.first()).map(String::as_str)
}

pub(crate) fn pct_decode(segment: &str) -> String {
    let mut out = Vec::with_capacity(segment.len());
    let bytes = segment.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn jitter_for(max_ms: u64) -> u64 {
    if max_ms == 0 {
        return 0;
    }
    (now_ms().unsigned_abs() ^ 0x517C_C1B7_2722_0A95) % max_ms
}
