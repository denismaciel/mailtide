//! Debounced push webhook notifier for external mailbox mutations.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

/// Debounces and posts `{emailAddress, historyId}` notifications to a configured webhook URL.
#[derive(Clone, Debug)]
pub(crate) struct PushNotifier {
    url: Option<String>,
    debounce: Duration,
    client: reqwest::Client,
    pending: Arc<Mutex<BTreeMap<String, u64>>>,
}

impl PushNotifier {
    /// Build a notifier posting to `url` (or a no-op if `None`).
    pub(crate) fn new(url: Option<String>, debounce: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("reqwest client");
        Self {
            url,
            debounce,
            client,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Configured webhook URL, if any.
    pub(crate) fn url(&self) -> Option<&str> {
        self.url.as_deref()
    }

    /// Schedule a debounced notification for `(email, history_id)`.
    pub(crate) fn notify(&self, email: String, history_id: u64) {
        let Some(url) = self.url.clone() else {
            return;
        };
        let should_spawn = {
            let mut map = self.pending.lock().expect("pending lock");
            let existed = map.contains_key(&email);
            map.insert(email.clone(), history_id);
            !existed
        };
        if !should_spawn {
            return;
        }
        let pending = Arc::clone(&self.pending);
        let client = self.client.clone();
        let debounce = self.debounce;
        tokio::spawn(async move {
            tokio::time::sleep(debounce).await;
            let latest = {
                let mut map = pending.lock().expect("pending lock");
                map.remove(&email)
            };
            if let Some(hid) = latest {
                let payload = json!({
                    "emailAddress": email,
                    "historyId": hid.to_string(),
                })
                .to_string();
                let _ = client
                    .post(&url)
                    .header("content-type", "application/json")
                    .body(payload)
                    .send()
                    .await;
            }
        });
    }

    /// Immediately fire a push webhook (used by explicit `/_control/accounts/{email}/push`).
    pub(crate) async fn push_now(&self, email: &str, history_id: u64) -> Result<u16, String> {
        let Some(url) = self.url.as_deref() else {
            return Err("no push_url configured on fake server".to_owned());
        };
        let payload = json!({
            "emailAddress": email,
            "historyId": history_id.to_string(),
        })
        .to_string();
        let resp = self
            .client
            .post(url)
            .header("content-type", "application/json")
            .body(payload)
            .send()
            .await
            .map_err(|e| format!("push request failed: {e}"))?;
        Ok(resp.status().as_u16())
    }
}
