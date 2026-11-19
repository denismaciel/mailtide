//! Minimal header extraction from raw messages: only what the engine stores.
//! Encoded words (RFC 2047) are left as sent; full parsing is
//! `mailtide-mime`'s job.

use mailtide::gmail::HeaderFacts;

/// Extract header facts from an RFC 822 message (the header block only).
#[must_use]
pub fn facts(rfc822: &[u8], snippet: Option<&str>) -> HeaderFacts {
    let headers = header_block(rfc822);
    let first = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    };
    let addresses = |name: &str| {
        headers
            .iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(name))
            .flat_map(|(_, v)| split_addresses(v))
            .collect()
    };
    HeaderFacts {
        rfc822_message_id: first("message-id"),
        in_reply_to: first("in-reply-to"),
        references: first("references")
            .map(|v| v.split_whitespace().map(str::to_owned).collect())
            .unwrap_or_default(),
        from: first("from"),
        to: addresses("to"),
        cc: addresses("cc"),
        subject: first("subject"),
        snippet: snippet.map(str::to_owned),
    }
}

/// Unfolded `(name, value)` pairs up to the first blank line.
fn header_block(rfc822: &[u8]) -> Vec<(String, String)> {
    let text = String::from_utf8_lossy(rfc822);
    let mut headers: Vec<(String, String)> = Vec::new();
    for line in text.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            break;
        }
        if line.starts_with([' ', '\t']) {
            if let Some((_, value)) = headers.last_mut() {
                value.push(' ');
                value.push_str(line.trim());
            }
        } else if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_owned(), value.trim().to_owned()));
        }
    }
    headers
}

/// Split an address list on commas outside quotes and angle brackets.
fn split_addresses(value: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut angle = 0u32;
    for c in value.chars() {
        match c {
            '"' => quoted = !quoted,
            '<' => angle += 1,
            '>' => angle = angle.saturating_sub(1),
            ',' if !quoted && angle == 0 => {
                if !current.trim().is_empty() {
                    out.push(current.trim().to_owned());
                }
                current.clear();
                continue;
            }
            _ => {}
        }
        current.push(c);
    }
    if !current.trim().is_empty() {
        out.push(current.trim().to_owned());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_unfolded_headers_and_address_lists() {
        let raw = b"Message-ID: <a@x>\r\nIn-Reply-To: <b@x>\r\nReferences: <c@x>\r\n <b@x>\r\nFrom: \"Doe, Jane\" <jane@x>\r\nTo: a@x, \"B, Bob\" <b@x>\r\nSubject: Quote\r\n renewal\r\n\r\nTo: not-a-header@x\r\n";
        let facts = facts(raw, Some("snip"));
        assert_eq!(facts.rfc822_message_id.as_deref(), Some("<a@x>"));
        assert_eq!(facts.references, vec!["<c@x>", "<b@x>"]);
        assert_eq!(facts.from.as_deref(), Some("\"Doe, Jane\" <jane@x>"));
        assert_eq!(facts.to, vec!["a@x", "\"B, Bob\" <b@x>"]);
        assert_eq!(facts.subject.as_deref(), Some("Quote renewal"));
        assert_eq!(facts.snippet.as_deref(), Some("snip"));
    }
}
