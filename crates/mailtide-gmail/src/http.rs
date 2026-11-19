//! Plain HTTP request and response values, independent of any HTTP client.

use std::fmt::Write as _;

/// An HTTP method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// GET.
    Get,
    /// POST.
    Post,
}

impl Method {
    /// The method's name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

/// The Gmail API's origin.
pub const ORIGIN: &str = "https://gmail.googleapis.com";

/// A request to send to [`ORIGIN`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// The method.
    pub method: Method,
    /// Path, e.g. `/gmail/v1/users/me/history`.
    pub path: String,
    /// Query parameters, in order (unencoded).
    pub query: Vec<(String, String)>,
    /// JSON body, if any.
    pub body: Option<Vec<u8>>,
}

impl HttpRequest {
    /// The path with its percent-encoded query string.
    #[must_use]
    pub fn path_and_query(&self) -> String {
        if self.query.is_empty() {
            return self.path.clone();
        }
        let query: Vec<String> = self
            .query
            .iter()
            .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
            .collect();
        format!("{}?{}", self.path, query.join("&"))
    }
}

/// A response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// Status code.
    pub status: u16,
    /// Header pairs, lowercased names.
    pub headers: Vec<(String, String)>,
    /// Body bytes.
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// The first header named `name` (case-insensitive).
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Whether the status is 2xx.
    #[must_use]
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// Percent-encode a path segment or query component (RFC 3986 unreserved
/// characters pass through).
#[must_use]
pub fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(char::from(byte));
            }
            other => {
                let _ = write!(out, "%{other:02X}");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_are_percent_encoded() {
        let request = HttpRequest {
            method: Method::Get,
            path: "/gmail/v1/users/me/messages".into(),
            query: vec![("q".into(), "a b/c".into())],
            body: None,
        };
        assert_eq!(
            request.path_and_query(),
            "/gmail/v1/users/me/messages?q=a%20b%2Fc"
        );
    }
}
