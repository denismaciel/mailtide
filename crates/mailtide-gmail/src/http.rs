//! Plain HTTP request and response values, independent of any HTTP client.

use std::fmt::Write as _;

/// An HTTP method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// GET.
    Get,
    /// POST.
    Post,
    /// PUT: replaces a resource whole (e.g. `drafts.update`).
    Put,
    /// PATCH: changes only the fields it names (e.g. `labels.patch`).
    Patch,
    /// DELETE: Gmail answers 204 with an empty body.
    Delete,
}

impl Method {
    /// The method's name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
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
    /// JSON body, if any, whatever the method.
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

    #[test]
    fn methods_have_their_wire_names() {
        let names: Vec<&str> = [
            Method::Get,
            Method::Post,
            Method::Put,
            Method::Patch,
            Method::Delete,
        ]
        .into_iter()
        .map(Method::as_str)
        .collect();
        assert_eq!(names, ["GET", "POST", "PUT", "PATCH", "DELETE"]);
    }

    #[test]
    fn an_empty_answer_to_a_delete_is_a_success() {
        let request = HttpRequest {
            method: Method::Delete,
            path: format!("/gmail/v1/users/me/labels/{}", encode("Label 1")),
            query: Vec::new(),
            body: None,
        };
        assert_eq!(
            request.path_and_query(),
            "/gmail/v1/users/me/labels/Label%201"
        );
        let answer = HttpResponse {
            status: 204,
            headers: Vec::new(),
            body: Vec::new(),
        };
        assert!(answer.is_success());
    }
}
