//! Gmail batch requests (`multipart/mixed`), with membership validated up
//! front and responses correlated to requests by part id (review #18).
//!
//! At most 50 calls per batch (Google's recommendation; larger batches trip
//! the per-user concurrency limit). Raw message gets are never batched.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::http::{HttpRequest, HttpResponse, Method};

/// The largest batch mailtide sends.
pub const MAX_BATCH: usize = 50;

const BOUNDARY: &str = "mailtide_batch_boundary";

/// A validated batch: 1 to [`MAX_BATCH`] requests, none of them raw gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchPlan {
    parts: Vec<HttpRequest>,
}

/// Why a batch couldn't be built or read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchError {
    /// No requests.
    Empty,
    /// More than [`MAX_BATCH`] requests.
    TooLarge(usize),
    /// A raw message get (too large to batch).
    RawNotBatchable,
    /// The response isn't `multipart/mixed` with a boundary.
    NotMultipart,
    /// A part couldn't be parsed.
    MalformedPart(String),
    /// A request has no response.
    Missing(usize),
    /// A request got two responses.
    Duplicate(usize),
    /// A response names no request of this batch.
    Unexpected(String),
}

impl BatchPlan {
    /// Validate `requests` as one batch.
    pub fn new(requests: Vec<HttpRequest>) -> Result<Self, BatchError> {
        match requests.len() {
            0 => Err(BatchError::Empty),
            n if n > MAX_BATCH => Err(BatchError::TooLarge(n)),
            _ if requests
                .iter()
                .any(|r| r.query.iter().any(|(k, v)| k == "format" && v == "raw")) =>
            {
                Err(BatchError::RawNotBatchable)
            }
            _ => Ok(Self { parts: requests }),
        }
    }

    /// The requests, in part order.
    #[must_use]
    pub fn requests(&self) -> &[HttpRequest] {
        &self.parts
    }
}

/// A batch ready to POST to `/batch/gmail/v1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedBatch {
    /// The `Content-Type` header, with boundary.
    pub content_type: String,
    /// The multipart body.
    pub body: Vec<u8>,
}

/// The batch endpoint's path.
pub const BATCH_PATH: &str = "/batch/gmail/v1";

/// Encode a plan. Part `i` has `Content-ID: <item-i>`.
#[must_use]
pub fn encode(plan: &BatchPlan) -> EncodedBatch {
    let mut body = String::new();
    for (index, request) in plan.parts.iter().enumerate() {
        let _ = write!(
            body,
            "--{BOUNDARY}\r\nContent-Type: application/http\r\nContent-ID: <item-{index}>\r\n\r\n"
        );
        let _ = write!(
            body,
            "{} {} HTTP/1.1\r\n",
            request.method.as_str(),
            request.path_and_query()
        );
        match (&request.method, &request.body) {
            (Method::Get, _) | (_, None) => body.push_str("\r\n"),
            (Method::Post | Method::Put | Method::Patch | Method::Delete, Some(json)) => {
                body.push_str("Content-Type: application/json\r\n\r\n");
                body.push_str(&String::from_utf8_lossy(json));
                body.push_str("\r\n");
            }
        }
    }
    let _ = write!(body, "--{BOUNDARY}--\r\n");
    EncodedBatch {
        content_type: format!("multipart/mixed; boundary={BOUNDARY}"),
        body: body.into_bytes(),
    }
}

/// Split a batch response into one response per request, in plan order.
pub fn decode(plan: &BatchPlan, response: &HttpResponse) -> Result<Vec<HttpResponse>, BatchError> {
    let content_type = response
        .header("content-type")
        .ok_or(BatchError::NotMultipart)?;
    let boundary = content_type
        .split(';')
        .map(str::trim)
        .find_map(|param| param.strip_prefix("boundary="))
        .map(|b| b.trim_matches('"').to_owned())
        .ok_or(BatchError::NotMultipart)?;
    let text = String::from_utf8_lossy(&response.body);
    let delimiter = format!("--{boundary}");
    let mut by_index: BTreeMap<usize, HttpResponse> = BTreeMap::new();
    for raw in text.split(delimiter.as_str()).skip(1) {
        let part = raw.trim_start_matches("\r\n").trim_start_matches('\n');
        if part.starts_with("--") || part.trim().is_empty() {
            continue;
        }
        let (headers, rest) =
            split_head(part).ok_or_else(|| BatchError::MalformedPart(first_line(part)))?;
        let id = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-id"))
            .map(|(_, v)| v.trim_matches(|c| c == '<' || c == '>').to_owned())
            .ok_or_else(|| BatchError::MalformedPart("part without Content-ID".into()))?;
        let index = id
            .strip_prefix("response-item-")
            .and_then(|n| n.parse::<usize>().ok())
            .filter(|n| *n < plan.parts.len())
            .ok_or_else(|| BatchError::Unexpected(id.clone()))?;
        let inner = parse_http(rest)?;
        if by_index.insert(index, inner).is_some() {
            return Err(BatchError::Duplicate(index));
        }
    }
    (0..plan.parts.len())
        .map(|index| by_index.remove(&index).ok_or(BatchError::Missing(index)))
        .collect()
}

fn first_line(text: &str) -> String {
    text.lines()
        .next()
        .unwrap_or_default()
        .chars()
        .take(80)
        .collect()
}

/// Headers and the rest of a MIME part or HTTP message.
fn split_head(text: &str) -> Option<(Vec<(String, String)>, &str)> {
    let (head, rest) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))?;
    let headers = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    Some((headers, rest))
}

fn parse_http(text: &str) -> Result<HttpResponse, BatchError> {
    let status_line = first_line(text);
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| BatchError::MalformedPart(status_line.clone()))?;
    let after_status = text.split_once('\n').map_or("", |(_, rest)| rest);
    let (headers, body) = match split_head(after_status) {
        Some((headers, body)) => (headers, body),
        None => (Vec::new(), after_status),
    };
    Ok(HttpResponse {
        status,
        headers,
        body: body
            .trim_end_matches("\r\n")
            .trim_end_matches('\n')
            .as_bytes()
            .to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(path: &str) -> HttpRequest {
        HttpRequest {
            method: Method::Get,
            path: path.into(),
            query: vec![("format".into(), "minimal".into())],
            body: None,
        }
    }

    #[test]
    fn plans_are_validated() {
        assert_eq!(BatchPlan::new(Vec::new()), Err(BatchError::Empty));
        assert_eq!(
            BatchPlan::new(vec![get("/x"); 51]),
            Err(BatchError::TooLarge(51))
        );
        let mut raw = get("/x");
        raw.query = vec![("format".into(), "raw".into())];
        assert_eq!(BatchPlan::new(vec![raw]), Err(BatchError::RawNotBatchable));
    }

    #[test]
    fn encodes_one_part_per_request() {
        let plan = BatchPlan::new(vec![get("/a"), get("/b")]).expect("valid");
        let encoded = encode(&plan);
        let body = String::from_utf8(encoded.body).expect("utf8");
        assert!(body.contains("Content-ID: <item-0>\r\n\r\nGET /a?format=minimal HTTP/1.1"));
        assert!(body.contains("Content-ID: <item-1>"));
        assert!(body.ends_with(&format!("--{BOUNDARY}--\r\n")));
    }

    #[test]
    fn a_body_goes_with_any_method_but_get() {
        let patch = HttpRequest {
            method: Method::Patch,
            path: "/l".into(),
            query: Vec::new(),
            body: Some(b"{\"name\":\"x\"}".to_vec()),
        };
        let delete = HttpRequest {
            method: Method::Delete,
            path: "/d".into(),
            query: Vec::new(),
            body: None,
        };
        let plan = BatchPlan::new(vec![patch, delete]).expect("valid");
        let body = String::from_utf8(encode(&plan).body).expect("utf8");
        assert!(body.contains(
            "PATCH /l HTTP/1.1\r\nContent-Type: application/json\r\n\r\n{\"name\":\"x\"}\r\n"
        ));
        assert!(body.contains("DELETE /d HTTP/1.1\r\n\r\n--"));
        assert!(body.ends_with(&format!("--{BOUNDARY}--\r\n")));
    }

    fn response(body: &str) -> HttpResponse {
        HttpResponse {
            status: 200,
            headers: vec![(
                "content-type".into(),
                "multipart/mixed; boundary=batch_x".into(),
            )],
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn decodes_out_of_order_parts_into_plan_order() {
        let plan = BatchPlan::new(vec![get("/a"), get("/b")]).expect("valid");
        let body = "--batch_x\r\nContent-Type: application/http\r\nContent-ID: <response-item-1>\r\n\r\nHTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n\r\n{\"error\":{}}\r\n--batch_x\r\nContent-Type: application/http\r\nContent-ID: <response-item-0>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"id\":\"a\"}\r\n--batch_x--\r\n";
        let parts = decode(&plan, &response(body)).expect("decodes");
        assert_eq!(parts[0].status, 200);
        assert_eq!(parts[0].body, b"{\"id\":\"a\"}");
        assert_eq!(parts[1].status, 404);
    }

    #[test]
    fn missing_and_duplicate_parts_are_errors() {
        let plan = BatchPlan::new(vec![get("/a"), get("/b")]).expect("valid");
        let one = "--batch_x\r\nContent-ID: <response-item-0>\r\n\r\nHTTP/1.1 200 OK\r\n\r\n{}\r\n--batch_x--\r\n";
        assert_eq!(decode(&plan, &response(one)), Err(BatchError::Missing(1)));
        let twice = format!("{}{}", &one[..one.len() - 13], one);
        assert_eq!(
            decode(&plan, &response(&twice)),
            Err(BatchError::Duplicate(0))
        );
    }
}
