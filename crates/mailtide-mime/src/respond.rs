//! Derive reply and forward inputs from an original RFC 5322 message.

use std::collections::HashSet;

use mail_parser::{HeaderName, Headers, Message, MessagePart, PartFlags, PartKind};
use mailtide::gmail::PartRef;

use crate::parts::{Leaf, PartRole, leaves};
use crate::{Address, Attachment, MessageId, MimeError, parse_addresses, parser, unfold};

/// Suggested headers and plain text for a reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyPlan {
    /// The original subject with one `Re:` prefix.
    pub subject: String,
    /// Suggested main recipients.
    pub to: Vec<Address>,
    /// Suggested copies for reply-all.
    pub cc: Vec<Address>,
    /// The original's Message-ID, if usable.
    pub in_reply_to: Option<MessageId>,
    /// The original's reference chain followed by its id.
    pub references: Vec<MessageId>,
    /// Original plain text for the host to quote, with flowed lines joined.
    pub quote_text: Option<String>,
}

/// Display data for a forwarded-message header block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardHeaders {
    /// The original authors.
    pub from: Vec<Address>,
    /// The original Date field, unfolded.
    pub date: Option<String>,
    /// The original decoded subject.
    pub subject: Option<String>,
    /// The original To recipients.
    pub to: Vec<Address>,
    /// The original Cc recipients.
    pub cc: Vec<Address>,
}

/// One original part available to carry forward.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardPart {
    /// The part's filename, media type, Content-ID and transfer-decoded bytes.
    pub attachment: Attachment,
    /// The Content-Type parameters in document order.
    pub content_type_parameters: Vec<(String, String)>,
    /// Its byte range in the original message.
    pub part: PartRef,
    /// Whether the original renders it inline.
    pub inline: bool,
}

/// Suggested headers, original content and parts for a forward.
/// The host chooses the recipients and assembles the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardPlan {
    /// The original subject with one `Fwd:` prefix.
    pub subject: String,
    /// The original's reference chain followed by its id.
    pub references: Vec<MessageId>,
    /// Original display headers.
    pub original: ForwardHeaders,
    /// Original plain text for the host to quote, with flowed lines joined.
    pub quote_text: Option<String>,
    /// Original HTML, if present, with no layout or link rewriting.
    pub html: Option<String>,
    /// Original attachments and inline parts, in document order.
    pub parts: Vec<ForwardPart>,
}

/// Derive a reply or reply-all without addressing the host's own mailboxes.
/// The address list can include the host's primary mailbox and aliases.
pub fn reply_plan(raw: &[u8], own_addresses: &[&str], all: bool) -> Result<ReplyPlan, MimeError> {
    let original = original(raw)?;
    let headers = original.headers();
    let from = addresses(&headers, HeaderName::From);
    let mut seen: HashSet<String> = own_addresses
        .iter()
        .map(|email| email.trim().to_ascii_lowercase())
        .collect();
    let authors = if from.is_empty() {
        addresses(&headers, HeaderName::Sender)
    } else {
        from
    };
    let own_message = authors
        .iter()
        .any(|address| seen.contains(&address.email.to_ascii_lowercase()));
    let mut to = if own_message {
        Vec::new()
    } else {
        let reply_to = addresses(&headers, HeaderName::ReplyTo);
        if reply_to.is_empty() {
            authors
        } else {
            reply_to
        }
    };
    if all || own_message {
        to.extend(addresses(&headers, HeaderName::To));
    }
    let mut cc = if all {
        addresses(&headers, HeaderName::Cc)
    } else {
        Vec::new()
    };
    for list in [&mut to, &mut cc] {
        list.retain(|address| seen.insert(address.email.to_ascii_lowercase()));
    }
    let (in_reply_to, references) = threading(&original);
    Ok(ReplyPlan {
        subject: subject(original.subject(), "Re", &["re"]),
        to,
        cc,
        in_reply_to,
        references,
        quote_text: quote_text(&original),
    })
}

/// Derive a forward from the original, failing if a carried part is unreadable.
pub fn forward_plan(raw: &[u8]) -> Result<ForwardPlan, MimeError> {
    let original = original(raw)?;
    let headers = original.headers();
    let root = original.root();
    if root.parts().any(|part| {
        part.is_multipart()
            && part.flags().intersects(
                PartFlags::LIMIT_REACHED | PartFlags::MISSING_DELIMITER | PartFlags::UNTERMINATED,
            )
    }) {
        return Err(MimeError("incomplete original multipart".into()));
    }
    let parts = leaves(&original, root)
        .into_iter()
        .filter(|leaf| matches!(leaf.role, PartRole::Attached | PartRole::Inline))
        .map(forward_part)
        .collect::<Result<Vec<_>, _>>()?;
    let html = original
        .html_body()
        .find(|part| matches!(part.kind(), PartKind::Html))
        .and_then(|part| part.text().map(std::borrow::Cow::into_owned));
    Ok(ForwardPlan {
        subject: subject(original.subject(), "Fwd", &["re", "fw", "fwd"]),
        references: threading(&original).1,
        original: ForwardHeaders {
            from: addresses(&headers, HeaderName::From),
            date: headers
                .all(HeaderName::Date)
                .next()
                .map(|header| unfold(header.raw_value())),
            subject: original.subject().map(str::to_owned),
            to: addresses(&headers, HeaderName::To),
            cc: addresses(&headers, HeaderName::Cc),
        },
        quote_text: quote_text(&original),
        html,
        parts,
    })
}

fn original(raw: &[u8]) -> Result<Message<'_>, MimeError> {
    parser()
        .parse(raw)
        .ok_or_else(|| MimeError("not an RFC 5322 message".into()))
}

fn addresses(headers: &Headers<'_>, name: HeaderName<'_>) -> Vec<Address> {
    headers
        .all(name)
        .flat_map(|header| parse_addresses(&unfold(header.raw_value())))
        .collect()
}

fn subject(original: Option<&str>, prefix: &str, strip: &[&str]) -> String {
    let mut text = original.unwrap_or_default().trim();
    while let Some((head, rest)) = text.split_once(':') {
        if !strip
            .iter()
            .any(|tag| head.trim_end().eq_ignore_ascii_case(tag))
        {
            break;
        }
        text = rest.trim_start();
    }
    if text.is_empty() {
        format!("{prefix}:")
    } else {
        format!("{prefix}: {text}")
    }
}

fn threading(message: &Message<'_>) -> (Option<MessageId>, Vec<MessageId>) {
    let Some(parent) = message
        .message_id()
        .and_then(|id| MessageId::parse(id).ok())
    else {
        return (None, Vec::new());
    };
    let mut references = message
        .references()
        .into_iter()
        .flat_map(|ids| ids.iter())
        .filter_map(|id| MessageId::parse(id).ok())
        .collect::<Vec<_>>();
    if references.is_empty() {
        references.extend(
            message
                .in_reply_to()
                .into_iter()
                .flat_map(|ids| ids.iter())
                .filter_map(|id| MessageId::parse(id).ok()),
        );
    }
    references.retain(|id| id != &parent);
    references.push(parent.clone());
    (Some(parent), references)
}

fn quote_text(message: &Message<'_>) -> Option<String> {
    let part = message.text_body().next()?;
    let text = message.body_text(0)?;
    if matches!(part.kind(), PartKind::Text)
        && let Some(content_type) = part.content_type()
        && content_type
            .attribute("format")
            .is_some_and(|value| value.eq_ignore_ascii_case("flowed"))
    {
        let delsp = content_type
            .attribute("delsp")
            .is_some_and(|value| value.eq_ignore_ascii_case("yes"));
        return Some(unflow(&text, delsp));
    }
    Some(text.into_owned())
}

/// Prefix each logical plain-text line with another level of quoting.
#[must_use]
pub fn quote_plain(text: &str) -> String {
    let mut quote = String::new();
    for line in text.lines() {
        quote.push('>');
        if !line.is_empty() && !line.starts_with('>') {
            quote.push(' ');
        }
        quote.push_str(line);
        quote.push('\n');
    }
    quote
}

fn unflow(text: &str, delsp: bool) -> String {
    let mut lines = text
        .split_inclusive('\n')
        .map(|line| {
            let newline = line.ends_with('\n');
            let line = line
                .strip_suffix("\r\n")
                .or_else(|| line.strip_suffix('\n'))
                .unwrap_or(line);
            let content = line.trim_start_matches('>');
            let depth = line.len() - content.len();
            (depth, content.strip_prefix(' ').unwrap_or(content), newline)
        })
        .peekable();
    let mut output = String::new();
    let mut continued = false;
    while let Some((depth, content, newline)) = lines.next() {
        if !continued {
            output.extend(std::iter::repeat_n('>', depth));
            if depth > 0 && !content.is_empty() {
                output.push(' ');
            }
        }
        continued = content.ends_with(' ')
            && content != "-- "
            && lines
                .peek()
                .is_some_and(|(next_depth, next, _)| *next_depth == depth && *next != "-- ");
        output.push_str(if continued && delsp {
            content.strip_suffix(' ').unwrap_or(content)
        } else {
            content
        });
        if newline && !continued {
            output.push('\n');
        }
    }
    output
}

fn forward_part(leaf: Leaf<'_>) -> Result<ForwardPart, MimeError> {
    let unreadable = || MimeError(format!("unreadable original part {}", leaf.part.id()));
    let part = leaf.reference.ok_or_else(unreadable)?;
    let (bytes, problems) = leaf.part.decoded_checked();
    if bytes.is_empty() || !problems.is_empty() {
        return Err(unreadable());
    }
    let content_type_parameters = leaf
        .part
        .content_type()
        .into_iter()
        .flat_map(|content_type| content_type.attributes())
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect();
    let filename = leaf
        .part
        .attachment_name()
        .filter(|name| !name.is_empty())
        .map_or_else(
            || fallback_filename(leaf.part, &leaf.content_type),
            str::to_owned,
        );
    Ok(ForwardPart {
        attachment: Attachment {
            filename: Some(filename),
            content_type: leaf.content_type,
            content_id: leaf.part.content_id().map(str::to_owned),
            bytes: bytes.into_owned(),
        },
        content_type_parameters,
        part,
        inline: leaf.role == PartRole::Inline,
    })
}

fn fallback_filename(part: MessagePart<'_>, content_type: &str) -> String {
    if let PartKind::Message(inner) = part.kind()
        && let Some(subject) = inner.subject()
    {
        let subject = subject.trim_matches(|ch: char| ch == '.' || ch.is_whitespace());
        if !subject.is_empty() {
            let safe = subject.replace(
                |ch: char| ch.is_control() || "/\\:*?\"<>|".contains(ch),
                "_",
            );
            return format!("{safe}.eml");
        }
    }
    let suffix = match content_type {
        "message/rfc822" | "message/global" => Some("eml"),
        "text/plain" => Some("txt"),
        "application/octet-stream" => Some("bin"),
        other => other
            .split_once('/')
            .map(|(_, subtype)| subtype)
            .filter(|subtype| {
                !subtype.starts_with("x-")
                    && subtype
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"+-_".contains(&byte))
            }),
    };
    suffix.map_or_else(
        || "attachment".into(),
        |suffix| format!("attachment.{suffix}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_forward_plan_gives_the_originals_header_block_as_data() {
        let plan = forward_plan(b"From: Author <a@example.com>\r\nDate: Wed, 4 Mar 2026 14:11:00 +0000\r\nSubject: Plan\r\nTo: Main <b@example.com>\r\nCc: c@example.com\r\nBcc: secret@example.com\r\n\r\nOriginal text").unwrap();
        assert_eq!(plan.original.from[0].email, "a@example.com");
        assert_eq!(
            plan.original.date.as_deref(),
            Some("Wed, 4 Mar 2026 14:11:00 +0000")
        );
        assert_eq!(plan.original.subject.as_deref(), Some("Plan"));
        assert_eq!(plan.original.to[0].name.as_deref(), Some("Main"));
        assert_eq!(plan.original.cc[0].email, "c@example.com");
        assert_eq!(plan.quote_text.as_deref(), Some("Original text"));
    }

    #[test]
    fn a_reply_to_a_message_without_a_message_id_has_no_threading_headers() {
        let plan = reply_plan(
            b"From: a@example.com\r\nReferences: <a@example.com>\r\n\r\nPlain",
            &[],
            false,
        )
        .unwrap();
        assert_eq!(plan.in_reply_to, None);
        assert!(plan.references.is_empty());
    }

    #[test]
    fn flowed_quotes_keep_their_depth_and_signature_boundary() {
        assert_eq!(
            unflow(
                ">> a soft \r\n>> wrap\r\n> Next \r\n> -- \r\n> signature\r\n From the start\r\n",
                false
            ),
            ">> a soft wrap\n> Next \n> -- \n> signature\nFrom the start\n",
        );
        assert_eq!(unflow(" joining \nlines", true), "joininglines");
    }
}
