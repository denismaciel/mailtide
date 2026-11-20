//! Values for Gmail's search box, for hosts that build a query from user
//! text. Each helper renders one value; neither parses a query, and
//! mailtide never rewrites the query a host passes to a search.
//!
//! Gmail reads `OR`, `AND`, braces and parentheses even inside double
//! quotes and has no escape character, so a user's text cannot simply be
//! quoted. The rule follows himalaya's `value_q` (pimalaya/himalaya,
//! MIT OR Apache-2.0, `src/gmail/search.rs`).

/// `text` as one value that matches it literally and cannot add operators
/// or groups, for `from:`, `subject:` or a bare term: quotes, braces and
/// parentheses become spaces (Gmail matches words and skips punctuation),
/// a standalone `OR` or `AND` is lowercased (Gmail's operators are
/// case-sensitive), and anything but one bare word is double-quoted, so no
/// `-` or `:` reads as an operator. `None` when no word is left: such a
/// value would match nothing a user meant, so it is refused rather than
/// sent.
///
/// The output matches the same messages as the text meant, or fewer, never
/// more (`search-literal-quoting`).
#[must_use]
pub fn literal(text: &str) -> Option<String> {
    let words: Vec<&str> = text
        .split(|c: char| c.is_whitespace() || matches!(c, '"' | '{' | '}' | '(' | ')'))
        .filter(|word| !word.is_empty())
        .map(|word| match word {
            "OR" => "or",
            "AND" => "and",
            word => word,
        })
        .collect();
    let bare = |c: char| c.is_alphanumeric() || matches!(c, '@' | '.' | '_' | '+');
    match words.as_slice() {
        [] => None,
        [word] if word.chars().all(bare) => Some((*word).to_owned()),
        words => Some(format!("\"{}\"", words.join(" "))),
    }
}

/// The search term finding a message by its `Message-ID` header:
/// `rfc822msgid:` and the id without its angle brackets. Gmail matches it
/// exactly, ignoring case, written bare (checked against Gmail for ids
/// holding `+ - = / $ ! %`; `search-msgid-match`). `None` for an id its
/// syntax cannot carry (one holding a blank, a quote, a brace, a
/// parenthesis or an angle bracket, or an empty one): such an id is
/// refused, never loosened into a term that may match another message.
///
/// It finds the `Message-ID` Gmail stored. Gmail replaces the one a client
/// writes when it sends a message or saves a draft through the API, so this
/// cannot find a message mailtide just sent; mailtide identifies those by
/// a private header instead.
#[must_use]
pub fn by_message_id(id: &str) -> Option<String> {
    let id = id.trim();
    let id = id
        .strip_prefix('<')
        .and_then(|inner| inner.strip_suffix('>'))
        .unwrap_or(id);
    let carried = |c: char| {
        !c.is_whitespace()
            && !c.is_control()
            && !matches!(c, '"' | '{' | '}' | '(' | ')' | '<' | '>')
    };
    (!id.is_empty() && id.chars().all(carried)).then(|| format!("rfc822msgid:{id}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_id_gmail_cannot_carry_is_refused_not_loosened() {
        for id in [
            "",
            "<>",
            "a b@x.test",
            "<a b@x.test>",
            "a\"b@x.test",
            "{a}@x.test",
            "a(b)@x.test",
            "a<b@x.test",
            "a\tb@x.test",
        ] {
            assert_eq!(by_message_id(id), None, "{id:?}");
        }
        assert_eq!(
            by_message_id("<a=b/c$d!e%f@x.test>").as_deref(),
            Some("rfc822msgid:a=b/c$d!e%f@x.test"),
            "these characters match bare"
        );
    }
}
