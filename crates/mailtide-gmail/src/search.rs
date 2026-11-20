//! Values for Gmail's search box, for hosts that build a query from user
//! text. Each helper renders one value; neither parses a query, and
//! mailtide never rewrites the query a host passes to a search.
//!
//! Gmail reads `OR`, `|`, braces, `AND` and `AROUND` even inside double
//! quotes and has no escape character, so a user's text cannot simply be
//! quoted. The rule starts from himalaya's `value_q` (pimalaya/himalaya,
//! MIT OR Apache-2.0, `src/gmail/search.rs`) and goes further where Gmail
//! was measured to (`search-literal-quoting`).

/// `text` as one value that matches it literally and cannot add operators
/// or groups, for `from:`, `subject:` or a bare term; `None` when it cannot
/// be made one (no letter or digit is left), never a value that means
/// something else.
///
/// Gmail reads `OR`, `|`, braces, `AND` and `AROUND` as operators even
/// inside double quotes, and a backslash escapes nothing; a colon after a
/// word inside quotes makes the phrase unreliable; parentheses, `+` and `-`
/// inside quotes are punctuation (`search-literal-quoting`). Gmail matches
/// words, case-insensitively, skipping punctuation. So the value is:
///
/// - ASCII-lowercased: the operators are upper case only, so no word reads
///   as one, however it is glued to punctuation (`\OR`), and a lower-case
///   `or`, `and` or `around` is a plain word that matches the same text;
/// - split into words at blanks, quotes, braces, parentheses, `|`, `\` and
///   `:`, which become spaces;
/// - one bare word (letters, digits, `@ . _ +`, starting with a letter or
///   digit) as it is, anything else double-quoted, so no `-` or `:` reads
///   as an operator.
///
/// What cannot be searched as a literal: case (Gmail ignores it anyway),
/// and the punctuation that tells two texts with the same words apart:
/// `from:alice` is searched as the phrase `from alice`, which also matches
/// `from alice`. Text without a letter or digit is refused.
#[must_use]
pub fn literal(text: &str) -> Option<String> {
    let lowered = text.to_ascii_lowercase();
    let words: Vec<&str> = lowered
        .split(|c: char| {
            c.is_whitespace() || matches!(c, '"' | '{' | '}' | '(' | ')' | '|' | '\\' | ':')
        })
        .filter(|word| !word.is_empty())
        .collect();
    if !words
        .iter()
        .any(|word| word.chars().any(char::is_alphanumeric))
    {
        return None;
    }
    let bare = |word: &str| {
        word.starts_with(char::is_alphanumeric)
            && word
                .chars()
                .all(|c| c.is_alphanumeric() || matches!(c, '@' | '.' | '_' | '+'))
    };
    match words.as_slice() {
        [word] if bare(word) => Some((*word).to_owned()),
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

    /// Every line of `search-literal-quoting`'s result (refuted): what
    /// Gmail still reads as syntax inside quotes comes out as plain words.
    #[test]
    fn literal_neutralises_every_operator_gmail_reads_inside_quotes() {
        for (text, value) in [
            ("Z OR K", "\"z or k\""),
            ("Z | K", "\"z k\""),
            ("Z|K", "\"z k\""),
            ("{Z K}", "\"z k\""),
            ("K AND Z", "\"k and z\""),
            ("Z AROUND 2 K", "\"z around 2 k\""),
            ("Z \\\"OR\\\" K", "\"z or k\""),
            ("Z \\OR K", "\"z or k\""),
            ("-OR- K", "\"-or- k\""),
            ("memo from:alice", "\"memo from alice\""),
            ("from:alice", "\"from alice\""),
            ("(Z K)", "\"z k\""),
            ("a-b +c", "\"a-b +c\""),
            ("OR", "or"),
            ("AND", "and"),
            ("AROUND", "around"),
            ("+tag", "\"+tag\""),
        ] {
            assert_eq!(literal(text).as_deref(), Some(value), "{text}");
        }
        for nothing in ["|", "\\", ":", "-", "+ -", "\" \\ | :"] {
            assert_eq!(literal(nothing), None, "{nothing:?}");
        }
    }

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
