//! Message ids: the one a message is written with, and the ones it echoes in
//! `In-Reply-To` and `References`.

use std::fmt;

use crate::address::{Address, is_dot_atom};
use crate::compose::Reason;

/// Longest id: bracketed after a fold's leading space, it still fits the
/// 998-byte line limit (RFC 5322 2.1.1).
const MAX_ID: usize = 995;

/// A message id, held without its angle brackets.
///
/// [`parse`](Self::parse) is lenient, since other senders' ids (which a reply
/// echoes) are often malformed: any one token without white space, control
/// characters, angle brackets or non-ASCII. The id a message is written with
/// must also be `left@right` in dot-atom form, which
/// [`compose`](crate::compose) checks and [`from_entropy`](Self::from_entropy)
/// always gives.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageId(String);

impl MessageId {
    /// One id, with or without its angle brackets (`<a@x>` or `a@x`).
    pub fn parse(value: &str) -> Result<Self, Reason> {
        let bare = value
            .strip_prefix('<')
            .and_then(|inner| inner.strip_suffix('>'))
            .unwrap_or(value);
        if bare.is_empty() {
            return Err(Reason::Missing);
        }
        if bare.chars().any(char::is_control) {
            return Err(Reason::ControlCharacter);
        }
        if !bare.is_ascii() {
            return Err(Reason::NotAscii);
        }
        if bare.contains(' ') {
            return Err(Reason::Whitespace);
        }
        if bare.contains(['<', '>']) {
            return Err(Reason::InvalidId);
        }
        if bare.len() > MAX_ID {
            return Err(Reason::TooLong);
        }
        Ok(Self(bare.to_owned()))
    }

    /// The ids in a list such as a `References` value: separated by white
    /// space, each with or without angle brackets.
    pub fn parse_list(value: &str) -> Result<Vec<Self>, Reason> {
        value
            .split(|ch: char| ch.is_ascii_whitespace())
            .flat_map(|token| token.split_inclusive('>'))
            .filter(|token| !token.is_empty())
            .map(Self::parse)
            .collect()
    }

    /// A new id for a message from `sender`: 32 lowercase hex digits of the
    /// 128 bits of `entropy` on the sender's domain. Pure: the caller draws
    /// the entropy, so no clock, host name or random source is read here,
    /// and the machine's name never reaches a recipient.
    pub fn from_entropy(entropy: [u8; 16], sender: &Address) -> Result<Self, Reason> {
        sender.check()?;
        let hex = u128::from_be_bytes(entropy);
        Ok(Self(format!("{hex:032x}@{}", sender.domain())))
    }

    /// The id without its angle brackets.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this id may be a message's own: `left@right`, both dot-atoms.
    pub(crate) fn check_own(&self) -> Result<(), Reason> {
        let (left, right) = self.0.rsplit_once('@').ok_or(Reason::NoAt)?;
        if is_dot_atom(left) && is_dot_atom(right) {
            Ok(())
        } else {
            Err(Reason::NotDotAtom)
        }
    }
}

/// The id in angle brackets, as written and as the mirror records it.
impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}>", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sender(email: &str) -> Address {
        Address {
            name: Some("Ann".into()),
            email: email.into(),
        }
    }

    #[test]
    fn the_same_entropy_and_sender_give_the_same_id() {
        let entropy = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        let id = MessageId::from_entropy(entropy, &sender("ann@mail.example.com")).unwrap();
        assert_eq!(
            id.as_str(),
            "00112233445566778899aabbccddeeff@mail.example.com"
        );
        assert_eq!(
            id.to_string(),
            "<00112233445566778899aabbccddeeff@mail.example.com>"
        );
        assert_eq!(
            MessageId::from_entropy(entropy, &sender("ann@mail.example.com")),
            Ok(id.clone())
        );
        assert_ne!(
            MessageId::from_entropy([0; 16], &sender("ann@mail.example.com")),
            Ok(id.clone())
        );
        assert_eq!(id.check_own(), Ok(()));
    }

    #[test]
    fn our_own_id_is_strict_and_an_echoed_id_is_lenient() {
        for (value, bare) in [
            ("<a.b@x.example>", "a.b@x.example"),
            ("a.b@x.example", "a.b@x.example"),
            ("<CAF=x+y/z@mail.gmail.com>", "CAF=x+y/z@mail.gmail.com"),
        ] {
            let id = MessageId::parse(value).unwrap();
            assert_eq!(id.as_str(), bare);
            assert_eq!(id.check_own(), Ok(()));
        }
        // Echoed, never written as our own.
        for value in [
            "<no-at-sign>",
            "a@b@c",
            "<a..b@x>",
            "<[weird]@x>",
            "a@[192.0.2.1]",
        ] {
            let id = MessageId::parse(value).unwrap();
            assert!(id.check_own().is_err(), "{value}");
        }
        assert_eq!(MessageId::parse(""), Err(Reason::Missing));
        assert_eq!(MessageId::parse("<>"), Err(Reason::Missing));
        assert_eq!(MessageId::parse("<a b@x>"), Err(Reason::Whitespace));
        assert_eq!(
            MessageId::parse("<a@x>\r\nBcc: e@x"),
            Err(Reason::ControlCharacter)
        );
        assert_eq!(MessageId::parse("<\u{e9}@x>"), Err(Reason::NotAscii));
        assert_eq!(MessageId::parse("<a@x"), Err(Reason::InvalidId));
        assert_eq!(MessageId::parse("<a<b@x>"), Err(Reason::InvalidId));
    }
}
