//! The private header that identifies a message mailtide writes.
//!
//! Gmail replaces a client's `Message-ID` and `Date` on every API send and
//! draft save, and keeps other client headers byte for byte. So the identity
//! that survives a save and a send is `X-Mailtide-Id: <token>`, the token 16
//! to 64 characters of `A-Za-z0-9_-`. [`compose`](crate::compose) writes it;
//! [`tracking_id`] reads it back from raw bytes.

use encodify::base64::URL_SAFE_NO_PAD;

/// The header's name.
pub const TRACKING_HEADER: &str = "X-Mailtide-Id";

/// Shortest and longest token.
const MIN_TOKEN: usize = 16;
const MAX_TOKEN: usize = 64;

/// Whether `token` is a tracking id: 16 to 64 characters of `A-Za-z0-9_-`.
#[must_use]
pub fn is_tracking_id(token: &str) -> bool {
    (MIN_TOKEN..=MAX_TOKEN).contains(&token.len())
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// A new tracking id: the 128 bits of `entropy` as base64url without
/// padding (22 characters). Pure: the caller draws the entropy.
#[must_use]
pub fn new_tracking_id(entropy: [u8; 16]) -> String {
    URL_SAFE_NO_PAD.encode(entropy)
}

/// The tracking id of a raw message: the value of the first
/// `X-Mailtide-Id` header (name in any letter case, folding undone) that is
/// a tracking id. Reads the header block only, never the body.
#[must_use]
pub fn tracking_id(raw: &[u8]) -> Option<String> {
    let mut fields: Vec<Vec<u8>> = Vec::new();
    for line in raw.split(|&byte| byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            break;
        }
        match (line.first(), fields.last_mut()) {
            (Some(b' ' | b'\t'), Some(field)) => field.extend_from_slice(line),
            _ => fields.push(line.to_vec()),
        }
    }
    fields.iter().find_map(|field| {
        let colon = field.iter().position(|&byte| byte == b':')?;
        let (name, value) = field.split_at(colon);
        if !name
            .trim_ascii()
            .eq_ignore_ascii_case(TRACKING_HEADER.as_bytes())
        {
            return None;
        }
        let value = std::str::from_utf8(value.get(1..)?).ok()?.trim_ascii();
        is_tracking_id(value).then(|| value.to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "AAECAwQFBgcICQoLDA0ODw";

    #[test]
    fn a_new_tracking_id_is_base64url_of_its_entropy() {
        let entropy: [u8; 16] = std::array::from_fn(|at| u8::try_from(at).unwrap_or_default());
        assert_eq!(new_tracking_id(entropy), TOKEN);
        assert_eq!(new_tracking_id(entropy), new_tracking_id(entropy));
        assert_eq!(new_tracking_id([0xff; 16]), "_____________________w");
        assert!(is_tracking_id(&new_tracking_id([0xfb; 16])));
    }

    #[test]
    fn a_tracking_id_is_16_to_64_url_safe_characters() {
        assert!(is_tracking_id(&"a".repeat(16)));
        assert!(is_tracking_id(&"Z9_-".repeat(16)));
        assert!(!is_tracking_id(&"a".repeat(15)));
        assert!(!is_tracking_id(&"a".repeat(65)));
        for bad in [
            "aaaaaaaaaaaaaaa=",
            "aaaaaaaa aaaaaaaa",
            "aaaaaaaa+aaaaaaaa",
            "aaaaaaaa\u{e9}aaaaaaa",
        ] {
            assert!(!is_tracking_id(bad), "{bad:?}");
        }
    }

    #[test]
    fn the_reader_finds_the_first_valid_header_and_never_reads_the_body() {
        let read = |raw: &str| tracking_id(raw.as_bytes());
        assert_eq!(
            read(&format!(
                "From: a@x\r\nX-Mailtide-Id: {TOKEN}\r\n\r\nbody\r\n"
            )),
            Some(TOKEN.to_owned())
        );
        // Folded, and with the name in another letter case.
        assert_eq!(
            read(&format!("x-mailtide-ID:\r\n  {TOKEN}\r\nTo: b@x\r\n\r\n")),
            Some(TOKEN.to_owned())
        );
        // LF line ends.
        assert_eq!(
            read(&format!("X-MAILTIDE-ID: {TOKEN}\n\nbody\n")),
            Some(TOKEN.to_owned())
        );
        // A second occurrence is ignored; an invalid value is skipped.
        let other = "b".repeat(20);
        assert_eq!(
            read(&format!(
                "X-Mailtide-Id: {TOKEN}\r\nX-Mailtide-Id: {other}\r\n\r\n"
            )),
            Some(TOKEN.to_owned())
        );
        assert_eq!(
            read(&format!(
                "X-Mailtide-Id: short\r\nX-Mailtide-Id: {other}\r\n\r\n"
            )),
            Some(other.clone())
        );
        assert_eq!(read("X-Mailtide-Id: not valid at all!\r\n\r\n"), None);
        // Only the header block: a header-like line in the body is not read.
        assert_eq!(
            read(&format!("From: a@x\r\n\r\nX-Mailtide-Id: {TOKEN}\r\n")),
            None
        );
        assert_eq!(read(&format!("X-Mailtide-Ids: {TOKEN}\r\n\r\n")), None);
        assert_eq!(tracking_id(b""), None);
        assert_eq!(tracking_id(b"\xff\xfe: \xff\r\n\r\n"), None);
    }
}
