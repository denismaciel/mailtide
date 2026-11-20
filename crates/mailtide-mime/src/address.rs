//! Mailboxes: the ones [`compose`](crate::compose) writes, and the ones
//! [`parse_addresses`] reads from a header value the mirror holds.

use mail_parser::HeaderForm;

use crate::compose::Reason;

/// Longest local part (RFC 5321 4.5.3.1.1).
const MAX_LOCAL: usize = 64;
/// Longest domain (RFC 5321 4.5.3.1.2).
const MAX_DOMAIN: usize = 255;
/// Longest address, the forward path less its angle brackets (RFC 5321 4.5.3.1.3).
const MAX_ADDRESS: usize = 254;
/// Longest domain label (RFC 1035 2.3.4).
const MAX_LABEL: usize = 63;

/// One mailbox: an optional display name and an address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address {
    /// The decoded display name.
    pub name: Option<String>,
    /// The address, `local@domain`.
    pub email: String,
}

impl Address {
    /// Whether [`compose`](crate::compose) writes this mailbox: a display
    /// name without control characters, and an ASCII `local@domain` with a
    /// dot-atom local part, a host name for a domain (an IDN in its A-label
    /// form) and RFC 5321's length limits. `Name <a@x>` given as the email
    /// is refused; [`parse_addresses`] splits such a string.
    pub fn check(&self) -> Result<(), Reason> {
        if self
            .name
            .as_deref()
            .is_some_and(|name| name.chars().any(char::is_control))
        {
            return Err(Reason::ControlCharacter);
        }
        check_email(&self.email)
    }

    /// The domain of a checked address.
    pub(crate) fn domain(&self) -> &str {
        self.email.rsplit_once('@').map_or("", |(_, domain)| domain)
    }
}

fn check_email(email: &str) -> Result<(), Reason> {
    if email.is_empty() {
        return Err(Reason::Missing);
    }
    if email.chars().any(char::is_control) {
        return Err(Reason::ControlCharacter);
    }
    if !email.is_ascii() {
        return Err(Reason::NotAscii);
    }
    if email.contains(['<', '>']) {
        return Err(Reason::NameInAddress);
    }
    if email.contains(' ') {
        return Err(Reason::Whitespace);
    }
    if email.contains('"') {
        return Err(Reason::QuotedLocalPart);
    }
    let Some((local, domain)) = email.rsplit_once('@') else {
        return Err(Reason::NoAt);
    };
    if local.is_empty() {
        return Err(Reason::EmptyLocalPart);
    }
    if domain.is_empty() {
        return Err(Reason::EmptyDomain);
    }
    if domain.starts_with('[') {
        return Err(Reason::DomainLiteral);
    }
    if local.len() > MAX_LOCAL || domain.len() > MAX_DOMAIN || email.len() > MAX_ADDRESS {
        return Err(Reason::TooLong);
    }
    if !is_dot_atom(local) {
        return Err(Reason::InvalidLocalPart);
    }
    if !is_host_name(domain) {
        return Err(Reason::InvalidDomain);
    }
    Ok(())
}

/// RFC 5322 `atext`.
fn is_atext(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-/=?^_`{|}~".contains(&byte)
}

/// RFC 5322 `dot-atom-text`: atoms joined by single dots.
pub(crate) fn is_dot_atom(value: &str) -> bool {
    !value.is_empty()
        && value
            .split('.')
            .all(|atom| !atom.is_empty() && atom.bytes().all(is_atext))
}

/// Letters, digits and hyphens in labels of 1 to 63 bytes that neither
/// start nor end with a hyphen, joined by single dots.
fn is_host_name(value: &str) -> bool {
    !value.is_empty()
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= MAX_LABEL
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

/// The addresses in a header value (`To`, `Cc`, or one entry of the header
/// facts' lists), groups flattened, display names decoded. Entries without
/// a readable address (`local@domain`) are left out.
#[must_use]
pub fn parse_addresses(value: &str) -> Vec<Address> {
    let parsed = HeaderForm::Addresses.parse(value.as_bytes());
    let Some(list) = parsed.value().as_address() else {
        return Vec::new();
    };
    list.mailboxes()
        .filter_map(|mailbox| {
            let email = mailbox.address()?;
            let (local, domain) = email.split_once('@')?;
            let readable =
                !local.is_empty() && !domain.is_empty() && !email.contains(char::is_whitespace);
            readable.then(|| Address {
                name: mailbox.name().map(str::to_owned),
                email: email.to_owned(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn email(email: &str) -> Result<(), Reason> {
        Address {
            name: None,
            email: email.to_owned(),
        }
        .check()
    }

    #[test]
    fn an_address_needs_a_local_part_and_a_domain() {
        assert_eq!(email("ann@example.com"), Ok(()));
        assert_eq!(email("ann.lee+tag@mail.example.co.uk"), Ok(()));
        assert_eq!(email("root@localhost"), Ok(()));
        assert_eq!(email(""), Err(Reason::Missing));
        assert_eq!(email("ann"), Err(Reason::NoAt));
        assert_eq!(email("@example.com"), Err(Reason::EmptyLocalPart));
        assert_eq!(email("ann@"), Err(Reason::EmptyDomain));
        assert_eq!(email("ann lee@example.com"), Err(Reason::Whitespace));
        assert_eq!(
            email("\"ann.lee\"@example.com"),
            Err(Reason::QuotedLocalPart)
        );
        assert_eq!(email("ann@[192.0.2.1]"), Err(Reason::DomainLiteral));
        assert_eq!(email(".ann@example.com"), Err(Reason::InvalidLocalPart));
        assert_eq!(email("ann..lee@example.com"), Err(Reason::InvalidLocalPart));
        assert_eq!(email("ann@bob@example.com"), Err(Reason::InvalidLocalPart));
        assert_eq!(email("ann@example..com"), Err(Reason::InvalidDomain));
        assert_eq!(email("ann@-example.com"), Err(Reason::InvalidDomain));
        assert_eq!(email("ann@exa_mple.com"), Err(Reason::InvalidDomain));
        assert_eq!(
            email("ann@example.com\r\nBcc: x@y"),
            Err(Reason::ControlCharacter)
        );
    }

    #[test]
    fn a_name_and_address_in_one_string_is_refused_as_an_email() {
        assert_eq!(email("Alice <a@x.org>"), Err(Reason::NameInAddress));
        assert_eq!(email("<a@x.org>"), Err(Reason::NameInAddress));
        let split = parse_addresses("Alice <a@x.org>");
        assert_eq!(
            split,
            [Address {
                name: Some("Alice".into()),
                email: "a@x.org".into()
            }]
        );
        assert_eq!(split[0].check(), Ok(()));
    }

    #[test]
    fn a_non_ascii_address_is_refused() {
        assert_eq!(email("j\u{f6}rg@example.com"), Err(Reason::NotAscii));
        assert_eq!(email("jorg@b\u{fc}cher.example"), Err(Reason::NotAscii));
        assert_eq!(email("jorg@xn--bcher-kva.example"), Ok(()));
        let named = Address {
            name: Some("J\u{f6}rg M\u{fc}ller".into()),
            email: "jorg@example.com".into(),
        };
        assert_eq!(named.check(), Ok(()));
        let bell = Address {
            name: Some("ring \u{7} the bell".into()),
            email: "jorg@example.com".into(),
        };
        assert_eq!(bell.check(), Err(Reason::ControlCharacter));
    }

    #[test]
    fn an_over_long_address_is_refused() {
        let local = "a".repeat(64);
        assert_eq!(email(&format!("{local}@example.com")), Ok(()));
        assert_eq!(
            email(&format!("a{local}@example.com")),
            Err(Reason::TooLong)
        );
        let label = "d".repeat(63);
        let domain = [label.as_str(); 4].join(".");
        assert_eq!(domain.len(), 255);
        assert_eq!(email(&format!("a@{domain}")), Err(Reason::TooLong));
        let domain = [label.as_str(); 3].join(".");
        assert_eq!(domain.len(), 191);
        assert_eq!(email(&format!("a@{domain}")), Ok(()));
        // Each part within its limit, the whole one byte over.
        let address = format!("{}@{domain}", "a".repeat(63));
        assert_eq!(address.len(), 255);
        assert_eq!(email(&address), Err(Reason::TooLong));
        assert_eq!(email(&address[1..]), Ok(()));
        assert_eq!(
            email(&format!("a@{}.example", "d".repeat(64))),
            Err(Reason::InvalidDomain)
        );
    }

    #[test]
    fn parse_list_flattens_groups_and_decodes_names() {
        let parsed = parse_addresses(
            "\"Doe, Jane\" <jane@x.org>, =?UTF-8?B?w4lsb2RpZQ==?= <elodie@x.org>, \
             Team: bob@x.org, Carol <carol@x.org>;, undisclosed-recipients:;, nobody",
        );
        let named = |name: Option<&str>, email: &str| Address {
            name: name.map(str::to_owned),
            email: email.to_owned(),
        };
        assert_eq!(
            parsed,
            [
                named(Some("Doe, Jane"), "jane@x.org"),
                named(Some("\u{c9}lodie"), "elodie@x.org"),
                named(None, "bob@x.org"),
                named(Some("Carol"), "carol@x.org"),
            ]
        );
        assert!(parsed.iter().all(|address| address.check().is_ok()));
        assert!(parse_addresses("").is_empty());
    }
}
