//! Tests ported by name from mail-builder's `src/headers/roundtrip.rs`
//! (stalwartlabs/mail-builder, Apache-2.0 OR MIT, commit 2fc4a942754f), as
//! `features/compose-mime.toml` maps them: seeded build-then-parse runs,
//! through `compose` and `mail-parser`, with the reference's folding
//! validator. Where the library encodes or drops a control character,
//! compose refuses the input. Address groups, raw and URL headers are not
//! inputs.

use std::borrow::Cow;

use encodify::{base64, qp};
use mail_parser::MessageParser;
use mailtide_mime::{
    Address, Attachment, ComposeError, ComposeOptions, MessageId, OutgoingMessage, Purpose, Reason,
    compose,
};

const CASES: usize = 2_000;
const DATE_CASES: usize = 50_000;
const MAX_LINE: usize = 78;
const ENCODED_LINE: usize = 76;
const MAX_ENCODED_WORD: usize = 75;

struct Prng(u64);

impl Prng {
    fn new(seed: u64) -> Self {
        Prng(seed | 1)
    }

    fn next(&mut self) -> u64 {
        let mut state = self.0;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.0 = state;
        state
    }

    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next() % bound.max(1) as u64).unwrap_or_default()
    }

    fn range(&mut self, low: usize, high: usize) -> usize {
        low + self.below(high - low + 1)
    }

    fn chance(&mut self, one_in: usize) -> bool {
        self.below(one_in) == 0
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
}

const ASCII_WORDS: &[&str] = &[
    "report",
    "Quarterly",
    "John",
    "Doe",
    "meeting",
    "x",
    "ab",
    "consolidated",
    "figures",
    "forecast",
    "headcount",
    "action-items",
    "2026",
    "Q3",
    "re:",
    "fwd",
    "a",
    "the",
];
const LATIN_CHARS: &[char] = &[
    'é', 'è', 'ü', 'ß', 'ñ', 'å', 'ø', 'î', 'ç', 'Δ', 'Ω', 'λ', 'ж', 'щ', 'א', 'ت',
];
const CJK_CHARS: &[char] = &[
    '안', '녕', '하', '세', '요', '世', '界', '添', '付', '日', '本', '語', '中', '文', 'こ', 'ん',
    'に', 'ち', 'は', '😀', '🌍',
];
const SPECIAL_CHARS: &[char] = &[
    '(', ')', '<', '>', '[', ']', ':', ';', '@', '\\', ',', '"', '.', '?', '=', '_', '\'', '!',
];
const CONTROL_CHARS: &[char] = &[
    '\0', '\u{1}', '\u{3}', '\u{7}', '\u{8}', '\t', '\n', '\u{b}', '\u{c}', '\r', '\u{e}',
    '\u{1f}', '\u{7f}',
];
const LOCAL: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789-_";
const LABEL: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

fn push_word(prng: &mut Prng, out: &mut String, pool: usize, len: usize) {
    match pool {
        0 => out.push_str(prng.pick(ASCII_WORDS)),
        1 => (0..len).for_each(|_| out.push(prng.pick(LATIN_CHARS))),
        2 => (0..len).for_each(|_| out.push(prng.pick(CJK_CHARS))),
        3 => (0..len).for_each(|_| out.push(prng.pick(SPECIAL_CHARS))),
        4 => {
            for _ in 0..len {
                match prng.below(4) {
                    0 => out.push_str(prng.pick(ASCII_WORDS)),
                    1 => out.push(prng.pick(LATIN_CHARS)),
                    2 => out.push(prng.pick(CJK_CHARS)),
                    _ => out.push(prng.pick(SPECIAL_CHARS)),
                }
            }
        }
        5 => {
            for _ in 0..len {
                match prng.below(6) {
                    0 | 1 => out.push(prng.pick(CONTROL_CHARS)),
                    2 => out.push('"'),
                    3 => out.push('\\'),
                    4 => out.push(prng.pick(LATIN_CHARS)),
                    _ => out.push(prng.pick(SPECIAL_CHARS)),
                }
            }
        }
        6 => (0..len).for_each(|_| out.push(prng.pick(CONTROL_CHARS))),
        _ => (0..len).for_each(|_| out.push(char::from(prng.pick(LOCAL)))),
    }
}

/// Display names, subjects and file names: every alphabet, white-space runs,
/// long words, leading or trailing white space, and (pools 5 and 6, or a
/// trailing line break) control characters.
fn gen_phrase(prng: &mut Prng) -> String {
    let mut out = String::new();
    let words = prng.range(0, 12);
    let pool = prng.below(8);
    let space = |prng: &mut Prng| if prng.chance(3) { '\t' } else { ' ' };
    if prng.chance(6) {
        for _ in 0..prng.range(1, 3) {
            out.push(space(prng));
        }
    }
    for word in 0..words {
        if word > 0 {
            let run = if prng.chance(4) { 6 } else { 1 };
            for _ in 0..prng.range(1, run) {
                out.push(if prng.chance(4) { '\t' } else { ' ' });
            }
        }
        let len = if prng.chance(12) {
            prng.range(40, 220)
        } else {
            prng.range(1, 10)
        };
        push_word(prng, &mut out, pool, len);
    }
    if prng.chance(6) {
        for _ in 0..prng.range(1, 3) {
            out.push(space(prng));
        }
    }
    if prng.chance(8) {
        out.push_str(["\r\n", "\r", "\n"][prng.below(3)]);
    }
    out
}

fn gen_atom(prng: &mut Prng, alphabet: &[u8], low: usize, high: usize) -> String {
    let len = prng.range(low, high);
    (0..len).map(|_| char::from(prng.pick(alphabet))).collect()
}

/// An address compose accepts: a dot-atom local part and a host name.
fn gen_email(prng: &mut Prng) -> String {
    let local = if prng.chance(20) { 55 } else { 12 };
    let label = if prng.chance(20) { 60 } else { 12 };
    let mut local = gen_atom(prng, LOCAL, 1, local);
    if prng.chance(4) {
        local.push('.');
        local.push_str(&gen_atom(prng, LOCAL, 1, 8));
    }
    let domain = gen_atom(prng, LABEL, 1, label);
    let tld = ["com", "org", "example", "co.uk"][prng.below(4)];
    format!("{local}@{domain}.{tld}")
}

fn gen_id(prng: &mut Prng) -> String {
    let head = prng.next();
    let mid = gen_atom(prng, LOCAL, 1, 20);
    let host = gen_email(prng);
    let host = host.split('@').nth(1).unwrap_or("example.org");
    format!("{head:016x}.{mid}@{host}")
}

fn has_control(value: &str) -> bool {
    value.chars().any(char::is_control)
}

fn base(from: Address) -> OutgoingMessage {
    let mut message = OutgoingMessage::new(from, 0);
    message.text = Some("b".into());
    message
}

fn plain(email: &str) -> Address {
    Address {
        name: None,
        email: email.to_owned(),
    }
}

fn write(message: &OutgoingMessage) -> Result<Vec<u8>, ComposeError> {
    let draft = ComposeOptions {
        purpose: Purpose::Draft,
        max_bytes: None,
    };
    compose(message, &draft).map(|composed| composed.raw)
}

/// One header of a message as written, `Name: value\r\n`, folding kept.
fn header<'a>(raw: &'a [u8], name: &str) -> &'a [u8] {
    let prefix = format!("{name}: ");
    let mut start = None;
    let mut at = 0;
    while at < raw.len() {
        let end = raw[at..]
            .windows(2)
            .position(|pair| pair == b"\r\n")
            .map_or(raw.len(), |pos| at + pos + 2);
        let line = &raw[at..end];
        match start {
            None if line.starts_with(prefix.as_bytes()) => start = Some(at),
            Some(from) if !line.starts_with(b" ") && !line.starts_with(b"\t") => {
                return &raw[from..at];
            }
            _ => {}
        }
        at = end;
    }
    panic!("no {name} in {:?}", String::from_utf8_lossy(raw));
}

fn is_wsp(byte: u8) -> bool {
    byte == b' ' || byte == b'\t'
}

fn lines(header: &[u8]) -> Vec<&[u8]> {
    let mut lines = Vec::new();
    let mut rest = header;
    while let Some(pos) = rest.windows(2).position(|pair| pair == b"\r\n") {
        lines.push(&rest[..pos]);
        rest = &rest[pos + 2..];
    }
    if !rest.is_empty() {
        lines.push(rest);
    }
    lines
}

struct Word {
    len: usize,
    quoted: bool,
}

fn encoded_words(line: &[u8]) -> Vec<Word> {
    let mut words = Vec::new();
    let mut rest = line;
    let mut offset = 0;
    while let Some(start) = rest.windows(2).position(|pair| pair == b"=?") {
        let body = &rest[start..];
        let Some(head) = body
            .iter()
            .enumerate()
            .filter(|&(_, &byte)| byte == b'?')
            .nth(2)
            .map(|(pos, _)| pos + 1)
        else {
            break;
        };
        let Some(end) = body
            .windows(2)
            .skip(head)
            .position(|pair| pair == b"?=")
            .map(|end| head + end + 2)
        else {
            break;
        };
        let word = &body[..end];
        let charset_end = word[2..].iter().position(|&byte| byte == b'?').unwrap_or(0) + 2;
        if !matches!(&word[2..charset_end], b"utf-8" | b"us-ascii") {
            offset += start + 2;
            rest = &body[2..];
            continue;
        }
        let quoted = offset + start > 0
            && line.get(offset + start - 1) == Some(&b'"')
            && line.get(offset + start + end) == Some(&b'"');
        let parts: Vec<&[u8]> = word[2..word.len() - 2]
            .splitn(3, |&byte| byte == b'?')
            .collect();
        assert_eq!(
            parts.len(),
            3,
            "malformed encoded word {:?}",
            String::from_utf8_lossy(word)
        );
        let decoded = match parts[1] {
            b"Q" | b"q" => qp::Q_TEXT.decode(parts[2]).ok().map(Cow::into_owned),
            b"B" | b"b" => base64::STANDARD.decode(parts[2]).ok(),
            _ => None,
        };
        let decoded = decoded.unwrap_or_else(|| {
            panic!(
                "encoded word does not decode: {:?}",
                String::from_utf8_lossy(word)
            )
        });
        assert!(
            String::from_utf8(decoded).is_ok(),
            "encoded word {:?} does not decode to UTF-8",
            String::from_utf8_lossy(word)
        );
        words.push(Word {
            len: word.len(),
            quoted,
        });
        offset += start + end;
        rest = &body[end..];
    }
    words
}

/// Every rule the folder keeps: CRLF line ends, no bare CR or LF,
/// continuation lines starting with white space, no empty or white-space
/// continuation, no trailing white space before a fold, the 78-character
/// target unless a single atom is longer, and RFC 2047 encoded words of at
/// most 75 characters that each decode on their own.
fn validate(header: &[u8], column: usize) {
    let text = String::from_utf8_lossy(header).into_owned();
    assert!(header.ends_with(b"\r\n"), "no CRLF at the end of {text:?}");
    let body = &header[..header.len() - 2];
    let all = lines(body);
    for (pos, line) in all.iter().enumerate() {
        assert!(
            !line.contains(&b'\r') && !line.contains(&b'\n'),
            "bare CR or LF in {text:?}"
        );
        assert!(!line.is_empty(), "empty line in {text:?}");
        let leading = line.iter().take_while(|&&byte| is_wsp(byte)).count();
        if pos > 0 {
            assert!(leading > 0, "continuation line without WSP in {text:?}");
        }
        assert!(leading < line.len(), "white-space only line in {text:?}");
        let trailing = line.iter().rev().take_while(|&&byte| is_wsp(byte)).count();
        let is_empty_value = pos == 0 && line.len() <= column;
        if !is_empty_value {
            assert_eq!(trailing, 0, "trailing white space before CRLF in {text:?}");
        }
        let value = if pos == 0 { column } else { 0 };
        let start = value
            + line
                .get(value..)
                .unwrap_or_default()
                .iter()
                .take_while(|&&byte| is_wsp(byte))
                .count();
        let content = line.get(start..line.len() - trailing).unwrap_or_default();
        if line.len() - trailing > MAX_LINE {
            assert!(
                !content.iter().any(|&byte| is_wsp(byte)),
                "line of {} bytes could have been folded in {text:?}",
                line.len()
            );
        }
        let words = encoded_words(line);
        if words.iter().any(|word| !word.quoted) && content.iter().any(|&byte| is_wsp(byte)) {
            assert!(
                line.len() - trailing <= ENCODED_LINE,
                "line of {} bytes carries an encoded word in {text:?}",
                line.len()
            );
        }
        for word in words {
            assert!(
                word.quoted || word.len <= MAX_ENCODED_WORD,
                "encoded word of {} characters in {text:?}",
                word.len
            );
        }
    }
}

fn collapse(value: &str) -> String {
    value.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn parsed(raw: &[u8]) -> mail_parser::Message<'_> {
    MessageParser::new().parse(raw).expect("parse")
}

#[test]
fn seeded_subjects_round_trip() {
    let mut prng = Prng::new(0x5eed_1234_abcd_0001);
    let mut accepted = 0;
    for case in 0..CASES {
        let value = gen_phrase(&mut prng);
        let mut message = base(plain("a@example.org"));
        message.subject = Some(value.clone());
        let raw = match write(&message) {
            Err(error) => {
                assert!(has_control(&value), "case {case}: {error}");
                assert_eq!(error.reason, Reason::ControlCharacter);
                continue;
            }
            Ok(raw) => raw,
        };
        assert!(!has_control(&value), "case {case}: {value:?} accepted");
        accepted += 1;
        let header = header(&raw, "Subject");
        validate(header, "Subject: ".len());
        let parsed = parsed(&raw).subject().unwrap_or_default().to_owned();
        assert_eq!(
            collapse(&parsed),
            collapse(&value),
            "case {case} of {value:?}"
        );
    }
    assert!(accepted > CASES / 10, "{accepted}");
}

#[test]
fn seeded_address_lists_round_trip() {
    let mut prng = Prng::new(0x5eed_1234_abcd_0003);
    let mut accepted = 0;
    for case in 0..CASES {
        let to: Vec<Address> = (0..prng.range(1, 6))
            .map(|_| {
                let email = gen_email(&mut prng);
                let name = (!prng.chance(4)).then(|| gen_phrase(&mut prng));
                Address { name, email }
            })
            .collect();
        let mut message = base(plain("a@example.org"));
        message.to.clone_from(&to);
        let raw = match write(&message) {
            Err(error) => {
                let named = &to[error.index.expect("an index")];
                assert!(
                    named.name.as_deref().is_some_and(has_control),
                    "case {case}: {error}"
                );
                continue;
            }
            Ok(raw) => raw,
        };
        accepted += 1;
        let header = header(&raw, "To");
        validate(header, "To: ".len());
        let list: Vec<(Option<String>, Option<String>)> = parsed(&raw).to().map_or_default(|to| {
            to.mailboxes()
                .map(|addr| {
                    (
                        addr.name().map(str::to_owned),
                        addr.address().map(str::to_owned),
                    )
                })
                .collect()
        });
        assert_eq!(
            list.len(),
            to.len(),
            "case {case}: {:?}",
            String::from_utf8_lossy(header)
        );
        for (parsed, given) in list.iter().zip(&to) {
            assert_eq!(
                parsed.1.as_deref(),
                Some(given.email.as_str()),
                "case {case}"
            );
            if let Some(name) = given.name.as_deref().filter(|name| !name.trim().is_empty()) {
                assert_eq!(
                    parsed.0.as_deref().map(collapse),
                    Some(collapse(name)),
                    "case {case}: {:?}",
                    String::from_utf8_lossy(header)
                );
            }
        }
    }
    assert!(accepted > CASES / 10, "{accepted}");
}

#[test]
fn seeded_attachment_types_and_file_names_round_trip() {
    let mut prng = Prng::new(0x5eed_1234_abcd_0006);
    for case in 0..CASES {
        let content_type = [
            "text/plain",
            "application/pdf",
            "image/png",
            "application/x-thing",
        ][prng.below(4)];
        let mut name = gen_phrase(&mut prng);
        if prng.chance(2) {
            name.push_str(".pdf");
        }
        let mut message = base(plain("a@example.org"));
        message.attachments = vec![Attachment {
            filename: Some(name.clone()),
            content_type: content_type.to_owned(),
            content_id: None,
            bytes: b"x".to_vec(),
        }];
        let raw = match write(&message) {
            Err(error) => {
                assert!(has_control(&name), "case {case}: {error}");
                continue;
            }
            Ok(raw) => raw,
        };
        validate(
            header(&raw, "Content-Disposition"),
            "Content-Disposition: ".len(),
        );
        let message = parsed(&raw);
        let part = message.attachments().next().expect("an attachment");
        let ct = part.content_type().expect("a content type");
        assert_eq!(
            format!("{}/{}", ct.ctype(), ct.subtype().unwrap_or_default()),
            content_type
        );
        if !name.trim().is_empty() {
            assert_eq!(
                part.attachment_name().map(collapse),
                Some(collapse(&name)),
                "case {case}: {:?}",
                String::from_utf8_lossy(header(&raw, "Content-Disposition"))
            );
        }
    }
}

#[test]
fn seeded_reference_lists_round_trip() {
    let mut prng = Prng::new(0x5eed_1234_abcd_0007);
    for _ in 0..CASES {
        let ids: Vec<String> = (0..prng.range(1, 6)).map(|_| gen_id(&mut prng)).collect();
        let mut message = base(plain("a@example.org"));
        message.references = ids
            .iter()
            .map(|id| MessageId::parse(id).expect("id"))
            .collect();
        let raw = write(&message).expect("compose");
        let header = header(&raw, "References");
        validate(header, "References: ".len());
        let parsed: Vec<String> = parsed(&raw)
            .references()
            .map_or_default(|ids| ids.iter().map(str::to_owned).collect());
        assert_eq!(parsed, ids, "{:?}", String::from_utf8_lossy(header));
    }
}

#[test]
fn seeded_from_headers_keep_the_folding_rules() {
    let mut prng = Prng::new(0x5eed_1234_abcd_0009);
    for _ in 0..CASES / 4 {
        let name = gen_phrase(&mut prng);
        let from = Address {
            name: Some(name.clone()),
            email: gen_email(&mut prng),
        };
        match write(&base(from)) {
            Ok(raw) => validate(header(&raw, "From"), "From: ".len()),
            Err(error) => {
                assert!(has_control(&name));
                assert_eq!(
                    (error.field, error.reason),
                    ("from", Reason::ControlCharacter)
                );
            }
        }
    }
}

#[test]
fn seeded_custom_headers_fold_within_78_columns_under_any_name_length() {
    let mut prng = Prng::new(0x5eed_1234_abcd_000a);
    for len in [1usize, 2, 8, 30, 60, 70, 74, 76] {
        let name = format!("X{}", "y".repeat(len - 1));
        for _ in 0..300 {
            let value = gen_phrase(&mut prng);
            let mut message = base(plain("a@example.org"));
            message.headers = vec![(name.clone(), value.clone())];
            match write(&message) {
                Ok(raw) => validate(header(&raw, &name), name.len() + 2),
                Err(error) => {
                    assert!(has_control(&value));
                    assert_eq!(error.reason, Reason::ControlCharacter);
                }
            }
        }
    }
}

#[test]
fn the_date_header_matches_a_civil_calendar_reference() {
    let mut prng = Prng::new(0x5eed_1234_abcd_000b);
    let low = -2_208_988_800i64;
    let high = 7_258_118_400i64;
    let span = u64::try_from(high - low).expect("positive");
    for case in 0..DATE_CASES / 10 {
        let date = match case {
            0 => 0,
            1 => low,
            2 => high,
            3 => -1,
            4 => 86399,
            5 => -86400,
            _ => low + i64::try_from(prng.next() % span).expect("in range"),
        };
        let mut message = base(plain("a@example.org"));
        message.sent_at = date;
        let raw = write(&message).expect("compose");
        assert_eq!(
            String::from_utf8_lossy(header(&raw, "Date")),
            format!("Date: {}\r\n", civil_reference(date)),
            "for timestamp {date}"
        );
    }
}

fn civil_reference(date: i64) -> String {
    const DOW: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTH: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = date.div_euclid(86400);
    let seconds = date.rem_euclid(86400);
    let leap = |year: i64| (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days_in_year = |year: i64| if leap(year) { 366 } else { 365 };
    let mut year = 1970i64;
    let mut left = days;
    while left < 0 {
        year -= 1;
        left += days_in_year(year);
    }
    while left >= days_in_year(year) {
        left -= days_in_year(year);
        year += 1;
    }
    let months = [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0;
    while left >= months[month] {
        left -= months[month];
        month += 1;
    }
    format!(
        "{}, {} {} {:04} {:02}:{:02}:{:02} +0000",
        DOW[usize::try_from((days + 4).rem_euclid(7)).expect("weekday")],
        left + 1,
        MONTH[month],
        year,
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60,
    )
}

#[test]
fn seeded_input_with_control_characters_is_refused() {
    let long = "a".repeat(40);
    let names: Vec<String> = [
        ":\n:\\",
        "\u{1}\n(\\",
        "\u{8}\0\0\n\\",
        "a\rb",
        "\r\n",
        "\\\n",
        "\"\n",
        "x\ty\nz\\",
    ]
    .iter()
    .flat_map(|name| {
        [
            (*name).to_owned(),
            format!("{long}{name}"),
            format!("{name}{long}"),
        ]
    })
    .collect();
    for name in &names {
        let refused = |message: &OutgoingMessage| {
            let error = write(message).expect_err("refused");
            assert_eq!(error.reason, Reason::ControlCharacter, "{name:?}");
            assert!(!error.to_string().contains(name.as_str()));
        };
        let mut display = base(plain("a@b.com"));
        display.to = vec![Address {
            name: Some(name.clone()),
            email: "a@b.com".into(),
        }];
        refused(&display);
        let mut file = base(plain("a@b.com"));
        file.attachments = vec![Attachment {
            filename: Some(name.clone()),
            content_type: "application/pdf".into(),
            content_id: None,
            bytes: Vec::new(),
        }];
        refused(&file);
        let mut bare = base(plain("a@b.com"));
        bare.to = vec![plain(name)];
        refused(&bare);
        let mut subject = base(plain("a@b.com"));
        subject.subject = Some(name.clone());
        refused(&subject);
    }
}
