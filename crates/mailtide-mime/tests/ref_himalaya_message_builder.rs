//! Tests ported by name from himalaya's `src/shared/message/builder.rs`
//! (pimalaya/himalaya, MIT OR Apache-2.0, commit a5623dcac615): its
//! composition cases, as `features/compose-mime.toml` maps them. Himalaya
//! takes `Name <addr>` strings and reads the clock and host name; compose
//! takes Address values, `parse_addresses` is the bridge, and the id and date
//! are inputs. Reply, forward and quoting are mapped in `features/send.toml`.

use mail_parser::MessageParser;
use mailtide_mime::{
    Address, Attachment, ComposeOptions, MessageId, OutgoingMessage, Purpose, Reason, compose,
    forward_plan, parse_addresses, quote_plain, reply_plan,
};

fn bare(email: &str) -> Address {
    Address {
        name: None,
        email: email.to_owned(),
    }
}

fn message(from: Address, to: Vec<Address>, subject: Option<&str>, body: &str) -> OutgoingMessage {
    let mut message = OutgoingMessage::new(from, 1_700_000_000);
    message.to = to;
    message.subject = subject.map(str::to_owned);
    message.text = Some(body.to_owned());
    message
}

fn write(message: &OutgoingMessage) -> Vec<u8> {
    let draft = ComposeOptions {
        purpose: Purpose::Draft,
        max_bytes: None,
    };
    compose(message, &draft).expect("compose").raw
}

fn mailboxes(raw: &[u8], header: &str) -> Vec<(Option<String>, String)> {
    let parsed = MessageParser::new().parse(raw).expect("parse");
    let list = match header {
        "From" => parsed.from(),
        "To" => parsed.to(),
        _ => parsed.cc(),
    };
    list.map(|list| {
        list.mailboxes()
            .map(|mailbox| {
                (
                    mailbox.name().map(str::to_owned),
                    mailbox.address().unwrap_or_default().to_owned(),
                )
            })
            .collect()
    })
    .unwrap_or_default()
}

#[test]
fn a_new_message_has_its_headers_and_body_and_no_in_reply_to() {
    let mut new = message(
        bare("alice@example.com"),
        vec![bare("bob@example.com")],
        Some("Hello"),
        "Hi Bob",
    );
    new.cc = vec![bare("carol@example.com")];

    let raw = write(&new);
    let parsed = MessageParser::new().parse(&raw).expect("parse");
    let text = String::from_utf8(raw.clone()).expect("ASCII");

    assert_eq!(parsed.subject(), Some("Hello"));
    assert!(parsed.body_text(0).unwrap_or_default().contains("Hi Bob"));
    assert!(text.contains("alice@example.com"));
    assert!(text.contains("bob@example.com"));
    assert!(text.contains("carol@example.com"));
    assert!(!text.contains("In-Reply-To"));
}

#[test]
fn a_from_name_with_a_comma_is_quoted() {
    let from = Address {
        name: Some("Doe, Alice".into()),
        email: "alice@example.com".into(),
    };
    let raw = write(&message(
        from,
        vec![bare("bob@example.com")],
        Some("Hello"),
        "Hi Bob",
    ));
    let text = String::from_utf8(raw).expect("ASCII");
    // NOTE: a comma is what an unquoted display name breaks on, the header
    // then reading as two addresses.
    assert!(
        text.contains("From: \"Doe, Alice\" <alice@example.com>"),
        "{text}"
    );
}

#[test]
fn a_minted_message_id_sits_on_the_senders_domain_and_is_written_once() {
    let from = parse_addresses("Alice <alice@example.com>").remove(0);
    let mut new = message(
        from.clone(),
        vec![bare("bob@example.com")],
        Some("Hello"),
        "Hi Bob",
    );
    new.message_id = Some(MessageId::from_entropy([7; 16], &from).expect("id"));

    let raw = write(&new);
    let parsed = MessageParser::new().parse(&raw).expect("parse");
    let id = parsed.message_id().expect("a message id");

    assert!(id.ends_with("@example.com"), "{id}");
    assert!(id.len() > "@example.com".len());
    let text = String::from_utf8(raw).expect("ASCII");
    assert_eq!(text.matches("Message-ID:").count(), 1);
}

#[test]
fn a_message_id_domain_must_be_a_plain_dot_atom() {
    let mint = |email: &str, entropy: u8| MessageId::from_entropy([entropy; 16], &bare(email));
    assert!(mint("alice@example.com", 1).is_ok());
    // Different entropy, different id; the caller draws it.
    assert_ne!(mint("alice@example.com", 1), mint("alice@example.com", 2));
    assert_eq!(mint("alice", 1), Err(Reason::NoAt));
    assert_eq!(mint("alice@", 1), Err(Reason::EmptyDomain));
    assert_eq!(mint("alice@[192.0.2.1]", 1), Err(Reason::DomainLiteral));
    assert_eq!(mint("alice@exa mple.com", 1), Err(Reason::Whitespace));
    assert_eq!(mint("alice@b\u{fc}cher.example", 1), Err(Reason::NotAscii));
}

#[test]
fn parse_list_splits_names_and_addresses() {
    let alice = |name: Option<&str>| {
        vec![Address {
            name: name.map(str::to_owned),
            email: "alice@example.org".into(),
        }]
    };

    assert_eq!(
        parse_addresses("Alice <alice@example.org>"),
        alice(Some("Alice"))
    );
    assert_eq!(parse_addresses("alice@example.org"), alice(None));
    assert_eq!(
        parse_addresses("\"Doe, Alice\" <alice@example.org>"),
        alice(Some("Doe, Alice"))
    );
    assert_eq!(
        parse_addresses("=?utf-8?B?QWxpY2U=?= <alice@example.org>"),
        alice(Some("Alice"))
    );
    let two = parse_addresses("alice@example.org, Bob <bob@example.org>");
    assert_eq!(two[0], bare("alice@example.org"));
    assert_eq!(
        two[1],
        Address {
            name: Some("Bob".into()),
            email: "bob@example.org".into()
        }
    );
    // NOTE: himalaya errors; parse_addresses reads what it can, nothing here.
    assert!(parse_addresses("not-an-address").is_empty());
    assert!(parse_addresses("").is_empty());
}

#[test]
fn a_from_parsed_from_text_is_written_as_name_and_address() {
    // The whole string given as the email is refused, never written as
    // `<Alice <alice@example.org>>`.
    let whole = message(bare("Alice <alice@example.org>"), vec![], None, "hi");
    let draft = ComposeOptions {
        purpose: Purpose::Draft,
        max_bytes: None,
    };
    let error = compose(&whole, &draft).expect_err("refused");
    assert_eq!((error.field, error.reason), ("from", Reason::NameInAddress));

    let from = parse_addresses("Alice <alice@example.org>").remove(0);
    let raw = write(&message(from, vec![], None, "hi"));
    let text = String::from_utf8(raw.clone()).expect("ASCII");
    assert!(!text.contains("<Alice <alice@example.org>>"));
    assert_eq!(
        mailboxes(&raw, "From"),
        [(Some("Alice".into()), "alice@example.org".into())]
    );
}

#[test]
fn a_to_parsed_from_text_is_written_as_name_and_address() {
    let to = parse_addresses("Alice <alice@example.org>");
    let raw = write(&message(bare("sender@example.org"), to, None, "hi"));
    let text = String::from_utf8(raw.clone()).expect("ASCII");
    // regression: the whole value used to land inside the brackets
    assert!(!text.contains("<Alice <alice@example.org>>"));
    assert_eq!(
        mailboxes(&raw, "To"),
        [(Some("Alice".into()), "alice@example.org".into())]
    );
}

#[test]
fn every_address_of_every_parsed_value_is_written() {
    let to: Vec<Address> = [
        "\"Doe, Alice\" <alice@example.org>, bob@example.org",
        "carol@example.org",
    ]
    .into_iter()
    .flat_map(parse_addresses)
    .collect();
    let raw = write(&message(bare("sender@example.org"), to, None, "hi"));
    assert_eq!(
        mailboxes(&raw, "To"),
        [
            (Some("Doe, Alice".into()), "alice@example.org".into()),
            (None, "bob@example.org".into()),
            (None, "carol@example.org".into()),
        ]
    );
}

#[test]
fn a_single_part_message_ends_with_a_line_break() {
    let raw = write(&message(bare("bob@example.com"), vec![], None, "hi"));
    assert!(
        raw.ends_with(b"\r\nhi\r\n"),
        "{}",
        String::from_utf8_lossy(&raw)
    );
}

#[test]
fn a_7_bit_attached_message_is_written_unencoded_with_crlf() {
    let mut new = message(bare("bob@example.com"), vec![], None, "see attached");
    new.attachments = vec![Attachment {
        filename: Some("inner.eml".into()),
        content_type: "message/rfc822".into(),
        content_id: None,
        bytes: b"Subject: inner\nFrom: a@example.org\n\nbody\n".to_vec(),
    }];
    let lf = String::from_utf8(write(&new)).expect("ASCII");
    assert!(lf.contains("Content-Type: message/rfc822\r\n"), "{lf}");
    assert!(!lf.contains("base64"), "{lf}");
    assert!(lf.contains("\r\nSubject: inner\r\nFrom: a@example.org\r\n\r\nbody\r\n"));
    // NOTE: himalaya re-encodes an 8-bit inner message; compose carries it as
    // a base64 file instead (an_8_bit_attached_message_becomes_a_base64_file).
}

const REPLY_SOURCE: &[u8] = b"From: Alice <alice@example.com>\r\n\
    To: Bob <bob@example.com>, Carol <carol@example.com>\r\n\
    Cc: Dan <dan@example.com>, ALICE@example.com\r\n\
    Subject: Project update\r\nMessage-ID: <parent@example.com>\r\n\
    References: <first@example.com> <second@example.com>\r\n\r\nOriginal body";

fn email_list(list: &[Address]) -> Vec<&str> {
    list.iter().map(|address| address.email.as_str()).collect()
}

#[test]
fn a_reply_plan_sets_subject_recipients_and_threading() {
    let plan = reply_plan(REPLY_SOURCE, &["bob@example.com"], false).unwrap();
    assert_eq!(plan.subject, "Re: Project update");
    assert_eq!(email_list(&plan.to), ["alice@example.com"]);
    assert_eq!(plan.quote_text.as_deref(), Some("Original body"));
    assert_eq!(
        plan.in_reply_to.as_ref().map(MessageId::as_str),
        Some("parent@example.com")
    );
}

#[test]
fn a_reply_subject_keeps_one_re_prefix() {
    let plan = reply_plan(b"Subject: Re: Already replied\r\n\r\nx", &[], false).unwrap();
    assert_eq!(plan.subject, "Re: Already replied");
}

#[test]
fn a_subject_that_only_starts_with_the_letters_re_gets_the_prefix() {
    let plan = reply_plan(b"Subject: Ready to ship\r\n\r\nx", &[], false).unwrap();
    assert_eq!(plan.subject, "Re: Ready to ship");
}

#[test]
fn a_plain_reply_plan_has_no_cc() {
    let plan = reply_plan(REPLY_SOURCE, &["bob@example.com"], false).unwrap();
    assert!(plan.cc.is_empty());
}

#[test]
fn reply_all_lists_every_other_recipient_once() {
    let plan = reply_plan(REPLY_SOURCE, &["bob@example.com"], true).unwrap();
    assert_eq!(
        email_list(&plan.to),
        ["alice@example.com", "carol@example.com"]
    );
    assert_eq!(email_list(&plan.cc), ["dan@example.com"]);
}

#[test]
fn reply_all_keeps_display_names() {
    let plan = reply_plan(REPLY_SOURCE, &["bob@example.com"], true).unwrap();
    assert_eq!(plan.to[0].name.as_deref(), Some("Alice"));
    assert_eq!(plan.to[1].name.as_deref(), Some("Carol"));
    assert_eq!(plan.cc[0].name.as_deref(), Some("Dan"));
}

#[test]
fn reply_all_never_repeats_a_to_address_in_cc() {
    let plan = reply_plan(
        b"From: alice@example.com\r\nTo: bob@example.com\r\nCc: ALICE@example.com, BOB@example.com\r\n\r\nx",
        &[],
        true,
    )
    .unwrap();
    assert_eq!(
        email_list(&plan.to),
        ["alice@example.com", "bob@example.com"]
    );
    assert!(plan.cc.is_empty());
}

#[test]
fn reply_all_without_own_addresses_keeps_every_recipient() {
    let plan = reply_plan(REPLY_SOURCE, &[], true).unwrap();
    assert_eq!(
        email_list(&plan.to),
        ["alice@example.com", "bob@example.com", "carol@example.com"],
    );
    assert_eq!(email_list(&plan.cc), ["dan@example.com"]);
}

#[test]
fn reply_all_leaves_out_every_own_address_given() {
    let plan = reply_plan(REPLY_SOURCE, &["BOB@example.com", "dan@example.com"], true).unwrap();
    assert_eq!(
        email_list(&plan.to),
        ["alice@example.com", "carol@example.com"]
    );
    assert!(plan.cc.is_empty());
}

#[test]
fn a_forward_plan_prefixes_the_subject_and_has_references_but_no_in_reply_to() {
    let plan = forward_plan(REPLY_SOURCE).unwrap();
    assert_eq!(plan.subject, "Fwd: Project update");
    assert_eq!(
        plan.references.last().map(MessageId::as_str),
        Some("parent@example.com")
    );
    let mut message = OutgoingMessage::new(bare("bob@example.com"), 0);
    message.to.push(bare("reader@example.com"));
    message.subject = Some(plan.subject);
    message.references = plan.references;
    message.text = plan.quote_text.map(|text| quote_plain(&text));
    let composed = compose(&message, &ComposeOptions::default()).unwrap();
    let parsed = MessageParser::new().parse(&composed.raw).unwrap();
    assert!(parsed.in_reply_to().is_none());
    assert!(parsed.references().unwrap().contains("parent@example.com"));
}

#[test]
fn a_subject_prefix_needs_its_colon_and_ignores_case() {
    for (input, expected) in [
        ("RE: Review", "Re: Review"),
        ("rE:Review", "Re: Review"),
        ("Review", "Re: Review"),
    ] {
        let source = format!("Subject: {input}\r\n\r\nx");
        assert_eq!(
            reply_plan(source.as_bytes(), &[], false).unwrap().subject,
            expected,
        );
    }
}

#[test]
fn references_are_the_originals_chain_then_its_own_id() {
    let plan = reply_plan(REPLY_SOURCE, &[], false).unwrap();
    let ids: Vec<_> = plan.references.iter().map(MessageId::as_str).collect();
    assert_eq!(
        ids,
        [
            "first@example.com",
            "second@example.com",
            "parent@example.com"
        ]
    );

    let plan = reply_plan(
        b"Message-ID: <b@example.com>\r\nIn-Reply-To: <a@example.com>\r\n\r\nx",
        &[],
        false,
    )
    .unwrap();
    let ids: Vec<_> = plan.references.iter().map(MessageId::as_str).collect();
    assert_eq!(ids, ["a@example.com", "b@example.com"]);
}

#[test]
fn forward_parts_keep_their_content_type_parameters_and_decoded_bytes() {
    let raw = b"Content-Type: multipart/mixed; boundary=b\r\n\r\n\
        --b\r\nContent-Type: text/plain\r\n\r\nSee attached.\r\n\
        --b\r\nContent-Type: text/plain; charset=iso-8859-1; format=flowed\r\n\
        Content-Disposition: attachment; filename=note.txt\r\n\
        Content-Transfer-Encoding: quoted-printable\r\n\r\ncaf=E9\r\n\
        --b\r\nContent-Type: message/rfc822\r\n\r\nSubject: inner\r\n\r\ninner body\r\n--b--\r\n";
    let plan = forward_plan(raw).unwrap();
    let [note, message] = plan.parts.as_slice() else {
        panic!("expected two parts");
    };
    assert_eq!(note.attachment.content_type, "text/plain");
    assert_eq!(note.attachment.bytes, b"caf\xe9");
    assert_eq!(
        note.content_type_parameters,
        [
            ("charset".into(), "iso-8859-1".into()),
            ("format".into(), "flowed".into()),
        ]
    );
    assert_eq!(
        message.attachment.bytes,
        b"Subject: inner\r\n\r\ninner body"
    );
}

#[test]
fn quoting_prefixes_every_line_and_leaves_no_trailing_blank_after_an_empty_one() {
    assert_eq!(quote_plain("a\r\n\r\n> b"), "> a\n>\n>> b\n");
    assert_eq!(quote_plain("x\n"), "> x\n");
    assert_eq!(quote_plain(""), "");
}
