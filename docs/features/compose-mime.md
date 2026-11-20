# Compose: RFC 5322 messages from structured input

Plan: `features/compose-mime.toml` (roadmap C1). Status: done.

## 1. Goal and scope

Afterwards a host can turn a structured message into the bytes Gmail takes as
`raw`, for a draft or a send:

- From, To, Cc, Bcc, Reply-To and Sender with display names (non-ASCII
  included), a subject, custom headers;
- a text body, an HTML body or both; inline images referenced by `cid:`;
  attachments, including an attached message;
- threading headers (`Message-ID`, `In-Reply-To`, `References`) from the
  values the mirror already holds;
- a private `X-Mailtide-Id` header, the identity that survives Gmail: Gmail
  replaces a client's `Message-ID` and `Date` on every API send and draft
  save and keeps every other header byte for byte (settled 2026-11-20; see
  the rule in `README.md`);
- a result that carries the bytes, the ids written and the recipients, and a
  pure reader that finds the tracking id in raw bytes.

`compose` is a pure function in `mailtide-mime`, the counterpart of `parse`.
It makes no Gmail call, touches no store and adds no feed entry.

Left out, with the owner:

- sending or saving the bytes, base64url, `threadId`, upload size limits:
  `drafts` and `send`;
- deriving a reply or forward from the original (recipients, `Re:`, the
  References chain, carried attachments): `send`;
- which From addresses are allowed: `identities`;
- quoting the original as plain text: `send`, which plans the helper
  (`quote_plain`) as a pure function in this crate, built after `compose`;
- placing a signature, quoting HTML, sanitising HTML, turning `data:` images
  into inline parts, deriving text from HTML: the host;
- address groups, PGP and S/MIME, calendar invitations, streaming output.

## 2. What the references do

**Zero** (MIT). `driver/outgoing-mime.ts` wraps the `mimetext` library in two
functions (send, draft): HTML only, inline images then attachments, a
caller-fixed Message-ID, and the deduplicated recipients returned beside the
bytes. HTML is sanitised and `data:` images become cid parts in a separate
step first. Its IMAP driver mints `<uuid@sender-domain>` and reuses the id
when a draft is saved again. *Take:* bytes plus recipients as the result, an
id the caller fixes and keeps for the life of a draft, ids accepted with or
without angle brackets, drafts without recipients. *Leave:* caller headers
written unchecked (a header named `Bcc` adds a recipient), an empty From left
for Gmail to fill, duplicates silently removed from headers, and a second
copy of the draft builder inside `google.ts` that splits recipients on `, `.

**mail-builder** (Apache-2.0 OR MIT, Stalwart, 2.0.1 of 2026-11-02). The
builder half of `mail-parser`, which `mailtide-mime` already wraps. It
handles RFC 2047 names and subjects, RFC 2231 file names, folding, and the
choice of 7bit, quoted-printable or base64; its 62 tests include seeded
build-then-parse runs against `mail-parser`, and recent releases closed
header-injection holes. One required dependency (`encodify`, already in our
lock file). Three things do not fit and are handled by our wrapper: it fills
in Message-ID (hostname, clock), Date (clock) and boundaries (clock, thread)
when missing; its default layout puts inline parts in `multipart/mixed`; and
it silently drops or encodes control characters instead of refusing them.

**pimalaya core** (MIT). `mml` compiles a text template language to MIME with
mail-builder 0.3; its `email` crate adds new, reply and forward templates
with signatures. *Take:* the precedent for wrapping mail-builder, and tests
that pin exact bytes by fixing Message-ID and Date. *Leave:* the template
language.

**himalaya** (MIT/Apache). `src/shared/message/builder.rs` is one assembler
for compose, reply and forward. It has since moved to its own `mime-rs`
builder, which writes only what it is given. *Take:* the Message-ID sits on
the sender's domain (a hostname names the machine to every recipient), name
and address are kept apart, an attached message is never base64. *Leave:*
reading clock and hostname inside the builder; quoting and signatures.

**jmap-client, Stalwart server.** JMAP's Email creation (RFC 8621 §4.6) is a
flat object (`from`, `to`, `textBody`, `htmlBody`, `attachments`,
`messageId`, `sentAt`) from which the server builds the MIME tree:
the shape of our input. The server does it with mail-builder and wraps HTML
and its inline parts in `multipart/related` (path and names only, AGPL; the
pinned tree has no tests for it).

**mail-send.** Nothing but the rule that a message without recipients is
refused.

## 3. Design in mailtide

### Public API (Rust, `mailtide_mime`)

```rust
pub struct OutgoingMessage {
    pub from: Address,                  // exactly one
    pub sender: Option<Address>,
    pub reply_to: Vec<Address>,
    pub to: Vec<Address>, pub cc: Vec<Address>, pub bcc: Vec<Address>,
    pub subject: Option<String>,
    pub sent_at: i64,                   // unix seconds; written as Date, in UTC
    pub message_id: Option<MessageId>,  // written as given; Gmail replaces it
    pub tracking_id: Option<String>,    // X-Mailtide-Id; Gmail keeps it
    pub in_reply_to: Option<MessageId>,
    pub references: Vec<MessageId>,
    pub headers: Vec<(String, String)>, // custom, in order
    pub text: Option<String>,
    pub html: Option<String>,
    pub inline: Vec<Attachment>,        // content_id required
    pub attachments: Vec<Attachment>,
}
pub struct Address { pub name: Option<String>, pub email: String }
pub struct ComposeOptions { pub purpose: Purpose, pub max_bytes: Option<usize> }
pub enum Purpose { Draft, Send }
pub struct Composed {
    pub raw: Vec<u8>, pub message_id: Option<String>,
    pub tracking_id: Option<String>, pub recipients: Vec<String>,
}

pub fn compose(message: &OutgoingMessage, options: &ComposeOptions)
    -> Result<Composed, ComposeError>;

pub const TRACKING_HEADER: &str = "X-Mailtide-Id";
pub fn new_tracking_id(entropy: [u8; 16]) -> String;   // base64url, 22 chars
pub fn is_tracking_id(token: &str) -> bool;             // 16..=64 of A-Za-z0-9_-
pub fn tracking_id(raw: &[u8]) -> Option<String>;      // first valid header
```

- `Attachment` is the struct `parse` already returns, so the attachments of
  a parsed message can be composed again.
- `parse_addresses(&str)` turns a header value as the mirror stores it
  (`"Doe, Jane" <jane@x>`) into addresses (the same function `message-facts`
  plans, at the crate root). `MessageId::parse` accepts
  `<a@x>` and `a@x`; `MessageId::from_entropy([u8; 16], &Address)` mints
  `<32 hex digits@sender-domain>`.
- `Composed.message_id` has angle brackets. It is what the bytes say, not
  what Gmail will store: Gmail replaces it on every send and draft save.
  `Composed.tracking_id` is what identifies the message after Gmail has it;
  `drafts` and `send` require it on the bytes they are given, and
  `tracking_id(raw)` reads it back (headers only, never the body). `recipients` is To, Cc, Bcc in order, bare,
  deduplicated ignoring case.
- `ComposeError { field, index, reason }` never contains the offending
  value, so a host can log it.

Python (`mailtide`), thin over the same function, run without the GIL:

```python
compose_message(message: OutgoingMessage, *, purpose="send", max_bytes=None) -> ComposedMessage
new_message_id(sender: str) -> str
new_tracking_id() -> str
tracking_id(raw: bytes) -> str | None
parse_addresses(value: str) -> list[Address]
```

`OutgoingMessage`, `Address` and `ComposedMessage` are frozen classes with
the Rust field names (`from_` for `from`; `sent_at_ms`); an address may also
be given as a bare `str`. `message_id=None` and `tracking_id=None` mint one
each (128 random bits) and `sent_at_ms=None` is now: the impure defaults live
in the binding. `Attachment` gains a
constructor. `ComposeError` is a `ValueError`.

A reply needs no helper for its ids:
`in_reply_to=orig.rfc822_message_id`,
`references=[*orig.references, orig.rfc822_message_id]`.

### What compose writes

Headers, in this order: From, Sender, Reply-To, To, Cc, Bcc, Subject, Date,
Message-ID, In-Reply-To, References, X-Mailtide-Id, custom headers,
MIME-Version. compose writes them itself through mail-builder's header
writers, since `MessageBuilder::serialize` makes up a Message-ID when none is
given. Then one
fixed tree, each level present only when it has more than one child:

```
multipart/mixed
├─ multipart/alternative
│  ├─ text/plain
│  └─ multipart/related; type="text/html"
│     ├─ text/html
│     └─ inline parts (Content-ID, Content-Disposition: inline)
└─ attachments (Content-Disposition: attachment; filename)
```

With neither text nor HTML an empty `text/plain` stands in. Bodies are UTF-8;
mail-builder picks each part's transfer encoding and normalises line ends to
CRLF. A body holding a control character other than tab, CR and LF (NUL,
ESC, ...) is base64-encoded by compose, since mail-builder would declare it
`7bit` and write it raw; long lines already make mail-builder encode. An
attached `message/rfc822` is written unencoded (`7bit`) when it is
7-bit clean, as RFC 2046 requires; otherwise it becomes an
`application/octet-stream` file in base64, bytes exact.

**Bcc stays in the bytes.** Gmail's send has no envelope: it "sends the
specified message to the recipients in the To, Cc, and Bcc headers", and a
draft must keep its Bcc until it is sent.

**Determinism.** `compose` reads no clock, hostname or random source. The
date and the ids are inputs, and each boundary is derived from them (the
Message-ID, the tracking id, the date and the author) and the part's level; a
candidate that occurs in a body written unencoded is skipped. Same
input, same bytes: a retried send re-sends exactly what was first sent, and
tests compare bytes.

### What is validated (refused, never repaired)

| Input | Rule |
|---|---|
| any header value, name, file name, id | no control character (CR, LF, NUL, …): the header-injection rule |
| address | `local@domain`, ASCII, dot-atom local part, LDH domain, length limits; `Name <a@x>` as an email is refused |
| From | exactly one, always, also for a draft |
| recipients | at least one across To, Cc, Bcc for `Purpose::Send` |
| `message_id` | optional; `left@right`, dot-atom; `in_reply_to` and `references` only need to be one token (other senders' ids are often malformed) |
| `tracking_id` | optional; 16 to 64 characters of `A-Za-z0-9_-` |
| custom header | printable ASCII name without `:`; not a header compose writes (`X-Mailtide-Id` included), not `Content-*` |
| inline part | HTML present; content id given and unique |
| attachment | `type/subtype`, not `multipart/*` |
| size | `max_bytes`, checked before encoding and on the result |

### Engine, store, quota

None. No Gmail call (0 units), no migration, no feed entry, no engine or
simulator change. `mailtide-io` does not depend on `mailtide-mime`: `drafts`
and `send` take bytes. They need two things from here: bytes that are stable
across retries, and the tracking id, which they read from those bytes with
`tracking_id(raw)` and store on their row, so a lost answer can be matched to
the `SENT` message the import records with the same `X-Mailtide-Id`.

### Failure and retry

Every error is a permanent fault of the input; nothing is retried and no
partial output exists. Peak memory is the input plus about 1.4 times the
attachments (borrowed until encoded).

## 4. Alternatives rejected

- **Our own MIME writer:** RFC 2047, 2231, folding and quoted-printable are
  where composers break; mail-builder has them tested and maintained.
- **mail-builder's defaults (`text_body`, `html_body`, `inline`):** inline
  images land in `multipart/mixed`, and id, date and boundaries come from the
  hostname and clock.
- **himalaya's `mime-rs`:** strict in the way we want, but 0.2, tied to one
  project, and a second parser beside `mail-parser`.
- **A new `mailtide-compose` crate:** one more crate to version; round-trip
  tests need the parser anyway; ARCHITECTURE.md already lists Zero's
  `outgoing-mime.ts` under `mailtide-mime`.
- **A builder or a caller-supplied part tree:** more API, and every host
  would rebuild the same tree. JMAP makes the same choice.
- **A template language (MML):** hosts have structured data.
- **Sanitising bad input as mail-builder does:** a stripped address is a
  different address; the caller should hear about it.
- **Stripping Bcc and returning an envelope (Zero's SMTP path):** Gmail has
  no envelope parameter.
- **Composing inside `Runner.send`:** the driver would depend on MIME, and
  hosts that already have bytes could not use it.

## 5. Tests

Suites were found with `grep -ri` for `mime`, `multipart`, `message-id`,
`bcc`, `attachment` and `compose` over every checkout, then read.

- **Zero `driver/outgoing-mime.test.ts`:** all 3 mapped. `smtp/client.test.ts`
  is not declared here: `send` lists its five tests (SMTP transport). Its
  Bcc-stripping helper is the opposite of our rule, which
  `bcc_is_written_as_a_header` pins.
- **himalaya `shared/message/builder.rs`:** 10 composition tests mapped; 19
  deferred to `send` (reply and forward derivation, and three on quoting and
  signature layout: `send` plans the plain-text quoting helper, and signature
  placement is host-side in both plans). Two signature tests `identities`
  left to this plan are claimed as `na`.
- **pimalaya core `mml` compilers:** 5 mapped by behaviour, 2 `na` (template
  markup, a file path).
- **mail-builder, seven files:** 31 mapped, 20 `na`. They are run through
  `compose` and `parse`, not against the library's types, so a dependency
  upgrade that changes our bytes fails here. Five are deliberately the
  opposite of the reference: where it strips or encodes a control character,
  ours refuses. `na` are library internals (its random boundary and id
  generators, output sinks, scans) and address groups.

Not declared: mail-builder `writer.rs` (buffering); the MML grammar parsers
and interpreters; pimalaya's template tests and Zero's
`assistant-addressing`, `assistant-forward` and `forward-attachments` suites
(reply and forward: `send`); `email-composition.test.ts` (sender identity:
`identities`); himalaya `email/submission.rs` and mail-send `smtp/message.rs`
(an SMTP envelope parsed from raw bytes; `send` may want the first as a
pre-flight check).

Our own 50 tests add: each level of the tree and the round trip through
`parse`; the reserved-header and injection rules for custom headers;
`Purpose`; identical bytes for identical input and boundary collisions; the
size limit; address syntax; id minting; the tracking id written once,
refused when malformed or duplicated, and read back from the headers only; a
message without a Message-ID; bodies with NUL, other control characters or
over-long lines encoded and round-tripped; the mirror's own header parse
reading back the ids compose wrote; and the Python API, its keywords checked
against the stub.

## 6. Slices

1. **Types and rules.** `Address` with `parse_addresses`, `MessageId`, the
   tracking id and its reader, `OutgoingMessage`, `ComposeError`, every
   validation; no bytes yet.
2. **Text and HTML.** The mail-builder dependency, header order, text, HTML
   and alternative, `Purpose`, boundaries; the Zero, pimalaya, himalaya and
   mail-builder header suites; `mirror_facts`.
3. **Inline parts and attachments.** `related` and `mixed`, file names, the
   size limit, the round trip through `parse`.
4. **Attached messages.** `message/rfc822`, unencoded or as a file.
5. **Python and docs.** Bindings, stubs, `tests/test_compose.py`, SPEC §5.5,
   §11 and §14, ARCHITECTURE.md, README.

## 7. Decisions

1. **Wrap mail-builder 2 (no default features).** Tested, maintained, same
   licence and author as our parser, no new transitive dependency. *Would
   change:* a release that cannot take explicit boundaries, id and date.
2. **In `mailtide-mime`, no feature flags.** Parse and compose share
   `Attachment`, `Address` and tests. *Would change:* a host that needs one
   half without the other's dependency.
3. **Refuse, never repair.** Injection attempts and typos surface as errors
   naming the field. *Would change:* nothing foreseen.
4. **Flat JMAP-shaped input, one tree.** *Would change:* a need for calendar
   invitations or signed mail, which want a part tree.
5. **The identity is `X-Mailtide-Id`, not the Message-ID.** Gmail replaces
   a client's Message-ID on every send and every draft save
   (`drafts-message-id-kept` and `send-message-id-kept`, refuted
   2026-11-20) and keeps other client headers byte for byte. So `tracking_id`
   (128 random bits, base64url) is minted by the caller, written as a
   private header, and read back by `tracking_id(raw)`; exactly-once in
   `send` and send confirmation in `drafts` compare it. The Message-ID stays
   an optional input written as given (RFC 5322 asks for one; other
   transports and tests keep it), and `new_message_id` still mints one on the
   sender's domain. *Would change:* Gmail stripping unknown `X-` headers.
6. **The crate stays pure; entropy and time come from the caller.** The
   binding supplies both by default. *Would change:* Rust hosts asking for a
   convenience, which would be a separate impure helper.
7. **Bcc is a header in the bytes.** Required by Gmail's API for delivery.
   *Would change:* `send-bcc-stripped` refuted (owned by `send`, which
   refuses a message with Bcc until it has passed); compose then gains a mode
   that leaves the header out.
8. **`Purpose` decides whether recipients are required; From always is.**
   Zero's empty From relies on Gmail filling it in. *Would change:* `send`
   preferring to check recipients itself.
9. **Headers are written as given; only `recipients` is deduplicated.** A
   host may copy itself on purpose.
10. **Non-ASCII addresses are refused; an IDN domain comes as A-labels.** No
    IDNA dependency, and Gmail's handling of SMTPUTF8 through the API is
    not relied on. *Would change:* a host with such recipients; then add an
    unknown and test it.
11. **Inline parts and attachments are two lists of the parser's
    `Attachment`.** No new type and no dependence on how `parse` reports
    disposition. Whether the HTML references each content id is not checked:
    that needs an HTML parser, which stays with the host.
12. **Signature placement, HTML quoting, sanitising and text-from-HTML stay
    host-side; reply and forward derivation and the plain-text quoting
    helper are planned by `send`**, as pure functions in `mailtide-mime`
    built after `compose`. The host-side ones are presentation choices, and
    `MessageContent` already gives a host the ids for a reply.
13. **`alternative > related`, not Gmail's own `related > alternative`.**
    The images belong to the HTML alternative (RFC 2387). *Would change:*
    `compose-mime-inline-related` refuted.
14. **An attached message is unencoded when 7-bit clean, else a base64
    file.** Never RFC-invalid, never altered. *Would change:* evidence that
    Gmail delivers `8bit` parts intact to every recipient.
15. **No built-in size limit.** `max_bytes` is the caller's; Gmail's limits
    belong to `drafts` and `send`.
16. **Date is always written, in UTC.** RFC 5322 requires it; a local offset
    would disclose the user's time zone. Gmail rewrites it with the time of
    each draft save and of the send (`compose-mime-date-kept`, refuted
    2026-11-20, harmless), so what compose writes is what other transports
    see.

Settled about Gmail:

- `compose-mime-date-kept` (refuted): Gmail rewrites Date at every save and
  send; compose keeps writing it.

Assumed about Gmail, to be tested before work starts:

- `compose-mime-rfc2231-filename`: Gmail reads RFC 2231 file names.
- `compose-mime-inline-related`: Gmail shows a `related` image under the HTML alternative inline.

Relied on, owned elsewhere:

- `drafts-message-id-kept` (`drafts`) and `send-message-id-kept` (`send`),
  both refuted: Gmail replaces the client's Message-ID and keeps a custom
  header through the draft calls and `messages.send`. Hence decision 5.
- `send-bcc-stripped` (`send`): Gmail removes Bcc from delivered copies and
  keeps it in a stored draft and in the sender's `SENT` copy. If a draft
  loses its Bcc, the host must keep Bcc beside the draft.
