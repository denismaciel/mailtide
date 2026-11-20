# Message facts: more envelope headers, an attachment summary, and reading one part

Plan: `features/message-facts.toml` (roadmap R2, R3, R8). Status: done.

## 1. Goal and scope

Afterwards a host can, per message and without parsing anything itself:

- read ten more envelope facts from `messages()` and from the import entry of
  the change feed: Bcc, Reply-To, the Date header, the sender's display name
  and address, List-Id, List-Unsubscribe (with one-click), Auto-Submitted and
  Precedence; and the `X-Mailtide-Id` token of a message mailtide wrote, which
  drafts and send settle a lost answer by;
- show a paperclip and an attachment list: a count, and per part its filename,
  media type, decoded size, and whether it is inline or attached;
- read one attachment or inline image by a reference taken from that list,
  with one ranged read of the stored blob, or stream it in bounded chunks;
- bring messages imported before this change up to date, on its own schedule,
  without a Gmail call.

Left out, with the owner:

- classifying mail (newsletter, automated): a host rule over the facts;
- rewriting `cid:` links, sanitising HTML, making a filename safe for a disk:
  the host's;
- an attachment index or search across messages: a host query over the
  summaries it copies from the feed;
- sniffing media types from content; decrypting or verifying signed mail;
- streaming the whole raw message (the same mechanism; add when asked for);
- forwarding attachments (`send`, which will read parts through this API).

## 2. What the references do

**Zero** (MIT). The Gmail driver reads `format=full` for the part list and one
`attachments.get` (20 units) per part, behind an object-store cache; a part is
an attachment when it has a filename and is not "inline with a Content-ID";
inline images are fetched during import and spliced into the HTML as data
URIs, where a 404 on one image must not look like a missing thread
(`gmail-inline-asset.ts`). Its IMAP driver is the closer model: it holds the
raw message, lists attachments from a parse, and addresses one by its index in
the parser's list, parsing the whole message again on every read.
`imap/categorize.ts` shows which headers a host classifier wants. *Take:*
parts come from the raw message; missing is told apart from failing; the
marker headers. *Leave:* the Gmail calls and their cache (our blob already
holds every part), the index as an address, assembling HTML at import.

**himalaya** (MIT/Apache). `src/shared/message/part.rs` is the most careful
reference: one walk shared by every command so a part id means the same
everywhere, an attached message is one leaf, sizes are counted after transfer
decoding, and a tested has-attachment rule (a disposition other than `inline`
counts; with none, anything but text or a part named by a Content-ID; RFC 1847
signature and encryption parts never). Its Gmail backend leaves
`has_attachment` unset because `format=metadata` cannot tell. *Take:* the rule
(ours agrees with all but one of its fifteen cases), the size definition, the
optional `has_attachment`, the lenient Date parse. *Leave:* refusing a
too-complex message (we must import it), part ids by position, the rules for
writing downloads to disk.

**mail-parser** (MIT/Apache), which `mailtide-mime` already wraps. It gives
typed getters for every header we add, the RFC 8621 body lists, each part's
byte offsets in the raw input, its transfer encoding, its decoded length, and
`Encoding::decode`, which decodes a byte slice exactly as a parsed part is
decoded. It never fails on malformed input and has depth and part limits.
*Take:* all of it. *Add:* it knows no protocol parts and applies no `cid:`
exception; its `has_attachments` is false for an undeclared image in
`multipart/mixed`, where himalaya and we say attached.

**jmap-client, Stalwart server.** JMAP names what we add: `bcc`, `replyTo`,
`sentAt`, `hasAttachment`, `attachments`, and a body part's `blobId`, `size`,
`name`, `type`, `disposition`, `cid`. The client's blob download returns whole
bodies and has no test for any of this. The server (AGPL: names only)
addresses a part as the message blob plus a section of start offset, size and
transfer encoding (`BlobSection`, `fetch_blob_section`), and computes
`hasAttachment` once at ingest. That is the addressing this plan uses.

**pimalaya core.** `has_attachment` only for `Content-Disposition:
attachment`, and attachments returned all at once with a sniffed type: the
two things we move away from. **mail-builder, mail-send:** nothing here.

## 3. Design in mailtide

**Approach.** The import already holds the raw bytes. The driver parses them
once with `mailtide-mime` and reports the result with the import; the core
stores it inside the content record the message row already has. Each listed
part carries a **part reference**: the byte range of its body in the stored
blob plus its transfer encoding and decoded size. Reading a part is a ranged
blob read and a decode: no parse, no store read, no Gmail call.

### What is stored

`StoredContent` (the JSON in `mailtide_messages.content`) gains one optional
field, `facts`. Rows written before have no such key and read as `None`.
**No table or column changes**; the schema version is raised by a
version-only migration step (see Compatibility below).

| Fact | Value |
|---|---|
| `version` | `FACTS_VERSION` (1) of the extractor that wrote it |
| `bcc`, `reply_to` | address lists as written, split like `to` and `cc` today |
| `sent_at` | the Date header as an instant plus the sender's UTC offset in minutes; absent when missing or unreadable (a wrong weekday is ignored) |
| `sender_name`, `sender_email` | decoded display name (RFC 2047) and address of the first From mailbox |
| `list_id` | the identifier inside the angle brackets, else the trimmed value |
| `list_unsubscribe` | its URIs, at most 4 of at most 2,048 bytes each |
| `one_click_unsubscribe` | true with `List-Unsubscribe-Post: List-Unsubscribe=One-Click` and an https URI (RFC 8058) |
| `auto_submitted`, `precedence` | the keyword, lowercased, parameters dropped |
| `tracking_id` | the first well-formed `X-Mailtide-Id` header (16 to 64 characters of `A-Za-z0-9_-`; name in any case, folding undone; a malformed one is passed over), else absent, as `compose`'s `tracking_id(raw)` reads it: the identity of a message mailtide wrote, which Gmail keeps byte for byte (feature rules) |
| `attached`, `inline` | exact counts of listed parts of each kind |
| `protocol` | exact count of RFC 1847 signature and encryption parts, never listed |
| `parts` | the first 32 listed parts, in document order |

Each listed part: `part` (reference), `filename` (RFC 2231/2047 decoded, at
most 255 bytes), `content_type` (declared, lowercased;
`application/octet-stream` when absent or unreadable), its size (decoded
bytes: exactly what a read returns; carried by the reference), `content_id`
(no angle brackets), and the three
ingredients of the inline rule: `disposition` as declared, `referenced`,
`related`.

The eight existing header facts are untouched and still come from
`mailtide-gmail`'s header parse; `subject`, `to`, `cc` stay as written. Two
pure helpers, `parse_addresses(value)` and `decode_header(value)`, let a host
decode those with the same decoder.

Worst case a record grows by about 5 KB (32 parts with long names); a typical
message by 150 bytes.

**Compatibility.** Two things reach the database that a build from before
this feature handles wrongly: the `facts` key, which it would read past and
then drop from every content record it rewrites, and `FactsRefreshed`
entries in the log, which it cannot decode. So the release that first stores
facts (slice 2) adds the next migration as a version-only step: no statement
but its row in `mailtide_schema`. That raises `SCHEMA_VERSION`, and an older
build sees the database as `ahead` and refuses to sync (`SchemaOutOfDate`)
before it writes a row. Its reads that check no schema (`changes()`) fail
with `CorruptState` on the first `FactsRefreshed` entry of a page; they
never skip it. `ENGINE_VERSION` stays as it is: it is compared only inside
`claim`, after rows have been parsed, and reads never claim. Reverting:

```sql
DELETE FROM mailtide_changes WHERE change LIKE '{"FactsRefreshed"%';
DELETE FROM mailtide_schema WHERE version = <this migration>;
```

The `facts` keys may stay: an older build ignores them. Lost: the facts of
any row the older build then rewrites, and the refresh entries; after the
next upgrade `refresh_facts` finds those rows (no facts, or an older
`FACTS_VERSION`) and restores them from the blobs. Part references held by a
host stay valid: they name bytes in a blob, not a row.

### Listed, attached, inline

Only the top-level message is examined; an attached `message/rfc822` is one
part and its own parts are not listed.

A leaf part is **listed** unless it is a text or HTML body part by RFC 8621's
body lists (as mail-parser computes them, the alternatives it drops
included), or an RFC 1847 protocol part: a child other than the first of
`multipart/signed`, or any child of `multipart/encrypted`. A container a
parse limit left unopened is one listed leaf: it holds content nothing else
reports.

A listed part is **inline** when any of these holds, else **attached**:

1. an HTML body part of the message contains `cid:` followed by its
   Content-ID (percent-decoded, whole id), whatever its disposition;
2. its `Content-Disposition` is `inline`;
3. it has no `Content-Disposition`, is not text, and is a child, other than
   the first, of `multipart/related`.

(Building it showed rule 3 needs "not text": on himalaya's own fixture an
undeclared `notes.txt` or calendar invite inside `multipart/related` would
otherwise be hidden as inline, where himalaya and a reader expect a file.)

An attached message is always attached. `has_attachment` is `attached > 0`.
The rule is one pure function over the stored ingredients, so changing it
needs no blob read.

**It deliberately differs from Gmail's `has:attachment`.** Tested live
(`message-facts-has-attachment-parity`, refuted), the operator matches every
message with any part that is not a text or HTML body: an image a `cid:` link
shows, an image declared inline, an undeclared component of
`multipart/related`, a lone signature part, a `text/calendar` alternative.
Our paperclip leaves out the first three (shown inside the message) and
signatures (not files), which is what a message list wants. The summary keeps
what reproduces Gmail's notion: `attached + inline + protocol > 0`
(`MessageFacts::gmail_has_attachment`; in Python, `attachment_count +
inline_count + protocol_count > 0`).

### Part reference

`offset.length.encoding.size.total`, for example `1042.6920.b.5110.48211`:
where the part's body starts in the blob, its encoded length, its transfer
encoding (`n` none, `q` quoted-printable, `b` base64; upper case when the
body is malformed for its encoding), its decoded size, and the length of the
blob it was computed against. Hosts treat it as opaque.

(Building it showed the size belongs in the reference: a read over
`max_bytes` must be refused before any read, and the encoded length bounds
the decoded size only loosely, so himalaya's 7-byte part under a 7-byte cap
would be refused. With the size there, the refusal is exact, a stream knows
its length up front, and a read whose decoded length differs is caught as a
stale reference.)

It is a fact about immutable bytes, so it stays valid across parser upgrades,
rule changes and refreshes, and a read needs nothing but the blob. A part
inside a *transfer-encoded* attached message has no range in the raw bytes
and so no reference: read the attached message and parse that.

### Public API

Rust, `mailtide_mime` (pure): `facts(raw) -> MessageFacts`,
`parts(raw) -> Vec<PartInfo>`, `PartRef` with its text form, `PartDecoder`
(chunked). `mailtide_io::BlobStore` gains `get_range(key, range)` and
`get_chunks(key, range)`, with default bodies over `get` so a host's own
store keeps working; `ObjectBlobs` implements both with `object_store`'s
ranged `get_opts`, which GCS, S3, Azure, local disk and memory all serve.

`mailtide_io::Runner`:

| Method | Returns | Reads |
|---|---|---|
| `parts(account, message)` | every leaf part: reference, role, type, filename, size, Content-ID, charset, the attached message it is in (its reference, and how many attached messages deep) | the whole blob, parsed in Rust; metadata only crosses to Python |
| `read_part(account, message, part, max_bytes)` | the decoded bytes | one ranged read of the encoded length |
| `open_part(account, message, part, chunk_size)` | `PartStream`: decoded chunks of at most `chunk_size` (default 256 KiB) | a streamed ranged read |
| `refresh_facts(account, after, limit, budget)` | `FactsRefresh { examined, refreshed, unreadable, next }` | see Backfill |

`PartError`: `BlobNotFound`, `BlobUnavailable`, `InvalidPart` (unreadable
reference, range outside the blob, or blob of another length than the
reference names), `TooLarge { size, limit }` (refused from the reference,
before any read).

Python: `MessageContent` gains `facts_version` (0 when not extracted), the ten
envelope fields (`sent_at_ms`, `sent_at_offset_min` for the date),
`tracking_id`, `attachments: list[AttachmentInfo] | None`,
`attachment_count`, `inline_count`, `protocol_count` and `has_attachment`,
the last five `None` when not extracted,
so "unknown" is never shown as "none". `AttachmentInfo` has `part`,
`filename`, `content_type`, `size`, `inline`, `disposition`, `content_id`.
`Runner` and `BlockingRunner` get the four methods; `open_part` returns an
async iterator (a plain iterator on `BlockingRunner`), also a context manager,
pulled one chunk at a time; cancelling or leaving the loop drops the Rust
stream. Exceptions `PartNotFound`, `PartTooLarge`; `BlobNotFound` and
`BlobUnavailable` exist. `parse_message` is unchanged except that each
attachment also carries its `part`.

### Gmail calls and quota

None. The facts come from the `format=raw` fetch an import already pays for
(60 units); part reads and the refresh touch only the blob store and the
database. The pass-through alternative costs 60 units for the part list plus
20 per attachment read.

### Meeting the sync engine

- **Import.** The driver computes the facts from the bytes it has just
  written to the blob store and puts them in `ImportedMessage`; the engine
  copies them into `MessageOp::MarkImported`; `mirror::mark_imported` stores
  them. The core parses nothing and stays deterministic: facts are event
  input. Parsing runs off the async workers. mail-parser does not fail on bad
  input; its depth and part limits are set, and if the extractor panics the
  message is imported with `facts = None` and a warning. An import never
  fails or blocks because of a message's shape.
- **Feed.** `MessageImported.content` is read from the row when the feed is
  read, so it carries the facts, references included, with no change to the
  log. A refresh records a new entry, `FactsRefreshed { id, thread_id }`,
  with `content` attached the same way.
- **Backfill.** `refresh_facts` is a host-scheduled maintenance call, like
  `drift` and `compact`. It pages `message_ids` from `after`, reads the
  views, and for each imported row whose facts are missing or older than
  `FACTS_VERSION`, or whose blob is not the length the row holds (an empty
  ranged read answers the length; where the store serves ranges that is a
  metadata call, not a read), reads the blob (8 at a time), extracts, and
  hands batches of 50 to `Store::refresh_facts`. That store call is **not fenced by an
  epoch**, like `admit`: it must neither supersede a running sync nor be
  rejected by one. It is conditional per row instead: applied only while the
  row exists, is imported, has the same blob and the size the refresher read,
  and holds older facts (or its blob's length differs from that size); then
  the row's size is set to the blob's length. The simulator checks each
  refresh with `invariants::check_refresh` (nothing but facts and sizes
  change). One batch is one transaction and
  one feed state. The call stops between batches when `limit` rows were
  examined or `budget` ran out and returns `next`; `None` means the pass is
  complete. It is idempotent: a second pass reads no blob. Cost: one blob
  read per stale message and no quota; a 100,000-message account is about
  7 GB of blob reads once.
- **Overlay, intents, jobs, cursor:** untouched.
- **Older builds.** A build without this change would drop the facts of
  every row it rewrites, and cannot read `FactsRefreshed`. The schema version
  refuses it at startup (Compatibility, above); `ENGINE_VERSION` is not
  raised.
- **Simulator.** Imports carry facts; a refresh step is interleaved with
  commits, deletes, crashes and supersessions, and the existing invariants
  must hold.

### Failure and retry

| Case | Result |
|---|---|
| Blob missing on a part read | `BlobNotFound`; the mirror is not touched |
| Blob store error, also mid-stream | `BlobUnavailable`; a stream ends with an error item, never short |
| Reference unreadable, out of range, or for another blob length | `InvalidPart` (Python `PartNotFound`) |
| Part larger than `max_bytes` | `TooLarge` before any read; use `open_part` |
| Malformed transfer encoding | marked at import; such a part is decoded whole, never in chunks, so its bytes and `size` always agree |
| Refresh: blob missing | named in `unreadable`, row untouched, the pass goes on |
| Refresh: message deleted or re-imported meanwhile | the update is dropped |
| Refresh: store unavailable | the batch rolls back; call again from the same `after` |

## 4. Alternatives rejected

- **Gmail's `format=full` and `attachments.get`** (Zero, himalaya's Gmail
  commands): 20 units per read, whole bodies in JSON, an attachment id not
  documented as stable, and a cache to build; the blob has the bytes.
- **Part index or parser part id as the address:** changes when the rule or
  the parser's reading of a damaged message changes.
- **IMAP section numbers:** same dependence on the parser, plus a parse on
  every read.
- **Content-ID as the address:** most attachments have none and it need not
  be unique; it is stored, to resolve `cid:` links.
- **New columns or a parts table:** a migration for data that fits the record
  a row already has and is always read with it.
- **Extending the hand-written header parse** to dates, encoded words and
  MIME structure: a second mail parser.
- **Replacing the eight existing facts with parsed ones:** changes values
  hosts already store.
- **Lazy extraction on read:** blob I/O inside `messages()`.
- **Backfill as engine jobs:** one job row per message, a new command and
  event, for work that needs no Gmail call and no fencing.
- **Refusing a too-complex message** (himalaya): an import must not fail.
- **Sniffing media types** (pimalaya core): a judgement, not a fact.

## 5. Tests

Suites were found by searching every checkout for attachment, inline,
Content-ID, disposition and the header names, then read.

- **Zero:** `gmail-inline-asset` (3 mapped onto part reads, 1 `na`),
  `imap/categorize` (the marker headers and the sender address; the
  classification is `na`), `imap/imap-store` (its header test; folder mapping
  and header threading `na`). Not declared: `mail-asset-cache` (owned by
  `scripts/zero_tests.py`), the forward and outgoing-MIME suites (`send`,
  `compose-mime`), `mail-contact-store` (a host index; it does show Bcc must
  not leak into anything shared).
- **himalaya:** `part.rs` (14 of its 15 has-attachment cases mapped, the
  tally tests `na`, two reply-quoting tests left to `send`), `read.rs` (13
  header and part cases mapped, 19 rendering and body-text cases `na`),
  `envelope.rs` (the four Date cases), `attachment/list.rs`,
  `attachment/download.rs` (three mapped; writing files is `rest_na`),
  `tests/message_parse.rs`, `header.rs` (`rest_na`), and the two
  `gmail/backend.rs` tests the `search` plan left here. Two mapped tests
  assert the opposite outcome on purpose: a too-complex or blank message is
  imported, not refused. Not declared: `tests/pimdir_attachments.rs` (an
  account-wide attachment index) and `gmail/messages/get.rs` (the
  `format=full` path we do not take).
- **mail-parser:** three files, with `rest_na` for parsing internals we get
  from the crate. Ported: the RFC 8621 example, inline images in
  `multipart/related`, raw ranges, offsets, attached messages (plain and
  transfer-encoded), limits, decode problems, truncated input. Its other test
  files are internals only and are not declared. Its fixtures may be copied
  with the licence notice.
- **jmap-client, Stalwart, pimalaya core:** no test covers this.

Our own 61 tests add: the envelope rules and caps; the inline rule's rows
that no reference tests (referenced but disposed as attachment, declared
inline but unreferenced, `cid:` matching); the reference's text form; a
property test that every range in mail-parser's corpus decodes to what the
parser decodes; chunked decoding equal to whole decoding at every split;
old content reading back; the refresh rule and its conformance cases; ranged
reads that touch only the range; streaming, cancellation and mid-stream
failure; a blob store without ranges; the simulator campaign; the Python API.

## 6. Slices

1. **Extraction.** `mailtide-mime`: `facts`, `parts`, `PartRef`, the rule,
   with the ported tests. Nothing calls it yet.
2. **Store at import.** Core types, `StoredContent.facts`, engine
   pass-through, the version-only migration step, the driver's import,
   Python fields. The feed carries the facts with no further work.
3. **Read a part.** `get_range`, `Runner::parts`, `read_part`, errors,
   Python.
4. **Stream.** `PartDecoder`, `get_chunks`, `open_part`, `PartStream`.
5. **Backfill.** The refresh rule, `Store::refresh_facts`, `FactsRefreshed`,
   conformance, simulator, `Runner::refresh_facts`, Python.
6. **Docs.** SPEC §4, §5.5, §10.3, §11; ARCHITECTURE.

## 7. Decisions

1. **Facts live in the content record; no new column or table.** One
   optional key that older rows lack, always read with the record it sits
   in; older builds are kept away by the schema version (Decision 8). *Would
   change it:* a host needing to query the facts in SQL inside mailtide's
   tables; today hosts copy them from the feed.
2. **The driver parses at import with `mailtide-mime`; the core does not.**
   SPEC §5.5 called the crate optional; the driver now needs it. *Would
   change it:* a host that must build the driver without a MIME parser.
3. **A part is addressed by its byte range.** Stable for ever, readable
   without a parse, and what Stalwart does. Cost: no address for parts inside
   a transfer-encoded attached message. *Would change it:* blobs that are
   re-encoded or compressed by mailtide, which would end byte offsets.
4. **Inline is "shown by the HTML, declared inline, or an undeclared,
   non-text component of `multipart/related`".** It agrees with himalaya's tests,
   adds RFC 8621's optional `cid:` exception, and leaves signature parts out.
   Gmail's `has:attachment` is broader (it came back refuted); we keep this
   rule for the paperclip and store the protocol count so a host can
   reproduce Gmail's. *Would change it:* a host that wants Gmail's notion as
   the paperclip; the ingredients are stored so the rule can move.
5. **The existing eight facts keep their values; only the sender gets a
   decoded name.** Hosts already store them. Helpers decode the rest.
   *Would change it:* a maintainer preferring a decoded `subject` fact, which
   is one more field and a version bump.
6. **Backfill is a host call, not engine work.** No Gmail call, so nothing to
   gate or fence, and the host decides when to pay for the blob reads.
   *Would change it:* hosts that cannot schedule a second call; then `run`
   calls it when idle.
7. **A refresh records `FactsRefreshed`.** A consumer that already processed
   an import would otherwise never see its facts. *Would change it:* nothing
   short of consumers that rebuild from scratch anyway.
8. **A version-only migration step, not `ENGINE_VERSION`.** Losing facts
   silently to an old build during a rolling deploy is worse than old builds
   stopping, and the schema version is the one mechanism that stops them
   before they parse or write anything: the engine version is compared only
   in `claim`, and `changes()` never claims. No case was found that the
   schema version does not cover. *Would change it:* a need to roll back
   without an operator step.
9. **Bounds:** 32 listed parts, 255-byte filenames, 4 unsubscribe URIs of
   2,048 bytes, with exact counts beyond the list; `parts()` gives the rest.
   *Would change it:* measured mail that hits them often.
10. **Markers stored:** List-Id, List-Unsubscribe, Auto-Submitted and
    Precedence, as the roadmap asks; not Feedback-ID or vendor headers.
    *Would change it:* a host classifier that needs one; each is a field and
    a version bump.
11. **Bcc is stored.** It is the user's own data in their own mailbox. Hosts
    must not show it to anyone else.

Gmail behaviour assumed, not proven (`scripts/features.py unknowns`):

- `message-facts-raw-bytes-stable`: the raw bytes of a message are the same
  on every fetch. If not, a reference notices a blob of another length and
  the refresh re-extracts; the fix is a create-only blob write.
- `send-bcc-stripped` (owned by `send`; one Gmail fact, one unknown): its
  steps 3, 5 and 6 show whether the sender's copy and a stored draft keep
  the Bcc header in `format=raw`. If not, `bcc` is empty for sent mail and
  nothing else changes.
- `message-facts-has-attachment-parity`: refuted. Gmail's `has:attachment`
  matches any non-body part; our paperclip stays narrower on purpose, and
  `protocol` was added so `attached + inline + protocol > 0` reproduces
  Gmail's (Listed, attached, inline).
