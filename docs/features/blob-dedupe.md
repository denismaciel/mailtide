# Blob dedupe across accounts: decided against

Plan: `features/blob-dedupe.toml` (roadmap A3). Status: done.
The plan is a decision, two facts to test, and six tests of what is kept.

## 1. Goal and scope

The roadmap item: when one message sits in two mirrored mailboxes (a sender
and a recipient in the same organisation, or two recipients), mailtide stores
its raw content twice. Could it store it once?

**Recommendation: do not build this.** Keep one blob per (account, message)
at `{account}/{message}.eml`. The reasons, in order of weight:

1. The two copies are, as far as the standards and the references show, not
   the same bytes, so a store keyed by the hash of a message would share
   close to nothing.
2. What is the same (the body and attachments below the headers) can only be
   shared by storing a message in pieces, which gives up "raw content, once,
   at a deterministic key" (SPEC Q4).
3. Sharing blobs between accounts removes three properties the engine has
   today: deletion without coordination, `forget` by prefix, and a write path
   that cannot lose content to a concurrent cleanup.
4. The saving is storage of duplicated internal mail at object-store prices.

In scope: the evidence, what content addressing would require, the
measurement that would reopen the question, and tests that pin per-account
isolation. Out of scope: any change to keys, the store or the engine.

## 2. What the references do

**Zero** (MIT) does not dedupe across mailboxes. Thread bodies live at
`<mailbox agent>/<threadId>.json` (`getThreadKey` in
`routes/agent/index.ts`), deleted by that mailbox's own durable cleanup job.
`MailAssetCache` stores attachment bytes under a SHA-256 name, but the hash
is of `[mailbox, kind, message id, attachment id]`, not of the content: the
same attachment in two mailboxes is two objects. Its tests
(`mail-asset-cache.test.ts`, `mail-cleanup-store.test.ts`) belong to
`scripts/zero_tests.py` and are ported already. The one test that
deduplicates anything, "dedupes the same Message-ID across folders and keeps
the key when moved" in `imap/imap-store.test.ts`, is about one IMAP account
whose folders hold copies; `message-facts` declares that suite and marks the
test not applicable (a Gmail message has one id and a label set). It
deduplicates an index row by `Message-ID`, not stored bytes.

**Stalwart server** (AGPL: names only) does content-address:
`BlobHash::generate` (`crates/types/src/blob_hash.rs`) keys a blob by a hash
of its bytes. A delivering server is in a position to: it holds one set of
bytes for all local recipients of a message (RFC 9228 notes such systems).
The price is visible in
`crates/store/src/write/blob.rs` and `crates/common/src/storage/blob.rs`: a
link record per holder (`BlobLinkValue`), a reservation taken before the
upload (`hold_blob`) so a concurrent purge cannot delete a blob that is
about to be linked, an existence check (`blob_exists`), and a purge pass
(`purge_blobs`) that removes blobs nothing links to. *Take:* the list of what
is needed. *Leave:* all of it, unless the measurement below says otherwise.

**JMAP / jmap-client** (MIT/Apache). A blob id belongs to an account; using
content in another account is an explicit `Blob/copy` (`CopyBlobRequest`,
`from_account_id`). The API mailtide follows keeps blobs per account even
where a server shares storage underneath.

**mail-parser** (MIT/Apache). `resources/eml/thirdparty/011.eml` is a stored
message as a receiving system keeps it: `Return-Path`, `Delivered-To` and
`Received` come first and name the recipient and the hop. It is a sample of
the header block that differs per mailbox, not a Gmail capture.

**Himalaya, pimalaya-core.** Nothing relevant.

## 3. Design in mailtide

### How blobs work today

| Step | Where | Key or rule |
|---|---|---|
| Write | `Runner::import`, before the engine is told | `blob_key(account, message)` = `{account}/{message}.eml`; overwriting identical bytes is fine, so a retried import is idempotent |
| Reference | `StoredContent.blob` on the message row; `MessageImported.blob` in the feed | the same string |
| Read | `read_blob(account, id)` | recomputes the key |
| Delete one | `BlobCleanup(message)` job, queued by the tombstone, due after `BLOB_GRACE` (24 h) | deletes that key; a missing object is success |
| Delete all | `forget(account)`: store rows, then `delete_account(account)` | lists and deletes the `{account}/` prefix |

Every step involves one account. No step reads another account's rows, and
no transaction spans the database and the object store: each order of crash
leaves either an orphan the cleanup job deletes or a retry that rewrites the
same key.

### What is the same in two copies

- **The RFC 5322 `Message-ID`**, which mailtide already stores per message
  (`rfc822_message_id`). A host can find the logical duplicates with it.
- **Probably the body**: everything after the header block, attachments
  included (unknown `blob-dedupe-body-identical`).
- **Not the header block.** A copy that was delivered carries trace headers
  the sent copy never had: RFC 5321 §4.4 obliges every receiving server to
  prepend `Received`, and final delivery to add `Return-Path`; RFC 9228
  describes `Delivered-To`, added at delivery for exactly one recipient. Two
  recipients' copies therefore name different addresses, and the sender's
  copy has none of this. Google documents `format=raw` as "the full email
  message data" of that mailbox's message and says nothing about copies
  matching across mailboxes, so this is recorded as unknown
  `blob-dedupe-raw-differs-per-mailbox`, with the calls that settle it.

One differing byte changes the hash. Whole-message content addressing would
share a blob only for copies with identical headers, which the above says do
not occur for mail that was delivered.

### What sharing would cost

For a key such as `sha256/<digest>.eml`, used by every account:

- **Reference counting in the store.** A row per (digest, account, message),
  written with the import and removed by cleanup, in the one database all
  accounts share.
- **A cross-account race.** Account A's cleanup sees no reference left and
  deletes the object while account B's import has uploaded the same bytes
  and not yet committed its reference: B's row now points at nothing. That
  is silent loss (goal 2). Closing it takes what Stalwart has: reserve the
  digest in the database before the upload, link on commit, and delete only
  from a purge pass that honours reservations. The reservation is an extra
  unfenced store call on every import, outside the engine's commit.
- **`forget` stops being a prefix delete.** It must walk the account's
  references before deleting its rows, release each, and delete the blobs
  left without holders. "Is this account's content gone?" can no longer be
  answered by listing a prefix, and per-account storage policies (a bucket
  prefix per tenant, its own encryption key or retention rule) no longer
  apply.
- **The grace period** becomes "24 h after the last holder's tombstone".
- **Hashing** every message at import, a migration for the reference table,
  and a new blob layout that existing hosts would have to be migrated to or
  run beside the old one.

Deduplicating below the message (hashing the body, or each attachment) would
share real bytes if the second unknown holds, but then a blob is no longer a
raw message: `read_blob` reassembles, `parse_message` and every host that
reads `.eml` objects directly change, and all of the above still applies.

### What it would save

Only mail present in two or more mirrored mailboxes, minus its headers. For
a deployment with `D` GB of such duplicated bytes, the saving is `D` times
the object-store price: at an assumed list price of two US cents per
GB-month, 100 GB of duplicates is two dollars a month. A host can measure
`D` today without any change to mailtide: join `messages()` across accounts
on `rfc822_message_id` and sum `size` over all but one copy of each.

### The decision in the repository

- SPEC §3 and §15 say that blobs are keyed per account and message on
  purpose, and under what measurement the question reopens.
- Six tests pin the properties sharing would remove (§5). They also close a
  gap: no existing test was found that runs `delete_account` against an
  account whose name is a string prefix of another's.

Public API, schema, Gmail calls, quota, feed, engine: unchanged.

### If it is ever built

Recorded so a later plan does not start from zero: content-address whole
messages only if the first unknown is refuted; key `sha256/<digest>.eml`
beside the existing layout; a reference table and a reservation taken by the
driver before the upload; cleanup and `forget` release references; one purge
job per store, not per account, deletes unreferenced blobs older than the
grace period; `StoredContent.blob` already carries the key, so readers that
use it instead of recomputing the key need no change.

## 4. Alternatives rejected

- **Content-address whole messages now**: shares nothing if copies differ,
  and pays the full cost above.
- **Content-address the body or attachments**: changes what a blob is; a
  different feature (attachment storage), to be judged with `message-facts`.
- **Canonicalise before hashing** (strip trace headers): the stored bytes
  would no longer be what Gmail returned, breaking fidelity and DKIM checks.
- **Dedupe by `Message-ID`** (store one copy, point the others at it): the
  header is sender-chosen and not unique; two different messages with one id
  would silently read as each other.
- **Record a content hash on each message** so hosts can measure: `size` and
  `rfc822_message_id` already allow the estimate; add the hash when a host
  needs exact numbers.
- **Leave it to the object store**: the common backends do not deduplicate.

## 5. Tests

Reference suites: none declared. Zero's blob tests are owned by
`scripts/zero_tests.py`; its IMAP dedupe test is accounted for by
`message-facts` (not applicable); Stalwart's blob code has no tests in the
pinned tree; jmap-client has none for blobs.

Own tests (6), all of behaviour that exists and must stay:

- `blobs.rs` (3): the key names account and message; identical bytes under
  two accounts are two objects and deleting one leaves the other, on the
  memory and local-disk backends; `delete_account("a@x.co")` spares
  `a@x.com` and `a@x.co.uk`.
- `end_to_end.rs` (1): two accounts import the same bytes; a deletion in one,
  past the 24 h grace, and then its `forget`, leave the other's blob and
  views untouched.
- Store conformance (1), `accounts_are_kept_apart`: two tracked accounts
  hold the same message id; a tombstone queues cleanup in one only, and
  `forget` of it leaves the other's row, content reference, jobs and feed.
  The other account's address extends the first one's
  (`conformance@example.com.au`), so a store matching rows by prefix fails.
  The suite's only other two-account case (`thread views never mix
  accounts`) is about reads. The new case sits in its own list,
  `isolation_cases()`.
- Python (1): `forget` of one account, then `read_blob` of the other, whose
  address again extends the first one's.

No simulator work: engine behaviour does not change, and the simulator runs
one account.

## 6. Slices

1. The six tests.
2. The SPEC paragraphs.
3. Run the two unknowns against real mailboxes (`scripts/features.py
   unknowns blob-dedupe`) and record the results; if the first is refuted,
   open a new plan from "If it is ever built".

## 7. Decisions

- **D1. Not built.** Evidence and costs above. *Would change it:* the first
  unknown refuted (copies are byte-identical) **and** a host measuring
  duplicate bytes worth more than the machinery, say above a tenth of its
  stored bytes.
- **D2. No content hash is added to stored content.** Nothing would read it,
  and the estimate needs no code. *Would change it:* a host wanting exact
  duplicate counts, or attachment-level storage being planned.
- **D3. Body-level dedupe is not pursued here.** It is a storage format
  change. *Would change it:* an attachment store being designed anyway; the
  second unknown tells that plan whether sharing is real.
- **D4. The plan still ships tests.** They cost little and make the
  isolation the decision relies on a checked property rather than a habit.
  *Would change it:* nothing.
- **D5. The price in §3 is an assumption, not a quotation.** The argument
  does not depend on its exact value; a host substitutes its own.

Unknowns:

- `blob-dedupe-raw-differs-per-mailbox`: assumed that the sender's and each
  recipient's raw bytes differ. Wrong is safe (storage only) and reopens D1.
- `blob-dedupe-body-identical`: assumed that the bytes below the headers
  match across copies. Wrong closes D3 for good; right leaves it as stated.
