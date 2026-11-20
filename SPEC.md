# mailtide — specification

Status: **v0.2, implemented** (2026-11-18). v0.1 was the pre-code draft; this
revision records what was built and the decisions made on the way (§15).

A sync engine that keeps a faithful, queryable copy of Gmail mailboxes in the
host's own database, and later carries the user's actions back to Gmail. The
core is written in Rust with no I/O of its own (sans-IO) and ships Python
bindings built with maturin, for hosts that want a mailbox mirror in their own
Postgres or SQLite database.

Sources this spec draws on:

- **[Z]** Zero (Mail-0/Zero, MIT), `apps/server/MAILBOX_SYNC.md`, read
  2026-11-18. Most of the sync rules below are restated from it. Where we copy
  code later, the MIT notice comes with it.
- **[G]** Google's Gmail API docs: the
  [sync guide](https://developers.google.com/workspace/gmail/api/guides/sync),
  [history.list](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history/list),
  [push](https://developers.google.com/workspace/gmail/api/guides/push),
  [quota](https://developers.google.com/workspace/gmail/api/reference/quota)
  and [batch](https://developers.google.com/workspace/gmail/api/guides/batch).
- **[J]** JMAP: RFC 8620 (core: state strings, `/get`, `/changes`, `/set`) and
  RFC 8621 (mail: Mailbox, Thread, Email). We borrow its vocabulary and the
  shape of its change API; we do not implement the protocol.

GPL/AGPL projects (lieer, Nylas sync-engine, Mailspring-Sync, Inbox Zero) may
inform the design. No code is copied from them.

---

## 1. Goals and non-goals

### Goals

1. **Fidelity.** Every message in an enrolled mailbox (all labels, including
   spam, trash, sent, drafts and archive) exists in the mirror with Gmail's
   current label set. Once the system is quiet, mirror = Gmail.
2. **No silent loss.** Every message the engine learns about ends in exactly
   one recorded state: imported, deleted (tombstoned) or failed (retryable or
   blocked). Nothing disappears without a record.
3. **Gmail is the source of truth.** The mirror stores Gmail's state plus, from
   phase 6 on, pending local intents. It never invents state of its own.
4. **Crash-safe and resumable.** A crash between any two steps loses nothing
   and duplicates nothing that matters.
5. **Provably tested.** Correctness is defined by written invariants (§9). A
   deterministic Gmail simulator (§12) checks them under fault injection.
6. **Ready for two-way sync.** The data model and command set leave room for
   actions (§7) without migrations that break the model.

### Non-goals (v1)

- Other providers (Outlook/Graph, generic IMAP). The external API is
  provider-neutral (JMAP-shaped), so they can be added behind it later.
- Full-text search over the mirror. An index of the mirror's content belongs
  to the host. What mailtide offers is a pass-through to Gmail's own search
  (`search`, §11): the host's query string goes to Gmail untouched, Gmail
  decides what matches, and the hits come back as ids marked with what the
  mirror holds. mailtide parses no query and keeps no results.
- Message parsing inside the sync core. Parsing is a separate crate the
  driver uses at import (§5.5).
- Sending mail and draft authoring. These come later; the design leaves
  room for them. (Label definitions are written: §6.6.)
- A JMAP server.

## 2. Glossary

| Term | Meaning |
|---|---|
| Account | One Gmail mailbox (a user address) being mirrored |
| Label | A Gmail label id: system (`INBOX`, `UNREAD`, `STARRED`, `SPAM`, `TRASH`, `SENT`, `DRAFT`, `IMPORTANT`, `CATEGORY_*`) or user (`Label_…`). The JMAP "Mailbox" |
| Message | A Gmail message (immutable content plus a mutable label set). The JMAP "Email" |
| Thread | A Gmail `threadId` grouping. The authoritative threading |
| historyId | Gmail's monotonically increasing mailbox revision |
| Cursor | The last historyId whose changes are fully committed to the mirror |
| Checkpoint | The historyId at which a message's stored state was observed |
| Tombstone | A record that a message was deleted as of a checkpoint |
| Intent | A pending local change to one (message, label) pair (phase 6) |
| Overlay | The effective state: observed state with pending intents applied |
| Host | The application embedding the engine. It does all I/O |
| Store | The host's persistence, implementing the store contract (§10.3) |
| State string | An opaque per-account token the mirror exposes for "changes since" [J] |

## 3. Architecture

```
            Events (facts)                    Commands (requests)
 Host ───────────────────────────▶ Engine ───────────────────────────▶ Host
 (Gmail client, Postgres,          (pure Rust state machine,          (performs I/O,
  blob store, scheduler, clock)      deterministic, no I/O)             reports results as events)
```

- **Sans-IO core.** The engine never makes network calls or reads the clock.
  It only consumes events and returns commands (§10). It is deterministic: the
  same events in the same order produce the same commands. That makes it fit
  inside Temporal workflows, replayable, and simulatable.
- **Commits are conditional operations** (Q3, revised). The engine emits
  `Commit`s: guarded by epoch, engine version, expected cursor and job claim,
  each carrying message, label, account, job and intent operations. A store
  evaluates each operation's condition (checkpoints, tombstones) and applies
  the whole commit atomically or rejects it.
- **mailtide ships the stores** (Q3, revised). The rules that evaluate
  commits are pure functions in the core (`mirror`), shared by every store:
  in-memory (`AccountMirror`, also used by the simulator), and one SQL store
  for Postgres and SQLite (`sqlx`), which loads only the rows a commit
  touches, applies the same rules, and writes the difference. Hosts may bring
  their own store if it passes the conformance suite (§10.3).
- **Raw content goes to blob storage, once** (Q4). Content is immutable [G];
  the driver fetches it once (`format=raw`), writes it to blob storage at a
  deterministic key (`{account}/{message}.eml`) **before** reporting the
  import, and the mirror keeps the reference. Any `object_store` backend
  works (GCS, S3, local disk, memory). Keys are per account on purpose:
  the same message in two mirrored mailboxes is two objects (delivered
  copies differ in their trace headers, so content addressing would share
  little), and deleting one account's content, a message after its grace
  period or the whole `{account}/` prefix in `forget`, never involves
  another account (`docs/features/blob-dedupe.md`).
- **Quota** (Q5). Every Gmail command carries its declared cost and priority
  (§8). The driver admits each one through a quota gate over the core's pure
  `Ledger`.
- **Layers.** `mailtide` (pure core) → `mailtide-gmail` (HTTP shapes, decoding,
  error classes, push verification; still no I/O) → `mailtide-store` →
  `mailtide-io` (async driver: `Runner`) → `mailtide-py` (bindings). See
  ARCHITECTURE.md.

## 4. Data model (JMAP-shaped)

The store keeps, per account:

- **account**: `account_id`, `email`, `cursor` (nullable), `watch_expiration`,
  `sync_phase` (`bootstrapping | reconciling | live | blocked`),
  `notification_revision`, `processed_revision`, poll timestamps, retry and
  backoff state, and the reconciliation progress record (§6.3).
- **label**: `label_id`, `name`, `type` (system/user), list and message
  visibility, `color` (a text and background pair from Gmail's palette);
  the last three are unknown when Gmail omits them (most system labels) and
  until a listing fills them (migration 8). Synced from `labels.list`, and
  written by the host's label calls (§6.6). Small and refreshed
  periodically. A label's parent is derived from its name (`A/B` is under
  `A`), never stored. The account row also keeps the **label generation**,
  bumped by every local label write, and the ids of labels known to be
  deleted with when (§6.6).
- **message**: `message_id`, `thread_id`, `label_ids` (a set, stored as Gmail
  ids), `checkpoint`, `internal_date`, `size_estimate`, `blob_ref` (nullable
  until imported), `import_state` (`pending | imported | failed | blocked`),
  and header facts the engine needs (`rfc822_message_id`, `in_reply_to`,
  `references`, `from`, `to`, `cc`, `subject`, `snippet`), filled on import.
  - **Message facts** (`content.facts`, `None` in rows imported before them
    and when the extractor gave up): extracted by the driver at import from
    the bytes it stored, with `mailtide-mime`, at a `FACTS_VERSION`. Envelope:
    `bcc`, `reply_to` (as written), `sent_at` (the Date header as an instant
    and the sender's UTC offset), `sender_name`/`sender_email` (first From
    mailbox, name decoded), `list_id`, `list_unsubscribe` (at most 4 URIs of
    at most 2,048 bytes) with `one_click_unsubscribe` (RFC 8058),
    `auto_submitted`, `precedence`, and `tracking_id` (the first
    well-formed `X-Mailtide-Id` token, as `tracking_id(raw)` reads it: the
    identity of a message mailtide wrote, §5.5). Attachment summary: every
    leaf of the top-level message that is neither a text/HTML body part nor
    an RFC 1847 signature or encryption part is *listed*; a listed part is
    *inline* when an HTML body shows it through `cid:`, when it is declared
    inline, or when it is an undeclared non-text component of
    `multipart/related`, else *attached* (an attached message always is).
    Stored: exact `attached`, `inline` and `protocol` counts and the first 32
    listed parts (part reference, filename up to 255 bytes, declared type,
    Content-ID, disposition and the rule's ingredients). `has_attachment`
    (the paperclip) is `attached > 0`. It deliberately differs from Gmail's
    `has:attachment` operator, which matches any non-body part (inline
    images and signatures included): `attached + inline + protocol > 0`
    reproduces that. Each listed part carries a **part reference**,
    `offset.length.encoding.size.total`: the byte range of its body in the
    stored blob, its transfer encoding (upper case when malformed for it),
    its decoded size, and the blob's length. Reading a part (§11) is one
    ranged blob read and a decode; a reference refuses a blob of another
    length. A part inside a transfer-encoded attached message has none.
  - Read, starred, archived, spam, trash, draft and important are **never
    separate columns**. They are views of `label_ids`. JMAP keywords map as
    `$seen = ¬UNREAD`, `$flagged = STARRED` and `$draft = DRAFT`; mailbox
    membership is `label_ids`.
- **tombstone**: `message_id`, `thread_id`, `deleted_at_checkpoint`.
- **thread** (derived at read time, never stored): Gmail's `threadId` groups
  messages; the empty id is never a thread. A thread's labels are the union of
  its non-draft messages' effective labels (trash and spam members included;
  a draft contributes nothing, although Gmail's `threads.list` also lists a
  thread under a label only its draft carries), and it is unread if any non-draft message is
  [Z]. Its messages are ordered by (`internal_date`, id), oldest first, with
  messages not yet imported (no date yet) after them, by id; its latest
  message is the newest *imported* non-draft one. Participants are `From`,
  `To` and `Cc` of its imported non-draft messages as the header facts hold
  them, one per address (the text inside the last `<…>`, else the whole
  value, case-insensitively). One pure rule (`mailtide::thread::view`) folds
  them; thread actions reach exactly the non-draft messages it counts. An
  index on `(account, thread_id, id)` (migration 6) serves both.
- **job**: durable work items (body import, verification, blob cleanup) with
  dedupe key, priority class, attempts, `next_attempt_at` and last error.
- **intent** (phase 6): `(message_id, label_id)`, `op` (`add | remove`),
  `version`, `state` (`pending | delivered | blocked`), attempts and error.
- **change log** (for consumers): an append-only list of
  `(state_seq, object_type, object_id, kind)`, so host features can ask "what
  changed since state S" [J]. Entries are numbered within their commit, and
  a state string names how far a consumer read: `mt2-<gen>.<seq>` is commit
  `seq` and everything before it, `mt2-<gen>.<seq>.<idx>` everything through
  entry `idx` of commit `seq` (fields hex). Pages end on commit boundaries,
  so `next` is the two-field form, which means what it always did; only a
  commit larger than the page's limit is split, and `next` inside it is the
  three-field form. An import
  entry is read together with the envelope its message row holds (header
  facts, message facts and blob) instead of keeping a copy in the log: a
  message's content never changes in Gmail, so the row matches the entry,
  and older entries carry it too. A facts refresh (§10.3) logs a
  `FactsRefreshed` entry read the same way, and an earlier import entry then
  shows the refreshed facts as well.
- **consumer cursor**: `(account, name, position, seen_at)`, at most 16 per
  account, names of 1 to 64 `[A-Za-z0-9._-]`. A consumer `ack`s the
  position it has durably processed (either form of state string; the
  cursor is *set*, also backwards); `seen_at` is its last ack, or the last
  compaction that found it at the head. Compaction removes whole commits up
  to a target: a cursor is **live** when it is not behind the floor (nor
  inside the floor commit) and `seen_at` is within `max_idle` (7 days by
  default) of the compaction's time, and each live cursor holds everything
  after the last commit wholly behind it (`FeedCursor::compactable`). An
  explicit bound is capped by the slowest live cursor; with no bound the
  target is that cursor's hold, the head when every registered cursor is
  idle or lost, and nothing when none is registered. Cursors are never
  removed by compaction: one left behind the floor is **lost** (its reads
  are `TooOld`) until it acks a valid position again or is dropped.
  Commits neither read nor write cursors, and `forget` deletes them.

## 5. Gmail facts the design relies on

Facts are from **[G]** unless marked otherwise.

1. A message's content never changes. Its label set does.
2. `history.list(startHistoryId)` returns ordered records:
   `messagesAdded`, `messagesDeleted`, `labelsAdded` and `labelsRemoved`.
   `maxResults` defaults to 100 (max 500). Follow every `nextPageToken`.
   Persist the returned `historyId` only after the last page.
3. History is "typically available for at least one week", may be
   "significantly shorter", and is "occasionally unavailable". A
   `startHistoryId` that is too old returns **HTTP 404**, which means full
   sync.
4. Empty history pages can still carry a `nextPageToken` (observed in lieer;
   treat as normal).
5. Push (`users.watch` → Pub/Sub) carries only `{emailAddress, historyId}`.
   - At most **1 notification/sec per user**; excess is dropped.
   - Notifications can be duplicated or reordered.
   - Watches expire within 7 days; renew daily.
   - For a full mirror, watch **all labels** (no label filter) [Z].
6. `messages.list` / `threads.list` pagination is **not a consistent
   snapshot** [Z]. A missing id does not prove deletion.
7. Moving a message to trash or spam implicitly removes `INBOX`. Restoring
   must re-add it [Z].
8. Quota: 6,000 units/min per user, 1,200,000 units/min per project, and an
   80M units/day per-project billing threshold. Per-call costs are in §8. Batch
   requests take up to 100 calls (≤ 50 recommended), and each call is charged
   separately.
9. `messages.batchModify`: up to 1,000 ids; label add/remove arrays up to 100
   ids each [Z].

### 5.5 Parsing and composition (separate crate)

`mailtide-mime` wraps Stalwart `mail-parser` (Apache-2.0/MIT, with depth and
part limits) to extract headers, text, HTML and attachments from stored raw
content. The driver uses it at import: it extracts the message facts (§4)
from the bytes it has just stored, on the blocking pool, and reports them
with the import (they are event input; the core parses nothing and stays
deterministic). An extractor that gives up leaves the message imported
without facts and logs a warning: an import never fails because of a
message's shape. The engine's own header facts come from a minimal header
parse in `mailtide-gmail`, as written. `mailtide-mime` also lists every leaf
part with its reference (`parts`), decodes one part in chunks with exactly
the parser's output (`PartDecoder`), and decodes encoded words
(`decode_header`); the facts take the tracking id from `tracking_id(raw)`.

It also composes: `compose(message, options)` writes RFC 5322 bytes (what
Gmail takes as `raw`) from a flat, JMAP-shaped `OutgoingMessage` through
Stalwart `mail-builder` (Apache-2.0/MIT). It is pure and deterministic: no
clock, host name or random source is read; the date, the ids and therefore
every multipart boundary come from the input, so the same input gives the
same bytes. Input is refused, never repaired (`ComposeError { field, index,
reason }`, which never holds the offending value): control characters in any
header value, file name or id (header injection), addresses that are not ASCII
`local@domain` dot-atoms, a custom header compose writes itself or a
`Content-*` one, inline parts without HTML or a unique content id, multipart
attachment types, no recipient for a send, more than `max_bytes`. One tree,
each level only when it has more than one child: `multipart/mixed` >
[`multipart/alternative` > [text, `multipart/related` > [html, inline
parts]], attachments]. A text or HTML body holding a control character other
than tab, CR and LF is base64-encoded, never written raw under `7bit`. An
attached `message/rfc822` is written `7bit` when it is 7-bit clean and
otherwise as a base64 `application/octet-stream` file. Bcc is a header:
Gmail's send has no envelope.

Gmail replaces a client's `Message-ID` and `Date` on every API send and
every draft save, and keeps every other client header byte for byte. So the
identity of a message mailtide writes is the private header `X-Mailtide-Id:
<token>` (16 to 64 characters of `A-Za-z0-9_-`): `compose` writes it from
`tracking_id`, `new_tracking_id(entropy)` mints one, and `tracking_id(raw)`
reads it back from the header block. Message-ID stays an optional input
written as given; Date is always written, in UTC.

## 6. Sync algorithms (read path, phases 1–3)

### 6.1 Incremental sync (history)

Triggered by a push, a poll timer or a follow-up flag.

1. Read history from `cursor`, following **every page** [G][Z].
2. Fold the records into one change set:
   - `messagesAdded`: if the message is unknown and has no tombstone, create
     it with `import_state=pending` and the record's labels; enqueue a body
     import job.
   - `labelsAdded` / `labelsRemoved`: update `label_ids` **without fetching
     bodies** [Z], subject to the checkpoint rule (§6.5).
   - `messagesDeleted`: tombstone it, remove the message, cancel its pending
     intents and jobs, and enqueue a blob cleanup [Z].
3. Emit **one** `Commit{changes, new_jobs, cursor=history.historyId}`. The
   host applies it in a single transaction (I1).
4. A notification that arrived during the fetch has a higher revision than
   the one being processed, so it forces a follow-up pass [Z].
5. On **404**, switch to reconciliation (§6.3). The cursor is not moved.

### 6.2 Scheduling

- A push is a **hint**: it records `notification_revision = max(...)` and
  schedules a pass. It never carries data the engine trusts.
- Poll as a safety net: active accounts about every 1 min, idle accounts about
  every 15 min [Z]. Stagger by a hash of the account id.
- Watch renewal is daily, or before expiry. **Renewal never moves the
  cursor** [Z]: a renewal that jumped the cursor to the watch's current id
  would silently skip any history window not yet applied.
- One sync gate per account: history apply, snapshot steps and action
  delivery for the same account are serialized [Z].

### 6.3 Full reconciliation (also the initial import)

Entered when there is no cursor (bootstrap), when history returns 404, or
when an operator asks for it. Repeated expiries back off.

1. **Labels, then the baseline.** List labels, then call `getProfile` and
   record `baseline = profile.historyId` **before** listing anything [Z][G].
2. **Enumerate.** `messages.list` (`includeSpamTrash`) over all mail, then
   once per label, newest first. Each page is one commit: listed ids are
   recorded in a durable seen-set, unknown ids get a row (provisional, see
   §6.5) and an import job. Progress (scope, page token) is part of the sync
   state, so a restart resumes.
3. **Finish** with one commit that also moves the cursor to the baseline and
   sets the state to Live:
   - Listings are not snapshots: a concurrent change can make a page skip a
     message, and labels changed while history was unavailable are lost.
     So **every message whose checkpoint is older than the baseline gets a
     verification fetch** (`format=minimal`) and becomes *unverified*
     (§6.5). A first bootstrap pays nothing extra: every message is new.
   - Only a confirmed 404 tombstones a message. Absence from a listing never
     does [Z].
4. Incremental sync resumes from the baseline, replaying every change made
   during the enumeration (I4). Imports and verifications needn't finish
   first: they are durable jobs [Z].

### 6.4 Body import jobs

- Deduplicated by `message_id`.
- Priority classes: `foreground` (a user opened it) > `live` (newly arrived) >
  `backfill` (historical import) [Z]. Backfill runs newest first.
- **A user opens a message** whose body is still queued:
  `Store::prioritise(account, ids, now)` (what `Runner.prioritise` calls)
  raises its import job to `foreground`. Like admission (§7.1) it is **not
  fenced by any engine's epoch**, and it is safe without the fence because
  it only ever raises a class and never creates or finishes a job. A ready
  job delayed without a failure (a quota deferral, a draft's settle delay)
  becomes due at once; a job backing off after a failure keeps its due time
  [Z]; a running job keeps its claim and comes back as `foreground` if it is
  retried or deferred. Nothing else changes: no row, no feed entry, and the
  state string does not move. Each distinct id is reported as `queued`,
  `imported` (read it now), `unknown` (no row: never discovered, or deleted;
  nothing is fetched for it), `blocked`, or `over_limit`: an account holds
  at most 24 `foreground` imports at once (the 30% of a minute's budget
  Backfill can't use, at 60 units each), and a call takes at most 100 ids.
  The engine holding the account takes the raised job on its next claim,
  also while the gate holds Backfill back; the host calls `run` if no run is
  in flight. The body's `MessageImported` feed entry says it arrived.
- Fetch with `format=raw` and hand the bytes to the host as `StoreBlob`. Then
  `Commit` the header facts, `blob_ref` and `import_state=imported`.
- Retries:
  - Transient errors (5xx, 408, network) retry with persisted exponential
    backoff plus jitter, capped (Zero: 60 s base cap). `Retry-After` is a lower
    bound.
  - A 404 at import time: tombstone at the fetch checkpoint.
  - Permanent errors (auth revoked, malformed payload): `blocked`, visible in
    status, retried only by an operator action.

### 6.5 Per-message checkpoints and tombstones

- Every message stores the `checkpoint` (a historyId) its label state reflects.
- A full fetch carries the profile/history position it was read at. A history
  delta carries its record's id.
- **An update is applied only if its checkpoint ≥ the stored checkpoint**
  (I3). A stale snapshot can't overwrite a newer delta, and an old delta can't
  overwrite a newer snapshot [Z].
- Tombstones reject every older event for that id, so a stale import job can't
  resurrect a deleted message (I5).
- **Unverified rows.** A row created by a listing, or left behind when history
  was lost, carries `unverified = Some(floor)`. Its labels can't be trusted:
  - a history delta still applies (so users see it), but on a base that may
    be wrong, so it does **not** advance the checkpoint; it raises the floor
    to its own id instead;
  - only a full observation (`get`) at or after the floor settles the row;
  - a verification fetch stays queued until one does.

  The simulator found why: without this, deltas applied to a stale base
  advanced the checkpoint, the verification fetch that would have corrected
  it then looked older and was rejected, and the stale labels stuck forever.

### 6.6 Labels

History carries label ids, never definitions (a new name leaves no record),
so the definitions are listed again (`labels.list`) on a schedule [Z]:

- **Periodically, and on start.** They are listed once they are older than
  `label_refresh` (default 15 min). Age is measured from the last listing,
  whose time the account row keeps (`labels_refreshed_at`, migration 4;
  `NULL` until the first listing), never from how long an engine has run: a
  starting engine lists them at once if that time is `label_refresh` ago or
  unset, and otherwise sets the `Labels` timer for when it will be. A host
  that starts a run every few seconds lists them once per interval; an
  account idle for an hour lists them on its next run. Reconciliation's own
  listing (§6.3) counts as one.
- **On an unknown label id.** A label id the stored definitions lack, in a
  history page or a fetched or imported message, asks for a listing: one
  for the whole batch, and no sooner than 1 min (or `label_refresh`, if
  shorter) after the last one. An id seen while a listing is in flight may
  postdate it, so if that listing lacks it, one more follows. An id still
  missing from a listing sent after it was seen asks for nothing more in
  that engine (the periodic listing still runs), so an id Gmail never lists
  can't make the engine list labels over and over.
- **Quiet when unchanged.** A listing replaces the stored definitions only
  if it differs from them (in any order), and only then logs a
  `LabelDefinitionsChanged` feed entry; its time is recorded either way.
  Reconciliation's listing follows the same rule.
- **Never in the way of mail.** A refresh runs beside history passes and
  jobs, never holds them back, and its commit never moves the cursor or the
  lifecycle. It is a Live read (1 unit, §8), so read-only runners do it too.
  A transient failure retries with the engine's backoff (a quota deferral
  waits as long as the gate says; a cooldown, until it ends); a permanent
  refusal or a 404 doesn't block the account, and the next periodic listing
  tries again.
- Store label ids, never names, on messages. Renames are a label-table update.
- **A label that goes leaves every message.** When the mirror learns a
  label is gone (a listing lacks a stored id, or the host's own delete was
  answered, or answered 404) it strips the id from every row, trusted or
  unverified, without moving a checkpoint (Gmail's own `labelsRemoved`
  records, one per message, then change nothing), drops the intents on it
  in every state, and logs `LabelDefinitionsChanged` and one
  `LabelsChanged` per row, in one commit. One listing is proof: Gmail's
  listing reflects every create and delete that returned before it.
- **A deleted id cannot come back.** It is remembered as deleted for 30
  days (longer than Gmail keeps history), pruned at an applied listing:
  observations, history deltas, imports, a finishing reconciliation's
  scopes and new admissions are stored without it, so a fetch in flight
  across the deletion, or history read from a cursor behind it, cannot put
  it back on a row. A listing that names it again restores it.
- **Local label writes** (`Runner.create_label`, `update_label`,
  `delete_label`) are direct calls (§8) whose answer the driver records at
  once with `Store::record_label`, unfenced like an admission: the
  definition, a deletion's strip, their feed entries, and a bump of the
  label generation. The driver reads the generation before every
  `labels.list` and reports it with the listing; a listing sent before a
  local write is not applied (the next one is), so it can neither remove a
  label just created nor restore one just deleted. Gmail renames and
  deletes only the label it is asked to: a rename moves the sublabels
  first and the label last (a retry finishes a half-done one), a delete
  refuses while sublabels exist unless asked to delete them, deepest
  first. Names are compared as Gmail compares them (case-insensitively,
  surrounding space ignored); colours are checked against Gmail's palette
  and sent lower-cased. Read-only mode refuses the writes before any call.

## 7. Actions (write path)

1. **Admission.** `Store::admit(account, Messages{ids, action} | Thread{id,
   action})` (what `Runner.act` calls; sans-IO hosts may instead send
   `ActionRequested` to the engine, which commits `IntentOp::Request`). It is
   **not fenced by any engine's epoch**: a user's action must survive a
   concurrent sync superseding another. The store
   expands it against each message's effective labels into one versioned
   intent per (message, label), and enqueues a Foreground `Deliver` job, in
   one transaction. A thread action reaches every non-draft message the
   mirror has for the thread at admission.
   - Actions are explicit enums (`SetRead(Read|Unread)`, `Archive`,
     `MoveToTrash`, `RestoreFromTrash(Inbox|Archive)`, `MarkSpam`,
     `ChangeLabels{add, remove}`, …).
   - Drafts are never targets; `SENT`, `DRAFT`, `CHAT` can't be set; only one
     of `INBOX`, `SPAM`, `TRASH` may be added at once.
   - A new action on the same (message, label) replaces the intent with a
     higher version.
2. **Implied changes are explicit and always sent.** Trash and spam add the
   label **and** remove `INBOX` (also when added through `ChangeLabels`);
   restoring goes where the action says. Every
   implied change is kept even if the mirror already shows it: the mirror can
   lag Gmail, and Gmail's changes are absolute, so resending is harmless while
   dropping could lose the user's action.
3. **Delivery** runs as a job. Claiming `Deliver(m)` attaches m's pending
   changes (at most 100 label ids per list; the rest go next) as a
   `ModifyGroup`; groups under 10 messages use
   `messages.modify`, larger ones `batchModify` (≤ 1,000). Success commits
   `Delivered` for exactly the versions sent (I8) and queues a confirmation
   `get` after `confirm_timeout`. Changes admitted meanwhile are delivered
   next. A transport error retries (idempotent); a permanent refusal blocks
   the intents; a 404 tombstones the message. Gmail refuses a whole modify
   with 400 when an add names a label it does not have (deleted, or never
   created), without naming it, and accepts removing one: so when a group
   refused for good adds labels that are not Gmail's fixed system labels
   *beside other changes*, only those adds are blocked, the rest is
   delivered again on its own, and a label listing is asked for (§6.6),
   which drops a deleted label's blocked intent.
4. **Overlay.** `Store::messages` returns observed labels **and** effective
   labels (pending, delivered and unresolved intents applied in version
   order), plus counts of pending and blocked actions.
5. **Confirmation** (I7). A delivered intent settles only on a fetch
   (`get`) claimed **after** its delivery (attempt ids are monotonic, so this
   is checkable). Whatever that fetch shows is Gmail's state, so the overlay
   goes (if someone changed it since, Gmail wins). History never settles a
   delivered intent: a record fetched after the delivery may still predate
   it, and settling on it showed stale state (found by a ported Zero test).
   The fetch runs `confirm_timeout` (default 1 min) after delivery and stays
   queued until one settles the intents. An identical change already pending
   or delivered is not sent again. Pending intents are always delivered.
6. Message deletion drops its intents and cancels its delivery (I5).

Sending and drafts reuse the same command and confirm-by-Gmail pattern
later.

## 8. Quota and rate limiting

Per-call costs [G]:

| Call | Units |
|---|---|
| `messages.get` (minimal/metadata) | 20 |
| `messages.get` (raw/full) | 60 |
| `messages.list` | 5 |
| `threads.list` | 10 |
| `history.list` | 2 |
| `messages.modify` | 5 |
| `messages.batchModify` | 50 |
| `getProfile` | 1 |
| `labels.list` | 1 |
| `labels.get` | 1 |
| `labels.create` / `labels.patch` / `labels.delete` | 5 (`patch` is not in Google's table: declared as `labels.update`) |
| `watch` | 100 |
| `messages.insert` / `import` | 25 |
| `messages.send` | 100 |

- Every engine command declares `cost_units` and a `priority`
  (`foreground | live | backfill`).
- **The host enforces budgets** over a rolling minute: per account and per
  project. Defaults are 80% of the published limits (4,800 / 960,000) [Z].
- **Reserves:** foreground may use 100% of the budget, live 90%, backfill 70%,
  so imports never starve user-facing work [Z].
- **Concurrency leases** per account and per project, with expiry, so a dead
  worker frees its slot [Z].
- **Error classification:**
  - `userRateLimitExceeded` and unclassified 429 → account cooldown.
  - Project quota or rate errors → project cooldown.
  - `Retry-After` is a minimum. Cooldowns never shorten an existing pause [Z].
- A quota denial is a **deferral**, not a failure: it never counts against an
  attempt budget [Z].
- **Direct calls** (`Runner::call_gmail`; drift checks use it): a request a
  host needs answered at once, made outside the engine through the same
  gate, with its declared cost and a priority the caller picks. A deferral
  is waited out while it ends within the call's budget; otherwise the call
  returns `QuotaDeferred { until }` and nothing is sent. A rate limit cools
  the gate down as the engine would. Read-only mode refuses a request that
  changes the mailbox before any credential or quota is spent. A direct
  call claims nothing, commits nothing and never moves the cursor (its
  command id has epoch 0, which no engine has). A refusal (a 400, a 403
  including a scope the token lacks, an account problem) is the caller's
  and blocks nothing; a real account problem is recorded by the account's
  next run. An answer lost after the request went out (a timeout, a dropped
  connection) is reported as outcome unknown, never as unsent. A caller that
  keeps Gmail's answer in the mirror records it afterwards with an unfenced
  store method, as `admit` is: the answer only, never the cursor.
- **Search** (`search`, `search_threads`) is a direct call: one
  `messages.list` (5 units) or `threads.list` (10) per page, at the caller's
  priority (Foreground by default; a background job passes Backfill), plus
  one `labels.get` (1 unit) per filter label the mirror does not know. It
  writes nothing; a refusal is the caller's and blocks nothing.
- **Import throughput** (raw gets): ≈ 6,000 messages/hour per account, so
  100k messages ≈ 17 h. About 200 accounts can import concurrently per
  project. 80M units/day ≈ 1.33M raw messages/day before billing.

## 9. Invariants

These are checked by the simulator after every step (§12) and by drift
detection in production (§13).

- **I1 Atomic progress.** The cursor advances only in the same transaction as
  every change and job derived from the history it covers.
- **I2 Monotonic cursor.** The cursor never decreases and never jumps past
  history it hasn't applied. Watch renewal never moves it.
- **I3 Monotonic message state.** A message's checkpoint never decreases.
  A trusted row's labels are the state at its checkpoint; an unverified row
  (§6.5) is settled only by a full observation at or after its floor.
- **I4 No skipped window.** After reconciliation, the cursor is the baseline
  captured before enumeration. Every change after it is applied by
  incremental sync.
- **I5 Deletion is final.** A tombstoned id is never recreated by an event
  older than its tombstone. Its intents and jobs are cancelled.
- **I6 No silent loss.** Every message id the engine has seen is in exactly
  one state: `pending | imported | failed | blocked | tombstoned`.
  `pending` and `failed` have a scheduled job.
- **I7 Confirmation by Gmail.** A delivered intent settles only on a fetch
  claimed after its delivery; it is otherwise only superseded, blocked, or
  dropped with its message.
- **I8 Version fencing.** An acknowledgment or result for version v never
  changes an intent at version > v.
- **I9 Convergence.** With no further Gmail changes, no failures and an
  unlimited budget, the mirror's message set and label sets equal Gmail's
  within a bounded number of steps.
- **I10 Determinism.** The same event sequence produces the same command
  sequence.
- **I11 Deleted labels stay deleted.** Label definition ids are unique, and
  no message row and no intent names a label remembered as deleted (§6.6).

## 10. Engine interface

The code is the reference (`crates/mailtide/src/{event,command,commit}.rs`);
this is the shape.

### 10.1 Events (host → engine)

Every event carries `now`. Results answer a `CommandId` (which embeds the
engine's epoch, so results meant for a dead engine are ignored).

- Triggers: `PushReceived{history_id}`, `TimerFired{timer}`,
  `ActionRequested{action}`.
- Gmail results: `ProfileFetched`, `HistoryFetched{page}`, `HistoryExpired`,
  `MessagesListed{page}`, `MessageFetched{observation}`,
  `MessageImported{message}` (after the blob is stored), `MessageNotFound`,
  `LabelsListed{labels, generation}` (the label generation the host read
  before sending it), `WatchRenewed{expiration}`, `LabelsModified`, and
  `Searched{listing}`, which answers the driver's own search request and
  which the engine ignores. The driver's own label requests answer
  `LabelFetched{label}`, `LabelSaved{label}`, `LabelNameTaken` (a 409) and
  `LabelDeleted`; the engine issues none.
- Store results: `CommitApplied`, `CommitRejected{reason}`,
  `JobsClaimed{jobs}`, `BlobDeleted`.
- Failures: `RequestFailed{failure}` (retryable with class and
  `Retry-After` / not found / blocking), `RequestDeferred{until}` (the quota
  gate said not yet; not a failure).

### 10.2 Commands (engine → host)

- `Gmail{account, request, priority, cost}`. The engine never issues
  `GmailRequest::Search`: only the driver's direct call does.
- `Commit(commit)`: atomic, guarded (epoch, engine version, expected cursor,
  job claim).
- `ClaimJobs{epoch, limit, now, attempt}`, `DeleteBlob{message}`.
- `Schedule{timer, at}`, `ReportStatus(status)`.

### 10.3 Store contract and conformance

`mailtide_store::Store`:

- `claim(account, version)`: bump the epoch (fencing every older writer),
  release jobs held by older engines, return the snapshot to build an engine
  from (validated into a `ValidSnapshot`), including when the label
  definitions were last listed and their ids (§6.6).
- `apply(commit)`: all or nothing; rejections (`StaleEpoch`, `StaleVersion`,
  `CursorMismatch`, `StaleClaim`) are outcomes, not errors.
- `claim_jobs(account, epoch, limit, now, attempt)`: refused for an older
  epoch; `Deliver` jobs carry their pending changes.
- `admit(account, request, now)` and `prioritise(account, ids, now)`:
  unfenced writes for users' actions (§7.1) and opened messages (§6.4).
- `record_label(account, op)` (an `Upsert` or a `Remove`): the unfenced
  record of a local label write, bumping the label generation;
  `label_generation(account)` reads it (0 for an untracked account) (§6.6).
- `changes(account, since: FeedCursor, limit)`: the change feed
  (`ChangeFeed::Page` of `ChangeEntry { state, index, change, content }`, or
  `TooOld` after compaction). The store reads the first `limit + 1` entries
  after `since` in `(state, index)` order (`limit` clamped to
  `1..=MAX_FEED_PAGE`, 5,000) in one keyset statement, and the page ends at
  the last commit boundary among the first `limit` (`next` is that commit).
  Only when none is there, because the first commit after `since` (or the
  rest of the one it is inside) is larger than `limit`, is the commit split:
  the page is its first `limit` entries and `next` the last of them. An
  empty page returns `since`; `has_more` says whether more follow. `TooOld` when `since` is behind the
  floor or inside the floor commit (compaction removes whole commits).
  `content` is the stored content of a `MessageImported` or
  `FactsRefreshed` entry's message, loaded for the page's entries in chunks
  of ids, `None` once it is deleted. `compact(account, retention)` removes whole commits under the
  account row lock: `Retention { through, now, max_idle }` gives an optional
  bound (a reader inside commit `S` passes `FeedCursor::compactable()`,
  `S - 1`), and the target is `compaction_target` over the account's
  consumer cursors (§4), after every cursor at the head is seen at `now`;
  `message_ids(account, after, limit)` to rebuild a consumer.
- `ack(account, name, position, now)`, `consumers(account)` and
  `drop_consumer(account, name)`: consumer cursors (§4), under the account
  row lock and unfenced like `admit`. An `ack` of a position behind the
  floor, inside the floor commit, beyond the head or of another generation
  is `AckOutcome::TooOld` and stores nothing; a new name beyond 16 is
  `TooManyConsumers`. `consumers` returns the cursors with the generation
  and floor (`ConsumerList`), so a caller can tell which are lost.
- `messages(account, ids)`: `MessageView`s for hosts.
- `threads(account, ids)`: `ThreadView`s (§4) in the order asked, each
  member its `messages` view; ids the mirror has no message in, and every id
  of an untracked account, are left out; no ids read nothing. The SQL store
  reads each chunk of ids with one statement joining rows and intents, so a
  view never mixes rows from before a commit with intents from after it.
- `locate(account, ids)`: per id (at most 500), a row with or without its
  content, a tombstone, or nothing; a read by primary key that claims
  nothing (what a search marks its hits with).
- `labels(account)`: the label definitions (`Label { id, name, kind,
  list_visibility, message_visibility, color }`) as last synced or written,
  in id order; empty before the first label sync.
- `refresh_facts(account, updates)`: bring stored message facts up to date
  in one transaction. Not fenced by an epoch (like `admit`: it neither
  supersedes a running sync nor is rejected by one) but conditional per row:
  an update applies only while the row exists, is imported, has the same
  blob and the size the refresher read, and its facts are missing or older
  (or the blob's length differs from that size); the row's facts and size
  are set, nothing else. Applied updates log `FactsRefreshed` under one new
  state.
- The SQL store refuses every one of these with
  `StoreError::SchemaOutOfDate { applied, expected }`, before reading or
  writing, while the database's schema is behind this build (migrations
  missing) or ahead of it (a newer mailtide migrated it). It asks once: after
  it has seen the schema current, it and its clones do not ask again.

Facts refresh (`Runner::refresh_facts(account, after, limit, budget)`) is
a host-scheduled maintenance call, like `drift` and `compact`, with no Gmail
call: it pages the messages from `after` in batches of 50, reads the blob (8
at a time) of each imported row whose facts are missing or older than
`FACTS_VERSION`, or whose blob is not the length the row holds (learnt from
an empty ranged read, so a current row costs no blob read), extracts, and
writes each batch with `Store::refresh_facts`. It stops between batches at
`limit` messages or when `budget` is spent and returns `FactsRefresh {
examined, refreshed, unreadable, next }`; `next == None` ends the pass. A
missing blob is named in `unreadable`; a store failure rolls the batch back
(call again from the same `after`).

The conformance suite (`mailtide_store::conformance`, 60 cases) runs against
the memory store and SQLite on every test run, and Postgres in CI.

## 11. Python bindings

Python stays thin; everything else is Rust. `mailtide.Runner(config,
token_source)` is async (every method is a coroutine); `mailtide.BlockingRunner`
has the same methods, blocking, for code without an event loop. `config` is a
`mailtide.config.Config` of plain frozen dataclasses (database URL and pool
limits, blob URL, watch mode, HTTP, engine tunables such as
`label_refresh_s`, quota budget); the Rust core parses and validates it once
and raises `ValueError` naming the first invalid field. Postgres pools
default to 2 connections opened lazily; a run uses one at a time. Build one
runner per process, after forking:

- `await migrate()`; `await run(account, budget_s) -> RunOutcome` (call on a
  schedule and on each verified push; `stop` says why it stopped: `idle` with
  the next due time, `budget_exhausted`, `cooling_down`, `blocked`,
  `superseded`);
- `await act(account, action, message_ids=… | thread_id=…) -> RunOutcome`:
  admitted durably first (`outcome.admitted`), then delivered;
- `await admit(account, action, …) -> int`: admit durably without
  delivering (the account's next run delivers); never preempts a sync;
- `await prioritise(account, message_ids) -> PrioritiseResult`: a user
  opened these messages; their bodies are fetched next, in the Foreground
  class (§6.4), by whichever run holds the account (call `run` if none
  does). One store write that never preempts a sync, allowed read-only.
  The result lists each distinct id once: `queued`, `imported`, `unknown`,
  `blocked`, `over_limit` (ask again as imports land). More than 100 ids
  raise `ValueError`;
- `await status(account)`, `await accounts()`, `await health()`: reads that
  never claim an account; `await resume(account)` after an operator fixed a
  block; `await forget(account)` deletes its rows and blobs;
- `await messages(account, ids) -> list[MessageView]` (labels with pending
  actions applied, observed labels, and `content` once imported);
- `await threads(account, thread_ids) -> list[ThreadView]` (§4): `messages`
  (each a `MessageView`), `labels`, `unread`, `latest` (the same object as
  in `messages`, or `None`), `message_count`, `draft_count`,
  `awaiting_import`, `participants`; in the order asked, unknown ids left
  out. A store read: no token, no Gmail call, no claim. Listing threads,
  sorting, paging and counting them is the host's read model: collect the
  thread ids of a `changes` page, read them in one call, upsert each answer
  and delete the ids left out;
- `await search(account, query, *, label_ids=(), include_spam_trash=False,
  limit=100, page_token=None, priority="foreground", budget_s=10.0) ->
  SearchPage`: one page of Gmail's own search for `query` (the search box's
  syntax, sent untouched; `messages.list`, 5 units, through the quota gate).
  Each `SearchHit(message_id, thread_id, state)` is `imported`,
  `not_imported` (a row, no content yet) or `unknown` (no row yet: Gmail is
  ahead of the mirror; the next `run` brings it), in Gmail's order; hits the
  mirror deleted are left out and counted in `deleted`. Read hits with
  `messages`. `label_ids` are label ids every hit carries (names go in the
  query as `label:`). Gmail answers a label id it does not have with no
  hits rather than an error, so an id the mirror's label definitions lack
  (all of them before the first label listing) is checked first with one
  `labels.get` (1 unit) per distinct id: a 404 raises `UnknownLabel` and no
  search is sent; an answer means the mirror's definitions are behind, and
  the search goes on (nothing is recorded; the label refresh brings it). `include_spam_trash` reaches Spam and Trash
  for a query that names neither (`in:trash`, `in:spam` and `in:anywhere`
  reach them anyway). `limit` is clamped to 1..=500. `next_page_token`,
  passed back with the same arguments, reads the next page; it is refused
  (`InvalidPageToken`, nothing sent) for any other search, because Gmail's
  own token is a bare position it accepts under any other request; a token
  Gmail refuses raises `PageTokenRejected`: start again. Paging is not a snapshot:
  ids can repeat or be skipped when the mailbox changes between pages.
  `mirror_state` is the feed state read before Gmail was asked: feed entries
  after it mean the page may be stale. `estimate` is Gmail's
  `resultSizeEstimate`, a hint. A search writes nothing, never preempts a
  sync, works read-only, and is never retried by mailtide. Refusals:
  `SearchRefused` (Gmail's 400 or 403; the account is not blocked),
  `QuotaDeferred`, `GmailUnavailable` (5xx, timeout, rate limit),
  `AccountBlocked`, `AccountNotTracked`, `ValueError` for a blank query;
- `await search_threads(...) -> ThreadSearchPage`: the same for thread ids
  (`threads.list`, 10 units), each `ThreadHit(thread_id, history_id)` with
  Gmail's history id for the thread (past the page's `cursor`, the mirror
  has not caught up with it); thread hits are not marked against the mirror;
- `search_literal(text)` renders user text as one search value that cannot
  add operators (it is ASCII-lowercased, since `OR`, `AND`, `AROUND` are
  operators only in upper case, even inside quotes; quotes, braces,
  parentheses, `|`, `\` and `:` become spaces; anything but one bare word
  is quoted; `ValueError` when no letter or digit is left); `search_by_message_id(id)` is the
  `rfc822msgid:` term for the `Message-ID` Gmail stored, which is not the one
  a client wrote on a message sent or a draft saved through the API;
- `await labels(account) -> list[Label]`: the label definitions (`id`,
  `name`, `kind`: `system` or `user`, `list_visibility`,
  `message_visibility`, `color`: a `LabelColor(text, background)` or
  `None`, and `parent_id`: the nearest label above it by name) as the
  mirror last synced them, in id order, to show label ids as names; empty
  before the first label sync. They are listed again at least every
  `Engine.label_refresh_s` (default 900 s, §6.6). A
  `LabelDefinitionsChanged` feed entry (logged only when they changed)
  means: read them again;
- `await create_label(account, name, *, list_visibility="show",
  message_visibility="show", color=None) -> Label`, `await
  update_label(account, label_id, *, name=None, list_visibility=None,
  message_visibility=None, color="keep", sublabels="follow") ->
  LabelUpdate` (`color=None` clears it) and `await delete_label(account,
  label_id, *, sublabels="refuse") -> LabelDeletion` (`deleted`,
  `already_gone`): Gmail's label writes, recorded at once (§6.6), each with
  a `budget_s` for the quota gate. They raise `LabelNotFound`,
  `LabelNameTaken` (`existing`: the label holding the name), `InvalidLabel`
  (before any call), `LabelHasSublabels` (`ids`), `LabelRefused`,
  `GmailUnavailable`, `QuotaDeferred`, `WriteOutcomeUnknown` and
  `ReadOnlyMode`; one that stopped half-way carries `done`, and the same
  call again finishes it;
- `await changes(account, since, limit=500) -> ChangePage | StateTooOld`:
  at most `limit` entries per page (at most 5,000), continuing from
  `page.next` while `page.has_more`. Pages hold whole commits, ending at the
  last boundary that fits; only a commit larger than `limit` is split, and
  `next` then points inside it. Every state string stored before still
  resumes where it did. Typed entries,
  each with a stable `(state, index)` identity: `MessageDiscovered`,
  `MessageImported` (once per message, with its labels then, e.g. `SENT`, its
  blob, and `content`: the `MessageContent` `messages` returns, so a message
  list needs no second call; `None` once the message is deleted),
  `LabelsChanged` (added/removed), `OverlayChanged`,
  `MessageDeleted` (last thread and labels), `LabelDefinitionsChanged`,
  `FactsRefreshed` (with `content`, as for an import);
  `await compact(account, through=None, max_idle_s=604_800.0)` (whole
  commits, no further than `through` when given, where a `through` inside a
  commit keeps that commit, nor past the slowest registered consumer heard
  from within `max_idle_s`; with neither a bound nor a consumer nothing
  goes); `await ack(account, consumer, through) -> ConsumerCursor |
  StateTooOld` (consumer cursors, §4: `ConsumerCursor { name, state,
  seen_at_ms, lost }`; a new name beyond 16 raises `ValueError`);
  `await consumers(account) -> list[ConsumerCursor]`;
  `await drop_consumer(account, consumer) -> bool`;
  `await message_ids(account, after)`;
  `await changes(account, since) -> ChangePage`;
  `await read_blob(account, id) -> bytes`;
- message facts on `MessageContent` (§4): `facts_version` (0 when not
  extracted), `bcc`, `reply_to`, `sent_at_ms` and `sent_at_offset_min`,
  `sender_name`, `sender_email`, `list_id`, `list_unsubscribe`,
  `one_click_unsubscribe`, `auto_submitted`, `precedence`, `tracking_id`,
  and the attachment summary: `attachments: list[AttachmentInfo]` (`part`,
  `filename`, `content_type`, `size`, `inline`, `disposition`,
  `content_id`), `attachment_count`, `inline_count`, `protocol_count`,
  `has_attachment`, all `None` when not extracted, so unknown is never
  shown as none. Filenames are the sender's: treat them as untrusted;
- parts, with no Gmail call: `await parts(account, id) -> list[PartInfo]`
  (every leaf, nested messages included: reference, role, type, filename,
  size, Content-ID, charset, container and depth; the blob is parsed in
  Rust); `await read_part(account, id, part, max_bytes=25 MiB) -> bytes`
  (one ranged read; `PartTooLarge` before any read above `max_bytes`);
  `await open_part(account, id, part, chunk_size=256 KiB) -> PartStream`
  (decoded chunks of at most `chunk_size` from a streamed ranged read; an
  async and a plain iterator and a context manager; leaving it or
  cancelling the task inside it drops the read; a failure ends it with an
  exception, never a short part); `await refresh_facts(account, after=None,
  limit=1000, budget_s=60) -> FactsRefresh` (§10.3);
- `verify_push(...) -> Notification`; `parse_message(raw) -> ParsedMessage`
  (each attachment also carries its `part`); `decode_header(value)` decodes
  a `subject` as the header facts keep it;
- composition (§5.5), pure: `compose_message(message, *, purpose="send",
  max_bytes=None) -> ComposedMessage` (`raw`, `message_id`, `tracking_id`,
  `recipients`) over a frozen `OutgoingMessage` (`from_`, `to`, `cc`, `bcc`,
  `reply_to`, `sender` as `Address` or a bare address string; `subject`,
  `sent_at_ms`, `message_id`, `tracking_id`, `in_reply_to`, `references`,
  `headers`, `text`, `html`, `inline`, `attachments` of `Attachment`, which
  now has a constructor). The binding supplies the impure defaults: a
  missing `message_id` or `tracking_id` is minted from 128 random bits and a
  missing `sent_at_ms` is now. `new_message_id(sender)`, `new_tracking_id()`,
  `tracking_id(raw)` and `parse_addresses(value) -> list[Address]` (a header
  value as `MessageContent` holds it) go with it. Invalid input raises
  `ComposeError`, a `ValueError` with `field`, `index` and `reason`;
- exceptions: `MailtideError` and its subclasses `StoreUnavailable`,
  `OutcomeUnknown`, `CorruptState`, `BlobUnavailable`, `QuotaGateError`,
  `QuotaDeferred` (a direct call, §8, that the quota gate would hold past
  its budget; nothing was sent; `until_ms` says when it could run),
  `WriteOutcomeUnknown` (a direct call that writes reached Gmail, or may
  have, and its answer was lost: it may or may not have been applied),
  `SearchRefused` and its subclass `UnknownLabel` (`label_id`),
  `InvalidPageToken` and its subclass `PageTokenRejected`,
  `GmailUnavailable`, `AccountBlocked`, the label errors above,
  `Superseded`, `InvalidPush`, and `DelegationRevoked` (raised by the token source
  to block an account; the block is committed, and later runs make no
  credential calls), `InvalidPush`, `BlobNotFound`, `PartNotFound` (a
  reference that names no part of the message: unreadable, outside it, or
  made for other content) and `PartTooLarge`.

Async methods run on one process-wide Tokio runtime and work with
`asyncio.create_task`, `TaskGroup` and `asyncio.timeout`. **Cancelling** an
awaiting task drops the Rust work: in-flight requests stop, an open
transaction rolls back, and the request's quota lease is returned. That is
safe because every commit is atomic and fenced. The token source may be sync
(run on Tokio's blocking pool, since minting may do network I/O) or async
(awaited on the caller's event loop; async `Runner` only).

Schema: `await migrate()` applies mailtide's versioned migrations under an
advisory lock; hosts that run migrations with their own tool execute
`mailtide.migrations()` (ordered SQL with its bookkeeping) instead.
`schema()` reports `current`, `behind` or `ahead`. Every call that reads or
writes the database refuses with `SchemaOutOfDate` unless it is current:
`run`, `act`, `admit`, `prioritise`, `track`, `drift`, `notify`, `search`,
`search_threads`, the label writes, and the reads too (`changes`, `messages`, `threads`,
`labels`, `message_ids`, `compact`, `ack`, `consumers`, `drop_consumer`,
`status`, `accounts`, `resume`,
`forget`), so a build that is too old
refuses instead of misreading. Only `schema`, `migrate`, `health` and
`read_blob` (blob storage only) answer whatever the schema. The SQL store
makes the check (§10.3), so Rust hosts get it too.

Migrations are additive. Migration 4 adds the nullable
`mailtide_accounts.labels_refreshed_at` (§6.6); a build from before it sees
a database migrated to version 4 as `ahead` and refuses to sync. Reverting
it is safe, because the column is disposable (without it, the next run just
lists the labels again):

```sql
ALTER TABLE mailtide_accounts DROP COLUMN labels_refreshed_at;  -- SQLite ≥ 3.35
DELETE FROM mailtide_schema WHERE version = 4;
```

Migration 5 has no statement of its own: it raises the version because
state strings may now name an entry inside a commit (`mt2-<gen>.<seq>.<idx>`,
§4), which a build from before it cannot decode, so such a build sees the
database as `ahead` and refuses to sync (its `changes` raises `ValueError`
for such a string, never a wrong page). Such strings arise only while a
consumer reads through a commit larger than its `limit`. Reverting loses
nothing:

```sql
DELETE FROM mailtide_schema WHERE version = 5;
```

A consumer holding a three-field position must first read on with the new
build until `next` is a commit boundary (a two-field string), or rebuild as
after `StateTooOld`; two-field positions mean the same in every build.

Migration 6 adds the index `mailtide_messages_thread` on
`mailtide_messages (account, thread_id, id)` (§4) and changes no row. On
Postgres it blocks writes to the table while it builds; a host with a very
large table can first run `CREATE INDEX CONCURRENTLY` under the same name,
and the migration is then a no-op. A build from before it sees the database
as `ahead`; reverting it loses nothing (thread reads and thread actions only
get slower):

```sql
DROP INDEX mailtide_messages_thread;
DELETE FROM mailtide_schema WHERE version = 6;
```

Migration 7 adds `mailtide_consumers (account, name, seq, idx, seen_at)`,
the consumer cursors (§4; `idx` set is a position inside a commit). A build
from before it sees the database as `ahead` and refuses to sync; its
`compact` checks no schema, so a host must not call it from an older build
while cursors are registered (it would compact past them). Reverting loses
only the cursors: the log and its floor are untouched, each consumer still
follows from its own stored position, the host passes `compact` a position
itself again, and consumers register again with their next `ack` after the
next upgrade:

```sql
DROP TABLE mailtide_consumers;
DELETE FROM mailtide_schema WHERE version = 7;
```

Migration 8 is version only: the message facts (§4) live in the existing
content record, but a build from before them would drop them from every row
it rewrites and cannot read a `FactsRefreshed` feed entry, so it must see the
database as `ahead` and refuse to sync. `ENGINE_VERSION` is unchanged.
Reverting it:

```sql
DELETE FROM mailtide_changes WHERE change LIKE '{"FactsRefreshed"%';
DELETE FROM mailtide_schema WHERE version = 8;
```

The `facts` keys may stay (an older build ignores them). Lost: the facts of
rows the older build rewrites, and the refresh entries; after the next
upgrade, `refresh_facts` finds those rows and restores their facts from the
blobs. Part references a host holds stay valid: they name bytes in a blob.

Migration 9 adds the nullable `mailtide_labels.attrs` (visibility and
colour, JSON) and, on `mailtide_accounts`, `label_generation` and the
nullable `deleted_labels` (the deleted-label memory, JSON), §6.6. A build
from before it sees the database as `ahead`, so it never writes a label
row without its attributes, applies a listing past the generation fence,
or stores a remembered-deleted id again. No feed entry kind and no job
kind is new. Reverting it loses the attributes (the first listing after
the next upgrade refills them) and the memory (a replayed history record
or a late import may then put a deleted id back on a row, until that
message is fetched again):

```sql
ALTER TABLE mailtide_labels DROP COLUMN attrs;                -- SQLite ≥ 3.35
ALTER TABLE mailtide_accounts DROP COLUMN label_generation;
ALTER TABLE mailtide_accounts DROP COLUMN deleted_labels;
DELETE FROM mailtide_schema WHERE version = 9;
```

Tests: `mailtide.testing`
ships the fake Gmail (`FakeGmail`) and a `ManualClock` (`clock=`) to reach
retries, cleanups and confirmations without sleeping.

Every method may be called concurrently from any thread, including on
free-threaded CPython, where the module keeps the GIL disabled (all classes
are immutable; shared Rust state is behind locks or the connection pool).
Wheels: abi3 for CPython ≥ 3.12, plus free-threaded 3.14t and 3.15t.

## 12. Testing

1. **Unit tests** per rule (§6–§8).
2. **Deterministic simulation.** A Gmail simulator implementing §5 with fault
   injection:
   - history expiry (404) at random times;
   - dropped, duplicated and reordered pushes;
   - empty pages with tokens;
   - non-snapshot pagination (concurrent adds and deletes during listing);
   - 429/5xx/408, `Retry-After`, quota denial;
   - a host crash between any two commands (commit lost or applied);
   - slow blob storage;
   - concurrent user changes in "Gmail" while actions are in flight
     (phase 6).

   Each run is seeded and reproducible. Invariants I1–I10 are checked after
   every step. Long seeded campaigns run in CI nightly.
3. **Property tests** (proptest) for the change-set fold and the overlay.
4. **Trace replay.** Recorded, scrubbed event traces from real accounts,
   replayed against the engine and the conformance stores.
5. **Store conformance** (§10.3).
6. **Live canary.** One real test mailbox with scripted changes, compared
   continuously.

Status: unit tests per rule; the simulator (`mailtide-sim`) runs the real
engine and the shared mirror rules against a fake Gmail with swarm-sampled
faults, including users acting through the engine, users opening messages
whose bodies are still queued (`prioritise`, §6.4), label definitions
created, renamed and deleted in Gmail by another client (deletions with
and without history records) and by the host through the driver (a crash
may lose the record), actions adding labels Gmail no longer has, and facts
refreshes run beside the engine (checked to change nothing but facts), and
checks invariants after every
commit and every open (also that no job's class goes down outside a claim's
finish) and convergence after healing (message sets, labels, thread ids and
thread views), label definitions within the refresh
interval; two named feed readers take one page of a random limit (1 to 8)
between steps and acknowledge it while the log is compacted at random
steps (§4, consumer cursors): the steady one must never be cut off
(`ConsumerCutOff`), no compaction may keep a commit every live reader had
passed (`AcknowledgedEntriesKept`), and it must end with the whole change
log, compacted entries included, entry for entry (`FeedPagesDiverged`
otherwise); the other stalls, and the host's housekeeping after a quiet
spell past the idle limit cuts it off (CI: 500 seeds × 1,000 steps; locally
3,000 seeds). Zero's suites
are ported by name and tracked by `scripts/zero_tests.py`. Python tests drive
the real `Runner` against a fake Gmail over HTTP.

## 12a. Running beside an existing system

- **Read-only (shadow) mode**: `Config(access="read_only")` mirrors without
  ever changing the mailbox: `act`/`admit` raise `ReadOnlyMode` (`prioritise`
  works: an import changes nothing in Gmail), and actions
  admitted earlier are blocked (shown as blocked, no longer overlaying)
  instead of sent. It works with the `gmail.readonly` scope. Combine with
  `Poll()` when the host already owns the account's watch.
- **Drift checks**: `drift(account, message_ids=None, sample=50)` fetches a
  sample from Gmail (through the quota gate) and compares labels with a pure
  rule: `matches`; `explained` (pending actions, awaiting verification, or
  Gmail ahead of the mirror's checkpoint, which the next sync closes);
  `differs` (drift: same checkpoint, different labels); `missing_in_gmail`.
  Read-only: nothing is repaired.
- **Host-verified pushes**: `notify(account, history_id)` syncs only when
  the mirror's cursor is behind `history_id`; it never starts an account
  mailtide doesn't already sync (`unknown_account`).

## 13. Observability

Logs: the driver emits `tracing` events (a span per run carrying the
account; `run finished` at INFO with stop, commits, imports, requests and
duration; each Gmail request at DEBUG, failures and refused commits at WARN,
a block at ERROR). Each Python runner bridges them to
`logging.getLogger(config.log.logger)` through its own dispatcher (never a
process-global subscriber): levels are filtered in Rust, a bounded queue and
a drain thread keep logging off the sync path (dropped events are counted
and reported), and fields arrive under `record.mailtide`. Accounts are
hashed unless the host opts in; addresses, subjects, error bodies and tokens
are never logged. Progress: `run(..., on_progress=...)` reports between
commands at most once a second; the async runner calls it on the caller's
event loop in the caller's context, so durable-workflow activities can
heartbeat. Metrics come from `RunOutcome` and `status()`; mailtide ships no
metrics or OpenTelemetry dependency.


- Per-account status: phase, cursor age, notification lag, jobs by state and
  priority, blocked items, cooldowns, import progress (discovered / stored /
  remaining).
  - Remaining never rounds to 100% while work is left [Z].
- **Drift detection:** a periodic sample compares mirror labels with Gmail
  (`format=minimal`) and reports mismatches as a metric. Any mismatch is a
  bug.

## 14. Phases and features

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 Spec | This document, agreed | Decisions closed (§15) |
| 1 Core + simulator | Rust sans-IO engine for the read path (§6), in-memory store, simulator, invariants, labels | Nightly simulation campaigns hold I1–I6, I9 and I10 |
| 2 Stores, driver, bindings | Postgres/SQLite stores passing conformance; async driver (HTTP, quota gate, blobs); maturin wheels | Conformance on every store; end-to-end tests against a fake Gmail |
| 3 Production hardening | Drift metrics, status, observability; first host running in shadow mode | 7 days in shadow on one mailbox with zero drift |
| 4 Read API | Change feed and message views for host UIs: labels, threads, read state | A host can render a full mailbox from the mirror |
| 6 Actions | Intents, delivery, confirmation (§7): read, archive, star, labels, trash/spam | Simulation holds I7 and I8; live canary passes |
| 7 Send + drafts | Composition in `mailtide-mime` (§5.5); send and draft authoring through the same pattern | — |
| OSS | Publish under `denismaciel` (MIT OR Apache-2.0) | Timing: open question |

Status (2026-11-18): phases 1, 2 and 6 are implemented, with most of phase 4
(`changes`, `messages`, `labels`, `threads`). Phase 3 needs a production host.

## 15. Decisions

| # | Decision | Status |
|---|---|---|
| Q1 | Written spec before code | **Yes** |
| Q2 | Gmail only in v1; JMAP-shaped API keeps others possible | **Yes** |
| Q3 | The engine emits atomic, guarded commits; **mailtide ships the stores** (memory, Postgres, SQLite) built on shared pure rules (revised from "the host owns storage") | **Yes** |
| Q4 | Raw content fetched once into blob storage; parsing is a separate optional crate | **Yes** |
| Q5 | The engine owns import ordering and resumability; a quota gate over the core `Ledger` enforces declared costs, priority shares and cooldowns | **Yes** |
| — | Thread model: Gmail `threadId` only; conversations across mailboxes (RFC headers) are left to hosts | **Yes** |
| — | Threads are derived at read time and read by thread ids (`threads`, one index); listing, sorting, paging and per-label counts stay in the host's read model, fed by the change feed (`docs/features/threads.md`) | **Yes** |
| — | Body retention and deletion: blobs of tombstoned messages are deleted after a 24 h grace period | **Yes** |
| — | Blobs are not shared across accounts (no content addressing). Reopened only if two mailboxes' copies of a message prove byte-identical and a host measures duplicate bytes worth the reference counting, say above a tenth of what it stores (`docs/features/blob-dedupe.md`) | **Yes** |
| — | Open source: public under `denismaciel`, wheels on GitHub Releases | **Yes** |
| — | License: MIT OR Apache-2.0 (dual) | **Yes** |
| — | Does the engine also own `watch` provisioning, or only renewal? | Open |
| — | Unverified rows: deltas never advance an unverified row's checkpoint (§6.5) | **Yes** |
| — | Actions send every implied change, even ones the mirror already shows (§7) | **Yes** |
| — | Delivery runs as durable per-message jobs, not a coalescing timer | **Yes** |
| — | Label definitions are written by direct calls recorded at once, not intents; a label that goes leaves every row and its id is remembered as deleted for 30 days (§6.6, `docs/features/label-edit.md`) | **Yes** |
