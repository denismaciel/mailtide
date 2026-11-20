# Drafts: create, update, delete, list, read and send

Plan: `features/drafts.toml` (roadmap C2). Status: in progress.

## 1. Goal and scope

Afterwards a host can, per account:

- save a draft from RFC 5322 bytes carrying one `X-Mailtide-Id` and get
  Gmail's draft id back in the same call;
- replace it (autosave), delete it, and send it;
- list drafts and read one from the mirror, keyed by the **stable draft id**,
  although Gmail gives a draft a new message id on every save;
- see drafts created, edited, deleted or sent **elsewhere** (Gmail's own UI,
  a phone) in the change feed;
- retry any of these after a timeout without sending a message twice.

Left out, with the feature that owns it:

- building the MIME (`compose-mime`); reply and forward headers, new mail
  without a draft (`send`); which
  From addresses and signatures are allowed (`identities`);
- undo-send and scheduled send (a host delay before `send_draft`);
- an idempotent create (Decisions, D4).

## 2. What the references do

Gmail facts come from Google's drafts guide, quota table and method pages
(the `[[source]]` entries, read 2026-11-20): a draft is "a container that
provides a stable ID because the underlying message IDs change every time
the message is replaced"; a draft message has no label but `DRAFT`; on send
"the draft is automatically deleted and a new message with an updated ID is
created with the `SENT` system label"; `drafts.delete` is permanent;
`drafts.list` entries carry only the draft id and the message's id and
thread id. What the design needs beyond that is listed as unknowns in §7.

**Zero** (MIT). `driver/google.ts` passes every draft call straight to Gmail
and the client keeps the draft id; `listThreadDrafts` recovers the
draft id ↔ message id mapping by scanning `drafts.list`. `MailDraftStore`
caches reads for 5–10 s behind a revision that every local write bumps, so an
older in-flight read cannot overwrite a newer write; `assistant-drafts.ts`
adds optimistic concurrency by hashing a draft before an update or send.
*Take:* synchronous pass-through writes, the fence, "a changed draft is not
overwritten or sent". *Leave:* the TTL cache (our mirror already holds the
content, immutable per message id), re-sending the MIME inside `drafts.send`,
reading drafts as `format=full` plus one request per attachment.

**himalaya** (MIT/Apache). `src/gmail/drafts/` is one thin command per
`users.drafts` method over raw bytes; a write reports `{id, messageId,
threadId}` and send carries only the draft id. *Take:* that shape, for
`DraftRef` and `send_draft`. Its JMAP backend stages a `$draft`, submits it,
and keeps the draft when the outcome is uncertain: the rule we apply too.
Its draft checks are live or CLI-level; one unit test covers draft get.

**jmap-client, Stalwart server.** No draft tests in either checkout (the
server's pinned tree has no JMAP test suite). Cited for the model only: a
draft is an Email with `$draft`, emails are immutable so an edit is create
plus destroy, and a send is an EmailSubmission whose success updates the
draft. Gmail's draft id is the stable handle JMAP lacks. Server paths are
names only (AGPL).

**pimalaya core, mail-builder, mail-send, mail-parser.** Nothing for drafts:
core knows drafts only as an IMAP/Maildir folder kind and flag; the others
compose, send over SMTP or parse.

## 3. Design in mailtide

**Approach.** Draft writes are synchronous driver calls through the quota
gate, as `Runner::drift` already calls Gmail outside the engine. The mirror
gains a small **draft index** (draft id → current message id) that the calls'
answers and the sync engine keep true.

### Public API

Rust (`mailtide_io::Runner`); every method that calls Gmail takes a `budget`:

| Method | Returns |
|---|---|
| `create_draft(account, raw, thread_id)` | `DraftRef { draft_id, message_id, thread_id }` |
| `update_draft(account, draft_id, raw, thread_id, if_message_id)` | `DraftRef` |
| `delete_draft(account, draft_id)` | `Deleted` or `AlreadyGone` |
| `send_draft(account, draft_id, if_message_id)` | `SentDraft { message_id, thread_id }` |
| `drafts(account, after, limit)` | `Vec<DraftView>`, from the store |
| `draft(account, draft_id)` | `Option<DraftView>`, from the store |
| `read_draft(account, draft_id, fresh)` | `DraftContent { draft_id, message_id, thread_id, raw }` |

`DraftView { draft_id, message_id, thread_id, tracking_id, rfc822_message_id,
state }`; `state` is `saved`, `stale` (its message was deleted in Gmail and the
refresh has not run yet), `sending` or `send_unconfirmed`. `rfc822_message_id`
is what Gmail's copy says after the last save, read from the import of the
draft's current message (`None` until then): Gmail assigns its own Message-ID
on every save and another on send, so it changes with each.
`DraftError`: `NotFound`, `Conflict { current }`, `TooLarge { limit }`,
`Invalid` (the bytes lack exactly one valid `X-Mailtide-Id`, or carry another
than the draft's), `Deferred { until }`, `ReadOnly`, `Refused` (400/403),
`Retryable`, `OutcomeUnknown`, `SendInProgress { retry_after }`,
`SendUnconfirmed`.

Python `Runner` and `BlockingRunner` get the same seven methods (`raw: bytes`,
keyword options), frozen dataclasses for the four result types, and the
exceptions `DraftNotFound`, `DraftConflict` (`current_message_id`),
`DraftTooLarge` (`limit`), `DraftRefused` (400/403), `DraftRetryable`,
`QuotaDeferred` (`until_ms`), `WriteOutcomeUnknown`, `SendInProgress`
(`retry_after_ms`), `SendUnconfirmed`; `ReadOnlyMode` exists; `Invalid` is
a `ValueError`. `delete_draft` answers `"deleted"` or `"already_gone"`.

- **The bytes carry the identity.** Gmail replaces a client's `Message-ID`
  and `Date` on every save and on send, and keeps an `X-` header byte for
  byte (`drafts-message-id-kept` refuted, `compose-mime-date-kept`). So
  `create_draft` and `update_draft` require exactly one `X-Mailtide-Id:
  <token>` (16 to 64 characters of `A-Za-z0-9_-`, minted by the caller;
  `compose` writes it) and refuse anything else before any call: mailtide
  never adds or repairs it. The row keeps the token (`tracking_id`); it is
  stable for the life of a draft, so an update carrying another token is
  refused. A draft made elsewhere has none until mailtide saves it.

- `if_message_id` is the optimistic check: the message id is Gmail's own
  version of a draft. The call is refused from the index when it already
  shows another message, else checked with `drafts.get?format=minimal`
  before writing.
- `update_draft` always sends a thread id: the caller's, else the index's,
  else one read with `drafts.get`.
- `read_draft` returns the blob when the current message is imported (content
  is immutable per message id: no Gmail call); otherwise, or with `fresh`,
  it asks Gmail. An answer whose message lacks `DRAFT` is `NotFound`.
- Actions still reject drafts (`DraftTarget`). A draft is deleted with
  `delete_draft`, which is permanent in Gmail.

### Gmail calls, quota and size

Costs are in Appendix A: 10 to create, 15 to update, 10 to delete, 100 to
send, 20 or 60 to read, 5 per page to list. An autosave every 5 s is
180 units/min, under 4% of the default account budget. The replaced
messages' import jobs are cancelled by their tombstones, so only the
surviving draft is imported (60 units, after `draft_settle`). Hosts should
debounce saves and pass `if_message_id` when a draft is opened, not on every
keystroke. A gate deferral waits within `budget`, then returns `Deferred`:
nothing sent, nothing recorded.

Writes always use the JSON `raw` field; there is no upload path, now or
later (the repository rule "One size limit, and no uploads"). A message above
`max_message_bytes` (default 35,651,584 bytes, 34 MiB; one setting shared
with `send`, `RunnerConfig.writes`) is refused with `TooLarge { limit }`
before any call: Gmail stores a 40 MiB draft but delivers sends only up to
34 MiB, and a draft that cannot be sent is of no use. At the limit the JSON
body is about 46 MB (base64 grows the bytes by a third) and the call holds
the bytes, their base64 and the body at once, about 140 MB; Gmail took 5 to
7 s for 40 MiB, so the default HTTP request timeout (60 s) covers it, and
the send fence's wait (`RunnerConfig.writes.request_timeout`) is that same
timeout. A caller's `budget` bounds only the quota gate's deferral, not the
request.

### Store

One migration (the next one; numbers are assigned at merge): a table
`mailtide_drafts (account, id, message, thread_id, tracking_id, state,
since)`, unique on `(account, message)` and indexed on `(account,
tracking_id)`, and two columns on
`mailtide_accounts`, `drafts_generation BIGINT NOT NULL DEFAULT 0` and
`drafts_refreshed_at BIGINT` (`NULL`: never listed). Both are columns of
their own, as the label refresh's `labels_refreshed_at` is; nothing is added
to the `sync` column.

`Store::record_draft(account, record, now)` writes the answer of a local
call: upsert or delete the row, bump the generation, append the feed entry,
in one transaction. Like `admit` it is **not fenced by an epoch**: a save
must survive a concurrent sync superseding another. Marking a row `sending`
is a compare-and-set from `saved`, so only one caller, in any process, wins.

**Compatibility.** Besides the table and the two columns, the feature writes
three new `Change` variants to the log (`DraftSaved`, `DraftRemoved`,
`DraftSent`) and `RefreshDrafts` rows to `mailtide_jobs`, none of which a
build from before it can decode. The migration raises `SCHEMA_VERSION`, so
such a build sees the database as `ahead` and refuses to sync
(`SchemaOutOfDate`) before it claims a job or writes a row. Its reads that
check no schema (`changes()`) fail with `CorruptState` on the first draft
entry of a page; they never skip it. Reverting:

```sql
DELETE FROM mailtide_jobs WHERE kind = <RefreshDrafts>;
DELETE FROM mailtide_changes WHERE change LIKE '{"DraftSaved"%'
    OR change LIKE '{"DraftRemoved"%' OR change LIKE '{"DraftSent"%';
DROP TABLE mailtide_drafts;
ALTER TABLE mailtide_accounts DROP COLUMN drafts_generation;      -- SQLite ≥ 3.35
ALTER TABLE mailtide_accounts DROP COLUMN drafts_refreshed_at;
DELETE FROM mailtide_schema WHERE version = <this migration>;
```

Lost: the index (the first refresh after the next upgrade rebuilds it from
`drafts.list`) and the `sending` markers: a send whose answer was lost at
that moment is never reported as `DraftSent`, though its `SENT` message still
arrives as a message. Drafts in Gmail are untouched. Feed consumers keep
their positions: the deleted entries leave gaps in `idx`, and entries are
read in key order.

### Meeting the sync engine

Store-evaluated rules in `mirror.rs`, in the same commit as the message op:

- a newly created `DRAFT` message that no row points at, a tombstone of a
  message a row points at (the row turns `stale`), and the end of a full
  reconciliation each enqueue the account's `RefreshDrafts` job, due after
  `draft_refresh_delay` (default 10 s, deduplicated);
- our own writes trigger nothing: the added message is the one the row
  already names.

The engine also enqueues `RefreshDrafts` **periodically**, at most every
15 minutes while the account is live. The schedule follows the label refresh
`main` has (SPEC §6.6; `LabelRefresh` and `pump_labels` in `engine.rs`,
`label_refresh_due` and `TimerKind::Labels` in `schedule.rs`,
`AccountOp::SetLabelsRefreshedAt`): age is measured from the persisted time
of the last listing (`drafts_refreshed_at`, handed over by the claim), never
from how long an engine has run; a starting engine enqueues the job at once
if that time is 15 minutes old or unset, and otherwise sets a `Drafts` timer
for when it will be; a listing records its time whether or not it changed
anything. It costs 5 units a page and appends no feed entry when nothing
changed. It is the safety net under every unknown about history and listing
consistency.

`RefreshDrafts` pages `drafts.list` and commits one `DraftOp::Reconcile`,
fenced by epoch, job claim and the **generation read at the claim**. If a
local write happened since, or a listed message is one the mirror knows is
deleted (the listing is stale, as §5.6 warns for messages), nothing applies
and the job retries. Otherwise listed drafts are upserted and missing ones
removed.

Feed entries (`object_type` draft):

| Entry | When |
|---|---|
| `DraftSaved { draft_id, message_id, thread_id, origin }` | created or re-pointed; `origin` is `local` or `remote` |
| `DraftRemoved { draft_id, last_message_id, reason }` | `deleted` here, `gone` from Gmail's list, or `send_unconfirmed` |
| `DraftSent { draft_id, message_id, thread_id }` | sent through mailtide, or a lost send confirmed |

Message entries are unchanged: each save still yields `MessageDiscovered`
(`DRAFT`) and `MessageDeleted` for the replaced message, and the sent message
arrives as any `SENT` message does. `DraftSaved` precedes its message's
entries; a host that wants the subject waits for that message's
`MessageImported`. No overlay is added: the host holds the content it just
wrote, and the index records only what Gmail answered.

### Failure and retry

mailtide never retries a draft write itself; Appendix B lists every case.
Create can leave a duplicate draft on retry, never a duplicate send. Update
and delete are safe to retry.

**Send is at most once.** Three fences, so that no single assumption about
Gmail carries it:

1. The row is marked `sending` (the compare-and-set) *before* the call. A
   second `send_draft` for it gets `SendInProgress` and calls nothing.
2. A retry is accepted only after the first request's timeout has passed,
   and it reads the draft first (`drafts.get`, 20 units). A 404 means the
   draft was consumed: `SendUnconfirmed`, no send. Another message id means
   it was edited: back to `saved`, `Conflict`. Only the same draft, still
   there, is sent again.
3. mailtide never sends on its own. A lost answer is settled by Gmail, as I7
   does for intents: importing a `SENT` message whose `X-Mailtide-Id`
   equals the row's tracking id emits `DraftSent` (in whatever thread it
   is); a listing claimed once the attempt's request has timed out that
   still shows the draft returns it to `saved` (the negative proof: it was
   not sent); a draft gone and unmatched once the cursor has moved past the
   attempt and `draft_send_grace` (default 1 h) has passed ends as
   `DraftRemoved{send_unconfirmed}`. No row lives forever. Message-IDs play
   no part: Gmail assigns a new one at send.

A send answered by a 5xx or not answered at all may have been applied, so
its row stays `sending` and the call returns `OutcomeUnknown`; only a
refusal (400, 403), a rate limit or a quota deferral returns it to
`saved`.

Read-only mode refuses the four writes before any call and keeps mirroring
the index (`drafts.list` and `drafts.get` accept `gmail.readonly`). A 400 or
403 from a draft call is the caller's error and does not block the account.

## 4. Alternatives rejected

- **Pure pass-through, no index (Zero):** the host could not tie a mirrored
  `DRAFT` message to a draft id, nor see drafts changed elsewhere.
- **Drafts as §7 intents with delivery jobs:** see D1.
- **A read cache with TTLs (Zero's `MailDraftStore`):** the mirror already
  has the content, and message ids make staleness checkable.
- **`send_draft` with replacement content:** saves 15 units, but the bytes
  sent would not be the ones last saved and reviewed.
- **Writing our own bytes as the blob after a save:** Gmail may not store
  them byte for byte; the mirror keeps only what Gmail returns.
- **Mapping one message to its draft on demand:** Gmail has no lookup by
  message id; `drafts.list` is the only source, at 5 units for 500 drafts.
- **A durable `creating` row for idempotent create:** see D4.

## 5. Tests

Suites were found with `grep -ri draft` over every checkout's test files,
then read. Declared:

- Zero `driver/draft-providers.test.ts`: 3 mapped, 4 `na` (three Outlook
  only, one editor presentation), 2 deferred to `identities` and `send`.
- Zero `mail-draft-store.test.ts`: the fence, invalidation by history and
  account isolation map onto index rules (renamed, since we have no cache);
  3 `na` (TTL, cached pages, row size). The isolation test carries a `note`:
  its second half, a 128-entry cap, has no counterpart.
- Zero `driver/imap.test.ts`: its two draft tests are provider-neutral
  (lifecycle with a stable id; a non-draft is never an editable draft) and
  are ported by behaviour against the fake Gmail. One SMTP test is deferred
  to `send`; the other 13 are listed `na` one by one, all IMAP internals
  (the suite is shared with `send`, so no `rest_na`).
- himalaya `src/gmail/drafts/get.rs`: its one test, as a wire-layer test.

Not declared: Zero `assistant.test.ts` (57 tests of a host approval workflow
over a mocked driver; its stale-update, stale-send and uncertain-save cases
are restated as our own tests), `outgoing-mime.test.ts` (`compose-mime`),
the count and thread-summary suites (read projections), the suites
`scripts/zero_tests.py` owns, and himalaya's `tests/send_outcome.rs` and
JMAP tests (`send`).

Our own 63 tests cover what no reference has: the request shapes and costs;
the index rules (stale rows, stale listings, the generation fence, the
periodic refresh); every row of Appendix B, including a crash between
Gmail's send and its record and the three send fences; drafts changed
elsewhere; two writers; history expiry; read-only mode; a simulator campaign
with the draft invariants; and the Python API.

## 6. Slices

1. **Wire.** `mailtide-gmail` requests and decoders, PUT and DELETE, costs,
   the `GmailRequest` and event variants.
2. **Pass-through.** `create`, `update`, `delete`, `read_draft` straight to
   Gmail: gate, read-only, size limit, error mapping. Already usable the way
   Zero uses drafts.
3. **Index for local writes.** Core rows and rules, `record_draft`,
   migration, `drafts` and `draft`, `DraftSaved` and `DraftRemoved`,
   conformance.
4. **Drafts changed elsewhere.** Mirror triggers, `RefreshDrafts`, the
   fence, the periodic refresh, simulator drafts and invariants.
5. **Send.** `send_draft`, the three fences, `DraftSent`, confirmation and
   the grace period. Run the `drafts-send-*`, `drafts-gone-is-404` and
   `drafts-message-id-kept` checks before it.
6. **Python.** Bindings, stubs, `FakeGmail` drafts, SPEC §4, §7 (its last
   sentence), §8, §10, §11.

**Shared groundwork.** No separate plan owns the pieces several plans need;
each is added by whichever plan lands first, and the later ones use it as it
is:

- PUT and DELETE in `crates/mailtide-gmail/src/http.rs` and
  `crates/mailtide-io/src/transport.rs` (today: GET and POST): added by
  whichever of `drafts`, `identities`, `label-edit`, `purge`, `send` lands
  first.
- the account-level job kind in `crates/mailtide/src/jobs.rs` (a
  `mailtide_jobs` row with an empty message key; today every kind names a
  message): added by whichever of `drafts`, `identities` lands first.
- the unfenced driver-call pattern in `crates/mailtide-io/src/runner.rs`
  (one Gmail request through the quota gate outside the engine, with a
  command id of epoch 0 as `drift` does, and the caller's 400 or 403 kept
  away from the account-level block): added by whichever of `drafts`,
  `identities`, `label-edit`, `purge`, `search` lands first.
- the `QuotaDeferred` Python exception in `crates/mailtide-py/src/lib.rs`
  (a gate deferral that outlasted the call's budget): added by whichever of
  `drafts`, `identities`, `label-edit`, `purge`, `search` lands first.

**Built here.** The account-level job kind: `JobKind::RefreshDrafts` is a
`mailtide_jobs` row with an empty message key, `JobKind::message()` returns
an `Option`, and its claim carries its own payload (the draft generation and
the claim time); `identities` adds its kind beside it. The HTTP request
timeout reaches the driver as `RunnerConfig.drafts.request_timeout`, which a
host sets to its transport's timeout (the Python runner passes its own).

**Landed.** The groundwork is on `main`: `Runner::call_gmail` with `CallError`
and `QuotaDeferred { until }`, the Python `QuotaDeferred` (`until_ms`) and
`WriteOutcomeUnknown` (their Rust-to-Python conversion comes with the first
method that raises them), PUT, PATCH and DELETE, and schema checks on every
store read and write. The account-level job kind is not part of it: the first
of `drafts` and `identities` adds it with its own claim payload. A send retry
waits out the HTTP request timeout, which `Runner` does not expose (it is the
transport's, and `Runner` is generic over the transport); this plan adds it,
e.g. as a `RunnerConfig` field.

## 7. Decisions

- **D1. Draft writes are synchronous quota-gated calls, not §7 intents.** A
  host needs Gmail's draft id back before its next autosave; draft content is
  megabytes of MIME, not the label state that intents and the overlay model;
  a queue would not make `drafts.create` idempotent; both permissive
  references do it this way. SPEC §7's last sentence changes accordingly.
  *Would change it:* a host that must save drafts while Gmail is unreachable.
- **D2. The mirror keeps a draft index.** C2 asks for the id mapping and for
  drafts edited elsewhere; neither exists without it. *Would change it:*
  nothing short of Gmail exposing the draft id on messages or in history.
- **D3. `send_draft` belongs here and sends the stored draft as is.** Its
  safety comes from draft semantics, not from the Message-ID search a
  draftless send needs; `send` can build on it and nothing here depends on
  `send`. *Would change it:* `send` choosing to own every outgoing call.
- **D4. No idempotent create.** A lost create answer can leave a visible
  duplicate draft, never a duplicate send; that does not justify a durable
  `creating` row and a second state machine. *Would reopen it:* duplicate
  drafts reported from real retries, or a host that creates drafts with no
  user watching. The key adoption would need now exists: the
  `X-Mailtide-Id` every create carries.
- **D5. The API takes bytes.** No hard dependency on `compose-mime`, and a
  host may save an `.eml` it did not compose. *Would change it:* a typed
  message type from `compose-mime` that hosts prefer; it would be added
  beside the bytes, not instead.
- **D6. `if_message_id` is a read then a write.** The message id is the
  version; a hash of content (Zero) costs a full read. A small race remains
  unless `drafts-no-precondition` is refuted.
- **D7. An update always sends a thread id**, so neither thread unknown can
  move a reply draft. Costs one `drafts.get` only for a draft the index has
  never seen.
- **D8. Send has three fences** (above) rather than trusting Gmail to refuse
  a second send. Costs 20 units on a retry and one refused call during a
  timeout window. `send-late-apply` (a request whose connection was closed
  unread was still applied within about a second, never later) backs the
  second: once the first request's timeout has passed, reading the draft
  tells whether it was consumed.
- **D9. A periodic refresh, at most every 15 minutes while live.** The index
  heals whatever Gmail's consistency turns out to be, for 5 units. Both
  history unknowns and `drafts-list-consistent` are confirmed; it stays,
  since it also covers drafts changed while history was expired and the
  stale-listing guard costs nothing. *Would change it:* a host with many
  accounts and few drafts (lengthen it).
- **D10. `DraftSaved{remote}` arrives early**, before the content is
  imported; a host that wants the subject waits for `MessageImported`.
  Holding it back would hide a draft for `draft_settle` for no gain.
- **D11. One size limit, in the JSON body.** `max_message_bytes`, 34 MiB,
  shared with `send`; above it a draft is refused before any call.
  `drafts-raw-json-limit` measured the JSON route at 40 MiB and more,
  `drafts-upload-limit` the upload URI at 36,700,160 bytes, and
  `send-size-limit` delivery at 34 MiB, so uploads would buy nothing.
  *Would change it:* Gmail delivering larger sends.
- **D12. A draft is identified by `X-Mailtide-Id`, not its Message-ID.**
  `drafts-message-id-kept` was refuted: Gmail assigns a Message-ID on every
  create, update and send. The token is required on the bytes, stored on
  the row, compared on update, and matched against the `tracking_id` fact
  of imported `SENT` messages. `DraftView.rfc822_message_id` is Gmail's,
  read from the import. *Would change it:* nothing Gmail does today; the
  rule is the repository's (`docs/features/README.md`).
- **D13. The tracking id comes from the import's facts.** `compose-mime`
  owns `mailtide_mime::tracking_id` (the first valid header) and
  `message-facts` the import's `tracking_id` fact (`MessageFacts`, on the
  stored content). The settle rule reads that fact; an import without
  facts (the extractor gave up) settles nothing, and its send ends
  unconfirmed after the grace period, never resent. `tracking_headers`
  beside the reader counts the headers, so a draft carrying two is refused.

Unknowns (`scripts/features.py unknowns drafts` prints each test):

- `drafts-send-twice`: a second send of one draft id is a 404 and sends nothing.
- `drafts-send-concurrent`: concurrent sends of one draft send at most once.
- `drafts-message-id-kept`: refuted. Gmail replaces the `Message-ID` on create, update and send and keeps `X-` headers: confirmation keys on `X-Mailtide-Id` (D12).
- `drafts-raw-json-limit`: confirmed: the JSON route took 40 MiB (D11).
- `drafts-upload-limit`: settled at 36,700,160 bytes; unused (D11).
- `drafts-sent-thread`: the sent message is in the draft's thread.
- `drafts-update-keeps-thread`: an update without `threadId` keeps a reply draft's thread.
- `drafts-update-resend-thread`: an update may send a standalone draft's own thread id.
- `drafts-list-consistent`: confirmed, 50 of 50 rounds; the periodic refresh stays (D9).
- `drafts-history-on-save`: draft saves and deletes appear as `messagesAdded` and `messagesDeleted`.
- `drafts-history-on-send`: a send appears as the draft's message deleted and a `SENT` one added.
- `drafts-gone-is-404`: confirmed, 24 of 24: NotFound and AlreadyGone key on 404.
- `drafts-id-namespace`: a message id is not a draft id.
- `drafts-write-answer-ids`: create and update answers carry the message and thread ids.
- `drafts-no-precondition`: Gmail honours no `If-Match` on draft writes.

## Appendix A. Gmail calls and quota

| Call | Units | Used by | Priority |
|---|---|---|---|
| `drafts.create` | 10 | `create_draft` | foreground |
| `drafts.update` | 15 | `update_draft` | foreground |
| `drafts.delete` | 10 | `delete_draft` | foreground |
| `drafts.send` | 100 | `send_draft` | foreground |
| `drafts.get` minimal | 20 | `if_message_id`, `read_draft(fresh)`, a send retry, an update of an unknown draft | foreground |
| `drafts.get` raw | 60 | `read_draft` without a blob | foreground |
| `drafts.list` | 5 per page of 500 | index refresh, triggered or periodic | live |

## Appendix B. A lost answer, call by call

| Call | Answer lost (timeout, dropped connection) | Retry by the host |
|---|---|---|
| create | `OutcomeUnknown`, no id invented, refresh scheduled; a draft Gmail did create appears as `DraftSaved{remote}` | may leave a second draft: visible and deletable, never a second send |
| update | `OutcomeUnknown`, row marked `stale` | safe, it is a replace; drop or re-read `if_message_id` first |
| delete | `OutcomeUnknown` | safe; a 404 is `AlreadyGone` |
| send | the row stays `sending` | `SendInProgress` until the first request has timed out; then the draft is read: gone is `SendUnconfirmed`, still there is sent |
