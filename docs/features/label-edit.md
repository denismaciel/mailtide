# Label editing: create, rename, delete, colour, visibility and nesting

Plan: `features/label-edit.toml` (roadmap W2). Status: done.

## 1. Goal and scope

Afterwards a host can, per account:

- read each label's **visibility** and **colour** with its id, name and kind
  (SPEC §4 lists them; today only `(id, name, kind)` is stored);
- **create** a user label and get Gmail's id back in the same call, ready to
  apply to messages;
- **rename** it, change its colour or visibility, and **delete** it;
- work with **nested labels**: Gmail nests by `/` in the name, so mailtide
  derives each label's parent, creates under a parent, and renames or deletes
  a label together with its sublabels;
- see labels created, renamed or deleted **elsewhere** in `labels()` and the
  change feed, and never see a mirrored message keep the id of a label that
  no longer exists.

Left out: label counts (`messagesTotal`, `threadsUnread`: a host derives them
from the mirror); theming colours for a host UI; applying labels to messages
(SPEC §7, exists); filters that apply labels (`identities`, settings mirror).

Built on what `main` has: `Store::labels` / `Runner.labels()` returning
`Label { id, name, kind }`, and the label refresh of SPEC §6.6
(`EngineConfig::label_refresh`, 15 minutes by default, and on a label id the
definitions lack; `TimerKind::Labels`; the time of the last listing in
`mailtide_accounts.labels_refreshed_at`; no `LabelDefinitionsChanged` when a
listing changes nothing). This plan adds to that listing and its rule; it
does not replace either.

## 2. What the references do

Gmail facts are from Google's Label reference, the `labels.delete` and
`labels.patch` pages, the labels guide and the quota table, read 2026-11-20
(`[[source]]` entries). The ones the design leans on: a label id is immutable;
deleting a label "removes it from any messages and threads that it's applied
to"; `labels.patch` takes a partial Label; colours are a text and background
pair from a fixed palette; the three label writes cost 5 units and need
`gmail.labels`, `gmail.modify` or the full scope.

**Zero** (MIT). `driver/google.ts` passes label writes straight through and
returns nothing, so a caller lists labels again to find a new id
(`resolveLabelId`). `updateLabel` uses `labels.update` (PUT) with name and
colour, which drops the visibility the label had. Locally a write only
invalidates the cached id-to-name catalogue (`MailCountStore`); message labels
are left to history. *Take:* label definitions are direct calls while label
changes on messages go through the outbox (`MAILBOX_SYNC.md` says so);
explicit visibility on create. *Leave:* the PUT, the void results, the dark
theme colour map (presentation).

**himalaya** (MIT/Apache). `src/gmail/backend.rs` creates under a parent as
`<parent>/<name>`, renames with `labels.patch` sending the name alone, and
renames every sublabel by prefix "as IMAP `RENAME` moves the inferiors",
because Gmail nests by name only. A 409 on create is `mailbox-duplicate`.
Their run against real Gmail (`cairn/spec/testing/gmail-specific.md`)
confirms that a patch keeps colour and `labelListVisibility`. *Take:* patch
only, the sublabel rule, the duplicate code, the name rules of
`src/email/mailbox.rs`. *Change:* they rename the parent first, which a
failure half-way cannot resume; we rename sublabels first.

**jmap-client, Stalwart server.** `Mailbox/set` is the shape we borrow:
create returns the server's id, rename and move are updates of one object,
destroy is refused with `mailboxHasChild` (and `mailboxHasEmail` unless
`onDestroyRemoveEmails`). We keep the first refusal. The second has no Gmail
counterpart: deleting a label never deletes mail. Server paths are names only
(AGPL); neither checkout has a test for this.

**pimalaya core.** Folder add and delete for IMAP, Maildir and Notmuch; no
Gmail API backend. **mail-builder, mail-parser, mail-send.** Nothing.

## 3. Design in mailtide

**Approach.** Label writes are synchronous driver calls through the quota
gate, like `drift` and the planned draft writes. Gmail's answer is recorded
in the mirror at once by an unfenced store call. They are not §7 intents: the
host needs the new id before it can use the label, each write is one small
idempotent call, and there is nothing to overlay (see Alternatives).

Three rules make deletion safe:

1. **A removed definition leaves every message.** When the mirror learns a
   label is gone (our own delete answered 2xx or 404, or a `labels.list`
   lacks a stored id) it strips the id from every message row, drops the
   intents on it, and logs each change, in the same transaction.
2. **A deleted id cannot come back.** The id is remembered as deleted for 30
   days; every later observation, history delta, import and late write
   answer (an `Upsert` racing the delete) is stored without it. Only a
   `labels.list` naming the id again (Gmail's own word) restores it.
3. **Only a whole listing is proof.** A listing must name `INBOX` and every
   system label the definitions hold (a mailbox always lists them), and
   carry no page token and no id twice; an empty answer is a decode error,
   retried. One that falls short changes nothing, and Gmail's own labels
   are never forgotten, whatever a listing or a `Remove` says.

These do not make deletion independent of history in one case: an id the
definitions never held (a label created elsewhere, applied, and deleted
before any listing named it) is not stripped by a listing, since a listing
can only show what it lacks among what was stored. Its rows lose it through
Gmail's own `labelsRemoved` records, which `label-edit-delete-history`
confirmed Gmail writes for every message, or when each message is fetched
again. Stripping ids no listing ever named was rejected: Gmail lists no
definition for some ids messages carry, and such a rule would strip them.

### Public API

Rust (`mailtide_io::Runner`); each write takes a `budget`:

| Method | Returns |
|---|---|
| `create_label(account, NewLabel { name, list_visibility, message_visibility, color })` | `Label` |
| `update_label(account, id, LabelPatch { name, list_visibility, message_visibility, color }, sublabels)` | `LabelUpdate { label, sublabels }` |
| `delete_label(account, id, sublabels)` | `LabelDeletion { deleted, already_gone }` |
| `labels(account)` (exists) | `Vec<Label>`, now with visibility and colour |

`Label { id, name, kind, list_visibility, message_visibility, color }`. The
three new fields are `Option`: Gmail omits them on most system labels, and
rows written before this feature have none until the next refresh.
`LabelColor { text, background }` is Gmail's pair as decoded; a colour the
caller gives is checked against Google's palette
(`mailtide_gmail::labels::palette_color`) and sent lower-cased.
`LabelPatch` fields are optional; `color` is `Keep`, `Set(c)` or `Clear`.
`update_label(.., sublabels)` is `Follow` (default) or `Leave`;
`delete_label(.., sublabels)` is `Refuse` (default) or `Delete`.

`LabelError`: `NotFound`, `NameTaken { existing }`, `Invalid` (blank name,
control character, colour outside the palette, system label),
`HasSublabels { ids }`, `Partial { done, error }`, `Deferred { until }`,
`ReadOnly`, `Refused` (other 400/403), `Retryable`, `OutcomeUnknown`, and
`Run` (the runner's own resources: the store, the quota gate).

Pure helpers in `mailtide::label_names`, for the driver and for hosts that
compose nested names: `validate`, `validate_leaf`, `parent`, `child`,
`renamed`, `sublabel_renames`, `plan_rename`, and `parents(&[Label])`, which gives each label
its parent: the longest other label name that is a prefix ending at a `/`
(`A/B/C` under `A/B`, else under `A`, else top level; `A/Subway` is not under
`A/Sub`). It is derived on read, never stored.

Python `Runner` and `BlockingRunner`: `create_label(account, name, *,
list_visibility="show", message_visibility="show", color=None)`,
`update_label(account, label_id, *, name=None, list_visibility=None,
message_visibility=None, color="keep", sublabels="follow")` (`color=None`
clears it; a string default keeps the signature typed without a sentinel
object), `delete_label(account, label_id, *, sublabels="refuse")`. `Label`
gains `list_visibility` (`"show" | "show_if_unread" | "hide" | None`),
`message_visibility` (`"show" | "hide" | None`), `color: LabelColor | None`
and `parent_id: str | None`. Exceptions: `LabelNotFound`, `LabelNameTaken`
(`existing`), `InvalidLabel`, `LabelHasSublabels` (`ids`), `LabelRefused`
(Gmail's other 400 or 403), plus the shared `QuotaDeferred` (`until_ms`),
`WriteOutcomeUnknown`, `GmailUnavailable` (a 5xx or a rate limit) and
`ReadOnlyMode`. A `Partial` failure raises its cause's exception with
`done`, the ids already changed.

### Gmail calls and quota

| Call | Units | Used by | Priority |
|---|---|---|---|
| `labels.create` | 5 | `create_label` | foreground |
| `labels.patch` | 5 (declared; see Decisions) | `update_label`, once per label renamed | foreground |
| `labels.delete` | 5 | `delete_label`, once per label deleted | foreground |
| `labels.list` | 1 | before a rename that moves sublabels and before every delete; after a 409 | foreground |

Renaming a label with n sublabels costs 1 + 5(n + 1) units. A deferral waits
within `budget`, then returns `Deferred`: nothing sent, nothing recorded.

- **Create** always sends both visibilities. On 409 (Gmail's `Label name
  exists or conflicts`, also for a name differing only in case or by a
  trailing space) it lists labels once and returns `NameTaken` with the
  label that holds the name, so a host retrying after a lost answer can
  adopt it.
- **Update** sends only the fields that change, through `labels.patch`; a
  colour is always sent as the pair, and `Clear` sends `color: null`, which
  Gmail answers by dropping the colour and keeping the rest
  (`label-edit-color-rules`). Colours are sent lower-cased: Gmail matches
  palette values case-sensitively.
- **Rename with sublabels:** list labels, compute `<old>/x → <new>/x`, refuse
  with `NameTaken` if any target is held by another label (compared as Gmail
  does: case-insensitively, surrounding space ignored; a label may take a
  case variant of its own name), then patch the sublabels and the label
  last. Gmail's API renames only the label it is asked to
  (`label-edit-rename-parent`), so the sublabels are mailtide's to move. A
  failure returns `Partial`; the same call again finds the rest still under
  the old prefix and finishes. A 409 to a patch is `NameTaken` too. A new
  name under the label's own (`A` to `A/x`) is refused as `Invalid` before
  anything is sent: its sublabels would move into its own subtree, and a
  retry after a failure half-way would move the moved ones again.
- **Delete:** one of Gmail's fixed system ids, or a system label of the
  mirror's definitions, is refused before any call. Then the labels are
  listed once (a listing that falls short is a `Retryable` failure, never
  proof): a system label it names is refused; a label it lacks is
  `already_gone` if the mirror defined it (one listing is proof,
  `label-edit-list-consistency`) and is recorded as a removal without a
  write, and `NotFound` if neither knows it (nothing is remembered for an
  id nobody had). With sublabels and `Refuse`,
  `HasSublabels`; with `Delete`, deepest first, the label last. Gmail's
  `labels.delete` removes only the label it names
  (`label-edit-delete-parent`). A 404 is `already_gone` too.

### Store

One migration (the next one; numbers are assigned at merge):

```sql
ALTER TABLE mailtide_labels ADD COLUMN attrs TEXT;            -- JSON: both visibilities and the colour; NULL reads as unknown
ALTER TABLE mailtide_accounts ADD COLUMN label_generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE mailtide_accounts ADD COLUMN deleted_labels TEXT; -- JSON: id → deleted_at; NULL: none
```

`attrs` is needed because `mailtide_labels` has only `id`, `name` and `kind`,
none of them a place for structured data. The **label generation** (a
counter) and the **deleted-label memory** (`id → deleted_at`, pruned after 30
days at an applied listing) are account facts and get columns of their own on
the account row, as the label refresh's `labels_refreshed_at` did. Nothing is
added to the `sync` column: its stored format stays exactly what earlier
builds write. The account row is loaded by every commit anyway, so the filter
costs no query.

**Compatibility.** The migration raises `SCHEMA_VERSION`, so a build from
before it sees the database as `ahead` and refuses to sync
(`SchemaOutOfDate`): it never writes a label row without `attrs`, applies a
listing past the generation fence, or stores a remembered-deleted id again.
The feature writes no new `Change` variant and no new job kind, so the log
and the job table stay readable by any build. Reverting it:

```sql
ALTER TABLE mailtide_labels DROP COLUMN attrs;                -- SQLite ≥ 3.35
ALTER TABLE mailtide_accounts DROP COLUMN label_generation;
ALTER TABLE mailtide_accounts DROP COLUMN deleted_labels;
DELETE FROM mailtide_schema WHERE version = <this migration>;
```

Lost: colours and visibility (the first listing after the next upgrade
refills them) and the deleted-label memory. Without the memory a replayed
history record or a late import can put a deleted id back on a row, where it
stays until that message is fetched again: what a build without this feature
does anyway.

`Store::record_label(account, &LabelOp)` (an `Upsert(label)` or a
`Remove { id, at }`) writes the answer of a local call: definition,
generation bump, strip and feed entries in one transaction. Like `admit` it is **not fenced by an epoch**: a label
write must survive a concurrent sync superseding another.
`Store::label_generation(account)` reads the counter.

In SQL a removal finds candidate rows by a text match on the `labels` column
and confirms each in Rust (`_` is a wildcard in `LIKE`, and `Label_1` is a
prefix of `Label_10`): one statement for every label a commit removes, not
one per label. It rewrites each row that carried the label and logs one
feed entry per row, both in multi-row statements, in one transaction under
the account lock. Measured with `crates/mailtide-store/examples/label_removal.rs`
on 50,000 rows carrying the label (release build, Apple-silicon builder):

| Store | Round trip | `record_label(Remove)`, 50,000 rows | A listing removing 4 labels, 25,000 rows |
|---|---|---|---|
| `SQLite` (memory) | 0.04 ms | 1.6 s | 0.8 s |
| Postgres over a 35-40 ms link | 40 ms | 11.7 s | 5.8 s |

About 330 statements make up a 50,000-row removal (one candidate scan,
rows loaded 2,000 ids at a time, rows and feed entries written 500 a
statement), so on Postgres it is round trips: at a host's usual sub-ms
link it is about the `SQLite` figure. Written one row and one entry per
statement, as the first version was, it took 1.5 s on `SQLite` and some
100,000 round trips on Postgres (over an hour on that link; the run was
stopped). The removal runs inside the host's `delete_label` call after
Gmail answered: the call's `budget` bounds the Gmail requests only, so a
removal from a very large label can outlast it. A listing that removes
several labels scans for all of them in one statement.

### Meeting the sync engine

Rules in `mirror.rs`, shared by every store:

- `LabelOp::Upsert` and `LabelOp::Remove { id, at }` apply local writes.
  `LabelOp::ReplaceAll` applies a listing. On `main` it already replaces the
  definitions only when the listing differs from them and logs
  `LabelDefinitionsChanged` only then; here it becomes
  `ReplaceAll { labels, generation, at }`, and a label the listing lacks is
  removed by the same rule as `Remove`, messages included. `at` is when the
  listing was made (a commit carries no clock): a removal is remembered
  from then, and the deleted memory is pruned against it.
- **Generation fence.** The driver reads the generation before sending
  `labels.list` and reports it with `LabelsListed`; the engine copies it into
  `ReplaceAll`, for reconciliation's listing and for the §6.6 refresh alike;
  the store skips the op if a local write happened since. A stale listing can
  therefore not remove a label just created here, nor restore one just
  deleted. The next refresh (at most `label_refresh` later) applies. When
  reconciliation lists the labels, a refresh still in flight is dropped, so
  two listings answered out of order cannot apply the older last.
- **Removal** strips the id from trusted and unverified rows alike without
  moving a checkpoint (if Gmail also sends `labelsRemoved`, those records
  then change nothing), and drops pending, delivered and blocked intents on
  the label, as a message's deletion drops its intents.
- **Filter.** `observe`, `patch_labels`, `mark_imported` and
  `finish_reconcile` drop remembered-deleted ids from what they store. To the
  §6.6 refresh such an id is one the definitions lack: it asks for the usual
  single, throttled listing, which either names the id again (restored) or
  does not (nothing more is asked in that engine).
- **Rename** touches no message: rows hold ids (SPEC §6.6).

Feed entries are the existing ones: `LabelDefinitionsChanged` when the
definitions differ; `LabelsChanged { removed }` per message a removal
touches; `OverlayChanged` where an intent was dropped. A consumer that
replays the feed needs no new rule.

One engine change: **a delivery refused for an added label waits for a
listing, then goes again.** Gmail refuses a `messages.modify` (and a
`batchModify`) whose `addLabelIds` names a deleted or never-created label
with 400 `labelId not found`, and refuses it whole: a valid label in the
same request is not applied either (`label-edit-modify-deleted-label`).
A delivery carries all of one message's pending changes, so blocking the
group would block a star, an archive or a valid user label with the dead
one. So the first time a delivery whose group adds a label that is not one
of Gmail's fixed system labels is refused for good, nothing is blocked: the
engine keeps the job's claim, lists the labels at once, and when that
listing has been applied (it removes a deleted label and the intents on
it) or has failed, finishes the job, so the store enqueues what is still
pending and it goes again. Refused a second time, the adds that may be
gone are blocked and the rest goes alone; refused again, everything left
blocks, so nothing loops. A run that ends while a delivery waits leaves its
claim to the next engine, which delivers it afresh. In read-only mode,
where the runner refuses every modify without sending it, a group with
such adds takes one listing and three rounds to block. Removing a deleted
label is not refused at all: Gmail answers 200 and changes nothing, so
such an intent is delivered and settles on its confirmation fetch.

The approved plan also verified a 404 answering a delivery before
tombstoning the message, in case Gmail answered 404 for a deleted label.
Gmail never did (it answers 400 for an add and 200 for a remove, and
`messages.get` found the message afterwards), so the change was dropped: a
404 to a modify names the message, the same evidence as a `get`'s 404,
which tombstones already, and verifying it would have added a delivery and
a verification racing each other for no case Gmail produces.

### Failure and retry

mailtide never repeats a label write on its own.

| Call | Answer lost | Retry by the host |
|---|---|---|
| create | `OutcomeUnknown`; a label Gmail did create appears at the next refresh | `NameTaken { existing }`: adopt it |
| update | `OutcomeUnknown` | safe: a patch sets absolute values |
| rename with sublabels | `Partial` or `OutcomeUnknown` | safe: finishes what is left |
| delete | `OutcomeUnknown` | safe: a 404 is `already_gone` |

A crash between Gmail's answer and `record_label` loses nothing: the next
listing brings the same change. A 400 or 403 from a label call is the
caller's error and never blocks the account (the driver does not pass these
answers to the account-level classification). Read-only mode refuses the
three writes before any call and still mirrors definitions with their new
fields (`labels.list` works with `gmail.readonly`).

## 4. Alternatives rejected

- **Label writes as §7 intents with delivery jobs:** the id does not exist
  until Gmail answers, so "create, then apply" would need temporary ids
  rewritten through intents; nothing gains from queueing a 5-unit call.
- **No local record, only trigger a refresh:** one writer of definitions and
  no fence, but `labels()` would lag the call that just succeeded.
- **Leave deleted labels on messages to history:** correct only if Gmail
  emits `labelsRemoved` for every message, which is not documented.
- **Verify every message that carried a deleted label:** 20 units per
  message, an hour of an account's budget for a large label, to learn what
  `labels.delete`'s documentation already states.
- **`labels.update` (PUT):** clears whatever the request omits (Zero's
  `updateLabel` loses visibility this way).
- **A `parent_id` column:** Gmail has no such field; it would be a second
  source of truth for something the names already say.
- **Rename the parent first (himalaya):** a failure leaves sublabels under a
  prefix that a retry can no longer find.
- **One `LabelRemoved` feed entry instead of one per message:** smaller, but
  every consumer would need a new rule and silently keep stale labels
  without it.
- **`kind = 'deleted'` rows as the deleted memory:** an extra query on every
  commit; the account row is loaded anyway.

## 5. Tests

Suites were found with `grep -ri` for label, mailbox, folder, rename and
colour over every checkout's test files, then read.

- himalaya `src/gmail/labels/{update,get,list}.rs`: one test each, as wire
  tests (the two "prints as Gmail sent it" tests become decoding tests).
- himalaya `src/gmail/backend.rs`: the sublabel rename (pure) and the
  duplicate create, which `search` deferred here. `search`, `send`,
  `message-facts`, `purge` and `delivery-retries` account for the other
  fifteen.
- himalaya `src/gmail/client.rs`: the missing label; 2 `na` (generic error
  classification, existing).
- himalaya `src/email/mailbox.rs`: three name and path rules; 3 `na` (name
  and role resolution, which mailtide does not do).
- himalaya `tests/mailbox.rs`: the create, rename under parent, delete
  lifecycle, ported by behaviour from a Maildir to the fake Gmail.
- himalaya `src/shared/mailbox/create.rs`: 1 `na`, kept because it shows the
  cost of the alternative: a queued create has no id.
- Zero `imap/sync.test.ts`: "forgets folders that disappear from LIST" is
  the provider-neutral form of a label deleted elsewhere; `rest_na` for five
  IMAP tests.
- Zero `mail-count-store.test.ts`: its two label-catalogue tests, which
  `threads` (the suite's other declarer) leaves to this plan. Both describe
  behaviour `main` already has, so they map onto tests that exist and report
  `implemented`: "uses updated label names and removes deleted labels" onto
  the conformance case `label_definitions` (a later listing renames, a label
  it lacks goes), and "refreshes label names after invalidation and backs
  off failed catalogue refreshes" onto
  `a_failed_refresh_neither_blocks_sync_nor_moves_the_cursor` in
  `crates/mailtide/tests/label_refresh.rs`, with
  `test_a_live_account_refreshes_renamed_labels_after_the_interval`
  (`tests/test_labels.py`) covering its first half. The count cache around
  them has no counterpart.

Not declared: Zero `mail-label-store.test.ts` and `mail-label-policy.test.ts`
(owned by `scripts/zero_tests.py`); Zero `driver/imap.test.ts` (declared by
`drafts`, which marks its label tests IMAP-only); himalaya's IMAP, Maildir,
Graph and JMAP mailbox tests (other providers; the Gmail suite has the same
cases); jmap-client and the Stalwart server (no tests for this).

Our own 62 tests add what no reference has: request shapes and the palette;
the removal, filter and fence rules; the store cases (including `Label_1`
versus `Label_10`); lost answers and resumable sublabel renames; labels
changed elsewhere **with and without** history records; a delivery refused
for an added label, held for a listing and retried once; empty and
truncated listings at every layer (decode, mirror, every store, driver,
simulator); late answers racing a delete; renames into a label's own
subtree; ids nobody had; each safeguard a reviewer found unpinned (the
generation read before the listing, the reconcile scope filter, the
tracked check, the listing's system-label refusal); a simulator campaign
with month-long gaps and fenced listings counted; the Python API.

## 6. Slices

1. **Attributes.** `Label` fields, decoding, the migration, `labels()` and
   Python `Label`. Useful alone: SPEC §4 becomes true.
2. **Safe removal.** `ReplaceAll` removals reach messages, strip,
   deleted-label memory, filter, dropped intents, a delivery refused for
   an added label; simulator labels and invariants. No public API yet; fixes labels deleted
   elsewhere.
3. **Writes.** Requests, PATCH and DELETE, costs, `record_label`, the
   generation fence, `create_label`, `update_label`, `delete_label` for one
   label.
4. **Nesting.** `label_names`, `parents`, sublabel rename and delete.
5. **Python and docs.** Bindings, stubs, `FakeGmail` labels, SPEC.

**Shared groundwork.** No separate plan owns the pieces several plans need;
each is added by whichever plan lands first, and the later ones use it as it
is:

- PATCH and DELETE in `crates/mailtide-gmail/src/http.rs` and
  `crates/mailtide-io/src/transport.rs` (today: GET and POST): added by
  whichever of `drafts`, `identities`, `label-edit`, `purge`, `send` lands
  first.
- the unfenced driver-call pattern in `crates/mailtide-io/src/runner.rs`
  (one Gmail request through the quota gate outside the engine, with a
  command id of epoch 0 as `drift` does, and the caller's 400 or 403 kept
  away from the account-level block): added by whichever of `drafts`,
  `identities`, `label-edit`, `purge`, `search` lands first.
- the `QuotaDeferred` Python exception in `crates/mailtide-py/src/lib.rs`
  (a gate deferral that outlasted the call's budget): added by whichever of
  `drafts`, `identities`, `label-edit`, `purge`, `search` lands first.

**Landed.** The groundwork is on `main`: `Runner::call_gmail` with `CallError`
and `QuotaDeferred { until }`, the Python `QuotaDeferred` (`until_ms`) and
`WriteOutcomeUnknown` (their Rust-to-Python conversion comes with the first
method that raises them), PUT, PATCH and DELETE, and schema checks on every
store read and write. A lost answer arrives as `CallError::OutcomeUnknown`.
Ruling shared with `drafts` and `purge` (from `purge`'s review): the public
`Runner::call_gmail` only reads, refusing any request that changes the
mailbox before any token or quota, so a Rust host cannot send a label write
past this feature's checks; the label writes go through the crate's
`pub(crate) direct_call`, the same path. Whichever of the three lands first
makes the change; this branch makes it, with
`the_public_call_refuses_every_write_before_any_token_or_quota`.

## 7. Decisions

1. **Direct calls, not intents.** The id comes from Gmail and every write is
   small and idempotent. Would change if hosts needed label writes queued
   while offline.
2. **Strip on proof and remember the id, whatever history does.** A message
   keeping a dead id forever is the failure to avoid.
   `label-edit-delete-history` came back confirmed (one `labelsRemoved`
   record per message, at once), so while history is available the records
   agree with the strip and change nothing more. The strip is still needed
   when history is not: a cursor that expires across a deletion skips those
   records, and reconciliation verifies rows but would keep a dead id on a
   row a listing never names again. Would simplify to "history only" if
   replayed history were shown never to carry a dead id.
3. **One whole listing without the id is proof of deletion** (fenced by
   the generation). A listing that is empty, paged, has an id twice, or
   lacks `INBOX` or a stored system label is not whole and proves nothing.
   A listing cut short that still names every system label is
   indistinguishable from deletions and is trusted: Gmail answered every
   listing whole in `label-edit-list-consistency` (always one page, never
   missing a label). Two listings a refresh apart would cover that case at
   the cost of a refresh interval; they would change if Gmail were ever
   seen to answer short.
4. **Per-message `LabelsChanged` entries for a removal**, up to one commit
   with as many entries as the label had messages. Would change to a single
   entry if `feed-pages` is not built and such commits prove too large.
5. **`labels.patch` is declared at 5 units.** Google's table omits it; 5 is
   `labels.update`'s cost. Not testable through the API, only in the Cloud
   console's quota metrics.
6. **A rename moves sublabels by default; a delete refuses while sublabels
   exist.** The first matches Gmail's web interface and IMAP; the second is
   JMAP's `mailboxHasChild`. Both have an explicit other value.
7. **`NameTaken` returns the existing label instead of adopting it.** The
   call cannot know whether its own earlier attempt created it.
8. **Names are validated lightly** (not blank, no control character). Length
   and reserved names are left to Gmail's 400, reported as `Refused`.
9. **Colours are Gmail's palette values, unmapped.** Theming is the host's.
10. **The deleted memory lasts 30 days**: longer than Gmail keeps history,
    so nothing replayed can carry the id after that. Kept although history
    does record a deletion (`label-edit-delete-history`): a fetch or an
    import in flight across the deletion, or a history page read from a
    cursor behind it, still names the id, and without the memory the row
    would carry it until the matching `labelsRemoved` record arrives, or
    for good if history expired in between. Its cost is a JSON column read
    with the account row every commit loads anyway.
11. **A delivery refused for an added label waits for a listing once**
    (`label-edit-modify-deleted-label`, refuted in one half): Gmail refuses
    the whole modify with 400 without naming the label, so blocking at once
    would block a message's unrelated changes, valid user labels included.
    The listing tells a deleted label from a live one; a second refusal
    means the label is not the cause and blocks the doubtful adds, then the
    rest. The planned "verify a delivery's 404" was
    dropped: Gmail never answers 404 for a label, and a 404 for the message
    is the same proof a `get`'s 404 is.

Unknowns (`features/label-edit.toml`, each with its test and result):

- `label-edit-delete-history`: confirmed. One `labelsRemoved` record per
  message, readable at once; the design is correct either way.
- `label-edit-list-consistency`: confirmed. One listing sent after a write
  returned always reflects it; one page. One listing is proof (Decision 3).
- `label-edit-duplicate-name`: confirmed. 409 `ABORTED` `Label name exists
  or conflicts` for create, patch and update, also for another case or a
  trailing space; a label may take a case variant of its own name.
- `label-edit-rename-parent`: confirmed. The API renames one label;
  sublabels keep their names, so mailtide moves them.
- `label-edit-delete-parent`: confirmed. Only the named label goes;
  sublabels and their uses stay.
- `label-edit-color-rules`: confirmed, with one more rule: palette values
  are matched case-sensitively (`#FFFFFF` is a 400), so mailtide sends them
  lower-cased; `color: null` clears and keeps the rest.
- `label-edit-modify-deleted-label`: refuted in one half. An add of a
  deleted or unknown label is a 400 (`labelId not found`) that refuses the
  whole request; a remove of one is a 200 that changes nothing; nothing
  answers 404. Decision 11 follows from it.
