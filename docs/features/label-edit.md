# Label editing: create, rename, delete, colour, visibility and nesting

Plan: `features/label-edit.toml` (roadmap W2). Status: planned.

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

Two rules make deletion safe whatever Gmail's history does:

1. **A removed definition leaves every message.** When the mirror learns a
   label is gone (our own delete answered 2xx or 404, or a `labels.list`
   lacks a stored id) it strips the id from every message row, drops the
   intents on it, and logs each change, in the same transaction.
2. **A deleted id cannot come back.** The id is remembered as deleted for 30
   days; every later observation, history delta and import is stored without
   it. Only a `labels.list` naming the id again (Gmail's own word) restores it.

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
`LabelColor { text, background }` is built only from Google's palette.
`LabelPatch` fields are optional; `color` is `Keep`, `Set(c)` or `Clear`.
`update_label(.., sublabels)` is `Follow` (default) or `Leave`;
`delete_label(.., sublabels)` is `Refuse` (default) or `Delete`.

`LabelError`: `NotFound`, `NameTaken { existing }`, `Invalid` (blank name,
control character, colour outside the palette, system label),
`HasSublabels { ids }`, `Partial { done, error }`, `Deferred { until }`,
`ReadOnly`, `Refused` (other 400/403), `Retryable`, `OutcomeUnknown`.

Pure helpers in `mailtide::label_names`: `validate`, `parent`, `leaf`,
`child`, `sublabel_renames`, and `parents(&[Label])`, which gives each label
its parent: the longest other label name that is a prefix ending at a `/`
(`A/B/C` under `A/B`, else under `A`, else top level; `A/Subway` is not under
`A/Sub`). It is derived on read, never stored.

Python `Runner` and `BlockingRunner`: `create_label(account, name, *,
list_visibility="show", message_visibility="show", color=None)`,
`update_label(account, label_id, *, name=None, list_visibility=None,
message_visibility=None, color=KEEP, sublabels="follow")`,
`delete_label(account, label_id, *, sublabels="refuse")`. `Label` gains
`list_visibility` (`"show" | "show_if_unread" | "hide" | None`),
`message_visibility` (`"show" | "hide" | None`), `color: LabelColor | None`
and `parent_id: str | None`. Exceptions: `LabelNotFound`, `LabelNameTaken`,
`InvalidLabel`, `LabelHasSublabels`, plus the shared `QuotaDeferred` (`until_ms`),
`WriteOutcomeUnknown` and `ReadOnlyMode`.

### Gmail calls and quota

| Call | Units | Used by | Priority |
|---|---|---|---|
| `labels.create` | 5 | `create_label` | foreground |
| `labels.patch` | 5 (declared; see Decisions) | `update_label`, once per label renamed | foreground |
| `labels.delete` | 5 | `delete_label`, once per label deleted | foreground |
| `labels.list` | 1 | before a write that touches sublabels; after a 409 | foreground |

Renaming a label with n sublabels costs 1 + 5(n + 1) units. A deferral waits
within `budget`, then returns `Deferred`: nothing sent, nothing recorded.

- **Create** always sends both visibilities. On 409 it lists labels once and
  returns `NameTaken` with the label that holds the name, so a host retrying
  after a lost answer can adopt it.
- **Update** sends only the fields that change, through `labels.patch`.
- **Rename with sublabels:** list labels, compute `<old>/x → <new>/x`, refuse
  with `NameTaken` if any target is held by another label, then patch the
  sublabels and the label last. A failure returns `Partial`; the same call
  again finds the rest still under the old prefix and finishes.
- **Delete:** system labels are refused from the definitions. With sublabels
  and `Refuse`, `HasSublabels`; with `Delete`, deepest first, the label last.
  A 404 is `already_gone` and is recorded as a removal.

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

`Store::record_label(account, Upsert(label) | Remove(id), now)` writes the
answer of a local call: definition, generation bump, strip and feed entries
in one transaction. Like `admit` it is **not fenced by an epoch**: a label
write must survive a concurrent sync superseding another.
`Store::label_generation(account)` reads the counter.

In SQL a removal finds candidate rows by a text match on the `labels` column
and confirms each in Rust (`_` is a wildcard in `LIKE`, and `Label_1` is a
prefix of `Label_10`). It is one transaction, of the size of a
reconciliation's last commit.

### Meeting the sync engine

Rules in `mirror.rs`, shared by every store:

- `LabelOp::Upsert` and `LabelOp::Remove` apply local writes.
  `LabelOp::ReplaceAll` applies a listing. On `main` it already replaces the
  definitions only when the listing differs from them and logs
  `LabelDefinitionsChanged` only then; here it becomes
  `ReplaceAll { labels, generation }`, and a label the listing lacks is
  removed by the same rule as `Remove`, messages included.
- **Generation fence.** The driver reads the generation before sending
  `labels.list` and reports it with `LabelsListed`; the engine copies it into
  `ReplaceAll`, for reconciliation's listing and for the §6.6 refresh alike;
  the store skips the op if a local write happened since. A stale listing can
  therefore not remove a label just created here, nor restore one just
  deleted. The next refresh (at most `label_refresh` later) applies.
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

One engine change: **a 404 answering a delivery is verified before the
message is tombstoned.** Today `messages.modify` → 404 tombstones the
message. If Gmail ever answers 404 because a *label* in the request is gone,
that would delete a live message from the mirror for good. The delivery now
enqueues a `Verify`; only that fetch's own 404 tombstones.

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

Our own 46 tests add what no reference has: request shapes and the palette;
the removal, filter and fence rules; the store cases (including `Label_1`
versus `Label_10`); lost answers and resumable sublabel renames; labels
changed elsewhere **with and without** history records; the verified 404; a
simulator campaign; the Python API.

## 6. Slices

1. **Attributes.** `Label` fields, decoding, the migration, `labels()` and
   Python `Label`. Useful alone: SPEC §4 becomes true.
2. **Safe removal.** `ReplaceAll` removals reach messages, strip,
   deleted-label memory, filter, dropped intents, the verified 404;
   simulator labels and invariants. No public API yet; fixes labels deleted
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

## 7. Decisions

1. **Direct calls, not intents.** The id comes from Gmail and every write is
   small and idempotent. Would change if hosts needed label writes queued
   while offline.
2. **Strip on proof and remember the id, whatever history does.** A message
   keeping a dead id forever is the failure to avoid, and Gmail's history
   behaviour is unproven. Would simplify to "history only" if
   `label-edit-delete-history` is confirmed *and* replayed history is shown
   never to carry a dead id.
3. **One listing without the id is proof of deletion** (fenced by the
   generation). Two listings would be safer against a Gmail inconsistency
   and slower; see `label-edit-list-consistency`.
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
    so nothing replayed can carry the id after that.
11. **The delivery 404 change is part of this plan**, not a separate fix:
    label deletion is what makes the case reachable.

Unknowns (`features/label-edit.toml`, each with its test):

- `label-edit-delete-history`: does deleting a label emit `labelsRemoved`
  per message? Assumed yes; the design is correct either way.
- `label-edit-list-consistency`: is `labels.list` never stale or partial?
- `label-edit-duplicate-name`: 409 on a taken name, for create and patch;
  case rules.
- `label-edit-rename-parent`: does the API rename sublabels itself?
- `label-edit-delete-parent`: does the API delete sublabels itself?
- `label-edit-color-rules`: any palette pair accepted; `color: null` clears.
- `label-edit-modify-deleted-label`: 400 or 404 for a modify naming a
  deleted label.
