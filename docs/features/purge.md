# Purge: permanent delete, empty trash and empty spam

Plan: `features/purge.toml` (roadmap W1). Status: built.

## 1. Goal and scope

Afterwards a host that has switched purge on can, per account:

- **permanently delete** named messages, or the messages of a thread;
- **empty trash** or **empty spam**: delete for good what the mirror holds
  there;
- learn per message what happened: purged, refused (with a reason), sent but
  not yet confirmed, or not sent;
- see each purged message in the change feed as `MessageDeleted`, the same
  tombstone as a deletion made anywhere else.

Existing actions (SPEC §7) trash, restore and mark spam but never destroy
mail. Purge is the only operation in mailtide that cannot be undone, so the
design is built around four safeguards:

1. it is **off by default** and refused in read-only mode;
2. by default it only touches messages **in trash or spam**, judged by
   Gmail's state up to the moment of the call, not by a mirror that may lag;
3. it deletes **by message id only**, and mailtide **never repeats** a delete;
4. a message becomes a tombstone only on **Gmail's own evidence**.

Left out: deleting drafts (`drafts`: `delete_draft`); the reversible bulk
"move all spam to trash" (already possible with `MoveToTrash` actions);
deleting an account's mirror (`forget`, exists); removing a purged message's
blob before the usual 24 h grace period.

## 2. What the references do

Gmail facts are from Google's `messages.delete`, `messages.batchDelete` and
`history.list` pages and the quota table, read 2026-11-20 (`[[source]]`
entries): `messages.delete` "immediately and permanently deletes the
specified message"; `batchDelete` "provides no guarantees that messages were
not already deleted or even existed at all" and answers with an empty body;
**both list `https://mail.google.com/` as their only scope**, so
`gmail.modify` is not enough; they cost 10 and 50 units.

**Zero** (MIT). `delete(id)` is a bare `messages.delete` with no check.
Everything in bulk is a *move*: `bulkDelete` adds `TRASH`, and
`deleteAllSpam` enumerates spam durably and trashes it through the label
outbox so that "newer user restores" win (`MAILBOX_SYNC.md`; its suite is
owned by `scripts/zero_tests.py` and already ported). Zero has no test of a
permanent delete. *Take:* a cleanup never overrides a restore made after it
was listed. *Leave:* the unchecked single delete.

**himalaya** (MIT/Apache). `message delete` "trashes first" and removes for
good only what is already in the trash. Its Gmail backend first reads each id
(`messages.get`, minimal) and keeps those "carrying the mailbox label", then
sends `messages.delete` per id in an HTTP batch; unknown ids (404, or 400
`Invalid id value`) are reported and "the others written". Their run against
real Gmail deleted 133 messages with one `batchDelete`. *Take:* trash-first
as the default policy, the look-up before the delete, per-id reporting.
*Change:* the look-up costs 20 units a message there; we have a mirror and
check it against history for 2 units a page.

**jmap-client, Stalwart server.** `Email/set destroy` is synchronous and
answers `destroyed` and `notDestroyed` per id: the shape of `PurgeOutcome`.
himalaya's JMAP backend adds the scoping rule (an id the mailbox does not
hold is not found). Server paths are names only (AGPL).

**pimalaya core.** Folder purge and expunge for IMAP and Maildir only.
**mail-builder, mail-parser, mail-send.** Nothing.

## 3. Design in mailtide

**Approach.** A purge is a synchronous driver call, not a §7 intent. It
happens while the caller waits, or not at all: **nothing destructive is ever
queued**, so no delete can fire hours later when an account is resumed.

### Public API

Rust (`mailtide_io::Runner`):

| Method | Returns |
|---|---|
| `purge(account, PurgeTarget::Messages(ids) \| Thread(id), anywhere, budget)` | `PurgeOutcome { purged, sent, refused, unsent }` |
| `empty(account, Bin::Trash \| Bin::Spam, budget)` | `EmptyOutcome { purged, sent, skipped, remaining }` (counts) |

- `purged`: Gmail's history shows the deletion; the tombstone is written.
- `sent`: the delete was sent, history has not shown it yet; the row stays
  until the next sync sees it.
- `refused`: `(id, reason)`, nothing sent for that id. Reasons:
  `not_in_mirror`, `draft`, `unverified` (labels awaiting verification),
  `not_in_trash_or_spam`, `gmail_refused`.
- `unsent`: the budget or a quota deferral ended the call first.

`PurgeError` (nothing was sent): `ReadOnly`, `Disabled`,
`AnywhereNotAllowed`, `TooMany { limit, given }`, `MirrorStale(Stale)`
(`NoCursor`, `NotLive`, `HistoryExpired`, `TooFarBehind`), `ScopeRequired`,
`Deferred { until }`, `Retryable`; and, as `search` has them, `Refused` (the
check's history read refused for good), `AccountBlocked`, `NotTracked` and
`Run` (the store or the gate failed).

Config: `purge = off | trash_and_spam | anywhere`, default `off`. The config
is the deployment's ceiling; `anywhere = true` on a call is the caller's
explicit request and needs the `anywhere` ceiling. In Rust it is
`Runner::with_purge(PurgePolicy)` rather than a `RunnerConfig` field, so
every existing construction of a runner stays off without a change.

Python: `await runner.purge(account, message_ids=… | thread_id=…,
anywhere=False, budget_s=…) -> PurgeOutcome`, `await runner.empty(account,
"trash" | "spam", budget_s=…) -> EmptyOutcome`, `Config(purge="off")`, and
the exceptions `PurgeDisabled`, `PurgeScopeRequired`, `MirrorStale`,
`TooManyMessages`; `ReadOnlyMode` and `QuotaDeferred` (`until_ms`) are shared.

### What a call does

1. **Gate.** Read-only mode, the policy, and at most 1,000 ids for an
   explicit purge. A thread target expands to the mirror's non-draft
   messages of that thread; `threads.delete` is never used, because it would
   also destroy a reply the mirror has not seen.
2. **Read the mirror.** The targets' views and the account's status. No
   cursor, or an account that is not `live`: `MirrorStale`.
3. **Catch up privately.** `history.list` from the mirror's cursor to the
   head, read by the driver and committed nowhere. A 404, or more than
   `PURGE_HISTORY_PAGES` (20) pages: `MirrorStale` ("run the account
   first"). The answer's history id is kept as `H`.
4. **Decide per message** (pure, `mailtide::purge`): fold the history
   records after the row's checkpoint into its observed labels, with the
   same rule the mirror uses, then overlay pending intents. The message is
   eligible if the result contains `TRASH` or `SPAM` (or the call is
   `anywhere`). So a restore made on a phone a second ago refuses the id,
   and a trash made here and not yet delivered allows it. Built from the
   view, the overlay also holds actions delivered and awaiting
   confirmation, which Gmail has already applied and history may since
   have undone; so an overlay may remove a label, but may add `TRASH` or
   `SPAM` only where history since the checkpoint has not moved the message
   in or out of that bin (a trash delivered here, then undone on a phone, is
   refused although the view still shows it trashed). Found while building:
   overlaying the view as it is would have purged that message.
5. **Delete.** Fewer than 5 ids: one `messages.delete` each. Otherwise
   `batchDelete` in chunks of 1,000. Each call passes the quota gate at
   foreground priority.
6. **Confirm.** `history.list` from `H`: ids with a `messagesDeleted` record
   are recorded with `Store::record_deleted` and reported `purged`. A single
   delete answered 404 is also proof. Everything else sent is `sent`.

`empty` is the same call over ids resolved from the mirror
(`Store::message_ids_labelled`, effective labels, drafts excluded), a chunk
at a time, with one rolling history reader: each confirmation read also
finds messages that left the bin meanwhile and removes them from the chunks
still to send. A message trashed elsewhere that the mirror has not yet seen
is not purged: the user of this host never saw it in the trash.

### Gmail calls and quota

| Call | Units | Used for |
|---|---|---|
| `history.list` | 2 per page of 500 | the check before and the confirmation after |
| `messages.delete` | 10 | each of fewer than 5 ids |
| `messages.batchDelete` | 50 | up to 1,000 ids |

Emptying a trash of 5,000 messages costs about 5 × 50 + 12 × 2 = 274 units,
under 6% of one minute of the default account budget. `messages.trash` (20)
and `threads.delete` (20) are not used.

### Store and the sync engine

No schema change and no new feed entry.

**Compatibility.** Nothing new is persisted: a purge writes the tombstones,
`MessageDeleted` entries and cleanup jobs every build already reads, so a
database a purge has written is an ordinary one to an older build and the
schema version does not move. Rolling back needs no statement; a purged
message stays deleted in Gmail.

- `Store::record_deleted(account, [(id, evidence)], now)`: tombstones with
  `DeletionEvidence::History { at }` (or `VerifiedMissing` for a single
  delete's 404) through the existing `tombstone` rule,
  in one transaction: the row goes, `MessageDeleted` is logged with the last
  thread and labels, the message's jobs and intents are cancelled, its blob
  cleanup is scheduled. Like `admit` it is **not fenced by an epoch**.
  Applying a deletion ahead of the cursor is sound because deletion is final
  (I5): when the engine later reads the same history window, every record
  about that id is skipped.
- One rule changes: a tombstone repeated for an id that is already
  tombstoned and has no row logs nothing, so the later sync does not emit a
  second `MessageDeleted`.
- `Store::message_ids_labelled(account, label, after, limit)`: a paged read;
  in SQL a text match on the `labels` column confirmed in Rust.
- The cursor never moves in a purge (I1, I2 untouched). A sync running at
  the same time is unaffected: a delivery or import of a purged message gets
  a 404 and tombstones it itself.

### Failure and retry

| What happens | Result |
|---|---|
| The check's history read fails or is deferred | error, nothing sent |
| A delete call times out or the connection drops | not sent again; the confirmation read tells `purged` from `sent` |
| Gmail answers 403 for a missing scope | `ScopeRequired`, nothing deleted, **no fallback to trash**, account not blocked |
| Gmail answers 429 or 5xx to a delete | that chunk is `unsent`; the caller may call again |
| Crash after Gmail deleted, before the record | the next sync's `messagesDeleted` records tombstone the rows |
| The same purge is called again | ids already tombstoned are `not_in_mirror`; the others are checked afresh |

A second call by the host is a new request and is checked from scratch.
That is why mailtide itself never retries: the only state a retry could
safely reuse is the id list, and the check that made those ids eligible ages
with every second.

The existing error classification turns a 403 `insufficientPermissions` into
an account-level block. A purge call's answer does not go through it: a
token without the full scope must stop the purge, not the mirror.

## 4. Alternatives rejected

- **Purge as a §7 intent with a delivery job:** durable, resumable and
  overlaid, but a queued irreversible action can fire long after the user
  asked (a cooldown, a blocked account resumed days later), and its
  eligibility would be judged at admission, not at delivery.
- **Tombstone on the delete's 2xx:** `batchDelete` documents "no
  guarantees", and a wrong tombstone can never heal (I5). The history read
  costs 2 units.
- **Check each id with `messages.get` first (himalaya):** 20 units a
  message, twice the delete itself; history since the cursor gives the same
  answer for all of them at once.
- **Trust the mirror alone:** an idle account polls every 15 minutes; a
  message restored elsewhere in that time would be destroyed.
- **Empty trash from `messages.list` of the bin:** deletes mail this host's
  user never saw there, and listings are not snapshots (SPEC §5.6).
- **`threads.delete`:** destroys messages the mirror has not seen.
- **Always `messages.delete` in an HTTP batch (himalaya):** per-id answers,
  but 10 units a message: 50,000 units to empty a trash of 5,000.
- **Fall back to trash when the scope is missing:** silently does something
  else than asked.
- **A pending-purge overlay in views:** the call ends in seconds; the rare
  `sent` id is reported to the caller, who can hide it.

## 5. Tests

Suites were found with `grep -ri` for delete, destroy, purge, expunge, trash
and spam over every checkout's test files, then read.

- himalaya `src/jmap/backend.rs`: the scoping tests of its JMAP delete map
  onto our eligibility rule and partial outcome (3 mapped; 1 `na`: a JMAP
  email in two mailboxes only leaves the trash, a Gmail message with `TRASH`
  is in the trash). `send` and `threads` declare the suite too.
- himalaya `src/gmail/backend.rs`: the three tests of its permanent delete,
  which `search` (the suite's first declarer) leaves to this plan:
  `a_lookup_keeps_the_ids_carrying_the_label` onto the restore seen only in
  history, `a_write_answered_404_is_not_found` onto the single delete
  answered 404, `unknown_ids_are_told_from_other_failures` onto the
  malformed id. They sit in the shared
  `crates/mailtide-io/tests/ref_himalaya_gmail_backend.rs`.
- himalaya `tests/google.rs`: `gmail_writes`, their live lifecycle ending in
  "trash, then delete for good, beside an unknown id", ported against the
  fake Gmail; `rest_na` for eleven live listing and IMAP checks.
- himalaya `src/shared/message/delete.rs`: 3 `na` (receipt text; IMAP
  expunge advice; a queued delete, which a purge never is).

Not declared: Zero has no test of a permanent delete, and its spam-cleanup
suite belongs to `scripts/zero_tests.py`. jmap-client and the Stalwart
server have no tests for destroy.

Our own 42 tests cover the request shapes and costs; every branch of the
eligibility fold (restore seen only in history, trash seen only in history,
pending intents either way, unverified rows, drafts, threads); confirmation
by history; the store methods; each row of the failure table; the scope
refusal; `empty` with messages arriving in and leaving the bin while it
runs; a simulator campaign with the invariant "a tombstone exists only for
a message Gmail no longer has"; and the Python API.

## 6. Slices

1. **Wire and rules.** Requests, DELETE, costs; `mailtide::purge`
   (eligibility fold, `PurgeScan`, chunking) with its unit tests.
2. **Store.** `record_deleted`, the repeated-tombstone rule,
   `message_ids_labelled`, conformance.
3. **`purge` for explicit ids and threads.** Policy, the two history reads,
   the delete calls, the failure table, the scope refusal.
4. **`empty`.** The rolling reader over chunks, budget and counts.
5. **Simulator.** Purges with faults and concurrent changes; the invariant.
6. **Python and docs.** `Config.purge`, bindings, `FakeGmail`, SPEC.

**Shared groundwork.** No separate plan owns the pieces several plans need;
each is added by whichever plan lands first, and the later ones use it as it
is:

- DELETE in `crates/mailtide-gmail/src/http.rs` and
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
store read and write. A history 404 arrives as the `HistoryExpired` event, not
`NotFound`; a missing scope is `CallError::Refused` with `ApiAccessDenied`.

Slice 3 starts after the unknowns below are run: two of them decide
constants, one decides the pre-check.

## 7. Decisions

1. **A call, not an intent.** Irreversible work is never queued. Would
   change only if a host needs purges to survive its own crash, and then
   with an expiry of minutes.
2. **Off by default, with a config ceiling and a per-call `anywhere`.** Two
   switches for the one operation that destroys mail. A maintainer may find
   the ceiling enough.
3. **Default policy: only what is in trash or spam**, as himalaya and
   Gmail's own interface do.
4. **Eligibility is the mirror folded with history since its cursor**, not
   the mirror alone and not a `get` per message. Rests on
   `purge-history-freshness`; the fallback is named there.
5. **Tombstone on a `messagesDeleted` record or a 404 only.** One evidence
   rule for every deletion in mailtide; costs one history page.
6. **mailtide never repeats a delete.** The host may; each call is checked
   afresh.
7. **Explicit purges take at most 1,000 ids; `empty` has no cap but its
   budget.** A larger explicit set is split by the caller on purpose.
8. **`empty` purges only what the mirror held in the bin**, never what
   Gmail lists there now. A message trashed elsewhere seconds ago survives
   one more emptying.
9. **Never `threads.delete`**; a thread is its messages in the mirror.
10. **Fewer than 5 ids use `messages.delete`** (per-id answers, at most 40
    units); 5 or more use `batchDelete` (50 units).
11. **`MirrorStale` instead of catching up for long.** More than 20 history
    pages behind means the account should sync first.
12. **A purged message's blob follows the normal 24 h cleanup.** Would
    change if a host must erase content at once; that is a retention
    feature, not this one.
13. **Drafts are refused**, as for actions; `delete_draft` owns them.

Unknowns (`features/purge.toml`, each with its test):

- `purge-history-after-delete`: are `messagesDeleted` records readable as
  soon as the delete answers?
- `purge-batch-delete-partial`: does `batchDelete` delete the ids that exist
  when others do not?
- `purge-batch-delete-limit`: 1,000 ids per call?
- `purge-scope-refusal`: the exact 403 for a token without the full scope.
- `purge-delete-missing`: 404 for an id already deleted?
- `purge-history-freshness`: is another client's restore in `history.list`
  the moment it returns? The one unknown whose wrong answer could cost mail.
