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
2. by default it checks that messages are **in trash or spam** using the
   mirror and a fresh Gmail history read before each request;
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
| `purge(account, PurgeTarget::Messages(ids) \| Thread(id), anywhere, budget)` | `PurgeOutcome { purged, sent, refused, unsent, retry_at, interrupted }` |
| `empty(account, Bin::Trash \| Bin::Spam, budget)` | `EmptyOutcome { purged, sent, skipped, remaining, retry_at, interrupted }` (counts) |

- `purged`: Gmail's history shows the deletion and the store has its tombstone.
- `sent`: Gmail may have applied the delete and there is no recorded
  tombstone. This includes ambiguous batch refusals and evidence the
  store could not record.
- `refused`: the target was ineligible or Gmail definitively refused it.
  Reasons are `not_in_mirror`, `draft`, `unverified`,
  `not_in_trash_or_spam` and `gmail_refused`.
- `unsent`: the call knows Gmail did not apply a delete. A 401 or quota
  rejection can be returned after a request reached Gmail. `retry_at`
  carries a quota deferral or the effective cooldown when one was installed.
- `interrupted`: the reason sending stopped after an ambiguous outcome,
  a failed confirmation, or an error affecting the partial result.

`PurgeError` (returned before a delete may have applied): `ReadOnly`,
`Disabled`, `AnywhereNotAllowed`, `TooMany { limit, given }`,
`MirrorStale(Stale)` (`NoCursor`, `NotLive`, `HistoryExpired`,
`TooFarBehind`), `ScopeRequired(Gmail's message)`, `Deferred { until }` (the
check's history read), `Retryable`; and, as `search` has them, `Refused` (the
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
2. **Read the account.** Its status and cursor. No cursor, or an account
   that is not `live`: `MirrorStale`.
3. **Catch up privately before every wire request.** Read `history.list`
   from the mirror's cursor or the head of the last completed read. A 404
   or more than 20 pages is `MirrorStale` on the initial check and stops
   further sending thereafter. Reread the remaining target rows and their
   active bin-removal intents after history.
4. **Decide per message.** Fold history at or after the row's checkpoint
   into observed labels, then subtract any active bin removals. A pending
   restore, move to inbox or not-spam can remove a bin label seen only in
   history. An unsettled bin removal protects a target even under
   anywhere or when it carries another bin label. A local trash or spam action counts once Gmail shows the bin
   label. Drafts, unverified rows and missing rows are refused.
5. **Dispatch by id.** Use single deletes below five eligible ids and a
   batch for up to 1,000 otherwise. A quota deferral ends sending with
   `retry_at`. If the check was started more than two seconds ago when
   the request would reach the transport, refresh it. This includes time
   spent on history pages, store reads, admission and credentials. An
   expired or zero budget prevents a new delete. An in-flight request can
   finish and its evidence can still be recorded.
6. **Confirm.** Read history from the last head after a send that might
   have applied or an ordinary refusal. Record only `messagesDeleted`
   evidence. A failed confirmation sets `interrupted` and stops issuing
   deletes. A later request still has its own pre-dispatch check.

Gmail has no conditional delete. A restore between even the latest
observation and the delete, or one published in history late, can escape
the check. The two-second deadline limits the time before dispatch.
`purge-history-freshness` saw 78 of 78 restores in the first history read.
A separate experiment saw a draft deletion appear 1.6 seconds late.

`empty` pages the selected bin's observed ids in groups of up to 1,000.
Every page with targets starts a fresh check, including after a page
that sent no requests. The mirror must have seen the bin label for
`empty` to consider an id. History is used to check those candidates.

### Gmail calls and quota

| Call | Units | Used for |
|---|---|---|
| `history.list` | 2 per page of 500 | the check before and the confirmation after |
| `messages.delete` | 10 | each of fewer than 5 ids |
| `messages.batchDelete` | 50 | up to 1,000 ids |

Five full batches with one check page and two confirmation pages each
use about 280 units, before any additional history backlog. `messages.trash` (20)
and `threads.delete` (20) are not used.

### Store and the sync engine

No schema change and no new feed entry.

**Compatibility.** Nothing new is persisted: a purge writes the tombstones,
`MessageDeleted` entries and cleanup jobs every build already reads, so a
database a purge has written is an ordinary one to an older build and the
schema version does not move. Rolling back needs no statement; a purged
message stays deleted in Gmail.

- `Store::record_deleted(account, [(id, evidence)], now)`: tombstones with
  `DeletionEvidence` (a purge passes only `History { at }`, from a
  `messagesDeleted` record) through the existing `tombstone` rule,
  in one transaction: the row goes, `MessageDeleted` is logged with the last
  thread and labels, the message's jobs and intents are cancelled, its blob
  cleanup is scheduled. An untracked account is reported separately from
  a repeated tombstone. Like `admit` it is **not fenced by an epoch**.
  Applying a deletion ahead of the cursor is sound because deletion is final
  (I5): when the engine later reads the same history window, every record
  about that id is skipped.
- One rule changes: a tombstone repeated for an id that is already
  tombstoned and has no row logs nothing, so the later sync does not emit a
  second `MessageDeleted`.
- `Store::message_ids_labelled(account, label, after, limit)`: a paged read;
  in SQL a text match on the `labels` column confirmed in Rust.
- The cursor never moves in a purge. An engine commit for a canceled job
  fails its claim guard. A concurrent history commit sees the tombstone and
  cannot recreate the row. Any content written by a late import is covered
  by the delayed cleanup.

### Failure and retry

| What happens | Result |
|---|---|
| Initial history check fails | Error before sending |
| The quota gate defers a delete | That request and the rest are `unsent`, with `retry_at` |
| A timeout, lost answer, 408 or 5xx | That request is `sent` unless confirmed as `purged`. The rest are `unsent` |
| 404 | The request is `sent` unless confirmed as `purged` |
| Recognized missing scope before any possible application | `ScopeRequired` with Gmail's reason. The account stays live and no mail is moved to trash |
| Recognized missing scope after a possible application | This request is refused and the call stops |
| 401 or a quota response | This request and the rest are `unsent`. A quota response reports the effective cooldown if installed |
| Ordinary single-delete 400 or 403 | `gmail_refused` unless history independently proves the deletion |
| Ambiguous batch refusal | Each id is `sent` unless its deletion is confirmed |
| Confirmation read or evidence write fails | Stop with `interrupted` and report each id once. Retain any evidence already recorded |
| Account forgotten before recording | Stop with `interrupted=not_tracked` and report the unrecorded ids as `sent` |
| Crash before recording | A later sync can record Gmail's history |
| The host calls again | Already tombstoned ids are absent. The other ids are checked afresh |

A second call by the host is a new request and is checked from scratch.
That is why mailtide itself never retries: the only state a retry could
safely reuse is the id list, and the check that made those ids eligible ages
with every second.

A 403 that names `insufficientPermissions` or
`ACCESS_TOKEN_SCOPE_INSUFFICIENT` stops the purge without blocking the
mirror. The exact batch scope response remains unverified.

**No way around the checks.** The public `Runner::call_gmail` only reads: it
refuses any request that changes the mailbox (`CallError::Refused`) before any token
or quota. Features inside the driver write through `direct_call`, so a
`messages.delete` reaches Gmail only through `purge` or `empty`.

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

The plan lists request shape and quota tests, label eligibility, store
conformance, delivery racing a tombstone, and the synchronous and async
Python interfaces. Driver tests pause quota admission or token acquisition,
restore a target during a preceding single or after its confirmation,
and force release, refusal and confirmation failures.

The simulator checks Gmail's bin labels at application and checks that
tombstones belong to deleted messages. Its empty path includes batches
and a short tail. Deterministic tests count restores during a previous
single, token and release failures, partial refusals and late history.
One characterization test restores the message after the last eligible
snapshot and expects the at-delete oracle to flag the resulting deletion.

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

Built on the unknowns' assumed answers, each made safe as the plan's
`if_wrong` says; `purge-scope-refusal` and `purge-history-freshness` are
confirmed.

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
   `purge-history-freshness`; the fallback is named there. Actions not yet
   confirmed here only refuse; a trash asked for here counts once Gmail
   shows it (review ruling: the view can't tell pending from delivered).
5. **Tombstone on a `messagesDeleted` record only.** A delete's answer is
   never evidence, a 404 included: `purge-delete-missing` is unsettled and
   a proxy can answer 404 for a live message, and a tombstone never heals.
   Costs one history page.
6. **mailtide never repeats a delete.** The host may; each call is checked
   afresh.
7. **Explicit purges take at most 1,000 ids; `empty` has no cap but its
   budget.** A larger explicit set is split by the caller on purpose.
8. **`empty` purges only what the mirror held in the bin**, never what
   Gmail lists there now. A message trashed elsewhere seconds ago survives
   one more emptying.
9. **Never `threads.delete`**; a thread is its messages in the mirror.
10. **Keep single deletes below five eligible ids.** Their definite per-id
    400/403 answers are useful while batch partial application is unsettled.
    Each single pays for a separate history check. Five or more ids use
    `batchDelete` (50 units), with unproven batch ids reported as `sent`.
11. **`MirrorStale` instead of catching up for long.** More than 20 history
    pages behind means the account should sync first.
12. **A purged message's blob follows the normal 24 h cleanup.** Would
    change if a host must erase content at once; that is a retention
    feature, not this one.
13. **Drafts are refused**, as for actions; `delete_draft` owns them.
14. **A delete never waits for the quota gate.** Not admitted at once is
    `unsent` with `retry_at`; the caller calls again and is checked again.
15. **The public `call_gmail` only reads** (review ruling, shared with
    `drafts` and `label-edit`): writes go through `direct_call` inside the
    driver, so nothing deletes around a purge's checks.

Unknowns (`features/purge.toml`, each with its test):

- `purge-history-after-delete`: are `messagesDeleted` records readable as
  soon as the delete answers?
- `purge-batch-delete-partial`: does `batchDelete` delete the ids that exist
  when others do not?
- `purge-batch-delete-limit`: 1,000 ids per call?
- `purge-scope-refusal`: single-delete 403 confirmed.
- `purge-batch-scope-refusal`: exact batch response remains unverified.
- `purge-delete-missing`: 404 for an id already deleted?
- `purge-history-freshness`: is another client's restore in `history.list`
  the moment it returns? The one unknown whose wrong answer could cost mail.
  Confirmed 78 of 78; side evidence: a draft's permanent removal appeared
  1.6 s late once in 150 (`features/drafts.toml`), the residual this design
  states rather than closes.
