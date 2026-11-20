# Identities and the settings mirror

Plan: `features/identities.toml` (roadmap C4 and A1). Status: planned.

## 1. Goal and scope

Afterwards a host can, per account:

- read the **identities** it may send as (Gmail's send-as aliases: address,
  display name, reply-to, HTML signature, primary, default, verification) from
  the mirror, with no Gmail call;
- read the **vacation responder**, the **filters** and the **forwarding**
  settings the same way;
- learn from the change feed that one of these changed, in Gmail's own UI or
  through mailtide, and which identity or filter it was;
- ask for a fresh read when it matters (a composer or a settings page opens);
- check a From address against the identities, and pick the identity a reply
  should go out as;
- later slices: change an identity's signature, name, reply-to and default
  flag; change the vacation responder; create and delete filters.

Reading comes first: slices 1 to 4 make no write and are useful alone.

Left out:

- placing a signature in a message body. That is the host's: `compose-mime`
  takes finished text and HTML and adds none. mailtide hands over the HTML and
  derives no plain-text form (Gmail stores HTML only);
- creating, deleting and verifying aliases, and every forwarding write. Gmail
  allows them only to service accounts with domain-wide delegation under
  `gmail.settings.sharing`; an alias carries SMTP relay credentials mailtide
  should not hold, and auto-forwarding copies a whole mailbox elsewhere. Both
  stay read-only (Decision 5);
- delegates, IMAP, POP and language settings, S/MIME and client-side
  encryption identities: nothing a mail client built on the mirror needs;
- enforcing From inside `drafts` or `send` (Decision 6).

## 2. What the references do

Gmail facts below are from Google's reference pages, read 2026-11-20 (the
`[[source]]` entries). Two matter for everything else:

- **Reads need no extra scope.** `sendAs.list`, `getVacation`, `filters.list`,
  `forwardingAddresses.list` and `getAutoForwarding` each accept any of
  `gmail.readonly`, `gmail.modify`, `https://mail.google.com/` or
  `gmail.settings.basic`. A sync engine already holds one of the first three.
  himalaya's live run agrees: every settings read passed with a delegated
  token scoped to `https://mail.google.com/` alone.
- **Nothing tells a client that a setting changed.** History has four record
  types, all about messages. So a mirror of settings can only poll or be asked
  (unknown `identities-settings-leave-no-history`).

**Zero** (MIT). `getEmailAliases` in `driver/google.ts` calls `getProfile` and
`sendAs.list` on every use and returns `{email, name, primary}` for the primary
and for aliases whose status is `accepted`. `assistant-senders.ts` checks a
From address against that list and fails closed when the list cannot be read;
`assistant-drafts.ts` picks a reply's sender (the identity the message was
sent as, else the only one in To, else the only one in Cc, else ambiguous).
No signatures, no other settings, no scope beyond `gmail.modify`.
*Take:* the sendable rule, the fail-closed check, the reply-sender rule.
*Leave:* a network round trip per compose, and no way to notice a change.

**himalaya** (MIT/Apache). `src/gmail/settings/` has one thin command per
`users.settings` resource and prints Gmail's JSON untouched. Its unified
`vacation` command maps Gmail and JMAP onto one shape, with a tri-state patch,
and writes by read-modify-write "so the Gmail-only ones survive".
*Take:* the wire facts (unpaged listings, a missing key is an empty list, enum
spellings such as `rejected` and `expired` that Google's page omits), the
vacation patch and the read-first write. *Leave:* the pass-through JSON
(a mirror needs typed, comparable values) and its local signature config.

**jmap-client, Stalwart server.** No identity or vacation tests in either
checkout. Cited for the object shapes (RFC 8621 `Identity` and
`VacationResponse`) and for `Identity/changes`, whose created / updated /
destroyed lists shape our feed entry. Server paths are names only (AGPL).

**pimalaya core, mail-builder, mail-send, mail-parser.** Nothing: core has a
signature only as local configuration; the others compose, send or parse.

## 3. Design in mailtide

**Approach.** Four settings *kinds* are mirrored state: `identities`,
`vacation`, `filters`, `forwarding`. Each is one small document per account,
always read and replaced whole. An account-level job refreshes them on a slow
interval, scheduled the way the label refresh on `main` is (SPEC §6.6); a
driver call refreshes them on demand; a feed entry is written only
when a value really changed. Writes are synchronous calls whose answer is
recorded at once.

### Objects

| mailtide | JMAP | Gmail |
|---|---|---|
| `Identity.id` | `id` | `sendAsEmail` as Gmail spells it (the path key) |
| `.email`, `.name` | `email`, `name` | `sendAsEmail`, `displayName` |
| `.reply_to: Option<String>` | `replyTo` | `replyToAddress` |
| `.html_signature` | `htmlSignature` | `signature` (sanitized by Gmail) |
| `.is_primary`, `.is_default`, `.treat_as_alias` | (none) | same names |
| `.verification` | (none) | `verificationStatus`: `Accepted`, `Pending`, `Other(String)`, `Absent` |
| `.relay: Option<{host, port, security}>` | (none) | `smtpMsa` without its credentials |
| `VacationResponse.is_enabled` | `isEnabled` | `enableAutoReply` |
| `.from_date`, `.to_date` | `fromDate`, `toDate` | `startTime`, `endTime` (epoch ms) |
| `.subject`, `.text_body`, `.html_body` | same | `responseSubject`, `responseBodyPlainText`, `responseBodyHtml` |
| `.restrict_to_contacts`, `.restrict_to_domain` | (none) | same names |
| `Filter { id, criteria, action }` | (Sieve, not used) | `Filter`, field for field |
| `Forwarding { enabled, address, disposition, addresses }` | (none) | `AutoForwarding` plus the `forwardingAddresses` list |

An identity **may send** when it is the primary or its verification is
`Accepted`; every other status fails closed.

### Public API

Rust (`mailtide_io::Runner`); Python `Runner` and `BlockingRunner` mirror it
with keyword arguments and frozen classes.

| Method | Gmail calls | Returns |
|---|---|---|
| `identities(account)` | none | `Option<Vec<Identity>>`; `None` before the first read |
| `vacation(account)`, `filters(account)`, `forwarding(account)` | none | `Option<…>` likewise |
| `settings_status(account)` | none | per kind: `fetched_at`, `problem` |
| `refresh_settings(account, kinds, budget)` | 1 per kind (2 for forwarding) | the statuses afterwards |
| `check_sender(account, address, fresh, budget)` | 0, or 1 with `fresh` | the `Identity`, or `SenderError` |
| `update_identity(account, id, patch, budget)` | `sendAs.patch` | the `Identity` Gmail stored |
| `set_vacation(account, patch, budget)` | `getVacation`, `updateVacation` | the `VacationResponse` Gmail stored |
| `create_filter(account, criteria, action, budget)` | `filters.create` | `Filter` |
| `delete_filter(account, id, budget)` | `filters.delete` | `Deleted` or `AlreadyGone` |

Pure helpers in `mailtide::identities` (exported to Python):
`sendable(identities)`, `find_sender(identities, address)` (ASCII
case-insensitive, no other normalisation) and
`reply_identity(identities, from, to, cc)`: the identity the original was sent
as, else the only sendable one in To, else the only one in Cc, else Gmail's
default identity; two candidates at one level are `Ambiguous`.

`SenderError`: `NotAnIdentity`, `NotVerified`, `Unknown` (identities never
read, or a fresh read failed: no guess is made).
`SettingsError`: `NotFound`, `Invalid { field }`, `Forbidden`, `Deferred { until }`,
`ReadOnly`, `Retryable`, `OutcomeUnknown`. Python: `SenderNotAllowed`,
`AmbiguousSender`, `SettingNotFound`, `SettingInvalid`, `SettingForbidden`,
and the shared `QuotaDeferred` (`until_ms`) and `WriteOutcomeUnknown`.

Patches are tri-state per field (keep, set, clear; `mailtide.UNSET` in
Python). `set_vacation` validates before any call: an enabled responder needs
a subject or a body, and `from_date` must precede `to_date`.

### Gmail calls, quota and scopes

| Call | Units | Scope needed | Used by | Priority |
|---|---|---|---|---|
| `sendAs.list`, `getVacation`, `filters.list`, `forwardingAddresses.list`, `getAutoForwarding` | 1 each | any scope a sync holds | refresh job | live |
| the same | 1 each | the same | `refresh_settings`, `check_sender(fresh)`, `set_vacation` | foreground |
| `sendAs.patch` | 100 (assumed, `identities-patch-quota-cost`) | `gmail.settings.basic` or `.sharing`; a non-primary alias only with domain-wide delegation | `update_identity` | foreground |
| `updateVacation` | 5 | `gmail.settings.basic` | `set_vacation` | foreground |
| `filters.create`, `filters.delete` | 5 each | `gmail.settings.basic` | filter writes | foreground |

A full refresh is 5 units. At the default interval that is 20 units a day per
account. `gmail.modify` and even `https://mail.google.com/` do **not** allow
settings writes: a host that wants slices 5 to 7 must grant
`gmail.settings.basic` (a restricted scope) to its token source.

### Store

One migration (the next one; numbers are assigned at merge), needed because
no existing table can hold a per-account document:

```sql
CREATE TABLE mailtide_settings (
  account TEXT NOT NULL, kind TEXT NOT NULL,   -- identities | vacation | filters | forwarding
  value TEXT,                                   -- JSON of the decoded value; NULL until first read
  generation BIGINT NOT NULL DEFAULT 0,         -- bumped by every write made through mailtide
  fetched_at BIGINT,                            -- when Gmail last answered
  problem TEXT,                                 -- why the last read was refused, else NULL
  PRIMARY KEY (account, kind));
ALTER TABLE mailtide_accounts ADD COLUMN settings_refreshed_at BIGINT;  -- NULL: never refreshed
```

One JSON document per kind rather than typed tables: the values are small
(Gmail caps filters at 1,000), never queried by column, and a new kind then
needs no new table. `settings_refreshed_at` is the time of the last refresh,
a column of its own beside `labels_refreshed_at`; nothing is added to the
`sync` column. The refresh job needs no table of its own: an account-level
job is a `mailtide_jobs` row with an empty message key, and feed entries are
stored as serialized `Change` values.

**Compatibility.** A build from before this feature can decode neither a
`RefreshSettings` row in `mailtide_jobs` nor a `SettingsChanged` entry in the
log, and it would ignore the table. The migration raises `SCHEMA_VERSION`,
so such a build sees the database as `ahead` and refuses to sync
(`SchemaOutOfDate`) before it claims a job. Its reads that check no schema
(`changes()`) fail with `CorruptState` on the first `SettingsChanged` entry
of a page; they never skip it. A later build that adds a field to a stored
value, or a fifth kind, changes what this one reads and drops on rewrite, so
it raises the schema version again (a version-only step if it has no
statement). Reverting:

```sql
DELETE FROM mailtide_jobs WHERE kind = <RefreshSettings>;
DELETE FROM mailtide_changes WHERE change LIKE '{"SettingsChanged"%';
DROP TABLE mailtide_settings;
ALTER TABLE mailtide_accounts DROP COLUMN settings_refreshed_at;   -- SQLite ≥ 3.35
DELETE FROM mailtide_schema WHERE version = <this migration>;
```

Lost: only the mirror of the settings, which the first refresh after the
next upgrade reads again for 5 units. Settings in Gmail are untouched. Feed
consumers keep their positions.

- `Store::settings(account, kind)` reads one row.
- `Store::record_setting(account, write, now)` records a local write (bumps
  the generation) or a foreground reading (applies only if the generation is
  the one read before the call), and appends the feed entry in the same
  transaction. Like `admit` it is not fenced by an epoch.
- `forget` deletes the rows.

### Meeting the sync engine

- **Job.** `JobKind::RefreshSettings`, account-level, live priority: the
  unit of work, with a job's claim, retry and deferral. One row per account
  at most, enqueued by the schedule below and by `record_setting` when a
  write's answer was lost. The claim hands the engine each kind's
  generation.
- **Schedule.** It follows the label refresh `main` has (SPEC §6.6;
  `LabelRefresh` and `pump_labels` in `engine.rs`, `label_refresh_due` and
  `TimerKind::Labels` in `schedule.rs`, `AccountOp::SetLabelsRefreshedAt`),
  with `settings_refresh` (default 6 h, `engine.settings_refresh_s`) for the
  interval. Age is measured from the persisted time of the last refresh
  (`settings_refreshed_at`, handed over by the claim), never from how long an
  engine has run. A starting engine enqueues the job at once if that time is
  `settings_refresh` ago or unset, which covers new accounts and accounts
  tracked before this feature with no data migration; otherwise it sets a
  `Settings` timer for when it will be. A host that starts a run every few
  seconds refreshes once per interval.
- **Run.** The engine issues the five reads and, when all have answered, one
  commit: `SettingsOp::Observe { kind, reading, generation }` per kind, the
  job finished, and `AccountOp::SetSettingsRefreshedAt(now)`; then it sets
  the `Settings` timer for `now + settings_refresh`. The time is recorded
  whether or not anything changed.
- **Store-evaluated rule** (pure, shared by commits and `record_setting`): an
  observation whose generation is older than the row's is skipped; an equal
  value only moves `fetched_at`; a different value replaces it and appends
  `SettingsChanged`.
- **Feed.** `SettingsChanged { kind, created, updated, destroyed, origin }`:
  ids of identities or filters; `["singleton"]` under `updated` for vacation
  and forwarding; `origin` is `local` or `remote`. The entry carries no
  values: read the kind again.
- **Epochs.** Engine commits are fenced as usual (epoch, claim). Driver writes
  and foreground readings are fenced by the generation only, so they never
  supersede a running sync.
- **Simulator.** The fake Gmail gains the four settings, edits by another
  client and refused reads; invariants: one row per kind, generations never
  decrease, and a healed mirror equals Gmail after one refresh.

### Failure and retry

| Situation | Behaviour |
|---|---|
| A read fails transiently (5xx, 429) | job retried with backoff; nothing applied |
| A read is refused (403, 400) | that kind keeps its last value and gets a `problem`; other kinds apply; the refresh time is recorded, so the next one is an interval away. **A settings call never blocks the account**: the mailbox still syncs |
| A reading was fenced by a local write | next refresh in one minute |
| The quota gate defers | job deferred (no failed attempt); a driver call waits within `budget`, then `Deferred` |
| A write is refused | `Forbidden`, `Invalid` or `NotFound` for the caller; nothing recorded |
| A write's answer is lost | `OutcomeUnknown`; the generation is bumped and a refresh made due now. Every write is idempotent (patch, whole-object put, delete), so a retry is safe; `create_filter` first looks for an equal filter in a fresh listing |
| Read-only mode | reads and the refresh job run; the four writes raise `ReadOnlyMode` |

## 4. Alternatives rejected

- **Pass-through reads (Zero):** a Gmail round trip for every composer and
  every From check, failing whenever Gmail or the quota is not available, and
  no signal that anything changed.
- **Settings writes as §7 intents:** the host needs Gmail's answer (sanitized
  HTML, validation errors, a scope refusal) while the user is on the settings
  page; an intent would turn those into a blocked row seen later.
- **Typed tables per kind:** four migrations' worth of columns for documents
  that are only ever swapped whole.
- **Keeping Gmail's JSON verbatim (himalaya):** values must be compared to
  decide whether a feed entry is due, and typed for the bindings.
- **A timer and bare requests, with no job** (what the label refresh does
  for its single 1-unit read): five reads that land in one commit, and a
  driver call that must be able to make a refresh due, want a job's claim,
  retry and deferral. Only the schedule is taken from the label refresh.
- **The job row as the schedule** (re-enqueue at `now + settings_refresh`):
  a second way to do what `labels_refreshed_at` and its timer already do,
  and a lost row would need a rule at `claim` to put it back.
- **Refreshing with every label refresh or every poll:** five requests where
  an idle account makes one; settings change a few times a year.
- **Refreshing when a `SENT` message arrives from an address that is no known
  identity:** imported mail (`messages.insert`) can carry any From and would
  trigger refresh storms during backfill.
- **Translating filters to Sieve for a JMAP-pure API:** lossy both ways.
- **`sendAs.update` (PUT):** it clears every field left out; patch does not.

## 5. Tests

Suites were found with `grep -riE "sendAs|alias|identity|signature|vacation|forward|filters"`
over every checkout's sources and tests, then read.

- Zero `driver/draft-providers.test.ts`: the one test `drafts` deferred here
  is claimed, as the pure sendable rule.
- Zero `assistant.test.ts` (57 tests of a host's approval workflow): declared
  with `rest_na`. Four alias tests are mapped onto `reply_identity`,
  `find_sender` and a fresh `check_sender`; one is `na` (a From change is a
  draft update).
- himalaya `gmail/settings/sendas/get.rs` and `filters/list.rs`: mapped as
  wire-layer decoding tests (three of the four listings; not delegates).
- himalaya `gmail/settings/pop/get.rs`: POP is not mirrored, but these are
  the only reference unit tests of a singleton setting's GET and PUT, so both
  are ported onto the vacation responder and say so.
- himalaya `email/vacation.rs`, `shared/vacation/set.rs`,
  `jmap/vacation/get.rs`: the epoch-millisecond window and the tri-state patch
  are mapped; five are `na` (command-line parsing, output naming, JMAP only).
- himalaya `account/context.rs`: the From resolution test is mapped; twelve
  are `na` one by one (local configuration: role aliases, save-copy,
  posting style, config merging), with no `rest_na` because `compose-mime`
  declares the suite too. Its two signature tests were deferred to
  `compose-mime`, which marked them `na` (the host places signatures).

Not declared: himalaya `jmap/backend.rs` (its one identity test checks JMAP's
`notFound` list; we read identities whole), `shared/message/builder.rs`
(signature placement in a composed body, declared by `compose-mime`), and
Zero `assistant-addressing.test.ts` (reply recipients: `send`, which takes
the "self" addresses from our identities). jmap-client and the Stalwart
server have no tests for these objects in their pinned trees.

Our own 55 tests cover what no reference has: request shapes and costs;
decoding refusals (an alias with no address, relay credentials); the mirror
rules (no feed entry for an unchanged reading, id diffs, the generation
fence, a refused reading); the job (five reads and one commit, the schedule
from the persisted time, retry, never blocking); sender checks failing
closed; every
row of the failure table; read-only mode; a simulator campaign; the Python
API.

## 6. Slices

1. **Wire reads.** `mailtide-gmail` settings requests and decoders, the
   `ReadSetting` requests, events, costs.
2. **Mirror.** Core types and rules, `SettingsOp`, migration,
   `Store::settings` and `record_setting`, the feed entry, conformance.
3. **Refresh job.** Account-level job, the schedule (`settings_refreshed_at`,
   the `Settings` timer), the engine run, the tunable, simulator settings and
   invariants.
4. **Driver and Python reads.** The read methods, `refresh_settings`,
   `check_sender`, the pure helpers, `FakeGmail` settings, SPEC. C4's read
   side and all of A1's are done here.
5. **Identity writes.** `update_identity` over `sendAs.patch`, PATCH in the
   transport, the write rows of the failure table.
6. **Vacation write.** `set_vacation` by read, patch, put.
7. **Filter writes.** `create_filter`, `delete_filter`.

**Shared groundwork.** No separate plan owns the pieces several plans need;
each is added by whichever plan lands first, and the later ones use it as it
is:

- PATCH, PUT and DELETE in `crates/mailtide-gmail/src/http.rs` and
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

**Landed.** The groundwork is on `main`: `Runner::call_gmail` with `CallError`
and `QuotaDeferred { until }`, the Python `QuotaDeferred` (`until_ms`) and
`WriteOutcomeUnknown` (their Rust-to-Python conversion comes with the first
method that raises them), PUT, PATCH and DELETE, and schema checks on every
store read and write. The account-level job kind is not part of it: the first
of `drafts` and `identities` adds it. The refresh reads run in the engine job,
not through `call_gmail`, so "a refused read never blocks the account" is this
plan's own engine handling; a write's missing scope arrives as
`CallError::Refused` with `ApiAccessDenied`.

## 7. Decisions

1. **Settings are mirrored, not passed through.** Composing and From checks
   then work from the store, and a change reaches the feed. *Would change:*
   if hosts only ever need identities at send time, slice 1 plus a
   pass-through read would do.
2. **Refresh is a slow job (6 h, scheduled like the label refresh) plus an
   on-demand call, with no change hints.** Gmail gives no signal, the values
   change rarely, and a host that
   needs freshness can buy it for 1 to 5 units when a composer opens. *Would
   change:* `identities-settings-leave-no-history` refuted (then refresh on
   the signal), or a host that cannot call `refresh_settings`.
3. **One JSON table, one timestamp column, one migration.** *Would change:*
   a need to query filters or identities across accounts in SQL.
4. **Writes are synchronous calls, not intents**, as `drafts` decided for its
   writes. All are idempotent, so the host retries. *Would change:* a host
   that must queue settings edits while offline.
5. **Writable, in this order: identity fields, then the vacation responder,
   then filters. Never through mailtide: alias create, delete and verify, and
   all forwarding.** The order follows the roadmap (C4 before A1) and falling
   use; the exclusions follow Gmail's own restriction to domain-wide
   delegation and the damage a forwarding write can do. *Would change:* a
   Workspace admin host that provisions aliases.
6. **`check_sender` is offered, not enforced inside `drafts` and `send`.**
   The mirror can lag in both directions and those APIs take raw bytes; Gmail
   is the authority at send time. *Would change:*
   `identities-from-outside-the-identities` showing Gmail sends a foreign
   From as given; then the check becomes mandatory and fresh.
7. **Typed values; fields mailtide does not model are dropped on read but
   kept on a vacation write** (the put is built from Gmail's own object).
8. **mailtide exposes the signature and does not place it**, and derives no
   plain-text form; `compose-mime` decided the same from its side. *Would
   change:* `identities-signature-not-added-by-the-api` refuted.
9. **The feed entry carries ids, not values**, like `LabelDefinitionsChanged`:
   a signature can be kilobytes and a filter list a thousand rows.
10. **A settings failure never blocks an account.** A refused read is a
    per-kind `problem`; account-wide trouble is the history path's to find.
11. **`reply_identity` lives in mailtide's core**, though a host could write
    it: it is pure, Zero tests it three times, and `send` needs the same
    notion of "our addresses".
12. **Relay host, port and security mode are mirrored; credentials never.**
13. **Not mirrored:** delegates, IMAP, POP, language, S/MIME, client-side
    encryption identities. *Would change:* a host asking for one; each is one
    more kind, with a version-only migration step and no new table.

Unknowns (each has runnable steps in the plan; `scripts/features.py unknowns identities`):

- `identities-settings-leave-no-history`: a settings change leaves no history record and no push.
- `identities-patch-quota-cost`: `sendAs.patch` costs 100 units like update.
- `identities-patch-answer-is-stored-state`: a write's answer is what Gmail stored.
- `identities-read-after-write`: a read right after a write shows the write.
- `identities-from-outside-the-identities`: Gmail rewrites a foreign From to the primary address.
- `identities-signature-not-added-by-the-api`: API sends get no signature from Gmail.
- `identities-status-of-primary-and-domain-aliases`: which entries carry `accepted`, which no status.
- `identities-non-primary-update-refusal`: refusals of settings writes are 403.
- `identities-duplicate-filter`: creating an equal filter is refused.
