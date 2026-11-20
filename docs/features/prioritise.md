# Prioritise: fetch a message a user opened ahead of the backlog

Plan: `features/prioritise.toml` (roadmap S1). Status: planned.

## 1. Goal and scope

During a first import a mailbox has tens of thousands of messages whose row
exists (`MessageDiscovered`) and whose body does not yet. Their import jobs
run newest first in the Backfill class, at about 56 messages a minute (70% of
4,800 units, 60 units each). A user who opens one of them waits for its turn,
which can be hours away.

Afterwards a host calls

```python
result = await runner.prioritise(account, message_ids)
```

and the bodies of those messages are the next ones fetched, in the Foreground
quota class, whichever process is running the account. The host learns the
body arrived from the feed (`MessageImported`) or from `messages()`.

Left out:

- a message the mirror has no row for (for example an id from a Gmail search
  the sync has not reached): reported as `unknown`, not created. Opening
  such a hit at once is not built now, by decision: `search` returns it as
  `unknown`, the host calls `run` or waits for the next sync, and
  `prioritise` only raises jobs that exist (D7; `search` Decision 4);
- raising verification or cleanup jobs; cancelling or lowering a priority;
- reading the body straight from Gmail in the caller's request;
- a thread selector (the host has the ids from the feed).

## 2. What the references do

**Zero** (MIT) has exactly this behaviour.

- `mail-history-store.ts`: `mailbox_import_jobs` has one row per thread with a
  `kind`; `jobPriority` orders `delete`, `verify`, `import` (live), then
  `backfill`. `ensureImport(threadId)` inserts an `import` job or turns an
  existing `backfill` job into `import`. It does not touch `next_at`,
  `attempts` or `version`, so opening a thread twice shares the job and does
  not reset a failed import's cooldown. `job(now)` walks the classes in order
  and skips only the backfill class while backfill is paused
  (`backfillPausedUntil`).
- `routes/agent/index.ts`: `getThreadFromDB` on a cache miss calls
  `ensureCachedThreadImport`, which promotes the job and calls
  `scheduleHistorySoon`, then returns an empty thread. When the import lands
  the browser gets a "refetch this thread" message. The request never waits
  on Gmail.
- `gmail-priority.ts`: the quota class is ambient (`withGmailPriority`); a
  promoted import runs as `sync` (live, 90% of the budget), not `foreground`.

*Take:* promotion is an idempotent change to the existing durable job;
attempts and backoff survive; the caller returns at once and is told later.
*Change:* mailtide raises to Foreground (100%), because its job row carries
the quota class and "a user is waiting" is what that class means (SPEC §6.4);
and it bounds how much can be raised, which Zero does not.

Zero's two tests of this ("prioritizes live changes and opened threads ahead
of historical imports", "deduplicates browser cache misses without resetting
a failed import cooldown") are in `mail-history-store.test.ts`, a suite
`scripts/zero_tests.py` owns. Both are already ported in
`crates/mailtide/tests/zero_mail_history_store.rs`, where the "open" is a
fenced `JobOp::Enqueue` at Foreground. The store rule exists; what is missing
is a way for a host to reach it.

**JMAP, jmap-client, Stalwart, Himalaya.** Nothing: a JMAP server has every
body, and Himalaya fetches on demand with no queue.

## 3. Design in mailtide

### What exists

`JobKind::Import(id)` rows carry a `Priority` (`Backfill < Live <
Foreground`). Claims take the highest class first, then the earliest due.
The engine issues each claimed import as `messages.get?format=raw` in the
job's class, and the quota gate admits Backfill up to 70% of the budget, Live
90%, Foreground 100%. While the gate holds a class back, the engine claims
only classes above it. `JobOp::Enqueue` on an existing job already raises its
class. Nothing outside an engine's fenced commit can do that.

### The rule (pure, `jobs.rs` and `mirror.rs`)

`AccountMirror::prioritise(ids, now) -> Prioritised` puts each distinct id in
one group:

| Group | When | Effect |
|---|---|---|
| `imported` | the row has content | none: read it now |
| `unknown` | no row (never discovered, or deleted) | none |
| `blocked` | its import job is parked after a permanent failure | none; `resume` is the operator's tool |
| `queued` | its import job is ready or running | class becomes Foreground; see below |
| `over_limit` | it could be raised but the account already has `FOREGROUND_IMPORT_CAP` Foreground imports | none; still queued in its own class |

Raising a job:

- **ready**: `priority = Foreground`. If `due_at` is in the future and the
  job has never failed (a quota deferral of its old class, or the two-minute
  settle delay for drafts), `due_at = now`. A job backing off after a failure
  keeps its `due_at`: the backoff protects Gmail, and Zero keeps it too.
- **running**: the claim, attempt and in-flight request are untouched; only
  the stored class changes, so a retry or deferral of that attempt comes back
  as Foreground.
- **already Foreground**: unchanged, counted as `queued`.

Nothing else changes: no row, no intent, no feed entry, and the account's
`seq` does not move, so state strings are unaffected.

**Compatibility.** Nothing new is persisted. A raise changes `priority` and
`due_at` on an existing `mailtide_jobs` row to values every build already
writes and reads, so the schema version does not move and an older build
simply claims the raised job first. Rolling back needs no statement; raised
jobs stay raised until they finish.

### Limits

- **Per call:** at most `PRIORITISE_MAX_IDS = 100` ids, checked by the runner
  before the store is touched (`ValueError` in Python). That covers a long
  thread in one call and bounds the transaction; more ids go in a second call.
- **Per account:** at most `FOREGROUND_IMPORT_CAP = 24` import jobs in the
  Foreground class at once (ready or running). Backfill may use 70% of the
  4,800 units a minute; the 1,440 units it cannot touch fetch 24 raw messages
  (60 units each). So the prioritised set is what one minute of reserved
  budget can serve, and a host that prioritises everything it lists cannot
  turn the backlog into Foreground work that delays actions and history
  sync. Ids beyond the cap come back as `over_limit`; the host asks again as
  imports land (each `MessageImported` frees a slot).

No time-based rate limit is needed: the cap bounds the queue whatever the
call rate, and each call is one short transaction.

### Store

```rust
fn prioritise(&self, account: &AccountId, ids: &[MessageId], now: Timestamp)
    -> impl Future<Output = Result<Prioritised, StoreError>> + Send;
```

**Unfenced, like `admit`.** It needs no engine and no epoch: a user's open
must not be lost because a sync run superseded another, and it must not
preempt the run in progress. It is safe without the fence because the change
is monotonic and idempotent (a class only goes up; a second call is a
no-op), and it never creates or finishes a job. It runs under the account
row lock, so it is serialized with commits and claims: a finish that removes
the job first makes the id `imported`; a prioritise that lands first is
simply overwritten by the job's removal.

SQL, one transaction, no schema change: lock the account row; select
`id, content IS NOT NULL` for the ids and their rows in `mailtide_jobs`
(`kind = import`), one `IN` query each (at most 100 ids); count Foreground
imports; update the raised rows. The existing index
`(account, state, priority, due_at)` already serves the claim that follows.
An untracked account answers every id as `unknown`.

### Driver and bindings

```rust
impl Runner { pub async fn prioritise(&self, account: &AccountId, ids: &[MessageId])
    -> Result<Prioritised, RunError>; }
```

```python
@final
class PrioritiseResult:
    queued: list[str]      # will be fetched first
    imported: list[str]    # already there: read_blob / messages() now
    unknown: list[str]
    blocked: list[str]
    over_limit: list[str]

await runner.prioritise(account, message_ids) -> PrioritiseResult
```

It is a store write only and returns in one round trip: use it from request
paths. It is allowed in read-only mode, since an import changes nothing in
Gmail.

**Who fetches.** The engine that holds the account. If a run is in flight
(the usual case while a backlog exists), its next claim, issued when any of
its at most four running jobs finishes, takes the Foreground job first: about
one request's latency, nothing superseded. If no run is in flight, or the run
is sleeping until a held-back class reopens, the host calls
`run(account, budget)` as it does on a push; that run's first claim takes the
job. `prioritise` then `run` is to opens what `act` is to actions.

**How the caller learns the body arrived.**

1. The feed: a `MessageImported` entry for the id, with `content` (envelope
   and blob reference). A host already following the feed needs nothing else.
2. Polling: `messages(account, ids)` returns `content` once imported.
3. If the message was deleted meanwhile: `MessageDeleted` in the feed, and
   the id disappears from `messages()`.

### Gmail calls and quota

No new call. The import is the existing `messages.get?format=raw`, 60 units
(Google's quota table), now admitted in the Foreground class. The engine
already issues a claimed job's request with the job's class; the only engine
work is a test that it does so for a job raised behind its back.

### Meeting the sync engine

- **Epochs:** not involved (see Store). A superseded engine's claim on the
  job is released by the next `claim`, with the raised class kept.
- **Invariants:** I6 is untouched (no job is added or removed). New
  simulator check: outside a claim's finish, no job's class goes down.
- **Held-back classes:** when the gate defers Backfill, the engine claims
  from Live up; a raised job is still claimed. When Gmail itself pushes back
  (account cooldown), nothing is claimed, raised or not: correct.
- **Failure and retry:** unchanged from any import. Transient errors back
  off (now in Foreground), a 404 tombstones, a permanent error blocks the
  job; a quota denial defers without counting an attempt.

## 4. Alternatives rejected

- **Fetch in the caller's request** (get the raw message through the gate,
  write the blob, return the bytes): lowest latency with no run in flight,
  but the import commit is fenced engine work, so the job would fetch the
  same message again (120 units), and a second path would write blobs.
- **A fenced engine event** (`OpenRequested`, like `ActionRequested`): only
  the process running the account could accept it; the request path usually
  is not that process.
- **`prioritise` also runs the account**: every open during a backlog would
  supersede the import run and discard its in-flight requests.
- **Raise to Live, as Zero does**: Live is capped at 90% and shared with
  history sync; the Foreground class exists for a waiting user.
- **Newest-opened-first within Foreground**: needs an ordering column; with
  at most 24 raised jobs, first-in first-out is at most a minute behind.
- **Lower the oldest raised job to make room**: the job row does not record
  its original class; `over_limit` tells the host instead.
- **Create a row for an unknown id**: a row needs an observation (thread and
  labels at a checkpoint), which only a fenced fetch provides.
- **A blocking `wait_imported`**: a polling loop over `messages()` the host
  can write in three lines, with its own timeout policy.

## 5. Tests

No reference suite is declared: the only tests of this behaviour are the two
in Zero's `mail-history-store.test.ts`, owned by `scripts/zero_tests.py` and
already passing against the fenced path. This plan adds 22 own tests:

- **Core** (8): the raise rule for ready, delayed, backing-off, running and
  Foreground jobs; grouping of ids; the cap; no feed entry; claim order; the
  engine issuing the request in the Foreground class while Backfill is held
  back.
- **Store conformance** (7), on memory, SQLite and Postgres: raise and claim
  order, no epoch needed, the report groups, backoff kept, a running claim
  finishing afterwards, the cap and untracked accounts, the feed untouched.
- **Driver** (3): end to end against the fake Gmail with a tight quota (the
  opened message is fetched in Foreground with at most `JOB_CONCURRENCY`
  backlog imports ahead of it); the call limit; read-only mode.
- **Python** (2): prioritise, run, then the feed entry and `messages()`; the
  report for imported and unknown ids and the `ValueError`.
- **Simulator** (2): `UserOp::HostOpen` in the random mix on every seed, with
  the new class-never-lowered check, I5, I6 and convergence; and a scripted
  run where the oldest of 200 pending messages is opened and must be imported
  before more than `JOB_CONCURRENCY` others.

## 6. Slices

1. The pure rule and outcome in `jobs.rs` / `mirror.rs` with the core tests.
2. `Store::prioritise` in memory and SQL with the conformance cases.
3. `Runner::prioritise`, the engine test and the end-to-end tests.
4. Bindings, stubs, Python tests, SPEC.
5. Simulator op and the two simulator tests.

## 7. Decisions

- **D1. One method, a store write; the host calls `run` itself when no run
  is in flight.** A combined "prioritise and run" would supersede the import
  run on every open. *Would change it:* hosts always pairing the two calls
  and measuring the supersede as harmless; then add the pair as one method,
  as `act` is to `admit`.
- **D2. Unfenced.** Same reason as `admit`; safe because raising is
  monotonic and idempotent. *Would change it:* prioritise ever creating rows
  or jobs; that would have to go through the engine.
- **D3. Foreground, not Live.** *Would change it:* evidence that opened
  imports crowd out actions despite the cap.
- **D4. Caps of 100 ids per call and 24 raised imports per account, as
  constants.** Derived above from the reserve Backfill cannot use. *Would
  change it:* a host with a different budget; then the cap follows the
  configured budget (30% of a minute's units ÷ 60) instead of a constant.
- **D5. A delayed job that never failed becomes due now; a failed one keeps
  its backoff.** *Would change it:* nothing foreseeable; it matches Zero for
  the failed case.
- **D6. The feed and `messages()` are the completion signal; no wait call
  and no new feed entry for "prioritised".** *Would change it:* a host
  without a feed consumer that needs a push-style signal.
- **D7. Unknown ids are reported, not fetched.** A row needs an observation,
  and until then the sync brings it (history, or the listing). So opening a
  `search` hit the mirror does not know is not built now, by decision:
  `search` returns it as `unknown`, the host calls `run` or waits for the
  next sync, and `prioritise` only raises jobs that exist (`search`
  Decision 4 says the same). *Would change it:* a host that must open such
  a message at once. It would be a second, fenced path here: an engine event
  that fetches the id (`messages.get`, 60 units raw) at Foreground and
  creates the row with its content, through the engine that holds the
  account.
- **D8. Message ids only.** *Would change it:* `threads` adding a thread
  index to the store; then `thread_id=` is a lookup away.
- **D9. Spelled `prioritise`**, as the feature id and the code's doc comments
  (British spelling). *Would change it:* a house rule for American spelling in
  public names; it is a rename before release, nothing else.

Unknowns: none. The feature adds no Gmail call and relies on no Gmail
behaviour beyond the documented cost of `messages.get`.
