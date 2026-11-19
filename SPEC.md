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
- Full-text search. That belongs to the host.
- Message parsing inside the sync core. Parsing is a separate optional crate
  (§5.5).
- Sending mail, draft authoring, label creation/rename/delete. These come
  later; the design leaves room for them.
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
  works (GCS, S3, local disk, memory).
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
- **label**: `label_id`, `name`, `type` (system/user), `visibility`, `color`.
  Synced from `labels.list`. Small and refreshed periodically.
- **message**: `message_id`, `thread_id`, `label_ids` (a set, stored as Gmail
  ids), `checkpoint`, `internal_date`, `size_estimate`, `blob_ref` (nullable
  until imported), `import_state` (`pending | imported | failed | blocked`),
  and header facts the engine needs (`rfc822_message_id`, `in_reply_to`,
  `references`, `from`, `to`, `cc`, `subject`, `snippet`), filled on import.
  - Read, starred, archived, spam, trash, draft and important are **never
    separate columns**. They are views of `label_ids`. JMAP keywords map as
    `$seen = ¬UNREAD`, `$flagged = STARRED` and `$draft = DRAFT`; mailbox
    membership is `label_ids`.
- **tombstone**: `message_id`, `thread_id`, `deleted_at_checkpoint`.
- **thread** (derived or materialized): the union of non-draft message labels.
  A thread is unread if any non-draft message is unread [Z]. Its latest
  message is chosen by `internal_date`.
- **job**: durable work items (body import, verification, blob cleanup) with
  dedupe key, priority class, attempts, `next_attempt_at` and last error.
- **intent** (phase 6): `(message_id, label_id)`, `op` (`add | remove`),
  `version`, `state` (`pending | delivered | blocked`), attempts and error.
- **change log** (for consumers): an append-only list of
  `(state_seq, object_type, object_id, kind)`, so host features can ask "what
  changed since state S" [J]. The state string encodes `state_seq`.

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

### 5.5 Parsing (separate crate, optional)

`mailtide-mime` may wrap Stalwart `mail-parser` (Apache-2.0/MIT) to
extract headers, text, HTML and attachments from stored raw content. The sync
core depends only on the headers listed in §4, which it extracts with a
minimal header parse.

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

- Refresh `labels.list` on start, on an unknown label id in history, and at
  most every 15 min otherwise [Z].
- Store label ids, never names, on messages. Renames are a label-table update.

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
   the intents; a 404 tombstones the message.
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
| `history.list` | 2 |
| `messages.modify` | 5 |
| `messages.batchModify` | 50 |
| `getProfile` | 1 |
| `labels.list` | 1 |
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
  `LabelsListed`, `WatchRenewed{expiration}`, `LabelsModified`.
- Store results: `CommitApplied`, `CommitRejected{reason}`,
  `JobsClaimed{jobs}`, `BlobDeleted`.
- Failures: `RequestFailed{failure}` (retryable with class and
  `Retry-After` / not found / blocking), `RequestDeferred{until}` (the quota
  gate said not yet; not a failure).

### 10.2 Commands (engine → host)

- `Gmail{account, request, priority, cost}`.
- `Commit(commit)`: atomic, guarded (epoch, engine version, expected cursor,
  job claim).
- `ClaimJobs{epoch, limit, now, attempt}`, `DeleteBlob{message}`.
- `Schedule{timer, at}`, `ReportStatus(status)`.

### 10.3 Store contract and conformance

`mailtide_store::Store`:

- `claim(account, version)`: bump the epoch (fencing every older writer),
  release jobs held by older engines, return the snapshot to build an engine
  from (validated into a `ValidSnapshot`).
- `apply(commit)`: all or nothing; rejections (`StaleEpoch`, `StaleVersion`,
  `CursorMismatch`, `StaleClaim`) are outcomes, not errors.
- `claim_jobs(account, epoch, limit, now, attempt)`: refused for an older
  epoch; `Deliver` jobs carry their pending changes.
- `changes(account, since, limit)`: the change feed, with opaque state
  strings.
- `messages(account, ids)`: `MessageView`s for hosts.

The conformance suite (`mailtide_store::conformance`, 12 cases) runs against
the memory store and SQLite on every test run, and Postgres in CI.

## 11. Python bindings

Python stays thin; everything else is Rust. `mailtide.Runner(database_url,
blob_url, token_source, *, watch_topic=None)` per process:

- `migrate()`; `run(account, budget_s) -> RunOutcome` (call on a schedule and
  on each verified push; `stop` says why it stopped: `idle` with the next due
  time, `budget_exhausted`, `cooling_down`, `blocked`, `superseded`);
- `act(account, action, message_ids=… | thread_id=…) -> RunOutcome`;
- `messages(account, ids) -> list[MessageView]`;
  `changes(account, since) -> ChangePage`; `read_blob(account, id) -> bytes`;
- `verify_push(...) -> Notification`; `parse_message(raw) -> ParsedMessage`;
- exceptions `MailtideError`, `DelegationRevoked` (raised by the token source
  to block an account), `InvalidPush`.

Calls release the GIL and run on a process-wide Tokio runtime. One `Runner`
is shared by all threads; every method may be called concurrently, including on
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
faults, including users acting through the engine, and checks invariants
after every commit and convergence after healing (CI: 500 seeds × 1,000
steps; locally 3,000 seeds). Zero's suites are ported by name and tracked by
`scripts/zero_tests.py`. Python tests drive the real `Runner` against a fake
Gmail over HTTP.

## 13. Observability

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
| 7 Send + drafts | Send and draft authoring through the same pattern | — |
| OSS | Publish under `denismaciel` (MIT OR Apache-2.0) | Timing: open question |

Status (2026-11-18): phases 1, 2 and 6 are implemented, with most of phase 4
(`changes`, `messages`). Phase 3 needs a production host.

## 15. Decisions

| # | Decision | Status |
|---|---|---|
| Q1 | Written spec before code | **Yes** |
| Q2 | Gmail only in v1; JMAP-shaped API keeps others possible | **Yes** |
| Q3 | The engine emits atomic, guarded commits; **mailtide ships the stores** (memory, Postgres, SQLite) built on shared pure rules (revised from "the host owns storage") | **Yes** |
| Q4 | Raw content fetched once into blob storage; parsing is a separate optional crate | **Yes** |
| Q5 | The engine owns import ordering and resumability; a quota gate over the core `Ledger` enforces declared costs, priority shares and cooldowns | **Yes** |
| — | Thread model: Gmail `threadId` only; conversations across mailboxes (RFC headers) are left to hosts | **Yes** |
| — | Body retention and deletion: blobs of tombstoned messages are deleted after a 24 h grace period | **Yes** |
| — | Open source: public under `denismaciel`, wheels on GitHub Releases | **Yes** |
| — | License: MIT OR Apache-2.0 (dual) | **Yes** |
| — | Does the engine also own `watch` provisioning, or only renewal? | Open |
| — | Unverified rows: deltas never advance an unverified row's checkpoint (§6.5) | **Yes** |
| — | Actions send every implied change, even ones the mirror already shows (§7) | **Yes** |
| — | Delivery runs as durable per-message jobs, not a coalescing timer | **Yes** |
