# Consumer cursors: compaction that waits for feed consumers

Plan: `features/consumer-cursors.toml` (roadmap S5). Status: in progress.
Depends on `feed-pages` for the cursor type.

## 1. Goal and scope

The change log grows by one row per change until a host calls
`compact(account, through)`. The host must know, itself, how far every one
of its consumers has got: an indexer, a notifier, a projection. If it
compacts too far, the consumer behind gets `StateTooOld` and rebuilds its
copy from the whole mirror. If it never compacts, the log grows for ever.

Afterwards:

- a consumer registers under a name by acknowledging a position:
  `ack(account, "indexer", page.next)`;
- `compact(account)` needs no position: it drops what every live consumer has
  acknowledged, and nothing more;
- a slow consumer is never cut off; a **dead** one (nothing heard for
  `max_idle`, 7 days by default) stops holding the log, and gets the usual
  `StateTooOld` if it comes back;
- a consumer rebuilding from the mirror can hold the log from where its
  rebuild started, however long the rebuild takes;
- hosts that pass a position to `compact` and register nothing see no change.

Left out: delivering pages to consumers (they still call `changes`),
exactly-once processing (a consumer keeps its own durable position and
deduplicates by entry identity, as now), compaction inside `run`, and a size
limit on the log.

## 2. What the references do

Neither JMAP implementation nor Zero registers consumers with a change log.

**Stalwart server** (AGPL: names only). `delete_changes` in
`crates/common/src/storage/transaction.rs` trims each account's change log to
`max_entries` or `max_duration` (`changes_max_history` in the configuration).
A client behind the trim gets `CannotCalculateChanges`
(`crates/jmap/src/changes/get.rs`). The server keeps no record of its
clients' states. *Take:* nothing new: this is mailtide today, with the host
choosing the trim point. It shows what the default of the protocol is, and
why a mirror feeding derived stores wants more: a rebuild here is a re-read
of every message.

**jmap-client** (MIT/Apache). `CannotCalculateChanges` in
`src/core/error.rs` is all a client sees. Its push subscriptions
(`src/push_subscription/set.rs`) are the one registered thing in JMAP: they
carry `expires` and lapse unless the client extends them. *Take:* the
liveness rule: a registration is kept alive by the consumer and lapses on
silence. No tests of either.

**Zero** (MIT). Changes reach browsers through `mailbox_notifications`: one
versioned row per thread, deleted by `acknowledgeNotification(thread,
version)`. No log, no cursor, nothing to compact. *Take:* acknowledge a
position, never "everything up to now".

**PostgreSQL replication slots** (documentation, not a pinned reference) are
the closest model: a slot records how far its consumer has confirmed and
holds the write-ahead log from there; `max_slot_wal_keep_size` and
`idle_replication_slot_timeout` exist because a forgotten slot otherwise
fills the disk, and a slot that loses its log is marked so and must be
rebuilt. This plan is that design at the size of one table.

## 3. Design in mailtide

### Data

```rust
pub struct ConsumerName(String);          // 1..=64 of [A-Za-z0-9._-]
pub struct ConsumerCursor {
    pub name: ConsumerName,
    pub at: FeedCursor,                   // from feed-pages: may be inside a commit
    pub seen_at: Timestamp,               // last ack, or last time compaction saw it at the head
}
```

Per account, at most 16. They live in `AccountMirror.consumers` for the
memory store and the simulator, and in SQL in a new table (the next
migration; numbers are assigned at merge):

```sql
CREATE TABLE IF NOT EXISTS mailtide_consumers (
    account TEXT NOT NULL, name TEXT NOT NULL,
    seq BIGINT NOT NULL, idx BIGINT, seen_at BIGINT NOT NULL,
    PRIMARY KEY (account, name))
```

A migration is needed because this is new state per (account, consumer):
nothing persisted today has a place for it, and the account row's single
`log_floor` cannot say who is holding it. Cursors are not part of
`StoreSnapshot`, and commits neither read nor write them: the engine does not
know consumers exist.

**Compatibility.** The table is one an older build must not ignore: a build
from before this feature compacts to whatever `through` it is given, past
registered consumers, and cannot read a row whose position is inside a
commit (`idx` set, the three-field form of `feed-pages`). The migration
raises `SCHEMA_VERSION`, so such a build sees the database as `ahead` and
refuses to sync (`SchemaOutOfDate`). Its `compact` checks no schema, so a
host must not call it from an older build while cursors are registered.
Reverting:

```sql
DROP TABLE mailtide_consumers;
DELETE FROM mailtide_schema WHERE version = <this migration>;
```

Lost: the registered cursors. The log and its floor are untouched, so each
consumer still follows from its own stored position; the host goes back to
passing `compact` a position itself, and consumers register again with
their next `ack` after the next upgrade.

### Rules (pure, in `revision.rs`, shared by every store)

**ack(name, at, now)**

- `at` behind the floor (or inside the floor commit), beyond the head, or of
  another generation: `TooOld { oldest }`. Nothing is stored. The consumer
  rebuilds, exactly as when `changes` says so.
- otherwise the cursor is **set** to `at` with `seen_at = now`, created if
  the name is new (refused with `TooManyConsumers` beyond 16). Set, not
  "advance": the consumer's own durable position is the truth, and a consumer
  that restarted from an older checkpoint must be able to say so. A late or
  repeated ack can only make the log be held a little longer.

**compact(Retention { through, now, max_idle })**

1. Every cursor at the head gets `seen_at = now`: the compactor has seen it
   caught up.
2. A cursor is **live** if it is not behind the floor and
   `seen_at >= now - max_idle`. Its *hold* is the last commit wholly behind
   it (`at.compactable()`: the commit itself, or the one before if the cursor
   is inside a commit).
3. The target:

   | `through` | live cursors | target |
   |---|---|---|
   | given | some | the smaller of `through` and the smallest hold |
   | given | none | `through` (today's behaviour) |
   | absent | some | the smallest hold |
   | absent | none, but cursors exist | the head: everyone registered is dead or lost |
   | absent | no cursors at all | nothing: the host is not using cursors |

4. Entries at or before the target are deleted and the floor rises to it,
   as now. Cursors are never deleted by compaction; one left behind the floor
   is **lost** and stays listed until it acks a valid position or is dropped.

Step 1 is what makes the idle rule safe for a quiet mailbox. A consumer that
caught up and then had nothing to acknowledge for a month is refreshed by
every compaction, so its idle clock starts when it first falls behind, not
when it last spoke. A consumer that is behind and silent for `max_idle` is
the only kind cut off.

**drop_consumer(name)** deletes the cursor (decommissioning; otherwise it
holds the log for `max_idle`).

**Reading** is untouched: `changes(account, since, limit)` takes a position,
not a name, and answers `StateTooOld` by the floor alone.

### Store

```rust
fn ack(&self, account: &AccountId, consumer: &ConsumerName, at: FeedPosition, now: Timestamp)
    -> impl Future<Output = Result<AckOutcome, StoreError>> + Send;
fn consumers(&self, account: &AccountId)          // the cursors, with generation and floor
    -> impl Future<Output = Result<ConsumerList, StoreError>> + Send;
fn drop_consumer(&self, account: &AccountId, consumer: &ConsumerName)
    -> impl Future<Output = Result<bool, StoreError>> + Send;
fn compact(&self, account: &AccountId, retention: &Retention)   // was: through: StateSeq
    -> impl Future<Output = Result<u64, StoreError>> + Send;
```

`ack`, `drop_consumer` and `compact` run under the account row lock, like
`admit`, so a registration cannot race a compaction past it. `ack` takes the
whole position, generation included, so the generation is checked under
that lock too: a `forget` and `track` between a check and the write cannot
register a position of the old log. `consumers` returns the account's
generation and floor with the cursors (`ConsumerList`), which a listing needs
to encode each position as a state string and to say which are lost. They are
**unfenced**: a consumer is not an engine and must never be refused, or
refuse an engine, because of an epoch. `forget` deletes the account's
cursors with its other rows; a re-tracked account has a new generation and no
cursors.

### Driver and bindings

`Runner` (`crates/mailtide-io/src/runner.rs`) gains the four methods and
supplies its clock (so `ManualClock`
reaches the idle rule in tests). All work in read-only mode: they write
mailtide's own tables, never Gmail.

```python
@final
class ConsumerCursor:
    name: str
    state: str          # the position, as changes() takes it
    seen_at_ms: int
    lost: bool          # behind the feed's floor: must rebuild

await runner.ack(account, consumer, through) -> ConsumerCursor | StateTooOld
# an invalid name, or a new one beyond 16, raises ValueError
await runner.consumers(account) -> list[ConsumerCursor]
await runner.drop_consumer(account, consumer) -> bool
await runner.compact(account, through=None, max_idle_s=604_800.0) -> int
```

`compact(account, through)` as called today keeps working.

### What a host calls

```python
# each consumer, after it has durably processed a page
await runner.ack(account, "indexer", page.next)

# housekeeping, e.g. after a run or hourly; no position needed
await runner.compact(account)

# rebuilding from the mirror without racing compaction
head = (await runner.status(account)).state
await runner.ack(account, "indexer", head)      # hold the log from here
...enumerate message_ids() + messages()...
page = await runner.changes(account, since=head)  # then follow as usual
```

`consumers(account)` is the operator's view: who holds the log, since when,
and who is lost.

### Meeting the sync engine

No Gmail calls, no quota, no commits, no feed entries, no epochs. The only
contact is the account row lock, held for one upsert per acknowledged page.

### Failure

- An ack that fails (store unavailable) is retried or simply superseded by
  the next one; meanwhile the log is held longer, never shorter.
- A consumer that acks a position it has not durably processed can lose
  entries to compaction. That is the consumer's contract, as with any
  acknowledgement.
- A compaction that fails leaves the log as it was (one transaction).

## 4. Alternatives rejected

- **Ack implied by reading** (`changes(..., consumer=name)` records `since`):
  one call fewer and a free heartbeat, but a read becomes a write under the
  account lock, and peeking with a name moves a cursor.
- **Monotonic acks** (keep the furthest position): a consumer restored from
  an older checkpoint would be cut off by its own earlier ack.
- **A size cap** (cut off the slowest once the log exceeds N entries): the
  largest logs are bootstraps, exactly when a consumer is slow but alive.
- **Idle measured from the last ack only**: cuts off a caught-up consumer of
  a quiet mailbox the moment mail arrives.
- **Timestamps on log entries** to measure "behind for how long": a column
  on the largest table, for what one timestamp per cursor gives.
- **Cursors as JSON on the account row**: still a migration, and every ack
  would rewrite the row every commit reads.
- **Compaction inside `run`**: puts feed housekeeping on the sync path and
  hides a delete in a call hosts budget for Gmail work.
- **Deleting an expired cursor**: "all consumers dead" would become "no
  consumers", which must mean "compact nothing".
- **The store reads the feed for the consumer** (named reads only): takes
  the durable position away from the consumer, which needs it in its own
  transaction to be exact.

## 5. Tests

No reference suite is declared: no reference has consumer registration, so
there is no test to port or to mark `na`. The cited behaviour (trim by
policy, `cannotCalculateChanges`) is what mailtide already has and already
tests in the `compaction` conformance case.

Own tests (28):

- **Core** (11): names; the target for every row of the table above, a
  cursor inside a commit, the idle limit and its boundary, lost cursors not
  pinning the floor; ack as set, ack behind the floor, the compactor
  refreshing a caught-up cursor, the per-account bound.
- **Store conformance** (10): the slowest consumer holds the log; no
  consumers means today's behaviour; an explicit bound cannot pass a live
  consumer; the idle cut-off and what the cut-off consumer then sees; the
  quiet-period case; starting again after being lost; drop; acks unfenced
  and invisible to commits; forget; a consumer inside a commit.
- **SQL** (1): the migration on a database with a log.
- **Python** (4), with `ManualClock`: two consumers, the idle cut-off,
  `compact(account, position)` unchanged, an ack from an old generation.
- **Simulator** (2): on every seed a steady reader that acks, with
  compaction at random steps, is never cut off and the log never keeps a
  commit every live cursor has passed; a stalled reader is cut off once the
  simulated clock passes `max_idle`, and only it.

## 6. Slices

1. `ConsumerName`, `ConsumerCursor`, `Retention`, `compaction_target` and the
   core tests.
2. Mirror methods and their tests.
3. The `Store` methods on every store: memory, and SQL (migration, the four
   methods, forget); conformance on memory, SQLite and Postgres. (Planned as
   two slices, memory then SQL; the trait changes once, and a commit that
   changed it for memory alone would not compile the SQL store.)
4. Runner methods, bindings, stubs, Python tests, SPEC.
5. Simulator readers and the two violations.

## 7. Decisions

- **D1. Explicit `ack`; reading stays read-only.** Conventional, and the
  consumer chooses the moment (after its own commit). *Would change it:*
  hosts forgetting to ack; then `changes` gains `consumer=` as a convenience
  over the same store call.
- **D2. The cap is idleness (7 days), not size.** It separates dead from
  slow, which a size cap cannot. Seven days matches the window in which Gmail
  history usually survives, the other "too long away, start again" bound in
  the system (SPEC §5, fact 3). *Would change it:* a host that cannot afford a
  week of log for a dead consumer; `max_idle_s` is per call, so it can pass
  less, and a size cap can be added beside it.
- **D3. Compaction refreshes cursors at the head.** Removes the one trap in
  an idle rule at the cost of a write per caught-up cursor per compaction.
  *Would change it:* nothing foreseeable.
- **D4. The host calls `compact(account)`; mailtide does not compact on its
  own.** It is how compaction works today and keeps `run` about Gmail.
  *Would change it:* hosts not scheduling it; then a config switch runs it at
  the end of a run.
- **D5. `ack` sets the position** (see §4). *Would change it:* consumers
  with several workers per name wanting "furthest wins"; they can use a name
  per worker.
- **D6. No cursors and no bound compacts nothing; dead cursors only and no
  bound compacts to the head.** The first protects hosts that track
  positions themselves and call `compact(account)` by mistake; the second is
  the cap doing its job. *Would change it:* nothing foreseeable.
- **D7. `Store::compact` changes signature** rather than gaining a sibling
  method: one compaction rule, one entry point. Hosts with their own `Store`
  implementation adapt once; Python is source compatible. *Would change it:*
  a stability promise for the trait before this lands.
- **D8. One table, one migration.** See §3. *Would change it:* nothing; the
  state is new.
- **D9. At most 16 consumers per account, names of 64 plain characters.**
  Bounds the compaction scan and keeps names safe in logs. *Would change
  it:* a host with more consumers than that.

Unknowns: none. The feature makes no Gmail call. The remark in D2 about how
long Gmail keeps history is SPEC §5's quotation of Google's sync guide, and
nothing here depends on it.
