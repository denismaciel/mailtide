# Feed pages bounded by `limit`, also inside a commit

Plan: `features/feed-pages.toml` (roadmap R6). Status: planned.

## 1. Goal and scope

Today `changes(account, since, limit)` returns whole commits: `limit` is a
hint, and a page ends on a commit boundary because a state string
(`mt2-<generation>.<seq>`) can only name a commit. Most commits are small,
but the one that ends a reconciliation rewrites the labels of every listed
message at once, so a mailbox of 34,000 messages produces one page of 34,000
entries. A consumer has to hold it, process it in one transaction of its own,
and start over from the beginning if it dies half way.

Afterwards:

- a page never holds more than `limit` entries (default 500, at most 5,000);
- a consumer can stop after any page and resume from `next`, also in the
  middle of a commit, with no entry skipped or repeated;
- every state string a consumer has stored keeps working and means what it
  meant;
- `StateTooOld`, compaction, entry identities (`state`, `index`) and the
  stored log are unchanged. No table or column changes; the schema version
  is raised by a version-only migration step, because older builds cannot
  read the new form of position (§3, Failure and rollback).

Left out: named consumer positions and compaction that waits for them
(`consumer-cursors`), a snapshot API (roadmap R5), and any change to what a
commit records.

## 2. What the references do

**JMAP (RFC 8620 §5.2).** `/changes` takes `maxChanges`; if the server has
more it "SHOULD generate an update to take the client to an intermediate
state", returns that state as `newState` and sets `hasMoreChanges`. A state
the server cannot continue from is `cannotCalculateChanges`. mailtide's feed
already has this shape (`limit`, `next`, `has_more`, `StateTooOld`); what it
lacks is the intermediate state.

**jmap-client** (MIT/Apache), `src/core/changes.rs`. The client side:
`max_changes` on the request, `has_more_changes` and `new_state` on the
response. States are opaque strings to it. *Take:* the contract, including
that an intermediate state is just another string to pass back. It has no
tests for `/changes`.

**Stalwart server** (AGPL: names and paths only).
`crates/jmap-proto/src/types/state.rs` has two forms of state, an exact one
and `JMAPIntermediateState` (a range of changes and `items_sent`, how far
into it the client is). `crates/jmap/src/changes/get.rs` clamps the client's
`maxChanges` to `changes_max_results` and answers `CannotCalculateChanges`
for a state it cannot serve. *Take:* two forms of one position, a server-side
maximum, and an error (not a guess) for a position whose changes are gone.
*Leave:* counting items sent within a range: our log already numbers each
entry within its commit, so the position can name the entry itself. The
pinned tree has no tests for this code.

**Zero** (MIT). No feed: `mail-data-revision.ts` keeps one counter that
triggers bump, used to invalidate cached aggregates; browsers are told a
thread changed and refetch it. Nothing to take, and no test to port.

**Himalaya, pimalaya-core.** Nothing relevant: they read providers' own
paging (`pageToken`, IMAP ranges) and keep no change log.

## 3. Design in mailtide

### The position

The log is `mailtide_changes (account, seq, idx, change)` with primary key
`(account, seq, idx)`: every entry already has a total order and a stable
identity. A position becomes a point in that order:

```rust
/// How far a consumer has read: everything up to and including `entry` of
/// commit `seq`, or the whole commit when `entry` is `None`.
pub struct FeedCursor { pub seq: StateSeq, pub entry: Option<u32> }
```

ordered by `(seq, entry)` with `None` after every `Some` of the same commit.
`FeedPosition` is `{ generation, at: FeedCursor }`.

| State string | Meaning |
|---|---|
| `mt2-<gen>.<seq>` | commit `seq` and everything before it is read (today's form, unchanged) |
| `mt2-<gen>.<seq>.<idx>` | everything through entry `idx` of commit `seq` is read; the commit has more |

Fields are hex, as today. The decoder accepts both; anything else is
`InvalidState`. The encoder writes the short form whenever the position is a
commit boundary, so:

- strings stored by consumers decode to exactly the position they named;
- a two-field string passed back means the whole commit, as before, and a
  page that ends on a commit boundary returns one; a page ends inside a
  commit wherever its `limit` falls there (not only in commits larger than
  the limit), so a consumer meets three-field strings whenever its pages and
  commits do not line up;
- an entry's `state` (the commit it belongs to) stays two-field, so
  `(state, index)` identities recorded for deduplication stay valid.

### Reading a page

`Store::changes(account, since: FeedCursor, limit: u32)`:

1. `limit` is clamped to `1..=MAX_FEED_PAGE` (5,000).
2. **Too old** if `since.seq < floor`, or `since.seq == floor` and
   `since.entry` is `Some`: compaction removes whole commits, so a reader
   inside the floor commit has lost the rest of it. `oldest` is the floor, a
   commit position, as today.
3. Take the first `limit + 1` entries strictly after `since` in
   `(seq, idx)` order. The page is the first `limit`; `has_more` is whether
   the extra one exists.
4. `next` is the last entry returned, written as the commit form if the extra
   entry is absent or belongs to another commit. An empty page returns
   `since`.
5. `MessageImported` entries of the page are hydrated with the stored
   content, as on `main`.

In SQL (both dialects support row comparison and it follows the primary key):

```sql
SELECT seq, idx, change FROM mailtide_changes
WHERE account = $1 AND (seq, idx) > ($2, $3)
ORDER BY seq, idx LIMIT $4
```

with `$3` above every index for a commit position, which is then `seq > $2`.
This one statement replaces the three the whole-commit rule needs (find the
cut, read the range, probe for more). The memory store and the simulator use
`AccountMirror::changes`, which numbers each entry from its commit's first
entry instead of from the start of the page.

`ChangePage.next` becomes a `FeedCursor`; `head` stays a `StateSeq`.
`From<StateSeq> for FeedCursor` keeps callers that work in whole commits
(the ported Zero tests, `status().state`) one conversion away.

### Why no entry is skipped or repeated

- A commit's entries are inserted in one transaction, under the account row
  lock, with `seq` one above the previous. A reader therefore never sees part
  of a commit, nor commit `S+1` before `S`. The page is read by a single
  statement.
- The log is append-only; nothing is ever inserted before an existing entry.
- A page is the first `limit` entries after the cursor in a total order and
  `next` is the last of them, so consecutive pages tile the log.
- Compaction removes whole commits and only raises the floor; rule 2 turns
  the one case where a reader's remaining entries were removed into
  `StateTooOld` instead of a silent gap.
- Rule 4 can call a commit finished only when no later entry of it exists.
  Commits are atomic, so none can appear afterwards.

### Compaction

`Store::compact(account, through: StateSeq)` is unchanged and still removes
whole commits. A caller holding a position inside commit `S` compacts
through `S - 1`: `FeedCursor::compactable()` returns the last commit fully
behind the cursor, and `Runner.compact(account, through)` applies it. At
most one commit's already-read entries stay until the reader finishes it.
The floor stays one number, so no schema change.

### Cost of a page

| | Whole commits (today) | Bounded (this plan) |
|---|---|---|
| Statements | 1 account read + 3 + ⌈imports ÷ 500⌉ | 1 account read + 1 + ⌈imports in page ÷ 500⌉ |
| Rows returned | the whole commit | ≤ `limit` (+1 probed) |
| Default page (`limit` 500) | unbounded | ≤ 3 statements, ≤ 500 entries |
| Largest page (`limit` 5,000) | unbounded | ≤ 12 statements |

Hydration reads one `content` JSON per imported message in the page, by
primary key, in chunks of 500 ids. It was never the cost of the reconcile
commit (those entries are `LabelsChanged`), but a run of imports fills pages
with `MessageImported`, each carrying its header facts, and a commit that
imports many messages at once (a host's own store, a test) was hydrated in
full. Bounded pages cap both. Content read at page time can be newer than
the page (a message deleted since has `None`): unchanged from `main`.

### Public API

Rust: `FeedCursor`, `FeedPosition { generation, at }`, `Store::changes`
with a cursor, `ChangePage.next: FeedCursor`, `MAX_FEED_PAGE`.

Python: no signature changes.

```python
page = await runner.changes(account, since=state, limit=500)  # ChangePage | StateTooOld
page.changes  # at most `limit` entries
page.next  # opaque; may point inside a commit
page.has_more
await runner.compact(account, page.next)  # whole commits before `next`
```

Gmail calls: none. Engine, epochs, intents and the quota gate are untouched:
the feed is read-only over rows the commit path already writes.

### Failure and rollback

A read that fails leaves nothing behind; the consumer retries from the
position it holds.

**Compatibility.** Nothing new is written to mailtide's tables. What is new
is a form of state string, `mt2-<gen>.<seq>.<idx>`, which consumers store
(and which `consumer-cursors` will keep in its table). A build from before
this feature cannot decode it. So the release adds the next migration as a
version-only step: no statement but its row in `mailtide_schema`. That
raises `SCHEMA_VERSION`, and an older build sees the database as `ahead` and
refuses to sync (`SchemaOutOfDate`). Its `changes()` checks no schema; given
a three-field string it raises `ValueError("invalid state string")`, never a
wrong page. Reverting:

```sql
DELETE FROM mailtide_schema WHERE version = <this migration>;
```

Nothing in the database is lost. A consumer holding a three-field position
must first read on with the new build until `next` is a commit boundary (a
two-field string again), or rebuild as after `StateTooOld` (D4). Two-field
positions mean the same in every build.

## 4. Alternatives rejected

- **Split large commits when they are written** (`FinishReconcile` in
  batches): the finish must move the cursor and settle every row atomically
  (I1, I4); the feed should not shape the write path.
- **Count of entries read past a commit** (`seq` + n of the *next* commit):
  keeps the too-old rule untouched but needs a lookup for which commit is
  next; the entry's own key is simpler and is what consumers already see.
- **A new prefix (`mt3-`) for every string**: breaks nothing less, and makes
  commit-boundary strings unreadable by older builds for no gain.
- **Compacting inside a commit** (floor as `(seq, idx)`): a migration to
  save at most one commit's entries for a short time.
- **A flag to keep whole-commit pages**: two paging behaviours to test and no
  consumer that needs it; entries are applied one by one and each is
  self-contained.
- **Offset paging** (`LIMIT … OFFSET n` within a commit): cost grows with the
  offset; keyset on the primary key does not.
- **Storing content in the log** to avoid hydration: a format change and
  duplicate storage; `main` chose read-time hydration, and bounded pages make
  it cheap.

## 5. Tests

No reference suite is declared. jmap-client has no `/changes` test, the
pinned Stalwart tree ships no test files beside its changes code, and Zero
has no feed. The JMAP behaviour is cited from the RFC.

Own tests (21), in four layers:

- **Core** (`revision.rs`, `mirror.rs`): the codec both ways, old strings,
  malformed strings, cursor order; a page never exceeds its limit; an
  exhaustive check over small logs and limits that pages concatenate to the
  log with the identities of one unbounded read; boundary normalisation;
  commits appended between pages; too-old inside the floor commit; clamping.
- **Store conformance** (memory, SQLite, Postgres): `whole_commit_pages` and
  `large_import_commits` assert the behaviour being removed and are replaced
  by `pages_split_inside_a_commit` and `a_large_commit_arrives_in_bounded_pages`
  (1,200 imports as 500 + 500 + 200, each hydrated). New cases: hydration
  only of the page's imports, commit positions reading as before, compaction
  around a reader inside a commit, writes between two pages of one commit.
- **Python**: bounded pages over a bootstrap, a hand-built old state string,
  compaction from a mid-commit position.
- **Simulator**: a reader takes one page of a random limit between steps of
  every seeded run; at the end its entries must equal the mirror's log. This
  is the check that paging holds under crashes, rebuilds, admissions and
  reconciliations, where the giant commits come from.

## 6. Slices

1. and 2., in one commit: `FeedCursor`, the codec and its tests;
   `AccountMirror::changes` by cursor with the core tests; `Store::changes`
   takes a cursor: memory and SQL (the keyset query), the conformance cases,
   clamping; the version-only migration. (Built apart, the first left the
   memory store, which reads through `AccountMirror`, splitting commits
   behind a `StateSeq` contract that cannot resume inside one.)
3. Bindings: codec, `compact` from a mid-commit position, Python tests, SPEC.
4. Simulator reader and `FeedPagesDiverged`.

## 7. Decisions

- **D1. Bounded pages are the only behaviour, on by default.** The bound is
  the feature, and a second mode doubles the conformance surface. Consumers
  apply entries one at a time; no entry depends on another of its commit
  arriving in the same page. *Would change it:* a consumer that must apply a
  commit atomically; then `ChangePage` gains a flag saying whether it ends
  inside a commit, and that consumer reads on until it does not.
- **D2. The position names the last entry read; the short form is kept for
  commit boundaries.** It reuses the identity consumers already see and keeps
  old strings byte-identical. *Would change it:* nothing foreseeable.
- **D3. `limit` is clamped to 5,000.** A page is held in memory with one
  parsed envelope per import (about 1–2 KB each); 5,000 keeps a page in the
  low megabytes and under a dozen statements. A caller that asked for more
  gets `has_more`. *Would change it:* a measured need for larger pages.
- **D4. No downgrade path for three-field strings; older builds are kept
  out by the schema version.** The strings exist only while a consumer's
  last page ended inside a commit. Writing them in a form an older
  build could misread as a commit boundary would skip entries; a refusal at
  startup, and an error if one is decoded anyway, are the safe failures.
  *Would change it:* a host that must roll back under load; it can drain to
  a commit boundary first.
- **D5. Compaction stays whole-commit.** See §3; avoids a second floor
  column.
- **D6. The simulator checks paging against the log, not a rebuilt replica
  against Gmail.** The first isolates this feature; the second would also
  test that every label change is logged, which is a separate property.
  *Would change it:* a decision to make the feed a complete replication
  stream, with its own invariant.

Unknowns: none. This feature makes no Gmail call and relies on no Gmail
behaviour.
