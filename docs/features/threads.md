# Thread views read from the mirror

Plan: `features/threads.toml` (roadmap R7). Status: planned.

## 1. Goal and scope

Afterwards a host can ask the mirror for any set of threads of an account and
get, per thread:

- its messages, oldest first, each as the `MessageView` that `messages()`
  returns (labels with pending actions applied, envelope once imported);
- the union of its non-draft messages' labels, and whether it is unread;
- its latest message, its message count and draft count, how many messages
  still await import, and its participants.

The read makes no Gmail call, claims no account and needs no credentials.
Thread actions use the same member lookup, which today reads every row of the
account.

**Left to the host, deliberately** (§3 "The scope decision" and Decisions 1):
listing threads by label, sorting and paging, per-label counts, multi-label
and unread-only filters, search (`search` owns Gmail queries), snooze,
conversations across mailboxes and reply trees. The engine gives the host
what it needs to build these in its own tables: the change feed names the
thread of every changed message, and `threads()` answers what each of those
threads looks like now. `tests/test_threads.py` carries that loop as a worked
example.

## 2. What the references do

**Zero** (MIT) is the only reference with our model, and it built option (c).
`MAILBOX_SYNC.md` states the rules we share: a thread's state is the union
over its non-draft messages with pending intent overlaid. In code:

- `mail-label-store.ts` `threadLabels`: one statement joining a thread's
  message rows with their intents, drafts skipped, behind an index on
  `thread_id` (added after "a live slow-query check caught the missing
  index"). *Taken*: the rule, the joined read, the index.
- `routes/agent/index.ts`: a `threads` table holding the latest date and a
  JSON copy of the label union, rewritten by `refreshThreadLabels` whenever
  labels change. `getThreadsFromDB` lists by filtering that JSON row by row
  (`json_each`), ordered by `latest_received_on`, and pages with
  `latest_received_on < token`, which drops threads sharing the cursor's
  timestamp. `thread-list-summary.ts` stores a body-free snapshot per thread,
  corrects it from the label store on every read, repairs it when stale and
  falls back to a full read when it cannot answer.
  *Not taken*: the stored copy. Half of that suite's tests exist to keep the
  copy honest.
- `mail-count-store.ts` `countThreads`: per-label counts by one pass over
  every message row, served only once the import is complete, cached, with
  Gmail's own totals as the fallback before that. *Not taken* (host's).

**himalaya** (MIT/Apache). `src/gmail/threads/` passes `users.threads.get`
and `list` through as Gmail sent them; its unified envelope drops Gmail's
thread id. `src/pimdir/index.rs` has the nearest thing to a summary in a
local index (`PimdirMailThread`: id, message count, first and last date) with
no labels, unread state or participants. `src/jmap/thread/get.rs` asks
`Thread/get` for several ids and reports the ones not found. The IMAP and
pimdir thread commands build reply trees from headers. *Taken*: the batch
read by ids, "oldest first" (`cairn/spec/search.md`). *Not taken*: reply
trees (SPEC §15 leaves header threading to hosts).

**pimalaya core** (MIT). `ThreadEnvelopes` builds a reply graph from
`In-Reply-To`; its seven tests parse IMAP THREAD answers. Nothing for us.

**jmap-client** (MIT/Apache) and **RFC 8621**. A JMAP `Thread` is an id and
its email ids, "sorted by the receivedAt date of the Email, oldest first",
ties stable ("sorting by id is recommended"). Labels, unread and counts are
not on the Thread: clients fold them from the Emails. A thread list is an
`Email/query` with `collapseThreads`; counters (`totalThreads`,
`unreadThreads`) belong to the Mailbox. *Taken*: the order, the tie-break,
and the split (a thread is a group of messages; lists and counters are
queries). We add the folded fields because only the engine knows the
overlay. The crate has no thread tests.

**Stalwart server** (AGPL, names only). `ThreadGet` groups the account's
cached messages by thread id at read time, `collapse_threads` collapses
while paging a message query, and `mailbox_counters` folds counters from
per-message rows. No thread table is kept. The pinned tree has no tests for
any of it (its unit tests live in a repository that is not vendored).

**mail-builder, mail-parser, mail-send.** Nothing about threads.

**Google's docs** (read 2026-11-20). A thread lets a client "retrieve all
messages in a conversation, in order"; which order is not stated.
`threads.list` filters by `labelIds` ("match all of the specified label
IDs"), states no order and returns no messages. `threads.get` costs 40 quota
units (120 with `format=full`), `threads.list` 10.

## 3. Design in mailtide

### The scope decision

SPEC §4 calls the thread "derived or materialized"; §1 and §15 and the
roadmap leave list and sort queries to hosts. Three ways to draw the line:

| | (a) nothing in the engine | (b) read by thread ids + one index | (c) thread table + listing by label |
|---|---|---|---|
| Host gets | the feed, as today | `threads(account, ids)` | (b) and `threads_by_label(account, label, cursor, limit)` |
| Migration | none | one `CREATE INDEX`; no data touched | two tables and their index, plus a backfill of every existing mailbox that cannot be SQL: labels and dates sit inside JSON text columns, and the two dialects read JSON differently. So a resumable Rust rebuild per account, with listing incomplete until it ends |
| Write cost per commit | none | one index entry per message inserted or deleted; label changes do not touch it | for every thread a commit touches: read all its rows and intents, rewrite the thread row and one row per label (the sort key lives in each). Every write path pays: sync commits, unfenced admission, settle and block. Each import commit changes a thread's date, so a 100k-message bootstrap adds at least 100k recomputations, and a commit of tens of thousands of messages grows by as many statements in one transaction (`write_diff` writes row by row) |
| Stores and conformance | none | one shared statement; three conformance cases | the memory store grows the same derived index; about eight cases (order, ties, paging while threads move, drafts, deletions, overlay, rebuild) |
| Simulator | none | convergence compares thread ids and views; nothing stored can drift | a new invariant after every commit: stored thread rows equal the fold of the messages. This is the bug class Zero's repair and fallback paths exist for |
| Host still builds | everything: its own message rows, the draft and overlay rules, latest, lists | lists, paging, sort, counts (see §1) | every list that is not "one label, newest first": unread only, two labels, search results, counts, snooze, cross-mailbox |

**Chosen: (b).**

- The engine should own what only it can get right: the *definition* of a
  thread. The draft rule, the overlay of pending actions and "latest" are
  easy to get subtly wrong outside, and thread actions must agree with what
  the view shows. (a) leaves each host to restate them, and leaves the
  account-wide scan in today's thread actions unfixed.
- A list is a product decision. The one list the engine could offer (one
  label, newest first) is the first thing a real client outgrows; a host
  that needs any other list keeps its own table anyway and would then pay
  for the engine's copy as well. JMAP and the Stalwart server draw the same
  line: threads are groups of messages, lists are queries.
- (c) taxes every host on every commit, including hosts that never list, and
  adds stored derived state to an engine whose invariants are all about not
  having any.
- (b) is a strict prefix of (c): the pure fold and the index are what (c)
  would be built on. Choosing (b) now throws nothing away.

### Public API

Pure core, `mailtide::thread`:

```rust
pub struct ThreadView {
    pub id: ThreadId,
    pub messages: Vec<MessageView>,   // every member, drafts included, in order
    pub labels: LabelSet,             // union over non-draft members, effective labels
    pub unread: bool,                 // labels contains UNREAD
    pub latest: Option<MessageId>,    // newest imported non-draft member
    pub message_count: u32,           // non-draft members
    pub draft_count: u32,
    pub awaiting_import: u32,         // non-draft members with no content yet
    pub participants: Vec<String>,
}
pub fn view(id: &ThreadId, members: Vec<MessageView>) -> Option<ThreadView>;
```

- `Store::threads(account, ids: &[ThreadId]) -> Vec<ThreadView>`; the SQL and
  memory stores implement it, `Runner::threads` forwards to it.
- Python: `await runner.threads(account, thread_ids) -> list[ThreadView]` on
  `Runner`, the same blocking on `BlockingRunner`. `ThreadView` is a frozen
  class with the fields above; `latest` is the `MessageView` itself (the
  same object as in `messages`), or `None`.
- Answers come in the order asked. An id the mirror has no message for is
  left out, as `messages()` does; so is every id of an untracked account.
  An empty id list returns at once, without a query.

The rules, all in the one pure function:

| Field | Rule |
|---|---|
| order of `messages` | imported members by (`internal_date`, id), oldest first; then members awaiting import, by id. The date exists only once a message is imported, so the tail is provisional and `awaiting_import` says so |
| `labels` | `labels::thread_labels` (exists) over the members' effective labels. A draft contributes nothing, so `DRAFT` never appears; `draft_count` tells a host the thread has one. Trashed and spam members count like any other |
| `unread` | some non-draft member is effectively `UNREAD` (SPEC §4) |
| `latest` | the last *imported* non-draft member in the order above. It always has an envelope a row can show. During backfill (newest first) and in live sync (imports land within seconds) this is also the true latest, or becomes it when the import commits |
| `participants` | for each imported non-draft member, oldest first: `From`, then `To`, then `Cc`, as the header facts hold them. One entry per address (the text inside the last `<…>`, else the whole value, compared case-insensitively), keeping the first spelling seen. The account's own address is included; `Bcc` is not a header fact and never appears |

A new unsent draft is a thread of one draft: `message_count` 0, no labels,
no latest. An empty thread id is never a thread (unknown
`threads-history-names-the-thread`).

### Store and schema

One migration (the next one; numbers are assigned at merge):

```sql
CREATE INDEX IF NOT EXISTS mailtide_messages_thread
    ON mailtide_messages (account, thread_id, id)
```

*Why it cannot be avoided:* thread membership lives only in
`mailtide_messages.thread_id`, and the table has its primary key
`(account, id)` and a checkpoint index. A lookup by thread therefore reads
every row of the account, and that lookup already runs on every thread
action. No data changes; the statement is the same on Postgres and SQLite.
On Postgres it blocks writes to the table while it builds; a host with a
very large table can first run `CREATE INDEX CONCURRENTLY` under the same
name, and the migration is then a no-op.

**Compatibility.** Only an index is added: no row, log entry or job changes
form, and nothing is stored about threads. The migration still raises
`SCHEMA_VERSION`, as every migration does, so a build from before it sees
the database as `ahead` and refuses to sync (`SchemaOutOfDate`) until the
migration is reverted:

```sql
DROP INDEX mailtide_messages_thread;
DELETE FROM mailtide_schema WHERE version = <this migration>;
```

Nothing is lost. In the other direction, a build with this feature on a
database without the index reports `behind` and refuses every call, reads
included (`SchemaOutOfDate`), until migrated.

The read is one statement per chunk of 500 thread ids, shared by both
databases:

```sql
SELECT m.id, m.thread_id, m.labels, m.checkpoint, m.unverified, m.content, i.intent
FROM mailtide_messages m
LEFT JOIN mailtide_intents i ON i.account = m.account AND i.message = m.id
WHERE m.account = $1 AND m.thread_id IN ($2, …)
```

One statement is one snapshot, so a view never pairs rows from before a
commit with intents from after it. Rows are grouped in Rust, turned into
`MessageView`s by the existing `MessageRecord::view`, and folded by
`thread::view`. The memory store calls `AccountMirror::thread`, which uses
the new `thread_members`; thread action admission moves to the same helper
(the SQL lookup it already issues now hits the index).

Nothing is stored about threads, so `forget`, compaction and `message_ids`
are untouched.

### Gmail calls and quota

None. Every field comes from rows the sync already keeps, so the feature
costs 0 quota units and works in read-only mode and while an account is
blocked or cooling down.

### Meeting the sync engine

- **Epochs and fencing.** A read claims nothing: it never preempts a running
  sync, and any process may call it at any time (as `messages()`).
- **Overlay and confirmation.** The union is over effective labels, so an
  admitted action shows at once, a delivered one keeps showing until its
  confirmation fetch settles it, and a blocked one stops showing. No new
  rule: it is `labels::effective` per member.
- **Feed.** No new entry kinds. `MessageDiscovered`, `MessageImported`,
  `LabelsChanged`, `OverlayChanged` and `MessageDeleted` each name the
  message's thread, and those are exactly the events that can change a view.
  A consumer collects the thread ids of a page, calls `threads()` once, and
  upserts each answer or deletes the ids left out. The view is "now", which
  may be newer than the page; a later entry then leads to the same answer
  again, so the loop converges. After `StateTooOld` it rebuilds from
  `message_ids` and `messages` (which give the thread ids).
- **Partial threads.** mailtide imports per message, so a thread can be half
  imported (Zero imports whole threads and never sees this). The view shows
  every member the mirror knows; labels and unread are exact from discovery
  on, `latest` and `participants` cover the imported part, and
  `awaiting_import` counts the rest. The `MessageImported` entry tells the
  consumer to read again.
- **Simulator.** Convergence today compares message sets and labels, not
  thread ids. It will also compare each message's thread id and each
  thread's view with a fold of the fake Gmail's messages.

### Failure and retry

`threads()` fails as `messages()` does: `StoreUnavailable` when the database
cannot be reached, `CorruptState` when a row does not parse. It changes
nothing, so the caller simply retries. Untracked accounts and unknown ids are
not errors.

## 4. Alternatives rejected

- **(a) Nothing in the engine:** see the table; the rule and the index are
  worth owning even if no host lists through us.
- **(c) A thread table with listing by label:** see the table; revisit when
  the condition in Decisions 1 holds.
- **Asking Gmail (`threads.get`, 40 units; `threads.list`, 10):** costs
  quota for data the mirror already has, cannot show pending actions, and a
  listing is not a snapshot (SPEC §5.6).
- **A single `thread(account, id)`:** a consumer of one feed page would make
  one round trip per thread; the batch read costs the same to build.
- **A `ThreadChanged` feed entry:** derivable from the message entries; it
  would be a schema-less but permanent addition to the log format.
- **Summaries without the member list:** a second shape to keep in step for
  little saving; the members are the rows the read loads anyway.
- **Ordering undated messages by Gmail's id as if it were time:** ids look
  time-ordered but Google does not say so.
- **A reply tree (References/In-Reply-To):** SPEC §15; the header facts are
  in each member for a host that wants one.
- **Per-label thread counts:** a whole-mailbox aggregate, the same read
  model as listing.

## 5. Tests

Suites were found with `grep -ril thread` over every checkout's test files,
then read. Declared:

- Zero `thread-list-summary.test.ts`: 6 mapped onto the pure rule (envelope
  and no bodies, group threads via participants, nothing imported, current
  labels and counts, deletions, the next message becoming latest); 3 `na`,
  all about the stored snapshot we do not keep.
- Zero `thread-list-page.test.ts`: 2 mapped onto the batch read (one call in
  the order asked, no read for no ids); 1 `na` (the host's page when the
  summary read fails).
- Zero `mail-count-store.test.ts`: 2 mapped. One checks that pending actions
  and deletions show in the view at once. The other rebuilds Zero's fixture
  mailbox and folds Zero's counting rule over our views inside the test,
  expecting Zero's numbers: the count is the host's, the test proves the
  view carries what it needs. 9 `na`: caches, throttles and fallbacks
  around counts fetched from Gmail, which we never fetch. Its 2
  label-catalogue tests are deferred to `label-edit`, which maps them onto
  the label tests `main` already has.
- Zero `mailbox-reader.test.ts`: 1 mapped (a cached read needs no provider
  auth: ours asks for no token and calls nothing); 3 `na` (RPC plumbing).
- himalaya `src/jmap/backend.rs`: its Thread/get not-found test, as unknown
  ids left out. The suite is shared with `purge` and `send`, so it has no
  `rest_na`: this plan lists its 14 other read and query tests `na` one by
  one, `send` its writes and refusals, `purge` its deletes.
- himalaya `src/gmail/threads/get.rs`, `src/imap/envelope/thread.rs`,
  `src/pimdir/thread.rs` and pimalaya core's `thread/imap.rs`: declared so
  they are accounted for, all `na` (a Gmail pass-through we do not call;
  reply trees).

Not declared: Zero's `mail-history-store` and `mail-label-store` suites
(owned by `scripts/zero_tests.py`; the thread label union and thread actions
are already ported there); `thread-short-summary` and `thread-ai-dispatch`
(an AI workflow); `mail-contact-store` (contact lookup, host-side);
`mail-search-*` (`search`); the IMAP suites' header-threading tests (another
provider); himalaya's listing and paging tests in `src/gmail/backend.rs`,
`src/pimdir/index.rs` and `tests/pimdir_search.rs` (they page Gmail or a
local index; under this plan paging is the host's, and they would be the
suites to port if (c) is ever built). jmap-client and the Stalwart server
have no thread tests in their pinned trees.

Our own 29 tests cover what no reference has: the order and its tie-break,
members awaiting import, drafts-only threads, a draft's labels staying out,
blocked actions, participants, independence from row order; that a thread
action reaches exactly the view's non-draft messages; the conformance cases
(including 1,200 ids and a 600-message thread across statement chunks, and
two accounts sharing a thread id); the migration on a populated database and
the query plan; the simulator's comparison; two end-to-end tests; and the
Python API with the host's feed loop as a worked example.

## 6. Slices

1. **The rule.** `thread.rs` with its unit tests and the ported summary and
   count suites, over `AccountMirror`. No storage change.
2. **Stores.** The migration, `Store::threads` on memory and SQL, admission
   through `thread_members`, conformance, the plan and migration tests.
3. **Driver and simulator.** `Runner::threads`, the end-to-end tests, the
   convergence comparison.
4. **Python and docs.** Bindings, stubs, `FakeGmail` threads, SPEC §4,
   §10.3, §11, §15 and the README.

## 7. Decisions

1. **Scope: (b), a read by thread ids and one index; listing stays with the
   host.** Reasons in §3. *Would change it:* two hosts needing the same
   label list, or a maintainer reading phase 4's exit criterion ("a host can
   render a full mailbox from the mirror") as "without tables of its own".
   Then build (c) on top of this plan; nothing here is discarded.
2. **Derived at read time, nothing stored.** No stored copy can drift, and
   there is no backfill. *Would change it:* a measured read that is too slow
   for threads of ordinary size, which a join on an index should not be.
3. **A batch read, in the order asked, unknown ids left out.** It matches
   `messages()` and makes a feed page one call. *Would change it:* nothing
   foreseeable; a single-id helper can be added in Python at no cost.
4. **Labels are the effective union over non-draft members; trash and spam
   members are included.** This is SPEC §4 and Zero's rule. A per-label view
   ("the thread as the inbox shows it") is a filter over `messages`. *Would
   change it:* `threads-label-union-matches-gmails-listing` showing Gmail
   lists differently, and then only the documentation.
5. **Order by `internal_date`, then id; `latest` is the newest imported
   non-draft member.** SPEC §4 and JMAP's order; a `latest` without an
   envelope is useless to a list row. *Would change it:* a host that needs
   Gmail's exact order if `threads-internal-date-is-gmails-order` is
   refuted.
6. **Participants are raw header values deduplicated by address, owner
   included, no Bcc.** The engine keeps header facts as sent (encoded words
   undecoded); deciding who "me" is needs aliases, which `identities` owns.
   *Would change it:* `message-facts` storing parsed addresses, in which
   case participants become those.
7. **No per-label counts.** They are the same read model as listing.
   *Would change it:* the same as Decision 1.
8. **The index is `(account, thread_id, id)` and ships as an ordinary
   migration.** The id makes the admission lookup index-only. Hosts with
   very large tables pre-create it concurrently. *Would change it:* a
   migration framework that can run statements outside a transaction.
9. **One joined statement, not a transaction.** It gives each chunk one
   snapshot on both databases without relying on isolation levels. The read
   carries no feed position; it is "now". *Would change it:* a snapshot API
   (roadmap R5) defining positions for reads, which this read would then
   adopt.
10. **No `ThreadChanged` entry.** Message entries already name the thread.
    *Would change it:* `threads-thread-id-is-stable` being refuted (a move
    would then need an entry of its own).

Unknowns (`scripts/features.py unknowns threads` prints the tests):

- `threads-thread-id-is-stable`: a message never changes thread; if wrong, a
  full observation still moves it and a feed entry is added.
- `threads-internal-date-is-gmails-order`: Gmail orders a thread by
  internalDate; if wrong, only presentation differs.
- `threads-label-union-matches-gmails-listing`: Gmail lists a thread under a
  label when any message carries it; if wrong, the host filters members.
- `threads-history-names-the-thread`: history and list entries always carry
  threadId; if wrong, the empty id is never grouped.
