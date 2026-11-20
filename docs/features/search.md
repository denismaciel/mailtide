# Search: a Gmail query to message and thread ids

Plan: `features/search.toml` (roadmap R9). Status: done.

## 1. Goal and scope

Afterwards a host can, per account:

- pass a Gmail search string (`from:alice has:attachment invoice`) and get one
  page of **message ids** (with their thread ids) that Gmail says match, or one
  page of **thread ids**;
- follow the page token to the next page;
- see, for every message hit, what the mirror holds: `imported`,
  `not_imported` or `unknown`;
- do all of it through mailtide's quota gate, so search spends from the same
  budget as sync and never pushes the account over Gmail's limit.

**Where the line is.** SPEC §1 keeps full-text search as a non-goal: an index
over the mirror's content belongs to the host. This feature is the other
thing: the query is forwarded to **Gmail's own search** and Gmail decides what
matches. mailtide does not parse the query, evaluate any term of it, rank,
build snippets, or decide that a message matches or does not. The answer is
"what Gmail's index says now", not "what the mirror contains".

Left out:

- an index, local evaluation, or a query language of our own;
- hydrated results: `search` returns ids, `messages(account, ids)` returns the
  views (the JMAP split between `/query` and `/get`);
- a result cache (Decisions 6) and a Gmail-query parser to correct results for
  pending local actions (Decisions 8);
- views of thread hits (`threads`), bumping a hit's import (`prioritise`),
  mapping a `DRAFT` hit to its draft id (`drafts`: the draft index has it);
- label-only questions ("unread in inbox", counts): those are queries on the
  mirror and cost no Gmail call. A search with a blank query is refused.

## 2. What the references do

Gmail facts are from Google's
[messages.list](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/list),
[threads.list](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.threads/list),
[filtering guide](https://developers.google.com/workspace/gmail/api/guides/filtering)
and [quota table](https://developers.google.com/workspace/gmail/api/reference/quota),
read 2026-11-20: `q` "supports the same query format as the Gmail search box"
and cannot be used with the `gmail.metadata` scope; `labelIds` must all match;
a page holds at most 500 entries, each only `id` and `threadId`;
`resultSizeEstimate` is an estimate; the API does no thread-wide search and no
alias expansion; a bare date is midnight PST and a value in seconds is exact.
`messages.list` costs 5 units and **`threads.list` costs 10** (not 5).

**Zero** (MIT). Search is `threads.list` with the user's `q`
(`driver/google.ts`). On top sit two things. `MailSearchStore` caches pages of
ids for 30 s, keyed by query, size, label set and token, fenced by a local
revision plus the history position, and shares one in-flight call between
identical requests. `mail-search-overlay.ts` parses the query and drops
threads whose locally pending labels make it definitely false. *Take:* the
pass-through, search in the foreground quota class, and the cache's rules as
rules for us (the query and token are never altered, label ids are a set, an
error is never an empty page, a page is stale once the mailbox moved).
*Leave:* the cache itself, the parser, folding folders into `q`, and
`resultSizeEstimate` used as a count.

**himalaya** (MIT/Apache). Two layers. The raw commands
(`src/gmail/messages/list.rs`, `threads/list.rs`) pass `q`, labels, size and
token through and return Gmail's page: our request shape. The unified search
(`src/gmail/search.rs`) translates himalaya's own filter tree into `q`, since
it serves five backends with one language. *Take:* `value_q`, which renders a
user's text as a literal that cannot add operators, and its notes on dates.
*Leave:* the language and its translation; page numbers emulated by listing
every earlier page (`page_ids`). Its `cairn/spec/search.md` states two rules
we keep: a page token is refused under another query, and a clause that cannot
be answered is refused, never dropped.

**jmap-client, Stalwart server.** No query tests in either checkout. Cited for
the model our API is named after (server paths are names only, AGPL):

| JMAP `Email/query` | Here |
|---|---|
| `filter` (structured) | `query`: Gmail's string, opaque to mailtide |
| `sort` | none: Gmail's order |
| `position`, `anchor` | none: a page token, forward only |
| `limit` | `limit`, clamped to 1..=500 |
| `calculateTotal` | `estimate` only, never a total |
| `collapseThreads` | `search_threads` |
| `queryState`, `canCalculateChanges` | `mirror_state`; always "cannot": run it again |

**pimalaya core** has an older copy of himalaya's query language for IMAP,
Maildir and notmuch, and no Gmail API backend. **mail-parser, mail-builder,
mail-send:** nothing for search.

## 3. Design in mailtide

**Approach.** `search` is a read-only driver call, as `Runner::drift` is: one
Gmail request through the quota gate, then one store read to mark the hits.
It writes nothing: no commit, no row, no job, no feed entry, no epoch. It
never claims the account, so it cannot supersede a running sync.

### Public API

Rust (`mailtide_io::Runner`):

```rust
search(account, &SearchQuery, budget) -> Result<SearchPage, SearchError>
search_threads(account, &SearchQuery, budget) -> Result<ThreadSearchPage, SearchError>
```

| `SearchQuery` field | Meaning |
|---|---|
| `query: String` | Sent as `q`, byte for byte. Blank is refused (`EmptyQuery`) |
| `label_ids: Vec<LabelId>` | Gmail label **ids**, all required (`labelIds`). Sorted and deduplicated; an id the mirror's label definitions lack is checked with `labels.get`, and one Gmail does not have is refused (`UnknownLabel`) |
| `include_spam_trash: bool` | Default false, as the Gmail search box. Only a query without `in:trash`, `in:spam` or `in:anywhere` depends on it |
| `limit: u32` | Default 100, clamped to 1..=500 |
| `page_token: Option<SearchPageToken>` | From the previous page of the same request |
| `priority: Priority` | Default `Foreground`; `Live` or `Backfill` for background jobs |

`SearchPage { hits, next_page_token, estimate, deleted, mirror_state, cursor, phase }`,
`SearchHit { message_id, thread_id, state }`.
`ThreadSearchPage` is the same with `ThreadHit { thread_id, history_id: Option<HistoryId> }`
and no `deleted`.

- `hits` keep Gmail's order. mailtide does not sort, merge or deduplicate.
- `estimate` is Gmail's `resultSizeEstimate`: a hint for a UI, not a count.
- `mirror_state` is the account's change-feed state, read **before** Gmail was
  asked; `cursor` and `phase` are the sync position read with it.

`SearchError`: `EmptyQuery`, `InvalidPageToken` (not ours, or issued for
another request; no Gmail call), `UnknownLabel` (a `label_ids` entry
Gmail's `labels.get` answered 404 for; no search sent), `PageTokenRejected` (Gmail
refused a continuation), `Refused` (Gmail's 400 or 403, an account problem,
an answer that is not a page), `Deferred { until }`, `Retryable`,
`AccountBlocked` (with its reason), `NotTracked`, and the runner's store and
gate errors.

Python: `Runner.search(account, query, *, label_ids=(), include_spam_trash=False,
limit=100, page_token=None, priority="foreground", budget_s=10.0)` and
`search_threads(...)`, on `Runner` and `BlockingRunner`; frozen dataclasses
`SearchPage`, `SearchHit`, `ThreadSearchPage`, `ThreadHit`; exceptions
`SearchRefused` and its subclass `UnknownLabel` (`label_id`),
`InvalidPageToken` and its subclass `PageTokenRejected`, the shared
`QuotaDeferred` (`until_ms`), `GmailUnavailable`, `AccountBlocked`;
`AccountNotTracked` exists; a blank query is a `ValueError`. Two pure
helpers, `mailtide.search_literal(text)` and
`mailtide.search_by_message_id(id)`. The binding takes the keyword
arguments as `**options` and checks them itself (an unknown one is a
`TypeError`), so no function takes more arguments than the lint allows.

### The query

The string goes to Gmail untouched: not trimmed, not case-folded, no folder
or category rewritten in. mailtide adds nothing to it.

`mailtide_gmail::search` has two pure helpers for a host that builds a query
from user text (a "from" chip, a lookup by Message-ID). They build one value,
not a query:

- `literal(text) -> Option<String>`: the text as a value that cannot add
  operators or groups. Inside double quotes Gmail still reads `OR`, `|`,
  braces, `AND` (consumed as syntax) and `AROUND`, a backslash escapes
  nothing, and a colon after a word makes the phrase unreliable; lower-case
  `or`/`and` are plain words, and parentheses, `+` and `-` are punctuation
  (`search-literal-quoting`, refuted). So the text is ASCII-lowercased (the
  operators are upper case only; matching ignores case), quotes, braces,
  parentheses, `|`, `\` and `:` become spaces, and anything but one bare
  word is double-quoted. `None` when no letter or digit is left, never a
  value that means something else. What cannot be searched as a literal:
  case, and punctuation that alone tells two texts apart (`from:alice` is
  searched as the phrase `from alice`).
- `by_message_id(id) -> Option<String>`: `rfc822msgid:` plus the id without
  angle brackets, bare (Gmail matched ids holding `+ - = / $ ! %` bare,
  ignoring case: `search-msgid-match`). `None` for an id Gmail's syntax
  cannot carry (a blank, a quote, a brace, a parenthesis, an angle
  bracket): refused, never loosened. It finds the `Message-ID` **Gmail
  stored**. Gmail replaces a client's `Message-ID` on every API send and
  draft save (rule "A message mailtide writes is identified by a private
  header" in `docs/features/README.md`), so it cannot find a message
  mailtide just sent; `send` does not use it.

**Dates.** A host that needs an exact bound writes `after:1794646800` (Unix
seconds); `after:2026/11/14` means midnight PST.

**Spam and Trash.** `in:trash`, `in:spam` and `in:anywhere` reach Trash and
Spam whatever `include_spam_trash` says (`search-spam-trash-default`); the
flag matters only for a query naming none of them, which then searches Spam
and Trash too when it is set.

**Labels.** Two different things, and mailtide translates neither:

- `label_ids` takes ids (`INBOX`, `Label_12`), the same ids the mirror stores
  (SPEC §6.6). It survives a rename and is the exact way to restrict a search
  to a label.
- Inside `query`, `label:` and `in:` are Gmail's operators and take **names**
  as the search box does (`search-label-terms`). mailtide does not rewrite a
  name to an id or an id to a name: that would mean parsing the query, and
  the mirror's label table can trail a rename.

`label_ids` **are** checked, before the search: Gmail answers a `labelIds`
value it does not have (never created, or deleted) with 200 and no hits,
which cannot be told from an empty result (`search-unknown-label-id`,
refuted). An id the mirror's label definitions (`Store::labels`) hold is
sent as it is. An id they lack (deleted, created since the last listing, or
any id before the first listing) is asked about once per call with
`labels.get` (1 unit, the search's priority): a 404 is `UnknownLabel` and no
search is sent; an answer means the mirror is behind, and the search goes on.
Nothing is recorded (search writes nothing; the engine's label refresh
brings the definition). Any other failure of that call maps as the search's
own (a 5xx is `Retryable`, a deferral `Deferred`, a 400 `Refused`). Gmail
answers a label *name* in `labelIds` with 400; a name is not a mirror id, so
it is checked with `labels.get` first. Inside the query, `label:` matches
names in the search box's spellings (lower case, hyphens for spaces and
slashes, or quoted) and never ids (`search-label-terms`).

### Paging

`SearchPageToken` is `s1.<binding>.<Gmail's token>`. The binding is a 64-bit
FNV-1a hash of the account, the scope (messages or threads), the query bytes,
the canonical label ids, `include_spam_trash` and the clamped `limit`. Gmail's
token follows verbatim. A token presented with any other request is
`InvalidPageToken` before Gmail is called. It is a guard against misuse, not a
secret, and nothing is stored.

That binding is the only protection there is. Gmail's `nextPageToken` is a
bare position (a 20-digit number) that Gmail accepts under any other request:
another `q`, `maxResults`, label filter or flag, or a made-up number, all
answer 200 with some page or an empty one; only a token that does not parse
is a 400 (`search-page-token-rejection`, refuted). A token stays valid for at
least an hour and behaves like "older than this point", also while the
mailbox changes (`search-page-token-lifetime`, confirmed; 24 hours and 7 days
not measured).

Because a continuation repeats a request Gmail already accepted, a 4xx on it
is attributed to the token: `PageTokenRejected`, and the host restarts from
the first page.

Paging is forward only and is **not a snapshot** (SPEC §5.6): when the mailbox
changes between two pages an id can repeat or be skipped. A page may be empty
and still carry a token; the run ends on a page without one.

### Gmail calls and quota

| Call | Units | Used by | Priority |
|---|---|---|---|
| `messages.list?q=` | 5 | `search`, one per page | foreground by default |
| `threads.list?q=` | 10 | `search_threads`, one per page | foreground by default |
| `labels.get` | 1 | a filter label the mirror lacks, once per call | the search's |

Nothing else is called: hits are marked from the mirror. At the default
account budget (4,800 units a minute) that is 960 message searches a minute; a
search box debounced at 300 ms spends at most 1,000 units a minute. Hosts
should debounce and not search on every keystroke.

A gate deferral is waited out within `budget`, then returned as `Deferred`:
nothing sent. A background job passes `priority = Backfill` so it yields to
user-facing work.

### Store

**Compatibility.** Nothing is persisted: no row, log entry, job or column,
so the schema version does not move, a database is the same to every build,
and rolling back needs no statement. A page token is not stored by mailtide;
an older build has no `search` to give it to.

One new read, no migration:
`Store::locate(account, ids) -> Vec<(MessageId, Located)>` with
`Located::Row { imported }`, `Tombstone` or `Absent`. It reads the message and
tombstone tables by primary key (at most 500 ids) and, like `status`, claims
nothing. `Store::messages` cannot serve here: it leaves out unknown and
deleted ids alike.

Thread hits are not marked: counting a thread's rows needs an index on
`thread_id` the schema does not have. That index and thread views belong to
`threads`.

### Meeting the sync engine

Gmail's search index and the mirror are two views of one mailbox that move at
different times. Per hit:

| Situation | What `search` returns | What the host does |
|---|---|---|
| Row with content | `imported` | `messages()` |
| Row, body not fetched yet | `not_imported` | `messages()` for labels; `prioritise` to have the body fetched first |
| No row, no tombstone: Gmail is ahead of the cursor, or bootstrap has not listed it | `unknown` | `run()`, or wait for the next sync; history or the listing brings it |
| Tombstone: Gmail's index trails a delete | dropped, counted in `deleted` | nothing (I5: deletion is final) |
| A pending action changed its labels here | still a hit | `messages()` shows effective labels and `pending_actions > 0` |
| It matches now but Gmail has not indexed it | absent | search again when the feed moves |

Absence from a result never proves anything and is never used to delete.

**An `unknown` hit cannot be opened at once, and that is decided, not
pending.** `search` returns it as `unknown`; the host calls `run` or waits
for the next sync, which brings the row; `prioritise` only raises import
jobs that exist, and reports such an id as `unknown` too (its D7). No call
in either plan fetches a message the mirror has no row for.

**Staleness.** Gmail has no query state. `mirror_state` is the nearest honest
thing: if `changes(account, since = mirror_state)` returns a discovered or
deleted message, a label change or an overlay change, the page may be stale
and the host runs the query again. Import entries do not change membership
(Zero: "ordinary backfill imports do not invalidate searches"). Because it is
read before the Gmail call, a change that lands during the call is after it. A
host cache should also set a time limit: `mirror_state` cannot see a change
the mirror has not synced or Gmail has not indexed (`search-index-lag`).

**Requests and events.** The driver issues `GmailRequest::Search(SearchRequest)`
(the canonical request: scope, query, sorted label ids, flag, clamped limit
and Gmail's own token) through `Runner::call_gmail`, with a command id of
epoch 0, as `drift` does, and reuses the runner's gate, lease guard and
logging (the request kind is `search_messages` or `search_threads`); the
answer is `EventKind::Searched`, which the engine ignores. The simulator is
not extended: search has no state and no invariant beyond "writes nothing",
which a driver test checks; its Gmail only refuses a search request, which
the engine never issues.

### Failure and retry

mailtide never retries a search; it is idempotent, so the host may.

| Gmail answers | Result |
|---|---|
| 400, 403 (a query it rejects, `gmail.metadata` scope) on a first page | `Refused`. The caller's error: the account is **not** blocked |
| 404 to the `labels.get` of a filter label | `UnknownLabel`; no search is sent |
| 4xx (refused for good) or 404 on a continuation | `PageTokenRejected` |
| 404 on a first page | `Refused` |
| 429, `userRateLimitExceeded`, project limits | `Retryable`; the gate is cooled down for `Retry-After`, as the engine would |
| 5xx, timeout or lost answer, 401 (token dropped and re-minted next call) | `Retryable` |
| An account problem (delegation revoked, mail disabled) | `Refused`; nothing is committed, the next `run()` records the block |
| A body that is not a list page | an error, never an empty page |

A blocked account is `AccountBlocked` before any credential or Gmail call; an
untracked one is `NotTracked`. Read-only mode allows search (it needs only
`gmail.readonly`). The query, label ids and Gmail's error text are never
logged.

## 4. Alternatives rejected

- **A structured filter (JMAP's, himalaya's):** Gmail's language is larger
  than any tree we would define and Google extends it; every operator we did
  not model would be lost.
- **Hydrated hits:** makes every page pay for labels, intents and content the
  caller may not need, and blurs "Gmail's answer" with "the mirror's rows".
- **Fetching unknown hits from Gmail:** 20 to 60 units each on a 5-unit call;
  the sync brings them anyway.
- **Admitting unknown hits as provisional rows (as a reconciliation listing
  does):** an unfenced write without a baseline, to save one sync pass.
- **Returning tombstoned hits with a `deleted` state:** every caller would
  have to filter them; the count keeps the information.
- **Gmail's raw page token:** a token replayed under another query has no
  documented meaning.
- **Page numbers (himalaya):** page n costs n pages.
- **`getProfile` per search to tell "mirror behind" from "drift":** one more
  call per page for a hint; `cursor` and a sync answer it.
- **`threads.list` only (Zero):** twice the cost, and it hides which message
  matched.
- **`drafts.list?q=` for drafts (Zero):** `in:draft` hits plus the draft index
  give draft ids without another endpoint (`search-drafts-in-results`).
- **Marking the account active on a search:** it would be a write.

## 5. Tests

Suites were found with `grep -ri "search\|query"` over every checkout's test
files, then read.

Declared:

- Zero `mail-search-store.test.ts`: 4 mapped as request, token and staleness
  rules (renamed, since we have no cache), 2 `na` (the cache's lifetime and
  size).
- Zero `mail-search-overlay.test.ts`: 1 mapped (hits hydrate with overlaid
  labels), 5 `na` (the query parser, Decisions 8).
- Zero `imap/search.test.ts`: `rest_na` for its 6 tests, IMAP only.
- himalaya `src/gmail/search.rs`: 5 mapped onto `literal()` and
  `by_message_id()`, 14 `na` (the filter tree).
- himalaya `src/gmail/messages/list.rs`: its one test, as a decode test.
- himalaya `src/gmail/backend.rs`: 2 paging tests mapped, 5 `na`, and 10
  deferred: 1 to `send`, 2 to `label-edit`, 2 to `message-facts`, 3 to
  `purge` (the permanent delete) and 2 to `delivery-retries` (retries of
  throttled batch parts). Only 5 of its 17 tests are about listing; the
  suite is declared for those, and every other test is named here so that
  no plan needs a `rest_na` on it.
- himalaya `tests/pimdir_search.rs`: 2 mapped for the paging contract (the
  reference pages a local index), 2 `na`.

Not declared: Zero `driver/imap.test.ts` (its two search tests are already
`na` in `drafts`, rightly: IMAP) and `mail-count-store.test.ts` (counts, a
mirror projection); himalaya `src/email/search/*`, `tests/envelope_search.rs`
and pimalaya core's `search_emails_query.rs` (a query language and a local
matcher: the host's side of the line); `tests/google.rs` (live). jmap-client
and the Stalwart server have no query tests in their pinned trees.

Our own 48 tests cover the request shape and costs; the token binding; the
mirror state of hits (unknown, not imported, tombstoned); "writes nothing" and
"does not supersede a sync"; the gate (deferral, priority, cooldown); every row
of the failure table; label ids the mirror lacks; read-only, blocked and
untracked accounts; logs (in a process of their own); thread
search; and the Python API.

## 6. Slices

1. **Wire.** `GmailRequest::Search`, costs, request building, page decoding,
   `literal` and `by_message_id`, with the himalaya tests.
2. **Rules and store.** `mailtide::search` (request rules, token binding,
   marking hits), `Store::locate` in the three stores, conformance.
3. **Message search.** `Runner::search`: gate, budget, errors, read-only,
   blocked accounts; the Zero and paging tests.
4. **Thread search.** `Runner::search_threads`.
5. **Python.** Bindings, stubs, `FakeGmail` scripted results, SPEC §1, §8,
   §10, §11.

**Shared groundwork.** No separate plan owns the pieces several plans need;
each is added by whichever plan lands first, and the later ones use it as it
is:

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
store read and write. Map `CallError::OutcomeUnknown` (a timeout) to
`Retryable`, and a continuation's `NotFound` (404) to `PageTokenRejected`.

Slices 1 to 3 are a usable feature. The `[[unknown]]` checks run before
slice 1.

## 7. Decisions

1. **The query is a raw string, passed untouched.** A filter type would be a
   second language to keep in step with Gmail. *Would change it:* another
   provider behind the same API.
2. **Two quoting helpers ship with it.** Hosts will build `from:<user text>`
   and look a received message up by its Message-ID; the quirks of Gmail's
   quoting belong in one tested place. (`send` no longer looks its own
   message up by Message-ID: Gmail replaces it, so mailtide identifies what
   it writes by a private header.) They build a value and parse nothing.
   Plain quotes are not enough (`search-literal-quoting`, refuted): Gmail
   reads operators inside them, so `literal()` neutralises each one it was
   measured to read and refuses what it cannot render. *Would change it:*
   Gmail gaining an escape character.
3. **Hits are ids with a mirror state; hydration is `messages()`.** JMAP's
   split, and it keeps a page cheap. The state is needed because `messages()`
   cannot tell "not synced yet" from "deleted". *Would change it:* hosts
   always hydrating every page; then add an option, not a second shape.
4. **Search writes nothing; unknown hits wait for the sync.** No fence, no new
   failure mode, safe beside a running sync. Opening a hit the mirror does
   not know is not built now, by decision: `search` returns it as `unknown`,
   the host calls `run` or waits for the next sync, and `prioritise` only
   raises jobs that exist (`prioritise` D7 says the same). *Would change
   it:* a host that must open such a hit at once; the fenced fetch that
   would take is sketched in `prioritise` D7 and belongs there.
5. **Tombstoned hits are dropped and counted.** Deletion is final (I5).
6. **No result cache and no shared in-flight calls; pages carry
   `mirror_state`.** A page costs 5 units, so a cache buys latency for a UI,
   which is the host's trade to make; a durable one needs a table, pruning and
   a time limit. `mirror_state` gives a host the fence Zero's cache has.
   *Would change it:* several hosts building the same cache.
7. **Page tokens are wrapped and bound to their request.** A defined refusal
   instead of Gmail's behaviour, at no storage cost: Gmail accepts its token
   under any request and answers some other page
   (`search-page-token-rejection`, refuted), so this is the only guard.
8. **No Gmail-query parser, so no correction of results for pending actions
   (Zero's overlay).** A host with the full mirror answers label-only views
   locally with the overlay applied, which is the case Zero built it for. For
   mixed queries the window is short (until delivery and confirmation, about
   a minute) and `messages()` shows the effective labels. A parser would rest
   on precedence rules Google does not document, and a wrong parse drops a
   real result silently. *Would change it:* hosts asking for it; Zero's
   matcher is pure, MIT and has five tests, so it can be added as an opt-in
   filter without changing this API.
9. **Thread search is included, without mirror state.** A conversation list
   needs pages of threads (JMAP `collapseThreads`), and `threads.list` is the
   only way to get even pages. Marking thread hits waits for `threads`, which
   owns the index.
10. **Foreground by default, with a `priority` field.** A user is usually
    waiting; a background job must be able to say it is not.
11. **Labels are not translated either way.** `label_ids` for ids, Gmail's
    own operators for names. Ids are checked, because Gmail answers an
    unknown id with no hits (`search-unknown-label-id`, refuted): one the
    mirror's definitions hold goes out as it is, one they lack is asked about
    with `labels.get` (1 unit, once per distinct id per call), so a label
    created since the last listing (or before the first) is searched and a
    deleted one is `UnknownLabel`. Refusing on the mirror alone would refuse
    new labels for up to `label_refresh`; asking Gmail every time would
    charge every filtered search. *Would change it:* Gmail answering an
    unknown id with an error.
12. **A refused search never blocks the account**, and a rate limit cools the
    gate but is not persisted: search commits nothing. The next engine request
    records what is still true.
13. **A blank query is refused.** A listing without a query is the mirror's
    to answer and would duplicate the sync's enumeration.

Unknowns (`features/search.toml`):

- `search-page-token-rejection` (refuted): Gmail's token is a bare position it
  accepts under any other request; only an unparsable one is a 400. The
  binding is the only protection, as planned.
- `search-page-token-lifetime` (confirmed): a token lives at least an hour,
  also while the mailbox changes.
- `search-index-lag` (confirmed): no measurable lag behind history.
- `search-literal-quoting` (refuted): `OR`, `|`, braces, `AND` and `AROUND`
  are read inside quotes, nothing escapes, a colon is unreliable; `literal()`
  lowercases and splits them away.
- `search-msgid-match` (confirmed): `rfc822msgid:` matches one id, bare,
  any case, `+ - = / $ ! %` included.
- `search-label-terms` (confirmed): `label:` takes names, not ids;
  `labelIds` takes ids and answers a name with 400.
- `search-unknown-label-id` (refuted): an unknown `labelIds` value answers
  200 with no hits, so an id the mirror lacks is checked with `labels.get`
  (404 for a deleted or never-created label).
- `search-spam-trash-default` (refuted in one detail): `in:trash` and
  `in:spam` work without `includeSpamTrash`, and so does `in:anywhere`; only
  a query naming none of them depends on the flag. Guidance only.
- `search-thread-history-id` (confirmed): every listed thread carries
  `historyId`, its newest message's.
- `search-drafts-in-results` (confirmed): `in:draft` returns a draft's current
  message, never an older one.
