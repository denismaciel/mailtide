# Integration review (fable), 2026-11-19, HEAD `7c40c0e`

Scope: can an existing production Python service add the wheel, point it at
its database, bucket, credentials, scheduler and logging, and have it work
safely? I read the docs, the Python surface, the driver, the stores and the
core types, and checked every claim below against the code. No files other
than this one were touched; nothing was run.

Note: while this was written the working tree gained uncommitted changes
(`crates/mailtide-py/src/lib.rs`, `runner.rs`, `quota_gate.rs`, the stubs,
`tests/fake_gmail.py`, `tests/test_async.py`) that appear to add an async
`Runner` on pyo3-async-runtimes, a shared process-wide Tokio runtime, a
token source run on Tokio's blocking pool, lease release on cancellation and
a reusable fake Gmail. They were not reviewed; all line references below are
to HEAD `7c40c0e`. Where they land, they touch P1-5 (async, runtime threads),
P1-2 (token calls) and P1-8 (shipped fake), and my "no native asyncio
bindings" recommendation under "What not to do" should be weighed against
whatever that branch demonstrates.

Short verdict: the core is in good shape and the Python surface is small and
honest, but four things block a production integration today: the database
pool is hard-coded, there is no way to read status or admit an action without
preempting the running sync, blocked accounts can never be unblocked, and the
change feed is too thin for a host pipeline to consume. Everything else is
friction or polish.

## P0: blocks a production integration

**P0-1. Database pool is hard-coded and eager.**
`crates/mailtide-store/src/sql.rs:180` sets `max_connections(8)` for Postgres,
with sqlx defaults for acquire timeout (30 s), idle and lifetime; `:182`
connects eagerly, so building a `Runner` in an API process opens a connection
it may never use. Five workers plus API replicas would take 40+ of a host's
remaining ~40 connections. A run uses at most one connection at a time (all
store calls are sequential awaits), so 2 is enough per worker. Unix-socket
DSNs already work through sqlx (`postgres://u:p@localhost/db?host=/cloudsql/…`
or a percent-encoded host) but are undocumented, and `:167-171` treats every
non-`sqlite:` URL as Postgres instead of rejecting unknown schemes.

Proposed: `SqlStore::connect_with(url, PoolConfig { max_connections,
min_connections, acquire_timeout, idle_timeout, max_lifetime, lazy: bool })`,
default `max_connections=2, lazy=true`; validate the scheme (`postgres`,
`postgresql`, `sqlite`) and fail otherwise. Surface it in the Python config
(below). Document the socket DSN form in the README.

**P0-2. The only snapshot read is `claim`, which fences the live run.**
`Store` (`crates/mailtide-store/src/lib.rs:43-94`) has no read-only snapshot;
`claim` (`sql.rs:606-660`) bumps the epoch and releases running jobs. So the
status an operator needs (`Status`, `crates/mailtide/src/status.rs:96`) is
reachable only through `run()`, and every `run`/`act` from any process
supersedes whatever run holds the account. `Runner.act` (`crates/mailtide-py/
src/lib.rs:445-483`, `crates/mailtide-io/src/runner.rs:171-185`) admits then
calls `run` with a 30 s budget, so a UI request path blocks for up to 30 s and
preempts the worker's import mid-pass each time. The fencing is correct; the
API just offers no cheaper verbs.

Proposed:
- `Store::peek(account) -> Option<StoreSnapshot>` (no epoch change) and
  `Runner.status(account) -> Status | None` built from it via
  `Engine::status`. Add `last_run_at_ms`, `watch_expiration_ms`,
  `jobs_ready/running/blocked` counts and `notification_lag` once `run`
  accepts a push id.
- `Runner.act(..., deliver: bool = True)`: with `deliver=False` admit only
  and return `admitted`; the scheduler's next run delivers (the engine already
  does this when a run is superseded). Document that `deliver=True` is for
  single-process hosts.
- `Runner.accounts() -> list[str]` from `mailtide_accounts`, for operator
  pages.

**P0-3. Blocked accounts can never be unblocked.**
`engine.rs:1147-1160` stores `SyncState::Blocked { reason, resume }` and
`:211-213` makes every later run stop at once. Nothing in the store, runner
or bindings clears it; the SPEC (§6.4, §13) says "retried only by an operator
action" but the action does not exist. A suspended-then-restored user, or a
transient delegation hiccup classified as `DelegationRevoked` by the host's
token source, is permanent until someone edits the `sync` JSON column by
hand. Also, first contact with an unknown email silently creates an account
row (`sql.rs:612-628`), and there is no way to remove one.

Proposed: `Store::unblock(account)` restoring `resume` and re-readying
blocked intents; `Runner.unblock(account)`. `Runner.forget(account,
delete_blobs: bool)` deleting all rows for the account (and blobs under its
prefix) for shadow-mode resets and offboarding. Optionally
`Runner.reconcile(account)` to force §6.3.

**P0-4. The change feed cannot drive a host pipeline.**
`Change` (`crates/mailtide/src/revision.rs:67-76`) is `Message(id)`,
`MessageDeleted(id)`, `Thread(id)`, `Labels`. `Message` is emitted for
creation, label patches, import completion and every intent change
(`mirror.rs:878, 894, 934, 971, 1094`). A consumer that needs "newly
imported", "labels removed" or "direction" must call `messages()` for every
id and keep its own copy of the previous labels to diff, which is the mirror
reimplemented host-side. The Python shape is also mis-typed:
`_core.pyi:34-37` says `list[dict[str, Any]]`, but serde's externally tagged
encoding yields `{"Message": "18c…"}` for data variants and the bare string
`"Labels"` for the unit variant, so the list mixes dicts and strings.

Proposed: keep the log append-only and the state string as is, but make
entries carry what the commit knew:

```rust
pub enum Change {
    MessageCreated { id, thread_id, labels, checkpoint },
    MessageImported { id, thread_id, labels, internal_date, blob, size },
    LabelsChanged { id, thread_id, added, removed, checkpoint, source: Gmail | PendingAction },
    MessageDeleted { id, thread_id },
    Thread(ThreadId),
    Labels,
}
```

Python: a frozen `Change` class with `kind: Literal[...]` and typed optional
fields, not dicts. All data is in the commit at emission time, so this costs
no extra reads. Also add `MessageView.blob_ref` and a module-level
`blob_key(account, id)` so a host can read the object with its own client or
sign URLs (`crates/mailtide-io/src/blobs.rs:114-116` already defines the
layout).

## P1: big friction

**P1-1. Logging and observability.** The whole workspace has one
`tracing::warn!` (`runner.rs:309`); nothing reports runs, Gmail calls,
commits, rejections, cooldowns or blocks. Recommendation:

- Instrument Rust with `tracing` only: a span per run (`account`, `epoch`,
  `run_id`) and events with a stable vocabulary and fields
  (`mailtide.run.stop {stop, commits, imported, gmail_requests, duration_ms}`,
  `mailtide.gmail.request {kind, status, cost, duration_ms}`,
  `mailtide.commit.rejected {reason}`, `mailtide.quota.deferred {until}`,
  `mailtide.account.blocked {class}`). Field values are ids, counts, classes
  and durations; never `HeaderFacts`, label names or token secrets
  (`AccessToken` already redacts `Debug`, `auth.rs:45-51`). Account ids are
  emails, so log a short hash by default with `log_account_ids="plain"` to
  opt in.
- Bridge to Python with a custom `tracing_subscriber::Layer` installed by
  `mailtide.configure_logging(level="INFO", account_ids="hash")`: each event
  becomes a `logging.LogRecord` on logger `mailtide.<target>` with fields in
  `extra`, so the host's JSON handler emits them unchanged. Filter by level
  in Rust before attaching to Python, so disabled events never cross the
  boundary; that keeps per-request logging cheap on GIL builds.
- Metrics: no OpenTelemetry dependency in the library. `RunOutcome` and
  `Status` already carry the counters a host needs; hosts emit metrics from
  them in the scheduler. Add a `Hooks` callback object only if a real host
  needs per-request metrics. Why: Python `logging` is what every host has,
  OTel couples SDK versions across the FFI boundary, and a callback per event
  from Tokio threads is the most expensive path, so filter first.

**P1-2. Token source is called once per Gmail request, and `expires_at` is
unused.** `runner.rs:388` mints a token per request; nothing reads
`AccessToken.expires_at` (grep: only `auth.rs`, `lib.rs`). A bootstrap of
100k messages calls into Python 100k+ times, each attaching to the
interpreter. Cache per account in `mailtide-io` until `expires_at - 60 s`,
and treat the Python callable as the miss path. Document that the callable
is invoked from several Rust threads at once.

**P1-3. Migrations from the host's tool.** `migrate()` (`sql.rs:188-227`) is
sound (advisory lock, `mailtide_schema` versions, append-only list at
`:47-74`), but it runs on mailtide's own pool, outside the host's migration
transaction, and nothing checks the schema at runtime: a deploy that forgot
it fails later with a raw sqlx error. Proposed: `mailtide.migrations(dialect)
-> list[Migration(version, statements)]` plus the convention "insert into
`mailtide_schema`", so an Alembic revision can execute them on its own
connection; `Runner.schema_version()` returning `(expected, applied)`; and on
first use raise `SchemaOutOfDate` when they differ (opt-out
`schema_check=False`). Forward-only; document the drop list. Table prefix is
hard-coded in every statement; do not template it. For a separate schema,
document `?options=-c search_path=mailtide,public` in the DSN. Note the fixed
advisory lock key `7366716` (`:194`) in the docs so hosts avoid it.

**P1-4. Change log grows forever.** `write_diff` only inserts
(`sql.rs:591`); nothing prunes `mailtide_changes`. A 100k bootstrap leaves
100k rows per account permanently. Add `Runner.compact(account, keep_after:
state)` or a `change_log_retention` config applied at the end of a run; a
`since` older than the retained range should return a distinct
`StateTooOld` so consumers know to resnapshot (JMAP's `cannotCalculateChanges`).

**P1-5. Async and threading.** Keep the blocking API; `asyncio.to_thread(
runner.run, …)` is the right call and should be in the README. Native
awaitables via pyo3-async-runtimes would bind a Runner to one loop and buy
nothing for five workers. What is missing is cancellation and progress:
`run` honours only its own deadline (`runner.rs:215`), so a durable-workflow
activity cannot heartbeat or stop early. Proposed: `run(account, budget_s,
on_progress: Callable[[Progress], bool] | None = None)`, called between
commands with the running `SyncReport`; returning `False` stops with
`stop="cancelled"` after the current commit. Expose `runtime_threads`
(`lib.rs:380-383` builds a multi-thread runtime with one worker per CPU per
Runner; default 2). Free-threaded: the module is `gil_used = false` with
frozen classes (`lib.rs:14`); that holds as long as the host's token callable
is thread-safe, which the docstring should say.

**P1-6. Error types are stringly.** `error()` (`lib.rs:71-73`) formats
`StoreError`/`BlobError` with `Debug` into one `MailtideError`, so a host
cannot tell `Unavailable` (retry) from `OutcomeUnknown` (retry, but the
commit may have applied) from `Corrupt` (page someone). Add subclasses
`StoreUnavailable`, `OutcomeUnknown`, `Corrupt`, `BlobUnavailable`,
`SchemaOutOfDate`, keeping `MailtideError` as the base; put the class in
`.kind` too.

**P1-7. Shadow mode and cutover.** Nothing stops it today, but three gaps
make "compare, then switch" manual:
- Gmail keeps one watch per user. With `watch_topic` set to a topic other
  than the host's existing one, mailtide's `users.watch` replaces the host's
  push (`runner.rs:342-351`). Document: in shadow mode use the host's topic
  or `None`; polling at `poll_active` covers it.
- The drift check in SPEC §13 and phase 3's exit criterion do not exist in
  code. Add `Runner.drift(account, sample: int) -> DriftReport` fetching
  `format=minimal` for a sample through the quota gate and comparing labels
  with the mirror. This is what the host runs daily during shadow.
- `Runner.state(account) -> str` returning the current head, so the consumer
  can start the change feed at cutover without paging through the backfill.
  `run()` should also accept the push's `history_id` so `PushReceived`
  (`engine.rs:308`) is used and notification lag appears in status.

**P1-8. Host testing.** The fake Gmail lives in `tests/test_end_to_end.py:
73-172` and is not shipped; the runner reads the wall clock
(`crates/mailtide-io/src/lib.rs:21`). Ship `mailtide.testing.FakeGmail`
(the HTTP server, with `add`/`modify`/`delete`) and accept `now_ms:
Callable[[], int] | None` in the config so hosts get deterministic time. With
`sqlite::memory:` and `memory:///` that is a complete offline harness.

## P2: polish

- HTTP (`transport.rs:229-238`): no `User-Agent` (set `mailtide/<version>`),
  no connect timeout, proxy only via environment. Expose `connect_timeout_s`,
  `user_agent`, `proxy`. Blob credentials come from the environment through
  `object_store::parse_url` (`blobs.rs:128-136`); accept `blob_options:
  dict[str, str]` and pass them to `parse_url_opts`.
- `Budget::GMAIL_DEFAULT` is fixed at `lib.rs:411`; a host that also calls
  Gmail itself needs a lower share. Expose `Budget` fields in config.
  Plugging the host's own limiter table is `QuotaGate`, which already exists
  in Rust; leave Python out of that path for now.
- `JOB_CONCURRENCY` (`engine.rs:69`) is a const; make it an `EngineConfig`
  field.
- Frozen value classes lack `__repr__`, `__eq__` and a `to_dict()` for
  structured logs.
- `act` takes `action: str`; the `Action` enum is right in Rust. A
  `Literal` alias is acceptable, but `restore`/`not_spam` silently default
  `destination="inbox"`; make it required for those two.
- `read_blob` copies through the Tokio runtime; fine for now, but with
  `blob_ref` exposed hosts can stream from their own client.

## One coherent configuration

Python, replacing the eight positional/keyword arguments at
`_core.pyi:98-108`:

```python
@dataclass(frozen=True)
class Config:
    database_url: str
    blob_url: str
    token_source: Callable[[str], tuple[str, int]]
    watch_topic: str | None = None                # None: poll only, never call users.watch
    database: DatabaseOptions = DatabaseOptions() # max_connections=2, min_connections=0,
                                                  # acquire_timeout_s=10, lazy=True, schema_check=True
    http: HttpOptions = HttpOptions()             # request_timeout_s=60, connect_timeout_s=10,
                                                  # user_agent=None, gmail_origin=None, proxy=None
    engine: EngineOptions = EngineOptions()       # poll_active_s=60, poll_idle_s=900, idle_after_s=3600,
                                                  # watch_renew_margin_s=3600, draft_settle_s=120,
                                                  # confirm_timeout_s=60, job_concurrency=4
    quota: QuotaBudget = QuotaBudget.gmail_default()
    runtime_threads: int = 2
    change_log_retention_s: int | None = 7 * 86400
    blob_options: dict[str, str] = field(default_factory=dict)
    now_ms: Callable[[], int] | None = None       # tests only

runner = mailtide.Runner(Config(...))
```

Rust: `mailtide_io::runner::RunnerConfig` already holds `engine` and `watch`;
add `pool: PoolConfig`, `http: HttpConfig`, `budget: Budget`,
`change_log_retention: Option<Duration>`, and have `mailtide-py` build the
store, transport and gate from it. Validation at construction, raising
`ValueError`: finite non-negative durations; `max_connections >= 1`;
`poll_active <= poll_idle`; budget fields at or below Google's published
limits (warn, not fail); known URL schemes for both stores; `job_concurrency
in 1..=8`.

## Must have for the first integration vs later

Must have: P0-1 to P0-4, P1-1 (at least the Python logging bridge with the
run and request events), P1-3 (`migrations()` and the schema check), P1-5
(progress callback), P1-7 (watch documentation, `state()`, `drift()`), P1-8.

Nice later: P1-2 caching (the first host caches already), P1-4 compaction
(size is tolerable for months), P1-6 subclasses, all P2 items, a Python
`QuotaGate` plug, a `threads()` read API.

## What not to do

- No native asyncio bindings (pyo3-async-runtimes). Blocking calls behind
  `to_thread` are simpler, loop-agnostic and enough for a handful of workers.
- No OpenTelemetry, metrics crate or callback-per-event hooks inside the
  library. Logs via `tracing` and counters via `RunOutcome`/`Status` suffice.
- No table-prefix or schema templating in SQL; `search_path` covers it.
- No pluggable store, blob store, HTTP client or quota gate from Python. The
  Rust traits exist for Rust hosts; a Python plug would put Python in the
  hot path of every request.
- No downgrades; forward-only migrations with a documented drop list.
- No general query API (search, listing by label or date). The host owns
  that over its own tables or by consuming the feed; keep `messages(ids)`
  and `changes()` as the read surface.
- Do not make `run()` manage the watch for hosts that already renew it.
  `watch_topic=None` plus the host calling `run()` on its own pushes is the
  right division of labour; just document it.
