# Architecture

Rust does the work; Python is a thin shell. Everything that can be pure is
pure, and I/O lives in one driver crate.

```
                   ┌──────────────────────── Rust ────────────────────────┐
 Python host  ───▶ │ mailtide-py (PyO3)                                     │
 (scheduler,       │   Mailbox / Runner (high level)   Engine (low level)   │
  credentials)     │        │                                               │
                   │ mailtide-io (async, tokio): runner, HTTP, quota gate,  │
                   │        │      blobs, token source                       │
                   │        ├──▶ mailtide-gmail  (pure wire layer)          │
                   │        ├──▶ mailtide-store  (memory/SQLite/Postgres)   │
                   │        └──▶ mailtide        (pure sans-IO engine)      │
                   │                                                         │
                   │ mailtide-sim   (Gmail simulator, tests only)            │
                   │ mailtide-mime  (optional parsing)                       │
                   └─────────────────────────────────────────────────────────┘
```

## Crates

| Crate | Pure? | Role | SPEC | Zero counterpart (MIT) |
|---|---|---|---|---|
| `mailtide` | yes | The engine: events in, commands out. Sync rules, state, rehydration, invariants, retry classes, quota ledger | §3–§9 | `MAILBOX_SYNC.md`, `mail-history-store`, `mail-label-store`, `mail-label-policy`, `mail-schedule`, `gmail-quota-cost`, `gmail-rate-limit` |
| `mailtide-gmail` | yes | Gmail wire layer: typed responses, request builders, decoders, error classification, push JWT verification | §5 | `driver/google.ts`, `gmail-push.ts`, `gmail-quota-error.ts` |
| `mailtide-store` | no | `Store` trait; memory, SQLite and Postgres stores with migrations; conformance suite | §4, §10.3 | the Durable Object SQLite stores |
| `mailtide-io` | no | Async driver: executes commands over HTTP, enforces the quota gate, writes blobs, applies commits, runs the loop | §3, §6.2, §8 | `routes/agent/index.ts`, `gmail-quota-store/client`, `mail-asset-cache` |
| `mailtide-mime` | yes | Optional MIME parsing of stored raw messages | §5.5 | `imap/parser.ts`, `driver/outgoing-mime.ts` |
| `mailtide-sim` | yes | Deterministic Gmail simulator with fault injection, invariant checking, seeded replay | §12 | `imap/testing/memory-server.ts` |
| `mailtide-py` | — | PyO3 bindings: `mailtide._core` | §11 | — |

## Python (thin)

`python/mailtide` re-exports `_core`, wraps the async runner's futures as
coroutines (`_runner.py`), and ships `.pyi` stubs and `py.typed`.
A host:

1. provides a `TokenSource` (e.g. Google domain-wide delegation) and config
   (database URL, bucket);
2. calls `Runner.run(account)` from its scheduler (a cron job, a queue, a
   durable workflow) and on
   each verified push, and `Runner.act(...)` for user actions;
3. runs mailtide's migrations in its deploy.

## Storage

mailtide ships its stores (SPEC Q3, revised): the pure commit rules live in
`mailtide::mirror`; the memory store and the simulator use `AccountMirror`
directly, and the SQL store (Postgres, SQLite) loads the rows a commit
touches, applies the same rules and writes the difference. Schema changes are
versioned migrations recorded in `mailtide_schema`.

## Tests

- `crates/*/tests/zero_*.rs`: Zero's sync tests ported by name, in the crate
  that owns the behaviour; `scripts/zero_tests.py report` tracks progress.
- Store conformance (`mailtide-store::conformance`) runs against every store.
- Simulation campaigns (`mailtide-sim`) run in CI (500 seeds) and longer
  locally.
- `cargo deny check` gates advisories, licenses and sources.
