# mailtide

A watertight Gmail sync engine: it mirrors whole mailboxes into your own
database (Postgres or SQLite), stores raw messages in blob storage, and
carries user actions back to Gmail. Rust core with no I/O of its own
(sans-IO), Python bindings via maturin.

Status: v0.1, pre-release. See [SPEC.md](SPEC.md) and
[ARCHITECTURE.md](ARCHITECTURE.md).

```python
import mailtide

runner = mailtide.Runner(
    "postgres://…/app",  # or "sqlite:mail.db?mode=rwc"
    "gs://bucket/mail",  # raw .eml blobs: gs://, s3://, file:///
    token_source,  # account -> (access_token, expires_at_ms)
    watch_topic="projects/p/topics/gmail",
)
runner.migrate()

outcome = runner.run("alice@example.com", budget_s=60)  # on a schedule and on each push
outcome.stop, outcome.next_run_at_ms

runner.act("alice@example.com", "archive", thread_id="18c…")  # shows at once, settles on Gmail
page = runner.changes("alice@example.com", since=None)  # change feed
views = runner.messages("alice@example.com", ["18c…"])  # effective labels, headers
```

## Layout

| Path | What |
|---|---|
| `crates/mailtide` | Pure Rust sans-IO engine (no I/O, no PyO3) |
| `crates/mailtide-py` | PyO3 bindings, built by maturin as `mailtide._core` |
| `crates/mailtide-sim` | Deterministic Gmail simulator for testing (SPEC §12) |
| `crates/mailtide-gmail` | Gmail wire layer (pure) |
| `crates/mailtide-store` | Store trait; memory, SQLite, Postgres stores; conformance suite |
| `crates/mailtide-io` | Async driver: HTTP, quota gate, blobs, runner |
| `crates/mailtide-mime` | Optional MIME parsing |
| `python/mailtide` | Thin Python shell: re-exports and type stubs |
| `scripts/zero_tests.py` | Tracks Zero's sync tests ported by name (see below) |

See [ARCHITECTURE.md](ARCHITECTURE.md). Module layout follows Zero's mailbox sync (Mail-0/Zero, MIT); each module's
doc comment names its Zero counterpart and SPEC section.

## Development

```sh
uv sync                     # builds the extension with maturin
uv run pytest
cargo test --workspace      # Postgres conformance too with MAILTIDE_TEST_POSTGRES_URL
cargo clippy --workspace --all-targets -- -D warnings
cargo run --release -p mailtide-sim --example campaign -- 0 500 1000   # simulation
cargo deny check
```

The toolchain is pinned in `rust-toolchain.toml` (rustup).

## Parity with Zero's tests

`tests/zero/manifest.json` pins 86 test names from Zero's sync suites
(commit `4b642dfd9890`). Each one has a Rust test named after it in the crate
that owns the behaviour (`crates/*/tests/zero_*.rs`); tests mailtide has no
equivalent for are marked `zero-na` with the reason.

```sh
uv run --no-project scripts/zero_tests.py report                       # counts per suite
uv run --no-project scripts/zero_tests.py port                         # add stubs for new names
uv run --no-project scripts/zero_tests.py extract --zero <zero-checkout>   # refresh the manifest
```

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
