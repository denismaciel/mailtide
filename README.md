# mailtide

A watertight Gmail sync engine: it mirrors whole mailboxes into your own
database (Postgres or SQLite), stores raw messages in blob storage, and
carries user actions back to Gmail. Rust core with no I/O of its own
(sans-IO), Python bindings via maturin.

Wheels for CPython ≥ 3.12 (abi3) and free-threaded 3.14t/3.15t; thread safe.

## Install

Wheels are attached to each [GitHub release](https://github.com/denismaciel/mailtide/releases)
with a flat package index. With uv, in `pyproject.toml`:

```toml
[project]
dependencies = ["mailtide==0.2.1"]

[[tool.uv.index]]
name = "mailtide"
url = "https://github.com/denismaciel/mailtide/releases/download/v0.2.1/index.html"
format = "flat"
explicit = true

[tool.uv.sources]
mailtide = { index = "mailtide" }
```

Or with pip: `pip install mailtide==0.2.1 --find-links
https://github.com/denismaciel/mailtide/releases/download/v0.2.1/index.html`.
Platforms: Linux x86_64/aarch64 (manylinux 2.28), macOS arm64/x86_64.

Status: v0.1, pre-release. See [SPEC.md](SPEC.md) and
[ARCHITECTURE.md](ARCHITECTURE.md).

```python
import mailtide
from mailtide.config import Blobs, Config, Database, Poll

runner = mailtide.Runner(
    Config(
        # sqlx URLs: postgres://u@host/db, a Unix socket as
        # postgres:///db?host=/cloudsql/project:region:instance, or sqlite:…
        database=Database("postgres:///app?host=/cloudsql/p:r:i", max_connections=2),
        blobs=Blobs("gs://bucket/mail"),  # raw .eml: gs://, s3://, file:///, memory:///
        watch=Poll(),  # the host owns users.watch; or Topic("projects/p/topics/gmail")
    ),
    token_source,  # account -> (access_token, expires_at_ms), sync or async; cached
)
await runner.migrate()  # in the deploy step
await runner.track("alice@example.com")  # only tracked accounts sync

outcome = await runner.run("alice@example.com", budget_s=60)  # on a schedule and on each push
await runner.admit("alice@example.com", "archive", thread_id="18c…")  # from request paths
await runner.prioritise("alice@example.com", ["18c…"])  # opened: fetch its body next
status = await runner.status("alice@example.com")  # never preempts a running sync
page = await runner.changes("alice@example.com", since=None)  # bounded pages; imports carry headers
await runner.ack("alice@example.com", "indexer", page.next)  # after processing a page
await runner.compact("alice@example.com")  # drops what every live consumer has acknowledged
views = await runner.messages("alice@example.com", ["18c…"])  # effective labels, headers
labels = await runner.labels("alice@example.com")  # ids to names, colours, parents
label = await runner.create_label("alice@example.com", "Work/2026")  # Gmail's id, recorded
threads = await runner.threads("alice@example.com", ["18c…"])  # thread views, in one call
hits = await runner.search("alice@example.com", "from:bob has:attachment")  # Gmail's own search
content = views[0].content  # sender, dates, list headers, an attachment summary
pdf = await runner.read_part("alice@example.com", "18c…", content.attachments[0].part)
await runner.refresh_facts("alice@example.com")  # facts for mail imported before them
```

A thread view (`ThreadView`) is folded from the mirror on every read: its
messages oldest first, the label union of its non-draft messages with pending
actions applied, unread, the latest message, counts and participants. Listing
threads by label, sorting, paging and per-label counts are the host's: keep a
table of your own, fed by `changes` (every entry names its thread; read the
threads of a page with one `threads` call and upsert, or delete the ids left
out).

Invalid configuration raises `ValueError` naming the field. Failures are typed:
`StoreUnavailable`, `OutcomeUnknown`, `CorruptState`, `BlobUnavailable`,
`QuotaGateError`, `Superseded`, `BlobNotFound`, `PartNotFound`, `PartTooLarge`
(all `MailtideError`). A blocked account
(`DelegationRevoked` from the token source) stays blocked until
`await runner.resume(account)`; `forget(account)` deletes its rows and blobs.

No event loop? `mailtide.BlockingRunner` has the same methods, blocking.

## Layout

| Path | What |
|---|---|
| `crates/mailtide` | Pure Rust sans-IO engine (no I/O, no PyO3) |
| `crates/mailtide-py` | PyO3 bindings, built by maturin as `mailtide._core` |
| `crates/mailtide-sim` | Deterministic Gmail simulator for testing (SPEC §12) |
| `crates/mailtide-gmail` | Gmail wire layer (pure) |
| `crates/mailtide-store` | Store trait; memory, SQLite, Postgres stores; conformance suite |
| `crates/mailtide-io` | Async driver: HTTP, quota gate, blobs, runner |
| `crates/mailtide-mime` | MIME: message facts, part ranges and chunked decoding; composing messages to send or save as drafts |
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
With Nix, `nix develop` gives that toolchain plus `uv` and `cargo-deny`
(Linux and Apple Silicon macOS); everything above applies unchanged inside it.

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
