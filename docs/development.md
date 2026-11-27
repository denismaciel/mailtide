# Development

## Environment

The Rust toolchain (`1.100.0`) is pinned in `rust-toolchain.toml`. Enter the Nix development shell (Linux and Apple Silicon macOS) for Rust, `uv`, `cargo-deny`, and `git`:

```sh
nix develop
```

## Build and test

```sh
make dev            # uv sync --locked (compiles mailtide._core via build_backend.py)
make check          # rustfmt, clippy, ruff format/check, ty check
make test           # cargo test --workspace --locked + pytest
make build          # uv build --no-sources
```

Use `uv` 0.12.4 (provided by `nix develop`). The in-tree `build_backend.py` delegates Python metadata and sdists to `uv_build==0.12.4` and compiles the `mailtide-py` PyO3 extension with locked Cargo dependencies. Rerun `uv sync --reinstall-package mailtide` after Rust edits.

Additional test and simulation commands:

```sh
# Include Postgres store conformance tests
MAILTIDE_TEST_POSTGRES_URL=postgres://... cargo test --workspace

# Deterministic Gmail fault-injection simulation campaign
cargo run --release -p mailtide-sim --example campaign -- 0 500 1000

# Advisories, licenses, and dependency sources
cargo deny check
```

## Crate layout

| Path | What |
| --- | --- |
| `crates/mailtide` | Pure Rust sans-IO engine (no I/O, no PyO3) |
| `crates/mailtide-py` | PyO3 bindings built as `mailtide._core` |
| `crates/mailtide-sim` | Deterministic Gmail simulator for testing ([SPEC.md](../SPEC.md) §12) |
| `crates/mailtide-gmail` | Pure Gmail wire layer |
| `crates/mailtide-store` | `Store` trait; memory, SQLite, and Postgres stores; conformance suite |
| `crates/mailtide-io` | Async driver: HTTP, quota gate, blobs, runner |
| `crates/mailtide-mime` | MIME facts, part ranges, chunked decoding, and message composition |
| `python/mailtide` | Thin Python shell: async coroutine wrapper, config, re-exports, `.pyi` stubs |
| `scripts/zero_tests.py` | Tracks Mail-0/Zero sync tests ported by name |

See [ARCHITECTURE.md](../ARCHITECTURE.md) for crate boundaries and data flow. Module layout follows Zero's mailbox sync (Mail-0/Zero, MIT); each module's doc comment names its Zero counterpart and SPEC section.

## Operational notes

- **Migrations:** Stop old workers before running `runner.migrate()` because running workers cache the schema check (`SchemaOutOfDate` guards mismatched builds).
- **Completeness audit:** A daily audit catches messages missed by Gmail history or reconciliation. Schedule each account's next run from `outcome.next_run_at_ms` (`Idle.next` in Rust), including when no push arrives; each claim advances at most one audit step.
- **Label refresh resilience:** A label refresh looks up to 50 previously defined user labels missing from Gmail's listing. Only a `labels.get` `404` removes a label definition and its memberships; a `200` rejects the incomplete listing, preserves the existing catalogue, and increments `label_listing_failures`.
- **Purge safety bounds:** Each permanent delete performs a fresh Gmail history check and mirror read. A two-second bound runs from check start to transport hand-off (connection setup occurs after hand-off), and purge stops after three consecutive expired checks. Each ID is sent at most once per call and recorded as `purged` only on a `messagesDeleted` history record. `sent` means deletion may have occurred without history-backed confirmation; `not_applied` covers undispatched deletes and Gmail `401`, `429`, or rate-limit `403` responses.
- **Raw mail delivery:** Raw submissions are staged before admission and sent on the next eligible run with a single wire request (including after an unknown outcome). A `SubmissionChanged` feed entry reports `sent` once import finds the `X-Mailtide-Id` token on a `SENT` message, or `unknown` after the confirmation deadline.

## Parity with Zero's tests

`tests/zero/manifest.json` pins 86 test names from Zero's sync suites (commit `4b642dfd9890`). Each maps to a Rust test in the owning crate (`crates/*/tests/zero_*.rs`), or is marked `zero-na` with a reason:

```sh
uv run --no-project scripts/zero_tests.py report
uv run --no-project scripts/zero_tests.py port
uv run --no-project scripts/zero_tests.py extract --zero <zero-checkout>
```

Feature design plans and test mappings live under [docs/features/README.md](features/README.md).
