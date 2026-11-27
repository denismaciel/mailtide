# mailtide

A watertight Gmail sync engine: it mirrors whole mailboxes into your own Postgres or SQLite database, stores raw `.eml` messages in blob storage, and carries user actions, drafts, and outgoing mail back to Gmail.

The sync core is pure sans-IO Rust (`crates/mailtide`) paired with an async Tokio driver and Python bindings (`mailtide._core`), shipping prebuilt wheels for CPython 3.12+ (`abi3`) and free-threaded 3.14t/3.15t.

## Install

Requires Python 3.12+. Wheels for Linux (`x86_64`/`aarch64`, `manylinux_2_28`) and macOS (`arm64`/`x86_64`) are distributed via [GitHub Releases](https://github.com/denismaciel/mailtide/releases) with a flat package index (not yet on PyPI).

With `uv` in `pyproject.toml`:

```toml
[project]
dependencies = ["mailtide==0.4.0"]

[[tool.uv.index]]
name = "mailtide"
url = "https://github.com/denismaciel/mailtide/releases/download/v0.4.0/index.html"
format = "flat"
explicit = true

[tool.uv.sources]
mailtide = { index = "mailtide" }
```

Or install directly with `uv pip` / `pip`:

```sh
uv pip install mailtide==0.4.0 --find-links https://github.com/denismaciel/mailtide/releases/download/v0.4.0/index.html
```

## Quick start

```python
import mailtide
from mailtide.config import Blobs, Config, Database, Poll

async def token_source(account: str) -> tuple[str, int]:
    # Return (oauth_access_token, expires_at_ms); sync or async, cached automatically.
    ...

runner = mailtide.Runner(
    Config(
        # sqlx URLs: postgres://u@host/db, postgres:///db?host=/cloudsql/p:r:i, or sqlite:...
        database=Database("sqlite:mail.db?mode=rwc"),
        blobs=Blobs("file:///var/lib/mailtide/blobs"),  # gs://, s3://, file:///, memory:///
        watch=Poll(),  # or Topic("projects/p/topics/gmail") for Gmail Pub/Sub push
    ),
    token_source,
)

await runner.migrate()                    # run during deploy (stop old workers first)
await runner.track("alice@example.com")   # only tracked accounts sync

outcome = await runner.run("alice@example.com", budget_s=60)
# Schedule the next run from outcome.next_run_at_ms, and call run()/notify() on pushes.
```

No event loop? `mailtide.BlockingRunner` exposes the same API synchronously.

## Usage

### Reading the mirror and change feed

```python
# Bounded change feed for updating your own read models or search indexes
page = await runner.changes("alice@example.com", since=None)
await runner.ack("alice@example.com", "indexer", page.next)
await runner.compact("alice@example.com")  # drops commits every live consumer has acked

# Effective message & thread views (pending local actions already overlaid)
views = await runner.messages("alice@example.com", ["18c..."])
threads = await runner.threads("alice@example.com", ["18c..."])

# Labels, Gmail search pass-through, and ranged attachment reads
labels = await runner.labels("alice@example.com")
label = await runner.create_label("alice@example.com", "Work/2026")
hits = await runner.search("alice@example.com", "from:bob has:attachment")

content = views[0].content
if content and content.attachments:
    pdf = await runner.read_part("alice@example.com", "18c...", content.attachments[0].part)
```

- `ThreadView` is folded from the mirror on every read: oldest-first messages, label union of non-draft messages with pending actions applied, `unread`, `latest`, counts, and `participants`. Feed your own thread-list table from `changes` (each entry carries its thread ID; fetch affected threads in one `threads()` call).
- `await runner.prioritise("alice@example.com", ["18c..."])` bumps opened messages ahead of backfill in the foreground quota class without preempting a running sync.
- `await runner.status("alice@example.com")` inspects sync state without claiming the account.
- `await runner.refresh_facts("alice@example.com")` backfills extracted MIME facts from stored blobs without calling Gmail.

### Actions, drafts, and sending

```python
# Queue a mailbox action from a request path (reflected in messages() immediately)
await runner.admit("alice@example.com", "archive", thread_id="18c...")

# Compose RFC 5322 bytes carrying a unique X-Mailtide-Id tracking header
msg = mailtide.OutgoingMessage(
    "alice@example.com",
    to=["bob@example.com"],
    subject="Hello",
    text="Hi Bob.",
)

# Drafts (saved immediately with Gmail's draft ID, reconciled on send)
draft_raw = mailtide.compose_message(msg, purpose="draft").raw
draft = await runner.create_draft("alice@example.com", draft_raw)
draft = await runner.update_draft("alice@example.com", draft.draft_id, draft_raw)
sent = await runner.send_draft("alice@example.com", draft.draft_id)  # at most once
drafts = await runner.drafts("alice@example.com")

# Durable raw mail submission (scheduled or immediate)
send_raw = mailtide.compose_message(msg).raw
scheduled = await runner.submit("alice@example.com", send_raw, send_at_ms=send_at_ms)
await runner.cancel_submission("alice@example.com", scheduled.key)  # before worker claim
result = await runner.send("alice@example.com", send_raw)           # submit and run now
```

- Gmail replaces `Message-ID` on every draft save and send; `X-Mailtide-Id` survives so lost HTTP answers reconcile cleanly against imported `SENT` messages (see [SPEC.md](SPEC.md) §7a–§7b).
- The default RFC 5322 size limit is 34 MiB (`Config.max_message_bytes`).

### Permanent deletion

Permanent deletion is off by default (`Config.purge="off"`). Switch it on explicitly and supply a token with the `https://mail.google.com/` scope (a missing scope raises `PurgeScopeRequired` before deleting anything):

```python
config = Config(database=..., blobs=..., purge="trash_and_spam")
outcome = await runner.purge("alice@example.com", message_ids=["18c..."])
# outcome.purged, outcome.sent, outcome.refused, outcome.not_applied
await runner.empty("alice@example.com", "trash")
```

### Errors and account lifecycle

- Invalid configuration raises `ValueError` naming the offending field.
- Runtime failures are typed subclasses of `MailtideError` (`StoreUnavailable`, `OutcomeUnknown`, `CorruptState`, `BlobUnavailable`, `QuotaGateError`, `Superseded`, `BlobNotFound`, `PartNotFound`, `PartTooLarge`, `PurgeScopeRequired`, etc.).
- Raising `DelegationRevoked` from `token_source` blocks the account until `await runner.resume(account)`.
- `await runner.forget(account)` deletes all mirrored rows and stored blobs for an account.

## Reference

- [Specification](SPEC.md)
- [Architecture](ARCHITECTURE.md)
- [Feature plans](docs/features/README.md)

## Development

See [docs/development.md](docs/development.md) for local setup, operational notes, crate layout, simulation testing, and Zero test parity.

## License

Dual-licensed under [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
