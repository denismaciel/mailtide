"""Runner configuration: plain immutable data.

Nothing is validated here: the Rust core parses and validates the whole
configuration once, when the runner is built, and raises ``ValueError`` with
the offending field. Defaults suit a production host; every value is
explicit so a host can set it from its own settings.
"""

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True, slots=True)
class Database:
    """Where the mirror lives.

    ``url``: ``postgres://user@host/db``, a Unix socket as
    ``postgres:///db?host=/socket/dir`` (sqlx syntax, not a SQLAlchemy
    ``postgresql+driver://`` URL), ``sqlite:mail.db?mode=rwc`` or
    ``sqlite::memory:``. Pool limits apply to Postgres; SQLite always uses one
    connection. A run uses one connection at a time.
    """

    url: str
    max_connections: int = 2
    min_connections: int = 0
    acquire_timeout_s: float = 10.0
    idle_timeout_s: float | None = 600.0
    max_lifetime_s: float | None = 1800.0
    connect: Literal["lazy", "eager"] = "lazy"


@dataclass(frozen=True, slots=True)
class Blobs:
    """Where raw ``.eml`` content lives: ``gs://bucket/prefix``,
    ``s3://bucket/prefix``, ``file:///dir`` or ``memory:///``. Objects are
    keyed ``{prefix}/{account}/{message_id}.eml``."""

    url: str


@dataclass(frozen=True, slots=True)
class Http:
    """The Gmail HTTP client. ``gmail_origin`` is for tests only: it receives
    the bearer token."""

    request_timeout_s: float = 60.0
    connect_timeout_s: float = 10.0
    user_agent: str | None = None
    gmail_origin: str | None = None


@dataclass(frozen=True, slots=True)
class Engine:
    """Sync tunables."""

    poll_active_s: float = 60.0
    poll_idle_s: float = 900.0
    idle_after_s: float = 3600.0
    watch_renew_margin_s: float = 3600.0
    draft_settle_s: float = 120.0
    confirm_timeout_s: float = 60.0


@dataclass(frozen=True, slots=True)
class Quota:
    """Gmail quota this process may use (defaults: 80% of Google's limits).
    Lower these if the host also calls Gmail itself."""

    account_per_minute: int = 4_800
    project_per_minute: int = 960_000
    project_per_day: int = 64_000_000
    account_concurrency: int = 8
    project_concurrency: int = 128
    account_bytes_per_day: int = 2_000_000_000


@dataclass(frozen=True, slots=True)
class Log:
    """How mailtide's events reach Python ``logging``.

    Events go to ``logging.getLogger(logger)`` with structured fields under
    ``record.mailtide`` (a dict: ``stop``, ``commits``, ``request``,
    ``duration_ms``, ``account``, ...), so the host's handlers and JSON
    formatters pick them up. Levels below ``level`` are dropped in Rust and
    cost nothing. Accounts are logged as stable hashes unless
    ``accounts="plain"``; addresses, subjects, error bodies and tokens are
    never logged.
    """

    level: Literal["off", "error", "warning", "info", "debug", "trace"] = "info"
    accounts: Literal["hashed", "plain"] = "hashed"
    logger: str = "mailtide"


@dataclass(frozen=True, slots=True)
class Poll:
    """No push: mailtide never calls ``users.watch`` and finds changes by
    polling (or when the host calls ``run`` on its own verified pushes). Use
    this when the host already owns the watch: Gmail keeps one per user."""

    kind: Literal["poll"] = field(default="poll", init=False)


@dataclass(frozen=True, slots=True)
class Topic:
    """mailtide renews ``users.watch`` to publish to this Pub/Sub topic."""

    name: str
    kind: Literal["topic"] = field(default="topic", init=False)


@dataclass(frozen=True, slots=True)
class Config:
    """Everything a runner needs except credentials."""

    database: Database
    blobs: Blobs
    watch: Poll | Topic = Poll()
    http: Http = Http()
    engine: Engine = Engine()
    quota: Quota = Quota()
    log: Log = Log()
    token_cache_margin_s: float = 60.0
