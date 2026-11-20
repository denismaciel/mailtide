"""The async Runner: a thin coroutine layer over the Rust core.

The Rust methods return asyncio futures; these wrappers make them coroutines,
so they work with ``asyncio.create_task`` and ``TaskGroup``. Cancelling the
awaiting task drops the Rust work (in-flight requests stop, an open
transaction rolls back; every commit is atomic and fenced, so that is safe).
"""

import asyncio
import contextvars
import random
from collections.abc import Awaitable, Callable
from typing import Literal

from mailtide import _core
from mailtide._core import (
    AccountStatus,
    ChangePage,
    DriftResult,
    ForgetResult,
    Health,
    Label,
    MessageView,
    NotifyResult,
    PrioritiseResult,
    Progress,
    ResumeResult,
    RunOutcome,
    SchemaStatus,
    StateTooOld,
    TrackResult,
)
from mailtide.config import Config

TokenSource = Callable[[str], tuple[str, int] | Awaitable[tuple[str, int]]]
Action = Literal[
    "mark_read",
    "mark_unread",
    "star",
    "unstar",
    "mark_important",
    "mark_not_important",
    "archive",
    "move_to_inbox",
    "trash",
    "restore",
    "spam",
    "not_spam",
    "change_labels",
]


class Runner:
    """Syncs Gmail accounts into a database and blob store; methods are coroutines.

    Build one per process (after forking) from a validated ``Config`` and
    share it: methods may run concurrently from any thread or event loop,
    also on free-threaded Python. Concurrent runs of one account are safe:
    all but the newest stop as ``superseded``. ``token_source(account)``
    returns ``(access_token, expires_at_ms)`` (sync or async) or raises
    ``DelegationRevoked``; tokens are cached until shortly before they expire.
    Construction raises ``ValueError`` naming the first invalid config field.
    """

    __slots__ = ("_core",)

    def __init__(
        self,
        config: Config,
        token_source: TokenSource,
        *,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._core = _core.Runner(config, token_source, clock=clock)

    async def migrate(self) -> None:
        """Create or upgrade mailtide's tables (safe to call concurrently)."""
        await self._core.migrate()

    async def track(self, account: str) -> TrackResult:
        """Start mirroring ``account`` (a no-op if it already is). Only tracked
        accounts sync: ``run``/``act`` on another raise ``AccountNotTracked``,
        so a stray push can never start a full import. Tracking again after
        ``forget`` starts a new generation of the change feed: state strings
        from before are answered with ``StateTooOld``."""
        return await self._core.track(account)

    async def schema(self) -> SchemaStatus:
        """Where the database's schema stands against this build. Every call
        that reads or writes the database (all but ``schema``, ``migrate``,
        ``health`` and ``read_blob``) refuses with ``SchemaOutOfDate`` unless
        it is ``current``."""
        return await self._core.schema()

    async def run(
        self,
        account: str,
        budget_s: float = 60.0,
        on_progress: Callable[[Progress], object] | None = None,
    ) -> RunOutcome:
        """Sync ``account`` until caught up, blocked or ``budget_s`` elapses.

        Call it on a schedule and whenever a verified push arrives.
        ``on_progress(Progress)`` runs on this event loop, in the caller's
        context (so e.g. a durable-workflow activity can heartbeat from it),
        at most once a second while the run makes progress.
        """
        if on_progress is None:
            return await self._core.run(account, budget_s, None)
        loop = asyncio.get_running_loop()
        context = contextvars.copy_context()

        def schedule(progress: Progress) -> None:
            loop.call_soon_threadsafe(on_progress, progress, context=context)

        return await self._core.run(account, budget_s, schedule)

    async def act(
        self,
        account: str,
        action: Action,
        *,
        message_ids: list[str] | None = None,
        thread_id: str | None = None,
        add_labels: list[str] | None = None,
        remove_labels: list[str] | None = None,
        destination: Literal["inbox", "archive"] = "inbox",
        budget_s: float = 30.0,
    ) -> RunOutcome:
        """Admit an action and deliver it now (claims the account: use
        ``admit`` from request paths). ``outcome.admitted`` is how many
        messages it reached."""
        return await self._core.act(
            account,
            action,
            message_ids=message_ids,
            thread_id=thread_id,
            add_labels=add_labels or [],
            remove_labels=remove_labels or [],
            destination=destination,
            budget_s=budget_s,
        )

    async def admit(
        self,
        account: str,
        action: Action,
        *,
        message_ids: list[str] | None = None,
        thread_id: str | None = None,
        add_labels: list[str] | None = None,
        remove_labels: list[str] | None = None,
        destination: Literal["inbox", "archive"] = "inbox",
    ) -> int:
        """Admit an action durably without delivering it: it shows in
        ``messages()`` at once and the account's next run delivers it. Never
        preempts a running sync. Returns how many messages it reached."""
        return await self._core.admit(
            account,
            action,
            message_ids=message_ids,
            thread_id=thread_id,
            add_labels=add_labels or [],
            remove_labels=remove_labels or [],
            destination=destination,
        )

    async def prioritise(self, account: str, message_ids: list[str]) -> PrioritiseResult:
        """A user opened these messages: fetch their bodies ahead of the
        backlog, in the Foreground quota class, by whichever process runs the
        account (call ``run`` if none is running). One store write that never
        preempts a sync; use it from request paths. Allowed read-only.

        Each distinct id lands in one group: ``queued`` (fetched next),
        ``imported`` (read it now), ``unknown`` (no row yet: the next sync
        brings it), ``blocked``, or ``over_limit`` (the account already has 24
        raised imports: ask again as they land). A ``MessageImported`` feed
        entry, or ``content`` in ``messages()``, says when a body is stored.
        At most 100 ids per call (``ValueError`` beyond)."""
        return await self._core.prioritise(account, message_ids)

    async def status(self, account: str) -> AccountStatus | None:
        """What the store holds about ``account``, read without claiming it."""
        return await self._core.status(account)

    async def accounts(self) -> list[str]:
        """Every account the store holds."""
        return await self._core.accounts()

    async def drift(
        self,
        account: str,
        message_ids: list[str] | None = None,
        sample: int = 50,
        budget_s: float = 120.0,
    ) -> list[DriftResult]:
        """Compare the mirror with Gmail, read-only, for ``message_ids`` or a
        random ``sample`` of mirrored messages. ``verdict == "differs"`` is
        drift; ``explained`` differences (pending actions, awaiting
        verification, Gmail ahead of the mirror) are not. Fetches go through
        the quota gate like a sync's, waiting out deferrals within
        ``budget_s``; any left over are reported as ``deferred``."""
        if message_ids is None:
            message_ids = await self._sample(account, sample)
        return await self._core.drift(account, message_ids, budget_s)

    async def _sample(self, account: str, size: int) -> list[str]:
        ids: list[str] = []
        after: str | None = None
        while page := await self._core.message_ids(account, after, 1000):
            ids.extend(page)
            after = page[-1]
        return random.sample(ids, min(size, len(ids)))

    async def notify(self, account: str, history_id: int, budget_s: float = 60.0) -> NotifyResult:
        """The host verified a Gmail push (its own Pub/Sub handler) saying the
        mailbox reached ``history_id``: sync only if the mirror is behind.
        Pushes never start an account mailtide doesn't sync yet."""
        return await self._core.notify(account, history_id, budget_s)

    async def resume(self, account: str) -> ResumeResult:
        """Resume a blocked account once its cause is fixed (e.g. credentials)."""
        return await self._core.resume(account)

    async def forget(self, account: str) -> ForgetResult:
        """Delete everything stored for ``account``: rows and raw content."""
        return await self._core.forget(account)

    async def health(self) -> Health:
        """Whether the database answers within the pool's acquire timeout."""
        return await self._core.health()

    async def labels(self, account: str) -> list[Label]:
        """``account``'s label definitions as the mirror last synced them, in id
        order (empty before the first label sync): turns the label ids on
        messages into names. Read again after a ``LabelDefinitionsChanged``."""
        return await self._core.labels(account)

    async def messages(self, account: str, message_ids: list[str]) -> list[MessageView]:
        """How these messages should look; unknown ids are left out."""
        return await self._core.messages(account, message_ids)

    async def changes(
        self, account: str, since: str | None = None, limit: int = 500
    ) -> ChangePage | StateTooOld:
        """Feed entries after state string ``since`` (``None``: from the start), or
        ``StateTooOld`` if that state was compacted away. A page holds at most
        ``limit`` entries (at most 5,000) and may end inside a commit; continue
        from ``page.next`` while ``page.has_more``."""
        return await self._core.changes(account, since, limit)

    async def compact(self, account: str, through: str) -> int:
        """Drop feed entries at or before state ``through`` once every consumer
        processed them: whole commits only, so a ``through`` inside a commit
        keeps that commit. Returns how many were removed."""
        return await self._core.compact(account, through)

    async def message_ids(
        self, account: str, after: str | None = None, limit: int = 1000
    ) -> list[str]:
        """Up to ``limit`` message ids after ``after``, in id order (to rebuild a
        consumer's copy)."""
        return await self._core.message_ids(account, after, limit)

    async def read_blob(self, account: str, message_id: str) -> bytes:
        """The raw RFC 822 bytes of an imported message."""
        return await self._core.read_blob(account, message_id)
