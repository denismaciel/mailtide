"""The async Runner: a thin coroutine layer over the Rust core.

The Rust methods return asyncio futures; these wrappers make them coroutines,
so they work with ``asyncio.create_task`` and ``TaskGroup``. Cancelling the
awaiting task drops the Rust work (in-flight requests stop, an open
transaction rolls back; every commit is atomic and fenced, so that is safe).
"""

import asyncio
import contextvars
from collections.abc import Awaitable, Callable
from typing import Literal

from mailtide import _core
from mailtide._core import (
    AccountStatus,
    ChangePage,
    ForgetResult,
    Health,
    MessageView,
    Progress,
    ResumeResult,
    RunOutcome,
    StateTooOld,
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

    def __init__(self, config: Config, token_source: TokenSource) -> None:
        self._core = _core.Runner(config, token_source)

    async def migrate(self) -> None:
        """Create or upgrade mailtide's tables (safe to call concurrently)."""
        await self._core.migrate()

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

    async def status(self, account: str) -> AccountStatus | None:
        """What the store holds about ``account``, read without claiming it."""
        return await self._core.status(account)

    async def accounts(self) -> list[str]:
        """Every account the store holds."""
        return await self._core.accounts()

    async def resume(self, account: str) -> ResumeResult:
        """Resume a blocked account once its cause is fixed (e.g. credentials)."""
        return await self._core.resume(account)

    async def forget(self, account: str) -> ForgetResult:
        """Delete everything stored for ``account``: rows and raw content."""
        return await self._core.forget(account)

    async def health(self) -> Health:
        """Whether the database answers within the pool's acquire timeout."""
        return await self._core.health()

    async def messages(self, account: str, message_ids: list[str]) -> list[MessageView]:
        """How these messages should look; unknown ids are left out."""
        return await self._core.messages(account, message_ids)

    async def changes(
        self, account: str, since: str | None = None, limit: int = 500
    ) -> ChangePage | StateTooOld:
        """Feed entries after state string ``since`` (``None``: from the start), or
        ``StateTooOld`` if that state was compacted away."""
        return await self._core.changes(account, since, limit)

    async def compact(self, account: str, through: str) -> int:
        """Drop feed entries at or before state ``through`` once every consumer
        processed them. Returns how many were removed."""
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
