"""The async Runner: a thin coroutine layer over the Rust core.

The Rust methods return asyncio futures; these wrappers make them coroutines,
so they work with ``asyncio.create_task`` and ``TaskGroup``. Cancelling the
awaiting task drops the Rust work (in-flight requests stop, an open
transaction rolls back; every commit is atomic and fenced, so that is safe).
"""

import asyncio
import contextvars
import random
from collections.abc import Awaitable, Callable, Sequence
from typing import Literal

from mailtide import _core
from mailtide._core import (
    AccountStatus,
    ChangePage,
    ConsumerCursor,
    DriftResult,
    EmptyOutcome,
    FactsRefresh,
    ForgetResult,
    Health,
    Label,
    LabelColor,
    LabelDeletion,
    LabelUpdate,
    MessageView,
    NotifyResult,
    PartInfo,
    PartStream,
    PrioritiseResult,
    Progress,
    PurgeOutcome,
    ResumeResult,
    RunOutcome,
    SchemaStatus,
    SearchPage,
    StateTooOld,
    ThreadSearchPage,
    ThreadView,
    TrackResult,
)
from mailtide.config import Config

TokenSource = Callable[[str], tuple[str, int] | Awaitable[tuple[str, int]]]
ListVisibility = Literal["show", "show_if_unread", "hide"]
MessageVisibility = Literal["show", "hide"]
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
Priority = Literal["foreground", "live", "backfill"]


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

    async def search(
        self,
        account: str,
        query: str,
        *,
        label_ids: Sequence[str] = (),
        include_spam_trash: bool = False,
        limit: int = 100,
        page_token: str | None = None,
        priority: Priority = "foreground",
        budget_s: float = 10.0,
    ) -> SearchPage:
        """One page of Gmail's own search for ``query`` (the search box's
        syntax, sent untouched; ``messages.list``, 5 quota units).

        Each hit is ``imported``, ``not_imported`` or ``unknown`` (no row
        yet: the next ``run`` brings it); hits the mirror deleted are left
        out and counted in ``deleted``. Read the hits with ``messages``.
        ``label_ids`` are label ids all hits carry (names go in the query as
        ``label:``). An id the mirror's label definitions lack is checked
        with Gmail first (``labels.get``, 1 unit): one Gmail does not have
        raises ``UnknownLabel``, since Gmail would answer the search with no
        hits; one it has (created since the last label sync) is searched. Pass
        ``next_page_token`` back with the same arguments for the next page.
        A page whose ``mirror_state`` has feed entries after it may be stale.
        Writes nothing and never preempts a sync; allowed read-only. Raises
        ``SearchRefused``, ``InvalidPageToken``, ``PageTokenRejected``,
        ``QuotaDeferred``, ``GmailUnavailable``, ``AccountBlocked``,
        ``AccountNotTracked``, or ``ValueError`` for a blank query."""
        return await self._core.search(
            account,
            query,
            label_ids=list(label_ids),
            include_spam_trash=include_spam_trash,
            limit=limit,
            page_token=page_token,
            priority=priority,
            budget_s=budget_s,
        )

    async def search_threads(
        self,
        account: str,
        query: str,
        *,
        label_ids: Sequence[str] = (),
        include_spam_trash: bool = False,
        limit: int = 100,
        page_token: str | None = None,
        priority: Priority = "foreground",
        budget_s: float = 10.0,
    ) -> ThreadSearchPage:
        """One page of thread ids Gmail's search matches (``threads.list``,
        10 quota units), each with Gmail's ``history_id`` for the thread:
        past the page's ``cursor``, the mirror has not caught up with it.
        Otherwise as ``search``."""
        return await self._core.search_threads(
            account,
            query,
            label_ids=list(label_ids),
            include_spam_trash=include_spam_trash,
            limit=limit,
            page_token=page_token,
            priority=priority,
            budget_s=budget_s,
        )

    async def purge(
        self,
        account: str,
        *,
        message_ids: list[str] | None = None,
        thread_id: str | None = None,
        anywhere: bool = False,
        budget_s: float = 30.0,
    ) -> PurgeOutcome:
        """Delete messages for good: ``message_ids`` (at most 1,000) or the
        non-draft messages the mirror holds for ``thread_id``. Off unless
        ``Config.purge`` switches it on; only what is in trash or spam as of
        the call, unless ``anywhere=True`` under ``Config.purge="anywhere"``.

        Gmail's history since the mirror's cursor is read first, so a message
        restored elsewhere a moment ago is refused. Each delete is sent once
        and never repeated by mailtide; a message counts as ``purged`` (and
        its ``MessageDeleted`` is in the feed) only once Gmail's history shows
        the deletion; one sent but not shown yet is ``sent``. Needs a token
        with ``https://mail.google.com/``. Raises ``PurgeDisabled``,
        ``ReadOnlyMode``, ``TooManyMessages``, ``MirrorStale`` (run the
        account first), ``PurgeScopeRequired`` (nothing deleted, nothing
        trashed instead, the account keeps syncing), ``QuotaDeferred``,
        ``GmailUnavailable``, ``AccountBlocked`` or ``AccountNotTracked``."""
        return await self._core.purge(
            account,
            message_ids=message_ids,
            thread_id=thread_id,
            anywhere=anywhere,
            budget_s=budget_s,
        )

    async def empty(
        self, account: str, bin: Literal["trash", "spam"], *, budget_s: float = 60.0
    ) -> EmptyOutcome:
        """Delete for good what the mirror holds in the trash (or spam),
        1,000 at a time, until ``budget_s`` ends. Messages that left the bin
        meanwhile, drafts and messages trashed elsewhere since the last sync
        are left. ``remaining`` > 0: call again. Raises as ``purge``."""
        return await self._core.empty(account, bin, budget_s=budget_s)

    async def labels(self, account: str) -> list[Label]:
        """``account``'s label definitions as the mirror last synced them, in id
        order (empty before the first label sync): turns the label ids on
        messages into names, with each label's visibility, colour and
        ``parent_id`` (Gmail nests by ``/`` in the name). Read again after a
        ``LabelDefinitionsChanged``."""
        return await self._core.labels(account)

    async def create_label(
        self,
        account: str,
        name: str,
        *,
        list_visibility: ListVisibility = "show",
        message_visibility: MessageVisibility = "show",
        color: LabelColor | None = None,
        budget_s: float = 30.0,
    ) -> Label:
        """Create a user label in Gmail (``Parent/Child`` nests it) and record
        it: the returned label carries Gmail's id, ready for ``act``, and
        ``labels()`` shows it before any run. ``color`` must be on Gmail's
        palette. Raises ``LabelNameTaken`` (with ``existing``) when another
        label holds the name in any case: after a ``WriteOutcomeUnknown``, the
        label the lost attempt created."""
        return await self._core.create_label(
            account,
            name,
            list_visibility=list_visibility,
            message_visibility=message_visibility,
            color=color,
            budget_s=budget_s,
        )

    async def update_label(
        self,
        account: str,
        label_id: str,
        *,
        name: str | None = None,
        list_visibility: ListVisibility | None = None,
        message_visibility: MessageVisibility | None = None,
        color: LabelColor | Literal["keep"] | None = "keep",
        sublabels: Literal["follow", "leave"] = "follow",
        budget_s: float = 30.0,
    ) -> LabelUpdate:
        """Change a label: only the fields given are sent (``color=None``
        clears it). A rename moves its sublabels (``Work/x`` follows ``Work``)
        unless ``sublabels="leave"``; every name it needs is checked first,
        then the sublabels move and the label last. Every exception a label
        write raises (``GmailUnavailable``, ``QuotaDeferred`` and
        ``WriteOutcomeUnknown`` included) carries ``done``, the ids already
        changed: empty unless it stopped half-way, when the same call again
        finishes it. A new name under the label's own is ``InvalidLabel``."""
        return await self._core.update_label(
            account,
            label_id,
            name=name,
            list_visibility=list_visibility,
            message_visibility=message_visibility,
            color=color,
            sublabels=sublabels,
            budget_s=budget_s,
        )

    async def delete_label(
        self,
        account: str,
        label_id: str,
        *,
        sublabels: Literal["refuse", "delete"] = "refuse",
        budget_s: float = 30.0,
    ) -> LabelDeletion:
        """Delete a label in Gmail and record it: it leaves every mirrored
        message at once (``LabelsChanged`` entries), and its pending actions
        go. A label with sublabels raises ``LabelHasSublabels`` unless
        ``sublabels="delete"`` (deepest first). A label already gone is
        reported in ``already_gone``: safe to retry."""
        return await self._core.delete_label(
            account, label_id, sublabels=sublabels, budget_s=budget_s
        )

    async def messages(self, account: str, message_ids: list[str]) -> list[MessageView]:
        """How these messages should look; unknown ids are left out."""
        return await self._core.messages(account, message_ids)

    async def threads(self, account: str, thread_ids: list[str]) -> list[ThreadView]:
        """How these threads should look, in the order asked: each member's
        ``MessageView``, the label union of the non-draft members, unread,
        ``latest`` and counts. Threads with no mirrored message are left out.
        Collect the thread ids of a ``changes`` page and read them in one call."""
        return await self._core.threads(account, thread_ids)

    async def changes(
        self, account: str, since: str | None = None, limit: int = 500
    ) -> ChangePage | StateTooOld:
        """Feed entries after state string ``since`` (``None``: from the start), or
        ``StateTooOld`` if that state was compacted away. A page holds at most
        ``limit`` entries (at most 5,000), ending at the last commit boundary that
        fits; only a commit larger than ``limit`` is split across pages. Continue
        from ``page.next`` while ``page.has_more``."""
        return await self._core.changes(account, since, limit)

    async def compact(
        self, account: str, through: str | None = None, max_idle_s: float = 604_800.0
    ) -> int:
        """Drop whole commits of the feed: no further than state ``through`` (when
        given; a ``through`` inside a commit keeps that commit), nor past the
        slowest registered consumer heard from within ``max_idle_s`` (7 days).
        With neither a bound nor a consumer nothing goes. Returns how many entries
        were removed."""
        return await self._core.compact(account, through, max_idle_s)

    async def ack(self, account: str, consumer: str, through: str) -> ConsumerCursor | StateTooOld:
        """Consumer ``consumer`` (1 to 64 letters, digits, ``.``, ``-``, ``_``) has
        durably processed the feed up to state ``through``: ``compact`` keeps
        everything after it. Registers the name on first use (at most 16 per
        account) and may move it backwards. ``StateTooOld`` stores nothing: rebuild,
        then ack the position the rebuild started from."""
        return await self._core.ack(account, consumer, through)

    async def consumers(self, account: str) -> list[ConsumerCursor]:
        """The registered consumers, in name order: who holds the log, since when,
        and who is lost."""
        return await self._core.consumers(account)

    async def drop_consumer(self, account: str, consumer: str) -> bool:
        """Forget a consumer (decommissioned): it no longer holds the log. Whether it
        was registered."""
        return await self._core.drop_consumer(account, consumer)

    async def message_ids(
        self, account: str, after: str | None = None, limit: int = 1000
    ) -> list[str]:
        """Up to ``limit`` message ids after ``after``, in id order (to rebuild a
        consumer's copy)."""
        return await self._core.message_ids(account, after, limit)

    async def read_blob(self, account: str, message_id: str) -> bytes:
        """The raw RFC 822 bytes of an imported message."""
        return await self._core.read_blob(account, message_id)

    async def parts(self, account: str, message_id: str) -> list[PartInfo]:
        """Every leaf part of an imported message, nested messages included, with
        its reference and role. The blob is read and parsed in Rust; only the
        metadata crosses over."""
        return await self._core.parts(account, message_id)

    async def refresh_facts(
        self,
        account: str,
        after: str | None = None,
        limit: int = 1_000,
        budget_s: float = 60.0,
    ) -> FactsRefresh:
        """Bring the facts of messages imported before the current extractor up
        to date from their stored content, without a Gmail call (one blob read
        per stale message). Schedule it like ``compact``: call again from
        ``next`` until it is ``None``; a second pass reads no blob. Each brought
        up to date logs a ``FactsRefreshed`` feed entry."""
        return await self._core.refresh_facts(account, after, limit, budget_s)

    async def open_part(
        self, account: str, message_id: str, part: str, chunk_size: int = 262_144
    ) -> PartStream:
        """Stream one part's decoded bytes in chunks of at most ``chunk_size``:
        ``async with await runner.open_part(...) as stream: async for chunk in
        stream``. The read follows the loop; leaving the ``async with``, or
        cancelling the task inside it, drops it. A failure ends the stream
        with an exception, never a short part."""
        return await self._core.open_part(account, message_id, part, chunk_size)

    async def read_part(
        self, account: str, message_id: str, part: str, max_bytes: int = 26_214_400
    ) -> bytes:
        """The decoded bytes of one part, by the reference from ``attachments`` or
        ``parts``: one ranged read of the stored blob, no Gmail call. Raises
        ``PartTooLarge`` above ``max_bytes`` before reading (stream it with
        ``open_part``), ``PartNotFound`` for a reference that names no part of
        the message, ``BlobNotFound`` when the content is gone."""
        return await self._core.read_part(account, message_id, part, max_bytes)
