from collections.abc import Awaitable, Callable, Sequence
from typing import Literal, final

from mailtide.config import Config

class MailtideError(Exception): ...
class DelegationRevoked(MailtideError): ...
class InvalidPush(MailtideError): ...
class StoreUnavailable(MailtideError): ...
class OutcomeUnknown(MailtideError): ...
class CorruptState(MailtideError): ...
class BlobUnavailable(MailtideError): ...
class QuotaGateError(MailtideError): ...

class QuotaDeferred(MailtideError):
    until_ms: int

class WriteOutcomeUnknown(MailtideError): ...
class Superseded(MailtideError): ...
class SchemaOutOfDate(MailtideError): ...
class ReadOnlyMode(MailtideError): ...
class AccountNotTracked(MailtideError): ...
class BlobNotFound(MailtideError): ...
class SearchRefused(MailtideError): ...

class UnknownLabel(SearchRefused):
    label_id: str

class InvalidPageToken(MailtideError): ...
class PageTokenRejected(InvalidPageToken): ...
class GmailUnavailable(MailtideError): ...
class AccountBlocked(MailtideError): ...

class ComposeError(ValueError):
    """compose_message refused its input; the message never holds the value."""

    field: str
    index: int | None
    reason: str

SCHEMA_VERSION: int

@final
class Status:
    account: str
    phase: Literal["bootstrapping", "live", "reconciling", "blocked"]
    blocked: str | None
    cursor: int | None
    cursor_advanced_at_ms: int | None
    cooldown_until_ms: int | None
    discovered: int | None
    imported: int | None
    discovery_complete: bool | None

@final
class RunOutcome:
    stop: Literal["idle", "budget_exhausted", "cooling_down", "blocked", "superseded"]
    next_run_at_ms: int | None
    blocked: str | None
    cursor_before: int | None
    cursor_after: int | None
    commits: int
    imported: int
    gmail_requests: int
    admitted: int
    failed_requests: int
    last_failure: str | None
    status: Status | None

@final
class MessageDiscovered:
    kind: Literal["message_discovered"]
    state: str
    index: int
    id: str
    thread_id: str
    labels: list[str]

@final
class MessageImported:
    kind: Literal["message_imported"]
    state: str
    index: int
    id: str
    thread_id: str
    labels: list[str]
    internal_date_ms: int
    blob: str
    size: int
    content: MessageContent | None

@final
class LabelsChanged:
    kind: Literal["labels_changed"]
    state: str
    index: int
    id: str
    thread_id: str
    added: list[str]
    removed: list[str]

@final
class OverlayChanged:
    kind: Literal["overlay_changed"]
    state: str
    index: int
    id: str
    thread_id: str
    labels: list[str]

@final
class MessageDeleted:
    kind: Literal["message_deleted"]
    state: str
    index: int
    id: str
    thread_id: str | None
    labels: list[str] | None

@final
class LabelDefinitionsChanged:
    kind: Literal["label_definitions_changed"]
    state: str
    index: int

Change = (
    MessageDiscovered
    | MessageImported
    | LabelsChanged
    | OverlayChanged
    | MessageDeleted
    | LabelDefinitionsChanged
)

@final
class StateTooOld:
    kind: Literal["too_old"]
    oldest: str

@final
class ChangePage:
    """At most ``limit`` entries: whole commits, unless one alone is larger."""

    changes: list[Change]
    next: str  # opaque; inside a commit only when that commit is larger than limit
    has_more: bool

@final
class ConsumerCursor:
    """How far a named consumer acknowledged the feed."""

    name: str
    state: str  # the position, as changes() takes it
    seen_at_ms: int  # last ack, or the last compaction that found it caught up
    lost: bool  # behind the feed's floor: rebuild, then ack again

@final
class MessageContent:
    blob: str
    size: int
    internal_date_ms: int
    rfc822_message_id: str | None
    in_reply_to: str | None
    references: list[str]
    sender: str | None
    to: list[str]
    cc: list[str]
    subject: str | None
    snippet: str | None
    facts_version: int
    bcc: list[str]
    reply_to: list[str]
    sent_at_ms: int | None
    sent_at_offset_min: int | None
    sender_name: str | None
    sender_email: str | None
    list_id: str | None
    list_unsubscribe: list[str]
    one_click_unsubscribe: bool
    auto_submitted: str | None
    precedence: str | None
    tracking_id: str | None
    attachments: list[AttachmentInfo] | None
    attachment_count: int | None
    inline_count: int | None
    protocol_count: int | None
    has_attachment: bool | None

@final
class AttachmentInfo:
    part: str
    filename: str | None
    content_type: str
    size: int
    inline: bool
    disposition: str | None
    content_id: str | None

@final
class MessageView:
    id: str
    thread_id: str
    labels: list[str]
    observed_labels: list[str]
    checkpoint: int | None
    unverified: bool
    pending_actions: int
    blocked_actions: int
    content: MessageContent | None

@final
class ThreadView:
    id: str
    messages: list[MessageView]
    labels: list[str]
    unread: bool
    latest: MessageView | None
    message_count: int
    draft_count: int
    awaiting_import: int
    participants: list[str]

@final
class Label:
    id: str
    name: str
    kind: Literal["system", "user"]

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

@final
class Notification:
    account: str
    history_id: int

@final
class Attachment:
    filename: str | None
    content_type: str
    content_id: str | None
    part: str | None
    data: bytes
    def __init__(
        self,
        content_type: str,
        data: bytes,
        *,
        filename: str | None = None,
        content_id: str | None = None,
    ) -> None: ...

@final
class Address:
    name: str | None
    email: str
    def __init__(self, email: str, name: str | None = None) -> None: ...

@final
class OutgoingMessage:
    """A message to compose. ``message_id`` and ``tracking_id`` left ``None``
    are minted at each compose and ``sent_at_ms`` left ``None`` is the time of
    the compose. Gmail replaces Message-ID and Date on every draft save and
    send; ``tracking_id`` (the ``X-Mailtide-Id`` header) survives both."""

    from_: Address
    sender: Address | None
    reply_to: list[Address]
    to: list[Address]
    cc: list[Address]
    bcc: list[Address]
    subject: str | None
    sent_at_ms: int | None
    message_id: str | None
    tracking_id: str | None
    in_reply_to: str | None
    references: list[str]
    headers: list[tuple[str, str]]
    text: str | None
    html: str | None
    inline: list[Attachment]
    attachments: list[Attachment]
    def __init__(
        self,
        from_: Address | str,
        *,
        sender: Address | str | None = None,
        reply_to: Sequence[Address | str] = (),
        to: Sequence[Address | str] = (),
        cc: Sequence[Address | str] = (),
        bcc: Sequence[Address | str] = (),
        subject: str | None = None,
        sent_at_ms: int | None = None,
        message_id: str | None = None,
        tracking_id: str | None = None,
        in_reply_to: str | None = None,
        references: Sequence[str] = (),
        headers: Sequence[tuple[str, str]] = (),
        text: str | None = None,
        html: str | None = None,
        inline: Sequence[Attachment] = (),
        attachments: Sequence[Attachment] = (),
    ) -> None: ...

@final
class ComposedMessage:
    raw: bytes
    message_id: str | None
    tracking_id: str | None
    recipients: list[str]

@final
class ParsedMessage:
    headers: list[tuple[str, str]]
    text: str | None
    html: str | None
    attachments: list[Attachment]

@final
class AccountStatus:
    account: str
    phase: Literal["bootstrapping", "live", "reconciling", "blocked"]
    blocked: str | None
    epoch: int
    cursor: int | None
    watch_expiration_ms: int | None
    last_activity_ms: int | None
    cooldown_until_ms: int | None
    state: str
    messages: int
    pending_imports: int
    unverified: int
    jobs_ready: int
    jobs_running: int
    jobs_blocked: int
    actions_pending: int
    actions_delivered: int
    actions_blocked: int

@final
class Progress:
    commits: int
    imported: int
    gmail_requests: int
    elapsed_ms: int

@final
class SchemaStatus:
    state: Literal["current", "behind", "ahead"]
    applied: int
    expected: int

@final
class Migration:
    version: int
    statements: list[str]

@final
class TrackResult:
    outcome: Literal["tracked", "already_tracked"]
    generation: int | None

@final
class DriftResult:
    message_id: str
    verdict: Literal["matches", "explained", "differs", "missing_in_gmail", "deferred", "failed"]
    reason: Literal["pending_actions", "unverified", "mirror_behind"] | None
    mirror_labels: list[str] | None
    gmail_labels: list[str] | None
    failure: str | None

@final
class NotifyResult:
    outcome: Literal["current", "ran", "unknown_account"]
    run: RunOutcome | None

@final
class ResumeResult:
    outcome: Literal["resumed", "not_blocked", "unknown"]
    phase: Literal["bootstrapping", "live", "reconciling"] | None
    jobs: int

@final
class PrioritiseResult:
    """What ``prioritise`` did with each distinct id, in request order."""

    queued: list[str]
    imported: list[str]
    unknown: list[str]
    blocked: list[str]
    over_limit: list[str]

@final
class SearchHit:
    """A message Gmail's search matched, and what the mirror holds for it."""

    message_id: str
    thread_id: str
    state: Literal["imported", "not_imported", "unknown"]

@final
class SearchPage:
    """One page of ``search``: hits in Gmail's order, deleted ones left out."""

    hits: list[SearchHit]
    next_page_token: str | None
    estimate: int
    deleted: int
    mirror_state: str
    cursor: int | None
    phase: Literal["bootstrapping", "live", "reconciling", "blocked"]

@final
class ThreadHit:
    thread_id: str
    history_id: int | None

@final
class ThreadSearchPage:
    """One page of ``search_threads``."""

    hits: list[ThreadHit]
    next_page_token: str | None
    estimate: int
    mirror_state: str
    cursor: int | None
    phase: Literal["bootstrapping", "live", "reconciling", "blocked"]

@final
class ForgetResult:
    outcome: Literal["forgotten", "unknown"]
    messages: int
    blobs: int

@final
class Health:
    database: Literal["ok", "unavailable"]
    detail: str | None

TokenSource = Callable[[str], tuple[str, int] | Awaitable[tuple[str, int]]]

@final
class Runner:
    """Internal: methods return asyncio futures; use ``mailtide.Runner``."""

    def __init__(
        self,
        config: Config,
        token_source: TokenSource,
        *,
        clock: Callable[[], int] | None = None,
    ) -> None: ...
    def migrate(self) -> Awaitable[None]: ...
    def schema(self) -> Awaitable[SchemaStatus]: ...
    def track(self, account: str) -> Awaitable[TrackResult]: ...
    def run(
        self,
        account: str,
        budget_s: float = 60.0,
        on_progress: Callable[[Progress], object] | None = None,
    ) -> Awaitable[RunOutcome]: ...
    def act(
        self,
        account: str,
        action: Action,
        *,
        message_ids: list[str] | None = None,
        thread_id: str | None = None,
        add_labels: list[str] = ...,
        remove_labels: list[str] = ...,
        destination: Literal["inbox", "archive"] = "inbox",
        budget_s: float = 30.0,
    ) -> Awaitable[RunOutcome]: ...
    def admit(
        self,
        account: str,
        action: Action,
        *,
        message_ids: list[str] | None = None,
        thread_id: str | None = None,
        add_labels: list[str] = ...,
        remove_labels: list[str] = ...,
        destination: Literal["inbox", "archive"] = "inbox",
    ) -> Awaitable[int]: ...
    def prioritise(self, account: str, message_ids: list[str]) -> Awaitable[PrioritiseResult]: ...
    def status(self, account: str) -> Awaitable[AccountStatus | None]: ...
    def accounts(self) -> Awaitable[list[str]]: ...
    def drift(
        self, account: str, message_ids: list[str], budget_s: float = 120.0
    ) -> Awaitable[list[DriftResult]]: ...
    def notify(
        self, account: str, history_id: int, budget_s: float = 60.0
    ) -> Awaitable[NotifyResult]: ...
    def resume(self, account: str) -> Awaitable[ResumeResult]: ...
    def forget(self, account: str) -> Awaitable[ForgetResult]: ...
    def health(self) -> Awaitable[Health]: ...
    def search(
        self,
        account: str,
        query: str,
        *,
        label_ids: list[str] = ...,
        include_spam_trash: bool = False,
        limit: int = 100,
        page_token: str | None = None,
        priority: Priority = "foreground",
        budget_s: float = 10.0,
    ) -> Awaitable[SearchPage]: ...
    def search_threads(
        self,
        account: str,
        query: str,
        *,
        label_ids: list[str] = ...,
        include_spam_trash: bool = False,
        limit: int = 100,
        page_token: str | None = None,
        priority: Priority = "foreground",
        budget_s: float = 10.0,
    ) -> Awaitable[ThreadSearchPage]: ...
    def labels(self, account: str) -> Awaitable[list[Label]]: ...
    def messages(self, account: str, message_ids: list[str]) -> Awaitable[list[MessageView]]: ...
    def threads(self, account: str, thread_ids: list[str]) -> Awaitable[list[ThreadView]]: ...
    def changes(
        self, account: str, since: str | None = None, limit: int = 500
    ) -> Awaitable[ChangePage | StateTooOld]: ...
    def compact(
        self, account: str, through: str | None = None, max_idle_s: float = 604_800.0
    ) -> Awaitable[int]: ...
    def ack(
        self, account: str, consumer: str, through: str
    ) -> Awaitable[ConsumerCursor | StateTooOld]: ...
    def consumers(self, account: str) -> Awaitable[list[ConsumerCursor]]: ...
    def drop_consumer(self, account: str, consumer: str) -> Awaitable[bool]: ...
    def message_ids(
        self, account: str, after: str | None = None, limit: int = 1000
    ) -> Awaitable[list[str]]: ...
    def read_blob(self, account: str, message_id: str) -> Awaitable[bytes]: ...

@final
class BlockingRunner:
    def __init__(
        self,
        config: Config,
        token_source: TokenSource,
        *,
        clock: Callable[[], int] | None = None,
    ) -> None: ...
    def migrate(self) -> None: ...
    def schema(self) -> SchemaStatus: ...
    def track(self, account: str) -> TrackResult: ...
    def run(
        self,
        account: str,
        budget_s: float = 60.0,
        on_progress: Callable[[Progress], object] | None = None,
    ) -> RunOutcome: ...
    def act(
        self,
        account: str,
        action: Action,
        *,
        message_ids: list[str] | None = None,
        thread_id: str | None = None,
        add_labels: list[str] = ...,
        remove_labels: list[str] = ...,
        destination: Literal["inbox", "archive"] = "inbox",
        budget_s: float = 30.0,
    ) -> RunOutcome: ...
    def admit(
        self,
        account: str,
        action: Action,
        *,
        message_ids: list[str] | None = None,
        thread_id: str | None = None,
        add_labels: list[str] = ...,
        remove_labels: list[str] = ...,
        destination: Literal["inbox", "archive"] = "inbox",
    ) -> int: ...
    def prioritise(self, account: str, message_ids: list[str]) -> PrioritiseResult: ...
    def status(self, account: str) -> AccountStatus | None: ...
    def accounts(self) -> list[str]: ...
    def drift(
        self, account: str, message_ids: list[str], budget_s: float = 120.0
    ) -> list[DriftResult]: ...
    def notify(self, account: str, history_id: int, budget_s: float = 60.0) -> NotifyResult: ...
    def resume(self, account: str) -> ResumeResult: ...
    def forget(self, account: str) -> ForgetResult: ...
    def health(self) -> Health: ...
    def search(
        self,
        account: str,
        query: str,
        *,
        label_ids: list[str] = ...,
        include_spam_trash: bool = False,
        limit: int = 100,
        page_token: str | None = None,
        priority: Priority = "foreground",
        budget_s: float = 10.0,
    ) -> SearchPage: ...
    def search_threads(
        self,
        account: str,
        query: str,
        *,
        label_ids: list[str] = ...,
        include_spam_trash: bool = False,
        limit: int = 100,
        page_token: str | None = None,
        priority: Priority = "foreground",
        budget_s: float = 10.0,
    ) -> ThreadSearchPage: ...
    def labels(self, account: str) -> list[Label]: ...
    def messages(self, account: str, message_ids: list[str]) -> list[MessageView]: ...
    def threads(self, account: str, thread_ids: list[str]) -> list[ThreadView]: ...
    def changes(
        self, account: str, since: str | None = None, limit: int = 500
    ) -> ChangePage | StateTooOld: ...
    def compact(
        self, account: str, through: str | None = None, max_idle_s: float = 604_800.0
    ) -> int: ...
    def ack(self, account: str, consumer: str, through: str) -> ConsumerCursor | StateTooOld: ...
    def consumers(self, account: str) -> list[ConsumerCursor]: ...
    def drop_consumer(self, account: str, consumer: str) -> bool: ...
    def message_ids(
        self, account: str, after: str | None = None, limit: int = 1000
    ) -> list[str]: ...
    def read_blob(self, account: str, message_id: str) -> bytes: ...

def version() -> str: ...
def migrations() -> list[Migration]: ...
def verify_push(
    authorization: str,
    body: bytes,
    jwks: bytes,
    audience: str,
    service_account: str,
    now_ms: int | None = None,
) -> Notification: ...
def parse_message(raw: bytes) -> ParsedMessage: ...
def compose_message(
    message: OutgoingMessage,
    *,
    purpose: Literal["send", "draft"] = "send",
    max_bytes: int | None = None,
) -> ComposedMessage: ...
def new_message_id(sender: str) -> str: ...
def new_tracking_id() -> str: ...
def tracking_id(raw: bytes) -> str | None: ...
def parse_addresses(value: str) -> list[Address]: ...
def search_literal(text: str) -> str: ...
def search_by_message_id(message_id: str) -> str: ...
def decode_header(value: str) -> str: ...
