from collections.abc import Awaitable, Callable
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
class Superseded(MailtideError): ...
class SchemaOutOfDate(MailtideError): ...
class ReadOnlyMode(MailtideError): ...
class AccountNotTracked(MailtideError): ...
class BlobNotFound(MailtideError): ...

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
    changes: list[Change]
    next: str
    has_more: bool

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

@final
class Notification:
    account: str
    history_id: int

@final
class Attachment:
    filename: str | None
    content_type: str
    content_id: str | None
    data: bytes

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
    def labels(self, account: str) -> Awaitable[list[Label]]: ...
    def messages(self, account: str, message_ids: list[str]) -> Awaitable[list[MessageView]]: ...
    def changes(
        self, account: str, since: str | None = None, limit: int = 500
    ) -> Awaitable[ChangePage | StateTooOld]: ...
    def compact(self, account: str, through: str) -> Awaitable[int]: ...
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
    def labels(self, account: str) -> list[Label]: ...
    def messages(self, account: str, message_ids: list[str]) -> list[MessageView]: ...
    def changes(
        self, account: str, since: str | None = None, limit: int = 500
    ) -> ChangePage | StateTooOld: ...
    def compact(self, account: str, through: str) -> int: ...
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
