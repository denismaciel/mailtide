from collections.abc import Callable
from typing import Any, Literal, final

class MailtideError(Exception): ...
class DelegationRevoked(MailtideError): ...
class InvalidPush(MailtideError): ...

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
    status: Status | None

@final
class ChangePage:
    changes: list[dict[str, Any]]
    next: str
    has_more: bool

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
    imported: bool
    size: int | None
    internal_date_ms: int | None
    rfc822_message_id: str | None
    in_reply_to: str | None
    references: list[str]
    sender: str | None
    to: list[str]
    cc: list[str]
    subject: str | None
    snippet: str | None

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
class Runner:
    def __init__(
        self,
        database_url: str,
        blob_url: str,
        token_source: Callable[[str], tuple[str, int]],
        *,
        watch_topic: str | None = None,
        gmail_origin: str | None = None,
        request_timeout_s: float = 60.0,
        confirm_timeout_s: float = 60.0,
    ) -> None: ...
    def migrate(self) -> None: ...
    def run(self, account: str, budget_s: float = 60.0) -> RunOutcome: ...
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
    def messages(self, account: str, message_ids: list[str]) -> list[MessageView]: ...
    def changes(self, account: str, since: str | None = None, limit: int = 500) -> ChangePage: ...
    def read_blob(self, account: str, message_id: str) -> bytes: ...

def version() -> str: ...
def verify_push(
    authorization: str,
    body: bytes,
    jwks: bytes,
    audience: str,
    service_account: str,
    now_ms: int | None = None,
) -> Notification: ...
def parse_message(raw: bytes) -> ParsedMessage: ...
