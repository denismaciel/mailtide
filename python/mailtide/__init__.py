"""mailtide: a sans-IO Gmail sync engine in Rust. See SPEC.md and ARCHITECTURE.md.

The host builds one ``Runner`` per process (``await`` its methods; ``BlockingRunner``
for code without an event loop), calls ``run(account)`` on a schedule
and whenever a verified push arrives, reads the mirror with ``changes`` and
``messages``, and sends user actions with ``act``.
"""

from mailtide import config
from mailtide._core import (
    SCHEMA_VERSION,
    AccountStatus,
    Attachment,
    BlobUnavailable,
    BlockingRunner,
    ChangePage,
    CorruptState,
    DelegationRevoked,
    DriftResult,
    ForgetResult,
    Health,
    InvalidPush,
    LabelDefinitionsChanged,
    LabelsChanged,
    MailtideError,
    MessageContent,
    MessageDeleted,
    MessageDiscovered,
    MessageImported,
    MessageView,
    Migration,
    Notification,
    NotifyResult,
    OutcomeUnknown,
    OverlayChanged,
    ParsedMessage,
    Progress,
    QuotaGateError,
    ReadOnlyMode,
    ResumeResult,
    RunOutcome,
    SchemaOutOfDate,
    SchemaStatus,
    StateTooOld,
    Status,
    StoreUnavailable,
    Superseded,
    migrations,
    parse_message,
    verify_push,
    version,
)
from mailtide._runner import Runner
from mailtide.config import Config

#: Every kind of feed entry; ``match`` on it with ``typing.assert_never``.
Change = (
    MessageDiscovered
    | MessageImported
    | LabelsChanged
    | OverlayChanged
    | MessageDeleted
    | LabelDefinitionsChanged
)

__all__ = [
    "AccountStatus",
    "Attachment",
    "BlobUnavailable",
    "BlockingRunner",
    "Change",
    "ChangePage",
    "Config",
    "CorruptState",
    "DelegationRevoked",
    "DriftResult",
    "ForgetResult",
    "Health",
    "InvalidPush",
    "LabelDefinitionsChanged",
    "LabelsChanged",
    "MailtideError",
    "MessageContent",
    "MessageDeleted",
    "MessageDiscovered",
    "MessageImported",
    "MessageView",
    "Migration",
    "Notification",
    "NotifyResult",
    "OutcomeUnknown",
    "OverlayChanged",
    "ParsedMessage",
    "Progress",
    "QuotaGateError",
    "ReadOnlyMode",
    "ResumeResult",
    "RunOutcome",
    "Runner",
    "SCHEMA_VERSION",
    "SchemaOutOfDate",
    "SchemaStatus",
    "StateTooOld",
    "Status",
    "StoreUnavailable",
    "Superseded",
    "config",
    "migrations",
    "parse_message",
    "verify_push",
    "version",
]
