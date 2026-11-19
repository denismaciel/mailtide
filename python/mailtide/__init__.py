"""mailtide: a sans-IO Gmail sync engine in Rust. See SPEC.md and ARCHITECTURE.md.

The host builds one ``Runner`` per process (``await`` its methods; ``BlockingRunner``
for code without an event loop), calls ``run(account)`` on a schedule
and whenever a verified push arrives, reads the mirror with ``changes`` and
``messages``, and sends user actions with ``act``.
"""

from mailtide import config
from mailtide._core import (
    AccountStatus,
    Attachment,
    BlobUnavailable,
    BlockingRunner,
    ChangePage,
    CorruptState,
    DelegationRevoked,
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
    Notification,
    OutcomeUnknown,
    OverlayChanged,
    ParsedMessage,
    QuotaGateError,
    ResumeResult,
    RunOutcome,
    StateTooOld,
    Status,
    StoreUnavailable,
    Superseded,
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
    "Notification",
    "OutcomeUnknown",
    "OverlayChanged",
    "ParsedMessage",
    "QuotaGateError",
    "ResumeResult",
    "RunOutcome",
    "Runner",
    "StateTooOld",
    "Status",
    "StoreUnavailable",
    "Superseded",
    "config",
    "parse_message",
    "verify_push",
    "version",
]
