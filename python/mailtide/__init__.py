"""mailtide: a sans-IO Gmail sync engine in Rust. See SPEC.md and ARCHITECTURE.md.

The host builds one ``Runner`` per process, calls ``run(account)`` on a schedule
and whenever a verified push arrives, reads the mirror with ``changes`` and
``messages``, and sends user actions with ``act``.
"""

from mailtide._core import (
    Attachment,
    ChangePage,
    DelegationRevoked,
    InvalidPush,
    MailtideError,
    MessageView,
    Notification,
    ParsedMessage,
    Runner,
    RunOutcome,
    Status,
    parse_message,
    verify_push,
    version,
)

__all__ = [
    "Attachment",
    "ChangePage",
    "DelegationRevoked",
    "InvalidPush",
    "MailtideError",
    "MessageView",
    "Notification",
    "ParsedMessage",
    "RunOutcome",
    "Runner",
    "Status",
    "parse_message",
    "verify_push",
    "version",
]
