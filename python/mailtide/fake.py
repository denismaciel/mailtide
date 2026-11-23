"""Stateful multi-account fake Gmail server, token source, and CLI.

Run standalone for local development and stress testing:

    mailtide-fake --port 8200 --state ./fake-gmail.db --push-url http://127.0.0.1:8000/wake

Or embed from Python:

    from mailtide.fake import FakeGmailServer, fake_token_source

    with FakeGmailServer() as server:
        runner = mailtide.Runner(
            Config(..., http=Http(gmail_origin=server.origin)),
            fake_token_source(),
        )
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from collections.abc import Awaitable, Callable
from urllib.parse import quote
from urllib.request import Request, urlopen

from mailtide._core import FakeGmailServer

__all__ = ["FakeGmailServer", "fake_token_source", "main"]

_FAR_FUTURE_MS = 4_102_444_800_000  # 2100-01-01T00:00:00Z


def fake_token_source() -> Callable[[str], Awaitable[tuple[str, int]]]:
    """Return an async ``TokenSource`` that routes each account via ``fake:<account>``."""

    async def source(account: str) -> tuple[str, int]:
        return f"fake:{account.strip().lower()}", _FAR_FUTURE_MS

    return source


def _post_json(url: str, payload: dict[str, object]) -> dict[str, object]:
    data = json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers={"content-type": "application/json"}, method="POST")
    with urlopen(req, timeout=30) as resp:  # noqa: S310
        return json.loads(resp.read().decode("utf-8"))


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for ``mailtide-fake``."""
    parser = argparse.ArgumentParser(
        prog="mailtide-fake",
        description="Run the stateful multi-account fake Gmail server (loopback only).",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Loopback bind host (default 127.0.0.1)",
    )
    parser.add_argument("--port", type=int, default=8200, help="Port to listen on (0 for random)")
    parser.add_argument(
        "--state",
        default=None,
        help="Optional SQLite file path for persisting mailbox state across restarts",
    )
    parser.add_argument(
        "--push-url",
        default=None,
        help="Optional webhook URL receiving debounced {emailAddress, historyId} POSTs",
    )
    parser.add_argument(
        "--push-debounce-ms",
        type=int,
        default=150,
        help="Push webhook debounce window in milliseconds (default 150)",
    )
    parser.add_argument(
        "--seed-account",
        action="append",
        default=[],
        help="Account email to create (and seed if empty); may be repeated",
    )
    parser.add_argument(
        "--seed-threads",
        type=int,
        default=40,
        help="Number of synthetic threads to seed per --seed-account when empty (default 40)",
    )
    args = parser.parse_args(argv)

    server = FakeGmailServer(
        host=args.host,
        port=args.port,
        state_path=args.state,
        push_url=args.push_url,
        push_debounce_ms=args.push_debounce_ms,
    )
    stop = threading.Event()

    def handle_signal(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        for email in args.seed_account:
            encoded = quote(email.strip().lower(), safe="")
            status_url = f"{server.origin}/_control/accounts/{encoded}"
            try:
                with urlopen(status_url, timeout=5) as resp:  # noqa: S310
                    existing = json.loads(resp.read().decode("utf-8"))
                count = int(existing.get("summary", {}).get("message_count", 0))
            except Exception:
                count = 0
            if count == 0:
                if args.seed_threads > 0:
                    _post_json(
                        f"{server.origin}/_control/accounts/{encoded}/seed",
                        {"threads": args.seed_threads},
                    )
                else:
                    _post_json(f"{server.origin}/_control/accounts", {"email": email})

        print(f"mailtide-fake listening on {server.origin}", flush=True)
        print(f"  Inspector UI: {server.origin}/_ui", flush=True)
        if args.state:
            print(f"  State file:   {args.state}", flush=True)
        if args.push_url:
            print(f"  Push webhook: {args.push_url}", flush=True)

        while not stop.is_set():
            stop.wait(timeout=0.5)
    finally:
        server.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
