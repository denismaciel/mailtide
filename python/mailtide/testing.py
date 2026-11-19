"""Test helpers for hosts: a fake Gmail over HTTP and a manual clock.

Point a runner at ``FakeGmail`` with ``Http(gmail_origin=gmail.origin)``,
a ``sqlite::memory:`` database and ``memory:///`` blobs, and pass a
``ManualClock`` as ``clock=`` to reach retries, cleanups and confirmations
by advancing time instead of sleeping.
"""

import base64
import json
import threading
import time
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

PREFIX = "/gmail/v1/users/me/"


class Mailbox:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.delay_s = 0.0
        self.requests = 0
        self.refuse: str | None = None  # a 403 reason every request gets
        self.history_id = 0
        self.messages: dict[str, dict] = {}
        self.history: list[dict] = []

    def add(self, mid: str, labels: list[str]) -> None:
        self.history_id += 1
        self.messages[mid] = {"labels": labels, "history_id": self.history_id}
        ref = {"id": mid, "threadId": f"t-{mid}", "labelIds": labels}
        self.history.append({"id": str(self.history_id), "messagesAdded": [{"message": ref}]})

    def modify(self, mid: str, add: list[str], remove: list[str]) -> None:
        m = self.messages[mid]
        ref = {"id": mid, "threadId": f"t-{mid}"}
        added = [label for label in add if label not in m["labels"]]
        removed = [label for label in remove if label in m["labels"]]
        for key, labels in (("labelsAdded", added), ("labelsRemoved", removed)):
            if labels:
                self.history_id += 1
                record = {"message": ref, "labelIds": labels}
                self.history.append({"id": str(self.history_id), key: [record]})
        m["labels"] = [label for label in m["labels"] + added if label not in removed]
        m["history_id"] = self.history_id


def handler_for(mailbox: Mailbox) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: object) -> None:
            pass

        def reply(self, status: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            if mailbox.delay_s:
                time.sleep(mailbox.delay_s)  # a slow Gmail, to test cancellation
            with mailbox.lock:
                mailbox.requests += 1
                if mailbox.refuse:
                    error = {
                        "code": 403,
                        "message": "refused",
                        "errors": [{"reason": mailbox.refuse}],
                    }
                    return self.reply(403, {"error": error})
                self._get()

        def do_POST(self) -> None:  # noqa: N802
            with mailbox.lock:
                self._post()

        def _get(self) -> None:
            assert self.headers["authorization"] == "Bearer test-token"
            url = urlparse(self.path)
            query = {k: v[0] for k, v in parse_qs(url.query).items()}
            parts = url.path.removeprefix(PREFIX).split("/")
            if parts == ["profile"]:
                return self.reply(200, {"historyId": str(mailbox.history_id)})
            if parts == ["labels"]:
                return self.reply(
                    200, {"labels": [{"id": "INBOX", "name": "INBOX", "type": "system"}]}
                )
            if parts == ["history"]:
                start = int(query["startHistoryId"])
                records = [h for h in mailbox.history if int(h["id"]) > start]
                return self.reply(200, {"history": records, "historyId": str(mailbox.history_id)})
            if parts == ["messages"]:
                label = query.get("labelIds")
                ids = [
                    {"id": mid, "threadId": f"t-{mid}"}
                    for mid, m in mailbox.messages.items()
                    if label is None or label in m["labels"]
                ]
                return self.reply(200, {"messages": ids})
            if len(parts) == 2 and parts[0] == "messages":
                mid = parts[1]
                m = mailbox.messages[mid]
                body = {
                    "id": mid,
                    "threadId": f"t-{mid}",
                    "labelIds": m["labels"],
                    "historyId": str(m["history_id"]),
                    "internalDate": "1700000000000",
                    "snippet": "hi",
                }
                if query.get("format") == "raw":
                    raw = f"Subject: Message {mid}\r\n\r\nBody of {mid}\r\n".encode()
                    body["raw"] = base64.urlsafe_b64encode(raw).decode().rstrip("=")
                return self.reply(200, body)
            return self.reply(404, {"error": {"code": 404, "message": "not found"}})

        def _post(self) -> None:
            assert self.headers["authorization"] == "Bearer test-token"
            length = int(self.headers.get("content-length", "0"))
            body = json.loads(self.rfile.read(length) or b"{}")
            parts = urlparse(self.path).path.removeprefix(PREFIX).split("/")
            if len(parts) == 3 and parts[0] == "messages" and parts[2] == "modify":
                add, remove = body.get("addLabelIds", []), body.get("removeLabelIds", [])
                mailbox.modify(parts[1], add, remove)
                return self.reply(200, {"id": parts[1]})
            return self.reply(404, {"error": {"code": 404, "message": "not found"}})

    return Handler


def serve() -> Generator[tuple[Mailbox, str], None, None]:
    mailbox = Mailbox()
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(mailbox))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield mailbox, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


class FakeGmail:
    """A fake Gmail mailbox served over HTTP on localhost.

    ``with FakeGmail() as gmail:`` starts it; ``gmail.origin`` is the URL to
    configure, ``gmail.mailbox`` the state (``add``, ``modify``, ``refuse``,
    ``delay_s``, ``requests``).
    """

    def __init__(self) -> None:
        self._serve = serve()
        self.mailbox, self.origin = next(self._serve)

    def __enter__(self) -> "FakeGmail":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._serve.close()

    def add(self, message_id: str, labels: list[str]) -> None:
        """A new message arrives with ``labels``."""
        with self.mailbox.lock:
            self.mailbox.add(message_id, labels)


class ManualClock:
    """A clock that only moves when told to; pass it as ``clock=``."""

    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self._now = start_ms
        self._lock = threading.Lock()

    def __call__(self) -> int:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._now += int(seconds * 1000)
