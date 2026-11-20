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
        self.history_floor = 0  # history requests from before it answer 404
        self.messages: dict[str, dict] = {}
        self.history: list[dict] = []
        self.labels: dict[str, dict] = {"INBOX": {"id": "INBOX", "name": "INBOX", "type": "system"}}
        # Search results by query, as message ids in Gmail's order. Like
        # Gmail's index they are not derived from the mailbox, and the fake
        # interprets no query: a query nobody scripted matches nothing.
        self.search_results: dict[str, list[str]] = {}
        self.search_refusal: tuple[int, str] | None = None  # (status, reason) searches get
        self.searches: list[dict[str, list[str]]] = []  # each search's parameters
        self.withheld: set[str] = set()  # raw fetches of these ids answer 503

    def add(
        self,
        mid: str,
        labels: list[str],
        *,
        thread: str | None = None,
        date_ms: int = 1_700_000_000_000,
        sender: str | None = None,
        to: list[str] | None = None,
        cc: list[str] | None = None,
    ) -> None:
        """A new message: in its own thread unless ``thread`` names one, from
        ``sender-{mid}@example.com`` unless ``sender`` says otherwise."""
        self.history_id += 1
        self.messages[mid] = {
            "labels": labels,
            "history_id": self.history_id,
            "thread": thread or f"t-{mid}",
            "date_ms": date_ms,
            "from": sender or f"sender-{mid}@example.com",
            "to": to or [],
            "cc": cc or [],
        }
        ref = {"id": mid, "threadId": self.messages[mid]["thread"], "labelIds": labels}
        self.history.append({"id": str(self.history_id), "messagesAdded": [{"message": ref}]})

    def delete(self, mid: str) -> None:
        """The message is deleted forever."""
        m = self.messages.pop(mid)
        self.history_id += 1
        ref = {"id": mid, "threadId": m["thread"]}
        self.history.append({"id": str(self.history_id), "messagesDeleted": [{"message": ref}]})

    def label(self, lid: str, name: str, kind: str = "user") -> None:
        """Create a label definition, or rename it (no history record, as in Gmail)."""
        self.labels[lid] = {"id": lid, "name": name, "type": kind}

    def search_page(self, query: dict[str, list[str]], threads: bool) -> tuple[int, dict]:
        """One page of the scripted results for ``q``, paged by
        ``maxResults`` with the offset as the page token."""
        self.searches.append(query)
        if self.search_refusal:
            status, reason = self.search_refusal
            error = {"code": status, "message": "refused", "errors": [{"reason": reason}]}
            return status, {"error": error}
        hits = self.search_results.get(query["q"][0], [])
        if threads:
            hits = list(dict.fromkeys(f"t-{mid}" for mid in hits))
        start = int(query.get("pageToken", ["0"])[0])
        end = start + int(query.get("maxResults", ["100"])[0])
        body: dict = {"resultSizeEstimate": len(hits)}
        if threads:
            body["threads"] = [
                {"id": tid, "snippet": "", "historyId": str(self.history_id)}
                for tid in hits[start:end]
            ]
        else:
            body["messages"] = [{"id": mid, "threadId": f"t-{mid}"} for mid in hits[start:end]]
        if end < len(hits):
            body["nextPageToken"] = str(end)
        return 200, body

    def expire_history(self) -> None:
        """Gmail dropped the history so far: syncing from before now answers 404."""
        self.history_id += 1
        self.history_floor = self.history_id

    def modify(self, mid: str, add: list[str], remove: list[str]) -> None:
        m = self.messages[mid]
        ref = {"id": mid, "threadId": m["thread"]}
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
            params = parse_qs(url.query, keep_blank_values=True)
            query = {k: v[0] for k, v in params.items()}
            parts = url.path.removeprefix(PREFIX).split("/")
            if parts == ["threads"] or (parts == ["messages"] and "q" in query):
                return self.reply(*mailbox.search_page(params, threads=parts == ["threads"]))
            if parts == ["profile"]:
                return self.reply(200, {"historyId": str(mailbox.history_id)})
            if parts == ["labels"]:
                return self.reply(200, {"labels": list(mailbox.labels.values())})
            if parts == ["history"]:
                start = int(query["startHistoryId"])
                if start < mailbox.history_floor:
                    return self.reply(404, {"error": {"code": 404, "message": "not found"}})
                records = [h for h in mailbox.history if int(h["id"]) > start]
                return self.reply(200, {"history": records, "historyId": str(mailbox.history_id)})
            if parts == ["messages"]:
                label = query.get("labelIds")
                ids = [
                    {"id": mid, "threadId": m["thread"]}
                    for mid, m in mailbox.messages.items()
                    if label is None or label in m["labels"]
                ]
                return self.reply(200, {"messages": ids})
            if len(parts) == 2 and parts[0] == "messages" and parts[1] in mailbox.messages:
                mid = parts[1]
                m = mailbox.messages[mid]
                body = {
                    "id": mid,
                    "threadId": m["thread"],
                    "labelIds": m["labels"],
                    "historyId": str(m["history_id"]),
                    "internalDate": str(m["date_ms"]),
                    "snippet": "hi",
                }
                if query.get("format") == "raw" and mid in mailbox.withheld:
                    error = {"code": 503, "message": "backend error"}
                    return self.reply(503, {"error": error})
                if query.get("format") == "raw":
                    recipients = "".join(
                        f"{name}: {', '.join(m[key])}\r\n"
                        for name, key in (("To", "to"), ("Cc", "cc"))
                        if m[key]
                    )
                    raw = (
                        f"Message-ID: <{mid}@example.com>\r\nFrom: {m['from']}\r\n{recipients}"
                        f"Subject: Message {mid}\r\n\r\nBody of {mid}\r\n"
                    ).encode()
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
    configure, ``gmail.mailbox`` the state (``add``, ``modify``, ``delete``,
    ``label``, ``expire_history``, ``refuse``, ``delay_s``, ``requests``,
    ``withheld``, and for searches ``search_results``, ``search_refusal``,
    ``searches``).
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

    def add(
        self,
        message_id: str,
        labels: list[str],
        *,
        thread: str | None = None,
        date_ms: int = 1_700_000_000_000,
        sender: str | None = None,
        to: list[str] | None = None,
        cc: list[str] | None = None,
    ) -> None:
        """A new message arrives with ``labels``, in ``thread`` if given."""
        with self.mailbox.lock:
            self.mailbox.add(
                message_id,
                labels,
                thread=thread,
                date_ms=date_ms,
                sender=sender,
                to=to,
                cc=cc,
            )

    def script_search(self, query: str, message_ids: list[str]) -> None:
        """Searches for exactly ``query`` match ``message_ids``, in this order
        (threads: their ``t-<id>`` threads)."""
        with self.mailbox.lock:
            self.mailbox.search_results[query] = message_ids


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
