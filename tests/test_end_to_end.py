"""The real Runner (reqwest, SQLite, blob store) against a fake Gmail over HTTP."""

import base64
import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import mailtide
import pytest

ACCOUNT = "alice@example.com"
PREFIX = "/gmail/v1/users/me/"


class Mailbox:
    def __init__(self) -> None:
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

        def do_POST(self) -> None:  # noqa: N802
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


@pytest.fixture
def gmail() -> Iterator[tuple[Mailbox, str]]:
    mailbox = Mailbox()
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(mailbox))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield mailbox, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_bootstrap_import_and_follow(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX", "UNREAD"])
    mailbox.add("m2", [])
    runner = mailtide.Runner(
        "sqlite::memory:",
        "memory:///",
        lambda account: ("test-token", 2**62),
        gmail_origin=origin,
    )
    runner.migrate()

    outcome = runner.run(ACCOUNT, budget_s=30)
    assert outcome.stop == "idle", outcome.blocked
    assert outcome.status is not None and outcome.status.phase == "live"
    assert outcome.imported == 2
    raw = runner.read_blob(ACCOUNT, "m2")
    assert mailtide.parse_message(raw).text.strip() == "Body of m2"

    page = runner.changes(ACCOUNT)
    assert page.changes
    mailbox.add("m3", ["INBOX"])
    outcome = runner.run(ACCOUNT, budget_s=30)
    assert outcome.stop == "idle"
    assert outcome.cursor_after == 3
    assert outcome.imported == 1
    later = runner.changes(ACCOUNT, since=page.next)
    assert any("m3" in json.dumps(change) for change in later.changes)


def test_actions_overlay_deliver_and_settle(gmail: tuple[Mailbox, str]) -> None:
    mailbox, origin = gmail
    mailbox.add("m1", ["INBOX", "UNREAD"])
    runner = mailtide.Runner(
        "sqlite::memory:",
        "memory:///",
        lambda account: ("test-token", 2**62),
        gmail_origin=origin,
        confirm_timeout_s=0,
    )
    runner.migrate()
    assert runner.run(ACCOUNT, budget_s=30).stop == "idle"

    outcome = runner.act(ACCOUNT, "archive", message_ids=["m1"])
    assert outcome.stop == "idle", outcome.blocked
    assert mailbox.messages["m1"]["labels"] == ["UNREAD"]
    [view] = runner.messages(ACCOUNT, ["m1"])
    assert view.labels == ["UNREAD"]
    assert view.subject == "Message m1"

    runner.act(ACCOUNT, "mark_read", thread_id="t-m1")
    assert mailbox.messages["m1"]["labels"] == []
    runner.run(ACCOUNT, budget_s=30)
    [view] = runner.messages(ACCOUNT, ["m1"])
    assert view.labels == [] and view.observed_labels == []
    assert view.pending_actions == 0

    with pytest.raises(ValueError):
        runner.act(ACCOUNT, "explode", message_ids=["m1"])
