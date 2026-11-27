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
from urllib.parse import parse_qs, unquote, urlparse

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
        self.send_calls = 0
        self.drop_send_answer = False
        self.send_refusal: tuple[int, str] | None = None
        self.drafts: dict[str, str] = {}  # draft id -> its current message id
        self.next_id = 0
        self.history: list[dict] = []
        self.labels: dict[str, dict] = {
            lid: {"id": lid, "name": lid, "type": "system"} for lid in ("INBOX", "UNREAD")
        }
        # Search results by query, as message ids in Gmail's order. Like
        # Gmail's index they are not derived from the mailbox, and the fake
        # interprets no query: a query nobody scripted matches nothing.
        self.search_results: dict[str, list[str]] = {}
        self.search_refusal: tuple[int, str] | None = None  # (status, reason) searches get
        self.searches: list[dict[str, list[str]]] = []  # each search's parameters
        self.withheld: set[str] = set()  # raw fetches of these ids answer 503
        self.next_label = 0
        # Whether the token holds https://mail.google.com/, the only scope
        # messages.delete and batchDelete accept; without it they answer 403.
        self.full_scope = True
        self.delete_failure: tuple[int, str] | None = None
        self.deletes: list[list[str]] = []  # the ids of each delete call Gmail applied

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
        raw: bytes | None = None,
    ) -> None:
        """A new message: in its own thread unless ``thread`` names one, from
        ``sender-{mid}@example.com`` unless ``sender`` says otherwise; ``raw``
        is its RFC 822 content (generated from the rest when omitted)."""
        self.history_id += 1
        self.messages[mid] = {
            "labels": labels,
            "history_id": self.history_id,
            "thread": thread or f"t-{mid}",
            "date_ms": date_ms,
            "from": sender or f"sender-{mid}@example.com",
            "to": to or [],
            "cc": cc or [],
            "raw": raw,
        }
        ref = {"id": mid, "threadId": self.messages[mid]["thread"], "labelIds": labels}
        self.history.append({"id": str(self.history_id), "messagesAdded": [{"message": ref}]})

    def stored(self, raw: bytes, labels: list[str], thread: str | None) -> str:
        """A new message with these bytes (Gmail's own Message-ID in place of
        the client's, as on every draft save and send) and a new id."""
        self.next_id += 1
        mid = f"m-{self.next_id:04d}"
        head, sep, body = raw.partition(b"\r\n\r\n")
        kept = [line for line in head.split(b"\r\n") if not line.lower().startswith(b"message-id:")]
        stamp = f"Message-ID: <gmail-{self.next_id:04d}@mail.gmail.com>".encode()
        self.add(mid, labels, thread=thread)
        self.messages[mid]["raw"] = b"\r\n".join([stamp, *kept]) + sep + body
        return mid

    def create_draft(self, raw: bytes, thread: str | None = None) -> tuple[str, str]:
        """Save a new draft (any client): ``(draft id, message id)``."""
        message = self.stored(raw, ["DRAFT"], thread)
        draft = f"r-{self.next_id:04d}"
        self.drafts[draft] = message
        return draft, message

    def update_draft(self, draft: str, raw: bytes, thread: str | None = None) -> str | None:
        """Replace a draft's message (any client): a new message id, ``None``
        if there is no such draft. Without ``thread`` the draft leaves its
        thread, as Gmail does."""
        if draft not in self.drafts:
            return None
        self.delete(self.drafts[draft])
        self.drafts[draft] = self.stored(raw, ["DRAFT"], thread)
        return self.drafts[draft]

    def delete_draft(self, draft: str) -> bool:
        """Delete a draft for good (any client); ``False`` if there is none."""
        if draft not in self.drafts:
            return False
        self.delete(self.drafts.pop(draft))
        return True

    def send_draft(self, draft: str) -> tuple[str, str] | None:
        """Send a draft (any client): the draft goes, a new SENT message in its
        thread comes. ``(message id, thread id)``, ``None`` if there is none."""
        if draft not in self.drafts:
            return None
        mid = self.drafts.pop(draft)
        old = self.messages[mid]
        self.delete(mid)
        sent = self.stored(old["raw"], ["SENT"], old["thread"])
        return sent, old["thread"]

    def delete(self, mid: str) -> None:
        """The message is deleted forever."""
        m = self.messages.pop(mid)
        self.history_id += 1
        ref = {"id": mid, "threadId": m["thread"]}
        self.history.append({"id": str(self.history_id), "messagesDeleted": [{"message": ref}]})

    def restore(self, mid: str) -> None:
        """Another client restores the message from the trash to the inbox."""
        self.modify(mid, ["INBOX"], ["TRASH"])

    def label(self, lid: str, name: str, kind: str = "user", **attrs: object) -> None:
        """Create a label definition, or rename it (no history record, as in Gmail).
        ``attrs`` are Gmail's fields: ``labelListVisibility``,
        ``messageListVisibility``, ``color``."""
        self.labels[lid] = {"id": lid, "name": name, "type": kind, **attrs}

    def create_label(self, name: str) -> str:
        """Another client creates a user label; returns its id."""
        self.next_label += 1
        lid = f"Label_{self.next_label}"
        while lid in self.labels:
            self.next_label += 1
            lid = f"Label_{self.next_label}"
        self.label(lid, name, labelListVisibility="labelShow", messageListVisibility="show")
        return lid

    def rename_label(self, lid: str, name: str) -> None:
        """Another client renames a label (no history record, as in Gmail)."""
        self.labels[lid]["name"] = name

    def delete_label(self, lid: str, *, history: bool = True) -> None:
        """A label is deleted: it leaves its messages, with one ``labelsRemoved``
        record each as Gmail writes them, or none if ``history`` is false."""
        del self.labels[lid]
        for mid, m in self.messages.items():
            if lid in m["labels"]:
                if history:
                    self.modify(mid, [], [lid])
                else:
                    m["labels"] = [label for label in m["labels"] if label != lid]

    def holder(self, name: str, other_than: str | None = None) -> str | None:
        """The label whose name Gmail would take ``name`` for (case and
        surrounding space ignored)."""
        wanted = name.strip().lower()
        for lid, label in self.labels.items():
            if lid != other_than and label["name"].strip().lower() == wanted:
                return lid
        return None

    def defines(self, lid: str) -> bool:
        return lid in self.labels or not lid.startswith("Label_")

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


def decode(raw: str) -> bytes:
    """Unpadded base64url, as Gmail's ``raw`` fields carry it."""
    return base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))


def handler_for(mailbox: Mailbox) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
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

        def do_PATCH(self) -> None:
            with mailbox.lock:
                self._patch()

        def do_PUT(self) -> None:
            # drafts.update: `_post` hands every drafts route to `_drafts`.
            self.do_POST()

        def do_DELETE(self) -> None:
            parts = urlparse(self.path).path.removeprefix(PREFIX).split("/")
            if parts[0] == "drafts":
                return self.do_POST()
            with mailbox.lock:
                self._delete()

        def refuse(self, status: int, reason: str, message: str) -> None:
            error = {"code": status, "message": message, "errors": [{"reason": reason}]}
            self.reply(status, {"error": error})

        def label_id(self) -> str | None:
            parts = urlparse(self.path).path.removeprefix(PREFIX).split("/")
            return unquote(parts[1]) if len(parts) == 2 and parts[0] == "labels" else None

        def body(self) -> dict:
            length = int(self.headers.get("content-length", "0"))
            return json.loads(self.rfile.read(length) or b"{}")

        def _patch(self) -> None:
            assert self.headers["authorization"] == "Bearer test-token"
            body = self.body()
            lid = self.label_id()
            if lid is None or lid not in mailbox.labels:
                return self.reply(404, {"error": {"code": 404, "message": "not found"}})
            if "name" in body and mailbox.holder(body["name"], other_than=lid):
                return self.refuse(409, "aborted", "Label name exists or conflicts")
            label = mailbox.labels[lid]
            for key in ("name", "labelListVisibility", "messageListVisibility"):
                if key in body:
                    label[key] = body[key]
            if "color" in body:
                if body["color"] is None:
                    label.pop("color", None)
                else:
                    label["color"] = body["color"]
            return self.reply(200, label)

        def no_content(self) -> None:
            self.send_response(204)
            self.send_header("content-length", "0")
            self.end_headers()

        def scope_refused(self) -> bool:
            """Answer a delete as Gmail does a token without the full scope."""
            if mailbox.full_scope:
                return False
            error = {
                "code": 403,
                "message": "Request had insufficient authentication scopes.",
                "errors": [{"reason": "insufficientPermissions"}],
                "status": "PERMISSION_DENIED",
                "details": [{"reason": "ACCESS_TOKEN_SCOPE_INSUFFICIENT"}],
            }
            self.reply(403, {"error": error})
            return True

        def _delete(self) -> None:
            assert self.headers["authorization"] == "Bearer test-token"
            parts = urlparse(self.path).path.removeprefix(PREFIX).split("/")
            if len(parts) == 2 and parts[0] == "messages":
                if self.scope_refused():
                    return None
                if mailbox.delete_failure:
                    status, reason = mailbox.delete_failure
                    return self.refuse(status, reason, "delete failed")
                if parts[1] not in mailbox.messages:
                    return self.reply(404, {"error": {"code": 404, "message": "not found"}})
                mailbox.delete(parts[1])
                mailbox.deletes.append([parts[1]])
                return self.no_content()
            lid = self.label_id()
            if lid is None or lid not in mailbox.labels:
                return self.reply(404, {"error": {"code": 404, "message": "not found"}})
            mailbox.delete_label(lid)
            self.send_response(204)
            self.send_header("content-length", "0")
            self.end_headers()

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
            if len(parts) == 2 and parts[0] == "labels":
                if parts[1] in mailbox.labels:
                    return self.reply(200, mailbox.labels[parts[1]])
                return self.reply(404, {"error": {"code": 404, "message": "not found"}})
            if parts == ["history"]:
                start = int(query["startHistoryId"])
                if start < mailbox.history_floor:
                    return self.reply(404, {"error": {"code": 404, "message": "not found"}})
                records = [h for h in mailbox.history if int(h["id"]) > start]
                return self.reply(200, {"history": records, "historyId": str(mailbox.history_id)})
            if parts[0] == "drafts":
                return self._drafts(parts, query, {})
            if parts == ["messages"]:
                if query.get("fields") == "messages(id),nextPageToken":
                    limit = int(query["maxResults"])
                    position = query.get("pageToken")
                    ids = [
                        mid
                        for mid in sorted(mailbox.messages, reverse=True)
                        if position is None or mid < position
                    ]
                    page: dict = {"messages": [{"id": mid} for mid in ids[:limit]]}
                    if len(ids) > limit:
                        page["nextPageToken"] = ids[limit - 1]
                    return self.reply(200, page)
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
                        m["raw"]
                        or (
                            f"Message-ID: <{mid}@example.com>\r\nFrom: {m['from']}\r\n{recipients}"
                            f"Subject: Message {mid}\r\n\r\nBody of {mid}\r\n"
                        ).encode()
                    )
                    body["raw"] = base64.urlsafe_b64encode(raw).decode().rstrip("=")
                return self.reply(200, body)
            return self.reply(404, {"error": {"code": 404, "message": "not found"}})

        def _post(self) -> None:
            assert self.headers["authorization"] == "Bearer test-token"
            length = int(self.headers.get("content-length", "0"))
            body = json.loads(self.rfile.read(length) or b"{}")
            parts = urlparse(self.path).path.removeprefix(PREFIX).split("/")
            if parts[0] == "drafts":
                return self._drafts(parts, {}, body)
            if parts == ["messages", "send"]:
                mailbox.send_calls += 1
                if mailbox.send_refusal:
                    status, reason = mailbox.send_refusal
                    return self.refuse(status, reason, "send refused")
                thread = body.get("threadId")
                if thread is not None and all(
                    m["thread"] != thread for m in mailbox.messages.values()
                ):
                    return self.refuse(404, "notFound", "Requested entity was not found.")
                mid = mailbox.stored(decode(body["raw"]), ["SENT"], thread)
                if mailbox.drop_send_answer:
                    mailbox.drop_send_answer = False
                    self.close_connection = True
                    return
                return self.reply(200, {"id": mid, "threadId": mailbox.messages[mid]["thread"]})
            if len(parts) == 3 and parts[0] == "messages" and parts[2] == "modify":
                add, remove = body.get("addLabelIds", []), body.get("removeLabelIds", [])
                if not all(mailbox.defines(label) for label in add):
                    # Gmail refuses the whole request for one unknown add.
                    return self.refuse(400, "invalidArgument", "labelId not found")
                mailbox.modify(parts[1], add, remove)
                return self.reply(200, {"id": parts[1]})
            if parts == ["messages", "batchDelete"]:
                if self.scope_refused():
                    return None
                if mailbox.delete_failure:
                    status, reason = mailbox.delete_failure
                    return self.refuse(status, reason, "delete failed")
                # Deletes the ids that exist; the answer names none.
                for mid in body["ids"]:
                    if mid in mailbox.messages:
                        mailbox.delete(mid)
                mailbox.deletes.append(list(body["ids"]))
                return self.no_content()
            if parts == ["labels"]:
                if mailbox.holder(body.get("name", "")):
                    return self.refuse(409, "aborted", "Label name exists or conflicts")
                fields = {k: v for k, v in body.items() if k != "name"}
                lid = mailbox.create_label(body["name"])
                mailbox.labels[lid].update(fields)
                return self.reply(200, mailbox.labels[lid])
            return self.reply(404, {"error": {"code": 404, "message": "not found"}})

        def _draft(self, draft: str, *, raw: bool = False) -> None:
            """A draft as drafts.get answers it, or 404."""
            if draft not in mailbox.drafts:
                error = {"code": 404, "message": "Requested entity was not found."}
                return self.reply(404, {"error": error})
            mid = mailbox.drafts[draft]
            m = mailbox.messages[mid]
            message = {"id": mid, "threadId": m["thread"], "labelIds": m["labels"]}
            if raw:
                message["raw"] = base64.urlsafe_b64encode(m["raw"]).decode().rstrip("=")
            return self.reply(200, {"id": draft, "message": message})

        def _drafts(self, parts: list[str], query: dict[str, str], body: dict) -> None:
            """The users.drafts routes."""
            method = self.command
            message = body.get("message", {})
            thread = message.get("threadId")
            if method == "GET" and parts == ["drafts"]:
                # Like Gmail, drafts in spam or trash only when asked for.
                hidden = set() if query.get("includeSpamTrash") == "true" else {"SPAM", "TRASH"}
                drafts = [
                    {"id": draft, "message": {"id": mid, "threadId": m["thread"]}}
                    for draft, mid in mailbox.drafts.items()
                    if not hidden & set((m := mailbox.messages[mid])["labels"])
                ]
                return self.reply(200, {"drafts": drafts, "resultSizeEstimate": len(drafts)})
            if method == "GET" and len(parts) == 2:
                return self._draft(parts[1], raw=query.get("format") == "raw")
            if method == "POST" and parts == ["drafts"]:
                draft, _ = mailbox.create_draft(decode(message["raw"]), thread)
                return self._draft(draft)
            if method == "PUT" and len(parts) == 2:
                mailbox.update_draft(parts[1], decode(message["raw"]), thread)
                return self._draft(parts[1])  # 404 if there was none
            if method == "DELETE" and len(parts) == 2:
                if not mailbox.delete_draft(parts[1]):
                    return self._draft(parts[1])  # 404
                self.send_response(204)
                self.send_header("content-length", "0")
                return self.end_headers()
            if method == "POST" and parts == ["drafts", "send"]:
                sent = mailbox.send_draft(body.get("id", ""))
                if sent is None:
                    return self._draft(body.get("id", ""))  # 404
                mid, thread_id = sent
                return self.reply(200, {"id": mid, "threadId": thread_id, "labelIds": ["SENT"]})
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
    ``label``, ``create_label``, ``rename_label``, ``delete_label``,
    ``expire_history``, ``refuse``, ``delay_s``, ``requests``, ``withheld``,
    ``restore``, ``drafts``, and for searches ``search_results``,
    ``search_refusal``, ``searches``; for permanent deletion ``deletes``, the
    ids of each delete call applied, and ``full_scope = False``, a token
    without ``https://mail.google.com/``, which deletes answer with 403).
    It serves the labels API as Gmail does: a name another label holds, in
    any case, is a 409; a modify adding a label it lacks is a 400 for the
    whole request. It serves ``users.drafts`` as Gmail does: every save gets
    a new message id and a Message-ID of Gmail's own, ``X-`` headers are
    kept, a send consumes the draft and adds a new ``SENT`` message in its
    thread, and history records each change. ``create_draft``,
    ``update_draft``, ``delete_draft`` and ``send_draft`` act as another
    client would.
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
        raw: bytes | None = None,
    ) -> None:
        """A new message arrives with ``labels``, in ``thread`` if given; ``raw``
        is its RFC 822 content (generated from the rest when omitted)."""
        with self.mailbox.lock:
            self.mailbox.add(
                message_id,
                labels,
                thread=thread,
                date_ms=date_ms,
                sender=sender,
                to=to,
                cc=cc,
                raw=raw,
            )

    def script_search(self, query: str, message_ids: list[str]) -> None:
        """Searches for exactly ``query`` match ``message_ids``, in this order
        (threads: their ``t-<id>`` threads)."""
        with self.mailbox.lock:
            self.mailbox.search_results[query] = message_ids

    def create_draft(self, raw: bytes, *, thread: str | None = None) -> tuple[str, str]:
        """Another client saves a new draft: ``(draft id, message id)``."""
        with self.mailbox.lock:
            return self.mailbox.create_draft(raw, thread)

    def update_draft(self, draft_id: str, raw: bytes, *, thread: str | None = None) -> str:
        """Another client replaces a draft's message: the new message id.
        Without ``thread`` the draft leaves its thread, as in Gmail."""
        with self.mailbox.lock:
            message = self.mailbox.update_draft(draft_id, raw, thread)
        if message is None:
            raise KeyError(draft_id)
        return message

    def delete_draft(self, draft_id: str) -> None:
        """Another client deletes a draft."""
        with self.mailbox.lock:
            if not self.mailbox.delete_draft(draft_id):
                raise KeyError(draft_id)

    def send_draft(self, draft_id: str) -> tuple[str, str]:
        """Another client sends a draft: ``(message id, thread id)`` of the
        new ``SENT`` message."""
        with self.mailbox.lock:
            sent = self.mailbox.send_draft(draft_id)
        if sent is None:
            raise KeyError(draft_id)
        return sent


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
