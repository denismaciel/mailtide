"""Tests for the Rust-backed multi-account FakeGmailServer Python API."""

from __future__ import annotations

import asyncio
import json
from urllib.request import Request, urlopen

import mailtide
from mailtide.config import Blobs, Config, Database, Http, Poll
from mailtide.fake import FakeGmailServer, fake_token_source


def _post(url: str, payload: dict[str, object]) -> dict[str, object]:
    req = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urlopen(req, timeout=10) as resp:  # noqa: S310
        return json.loads(resp.read().decode("utf-8"))


def test_fake_gmail_server_roundtrip_and_ui() -> None:
    async def run() -> None:
        with FakeGmailServer() as server:
            # Web inspector UI serves HTML cleanly.
            with urlopen(f"{server.origin}/_ui", timeout=5) as resp:  # noqa: S310
                html = resp.read().decode("utf-8")
            assert "mailtide-fake" in html

            # Seed alice@example.com and sync with real mailtide.Runner.
            seeded = _post(
                f"{server.origin}/_control/accounts/alice@example.com/seed",
                {"threads": 6, "max_messages_per_thread": 2, "draft_count": 1},
            )
            assert seeded["threads_created"] == 6

            runner = mailtide.Runner(
                Config(
                    database=Database("sqlite::memory:"),
                    blobs=Blobs("memory:///"),
                    watch=Poll(),
                    http=Http(gmail_origin=server.origin),
                ),
                fake_token_source(),
            )
            await runner.migrate()
            await runner.track("alice@example.com")
            outcome = await runner.run("alice@example.com", budget_s=15)
            assert outcome.imported >= 6
            assert not outcome.blocked

            drafts = await runner.drafts("alice@example.com")
            assert len(drafts) == 1

    asyncio.run(run())
