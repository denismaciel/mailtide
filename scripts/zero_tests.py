"""Track which of Zero's sync tests mailtide implements, by name.

Zero (Mail-0/Zero, MIT) has a test suite for its Gmail mailbox sync. This
script carries those test names into mailtide's Rust tests and reports how many
we have implemented.

    uv run --no-project scripts/zero_tests.py extract --zero ~/.cache/mailtide/zero
        Parse Zero's TypeScript suites into tests/zero/manifest.json.
    uv run --no-project scripts/zero_tests.py port
        Add a Rust stub for every manifest test that has no Rust test yet.
        Never touches existing tests.
    uv run --no-project scripts/zero_tests.py report [--min-implemented N]
        Parse both suites and count implemented / todo / not applicable /
        missing per suite. With --min-implemented, exit 1 below N.

Matching is by Rust function name, derived from Zero's describe path plus test
title. A Rust test counts as implemented when it exists without `#[ignore]`.
Stubs carry `#[ignore = "zero-todo: …"]`. A test that doesn't apply to
mailtide carries `#[ignore = "zero-na: <reason>"]`.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "tests" / "zero" / "manifest.json"
RUST_TESTS = ROOT / "crates"

# Zero suite -> Rust test file under crates/, in the crate that owns the behaviour.
# Only suites in mailtide's scope (SPEC §1).
LIB = "apps/server/src/lib"
SUITES: dict[str, str] = {
    f"{LIB}/mail-history-store.test.ts": "mailtide/tests/zero_mail_history_store.rs",
    f"{LIB}/mail-label-store.test.ts": "mailtide/tests/zero_mail_label_store.rs",
    f"{LIB}/mail-label-policy.test.ts": "mailtide/tests/zero_mail_label_policy.rs",
    f"{LIB}/mail-schedule.test.ts": "mailtide/tests/zero_mail_schedule.rs",
    f"{LIB}/mail-cleanup-store.test.ts": "mailtide/tests/zero_mail_cleanup_store.rs",
    f"{LIB}/mail-asset-cache.test.ts": "mailtide-io/tests/zero_mail_asset_cache.rs",
    f"{LIB}/gmail-push.test.ts": "mailtide-gmail/tests/zero_gmail_push.rs",
    f"{LIB}/gmail-push-auth.test.ts": "mailtide-gmail/tests/zero_gmail_push_auth.rs",
    f"{LIB}/gmail-quota-auth.test.ts": "mailtide-io/tests/zero_gmail_quota_auth.rs",
    f"{LIB}/gmail-quota-client.test.ts": "mailtide-io/tests/zero_gmail_quota_client.rs",
    f"{LIB}/gmail-quota-store.test.ts": "mailtide/tests/zero_gmail_quota_store.rs",
}

# Describe paths outside mailtide's scope: stubbed as zero-na, not todo.
NOT_APPLICABLE: dict[str, str] = {
    "AI runs": "Zero's AI thread workflow; not part of a sync engine",
}

CALL = re.compile(r"\b(describe|it|test)((?:\.[A-Za-z]+)*)\s*\(")
EACH_TABLE = re.compile(r"\.each\s*\(")


@dataclass(frozen=True)
class ZeroTest:
    suite: str
    path: tuple[str, ...]
    title: str

    @property
    def fn_name(self) -> str:
        words = re.sub(r"[^0-9a-zA-Z]+", " ", " ".join([*self.path, self.title])).split()
        name = "_".join(w.lower() for w in words) or "unnamed"
        return f"t_{name}" if name[0].isdigit() else name

    @property
    def label(self) -> str:
        return " > ".join([Path(self.suite).name.removesuffix(".test.ts"), *self.path, self.title])


# ---------------------------------------------------------------- TS parsing


def _skip_string(src: str, i: int) -> int:
    """Index just past the string literal starting at i (quote included)."""
    quote = src[i]
    i += 1
    depth = 0
    while i < len(src):
        c = src[i]
        if c == "\\":
            i += 2
            continue
        if quote == "`" and src.startswith("${", i):
            depth += 1
            i += 2
            continue
        if quote == "`" and depth and c == "}":
            depth -= 1
        elif c == quote and not depth:
            return i + 1
        i += 1
    return i


def _read_string(src: str, i: int) -> tuple[str, int] | None:
    """The string literal at i (after whitespace), unescaped; None if absent."""
    while i < len(src) and src[i].isspace():
        i += 1
    if i >= len(src) or src[i] not in "'\"`":
        return None
    end = _skip_string(src, i)
    raw = src[i + 1 : end - 1]
    return re.sub(r"\\(.)", r"\1", raw), end


def parse_suite(suite: str, src: str) -> list[ZeroTest]:
    """Tests in one TypeScript file, with their describe paths.

    A small lexer: skips strings and comments, tracks brace depth, and pops a
    describe when the depth falls back below where its body opened.
    """
    tests: list[ZeroTest] = []
    stack: list[tuple[str, int]] = []
    depth = 0
    i = 0
    while i < len(src):
        c = src[i]
        if src.startswith("//", i):
            i = src.find("\n", i)
            i = len(src) if i < 0 else i
            continue
        if src.startswith("/*", i):
            end = src.find("*/", i + 2)
            i = len(src) if end < 0 else end + 2
            continue
        if c in "'\"`":
            i = _skip_string(src, i)
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            while stack and depth < stack[-1][1]:
                stack.pop()
        elif c.isalpha() and (i == 0 or not (src[i - 1].isalnum() or src[i - 1] in "_.$")):
            m = CALL.match(src, i)
            if m:
                kind, modifiers = m.group(1), m.group(2)
                j = m.end()
                if ".each" in modifiers:
                    # it.each(table)('title', fn): skip the table call.
                    j = _skip_call(src, m.end() - 1)
                    while j < len(src) and src[j].isspace():
                        j += 1
                    if j < len(src) and src[j] == "(":
                        j += 1
                got = _read_string(src, j)
                if got:
                    title, end = got
                    if kind == "describe":
                        stack.append((title, depth + 1))
                    else:
                        tests.append(ZeroTest(suite, tuple(n for n, _ in stack), title))
                    i = end
                    continue
                i = m.end()
                continue
        i += 1
    return tests


def _skip_call(src: str, open_paren: int) -> int:
    """Index just past the parenthesized group opening at open_paren."""
    depth = 0
    i = open_paren
    while i < len(src):
        c = src[i]
        if c in "'\"`":
            i = _skip_string(src, i)
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return i


# ---------------------------------------------------------------- Rust parsing

RUST_TEST = re.compile(
    r"#\[(?:tokio::)?test(?:\([^)]*\))?\]\s*(?P<attrs>(?:#\[[^\]]*\]\s*)*)(?:pub\s+)?(?:async\s+)?fn\s+(?P<name>\w+)",
)
IGNORE = re.compile(r'#\[ignore(?:\s*=\s*"(?P<reason>[^"]*)")?\]')


def rust_tests(path: Path) -> dict[str, str]:
    """Function name -> status (implemented | todo | na) in one Rust test file."""
    if not path.exists():
        return {}
    found: dict[str, str] = {}
    for m in RUST_TEST.finditer(path.read_text()):
        ignore = IGNORE.search(m.group("attrs"))
        if ignore is None:
            found[m.group("name")] = "implemented"
        elif (ignore.group("reason") or "").startswith("zero-na"):
            found[m.group("name")] = "na"
        else:
            found[m.group("name")] = "todo"
    return found


# ---------------------------------------------------------------- commands


def load_manifest() -> tuple[str, list[ZeroTest]]:
    data = json.loads(MANIFEST.read_text())
    tests = [ZeroTest(t["suite"], tuple(t["path"]), t["title"]) for t in data["tests"]]
    return data["commit"], tests


def cmd_extract(zero: Path) -> None:
    commit = subprocess.run(
        ["git", "-C", str(zero), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    tests: list[ZeroTest] = []
    for suite in SUITES:
        tests += parse_suite(suite, (zero / suite).read_text())
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(
        json.dumps(
            {
                "source": "https://github.com/Mail-0/Zero",
                "license": "MIT",
                "commit": commit,
                "tests": [
                    {"suite": t.suite, "path": list(t.path), "title": t.title} for t in tests
                ],
            },
            indent=2,
        )
        + "\n"
    )
    print(
        f"{len(tests)} tests from {len(SUITES)} suites at Zero {commit[:12]}"
        f" -> {MANIFEST.relative_to(ROOT)}"
    )


def _rust_string(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def cmd_port() -> None:
    commit, tests = load_manifest()
    added = 0
    for suite, rust_file in SUITES.items():
        path = RUST_TESTS / rust_file
        existing = rust_tests(path)
        stubs: list[str] = []
        seen = set(existing)
        for t in (t for t in tests if t.suite == suite):
            if t.fn_name in seen:
                continue
            seen.add(t.fn_name)
            na = next((r for d, r in NOT_APPLICABLE.items() if d in t.path), None)
            reason = f"zero-na: {na}" if na else f"zero-todo: {t.label}"
            stubs.append(
                f"/// Zero: {t.label}\n"
                f"#[test]\n"
                f'#[ignore = "{_rust_string(reason)}"]\n'
                f"fn {t.fn_name}() {{\n"
                f'    todo!("port from Zero {commit[:12]}")\n'
                f"}}\n"
            )
        if not stubs:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        header = (
            ""
            if path.exists()
            else (
                f"//! Tests ported by name from Zero's `{Path(suite).name}`\n"
                f"//! (Mail-0/Zero, MIT, commit {commit[:12]}).\n"
                "//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.\n"
            )
        )
        with path.open("a") as f:
            f.write(header + "".join("\n" + s for s in stubs))
        added += len(stubs)
    print(f"added {added} stubs")


def cmd_report(min_implemented: int | None) -> int:
    commit, tests = load_manifest()
    totals = {"implemented": 0, "todo": 0, "na": 0, "missing": 0}
    rows: list[tuple[str, dict[str, int]]] = []
    for suite, rust_file in SUITES.items():
        found = rust_tests(RUST_TESTS / rust_file)
        counts = {"implemented": 0, "todo": 0, "na": 0, "missing": 0}
        for name in dict.fromkeys(t.fn_name for t in tests if t.suite == suite):
            counts[found.get(name, "missing")] += 1
        rows.append((Path(suite).name.removesuffix(".test.ts"), counts))
        for k, v in counts.items():
            totals[k] += v
    applicable = totals["implemented"] + totals["todo"] + totals["missing"]
    width = max(len(n) for n, _ in rows)
    print(f"Zero {commit[:12]}: tests ported by name into crates/*/tests")
    print(f"{'suite':<{width}}  impl  todo    na  missing")
    for name, c in rows:
        print(
            f"{name:<{width}}  {c['implemented']:>4}  {c['todo']:>4}  {c['na']:>4}"
            f"  {c['missing']:>7}"
        )
    pct = 100 * totals["implemented"] / applicable if applicable else 100.0
    print(
        f"{'total':<{width}}  {totals['implemented']:>4}  {totals['todo']:>4}  "
        f"{totals['na']:>4}  {totals['missing']:>7}"
    )
    print(f"implemented {totals['implemented']}/{applicable} applicable ({pct:.0f}%)")
    if min_implemented is not None and totals["implemented"] < min_implemented:
        print(f"FAIL: expected at least {min_implemented} implemented", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    extract = sub.add_parser("extract")
    extract.add_argument("--zero", type=Path, required=True, help="a Zero checkout")
    sub.add_parser("port")
    report = sub.add_parser("report")
    report.add_argument("--min-implemented", type=int)
    args = parser.parse_args()
    match args.cmd:
        case "extract":
            cmd_extract(args.zero.expanduser())
        case "port":
            cmd_port()
        case "report":
            return cmd_report(args.min_implemented)
    return 0


if __name__ == "__main__":
    sys.exit(main())
