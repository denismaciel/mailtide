"""Show and check mailtide's feature plans against their reference implementations.

A feature plan is `features/<id>.toml` plus a design note under `docs/features/`.
It names the mailtide files the feature adds or changes, where each reference
implementation (features/references.toml) has the behaviour, and which reference
tests mailtide must account for, each mapped by name to a mailtide test or marked
not applicable. The format is described in docs/features/README.md.

    uv run --no-project scripts/features.py refs [--fetch]
        The pinned reference checkouts; --fetch clones the missing ones.
    uv run --no-project scripts/features.py suite-tests IMPL PATH [--target FILE]
        The tests in one reference test file, as a `[[suite]]` block to paste
        into a plan and edit.
    uv run --no-project scripts/features.py check
        Validate every plan; exit 1 on any error.
    uv run --no-project scripts/features.py report [FEATURE ...]
        One line per feature; with names, the files, references and every test.
    uv run --no-project scripts/features.py order
        The features in waves: each wave needs only the waves before it.
    uv run --no-project scripts/features.py unknowns [FEATURE ...]
        Every Gmail behaviour a plan assumes without proof, with the test that
        settles it against the real API.

A reference test's status comes from the mailtide test it maps to: `implemented`
(the test exists), `todo` (it exists with `ref-todo` in an ignore/skip reason),
`planned` (not written yet) or `na` (the plan says why it does not apply).
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FEATURES = ROOT / "features"
REFS_DIR = Path(os.environ.get("MAILTIDE_REFS", Path.home() / ".cache" / "mailtide" / "refs"))
STATUSES = ("planned", "approved", "in_progress", "done")
UNKNOWN_STATES = ("open", "confirmed", "refuted")

RUST_REF_TEST = re.compile(
    r"#\[(?:[A-Za-z_]\w*::)*(?:test|rstest|test_case)\b[^\]]*\]"
    r"(?:\s*#\[[^\]]*\])*\s*(?:pub(?:\([a-z]+\))?\s+)?(?:async\s+)?fn\s+(?P<name>[A-Za-z_]\w*)"
)
RUST_OWN_TEST = re.compile(
    r"(?P<attrs>(?:#\[[^\]]*\]\s*)+)(?:pub\s+)?(?:async\s+)?fn\s+(?P<name>[A-Za-z_]\w*)"
)
RUST_FN = re.compile(r"\bfn\s+(?P<name>[A-Za-z_]\w*)")
PY_OWN_TEST = re.compile(
    r"(?P<attrs>(?:^@[^\n]*\n)*)^(?:async\s+)?def\s+(?P<name>test_\w*)", re.MULTILINE
)


@dataclass(frozen=True)
class Reference:
    key: str
    repo: str
    commit: str
    license: str
    copy: str
    checkout: Path
    lang: str


def load_references() -> dict[str, Reference]:
    raw = tomllib.loads((FEATURES / "references.toml").read_text())
    return {
        key: Reference(
            key,
            r["repo"],
            r["commit"],
            r["license"],
            r["copy"],
            REFS_DIR / r["checkout"],
            r["lang"],
        )
        for key, r in raw.items()
    }


def load_plans() -> dict[str, dict]:
    plans = {}
    for path in sorted(FEATURES.glob("*.toml")):
        if path.name != "references.toml":
            plans[path.stem] = tomllib.loads(path.read_text())
    return plans


def checkout_commit(ref: Reference) -> str | None:
    if not (ref.checkout / ".git").exists():
        return None
    done = subprocess.run(
        ["git", "-C", str(ref.checkout), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return done.stdout.strip() or None


def snake(name: str) -> str:
    """A reference test name as a mailtide test function name."""
    words = re.sub(r"[^0-9a-zA-Z]+", " ", name).split()
    out = "_".join(w.lower() for w in words) or "unnamed"
    return f"t_{out}" if out[0].isdigit() else out


def typescript_tests(path: str, src: str) -> list:
    """Parse a TypeScript suite with the parser scripts/zero_tests.py already has."""
    spec = importlib.util.spec_from_file_location(
        "zero_tests", Path(__file__).with_name("zero_tests.py")
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("zero_tests", module)
    spec.loader.exec_module(module)
    return module.parse_suite(path, src)


def reference_tests(ref: Reference, path: str) -> list[str] | None:
    """Test names in one reference test file; None when the file is missing."""
    file = ref.checkout / path
    if not file.is_file():
        return None
    src = file.read_text(errors="replace")
    if ref.lang == "typescript":
        names = [" > ".join([*t.path, t.title]) for t in typescript_tests(path, src)]
    else:
        names = [m.group("name") for m in RUST_REF_TEST.finditer(src)]
    seen: dict[str, int] = {}
    unique = []
    for name in names:
        seen[name] = seen.get(name, 0) + 1
        unique.append(name if seen[name] == 1 else f"{name} #{seen[name]}")
    return unique


def own_tests(target: str) -> dict[str, str]:
    """mailtide test name -> implemented | todo, in one of our test files.

    A plain function counts as implemented too: store conformance cases are
    functions listed in a table, not attributed tests.
    """
    file = ROOT / target
    if not file.is_file():
        return {}
    src = file.read_text()
    if target.endswith(".py"):
        return {
            m.group("name"): "todo" if "ref-todo" in m.group("attrs") else "implemented"
            for m in PY_OWN_TEST.finditer(src)
        }
    found = {m.group("name"): "implemented" for m in RUST_FN.finditer(src)}
    for m in RUST_OWN_TEST.finditer(src):
        if "ref-todo" in m.group("attrs"):
            found[m.group("name")] = "todo"
    return found


@dataclass(frozen=True)
class Mapped:
    feature: str
    impl: str
    suite: str
    name: str
    target: str | None
    test: str | None
    na: str | None
    note: str | None = None

    @property
    def status(self) -> str:
        if self.na is not None:
            return "na"
        assert self.target is not None and self.test is not None
        return own_tests(self.target).get(self.test, "planned")


def mapped_tests(feature: str, plan: dict) -> list[Mapped]:
    out = []
    for suite in plan.get("suite", []):
        for test in suite.get("test", []):
            na = test.get("na")
            out.append(
                Mapped(
                    feature,
                    suite["impl"],
                    suite["path"],
                    test["name"],
                    None if na else test.get("target", suite.get("target")),
                    None if na else test.get("test", snake(test["name"])),
                    na,
                    test.get("note"),
                )
            )
    return out


@dataclass
class Suite:
    """One reference test file, as every plan together accounts for it."""

    impl: str
    path: str
    manual: bool
    tests: list[str]
    declared_by: list[str] = field(default_factory=list)
    claimed: dict[str, str] = field(default_factory=dict)
    deferred: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    rest_na: dict[str, str] = field(default_factory=dict)

    @property
    def rest(self) -> list[str]:
        """Tests no plan lists; `na` when a plan gave the suite a `rest_na` reason."""
        return [t for t in self.tests if t not in self.claimed and t not in self.deferred]

    @property
    def waiting(self) -> list[str]:
        """Deferred to another plan that has not claimed them yet."""
        return [t for t in self.deferred if t not in self.claimed]


@dataclass
class Problems:
    errors: list[tuple[str, set[str]]] = field(default_factory=list)
    warnings: list[tuple[str, set[str]]] = field(default_factory=list)

    def about(self, feature: str) -> tuple[list[str], list[str]]:
        return (
            [m for m, who in self.errors if feature in who],
            [m for m, who in self.warnings if feature in who],
        )


def has_symbol(text: str, symbol: str) -> bool:
    return re.search(rf"(?<!\w){re.escape(symbol)}(?!\w)", text) is not None


def survey(plans: dict[str, dict], refs: dict[str, Reference]) -> tuple[Problems, list[Suite]]:
    """Every problem in the plans, and how each declared suite is accounted for."""
    problems = Problems()

    def err(message: str, *features: str) -> None:
        problems.errors.append((message, set(features)))

    def warn(message: str, *features: str) -> None:
        problems.warnings.append((message, set(features)))

    for key, ref in refs.items():
        at = checkout_commit(ref)
        if at is None:
            warn(f"references: {key} is not checked out at {ref.checkout} (run `refs --fetch`)")
        elif at != ref.commit:
            warn(f"references: {key} is at {at[:9]}, pinned {ref.commit[:9]}")

    suites: dict[tuple[str, str], Suite] = {}
    own_names: dict[tuple[str, str], str] = {}
    unknown_ids: dict[str, str] = {}
    for fid, plan in plans.items():
        where = f"{fid}:"
        if plan.get("id") != fid:
            err(f"{where} id must equal the file name", fid)
        if plan.get("status") not in STATUSES:
            err(f"{where} status must be one of {', '.join(STATUSES)}", fid)
        for key in ("title", "summary", "design"):
            if not plan.get(key):
                err(f"{where} missing `{key}`", fid)
        if plan.get("design") and not (ROOT / plan["design"]).is_file():
            err(f"{where} design note {plan['design']} does not exist", fid)
        for dep in plan.get("depends_on", []):
            if dep not in plans:
                warn(f"{where} depends on `{dep}`, which has no plan yet", fid)
        for f in plan.get("file", []):
            if f.get("change") not in ("new", "modify"):
                err(f"{where} file {f.get('path')}: change must be new or modify", fid)
            elif f["change"] == "modify" and not (ROOT / f["path"]).exists():
                err(f"{where} file {f['path']} is marked modify but does not exist", fid)
        for source in plan.get("source", []):
            if not source.get("url") or not source.get("note"):
                err(f"{where} a source needs `url` and `note`", fid)
        for r in plan.get("reference", []):
            ref = refs.get(r.get("impl", ""))
            if ref is None:
                err(f"{where} reference names unknown implementation `{r.get('impl')}`", fid)
                continue
            file = ref.checkout / r["path"]
            if not file.exists():
                err(f"{where} reference {ref.key}:{r['path']} does not exist", fid)
            elif file.is_dir() and r.get("symbols"):
                err(f"{where} reference {ref.key}:{r['path']} is a directory; cite the file", fid)
            elif file.is_file():
                text = file.read_text(errors="replace")
                for symbol in r.get("symbols", []):
                    if not has_symbol(text, symbol):
                        err(f"{where} reference {ref.key}:{r['path']} has no `{symbol}`", fid)
        for declared in plan.get("suite", []):
            ref = refs.get(declared.get("impl", ""))
            if ref is None:
                err(f"{where} suite names unknown implementation `{declared.get('impl')}`", fid)
                continue
            sid = (ref.key, declared["path"])
            manual = bool(declared.get("manual", False))
            found = reference_tests(ref, declared["path"])
            if found is None:
                err(f"{where} suite {sid[0]}:{sid[1]} does not exist", fid)
                continue
            suite = suites.setdefault(sid, Suite(ref.key, declared["path"], manual, found))
            if suite.manual != manual:
                err(
                    f"{where} suite {sid[0]}:{sid[1]} is manual in one plan and not in another", fid
                )
            suite.declared_by.append(fid)
            if declared.get("rest_na"):
                suite.rest_na[fid] = declared["rest_na"]
            text = (ref.checkout / declared["path"]).read_text(errors="replace")

            def known(name: str, manual: bool = manual, found=found, text: str = text) -> bool:
                return name in text if manual else name in found

            for test in declared.get("test", []):
                name = test["name"]
                if not known(name):
                    err(f"{where} suite {sid[1]} has no test `{name}`", fid)
                if name in suite.claimed:
                    other = suite.claimed[name]
                    err(f"{where} `{name}` is already claimed by {other}", fid, other)
                suite.claimed[name] = fid
                if "na" in test and ("test" in test or "target" in test):
                    err(f"{where} `{name}` has both `na` and a mapping", fid)
                if "na" not in test and not test.get("target", declared.get("target")):
                    err(f"{where} `{name}` has no target file (set suite.target)", fid)
            for defer in declared.get("defer", []):
                name, to = defer.get("name", ""), defer.get("to", "")
                if not known(name):
                    err(f"{where} suite {sid[1]} has no test `{name}` to defer", fid)
                if not to or to == fid:
                    err(f"{where} `{name}` must be deferred to another feature", fid)
                suite.deferred.setdefault(name, []).append((fid, to))
        for mapped in mapped_tests(fid, plan):
            if mapped.na is None:
                slot = (mapped.target or "", mapped.test or "")
                if slot in own_names and own_names[slot] != f"{mapped.suite}::{mapped.name}":
                    err(f"{where} {slot[0]}::{slot[1]} maps two reference tests", fid)
                own_names[slot] = f"{mapped.suite}::{mapped.name}"
        for test in plan.get("test", []):
            for key in ("target", "name", "covers"):
                if not test.get(key):
                    err(f"{where} own test is missing `{key}`", fid)
        for unknown in plan.get("unknown", []):
            uid = unknown.get("id", "")
            for key in ("id", "question", "assumed", "if_wrong", "test"):
                if not unknown.get(key):
                    err(f"{where} unknown `{uid}` is missing `{key}`", fid)
            if unknown.get("status", "open") not in UNKNOWN_STATES:
                err(f"{where} unknown `{uid}`: status must be one of {UNKNOWN_STATES}", fid)
            if unknown.get("status", "open") != "open" and not unknown.get("result"):
                err(f"{where} unknown `{uid}` is settled but has no `result`", fid)
            if uid in unknown_ids:
                err(f"{where} unknown id `{uid}` is also used by {unknown_ids[uid]}", fid)
            unknown_ids[uid] = fid

    for suite in suites.values():
        label = f"{suite.impl}:{suite.path}"
        for name in suite.waiting:
            for by, to in suite.deferred[name]:
                state = "has not claimed it" if to in plans else "has no plan yet"
                warn(f"deferred: {label} :: {name} ({by} left it to {to}, which {state})", by, to)
        if not suite.manual and not suite.rest_na:
            for name in suite.rest:
                err(
                    f"unassigned: {label} :: {name} (map it, mark it na, defer it, "
                    f"or give the suite a rest_na reason)",
                    *suite.declared_by,
                )
    return problems, list(suites.values())


def counts(tests: list[Mapped]) -> dict[str, int]:
    out = {"implemented": 0, "todo": 0, "planned": 0, "na": 0}
    for t in tests:
        out[t.status] += 1
    return out


def own_status(test: dict) -> str:
    return own_tests(test["target"]).get(test["name"], "planned")


def cmd_refs(fetch: bool) -> int:
    for key, ref in load_references().items():
        at = checkout_commit(ref)
        if at is None and fetch:
            ref.checkout.mkdir(parents=True, exist_ok=True)
            git = ["git", "-C", str(ref.checkout)]
            subprocess.run([*git, "init", "-q"], check=True)
            subprocess.run([*git, "fetch", "-q", "--depth", "1", ref.repo, ref.commit], check=True)
            subprocess.run([*git, "checkout", "-q", "FETCH_HEAD"], check=True)
            at = checkout_commit(ref)
        state = "missing" if at is None else "pinned" if at == ref.commit else f"at {at[:9]}"
        print(f"{key:14} {ref.commit[:9]}  {state:12} copy={ref.copy:5}  {ref.license}")
        print(f"{'':14} {ref.repo}")
    return 0


def cmd_suite_tests(impl: str, path: str, target: str | None) -> int:
    ref = load_references()[impl]
    found = reference_tests(ref, path)
    if found is None:
        print(f"{impl}:{path} does not exist under {ref.checkout}", file=sys.stderr)
        return 1
    print(f'[[suite]]\nimpl = "{impl}"\npath = "{path}"')
    print(f'target = "{target or "TODO: mailtide test file"}"')
    for name in found:
        escaped = name.replace("\\", "\\\\").replace('"', '\\"')
        print(f'\n[[suite.test]]\nname = "{escaped}"\n# test = "{snake(name)}"')
    print(f"\n# {len(found)} tests", file=sys.stderr)
    return 0


def open_unknowns(plan: dict) -> int:
    return sum(1 for u in plan.get("unknown", []) if u.get("status", "open") == "open")


def cmd_unknowns(names: list[str]) -> int:
    plans = load_plans()
    total = 0
    for fid, plan in plans.items():
        if names and fid not in names:
            continue
        for unknown in plan.get("unknown", []):
            total += 1
            print(f"[{unknown.get('status', 'open')}] {unknown['id']}  ({fid})")
            print(f"  question: {unknown['question']}")
            print(f"  assumed:  {unknown['assumed']}")
            print(f"  if wrong: {unknown['if_wrong']}")
            print(f"  test:     {unknown['test']}")
            if unknown.get("result"):
                print(f"  result:   {unknown['result']}")
            print()
    still = sum(open_unknowns(p) for f, p in plans.items() if not names or f in names)
    print(f"{total} unknowns, {still} open")
    return 0


def cmd_order() -> int:
    plans = load_plans()
    placed: set[str] = set()
    wave = 0
    while len(placed) < len(plans):
        ready = [
            fid
            for fid, plan in plans.items()
            if fid not in placed
            and all(dep in placed or dep not in plans for dep in plan.get("depends_on", []))
        ]
        if not ready:
            stuck = ", ".join(sorted(set(plans) - placed))
            print(f"dependency cycle among: {stuck}", file=sys.stderr)
            return 1
        wave += 1
        print(f"wave {wave}")
        for fid in ready:
            plan = plans[fid]
            needs = ", ".join(plan.get("depends_on", [])) or "-"
            print(
                f"  {fid:20} {' '.join(plan.get('roadmap', [])):9} needs: {needs:32} "
                f"open unknowns: {open_unknowns(plan)}"
            )
        placed.update(ready)
    return 0


def cmd_check() -> int:
    problems, _ = survey(load_plans(), load_references())
    for line, _ in problems.warnings:
        print(f"warning: {line}")
    for line, _ in problems.errors:
        print(f"error: {line}")
    print(f"{len(problems.errors)} errors, {len(problems.warnings)} warnings")
    return 1 if problems.errors else 0


def cmd_report(names: list[str]) -> int:
    plans, refs = load_plans(), load_references()
    unknown = [n for n in names if n not in plans]
    if unknown:
        print(f"unknown feature(s): {', '.join(unknown)}", file=sys.stderr)
        return 1
    problems, suites = survey(plans, refs)
    print(
        f"{'feature':20} {'status':9} {'files':>5} {'refs':>4} {'ref tests':>9} {'impl':>4} "
        f"{'todo':>4} {'plan':>4} {'na':>3} {'rest':>4} {'defer':>5} {'own':>3} {'unk':>3} "
        f"{'err':>3}  roadmap"
    )
    for fid, plan in plans.items():
        if names and fid not in names:
            continue
        tests = mapped_tests(fid, plan)
        c = counts(tests)
        mine = [s for s in suites if fid in s.declared_by]
        rest = sum(len(s.rest) for s in mine if fid in s.rest_na)
        defer = sum(1 for s in mine for by in s.deferred.values() for who, _ in by if who == fid)
        errors, _ = problems.about(fid)
        print(
            f"{fid:20} {plan.get('status', '?'):9} {len(plan.get('file', [])):>5} "
            f"{len(plan.get('reference', [])):>4} {len(tests):>9} {c['implemented']:>4} "
            f"{c['todo']:>4} {c['planned']:>4} {c['na']:>3} {rest:>4} {defer:>5} "
            f"{len(plan.get('test', [])):>3} {open_unknowns(plan):>3} {len(errors):>3}  "
            f"{' '.join(plan.get('roadmap', []))}"
        )
    for fid in names:
        plan = plans[fid]
        print(f"\n== {fid}: {plan.get('title', '')}")
        print(f"   {plan.get('summary', '')}")
        depends = ", ".join(plan.get("depends_on", [])) or "-"
        print(f"   design: {plan.get('design', '-')}   depends on: {depends}")
        print("\n   mailtide files")
        for f in plan.get("file", []):
            state = "exists" if (ROOT / f["path"]).exists() else "planned"
            print(f"     {f['change']:6} {state:7} {f['path']}  — {f.get('role', '')}")
        print("\n   where the references have it")
        for r in plan.get("reference", []):
            ref = refs.get(r["impl"])
            ok = ref is not None and (ref.checkout / r["path"]).exists()
            symbols = ", ".join(r.get("symbols", []))
            print(f"     {'ok' if ok else 'MISSING':7} {r['impl']}:{r['path']}  {symbols}")
            if r.get("note"):
                print(f"             {r['note']}")
        for source in plan.get("source", []):
            print(f"     source  {source.get('url')}\n             {source.get('note')}")
        print("\n   reference tests -> mailtide tests")
        mapped = mapped_tests(fid, plan)
        for suite in (s for s in suites if fid in s.declared_by):
            here = [t for t in mapped if (t.impl, t.suite) == (suite.impl, suite.path)]
            c = counts(here)
            kind = " (manual list)" if suite.manual else f" ({len(suite.tests)} tests in the file)"
            print(f"\n     {suite.impl}:{suite.path}{kind}")
            print(
                f"       this plan: {len(here) - c['na']} mapped, {c['na']} na; "
                f"other plans: {sum(1 for o in suite.claimed.values() if o != fid)}; "
                f"deferred and waiting: {len(suite.waiting)}; "
                f"rest: {len(suite.rest)}"
                + (" (na)" if suite.rest_na else " (UNASSIGNED)" if suite.rest else "")
            )
            for t in here:
                to = f"na: {t.na}" if t.na else f"{t.target}::{t.test}"
                print(f"       {t.status:11} {t.name}\n{'':19}-> {to}")
                if t.note:
                    print(f"{'':19}   note: {t.note}")
            for name, by in suite.deferred.items():
                for who, to in by:
                    if who == fid:
                        owner = suite.claimed.get(name)
                        state = f"claimed by {owner}" if owner else "not claimed yet"
                        print(f"       {'deferred':11} {name}\n{'':19}-> left to {to} ({state})")
            if fid in suite.rest_na:
                print(f"       rest na: {suite.rest_na[fid]}")
                for name in suite.rest:
                    print(f"       {'na (rest)':11} {name}")
        if plan.get("test"):
            print("\n   mailtide's own tests")
            for test in plan["test"]:
                where = f"{test['target']}::{test['name']}"
                print(f"     {own_status(test):11} {where}  — {test['covers']}")
        if plan.get("unknown"):
            print("\n   Gmail behaviour assumed, not proven (`unknowns` shows the tests)")
            for unknown in plan["unknown"]:
                print(
                    f"     {unknown.get('status', 'open'):9} {unknown['id']}: {unknown['question']}"
                )
        errors, warnings = problems.about(fid)
        if errors or warnings:
            print("\n   problems")
            for line in warnings:
                print(f"     warning: {line}")
            for line in errors:
                print(f"     error: {line}")
    if problems.errors:
        print(f"\n{len(problems.errors)} plan errors in all: run `scripts/features.py check`")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    refs = sub.add_parser("refs")
    refs.add_argument("--fetch", action="store_true")
    suite = sub.add_parser("suite-tests")
    suite.add_argument("impl")
    suite.add_argument("path")
    suite.add_argument("--target")
    sub.add_parser("check")
    report = sub.add_parser("report")
    report.add_argument("features", nargs="*")
    sub.add_parser("order")
    unknowns = sub.add_parser("unknowns")
    unknowns.add_argument("features", nargs="*")
    args = parser.parse_args()
    if args.cmd == "refs":
        return cmd_refs(args.fetch)
    if args.cmd == "suite-tests":
        return cmd_suite_tests(args.impl, args.path, args.target)
    if args.cmd == "check":
        return cmd_check()
    if args.cmd == "order":
        return cmd_order()
    if args.cmd == "unknowns":
        return cmd_unknowns(args.features)
    return cmd_report(args.features)


if __name__ == "__main__":
    sys.exit(main())
