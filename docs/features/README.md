# Feature plans

A feature is planned before it is built. A plan is two files:

- `features/<id>.toml`: the machine-readable part. It lists the mailtide files the
  feature adds or changes, where each reference implementation has the behaviour,
  and which reference tests mailtide accounts for, each mapped by name to a
  mailtide test.
- `docs/features/<id>.md`: the design note a maintainer reads before approving.

`scripts/features.py` reads the plans and the pinned reference checkouts:

```sh
uv run --no-project scripts/features.py refs --fetch     # clone the references
uv run --no-project scripts/features.py suite-tests zero apps/server/src/lib/driver/draft-providers.test.ts
uv run --no-project scripts/features.py check            # validate every plan
uv run --no-project scripts/features.py report           # one line per feature
uv run --no-project scripts/features.py report drafts    # files, references and every test
uv run --no-project scripts/features.py order            # features in dependency waves
uv run --no-project scripts/features.py unknowns         # Gmail behaviour to test on the real API
```

The sync engine's own Zero suites stay under `scripts/zero_tests.py`; do not
declare those suites here.

## References and licences

`features/references.toml` pins each reference to a commit and says what may be
carried over:

- `copy = "code"` (permissive): behaviour, code and tests may be ported, with the
  licence notice.
- `copy = "names"` (copyleft, e.g. the Stalwart server): record file paths and test
  names only. Read it to learn what cases exist; never copy code or test bodies,
  and write mailtide's test from the behaviour, not from the source.

## The plan file

```toml
id = "drafts"                       # equals the file name
title = "Drafts: create, update, delete and send"
summary = "One sentence a maintainer can judge the scope by."
roadmap = ["C2"]                    # ids from the roadmap, if any
status = "planned"                  # planned | approved | in_progress | done
design = "docs/features/drafts.md"
depends_on = ["compose-mime"]       # other feature ids (a warning until that plan exists)

[[file]]                            # every file the feature touches: source, tests, docs, stubs
path = "crates/mailtide-gmail/src/drafts.rs"
change = "new"                      # new | modify (modify: the file must exist)
role = "Gmail drafts requests and response decoding"

[[reference]]                       # where a reference has the behaviour
impl = "zero"                       # a key of references.toml
path = "apps/server/src/lib/driver/google.ts"    # a file, not a directory
symbols = ["createDraft", "sendDraft"]   # each must appear in that file as a whole word
note = "Pass-through calls; the client keeps Gmail's draft id."

[[source]]                          # anything that is not a pinned repository
url = "https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.drafts"
note = "Endpoints and quota costs, read 2026-11-20."

[[suite]]                           # a reference test file this feature accounts for
impl = "zero"
path = "apps/server/src/lib/driver/draft-providers.test.ts"
target = "crates/mailtide-io/tests/ref_zero_draft_providers.rs"   # where our tests go
# rest_na = "reason"                # every test no plan lists is not applicable, for this reason

[[suite.test]]
name = "provider draft safety > writes the selected Gmail From address into the real saved MIME"
# test = "..."                      # our test's name; default is derived from `name`
# target = "..."                    # another file than the suite's
# note = "..."                      # e.g. which half of the reference test applies

[[suite.test]]
name = "provider draft safety > does not treat a sent Outlook message as a draft"
na = "Outlook only; mailtide is Gmail only"

[[suite.defer]]                     # belongs to another feature's plan
name = "provider draft safety > offers only the primary Gmail sender and accepted sending aliases"
to = "identities"

[[test]]                            # our own tests, with no reference counterpart
target = "crates/mailtide-io/tests/drafts.rs"
name = "a_draft_update_keeps_its_draft_id"
covers = "Gmail replaces the message id on update; the draft id is stable"

[[unknown]]                         # Gmail behaviour the design assumes without proof
id = "drafts-send-twice"            # unique across all plans; prefix it with the feature id
question = "Does drafts.send on an already-sent draft always return 404?"
assumed = "Yes: the second call returns 404 and sends nothing."
if_wrong = "Send is no longer at most once; a retry needs a Message-ID lookup first."
test = "Create a draft, send it, send the same draft id again; expect 404 and one SENT message."
# status = "open"                   # open | confirmed | refuted (the last two need `result`)
# result = "..."
```

Rules the script enforces:

- **Every test of a declared suite is accounted for.** Each test the script
  extracts from a `[[suite]]` file must be mapped, marked `na` with a reason, or
  deferred to the feature that owns it. Several plans may declare the same suite
  and each claim their own tests; a test claimed twice is an error. Print the
  block to start from with `suite-tests`.
- **A deferred test is a warning until its owner claims it.** Defer instead of
  leaving a sibling's test unlisted, so "left for `send`" is told apart from
  "forgotten".
- **`rest_na` is for a large suite that is mostly out of scope.** It marks every
  test no plan lists as not applicable, with one reason. Use it only when the
  reason holds for all of them (another provider, a host workflow); the report
  prints each one so the claim can be checked.
- **Names match exactly.** `name` is the reference's own name: for TypeScript the
  describe path and title joined by ` > `; for Rust the test function's name.
- **Our test's name says what our test checks.** By default it is `name`
  lower-cased with every run of non-alphanumerics turned into `_`. Set `test`
  whenever that name carries the reference's vocabulary instead of ours. The plan
  keeps the link either way.
- **Target files name their source.** Tests ported from a reference go in
  `crates/<crate>/tests/ref_<impl>_<suite>.rs`, in the crate that owns the
  behaviour, where `<suite>` is the reference file's name without its extension or
  `.test`, prefixed by as many parent directories as make it unique
  (`ref_zero_driver_imap.rs`, `ref_himalaya_gmail_drafts_get.rs`). Our own tests go
  where the crate's other tests are.
- **A suite the script cannot parse** (tests driven by a harness, not by test
  attributes) sets `manual = true` and lists its cases by a name that appears in
  the file. The script checks the names exist but cannot tell if any is missing,
  so say in the design note how the list was built.
- **Status comes from our test file.** A mapped test is `planned` until a function
  of that name exists in the target file, `todo` while it carries `ref-todo` in an
  ignore or skip reason, and `implemented` after that.

## Decisions and unknowns

A plan leaves no question for the maintainer to answer.

- **A design question is decided in the note**, under "Decisions", with the
  reason and what would change the answer. The maintainer reviews decisions; they
  do not make them from a list of options.
- **A fact about Gmail the plan could not prove** from Google's documentation or
  a reference's tested behaviour is an `[[unknown]]`: what is assumed, what the
  design must change if that is wrong, and the exact calls that settle it against
  the real API. `scripts/features.py unknowns` collects them from every plan, so
  they can be run together before work starts, and the answers recorded with
  `status` and `result`. Design so that a wrong assumption fails safe: say which
  way that is in `if_wrong`.

## Rules every plan follows

These settle questions that more than one plan met. A plan that needs an
exception says so under its Decisions.

- **Direct call or durable row.** A write whose answer the host needs at once and
  which is harmless when repeated (drafts, labels, settings, permanent delete) is
  a quota-gated driver call that records Gmail's answer in the store. A write that
  must happen at most once and returns no handle (send) is a durable row
  delivered by a job and confirmed by Gmail.
- **Every Gmail call goes through the quota gate** with its declared cost, and
  never moves the history cursor.
- **Stored formats.** New persisted state gets its own column or table through a
  migration; nothing new goes into `mailtide_accounts.sync`. Anything an older
  build cannot read correctly (a new feed entry kind, a new state-string form, a
  field an older build would drop on rewrite) raises `SCHEMA_VERSION` in the same
  release, as a version-only step when there is nothing else to migrate. Each
  note's Compatibility paragraph gives the rollback. Migration numbers are
  assigned when a feature lands.
- **Reads check the schema too.** `changes`, `messages`, `labels`, `compact` and
  every new read refuse with `SchemaOutOfDate` on a database that is ahead, as
  `run` does, so a build that is too old refuses instead of misreading.
- **An unproven Gmail behaviour that is unsafe if wrong ships switched off.** The
  code path exists and is tested, but needs an explicit setting until its
  `[[unknown]]` is confirmed (today: Bcc on send, and permanent delete as a
  whole). Everything else is built on the assumed behaviour and fails safe.
- **One inline size limit.** A draft or a send whose message is at most 3 MiB goes
  in the JSON body; a send above that uses a resumable upload; a draft above it is
  refused until uploads exist for drafts.
- **A message mailtide writes is identified by a private header, not by its
  Message-ID.** Gmail replaces a client-supplied `Message-ID` and `Date` on every
  API send and every draft save (settled 2026-11-20: `send-message-id-kept`,
  `drafts-message-id-kept`, `compose-mime-date-kept`), and keeps other client
  headers byte for byte. So the identity that survives a save and a send is
  `X-Mailtide-Id: <token>`, where the token is 16 to 64 characters of
  `A-Za-z0-9_-` and unguessable. `compose` writes it (`tracking_id`, minted by the
  caller like the Message-ID was); `mailtide-mime` reads it from raw bytes; the
  import stores it as a header fact (`tracking_id`), so the feed carries it;
  drafts and send require it on the bytes they are given, store it on their row,
  and settle a lost answer by the imported `SENT` message that carries it. No
  search is involved: Gmail cannot search a private header. `In-Reply-To` and
  `References` are kept as written, so threading headers stay the caller's. A
  draft's `Message-ID` is whatever Gmail's copy says after each save.
- **Periodic refreshes share one shape**: due from a persisted time, a timer of
  their own, no feed entry when nothing changed, never in the way of mail
  (labels 15 min, draft index 15 min, settings 6 h).
- **Shared groundwork** (HTTP PUT, PATCH and DELETE, the unfenced driver-call
  pattern, `QuotaDeferred`, `WriteOutcomeUnknown`, schema-checked reads) lands
  before the features that use it.
- **Tests carry their plan's names.** A mapped reference test and an own test
  exist under exactly the name and file the plan gives; when building shows a
  planned test is wrong, the plan changes in the same commit, with the reason.

## The design note

Aim for a ten-minute read; tables are fine, and long ones (quota costs, error
mappings) go in an appendix. Use these headings:

1. **Goal and scope**: what a host can do afterwards, and what is left out.
2. **What the references do**: per reference, the files and the approach, with
   what is worth taking and what is not.
3. **Design in mailtide**: the public API (Rust and Python), store and schema
   changes, Gmail calls with their quota cost, how it meets the sync engine (feed
   entries, overlay, confirmation, epochs), and failure and retry behaviour.
4. **Alternatives rejected**: one line each.
5. **Tests**: how the reference suites were chosen, what is `na` and why, and the
   cases our own tests add.
6. **Slices**: the order to build it in, each small enough to review.
7. **Decisions**: each design question that had more than one reasonable answer,
   the answer chosen and why. Then one line per `[[unknown]]`, by id.
