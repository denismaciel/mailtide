# Send: new, reply and forward, at most once, undo and schedule

Plan `features/send.toml` (roadmap C3, C5), in progress. Requires `drafts` for
saved-draft delivery, `compose-mime` for tracking tokens and reply/forward
types, and `message-facts` for the token on imported mail.

## 1. Goal and scope

A host can send RFC 5322 bytes as a new message, reply or forward, or ask to
send an existing Gmail draft. A raw submission survives a restart and makes
at most one wire request. The host can give a raw submission a due time and
cancel it until a worker claims it. The host can also derive recipients,
headers, quote text and parts from a mirrored original.

The same `RunnerConfig.writes.max_message_bytes` setting applies to
raw-send admission and draft writes. Its default is 35,651,584 bytes (34 MiB).
Gmail accepted JSON `raw`
at 40 MiB in live tests. Messages up to 34 MiB were delivered, and sends from
35 MiB could answer 200 and then bounce. The feed reports Gmail acceptance
and import. The mirror handles bounces as ordinary incoming mail.

The host supplies its own addresses and chooses the From address, signatures,
body layout, HTML quoting, undo window and wake-up. Identity settings belong
to the separate `identities` plan. `compose-mime` builds RFC 5322 bytes and
`message-facts` reads parts. The host schedules existing drafts by delaying
its call to send them.

## 2. What the references do

Google's method, sending, threading, quota and error documentation is listed
in the plan. The live probes in §7 settle where API behaviour differs from
the documentation or leaves an ambiguity.

**Zero** (MIT). `driver/google.ts` sends JSON `raw` and an optional thread
id directly. `routes/assistant.ts` claims the action row before calling
Gmail and leaves an unanswered send indeterminate. We use the claim from
the latter, the request shape from the former, and the recipient and
fail-closed forwarding rules from `assistant-addressing.ts` and
`assistant-forward.ts`. Gmail derives the envelope, strips Bcc from
recipient copies and files the sent copy.

**himalaya** (MIT/Apache). `src/gmail/messages/send.rs` wraps one JSON
send. `src/gmail/backend.rs` uses `threadId` to thread a reply. We take the
id from our mirror. The JMAP backend and local queue keep an uncertain
send visible and refuse to undo it. `shared/message/builder.rs` gives the
reply-header, recipient and carried-attachment cases.

**pimalaya core** (MIT). The reply and forward templates give the cases
for recipient fallback, replies to one's own mail and subject prefixes.

**jmap-client, Stalwart server.** `EmailSubmission` has a due time and a
pending-to-final boundary for cancellation. The cited Stalwart paths are
names only (AGPL). There are no matching tests in those pinned trees.

**mail-send, mail-builder, mail-parser.** The first covers SMTP. Composition
and parsing already have their own feature plans.

## 3. Design in mailtide

### Host interface

Rust methods on `mailtide_io::Runner`:

| Method | Result |
|---|---|
| `submit(account, key, raw, options)` | Admit a raw submission without calling Gmail and return its `Submission`. |
| `send(account, source, budget)` | For `Raw { key, raw, options }`, submit then run within budget and return `SendOutcome::Raw { submission, run }`. For `Draft { draft_id, if_message_id }`, return `SendOutcome::Draft(SentDraft)` or the draft error. |
| `cancel_submission(account, key)` | Cancel while `pending`. Repeated cancellation returns the same canceled row. After claim, return `CannotUnsend` with the known disposition. |
| `submission(account, key)`, `submissions(account, states, after, limit)` | Read raw submissions from the schema-checked store. |
| `submission_content(account, key)` | Read staged bytes while retained. |
| `reply_plan(account, message, own_addresses, all)`, `forward_plan(account, message)` | Derive a plan from an imported original. |

Raw options are `send_at` and at most one of `thread_id` and `thread_of`.
The latter resolves to the thread of a live, non-draft mirrored message at
admission. A missing original is `UnknownOriginal`. Omitting both omits
`threadId`, so Gmail creates a new thread. Gmail uses the supplied
`threadId` alone on the sender's side even when the Subject or reply
headers differ. A gone thread is refused. A confirmed submission reports
the ids of the imported message.

The caller mints an unguessable tracking token of 16–64 `A-Za-z0-9_-`
characters, passes it to `compose` or supplies it on the raw bytes as exactly
one `X-Mailtide-Id` header. Admission reuses the MIME header count
(`tracking_headers`) and token reader (`tracking_id`) used by drafts.
The validated token is saved with the submission.
Raw admission also requires a recipient in To, Cc or Bcc, and checks decoded
RFC 5322 byte length against `max_message_bytes` before staging. The bytes
go out as supplied. Gmail assigns Message-ID and Date.

The optional host key defaults to the token. A repeated key with the same
raw bytes and requested options returns the existing row, including after
the original message disappears or staged bytes are removed. A different
request under that key is `KeyConflict`. A token owned by another send is
`DuplicateTrackingId`. Admission reports `InvalidTrackingId`,
`NoRecipients`, `TooLarge { limit }` or `ReadOnly` as appropriate. A
reply/forward without readable original content is `OriginalUnavailable`.

Python `Runner` and `BlockingRunner` mirror these methods and return frozen
result types. `send` takes either raw input or `draft_id` and its optional
version check. Draft errors and receipts are the same as for `send_draft`.
Module-level `reply_plan(raw, own_addresses, all=False)`,
`forward_plan(raw)` and `quote_plain(text)` expose the pure helpers.

### Raw submission lifecycle

| State | Host-visible meaning |
|---|---|
| `pending` | Admitted and cancellable. Eligible at `send_at`, or immediately if omitted. |
| `claimed` | The first worker closed the cancel window. Pre-dispatch work may resume on a later run. |
| `sending` | A fenced dispatch marker has committed. Gmail may see the request. |
| `confirming` | Waiting for import. A decoded 2xx can expose a provisional Gmail receipt. |
| `sent` | Sync imported a `SENT` message carrying the token. The submission exposes that message and thread id. |
| `unknown` | The exposed confirmation deadline passed. A later matching import can still make the row `sent`. The send can never be dispatched again. |
| `refused`, `canceled` | Definitive refusal with a reason, or cancellation before claim. |

The first job claim atomically changes a due `pending` row to `claimed`.
Cancellation and claim serialize on the same row. The driver reads the
staged blob, verifies its fingerprint and prepares the request before a
fenced `BeginSubmissionSend` commit changes the row to `sending`. That
commit must succeed before the worker can call Gmail. The marker records
the attempt and absolute confirmation deadline (10 minutes from the
marker). A successor can take over pre-dispatch work on a `claimed` row.
If it finds an unresolved marker, it can only reconcile or expire it.
Result commits carry the claim and epoch. A result racing a matching import
cannot roll back `sent`.

The driver uses the detailed, dispatch-bounded `direct_call_before`, through
the same quota gate as ordinary sync. Its hard dispatch deadline is the
earlier of the run budget and the marker's confirmation deadline. The
transport makes one attempt, with redirects and automatic retries disabled.
The public `Runner::call_gmail` excludes the send request. When quota release
or cooldown cleanup fails after the write, the driver first records the
returned HTTP disposition if its claim still holds, then surfaces the
cleanup failure. Any crash or ambiguous error around the dispatch marker
leaves it closed to further writes.

| Disposition | What the job records |
|---|---|
| Quota deferral, dispatch budget expired before a write, or explicit `NotSent` proving no HTTP request left | Clear that attempt's marker under its fence. Keep `claimed` with a retry time using the gate's deferral/cooldown, or existing bounded backoff. |
| Definitive HTTP refusal, including 400, 401, 403, 404, 413 or 429 | `refused`, with Gmail's reason and any returned `retry_at`. A cooldown still applies to other work. |
| Decoded 2xx | `confirming` with the provisional receipt. |
| Unusable 2xx, 408, 5xx, unexpected response, timeout or disconnect after possible dispatch | `confirming`. |
| Crash, dropped future or supersession with marker still present | The successor treats it as `confirming` without making a write. |
| No matching import by the deadline | `unknown` when `run` processes the deadline, including when the account is blocked. |
| Read-only mode | Admission fails before staging. Previously admitted work that has no dispatch marker is refused locally. Work with a marker can still settle through sync. |

A definitive HTTP response already consumed the wire request. Only proof
that no HTTP request left allows another pre-dispatch attempt. Credential or
blob failures before dispatch follow the existing backoff or local refusal
paths. An account already blocked by sync holds delivery until resumed.

### Drafts and the import path

A submission naming a draft uses `drafts.send` through the existing
`Runner::send_draft` path. It uses the draft index's stable id, the
`if_message_id` version check and the tracking token already on the saved
bytes. Drafts owns its `BeginSend` fence, hard dispatch deadline,
`retry_at` and outcome after a lost reply. Gmail consumes the draft id on
send. The sequential and concurrent probes in the drafts plan found one
successful send and 404 for further calls on the same id.
The host gets its standard immediate receipt or draft error, and its
standard `DraftSent` or unknown-outcome notification for reconciliation.
An immediate receipt acknowledges Gmail's answer. When the answer is
lost, a matching `SENT` import confirms the send. From `retry_at`, drafts
can retry when a fresh `drafts.get` still finds the same draft version.
It can also retry after a definitive HTTP refusal. A consumed id cannot
send another message. Draft admission and validation take place in drafts.
The draft path creates
no raw outbox row, staged blob, raw claim or `SubmissionChanged` entry.
Draft sends use the draft's saved content and thread. Raw options belong
only to the raw variant.

Both paths use one import hook, owned by drafts. In the transaction applying
`MarkImported`, the hook takes the imported account, `SENT` label and
`content.facts.tracking_id` from `message-facts`. It dispatches to the
token's owner. For a raw row with a dispatch marker, or in `confirming` or
`unknown`, it records `sent` and emits `SubmissionChanged` in the import
commit. A draft token goes to the existing draft confirmation rule.
A draft or an incoming copy without `SENT` cannot settle a send. Ordinary
sync finds the sent message. After a possible send or recovery of an
unresolved marker, the engine requests a normal history pass without
changing its cursor directly. A history pass costs 2 units per page and
a raw import costs 60. Either send endpoint itself costs 100 units at
Foreground priority.

### Store, feed and compatibility

The next free migration adds `mailtide_submissions`, keyed by
`(account, key)`, with the request fingerprint, tracking token, thread,
blob reference, state, claim/dispatch data, receipt and due time. An
`(account, due_at, key)` index bounds selection of ready work. A small
`mailtide_send_tokens` table reserves `(account, tracking_id)` for one
raw key or one draft id. Raw admission reserves the token with its row.
The raw admission slice adds the reservation to drafts' existing `BeginSend`
transaction after drafts lands. A different owner causes a draft validation
error before dispatch.
The same draft id may reuse its reservation only as permitted by the
draft fence. For draft tokens imported without a reservation from a
pre-send build, the hook keeps its existing draft matching path.

A `SendSubmissions` Foreground job reuses the account-level job shape
introduced for drafts' `RefreshDrafts`, with its own kind. The claim selects one due raw
row in due-time/key order or processes its confirmation deadline or blob
cleanup. Assigning the row to a claim and publishing the next due work
use the existing account locking and epoch rules. The run's `Idle { next }`
reports the next scheduled work so the host can call `run` at the right time.
Admission and cancellation are unfenced, as with `admit`. The engine owns
fenced delivery and reconciliation. When an account is blocked, `run`
still applies due confirmation expiries locally under the account lock.
The expiry compares the row's state and deadline so it can race an
import safely. The host can use the exposed deadline to wake a blocked
account for this step. A host reads submissions to draw the
outbox. `SubmissionChanged` includes the key, token, state and any Gmail
receipt. Message and thread views keep showing mirrored Gmail state.

Stage each candidate blob under an attempt-unique key within the account
prefix before committing admission. A known losing candidate can be
deleted without touching the winner. A failed blob write leaves no row.
After an ambiguous transaction result, keep the candidate until its
ownership is known. Account offboarding removes orphan objects as well
as referenced objects and the two tables' account rows. Terminal rows
retain staged content for seven days from their first terminal state.
Cleanup removes the blob idempotently and clears its reference. Small
receipts and token reservations remain until `forget` so a late import
can settle an unknown and replays cannot send again.

**Compatibility.** The migration raises `SCHEMA_VERSION` for the new
store state, job kind and feed entry. Schema-checked operations on older
builds return `SchemaOutOfDate`. For rollback, quiesce sends, cancel
pending raw work and drain claimed work and confirmation deadlines first.
Stop workers, delete `SendSubmissions` jobs and `SubmissionChanged` feed
entries, drop the two send tables, remove the send migration's version
record and delete staged objects under the account outbox prefixes. Draft
index and draft feed entries follow drafts' own rollback contract. Removing
send receipts loses raw idempotency history and cross-route reservations.
Hosts must preserve those keys and tokens across rollback to avoid replay.
Feed cursors can retain their positions across the deleted entries.

### Reply and forward

In `mailtide-mime`, build on `compose-mime`'s `Address` and `MessageId`
types. A reply derives In-Reply-To and References from the original and sets
one `Re:` prefix. It addresses Reply-To, else From or Sender, or the original
recipients when the user wrote it. Reply-all keeps other To in To and Cc in
Cc, excluding the host's own addresses and deduplicating case-insensitively.
Forwards set one `Fwd:` prefix and provide the original's headers as data,
plain text to quote, and attachment and inline parts with type parameters
and Content-ID. An unreadable original or part fails the plan. The host
composes the outgoing bytes and can pass `thread_of` to join the source
thread.

## 4. Alternatives rejected

- Synchronous raw sends with no row lose the dispatch fence on crash.
- A hidden draft adds a mailbox object, another Gmail write and 10 quota units.
- Resumable upload can duplicate a send after a misleading 308. JSON `raw`
  covers the measured delivery range.
- Searching by Message-ID cannot recover a lost answer because Gmail rewrites
  it. A private tracking token survives and normal import exposes it.
- Explicit resend and automatic retries after dispatch can duplicate mail.
- Requiring matching subject or reply headers would reject cases Gmail threads
  successfully by `threadId`.
- Scheduling saved drafts inside raw submissions would add a second owner for
  the draft's send and a delayed draft-version fence.
- Reschedule, a separate latest-send cutoff, and background timers add queue
  transitions or scheduling machinery. The raw row needs only a due time and
  cancellation before claim.
- A synthetic sent message from the HTTP receipt has no sync checkpoint.
- Storing staged bytes in SQL would duplicate the existing blob storage path.

## 5. Tests

- **Zero.** The two deferred tests cover fail-closed forwarding and one
  Gmail reply carrying Bcc, joining its thread and importing under `SENT`.
  The remaining mappings cover reply recipients and forward parts. SMTP
  mechanics, HTML layout and Gmail attachment re-fetching are marked `na`.
- **himalaya.** The reply parent query guard is `na` because submission
  confirmation and threading make no such query. The reply, forward,
  plain quoting, refusal and uncertain queue mappings exercise the
  corresponding raw-send behaviour. The existing-draft refusal goes through
  drafts' own path. The other shared-suite cases belong to their listed owners.
- **pimalaya core.** Recipient fallback and prefix trimming map. Template
  layout stays with the host.

Own tests cover the JSON request, quota and write guards, one-request
dispositions, admission and cross-route token collisions, the claim/cancel
race, restart and schedule, atomic import confirmation, unknown and late
import, receipt and blob retention, draft routing, reply/forward and Python.
Store conformance runs on every backend. The simulator varies crashes,
supersession, and Gmail accepting, refusing, dropping or applying a request
after its answer is lost.

## 6. Slices

1. **Single JSON call.** Build the typed wire request and receipt, quota
   cost and detailed direct-call disposition. Exercise the request,
   decoder and public write guard without enabling delivery.
2. **Raw admission and store.** Add key and token reservations, migration
   and its schema assertion, reads, due time, atomic claim/cancel and feed.
   Integrate the draft token reservation with `BeginSend` after drafts
   lands. Exercise all stores before exposing public raw admission.
3. **Fenced delivery and confirmation.** Enable the account job, bounded
   direct call, history wake-up and shared import hook together. Add staging
   and expose raw admission, delivery, cancellation and reads. Cover
   deadlines, cleanup, restart, read-only, the raw reference tests and
   simulator faults here.
4. **Draft routing.** Add the `send` source dispatch to the existing
   `send_draft` entry point and check its receipt, refusal and unknown
   handling. The draft path keeps its own attempt rules and record.
5. **Reply and forward.** Implement the pure helpers and mirror-backed
   Runner methods with their mapped tests.
6. **Python and documentation.** Bind the methods and feed types, extend
   FakeGmail, and document the API and rollback in SPEC.

## 7. Decisions

1. Raw sends use durable claims because the host needs restart-safe delay,
   cancellation and a fence before Gmail. A Gmail idempotency key would
   justify revisiting the dispatch fence.
2. The token identifies the outgoing message across both routes. Scoped
   token ownership prevents either consuming a submission as a different
   source. Retaining small receipts allows safe replays and late imports.
3. Gmail-confirmed mail comes from import. A successful HTTP answer is
   available as a receipt while the submission waits. The ten-minute raw
   window allows for sync and backoff. Ordinary late imports still settle
   `unknown`; observed import delays could change that window.
4. Use the existing no-retry transport and detailed direct-call path.
   A refusal ends a raw submission even when Gmail suggests retry. Only
   definite pre-dispatch failures can requeue the same raw work. Drafts
   relies on Gmail consuming the draft id and keeps its retry rules.
5. Use one JSON path and one admission limit. Enable Bcc on that route
   based on the measured recipient copies.
6. Persist the raw due time and make claim the undo boundary. Saved drafts
   use the drafts send lifecycle; the host can delay entry into it.
7. Keep delivery bytes in the existing blob store. Unique staging keys
   prevent concurrent submissions from overwriting each other's content.
   Seven-day retention lets a host recover canceled or refused content.
8. Take the thread id from the mirror or the host. Use MIME reply headers
   for message semantics and the host's own address list for recipient
   suggestions.

Recorded Gmail probes (`scripts/features.py unknowns send` prints the details):

- `send-message-id-kept` was refuted. Gmail rewrites Message-ID and Date
  and preserves private X- headers.
- `send-lookup-latency` was refuted for the client Message-ID. The same
  test saw prompt history records.
- `send-late-apply` confirmed all 200 fully written requests applied even
  when the client aborted. The observed timing was within about a second.
- `send-bcc-stripped` confirmed the recipient copy strips Bcc, 5 of 5.
- `send-thread-mismatch` confirmed sender-side threading by `threadId`
  alone.
- `send-thread-gone` confirmed a gone or nonexistent thread id is refused.
- `send-session-completed-status` was refuted as a recovery guarantee.
  A re-upload following a transient 308 duplicated sends.
- `send-session-thread` confirmed the historical upload metadata test.
  Production uses the JSON send endpoint.
- `send-size-limit` was refuted as a wire refusal limit. Gmail accepts
  JSON `raw` at 40 MiB and can bounce accepted sends from 35 MiB.
- `send-history-on-send` confirmed the sent id was in the first history
  read after the response.
