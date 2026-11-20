# Send: new, reply and forward, large uploads, at most once, undo and schedule

Plan: `features/send.toml` (roadmap C3, C5). Status: planned.
Depends on `drafts` (draft submissions run its send routine), `compose-mime`
(the reply, forward and quoting helpers are built in its crate, on its
`Address` and `MessageId` types) and `search` (`by_message_id()` builds the
lookup that confirms a lost answer).

## 1. Goal and scope

Afterwards a host can, per account:

- send RFC 5322 bytes as a new message, a reply or a forward, and have it land
  in the right Gmail thread;
- send a message of any size Gmail takes (a resumable upload above 2 MiB);
- retry anything after a timeout or a crash and know the message leaves
  **at most once**, with Gmail itself saying whether a send whose answer was
  lost went out;
- delay a send (undo window, scheduled send), cancel it until delivery starts,
  and have that survive a restart;
- delay the send of an existing draft the same way;
- get from a mirrored original what a reply or forward needs: threading
  headers, subject, recipients, the text to quote and the parts to carry.

Left out, with the owner:

- building the MIME, Message-ID generation (`compose-mime`); which From
  addresses and signatures are allowed (`identities`); `send_draft` itself and
  its rules (`drafts`); reading one attachment (`message-facts`); how a
  Message-ID is written into a Gmail query (`search`: `by_message_id()`);
- where body, quote and signature go in a reply (signature placement), HTML
  quoting, the undo countdown, waking up at the scheduled time: the host's.
  The plain-text quoting helper (`quote_plain`) is planned here;
- delivery status after Gmail accepted the message. `sent` means "Gmail filed
  it under `SENT`". Google says a 200 does not mean delivery; a bounce is a
  new inbox message the mirror imports like any other.

## 2. What the references do

Gmail facts are from Google's pages listed under `[[source]]` in the plan,
read 2026-11-20. What those pages do not say is an `[[unknown]]` (§7).

**Zero** (MIT). `driver/google.ts` `create` is one `messages.send` with
base64 `raw` and the thread id, called from the request handler: no key, no
record, no retry. It has no undo or scheduled send, and never uploads. The
one guard is in `routes/assistant.ts`: an action row is claimed once before
the call, and "a timeout after sending remains indeterminate and is never
retried automatically: duplicate mail is worse than an error".
`assistant-addressing.ts` and `assistant-forward.ts` derive reply recipients
and forward content on the server. *Take:* claim once before the call; never
resend blind; the reply-recipient rules; a forward fails when an original
part is missing. *Leave:* re-fetching attachments from Gmail (we hold the
blob), rewriting `cid:` images into the HTML, the SMTP path (strip Bcc,
append a Sent copy: Gmail does both).

**himalaya** (MIT/Apache). `src/gmail/messages/send.rs` is the same single
call. `src/gmail/backend.rs` records that Gmail threads by `threadId` only and
finds a reply's thread with an `rfc822msgid:` search, skipping ids a query
cannot carry. Its JMAP backend stages a draft, submits it, and after a request
that failed without an answer destroys nothing, "the message possibly sent".
Its local queue calls a row whose request left without an answer
`unconfirmed`; cancelling it "undoes nothing". `shared/message/builder.rs`
derives reply and forward headers, recipients and carried attachments.
*Take:* the query form and its guard, `unconfirmed` as a named state, the
reply and forward derivation. *Leave:* the thread search (the mirror knows the
thread), body templates.

**pimalaya core** (MIT). Older reply and forward templates: useful cases for
who is answered (Reply-To, From, Sender, one's own message) and prefix
trimming. Its sending is SMTP and sendmail.

**jmap-client, Stalwart server.** The model: an `EmailSubmission` apart from
the Email, with `sendAt` and `undoStatus` (`pending`, `final`, `canceled`),
cancel refused with `cannotUnsend` once the message left, the delay held by
the server (FUTURERELEASE). No tests in either pinned tree; server paths are
names only (AGPL).

**mail-send, mail-builder, mail-parser.** Nothing to take: SMTP transport,
composition and parsing. Gmail derives the envelope from the headers.

## 3. Design in mailtide

**Approach.** A send is a **submission**: a durable row admitted without
calling Gmail, delivered by a Foreground job, confirmed by Gmail. This is the
SPEC §7 pattern, which `drafts` set aside for draft writes; Decision 1 says
why it fits here. The bytes are staged in blob storage; the row holds only
ids and state.

### States

| State | Meaning | Cancel |
|---|---|---|
| `pending` | waiting for `send_at` (or for a retry that is proven safe) | yes |
| `sending` | a job claimed it; a request may be in flight | `CannotUnsend` |
| `confirming` | a request left and its answer is lost; Gmail is being asked | `CannotUnsend` |
| `sent` | Gmail accepted it; has `message_id`, `thread_id` | final |
| `unconfirmed` | Gmail did not show it within the window; may still be out | host may `resend` |
| `refused` | Gmail (or the draft rules) said no; nothing sent; has a reason | final |
| `canceled`, `expired` | never sent | final |

**The one rule that makes it at most once:** a send request is issued only by
the job claim that moved the row `pending → sending`, in the same store
transaction as the claim. A later claim that finds `sending` under an older
attempt (a crash, a dropped future, a superseded engine) never sends: it turns
the row `confirming`. A row returns to `pending` only on proof that nothing
was sent.

### Public API

Rust (`mailtide_io::Runner`):

| Method | Does |
|---|---|
| `submit(account, key, source, options)` | stage and admit; no Gmail call; returns `Submission` |
| `send(account, key, source, options, budget)` | `submit`, then deliver within `budget` (as `act` does); returns `SendOutcome { submission, run }` |
| `cancel_submission(account, key)` | `pending → canceled`, else `CannotUnsend { maybe_sent }` |
| `reschedule_submission(account, key, send_at)` | only while `pending` |
| `resend_submission(account, key)` | only from `unconfirmed`: the host's explicit decision |
| `submission(account, key)`, `submissions(account, states, after, limit)` | from the store |
| `submission_content(account, key)` | the staged bytes (e.g. to reopen a cancelled message) |
| `reply_plan(account, message, own_addresses, all)`, `forward_plan(account, message)` | from the stored original |

- `key`: the host's idempotency key, unique per account; the Message-ID when
  the host gives none. `submit` with a known key and the same content returns
  the existing submission; other content is `KeyConflict`.
- `source`: `Raw(bytes)` or `Draft { draft_id, if_message_id }`.
- `options`: `thread_of: Option<MessageId>` (the mirrored message answered or
  forwarded) or `thread_id`; `send_at`; `not_after`.
- Admission checks, before anything is staged: exactly one Message-ID, in a
  form a query can carry (`search`'s `by_message_id()` returns a term for
  it); no live submission or mirrored `SENT` message with
  that Message-ID; at least one recipient header; size ≤ `send_max_size`;
  `thread_of` names a live non-draft message, and when that message is
  imported its Message-ID is in the bytes' `References`.
- mailtide never rewrites the bytes: what the host reviewed is what leaves.

`SendError`: `KeyConflict`, `MissingMessageId`, `DuplicateMessageId`,
`NoRecipients`, `TooLarge { limit }`, `UnknownOriginal`, `ThreadingMismatch`,
`CannotUnsend { maybe_sent }`, `UnknownSubmission`, `ReadOnly`,
`OriginalUnavailable`.

Python `Runner` and `BlockingRunner` get the same methods (`raw: bytes` or
`draft_id=`, keyword options), frozen dataclasses `Submission`, `SendOutcome`,
`ReplyPlan`, `ForwardPlan`, module functions `reply_plan(raw, own_addresses,
all=False)`, `forward_plan(raw)`, `quote_plain(text)`, and one exception per
error above.

**Reply and forward** are pure functions in `mailtide-mime`, built after
`compose-mime` and on its `Address` and `MessageId` types, over the original:
`In-Reply-To` and `References` (the original's chain, then its id), a subject
with one `Re:` or `Fwd:`, recipients (Reply-To, else From; the original's
recipients when the user wrote it; reply-all keeps other To in To and Cc in
Cc, without own addresses, each once), the plain text to quote, and for a
forward every attachment and inline part with its name, type parameters and
Content-ID. A part that cannot be read makes the plan an error. The host
composes with `compose-mime` and submits with `thread_of`.

### Gmail calls and quota

| Call | Units | When |
|---|---|---|
| `messages.send`, JSON `raw` | 100 | bytes ≤ `send_inline_limit` (2 MiB) |
| upload session start (`uploadType=resumable`) | 100 | larger messages |
| chunk `PUT`, status query | 0, leased | within the session (`send-upload-quota`) |
| `messages.list?q=rfc822msgid:…&labelIds=SENT` | 5 | confirming a lost answer |
| `drafts.send` | 100 | a draft submission (drafts' call) |

All Foreground. An ordinary send costs 100 + 2 (the history pass) + 60 (the
import of the sent message) = 162 units: about 29 a minute under the default
account budget, far above Gmail's own sending limits. A gate deferral is not
an attempt. A 429 "Mail sending" limit is an account cooldown and the row is
`pending` again.

**Upload.** The engine picks the protocol from the size alone. Resumable:
start the session, **commit its URI to the row**, then `PUT` chunks
(`send_chunk_size`, 8 MiB, multiples of 256 KiB). A later claim that finds a
session asks it first (empty `PUT`, `Content-Range: bytes */N`): 308 resumes
at Gmail's offset on the same session; 200 or 201 is `sent`; 404 or 410
restarts if the final chunk was never issued, else confirms by Message-ID. A
session URI outside Gmail's origin is refused, so the token and the message
go nowhere else.

### Store

One migration (the next one; numbers are assigned at merge):
`mailtide_submissions (account, key, rfc822_message_id, state,
due_at, submission)` with primary key `(account, key)` and an index on
`(account, rfc822_message_id)`. It is needed because a submission has no
message id until Gmail answers, so it fits neither the message nor the intent
table, and the import-time lookup by Message-ID must be indexed. Jobs need no
change: `Send` uses the existing `(account, kind, message)` key with the
submission key. Bytes are in blob storage at `outbox_key(account, key)`,
written before the row and deleted once `sent` or at retention.

`Store::submit`, `cancel_submission`, `reschedule_submission` and
`resend_submission` are, like `admit`, **not fenced by an epoch**: a user's
send or undo must survive one sync superseding another. Cancel and the job
claim are two transactions on one row; exactly one wins.

**Compatibility.** Besides the table, the feature writes `SubmissionChanged`
entries to the log and `Send` rows to `mailtide_jobs` (keyed by a submission
key where every other kind has a message id), and stages bytes under
`outbox_key`. A build from before it can decode none of these. The migration
raises `SCHEMA_VERSION`, so such a build sees the database as `ahead` and
refuses to sync (`SchemaOutOfDate`) before it claims a job. Its reads that
check no schema (`changes()`) fail with `CorruptState` on the first
`SubmissionChanged` entry of a page; they never skip it. Reverting, once no
row is `pending`, `sending` or `confirming` (cancel or let them finish
first):

```sql
DELETE FROM mailtide_jobs WHERE kind = <Send>;
DELETE FROM mailtide_changes WHERE change LIKE '{"SubmissionChanged"%';
DROP TABLE mailtide_submissions;
DELETE FROM mailtide_schema WHERE version = <this migration>;
```

and delete the staged objects under the outbox prefix. Lost: the outbox and
its history, and with it the idempotency keys: a host that retries a key
after the next upgrade admits a new submission, unless the mirror already
holds the `SENT` message with that Message-ID. A `pending` row dropped by a
rollback is a message that is never sent; a `sending` or `confirming` one is
a send that is never confirmed. Feed consumers keep their positions.

### Meeting the sync engine

- **Delivery.** `claim_jobs` hands a due `Send` job its `SendPlan`, read in
  the claim: `Deliver`, `Resume { session }`, `Confirm`, `SendDraft` or
  `Settled`. Results are committed under the claim and the epoch, so a
  superseded engine's answer is rejected and its successor confirms instead.
- **The sent message** is not synthesised. After `MessageSent` the engine
  reads history at once; the `SENT` message is discovered and imported like
  any other, usually in the same run.
- **Confirmation by Gmail**, the rule `drafts` uses: when a commit imports a
  `SENT` message whose Message-ID a `sending`, `confirming` or `unconfirmed`
  row holds, the row becomes `sent` in that commit. A draft or a received
  copy with the same Message-ID settles nothing.
- **Feed:** `SubmissionChanged { key, state, message_id, thread_id }` on every
  state change. `sent` precedes that message's `MessageDiscovered` and
  `MessageImported` unless the import is what confirmed it.
- **No overlay.** Message and thread views show Gmail's state. A host lists
  `submissions` to draw an outbox.
- **Schedule.** mailtide has no timers. A pending send is a job due at
  `send_at`; `RunOutcome` `idle` already names the next due time, and the host
  calls `run` then. `send` delivers at once.
- **Draft submissions** stage nothing. At `send_at` the job runs drafts'
  `send_draft` routine with `if_message_id`; its outcomes map to `sent`,
  `refused{draft_changed | draft_gone}` or `confirming`, and it settles on
  `DraftSent`. Cancel leaves the draft in Gmail untouched.

### Failure and retry

| What happened | Row | Next |
|---|---|---|
| Gate deferral, failure to connect, 429, rate-limit 403, 401 | `pending` (not sent) | retried with backoff; still cancellable |
| 400, other 403, 413, 404 on the thread | `refused` with Gmail's message | nothing; the account is not blocked |
| Timeout, connection lost after the request was written, 5xx, a 200 without an id | `confirming` | session status (uploads), then lookups at 15 s, 1, 3 and 10 min |
| Crash, dropped future or supersession while `sending` | `confirming` at the next claim | same |
| Lookup or import shows the message | `sent` | history pass |
| Nothing shown after `send_confirm_window` (10 min) | `unconfirmed` | never resent by mailtide; a later import still makes it `sent` |
| Account blocked (credentials) | stays `pending` | goes out after `resume()` |
| Read-only mode | `submit` refused; an older row ends `refused{read_only}` | none |

**Where this differs from `drafts.send`, and why.**

| | `drafts.send` (drafts) | `messages.send` (here) |
|---|---|---|
| At most once comes from | Gmail consuming the draft | the claim rule above |
| Marker before the call | row `sending` | row `sending`, set by the claim |
| Proof it was sent | `SENT` import with the Message-ID | the same, plus the lookup and the session status |
| Proof it was *not* sent | the draft is still there | none exists, so no automatic retry |
| Ends without proof as | `send_unconfirmed` after 1 h | `unconfirmed` after 10 min (the lookup asks Gmail directly) |

Final rows and their bytes are deleted `submission_retention` (7 days) after
their last change.

## 4. Alternatives rejected

- **A synchronous driver call with no row (Zero, himalaya):** nothing tells a
  crashed send from one never made, and every host would rebuild the outbox,
  the delay and the cancel race.
- **Send every message through a hidden draft** (`drafts.create`, then
  `drafts.send`): retries would be safe by consumption, but a draft flickers
  through the mailbox and the feed, it costs 110 units, and it rests on
  `drafts-send-twice`. It is the fallback if `send-message-id-kept` is refuted.
- **Resend when the lookup finds nothing:** absence is not proof (SPEC §5.6),
  and a request can land late. Duplicate mail is worse than an error.
- **Bytes in the database row:** up to 35 MB per row; blob storage exists for
  this.
- **`uploadType=multipart` or `media`:** a third path with no recovery story;
  `media` cannot carry a thread id.
- **Finding the reply's thread with `rfc822msgid:` (himalaya):** the mirror
  already has the original's thread id.
- **Adding or rewriting headers in the engine** (a Message-ID, threading
  headers): the bytes sent would not be the bytes reviewed.
- **Timers or a background task inside mailtide:** the engine is sans-IO and
  hosts already schedule `run`.
- **A row for the sent message from the send's answer:** the answer has no
  checkpoint; history gives one two units later.

## 5. Tests

Suites were found with `grep -ri` for send, submit, reply, forward, undo and
schedule over every checkout's test files, then read.

- **Zero.** The two tests `drafts` left: the forward one maps to "no plan
  with a part missing", the SMTP one is ported by behaviour (one send, Bcc
  intact, `SENT`, same thread). `assistant-forward` (3 mapped, 2 `na`: HTML
  rewriting, content hash), `assistant-addressing` (5 mapped, 1 `na`),
  `forward-attachments` and `smtp/client` (all `na`, with reasons).
- **himalaya.** `shared/message/builder.rs` is shared with `compose-mime`: we
  take reply, forward and plain quoting (14 mapped, 5 `na`: explicit
  recipients and layout); 10 are deferred to it. `tests/message_reply.rs`,
  `tests/message_forward.rs`, the two quote-text tests `message-facts` left
  in `part.rs`, `tests/send_outcome.rs` (1 mapped, 2 `na`: Gmail files the
  copy and has no answered flag), `email/submission.rs`, the queue's two
  `unconfirmed` tests. `gmail/backend.rs` and `jmap/backend.rs` are large,
  mostly foreign and shared with other plans, so neither has a `rest_na`:
  one test of the first is mapped (`search` lists the other 16 or defers
  them to their owners), and of the second two are mapped and nine are `na`
  one by one (its writes and refusals; `threads` lists its reads and
  queries, `purge` its deletes).
- **pimalaya core.** Reply and forward templates: who is answered and prefix
  trimming map (7); template bodies are `na` (11).
- **Not declared.** Zero `assistant.test.ts` (a host approval workflow; its
  "never retried" rule is restated as ours), `gmail-delivery-failure.test.ts`
  (retry classes of Zero's label deliveries, which retry a network failure:
  the opposite of what a send needs; `delivery-retries` accounts for it),
  `outgoing-mime.test.ts` and
  `email-composition.test.ts` (siblings); himalaya's live suites
  (`tests/google.rs`); mail-send's SMTP test; core's SMTP and sendmail tests.

Our own 76 tests cover what no reference has: request shapes and costs; every
admission check; the claim rule and each row of the failure table; a crash
between Gmail's answer and its record; supersession mid-send; the upload
session (resume, lost final answer, dead session, foreign origin); undo
racing delivery; a scheduled send across a restart; draft submissions;
read-only mode; conformance on every store; a simulator campaign whose Gmail
applies a send and loses the answer, applies it late, refuses and rate
limits, checking that Gmail never holds two messages for one submission; and
the Python API.

## 6. Slices

1. **Wire.** JSON send, the lookup, failure classes, costs, command and event
   variants.
2. **Submissions in the store.** Rows and pure rules, the unfenced store
   methods, migration, conformance, the feed entry. No Gmail call yet.
3. **Immediate send.** Staging, the `Send` job, `submit` and `send`,
   `thread_of`, success, refusal, proven-not-sent retry, the history pass,
   read-only. Gate: `send-bcc-stripped`.
4. **Lost answers.** `confirming`, lookups, the import rule, `unconfirmed`,
   `resend`; simulator sends and invariants. Gate: `send-message-id-kept`,
   `send-lookup-latency`.
5. **Delay.** `send_at`, cancel, reschedule, `not_after`, retention.
6. **Large messages.** Headers and raw bodies in the wire layer, the session,
   resume and status. Gate: the `send-session-*` and `send-size-limit` unknowns.
7. **Draft submissions.** After drafts' send slice.
8. **Reply and forward plans** in `mailtide-mime`, and the Runner methods.
9. **Python**, `FakeGmail`, SPEC §4, §7–§11, §14.

**Shared groundwork.** No separate plan owns the pieces several plans need;
each is added by whichever plan lands first, and the later ones use it as it
is:

- PUT (upload chunks; request headers and raw bodies are this plan's own) in `crates/mailtide-gmail/src/http.rs` and
  `crates/mailtide-io/src/transport.rs` (today: GET and POST): added by
  whichever of `drafts`, `identities`, `label-edit`, `purge`, `send` lands
  first.

**Landed.** PUT, PATCH and DELETE are on `main`; headers, raw bodies and
session URIs stay this plan's. The transport now reports a connection dropped
after the request went out as `OutcomeUnknown`, never `NotSent`.

## 7. Decisions

1. **A send is an intent with delivery and confirmation; draft writes are
   not.** A draft write needs Gmail's id at once and is harmless to repeat. A
   send needs neither: the host has its key, and the call must never repeat.
   Undo and schedule need a durable row with a due time anyway. *Would
   change:* nothing short of Gmail offering an idempotency key.
2. **The key is the host's; the Message-ID is required and is what Gmail
   confirms by.** A host already has an id for its compose session, and a
   draft submission may not know its Message-ID yet. *Would change:*
   `send-message-id-kept` refuted (then: hidden draft, or a private header).
3. **mailtide never resends on its own after an unanswered request.** It ends
   `unconfirmed` and the host decides. A 5xx counts as unanswered: Google asks
   for a retry, but nothing says a 5xx send did not go out. *Would change:*
   measurements showing a 5xx never sends, to retry those at once.
4. **Bytes are staged in blob storage, not in Gmail and not in the row.**
   *Would change:* a host without writable blob storage.
5. **JSON `raw` up to 2 MiB, a resumable session above.** Both references
   prove the JSON path; only the session can recover a lost answer. The limit
   is a setting: once `send-session-completed-status` is confirmed, setting it
   to 0 makes every send recoverable. *Would change:* that confirmation.
6. **Undo and schedule: the engine owns `send_at`, the cancel race and
   durability; the host owns the window, the wake-up and the UI.** The race
   between undo and delivery must be one store transaction, and only the
   engine can test it under faults. A late send goes out late unless the host
   set `not_after`.
7. **A delayed draft send is a submission too**, though `drafts` left the
   delay to the host: a real "undo send" is almost always the send of a draft,
   and cancel then simply leaves the draft. *Would change:* the maintainer
   keeping every draft concern in `drafts`; slice 7 can be dropped alone.
8. **The sent message reaches the mirror by ordinary sync.** *Would change:*
   `send-history-on-send` refuted (then: a `Verify` job for the answered id).
9. **Reply and forward: plans, not messages.** mailtide derives headers,
   recipients, quote text and parts, quotes plain text, and builds no body.
   Reply-all follows Gmail's placement, not himalaya's. The helpers
   (`reply_plan`, `forward_plan`, `quote_plain`) stay planned here, as pure
   functions in `mailtide-mime` built after `compose-mime`, whose note says
   the same from its side. Signature placement and HTML quoting are
   host-side in both plans.
10. **Admission refuses instead of repairing** (no Message-ID, References
    not naming the original, no recipient): the bytes are never rewritten.
11. **No overlay; submissions are their own list.**
12. **Read-only mode refuses `submit` and ends older rows `refused`**, as
    actions are blocked there.
13. **Final rows last 7 days**, long enough for a host's own retries to meet
    the same key.

Unknowns (`scripts/features.py unknowns send` prints the tests):

- `send-message-id-kept`: Gmail keeps our Message-ID through `messages.send`.
- `send-lookup-latency`: how soon `rfc822msgid:` shows a sent message.
- `send-late-apply`: how late Gmail can apply a request the client abandoned.
- `send-bcc-stripped`: Bcc is delivered and hidden from other recipients,
  and kept in the `SENT` copy and in a stored draft. The only one that is
  unsafe if wrong; it gates slice 3. It is the one unknown for Bcc:
  `compose-mime` and `message-facts` refer to it.
- `send-thread-mismatch`: what `threadId` with a changed subject or missing
  headers does.
- `send-thread-gone`: what a deleted `threadId` answers.
- `send-session-completed-status`: a finished session answers a status query
  with the message and sends nothing again.
- `send-session-thread`: a session's JSON metadata carries the thread id.
- `send-size-limit`: the largest message the upload takes (35 MB assumed).
- `send-upload-quota`: a session costs 100 units once.
- `send-history-on-send`: the sent message is in history when the send answers.

Owned elsewhere, not repeated: `drafts-message-id-kept`,
`drafts-send-twice` and `drafts-send-concurrent` (draft submissions),
`drafts-raw-json-limit` (our 2 MiB inline limit sits under its 3 MiB; its
test also sizes `messages.send`), `drafts-history-on-send` (`drafts.send`;
ours is the same question for `messages.send`, a different endpoint, run
together), and `search-msgid-match` (which Message-IDs `rfc822msgid:` can
find).
