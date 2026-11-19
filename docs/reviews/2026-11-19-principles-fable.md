# Principles review (fable), snapshot `b4273ee`

Scope: every crate and the Python layer in `.review/b4273ee/`. Paths below are
relative to that snapshot. I read code only; nothing was built or run. The
2026-11-18 types review already covers several contract gaps (lifecycle sum
type, enumeration scope, batch validation); I don't repeat those except where
the snapshot still has the problem and it bites.

Overall: the core is in good shape for these principles. The engine is a real
sans-IO state machine, the mirror rules are pure and shared by every store,
failures and deferrals are values, and the Gmail wire layer parses once at
the boundary. The findings below are mostly about places where a type admits a
state the design never produces and the code then compensates with sentinels,
`unwrap_or_default`, or a fallback arm. Two are real bugs.

## P0

### 1. A permanent failure on a sync request re-reconciles with no backoff

Principle: explicit policy; no implicit control flow.

- `crates/mailtide/src/engine.rs:1061-1066`: a `Failure::Blocking(Permanent)`
  on `Waiting::History | Labels | Baseline | Listing` calls
  `begin_reconcile(Requested)`. That commits a lifecycle change, and on
  `CommitApplied` → `AfterCommit::Reconcile` → `continue_reconcile` issues
  `ListLabels` again (engine.rs:580-585). No timer, no `sync_failures`
  increment, no `reconcile_not_before`.
- `crates/mailtide-gmail/src/errors.rs:96`: every unrecognised non-2xx,
  including 403 `insufficientPermissions`, `accessNotConfigured` and
  `forbidden`, classifies as `Blocking::Permanent`.

Together: a mailbox whose delegation lacks a scope, or a project with the Gmail
API disabled, makes `labels.list` return 403 forever. The engine then loops
`ListLabels → 403 → commit Reconciling → applied → ListLabels …` as fast as
the store commits, for the whole run budget, on every run. The simulator never
injects blocking failures, so it can't see this. A 400 on `history.list`
produces the same loop through a full reconciliation each time.

Proposed change. Two parts, both small:

```rust
// errors.rs: name the account-level 403s instead of falling through.
403 if has("insufficientPermissions") || has("accessNotConfigured") || has("forbidden")
    => blocking(Blocking::Account(AccountProblem::Forbidden)),

// engine.rs on_failed: a request the engine built itself that Gmail refuses
// for good will be refused next time too. Make that an operator-visible stop.
Waiting::History | Waiting::Labels | Waiting::Baseline | Waiting::Listing => {
    self.pass = None;
    match failure {
        Failure::NotFound => self.begin_reconcile(ReconcileReason::Requested, now),
        Failure::Blocking(blocking) => self.block(blocking.clone(), now),
        Failure::Retryable(_) => unreachable_by_construction, // see P2.3
    }
}
```

Why better: the policy "what does a permanent failure on sync mean" becomes a
visible arm instead of falling into the "start over" arm, and the account
reaches `Blocked` with a reason the host can show.

### 2. The Python change feed's declared type is wrong for `Labels`

Principle: parse/validate at the boundary; make the declared type true.

- `crates/mailtide/src/revision.rs:67-76`: `Change` is an externally-tagged
  serde enum. `Change::Message(id)` serializes as `{"Message": "m1"}`
  (`MessageId` is a newtype, so a string, not an object), and the unit variant
  `Change::Labels` serializes as the bare string `"Labels"`.
- `crates/mailtide-py/src/lib.rs:479-497` runs that JSON through `json.loads`
  and returns it as-is; `lib.rs:159` documents `{"Message": {...}}`;
  `python/mailtide/_core.pyi:35` declares `changes: list[dict[str, Any]]`.

A host that writes `change["Message"]` crashes with `TypeError: string
indices must be integers` the first time labels are refreshed, which happens
on every reconciliation (`LabelOp::ReplaceAll` logs `Change::Labels`,
mirror.rs:713-716). The stub and docstring both promise a shape that is false
for one variant and misleading for the others.

Proposed change: one frozen pyclass per variant, so Python can `match` with
`assert_never`:

```python
@final
class MessageChanged:  id: str
@final
class MessageDeleted:  id: str
@final
class LabelsChanged:   ...
Change = MessageChanged | MessageDeleted | LabelsChanged
```

and in Rust an exhaustive `match change { Change::Message(id) => …, Change::MessageDeleted(id) => …, Change::Thread(_) => …, Change::Labels => … }` building those objects. If JSON must stay, use `#[serde(tag = "kind", content = "id")]` so every variant is a dict, and fix the stub. Also drop `Change::Thread` (never emitted, see P2.1) rather than exporting it.

## P1

### 3. Undecodable responses are retried as transient forever

Principle: explicit policy; SPEC §6.4 says malformed payloads are permanent.

`crates/mailtide-io/src/runner.rs:482` maps every `DecodeError` to
`Retryable::Transient`. For an import job that means a 60-unit fetch every
backoff interval (capped at a minute) for as long as the message exists, with
no operator visibility. `DecodeError` already distinguishes
`InvalidJson | MissingField | InvalidField`; none of those heals by waiting.

Proposed: `Outcome::Failed(Failure::Blocking(BlockingFailure { class:
Blocking::Permanent, detail }))`. For a job that yields `JobOutcome::Block`
(visible in the mirror); for a sync request it reaches finding 1's arm. If
"our decoder is wrong, not Gmail" deserves its own class, add
`Blocking::Malformed` so hosts can tell the two apart.

### 4. A rejected job claim is an `Err`, and the two hosts disagree about it

Principle: rejections are outcomes, not exceptions; make the engine's
expectation explicit.

- `crates/mailtide-store/src/lib.rs:61-68` returns
  `Err(StoreError::Rejected(StaleEpoch))` from `claim_jobs`, although the trait
  doc for `apply` says rejections are outcomes.
- `crates/mailtide-io/src/runner.rs:321`: `Err(Rejected(_)) => Ok(None)` —
  no event. The engine keeps `claiming = true` (engine.rs:704) and a
  `Waiting::Claim` entry forever; every later `claim_jobs` returns early. The
  engine only learns it is superseded if it happens to send a commit.
- `crates/mailtide-sim/src/lib.rs:905-909`: `.unwrap_or_default()` delivers an
  empty `JobsClaimed`, which clears `claiming`. Different behaviour from the
  runner for the same store answer.

Proposed:

```rust
// Store
fn claim_jobs(...) -> impl Future<Output = Result<ClaimOutcome, StoreError>>;
pub enum ClaimOutcome { Claimed(Vec<ClaimedJob>), Rejected(Rejection) }
// Event
EventKind::JobsClaimRejected { command: CommandId, reason: Rejection }
// Engine: on_claim_rejected → self.stopped = Some(Stopped::Superseded(reason))
```

Why better: the contract is written once in the core; both hosts become
forwarders; a superseded engine stops at the first signal instead of idling.

### 5. `Store::admit → u32` throws away why targets were skipped

Principle: explicit outcomes; `AdmissionError` exists but is swallowed.

`crates/mailtide/src/mirror.rs:544-546`: `let Ok(changes) = outbox::expand(..)
else { return Vec::new() }` discards `DraftTarget`, `ReadOnlyLabel`,
`ConflictingLocations`. `apply_intent` (mirror.rs:923-928) `continue`s on
unknown ids and on "nothing new". The trait doc (`store/src/lib.rs:79-80`)
lists four meanings of `0`. Python surfaces it as `admitted: int`.

Proposed:

```rust
pub struct Admission {
    pub admitted: Vec<MessageId>,
    pub skipped: Vec<(MessageId, Skipped)>,
}
pub enum Skipped { Unknown, Refused(AdmissionError), AlreadyPending }
```

returned by `AccountMirror::admit_action` and `Store::admit`, and exported to
Python as a frozen class. A UI can then say "drafts can't be starred" instead
of silently doing nothing. `IntentOp::Request` can stay fire-and-forget inside
commits; only the host-facing admission needs the report.

### 6. Delivery types admit states the design never produces

Principle: make invalid states unrepresentable.

The design is one `Deliver(message)` job per message carrying that message's
pending changes (SPEC §7.3; `claim_jobs` builds the group from
`intents_of(id)`, mirror.rs:1159). The types say otherwise:

- `ModifyGroup.messages: Vec<MessageId>` (outbox.rs:283) allows 0..1000
  messages. `request::build` returns zero requests for an empty group and the
  runner invents a `transient("no request built")` (runner.rs:448-450). The
  sim indexes `group.messages[0]` (sim/lib.rs:974).
- `ClaimedJob.modify: Option<ModifyGroup>` (jobs.rs:86) exists for every job
  kind. `on_modified` and the Deliver-blocking arm `unwrap_or_default()` the
  versions (engine.rs:835-839, 1032-1036) for a state (`Deliver` waiting with
  `modify: None`) that `on_claimed` already finished at line 752.
- A 404 on a delivery tombstones `job.kind`'s message (engine.rs:1016-1025),
  which is only right because the group has exactly one message.

Proposed: put the payload where the kind is.

```rust
pub enum ClaimedWork {
    Import(MessageId),
    Verify(MessageId),
    BlobCleanup(MessageId),
    Deliver { message: MessageId, changes: MessageChanges }, // pending at claim
}
pub struct MessageChanges { pub add: Vec<LabelId>, pub remove: Vec<LabelId>, pub versions: Vec<Delivery> }
```

The store returns a `Deliver` claim only when something is pending (otherwise
it finishes the job itself, which `on_claimed` already does on the engine
side). `ModifyGroup` with many messages can stay as a planner output for a
future coalescing path, but nothing today should hold one.

### 7. `AttemptId(0)` stands in for "no claim" and makes any fetch a confirmation

Principle: no sentinels; I7 depends on the attempt.

`mirror.rs:946` records `IntentState::Delivered { attempt: claim.map_or(AttemptId(0), ..) }`
when an `IntentOp::Delivered` arrives in a commit without a claim; `settle()`
(mirror.rs:889) does the same for the confirming side. `settles()` then
compares `confirmation > AttemptId(0)`, so a delivery recorded without a claim
is settled by any verify at all, including one claimed before the delivery.
The engine always sets both (`finish_job_with`), but the `Commit` type lets
`claim` and `JobOp::Finish { claim }` disagree, and conformance's `actions`
case is the only thing enforcing the pairing.

Proposed: `IntentOp::Delivered { versions, at, by: AttemptId }` filled from the
job claim at the engine, and `settle` taking `Option<AttemptId>` where `None`
means "cannot settle". Consider deriving `Commit.claim` from the single
`JobOp::Finish` instead of storing it twice.

## P2

1. **Dead variants** that readers and hosts must still handle:
   `IntentState::Unresolved` (outbox.rs:76), `ReconcileStep::Complete`
   (reconcile.rs:67), `ReconcileReason::HistoryRewound` (reconcile.rs:41),
   `Change::Thread` (revision.rs:73). None is constructed anywhere.
   `JobOutcome::Retry.failure` (jobs.rs:125-131) is carried into the commit and
   discarded by `finish` (mirror.rs:519-523), while SPEC §4 promises a last
   error per job. `AccessToken.expires_at` (auth.rs:16) is demanded from every
   Python token source and never read. Remove, or wire them in.
2. **`HistoryId` means two things.** `decode.rs:203` sets
   `MessageObservation.checkpoint` from the message's own `historyId` (its last
   revision), while SPEC §6.5 and `DeletionEvidence::VerifiedMissing` treat
   checkpoints as mailbox positions. `finish_reconcile` (mirror.rs:468-492)
   compares a revision with the baseline position, so every message imported
   during a reconciliation whose revision predates the baseline gets one extra
   verification fetch, though the import observed it after the baseline.
   Correctness holds (I traced the floor logic: a fresh fetch always has
   revision ≥ any floor derived from earlier records). Cheap fix: rename the
   field `revision` and fix the SPEC text. Costlier fix, only if reconciliation
   quota shows up in practice: carry `observed_after: Option<HistoryId>` (the
   baseline or cursor at issue time) on `Observe`/`MarkImported` and use
   `max(revision, observed_after)` in `finish_reconcile`.
3. **Match shape.** `engine.rs:997-1069` matches `failure` first, then
   `waiting`, leaving an unreachable `(Failure::Retryable(_), _)` arm at 1051
   and the fallthrough that causes finding 1. One nested `match (waiting,
   failure)` with no wildcard reads better and would have exposed it.
   `mirror.rs:887` `_ => return` in `settle` hides any new `MessageOp` variant
   from intent settlement; spell the arms out.
4. **Sentinels.** `HistoryId(0)` in `listed` (mirror.rs:440),
   `finish_reconcile` (486-487) and `on_not_found` when the cursor is `None`
   (engine.rs:888). The tombstone checkpoint is write-only: every consumer
   checks `tombstone.is_some()` and never compares it. Either drop the value
   or use it. Not worth much on its own; do it when touching finding 6.
5. **SQL boundary clamps instead of failing.** `sql.rs:91-97` turns a negative
   epoch/cursor/checkpoint into 0 and an oversized `engine_version` into 0
   (`load_account` line 292) rather than `StoreError::Corrupt`. `connect`
   (sql.rs:167-171) treats any non-`sqlite:` URL as Postgres; parse the scheme
   into `Dialect` and reject the rest. The claim-ordering rule (`ORDER BY`,
   sql.rs:707-710) and the release-running-jobs rule (639-646) exist twice,
   once in SQL and once in `pick_next`/`AccountMirror::claim`. Acceptable for
   performance; add a conformance case that pins the order so they can't drift.
6. **Watch disabled is expressed as a quota deferral.** `runner.rs:362-371`
   answers `Watch` with `RequestDeferred { until: now + 24h }` and passes
   `watch_topic: String::new()` to the request builder. Make it engine policy:
   `EngineConfig { watch: WatchPolicy::Topic(..) | WatchPolicy::Disabled }` so
   the engine never issues `Watch`, and `RequestConfig.watch_topic` becomes
   required and non-empty.
7. **Python flattening.** `MessageView` (py/lib.rs:168-191) spreads
   `Option<StoredContent>` over `imported: bool` plus seven optionals, so
   `imported=True, size=None` is representable; nest a `Content | None`.
   `Status.blocked` and `RunOutcome.blocked` are `"class: detail"` strings;
   export the class as a `Literal`. `act()` accepts `add_labels`,
   `remove_labels` and `destination` for every action and ignores them when
   irrelevant (`parse_action`, lib.rs:226-260); reject them with `ValueError`
   or take a typed action object.
8. **Admission gaps.** `outbox::expand` (outbox.rs:215-246) lets the same label
   appear in `add` and `remove`; `admit` then creates two intents with the same
   version and the map keeps whichever came last. Add
   `AdmissionError::Conflicting(LabelId)`. `decode.rs:120` turns a missing
   `threadId` into `ThreadId("")`; make it a `MissingField`.
9. **Engine-side admission is fenced.** `on_action` (engine.rs:822-829) commits
   `IntentOp::Request` under the epoch guard, so a sans-IO host using
   `ActionRequested` loses the action when superseded, contrary to SPEC §7.1.
   Document it or route sans-IO hosts to the store too.
10. **Import counters live in the engine** (engine.rs:167-168) and reset per
    run, so `Status.import` under the Python `Runner` only ever shows one
    run's worth. Derive from the store or drop from status until phase 3.
11. Prior review items still present but validated at runtime, fine to leave:
    `SyncState::Blocked { resume: Box<SyncState> }` nesting (state.rs:25-30,
    rejected by `validate`), `Enumerating { labels_left }` empty (reconcile.rs:53,
    handled by `listing() == None`).

## Already right; keep

- `SyncState` as one sum type with `StoreSnapshot::validate → ValidSnapshot`
  at the store→engine boundary (state.rs:17-134), and `Engine::new` accepting
  only the validated type.
- `Action` with `ReadState`, `StarState`, `Importance`, `RestoreDestination`
  instead of booleans (outbox.rs:103-165); `expand` returning `AdmissionError`.
- `Failure::{Retryable, NotFound, Blocking}` with typed classes, built once in
  `errors::classify`; `Admission::{Granted, Deferred}` and `JobOutcome::Defer`
  keeping quota deferral out of failure counts; `TransportError::{NotSent,
  OutcomeUnknown}`; `StopReason` as a value, not an error (runner.rs:81-98).
- `CommitOutcome::Rejected(Rejection)` as a value and `check_guards` as one
  pure function (mirror.rs:167-189).
- The mirror decision functions (`observe`, `patch_labels`, `mark_imported`,
  `tombstone`, `listed`, `finish_reconcile`) returning `Decision` values, with
  the SQL store loading, applying the same rules, and diffing (sql.rs:663-685).
  This is the functional-core/imperative-shell split done properly.
- `CursorUpdate` replacing an `is_last` flag; `fold_page` pure (history.rs).
- `Ledger` with time passed in (quota.rs:139-272).
- Exhaustive matches with no wildcard across `Waiting`, `AfterCommit`,
  `CommandKind`, `EventKind`, `TimerKind` in the engine, runner and sim.
- Wire structs separate from domain types in `decode.rs`; `BatchPlan::new`
  validating before encoding; `Probability` newtype in the sim; `Literal`
  types for actions and stop reasons in Python.

## What I would do first

Fix 1 and 2 now (both are small). Then 4 and 6 together, since both are about
the engine's expectations of the store being written in the types. 3, 5 and 7
are each an afternoon. The P2 items are cleanup to fold into whichever of those
touches the same file.
