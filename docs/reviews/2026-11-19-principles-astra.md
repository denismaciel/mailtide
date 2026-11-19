# Design principles review for mailtide

Reviewed the frozen source at `b4273ee`. A crash between blob storage and import acknowledgement can defeat cleanup. A concurrent takeover can also make a runner report a block the store rejected. Both fixes belong in the transitions that decide what the host does next. Paths and line numbers below refer to the snapshot. This was a source review. No builds or tests were run.

## P0 · Real bugs or reachable invalid states

### 1. A crash can leave a deleted message’s raw content behind

**Principle — represent uncertainty about an external effect.** The runner writes the raw object before reporting the import. Consider a successful write followed by a process exit before the message commit. The durable row still says `Pending`. On restart, both stores turn the running import back into a ready job. If deletion history arrives before that job is reclaimed, the tombstone cancels the import and sees neither imported content nor a running job. It creates no cleanup job. The object survives its retention period, even if every later operation succeeds. See `crates/mailtide-io/src/runner.rs:494`, `crates/mailtide/src/mirror.rs:658`, `crates/mailtide-store/src/sql.rs:639`, and `crates/mailtide/src/mirror.rs:413`.

The smallest fix for this sequence is to enqueue delayed cleanup for every newly tombstoned message. The mirror knows the message ID and the driver can derive the key. The blob contract already makes deleting a missing key a success (`crates/mailtide-io/src/blobs.rs:29`). This costs a delete for some messages whose import never ran. It removes the need to infer object existence from current job ownership. Have the simulator flag tombstoned blobs without a cleanup job and check that draining cleanup after the grace period removes them. Its convergence check currently inspects blobs for live Gmail messages and explicitly exempts cleanup jobs (`crates/mailtide-sim/src/lib.rs:1023`, `:1043`).

### 2. A superseded runner can report idle or a durable block

**Principle — carry expected outcomes through the shell.** SQL explicitly rejects a stale epoch during a job claim. The runner maps that rejection to `Ok(None)` after the engine has set `claiming = true` and registered an outstanding command. On a quiet mailbox the run can drain its queue and report `Idle`. The simulator turns the same rejection into a successful empty claim. A second race affects blocking. The engine sets `Stopped::Blocked` before emitting the commit that persists the block. If another runner takes over first, the stopped runner flushes that commit, discards its rejection event, and returns the original blocked status. A scheduler could then stop scheduling an account that the store still considers live. See `crates/mailtide-store/src/sql.rs:700`, `crates/mailtide-io/src/runner.rs:321`, `crates/mailtide/src/engine.rs:704`, `crates/mailtide-sim/src/lib.rs:904`, `crates/mailtide/src/engine.rs:1153`, and `crates/mailtide-io/src/runner.rs:218` and `:280`.

Give job claims a domain outcome such as `Claimed(batch) | Superseded`, inside the infrastructure `Result`. Deliver that outcome in both drivers. Model a newly requested block as `Blocking { commit_id, reason }` until that specific commit is acknowledged. In that state, stop issuing work but accept the commit result. Applied confirms the block, a fencing rejection yields superseded, and an unknown write outcome goes through the existing store error path. An engine loaded from an already blocked snapshot can still stop immediately.

### 3. An import for A can complete successfully with B’s message

**Principle — validate identity at ingress and correlate typed results.** Both message response branches discard the requested ID before decoding. The raw import then selects the blob key from the response body. A well-formed body naming B in response to a request for A writes B’s key, produces `MarkImported(B)`, and finishes A’s claim. A can be left pending without an import job. The guard checks ownership of A’s job, which is valid throughout. The public sans-IO API also accepts, for example, `BlobDeleted` on an outstanding import because `take_job` accepts any job. See `crates/mailtide-io/src/runner.rs:465` and `:494`, `crates/mailtide/src/engine.rs:776`, `:808`, `:850`, and `:894`. The existing state checker names the resulting violation `MissingImportJob` (`crates/mailtide/src/invariants.rs:125`).

Pass the expected message ID into the wire decoder and return an identity-mismatch error before writing a blob. For the engine, inspect the pending entry and validate its job kind and target before consuming it. Derive the target of a missing-message result from that entry. A mismatched event should leave the legitimate result eligible to complete its job. A small exhaustive match over pending work and accepted replies addresses this without adding generic type parameters to the entire event protocol.

### 4. SQL silently changes cursor values

**Principle — perform lossless conversion at the storage boundary.** The wire layer accepts a full `u64` history ID. Both SQL backends use signed `BIGINT` columns. The write helper saturates large values, and the read helper turns negatives into zero. A cursor of `2^63`, for example, is accepted, stored as `2^63 − 1`, and acknowledged as applied. The engine adopts the original value on acknowledgement. A subsequent guarded advance can be rejected for cursor mismatch, and reopening the store resumes from a different position. Memory retains the original. Separately, a malformed persisted engine version is treated as version zero before the version fence is checked. See `crates/mailtide-gmail/src/decode.rs:42`, `crates/mailtide-store/src/sql.rs:48`, `:91`, `:292`, `:325`, and `:633`, and `crates/mailtide/src/engine.rs:487`.

Use checked, field-specific conversions and fail the transaction on an unrepresentable write. Report invalid stored numbers as `StoreError::Corrupt` with the column name. Supporting the documented unsigned history domain end to end requires an order-preserving encoding and migration for cursors, checkpoints, and baseline comparisons. Until then an explicit range error preserves the last good state. Keep the intentional bit-preserving casts for the RNG seed and attempt IDs distinct from numeric conversions.

### 5. The Python change-feed stub promises dictionaries but emits a string

**Principle — preserve the cases of a closed sum type across an API boundary.** Rust’s `Change::Labels` is a unit variant. The binding serializes each change with Serde and calls Python’s `json.loads`, so a label refresh produces the Python string `"Labels"`. Message changes produce single-key dictionaries. The stub declares `list[dict[str, Any]]`, so an annotated consumer can call `.items()` on every entry and fail during bootstrap. Label replacement actually appends this variant. See `crates/mailtide/src/revision.rs:67`, `crates/mailtide/src/mirror.rs:713`, `crates/mailtide-py/src/lib.rs:479`, and `python/mailtide/_core.pyi:34`.

Preserve the published runtime shapes. Type each keyed case with a one-key `TypedDict` and include `Literal["Labels"]` in their union. Construct those values with an exhaustive Rust match in the binding. That makes a new variant a compile-time review point and removes the JSON round trip. A Python consumer can narrow the union by literal and key, with `typing.assert_never` on the remaining case. Include a label refresh alongside a message deletion when checking that contract.

### 6. Token acquisition can supply false evidence of message deletion

**Principle — limit each error type to facts its producer can know.** The Rust `TokenSource` trait returns the same `Failure` as a Gmail request. A host token provider can return `Failure::NotFound` for a missing credential. The runner forwards it before sending any Gmail request. During an import, the engine then treats it as evidence that the message itself was missing, tombstones it, and cancels its jobs. During a history pass the same error starts reconciliation. The built-in token sources use narrower outcomes in practice, but the public type admits both sequences. See `crates/mailtide-io/src/auth.rs:28`, `crates/mailtide-io/src/runner.rs:415`, and `crates/mailtide/src/engine.rs:1017` and `:1053`.

Give token providers a `TokenError` with transient and account-problem cases. At the HTTP boundary, interpret an actual Gmail 404 together with the request that produced it. Forward a message-missing or history-expired event from that evidence. Keep retry timing, account blocking, and tombstoning in the core. That also removes the need for two layers to infer the meaning of a bare “not found.”

### 7. Enforce the lifecycle invariant when writing a commit

**Principle — make invalid transitions unrepresentable.** `Commit` exposes independent optional lifecycle and cursor updates. On a newly claimed account, a caller can submit a correctly fenced commit containing `sync = Some(Live)` and no cursor move. The mirror applies it and SQL persists the result. Validation catches the contradiction when building the next engine. Normal reconciliation constructs the pair correctly, so the exposed store contract is the concern here. See `crates/mailtide/src/commit.rs:36`, `crates/mailtide/src/mirror.rs:690` and `:705`, `crates/mailtide/src/state.rs:122`, and `crates/mailtide/src/engine.rs:689`.

Give lifecycle-changing commits named transitions, including `FinishReconciliation { expected_cursor, baseline, to }`, which couples the guarded advance with entry into live sync. A smaller first change is to validate the proposed account state before publishing the mirror or writing its SQL diff and return a distinct invalid-transition result. Keep snapshot validation for old rows and third-party stores. Both checks can use the same pure invariant.

## P1 · Significant design improvements

### 8. Tell the scheduler when durable work becomes eligible

**Principle — give “nothing ready now” a precise type.** A successful claim returns only a vector of jobs. An empty vector covers both an empty queue and a queue of future work. For example, acknowledging a delivery enqueues verification for `confirm_due_at`. Its immediate follow-up claim can return empty. The engine then emits only status, and the runner computes `Idle.next` from its in-memory timers. A host using a short confirmation timeout can be told to wake at the later poll, leaving the action overlay visible beyond the requested delay. Restarting the runner also discards its prior timers. See `crates/mailtide/src/mirror.rs:951`, `crates/mailtide/src/engine.rs:770`, `:926`, and `:1265`, `crates/mailtide-store/src/sql.rs:707`, and `crates/mailtide-io/src/runner.rs:252`.

Include the earliest remaining ready-job deadline in the successful claim batch. Memory can derive it from job states and SQL can query the minimum ready deadline while holding the account transaction. The engine can then schedule its existing jobs timer at the earlier known deadline. Keep blocked and running jobs out of that calculation and preserve the existing handling of due work at capacity. The same result supports delayed drafts and cleanup across restarts.

### 9. Separate invalid label changes from per-message admission decisions

**Principle — parse policy once and preserve meaningful results.** The Python API permits `change_labels` with the same label in both lists. Expansion emits an add followed by a remove. On a message with no prior intent for that label, both get the same next version, and the second insertion wins. Supplying a read-only label has a different failure mode. Expansion produces a specific `AdmissionError`, which the mirror converts to an empty list. The host sees the same zero count it would receive for an unknown target or an identical pending action. See `python/mailtide/_runner.py:84`, `crates/mailtide-py/src/lib.rs:254`, `crates/mailtide/src/outbox.rs:215`, and `crates/mailtide/src/mirror.rs:542` and `:930`.

Construct a validated `LabelDelta` from the two collections. Check overlap, read-only labels and mutually exclusive added locations once, including when the account has no targets. Use the same constructor for Rust callers and translate its input errors to Python argument errors. Keep draft eligibility and deduplication in the pure per-message decision, where the effective labels and existing intents are available. This preserves the documented count for ordinary skips and gives callers an actionable reason to correct an invalid request.

## P2 · Polish

**Make disabled watches a scheduling policy.** Every engine schedules a renewal on start. With watches disabled, the runner converts the resulting watch request into a one-day deferral, and the engine schedules it again. This works, but the timer now describes work the host never intends to perform. Put watch enablement in the scheduling configuration and retain the topic in the request-building shell. Poll-only hosts would then produce only relevant deadlines. This is lower priority than reporting durable job deadlines. See `crates/mailtide/src/engine.rs:217`, `:327`, and `:992`, and `crates/mailtide-io/src/runner.rs:362`.

## Preserve

- The separation between pure mirror decisions and transactional persistence already pays off. Memory and SQL run the same guarded transition, and SQL writes its effects in one transaction. Implement the cleanup and lifecycle rules there once. See `crates/mailtide-store/src/memory.rs:69` and `crates/mailtide-store/src/sql.rs:663`.
- History folding explicitly distinguishes an intermediate empty page from the final page. Keep this decision in the core and retain the simulator’s per-commit invariant checks and seeded replay. See `crates/mailtide/src/history.rs:48` and `crates/mailtide-sim/src/lib.rs:866`.
- Optional MIME extraction already has a clean byte-input boundary and a typed result. The async Python wrapper likewise delegates sync work while supplying native coroutine semantics. Neither layer needs its own mailbox policy or a broader redesign. See `crates/mailtide-mime/src/lib.rs:39` and `python/mailtide/_runner.py:77`.
