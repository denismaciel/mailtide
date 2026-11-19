The crate split is sound. The highest-impact improvements are to encode **durable state transitions, causal evidence, and related commit conditions** in the types. Renaming booleans helps less than preventing callers from constructing contradictory inputs.

I reviewed all crates and Python declarations, treating intentional `todo!()` bodies as out of scope. The checkout changed during review; the findings below incorporate the newer `PagePosition`, named `Delivery`, and unknown-message commit results. No files were edited or tests run. Code sketches show proposed contracts; supporting types are abbreviated.

1. **Related commit effects need one condition covering the entire transition.**

   Locations: [commit.rs:21](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/commit.rs:21), [jobs.rs:57](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/jobs.rs:57), [event.rs:100](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/event.rs:100).

   `Commit` separates message, job, and intent operations. Transactional execution does not guarantee that their *conditions* agree. For example, `JobOp::Complete` can reject an old attempt while the separate `MessageOp::MarkImported` has no attempt fence. Likewise, discovery can be skipped independently of its job enqueue.

   The new `unknown: Vec<MessageId>` result is useful, but returning an unknown ID after advancing the cursor does not itself durably record the obligation to fetch it. A crash before processing that result needs a persisted recovery path.

   Prefer aggregate operations whose guards cover every related effect:

   ```rust
   pub enum CommitOp {
       DiscoverAndSchedule {
           observation: MessageObservation,
           import: NewImportJob,
       },
       CompleteImport {
           claim: JobClaim,
           imported: ImportedMessage,
       },
       ApplyHistoryRecord(HistoryMutation),
       DeleteMessage {
           evidence: DeletionEvidence,
           cleanup: CleanupPolicy,
       },
   }

   pub enum OperationOutcome {
       Applied,
       Skipped(SkipReason),
       JobClaimed(JobClaim),
   }
   ```

   `CompleteImport` checks the attempt before changing either the message or job. `ApplyHistoryRecord` durably schedules hydration when its target is unknown, in the transaction advancing the cursor. Return typed operation receipts where subsequent work depends on whether a condition succeeded.

   Keep the transition rules in pure core functions called by each store inside its transaction. This preserves the bounded engine without making each database implementation invent business semantics.

2. **The account lifecycle is a product of fields that should be a sum type.**

   Locations: [state.rs:16](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/state.rs:16), [commit.rs:30](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/commit.rs:30), [engine.rs:70](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/engine.rs:70).

   `phase`, `cursor`, and `reconcile` can contradict one another: live without a cursor, reconciling without progress, or live with active reconciliation. `Blocked` carries neither a reason nor the state to resume.

   The write contract also cannot explicitly clear reconciliation or persist several snapshot fields, including watch expiration and cooldown. These are contract gaps, independent of implementation bodies.

   ```rust
   pub enum ActiveSync {
       Bootstrap(BootstrapProgress),
       Following(CursorCheckpoint),
       Reconciling(ReconcileSession),
   }

   pub enum SyncState {
       Active(ActiveSync),
       Blocked {
           reason: BlockingFailure,
           resume: ActiveSync,
       },
   }

   pub struct AccountTransition {
       pub expected: AccountRevision,
       pub next: AccountState,
   }

   pub struct StoreSnapshot {
       pub writer: WriterFence,
       pub revision: AccountRevision,
       pub state: AccountState,
   }
   ```

   `AccountState` should include durable watch, scheduling, notification, and retry state. A typed account transition, committed with related message changes, makes entering, completing, and resuming reconciliation explicit.

   Validate persisted rows when constructing `StoreSnapshot`. `Engine::new` can remain infallible if it accepts only a validated snapshot and configuration.

3. **Intent confirmation needs causal evidence, not just matching labels.**

   Locations: [outbox.rs:33](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/outbox.rs:33), [outbox.rs:61](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/outbox.rs:61), [outbox.rs:312](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/outbox.rs:312).

   `confirms(intent, observed_labels)` cannot establish that an observation is fresh enough. It receives no observation checkpoint, source, or relationship to the delivery. `base_checkpoint` already exists, but the signature cannot compare it with the observation.

   `Unresolved` carries no delivery identity. Replacing an intent with a newer desire also needs to retain any older unresolved delivery that still prevents safe dispatch. `IntentOp::Admit(Intent)` permits admitting an already-delivered or blocked value.

   ```rust
   pub struct IntentSlot {
       pub desired: DesiredIntent,
       pub delivery: DeliveryState,
   }

   pub enum DeliveryState {
       Ready,
       Sending(DeliveryAttempt),
       AwaitingConfirmation(DeliveryReceipt),
       OutcomeUnknown(DeliveryAttempt),
       Blocked(BlockingFailure),
   }

   pub enum ObservationEvidence {
       History { record: HistoryId },
       Verification {
           after_delivery: DeliveryId,
           request: CommandKey,
           checkpoint: HistoryId,
       },
   }

   pub fn reconcile_intent(
       intent: &IntentSlot,
       observation: &LabelObservation,
   ) -> IntentDecision;
   ```

   `LabelObservation` contains message identity, labels, and evidence. `IntentDecision` distinguishes awaiting evidence, confirming a particular delivery/version, and accepting a later external change.

   Admission should take `NewIntent` or an action admission request, not arbitrary persisted `Intent`. If unknown targets are supported, represent awaiting target resolution explicitly instead of silently admitting an intent with an unexplained missing base checkpoint.

4. **Command and timer results need identities that survive engine replacement.**

   Locations: [ids.rs:89](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/ids.rs:89), [engine.rs:49](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/engine.rs:49), [event.rs:34](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/event.rs:34).

   Command IDs are unique only within an epoch, but result events carry just the numeric ID. A new engine resets its counter. A delayed result can therefore collide unless the shell supplies an undocumented routing guarantee. Commit epoch fencing does not protect a new engine that mistakenly interprets an old result and emits a fresh commit.

   `InFlight::History | Other` also cannot retain the context needed for verification, job attempts, delivery versions, or pagination. Timer replacement has the same identity problem: `TimerFired` names only its kind.

   ```rust
   pub struct CommandKey {
       pub account: AccountId,
       pub epoch: Epoch,
       pub sequence: CommandSequence,
   }

   pub struct TimerTicket {
       pub writer: WriterFence,
       pub kind: TimerKind,
       pub generation: TimerGeneration,
   }

   pub enum PendingRequest {
       History(HistoryReadContext),
       Verify(VerificationContext),
       Import(JobClaim),
       Modify(DeliveryAttempt),
       Commit(PendingTransition),
   }

   pub fn handle(&mut self, event: Event)
       -> Result<Transition, ProtocolError>;
   ```

   Stale or duplicate results should produce an explicit ignored disposition; mismatched result kinds should produce a protocol error. Keep account/epoch checks at event ingress.

5. **The core needs a complete protocol for obtaining facts and requesting effects.**

   Locations: [command.rs:22](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/command.rs:22), [event.rs:27](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/event.rs:27), [mailtide-store/src/lib.rs:42](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-store/src/lib.rs:42), [runner.rs:86](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-io/src/runner.rs:86).

   The engine deliberately holds no mirror, jobs, or intents. However, its command/event protocol has no bounded store-read exchange for those facts. `Store` exposes due jobs, but there is no corresponding event. There are also no reads for action targets, deliverable intents, or reconciliation absences.

   This leaves the runner either unable to supply the core or responsible for deciding which jobs, intents, and targets to process. Blob cleanup is similarly a job kind without an executable core command.

   ```rust
   pub enum StoreQuery {
       DueJobs(JobQuery),
       ActionTargets(TargetQuery),
       DeliverableIntents(IntentQuery),
       UnseenMessages(ReconcileQuery),
       AccountStatistics,
   }

   pub enum Effect {
       ReadStore(StoreQuery),
       Gmail(GmailCall),
       Commit(Commit),
       DeleteBlob { claim: JobClaim, blob: BlobRef },
       Schedule(TimerTicket, Timestamp),
   }

   pub enum EffectResult {
       StoreRead(StoreReadResult),
       Gmail(GmailResult),
       Commit(CommitReceipt),
       BlobDeleted(JobClaim),
       Failed(EffectFailure),
   }
   ```

   Carry read revisions into dependent writes, or resolve targets and run the pure admission reducer inside the transaction. Thread membership “when admitted” must not silently become membership at an earlier, unguarded read.

   A fixed fetch-and-store import pipeline can remain in the shell. Retry, cleanup timing, admission, confirmation, and scheduling decisions belong in the core.

6. **Reconciliation needs a scan identity and an explicit current listing scope.**

   Locations: [reconcile.rs:37](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/reconcile.rs:37), [reconcile.rs:84](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/reconcile.rs:84), [command.rs:67](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/command.rs:67).

   `Enumerating { labels_left, page_token }` permits a continuation token with no current label. The current label is implicitly the vector’s first element. `with_baseline` accepts any lifecycle state; `after_page` silently preserves unrelated states.

   There is also no scan identity or explicit “seen in this scan” operation. That makes absence verification and delayed page results difficult to fence. A per-label plan should explicitly include full-mailbox coverage; messages with no enumerated label otherwise have no discovery path.

   ```rust
   pub enum ListingScope {
       AllMessages,
       LabelMembers(LabelId),
   }

   pub struct ListingCursor {
       pub scope: ListingScope,
       pub page: PageStart<MessagePageToken>,
   }

   pub struct Enumeration {
       pub scan: ReconcileId,
       pub baseline: HistoryId,
       pub current: ListingCursor,
       pub remaining: Vec<ListingScope>,
   }

   pub fn apply_list_page(
       state: Enumeration,
       page: ListPage,
   ) -> EnumerationTransition;
   ```

   Commit page membership as `RecordSeen { scan, scope, messages }` before requesting the next page. Tie absence queries and verification jobs to that scan.

   Put state-specific operations on state-specific types, or return `Result<_, InvalidTransition>`. Do not silently accept an enumeration result while capturing the baseline.

7. **Message types need to distinguish identity, partial evidence, and complete observations.**

   Locations: [gmail.rs:13](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/gmail.rs:13), [gmail.rs:62](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/gmail.rs:62), [commit.rs:63](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/commit.rs:63).

   `MessageSummary` means both a history summary and a fetched message, with an optional checkpoint. That lets a supposedly imported observation lack the checkpoint needed for safe application. A label-specific listing provides partial membership evidence, which must not be mistaken for a complete label set.

   History fragments also repeat the same revision independently. With `PatchLabels` documented as accepting only a strictly newer checkpoint, multiple changes for one message at the same revision need an explicit grouping rule.

   ```rust
   pub struct MessageIdentity {
       pub message: MessageId,
       pub thread: ThreadId,
   }

   pub struct MessageObservation {
       pub identity: MessageIdentity,
       pub labels: ObservedLabels,
       pub checkpoint: HistoryId,
   }

   pub struct HistoryMutation {
       pub at: HistoryId,
       pub message: MessageId,
       pub change: MessageChange,
   }

   pub enum MessageChange {
       Added(MessageObservation),
       LabelsChanged(LabelPatch),
       Deleted,
   }
   ```

   Normalize related history fragments into one message mutation per revision before applying its checkpoint condition. Specify equal-checkpoint replay semantics once.

   Distinguish deletion evidence from an arbitrary checkpoint:

   ```rust
   pub enum DeletionEvidence {
       History { message: MessageId, at: HistoryId },
       VerifiedMissing(VerificationReceipt),
   }
   ```

   This gives a message-fetch 404 the request context it needs without pretending the response itself supplied a revision.

8. **Jobs cannot represent the blocked state their API promises.**

   Locations: [jobs.rs:31](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/jobs.rs:31), [jobs.rs:49](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/jobs.rs:49), [jobs.rs:89](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/jobs.rs:89).

   `Job` has `due_at`, failed attempts, and an optional claim, but no blocked state or claim expiry. `pick_next` promises to skip blocked work without receiving enough information to do so. `Enqueue(Job)` can enqueue a value already marked claimed.

   Failed-attempt count and claim identity are legitimately different; name and model them separately.

   ```rust
   pub struct Job {
       pub kind: JobKind,
       pub priority: Priority,
       pub failed_attempts: FailureCount,
       pub state: JobState,
   }

   pub enum JobState {
       Ready { due_at: Timestamp },
       Running(JobClaim),
       Retrying { due_at: Timestamp, failure: RetryableFailure },
       Blocked(BlockingFailure),
   }

   pub struct JobClaim {
       pub job: JobKey,
       pub attempt: AttemptId,
       pub owner: WriterFence,
       pub expires_at: Timestamp,
   }

   pub enum JobOp {
       Enqueue(NewJob),
       Finish { claim: JobClaim, outcome: JobCompletion },
   }
   ```

   The completion outcome should distinguish success, retry, block, and quota deferral. Only genuine failures increment `failed_attempts`. Claim receipts also make it clear when the runner is authorized to execute work.

9. **A logical modify group should not hide multiple HTTP requests.**

   Locations: [outbox.rs:226](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/outbox.rs:226), [quota.rs:35](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/quota.rs:35), [request.rs:10](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-gmail/src/request.rs:10).

   A group below ten messages is documented as multiple individual modifications, but `build` returns one `HttpRequest`, and `LabelsModified` acknowledges one command. The shape leaves partial success and per-request quota admission unresolved.

   `ModifyGroup` also exposes independently mutable message IDs, add/remove arrays, and delivery references. They can disagree or exceed limits. The new named `Delivery` fixes the tuple issue; retain it.

   ```rust
   pub enum ModifyRequest {
       One(ValidatedMessageModification),
       Batch(ValidatedBatchModification),
   }

   pub struct PlannedModification {
       pub request: ModifyRequest,
       pub deliveries: NonEmpty<Delivery>,
   }

   pub fn plan_delivery(
       intents: &DeliverableIntents,
   ) -> Vec<PlannedModification>;

   impl GmailCall {
       pub fn cost(&self) -> QuotaUnits;
   }
   ```

   Let the pure planner choose individual versus batch calls. Each emitted command then has one independently accountable result.

   Keep request fields private and validate nonempty targets, unique IDs, disjoint add/remove sets, label limits, and exact delivery coverage. Derive cost from the request instead of allowing the public `cost` field to contradict it.

10. **Errors need structured causes, delivery certainty, and explicit recovery decisions.**

   Locations: [retry.rs:17](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/retry.rs:17), [retry.rs:100](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/retry.rs:100), [transport.rs:11](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-io/src/transport.rs:11), [mailtide-store/src/lib.rs:39](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-store/src/lib.rs:39).

   `FailureClass` is a useful start, but `Backoff::next` accepts permanent failures and `JobOp::Block` accepts transient ones. `TransportError(String)` cannot distinguish definitely-not-sent from an uncertain outcome, which matters for intents. `StoreError(String)` cannot distinguish a rejected transaction from a commit whose acknowledgement was lost.

   ```rust
   pub enum TransportError {
       NotSent(TransportCause),
       OutcomeUnknown(TransportCause),
   }

   pub enum ApplyError {
       NotApplied(StoreFault),
       OutcomeUnknown { commit: CommitKey, cause: StoreFault },
   }

   pub enum RecoveryDecision {
       Retry { at: Timestamp },
       CoolDown(Cooldown),
       VerifyDelivery(DeliveryId),
       Block(BlockingFailure),
   }

   pub fn recovery(
       operation: &OperationContext,
       failure: &OperationFailure,
       policy: &RetryPolicy,
       now: Timestamp,
   ) -> RecoveryDecision;
   ```

   Narrow backoff to `RetryableFailure`. Handle expected 404s as operation-specific results—expired history and missing message already have distinct event variants—rather than allowing them to drift into generic retry logic.

   Replace the string-only errors in Gmail decoding, multipart batches, blobs, MIME, and runner errors with meaningful variants:

   ```rust
   pub enum DecodeError {
       InvalidJson(JsonDiagnostic),
       MissingField(FieldPath),
       InvalidField { field: FieldPath, reason: InvalidValue },
   }

   pub enum BlobError {
       Unavailable(Diagnostic),
       AccessDenied,
       Missing(BlobRef),
       Corrupt(BlobRef),
   }

   pub enum MimeError {
       MalformedStructure { offset: usize },
       ResourceLimitExceeded(ParseLimit),
   }
   ```

   Keep diagnostic text for humans. `String detail` is acceptable when no control flow depends on it; it should supplement structured causes, not substitute for them. Unknown provider error codes should remain representable at the boundary.

11. **Quota scope needs its target, and persistent quota operations must be fallible.**

   Locations: [quota.rs:111](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/quota.rs:111), [quota_gate.rs:13](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-io/src/quota_gate.rs:13), [retry.rs:63](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/retry.rs:63).

   `Ledger` accounts across accounts, but `cool_down(scope, until)` cannot identify which account to pause. The gate takes an account even for project-wide pauses, making that argument conditionally meaningful.

   The persistent gate returns bare `Admission` or `()`. Storage failure therefore has no representation distinct from quota deferral or successful persistence.

   ```rust
   pub enum QuotaTarget {
       Account(AccountId),
       Project, // Ledger/gate is scoped to one project.
   }

   pub struct Cooldown {
       pub target: QuotaTarget,
       pub until: Timestamp,
   }

   impl Ledger {
       pub fn cool_down(&mut self, cooldown: Cooldown);
       pub fn snapshot(&self) -> LedgerSnapshot;
       pub fn restore(snapshot: LedgerSnapshot) -> Result<Self, InvalidLedger>;
   }

   // QuotaGate futures:
   // admit(...)     -> Result<Admission, QuotaStoreError>
   // release(...)   -> Result<ReleaseOutcome, QuotaStoreError>
   // cool_down(...) -> Result<(), QuotaStoreError>
   ```

   This preserves the existing good distinction between denial and failure. Keep account and project cooldowns independently representable when both apply.

12. **Domain identifiers and configuration are named wrappers without enforced validity.**

   Locations: [ids.rs:11](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/ids.rs:11), [push.rs:7](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-gmail/src/push.rs:7), [decode.rs:21](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-gmail/src/decode.rs:21), [revision.rs:17](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/revision.rs:17).

   Public tuple fields let callers bypass the stated parsing boundary. `AccountId` does not enforce its documented canonical form. `PageToken` can be used for unrelated endpoints. `Jwks` is named as parsed keys but contains unchecked bytes, and a verified notification still exposes an unvalidated email string.

   ```rust
   pub struct AccountId(String);
   pub struct MessageId(String);
   pub struct ThreadId(String);
   pub struct HistoryPageToken(String);
   pub struct MessagePageToken(String);

   impl AccountId {
       pub fn parse(raw: &str) -> Result<Self, InvalidAccount>;
       pub fn as_str(&self) -> &str;
   }

   pub struct ParsedJwks { keys: Vec<VerificationKey> }

   pub struct VerifiedNotification {
       account: AccountId,
       history_id: HistoryId,
   }
   ```

   Apply the same boundary discipline to database decoding and Python inputs. Preserve IDs as opaque values; validate documented requirements without inventing restrictive provider formats.

   Parse URLs, finite durations, and policy relationships during construction. For example, validate `Backoff`’s base/cap relationship and explicitly define what zero polling or concurrency limits mean.

   Keep raw wire strings inside boundary DTOs. Free-text subjects, snippets, and header values need not become restrictive domain enums. In MIME, parse media types if downstream code reasons about them; retain malformed source values or warnings where faithful import requires that.

13. **`PagePosition` improves naming but still duplicates information already in the page.**

   Locations: [history.rs:18](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/history.rs:18), [history.rs:49](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/history.rs:49), [gmail.rs:50](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/gmail.rs:50).

   The original `is_last: bool` has been replaced, but callers can still pass `PagePosition::Last` for a page containing a continuation token. That allows an incorrect cursor advance. The new optional cursor correctly represents an empty intermediate page; retain that behavior.

   ```rust
   pub fn fold_page(page: &ValidatedHistoryPage) -> PageFold;

   pub enum CursorUpdate {
       Unchanged,
       AdvanceTo(HistoryId),
   }

   pub enum Continuation<T> {
       More(T),
       Finished,
   }

   pub struct PageFold {
       pub mutations: Vec<HistoryMutation>,
       pub cursor: CursorUpdate,
       pub continuation: Continuation<HistoryPageToken>,
   }
   ```

   Derive finality inside the fold. Validation establishes ordering and page consistency; the core decides how to react to repeated tokens. Do not ask callers to supply an independently contradictory answer.

14. **Action booleans hide user policy; label predicates generally do not.**

   Locations: [outbox.rs:96](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/outbox.rs:96), [outbox.rs:133](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/outbox.rs:133), [labels.rs:50](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/labels.rs:50).

   `Trash(false)` hides the restore destination. `Archive(false)` currently means moving to inbox and clearing other location labels. The `Trash` documentation says restoring to the previous location while expansion documents restoring to inbox: the type should settle that policy explicitly.

   ```rust
   pub enum ReadState { Read, Unread }
   pub enum StarState { Starred, Unstarred }
   pub enum Importance { Important, Normal }
   pub enum RestoreDestination { Inbox, Archive }

   pub enum Action {
       SetRead(ReadState),
       SetStar(StarState),
       SetImportance(Importance),
       Archive,
       MoveToInbox,
       MoveToTrash,
       RestoreFromTrash(RestoreDestination),
       MarkSpam,
       MarkNotSpam(RestoreDestination),
       ChangeLabels(UserLabelPatch),
   }

   pub fn expand(
       target: &ActionTarget,
       action: &Action,
   ) -> Result<LabelPatch, AdmissionError>;
   ```

   `UserLabelPatch` should validate label eligibility, duplicate changes, and add/remove conflicts. `ActionTarget` should carry identity and effective state together.

   Also distinguish `ObservedLabels` from `EffectiveLabels`; both are currently interchangeable `LabelSet`s. `effective` should accept intents validated for that message, not an arbitrary slice that could include other messages.

   **Keep `is_unread` and `is_draft` as boolean predicates.** They derive facts from one source of truth and do not hide policy. Introduce `ReadState` or a validated non-draft target where a decision requires those distinctions; do not persist redundant booleans.

15. **Workspace enums currently prevent exhaustive handling across crate boundaries.**

   Locations: [command.rs:21](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/command.rs:21), [command.rs:55](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/command.rs:55), [event.rs:26](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/event.rs:26), [mailtide-py/src/lib.rs:152](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-py/src/lib.rs:152).

   `#[non_exhaustive]` applies across crate boundaries, including other crates in this workspace. It forces the driver, wire layer, simulator, and bindings to have a fallback when they should be required to handle every operation.

   The bindings currently silently discard unrecognized command/request variants. `Engine::handle` and reconciliation’s `other => other` also hide future variant additions.

   ```rust
   // Closed workspace protocol: no #[non_exhaustive].
   pub enum CommandKind {
       Gmail(GmailCall),
       Commit(Commit),
       Schedule { timer: TimerTicket, at: Timestamp },
       ReportStatus(Status),
   }

   fn classify_command(command: &CommandKind) -> CommandClass {
       match command {
           CommandKind::Gmail(_) => CommandClass::Network,
           CommandKind::Commit(_) => CommandClass::Storage,
           CommandKind::Schedule { .. } => CommandClass::Timer,
           CommandKind::ReportStatus(_) => CommandClass::Reporting,
       }
   }
   ```

   Use explicit variants even when several share a handler. Reserve unknown-value handling for external protocol inputs. If external API compatibility later requires an extensible facade, keep the internal workspace protocol closed.

   The token’s redacted `Debug::finish_non_exhaustive()` is unrelated to enum exhaustiveness and should remain.

16. **Status and run outcomes should expose coherent states and normal stopping reasons.**

   Locations: [status.rs:9](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/status.rs:9), [status.rs:26](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/status.rs:26), [runner.rs:30](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-io/src/runner.rs:30), [runner.rs:45](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-io/src/runner.rs:45).

   Status repeats lifecycle contradictions. Independently writable counters allow imported counts inconsistent with discovery, and `discovery_complete` does not establish that remaining imports are finished.

   `RunError::Blocked` and `Superseded` are expected operating outcomes, while `SyncReport` does not say whether execution completed, exhausted its budget, or deferred. That encourages exceptions and runner-side guesses about completion.

   ```rust
   pub enum DiscoveryProgress {
       Enumerating { seen: MessageCount },
       Finished { total: MessageCount },
   }

   pub struct ImportProgress {
       discovery: DiscoveryProgress,
       counts: ImportCounts, // Validated partition of the same target set.
   }

   pub enum StopReason {
       CaughtUp,
       NoDueWork,
       Deferred { until: Timestamp },
       BudgetExhausted,
       Blocked(BlockingFailure),
       Superseded,
   }

   pub struct RunOutcome {
       pub report: SyncReport,
       pub stop: StopReason,
   }

   // Runner:
   // sync(...) -> Result<RunOutcome, InfrastructureError>
   ```

   Derive status from validated account state and a coherent statistics snapshot. Keep import progress independent of sync phase: live sync may still have outstanding backfill.

   Define “caught up” against an explicit run target. Let the core signal completion or waiting; the shell enforces operational budgets and performs effects.

17. **Python currently erases useful Rust distinctions at the public boundary.**

   Locations: [mailtide-py/src/lib.rs:48](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-py/src/lib.rs:48), [mailtide-py/src/lib.rs:74](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-py/src/lib.rs:74), [_core.pyi:10](/home/denis/github.com/denismaciel/mailtide/python/mailtide/_core.pyi:10), [_core.pyi:27](/home/denis/github.com/denismaciel/mailtide/python/mailtide/_core.pyi:27).

   `phase: str`, independently optional counters, and a scope-less cooldown timestamp discard domain structure. The token callback’s tuple hides names and timestamp units, and offers no typed expected failure.

   ```python
   from dataclasses import dataclass, field
   from typing import Protocol, assert_never

   @dataclass(frozen=True)
   class AccessToken:
       secret: str = field(repr=False)
       expires_at: Timestamp

   TokenResult = AccessToken | TokenUnavailable

   class TokenSource(Protocol):
       def __call__(self, account: AccountId, /) -> TokenResult: ...

   SyncState = Bootstrap | Reconciling | Live | Blocked

   def state_label(state: SyncState) -> str:
       match state:
           case Bootstrap():
               return "Importing"
           case Reconciling():
               return "Reconciling"
           case Live():
               return "Live"
           case Blocked():
               return "Blocked"
       assert_never(state)
   ```

   Export immutable typed values reflecting Rust’s states and `RunOutcome`. Keep expected blocking, quota deferral, and token unavailability as values; reserve exceptions for invalid caller input and infrastructure/programming failures.

   Validate account strings, URL configuration, integer ranges, callback results, and finite nonnegative `budget_s` in PyO3. Stubs alone do not establish runtime validity.

   `commands_for_push` is explicitly a smoke helper. Keep it test-only or return typed commands; its string tuples and filtering should not become the production conversion contract.

18. **Multipart batch APIs need validated membership and response correlation.**

   Locations: [batch.rs:20](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-gmail/src/batch.rs:20), [batch.rs:29](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-gmail/src/batch.rs:29).

   `encode(&[HttpRequest])` accepts empty or oversized batches and raw imports despite the documented exclusions. `decode` promises request-order results without receiving the expected request manifest, so it cannot establish complete one-to-one correspondence.

   ```rust
   pub struct BatchPlan {
       parts: BoundedNonEmpty<BatchPart, 50>,
   }

   pub struct BatchPart {
       pub id: PartId,
       pub request: BatchableRequest,
   }

   pub struct PartResponse {
       pub id: PartId,
       pub response: HttpResponse,
   }

   pub fn encode(plan: &BatchPlan) -> EncodedBatch;

   pub fn decode(
       expected: &BatchPlan,
       response: &HttpResponse,
   ) -> Result<Vec<PartResponse>, BatchError>;
   ```

   Give `BatchError` explicit missing, duplicate, unexpected-part, and malformed-payload cases. The core chooses batching policy; the wire layer validates and encodes that plan.

   Ordinary HTTP header/query name-value tuples are reasonable boundary representations. Delivery identities and correlated responses deserve named types because they carry stronger semantics.

19. **Invariant and conformance interfaces do not expose enough evidence to support their promises.**

   Locations: [invariants.rs:7](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/invariants.rs:7), [invariants.rs:44](/home/denis/github.com/denismaciel/mailtide/crates/mailtide/src/invariants.rs:44), [conformance.rs:19](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-store/src/conformance.rs:19).

   `MirrorView` exposes only the cursor. It cannot support checks about tombstones, message checkpoints, jobs, intents, or blobs. `Violation` variants identify the invariant but put all evidence in strings.

   The conformance factory returns `S`, although real store construction is fallible. Setup failure currently has no value-level path.

   ```rust
   pub trait MirrorView {
       fn account(&self) -> &AccountState;
       fn message(&self, id: &MessageId) -> MessageStateView<'_>;
       fn jobs(&self, id: &MessageId) -> Vec<JobView<'_>>;
       fn intents(&self, id: &MessageId) -> Vec<IntentView<'_>>;
   }

   pub enum Violation {
       CheckpointRegressed {
           message: MessageId,
           before: HistoryId,
           after: HistoryId,
       },
       MissingImportJob { message: MessageId },
       StaleAttemptApplied { claim: JobClaim },
       // Explicit variants for the other invariants.
   }

   // Conformance factory:
   // Fut: Future<Output = Result<S, StoreError>>
   ```

   Give commit checks the actual receipt as well as before/after views. Use separate reference-model comparisons for convergence and blob inventories for blob integrity.

   This adds evidence to the verification interface without loading the mirror into the production engine.

20. **Simulator inputs need valid probabilities and concrete replay operations.**

   Locations: [mailtide-sim/src/lib.rs:27](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-sim/src/lib.rs:27), [mailtide-sim/src/lib.rs:54](/home/denis/github.com/denismaciel/mailtide/crates/mailtide-sim/src/lib.rs:54), [mailtide-sim/src/lib.rs:69](/home/denis/github.com/denismaciel/mailtide-sim/src/lib.rs:69).

   Public `f64` probabilities admit negative values, values above one, and NaN. Unit operations such as `Relabel` and `Act` leave target selection and action details implicit in RNG consumption. Removing a step during shrinking can therefore change the meaning of subsequent steps.

   `converged: bool` also cannot distinguish checked convergence from a check that was never reached.

   ```rust
   pub struct Probability(f64); // Validated: finite and in [0, 1].

   pub enum UserOp {
       Receive(SimMessage),
       SaveDraft { draft: SimDraftId, content: DraftContent },
       Relabel { message: SimMessageId, patch: LabelPatch },
       Delete { message: SimMessageId },
       Act(ActionRequest),
   }

   pub struct ObservedViolation {
       pub step: StepNumber,
       pub violation: Violation,
   }

   pub enum Convergence {
       Reached { after_steps: u64 },
       NotReached(MirrorDifference),
       NotChecked(StopReason),
   }
   ```

   Generate concrete operations first, then replay them. Define invalid-target behavior during shrinking explicitly. Keep historical invariant failures separate from eventual convergence: both can legitimately occur in one run.

I would preserve the crate boundaries and restructure their contracts around three layers:

- **Boundary decoding:** HTTP, database rows, Python values, and configuration become validated domain values.
- **Pure domain transitions:** account lifecycle, message knowledge, jobs, intents, retry, delivery planning, and quota produce explicit decisions and guarded mutations.
- **Effect execution:** stores perform transactions, the driver performs network/blob/timer operations, and Python converts values. Each reports typed outcomes without choosing mailbox policy.

Use private constructors for relationships enums alone cannot enforce. Avoid turning every scalar, predicate, or free-text field into another abstraction; concentrate types around identity, evidence, transitions, and policy.

The five changes I would make first:

1. Couple related commit effects under shared guards, including durable hydration obligations for unknown messages.
2. Replace independent account lifecycle fields with validated state variants and explicit durable transitions.
3. Model intent delivery attempts and confirmation evidence, retaining unresolved older deliveries across newer desires.
4. Fence command, commit, job, and timer results with explicit identities and request context.
5. Complete the core’s store-read/effect protocol so the runner can remain an executor.

REVIEW-COMPLETE
