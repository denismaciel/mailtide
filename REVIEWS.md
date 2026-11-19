# Design reviews and decisions

## 2026-11-18: sync-design and Gmail API reviews (Claude subagents)

Folded into the types laid out in `8f7fba4`: time on events, conditional
commit ops, epoch/version fencing, snapshot rehydration, per-page commits,
attempt-fenced jobs, versioned intents with base checkpoints, per-label
reconciliation with an all-mail pass, bandwidth/concurrency failure classes,
draft settle delay, message `historyId` as the per-message checkpoint.

## 2026-11-18: types and signatures (GPT-6-Astra, xhigh)

Full text: [docs/reviews/2026-11-18-astra-types.md](docs/reviews/2026-11-18-astra-types.md).

| # | Finding | Decision |
|---|---|---|
| 1 | Related commit effects need one guard | **Applied**: commits tied to a job carry its claim and apply only while the claim holds; stores enqueue hydration/cleanup jobs atomically; store transition rules are pure core functions shared by every store |
| 2 | Account lifecycle is a product of fields | **Applied**: `SyncState` sum type (bootstrapping, live, reconciling, blocked with reason and resume state), validated snapshot |
| 3 | Intent confirmation needs causal evidence | **Deferred to the write path (phase 6)**, designed with #5 and #9 |
| 4 | Command identities don't survive engine replacement | **Applied**: command ids carry the epoch; stale results are ignored |
| 5 | Store-read protocol incomplete | **Partly applied**: `ClaimJobs`/`JobsClaimed`; action-target and intent reads come with phase 6 |
| 6 | Reconciliation scan identity, explicit scope | **Partly applied**: explicit `ListScope` with an all-mail pass; seen-set lives in the store keyed by baseline |
| 7 | Identity vs partial evidence vs observation | **Applied**: `MessageRef` (history) vs `MessageObservation` (always checkpointed); deletion evidence |
| 8 | Jobs can't represent blocked | **Applied**: `JobState`, separate failure count, `NewJob` for enqueue |
| 9 | Modify group hides multiple requests | **Deferred to phase 6** |
| 10 | Errors need structure | **Applied in core** (retryable / not found / blocking; backoff takes retryable only); transport and store error enums applied with those crates |
| 11 | Quota scope needs its target | **Applied** |
| 12 | Ids are unvalidated wrappers | **Partly applied**: `AccountId::parse`; other ids stay opaque public wrappers for now |
| 13 | `PagePosition` duplicates the page | **Applied**: finality derived inside `fold_page` |
| 14 | Action booleans hide policy | **Applied**: explicit action variants, `RestoreDestination` |
| 15 | `#[non_exhaustive]` inside the workspace | **Applied**: removed; no wildcard arms on workspace enums |
| 16 | Status and run outcomes | **Applied with the runner** (`StopReason`) |
| 17 | Python erases distinctions | **Applied with the bindings** |
| 18 | Batch membership and correlation | **Applied with the Gmail layer** |
| 19 | Invariant/conformance evidence | **Applied with the memory store** |
| 20 | Simulator inputs | **Applied with the simulator** |
