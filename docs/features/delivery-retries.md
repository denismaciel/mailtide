# Retry and failure classes of Gmail writes and batches

Plan: `features/delivery-retries.toml` (no roadmap id). Status: done.

## 1. Goal and scope

No new feature. Two reference suites test what happens when a Gmail write
fails, and no feature plan owned them. The behaviour is on `main` already
(SPEC §7.3, §8): this plan says which existing test covers each reference
test, plans the tests that are missing, and marks what does not apply.

Left out: sends (`send` has its own failure table, where a lost answer is
never retried), draft and label writes (synchronous calls that mailtide never
retries), and any change to the classes themselves.

## 2. What the references do

**Zero** (MIT). `gmail-delivery-failure.ts` is one pure function for a failed
label delivery: a local quota deferral retries at the gate's time; a 429 or a
rate-limit reason on a 403 retries with `Retry-After` as a minimum;
`invalid_grant` and any 401 block until re-authorised; other 4xx block;
everything else is transient with backoff. Four tests.

**himalaya** (MIT/Apache). `src/gmail/backend.rs` sends writes as HTTP
batches of 50 parts and retries a throttled part alone, three times, before
reporting it rate-limited. Two tests.

The other references have nothing here.

## 3. Design in mailtide

Nothing is designed; this is what exists.

| Failure of a delivery | mailtide today | Where |
|---|---|---|
| Quota gate says not yet | a deferral, no failed attempt; due again when the gate says | `Engine::on_deferred` |
| 429, 403 `userRateLimitExceeded` | account cooldown of at least `Retry-After`; the job retries in full | `errors.rs`, `Engine::cool_down` |
| Network error (not sent, or the answer lost), 5xx | transient; retry with backoff, no attempt cap | driver, `retry.rs` |
| 400, other permanent 4xx | the intents sent are blocked and stop overlaying; the job finishes; the account keeps syncing | `Engine::on_failed` |
| 401 | the cached token is dropped, the request is transient | driver |
| Revoked delegation, deleted user | the account is blocked, by the token source or the classification | `errors.rs`, `auth.rs` |
| 404 | the message is tombstoned | SPEC §7.3 |

Two differences from the references, both deliberate. A 401 does not block:
an access token can expire between minting and use, and the token source is
the authority on a revoked grant. And there is no HTTP batch: a `Deliver`
job carries one message's pending changes and sends them as one
`messages.modify`, answered whole, so a throttled write retries its whole
change, which is harmless because label changes are absolute.
`crates/mailtide-gmail/src/request.rs` builds a `batchModify` only for a
group of ten or more messages, which delivery never forms, and
`crates/mailtide-gmail/src/batch.rs` can encode an HTTP batch that nothing in
the driver uses.

The transport tells a request that was never sent (`NotSent`) from one whose
answer was lost after it went out (`OutcomeUnknown`). Only a direct call
(`Runner::call_gmail`) reports the second as such; to the engine both are a
transient failure, and a delivery is sent again, which is safe for the same
reason.

No public API, schema, Gmail call or quota cost changes; nothing new is
persisted.

## 4. Alternatives rejected

- **Leave both suites undeclared:** they describe paths every write feature
  leans on, and "nobody listed it" is not "it does not apply".
- **Port Zero's classifier as one function:** mailtide splits classification
  (wire crate) from consequence (engine); the tests follow that split.
- **Use the batch encoder for deliveries so himalaya's tests apply:** a new
  behaviour, and one `messages.modify` per message already answers whole.

## 5. Tests

- Zero `gmail-delivery-failure.test.ts` (4; confirmed not in
  `scripts/zero_tests.py` `SUITES`): 2 map onto existing classification
  tests in `crates/mailtide-gmail/src/errors.rs` and report `implemented`;
  2 are planned at engine level, because the classes are tested but their
  effect on a `Deliver` job is not (a deferral with many attempts behind it;
  transient against permanent). One note records the 401 difference.
- himalaya `src/gmail/backend.rs` (2 of its 17, deferred here by `search`):
  both `na`, no HTTP batch.
- Own tests (3): a throttled modify retries the whole delivery after the
  cooldown; a delivery throttled again and again keeps backing off; a 401
  drops the token and retries.
- The engine tests share one host (`crates/mailtide/tests/delivery_host/mod.rs`):
  store commands go to an `AccountMirror` at once; Gmail calls and timers are
  the test's to answer. The 401 test runs the real runner on a host clock, so
  the retry waits for the next run instead of a sleep.

## 6. Slices

1. The two engine-level reference tests and the two own engine tests.
2. The driver test for 401.

Each is test-only and can land at any time.

## 7. Decisions

1. **A plan of its own, with no feature.** The reference tests need an owner
   so the report shows them; no write feature is the right one. *Would
   change it:* nothing.
2. **Map onto existing tests where one exists, even when it is a unit test
   of the class and the reference tests the whole decision.** The gap, the
   effect on a delivery, gets its own test. *Would change it:* a maintainer
   preferring one ported test per reference test; then the two mapped ones
   become engine tests too.
3. **The 401 difference is kept and pinned by a test**, not aligned with
   Zero. *Would change it:* evidence of a host whose token source cannot
   report a revoked grant.
4. **The batch tests are `na`, not ported onto a whole delivery.** They test
   per-part retry, which has no counterpart. *Would change it:* the driver
   starting to send HTTP batches.

Unknowns: none. Nothing here assumes a Gmail behaviour that the existing
code does not already rely on.
