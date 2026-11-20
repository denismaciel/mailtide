//! Search: one page of Gmail's own search, through the quota gate, each
//! message hit marked with what the mirror holds for it.
//!
//! A read-only direct call, as a drift check is: the account's status is
//! read, Gmail is asked once ([`Runner::call_gmail`]: quota-gated, epoch 0,
//! never claiming the account), and the hits are marked with one store
//! read. Nothing is written: no commit, row, job, feed entry or epoch, so a
//! search can run beside a sync and never supersedes it. The query is
//! Gmail's to interpret; mailtide never logs it.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use mailtide::command::GmailRequest;
use mailtide::event::EventKind;
use mailtide::ids::{AccountId, HistoryId, LabelId};
use mailtide::retry::{Blocking, BlockingFailure, Retryable, RetryableFailure};
use mailtide::revision::FeedPosition;
use mailtide::search::{
    ListedHits, SearchHit, SearchListing, SearchPageToken, SearchQuery, SearchRefusal,
    SearchRequest, SearchScope, ThreadHit,
};
use mailtide::state::{SyncPhase, SyncState};
use mailtide_store::Store;

use crate::auth::TokenSource;
use crate::blobs::BlobStore;
use crate::quota_gate::QuotaGate;
use crate::runner::{CallError, QuotaDeferred, RunError, Runner};
use crate::transport::GmailTransport;

/// One page of message hits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchPage {
    /// The hits the mirror has not deleted, in Gmail's order (not sorted,
    /// merged or deduplicated), each with what the mirror holds for it.
    pub hits: Vec<SearchHit>,
    /// The next page of the same request; `None` ends the paging run. A
    /// page may be empty and still carry one.
    pub next_page_token: Option<SearchPageToken>,
    /// Gmail's `resultSizeEstimate`: a hint for a UI, not a count.
    pub estimate: u64,
    /// Hits dropped because the mirror deleted them (Gmail's index trails
    /// the delete).
    pub deleted: u32,
    /// The account's change-feed position, read before Gmail was asked: a
    /// feed entry after it may make this page stale.
    pub mirror_state: FeedPosition,
    /// The mirror's history cursor, read with `mirror_state`.
    pub cursor: Option<HistoryId>,
    /// The sync phase, read with `mirror_state`.
    pub phase: SyncPhase,
}

/// One page of thread hits. Not marked against the mirror: compare each
/// hit's history id with `cursor` to tell whether the mirror has caught up
/// with the thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadSearchPage {
    /// The threads, in Gmail's order.
    pub hits: Vec<ThreadHit>,
    /// The next page of the same request; `None` ends the paging run.
    pub next_page_token: Option<SearchPageToken>,
    /// Gmail's `resultSizeEstimate`: a hint, not a count.
    pub estimate: u64,
    /// The account's change-feed position, read before Gmail was asked.
    pub mirror_state: FeedPosition,
    /// The mirror's history cursor, read with `mirror_state`.
    pub cursor: Option<HistoryId>,
    /// The sync phase, read with `mirror_state`.
    pub phase: SyncPhase,
}

/// Why a search brought back no page. Nothing here is recorded on the
/// account: a search writes nothing, and mailtide never retries one (it is
/// idempotent, so the host may).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchError {
    /// The query is blank: a listing without a query is the mirror's to
    /// answer. Nothing was read or sent.
    EmptyQuery,
    /// The page token is not one mailtide issued for this very request.
    /// Nothing was sent.
    InvalidPageToken,
    /// Gmail has no such label id (deleted, or never created): its
    /// `labels.get` answered 404. Gmail would answer a search filtered on
    /// it with an empty page, so it is refused instead; no search was sent.
    UnknownLabel(LabelId),
    /// Gmail refused a continuation mailtide issued for this request: start
    /// again from the first page.
    PageTokenRejected,
    /// Gmail refused the search for good: a 400 or 403 (a query or filter it
    /// rejects, the `gmail.metadata` scope), an account problem, or an
    /// answer that is not a page. The caller's error: the account is not
    /// blocked by it.
    Refused(BlockingFailure),
    /// The quota gate would make it wait past its budget: nothing was sent.
    Deferred(QuotaDeferred),
    /// Worth asking again later: a 5xx, a timeout, an expired token, or a
    /// rate limit (the gate is cooled down for `Retry-After`).
    Retryable(RetryableFailure),
    /// The account is blocked until an operator acts (its reason). Nothing
    /// was sent and no credential was asked for.
    AccountBlocked(BlockingFailure),
    /// The account isn't tracked: search never starts mirroring one.
    NotTracked,
    /// The runner's own resources failed (the store, the quota gate).
    Run(RunError),
}

impl SearchError {
    /// A stable name for logs (no detail: Gmail's error text can quote the
    /// query).
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::EmptyQuery => "empty_query",
            Self::InvalidPageToken => "invalid_page_token",
            Self::UnknownLabel(_) => "unknown_label",
            Self::PageTokenRejected => "page_token_rejected",
            Self::Refused(_) => "refused",
            Self::Deferred(_) => "deferred",
            Self::Retryable(_) => "retryable",
            Self::AccountBlocked(_) => "account_blocked",
            Self::NotTracked => "not_tracked",
            Self::Run(error) => error.code(),
        }
    }
}

impl From<SearchRefusal> for SearchError {
    fn from(refusal: SearchRefusal) -> Self {
        match refusal {
            SearchRefusal::EmptyQuery => Self::EmptyQuery,
            SearchRefusal::InvalidPageToken => Self::InvalidPageToken,
        }
    }
}

/// Where the mirror stood when a search asked Gmail.
struct Position {
    mirror_state: FeedPosition,
    cursor: Option<HistoryId>,
    phase: SyncPhase,
}

/// A page of Gmail's answer with the request it answers.
struct Answered {
    request: SearchRequest,
    listing: SearchListing,
    position: Position,
}

impl<S, B, T, Q, A> Runner<S, B, T, Q, A>
where
    S: Store,
    B: BlobStore,
    T: GmailTransport,
    Q: QuotaGate,
    A: TokenSource,
{
    /// One page of message ids Gmail's search matches for `query`
    /// (`messages.list`, 5 units), each marked with what the mirror holds:
    /// imported, not imported, or unknown (the next sync brings it); hits
    /// the mirror has deleted are dropped and counted. Hydrate the hits
    /// with `messages`. A quota deferral is waited out within `budget`.
    /// Allowed read-only: it needs only `gmail.readonly`.
    pub async fn search(
        &self,
        account: &AccountId,
        query: &SearchQuery,
        budget: Duration,
    ) -> Result<SearchPage, SearchError> {
        let answered = self
            .search_gmail(account, query, SearchScope::Messages, budget)
            .await?;
        let ListedHits::Messages(listed) = answered.listing.hits else {
            return Err(SearchError::Run(RunError::Invariant(
                "a message search answered with threads",
            )));
        };
        let ids: Vec<_> = listed.iter().map(|(id, _)| id.clone()).collect();
        let located: BTreeMap<_, _> = self
            .store()
            .locate(account, &ids)
            .await
            .map_err(|e| SearchError::Run(RunError::Store(e)))?
            .into_iter()
            .collect();
        let marked = mailtide::search::mark(listed, &located);
        Ok(SearchPage {
            hits: marked.hits,
            next_page_token: answered
                .listing
                .next_page_token
                .map(|next| answered.request.next_token(account, &next)),
            estimate: answered.listing.estimate,
            deleted: marked.deleted,
            mirror_state: answered.position.mirror_state,
            cursor: answered.position.cursor,
            phase: answered.position.phase,
        })
    }

    /// One page of thread ids Gmail's search matches for `query`
    /// (`threads.list`, 10 units), each with Gmail's history id for the
    /// thread. As [`Self::search`] otherwise; thread hits are not marked
    /// against the mirror.
    pub async fn search_threads(
        &self,
        account: &AccountId,
        query: &SearchQuery,
        budget: Duration,
    ) -> Result<ThreadSearchPage, SearchError> {
        let answered = self
            .search_gmail(account, query, SearchScope::Threads, budget)
            .await?;
        let ListedHits::Threads(hits) = answered.listing.hits else {
            return Err(SearchError::Run(RunError::Invariant(
                "a thread search answered with messages",
            )));
        };
        Ok(ThreadSearchPage {
            hits,
            next_page_token: answered
                .listing
                .next_page_token
                .map(|next| answered.request.next_token(account, &next)),
            estimate: answered.listing.estimate,
            mirror_state: answered.position.mirror_state,
            cursor: answered.position.cursor,
            phase: answered.position.phase,
        })
    }

    /// Check the request, read where the mirror stands, and ask Gmail.
    async fn search_gmail(
        &self,
        account: &AccountId,
        query: &SearchQuery,
        scope: SearchScope,
        budget: Duration,
    ) -> Result<Answered, SearchError> {
        let deadline = Instant::now() + budget;
        let request = query.request(account, scope)?;
        let status = self
            .store()
            .status(account)
            .await
            .map_err(|e| SearchError::Run(RunError::Store(e)))?
            .ok_or(SearchError::NotTracked)?;
        if let SyncState::Blocked { reason, .. } = &status.sync {
            return Err(SearchError::AccountBlocked(reason.clone()));
        }
        // Read before Gmail is asked, so a change landing during the call
        // is after it.
        let position = Position {
            mirror_state: FeedPosition {
                generation: status.generation,
                at: status.state.into(),
            },
            cursor: status.cursor,
            phase: status.sync.phase(),
        };
        self.check_labels(account, &request, query, deadline)
            .await?;
        let continuation = request.page_token.is_some();
        let answer = self
            .call_gmail(
                account,
                GmailRequest::Search(request.clone()),
                query.priority,
                deadline,
            )
            .await;
        match answer {
            Ok(EventKind::Searched { listing, .. }) => Ok(Answered {
                request,
                listing,
                position,
            }),
            Ok(_) => Err(SearchError::Run(RunError::Invariant(
                "unexpected answer to a search",
            ))),
            Err(error) => Err(call_error(error, continuation)),
        }
    }
}

impl<S, B, T, Q, A> Runner<S, B, T, Q, A>
where
    S: Store,
    B: BlobStore,
    T: GmailTransport,
    Q: QuotaGate,
    A: TokenSource,
{
    /// Make sure Gmail has every label of the filter. Gmail answers a
    /// `labelIds` value it does not have with an empty page, so an id the
    /// mirror's definitions lack (deleted, never created, created since the
    /// last listing, or no listing yet) is asked about once, with
    /// `labels.get` (1 unit): a 404 is [`SearchError::UnknownLabel`], an
    /// answer means the mirror is behind and the search goes on. Nothing is
    /// recorded: the engine's label refresh brings the definition.
    async fn check_labels(
        &self,
        account: &AccountId,
        request: &SearchRequest,
        query: &SearchQuery,
        deadline: Instant,
    ) -> Result<(), SearchError> {
        if request.label_ids.is_empty() {
            return Ok(());
        }
        let known = self
            .store()
            .labels(account)
            .await
            .map_err(|e| SearchError::Run(RunError::Store(e)))?;
        for id in mailtide::search::missing_labels(&request.label_ids, &known) {
            let answer = self
                .call_gmail(
                    account,
                    GmailRequest::GetLabel { id: id.clone() },
                    query.priority,
                    deadline,
                )
                .await;
            match answer {
                Ok(EventKind::LabelFetched { .. }) => {}
                Ok(_) => {
                    return Err(SearchError::Run(RunError::Invariant(
                        "unexpected answer to a label get",
                    )));
                }
                Err(CallError::NotFound) => return Err(SearchError::UnknownLabel(id.clone())),
                Err(error) => return Err(call_error(error, false)),
            }
        }
        Ok(())
    }
}

/// A failed search call in search's terms. A continuation repeats a
/// request Gmail already accepted, so a 4xx on it is the token's.
fn call_error(error: CallError, continuation: bool) -> SearchError {
    match error {
        CallError::NotFound | CallError::Refused(_) if continuation && refuses_token(&error) => {
            SearchError::PageTokenRejected
        }
        CallError::NotFound => SearchError::Refused(BlockingFailure {
            class: Blocking::Permanent,
            detail: "404 not found".into(),
        }),
        CallError::Refused(refusal) => SearchError::Refused(refusal),
        CallError::Deferred(deferred) => SearchError::Deferred(deferred),
        CallError::Retryable(failure) => SearchError::Retryable(failure),
        // A lost answer to a read is only a transient failure.
        CallError::OutcomeUnknown(detail) => SearchError::Retryable(RetryableFailure {
            class: Retryable::Transient,
            retry_after: None,
            detail,
        }),
        CallError::ReadOnly => SearchError::Run(RunError::Invariant("a search is a read")),
        CallError::Run(error) => SearchError::Run(error),
    }
}

/// Whether Gmail's refusal of a continuation is about the request itself
/// (a 404, or a 4xx refused for good) rather than the account.
fn refuses_token(error: &CallError) -> bool {
    match error {
        CallError::NotFound => true,
        CallError::Refused(refusal) => refusal.class == Blocking::Permanent,
        CallError::ReadOnly
        | CallError::Deferred(_)
        | CallError::Retryable(_)
        | CallError::OutcomeUnknown(_)
        | CallError::Run(_) => false,
    }
}
