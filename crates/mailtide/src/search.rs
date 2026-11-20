//! Search: a Gmail query to one page of message or thread ids (the pure
//! part; the driver's `Runner::search` performs it).
//!
//! The query is Gmail's own search-box string, sent untouched: mailtide
//! parses no term of it, evaluates nothing and decides no match. What is
//! decided here is the request around it (a blank query is refused, label
//! ids are a set, the page size is clamped), the page token that binds a
//! continuation to the request that issued it, and how each hit is marked
//! against the mirror. Nothing here is stored.
//!
//! Zero counterpart: the request and token rules of
//! `lib/mail-search-store.ts`, as rules rather than a cache.

use std::collections::BTreeMap;

use crate::gmail::Label;
use crate::ids::{AccountId, HistoryId, LabelId, MessageId, PageToken, ThreadId, stable_hash};
use crate::mirror::Located;
use crate::quota::Priority;

/// The page size when the host names none.
pub const DEFAULT_LIMIT: u32 = 100;

/// Gmail's largest page.
pub const MAX_LIMIT: u32 = 500;

/// Which Gmail listing a search reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchScope {
    /// `messages.list`: message ids with their thread ids (5 units).
    Messages,
    /// `threads.list`: thread ids with Gmail's history id (10 units).
    Threads,
}

/// A continuation handed to the host: `s1.<binding>.<Gmail's token>`. The
/// binding names the request that issued it, so it is refused for any
/// other. A guard against misuse, not a secret.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SearchPageToken(pub String);

/// What the host asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchQuery {
    /// Gmail's search string, sent as `q` byte for byte. Blank is refused.
    pub query: String,
    /// Label ids every hit must carry (`labelIds`): ids, never names.
    pub label_ids: Vec<LabelId>,
    /// Also search Spam and Trash (`includeSpamTrash`). A query naming
    /// `in:trash`, `in:spam` or `in:anywhere` reaches them without it.
    pub include_spam_trash: bool,
    /// The page size, clamped to `1..=500`.
    pub limit: u32,
    /// The previous page's token, to read the next page of the same request.
    pub page_token: Option<SearchPageToken>,
    /// The quota class: `Foreground` (a user waits) unless a background job
    /// asks.
    pub priority: Priority,
}

impl SearchQuery {
    /// A first-page search for `query` with the defaults: no label filter,
    /// no Spam or Trash, 100 hits, Foreground.
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            label_ids: Vec::new(),
            include_spam_trash: false,
            limit: DEFAULT_LIMIT,
            page_token: None,
            priority: Priority::Foreground,
        }
    }

    /// The request Gmail gets for this query in `scope`, or why there is
    /// none. Refused before anything is read or sent: a blank query, and a
    /// page token not issued for this very request (another account, scope,
    /// query, label filter, Spam/Trash flag or page size).
    pub fn request(
        &self,
        account: &AccountId,
        scope: SearchScope,
    ) -> Result<SearchRequest, SearchRefusal> {
        if self.query.trim().is_empty() {
            return Err(SearchRefusal::EmptyQuery);
        }
        let mut label_ids = self.label_ids.clone();
        label_ids.sort();
        label_ids.dedup();
        let mut request = SearchRequest {
            scope,
            query: self.query.clone(),
            label_ids,
            include_spam_trash: self.include_spam_trash,
            limit: clamp_limit(self.limit),
            page_token: None,
        };
        if let Some(token) = &self.page_token {
            request.page_token = Some(token.open(request.binding(account))?);
        }
        Ok(request)
    }
}

/// Why a search is refused before anything is read or sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchRefusal {
    /// The query is blank: a listing without one is the mirror's to answer.
    EmptyQuery,
    /// The page token was not issued by mailtide for this request.
    InvalidPageToken,
}

/// `limit` within Gmail's page sizes: 0 becomes 1, above 500 becomes 500.
#[must_use]
pub fn clamp_limit(limit: u32) -> u32 {
    limit.clamp(1, MAX_LIMIT)
}

/// The Gmail request of a search, canonical: label ids sorted and
/// deduplicated, the limit clamped, Gmail's own page token unwrapped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    /// Which listing.
    pub scope: SearchScope,
    /// `q`, as the host wrote it.
    pub query: String,
    /// `labelIds`, sorted and deduplicated.
    pub label_ids: Vec<LabelId>,
    /// `includeSpamTrash`.
    pub include_spam_trash: bool,
    /// `maxResults`, in `1..=500`.
    pub limit: u32,
    /// Gmail's `pageToken`, verbatim.
    pub page_token: Option<PageToken>,
}

/// The token format's version prefix.
const TOKEN_PREFIX: &str = "s1.";

impl SearchRequest {
    /// The page token continuing this request at Gmail's `next`.
    #[must_use]
    pub fn next_token(&self, account: &AccountId, next: &PageToken) -> SearchPageToken {
        SearchPageToken(format!(
            "{TOKEN_PREFIX}{:016x}.{}",
            self.binding(account),
            next.0
        ))
    }

    /// FNV-1a over the account and every field but the page token, each
    /// length-prefixed so no two requests share an encoding.
    fn binding(&self, account: &AccountId) -> u64 {
        let mut bytes = Vec::new();
        let mut field = |value: &[u8]| {
            let len = u64::try_from(value.len()).unwrap_or(u64::MAX);
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(value);
        };
        field(account.as_str().as_bytes());
        field(match self.scope {
            SearchScope::Messages => b"messages",
            SearchScope::Threads => b"threads",
        });
        field(self.query.as_bytes());
        for label in &self.label_ids {
            field(label.0.as_bytes());
        }
        field(if self.include_spam_trash { b"1" } else { b"0" });
        field(&self.limit.to_le_bytes());
        stable_hash(&bytes)
    }
}

impl SearchPageToken {
    /// Gmail's token inside this one, if it was issued for `binding`.
    fn open(&self, binding: u64) -> Result<PageToken, SearchRefusal> {
        let (bound, gmail) = self
            .0
            .strip_prefix(TOKEN_PREFIX)
            .and_then(|rest| rest.split_once('.'))
            .ok_or(SearchRefusal::InvalidPageToken)?;
        let issued_for = (bound.len() == 16)
            .then(|| u64::from_str_radix(bound, 16).ok())
            .flatten();
        if issued_for != Some(binding) || gmail.is_empty() {
            return Err(SearchRefusal::InvalidPageToken);
        }
        Ok(PageToken(gmail.to_owned()))
    }
}

/// The label ids among `label_ids` the mirror's label definitions do not
/// hold, each once, in order. The driver asks Gmail about each
/// (`labels.get`) before searching: Gmail answers a `labelIds` value it does
/// not have (never created, or deleted) with an empty page, not an error,
/// so an unchecked filter could pass "no hits" off as an answer, while the
/// mirror's definitions can trail a label created since the last listing
/// (or hold none before the first).
#[must_use]
pub fn missing_labels<'a>(label_ids: &'a [LabelId], known: &[Label]) -> Vec<&'a LabelId> {
    let mut missing: Vec<&LabelId> = Vec::new();
    for id in label_ids {
        if !known.iter().any(|label| label.id == *id) && !missing.contains(&id) {
            missing.push(id);
        }
    }
    missing
}

/// One decoded page of a Gmail search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchListing {
    /// The hits, in Gmail's order.
    pub hits: ListedHits,
    /// Gmail's `resultSizeEstimate`: a hint, not a count.
    pub estimate: u64,
    /// Gmail's next page, if any. A page may be empty and still carry one.
    pub next_page_token: Option<PageToken>,
}

/// The hits of a page, by scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListedHits {
    /// `messages.list`: message and thread ids.
    Messages(Vec<(MessageId, ThreadId)>),
    /// `threads.list`: thread ids with their history ids.
    Threads(Vec<ThreadHit>),
}

/// A thread Gmail's search matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadHit {
    /// The thread.
    pub thread_id: ThreadId,
    /// Gmail's `historyId` for the thread (its newest message's): compared
    /// with the mirror's cursor, it says whether the mirror has caught up
    /// with the thread. Optional, as Gmail does not document it per entry.
    pub history_id: Option<HistoryId>,
}

/// What the mirror holds for a message hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HitState {
    /// A row with its content stored: `messages()` has all of it.
    Imported,
    /// A row whose content is not stored yet: `messages()` has its labels.
    NotImported,
    /// No row: Gmail's index is ahead of the mirror (or bootstrap has not
    /// listed it yet). The next sync brings it.
    Unknown,
}

/// A message Gmail's search matched, marked against the mirror.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchHit {
    /// The message.
    pub message_id: MessageId,
    /// Its thread.
    pub thread_id: ThreadId,
    /// What the mirror holds for it.
    pub state: HitState,
}

/// A page's hits marked against the mirror.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marked {
    /// The hits the mirror has not deleted, in Gmail's order.
    pub hits: Vec<SearchHit>,
    /// Hits dropped because the mirror holds a tombstone for them (Gmail's
    /// index trails a delete; deletion is final, I5).
    pub deleted: u32,
}

/// Mark each listed hit with what `located` says the mirror holds, keeping
/// Gmail's order (nothing sorted, merged or deduplicated) and dropping
/// tombstoned ones. An id `located` does not name is unknown.
#[must_use]
pub fn mark(listed: Vec<(MessageId, ThreadId)>, located: &BTreeMap<MessageId, Located>) -> Marked {
    let mut marked = Marked {
        hits: Vec::with_capacity(listed.len()),
        deleted: 0,
    };
    for (message_id, thread_id) in listed {
        let state = match located.get(&message_id) {
            Some(Located::Tombstone) => {
                marked.deleted += 1;
                continue;
            }
            Some(Located::Row { imported: true }) => HitState::Imported,
            Some(Located::Row { imported: false }) => HitState::NotImported,
            Some(Located::Absent) | None => HitState::Unknown,
        };
        marked.hits.push(SearchHit {
            message_id,
            thread_id,
            state,
        });
    }
    marked
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gmail::LabelKind;

    fn account() -> AccountId {
        AccountId("alice@example.com".into())
    }

    fn query(q: &str) -> SearchQuery {
        SearchQuery::new(q)
    }

    fn labels(ids: &[&str]) -> Vec<LabelId> {
        ids.iter().map(|id| LabelId((*id).into())).collect()
    }

    fn first_page(query: &SearchQuery) -> SearchRequest {
        query
            .request(&account(), SearchScope::Messages)
            .expect("a request")
    }

    /// The token the first page of `query` would hand out for Gmail's `next`.
    fn continuation(query: &SearchQuery, next: &str) -> SearchPageToken {
        first_page(query).next_token(&account(), &PageToken(next.into()))
    }

    #[test]
    fn a_blank_query_is_refused() {
        for blank in ["", " ", "\t\n "] {
            assert_eq!(
                query(blank).request(&account(), SearchScope::Messages),
                Err(SearchRefusal::EmptyQuery),
                "{blank:?}"
            );
        }
        let spaced = first_page(&query("  from:alice  "));
        assert_eq!(spaced.query, "  from:alice  ", "sent untrimmed");
    }

    #[test]
    fn limit_is_clamped_to_one_through_five_hundred() {
        assert_eq!(SearchQuery::new("x").limit, 100, "the default");
        for (asked, sent) in [(0, 1), (1, 1), (100, 100), (500, 500), (501, 500)] {
            let request = first_page(&SearchQuery {
                limit: asked,
                ..query("x")
            });
            assert_eq!(request.limit, sent, "{asked}");
        }
        assert_eq!(clamp_limit(u32::MAX), MAX_LIMIT);
    }

    #[test]
    fn a_page_token_round_trips_gmails_token_byte_for_byte() {
        let first = query("invoice");
        for gmail in ["0", "abc+/=", "09.876.54", "Zm9v.YmFy=="] {
            let next = SearchQuery {
                page_token: Some(continuation(&first, gmail)),
                ..first.clone()
            };
            assert_eq!(
                first_page(&next).page_token,
                Some(PageToken(gmail.into())),
                "{gmail}"
            );
        }
    }

    #[test]
    fn a_garbled_page_token_is_refused() {
        let first = query("invoice");
        let good = continuation(&first, "abc").0;
        let foreign_binding = format!("s1.{:016x}.abc", 7_u64);
        for bad in [
            "abc".to_owned(),
            good.replacen("s1.", "s2.", 1),
            good.replacen("s1.", "", 1),
            foreign_binding,
            "s1.zzzzzzzzzzzzzzzz.abc".to_owned(),
            "s1.0.abc".to_owned(),
            good.trim_end_matches("abc").to_owned(),
        ] {
            let next = SearchQuery {
                page_token: Some(SearchPageToken(bad.clone())),
                ..first.clone()
            };
            assert_eq!(
                next.request(&account(), SearchScope::Messages),
                Err(SearchRefusal::InvalidPageToken),
                "{bad}"
            );
        }
    }

    #[test]
    fn the_token_binding_does_not_depend_on_label_id_order() {
        let first = SearchQuery {
            label_ids: labels(&["Label_2", "INBOX", "Label_2"]),
            ..query("invoice")
        };
        assert_eq!(first_page(&first).label_ids, labels(&["INBOX", "Label_2"]));
        let reordered = SearchQuery {
            label_ids: labels(&["INBOX", "Label_2"]),
            page_token: Some(continuation(&first, "abc")),
            ..query("invoice")
        };
        assert_eq!(
            first_page(&reordered).page_token,
            Some(PageToken("abc".into()))
        );
    }

    #[test]
    fn label_ids_the_mirror_does_not_hold_are_named() {
        let known = [Label::plain(
            LabelId("INBOX".into()),
            "INBOX",
            LabelKind::System,
        )];
        let asked = labels(&["Label_9", "INBOX", "Label_9", "Label_2"]);
        assert_eq!(
            missing_labels(&asked, &known),
            [&LabelId("Label_9".into()), &LabelId("Label_2".into())],
            "each missing id once, in order"
        );
        assert!(missing_labels(&asked[1..2], &known).is_empty());
        assert_eq!(
            missing_labels(&asked[1..2], &[]),
            [&LabelId("INBOX".into())],
            "before the first label listing every id is missing"
        );
    }

    fn listed(ids: &[&str]) -> Vec<(MessageId, ThreadId)> {
        ids.iter()
            .map(|id| (MessageId((*id).into()), ThreadId(format!("t-{id}"))))
            .collect()
    }

    fn located(entries: &[(&str, Located)]) -> BTreeMap<MessageId, Located> {
        entries
            .iter()
            .map(|(id, at)| (MessageId((*id).into()), *at))
            .collect()
    }

    #[test]
    fn tombstoned_hits_are_dropped_and_counted() {
        let marked = mark(
            listed(&["m1", "gone", "m2", "gone2"]),
            &located(&[
                ("m1", Located::Row { imported: true }),
                ("gone", Located::Tombstone),
                ("m2", Located::Absent),
                ("gone2", Located::Tombstone),
            ]),
        );
        let ids: Vec<&str> = marked
            .hits
            .iter()
            .map(|h| h.message_id.0.as_str())
            .collect();
        assert_eq!(ids, ["m1", "m2"]);
        assert_eq!(marked.deleted, 2);
    }

    #[test]
    fn hits_keep_gmails_order_and_carry_their_mirror_state() {
        let marked = mark(
            listed(&["m3", "m1", "m2", "m1"]),
            &located(&[
                ("m1", Located::Row { imported: true }),
                ("m2", Located::Row { imported: false }),
                ("m3", Located::Absent),
            ]),
        );
        let hits: Vec<(&str, &str, HitState)> = marked
            .hits
            .iter()
            .map(|h| (h.message_id.0.as_str(), h.thread_id.0.as_str(), h.state))
            .collect();
        assert_eq!(
            hits,
            [
                ("m3", "t-m3", HitState::Unknown),
                ("m1", "t-m1", HitState::Imported),
                ("m2", "t-m2", HitState::NotImported),
                ("m1", "t-m1", HitState::Imported),
            ],
            "Gmail's order, repeats kept"
        );
        assert_eq!(marked.deleted, 0);
        let unlocated = mark(listed(&["m9"]), &BTreeMap::new());
        assert_eq!(unlocated.hits[0].state, HitState::Unknown);
    }
}
