//! Gmail quota: declared costs and priorities, and the pure budget ledger the
//! driver uses to admit requests (SPEC §8).
//!
//! Units per minute per user and per project, a daily project threshold,
//! per-user and per-project concurrency, and a per-user daily byte budget
//! (Gmail's bandwidth cap, shared with IMAP). Time is passed in.
//!
//! Zero counterpart: `lib/gmail-quota-cost.ts`, `lib/gmail-quota-store.ts`.

use std::time::Duration;

use crate::command::GmailRequest;
use crate::drafts::DraftFormat;
use crate::ids::{AccountId, Timestamp};
use crate::search::SearchScope;

/// Gmail quota units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuotaUnits(pub u32);

/// Admission class. Higher classes may use more of the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    /// Historical import and reconciliation (70% of budget).
    Backfill,
    /// New mail and history (90%).
    Live,
    /// A user is waiting (100%).
    Foreground,
}

/// The declared cost of a request, from Gmail's quota table.
///
/// A modify group under 10 messages is sent as individual `messages.modify`
/// calls (5 units each), since `batchModify` costs 50 (SPEC §7.3).
#[must_use]
pub fn cost(request: &GmailRequest) -> QuotaUnits {
    QuotaUnits(match request {
        GmailRequest::GetProfile | GmailRequest::ListLabels | GmailRequest::GetLabel { .. } => 1,
        GmailRequest::ListHistory { .. } => 2,
        // labels.patch is not in Google's table: declared as labels.update.
        GmailRequest::ListMessages { .. }
        | GmailRequest::CreateLabel(_)
        | GmailRequest::PatchLabel { .. }
        | GmailRequest::DeleteLabel { .. } => 5,
        GmailRequest::GetMessage { .. } => 20,
        GmailRequest::ImportMessage { .. } => 60,
        GmailRequest::Watch => 100,
        GmailRequest::Modify(group) => match u32::try_from(group.messages.len()) {
            Ok(n) if n < BATCH_MODIFY_THRESHOLD => 5 * n,
            _ => 50,
        },
        GmailRequest::Search(request) => match request.scope {
            SearchScope::Messages => 5,
            SearchScope::Threads => 10,
        },
        GmailRequest::DeleteMessage { .. } => 10,
        GmailRequest::BatchDeleteMessages { .. } => 50,
        GmailRequest::ListDrafts { .. } => 5,
        GmailRequest::GetDraft { format, .. } => match format {
            DraftFormat::Minimal => 20,
            DraftFormat::Raw => 60,
        },
        GmailRequest::CreateDraft { .. } | GmailRequest::DeleteDraft { .. } => 10,
        GmailRequest::UpdateDraft { .. } => 15,
        GmailRequest::SendDraft { .. } => 100,
    })
}

/// Groups at least this large use `batchModify`.
pub const BATCH_MODIFY_THRESHOLD: u32 = 10;

/// A purge of at least this many ids uses `messages.batchDelete` (50
/// units); fewer use one `messages.delete` each (10 units, at most 40).
pub const PURGE_BATCH_THRESHOLD: u32 = 5;

/// Budgets the ledger enforces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Budget {
    /// Units per rolling minute per account.
    pub account_per_minute: u32,
    /// Units per rolling minute per project.
    pub project_per_minute: u32,
    /// Units per day per project before Google bills.
    pub project_per_day: u64,
    /// Concurrent requests per account.
    pub account_concurrency: u32,
    /// Concurrent requests per project.
    pub project_concurrency: u32,
    /// Download bytes per day per account (Gmail's bandwidth cap).
    pub account_bytes_per_day: u64,
}

impl Budget {
    /// 80% of Google's published limits (Zero's defaults): 6,000 units per
    /// minute per user, 1.2M per project, the 80M/day billing threshold,
    /// and Gmail's 2.5 GB/day per-user download cap.
    pub const GMAIL_DEFAULT: Self = Self {
        account_per_minute: 4_800,
        project_per_minute: 960_000,
        project_per_day: 64_000_000,
        account_concurrency: 8,
        project_concurrency: 128,
        account_bytes_per_day: 2_000_000_000,
    };
}

impl Priority {
    /// The share of every budget this class may use (SPEC §8).
    fn share(self) -> (u32, u32) {
        match self {
            Self::Foreground => (100, 100),
            Self::Live => (90, 75),
            Self::Backfill => (70, 50),
        }
    }
}

/// The admission decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// Send it now; release the lease when done.
    Granted(Lease),
    /// Not now; try again at `until`. Not a failure.
    Deferred {
        /// When to try again.
        until: Timestamp,
    },
}

/// A concurrency slot held by an in-flight request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lease(pub u64);

const MINUTE: Duration = Duration::from_mins(1);
const DAY_MILLIS: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Clone)]
struct Spend {
    at: Timestamp,
    account: AccountId,
    units: u32,
}

#[derive(Debug, Clone)]
struct Held {
    account: AccountId,
    since: Timestamp,
}

/// Rolling-window accounting across accounts (pure; time is passed in).
#[derive(Debug, Clone, Default)]
pub struct Ledger {
    spent: Vec<Spend>,
    leases: std::collections::BTreeMap<Lease, Held>,
    next_lease: u64,
    day: i64,
    day_units: u64,
    day_bytes: std::collections::BTreeMap<AccountId, u64>,
    cooldowns: Vec<crate::retry::Cooldown>,
}

impl Ledger {
    /// Admit a request of `cost` and `priority` for `account` at `now`.
    pub fn admit(
        &mut self,
        budget: &Budget,
        account: &AccountId,
        cost: QuotaUnits,
        priority: Priority,
        now: Timestamp,
    ) -> Admission {
        self.roll(now);
        if let Some(until) = self.cooldown_for(account, now) {
            return Admission::Deferred { until };
        }
        let tomorrow = Timestamp((self.day + 1) * DAY_MILLIS);
        if self.day_units + u64::from(cost.0) > budget.project_per_day
            || self.day_bytes.get(account).copied().unwrap_or(0) >= budget.account_bytes_per_day
        {
            return Admission::Deferred { until: tomorrow };
        }
        let (units_share, slots_share) = priority.share();
        let allowed = |limit: u32, share: u32| u64::from(limit) * u64::from(share) / 100;
        let account_units: u64 = self
            .spent
            .iter()
            .filter(|s| &s.account == account)
            .map(|s| u64::from(s.units))
            .sum();
        let project_units: u64 = self.spent.iter().map(|s| u64::from(s.units)).sum();
        let cost_units = u64::from(cost.0);
        if account_units + cost_units > allowed(budget.account_per_minute, units_share) {
            return Admission::Deferred {
                until: self.window_frees(Some(account), now),
            };
        }
        if project_units + cost_units > allowed(budget.project_per_minute, units_share) {
            return Admission::Deferred {
                until: self.window_frees(None, now),
            };
        }
        let account_slots = self
            .leases
            .values()
            .filter(|h| &h.account == account)
            .count() as u64;
        let project_slots = self.leases.len() as u64;
        if account_slots >= allowed(budget.account_concurrency, slots_share).max(1)
            || project_slots >= allowed(budget.project_concurrency, slots_share).max(1)
        {
            return Admission::Deferred {
                until: now.plus(Duration::from_secs(1)),
            };
        }
        self.spent.push(Spend {
            at: now,
            account: account.clone(),
            units: cost.0,
        });
        self.day_units += cost_units;
        self.next_lease += 1;
        let lease = Lease(self.next_lease);
        self.leases.insert(
            lease,
            Held {
                account: account.clone(),
                since: now,
            },
        );
        Admission::Granted(lease)
    }

    /// Release a lease; record downloaded bytes against the daily budget.
    pub fn release(&mut self, lease: Lease, bytes: u64, now: Timestamp) {
        self.roll(now);
        if let Some(held) = self.leases.remove(&lease) {
            *self.day_bytes.entry(held.account).or_default() += bytes;
        }
    }

    /// Pause an account or the project after Gmail pushed back. Never
    /// shortens an existing pause. Returns the deadline in effect.
    pub fn cool_down(&mut self, cooldown: crate::retry::Cooldown) -> Timestamp {
        if let Some(existing) = self
            .cooldowns
            .iter_mut()
            .find(|c| c.target == cooldown.target)
        {
            existing.until = existing.until.max(cooldown.until);
            existing.until
        } else {
            let until = cooldown.until;
            self.cooldowns.push(cooldown);
            until
        }
    }

    /// Leases older than this are assumed dead and released.
    pub fn expire_leases(&mut self, now: Timestamp, max_age: Duration) {
        self.leases
            .retain(|_, held| now.since(held.since) < max_age);
    }

    fn roll(&mut self, now: Timestamp) {
        let window_start = now.minus(MINUTE);
        self.spent.retain(|s| s.at > window_start);
        self.cooldowns.retain(|c| c.until > now);
        let day = now.0.div_euclid(DAY_MILLIS);
        if day != self.day {
            self.day = day;
            self.day_units = 0;
            self.day_bytes.clear();
        }
    }

    fn cooldown_for(&self, account: &AccountId, now: Timestamp) -> Option<Timestamp> {
        self.cooldowns
            .iter()
            .filter(|c| c.until > now)
            .filter(|c| match &c.target {
                crate::retry::QuotaTarget::Project => true,
                crate::retry::QuotaTarget::Account(a) => a == account,
            })
            .map(|c| c.until)
            .max()
    }

    /// When the oldest spend in the window (for `account`, or the project)
    /// leaves it.
    fn window_frees(&self, account: Option<&AccountId>, now: Timestamp) -> Timestamp {
        self.spent
            .iter()
            .filter(|s| account.is_none_or(|a| &s.account == a))
            .map(|s| s.at.plus(MINUTE))
            .min()
            .unwrap_or(now)
            .max(now.plus(Duration::from_millis(1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{HistoryId, MessageId};
    use crate::outbox::ModifyGroup;

    fn modify(n: usize) -> GmailRequest {
        GmailRequest::Modify(ModifyGroup {
            messages: (0..n).map(|i| MessageId(i.to_string())).collect(),
            add: Vec::new(),
            remove: Vec::new(),
            versions: Vec::new(),
        })
    }

    #[test]
    fn label_write_costs_follow_gmails_quota_table() {
        use crate::gmail::{LabelListVisibility, LabelPatch, MessageListVisibility, NewLabel};
        use crate::ids::LabelId;
        let create = GmailRequest::CreateLabel(NewLabel {
            name: "Work".into(),
            list_visibility: LabelListVisibility::Show,
            message_visibility: MessageListVisibility::Show,
            color: None,
        });
        // labels.patch is not in Google's table: labels.update's cost.
        let patch = GmailRequest::PatchLabel {
            id: LabelId("Label_1".into()),
            patch: LabelPatch::default(),
        };
        let delete = GmailRequest::DeleteLabel {
            id: LabelId("Label_1".into()),
        };
        for request in [create, patch, delete] {
            assert_eq!(cost(&request), QuotaUnits(5), "{request:?}");
        }
        assert_eq!(cost(&GmailRequest::ListLabels), QuotaUnits(1));
    }

    #[test]
    fn costs_follow_gmails_quota_table() {
        assert_eq!(cost(&GmailRequest::GetProfile), QuotaUnits(1));
        assert_eq!(
            cost(&GmailRequest::ListHistory {
                start: HistoryId(1),
                page_token: None
            }),
            QuotaUnits(2)
        );
        assert_eq!(
            cost(&GmailRequest::ImportMessage {
                id: MessageId("m".into())
            }),
            QuotaUnits(60)
        );
    }

    #[test]
    fn search_costs_follow_gmails_quota_table() {
        let search = |scope| {
            let query = crate::search::SearchQuery::new("from:alice");
            let account = AccountId("a@example.com".into());
            GmailRequest::Search(query.request(&account, scope).expect("a request"))
        };
        assert_eq!(cost(&search(SearchScope::Messages)), QuotaUnits(5));
        assert_eq!(cost(&search(SearchScope::Threads)), QuotaUnits(10));
        let label = crate::ids::LabelId("Label_1".into());
        assert_eq!(cost(&GmailRequest::GetLabel { id: label }), QuotaUnits(1));
    }

    fn account(name: &str) -> AccountId {
        AccountId(format!("{name}@example.com"))
    }

    #[test]
    fn backfill_leaves_headroom_for_live_and_foreground() {
        let budget = Budget {
            account_per_minute: 100,
            ..Budget::GMAIL_DEFAULT
        };
        let mut ledger = Ledger::default();
        let a = account("a");
        let now = Timestamp(1_000_000);
        for _ in 0..7 {
            let Admission::Granted(lease) =
                ledger.admit(&budget, &a, QuotaUnits(10), Priority::Backfill, now)
            else {
                panic!("backfill within its share");
            };
            ledger.release(lease, 0, now);
        }
        assert!(matches!(
            ledger.admit(&budget, &a, QuotaUnits(10), Priority::Backfill, now),
            Admission::Deferred { .. }
        ));
        assert!(matches!(
            ledger.admit(&budget, &a, QuotaUnits(10), Priority::Live, now),
            Admission::Granted(_)
        ));
        assert!(matches!(
            ledger.admit(&budget, &a, QuotaUnits(10), Priority::Foreground, now),
            Admission::Granted(_)
        ));
    }

    #[test]
    fn the_window_rolls_and_accounts_are_separate() {
        let budget = Budget {
            account_per_minute: 10,
            ..Budget::GMAIL_DEFAULT
        };
        let mut ledger = Ledger::default();
        let now = Timestamp(1_000_000);
        assert!(matches!(
            ledger.admit(
                &budget,
                &account("a"),
                QuotaUnits(10),
                Priority::Foreground,
                now
            ),
            Admission::Granted(_)
        ));
        let Admission::Deferred { until } = ledger.admit(
            &budget,
            &account("a"),
            QuotaUnits(1),
            Priority::Foreground,
            now,
        ) else {
            panic!("over budget");
        };
        assert_eq!(until, now.plus(MINUTE));
        assert!(matches!(
            ledger.admit(
                &budget,
                &account("b"),
                QuotaUnits(10),
                Priority::Foreground,
                now
            ),
            Admission::Granted(_)
        ));
        assert!(matches!(
            ledger.admit(
                &budget,
                &account("a"),
                QuotaUnits(1),
                Priority::Foreground,
                until
            ),
            Admission::Granted(_)
        ));
    }

    #[test]
    fn cooldowns_and_concurrency_defer() {
        let budget = Budget {
            account_concurrency: 1,
            ..Budget::GMAIL_DEFAULT
        };
        let mut ledger = Ledger::default();
        let a = account("a");
        let now = Timestamp(1_000_000);
        let Admission::Granted(lease) =
            ledger.admit(&budget, &a, QuotaUnits(1), Priority::Foreground, now)
        else {
            panic!("granted");
        };
        assert!(matches!(
            ledger.admit(&budget, &a, QuotaUnits(1), Priority::Foreground, now),
            Admission::Deferred { .. }
        ));
        ledger.release(lease, 10, now);
        ledger.cool_down(crate::retry::Cooldown {
            target: crate::retry::QuotaTarget::Account(a.clone()),
            until: now.plus(Duration::from_secs(30)),
        });
        assert_eq!(
            ledger.admit(&budget, &a, QuotaUnits(1), Priority::Foreground, now),
            Admission::Deferred {
                until: now.plus(Duration::from_secs(30))
            }
        );
    }

    #[test]
    fn purge_costs_follow_gmails_quota_table() {
        let ids =
            |n: usize| -> Vec<MessageId> { (0..n).map(|i| MessageId(format!("m{i}"))).collect() };
        let total = |n: usize| -> u32 {
            crate::purge::delete_requests(&ids(n))
                .iter()
                .map(|request| cost(request).0)
                .sum()
        };
        assert_eq!(
            cost(&GmailRequest::DeleteMessage {
                id: MessageId("m".into())
            }),
            QuotaUnits(10)
        );
        assert_eq!(
            cost(&GmailRequest::BatchDeleteMessages { ids: ids(1_000) }),
            QuotaUnits(50)
        );
        // Below the threshold each id is its own messages.delete.
        assert_eq!(PURGE_BATCH_THRESHOLD, 5);
        assert_eq!(total(1), 10);
        assert_eq!(total(4), 40);
        // From there on one batchDelete per 1,000 ids.
        assert_eq!(total(5), 50);
        assert_eq!(total(1_000), 50);
        assert_eq!(total(1_001), 100);
    }

    #[test]
    fn draft_costs_follow_gmails_quota_table() {
        use crate::drafts::Rfc822;
        use crate::ids::{DraftId, ThreadId};
        let id = || DraftId("r1".into());
        let raw = || Rfc822(b"X-Mailtide-Id: 0123456789abcdef\r\n\r\nhi".to_vec());
        let costs = [
            (
                GmailRequest::CreateDraft {
                    raw: raw(),
                    thread_id: None,
                },
                10,
            ),
            (
                GmailRequest::UpdateDraft {
                    id: id(),
                    raw: raw(),
                    thread_id: ThreadId("t".into()),
                },
                15,
            ),
            (GmailRequest::DeleteDraft { id: id() }, 10),
            (GmailRequest::SendDraft { id: id() }, 100),
            (GmailRequest::ListDrafts { page_token: None }, 5),
            (
                GmailRequest::GetDraft {
                    id: id(),
                    format: DraftFormat::Minimal,
                },
                20,
            ),
            (
                GmailRequest::GetDraft {
                    id: id(),
                    format: DraftFormat::Raw,
                },
                60,
            ),
        ];
        for (request, units) in costs {
            assert_eq!(cost(&request), QuotaUnits(units), "{request:?}");
        }
    }

    #[test]
    fn small_modify_groups_use_individual_modifies() {
        assert_eq!(cost(&modify(3)), QuotaUnits(15));
        assert_eq!(cost(&modify(9)), QuotaUnits(45));
        assert_eq!(cost(&modify(10)), QuotaUnits(50));
        assert_eq!(cost(&modify(1000)), QuotaUnits(50));
    }
}
