//! Tests ported by name from Zero's `gmail-quota-store.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.

use std::time::Duration;

use mailtide::command::GmailRequest;
use mailtide::ids::{AccountId, HistoryId, MessageId, Timestamp};
use mailtide::outbox::ModifyGroup;
use mailtide::quota::{Admission, Budget, Ledger, Priority, QuotaUnits, cost};
use mailtide::reconcile::ListScope;
use mailtide::retry::{Cooldown, QuotaTarget};

fn account(name: &str) -> AccountId {
    AccountId::parse(&format!("{name}@example.com")).expect("account")
}

fn budget(project: u32, mailbox: u32) -> Budget {
    Budget {
        account_per_minute: mailbox,
        project_per_minute: project,
        ..Budget::GMAIL_DEFAULT
    }
}

fn granted(admission: &Admission) -> bool {
    matches!(admission, Admission::Granted(_))
}

/// Zero: gmail-quota-store > shared Gmail quota > bounds concurrent requests, reserves foreground slots and recovers leaked leases after eviction
#[test]
fn shared_gmail_quota_bounds_concurrent_requests_reserves_foreground_slots_and_recovers_leaked_leases_after_eviction()
 {
    // Eight slots per account: backfill may hold half, foreground all.
    let budget = Budget {
        account_concurrency: 8,
        ..budget(1000, 1000)
    };
    let mut ledger = Ledger::default();
    let a = account("a");
    let t0 = Timestamp(0);
    let one = QuotaUnits(1);
    let leases: Vec<_> = (0..4)
        .map(
            |_| match ledger.admit(&budget, &a, one, Priority::Backfill, t0) {
                Admission::Granted(lease) => lease,
                Admission::Deferred { .. } => panic!("backfill within its slot share"),
            },
        )
        .collect();
    assert_eq!(
        ledger.admit(&budget, &a, one, Priority::Backfill, t0),
        Admission::Deferred {
            until: t0.plus(Duration::from_secs(1))
        },
        "a concurrency denial retries shortly"
    );
    assert!(granted(&ledger.admit(
        &budget,
        &a,
        one,
        Priority::Foreground,
        t0
    )));
    ledger.release(leases[0], 0, t0);
    assert!(
        !granted(&ledger.admit(&budget, &a, one, Priority::Backfill, t0)),
        "foreground still occupies a shared slot"
    );
    // The holders died without releasing: their leases expire.
    let later = Timestamp(120_000);
    ledger.expire_leases(later, Duration::from_mins(1));
    assert!(granted(&ledger.admit(
        &budget,
        &a,
        one,
        Priority::Backfill,
        later
    )));
}

/// Zero: gmail-quota-store > shared Gmail quota > shares the project budget across mailbox instances without partially charging a denial
#[test]
fn shared_gmail_quota_shares_the_project_budget_across_mailbox_instances_without_partially_charging_a_denial()
 {
    let budget = budget(100, 100);
    let mut ledger = Ledger::default();
    let t0 = Timestamp(0);
    let mut admit = |name: &str, units| {
        ledger.admit(
            &budget,
            &account(name),
            QuotaUnits(units),
            Priority::Foreground,
            t0,
        )
    };
    assert!(granted(&admit("a", 60)));
    // b's own budget is untouched, but the project's is not.
    assert_eq!(
        admit("b", 50),
        Admission::Deferred {
            until: Timestamp(60_000)
        }
    );
    // The denial charged nothing: exactly the remaining 40 units still fit.
    assert!(granted(&admit("b", 40)));
    assert!(!granted(&admit("c", 1)));
}

/// Zero: gmail-quota-store > shared Gmail quota > isolates mailbox limits and rolls usage out conservatively at minute boundaries
#[test]
fn shared_gmail_quota_isolates_mailbox_limits_and_rolls_usage_out_conservatively_at_minute_boundaries()
 {
    let budget = budget(100, 50);
    let mut ledger = Ledger::default();
    let mut admit = |name: &str, units, at| {
        ledger.admit(
            &budget,
            &account(name),
            QuotaUnits(units),
            Priority::Foreground,
            Timestamp(at),
        )
    };
    assert!(granted(&admit("a", 50, 999)));
    // Mailtide's window is exact: the spend at 999 leaves it at 60_999.
    assert_eq!(
        admit("a", 1, 60_000),
        Admission::Deferred {
            until: Timestamp(60_999)
        }
    );
    assert!(
        !granted(&admit("a", 1, 60_998)),
        "never admitted before the spend leaves the window"
    );
    assert!(granted(&admit("b", 10, 60_000)));
    assert!(granted(&admit("a", 50, 60_999)));
}

/// Zero: gmail-quota-store > shared Gmail quota > reserves headroom for live sync and user requests when backfill is full
#[test]
fn shared_gmail_quota_reserves_headroom_for_live_sync_and_user_requests_when_backfill_is_full() {
    let budget = budget(100, 100);
    let mut ledger = Ledger::default();
    let a = account("a");
    let mut admit = |units, priority| {
        granted(&ledger.admit(&budget, &a, QuotaUnits(units), priority, Timestamp(0)))
    };
    assert!(admit(70, Priority::Backfill));
    assert!(!admit(1, Priority::Backfill));
    assert!(admit(20, Priority::Live));
    assert!(!admit(1, Priority::Live));
    assert!(admit(10, Priority::Foreground));
    assert!(!admit(1, Priority::Foreground));
}

/// Zero: gmail-quota-store > shared Gmail quota > persists mailbox and project cooldowns and never shortens Retry-After
#[test]
fn shared_gmail_quota_persists_mailbox_and_project_cooldowns_and_never_shortens_retry_after() {
    // The ledger is in memory and shared by the host; persistence across
    // restarts is the host's concern, so only the cooldown rules are ported.
    let budget = budget(100, 50);
    let mut ledger = Ledger::default();
    let (a, b) = (account("a"), account("b"));
    let now = Timestamp(1000);
    ledger.cool_down(Cooldown {
        target: QuotaTarget::Account(a.clone()),
        until: Timestamp(300_000),
    });
    ledger.cool_down(Cooldown {
        target: QuotaTarget::Account(a.clone()),
        until: Timestamp(1000),
    });
    assert_eq!(
        ledger.admit(&budget, &a, QuotaUnits(1), Priority::Foreground, now),
        Admission::Deferred {
            until: Timestamp(300_000)
        }
    );
    assert!(granted(&ledger.admit(
        &budget,
        &b,
        QuotaUnits(1),
        Priority::Foreground,
        now
    )));
    ledger.cool_down(Cooldown {
        target: QuotaTarget::Project,
        until: Timestamp(400_000),
    });
    for who in [&b, &a] {
        assert_eq!(
            ledger.admit(&budget, who, QuotaUnits(1), Priority::Foreground, now),
            Admission::Deferred {
                until: Timestamp(400_000)
            },
            "the longest applicable pause wins"
        );
    }
}

/// Zero: gmail-quota-store > shared Gmail quota > charges current documented method costs, including uploads and attachments
#[test]
fn shared_gmail_quota_charges_current_documented_method_costs_including_uploads_and_attachments() {
    // Mailtide sends no threads, attachment, upload, People or OAuth calls
    // through the budget; its own requests follow SPEC §8.
    let id = || MessageId("m".into());
    let modify = |n: usize| {
        GmailRequest::Modify(ModifyGroup {
            messages: (0..n).map(|i| MessageId(i.to_string())).collect(),
            add: Vec::new(),
            remove: Vec::new(),
            versions: Vec::new(),
        })
    };
    let cases = [
        (GmailRequest::GetProfile, 1),
        (GmailRequest::ListLabels, 1),
        (
            GmailRequest::ListHistory {
                start: HistoryId(1),
                page_token: None,
            },
            2,
        ),
        (
            GmailRequest::ListMessages {
                scope: ListScope::All,
                page_token: None,
            },
            5,
        ),
        (GmailRequest::GetMessage { id: id() }, 20),
        (GmailRequest::ImportMessage { id: id() }, 60),
        (GmailRequest::Watch, 100),
        (modify(1), 5),
        (modify(10), 50),
    ];
    for (request, units) in cases {
        assert_eq!(cost(&request), QuotaUnits(units), "{request:?}");
    }
}
