//! Run many seeds: `cargo run --release -p mailtide-sim --example campaign -- FROM TO STEPS`.

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let from: u64 = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(0);
    let to: u64 = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(1_000);
    let steps: u64 = args.get(3).and_then(|a| a.parse().ok()).unwrap_or(1_000);
    let mut failed = 0;
    let mut totals = mailtide_sim::Stats::default();
    for seed in from..to {
        let result = mailtide_sim::run(seed, steps);
        totals.commits += result.stats.commits;
        totals.rebuilds += result.stats.rebuilds;
        totals.expiries += result.stats.expiries;
        totals.user_ops += result.stats.user_ops;
        totals.rejections += result.stats.rejections;
        totals.messages += result.stats.messages;
        totals.threads += result.stats.threads;
        totals.deliveries += result.stats.deliveries;
        totals.label_edits += result.stats.label_edits;
        totals.label_writes_recorded += result.stats.label_writes_recorded;
        totals.label_writes_lost += result.stats.label_writes_lost;
        totals.quiet_label_deletes += result.stats.quiet_label_deletes;
        totals.refused_deliveries += result.stats.refused_deliveries;
        totals.bad_listings += result.stats.bad_listings;
        totals.fenced_listings += result.stats.fenced_listings;
        totals.forgotten_deletions += result.stats.forgotten_deletions;
        totals.label_listings += result.stats.label_listings;
        totals.opens += result.stats.opens;
        totals.raised += result.stats.raised;
        totals.draft_writes += result.stats.draft_writes;
        totals.drafts_sent += result.stats.drafts_sent;
        totals.feed_pages_split += result.stats.feed_pages_split;
        totals.compactions += result.stats.compactions;
        totals.compacted += result.stats.compacted;
        totals.consumers_cut_off += result.stats.consumers_cut_off;
        totals.facts_refreshed += result.stats.facts_refreshed;
        totals.purges += result.stats.purges;
        totals.purge_deletes += result.stats.purge_deletes;
        totals.purged += result.stats.purged;
        totals.purge_batches += result.stats.purge_batches;
        totals.purge_restores_between_requests += result.stats.purge_restores_between_requests;
        totals.purge_token_failures += result.stats.purge_token_failures;
        totals.purge_release_failures += result.stats.purge_release_failures;
        totals.purge_partial_refusals += result.stats.purge_partial_refusals;
        totals.purge_late_confirmations += result.stats.purge_late_confirmations;
        if !result.passed() {
            failed += 1;
            println!(
                "FAIL seed {seed}: {:?} {:?}",
                result.convergence,
                result.violations.first()
            );
        }
    }
    println!("seeds {from}..{to} x {steps} steps: {failed} failed; totals {totals:?}");
    if failed > 0 {
        std::process::exit(1);
    }
}
