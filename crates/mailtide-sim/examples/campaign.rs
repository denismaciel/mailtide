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
        totals.deliveries += result.stats.deliveries;
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
