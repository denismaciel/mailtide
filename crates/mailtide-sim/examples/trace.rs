//! Follow one id through a seed:
//! `cargo run -p mailtide-sim --example trace -- SEED STEPS TEXT`.

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let seed = args.get(1).and_then(|a| a.parse().ok()).unwrap_or(0);
    let steps = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(400);
    let grep = args.get(3).cloned().unwrap_or_default();
    let (result, matched) = mailtide_sim::debug_run(seed, steps, &grep);
    println!("passed: {} stats: {:?}", result.passed(), result.stats);
    for line in matched {
        let short: String = line.chars().take(400).collect();
        println!("{short}");
    }
}
