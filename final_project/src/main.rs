// main.rs
// CLI entry. Usage:
//   cargo run --release                          (default: runs all 4 combos)
//   cargo run --release -- compare               (same as default)
//   cargo run --release -- balanced fifo
//   cargo run --release -- balanced optimized
//   cargo run --release -- stressed fifo
//   cargo run --release -- stressed optimized

mod task;
mod generator;
mod dispatcher;
mod worker;
mod metrics;

use std::env;

use crate::dispatcher::Policy;

const NUM_TASKS:   usize = 500;
const NUM_WORKERS: usize = 6;
const SEED:        u64   = 42;

fn main() {
    let args: Vec<String> = env::args().collect();

    // No arg, or "compare": run everything in order.
    if args.len() < 2 || args[1] == "compare" {
        run_one("balanced", Policy::Fifo);
        run_one("balanced", Policy::Optimized);
        run_one("stressed", Policy::Fifo);
        run_one("stressed", Policy::Optimized);
        return;
    }

    if args.len() < 3 {
        eprintln!("Usage:");
        eprintln!("  cargo run --release                              (compare all 4)");
        eprintln!("  cargo run --release -- <workload> <policy>");
        eprintln!("    <workload> = balanced | stressed");
        eprintln!("    <policy>   = fifo | optimized");
        std::process::exit(1);
    }

    let workload = args[1].as_str();
    let policy = match args[2].as_str() {
        "fifo" => Policy::Fifo,
        "optimized" | "opt" => Policy::Optimized,
        other => {
            eprintln!("Unknown policy: {}", other);
            std::process::exit(1);
        }
    };

    if workload != "balanced" && workload != "stressed" {
        eprintln!("Unknown workload: {}", workload);
        std::process::exit(1);
    }

    run_one(workload, policy);
}

fn run_one(workload: &str, policy: Policy) {
    println!(">>> Experiment: workload={} policy={} (n={}, workers={}, seed={})",
             workload, policy.as_str(), NUM_TASKS, NUM_WORKERS, SEED);

    let tasks = generator::generate_workload(NUM_TASKS, SEED, workload);
    let m = dispatcher::run_simulation(tasks, NUM_WORKERS, policy);
    m.print_summary(workload, policy, NUM_WORKERS);
}
