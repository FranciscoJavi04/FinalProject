// generator.rs - builds the task list from a fixed seed (reproducible runs)

use crate::task::{Task, TaskKind};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub fn generate_workload(num_tasks: usize, seed: u64, mode: &str) -> Vec<Task> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut tasks = Vec::with_capacity(num_tasks);

    let mut current_arrival: u64 = 0;

    for i in 0..num_tasks {
        // pick task kind based on mode
        let kind = match mode {
            "balanced" => {
                if rng.gen_bool(0.5) { TaskKind::CPU } else { TaskKind::IO }
            }
            "stressed" => {
                // mostly CPU work — exercises the scheduler
                if rng.gen_bool(0.85) { TaskKind::CPU } else { TaskKind::IO }
            }
            _ => {
                // fall back to balanced
                if rng.gen_bool(0.5) { TaskKind::CPU } else { TaskKind::IO }
            }
        };

        // task duration in ms
        // CPU tasks tend to be shorter and tighter, IO tasks more variable
        let duration_ms: u64 = match kind {
            TaskKind::CPU => rng.gen_range(20..=120),
            TaskKind::IO => rng.gen_range(40..=200),
        };

        // arrival gap (ms between this and previous task)
        let gap: u64 = match mode {
            "balanced" => rng.gen_range(2..=15),
            "stressed" => {
                // burst arrivals: 30% of the time, no gap at all
                if rng.gen_bool(0.3) { 0 } else { rng.gen_range(1..=5) }
            }
            _ => rng.gen_range(2..=15),
        };
        current_arrival += gap;

        let mut t = Task::new(i as u64, current_arrival, kind, duration_ms);

        // 5% of tasks get bumped to high priority
        if rng.gen_bool(0.05) {
            t.priority = 1;
        }

        tasks.push(t);
    }

    tasks
}
