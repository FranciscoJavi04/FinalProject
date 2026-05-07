// metrics.rs

use crate::dispatcher::Policy;
use crate::task::{Task, TaskKind};

pub struct Metrics {
    pub completed: Vec<Task>,
    pub start_wallclock_ms: u64,
    pub end_wallclock_ms:   u64,
    pub max_queue_len_cpu:  usize,  // also reused as "FIFO queue length"
    pub max_queue_len_io:   usize,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            completed: Vec::new(),
            start_wallclock_ms: 0,
            end_wallclock_ms:   0,
            max_queue_len_cpu:  0,
            max_queue_len_io:   0,
        }
    }

    pub fn print_summary(&self, label: &str, policy: Policy, num_workers: usize) {
        let n = self.completed.len();
        if n == 0 {
            println!("[{}] No tasks completed -- something went wrong.", label);
            return;
        }

        let total_wait: u64 = self.completed.iter().map(|t| t.wait_time_ms()).sum();
        let total_turn: u64 = self.completed.iter().map(|t| t.turnaround_time_ms()).sum();
        let max_wait = self.completed.iter().map(|t| t.wait_time_ms()).max().unwrap_or(0);

        let cpu_count = self.completed.iter().filter(|t| t.kind == TaskKind::CPU).count();
        let io_count  = self.completed.iter().filter(|t| t.kind == TaskKind::IO).count();

        let cpu_avg_wait: f64 = if cpu_count > 0 {
            self.completed.iter()
                .filter(|t| t.kind == TaskKind::CPU)
                .map(|t| t.wait_time_ms() as f64)
                .sum::<f64>() / cpu_count as f64
        } else { 0.0 };

        let io_avg_wait: f64 = if io_count > 0 {
            self.completed.iter()
                .filter(|t| t.kind == TaskKind::IO)
                .map(|t| t.wait_time_ms() as f64)
                .sum::<f64>() / io_count as f64
        } else { 0.0 };

        let makespan = self.end_wallclock_ms.saturating_sub(self.start_wallclock_ms);

        // Average CPU usage = (sum of all task durations) / (workers * makespan).
        // The amendment specifically asks for this.
        let total_busy_ms: u64 = self.completed.iter()
            .map(|t| t.duration.as_millis() as u64)
            .sum();
        let theoretical = (makespan.max(1)) * (num_workers as u64);
        let avg_cpu_usage = (total_busy_ms as f64) / (theoretical as f64) * 100.0;

        println!("===== Results: {} ({}) =====", label, policy.as_str());
        println!("Total runtime / makespan (ms) : {}", makespan);
        println!("Total tasks completed         : {}", n);
        println!("  CPU tasks completed         : {}", cpu_count);
        println!("  IO  tasks completed         : {}", io_count);
        println!("Average wait time (ms)        : {:.2}", total_wait as f64 / n as f64);
        println!("Average turnaround (ms)       : {:.2}", total_turn as f64 / n as f64);
        println!("Max wait time (ms)            : {}", max_wait);
        println!("CPU avg wait (ms)             : {:.2}", cpu_avg_wait);
        println!("IO  avg wait (ms)             : {:.2}", io_avg_wait);
        println!("Fairness gap |CPU-IO|         : {:.2}", (cpu_avg_wait - io_avg_wait).abs());
        match policy {
            Policy::Fifo => {
                println!("Max queue length (single)     : {}", self.max_queue_len_cpu);
            }
            Policy::Optimized => {
                println!("Max CPU queue length          : {}", self.max_queue_len_cpu);
                println!("Max IO  queue length          : {}", self.max_queue_len_io);
            }
        }
        println!("Average CPU usage             : {:.1} %", avg_cpu_usage);
        println!();
    }
}
