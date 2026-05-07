// task.rs - Task struct and TaskKind enum

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    CPU,
    IO,
}

impl TaskKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskKind::CPU => "CPU",
            TaskKind::IO => "IO",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: u64,
    pub arrival_time_ms: u64,   // ms from simulation start
    pub kind: TaskKind,
    pub duration: Duration,     // how long the work "takes"
    pub priority: u8,           // 0 = normal, 1 = high (jumps the queue)

    // Filled in as the task moves through the system.
    pub enqueue_time_ms: Option<u64>,
    pub start_time_ms:   Option<u64>,
    pub finish_time_ms:  Option<u64>,
}

impl Task {
    pub fn new(id: u64, arrival_time_ms: u64, kind: TaskKind, duration_ms: u64) -> Self {
        Task {
            id,
            arrival_time_ms,
            kind,
            duration: Duration::from_millis(duration_ms),
            priority: 0,
            enqueue_time_ms: None,
            start_time_ms:   None,
            finish_time_ms:  None,
        }
    }

    /// Time the task spent waiting between arrival and starting execution.
    pub fn wait_time_ms(&self) -> u64 {
        match self.start_time_ms {
            Some(s) => s.saturating_sub(self.arrival_time_ms),
            None => 0,
        }
    }

    /// Time from arrival to completion.
    pub fn turnaround_time_ms(&self) -> u64 {
        match self.finish_time_ms {
            Some(f) => f.saturating_sub(self.arrival_time_ms),
            None => 0,
        }
    }
}
