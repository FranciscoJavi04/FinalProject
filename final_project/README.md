# Concurrent Task Dispatcher

CSCI Final Project — a multi-threaded task dispatcher in Rust.

This program simulates a stream of tasks arriving over time, places them into
queues, and assigns them to a bounded pool of worker threads using a
weighted round-robin scheduling policy with anti-starvation aging.

---

## Build and Run

Requires Rust (tested on `rustc 1.75.0`).

```
cargo build --release

# run all four combinations back to back (default)
cargo run --release

# or pick a specific workload + policy
cargo run --release -- balanced fifo
cargo run --release -- balanced optimized
cargo run --release -- stressed fifo
cargo run --release -- stressed optimized
```

The workload is fully reproducible — fixed RNG seed (42) means every run
with the same arguments produces the same task set, so differences in the
numbers come from the OS scheduler, not the input.

---

## Configuration

The defaults live at the top of `src/main.rs`:

```
NUM_TASKS   = 500
NUM_WORKERS = 6
SEED        = 42
```

Change them and rebuild. Most aspects of the policy
(`STARVATION_MS`, the 3:2 round-robin ratio, the high-priority probability)
are also constants in their respective files.

---

## Design at a glance

There are four kinds of threads:

1. **Generator** — produces 500 pre-built tasks and releases them into
   the system over time, sleeping between sends to honor each task's
   `arrival_time_ms`.
2. **Dispatcher** — runs on the main thread. Maintains the two queues
   (`cpu_q`, `io_q`), tracks which workers are idle, and decides which
   task each idle worker gets next.
3. **Worker pool** — six workers. Each one gets its own command channel
   from the dispatcher, signals "ready" when it wants work, and forwards
   each completed task to the collector.
4. **Collector** — accumulates completed tasks into the `Metrics` struct.

Channels are used for *handoff* (generator → dispatcher, dispatcher →
worker, worker → collector). The only `Arc<Mutex<_>>` in the program
guards the `Metrics` struct, which the dispatcher and the collector both
write to.

The scheduler uses a **weighted round-robin (3 CPU : 2 IO)** policy with
two extras:

* **Aging** — if the front task of either queue has been waiting more
  than 250 ms, it gets dispatched next regardless of the round-robin
  counter. This stops the minority queue from starving when one class
  dominates.
* **Priority bypass** — about 5% of tasks are generated with priority 1
  and are pushed to the *front* of their queue at enqueue time, so they
  jump ahead of the FIFO order within their class.

---

## Experiments

Two workloads, both with 500 tasks, 6 workers, seed 42:

### A. Balanced

50/50 mix of CPU and IO tasks, arrival gaps 2–15 ms.

```
Total tasks completed   : 500   (245 CPU, 255 IO)
Makespan                : 8103 ms
Average wait            : 1576.46 ms
Average turnaround      : 1672.55 ms
Max wait                : 3845 ms
CPU avg wait            : 558.77 ms
IO  avg wait            : 2554.25 ms
Fairness gap |CPU-IO|   : 1995.48 ms
Max CPU queue length    : 56
Max IO  queue length    : 172
Utilization             : 98.7 %
```

### B. Stressed

85% CPU, 15% IO with bursty arrivals (30% chance of zero gap).

```
Total tasks completed   : 500   (431 CPU, 69 IO)
Makespan                : 6536 ms
Average wait            : 2703.27 ms
Average turnaround      : 2780.30 ms
Max wait                : 5318 ms
CPU avg wait            : 2854.78 ms
IO  avg wait            : 1756.93 ms
Fairness gap |CPU-IO|   : 1097.85 ms
Max CPU queue length    : 399
Max IO  queue length    : 31
Utilization             : 98.1 %
```

See the attached PDF report for the full discussion, including why the
scheduler ends up *less fair* under the balanced workload than the stressed one.

---

## File layout

```
final_project/
├── Cargo.toml
├── README.md         (this file)
└── src/
    ├── main.rs       CLI + experiment driver
    ├── task.rs       Task struct, TaskKind enum
    ├── generator.rs  Workload generation (seeded RNG)
    ├── dispatcher.rs run_simulation, dispatcher loop, pick_next policy
    ├── worker.rs     Worker thread loop
    └── metrics.rs    Metrics struct, summary printing
```

---

## Note on AI Use

I used Claude as a reference while building this — mostly to sanity-check
my channel design and talk through the aging logic. All code is mine and
I can walk through any of it at the demo.
