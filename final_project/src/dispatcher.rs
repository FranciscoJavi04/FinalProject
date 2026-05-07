// dispatcher.rs
// Spawns generator, workers, and collector threads, then runs the
// dispatcher loop on the calling thread.
//
// Policy::Fifo    - single queue, arrival order, no frills (baseline)
// Policy::Optimized - two queues (cpu/io), weighted round-robin 3:2,
//                    aging override at 250ms, priority bypass at enqueue

use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::metrics::Metrics;
use crate::task::{Task, TaskKind};
use crate::worker;

const STARVATION_MS: u64 = 250;
const TICK_SLEEP_MS: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    Fifo,
    Optimized,
}

impl Policy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Policy::Fifo => "FIFO",
            Policy::Optimized => "Optimized",
        }
    }
}

pub fn run_simulation(tasks: Vec<Task>, num_workers: usize, policy: Policy) -> Metrics {
    let sim_start = Instant::now();
    let metrics = Arc::new(Mutex::new(Metrics::new()));

    // ---- channels ----
    let (raw_tx, raw_rx) = mpsc::channel::<Task>();
    let (ready_tx, ready_rx) = mpsc::channel::<usize>();
    let (done_tx, done_rx) = mpsc::channel::<Task>();

    let mut worker_txs: Vec<Sender<Option<Task>>> = Vec::with_capacity(num_workers);
    let mut worker_handles = Vec::with_capacity(num_workers);

    // ---- spawn workers ----
    for i in 0..num_workers {
        let (wtx, wrx) = mpsc::channel::<Option<Task>>();
        worker_txs.push(wtx);

        let ready = ready_tx.clone();
        let done  = done_tx.clone();
        let h = thread::spawn(move || {
            worker::worker_loop(i, wrx, ready, done, sim_start);
        });
        worker_handles.push(h);
    }
    drop(ready_tx);
    drop(done_tx);

    // ---- spawn generator ----
    let gen_handle = {
        let raw_tx = raw_tx.clone();
        thread::spawn(move || {
            for t in tasks {
                let now = sim_start.elapsed().as_millis() as u64;
                if t.arrival_time_ms > now {
                    thread::sleep(Duration::from_millis(t.arrival_time_ms - now));
                }
                if raw_tx.send(t).is_err() {
                    return;
                }
            }
        })
    };
    drop(raw_tx);

    // ---- spawn collector ----
    let collector_handle = {
        let m = Arc::clone(&metrics);
        thread::spawn(move || {
            while let Ok(t) = done_rx.recv() {
                let mut g = m.lock().unwrap();
                g.completed.push(t);
            }
        })
    };

    // ---- run dispatcher on this thread ----
    {
        let m = Arc::clone(&metrics);
        dispatcher_loop(raw_rx, &worker_txs, ready_rx, m, sim_start, policy);
    }

    gen_handle.join().expect("generator thread panicked");
    for h in worker_handles {
        h.join().expect("worker thread panicked");
    }
    collector_handle.join().expect("collector thread panicked");

    let mut m = match Arc::try_unwrap(metrics) {
        Ok(mu) => mu.into_inner().expect("metrics mutex poisoned"),
        Err(_) => panic!("metrics still has multiple owners at end of run"),
    };
    m.end_wallclock_ms = sim_start.elapsed().as_millis() as u64;
    m
}

fn dispatcher_loop(
    raw_rx: Receiver<Task>,
    worker_txs: &[Sender<Option<Task>>],
    ready_rx: Receiver<usize>,
    metrics: Arc<Mutex<Metrics>>,
    sim_start: Instant,
    policy: Policy,
) {
    // FIFO uses fifo_q; Optimized uses cpu_q + io_q. The unused ones
    // just stay empty -- tiny memory cost, way simpler than two functions.
    let mut fifo_q: VecDeque<Task> = VecDeque::new();
    let mut cpu_q:  VecDeque<Task> = VecDeque::new();
    let mut io_q:   VecDeque<Task> = VecDeque::new();
    let mut idle:   VecDeque<usize> = VecDeque::new();
    let mut gen_done = false;
    let mut counter: u32 = 0;

    loop {
        // 1. drain any newly arrived tasks
        loop {
            match raw_rx.try_recv() {
                Ok(mut t) => {
                    t.enqueue_time_ms = Some(sim_start.elapsed().as_millis() as u64);
                    match policy {
                        Policy::Fifo => fifo_q.push_back(t),
                        Policy::Optimized => enqueue_optimized(&mut cpu_q, &mut io_q, t),
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    gen_done = true;
                    break;
                }
            }
        }

        // 2. update queue-length high-water marks
        {
            let mut m = metrics.lock().unwrap();
            match policy {
                Policy::Fifo => {
                    // For FIFO we only have one queue. Reuse the cpu field
                    // to record its high-water mark (a bit hacky, but the
                    // print_summary function knows about the policy).
                    if fifo_q.len() > m.max_queue_len_cpu {
                        m.max_queue_len_cpu = fifo_q.len();
                    }
                }
                Policy::Optimized => {
                    if cpu_q.len() > m.max_queue_len_cpu { m.max_queue_len_cpu = cpu_q.len(); }
                    if io_q.len()  > m.max_queue_len_io  { m.max_queue_len_io  = io_q.len(); }
                }
            }
        }

        // 3. drain ready workers
        while let Ok(w) = ready_rx.try_recv() {
            idle.push_back(w);
        }

        // 4. dispatch to idle workers
        while !idle.is_empty() {
            let next = match policy {
                Policy::Fifo => fifo_q.pop_front(),
                Policy::Optimized => pick_next_optimized(
                    &mut cpu_q, &mut io_q, &mut counter, sim_start,
                ),
            };

            match next {
                Some(t) => {
                    let w = idle.pop_front().unwrap();
                    let _ = worker_txs[w].send(Some(t));
                }
                None => break,
            }
        }

        // 5. termination check
        let queues_empty = match policy {
            Policy::Fifo => fifo_q.is_empty(),
            Policy::Optimized => cpu_q.is_empty() && io_q.is_empty(),
        };
        if gen_done && queues_empty {
            for tx in worker_txs {
                let _ = tx.send(None);
            }
            return;
        }

        // 6. brief sleep so we don't spin
        // TODO: replace with a recv_timeout on a unified event channel
        thread::sleep(Duration::from_millis(TICK_SLEEP_MS));
    }
}

fn enqueue_optimized(cpu_q: &mut VecDeque<Task>, io_q: &mut VecDeque<Task>, t: Task) {
    let high_pri = t.priority > 0;
    match t.kind {
        TaskKind::CPU => {
            if high_pri { cpu_q.push_front(t); } else { cpu_q.push_back(t); }
        }
        TaskKind::IO => {
            if high_pri { io_q.push_front(t); } else { io_q.push_back(t); }
        }
    }
}

// Optimized pick_next: aging override first, then weighted round-robin (3:2).
fn pick_next_optimized(
    cpu_q: &mut VecDeque<Task>,
    io_q:  &mut VecDeque<Task>,
    counter: &mut u32,
    sim_start: Instant,
) -> Option<Task> {
    let now = sim_start.elapsed().as_millis() as u64;

    // ---- aging override ----
    if let Some(front) = cpu_q.front() {
        let waited = now.saturating_sub(front.enqueue_time_ms.unwrap_or(now));
        if waited > STARVATION_MS {
            return cpu_q.pop_front();
        }
    }
    if let Some(front) = io_q.front() {
        let waited = now.saturating_sub(front.enqueue_time_ms.unwrap_or(now));
        if waited > STARVATION_MS {
            return io_q.pop_front();
        }
    }

    // ---- weighted round-robin (3 CPU : 2 IO) ----
    let prefer_cpu = (*counter % 5) < 3;
    *counter = counter.wrapping_add(1);

    if prefer_cpu {
        if let Some(t) = cpu_q.pop_front() { return Some(t); }
        if let Some(t) = io_q.pop_front()  { return Some(t); }
    } else {
        if let Some(t) = io_q.pop_front()  { return Some(t); }
        if let Some(t) = cpu_q.pop_front() { return Some(t); }
    }
    None
}
