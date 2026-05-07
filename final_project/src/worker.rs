// worker.rs
// Each worker signals ready, blocks waiting for a task, sleeps to simulate
// work, then signals ready again. None from the dispatcher means shutdown.
// Using per-worker channels so the dispatcher can target specific workers
// instead of sharing one receiver behind a mutex.

use std::sync::mpsc::{Receiver, Sender};
use std::time::Instant;

use crate::task::Task;

pub fn worker_loop(
    id: usize,
    rx: Receiver<Option<Task>>,
    ready_tx: Sender<usize>,
    done_tx: Sender<Task>,
    sim_start: Instant,
) {
    // announce that we're ready for our first task
    if ready_tx.send(id).is_err() {
        return; // dispatcher already gone
    }

    loop {
        match rx.recv() {
            Ok(Some(mut t)) => {
                t.start_time_ms = Some(now_ms(sim_start));
                // simulate the task running. In a real system we'd be doing
                // CPU or IO work here -- the sleep is our stand-in.
                std::thread::sleep(t.duration);
                t.finish_time_ms = Some(now_ms(sim_start));

                // hand off the completed task. If the collector is gone
                // we just drop the result and exit.
                if done_tx.send(t).is_err() {
                    return;
                }

                // ready for the next one
                if ready_tx.send(id).is_err() {
                    return;
                }
            }
            Ok(None) => {
                // shutdown signal from dispatcher
                return;
            }
            Err(_) => {
                // dispatcher dropped the sender -- treat as shutdown
                return;
            }
        }
    }
}

fn now_ms(sim_start: Instant) -> u64 {
    sim_start.elapsed().as_millis() as u64
}
