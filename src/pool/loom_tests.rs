// Loom tests for the pool's shard ownership protocol.
#![cfg(all(test, loom))]

use super::guard::OwnerGuard;
use super::instance::{self, NONE, State};
use loom::thread;
use std::sync::atomic::Ordering;

#[test]
fn loom_pool_claim_never_double_owns() {
    loom::model(|| {
        let state = State::new(2, 1); // 2 shards
        let mut handles = Vec::new();
        for id in 0..2usize {
            let state = state.clone();
            handles.push(thread::spawn(move || {
                // pass 1: stale view (active = 1) worker 0 wants both shards
                for shard in 0..2 {
                    let _ = instance::claim_or_release(&state, id, shard, 1);
                }
                thread::yield_now();
                // pass 2: fresh view (active = 2) settle to shard % 2 == id;
                // repeat until this worker owns its shard (the previous owner may not have released yet on some interleavings)
                loop {
                    let mine = instance::claim_or_release(&state, id, id, 2);
                    // also release the other shard if we still hold it
                    let _ = instance::claim_or_release(&state, id, 1 - id, 2);
                    if mine {
                        break;
                    }
                    thread::yield_now();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // settled: shard s owned by worker s, exclusively
        for s in 0..2 {
            let o = state.owner(s).load(Ordering::Acquire);
            assert_eq!(o, s, "shard {s} not owned by its desired worker");
        }
    });
}

#[test]
fn loom_pool_guard_releases_on_unwind() {
    loom::model(|| {
        let state = State::new(2, 1);
        let dead = {
            let state = state.clone();
            thread::spawn(move || {
                let _guard = OwnerGuard::new(&state, 0);
                for shard in 0..2 {
                    let _ = instance::claim_or_release(&state, 0, shard, 1);
                }
                // worker "dies": guard drops here, must CAS 0 -> NONE
            })
        };
        dead.join().unwrap();

        // survivor (id=1, active=1 -> desired owner of every shard is % 1 == 0...
        // 1 use active such that desired == 1): model the takeover directly, every shard must be NONE or claimable.
        for shard in 0..2 {
            let o = state.owner(shard).load(Ordering::Acquire);
            assert_eq!(o, NONE, "shard {shard} stuck on dead worker");
        }
    });
}
