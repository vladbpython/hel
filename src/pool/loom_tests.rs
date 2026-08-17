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
                // pass 1: stale view active = 1, => every shard's target owner is 0.
                for shard in 0..2 {
                    let _ = instance::claim_or_release_to(&state, id, shard, 0);
                }
                thread::yield_now();
                // pass 2: fresh view active = 2, => target owner is shard % 2 == shard.
                loop {
                    let mine = instance::claim_or_release_to(&state, id, id, id);
                    let _ = instance::claim_or_release_to(&state, id, 1 - id, 1 - id);
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
        for s in 0..2 {
            let o = state.owner(s).load(Ordering::Acquire);
            assert_eq!(o, s, "shard {s} not owned by its desired worker");
        }
    });
}
