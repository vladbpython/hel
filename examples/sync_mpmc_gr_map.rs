use hel::channel::{
    errors::*,
    mpmc::{ShardGroupCase, shard_group},
    nearest_power_of_two,
};
use std::collections::HashMap;
use std::thread;

// N producers → handle (resolve ONCE) → S shards → S consumers.
// The grouping is set though a HashMap (character map → group) constructed
// LOGIC (sector classifier). Unfamiliar characters are IGNORED:
// classifier returns None → the character is not included in the map → handle() == None
// producer skips it. This way, the symbol will not silently go to someone else’s shard.

const CAPACITY: usize = nearest_power_of_two(256);
const NUM_GROUPS: usize = 4; // tech / auto / crypto / media

/// Classifier: symbol → Some(group index) or None for unfamiliar.
/// tech=0, auto=1, crypto=2, media=3. Unfamiliar → None (ignore).
fn sector_of(sym: &str) -> Option<usize> {
    match sym {
        "AAPL" | "MSFT" | "GOOG" | "ORCL" | "INTC" | "AMD" | "NVDA" => Some(0), // tech
        "TSLA" | "UBER" | "LYFT" => Some(1),                                    // auto/mobility
        "BTC" | "ETH" => Some(2),                                               // crypto
        "META" | "SNAP" | "NFLX" | "AMZN" => Some(3),                           // media/consumer
        _ => None, // unknown ignore
    }
}

fn main() {
    let symbols = [
        "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AMD", "INTC", "NFLX", "BTC",
        "ETH", "ORCL", "UBER", "LYFT", "SNAP",
    ];

    //cbuild a map symbol → group with a classifier.
    //cfilter_map filters out None.
    let route: HashMap<String, usize> = symbols
        .iter()
        .filter_map(|&s| sector_of(s).map(|g| (s.to_string(), g)))
        .collect();

    // create a ShardGroup from the finished map + number of groups
    let (tx, rx) = shard_group::<u64, CAPACITY>(ShardGroupCase::Map {
        map: route,
        num_groups: NUM_GROUPS,
    });

    // consumer thread for each shard. Everyone reads their shard.
    let consumers: Vec<_> = rx
        .into_receivers()
        .into_iter()
        .enumerate()
        .map(|(id, r)| {
            thread::spawn(move || {
                let mut count = 0u64;
                loop {
                    match r.recv() {
                        Ok(v) => count += v,
                        Err(RecvError::Disconnected) => break,
                        Err(RecvError::TimeOut(_)) => unreachable!(),
                    }
                }
                println!("[shard {id}] total = {count}");
            })
        })
        .collect();

    let producers: Vec<_> = (0..4)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                let handles: Vec<_> = symbols
                    .iter()
                    .filter_map(|&s| tx.handle(s).map(|h| (s, h)))
                    .collect();

                for i in 0..1000u64 {
                    let idx = (p * 250 + i as usize) % handles.len();
                    let (_sym, h) = handles[idx];
                    tx.send(h, i).unwrap();
                }
            })
        })
        .collect();

    drop(tx);

    for h in producers {
        h.join().unwrap();
    }
    for h in consumers {
        h.join().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sector_classification() {
        assert_eq!(sector_of("AAPL"), Some(0)); // tech
        assert_eq!(sector_of("TSLA"), Some(1)); // auto
        assert_eq!(sector_of("BTC"), Some(2)); // crypto
        assert_eq!(sector_of("NFLX"), Some(3)); // media
        assert_eq!(sector_of("UNKNOWN"), None); // ignore
    }

    #[test]
    fn unknown_symbols_excluded_from_route() {
        let symbols = ["AAPL", "BTC", "UNKNOWN", "FOOBAR"];
        let route: HashMap<String, usize> = symbols
            .iter()
            .filter_map(|&s| sector_of(s).map(|g| (s.to_string(), g)))
            .collect();
        assert_eq!(route.len(), 2);
        assert!(route.contains_key("AAPL"));
        assert!(route.contains_key("BTC"));
        assert!(!route.contains_key("UNKNOWN"));
        assert!(!route.contains_key("FOOBAR"));
    }
}
