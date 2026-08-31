// Hash routing

#[inline(always)]
fn fnv1a(key: &str) -> usize {
    let h = key.bytes().fold(14695981039346656037u64, |h, b| {
        (h ^ b as u64).wrapping_mul(1099511628211)
    });
    (h.wrapping_mul(0x9E3779B97F4A7C15) >> 32) as usize
}

#[inline(always)]
fn xxhash(key: &str) -> usize {
    xxhash_rust::xxh3::xxh3_64(key.as_bytes()) as usize
}

/// Adaptive hash: ≤16 bytes → FNV-1a, >16 bytes → xxHash3.
#[inline(always)]
pub fn hash_key(key: &str) -> usize {
    if key.len() <= 16 {
        fnv1a(key)
    } else {
        xxhash(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn case_differing_keys_do_not_all_share_a_shard() {
        let pairs = [
            ("aapl", "AAPL"),
            ("msft", "MSFT"),
            ("googl", "GOOGL"),
            ("tsla", "TSLA"),
            ("nvda", "NVDA"),
            ("amzn", "AMZN"),
            ("meta", "META"),
            ("brk.b", "BRK.B"),
        ];
        for shards in [2usize, 4, 8, 16, 32] {
            let mask = shards - 1;
            let colliding = pairs
                .iter()
                .filter(|(a, b)| hash_key(a) & mask == hash_key(b) & mask)
                .count();
            assert!(
                colliding < pairs.len(),
                "all {} case-pairs collided at {shards} shards: the low bits \
                 still ignore the case bit",
                pairs.len()
            );
        }
    }

    #[test]
    fn short_key_distribution_stays_even() {
        for shards in [4usize, 16] {
            let mask = shards - 1;
            let mut counts = vec![0usize; shards];
            for i in 0..100_000u64 {
                counts[hash_key(&format!("user:{i}")) & mask] += 1;
            }
            let (min, max) = (*counts.iter().min().unwrap(), *counts.iter().max().unwrap());
            assert!(min > 0, "empty shard at {shards} shards");
            assert!(
                (max as f64) / (min as f64) < 1.5,
                "skewed spread at {shards} shards: min {min} max {max}"
            );
        }
    }
}
