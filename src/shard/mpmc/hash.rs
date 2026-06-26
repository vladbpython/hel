// Hash routing

#[inline(always)]
fn fnv1a(key: &str) -> usize {
    key.bytes().fold(14695981039346656037u64, |h, b| {
        (h ^ b as u64).wrapping_mul(1099511628211)
    }) as usize
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
