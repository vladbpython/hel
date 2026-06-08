// Convenience macros for creating channels.
// Capacity is automatically rounded up to the nearest power of two.
// All macros do this rounding at compile time via const expressions.
// 500 → rounds up to 512 (2^9)
// let (tx, rx) = bounded!(u64, 500);
// 430 → rounds up to 512
// let (tx, rx) = bounded_concurrent!(String, 430);
// Sharded: 300 → 512, num_shards = num_cpus::get() → nearest power of two
// let (tx, rx) = sharded!(u64, 300);
// Sharded with explicit shard count: 100 → 128, 6 → 8
// let (tx, rx) = sharded!(u64, 100, 6);

/// Creates a sharded channel with an optional policy selection.
///
/// Call options
///
/// ```
/// use hel::mpmc;
/// // RoundRobin (default): even load, no key
/// let (tx, rx) = mpmc!(u64, 256); //num_shards = num_cpus
/// let (tx, rx) = mpmc!(u64, 256, RoundRobin); //explicitly RoundRobin, num_cpus
/// let (tx, rx) = mpmc!(u64, 256, RoundRobin, 8); //RoundRobin, 8 shards
///
/// // ByKey: hash(key) → ordering per symbol guaranteed
/// let (tx, rx) = mpmc!(u64, 256, ByKey); //num_shards = num_cpus
/// let (tx, rx) = mpmc!(u64, 256, ByKey, 8); //ByKey, 8 shards
/// ```
#[macro_export]
macro_rules! mpmc {
    // RoundRobin: auto shards (default)
    ($t:ty, $cap:expr) => {{
        const __CAP: usize = {
            let c = $cap as usize;
            assert!(c > 0, "capacity must be > 0");
            c.next_power_of_two()
        };
        let num_shards = $crate::channel::nearest_power_of_two(::num_cpus::get());
        $crate::channel::mpmc::round_robin::<$t, __CAP>(num_shards)
    }};

    // RoundRobin: auto shards (explicit)
    ($t:ty, $cap:expr, RoundRobin) => {{
        const __CAP: usize = {
            let c = $cap as usize;
            assert!(c > 0, "capacity must be > 0");
            c.next_power_of_two()
        };
        let num_shards = $crate::channel::nearest_power_of_two(::num_cpus::get());
        $crate::channel::mpmc::round_robin::<$t, __CAP>(num_shards)
    }};

    // RoundRobin: explicit shards
    ($t:ty, $cap:expr, RoundRobin, $shards:expr) => {{
        const __CAP: usize = {
            let c = $cap as usize;
            assert!(c > 0, "capacity must be > 0");
            c.next_power_of_two()
        };
        let num_shards = $crate::channel::nearest_power_of_two($shards as usize);
        $crate::channel::mpmc::round_robin::<$t, __CAP>(num_shards)
    }};

    // ByKey: auto shards
    ($t:ty, $cap:expr, ByKey) => {{
        const __CAP: usize = {
            let c = $cap as usize;
            assert!(c > 0, "capacity must be > 0");
            c.next_power_of_two()
        };
        let num_shards = $crate::channel::nearest_power_of_two(::num_cpus::get());
        $crate::channel::mpmc::shard_key::<$t, __CAP>(num_shards)
    }};

    // ByKey: explicit shards
    ($t:ty, $cap:expr, ByKey, $shards:expr) => {{
        const __CAP: usize = {
            let c = $cap as usize;
            assert!(c > 0, "capacity must be > 0");
            c.next_power_of_two()
        };
        let num_shards = $crate::channel::nearest_power_of_two($shards as usize);
        $crate::channel::mpmc::shard_key::<$t, __CAP>(num_shards)
    }};
}

/// Creates `sharded_spsc()` builder N independent SPSC channels.
/// Maximum throughput with 1 producer per shard.
/// Returns `SpscSharded` builder with `into_pairs()` and `take_pair()`.
/// Call options
///
/// ```
/// use hel::spsc;
/// // 1) num_shards = num_cpus
/// let ch = spsc!(u64, 256);
/// for (id, tx, rx) in ch.into_pairs() { /*... */}
///
/// // 2) Explicit number of shards (does not have to be a power of 2)
/// let mut ch = spsc!(u64, 256, 4);
/// let (tx0, rx0) = ch.take_pair(0).unwrap();
/// let (tx1, rx1) = ch.take_pair(1).unwrap();
/// ```
#[macro_export]
macro_rules! spsc {
    ($t:ty, $cap:expr) => {{
        const __CAP: usize = {
            let c = $cap as usize;
            assert!(c > 0, "capacity must be > 0");
            c.next_power_of_two()
        };
        // sharded_spsc does not require power 2 num_cpus directly
        $crate::channel::spsc::SpscShard::<$t, __CAP>::new(::num_cpus::get())
    }};
    ($t:ty, $cap:expr, $shards:expr) => {{
        const __CAP: usize = {
            let c = $cap as usize;
            assert!(c > 0, "capacity must be > 0");
            c.next_power_of_two()
        };
        $crate::channel::spsc::SpscShard::<$t, __CAP>::new($shards as usize)
    }};
}

#[cfg(test)]
mod tests {
    use crate::internal_channel::nearest_power_of_two;

    // nearest_power_of_two

    #[test]
    fn pow2_exact() {
        assert_eq!(nearest_power_of_two(1), 1);
        assert_eq!(nearest_power_of_two(2), 2);
        assert_eq!(nearest_power_of_two(4), 4);
        assert_eq!(nearest_power_of_two(128), 128);
        assert_eq!(nearest_power_of_two(1024), 1024);
    }

    #[test]
    fn pow2_rounds_up() {
        assert_eq!(nearest_power_of_two(3), 4);
        assert_eq!(nearest_power_of_two(5), 8);
        assert_eq!(nearest_power_of_two(100), 128);
        assert_eq!(nearest_power_of_two(300), 512);
        assert_eq!(nearest_power_of_two(430), 512);
        assert_eq!(nearest_power_of_two(500), 512);
        assert_eq!(nearest_power_of_two(513), 1024);
        assert_eq!(nearest_power_of_two(1000), 1024);
    }

    #[test]
    fn pow2_zero_returns_one() {
        assert_eq!(nearest_power_of_two(0), 1);
    }

    // sharded! policy arms

    #[test]
    fn sharded_macro_round_robin_default() {
        let (tx, _rx) = mpmc!(u64, 128);
        assert!(tx.shards().is_power_of_two());
        tx.try_send(1).unwrap();
    }

    #[test]
    fn sharded_macro_round_robin_explicit() {
        let (tx, _rx) = mpmc!(u64, 128, RoundRobin);
        assert!(tx.shards().is_power_of_two());
        tx.try_send(1).unwrap();
    }

    #[test]
    fn sharded_macro_round_robin_with_shards() {
        let (tx, _rx) = mpmc!(u64, 128, RoundRobin, 4);
        assert_eq!(tx.shards(), 4);
        tx.try_send(1).unwrap();
    }

    #[test]
    fn sharded_macro_bykey_auto_shards() {
        let (tx, _rx) = mpmc!(u64, 128, ByKey);
        assert!(tx.shards().is_power_of_two());
        let shard = tx.shard_for("AAPL");
        assert!(shard < tx.shards());
    }

    #[test]
    fn sharded_macro_bykey_explicit_shards() {
        let (tx, rx) = mpmc!(u64, 128, ByKey, 4);
        assert_eq!(tx.shards(), 4);
        let shard = tx.shard_for("AAPL");
        tx.try_send("AAPL", 42).unwrap();
        assert_eq!(rx.receiver(shard).try_recv().unwrap(), 42);
    }

    #[test]
    fn sharded_macro_policy_ordering() {
        let (tx, rx) = mpmc!(u64, 64, ByKey, 4);
        let s = tx.shard_for("ETH");
        for i in 0..5u64 {
            tx.try_send("ETH", i).unwrap();
        }
        let mut buf = Vec::new();
        rx.receiver(s).recv_batch(&mut buf, 5);
        assert_eq!(buf, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn sharded_key_macro_auto_shards() {
        let (tx, rx) = mpmc!(u64, 256, ByKey);
        assert!(tx.shards().is_power_of_two());
        assert!(tx.shards() >= 1);
        let shard = tx.shard_for("AAPL");
        assert!(shard < tx.shards());
        drop(rx);
    }

    #[test]
    fn sharded_key_macro_explicit_shards() {
        let (tx, _rx) = mpmc!(u64, 512, ByKey, 8);
        assert_eq!(tx.shards(), 8);
        // cap 500 → 512 (degree 2)
        let (tx2, _rx2) = mpmc!(u64, 500, ByKey, 4);
        assert_eq!(tx2.shards(), 4);
    }

    #[test]
    fn sharded_key_macro_routing() {
        let (tx, rx) = mpmc!(u64, 64, ByKey, 4);
        let shard = tx.shard_for("AAPL");
        tx.try_send("AAPL", 42).unwrap();
        assert_eq!(rx.receiver(shard).try_recv().unwrap(), 42);
    }

    #[test]
    fn sharded_key_macro_shards_rounded() {
        // 6 → 8
        let (tx, _rx) = mpmc!(u64, 128, ByKey, 6);
        assert_eq!(tx.shards(), 8);
        // 12 → 16
        let (tx2, _rx2) = mpmc!(u64, 128, ByKey, 12);
        assert_eq!(tx2.shards(), 16);
    }

    // sharded_spsc! macro

    #[test]
    fn sharded_spsc_macro_auto_shards() {
        let ch = spsc!(u64, 256);
        assert!(ch.shards() >= 1);
    }

    #[test]
    fn sharded_spsc_macro_explicit_shards() {
        // shard_spsc does not require degree 2
        let ch = spsc!(u64, 256, 3);
        assert_eq!(ch.shards(), 3);
    }

    #[test]
    fn sharded_spsc_macro_into_pairs() {
        let ch = spsc!(u64, 64, 4);
        let pairs: Vec<_> = ch.into_pairs().collect();
        assert_eq!(pairs.len(), 4);
        for (shard_id, tx, rx) in pairs {
            tx.try_send(shard_id as u64).unwrap();
            assert_eq!(rx.try_recv().unwrap(), shard_id as u64);
        }
    }

    #[test]
    fn sharded_spsc_macro_take_pair() {
        let mut ch = spsc!(u64, 64, 4);
        let (tx0, rx0) = ch.take_pair(0).unwrap();
        let (tx2, rx2) = ch.take_pair(2).unwrap();
        assert_eq!(ch.remaining(), vec![1, 3]);
        tx0.try_send(10).unwrap();
        tx2.try_send(20).unwrap();
        assert_eq!(rx0.try_recv().unwrap(), 10);
        assert_eq!(rx2.try_recv().unwrap(), 20);
    }

    #[test]
    fn sharded_explicit_rounds_both() {
        // cap: 500 → 512, shards: 6 → 8
        let (tx, _rx) = mpmc!(u64, 500, ByKey, 6);
        assert_eq!(tx.shards(), 8);
        // Checking that routing is working
        let s = tx.shard_for("AAPL");
        assert!(s < 8);
    }

    #[test]
    fn sharded_explicit_exact_powers() {
        let (tx, _rx) = mpmc!(u64, 128, RoundRobin, 4);
        assert_eq!(tx.shards(), 4);
    }

    #[test]
    fn sharded_explicit_large_cap() {
        // 1000 → 1024, 12 → 16
        let (tx, _rx) = mpmc!(u64, 1000, ByKey, 12);
        assert_eq!(tx.shards(), 16);
        assert!(tx.shard_for("BTC") < 16);
    }

    #[test]
    fn sharded_send_recv_after_macro() {
        let (tx, rx) = mpmc!(u64, 100, ByKey, 4);
        let shard = tx.shard_for("ETH");
        tx.try_send("ETH", 999u64).unwrap();
        assert_eq!(rx.receiver(shard).try_recv().unwrap(), 999);
    }
}
