    /// Internal helper for error paths of keyed batch methods:
    /// returns the failed group's remainder AND every not-yet-attempted
    /// group back into `buf`. Without this, the early `return Err` would
    /// silently DROP all unprocessed groups (data loss).
    /// NOTE: after an error `buf` holds elements grouped by shard, not in
    /// the original insertion order; per-key FIFO order within each group
    /// is preserved, which is the only ordering ShardKey guarantees.
    #[inline]
    pub fn refill_on_error<T>(
        buf: &mut Vec<T>,
        failed_group: Vec<T>,
        remaining: impl Iterator<Item = (usize, Vec<T>)>,
    ) {
        buf.extend(failed_group);
        for (_, g) in remaining {
            buf.extend(g);
        }
    }