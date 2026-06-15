/// BATCH DRAIN: Safe stop all 6 sync/async × shard_key/round_robin/spsc combinations.

use std::mem;

/// SYNC batch drain for all 3 channels.
/// The correct order is sewn: drain buf → check dc. `handler` for each element;
/// `acc` accumulates Output. Only comes out when the channel is closed and
/// empty without losing the last batch.
pub fn drain_batch<T, F, H, O>(max: usize, mut recv: F, mut handler: H, init: O) -> O
where
    F: FnMut(&mut Vec<T>, usize) -> (usize, bool),
    H: FnMut(T, &mut O),
{
    let mut buf: Vec<T> = Vec::with_capacity(max);
    let mut acc = init;
    loop {
        let (_n, dc) = recv(&mut buf, max);
        for v in buf.drain(..) {
            handler(v, &mut acc);
        } // process everything first
        if dc {
            break;
        } // then check the closure
    }
    acc
}

/// SYNC hook receives the ENTIRE batch of ownership (Vec<T>) and processes it AT ONCE for calculations across the entire array
/// (sum, min/max, SIMD, statistics), rather than element wise.
/// Returns an empty Vec back allocation is reused. on_item NO: the point is to process the entire slice.
///```ignore
///drain_batch_sink(64, |buf, m| rx.recv_batch(buf, m),
/// |batch: Vec<T>, acc: &mut O| {
///   acc.sum += batch.iter().sum();   // calculation over the entire array
///   let mut b = batch; b.clear(); b // return allocation
///   },
/// init)
///```
/// The drain → check invariant is preserved: the last nonempty batch (with dc) too
/// goes to sink. For an empty final (n=0, dc=true) sink is not called.
pub fn drain_batch_sink<T, F, S, O>(max: usize, mut recv: F, mut sink: S, init: O) -> O
where
    F: FnMut(&mut Vec<T>, usize) -> (usize, bool),
    S: FnMut(Vec<T>, &mut O) -> Vec<T>,
{
    let mut buf: Vec<T> = Vec::with_capacity(max);
    let mut acc = init;
    loop {
        let (n, dc) = recv(&mut buf, max);
        if n > 0 {
            let batch = mem::take(&mut buf); // take possession of the entire batch
            buf = sink(batch, &mut acc); // process at once, return empty Vec
        }
        if dc {
            break;
        }
    }
    acc
}

/// ASYNC batch drain for all 3 channels.
/// Reception: `recv` owns both receiver and buffer (takes by value, returns
/// `(rx, buf, n, dc)`). future does not borrow external `&mut` → lifetime is clean,
/// Send is intact. Both resources are reused via back and forth.
/// The drain→check invariant is the same as in sync.
pub async fn drain_batch_async<R, F, Fut, T, H, O>(
    mut rx: R,
    max: usize,
    recv: F,
    mut handler: H,
    init: O,
) -> O
where
    F: Fn(R, Vec<T>, usize) -> Fut,
    Fut: Future<Output = (R, Vec<T>, usize, bool)>,
    H: FnMut(T, &mut O),
{
    let mut buf: Vec<T> = Vec::with_capacity(max);
    let mut acc = init;
    loop {
        let (r, mut b, _n, dc) = recv(rx, buf, max).await;
        rx = r;
        for v in b.drain(..) {
            handler(v, &mut acc);
        } // Process fiest
        buf = b;
        if dc {
            break;
        }
    }
    acc
}

/// ASYNC batch drain, where the hook takes ownership of the ENTIRE batch (Vec<T>) and
/// returns it (empty) back to send the array over the network/to the database in one
/// an async call, WITHOUT copying. Allocation is reused.
/// `sink` gets the elements themselves (as opposed to element-wise drain_batch_async):
/// ```ignore
/// drain_batch_async_sink(rx, max,
/// |rx, mut buf, max| async move {
/// let (n, dc) = rx.recv_batch_async(&mut buf, max).await; (rx, buf, n, dc) },
/// |mut batch: Vec<T>| async move {
/// socket.write_all(&serialize(&batch)).await?;  // send the entire batch
/// batch.clear(); batch // return allocation
/// }).await
/// ```
/// `O` accumulator (as in sync drain_batch_sink), but passed by OWNERSHIP
/// `(Vec<T>, O) -> Future<(Vec<T>, O)>`: async closure cannot hold
/// `&mut O` via .await (lifetime), so acc leaves and returns.
/// The drain → check invariant is preserved: the last non empty batch also goes to sink.
pub async fn drain_batch_async_sink<R, F, Fut, T, S, SFut, O>(
    mut rx: R,
    max: usize,
    recv: F,
    mut sink: S,
    init: O,
) -> O
where
    F: Fn(R, Vec<T>, usize) -> Fut,
    Fut: Future<Output = (R, Vec<T>, usize, bool)>,
    S: FnMut(Vec<T>, O) -> SFut,
    SFut: Future<Output = (Vec<T>, O)>,
{
    let mut buf: Vec<T> = Vec::with_capacity(max);
    let mut acc = init;
    loop {
        let (r, b, n, dc) = recv(rx, buf, max).await;
        rx = r;
        if n > 0 {
            let (empty, a) = sink(b, acc).await; //whole batch + acc → sink, (empty Vec, acc) back
            buf = empty;
            acc = a;
        } else {
            buf = b;
        }
        if dc {
            break;
        }
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::{mpmc::shard_key, spsc::shard_spsc};

    /// Regression on a window (n>0, dc=true) in one call: prefill=1, close,
    /// then drain. A naive `if dc { break }` before processing would lose the element.
    /// drain_batch must return 1.
    #[test]
    fn drain_batch_keeps_last_when_n_and_dc_together() {
        for prefill in [1u64, 2, 5, 255, 256] {
            let mut ch = shard_spsc::<u64, 256>(1);
            let (tx, rx) = ch.take_pair(0).unwrap();
            for i in 0..prefill {
                tx.try_send(i).unwrap();
            }
            drop(tx); // закрыли ДО чтения → первый recv_batch даст (n, dc=true)
            let got = drain_batch(
                256,
                move |buf: &mut Vec<u64>, m| rx.recv_batch(buf, m),
                |_v, c: &mut u64| *c += 1,
                0u64,
            );
            assert_eq!(got, prefill, "prefill={prefill}: batch-drain lost elements");
        }
    }

    /// drain_batch_sink: calculation for the ENTIRE array at once (sum+max),
    /// batch comes in its entirety, the allocation is reused (ptr is stable).
    #[test]
    fn drain_batch_sink_computes_over_whole_array() {
        for prefill in [1u64, 5, 64, 200] {
            let mut ch = shard_spsc::<u64, 256>(1);
            let (tx, rx) = ch.take_pair(0).unwrap();
            for i in 1..=prefill {
                tx.try_send(i).unwrap();
            }
            drop(tx);

            // acc = (sum, max, batches, sink_ptr_changes)
            let (sum, mx, batches) = drain_batch_sink(
                64,
                move |buf: &mut Vec<u64>, m| rx.recv_batch(buf, m),
                |batch: Vec<u64>, acc: &mut (u64, u64, u64)| {
                    acc.0 += batch.iter().sum::<u64>(); // calculation by array
                    acc.1 = acc.1.max(batch.iter().copied().max().unwrap_or(0));
                    acc.2 += 1;
                    let mut b = batch;
                    b.clear();
                    b // return allocation
                },
                (0u64, 0u64, 0u64),
            );
            assert_eq!(
                sum,
                (1..=prefill).sum::<u64>(),
                "prefill={prefill}: sum is wrong"
            );
            assert_eq!(mx, prefill, "prefill={prefill}: max false");
            assert!(batches >= 1, "prefill={prefill}: sink didn't volunteer");
        }
    }

    /// sink is not called for an empty final (n=0, dc=true).
    #[test]
    fn drain_batch_sink_no_call_on_empty_final() {
        let mut ch = shard_spsc::<u64, 256>(1);
        let (tx, rx) = ch.take_pair(0).unwrap();
        drop(tx); // закрыт пустым
        let calls = drain_batch_sink(
            64,
            move |buf: &mut Vec<u64>, m| rx.recv_batch(buf, m),
            |batch: Vec<u64>, acc: &mut u64| {
                *acc += 1;
                let mut b = batch;
                b.clear();
                b
            },
            0u64,
        );
        assert_eq!(calls, 0, "sink should not be called for an empty final");
    }

    /// drain_batch_sink: the entire batch comes to the sink as a whole, contents
    /// assembled from batches == the original one (no losses, no duplicates).
    /// Sync mirror async_sink_sends_whole_batch.
    #[test]
    fn drain_batch_sink_collects_whole_batch() {
        let mut ch = shard_spsc::<u64, 256>(1);
        let (tx, rx) = ch.take_pair(0).unwrap();
        for i in 0..200u64 {
            tx.try_send(i).unwrap();
        }
        drop(tx);

        // acc = (collected, sink_calls): we accumulate all the elements and count the calls
        let (collected, calls) = drain_batch_sink(
            64,
            move |buf: &mut Vec<u64>, m| rx.recv_batch(buf, m),
            |batch: Vec<u64>, acc: &mut (Vec<u64>, u64)| {
                acc.0.extend(batch.iter().copied()); // collect the whole batch
                acc.1 += 1; // call counter
                let mut b = batch;
                b.clear();
                b
            },
            (Vec::new(), 0u64),
        );

        assert_eq!(
            collected.len(),
            200,
            "not all elements went through the sink"
        );
        assert!(calls >= 1, "sink didn't volunteer");
        // содержимое == исходному (0..200), ни потерь, ни дублей
        let mut got = collected;
        got.sort_unstable();
        assert_eq!(got, (0..200u64).collect::<Vec<_>>(), "contents != original");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_drain_batch_keeps_last() {
        // SPSC
        for prefill in [1u64, 2, 5, 255, 256] {
            let ch = shard_spsc::<u64, 256>(1);
            let (tx, rx) = ch.into_wrapped_pairs().next().unwrap();
            for i in 0..prefill {
                tx.try_send(i).unwrap();
            }
            drop(tx);
            let got = drain_batch_async(
                rx,
                256,
                |rx, mut buf, max| async move {
                    let (n, dc) = rx.recv_batch_async(&mut buf, max).await;
                    (rx, buf, n, dc)
                },
                |_v, c: &mut u64| *c += 1,
                0u64,
            )
            .await;
            assert_eq!(got, prefill, "spsc async prefill={prefill}");
        }

        // MPMC (key)
        for prefill in [1u64, 5, 256] {
            let (tx, rx) = shard_key::<u64, 256>(1);
            for i in 0..prefill {
                tx.try_send("K", i).unwrap();
            }
            drop(tx);
            let r = rx.into_receivers().into_iter().next().unwrap();
            let got = drain_batch_async(
                r,
                256,
                |rx, mut buf, max| async move {
                    let (n, dc) = rx.recv_batch_async(&mut buf, max).await;
                    (rx, buf, n, dc)
                },
                |_v, c: &mut u64| *c += 1,
                0u64,
            )
            .await;
            assert_eq!(got, prefill, "mpmc async prefill={prefill}");
        }
    }

    /// drain_batch_async_sink: the entire batch goes into the sink, elements are not lost,
    /// number of sink calls = number of non empty batches (not number of elements).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_sink_sends_whole_batch() {
        use crate::channel::spsc::shard_spsc;
        use std::sync::{
            Arc,
            atomic::{AtomicU64, Ordering::Relaxed},
        };

        let ch = shard_spsc::<u64, 256>(1);
        let (tx, rx) = ch.into_wrapped_pairs().next().unwrap();
        for i in 0..200u64 {
            tx.try_send(i).unwrap();
        }
        drop(tx);

        let sink_calls = Arc::new(AtomicU64::new(0));
        let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sc = sink_calls.clone();
        let col = collected.clone();

        // acc = u64 element counter (demonstrates O accumulator)
        let total = drain_batch_async_sink(
            rx,
            256,
            |rx, mut buf, max| async move {
                let (n, dc) = rx.recv_batch_async(&mut buf, max).await;
                (rx, buf, n, dc)
            },
            |mut batch: Vec<u64>, mut acc: u64| {
                let sc = sc.clone();
                let col = col.clone();
                async move {
                    sc.fetch_add(1, Relaxed);
                    acc += batch.len() as u64; // count in acc
                    col.lock().unwrap().extend(batch.iter().copied());
                    batch.clear();
                    (batch, acc) // return (allocation, acc)
                }
            },
            0u64,
        )
        .await;

        assert_eq!(total, 200, "not all elements went through the sink");
        assert!(sink_calls.load(Relaxed) >= 1, "sink didn't volunteer");
        //collected in sink == original data (no losses, no duplicates)
        let mut got = collected.lock().unwrap().clone();
        got.sort_unstable();
        assert_eq!(
            got,
            (0..200u64).collect::<Vec<_>>(),
            "batch contents != original"
        );
    }
}
